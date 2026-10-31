//! The pool of the dependencies of a service.
//!
//! A service adds a dependency (the hash of a service, and a configuration), and takes an
//! instance of it with [`ServiceHandle::acquire`]. The pool starts one if none is idle. The
//! [`Lease`] gives the instance back when it is dropped. [`DependencyManager::maintain`]
//! stops the idle instances that nobody used for a while and the ones that are broken.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use celaut_proto::celaut::Configuration;
use prost::Message;
use tokio::time::Instant;

use crate::config::grpc_uri;
use crate::gateway::Node;
use crate::instance::ServiceInstance;
use crate::Error;

/// When the pool gives an instance up.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// An idle instance that nobody used for this long is stopped.
    pub idle_timeout: Duration,
    /// More timeouts than this, with a failed liveness check, make a zombie.
    pub pass_timeout_times: u32,
    /// More failures than this make a zombie.
    pub failed_attempts: u32,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            idle_timeout: Duration::from_secs(60 * 60),
            pass_timeout_times: 5,
            failed_attempts: 20,
        }
    }
}

/// What one pass of [`DependencyManager::maintain`] did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct MaintenanceReport {
    /// The instances that it stopped.
    pub stopped: usize,
    /// The tokens of instances that it could not stop. They can still run and bill.
    pub leaked: Vec<String>,
}

type PoolKey = (String, Vec<u8>);

struct Pool {
    service_hash: [u8; 32],
    configuration: Configuration,
    idle: VecDeque<ServiceInstance>,
}

/// The pool of every dependency of the service.
pub struct DependencyManager<N: Node> {
    node: N,
    limits: Limits,
    pools: Mutex<HashMap<PoolKey, Pool>>,
}

impl<N: Node> DependencyManager<N> {
    /// A pool on `node`.
    pub fn new(node: N, limits: Limits) -> Arc<Self> {
        Arc::new(DependencyManager {
            node,
            limits,
            pools: Mutex::new(HashMap::new()),
        })
    }

    /// Add a dependency: the service `service_hash` with `configuration`. Adding the same
    /// pair again gives a handle to the same pool.
    pub fn add_service(
        self: &Arc<Self>,
        service_hash: [u8; 32],
        configuration: Configuration,
    ) -> ServiceHandle<N> {
        let key = (hex(&service_hash), configuration.encode_to_vec());
        self.pools
            .lock()
            .unwrap()
            .entry(key.clone())
            .or_insert_with(|| Pool {
                service_hash,
                configuration,
                idle: VecDeque::new(),
            });
        ServiceHandle {
            manager: self.clone(),
            key,
        }
    }

    /// How many instances wait in the pools.
    pub fn idle_count(&self) -> usize {
        self.pools
            .lock()
            .unwrap()
            .values()
            .map(|p| p.idle.len())
            .sum()
    }

    /// One pass over the idle instances: stop the ones that nobody used for
    /// [`Limits::idle_timeout`] and the zombies, keep the others. `is_alive` is the
    /// liveness check of an instance, from its `ip:port`; it runs only for an instance
    /// that timed out too often. A failed stop is in the report, never an error, so the
    /// pass goes on with the other instances.
    pub async fn maintain<F, Fut>(&self, is_alive: F) -> MaintenanceReport
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = bool>,
    {
        // The instances leave the pools for the pass: a service that asks for one meanwhile
        // starts another, and nothing waits on a stop that is slow.
        let taken: Vec<(PoolKey, Vec<ServiceInstance>)> = {
            let mut pools = self.pools.lock().unwrap();
            pools
                .iter_mut()
                .map(|(k, p)| (k.clone(), p.idle.drain(..).collect()))
                .collect()
        };
        let mut report = MaintenanceReport::default();
        let now = Instant::now();
        for (key, instances) in taken {
            let mut kept = Vec::new();
            for instance in instances {
                let idle_too_long =
                    now.duration_since(instance.last_used) > self.limits.idle_timeout;
                let alive = if instance.timeouts > self.limits.pass_timeout_times {
                    is_alive(instance.uri.clone()).await
                } else {
                    true
                };
                let zombie = instance.is_zombie(
                    self.limits.pass_timeout_times,
                    self.limits.failed_attempts,
                    alive,
                );
                if idle_too_long || zombie {
                    match self.node.stop(&instance.token).await {
                        Ok(_) => report.stopped += 1,
                        Err(_) => report.leaked.push(instance.token),
                    }
                } else {
                    kept.push(instance);
                }
            }
            if let Some(pool) = self.pools.lock().unwrap().get_mut(&key) {
                pool.idle.extend(kept);
            }
        }
        report
    }

    /// Run [`DependencyManager::maintain`] every `interval`, until the task is dropped or
    /// aborted.
    pub fn spawn_maintenance<F, Fut>(
        self: &Arc<Self>,
        interval: Duration,
        is_alive: F,
    ) -> tokio::task::JoinHandle<()>
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = bool> + Send + 'static,
    {
        let manager = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                manager.maintain(&is_alive).await;
            }
        })
    }

    /// Stop every idle instance, for the end of the service. The instances that a lease
    /// holds are not here: their holders give them back, or call [`Lease::stop`].
    pub async fn shutdown(&self) -> MaintenanceReport {
        let taken: Vec<ServiceInstance> = {
            let mut pools = self.pools.lock().unwrap();
            pools.values_mut().flat_map(|p| p.idle.drain(..)).collect()
        };
        let mut report = MaintenanceReport::default();
        for instance in taken {
            match self.node.stop(&instance.token).await {
                Ok(_) => report.stopped += 1,
                Err(_) => report.leaked.push(instance.token),
            }
        }
        report
    }

    fn give_back(&self, key: &PoolKey, mut instance: ServiceInstance) {
        instance.last_used = Instant::now();
        if let Some(pool) = self.pools.lock().unwrap().get_mut(key) {
            pool.idle.push_back(instance);
        }
    }
}

/// A dependency of the service.
pub struct ServiceHandle<N: Node> {
    manager: Arc<DependencyManager<N>>,
    key: PoolKey,
}

impl<N: Node> ServiceHandle<N> {
    /// An idle instance of the dependency, or a new one if none is idle.
    pub async fn acquire(&self) -> Result<Lease<N>, Error> {
        let (idle, hash, configuration) = {
            let mut pools = self.manager.pools.lock().unwrap();
            let pool = pools
                .get_mut(&self.key)
                .ok_or_else(|| Error::Protocol("the dependency was removed".to_string()))?;
            (
                pool.idle.pop_front(),
                pool.service_hash,
                pool.configuration.clone(),
            )
        };
        let instance = match idle {
            Some(instance) => instance,
            None => {
                let started = self.manager.node.start(hash, configuration).await?;
                let uri = started
                    .instance
                    .as_ref()
                    .ok_or_else(|| Error::Protocol("the node gave no addresses".to_string()))
                    .and_then(grpc_uri);
                match uri {
                    Ok(uri) => ServiceInstance::new(uri, started.token),
                    Err(e) => {
                        // An instance that the service cannot use must not stay on the node.
                        let _ = self.manager.node.stop(&started.token).await;
                        return Err(e);
                    }
                }
            }
        };
        Ok(Lease {
            manager: self.manager.clone(),
            key: self.key.clone(),
            instance: Some(instance),
        })
    }
}

/// An instance that the service holds. Dropping it gives the instance back to the pool.
pub struct Lease<N: Node> {
    manager: Arc<DependencyManager<N>>,
    key: PoolKey,
    instance: Option<ServiceInstance>,
}

impl<N: Node> Lease<N> {
    /// The instance.
    pub fn instance(&self) -> &ServiceInstance {
        self.instance.as_ref().expect("a lease holds its instance")
    }

    /// The instance, to report how a call went.
    pub fn instance_mut(&mut self) -> &mut ServiceInstance {
        self.instance.as_mut().expect("a lease holds its instance")
    }

    /// `ip:port` of the gRPC port of the instance.
    pub fn uri(&self) -> &str {
        &self.instance().uri
    }

    /// Stop the instance now instead of giving it back. Use it for an instance that you
    /// know is broken. If the stop fails the error says that the instance may still run.
    pub async fn stop(mut self) -> Result<u128, Error> {
        let instance = self.instance.take().expect("a lease holds its instance");
        self.manager.node.stop(&instance.token).await
    }
}

impl<N: Node> Drop for Lease<N> {
    fn drop(&mut self) {
        if let Some(instance) = self.instance.take() {
            self.manager.give_back(&self.key, instance);
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use celaut_proto::celaut::instance::{Uri, UriSlot};
    use celaut_proto::celaut::service::{api::Slot, Api};
    use celaut_proto::celaut::{Instance, ServiceInstance as Started};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct FakeNode {
        started: AtomicUsize,
        stopped: Mutex<Vec<String>>,
        fail_stop: bool,
        no_addresses: bool,
    }

    impl Node for Arc<FakeNode> {
        async fn start(&self, _hash: [u8; 32], _c: Configuration) -> Result<Started, Error> {
            let n = self.started.fetch_add(1, Ordering::SeqCst);
            let instance = if self.no_addresses {
                Instance::default()
            } else {
                Instance {
                    api: Some(Api {
                        slot: vec![Slot {
                            port: 50051,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    uri_slot: vec![UriSlot {
                        internal_port: 50051,
                        uri: vec![Uri {
                            ip: "10.0.0.2".into(),
                            port: 40000 + n as i32,
                        }],
                    }],
                }
            };
            Ok(Started {
                token: format!("t{n}"),
                instance: Some(instance),
                receipt: None,
            })
        }

        async fn stop(&self, token: &str) -> Result<u128, Error> {
            if self.fail_stop {
                return Err(Error::Protocol("down".into()));
            }
            self.stopped.lock().unwrap().push(token.to_string());
            Ok(0)
        }
    }

    fn manager(node: &Arc<FakeNode>) -> Arc<DependencyManager<Arc<FakeNode>>> {
        DependencyManager::new(node.clone(), Limits::default())
    }

    #[tokio::test]
    async fn an_instance_is_started_once_and_reused() {
        let node = Arc::new(FakeNode::default());
        let m = manager(&node);
        let dep = m.add_service([1; 32], Configuration::default());
        let first = dep.acquire().await.unwrap();
        assert_eq!(first.uri(), "10.0.0.2:40000");
        drop(first);
        let second = dep.acquire().await.unwrap();
        assert_eq!(second.instance().token, "t0");
        assert_eq!(node.started.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn two_leases_at_once_start_two_instances() {
        let node = Arc::new(FakeNode::default());
        let m = manager(&node);
        let dep = m.add_service([1; 32], Configuration::default());
        let a = dep.acquire().await.unwrap();
        let b = dep.acquire().await.unwrap();
        assert_ne!(a.instance().token, b.instance().token);
        drop((a, b));
        assert_eq!(m.idle_count(), 2);
    }

    #[tokio::test]
    async fn the_same_service_with_the_same_configuration_shares_a_pool() {
        let node = Arc::new(FakeNode::default());
        let m = manager(&node);
        drop(
            m.add_service([1; 32], Configuration::default())
                .acquire()
                .await
                .unwrap(),
        );
        let again = m.add_service([1; 32], Configuration::default());
        again.acquire().await.unwrap();
        assert_eq!(node.started.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn an_instance_that_nobody_used_is_stopped() {
        let node = Arc::new(FakeNode::default());
        let m = manager(&node);
        let dep = m.add_service([1; 32], Configuration::default());
        drop(dep.acquire().await.unwrap());
        tokio::time::advance(Limits::default().idle_timeout + Duration::from_secs(1)).await;
        let report = m.maintain(|_| async { true }).await;
        assert_eq!(
            report,
            MaintenanceReport {
                stopped: 1,
                leaked: vec![]
            }
        );
        assert_eq!(*node.stopped.lock().unwrap(), vec!["t0".to_string()]);
        assert_eq!(m.idle_count(), 0);
    }

    #[tokio::test]
    async fn a_recent_instance_stays() {
        let node = Arc::new(FakeNode::default());
        let m = manager(&node);
        let dep = m.add_service([1; 32], Configuration::default());
        drop(dep.acquire().await.unwrap());
        let report = m.maintain(|_| async { true }).await;
        assert_eq!(report.stopped, 0);
        assert_eq!(m.idle_count(), 1);
    }

    #[tokio::test]
    async fn a_broken_instance_is_stopped() {
        let node = Arc::new(FakeNode::default());
        let m = manager(&node);
        let dep = m.add_service([1; 32], Configuration::default());
        let mut lease = dep.acquire().await.unwrap();
        for _ in 0..=Limits::default().failed_attempts {
            lease.instance_mut().record_failure();
        }
        drop(lease);
        assert_eq!(m.maintain(|_| async { true }).await.stopped, 1);
    }

    #[tokio::test]
    async fn a_failed_stop_is_reported_as_a_leak_and_the_pass_goes_on() {
        let node = Arc::new(FakeNode {
            fail_stop: true,
            ..Default::default()
        });
        let m = manager(&node);
        let dep = m.add_service([1; 32], Configuration::default());
        let (a, b) = (dep.acquire().await.unwrap(), dep.acquire().await.unwrap());
        drop((a, b));
        let report = m.shutdown().await;
        assert_eq!(report.stopped, 0);
        assert_eq!(report.leaked.len(), 2);
    }

    #[tokio::test]
    async fn an_instance_with_no_grpc_address_is_stopped_and_is_an_error() {
        let node = Arc::new(FakeNode {
            no_addresses: true,
            ..Default::default()
        });
        let m = manager(&node);
        let dep = m.add_service([1; 32], Configuration::default());
        assert!(dep.acquire().await.is_err());
        assert_eq!(*node.stopped.lock().unwrap(), vec!["t0".to_string()]);
    }
}
