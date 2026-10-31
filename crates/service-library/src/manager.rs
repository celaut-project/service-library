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

/// What is special about one dependency. What is `None` is what the pool has in [`Limits`].
#[derive(Debug, Clone, Copy)]
pub struct ServiceOptions {
    /// How many times a start is tried before it is an error. A start that fails with an
    /// answer of the node that is not a gateway failure (a receipt that does not pass, an
    /// instance with no gRPC address) is not tried again.
    pub launch_attempts: u32,
    /// Overrides [`Limits::idle_timeout`].
    pub idle_timeout: Option<Duration>,
    /// Overrides [`Limits::pass_timeout_times`].
    pub pass_timeout_times: Option<u32>,
    /// Overrides [`Limits::failed_attempts`].
    pub failed_attempts: Option<u32>,
    /// For [`DependencyManager::add_package`]: check that each block of the file holds what
    /// its id names before it is used. It reads the whole file once.
    pub verify_package: bool,
}

impl Default for ServiceOptions {
    fn default() -> Self {
        ServiceOptions {
            launch_attempts: 5,
            idle_timeout: None,
            pass_timeout_times: None,
            failed_attempts: None,
            verify_package: true,
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

/// What the pool does with the start receipt of a new instance (nodo#419).
#[derive(Debug, Clone, Default)]
pub enum ReceiptCheck {
    /// Do not look at it.
    #[default]
    Off,
    /// Check it, and stop an instance whose receipt fails. `peer_id` is the identity of the
    /// node, if the service knows it: without it the check proves only that some key signed
    /// the receipt. `require` also refuses an instance with no receipt (a node with no
    /// identity key gives none).
    Check {
        peer_id: Option<String>,
        require: bool,
    },
}

type PoolKey = (String, Vec<u8>);

/// A service that comes as a file: the first start sends it, the next ones use the hash.
struct Package {
    path: std::path::PathBuf,
    info: crate::package::PackageInfo,
    sent: bool,
}

struct Pool {
    options: ServiceOptions,
    package: Option<Package>,
    service_hash: [u8; 32],
    configuration: Configuration,
    idle: VecDeque<ServiceInstance>,
}

/// The pool of every dependency of the service.
pub struct DependencyManager<N: Node> {
    node: N,
    limits: Limits,
    receipts: ReceiptCheck,
    pools: Mutex<HashMap<PoolKey, Pool>>,
}

impl<N: Node> DependencyManager<N> {
    /// A pool on `node`.
    pub fn new(node: N, limits: Limits) -> Arc<Self> {
        Self::with_receipts(node, limits, ReceiptCheck::Off)
    }

    /// A pool on `node` that checks the start receipt of each new instance.
    pub fn with_receipts(node: N, limits: Limits, receipts: ReceiptCheck) -> Arc<Self> {
        Arc::new(DependencyManager {
            node,
            limits,
            receipts,
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
        self.add_service_with(service_hash, configuration, ServiceOptions::default())
    }

    /// [`DependencyManager::add_service`] with options for this dependency. If the pool
    /// exists already, it keeps the options that it has.
    pub fn add_service_with(
        self: &Arc<Self>,
        service_hash: [u8; 32],
        configuration: Configuration,
        options: ServiceOptions,
    ) -> ServiceHandle<N> {
        let key = (hex(&service_hash), configuration.encode_to_vec());
        self.pools
            .lock()
            .unwrap()
            .entry(key.clone())
            .or_insert_with(|| Pool {
                options,
                package: None,
                service_hash,
                configuration,
                idle: VecDeque::new(),
            });
        ServiceHandle {
            manager: self.clone(),
            key,
        }
    }

    /// Add a dependency that comes as a `.bee` file. The first instance sends the file to
    /// the node, and the following ones start by hash. The file is read now only up to
    /// the Service, to know the id.
    pub async fn add_package(
        self: &Arc<Self>,
        path: impl Into<std::path::PathBuf>,
        configuration: Configuration,
    ) -> Result<ServiceHandle<N>, Error> {
        self.add_package_with(path, configuration, ServiceOptions::default())
            .await
    }

    /// [`DependencyManager::add_package`] with options for this dependency.
    pub async fn add_package_with(
        self: &Arc<Self>,
        path: impl Into<std::path::PathBuf>,
        configuration: Configuration,
        options: ServiceOptions,
    ) -> Result<ServiceHandle<N>, Error> {
        let path = path.into();
        let (path, info) = tokio::task::spawn_blocking(move || {
            let info = crate::package::inspect(&path)?;
            if options.verify_package {
                crate::package::verify_blocks(&path)?;
            }
            Ok::<_, Error>((path, info))
        })
        .await
        .map_err(|e| Error::Protocol(format!("the check of the package failed: {e}")))??;
        Ok(self.register_package(path, info, configuration, options))
    }

    /// Add a dependency that is stored as directories of the block store: the Service in
    /// `root/__services__/<hash>` and the Metadata, if there is one, in
    /// `root/__metadata__/<hash>` (the hash is the lowercase hex of `service_id`), with the
    /// blocks in `block_dir`. The first instance sends the objects to the node, and the
    /// following ones start by hash. A block that is a single file is checked against its id
    /// when it is read to be sent; `options.verify_package` has no effect here.
    pub async fn add_directory_package(
        self: &Arc<Self>,
        root: impl Into<std::path::PathBuf>,
        block_dir: impl Into<std::path::PathBuf>,
        service_id: [u8; 32],
        configuration: Configuration,
    ) -> Result<ServiceHandle<N>, Error> {
        self.add_directory_package_with(
            root,
            block_dir,
            service_id,
            configuration,
            ServiceOptions::default(),
        )
        .await
    }

    /// [`DependencyManager::add_directory_package`] with options for this dependency.
    pub async fn add_directory_package_with(
        self: &Arc<Self>,
        root: impl Into<std::path::PathBuf>,
        block_dir: impl Into<std::path::PathBuf>,
        service_id: [u8; 32],
        configuration: Configuration,
        options: ServiceOptions,
    ) -> Result<ServiceHandle<N>, Error> {
        let root = root.into();
        let block_dir = block_dir.into();
        let (root, info) = tokio::task::spawn_blocking(move || {
            let info = crate::package::inspect_directory(&root, &block_dir, service_id)?;
            Ok::<_, Error>((root, info))
        })
        .await
        .map_err(|e| Error::Protocol(format!("the check of the package failed: {e}")))??;
        Ok(self.register_package(root, info, configuration, options))
    }

    fn register_package(
        self: &Arc<Self>,
        path: std::path::PathBuf,
        info: crate::package::PackageInfo,
        configuration: Configuration,
        options: ServiceOptions,
    ) -> ServiceHandle<N> {
        let handle = self.add_service_with(info.service_id, configuration, options);
        if let Some(pool) = self.pools.lock().unwrap().get_mut(&handle.key) {
            if pool.package.is_none() {
                pool.package = Some(Package {
                    path,
                    info,
                    sent: false,
                });
            }
        }
        handle
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
        let taken: Vec<(PoolKey, Limits, Vec<ServiceInstance>)> = {
            let mut pools = self.pools.lock().unwrap();
            pools
                .iter_mut()
                .map(|(k, p)| {
                    (
                        k.clone(),
                        self.limits_of(&p.options),
                        p.idle.drain(..).collect(),
                    )
                })
                .collect()
        };
        let mut report = MaintenanceReport::default();
        let now = Instant::now();
        for (key, limits, instances) in taken {
            let mut kept = Vec::new();
            for instance in instances {
                let idle_too_long = now.duration_since(instance.last_used) > limits.idle_timeout;
                let alive = if instance.timeouts > limits.pass_timeout_times {
                    is_alive(instance.uri.clone()).await
                } else {
                    true
                };
                let zombie =
                    instance.is_zombie(limits.pass_timeout_times, limits.failed_attempts, alive);
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

    fn check_receipt(
        &self,
        started: &celaut_proto::celaut::ServiceInstance,
        hash: &[u8; 32],
        configuration: &Configuration,
    ) -> Result<(), Error> {
        let ReceiptCheck::Check { peer_id, require } = &self.receipts else {
            return Ok(());
        };
        let Some(receipt) = &started.receipt else {
            return if *require {
                Err(Error::Receipt("the node gave no receipt".to_string()))
            } else {
                Ok(())
            };
        };
        let initial = configuration.initial_mu.as_ref().map(|a| a.n.as_str());
        let faults = crate::receipt::check_start(
            receipt,
            peer_id.as_deref(),
            &started.token,
            hash,
            &configuration.environment_variables,
            initial,
        );
        if faults.is_empty() {
            Ok(())
        } else {
            Err(Error::Receipt(
                faults
                    .iter()
                    .map(|f| f.to_string())
                    .collect::<Vec<_>>()
                    .join("; "),
            ))
        }
    }

    fn limits_of(&self, options: &ServiceOptions) -> Limits {
        Limits {
            idle_timeout: options.idle_timeout.unwrap_or(self.limits.idle_timeout),
            pass_timeout_times: options
                .pass_timeout_times
                .unwrap_or(self.limits.pass_timeout_times),
            failed_attempts: options
                .failed_attempts
                .unwrap_or(self.limits.failed_attempts),
        }
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
    /// One attempt to start an instance: from the file the first time, by hash after that,
    /// then the checks of the receipt and of the address.
    async fn launch(
        &self,
        hash: &[u8; 32],
        configuration: &Configuration,
        to_send: Option<&(std::path::PathBuf, crate::package::PackageInfo)>,
    ) -> Result<ServiceInstance, Error> {
        let started = match to_send {
            Some((path, info)) => {
                let started = self
                    .manager
                    .node
                    .start_package(path, info, configuration.clone())
                    .await?;
                // The node has the service now: the next starts use the hash.
                if let Some(Package { sent, .. }) = self
                    .manager
                    .pools
                    .lock()
                    .unwrap()
                    .get_mut(&self.key)
                    .and_then(|p| p.package.as_mut())
                {
                    *sent = true;
                }
                started
            }
            None => {
                self.manager
                    .node
                    .start(*hash, configuration.clone())
                    .await?
            }
        };
        if let Err(e) = self.manager.check_receipt(&started, hash, configuration) {
            // An instance that the service does not trust must not stay on the node.
            let _ = self.manager.node.stop(&started.token).await;
            return Err(e);
        }
        let uri = started
            .instance
            .as_ref()
            .ok_or_else(|| Error::Protocol("the node gave no addresses".to_string()))
            .and_then(grpc_uri);
        match uri {
            Ok(uri) => Ok(ServiceInstance::new(uri, started.token)),
            Err(e) => {
                // An instance that the service cannot use must not stay on the node.
                let _ = self.manager.node.stop(&started.token).await;
                Err(e)
            }
        }
    }

    /// An idle instance of the dependency, or a new one if none is idle. The idle instance
    /// that was given back last is the first to go out, so the surplus ones stay unused and
    /// the pass of maintenance can stop them.
    pub async fn acquire(&self) -> Result<Lease<N>, Error> {
        let (idle, hash, configuration, to_send, launch_attempts) = {
            let mut pools = self.manager.pools.lock().unwrap();
            let pool = pools
                .get_mut(&self.key)
                .ok_or_else(|| Error::Protocol("the dependency was removed".to_string()))?;
            let to_send = pool
                .package
                .as_ref()
                .filter(|p| !p.sent)
                .map(|p| (p.path.clone(), p.info.clone()));
            (
                pool.idle.pop_back(),
                pool.service_hash,
                pool.configuration.clone(),
                to_send,
                pool.options.launch_attempts.max(1),
            )
        };
        let instance = match idle {
            Some(instance) => instance,
            None => {
                let mut attempt = 1;
                loop {
                    match self.launch(&hash, &configuration, to_send.as_ref()).await {
                        Ok(instance) => break instance,
                        // Only a gateway failure is tried again: a receipt that does not
                        // pass, or an instance with no address, would fail again.
                        Err(Error::Gateway {
                            operation,
                            unreachable,
                            message,
                            ..
                        }) => {
                            if attempt >= launch_attempts {
                                return Err(Error::Gateway {
                                    operation,
                                    attempts: attempt,
                                    unreachable,
                                    message,
                                });
                            }
                            tokio::time::sleep(Duration::from_millis(500 * u64::from(attempt)))
                                .await;
                            attempt += 1;
                        }
                        Err(other) => return Err(other),
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

/// Why a call to an instance failed ([`Lease::call`]).
#[derive(Debug, PartialEq, Eq)]
pub enum CallFailure<E> {
    /// The call took longer than its time limit.
    Timeout,
    /// The call gave an error.
    Failed(E),
}

/// The liveness check of an instance that the pool uses when you have no better one: the
/// port of the instance accepts a TCP connection within 5 s. Give it to
/// [`DependencyManager::maintain`].
pub async fn tcp_is_alive(uri: String) -> bool {
    matches!(
        tokio::time::timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(uri)).await,
        Ok(Ok(_))
    )
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

    /// Run a call to the instance and record how it went: a good answer clears the counts, a
    /// call that takes longer than `timeout` is a timeout, and an error is a failure. The pool
    /// stops an instance that fails or times out too often (see [`Limits`]).
    pub async fn call<T, E, F, Fut>(&mut self, timeout: Duration, f: F) -> Result<T, CallFailure<E>>
    where
        F: FnOnce(String) -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let uri = self.uri().to_string();
        match tokio::time::timeout(timeout, f(uri)).await {
            Ok(Ok(value)) => {
                self.instance_mut().record_success();
                Ok(value)
            }
            Ok(Err(e)) => {
                self.instance_mut().record_failure();
                Err(CallFailure::Failed(e))
            }
            Err(_) => {
                self.instance_mut().record_timeout();
                Err(CallFailure::Timeout)
            }
        }
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
    use bee_rpc::buffer::Buffer;
    use celaut_proto::celaut::instance::{Uri, UriSlot};
    use celaut_proto::celaut::service::{api::Slot, Api};
    use celaut_proto::celaut::{Instance, ServiceInstance as Started};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct FakeNode {
        started: AtomicUsize,
        packages: AtomicUsize,
        stopped: Mutex<Vec<String>>,
        fail_stop: bool,
        no_addresses: bool,
        /// How many starts fail, with an error of the gateway, before one works.
        fail_starts: AtomicUsize,
    }

    impl Node for Arc<FakeNode> {
        async fn start(&self, _hash: [u8; 32], _c: Configuration) -> Result<Started, Error> {
            if self
                .fail_starts
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(Error::Gateway {
                    operation: "starting a service".into(),
                    attempts: 1,
                    unreachable: true,
                    message: "down".into(),
                });
            }
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

        async fn start_package(
            &self,
            _path: &std::path::Path,
            info: &crate::package::PackageInfo,
            c: Configuration,
        ) -> Result<Started, Error> {
            self.packages.fetch_add(1, Ordering::SeqCst);
            self.start(info.service_id, c).await
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
    async fn a_package_is_sent_with_the_first_instance_and_the_next_start_by_hash() {
        use bee_rpc::bee_file::write_bee;
        use bee_rpc::frame::{serialize_part, Part};
        use celaut_proto::celaut::metadata::{hash_tag::Hash, HashTag};
        use celaut_proto::celaut::Metadata;
        let metadata = Metadata {
            hashtag: Some(HashTag {
                hash: vec![Hash {
                    r#type: crate::gateway::SHA3_256_TYPE.to_vec(),
                    value: vec![3; 32],
                }],
                ..Default::default()
            }),
            ..Default::default()
        };
        let path = std::env::temp_dir().join(format!("pool-{}.bee", std::process::id()));
        let mut buffers: Vec<_> = serialize_part(&Part::new(1, metadata.encode_to_vec())).collect();
        buffers.extend(serialize_part(&Part::new(2, vec![9])));
        write_bee(&mut std::fs::File::create(&path).unwrap(), buffers).unwrap();

        let node = Arc::new(FakeNode::default());
        let m = manager(&node);
        let dep = m
            .add_package(&path, Configuration::default())
            .await
            .unwrap();
        let (a, b) = (dep.acquire().await.unwrap(), dep.acquire().await.unwrap());
        drop((a, b));
        assert_eq!(node.started.load(Ordering::SeqCst), 2);
        assert_eq!(node.packages.load(Ordering::SeqCst), 1);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn a_directory_package_is_sent_once_and_a_missing_one_is_refused() {
        let id = [4u8; 32];
        let name: String = id.iter().map(|b| format!("{b:02x}")).collect();
        let root = std::env::temp_dir().join(format!("pool-dir-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("blocks")).unwrap();
        std::fs::create_dir_all(root.join("__services__")).unwrap();
        // A service object that is one plain file: `_.json` names the file 1.
        let service = root.join("__services__").join(&name);
        std::fs::create_dir_all(&service).unwrap();
        std::fs::write(service.join("_.json"), "[1]").unwrap();
        std::fs::write(service.join("1"), [9u8]).unwrap();

        let node = Arc::new(FakeNode::default());
        let m = manager(&node);
        let dep = m
            .add_directory_package(&root, root.join("blocks"), id, Configuration::default())
            .await
            .unwrap();
        let (a, b) = (dep.acquire().await.unwrap(), dep.acquire().await.unwrap());
        drop((a, b));
        assert_eq!(node.started.load(Ordering::SeqCst), 2);
        assert_eq!(node.packages.load(Ordering::SeqCst), 1);

        let missing = m
            .add_directory_package(
                &root,
                root.join("blocks"),
                [5; 32],
                Configuration::default(),
            )
            .await;
        assert!(missing.is_err());
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn a_missing_receipt_is_refused_only_when_it_is_required() {
        let check = |require| ReceiptCheck::Check {
            peer_id: None,
            require,
        };
        let node = Arc::new(FakeNode::default());
        let lax = DependencyManager::with_receipts(node.clone(), Limits::default(), check(false));
        assert!(lax
            .add_service([1; 32], Configuration::default())
            .acquire()
            .await
            .is_ok());

        let strict = DependencyManager::with_receipts(node.clone(), Limits::default(), check(true));
        let err = strict
            .add_service([2; 32], Configuration::default())
            .acquire()
            .await
            .err()
            .unwrap();
        assert!(matches!(err, Error::Receipt(_)));
        // The instance with no receipt was stopped.
        assert_eq!(node.stopped.lock().unwrap().len(), 1);
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

    #[tokio::test(start_paused = true)]
    async fn a_start_that_fails_is_tried_again() {
        let node = Arc::new(FakeNode {
            fail_starts: AtomicUsize::new(2),
            ..Default::default()
        });
        let m = manager(&node);
        let dep = m.add_service([1; 32], Configuration::default());
        assert!(dep.acquire().await.is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn a_start_that_never_works_says_how_many_times_it_tried() {
        let node = Arc::new(FakeNode {
            fail_starts: AtomicUsize::new(100),
            ..Default::default()
        });
        let m = manager(&node);
        let options = ServiceOptions {
            launch_attempts: 3,
            ..Default::default()
        };
        let dep = m.add_service_with([1; 32], Configuration::default(), options);
        match dep.acquire().await.err().unwrap() {
            Error::Gateway {
                attempts,
                unreachable,
                ..
            } => {
                assert_eq!(attempts, 3);
                assert!(unreachable);
            }
            other => panic!("{other}"),
        }
        assert_eq!(node.fail_starts.load(Ordering::SeqCst), 97);
    }

    #[tokio::test(start_paused = true)]
    async fn an_error_that_would_repeat_is_not_tried_again() {
        let node = Arc::new(FakeNode {
            no_addresses: true,
            ..Default::default()
        });
        let m = manager(&node);
        let dep = m.add_service([1; 32], Configuration::default());
        assert!(dep.acquire().await.is_err());
        assert_eq!(node.started.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn the_instance_given_back_last_goes_out_first() {
        let node = Arc::new(FakeNode::default());
        let m = manager(&node);
        let dep = m.add_service([1; 32], Configuration::default());
        let (a, b) = (dep.acquire().await.unwrap(), dep.acquire().await.unwrap());
        let (a_token, b_token) = (a.instance().token.clone(), b.instance().token.clone());
        drop(a);
        drop(b);
        assert_eq!(dep.acquire().await.unwrap().instance().token, b_token);
        assert_ne!(a_token, b_token);
    }

    #[tokio::test]
    async fn a_dependency_can_have_its_own_limit_of_failures() {
        let node = Arc::new(FakeNode::default());
        let m = manager(&node);
        let options = ServiceOptions {
            failed_attempts: Some(1),
            ..Default::default()
        };
        let dep = m.add_service_with([1; 32], Configuration::default(), options);
        let mut lease = dep.acquire().await.unwrap();
        lease.instance_mut().record_failure();
        lease.instance_mut().record_failure();
        drop(lease);
        // Two failures are more than its limit of 1, and less than the default of 20.
        assert_eq!(m.maintain(|_| async { true }).await.stopped, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_call_records_how_it_went() {
        let node = Arc::new(FakeNode::default());
        let m = manager(&node);
        let dep = m.add_service([1; 32], Configuration::default());
        let mut lease = dep.acquire().await.unwrap();
        let slow = lease
            .call(Duration::from_secs(1), |_| {
                std::future::pending::<Result<(), ()>>()
            })
            .await;
        assert_eq!(slow, Err(CallFailure::Timeout));
        assert_eq!(lease.instance().timeouts(), 1);
        let failed = lease
            .call(Duration::from_secs(1), |_| async { Err::<(), _>("no") })
            .await;
        assert_eq!(failed, Err(CallFailure::Failed("no")));
        assert_eq!(lease.instance().failures(), 1);
        let uri = lease
            .call(
                Duration::from_secs(1),
                |uri| async move { Ok::<_, ()>(uri) },
            )
            .await
            .unwrap();
        assert_eq!(uri, "10.0.0.2:40000");
        assert_eq!(
            (lease.instance().failures(), lease.instance().timeouts()),
            (0, 0)
        );
    }

    #[tokio::test]
    async fn a_package_with_a_bad_block_is_refused_unless_the_check_is_off() {
        use bee_rpc::bee_file::write_bee;
        use bee_rpc::buffer::buffer::{block::Hash as BlockHash, Block};
        use bee_rpc::frame::{serialize_part, Part};
        use celaut_proto::celaut::metadata::{hash_tag::Hash, HashTag};
        use celaut_proto::celaut::Metadata;
        let metadata = Metadata {
            hashtag: Some(HashTag {
                hash: vec![Hash {
                    r#type: crate::gateway::SHA3_256_TYPE.to_vec(),
                    value: vec![3; 32],
                }],
                ..Default::default()
            }),
            ..Default::default()
        };
        let marker = || Buffer {
            block: Some(Block {
                hashes: vec![BlockHash {
                    r#type: crate::gateway::SHA3_256_TYPE.to_vec(),
                    value: vec![1; 32],
                }],
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut buffers: Vec<_> = serialize_part(&Part::new(1, metadata.encode_to_vec())).collect();
        buffers.extend([
            marker(),
            Buffer {
                chunk: Some(vec![9]),
                ..Default::default()
            },
            marker(),
        ]);
        let path = std::env::temp_dir().join(format!("pool-bad-{}.bee", std::process::id()));
        write_bee(&mut std::fs::File::create(&path).unwrap(), buffers).unwrap();

        let m = manager(&Arc::new(FakeNode::default()));
        // The Service part has no head here, so `inspect` stops at the end: the block hashes
        // are what `verify_blocks` catches.
        assert!(m
            .add_package(&path, Configuration::default())
            .await
            .is_err());
        let off = ServiceOptions {
            verify_package: false,
            ..Default::default()
        };
        assert!(m
            .add_package_with(&path, Configuration::default(), off)
            .await
            .is_ok());
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn the_tcp_check_tells_an_open_port_from_a_closed_one() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let open = listener.local_addr().unwrap().to_string();
        assert!(tcp_is_alive(open).await);
        let closed = {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            l.local_addr().unwrap().to_string()
        };
        assert!(!tcp_is_alive(closed).await);
    }
}
