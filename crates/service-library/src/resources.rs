//! The memory of the service, as a pool that grows and shrinks with what is in use.
//!
//! This is the Rust version of the `ResourceManager` of `celaut-service-libraries`. A service
//! that works on big data locks the memory that a job needs before it allocates it
//! ([`ResourceManager::lock`]). That memory is room for jobs on top of `base`, the memory that
//! the service needs by itself. If the room is too small, the manager asks the node for `base`
//! plus the memory that is locked plus all that waits (`ModifyServiceSystemResources`) and the
//! job goes on when the node has given it; when the last waiter is served and memory is
//! unlocked, the manager asks for `base` plus what is still locked, so the service does not
//! pay for room that it does not use. It never asks for less than `base`: a limit of 0 is,
//! for nodo, a request to hibernate the instance, and a service that wants that asks for it
//! with [`ResourceManager::hibernate`]. The node charges a fee per request: the manager does
//! not ask twice in a row for the same amount.
//!
//! Differences with the Python version: a waiter is woken when memory is unlocked, and does
//! not poll every second; a lock can have a time limit ([`ResourceManager::lock_for`]) or
//! not wait at all ([`ResourceManager::try_lock`]); a failed request of the node is an
//! error of the lock, and not a hang; the memory of the service itself is not room for jobs,
//! and the pool never goes under it.

use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use celaut_proto::celaut::Sysresources;
use tokio::sync::{Mutex as AsyncMutex, Notify};

use crate::gateway::{amount, Gateway};
use crate::Error;

/// What the node says after a change of the resources.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Resized {
    /// The memory limit of the service now, in bytes.
    pub mem_limit: u64,
    /// The balance of the instance, in MU.
    pub balance_mu: u128,
}

/// What the manager needs from the node: set the memory limit of the service.
/// [`Gateway`] is the real one, and a test can give another.
pub trait Resizer: Send + Sync + 'static {
    /// Ask for a memory limit of `mem_limit` bytes.
    fn resize(&self, mem_limit: u64) -> impl Future<Output = Result<Resized, Error>> + Send;
}

impl Resizer for Gateway {
    async fn resize(&self, mem_limit: u64) -> Result<Resized, Error> {
        let answer = self
            .modify_resources(Sysresources {
                mem_limit: Some(mem_limit),
                ..Default::default()
            })
            .await?;
        let mem_limit = answer
            .sysreq
            .and_then(|s| s.mem_limit)
            .ok_or_else(|| Error::Protocol("the node gave no memory limit".to_string()))?;
        Ok(Resized {
            mem_limit,
            balance_mu: amount(answer.balance.as_ref())?,
        })
    }
}

/// The numbers of the manager at one moment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    /// The memory limit of the service now, in bytes: `base` plus the room for jobs.
    pub pool: u64,
    /// The memory that is locked.
    pub locked: u64,
    /// The room for jobs that is not locked: the pool minus `base` minus what is locked.
    pub available: u64,
    /// The memory that waits for a lock.
    pub waiting: u64,
    /// The balance in MU that the node reported on the last request.
    pub balance_mu: u128,
}

struct State {
    /// The memory that the service needs by itself, with no job: the limit that the node gave
    /// it at the start. It is not room for locks, and the manager never asks for less.
    base: u64,
    /// The memory limit in force: `base` plus the room for jobs.
    pool: u64,
    locked: u64,
    last_request: Option<u64>,
    balance_mu: u128,
}

impl State {
    fn available(&self) -> u64 {
        self.pool
            .saturating_sub(self.base)
            .saturating_sub(self.locked)
    }

    /// The limit that holds what is locked and waits: `base` plus the jobs.
    fn limit_for(&self, jobs: u64) -> u64 {
        self.base.saturating_add(jobs)
    }
}

/// The pool of memory of a service. Share it in an `Arc`.
pub struct ResourceManager<R: Resizer> {
    resizer: R,
    state: AsyncMutex<State>,
    waiting: Mutex<Vec<u64>>,
    changed: Notify,
}

impl<R: Resizer> ResourceManager<R> {
    /// A manager for a service that needs `base` bytes by itself: the `mem_limit` that the
    /// node gave it at the start (`NodeConfig`'s initial resources). Locks are room on top
    /// of it, so the first lock asks the node for more.
    pub fn new(resizer: R, base: u64) -> Arc<Self> {
        Arc::new(ResourceManager {
            resizer,
            state: AsyncMutex::new(State {
                base,
                pool: base,
                locked: 0,
                // The limit in force is the one that the node gave at the start.
                last_request: Some(base),
                balance_mu: 0,
            }),
            waiting: Mutex::new(Vec::new()),
            changed: Notify::new(),
        })
    }

    /// The numbers of the manager now.
    pub async fn stats(&self) -> Stats {
        let state = self.state.lock().await;
        Stats {
            pool: state.pool,
            locked: state.locked,
            available: state.available(),
            waiting: self.waiting_sum(),
            balance_mu: state.balance_mu,
        }
    }

    fn waiting_sum(&self) -> u64 {
        self.waiting
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .fold(0u64, |a, b| a.saturating_add(*b))
    }

    /// Lock `len` bytes, and wait until they are free. If the pool is too small it asks the
    /// node to grow it; an error of the node is the error of the lock. It waits for as long as
    /// the memory is not free: use [`ResourceManager::lock_for`] to put a limit.
    pub async fn lock(self: &Arc<Self>, len: u64) -> Result<RamLock<R>, Error> {
        self.lock_inner(len, true).await
    }

    /// [`ResourceManager::lock`] that gives up after `limit`.
    pub async fn lock_for(
        self: &Arc<Self>,
        len: u64,
        limit: Duration,
    ) -> Result<RamLock<R>, Error> {
        tokio::time::timeout(limit, self.lock_inner(len, true))
            .await
            .map_err(|_| Error::Resources(format!("{len} bytes were not free in {limit:?}")))?
    }

    /// Lock `len` bytes if they are free now, growing the pool if it can; it does not wait
    /// for memory that other locks hold.
    pub async fn try_lock(self: &Arc<Self>, len: u64) -> Result<RamLock<R>, Error> {
        self.lock_inner(len, false).await
    }

    async fn lock_inner(self: &Arc<Self>, len: u64, wait: bool) -> Result<RamLock<R>, Error> {
        // The entry leaves the list when this future ends, also when it is dropped.
        let _entry = WaitEntry::push(self, len);
        loop {
            // Take the notice before the check: an unlock between the two is not lost.
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.state.lock().await;
                if state.available() < len && self.waiting_sum() > state.available() {
                    let request = state.limit_for(state.locked.saturating_add(self.waiting_sum()));
                    self.update(&mut state, request).await?;
                    // The request counted every waiter, so the pool may now hold one that
                    // is parked: wake them all to look again, once this check is done.
                    self.changed.notify_waiters();
                }
                if state.available() >= len {
                    state.locked += len;
                    return Ok(RamLock {
                        manager: self.clone(),
                        len,
                    });
                }
            }
            if !wait {
                return Err(Error::Resources(format!(
                    "{len} bytes are not free now and the node gave no more"
                )));
            }
            notified.await;
        }
    }

    /// Ask the node for a limit of `request` bytes, unless it is what is already in force.
    /// It is never 0, which hibernates: [`ResourceManager::hibernate`] is for that.
    async fn update(&self, state: &mut State, request: u64) -> Result<(), Error> {
        if request == 0 || state.last_request == Some(request) {
            return Ok(());
        }
        let resized = self.resizer.resize(request).await?;
        state.last_request = Some(request);
        state.pool = resized.mem_limit;
        state.balance_mu = resized.balance_mu;
        Ok(())
    }

    /// Hibernate the service: ask the node for a limit of 0, which for nodo stops the instance
    /// until it is needed again. It is the one request that goes under `base`. It fails, and
    /// asks nothing, while memory is locked or waits: those jobs would stop with the instance.
    /// The next lock asks for the memory that it needs, as after any other request.
    pub async fn hibernate(&self) -> Result<(), Error> {
        let mut state = self.state.lock().await;
        if state.locked > 0 || self.waiting_sum() > 0 {
            return Err(Error::Resources(
                "the service has jobs that hold or wait for memory: it cannot hibernate".into(),
            ));
        }
        let resized = self.resizer.resize(0).await?;
        state.last_request = Some(0);
        state.pool = resized.mem_limit;
        state.balance_mu = resized.balance_mu;
        Ok(())
    }

    async fn unlock(&self, len: u64) {
        let mut state = self.state.lock().await;
        state.locked = state.locked.saturating_sub(len);
        if self.waiting_sum() == 0 {
            let request = state.limit_for(state.locked);
            // A refusal to shrink leaves the pool as it was: nothing is lost but the saving.
            let _ = self.update(&mut state, request).await;
        }
        drop(state);
        self.changed.notify_waiters();
    }
}

struct WaitEntry<R: Resizer> {
    manager: Arc<ResourceManager<R>>,
    len: u64,
}

impl<R: Resizer> WaitEntry<R> {
    fn push(manager: &Arc<ResourceManager<R>>, len: u64) -> Self {
        manager
            .waiting
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(len);
        WaitEntry {
            manager: manager.clone(),
            len,
        }
    }
}

impl<R: Resizer> Drop for WaitEntry<R> {
    fn drop(&mut self) {
        let mut waiting = self
            .manager
            .waiting
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(i) = waiting.iter().position(|l| *l == self.len) {
            waiting.swap_remove(i);
        }
    }
}

/// Memory that is locked. It is unlocked when it is dropped; [`RamLock::release`] does it
/// and waits for the request of the node that may follow.
pub struct RamLock<R: Resizer> {
    manager: Arc<ResourceManager<R>>,
    len: u64,
}

impl<R: Resizer> RamLock<R> {
    /// The bytes that this lock holds.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// True if the lock holds nothing.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Unlock `amount` bytes of this lock before the end (at most what it holds).
    pub async fn unlock(&mut self, amount: u64) {
        let amount = amount.min(self.len);
        self.len -= amount;
        self.manager.unlock(amount).await;
    }

    /// Unlock all of it, and wait until the manager has made its request to the node.
    pub async fn release(mut self) {
        let len = std::mem::take(&mut self.len);
        self.manager.unlock(len).await;
    }
}

impl<R: Resizer> Drop for RamLock<R> {
    fn drop(&mut self) {
        let len = std::mem::take(&mut self.len);
        if len == 0 {
            return;
        }
        let manager = self.manager.clone();
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move { manager.unlock(len).await });
            }
            // No runtime to ask the node: free the memory in the count, which is enough for
            // the next lock; the request that shrinks the pool waits for the next unlock.
            Err(_) => {
                if let Ok(mut state) = manager.state.try_lock() {
                    state.locked = state.locked.saturating_sub(len);
                }
                manager.changed.notify_waiters();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// A node that gives what is asked, up to `cap`, and records the requests.
    struct FakeResizer {
        requests: Mutex<Vec<u64>>,
        cap: u64,
        fail: std::sync::atomic::AtomicBool,
        fee: AtomicU64,
    }

    impl FakeResizer {
        fn new(cap: u64) -> Arc<Self> {
            Arc::new(FakeResizer {
                requests: Mutex::new(Vec::new()),
                cap,
                fail: std::sync::atomic::AtomicBool::new(false),
                fee: AtomicU64::new(0),
            })
        }
        fn requests(&self) -> Vec<u64> {
            self.requests.lock().unwrap().clone()
        }
    }

    impl Resizer for Arc<FakeResizer> {
        async fn resize(&self, mem_limit: u64) -> Result<Resized, Error> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(Error::Protocol("the node refused".into()));
            }
            self.requests.lock().unwrap().push(mem_limit);
            let fee = self.fee.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(Resized {
                mem_limit: mem_limit.min(self.cap),
                balance_mu: 1000 - u128::from(fee),
            })
        }
    }

    #[tokio::test]
    async fn a_lock_is_room_on_top_of_the_base_and_goes_back_to_it() {
        let node = FakeResizer::new(1000);
        let m = ResourceManager::new(node.clone(), 100);
        let lock = m.lock(60).await.unwrap();
        // The base is 100 and the job is 60 more.
        assert_eq!(node.requests(), vec![160]);
        let stats = m.stats().await;
        assert_eq!((stats.pool, stats.locked, stats.available), (160, 60, 0));
        lock.release().await;
        // Nothing is locked: the base, and never 0.
        assert_eq!(node.requests(), vec![160, 100]);
        assert_eq!(m.stats().await.pool, 100);
    }

    #[tokio::test]
    async fn the_base_is_not_room_for_a_lock() {
        // The node gives nothing above the base: no lock fits.
        let node = FakeResizer::new(100);
        let m = ResourceManager::new(node.clone(), 100);
        assert!(matches!(m.try_lock(1).await, Err(Error::Resources(_))));
        assert_eq!(m.stats().await.available, 0);
    }

    #[tokio::test]
    async fn a_service_that_started_with_no_limit_is_never_asked_for_zero() {
        let node = FakeResizer::new(1000);
        let m = ResourceManager::new(node.clone(), 0);
        m.lock(50).await.unwrap().release().await;
        assert_eq!(node.requests(), vec![50]);
    }

    #[tokio::test]
    async fn a_lock_that_does_not_fit_grows_the_pool_and_shrinks_it_on_release() {
        let node = FakeResizer::new(1000);
        let m = ResourceManager::new(node.clone(), 100);
        let a = m.lock(80).await.unwrap();
        let b = m.lock(120).await.unwrap();
        // base 100 + locked 80 + waiting 120.
        assert_eq!(node.requests(), vec![180, 300]);
        let stats = m.stats().await;
        assert_eq!((stats.pool, stats.locked, stats.waiting), (300, 200, 0));
        assert_eq!(stats.balance_mu, 998);
        b.release().await;
        assert_eq!(node.requests(), vec![180, 300, 180]);
        a.release().await;
        assert_eq!(node.requests(), vec![180, 300, 180, 100]);
        assert_eq!(m.stats().await.balance_mu, 996);
    }

    #[tokio::test]
    async fn the_same_amount_is_not_asked_twice_in_a_row() {
        let node = FakeResizer::new(1000);
        let m = ResourceManager::new(node.clone(), 100);
        let _held = m.lock(50).await.unwrap();
        let mut other = m.lock(10).await.unwrap();
        assert_eq!(node.requests(), vec![150, 160]);
        // Both unlocks leave 60 bytes locked, which is the 160 in force: no request.
        other.unlock(0).await;
        other.unlock(0).await;
        assert_eq!(node.requests(), vec![150, 160]);
    }

    #[tokio::test]
    async fn hibernate_asks_for_zero_only_when_nothing_is_locked() {
        let node = FakeResizer::new(1000);
        let m = ResourceManager::new(node.clone(), 100);
        let lock = m.lock(10).await.unwrap();
        assert!(matches!(m.hibernate().await, Err(Error::Resources(_))));
        assert_eq!(node.requests(), vec![110]);
        lock.release().await;
        m.hibernate().await.unwrap();
        assert_eq!(node.requests(), vec![110, 100, 0]);
        // The service is awake again with the next lock: it asks for what it needs.
        m.lock(10).await.unwrap().release().await;
        assert_eq!(node.requests(), vec![110, 100, 0, 110, 100]);
    }

    #[tokio::test]
    async fn a_lock_waits_for_memory_that_another_lock_frees() {
        // The node gives nothing above 170: 70 of room on top of the base of 100.
        let node = FakeResizer::new(170);
        let m = ResourceManager::new(node.clone(), 100);
        let a = m.lock(70).await.unwrap();
        let waiter = tokio::spawn({
            let m = m.clone();
            async move { m.lock(60).await.unwrap().len() }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiter.is_finished(), "70 + 60 do not fit in 70 of room");
        assert_eq!(m.stats().await.waiting, 60);
        a.release().await;
        assert_eq!(waiter.await.unwrap(), 60);
    }

    /// A node whose cap can be raised after the first requests.
    struct RaisableResizer {
        cap: AtomicU64,
    }

    impl Resizer for Arc<RaisableResizer> {
        async fn resize(&self, mem_limit: u64) -> Result<Resized, Error> {
            Ok(Resized {
                mem_limit: mem_limit.min(self.cap.load(Ordering::SeqCst)),
                balance_mu: 0,
            })
        }
    }

    #[tokio::test]
    async fn a_waiter_is_woken_when_the_pool_grows_for_it() {
        let node = Arc::new(RaisableResizer {
            cap: AtomicU64::new(120),
        });
        let m = ResourceManager::new(node.clone(), 100);
        let _full = m.lock(100).await.unwrap();
        // A asks for 150 and the node gives 120: A waits.
        let a = tokio::spawn({
            let m = m.clone();
            async move { m.lock(50).await.unwrap().len() }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!a.is_finished());
        // Another lock now asks for 100 + 50 + 30 and gets it: the pool has room for A, and
        // nobody unlocks anything.
        node.cap.store(1000, Ordering::SeqCst);
        let _c = m.lock(30).await.unwrap();
        let woken = tokio::time::timeout(Duration::from_secs(2), a).await;
        assert_eq!(woken.expect("A was not woken").unwrap(), 50);
    }

    #[tokio::test]
    async fn try_lock_does_not_wait_and_lock_for_gives_up() {
        let node = FakeResizer::new(170);
        let m = ResourceManager::new(node.clone(), 100);
        let _a = m.lock(70).await.unwrap();
        assert!(matches!(m.try_lock(60).await, Err(Error::Resources(_))));
        let r = m.lock_for(60, Duration::from_millis(50)).await;
        assert!(matches!(r, Err(Error::Resources(_))));
        // A lock that gave up leaves nothing in the waiting list.
        assert_eq!(m.stats().await.waiting, 0);
        assert_eq!(m.stats().await.locked, 70);
    }

    #[tokio::test]
    async fn a_refusal_of_the_node_is_the_error_of_the_lock() {
        let node = FakeResizer::new(1000);
        node.fail.store(true, Ordering::SeqCst);
        let m = ResourceManager::new(node.clone(), 10);
        let r = m.lock(50).await;
        assert!(matches!(r, Err(Error::Protocol(_))));
        let stats = m.stats().await;
        assert_eq!((stats.locked, stats.waiting), (0, 0));
    }

    #[tokio::test]
    async fn dropping_a_lock_unlocks_it() {
        let node = FakeResizer::new(1000);
        let m = ResourceManager::new(node.clone(), 100);
        drop(m.lock(60).await.unwrap());
        for _ in 0..50 {
            if m.stats().await.locked == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(m.stats().await.locked, 0);
    }

    #[tokio::test]
    async fn a_partial_unlock_keeps_the_rest() {
        let node = FakeResizer::new(1000);
        let m = ResourceManager::new(node.clone(), 100);
        let mut lock = m.lock(80).await.unwrap();
        lock.unlock(30).await;
        assert_eq!(lock.len(), 50);
        assert_eq!(m.stats().await.locked, 50);
        lock.unlock(500).await;
        assert!(lock.is_empty());
        assert_eq!(m.stats().await.locked, 0);
    }
}
