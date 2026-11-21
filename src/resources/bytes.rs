//! Atomic byte claims: waiting claims never capture a partial reservation.
//! Internal ownership is separate from allocation lifetime, because Arrow output
//! can retain a frame after its decoder has stopped retaining that frame.
use crate::{Cause, Error, Phase};
use std::sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::{sync::Notify, time::Instant};

struct State {
    available: usize,
    internal: usize,
}
pub(super) struct BytePool {
    capacity: usize,
    state: Mutex<State>,
    changed: Notify,
    wait_timeout: Option<std::time::Duration>,
}
impl BytePool {
    #[cfg(test)]
    pub(super) fn new(capacity: usize) -> Arc<Self> {
        Self::with_wait_timeout(capacity, None)
    }

    pub(super) fn with_wait_timeout(
        capacity: usize,
        wait_timeout: Option<std::time::Duration>,
    ) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            state: Mutex::new(State {
                available: capacity,
                internal: 0,
            }),
            changed: Notify::new(),
            wait_timeout,
        })
    }
    fn state(&self) -> MutexGuard<'_, State> {
        // Critical sections contain only accounting, never user code or awaits.
        // Drop must still release ownership if an unrelated unwind poisoned it.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub(super) async fn reserve(
        self: &Arc<Self>,
        bytes: usize,
        internal: bool,
        holds_internal: bool,
        deadline: Instant,
    ) -> Result<ByteLease, Error> {
        super::check_deadline(deadline)?;
        if bytes > self.capacity {
            return Err(Error::at(Phase::Queue, Cause::LimitExceeded));
        }
        let wait_deadline = self
            .wait_timeout
            .map(|timeout| {
                Instant::now()
                    .checked_add(timeout)
                    .ok_or(Error::InvalidSetting)
            })
            .transpose()?
            .map_or(deadline, |wait| wait.min(deadline));
        let mut waited = false;
        loop {
            // Register before examining accounting to avoid a release between
            // the check and registration becoming a lost wakeup.
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if waited && Instant::now() >= wait_deadline {
                return Err(Error::at(Phase::Queue, Cause::CapacityTimeout));
            }
            let lease = {
                let mut state = self.state();
                if bytes <= state.available {
                    state.available -= bytes;
                    if internal {
                        state.internal += bytes;
                    }
                    Some(ByteLease::new(Arc::clone(self), bytes, internal))
                } else if holds_internal
                    && state
                        .internal
                        .checked_add(bytes)
                        .is_none_or(|total| total > self.capacity)
                {
                    // Even releasing every externally retained result cannot
                    // fund this nested claim. Refuse rather than wait on peers
                    // that themselves need more bytes to release their state.
                    return Err(Error::at(Phase::Queue, Cause::Capacity));
                } else {
                    None
                }
            };
            if let Some(lease) = lease {
                if Instant::now() >= deadline {
                    return Err(Error::at(
                        Phase::Queue,
                        if waited {
                            Cause::CapacityTimeout
                        } else {
                            Cause::Timeout
                        },
                    ));
                }
                // New internal ownership can make another nested wait impossible.
                self.changed.notify_waiters();
                return Ok(lease);
            }
            waited = true;
            tokio::time::timeout_at(wait_deadline, changed)
                .await
                .map_err(|_| Error::at(Phase::Queue, Cause::CapacityTimeout))?;
        }
    }
}

struct Allocation {
    pool: Arc<BytePool>,
    // Mutated only under pool.state; atomics allow read-only liveness probes.
    bytes: AtomicUsize,
    internal: AtomicBool,
}
/// Byte ownership follows the covered allocation, including external aliases.
pub(crate) struct ByteLease {
    allocation: Arc<Allocation>,
    owner: Option<InternalOwner>,
}
impl std::fmt::Debug for ByteLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ByteLease")
            .field("bytes", &self.num_permits())
            .finish()
    }
}
/// Marks an allocation as internally retained without extending its lifetime.
/// A decoder keeps this marker while a dictionary may retain the encoded frame.
/// If Arrow copies the frame, dropping the actual lease clears its charge even
/// while this marker remains. Old returned dictionaries may outlive the marker.
pub(crate) struct InternalOwner(Arc<Allocation>);
impl InternalOwner {
    pub(crate) fn is_live(&self) -> bool {
        self.0.bytes.load(Ordering::Relaxed) != 0
    }
}
impl Drop for InternalOwner {
    fn drop(&mut self) {
        let mut state = self.0.pool.state();
        if self.0.internal.swap(false, Ordering::Relaxed) {
            state.internal -= self.0.bytes.load(Ordering::Relaxed);
        }
        drop(state);
        self.0.pool.changed.notify_waiters();
    }
}
impl ByteLease {
    fn new(pool: Arc<BytePool>, bytes: usize, internal: bool) -> Self {
        let allocation = Arc::new(Allocation {
            pool,
            bytes: AtomicUsize::new(bytes),
            internal: AtomicBool::new(internal),
        });
        let owner = internal.then(|| InternalOwner(Arc::clone(&allocation)));
        Self { allocation, owner }
    }
    pub(crate) fn num_permits(&self) -> usize {
        self.allocation.bytes.load(Ordering::Relaxed)
    }
    pub(crate) fn split(&mut self, bytes: usize) -> Option<Self> {
        let state = self.allocation.pool.state();
        let current = self.num_permits();
        if bytes > current {
            return None;
        }
        let internal = self.allocation.internal.load(Ordering::Relaxed);
        self.allocation
            .bytes
            .store(current - bytes, Ordering::Relaxed);
        let split = Self::new(Arc::clone(&self.allocation.pool), bytes, internal);
        drop(state);
        Some(split)
    }
    pub(crate) fn take_internal_owner(&mut self) -> Option<InternalOwner> {
        self.owner.take()
    }
    pub(crate) fn make_external(&mut self) {
        self.owner = None;
    }
}
impl Drop for ByteLease {
    fn drop(&mut self) {
        self.owner = None;
        let mut state = self.allocation.pool.state();
        let bytes = self.allocation.bytes.swap(0, Ordering::Relaxed);
        state.available += bytes;
        if self.allocation.internal.swap(false, Ordering::Relaxed) {
            state.internal -= bytes;
        }
        drop(state);
        self.allocation.pool.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::Future,
        task::{Context, Poll, Waker},
        time::Duration,
    };

    fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(3)
    }

    #[tokio::test]
    async fn internal_cycle_refuses_but_external_pressure_waits_and_resumes() {
        let pool = BytePool::new(8);
        let mut internal = pool.reserve(6, true, false, deadline()).await.unwrap();
        let error = pool.reserve(4, true, true, deadline()).await.unwrap_err();
        assert_eq!(error.cause(), Some(Cause::Capacity));
        internal.make_external();
        let mut waiting = Box::pin(pool.reserve(4, true, true, deadline()));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        drop(waiting); // A cancelled wait must not capture partial capacity.
        assert_eq!(pool.state().available, 2);
        drop(internal);
        assert_eq!(
            pool.reserve(8, false, false, deadline())
                .await
                .unwrap()
                .num_permits(),
            8
        );
    }

    #[tokio::test]
    async fn four_frame_claims_allow_small_wire_claims_to_make_progress() {
        // Scaled form of the default-capacity reviewed schedule. Three readers
        // hold 24-byte frames and 2-byte wire chunks; the fourth needs 24 bytes.
        let pool = BytePool::new(96);
        let mut frames = Vec::new();
        let mut wires = Vec::new();
        for _ in 0..3 {
            frames.push(pool.reserve(24, true, false, deadline()).await.unwrap());
            wires.push(pool.reserve(2, true, true, deadline()).await.unwrap());
        }
        // Use 25 for the waiting frame so even after all wire chunks drop it
        // cannot acquire the 24-byte residual capacity.
        let mut fourth = Box::pin(pool.reserve(25, true, false, deadline()));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(fourth.as_mut().poll(&mut cx).is_pending());
        drop(wires);
        for _ in 0..3 {
            let mut wire = Box::pin(pool.reserve(2, true, true, deadline()));
            let Poll::Ready(Ok(lease)) = wire.as_mut().poll(&mut cx) else {
                panic!("partial frame reader must continue despite a large waiter");
            };
            drop(lease);
        }
        drop(frames);
        drop(fourth.await.unwrap());
        assert_eq!(pool.state().available, 96);
        assert_eq!(pool.state().internal, 0);
    }

    #[tokio::test]
    async fn split_and_detached_owner_follow_allocation_and_dictionary_lifetimes() {
        let pool = BytePool::new(16);
        let mut bytes = pool.reserve(12, true, false, deadline()).await.unwrap();
        let extra = bytes.split(4).unwrap();
        assert_eq!(pool.state().internal, 12);
        drop(extra);
        assert_eq!(pool.state().internal, 8);
        let dictionary_owner = bytes.take_internal_owner().unwrap();
        let output = Arc::new(bytes);
        let retained = Arc::clone(&output);
        drop(dictionary_owner); // Dictionary replaced; caller still owns backing.
        assert_eq!(pool.state().internal, 0);
        assert_eq!(pool.state().available, 8);
        drop(output);
        assert_eq!(pool.state().available, 8);
        drop(retained);
        assert_eq!(pool.state().available, 16);
        // A marker never extends the native allocation's actual byte lease.
        let mut bytes = pool.reserve(16, true, false, deadline()).await.unwrap();
        let marker = bytes.take_internal_owner().unwrap();
        assert!(marker.is_live());
        drop(bytes);
        assert!(!marker.is_live());
        assert_eq!(pool.state().available, 16);
        assert_eq!(pool.state().internal, 0);
        drop(marker);
        assert_eq!(pool.state().internal, 0);
    }

    #[tokio::test]
    async fn waiter_rechecks_when_new_internal_ownership_makes_progress_impossible() {
        let pool = BytePool::new(8);
        let external = pool.reserve(4, false, false, deadline()).await.unwrap();
        let internal = pool.reserve(2, true, false, deadline()).await.unwrap();
        let waiting_pool = Arc::clone(&pool);
        let waiter =
            tokio::spawn(async move { waiting_pool.reserve(6, true, true, deadline()).await });
        tokio::task::yield_now().await;
        let peer = pool.reserve(2, true, false, deadline()).await.unwrap();
        assert_eq!(
            waiter.await.unwrap().unwrap_err().cause(),
            Some(Cause::Capacity)
        );
        drop((external, internal, peer));
        assert_eq!(pool.state().available, 8);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_claims_release_all_capacity_and_wake_waiters() {
        let pool = BytePool::new(32);
        let mut tasks = tokio::task::JoinSet::new();
        for n in 1..=32 {
            let pool = Arc::clone(&pool);
            tasks.spawn(async move {
                for _ in 0..32 {
                    let mut lease = pool
                        .reserve(n % 7 + 1, true, false, deadline())
                        .await
                        .unwrap();
                    tokio::task::yield_now().await;
                    lease.make_external();
                    drop(lease);
                }
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        assert_eq!(pool.state().available, 32);
        assert_eq!(pool.state().internal, 0);
    }
}
