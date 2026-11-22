//! Atomic byte claims: waiting claims never capture a partial reservation.
//! Internal ownership is separate from allocation lifetime, because Arrow output
//! can retain a frame after its decoder has stopped retaining that frame.
use crate::{Cause, Error, Phase};
use std::sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::{sync::Notify, time::Instant};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Ownership {
    External,
    Internal,
    // Reads/workers that can complete without another byte reservation.
    Active,
}
struct State {
    available: usize,
    internal: usize,
    active: usize,
}
impl State {
    fn add_owner(&mut self, ownership: Ownership, bytes: usize) {
        match ownership {
            Ownership::Internal => self.internal += bytes,
            Ownership::Active => self.active += bytes,
            Ownership::External => (),
        }
    }
    fn remove_owner(&mut self, ownership: Ownership, bytes: usize) {
        match ownership {
            Ownership::Internal => self.internal -= bytes,
            Ownership::Active => self.active -= bytes,
            Ownership::External => (),
        }
    }
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
                active: 0,
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
        ownership: Ownership,
        holds_internal: bool,
        deadline: Instant,
    ) -> Result<ByteLease, Error> {
        super::check_deadline(deadline)?;
        if bytes > self.capacity {
            return Err(Error::at(Phase::Queue, Cause::LimitExceeded));
        }
        let mut retention_deadline = None;
        loop {
            // Register before examining accounting to avoid a release between
            // the check and registration becoming a lost wakeup.
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            super::check_deadline(deadline)?;
            let mut wait_deadline = deadline;
            let lease = {
                let mut state = self.state();
                if bytes <= state.available {
                    state.available -= bytes;
                    state.add_owner(ownership, bytes);
                    Some(ByteLease::new(Arc::clone(self), bytes, ownership))
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
                    // The short timer applies only when external retention alone
                    // prevents this claim fitting, even if active work completed.
                    if bytes > state.available + state.internal + state.active {
                        if let Some(timeout) = self.wait_timeout {
                            let until = match retention_deadline {
                                Some(until) => until,
                                None => Instant::now()
                                    .checked_add(timeout)
                                    .ok_or(Error::InvalidSetting)?,
                            };
                            retention_deadline = Some(until);
                            wait_deadline = deadline.min(until);
                            if Instant::now() >= until {
                                super::check_deadline(deadline)?;
                                return Err(Error::at(Phase::Queue, Cause::CapacityTimeout));
                            }
                        }
                    } else {
                        retention_deadline = None;
                    }
                    None
                }
            };
            if let Some(lease) = lease {
                super::check_deadline(deadline)?;
                // New internal ownership can make another nested wait impossible.
                self.changed.notify_waiters();
                return Ok(lease);
            }
            // Recheck ownership even on timeout: output may have been released
            // while the timer and notification became ready together.
            let _ = tokio::time::timeout_at(wait_deadline, changed).await;
        }
    }
}

struct Allocation {
    pool: Arc<BytePool>,
    // Mutated only under pool.state; atomics allow read-only liveness probes.
    bytes: AtomicUsize,
    owned: AtomicBool,
    ownership: Ownership,
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
        if self.0.owned.swap(false, Ordering::Relaxed) {
            state.remove_owner(self.0.ownership, self.0.bytes.load(Ordering::Relaxed));
        }
        drop(state);
        self.0.pool.changed.notify_waiters();
    }
}
impl ByteLease {
    fn new(pool: Arc<BytePool>, bytes: usize, ownership: Ownership) -> Self {
        let allocation = Arc::new(Allocation {
            pool,
            bytes: AtomicUsize::new(bytes),
            owned: AtomicBool::new(ownership != Ownership::External),
            ownership,
        });
        let owner =
            (ownership != Ownership::External).then(|| InternalOwner(Arc::clone(&allocation)));
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
        let ownership = if self.allocation.owned.load(Ordering::Relaxed) {
            self.allocation.ownership
        } else {
            Ownership::External
        };
        self.allocation
            .bytes
            .store(current - bytes, Ordering::Relaxed);
        let split = Self::new(Arc::clone(&self.allocation.pool), bytes, ownership);
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
        let mut state = self.allocation.pool.state();
        let bytes = self.allocation.bytes.swap(0, Ordering::Relaxed);
        state.available += bytes;
        if self.allocation.owned.swap(false, Ordering::Relaxed) {
            state.remove_owner(self.allocation.ownership, bytes);
        }
        drop(state);
        // Clear ownership and release bytes atomically. Dropping the marker
        // first would briefly turn an active claim into external pressure.
        self.owner = None;
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
    async fn capacity_timer_starts_when_active_claim_becomes_retained_output() {
        let pool = BytePool::with_wait_timeout(8, Some(Duration::from_millis(40)));
        let mut active = pool
            .reserve(8, Ownership::Active, false, deadline())
            .await
            .unwrap();
        let waiting = pool.reserve(8, Ownership::Active, false, deadline());
        tokio::pin!(waiting);
        assert!(
            tokio::time::timeout(Duration::from_millis(80), &mut waiting)
                .await
                .is_err()
        );
        let started = Instant::now();
        active.make_external();
        let error = waiting.await.unwrap_err();
        assert_eq!(error.cause(), Some(Cause::CapacityTimeout));
        assert!(started.elapsed() >= Duration::from_millis(40));
        drop(active);
        assert_eq!(pool.state().available, 8);
    }

    #[tokio::test]
    async fn cleared_retention_pressure_restores_operation_deadline() {
        let pool = BytePool::with_wait_timeout(8, Some(Duration::from_millis(60)));
        let retained = pool
            .reserve(1, Ownership::External, false, deadline())
            .await
            .unwrap();
        let active = pool
            .reserve(7, Ownership::Active, false, deadline())
            .await
            .unwrap();
        let waiting = pool.reserve(8, Ownership::Active, false, deadline());
        tokio::pin!(waiting);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut waiting)
                .await
                .is_err()
        );
        drop(retained);
        // Still blocked by an active read, but no longer by retained output.
        assert!(
            tokio::time::timeout(Duration::from_millis(90), &mut waiting)
                .await
                .is_err()
        );
        drop(active);
        drop(waiting.await.unwrap());
        assert_eq!(pool.state().available, 8);
    }

    #[tokio::test]
    async fn active_read_pressure_still_obeys_the_operation_deadline() {
        let pool = BytePool::with_wait_timeout(8, Some(Duration::ZERO));
        let active = pool
            .reserve(8, Ownership::Active, false, deadline())
            .await
            .unwrap();
        let until = Instant::now() + Duration::from_millis(40);
        let error = pool
            .reserve(8, Ownership::Active, false, until)
            .await
            .unwrap_err();
        assert!(Instant::now() >= until);
        assert_eq!(error.cause(), Some(Cause::Timeout));
        assert_eq!(error.kind(), crate::ErrorKind::Timeout);
        assert_eq!(error.phase(), Some(Phase::Queue));
        drop(active);
        assert_eq!(pool.state().available, 8);
    }

    #[tokio::test]
    async fn operation_deadline_precedes_a_longer_retention_wait_budget() {
        for wait in [None, Some(Duration::from_secs(1))] {
            let pool = BytePool::with_wait_timeout(8, wait);
            let retained = pool
                .reserve(8, Ownership::External, false, deadline())
                .await
                .unwrap();
            let until = Instant::now() + Duration::from_millis(20);
            let error = pool
                .reserve(8, Ownership::Active, false, until)
                .await
                .unwrap_err();
            assert!(Instant::now() >= until);
            assert_eq!(error.cause(), Some(Cause::Timeout));
            assert_eq!(error.kind(), crate::ErrorKind::Timeout);
            drop(retained);
            assert_eq!(pool.state().available, 8);
        }
    }

    #[tokio::test]
    async fn operation_deadline_wins_when_both_timers_expire_before_repoll() {
        let pool = BytePool::with_wait_timeout(8, Some(Duration::from_millis(50)));
        let retained = pool
            .reserve(8, Ownership::External, false, deadline())
            .await
            .unwrap();
        let until = Instant::now() + Duration::from_millis(150);
        let mut waiting = Box::pin(pool.reserve(8, Ownership::Active, false, until));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        tokio::time::sleep_until(until + Duration::from_millis(1)).await;
        assert_eq!(waiting.await.unwrap_err().cause(), Some(Cause::Timeout));
        drop(retained);
        assert_eq!(pool.state().available, 8);
    }

    #[tokio::test]
    async fn independent_reads_do_not_create_a_false_decoder_cycle() {
        let pool = BytePool::with_wait_timeout(8, Some(Duration::ZERO));
        let active = pool
            .reserve(6, Ownership::Active, false, deadline())
            .await
            .unwrap();
        let frame = pool
            .reserve(1, Ownership::Internal, false, deadline())
            .await
            .unwrap();
        let waiting = pool.reserve(2, Ownership::Internal, true, deadline());
        tokio::pin!(waiting);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut waiting)
                .await
                .is_err()
        );
        drop(active);
        drop(waiting.await.unwrap());
        drop(frame);
        let state = pool.state();
        assert_eq!((state.available, state.internal, state.active), (8, 0, 0));
    }

    #[tokio::test]
    async fn small_retained_output_does_not_limit_waiting_for_active_reads() {
        let pool = BytePool::with_wait_timeout(8, Some(Duration::ZERO));
        let retained = pool
            .reserve(3, Ownership::External, false, deadline())
            .await
            .unwrap();
        let active = pool
            .reserve(5, Ownership::Active, false, deadline())
            .await
            .unwrap();
        let waiting = pool.reserve(4, Ownership::Active, false, deadline());
        tokio::pin!(waiting);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut waiting)
                .await
                .is_err()
        );
        drop(active);
        drop(waiting.await.unwrap());
        drop(retained);
        let state = pool.state();
        assert_eq!((state.available, state.internal, state.active), (8, 0, 0));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn releasing_active_claims_never_creates_external_pressure() {
        let pool = BytePool::with_wait_timeout(8, Some(Duration::ZERO));
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let pool = Arc::clone(&pool);
            tasks.spawn(async move {
                for _ in 0..1000 {
                    let lease = pool
                        .reserve(8, Ownership::Active, false, deadline())
                        .await
                        .unwrap();
                    tokio::task::yield_now().await;
                    drop(lease);
                }
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        let state = pool.state();
        assert_eq!((state.available, state.internal, state.active), (8, 0, 0));
    }

    #[tokio::test]
    async fn internal_cycle_refuses_but_external_pressure_waits_and_resumes() {
        let pool = BytePool::new(8);
        let mut internal = pool
            .reserve(6, Ownership::Internal, false, deadline())
            .await
            .unwrap();
        let error = pool
            .reserve(4, Ownership::Internal, true, deadline())
            .await
            .unwrap_err();
        assert_eq!(error.cause(), Some(Cause::Capacity));
        internal.make_external();
        let mut waiting = Box::pin(pool.reserve(4, Ownership::Internal, true, deadline()));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        drop(waiting); // A cancelled wait must not capture partial capacity.
        assert_eq!(pool.state().available, 2);
        drop(internal);
        assert_eq!(
            pool.reserve(8, Ownership::External, false, deadline())
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
            frames.push(
                pool.reserve(24, Ownership::Internal, false, deadline())
                    .await
                    .unwrap(),
            );
            wires.push(
                pool.reserve(2, Ownership::Internal, true, deadline())
                    .await
                    .unwrap(),
            );
        }
        // Use 25 for the waiting frame so even after all wire chunks drop it
        // cannot acquire the 24-byte residual capacity.
        let mut fourth = Box::pin(pool.reserve(25, Ownership::Internal, false, deadline()));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(fourth.as_mut().poll(&mut cx).is_pending());
        drop(wires);
        for _ in 0..3 {
            let mut wire = Box::pin(pool.reserve(2, Ownership::Internal, true, deadline()));
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
        let mut bytes = pool
            .reserve(12, Ownership::Internal, false, deadline())
            .await
            .unwrap();
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
        let mut bytes = pool
            .reserve(16, Ownership::Internal, false, deadline())
            .await
            .unwrap();
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
        let external = pool
            .reserve(4, Ownership::External, false, deadline())
            .await
            .unwrap();
        let internal = pool
            .reserve(2, Ownership::Internal, false, deadline())
            .await
            .unwrap();
        let waiting_pool = Arc::clone(&pool);
        let waiter = tokio::spawn(async move {
            waiting_pool
                .reserve(6, Ownership::Internal, true, deadline())
                .await
        });
        tokio::task::yield_now().await;
        let peer = pool
            .reserve(2, Ownership::Internal, false, deadline())
            .await
            .unwrap();
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
                        .reserve(n % 7 + 1, Ownership::Internal, false, deadline())
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
