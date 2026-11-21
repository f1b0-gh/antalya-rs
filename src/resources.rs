mod bytes;
use crate::{Cause, Error, Phase, ResourceLimits};
pub(crate) use bytes::{ByteLease, InternalOwner};
use std::sync::Arc;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError},
    time::Instant,
};

#[derive(Clone, Copy, Debug)]
pub(crate) enum Lane {
    Data,
    Control,
    Export,
}

struct Admission {
    active: Arc<Semaphore>,
    queued: Arc<Semaphore>,
}
impl Admission {
    fn new(active: usize, queued: usize) -> Self {
        Self {
            active: Arc::new(Semaphore::new(active)),
            queued: Arc::new(Semaphore::new(queued)),
        }
    }

    async fn acquire(&self, deadline: Instant) -> Result<OwnedSemaphorePermit, Error> {
        check_deadline(deadline)?;
        match Arc::clone(&self.active).try_acquire_owned() {
            Ok(permit) => {
                check_deadline(deadline)?;
                return Ok(permit);
            }
            Err(TryAcquireError::Closed) => return Err(Error::at(Phase::Queue, Cause::Capacity)),
            Err(TryAcquireError::NoPermits) => {}
        }
        // Holding this lease bounds queued futures; dropping or timing out releases it.
        let _queued = Arc::clone(&self.queued)
            .try_acquire_owned()
            .map_err(|_| Error::at(Phase::Queue, Cause::Capacity))?;
        let permit = tokio::time::timeout_at(deadline, Arc::clone(&self.active).acquire_owned())
            .await
            .map_err(|_| Error::at(Phase::Queue, Cause::Timeout))?
            .map_err(|_| Error::at(Phase::Queue, Cause::Capacity))?;
        // A timeout polls the acquisition first: a ready permit can arrive after expiry.
        check_deadline(deadline)?;
        Ok(permit)
    }
}

/// All clients cloned from one instance share these admission and byte budgets.
pub(crate) struct Resources {
    data: Admission,
    control: Admission,
    export: Admission,
    bytes: Arc<bytes::BytePool>,
    retained_capacity: usize,
}
impl Resources {
    pub(crate) fn new(limits: &ResourceLimits) -> Result<Self, Error> {
        limits.validate()?;
        Ok(Self {
            data: Admission::new(limits.max_active_queries, limits.max_queued_queries),
            control: Admission::new(limits.max_control_queries, limits.max_queued_queries),
            export: Admission::new(limits.max_export_tasks, limits.max_queued_queries),
            bytes: bytes::BytePool::new(limits.max_retained_bytes),
            retained_capacity: limits.max_retained_bytes,
        })
    }

    pub(crate) async fn acquire(
        &self,
        lane: Lane,
        deadline: Instant,
    ) -> Result<OwnedSemaphorePermit, Error> {
        match lane {
            Lane::Data => &self.data,
            Lane::Control => &self.control,
            Lane::Export => &self.export,
        }
        .acquire(deadline)
        .await
    }

    pub(crate) fn retained_capacity(&self) -> usize {
        self.retained_capacity
    }

    pub(crate) async fn reserve_bytes(
        &self,
        bytes: usize,
        deadline: Instant,
    ) -> Result<ByteLease, Error> {
        self.bytes.reserve(bytes, false, false, deadline).await
    }

    pub(crate) async fn reserve_internal(
        &self,
        bytes: usize,
        holds_internal: bool,
        deadline: Instant,
    ) -> Result<ByteLease, Error> {
        self.bytes
            .reserve(bytes, true, holds_internal, deadline)
            .await
    }
}

fn check_deadline(deadline: Instant) -> Result<(), Error> {
    if Instant::now() >= deadline {
        Err(Error::at(Phase::Queue, Cause::Timeout))
    } else {
        Ok(())
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

    fn resources(queued: usize) -> Resources {
        Resources::new(&ResourceLimits {
            max_active_queries: 1,
            max_queued_queries: queued,
            max_control_queries: 1,
            max_export_tasks: 1,
            max_retained_bytes: 8,
        })
        .unwrap()
    }

    fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(1)
    }

    #[tokio::test]
    async fn large_byte_waiter_does_not_block_progress_using_available_capacity() {
        let resources = resources(1);
        let held = resources.reserve_bytes(6, deadline()).await.unwrap();
        let mut large = Box::pin(resources.reserve_bytes(4, deadline()));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(large.as_mut().poll(&mut cx).is_pending());
        let mut small = Box::pin(resources.reserve_bytes(2, deadline()));
        let Poll::Ready(Ok(progress)) = small.as_mut().poll(&mut cx) else {
            panic!("a queued large claim must not capture bytes needed for progress");
        };
        drop(progress);
        drop(held);
        assert_eq!(large.await.unwrap().num_permits(), 4);
    }

    #[tokio::test]
    async fn saturated_data_does_not_consume_control_or_export_capacity() {
        let resources = resources(0);
        let data = resources.acquire(Lane::Data, deadline()).await.unwrap();
        assert_eq!(
            resources
                .acquire(Lane::Data, deadline())
                .await
                .unwrap_err()
                .cause(),
            Some(Cause::Capacity)
        );
        let control = resources.acquire(Lane::Control, deadline()).await.unwrap();
        let export = resources.acquire(Lane::Export, deadline()).await.unwrap();
        drop((data, control, export));
        assert!(resources.acquire(Lane::Data, deadline()).await.is_ok());
    }

    #[tokio::test]
    async fn queue_drop_releases_waiting_slot_and_never_leaks_active_permit() {
        let resources = resources(1);
        let active = resources.acquire(Lane::Data, deadline()).await.unwrap();
        let mut queued = Box::pin(resources.acquire(Lane::Data, deadline()));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(queued.as_mut().poll(&mut cx), Poll::Pending));
        assert_eq!(
            resources
                .acquire(Lane::Data, deadline())
                .await
                .unwrap_err()
                .cause(),
            Some(Cause::Capacity)
        );
        drop(queued);
        let mut replacement = Box::pin(resources.acquire(Lane::Data, deadline()));
        assert!(matches!(replacement.as_mut().poll(&mut cx), Poll::Pending));
        drop(active);
        let replacement = replacement.await.unwrap();
        drop(replacement);
        assert!(resources.acquire(Lane::Data, deadline()).await.is_ok());
    }

    #[tokio::test]
    async fn timeout_releases_queue_and_byte_waiters() {
        let resources = resources(1);
        let active = resources.acquire(Lane::Data, deadline()).await.unwrap();
        let short = Instant::now() + Duration::from_millis(2);
        assert_eq!(
            resources
                .acquire(Lane::Data, short)
                .await
                .unwrap_err()
                .cause(),
            Some(Cause::Timeout)
        );
        drop(active);
        assert!(resources.acquire(Lane::Data, deadline()).await.is_ok());
        let bytes = resources.reserve_bytes(8, deadline()).await.unwrap();
        let short = Instant::now() + Duration::from_millis(2);
        assert_eq!(
            resources.reserve_bytes(1, short).await.unwrap_err().cause(),
            Some(Cause::Timeout)
        );
        drop(bytes);
        assert_eq!(
            resources
                .reserve_bytes(8, deadline())
                .await
                .unwrap()
                .num_permits(),
            8
        );
    }

    #[tokio::test]
    async fn byte_leases_survive_clones_and_release_when_last_owner_drops() {
        let resources = resources(1);
        let lease = Arc::new(resources.reserve_bytes(8, deadline()).await.unwrap());
        let retained = Arc::clone(&lease);
        drop(lease);
        let mut waiter = Box::pin(resources.reserve_bytes(8, deadline()));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(waiter.as_mut().poll(&mut cx), Poll::Pending));
        drop(retained);
        assert_eq!(waiter.await.unwrap().num_permits(), 8);
    }

    #[tokio::test]
    async fn oversized_byte_reservation_and_expired_deadline_refuse_immediately() {
        let resources = resources(1);
        assert_eq!(
            resources
                .reserve_bytes(9, deadline())
                .await
                .unwrap_err()
                .cause(),
            Some(Cause::LimitExceeded)
        );
        assert_eq!(
            resources
                .acquire(Lane::Data, Instant::now())
                .await
                .unwrap_err()
                .cause(),
            Some(Cause::Timeout)
        );
        assert_eq!(
            resources
                .reserve_bytes(1, Instant::now())
                .await
                .unwrap_err()
                .cause(),
            Some(Cause::Timeout)
        );
        assert!(resources.acquire(Lane::Data, deadline()).await.is_ok());
    }
    #[tokio::test]
    async fn expired_ready_admission_releases_acquired_permit() {
        let resources = resources(1);
        let active = resources.acquire(Lane::Data, deadline()).await.unwrap();
        let expires = Instant::now() + Duration::from_millis(20);
        let mut waiter = Box::pin(resources.acquire(Lane::Data, expires));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(waiter.as_mut().poll(&mut cx), Poll::Pending));
        drop(active);
        // Do not poll the ready waiter until its deadline has elapsed.
        tokio::time::sleep_until(expires + Duration::from_millis(1)).await;
        assert_eq!(waiter.await.unwrap_err().cause(), Some(Cause::Timeout));
        assert!(resources.acquire(Lane::Data, deadline()).await.is_ok());
    }

    #[tokio::test]
    async fn expired_ready_bytes_release_acquired_capacity() {
        let resources = resources(1);
        let active = resources.reserve_bytes(8, deadline()).await.unwrap();
        let expires = Instant::now() + Duration::from_millis(20);
        let mut waiter = Box::pin(resources.reserve_bytes(8, expires));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(waiter.as_mut().poll(&mut cx), Poll::Pending));
        drop(active);
        tokio::time::sleep_until(expires + Duration::from_millis(1)).await;
        assert_eq!(waiter.await.unwrap_err().cause(), Some(Cause::Timeout));
        assert_eq!(
            resources
                .reserve_bytes(8, deadline())
                .await
                .unwrap()
                .num_permits(),
            8
        );
    }
}
