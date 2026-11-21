//! Read deadlines belong to the stream, not to a cancellable borrowed future.
use crate::{Cause, Error, Phase};
use std::{future::Future, time::Duration};
use tokio::time::Instant;

pub(crate) struct ReadTimer {
    timeout: Option<Duration>,
    deadline: Option<Instant>,
}

impl ReadTimer {
    pub(crate) fn new(timeout: Option<Duration>) -> Self {
        Self {
            timeout,
            deadline: None,
        }
    }

    pub(crate) async fn wait<F: Future>(
        &mut self,
        phase: Phase,
        future: F,
    ) -> Result<F::Output, Error> {
        let Some(timeout) = self.timeout else {
            return Ok(future.await);
        };
        let deadline = match self.deadline {
            Some(deadline) => deadline,
            None => {
                let deadline = Instant::now()
                    .checked_add(timeout)
                    .ok_or(Error::InvalidSetting)?;
                self.deadline = Some(deadline);
                deadline
            }
        };
        if Instant::now() >= deadline {
            return Err(Error::at(phase, Cause::ReadTimeout));
        }
        let output = tokio::time::timeout_at(deadline, future)
            .await
            .map_err(|_| Error::at(phase, Cause::ReadTimeout))?;
        if Instant::now() >= deadline {
            return Err(Error::at(phase, Cause::ReadTimeout));
        }
        self.deadline = None;
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Cause;
    use std::future::{pending, ready};

    #[tokio::test]
    async fn pending_network_read_has_its_own_inactivity_bound() {
        let mut timer = ReadTimer::new(Some(Duration::from_millis(30)));
        let error = tokio::time::timeout(
            Duration::from_millis(500),
            timer.wait(Phase::Data, pending::<()>()),
        )
        .await
        .expect("read inactivity must end the pending read")
        .unwrap_err();
        assert_eq!(error.cause(), Some(Cause::ReadTimeout));
    }

    #[tokio::test]
    async fn cancelled_pending_read_does_not_restart_its_inactivity_deadline() {
        let mut timer = ReadTimer::new(Some(Duration::from_millis(50)));
        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                timer.wait(Phase::Data, pending::<()>()),
            )
            .await
            .is_err()
        );
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(
            timer.wait(Phase::Data, ready(7)).await.unwrap_err().cause(),
            Some(Cause::ReadTimeout)
        );
    }

    #[tokio::test]
    async fn completed_read_clears_deadline_before_caller_processing() {
        let mut timer = ReadTimer::new(Some(Duration::from_millis(30)));
        assert_eq!(timer.wait(Phase::Data, ready(7)).await.unwrap(), 7);
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(timer.wait(Phase::Data, ready(8)).await.unwrap(), 8);
    }
}
