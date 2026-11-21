// SPDX-License-Identifier: Apache-2.0
use crate::Error;
use std::time::Duration;

/// Per-operation clocks, including elapsed time from cancelled polling attempts.
/// Durations may overlap and must not be blindly summed.
/// No SQL, credentials, endpoint, or row data are retained.
#[derive(Clone, Debug, Default)]
pub struct ExecutionMetrics {
    pub queued: Duration,
    pub identity: Duration,
    /// Time polling the data cursor, combining body/server wait, framing and decoding.
    pub data_poll: Duration,
    /// Offsets from operation start, including admission and authorization identity check.
    pub first_batch: Option<Duration>,
    pub stream_end: Option<Duration>,
    pub terminal: Duration,
    pub total: Duration,
    /// HTTP body/request wait including server execution and scheduling.
    /// None means this path is not instrumented; this is not pure network time.
    pub transfer: Option<Duration>,
    /// Synchronous Arrow envelope validation and decoding elapsed time.
    pub decode: Option<Duration>,
    /// HTTP entity bytes, excluding headers/TCP/TLS; framed mode includes envelopes.
    pub wire_bytes: u64,
    /// NDJSON/base64 processing, separate from Arrow decoding.
    pub framing: Duration,
    pub control_requests: u64,
    pub flush_attempts: u64,
    pub flush_time: Duration,
    pub observe_time: Duration,
    pub poll_sleeps: u64,
    pub flush_failures: u64,
    pub polls: u64,
    pub poll_wait: Duration,
    pub last_error: Option<Error>,
    /// A failed FLUSH need not prevent later proof from the server's asynchronous log flush.
    pub last_control_error: Option<Error>,
    pub write: Duration,
    pub validation: Duration,
    pub sync: Duration,
    pub publication: Duration,
}

/// Adds elapsed time even when the caller drops the future before it completes.
pub(crate) async fn timed<T>(
    duration: &mut Duration,
    future: impl std::future::Future<Output = T>,
) -> T {
    let _measurement = Measurement {
        duration,
        started: tokio::time::Instant::now(),
    };
    future.await
}

struct Measurement<'a> {
    duration: &'a mut Duration,
    started: tokio::time::Instant,
}
impl Drop for Measurement<'_> {
    fn drop(&mut self) {
        *self.duration = self.duration.saturating_add(self.started.elapsed());
    }
}

/// Allows terminal work to update all metrics while its encompassing timer is
/// recorded on cancellation. The exclusive borrow prevents observing the temporary
/// empty target; drop restores every update made by the inner phase timers.
pub(crate) struct TerminalMeasurement<'a> {
    target: &'a mut ExecutionMetrics,
    pub(crate) metrics: ExecutionMetrics,
    started: tokio::time::Instant,
}
impl<'a> TerminalMeasurement<'a> {
    pub(crate) fn new(target: &'a mut ExecutionMetrics) -> Self {
        Self {
            metrics: std::mem::take(target),
            target,
            started: tokio::time::Instant::now(),
        }
    }
}
impl Drop for TerminalMeasurement<'_> {
    fn drop(&mut self) {
        self.metrics.terminal = self.metrics.terminal.saturating_add(self.started.elapsed());
        *self.target = std::mem::take(&mut self.metrics);
    }
}
