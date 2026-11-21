//! Bounded disk workers. Cancellation can prevent the commit decision, but cannot
//! interrupt a running filesystem syscall or undo an already committed rename.
use crate::{
    AntalyaClient, Cause, Error, ExecutionLimits, ExecutionMetrics, Format, Outcome, Phase, Query,
};
use crate::{execution::tracked, resources::Lane};
use std::{
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, mpsc, oneshot},
    time::{Instant, timeout_at},
};

const CHUNK_BYTES: usize = 64 * 1024;
const QUEUE_DEPTH: usize = 2;
const ACTIVE: u8 = 0;
const CANCELLED: u8 = 1;
const COMMITTED: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Overwrite {
    Forbid,
    Replace,
}
#[derive(Clone, Copy, Debug)]
pub struct ExportOptions {
    pub overwrite: Overwrite,
    pub max_bytes: u64,
}
impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            overwrite: Overwrite::Forbid,
            max_bytes: 1024 * 1024 * 1024,
        }
    }
}
#[derive(Debug)]
pub struct ExportReport {
    pub query_id: String,
    pub bytes: u64,
    pub rows: u64,
    pub batches: u64,
    pub outcome: Outcome,
    pub metrics: ExecutionMetrics,
}

#[derive(Default)]
struct CommitDecision(AtomicU8);
impl CommitDecision {
    fn cancel(&self) {
        let _ = self
            .0
            .compare_exchange(ACTIVE, CANCELLED, Ordering::AcqRel, Ordering::Acquire);
    }
    fn commit(&self) -> bool {
        self.0
            .compare_exchange(ACTIVE, COMMITTED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
    fn cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire) == CANCELLED
    }
}
struct CancelOnDrop(Arc<CommitDecision>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

struct Chunk {
    bytes: Vec<u8>,
    _lease: crate::resources::ByteLease,
}
enum Work {
    Write(Chunk),
    Validate {
        lease: crate::resources::ByteLease,
        reply: oneshot::Sender<Result<Validated, Error>>,
    },
    Commit,
}
#[derive(Clone, Copy, Default)]
struct DiskMetrics {
    write: Duration,
    validation: Duration,
    sync: Duration,
    publication: Duration,
}
#[derive(Clone, Copy)]
struct Validated {
    rows: u64,
    batches: u64,
}
struct DiskJob {
    destination: PathBuf,
    options: ExportOptions,
    format: Format,
    limits: ExecutionLimits,
    deadline: Instant,
    decision: Arc<CommitDecision>,
    #[cfg(test)]
    hook: Option<TestHook>,
}
#[cfg(test)]
type TestHook = std::cell::RefCell<Box<dyn FnMut(Stage) + Send>>;
#[cfg(test)]
#[derive(PartialEq, Eq)]
enum Stage {
    Created,
    BeforeCommit,
    AfterCommit,
}

impl DiskJob {
    fn check(&self, phase: Phase) -> Result<(), Error> {
        if self.decision.cancelled() {
            return Err(Error::at(phase, Cause::InvalidProof));
        }
        if Instant::now() >= self.deadline {
            return Err(Error::at(phase, Cause::Timeout));
        }
        Ok(())
    }
    #[cfg(test)]
    fn stage(&self, stage: Stage) {
        if let Some(hook) = self.hook.as_ref() {
            hook.borrow_mut()(stage);
        }
    }
    fn run(
        self,
        receiver: mpsc::Receiver<Work>,
        ready: oneshot::Sender<Result<(), Error>>,
        _admission: OwnedSemaphorePermit,
    ) -> Result<DiskMetrics, Error> {
        // run_disk drops the tempfile and queued byte leases before admission.
        self.run_disk(receiver, ready)
    }
    fn run_disk(
        self,
        mut receiver: mpsc::Receiver<Work>,
        ready: oneshot::Sender<Result<(), Error>>,
    ) -> Result<DiskMetrics, Error> {
        let created = self.create();
        let mut temporary = match created {
            Ok(temporary) => {
                #[cfg(test)]
                self.stage(Stage::Created);
                let _ = ready.send(Ok(()));
                temporary
            }
            Err(error) => {
                let _ = ready.send(Err(error));
                return Err(error);
            }
        };
        let mut metrics = DiskMetrics::default();
        let mut validated = false;
        while let Some(work) = receiver.blocking_recv() {
            self.check(Phase::Write)?;
            match work {
                Work::Write(chunk) if !validated => {
                    let started = Instant::now();
                    temporary
                        .write_all(&chunk.bytes)
                        .map_err(|error| Error::from_io(Phase::Write, &error))?;
                    metrics.write += started.elapsed();
                }
                Work::Validate { lease, reply } if !validated => {
                    let started = Instant::now();
                    let result = self.validate(temporary.as_file_mut());
                    metrics.validation += started.elapsed();
                    drop(lease);
                    let _ = reply.send(result);
                    result?;
                    validated = true;
                }
                Work::Commit if validated => {
                    let started = Instant::now();
                    temporary
                        .as_file()
                        .sync_all()
                        .map_err(|error| Error::from_io(Phase::Sync, &error))?;
                    metrics.sync += started.elapsed();
                    #[cfg(test)]
                    self.stage(Stage::BeforeCommit);
                    self.check(Phase::Publication)?;
                    // This CAS is the publication linearization point. A drop that
                    // wins first forbids persist; cancellation after this point
                    // may observe the rename complete after its future is dropped.
                    if !self.decision.commit() {
                        return Err(Error::at(Phase::Publication, Cause::InvalidProof));
                    }
                    #[cfg(test)]
                    self.stage(Stage::AfterCommit);
                    let started = Instant::now();
                    match self.options.overwrite {
                        Overwrite::Forbid => temporary.persist_noclobber(&self.destination),
                        Overwrite::Replace => temporary.persist(&self.destination),
                    }
                    .map_err(|error| Error::from_io(Phase::Publication, &error.error))?;
                    metrics.publication += started.elapsed();
                    return Ok(metrics);
                }
                _ => return Err(Error::InvalidRequest),
            }
        }
        Err(Error::at(Phase::Publication, Cause::InvalidProof))
    }
    fn create(&self) -> Result<tempfile::NamedTempFile, Error> {
        self.check(Phase::Write)?;
        if self.options.overwrite == Overwrite::Forbid {
            match std::fs::symlink_metadata(&self.destination) {
                Ok(_) => {
                    return Err(Error::at(
                        Phase::Publication,
                        Cause::Io(std::io::ErrorKind::AlreadyExists),
                    ));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(Error::from_io(Phase::Publication, &error)),
            }
        }
        let parent = self
            .destination
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        tempfile::NamedTempFile::new_in(parent)
            .map_err(|error| Error::from_io(Phase::Write, &error))
    }
    fn validate(&self, file: &mut std::fs::File) -> Result<Validated, Error> {
        self.check(Phase::Validation)?;
        file.flush()
            .map_err(|error| Error::from_io(Phase::Write, &error))?;
        validate_ipc(file, self.format, self.limits, || {
            self.check(Phase::Validation)
        })
    }
}

impl AntalyaClient {
    /// Spools and validates on a bounded blocking worker, then publishes after
    /// exact terminal proof. All filesystem work and temporary cleanup run there.
    /// Dropping closes the queue and cancels publication if commit has not won;
    /// an in-flight syscall, or a rename whose commit already won, may finish.
    /// Queued copies are byte-accounted in pieces no larger than 64 KiB. Upstream
    /// transport buffers and transient IPC decoder allocations are not an RSS bound.
    pub async fn export(
        &self,
        query: Query,
        format: Format,
        destination: impl AsRef<Path>,
        options: ExportOptions,
    ) -> Result<ExportReport, Error> {
        if format == Format::Json || options.max_bytes == 0 {
            return Err(Error::InvalidRequest);
        }
        let started = Instant::now();
        let deadline = started
            .checked_add(self.config.timeout)
            .ok_or(Error::InvalidSetting)?;
        let mut metrics = ExecutionMetrics::default();
        let queued = Instant::now();
        let admission = self.resources.acquire(Lane::Export, deadline).await?;
        metrics.queued += queued.elapsed();
        let decision = Arc::new(CommitDecision::default());
        let guard = CancelOnDrop(decision.clone());
        let (sender, receiver) = mpsc::channel(QUEUE_DEPTH);
        let (ready, ready_receiver) = oneshot::channel();
        let job = DiskJob {
            destination: destination.as_ref().to_path_buf(),
            options,
            format,
            limits: self.config.execution_limits,
            deadline,
            decision,
            #[cfg(test)]
            hook: None,
        };
        let worker = tokio::task::spawn_blocking(move || job.run(receiver, ready, admission));
        let result = async {
            receive(ready_receiver, deadline, Phase::Write).await??;
            let queued = Instant::now();
            let data_permit = self.resources.acquire(Lane::Data, deadline).await?;
            metrics.queued += queued.elapsed();
            let (query, nonce) = tracked(self.scoped(&query));
            let mut cursor = self.fetch_bytes(&query, format)?;
            let mut bytes = 0u64;
            let chunk_size = CHUNK_BYTES.min(self.resources.retained_capacity());
            loop {
                let polled = Instant::now();
                let next = timeout_at(deadline, cursor.next()).await;
                metrics.data_poll += polled.elapsed();
                let next = next
                    .map_err(|_| Error::at(Phase::Data, Cause::Timeout))?
                    .map_err(|error| Error::from_upstream(Phase::Data, &error))?;
                let Some(chunk) = next else {
                    break;
                };
                bytes = bytes
                    .checked_add(
                        u64::try_from(chunk.len())
                            .map_err(|_| Error::at(Phase::Data, Cause::LimitExceeded))?,
                    )
                    .ok_or(Error::at(Phase::Data, Cause::LimitExceeded))?;
                if bytes > options.max_bytes || bytes > self.config.execution_limits.max_total_bytes
                {
                    return Err(Error::at(Phase::Data, Cause::LimitExceeded));
                }
                for piece in chunk.chunks(chunk_size) {
                    let queued = Instant::now();
                    let lease = self.resources.reserve_bytes(piece.len(), deadline).await?;
                    // Acquire before making the independent copy: slicing Bytes
                    // would retain its potentially much larger original allocation.
                    send(
                        &sender,
                        Work::Write(Chunk {
                            bytes: piece.to_vec(),
                            _lease: lease,
                        }),
                        deadline,
                    )
                    .await?;
                    metrics.queued += queued.elapsed();
                }
            }
            metrics.stream_end = Some(started.elapsed());
            drop(cursor);
            drop(data_permit);
            let queued = Instant::now();
            let lease = self
                .resources
                .reserve_bytes(self.config.execution_limits.max_batch_bytes, deadline)
                .await?;
            let (reply, response) = oneshot::channel();
            send(&sender, Work::Validate { lease, reply }, deadline).await?;
            metrics.queued += queued.elapsed();
            let validated = receive(response, deadline, Phase::Validation).await??;
            let (outcome, progress) = self
                .terminal_until(&query.id, &nonce, deadline, &mut metrics, None)
                .await;
            if outcome != Outcome::Succeeded {
                return Err(if outcome == Outcome::Unknown {
                    metrics
                        .last_error
                        .unwrap_or(Error::at(Phase::Control, Cause::InvalidProof))
                } else {
                    Error::at(Phase::Control, Cause::QueryFailed)
                });
            }
            if progress.and_then(|progress| progress.result_rows) != Some(validated.rows) {
                return Err(Error::at(Phase::Control, Cause::InvalidProof));
            }
            send(&sender, Work::Commit, deadline).await?;
            Ok((query.id, bytes, validated))
        }
        .await;
        if result.is_err() {
            guard.0.cancel();
        }
        drop(sender);
        // Normal failures wait for cleanup within the same deadline. Timeout or
        // dropping this future leaves the worker owning its permit and tempfile.
        let disk_result = timeout_at(deadline, worker)
            .await
            .map_err(|_| Error::at(Phase::Publication, Cause::Timeout))
            .and_then(|joined| {
                joined.map_err(|_| {
                    Error::at(Phase::Publication, Cause::Io(std::io::ErrorKind::Other))
                })
            })
            .and_then(|result| result);
        let (query_id, bytes, validated) = match result {
            Ok(report) => report,
            Err(error) => {
                // A closed worker channel is a symptom; preserve its actual I/O
                // failure when available instead of reporting only BrokenPipe.
                if error.cause() == Some(Cause::Io(std::io::ErrorKind::BrokenPipe)) {
                    disk_result?;
                }
                return Err(error);
            }
        };
        let disk = disk_result?;
        metrics.write = disk.write;
        metrics.validation = disk.validation;
        metrics.sync = disk.sync;
        metrics.publication = disk.publication;
        metrics.total = started.elapsed();
        Ok(ExportReport {
            query_id,
            bytes,
            rows: validated.rows,
            batches: validated.batches,
            outcome: Outcome::Succeeded,
            metrics,
        })
    }
}
async fn send(sender: &mpsc::Sender<Work>, work: Work, deadline: Instant) -> Result<(), Error> {
    timeout_at(deadline, sender.send(work))
        .await
        .map_err(|_| Error::at(Phase::Write, Cause::Timeout))?
        .map_err(|_| Error::at(Phase::Write, Cause::Io(std::io::ErrorKind::BrokenPipe)))
}
async fn receive<T>(
    receiver: oneshot::Receiver<T>,
    deadline: Instant,
    phase: Phase,
) -> Result<T, Error> {
    timeout_at(deadline, receiver)
        .await
        .map_err(|_| Error::at(phase, Cause::Timeout))?
        .map_err(|_| Error::at(phase, Cause::Io(std::io::ErrorKind::BrokenPipe)))
}
fn validate_ipc(
    file: &mut std::fs::File,
    format: Format,
    limits: ExecutionLimits,
    check: impl Fn() -> Result<(), Error>,
) -> Result<Validated, Error> {
    let invalid = || Error::at(Phase::Validation, Cause::Decoder);
    let limited = || Error::at(Phase::Validation, Cause::LimitExceeded);
    file.seek(SeekFrom::Start(0))
        .map_err(|error| Error::from_io(Phase::Validation, &error))?;
    let mut result = Validated {
        rows: 0,
        batches: 0,
    };
    let mut total_bytes = 0u64;
    let mut count =
        |batch: Result<arrow_array::RecordBatch, arrow_schema::ArrowError>| -> Result<(), Error> {
            check()?;
            let batch = batch.map_err(|_| invalid())?;
            let bytes =
                crate::memory::batch_bytes(&batch, limits.max_batch_bytes).ok_or_else(limited)?;
            result.rows = result
                .rows
                .checked_add(u64::try_from(batch.num_rows()).map_err(|_| limited())?)
                .ok_or_else(limited)?;
            result.batches = result.batches.checked_add(1).ok_or_else(limited)?;
            total_bytes = total_bytes
                .checked_add(u64::try_from(bytes).map_err(|_| limited())?)
                .ok_or_else(limited)?;
            if result.rows > limits.max_rows
                || result.batches > limits.max_batches
                || total_bytes > limits.max_total_bytes
                || bytes > limits.max_batch_bytes
            {
                return Err(limited());
            }
            Ok(())
        };
    match format {
        Format::Arrow => {
            let reader =
                arrow_ipc::reader::FileReader::try_new(file, None).map_err(|_| invalid())?;
            for batch in reader {
                count(batch)?;
            }
        }
        Format::ArrowStream => {
            if file
                .metadata()
                .map_err(|error| Error::from_io(Phase::Validation, &error))?
                .len()
                < 8
            {
                return Err(invalid());
            }
            file.seek(SeekFrom::End(-8))
                .map_err(|error| Error::from_io(Phase::Validation, &error))?;
            let mut end = [0; 8];
            file.read_exact(&mut end)
                .map_err(|error| Error::from_io(Phase::Validation, &error))?;
            if end != [255, 255, 255, 255, 0, 0, 0, 0] {
                return Err(invalid());
            }
            file.seek(SeekFrom::Start(0))
                .map_err(|error| Error::from_io(Phase::Validation, &error))?;
            let mut reader = arrow_ipc::reader::StreamReader::try_new(&mut *file, None)
                .map_err(|_| invalid())?;
            for batch in &mut reader {
                count(batch)?;
            }
            drop(reader);
            if file
                .stream_position()
                .map_err(|error| Error::from_io(Phase::Validation, &error))?
                != file
                    .metadata()
                    .map_err(|error| Error::from_io(Phase::Validation, &error))?
                    .len()
            {
                return Err(invalid());
            }
        }
        Format::Json => return Err(invalid()),
    }
    check()?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_before_commit_forbids_publication() {
        let decision = CommitDecision::default();
        decision.cancel();
        assert!(!decision.commit());
    }

    #[test]
    fn committed_decision_cannot_be_revoked_by_late_cancellation() {
        let decision = CommitDecision::default();
        assert!(decision.commit());
        decision.cancel();
        assert!(!decision.cancelled());
    }

    #[test]
    fn cancellation_and_commit_have_exactly_one_winner() {
        for _ in 0..100 {
            let decision = Arc::new(CommitDecision::default());
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let other = decision.clone();
            let other_barrier = barrier.clone();
            let thread = std::thread::spawn(move || {
                other_barrier.wait();
                other.cancel();
            });
            barrier.wait();
            let committed = decision.commit();
            thread.join().unwrap();
            assert_ne!(committed, decision.cancelled());
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocked_disk_does_not_block_runtime_and_holds_admission_until_cleanup() {
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("result");
        std::fs::write(&destination, b"previous").unwrap();
        let admission = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = admission.clone().acquire_owned().await.unwrap();
        let decision = Arc::new(CommitDecision::default());
        let guard = CancelOnDrop(decision.clone());
        let (sender, receiver) = mpsc::channel(QUEUE_DEPTH);
        let (ready, _ready_receiver) = oneshot::channel();
        let (entered, entered_receiver) = oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let mut entered = Some(entered);
        let job = DiskJob {
            destination,
            options: ExportOptions {
                overwrite: Overwrite::Replace,
                ..ExportOptions::default()
            },
            format: Format::ArrowStream,
            limits: ExecutionLimits::default(),
            deadline: Instant::now() + std::time::Duration::from_secs(3),
            decision,
            hook: Some(std::cell::RefCell::new(Box::new(move |stage| {
                if stage == Stage::Created {
                    let _ = entered.take().unwrap().send(());
                    released.recv().unwrap();
                }
            }))),
        };
        let worker = tokio::task::spawn_blocking(move || job.run(receiver, ready, permit));
        timeout_at(
            Instant::now() + std::time::Duration::from_secs(1),
            entered_receiver,
        )
        .await
        .unwrap()
        .unwrap();
        let bytes = crate::resources::Resources::new(&crate::ResourceLimits {
            max_retained_bytes: 2,
            ..Default::default()
        })
        .unwrap();
        let byte_deadline = Instant::now() + Duration::from_secs(3);
        for _ in 0..QUEUE_DEPTH {
            let lease = bytes.reserve_bytes(1, byte_deadline).await.unwrap();
            sender
                .send(Work::Write(Chunk {
                    bytes: vec![1],
                    _lease: lease,
                }))
                .await
                .unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        drop(guard);
        drop(sender);
        assert_eq!(admission.available_permits(), 0);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(5),
                bytes.reserve_bytes(1, byte_deadline)
            )
            .await
            .is_err()
        );
        release.send(()).unwrap();
        assert!(worker.await.unwrap().is_err());
        assert_eq!(admission.available_permits(), 1);
        assert_eq!(
            bytes
                .reserve_bytes(2, byte_deadline)
                .await
                .unwrap()
                .num_permits(),
            2
        );
        assert_eq!(
            std::fs::read(directory.path().join("result")).unwrap(),
            b"previous"
        );
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    fn stream_bytes(batch_count: usize) -> Vec<u8> {
        let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "v",
            arrow_schema::DataType::Int32,
            false,
        )]);
        let batch = arrow_array::RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(arrow_array::Int32Array::from(vec![1, 2]))],
        )
        .unwrap();
        let mut bytes = Vec::new();
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &schema).unwrap();
        for _ in 0..batch_count {
            writer.write(&batch).unwrap();
        }
        writer.finish().unwrap();
        drop(writer);
        bytes
    }

    #[test]
    fn validation_enforces_each_decoded_limit_and_rejects_early_eos() {
        let bytes = stream_bytes(2);
        for limits in [
            ExecutionLimits {
                max_rows: 3,
                ..ExecutionLimits::default()
            },
            ExecutionLimits {
                max_batches: 1,
                ..ExecutionLimits::default()
            },
            ExecutionLimits {
                max_batch_bytes: 1,
                ..ExecutionLimits::default()
            },
            ExecutionLimits {
                max_total_bytes: 1,
                ..ExecutionLimits::default()
            },
        ] {
            let mut file = tempfile::tempfile().unwrap();
            file.write_all(&bytes).unwrap();
            let result = validate_ipc(&mut file, Format::ArrowStream, limits, || Ok(()));
            assert!(result.is_err_and(|error| error.kind() == crate::ErrorKind::LimitExceeded));
        }
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&bytes).unwrap();
        file.write_all(&[255, 255, 255, 255, 0, 0, 0, 0]).unwrap();
        let result = validate_ipc(
            &mut file,
            Format::ArrowStream,
            ExecutionLimits::default(),
            || Ok(()),
        );
        assert!(result.is_err_and(|error| error.kind() == crate::ErrorKind::InvalidIpc));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancellation_at_commit_boundary_preserves_or_publishes_atomically() {
        for stage_to_block in [Stage::BeforeCommit, Stage::AfterCommit] {
            let should_publish = stage_to_block == Stage::AfterCommit;
            let directory = tempfile::tempdir().unwrap();
            let destination = directory.path().join("result");
            std::fs::write(&destination, b"previous").unwrap();
            let admission = Arc::new(tokio::sync::Semaphore::new(1));
            let permit = admission.clone().acquire_owned().await.unwrap();
            let decision = Arc::new(CommitDecision::default());
            let guard = CancelOnDrop(decision.clone());
            let (sender, receiver) = mpsc::channel(QUEUE_DEPTH);
            let (ready, ready_receiver) = oneshot::channel();
            let (entered, entered_receiver) = oneshot::channel();
            let (release, released) = std::sync::mpsc::channel();
            let mut entered = Some(entered);
            // This guards a barrier around real fsync under host load; it is
            // not a disk-latency bound or a timeout-behavior assertion.
            let deadline = Instant::now() + Duration::from_secs(30);
            let job = DiskJob {
                destination: destination.clone(),
                options: ExportOptions {
                    overwrite: Overwrite::Replace,
                    ..ExportOptions::default()
                },
                format: Format::ArrowStream,
                limits: ExecutionLimits::default(),
                deadline,
                decision,
                hook: Some(std::cell::RefCell::new(Box::new(move |stage| {
                    if stage == stage_to_block {
                        let _ = entered.take().unwrap().send(());
                        let _ = released.recv();
                    }
                }))),
            };
            let worker = tokio::task::spawn_blocking(move || job.run(receiver, ready, permit));
            receive(ready_receiver, deadline, Phase::Write)
                .await
                .unwrap()
                .unwrap();
            let bytes = stream_bytes(1);
            let budget = crate::resources::Resources::new(&crate::ResourceLimits {
                max_retained_bytes: bytes.len(),
                ..Default::default()
            })
            .unwrap();
            let lease = budget.reserve_bytes(bytes.len(), deadline).await.unwrap();
            send(
                &sender,
                Work::Write(Chunk {
                    bytes: bytes.clone(),
                    _lease: lease,
                }),
                deadline,
            )
            .await
            .unwrap();
            let lease = budget.reserve_bytes(1, deadline).await.unwrap();
            let (reply, response) = oneshot::channel();
            send(&sender, Work::Validate { lease, reply }, deadline)
                .await
                .unwrap();
            assert_eq!(
                receive(response, deadline, Phase::Validation)
                    .await
                    .unwrap()
                    .unwrap()
                    .rows,
                2
            );
            send(&sender, Work::Commit, deadline).await.unwrap();
            receive(entered_receiver, deadline, Phase::Publication)
                .await
                .unwrap();
            drop(guard);
            drop(sender);
            assert_eq!(std::fs::read(&destination).unwrap(), b"previous");
            assert_eq!(admission.available_permits(), 0);
            release.send(()).unwrap();
            assert_eq!(worker.await.unwrap().is_ok(), should_publish);
            assert_eq!(
                std::fs::read(&destination).unwrap(),
                if should_publish {
                    bytes
                } else {
                    b"previous".to_vec()
                }
            );
            assert_eq!(admission.available_permits(), 1);
            assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
        }
    }
}
