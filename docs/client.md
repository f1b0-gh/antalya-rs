# Client guide

[Getting started](../README.md) · [Testing](testing.md)

Start with `fetch_arrow` and `next_batch`, `execute_command` for DDL, or `export`
for validated files. The prepared/admitted APIs support applications that persist
intent and authorize dispatch and recovery separately.

## Configuration and credentials

`Config` sets the endpoint, bearer credentials, database and operation timeout.
Query-local settings apply only to that query. Enable `tls` for HTTPS with system
trust. Configured transports disable redirects, proxy discovery and automatic
retries. `Config::pool` sets idle connection limits for both HTTP transports;
defaults are two connections per host with a two-second idle lifetime.

Reuse the client. Clones share connection pools, admission lanes and the byte
budget. `client.with_bearer(new_token)?` creates a view with new credentials and
those same resources; existing clones and prepared executions keep their original
credentials. Applications acquire and refresh tokens. Tokens are syntax-checked
and redacted from debug output. The examples read `ANTALYA_TOKEN`.

Clients with JWT receiver constraints use `with_jwt_receiver(profile, token)`
instead, so changing credentials also validates the new receiver profile.

## Queries and parameters

Supply application-approved SQL without a trailing semicolon or FORMAT clause.
`Query::parameter` accepts strings or a `Parameter::{String,Int64,UInt64,Bool,Null}`.
Match the SQL placeholder type, for example `{value:Nullable(Int64)}`. String
escaping distinguishes a literal backslash-N from the server's null encoding.

For HTTP request construction without Arrow, disable default features and use
`Query::build_request` or `build_command_request`. The latter omits FORMAT for
DDL. A caller-owned HTTP client supplies transport policy and completion checks.

## Results and completion

`Execution::next_batch()` yields provisional `BudgetedBatch` values, then checks
completion before returning `None`. Inspect `outcome()` for
`Running`, `Succeeded`, `Failed`, `Cancelled` or `Unknown`. A failure to verify
completion returns a typed error and leaves the result `Unknown`.

For a UI that displays results before confirmation, call
`next_provisional_batch()` until `None`, retain the execution, and call
`confirm_completion()`. The outcome stays `Running` and admission remains held
until confirmation. Calling confirmation before data EOF returns an error.

| Strategy | Requirements and behavior |
|---|---|
| `FlushFirst` (default) | Permitted logging settings and SELECT on `system.query_log`. Attempts `SYSTEM FLUSH LOGS query_log`, then polls. |
| `ObserveOnly` | Same logging requirements, without an explicit flush. Allow for the server's publication interval: 7.5 seconds in the fixtures. |
| `InBand` | Framing-capable server. High-level Arrow completion is verified from the response. See [framed completion](#measured-arrow-transport-and-framed-completion). |

Query-log verification matches the query ID, a private random marker and, for
authorized requests, the principal. It requires one start, one finish, no
exception and an exact decoded row count. Lookups include a date lower bound
based on preparation time minus one day. Data and verification requests need
backend affinity behind a load balancer. `InBand` avoids that requirement for
normal completion; cancellation and recovery still use query-log observation.

`readonly=2` can use `ObserveOnly` when query logging is permitted. `readonly=1`
locks required settings, so the client returns the server refusal promptly.
Logging, framing and Arrow compression settings are reserved by the client.

`refresh_progress()` observes the matching `system.processes` entry. `cancel()`
issues KILL for the query ID and private marker, then verifies termination; it
requires KILL permission. If cancellation races with server success, discarded
output remains unconfirmed and can be reconciled separately.

Three contracts apply throughout the API:

- Received batches are provisional until completion is confirmed. Transport EOF
  and HTTP status alone cannot establish a complete result.
- Dropping a future or execution does not confirm remote cancellation. Recovery
  observes the original operation; the library never automatically replays it.
- The application authorizes SQL, settings, data access and recovery. Credentials
  authenticate requests; a receipt records identity rather than granting access.

## Resource limits and diagnostics

`Config::timeout` bounds the entire operation, including admission, time spent
processing batches, and completion verification. The default is 60 seconds.
`read_timeout` optionally limits an inactive streaming/control response read;
successful reads reset it, and cancelled borrowed polls preserve it. It covers
plain/framed Arrow, admitted responses, exports and control HTTP requests.
Convenience commands use the operation deadline; raw cursors require
caller-managed deadlines.

`capacity_wait_timeout` gives a reservation blocked by retained output a
one-second wait budget by default. Pending network reads
keep their reservations, but waiting on those reads uses the operation deadline.
A waiter observing only active work resets its retention wait budget. Zero refuses immediately under retention pressure; a longer
value allows more backpressure. Cancelling and restarting a reservation wait can
restart this timer, but cannot extend the operation deadline.

A retained-output wait expiring reports `ErrorKind::LimitExceeded` /
`Cause::CapacityTimeout`. The blocking output may belong to any query sharing the
client, including another clone or credential view. Read inactivity reports
`Cause::ReadTimeout`. Reaching the total operation deadline reports
`ErrorKind::Timeout` / `Cause::Timeout`, including while waiting for byte capacity.

| Configuration | Defaults | Scope |
|---|---|---|
| `ResourceLimits` | 16 active data, 64 queued per lane, 4 control, 2 export tasks | Shared by client views; zero queue capacity refuses excess work immediately |
| `max_retained_bytes` | 64 MiB | Accounted batches, pending read reservations and export buffers |
| `ExecutionLimits` | 1M rows, 65536 batches, 512 MiB cumulative, 16 MiB per batch | Per stream/export; exports also enforce their wire-byte limit |
| `PoolOptions` | 2 idle connections/host, 2s idle lifetime | Per transport |

Reservations are atomic and acquired before reading/decoding. A blocked large
claim leaves free capacity available to smaller claims; admission is not FIFO.
The high-level reader reserves `max_batch_bytes` before waiting for data. Query
admission and pending-read concurrency are separate limits. With no other byte
reservations, at most
`min(max_active_queries, max_retained_bytes / max_batch_bytes)` reads can hold
reservations at once (integer division). **Defaults admit 16 queries but fund
only four simultaneous reads**: 64 MiB / 16 MiB. Retained output, exports and
admitted decoder buffers reduce the capacity available for those reads.

For up to 16 pending reads, a 64 MiB budget with 4 MiB maximum batches, or a
256 MiB budget with 16 MiB maximum batches, provides the reservation capacity
before other storage is counted. Lower the batch limit only when actual batches
fit it; allow additional shared capacity for output consumers keep.

Consume and drop batches to stream more than the shared budget. Collecting results
requires room for the collection plus the next full reservation. `BudgetedBatch`
dereferences to `RecordBatch`; retain its wrapper while using its arrays. Wrapper
clones share a lease. Pending reservations survive cancellation of a batch poll.
`into_untracked()` transfers accounting responsibility to the caller; extracting
raw array clones also leaves the wrapper's lifetime accounting. The budget covers
accounted storage, not every transport/decoder allocation or process RSS.

Admitted readers track frames, schemas and dictionaries separately from returned
output. A nested claim that cannot fit even after external output is released
returns `Cause::Capacity`, avoiding internal allocation cycles. This conservative
check may refuse a reader while a peer could later release its state. The reader
discards buffered data and leaves its result unconfirmed.

`Execution::metrics()`, `CommandReport` and `ExportReport` expose timings and typed
failures. Phases overlap: use the total duration rather than adding phase values.
`Error::kind()` gives a category; `phase()`, `cause()` and `source()` provide
sanitized diagnostics. Errors preserve HTTP status, numeric server codes and
static explanations for common codes. Raw server text is omitted because it can
contain SQL or credentials. Use the query ID to investigate server logs.

`execute_command()` returns a `CommandReport`; inspect `outcome` and
`metrics.last_error`. Setting or privilege refusals return an `Unknown` report
with the server code immediately, without polling for a missing terminal record.

## Validated IPC exports

`client.export(query, Format::Arrow | Format::ArrowStream, path, options)` writes
private temporary storage, enforces `max_bytes`, validates Arrow batches and
completion, then publishes atomically. `Overwrite::Forbid` is the default.
`Replace` overwrites only after validation. Bounded blocking workers perform
filesystem operations. A two-message queue carries charged copies of at most
64 KiB; validation reserves a maximum-batch allowance.

Cancellation before the commit decision preserves the destination. A commit
already in progress may finish after the future is dropped. Worker cleanup keeps
its permit until it finishes. Publication provides atomic visibility; power-loss
durability requires the appropriate filesystem guarantees.

## Recovery and prepared execution

`confirm_termination()` observes the original query with a fresh bounded deadline
and returns `RemoteTermination::{Succeeded,Failed,Cancelled}` when proven.
It leaves the delivered result's outcome unchanged: proof that the server finished
cannot reconstruct lost rows.

For durable intent, `prepare_authorized_arrow` checks admission and identity before
creating a data request. Persist `recovery_receipt()` before `start()`. Dropping a
prepared value sends no data query; journal latency consumes the original deadline.
Restore trusted receipt fields with `RecoveryReceipt::restore`, including
`log_since()` via `with_log_since`, and use a client on the same endpoint to observe
termination. Receipts without a date bound search older logs without that filter.
Correct host clocks that differ by more than one day.

For separate synchronous dispatch, call `ready_transport().await` to establish a
dedicated HTTP/1 connection without sending the query. Revalidate host authority,
then call `admit()`. The request is handed to the ready transport before this
returns; response polling only receives data. Expiry or unavailable transport
refuses admission. A connection failure racing admission remains uncertain.
Async methods require a Tokio runtime with I/O and timers enabled.

## Receiver expiry profiles

`Config::jwt_receiver(profile, bearer)` binds credentials to a host-reviewed
receiver expiry policy. The host must enforce signature validation, exact
issuer/audience, `username_claim=sub`, required expiry, `verifier_leeway=0`, no
token cache, and bounded server-clock lag at every entry point. Local claim
parsing constrains sending; the server authenticates the token. This profile does
not implement revocation or distributed owner fencing.

`with_jwt_receiver(profile, bearer)` keeps shared resources and existing
preparations intact. A current credential view can observe an older receipt and
apply the proof to its original response owner.

`admit_arrow_before(unix_seconds)` requires
`exp + maximum_clock_lag_seconds + 1` to fit the host admission deadline, and checks
local expiry and the original principal. The extra second covers timestamp
precision. Integer expiry is restricted to `1..=u32::MAX`. Tokens are limited to
8 KiB, decoded headers to 1 KiB, payloads to 4 KiB, and objects to 32 distinct
members. Duplicate claims, unsupported critical extensions and ambiguous expiry
are rejected. Plain `admit`, `admit_ipc` and `admit_arrow` omit this receiver guard.

## Admitted transport and IPC frames

`AdmittedResponse::next_chunk()` yields leased response bytes without control
requests. `ReadyExecution::admit_ipc()` selects an IPC framer and checks room for
a maximum frame plus a wire chunk before dispatch. `into_ipc()` can also select
framing on a response before any raw bytes have been consumed.

`AdmittedIpc::next_frame()` yields leased encoded bytes, metadata and body. Partial
framing survives cancelled polls. Each frame allows 64 KiB metadata,
`max_batch_bytes` body and eight prefix bytes. FlatBuffers verification bounds
depth (32), tables (4096) and apparent size (256 KiB). Invalid lengths/layouts,
missing or repeated schemas, compressed batches, truncation and trailing bytes
after EOS are rejected. `declared_rows()` counts untrusted declarations; use the
Arrow decoder to validate values and exact row counts.

## Admitted Arrow decoding

`ReadyExecution::admit_arrow()` selects `AdmittedArrow::next_batch()`, which yields
`BudgetedBatch` values without terminal-control I/O. It validates schema features,
layouts and values before delivery. Input frames retain leases through shared
Arrow buffers; separate reservations cover decoded values, schema conversion and
dictionary growth. Dictionary replacement preserves earlier returned batches.

Schema work, each batch and total dictionaries are bounded by `max_batch_bytes`.
Logical array/dictionary lengths are bounded to eight times that value. Returned
batches include a conservative schema charge; shared buffers may be charged more
than once. `shared_schema()` clones only the schema Arc: separately account for
`schema_bytes()` if retaining that clone beyond its decoder.

After clean decoded EOF, explicitly call `confirm_completion()`, authorizing its
control I/O separately when required by the host. Alternatively, call
`client.observe_terminal(&receipt)` through a separately admitted current client,
then `accept_completion(&observation)` on the original owner. The opaque
observation binds the original receipt, principal, row count and terminal proof;
local acceptance checks the original operation deadline. Foreign observations
leave the owner intact.

`accept_termination` and `confirm_termination` support recovery after discarded
output. An admitted slot is released only after proven remote termination and
response EOF or discard. Early stop evidence retains the slot while unread data
remains. Dropping an unresolved admitted response permanently quarantines its
slot in that client; retain the owner for reconciliation. `cancel()` invalidates
delivery before requesting remote cancellation; cancelling that future keeps the
slot quarantined. A new client does not recover an outstanding-work ledger.

## Commands and durable side effects

For DDL requiring durable intent, use `prepare_authorized_command` with
`Format::Json`. Persist its receipt, then choose either driver:

- `start()` returns `CommandExecution`; `run()` sends once and returns a report.
  After interruption, `reconcile()` observes the original receipt with a fresh
  deadline. Repeated `run()` calls do not redispatch.
- `ready_transport().await` followed by `admit_before(unix_seconds)` sends under
  the reviewed receiver guard. Plain `admit()` omits that guard. Read response
  chunks, observe the receipt separately, then call `accept_termination`.

A command's effect can be proven after a lost reply using its exact terminal
receipt. An unresolved dispatched owner retains admission; dropping it
permanently quarantines that slot. Recover the durable outstanding-work ledger
before admitting work after a restart. Absence of a log record remains Unknown,
including for a prepared operation that might never have been sent.

The simpler `execute_command()` returns a report without a durable pre-dispatch
receipt or retained recovery owner. Bound parameter/setting counts and values,
database names and generic token sizes in the application; request construction
can copy these inputs.

## Raw transport escape hatches

`client.fetch_bytes` returns an unvalidated upstream cursor using configured
credentials. The caller supplies admission, total deadlines, IPC validation and
completion checks. Prefer `export` for files and `fetch_arrow` for decoded results.

## Host authorization

`AuthorizedRequest` freezes approved SQL, parameters, settings, destination,
format, expected principal and receipt ID. Construct a new request after a new
host authorization decision to change them. Configured authorized preparation
checks the target, format and server's `currentUser()`; it rejects a configured
database that would alter an unspecified frozen database. Revalidate expiry and
revocation before dispatch and recovery.

## SQL helpers

`sql::{Catalog, Hybrid, ExportPart}` build catalog, Hybrid table and Parquet export
commands. Identifiers and values are quoted separately. Catalog credentials travel
in the SQL body; treat DDL and stored metadata as sensitive. Both fixtures test
masking in responses, SQL-visible metadata and query-log fields.

`client.capabilities().await?` checks engines/settings and the supported server
family for these helpers: 26.6 from 26.6.8.20004 and 26.8 from 26.8.34.20001.
Later patches within those families are accepted; other families need crate
qualification. Ordinary query execution does not use this helper version gate.
Use `execute_command()` and inspect its report for DDL. Iceberg metadata updates
and Puffin operations belong to a separate `iceberg-rust` integration.

## Measured Arrow transport and framed completion

High-level metrics distinguish HTTP body/request wait (`transfer`), Arrow
validation/decoding, and NDJSON/base64 processing (`framing`). Transfer includes
server execution and scheduling. `wire_bytes` counts HTTP entity bytes, excluding
headers and network overhead. Use the profiling example for deployment timings.

`InBand` selects `JSONEachPacketBase64` around ArrowStream. Completion requires
the response query ID, the expected base64 content type, valid Arrow data, final
counter-bearing progress matching decoded rows, and clean HTTP EOF. Exceptions,
duplicate/trailing packets and truncation fail verification. It completes without
a query-log round trip or fallback.

The default 26.6 fixture lacks `framing_output_format`; use the separate compatible
fixture to qualify this mode. Commands and admitted transports reject `InBand`
before dispatch. Query logging remains enabled for explicit recovery/cancellation.
The measured decoder validates metadata, layouts and dictionary growth, requests
uncompressed IPC and rejects compressed bodies before expansion.

See [testing](testing.md) for fixture versions and reproducible commands.
