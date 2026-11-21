# Client guide

[Getting started](../README.md) · [Testing](testing.md)

## Configuration and credentials

`Config` shares endpoint, bearer identity, database and operation timeout across
configured paths. Query-local database/settings/parameters never modify later
queries. `tls` forwards Rustls support to reqwest and ClickHouse, using system
trust. Configured clients disable redirects, proxy discovery and reqwest retries.
The upstream Hyper client also disables cancelled-request retries. Each transport
defaults to two idle connections per host for two seconds. `Config::pool` accepts
validated `PoolOptions` and applies them to both transports. Clones share bounded
data/control/export admission and retained-byte budgets; separate clients have
separate budgets and immutable bearer identities.
Token acquisition/refresh belongs to the host; operations are never replayed by
this library. Tokens are validated before network access and redacted from
configuration debugging. Use `ANTALYA_TOKEN` for the examples; they reject legacy username/password variables.

## Queries and parameters

`Query::parameter` escapes strings; `typed_parameter` accepts `Parameter::Int64`,
`UInt64`, `Bool`, `String` and `Null`. Supply the matching ClickHouse type in the
SQL placeholder, such as `{value:Nullable(Int64)}`. `Null` uses the server's `\N`
encoding, distinct from a literal string containing backslash-N.

For the small JSON request path, use `default-features = false` and
`Query::build_request(&reqwest_client, &endpoint, &credentials, format)`.
`build_command_request` and configured `execute_command` avoid appending FORMAT
to DDL. SQL is application-approved input without a trailing semicolon or FORMAT;
this crate does not parse SQL or authorize it. A caller-owned reqwest transport
is responsible for its own redirects, timeouts, retries and completion checks.

## Results and completion

Execution retains query ID, delivered rows/batches, optional observed progress,
and `Outcome::{Running,Succeeded,Failed,Cancelled,Unknown}`. `next_batch()` returns
incremental provisional batches and reports late errors. With a query-log terminal strategy, success requires a
unique execution marker in `system.query_log`, exactly one start and finish,
and no exception. Arrow results additionally require a decoded schema and a
row count matching `query_log.result_rows`, detecting loss of whole batches at
otherwise valid IPC boundaries. Transport EOF, IPC validity, HTTP 200 and progress alone are
insufficient. A failure to verify is `Unknown`, with a typed error (inspect `kind()` and
`cause()`), including
unavailable query logging. Decoder/transport failures cannot prove the server
stopped. No automatic retry follows data delivery.

Applications that explicitly support provisional display can consume
`next_provisional_batch()` until `None`, then call `confirm_completion()`.
Provisional EOF means the bounded data stream ended; the outcome stays `Running`
and admission stays held. It is not success. Confirmation before data EOF refuses
without consuming data. Confirmation after EOF uses the same deadline, private
marker, principal and result-row proof as the strict API. `next_batch()` keeps its
existing behavior and confirms before returning EOF.

Hosts remain responsible for access checks before showing any provisional page,
for clear pending/unconfirmed UI states, and for retaining the execution while
confirming in background. A dropped UI or HTTP connection does not prove native
cancellation. Do not present provisional latency as confirmed-result latency.

Query-log verification needs SELECT on `system.query_log`; the server must permit
query-local logging settings. The default `TerminalPolicy` tries `SYSTEM FLUSH
LOGS`, then observes with bounded exponential polling (10–100 ms). Opt-in
`TerminalStrategy::ObserveOnly` is an explicit alternative for principals relying
on asynchronous query-log publication. It issues no `SYSTEM FLUSH LOGS`; the
same fresh identity/nonce-bound terminal proof, result-row count checks, backoff
bounds and original deadline remain required. It does not turn EOF into success
or change the default strategy. It avoids a known refused flush, not the log
publication delay itself.

`TerminalStrategy::ObserveFirst` checks for complete proof before forcing one
flush, avoiding that call only if proof already exists. A refused flush is recorded
and asynchronous log visibility can still establish success. Identity checks, data transfer and
verification share one operation deadline. Cancellation gets its own deadline.
The lifecycle settings `log_comment`, `log_queries`, `log_queries_probability`,
`log_queries_min_query_duration_ms`, `log_query_settings` and
`output_format_arrow_compression_method`, along with framing settings, are reserved: `Query::setting` rejects
caller overrides before authorization freezes the query. Configured execution
adds a random log marker and enables query/settings logging; the upstream Arrow
extension selects uncompressed IPC. These are fixed lifecycle semantics, not
host-overridable policy fields. This prevents stale entries for reused query IDs from
proving completion. A load balancer must keep data and verification requests on
the same server; otherwise verification remains unknown. `refresh_progress`
observes the matching `system.processes` entry. `cancel()` issues a synchronous
KILL scoped to both ID and marker and confirms a cancellation exception in the
query log. It needs KILL permission. Dropping a cursor is not confirmed cancellation.

## Recovery and prepared execution

`Execution::confirm_termination` rechecks the original query ID, random marker and
principal with a fresh bounded deadline, without replaying the query or issuing
KILL. It returns `RemoteTermination::{Succeeded,Failed,Cancelled}` only after
terminal proof, leaving `Execution::outcome()` unchanged. This lets a host release
quarantined capacity when the server finished but data delivery was truncated or
discarded. It never makes those data publishable; unknown proof remains an error.

For a durable journal, `prepare_authorized_arrow` performs admission and identity
checks without creating a data request. Persist its `recovery_receipt()` fields
before consuming the prepared value with `start()`. Dropping a prepared value
never dispatches data; the original deadline includes the time spent persisting.
A restarted client can call `confirm_termination(&RecoveryReceipt)` to observe
the original query, marker and principal on the original endpoint. Restore only
trusted journal records; receipts are bounded provenance, not authorization or
proof that lost result data were delivered. Revalidate host authorization before
start and recovery. Recovery never replays the SELECT and never yields a verified
data result. A prepared-but-never-started attempt may have no remote record:
absence is Unknown, so hosts must not infer remote termination from it.

Hosts that need synchronous transport admission can consume a prepared SELECT
with `ready_transport().await`. It establishes a dedicated HTTP/1 connection,
using the same destination, bearer credentials and configured TLS connector,
without sending the SELECT. Retain `ReadyExecution::recovery_receipt()` and
revalidate host authority, then call `admit()`. This hands the request to the
ready transport before returning; response polling cannot send or replay it.
Expiry or unavailable transport refuses admission. A connection failure racing
admission remains uncertain. Local timeout checks do not implement an upstream
admission deadline, receiver fencing or a complete host authorization contract.
Poll the asynchronous methods in a Tokio runtime with I/O and timers enabled;
an absent runtime context produces a typed `TransportError::Runtime`.

## Receiver expiry profiles

For a host-reviewed JWT deployment, `Config::jwt_receiver(JwtReceiverProfile,
bearer)` freezes the exact bearer and a conservative receiver refusal bound.
The host must independently enforce signature validation, exact issuer/audience,
`username_claim=sub`, required expiry, `verifier_leeway=0`, no token cache, and
a bounded server-clock lag across every entry point. The SDK does not discover
or attest that configuration. Decoded claims constrain sending; they do not
verify signatures or grant authority. The server still authenticates every
request. The profile provides neither revocation nor distributed owner fencing.

`AntalyaClient::with_jwt_receiver(profile, bearer)` selects credentials for future
preparations without constructing a new capacity pool. Use it when the host can
select a short-lived grant only after host admission. It retains the endpoint,
configuration, HTTP pools, all admission lanes and retained-byte budget; existing
preparations keep their original credentials and deadlines. Changed endpoints
are refused. Uncertain executions still quarantine the same shared capacity.
The method performs no I/O, authorization, token acquisition or automatic refresh.
A separately authorized current view can observe an original receipt; apply its
proof locally to the original response without replaying the data query.

`ReadyExecution::admit_arrow_before(unix_seconds)` additionally requires that
bound (`exp + maximum_clock_lag_seconds + 1`) to be no later than the host's
admission deadline, and checks local nonexpiry and the original principal. The
extra second covers receiver timestamp precision; it never extends the host's
grant. JWT integer expiry is restricted to `1..=u32::MAX` (through 2106).
Tokens are at most 8 KiB, decoded headers 1 KiB, decoded payloads 4 KiB and each
top-level object at most 32 distinct members. Duplicate members, unsupported
critical extensions and ambiguous expiry types are refused with typed errors.
Parsing uses fixed decode buffers and bounded selected fields, with no per-batch
claim decoding; this is not a measured RSS improvement. The original unguarded
`admit`, `admit_ipc` and `admit_arrow` APIs retain their existing contracts.

The pinned live fixture exercises a valid signed credential, rejects a corrupted
signature despite valid metadata, and refuses a request sent after expiry on a
connection prepared beforehand. These checks qualify that fixture profile, not
a deployed swarm or an end-to-end authorization provider. Separate control credentials may
observe the original receipt after data-credential expiry without changing the
original completion deadline.

## Separately admitted responses

`AdmittedResponse::next_chunk()` only receives that response. It follows no
redirects, runs no terminal-control queries and preserves state across cancellation
of a borrowed poll. It accepts HTTP 200, rejects exception indications and bounds
cumulative wire bytes by `ExecutionLimits::max_total_bytes`. Chunks reserve at
most 64 KiB from shared retained-byte capacity before polling. Each payload is
split from a pending HTTP frame bounded to 256 KiB (separate from that shared
payload budget), allowing large responses without accepting an oversized frame.
Each returned payload is
detached once from Hyper's potentially larger allocation; clones then share the
payload and lease. Keep the wrapper while retaining its bytes. Connection buffers,
TLS, allocation metadata and subsequent decoding are separate from that payload
budget; these limits are not a measured RSS bound or a performance claim.
The raw path does not apply decoded row, batch-count or Arrow batch-size limits;
the decoder must enforce them before accepting results.

## IPC framing

For IPC framing, select `ReadyExecution::admit_ipc()` instead. It checks that
shared capacity can hold a maximum IPC frame and one wire chunk before sending
the query. `AdmittedResponse::into_ipc()` can also select framing before any raw
bytes have been consumed; refusal there drops an already admitted owner.
`AdmittedIpc::next_frame()` returns a leased `IpcFrame` with its original encoded
bytes, metadata and body. It retains partial framing state across cancelled
borrowed polls and applies backpressure while earlier frames remain owned.

Each frame has at most 64 KiB of metadata, `max_batch_bytes` of encoded body and
eight prefix bytes. The full frame reservation precedes buffering and shrinks to
the retained vector capacity when returned. FlatBuffers verification also bounds
depth (32), tables (4,096) and apparent size (256 KiB). The ready transport requests
the same uncompressed Arrow profile as the legacy cursor; framing refuses any
compressed batch before decompression. Negative/oversized lengths, invalid buffer
ranges, missing/repeated schemas, incomplete frames and trailing bytes after EOS
fail with typed errors. Metadata failures preserve the original FlatBuffers cause.

Frames are **not decoded batches**. `declared_rows()` checks untrusted declarations
against row/batch limits; it is not a verified row count. Schema semantics, array
validation, decoded allocation/dictionary bounds and completion verification remain
decoder/host responsibilities. The frame copy and existing detached wire copy are
explicit costs; no throughput or process-memory improvement is claimed. Reading
frames never issues terminal-control queries.

## Admitted Arrow decoding

`ReadyExecution::admit_arrow()` selects incremental decoded `BudgetedBatch` values
instead. It checks minimum frame/decoder capacity before dispatch. Its
`AdmittedArrow::next_batch()` performs no terminal-control I/O, including on errors
and EOF. Schema conversion first checks known types, enum values, child shapes,
endianness and bounded conversion work. Layout validation checks exact node,
buffer and variadic counts before calling the upstream Arrow decoder with value
validation enabled. Unknown schema features and unsupported IPC versions are
typed refusals. Dictionary replacements and deltas preserve earlier batches.

Input frames retain their leases through shared Arrow buffers using safe library
ownership. Separate reservations cover decoded values and possible dictionary
concatenation; alignment can still copy data. Schema work, each returned batch
and total retained dictionary memory are bounded by `max_batch_bytes`. Logical
array lengths and accumulated dictionary lengths are additionally bounded to
eight times that value, so a possible logical-null bitmap cannot exceed the byte
bound. Returned batches include a conservative schema memory charge. Cumulative
decoded bytes, rows and batches are checked independently of wire bytes. This
accounting can double-charge shared buffers; it is not a zero-copy, throughput
or process-RSS claim. Retaining batch clones keeps their reservations alive;
cancelled borrowed reads preserve pending frames and resume without replay.
`shared_schema()` clones only the schema Arc; an independent owner must charge
`schema_bytes()` while retaining it, including for empty results. That escape
does not retain the decoder's schema reservation.

Decoded EOF is still provisional (`Outcome::Running`). Call
`AdmittedArrow::confirm_completion()` explicitly to check the original private
query identity, principal and exact `result_rows` under the original operation
deadline. Missing whole batches, failed proof and expiry cannot become successful
empty results. A governed host must admit this control operation separately; the
method is not itself a host admission implementation. After decode refusal,
`confirm_termination()` remains available for recovery with the existing fresh
bounded observation deadline, but cannot validate discarded results.
For independently admitted controls with current credentials, call
`AntalyaClient::observe_terminal(&original_receipt)` on a separate client using
the same endpoint and principal. Its opaque `TerminalObservation` retains the
exact original receipt and query-log counters; it cannot be constructed or
deserialized by callers. `AdmittedArrow::accept_completion(&observation)` checks
that binding, clean decoded EOF, exact rows and the original operation deadline
locally, without sending another request. Foreign observations leave the original
response intact. `accept_termination` accepts only remote-stop evidence, including
after decode refusal; it never upgrades discarded output. Early stop evidence
keeps transport capacity reserved until EOF or discard. The observation's fresh
control deadline does not extend result validity, reset the original client pool
or authorize data replay. The host still owns admission of each control operation.
`cancel()` is another explicit control operation: it invalidates delivery before
awaiting cancellation of the original query. A dropped cancellation future keeps
the original slot quarantined. Remote success after discarded output requires
reconciliation and cannot restore a successful result.

Transport EOF does not validate Arrow, row counts or query success. Decode and
completion verification remain separate responsibilities. The explicit
`confirm_termination()` operation observes the original receipt. Releasing the
active slot requires both proven remote termination and response EOF or discard;
an early proof keeps capacity reserved while unread buffers remain. It sends control requests and
must not run inside a response-only wait. Dropping an admitted response without
that proof permanently quarantines its slot in the original client; retaining
the owner permits later observation. Recreating a client is not permission to
forget unresolved remote work or replay it. The existing Arrow execution APIs
retain their current decoding and completion behavior.

## Commands and durable side effects

For durable side effects, `prepare_authorized_command` accepts an approved request
with `Format::Json`, checks identity and reserves admission without dispatching.
For a response-only host wait, retain `PreparedCommand::recovery_receipt()`, call
`ready_transport().await` to establish the original connection, then synchronously
call `ReadyCommand::admit_before(unix_seconds)` under current host authority. This
uses the same reviewed receiver-expiry profile as guarded Arrow admission and
sends the command without a result FORMAT. `admit()` is the explicitly unguarded
variant. The returned `AdmittedResponse::next_chunk` only reads bounded response
bytes; it sends no controls. EOF alone does not prove command success. Separately
authorize `observe_terminal` through a current control client and locally call
`accept_termination` on the original response owner. Foreign observations fail
without releasing its slot. Unlike missing SELECT result bytes, a command's effect
can be proven after a lost reply by its exact original terminal receipt. The host still
owns retained intent, admission, acceptance and recovery; this primitive does not
implement a complete provider or receiver-owner fence.

The convenience command driver remains available:
Persist `PreparedCommand::recovery_receipt()` before `start()`, then poll the owned
`CommandExecution::run()` to send the command without a result FORMAT clause.
The original deadline includes journal latency. Calls to `run()` never redispatch:
after cancellation or an ambiguous response, retain the owner and call
`reconcile()` to observe the original ID, marker and principal with a fresh bounded
deadline. Proven success, failure or cancellation releases admission; Unknown does
not. Reports preserve typed failure categories and contain no raw SQL or credentials.

Dropping an unresolved dispatched `CommandExecution` permanently removes its slot
from that client's shared admission pool; it frees the owned SQL and buffers but
does not pretend the remote work ended. Prefer retaining and reconciling the owner.
A restart can observe its persisted receipt using `confirm_termination`, but the
host must recover its durable outstanding-work ledger before admitting new work.
Receipt observation does not restore a slot forgotten by a dropped owner. A new
client is not permission to replay a side effect. Missing query-log evidence stays
Unknown, even if a destination table exists. Verify source identity/content and
retention separately before publishing a frozen-source manifest.

The `execute_command_report` API has no pre-dispatch receipt or retained owner.
Command preparation bounds SQL and query-ID lengths and uses shared admission;
the application must bound parameter/setting counts and values, database names,
and generic bearer-token sizes. Request construction can copy and escape these
inputs. These limits do not bound process RSS. The ready-transport path owns one
connection task, aborted with its owner, just like prepared Arrow. Underlying request
serialization allocations remain governed by the existing client contract.

## Validated IPC exports

`client.export(query, Format::Arrow | Format::ArrowStream, path, options)` writes
to private temporary storage on the destination filesystem, enforces `max_bytes`,
validates each batch with the corresponding arrow-rs reader, verifies completion,
then publishes atomically. `Overwrite::Forbid` is the default; `Replace` replaces
only after all checks. Filesystem creation, writes, validation, synchronization,
publication and temporary cleanup run on bounded blocking workers. A two-message
queue carries copies of at most 64 KiB, charged before allocation; a slow worker
backpressures the producer. Validation reserves a maximum-batch allowance and
checks each decoded batch and cumulative totals.

Failure or cancellation before the atomic commit decision preserves the
destination. A commit that wins before cancellation may finish its rename after
the future is dropped. Running syscalls cannot be interrupted; detached cleanup
retains its export permit until it actually finishes. Deadlines are checked before
commit, but cannot undo an already started publication syscall. Publication gives
atomic visibility, not power-loss durability. Dropping an export future does not
prove server cancellation.

## Resource limits and diagnostics

All configuration is validated by `AntalyaClient::new`. Zero queued operations
is supported (fail immediately when active capacity is exhausted).

| Configuration | Defaults | Scope |
|---|---|---|
| `ResourceLimits` | 16 active data, 64 queued per lane, 4 control, 2 export tasks | Shared by clones; queue waits use the operation deadline |
| `max_retained_bytes` | 64 MiB | Accounted Arrow batches and queued export copies/validation reservation |
| `ExecutionLimits` | 1M rows, 65536 batches, 512 MiB cumulative, 16 MiB per batch | Each configured stream/export; export also enforces its wire-byte limit |
| `PoolOptions` | 2 idle connections/host, 2s idle lifetime | Each of the two transports; not an active-query limit |

Byte reservations are acquired in full. A waiting large request does not reserve
partial capacity or block a smaller request that can make progress. Byte admission
is not FIFO: sustained smaller requests can delay a larger one until its deadline.

Admitted IPC and Arrow readers distinguish internally retained frames, schemas,
and dictionaries from caller-retained output. Waiting for callers to drop output
applies backpressure. If a nested reservation cannot fit even after all external
output is released, the reader returns a typed `Cause::Capacity` instead of waiting
on other internal allocations. This can conservatively refuse a reader even when
a peer might later free memory. The refused reader discards its buffered data and
remains unconfirmed; it does not replay the query or issue implicit controls.
Dictionary replacement and deltas release internal ownership of old backing
buffers while earlier returned batches keep their own byte charges.

`BudgetedBatch` dereferences to `RecordBatch`. Keep the wrapper for as long as its
arrays are retained; wrapper clones share a lease. The reservation also survives
cancellation of a pending `next_batch()` future, like the upstream cursor's partial
IPC state. Dropping the execution releases admission and pending buffers, while
already returned wrappers retain their leases. EOF proof can be resumed after a
cancelled poll without mistaking decoder failure or discarded data for success.

Legacy `next()` and `BudgetedBatch::into_untracked()` explicitly transfer buffer
accounting responsibility to the caller; cloning raw arrays out of a wrapper also
bypasses its lifetime contract. The high-level cursor validates bounded IPC envelopes and layouts before Arrow
conversion, then checks decoded batch sizes before delivery. Accounting does not
bound every upstream allocation, transport buffer or memory retained through
escape hatches, and is not a process-RSS guarantee. The separately admitted
decoder also reserves capacity for schema conversion and dictionary growth.

`Execution::metrics()` reports admission wait, identity, upstream poll time, first
batch/EOF offsets, terminal/flush/observation times, poll/sleep counts, elapsed total
and typed failures. `CommandReport` and `ExportReport` provide their metrics too;
exports add write/validation/sync/publication clocks. The high-level Arrow path separately measures HTTP body/request wait and
synchronous Arrow decoding; other uninstrumented paths keep these fields `None`. Phases can overlap;
do not sum them into a fabricated total or equate first provisional batch with
confirmed result delivery. Metrics and errors contain no SQL, token or endpoint.

Use `Error::kind()` for category matching, `phase()`/`cause()` for diagnostics and
`std::error::Error::source()` for the sanitized typed source. Existing unit variants
remain available, but runtime errors may now be contextual. HTTP status, numeric
server code when available, I/O kind, malformed JSON, Arrow decoding, capacity,
timeout and invalid proof remain distinguishable without retaining raw server
messages. A transport/server response error alone does not prove remote completion.

## Raw transport escape hatches

`client.fetch_bytes` is explicitly raw/unvalidated and exposes the upstream
cursor. Callers must enforce their own total deadline, validate IPC, and verify
server completion. `Query::upstream`, `Query::fetch_bytes` and re-exported upstream
types are advanced escape hatches **outside** the configured bearer/transport
guarantees; upstream itself offers additional authentication choices.

## Host authorization

`AuthorizedRequest` freezes approved SQL, parameters, settings (including enforced
filters, snapshot and resource limits), target, format, expected principal and
receipt ID. It exposes no mutable query or target. The host owns authorization,
expiry/revocation checks and receipt correlation. Changing bound fields requires
a fresh host authorization decision and a new request. `fetch_authorized_arrow`
checks target/format and the server's `currentUser()` before consuming it. It
rejects a configured database that would change an unspecified frozen database.
This API prevents accidental mutation; it does not make arbitrary code in the same process trustworthy.

## SQL helpers

`sql::{Catalog, Hybrid, ExportPart}` provide narrow typed builders for REST
catalogs, Hybrid tables and Parquet part export.
Identifiers are quoted separately from values. Catalog credentials are sent in
the SQL body, never URL parameters; treat catalog DDL as sensitive.
`client.capabilities().await?` checks the exact qualified version and observed
engines/settings before these helpers build a query. Use `execute_command` for
these commands. Part export writes Parquet; Iceberg metadata updates and Puffin
operations belong to a separate `iceberg-rust` integration.

A `KILL QUERY ... SYNC FORMAT JSON` acknowledgement can be HTTP 200 with an empty
body when no active query matches, including a completion race. Cancellation
accepts that empty acknowledgement but still requires the original query-log
proof. Nonempty malformed JSON remains an error. A query that completed before
cancellation does not make discarded output publishable: cancellation stays
uncertain and the owner must separately reconcile the original receipt.

## Measured Arrow transport and framed completion

The high-level `Execution` path exposes body/request wait separately from Arrow
validation/decoding elapsed time. `transfer` includes server execution and scheduling while an
HTTP-body poll is pending; it is **not** pure network time. `wire_bytes` counts
HTTP entity bytes, excluding headers, TCP/TLS overhead and retransmissions.
`framing` records NDJSON/base64 processing separately. These clocks can overlap
consumer wall time; they are not an additive server/client latency decomposition.
The separate admitted APIs keep their existing explicit observation contract.

`TerminalStrategy::InBand` selects `JSONEachPacketBase64` around ArrowStream on
servers supporting the documented HTTP framing protocol. It requires the exact
response query ID, the base64 NDJSON content type, complete Arrow data, a final
counter-bearing progress packet with the decoded row count, and clean HTTP EOF.
Only `result_rows`, `result_bytes` or `memory_usage` identify terminal progress;
missing zero row counts are accepted only after such a final marker. A generic
progress packet or Arrow EOS is insufficient. Exceptions, duplicate/trailing
packets, truncation and ambiguous empty tails fail closed. No hidden query-log
round trip, replay or compatibility fallback occurs on this mode's completion.

Canonical data packets decode base64 directly from the bounded input slice,
avoiding an intermediate JSON string and object. Escaped, reordered or other
representations keep the existing duplicate-rejecting parser. A malformed
canonical-looking payload can incur one extra bounded decode attempt. All
line/wire/packet limits, terminal ordering, deadlines, IPC checks and final
proof remain active. This reduces parser allocations; it does not remove the
server's base64 encoding cost or imply a universal query-latency improvement.

The default terminal policy remains `FlushFirst`. Antalya 26.6.8.20004 lacks
`framing_output_format`; use a separately qualified compatible server before
selecting `InBand`. Command and admitted transports reject this mode before dispatch. Explicit cancellation/recovery observations are
separate operations and may still consult the query log. Background query logging
is not disabled or made free by transporting the success proof with the results.

The measured high-level decoder also validates bounded FlatBuffer metadata,
record/dictionary layouts and dictionary growth before Arrow conversion. It
requests uncompressed Arrow IPC and refuses compressed bodies before their
expansion; exported-file codecs keep their separate contract. These checks and
the configured accounting budgets are not a hard process-RSS guarantee.


See [testing](testing.md) for the pinned server fixtures and reproducible commands.
