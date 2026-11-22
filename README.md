# antalya-rs

[![CI](https://github.com/f1b0-gh/antalya-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/f1b0-gh/antalya-rs/actions/workflows/ci.yml)

Bearer-authenticated HTTP and Apache Arrow integration for Altinity Antalya.
The crate combines the official ClickHouse client and Arrow readers with explicit query completion, resource limits, and validated
IPC exports. Authorization and token acquisition belong to your application.

## Getting started

Add the GitHub source to your application (no crates.io release is required):

```toml
[dependencies]
antalya-rs = { git = "https://github.com/f1b0-gh/antalya-rs", features = ["tls"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Cargo records the resolved Git revision in your lockfile. Use a `rev` requirement
when you need to select a particular reviewed commit.

The default `arrow` feature enables streaming queries, execution tracking, and
IPC exports. Enable `tls` for HTTPS with system trust. Use
`default-features = false` for the lightweight HTTP request builder; your own
HTTP client then owns transport policy and completion checks.

```rust,no_run
use antalya_rs::{AntalyaClient, Config, Credentials, Endpoint, Outcome, Query};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = AntalyaClient::new(Config::new(
        Endpoint::parse("https://antalya.example:8443")?,
        Credentials::bearer(&std::env::var("ANTALYA_TOKEN")?)?,
    ))?;

    let query = Query::new("SELECT number FROM numbers(10)", "example-1")?;
    let mut execution = client.fetch_arrow(query).await?;
    while let Some(batch) = execution.next_batch().await? {
        println!("{} provisional rows", batch.num_rows());
    }
    assert_eq!(execution.outcome(), Outcome::Succeeded);
    Ok(())
}
```

Run async operations in a Tokio runtime with I/O and timers enabled. Reuse the
client: clones share connection pools, admission limits, and retained-byte
budgets. Configured transports disable redirects, proxy discovery, and automatic
request retries. `client.with_bearer(new_token)?` selects a new bearer for future
operations while preserving those shared pools and limits; existing executions
keep their original credentials. Receiver-aware JWT clients use
`with_jwt_receiver` to preserve their reviewed credential constraints.

## Completion and server requirements

Batches are provisional until completion is confirmed. `next_batch()` checks
completion before returning `None` and reports late failures. HTTP 200, transport
EOF, and a valid Arrow stream alone do not prove query success. Dropping an
execution does not confirm remote cancellation.

The default completion strategy needs permitted query-local logging settings,
`SELECT` access to `system.query_log`, and routing of data and verification
requests to the same server. It attempts `SYSTEM FLUSH LOGS query_log`, then polls
for an execution-specific terminal record. Explicit cancellation needs
`KILL QUERY` permission and terminal confirmation.

| Deployment | Completion choice |
|---|---|
| Permitted logging settings and query-log SELECT access | `FlushFirst` (default); scoped flush reduces visibility delay when permitted |
| Read-only queries with setting changes allowed (`readonly=2`) | `ObserveOnly`; allow for the server's log publication interval (7.5 seconds in the pinned fixtures) |
| Framing-capable server and permitted request settings | `InBand` for high-level Arrow; completion arrives with the response, without a query-log lookup |
| Settings locked by `readonly=1` | Tracked execution is unsupported; the client returns the refusal promptly |

Query-log modes require backend affinity behind a load balancer. `InBand` avoids
that requirement for successful high-level Arrow completion; explicit recovery
and cancellation still use query-log observation.

The default fixture targets `26.6.8.20004.altinityantalya`. The optional `InBand`
strategy uses framed HTTP completion on a separate compatible server fixture;
see the [client guide](https://github.com/f1b0-gh/antalya-rs/blob/main/docs/client.md#measured-arrow-transport-and-framed-completion).
Arrow dependencies are pinned to **59.3.3**. Consumers exchanging `RecordBatch`
values must use compatible Arrow crate versions. Project Dynamic/JSON values
explicitly in SQL when an Arrow representation is needed.

## API guide

Start with `AntalyaClient::fetch_arrow` and `Execution::next_batch`, or `export`
for validated Arrow files. Prepared/admitted execution and recovery receipts are
for applications that need durable intent, separate authorization of dispatch,
or reconciliation after a lost response.


- [Configuration and queries](https://github.com/f1b0-gh/antalya-rs/blob/main/docs/client.md#configuration-and-credentials):
  bearer credentials, database selection, typed parameters, and transport policy.
- [Results and completion](https://github.com/f1b0-gh/antalya-rs/blob/main/docs/client.md#results-and-completion):
  provisional batches, terminal strategies, progress, and cancellation.
- [Recovery and prepared execution](https://github.com/f1b0-gh/antalya-rs/blob/main/docs/client.md#recovery-and-prepared-execution):
  receipts, explicit admission, and observation without replay.
- [Validated IPC exports](https://github.com/f1b0-gh/antalya-rs/blob/main/docs/client.md#validated-ipc-exports):
  Arrow file/stream validation and atomic publication.
- [Resource limits and diagnostics](https://github.com/f1b0-gh/antalya-rs/blob/main/docs/client.md#resource-limits-and-diagnostics):
  bounded admission, retained-byte accounting, metrics, and typed errors.
- [Host authorization](https://github.com/f1b0-gh/antalya-rs/blob/main/docs/client.md#host-authorization) and
  [SQL helpers](https://github.com/f1b0-gh/antalya-rs/blob/main/docs/client.md#sql-helpers): frozen requests and typed DDL builders.

Keep `BudgetedBatch` wrappers while retaining their arrays. When retained output
prevents a full reservation fitting, `capacity_wait_timeout` limits the wait to
one second by default. Waiting on other queries' pending reads uses the operation
deadline. Consume and drop batches to keep streaming, or configure enough shared
capacity for a collection plus the next batch. See the guide for accounting scope
and explicit raw-data escape hatches.

## Examples and development

Set `ANTALYA_URL` and `ANTALYA_TOKEN` for your server, then run:

```sh
cargo run --locked --features tls --example arrow
cargo run --locked --features tls --example ipc -- file result.arrow
```

The examples default to `http://127.0.0.1:8123` if `ANTALYA_URL` is unset.
See [testing](https://github.com/f1b0-gh/antalya-rs/blob/main/docs/testing.md) for the feature matrix, Docker fixtures, and
profiling commands. Generate the local API reference with
`cargo doc --locked --all-features --no-deps`.

Licensed under [Apache-2.0](https://github.com/f1b0-gh/antalya-rs/blob/main/LICENSE).
