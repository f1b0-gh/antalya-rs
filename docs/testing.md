# Testing and server fixtures

[Getting started](../README.md) · [Client guide](client.md)

## Local checks

Run from the repository root with Rust and Cargo installed.
`rust-toolchain.toml` pins the tested toolchain to Rust 1.99.1. `Cargo.toml`
declares the same supported compiler floor; older compilers are not qualified:

```sh
cargo test --locked
cargo test --locked --all-features
cargo test --locked --no-default-features
cargo test --locked --no-default-features --features tls
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo fmt --check
cargo package --locked --all-features
```

The tests exercise request construction, bearer handling, typed parameters,
execution outcomes, cancellation, deadlines, resource accounting, IPC validation,
atomic exports, and prepared/admitted transports. Loopback HTTP fault tests
need permission to bind local sockets. Live server tests are marked `ignored`
and run separately; a successful default test run does not execute them.

The [GitHub Actions workflow](../.github/workflows/ci.yml) defines the four feature
combinations above, formatting, strict Clippy, rustdoc, package-content checks,
and a package build that compiles the archived source,
and separate query-log and framed server jobs. The query-log job also runs the
admitted transport, IPC, Arrow decoder and prepared-command live tests. Both
server jobs run the restrictive-user and Catalog deployment qualification.
CI runs on pushes to `main`, pull requests, and manual dispatch.
See the [Actions runs](https://github.com/f1b0-gh/antalya-rs/actions/workflows/ci.yml)
for executed results.

Package verification requires a clean working tree and registry access. It verifies
the archive's build, while the feature matrix and server jobs exercise behavior.
Before publishing a new version, also run
`cargo publish --dry-run --locked --all-features`. CI uses `cargo package` so it
can verify an already-published version without registry duplicate-version warnings.
The docs.rs build enables all features, including TLS.

## Arrow compatibility

The qualified Rust dependency set is `clickhouse =0.15.3` and Arrow `=59.3.3`.
All Arrow crates in the resolved graph must use the same release because the
client exchanges Rust Arrow types with consumers. CI checks manifest pins and
lockfile alignment, including transitive Arrow crates.

Dependabot groups ClickHouse and Arrow updates for joint qualification. Arrow
major updates are excluded from automatic proposals; moving beyond 59 requires
coordinated pin changes and revision of that update restriction. Patch and minor
updates still require the full CI matrix before merging. An advisory requiring a new major needs a manual
upgrade; the restriction is not a security-advisory suppression.

Rust crate alignment does not establish server compatibility. The two live
fixtures below qualify Arrow IPC and completion behavior against specific
ClickHouse/Antalya server builds. Re-run both when upgrading the dependency set;
support for other server builds needs separate qualification.

## Default server fixture

The fixture requires Linux with a local Docker engine, Python 3's standard
library, and Rust. Docker must be able to obtain the pinned image. It creates a
unique disposable container on an internal Docker bridge and a temporary JWT
signing configuration. Tests connect to that bridge address; the container has
no external catalog route and publishes no host ports.
It verifies the running server version and accepted/rejected bearer credentials
before invoking the test command.

```sh
python3 scripts/fixture.py cargo test --locked --test live_qualification -- --ignored --test-threads=1
```

The default target is `26.6.8.20004.altinityantalya`, pinned to
`altinity/clickhouse-server@sha256:aec9534624af9142abe87aab1b51ff5bcca9e74ca3b7e58fead2c1ec5e157cdf`.
The fixture passes connection settings and synthetic tokens through the child
environment. On exit it removes its container and signing configuration and
removes its internal network, and retains server logs in its owned `/tmp/antalya-rs/qualification-*` directory.
See [fixture.py](../scripts/fixture.py) for the configuration and cleanup logic.

The live suite covers Arrow file/stream schemas and values, typed parameters,
authorized principal and result limits, cancellation, export failure handling,
and capability checks. The original DDL syntax tests use `EXPLAIN AST`. The
separate deployment suite performs actual Catalog creation against a REST stub
on the container's loopback and checks both success and a malformed-config
exception. It verifies that the synthetic bearer reaches the stub and is absent
from HTTP responses, `SHOW CREATE`, `system.databases.engine_full`, and query-log
query/exception/settings fields. Stored server metadata remains sensitive; these
checks do not establish masking for every exception or log destination.

```sh
python3 scripts/fixture.py cargo test --locked --all-features --test deployment_qualification -- --ignored --test-threads=1
```

The deployment suite also verifies plain SELECT versus tracked refusal for
`readonly=1`, SELECT-only `readonly=2` with `ObserveOnly`, and supported helper
syntax. It uses the unmodified 7,500 ms query-log flush interval, and serializes
its flush-using tests so they cannot accelerate the ObserveOnly test. Tests assert
visibility within the operation deadline, without promising a fixed latency.
This one-server HTTP fixture does not establish HTTPS or distributed behavior.

## Framed completion fixture

The high-level Arrow `InBand` terminal strategy has a separate fixture:

```sh
python3 scripts/fixture.py --framed cargo test --locked --all-features --test in_band -- --ignored --nocapture
```

This pins `26.8.34.20001.altinityantalya` to
`altinityinfra/clickhouse-server@sha256:0989fbcd21552463cff9ef8d7a44bd2213a2c8e1879816f856a744a14b28e77a`.
It leaves the default fixture target unchanged. The native test covers empty
results, multiple batches, intermediate progress, late exceptions, and bearer
rejection. The deployment suite also runs against this build in CI. The default server lacks the required framing setting; the client
does not silently fall back to query-log completion in `InBand` mode.

The framing protocol is documented in the upstream [ClickHouse 26.8 release
presentation](https://presentations.clickhouse.com/2026-release-26.8/) and
[streaming HTTP API description](https://clickhouse.com/blog/clickhouse-streaming-http-api).
That documents the protocol, not the release/support status of the separately
pinned `altinityinfra` image. Qualify the actual server image used in deployment.

## Profiling

The profiling example compares `FlushFirst` and `ObserveFirst` on the same
result contract with terminal proof inside the measured execution:

```sh
python3 scripts/fixture.py cargo run --locked --release --example client_profile
```

Inspect the example's raw output and record the server build, client revision,
machine load, resource limits, and compilation/setup time separately. Phase
durations can overlap, and first provisional batch latency differs from
confirmed-result latency. Local component measurements do not establish service
latency or a performance advantage over another database.
