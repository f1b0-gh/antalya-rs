# Testing and server fixtures

[Getting started](../README.md) · [Client guide](client.md)

## Local checks

Run from the repository root with Rust and Cargo installed.
`rust-toolchain.toml` pins the tested toolchain to Rust 1.99.1:

```sh
cargo test --locked
cargo test --locked --all-features
cargo test --locked --no-default-features
cargo test --locked --no-default-features --features tls
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo fmt --check
```

The tests exercise request construction, bearer handling, typed parameters,
execution outcomes, cancellation, deadlines, resource accounting, IPC validation,
atomic exports, and prepared/admitted transports. Loopback HTTP fault tests
need permission to bind local sockets. Live server tests are marked `ignored`
and run separately; a successful default test run does not execute them.

The [GitHub Actions workflow](../.github/workflows/ci.yml) defines the four feature
combinations above, formatting, strict Clippy, rustdoc and package-content checks,
and separate query-log and framed server jobs. The query-log job also runs the
admitted transport, IPC, Arrow decoder and prepared-command live tests.
CI runs on pushes to `main`, pull requests, and manual dispatch.
See the [Actions runs](https://github.com/f1b0-gh/antalya-rs/actions/workflows/ci.yml)
for executed results.

## Default server fixture

The fixture requires Docker, Python 3's standard library, and Rust. Docker must
be able to obtain the pinned image. It creates a unique disposable container,
an ephemeral loopback HTTP port, and a temporary JWT signing configuration.
It verifies the running server version and accepted/rejected bearer credentials
before invoking the test command.

```sh
python3 scripts/fixture.py cargo test --locked --test live_qualification -- --ignored --test-threads=1
```

The default target is `26.6.8.20004.altinityantalya`, pinned to
`altinity/clickhouse-server@sha256:aec9534624af9142abe87aab1b51ff5bcca9e74ca3b7e58fead2c1ec5e157cdf`.
The fixture passes connection settings and synthetic tokens through the child
environment. On exit it removes its container and signing configuration and
retains server logs in its owned `/tmp/antalya-rs/qualification-*` directory.
See [fixture.py](../scripts/fixture.py) for the configuration and cleanup logic.

The live suite covers Arrow file/stream schemas and values, typed parameters,
authorized principal and result limits, cancellation, export failure handling,
and capability checks. DDL tests use `EXPLAIN AST`: parser acceptance does not
establish successful cloud catalog or storage side effects. This fixture uses
loopback HTTP and one server; it does not establish HTTPS or distributed behavior.

## Framed completion fixture

The high-level Arrow `InBand` terminal strategy has a separate fixture:

```sh
python3 scripts/fixture.py --framed cargo test --locked --all-features --test in_band -- --ignored --nocapture
```

This pins `26.8.34.20001.altinityantalya` to
`altinityinfra/clickhouse-server@sha256:0989fbcd21552463cff9ef8d7a44bd2213a2c8e1879816f856a744a14b28e77a`.
It leaves the default fixture target unchanged. The native test covers empty
results, multiple batches, intermediate progress, late exceptions, and bearer
rejection. The default server lacks the required framing setting; the client
does not silently fall back to query-log completion in `InBand` mode.

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
