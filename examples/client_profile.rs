//! Small pinned-fixture comparison of terminal policies, including actual client proof.
//! Run: scripts/fixture.py cargo run --release --example client_profile
use antalya_rs::{
    AntalyaClient, Config, Credentials, Endpoint, Error, Outcome, Query, TerminalPolicy,
    TerminalStrategy,
};
use arrow_array::{Array, UInt64Array};
use std::time::{Duration, Instant};

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = Endpoint::parse(&std::env::var("ANTALYA_TEST_URL")?)?;
    let credentials = Credentials::bearer(&std::env::var("ANTALYA_TEST_TOKEN")?)?;
    let base = Config::new(endpoint, credentials).timeout(Duration::from_secs(10));
    let clients = [
        AntalyaClient::new(base.clone())?,
        AntalyaClient::new(base.terminal_policy(TerminalPolicy {
            strategy: TerminalStrategy::ObserveFirst,
            ..TerminalPolicy::default()
        }))?,
    ];
    for client in &clients {
        client.capabilities().await?;
    }
    let started = Instant::now();
    let mut samples = Vec::new();
    for round in 0..26 {
        for offset in 0..2 {
            let index = (round + offset) % 2;
            let id = uuid::Uuid::new_v4().to_string();
            let query = Query::new("SELECT count() AS value FROM numbers(32768)", &id)?
                .setting("max_threads", "1")?
                .setting("use_query_cache", "0")?
                .setting("max_execution_time", "5")?
                .setting("max_memory_usage", "200000000")?;
            let begin = Instant::now();
            let mut execution = clients[index].fetch_arrow(query).await?;
            let mut count = 0;
            while let Some(batch) = execution.next_batch().await? {
                if batch.num_rows() != 1 || batch.num_columns() != 1 {
                    return Err(Error::InvalidIpc.into());
                }
                let array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or(Error::InvalidIpc)?;
                if array.is_null(0) || array.value(0) != 32768 {
                    return Err(Error::InvalidIpc.into());
                }
                count += 1;
            }
            if count != 1 || execution.outcome() != Outcome::Succeeded {
                return Err(Error::Unconfirmed.into());
            }
            let m = execution.metrics();
            let millis = |d: Duration| d.as_secs_f64() * 1000.0;
            samples.push(serde_json::json!({"round":round,"warmup":round<2,"order":offset,
                "policy":if index==0 {"flush_first"} else {"observe_first"},"query_id":id,
                "wall_ms":millis(begin.elapsed()),"queue_ms":millis(m.queued),"data_poll_ms":millis(m.data_poll),
                "first_batch_ms":m.first_batch.map(millis),"terminal_ms":millis(m.terminal),
                "flush_ms":millis(m.flush_time),"observe_ms":millis(m.observe_time),
                "flush_attempts":m.flush_attempts,"flush_failures":m.flush_failures,"polls":m.polls,
                "control_requests":m.control_requests,"poll_sleeps":m.poll_sleeps,"rows":execution.rows_delivered()}));
        }
    }
    let report = serde_json::json!({"qualified_version":antalya_rs::QUALIFICATION_VERSION,
        "scope":"warm shared-host exact count, includes configured client terminal proof; no LO/FDW authorization hop or Elastic reference",
        "samples":samples,"total_seconds":started.elapsed().as_secs_f64()});
    use std::io::Write;
    writeln!(
        std::io::stdout().lock(),
        "{}",
        serde_json::to_string(&report)?
    )?;
    Ok(())
}
#[tokio::main]
async fn main() {
    if run().await.is_err() {
        use std::io::Write;
        let _ = writeln!(
            std::io::stderr().lock(),
            "client profile failed; inspect fixture and typed test diagnostics"
        );
        std::process::exit(1);
    }
}
