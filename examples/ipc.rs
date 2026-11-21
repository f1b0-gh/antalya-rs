use antalya_rs::{AntalyaClient, Config, Credentials, Endpoint, ExportOptions, Format, Query};
/// Validates IPC and query-log completion before atomically publishing a file.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("ANTALYA_PASSWORD").is_some() || std::env::var_os("ANTALYA_USER").is_some()
    {
        return Err("password/user authentication was removed; configure ANTALYA_TOKEN".into());
    }
    let format = match std::env::args().nth(1).as_deref() {
        None | Some("file") => Format::Arrow,
        Some("stream") => Format::ArrowStream,
        _ => return Err("usage: ipc [file|stream] [destination]".into()),
    };
    let destination = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "result.arrow".into());
    let client = AntalyaClient::new(Config::new(
        Endpoint::parse(
            &std::env::var("ANTALYA_URL").unwrap_or_else(|_| "http://127.0.0.1:8123".into()),
        )?,
        Credentials::bearer(&std::env::var("ANTALYA_TOKEN")?)?,
    ))?;
    let query = Query::new(
        "SELECT number FROM numbers(10)",
        format!("ipc-{}", std::process::id()),
    )?;
    let report = client
        .export(query, format, destination, ExportOptions::default())
        .await?;
    println!(
        "Published {} rows ({} bytes), query {}",
        report.rows, report.bytes, report.query_id
    );
    Ok(())
}
