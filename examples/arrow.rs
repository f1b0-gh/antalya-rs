use antalya_rs::{AntalyaClient, Config, Credentials, Endpoint, Outcome, Query};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("ANTALYA_PASSWORD").is_some() || std::env::var_os("ANTALYA_USER").is_some()
    {
        return Err("password/user authentication was removed; configure ANTALYA_TOKEN".into());
    }
    let client = AntalyaClient::new(Config::new(
        Endpoint::parse(
            &std::env::var("ANTALYA_URL").unwrap_or_else(|_| "http://127.0.0.1:8123".into()),
        )?,
        Credentials::bearer(&std::env::var("ANTALYA_TOKEN")?)?,
    ))?;
    let query = Query::new(
        "SELECT number FROM numbers(10)",
        format!("arrow-{}", std::process::id()),
    )?;
    let mut execution = client.fetch_arrow(query).await?;
    while let Some(batch) = execution.next_batch().await? {
        println!("{} provisional rows", batch.num_rows());
    }
    println!("Query {}: {:?}", execution.query_id(), execution.outcome());
    if execution.outcome() != Outcome::Succeeded {
        return Err("query completion is unconfirmed".into());
    }
    Ok(())
}
