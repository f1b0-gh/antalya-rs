use antalya_rs::sql::{
    Capabilities, Catalog, ExportPart, Feature, Hybrid, Identifier, PartitionBy, TableName,
};
use antalya_rs::{Credentials, Endpoint, Format};
fn caps() -> Capabilities {
    Capabilities::from_observation(
        antalya_rs::QUALIFICATION_VERSION,
        [Feature::Catalog, Feature::Hybrid, Feature::ExportPart],
    )
    .unwrap()
}
#[test]
fn builders_keep_values_out_of_sql_and_quote_identifiers() {
    let c = Catalog::new(
        Identifier::new("lake`name").unwrap(),
        Endpoint::parse("https://catalog.example").unwrap(),
        "s3://bucket",
        Credentials::bearer("catalog-secret").unwrap(),
    )
    .unwrap();
    let q = c.query(&caps(), "catalog-1").unwrap();
    let r = q
        .build_command_request(
            &reqwest::Client::new(),
            &Endpoint::parse("http://localhost").unwrap(),
            &Credentials::bearer("token").unwrap(),
        )
        .unwrap();
    let sql = std::str::from_utf8(r.body().unwrap().as_bytes().unwrap()).unwrap();
    assert!(sql.contains("`lake\\`name`"));
    assert!(!r.url().as_str().contains("catalog-secret"));
    assert!(!sql.contains("FORMAT"));
    assert!(sql.contains("warehouse = 's3://bucket'"));
    let hot = TableName::new("db", "hot").unwrap();
    let cold = TableName::new("lake", "db.cold").unwrap();
    let h = Hybrid::new(
        TableName::new("db", "hybrid").unwrap(),
        hot.clone(),
        "default",
        cold,
        Identifier::new("ts").unwrap(),
        "2025-01-01",
    )
    .unwrap();
    let r = h
        .query(&caps(), "hybrid-1")
        .unwrap()
        .build_request(
            &reqwest::Client::new(),
            &Endpoint::parse("http://localhost").unwrap(),
            &Credentials::bearer("token").unwrap(),
            Format::Json,
        )
        .unwrap();
    assert!(
        std::str::from_utf8(r.body().unwrap().as_bytes().unwrap())
            .unwrap()
            .contains("`lake`.`db.cold`")
    );
    assert!(
        ExportPart::new(
            hot,
            "part'1",
            "s3://bucket/data/{_file}.parquet",
            PartitionBy::YearMonth(Identifier::new("ts").unwrap())
        )
        .unwrap()
        .query(&caps(), "export-1")
        .is_ok()
    );
}
#[test]
fn unsupported_version_and_missing_capabilities_fail() {
    assert!(Capabilities::from_observation("26.3.1", [Feature::Hybrid]).is_err());
    assert!(
        Capabilities::from_observation(antalya_rs::QUALIFICATION_VERSION, [])
            .unwrap()
            .require(Feature::Hybrid)
            .is_err()
    );
}
