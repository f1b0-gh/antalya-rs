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

#[test]
fn supported_release_families_accept_compatible_patches_with_observed_features() {
    for version in [
        "26.6.8.20004.altinityantalya",
        "26.6.8.20005.altinityantalya",
        "26.6.9.1.altinityantalya",
        "26.8.34.20001.altinityantalya",
        "26.8.34.20002.altinityantalya",
        "26.8.35.1.altinityantalya",
    ] {
        let capabilities = Capabilities::from_observation(version, [Feature::Hybrid])
            .unwrap_or_else(|error| panic!("{version}: {error}"));
        assert_eq!(capabilities.version(), version);
        assert!(capabilities.require(Feature::Hybrid).is_ok(), "{version}");
        assert!(capabilities.require(Feature::Catalog).is_err(), "{version}");
        assert!(
            capabilities.require(Feature::ExportPart).is_err(),
            "{version}"
        );
    }
}

#[test]
fn capability_observations_reject_unqualified_families_and_malformed_versions() {
    for version in [
        "",
        "26.6.8.20004",
        "26.6.8.20004.altinitystable",
        "26.6.8.20004.altinityantalya.extra",
        "26.6.8.altinityantalya",
        "26.6.8.20004.1.altinityantalya",
        "26.6.8.20003.altinityantalya",
        "26.6.7.99999.altinityantalya",
        "26.8.34.20000.altinityantalya",
        "26.8.33.99999.altinityantalya",
        "26.7.99.99999.altinityantalya",
        "26.9.99.99999.altinityantalya",
        "27.6.8.20004.altinityantalya",
        "026.6.8.20004.altinityantalya",
        "26.06.8.20004.altinityantalya",
        "26.6.08.20004.altinityantalya",
        "26.6.8.020004.altinityantalya",
        "26.6.+8.20004.altinityantalya",
        "26.6.-8.20004.altinityantalya",
        "26.6..20004.altinityantalya",
        "26.6.8.4294967296.altinityantalya",
        "26.6.4294967296.20004.altinityantalya",
        "26.6.８.20004.altinityantalya",
        " 26.6.8.20004.altinityantalya",
        "26.6.8.20004.altinityantalya\n",
    ] {
        assert!(
            matches!(
                Capabilities::from_observation(
                    version,
                    [Feature::Catalog, Feature::Hybrid, Feature::ExportPart]
                ),
                Err(antalya_rs::Error::Unsupported)
            ),
            "{version:?}"
        );
    }
}

#[test]
fn compatible_release_does_not_infer_features_from_version() {
    for version in [
        "26.6.8.20005.altinityantalya",
        "26.8.34.20001.altinityantalya",
    ] {
        let capabilities = Capabilities::from_observation(version, []).unwrap();
        for feature in [Feature::Catalog, Feature::Hybrid, Feature::ExportPart] {
            assert!(capabilities.require(feature).is_err(), "{version}");
        }
    }
}
