//! Small builders for the documented Antalya 26.6 syntax.
//! <https://docs.altinity.com/altinityantalya/antalya-quick-start-guide/working-with-hybrid-tables/>
use crate::{Credentials, Endpoint, Error, QUALIFICATION_VERSION, Query};
use std::collections::BTreeSet;
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Feature {
    Catalog,
    Hybrid,
    ExportPart,
}
/// A snapshot of observed server metadata; not an authorization grant.
#[derive(Clone, Debug)]
pub struct Capabilities {
    version: String,
    features: BTreeSet<Feature>,
}
impl Capabilities {
    /// For applications that obtain metadata themselves. Supplied observations
    /// must come from the intended server. Prefer `AntalyaClient::capabilities`.
    pub fn from_observation(
        version: &str,
        features: impl IntoIterator<Item = Feature>,
    ) -> Result<Self, Error> {
        if version != QUALIFICATION_VERSION {
            return Err(Error::Unsupported);
        }
        Ok(Self {
            version: version.into(),
            features: features.into_iter().collect(),
        })
    }
    pub fn version(&self) -> &str {
        &self.version
    }
    pub fn require(&self, feature: Feature) -> Result<(), Error> {
        if self.features.contains(&feature) {
            Ok(())
        } else {
            Err(Error::Unsupported)
        }
    }
}
#[derive(Clone)]
pub struct Identifier(String);
impl Identifier {
    pub fn new(value: &str) -> Result<Self, Error> {
        if value.is_empty() || value.chars().any(char::is_control) {
            return Err(Error::InvalidQuery);
        }
        Ok(Self(value.into()))
    }
    fn sql(&self) -> String {
        format!("`{}`", self.0.replace('\\', "\\\\").replace('`', "\\`"))
    }
}
#[derive(Clone)]
pub struct TableName {
    database: Identifier,
    table: Identifier,
}
impl TableName {
    pub fn new(database: &str, table: &str) -> Result<Self, Error> {
        Ok(Self {
            database: Identifier::new(database)?,
            table: Identifier::new(table)?,
        })
    }
    fn sql(&self) -> String {
        format!("{}.{}", self.database.sql(), self.table.sql())
    }
}
fn literal(value: &str) -> String {
    format!("'{}'", crate::escape_parameter(value))
}
/// REST DataLakeCatalog setup. The catalog token is sent in the SQL body, never
/// a URL parameter. Treat this DDL as sensitive application input.
pub struct Catalog {
    name: Identifier,
    endpoint: Endpoint,
    warehouse: String,
    credentials: Credentials,
}
impl Catalog {
    pub fn new(
        name: Identifier,
        endpoint: Endpoint,
        warehouse: &str,
        credentials: Credentials,
    ) -> Result<Self, Error> {
        if warehouse.is_empty() {
            return Err(Error::InvalidSetting);
        }
        Ok(Self {
            name,
            endpoint,
            warehouse: warehouse.into(),
            credentials,
        })
    }
    pub fn query(&self, capabilities: &Capabilities, id: &str) -> Result<Query, Error> {
        capabilities.require(Feature::Catalog)?;
        let authorization = self.credentials.0["Authorization"]
            .to_str()
            .map_err(|_| Error::InvalidCredentials)?;
        Query::new(format!("CREATE DATABASE {} ENGINE = DataLakeCatalog({{catalog_url:String}}) SETTINGS catalog_type = 'rest', auth_header = {}, warehouse = {}",self.name.sql(),literal(&format!("Authorization: {authorization}")),literal(&self.warehouse)),id)?.parameter("catalog_url",self.endpoint.0.as_str())
    }
}
/// A single ordered watermark partitions hot and cold data without overlap.
pub struct Hybrid {
    target: TableName,
    hot: TableName,
    cluster: String,
    cold: TableName,
    column: Identifier,
    watermark: String,
}
impl Hybrid {
    pub fn new(
        target: TableName,
        hot: TableName,
        cluster: &str,
        cold: TableName,
        column: Identifier,
        watermark: &str,
    ) -> Result<Self, Error> {
        if cluster.is_empty() || watermark.is_empty() {
            return Err(Error::InvalidQuery);
        }
        Ok(Self {
            target,
            hot,
            cluster: cluster.into(),
            cold,
            column,
            watermark: watermark.into(),
        })
    }
    pub fn query(&self, capabilities: &Capabilities, id: &str) -> Result<Query, Error> {
        capabilities.require(Feature::Hybrid)?;
        Query::new(format!("CREATE TABLE {} AS {} ENGINE = Hybrid(cluster({{cluster:String}}, {}), {} >= {{watermark:String}}, {}, {} < {{watermark:String}}) SETTINGS allow_experimental_hybrid_table = 1",self.target.sql(),self.hot.sql(),self.hot.sql(),self.column.sql(),self.cold.sql(),self.column.sql()),id)?.parameter("cluster",&self.cluster)?.parameter("watermark",&self.watermark)
    }
}
pub enum PartitionBy {
    Column(Identifier),
    YearMonth(Identifier),
}
impl PartitionBy {
    fn sql(&self) -> String {
        match self {
            Self::Column(c) => c.sql(),
            Self::YearMonth(c) => format!("toYYYYMM({})", c.sql()),
        }
    }
}
/// Exports a MergeTree part to Parquet; does not update Iceberg metadata or drop data.
pub struct ExportPart {
    source: TableName,
    part: String,
    destination: String,
    partition: PartitionBy,
}
impl ExportPart {
    pub fn new(
        source: TableName,
        part: &str,
        destination: &str,
        partition: PartitionBy,
    ) -> Result<Self, Error> {
        let url = reqwest::Url::parse(destination).map_err(|_| Error::InvalidEndpoint)?;
        if part.is_empty()
            || !matches!(url.scheme(), "s3" | "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err(Error::InvalidQuery);
        }
        Ok(Self {
            source,
            part: part.into(),
            destination: destination.into(),
            partition,
        })
    }
    pub fn query(&self, capabilities: &Capabilities, id: &str) -> Result<Query, Error> {
        capabilities.require(Feature::ExportPart)?;
        Query::new(format!("ALTER TABLE {} EXPORT PART {{part:String}} TO TABLE FUNCTION s3({{destination:String}}, format='Parquet') PARTITION BY {} SETTINGS allow_experimental_export_merge_tree_part = 1",self.source.sql(),self.partition.sql()),id)?.parameter("part",&self.part)?.parameter("destination",&self.destination)
    }
}
#[cfg(feature = "arrow")]
impl crate::AntalyaClient {
    pub async fn capabilities(&self) -> Result<Capabilities, Error> {
        let query = Query::new(
            "SELECT version() AS version, (SELECT count() FROM system.database_engines WHERE name='DataLakeCatalog') AS catalog, (SELECT count() FROM system.table_engines WHERE name='Hybrid') AS hybrid, (SELECT count() FROM system.settings WHERE name='allow_experimental_export_merge_tree_part') AS export_part",
            format!("antalya-capabilities-{}", uuid::Uuid::new_v4()),
        )?;
        let value = self.json(&query).await?;
        let rows = value["data"]
            .as_array()
            .filter(|rows| rows.len() == 1)
            .ok_or(Error::Unsupported)?;
        let row = &rows[0];
        let mut features = Vec::new();
        for (key, feature) in [
            ("catalog", Feature::Catalog),
            ("hybrid", Feature::Hybrid),
            ("export_part", Feature::ExportPart),
        ] {
            if row[key].as_u64() == Some(1) || row[key].as_str() == Some("1") {
                features.push(feature);
            }
        }
        Capabilities::from_observation(row["version"].as_str().ok_or(Error::Unsupported)?, features)
    }
}
