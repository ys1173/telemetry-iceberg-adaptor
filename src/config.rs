use std::{
    collections::HashSet,
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use anyhow::{Context, Result};
use serde::Deserialize;

const DEFAULT_CONFIG_PATH: &str = "telemetry-iceberg-adapter.toml";

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub grpc_bind_addr: SocketAddr,
    pub adapter_instance_id: String,
    pub wal_dir: PathBuf,
    pub parquet_dir: PathBuf,
    pub parquet_s3_base: Option<String>,
    pub max_http_body_bytes: usize,
    pub max_pending_wal_bytes: u64,
    pub batch_max_rows: usize,
    pub batch_max_bytes: usize,
    pub batch_max_age: Duration,
    pub log_worker_count: usize,
    pub commit_retry_attempts: usize,
    pub commit_retry_initial_backoff: Duration,
    pub commit_retry_max_backoff: Duration,
    pub catalog: CatalogBackend,
    pub wal_backend: WalBackend,
    pub tables: TableMappings,
}

#[derive(Debug, Clone)]
pub struct TableMappings {
    pub logs: LogTableMapping,
    pub metrics: TableMapping,
    pub traces: TableMapping,
}

impl Default for TableMappings {
    fn default() -> Self {
        Self {
            logs: LogTableMapping {
                name: "logs".to_owned(),
                s3_prefix: None,
                schema_profile: LogSchemaProfile::Otel,
                projections: vec![],
            },
            metrics: TableMapping {
                name: "metrics".to_owned(),
                s3_prefix: None,
            },
            traces: TableMapping {
                name: "traces".to_owned(),
                s3_prefix: None,
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableMapping {
    pub name: String,
    pub s3_prefix: Option<String>,
}

#[derive(Debug, Clone)]
pub struct LogTableMapping {
    pub name: String,
    pub s3_prefix: Option<String>,
    pub schema_profile: LogSchemaProfile,
    pub projections: Vec<LogProjectionColumn>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogSchemaProfile {
    Otel,
    GenericEvent,
}

impl Default for LogSchemaProfile {
    fn default() -> Self {
        Self::Otel
    }
}

impl FromStr for LogSchemaProfile {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "otel" | "otel_logs_v1" => Ok(Self::Otel),
            "generic_event" => Ok(Self::GenericEvent),
            other => anyhow::bail!("unsupported log schema profile `{other}`"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LogProjectionColumn {
    pub column: String,
    pub source: LogProjectionSource,
    pub data_type: ProjectionDataType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogProjectionSource {
    Builtin(LogBuiltinField),
    LogAttribute(String),
    ResourceAttribute(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogBuiltinField {
    ObservedTimestampNanos,
    TimestampNanos,
    SeverityNumber,
    SeverityText,
    Body,
    TraceId,
    SpanId,
    ServiceName,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProjectionDataType {
    String,
    Int,
    Long,
    Double,
}

impl Default for ProjectionDataType {
    fn default() -> Self {
        Self::String
    }
}

#[derive(Debug, Clone)]
pub enum CatalogBackend {
    Noop,
    Glue(GlueCatalogSettings),
    Rest(RestCatalogSettings),
    Hive(HiveCatalogSettings),
}

impl CatalogBackend {
    pub fn namespace(&self) -> Option<&str> {
        match self {
            Self::Noop => None,
            Self::Glue(settings) => Some(&settings.namespace),
            Self::Rest(settings) => Some(&settings.namespace),
            Self::Hive(settings) => Some(&settings.namespace),
        }
    }
}

#[derive(Debug, Clone)]
pub struct GlueCatalogSettings {
    pub warehouse: String,
    pub namespace: String,
}

#[derive(Debug, Clone)]
pub struct RestCatalogSettings {
    pub uri: String,
    pub warehouse: Option<String>,
    pub namespace: String,
    pub credential: Option<String>,
    pub token: Option<String>,
    pub oauth2_server_uri: Option<String>,
    pub prefix: Option<String>,
    pub scope: Option<String>,
}

#[derive(Debug, Clone)]
pub struct HiveCatalogSettings {
    pub uri: Option<String>,
    pub warehouse: Option<String>,
    pub namespace: String,
}

#[derive(Debug, Clone)]
pub enum WalBackend {
    Local,
    Kafka(KafkaWalSettings),
}

#[derive(Debug, Clone)]
pub struct KafkaWalSettings {
    pub bootstrap_servers: String,
    pub group_id: String,
    pub records_topic: String,
    pub manifests_topic: String,
    pub produce_timeout: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let file = FileConfig::load()?;
        Self::from_file_and_env(file)
    }

    fn from_file_and_env(file: FileConfig) -> Result<Self> {
        Ok(Self {
            bind_addr: value("OTLP_ADAPTER_BIND", file.server.http_bind, "0.0.0.0:4318")
                .parse()
                .context("parsing OTLP_ADAPTER_BIND")?,
            grpc_bind_addr: value(
                "OTLP_ADAPTER_GRPC_BIND",
                file.server.grpc_bind,
                "0.0.0.0:4317",
            )
            .parse()
            .context("parsing OTLP_ADAPTER_GRPC_BIND")?,
            adapter_instance_id: env::var("OTLP_ADAPTER_INSTANCE_ID")
                .ok()
                .or(file.server.instance_id)
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            wal_dir: PathBuf::from(value(
                "OTLP_ADAPTER_WAL_DIR",
                file.wal.dir.clone(),
                "./data/wal",
            )),
            parquet_dir: PathBuf::from(value(
                "OTLP_ADAPTER_PARQUET_DIR",
                file.storage.parquet_dir,
                "./data/parquet",
            )),
            parquet_s3_base: optional_value(
                "OTLP_ADAPTER_PARQUET_S3_BASE",
                file.storage.parquet_s3_base,
            ),
            max_http_body_bytes: parse_value(
                "OTLP_ADAPTER_MAX_HTTP_BODY_BYTES",
                file.limits.max_http_body_bytes,
                16 * 1024 * 1024,
            )?,
            max_pending_wal_bytes: parse_value(
                "OTLP_ADAPTER_MAX_PENDING_WAL_BYTES",
                file.limits.max_pending_wal_bytes,
                512 * 1024 * 1024,
            )?,
            batch_max_rows: parse_value(
                "OTLP_ADAPTER_BATCH_MAX_ROWS",
                file.batch.max_rows,
                100_000,
            )?,
            batch_max_bytes: parse_value(
                "OTLP_ADAPTER_BATCH_MAX_BYTES",
                file.batch.max_bytes,
                128 * 1024 * 1024,
            )?,
            batch_max_age: Duration::from_secs(parse_value(
                "OTLP_ADAPTER_BATCH_MAX_AGE_SECS",
                file.batch.max_age_secs,
                60,
            )?),
            log_worker_count: parse_value(
                "OTLP_ADAPTER_LOG_WORKERS",
                file.pipeline.log_workers,
                1,
            )?
            .max(1),
            commit_retry_attempts: parse_value(
                "OTLP_ADAPTER_COMMIT_RETRY_ATTEMPTS",
                file.commit.retry_attempts,
                8,
            )?,
            commit_retry_initial_backoff: Duration::from_millis(parse_value(
                "OTLP_ADAPTER_COMMIT_RETRY_INITIAL_BACKOFF_MS",
                file.commit.retry_initial_backoff_ms,
                200,
            )?),
            commit_retry_max_backoff: Duration::from_secs(parse_value(
                "OTLP_ADAPTER_COMMIT_RETRY_MAX_BACKOFF_SECS",
                file.commit.retry_max_backoff_secs,
                10,
            )?),
            catalog: catalog_from_sources(file.catalog, file.glue)?,
            wal_backend: wal_backend_from_sources(file.wal)?,
            tables: tables_from_sources(file.tables)?,
        })
    }

    pub fn table_dir(&self, table: &str) -> PathBuf {
        self.parquet_dir.join(table)
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct FileConfig {
    server: ServerFileConfig,
    storage: StorageFileConfig,
    limits: LimitsFileConfig,
    batch: BatchFileConfig,
    pipeline: PipelineFileConfig,
    commit: CommitFileConfig,
    catalog: CatalogFileConfig,
    glue: GlueFileConfig,
    wal: WalFileConfig,
    tables: TablesFileConfig,
}

impl FileConfig {
    fn load() -> Result<Self> {
        match env::var("OTLP_ADAPTER_CONFIG") {
            Ok(path) => Self::from_path(Path::new(&path)),
            Err(_) if Path::new(DEFAULT_CONFIG_PATH).exists() => {
                Self::from_path(Path::new(DEFAULT_CONFIG_PATH))
            }
            Err(_) => Ok(Self::default()),
        }
    }

    fn from_path(path: &Path) -> Result<Self> {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("reading config file {}", path.display()))?;
        toml::from_str(&contents).with_context(|| format!("parsing config file {}", path.display()))
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct ServerFileConfig {
    http_bind: Option<String>,
    grpc_bind: Option<String>,
    instance_id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct StorageFileConfig {
    parquet_dir: Option<String>,
    parquet_s3_base: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct LimitsFileConfig {
    max_http_body_bytes: Option<usize>,
    max_pending_wal_bytes: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct BatchFileConfig {
    max_rows: Option<usize>,
    max_bytes: Option<usize>,
    max_age_secs: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct PipelineFileConfig {
    log_workers: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct CommitFileConfig {
    retry_attempts: Option<usize>,
    retry_initial_backoff_ms: Option<u64>,
    retry_max_backoff_secs: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct GlueFileConfig {
    warehouse: Option<String>,
    namespace: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct CatalogFileConfig {
    #[serde(rename = "type")]
    catalog_type: Option<String>,
    warehouse: Option<String>,
    namespace: Option<String>,
    glue: GlueCatalogFileConfig,
    rest: RestCatalogFileConfig,
    hive: HiveCatalogFileConfig,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct GlueCatalogFileConfig {
    warehouse: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct RestCatalogFileConfig {
    uri: Option<String>,
    credential: Option<String>,
    token: Option<String>,
    oauth2_server_uri: Option<String>,
    prefix: Option<String>,
    scope: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct HiveCatalogFileConfig {
    uri: Option<String>,
    warehouse: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct WalFileConfig {
    backend: Option<String>,
    dir: Option<String>,
    kafka: KafkaWalFileConfig,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct KafkaWalFileConfig {
    bootstrap_servers: Option<String>,
    group_id: Option<String>,
    records_topic: Option<String>,
    manifests_topic: Option<String>,
    produce_timeout_secs: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct TablesFileConfig {
    logs: LogTableFileConfig,
    metrics: TableFileConfig,
    traces: TableFileConfig,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct TableFileConfig {
    name: Option<String>,
    s3_prefix: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct LogTableFileConfig {
    name: Option<String>,
    s3_prefix: Option<String>,
    schema_profile: Option<LogSchemaProfile>,
    projections: Vec<LogProjectionFileConfig>,
}

#[derive(Debug, Deserialize)]
struct LogProjectionFileConfig {
    column: String,
    source: String,
    #[serde(default)]
    data_type: ProjectionDataType,
}

pub fn ensure_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn value(key: &str, file_value: Option<String>, default: &str) -> String {
    env::var(key)
        .ok()
        .or(file_value)
        .unwrap_or_else(|| default.to_owned())
}

fn parse_value<T>(key: &str, file_value: Option<T>, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match env::var(key) {
        Ok(value) => value.parse().with_context(|| format!("parsing {key}")),
        Err(_) => Ok(file_value.unwrap_or(default)),
    }
}

fn optional_value(key: &str, file_value: Option<String>) -> Option<String> {
    env::var(key).ok().or(file_value)
}

fn tables_from_sources(file: TablesFileConfig) -> Result<TableMappings> {
    let logs = LogTableMapping {
        name: value("OTLP_ADAPTER_LOGS_TABLE", file.logs.name, "logs"),
        s3_prefix: optional_value("OTLP_ADAPTER_LOGS_S3_PREFIX", file.logs.s3_prefix),
        schema_profile: match env::var("OTLP_ADAPTER_LOGS_SCHEMA_PROFILE") {
            Ok(value) => value.parse()?,
            Err(_) => file.logs.schema_profile.unwrap_or_default(),
        },
        projections: file
            .logs
            .projections
            .into_iter()
            .map(LogProjectionColumn::try_from)
            .collect::<Result<Vec<_>>>()?,
    };
    validate_log_projection_columns(logs.schema_profile, &logs.projections)?;

    Ok(TableMappings {
        logs,
        metrics: TableMapping {
            name: value("OTLP_ADAPTER_METRICS_TABLE", file.metrics.name, "metrics"),
            s3_prefix: optional_value("OTLP_ADAPTER_METRICS_S3_PREFIX", file.metrics.s3_prefix),
        },
        traces: TableMapping {
            name: value("OTLP_ADAPTER_TRACES_TABLE", file.traces.name, "traces"),
            s3_prefix: optional_value("OTLP_ADAPTER_TRACES_S3_PREFIX", file.traces.s3_prefix),
        },
    })
}

impl TryFrom<LogProjectionFileConfig> for LogProjectionColumn {
    type Error = anyhow::Error;

    fn try_from(value: LogProjectionFileConfig) -> Result<Self> {
        Ok(Self {
            column: value.column,
            source: parse_log_projection_source(&value.source)?,
            data_type: value.data_type,
        })
    }
}

fn parse_log_projection_source(source: &str) -> Result<LogProjectionSource> {
    let source = source.trim();
    if let Some(key) = source.strip_prefix("$.") {
        return Ok(LogProjectionSource::LogAttribute(key.to_owned()));
    }
    if let Some(key) = source.strip_prefix("log_attributes.") {
        return Ok(LogProjectionSource::LogAttribute(key.to_owned()));
    }
    if let Some(key) = source.strip_prefix("attributes.") {
        return Ok(LogProjectionSource::LogAttribute(key.to_owned()));
    }
    if let Some(key) = source.strip_prefix("resource_attributes.") {
        return Ok(LogProjectionSource::ResourceAttribute(key.to_owned()));
    }
    if let Some(key) = source.strip_prefix("resource.") {
        return Ok(LogProjectionSource::ResourceAttribute(key.to_owned()));
    }

    let builtin = match source {
        "observed_timestamp_nanos" => LogBuiltinField::ObservedTimestampNanos,
        "timestamp_nanos" => LogBuiltinField::TimestampNanos,
        "severity_number" => LogBuiltinField::SeverityNumber,
        "severity_text" => LogBuiltinField::SeverityText,
        "body" => LogBuiltinField::Body,
        "trace_id" => LogBuiltinField::TraceId,
        "span_id" => LogBuiltinField::SpanId,
        "service_name" => LogBuiltinField::ServiceName,
        other => anyhow::bail!("unsupported log projection source `{other}`"),
    };
    Ok(LogProjectionSource::Builtin(builtin))
}

fn validate_log_projection_columns(
    profile: LogSchemaProfile,
    projections: &[LogProjectionColumn],
) -> Result<()> {
    let builtin_columns = match profile {
        LogSchemaProfile::Otel => &[
            "observed_timestamp_nanos",
            "timestamp_nanos",
            "severity_number",
            "severity_text",
            "body",
            "trace_id",
            "span_id",
            "service_name",
            "resource_attributes",
            "log_attributes",
        ][..],
        LogSchemaProfile::GenericEvent => &[
            "timestamp_nanos",
            "observed_timestamp_nanos",
            "message",
            "severity",
            "hostname",
            "attributes",
        ][..],
    };
    let mut columns = HashSet::new();
    for projection in projections {
        if builtin_columns.contains(&projection.column.as_str()) {
            anyhow::bail!(
                "log projection column `{}` conflicts with a built-in log schema column",
                projection.column
            );
        }
        if !columns.insert(projection.column.as_str()) {
            anyhow::bail!("duplicate log projection column `{}`", projection.column);
        }
    }
    Ok(())
}

fn catalog_from_sources(
    file: CatalogFileConfig,
    legacy_glue: GlueFileConfig,
) -> Result<CatalogBackend> {
    let legacy_glue_warehouse = env::var("OTLP_ADAPTER_GLUE_WAREHOUSE")
        .ok()
        .or(legacy_glue.warehouse);
    let catalog_type = env::var("OTLP_ADAPTER_CATALOG_TYPE")
        .ok()
        .or(file.catalog_type)
        .unwrap_or_else(|| {
            if legacy_glue_warehouse.is_some() {
                "glue".to_owned()
            } else {
                "noop".to_owned()
            }
        });

    let namespace = value(
        "OTLP_ADAPTER_CATALOG_NAMESPACE",
        file.namespace.or(legacy_glue.namespace),
        "default",
    );
    let warehouse = optional_value(
        "OTLP_ADAPTER_CATALOG_WAREHOUSE",
        file.warehouse
            .or(file.glue.warehouse)
            .or(legacy_glue_warehouse),
    );

    match catalog_type.as_str() {
        "noop" => Ok(CatalogBackend::Noop),
        "glue" => Ok(CatalogBackend::Glue(GlueCatalogSettings {
            warehouse: warehouse.context("catalog.warehouse is required for Glue catalog")?,
            namespace,
        })),
        "rest" => Ok(CatalogBackend::Rest(RestCatalogSettings {
            uri: optional_value("OTLP_ADAPTER_REST_CATALOG_URI", file.rest.uri)
                .context("catalog.rest.uri is required for REST catalog")?,
            warehouse,
            namespace,
            credential: optional_value(
                "OTLP_ADAPTER_REST_CATALOG_CREDENTIAL",
                file.rest.credential,
            ),
            token: optional_value("OTLP_ADAPTER_REST_CATALOG_TOKEN", file.rest.token),
            oauth2_server_uri: optional_value(
                "OTLP_ADAPTER_REST_CATALOG_OAUTH2_SERVER_URI",
                file.rest.oauth2_server_uri,
            ),
            prefix: optional_value("OTLP_ADAPTER_REST_CATALOG_PREFIX", file.rest.prefix),
            scope: optional_value("OTLP_ADAPTER_REST_CATALOG_SCOPE", file.rest.scope),
        })),
        "hive" => Ok(CatalogBackend::Hive(HiveCatalogSettings {
            uri: optional_value("OTLP_ADAPTER_HIVE_CATALOG_URI", file.hive.uri),
            warehouse: optional_value(
                "OTLP_ADAPTER_HIVE_CATALOG_WAREHOUSE",
                file.hive.warehouse.or(warehouse),
            ),
            namespace,
        })),
        other => anyhow::bail!("unsupported OTLP_ADAPTER_CATALOG_TYPE: {other}"),
    }
}

fn wal_backend_from_sources(file: WalFileConfig) -> Result<WalBackend> {
    match value("OTLP_ADAPTER_WAL_BACKEND", file.backend, "local").as_str() {
        "local" => Ok(WalBackend::Local),
        "kafka" => Ok(WalBackend::Kafka(KafkaWalSettings {
            bootstrap_servers: env::var("OTLP_ADAPTER_KAFKA_BOOTSTRAP_SERVERS")
                .ok()
                .or(file.kafka.bootstrap_servers)
                .context("OTLP_ADAPTER_KAFKA_BOOTSTRAP_SERVERS is required for Kafka WAL")?,
            group_id: value(
                "OTLP_ADAPTER_KAFKA_GROUP_ID",
                file.kafka.group_id,
                "telemetry-iceberg-adapter",
            ),
            records_topic: value(
                "OTLP_ADAPTER_KAFKA_RECORDS_TOPIC",
                file.kafka.records_topic,
                "telemetry-iceberg-wal-records",
            ),
            manifests_topic: value(
                "OTLP_ADAPTER_KAFKA_MANIFESTS_TOPIC",
                file.kafka.manifests_topic,
                "telemetry-iceberg-wal-manifests",
            ),
            produce_timeout: Duration::from_secs(parse_value(
                "OTLP_ADAPTER_KAFKA_PRODUCE_TIMEOUT_SECS",
                file.kafka.produce_timeout_secs,
                30,
            )?),
        })),
        other => anyhow::bail!("unsupported OTLP_ADAPTER_WAL_BACKEND: {other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_toml_config_file_shape() {
        let config: FileConfig = toml::from_str(
            r#"
[server]
http_bind = "127.0.0.1:4318"
grpc_bind = "127.0.0.1:4317"
instance_id = "local-dev"

[wal]
backend = "local"
dir = "./tmp/wal"

[storage]
parquet_dir = "./tmp/parquet"

[limits]
max_http_body_bytes = 1024
max_pending_wal_bytes = 2048

[batch]
max_rows = 10
max_bytes = 4096
max_age_secs = 5

[pipeline]
log_workers = 4

[commit]
retry_attempts = 3
retry_initial_backoff_ms = 50
retry_max_backoff_secs = 2

[catalog]
type = "glue"
warehouse = "s3://warehouse"
namespace = "observability"

[tables.logs]
name = "logs"
schema_profile = "generic_event"

[[tables.logs.projections]]
column = "src_ip"
source = "$.src_ip"
data_type = "string"
"#,
        )
        .unwrap();

        assert_eq!(config.server.instance_id.as_deref(), Some("local-dev"));
        assert_eq!(config.wal.backend.as_deref(), Some("local"));
        assert_eq!(config.batch.max_rows, Some(10));
        assert_eq!(config.pipeline.log_workers, Some(4));
        assert_eq!(config.catalog.namespace.as_deref(), Some("observability"));
        assert_eq!(config.tables.logs.name.as_deref(), Some("logs"));
        assert_eq!(
            config.tables.logs.schema_profile,
            Some(LogSchemaProfile::GenericEvent)
        );
        assert_eq!(config.tables.logs.projections.len(), 1);
    }
}
