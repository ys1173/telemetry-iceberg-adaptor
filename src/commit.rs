use std::{collections::HashMap, fmt, sync::Arc, time::Duration};

use anyhow::Result;
use arrow_schema::{DataType, Field, SchemaRef};
use async_trait::async_trait;
use iceberg::{
    Catalog, CatalogBuilder, TableIdent,
    spec::{DataContentType, DataFileBuilder, DataFileFormat, PrimitiveType, Struct, Type},
    transaction::{ApplyTransactionAction, Transaction},
};
use iceberg_catalog_glue::{GLUE_CATALOG_PROP_WAREHOUSE, GlueCatalog, GlueCatalogBuilder};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalog, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::{
    config::{Config, GlueCatalogSettings, RestCatalogSettings, TableMappings},
    schema::{log_schema_for_profile, metric_schema, trace_schema},
    wal::{CommitManifest, WalStore},
};

#[derive(Debug, thiserror::Error)]
pub enum CommitError {
    #[error("optimistic commit conflict")]
    Conflict,
    #[error("retryable catalog or storage error: {0}")]
    Retryable(String),
    #[error("permanent commit error: {0}")]
    Permanent(String),
}

#[async_trait]
pub trait CatalogCommitter: Send + Sync + fmt::Debug + 'static {
    async fn commit_append(
        &self,
        manifest: &CommitManifest,
    ) -> std::result::Result<(), CommitError>;

    async fn refresh_table(&self, table: &str) -> std::result::Result<(), CommitError>;

    async fn validate_startup(
        &self,
        tables: &TableMappings,
    ) -> std::result::Result<(), CommitError>;
}

#[derive(Debug, Default)]
pub struct NoopCatalogCommitter;

#[async_trait]
impl CatalogCommitter for NoopCatalogCommitter {
    async fn commit_append(
        &self,
        manifest: &CommitManifest,
    ) -> std::result::Result<(), CommitError> {
        info!(
            manifest_id = %manifest.id,
            table = %manifest.table,
            files = manifest.files.len(),
            "noop catalog append commit"
        );
        Ok(())
    }

    async fn refresh_table(&self, _table: &str) -> std::result::Result<(), CommitError> {
        Ok(())
    }

    async fn validate_startup(
        &self,
        _tables: &TableMappings,
    ) -> std::result::Result<(), CommitError> {
        info!("skipping Iceberg table validation for noop catalog");
        Ok(())
    }
}

#[derive(Debug)]
pub enum ConfiguredCommitter {
    Noop(NoopCatalogCommitter),
    Glue(GlueCatalogCommitter),
    Rest(RestCatalogCommitter),
}

#[async_trait]
impl CatalogCommitter for ConfiguredCommitter {
    async fn commit_append(
        &self,
        manifest: &CommitManifest,
    ) -> std::result::Result<(), CommitError> {
        match self {
            Self::Noop(committer) => committer.commit_append(manifest).await,
            Self::Glue(committer) => committer.commit_append(manifest).await,
            Self::Rest(committer) => committer.commit_append(manifest).await,
        }
    }

    async fn refresh_table(&self, table: &str) -> std::result::Result<(), CommitError> {
        match self {
            Self::Noop(committer) => committer.refresh_table(table).await,
            Self::Glue(committer) => committer.refresh_table(table).await,
            Self::Rest(committer) => committer.refresh_table(table).await,
        }
    }

    async fn validate_startup(
        &self,
        tables: &TableMappings,
    ) -> std::result::Result<(), CommitError> {
        match self {
            Self::Noop(committer) => committer.validate_startup(tables).await,
            Self::Glue(committer) => committer.validate_startup(tables).await,
            Self::Rest(committer) => committer.validate_startup(tables).await,
        }
    }
}

#[derive(Debug)]
pub struct GlueCatalogCommitter {
    catalog: GlueCatalog,
    pub warehouse: String,
    pub namespace: String,
}

impl GlueCatalogCommitter {
    pub async fn load(settings: GlueCatalogSettings) -> std::result::Result<Self, CommitError> {
        // Derive the S3 scheme from the warehouse URI so that the OpenDAL
        // storage operator accepts paths in exactly that scheme.  Athena
        // creates table metadata at s3:// paths; configuring "s3" here lets
        // the catalog work with both s3:// and s3a:// warehouses.
        let scheme = if settings.warehouse.starts_with("s3a://") {
            "s3a"
        } else {
            "s3"
        };

        let catalog = GlueCatalogBuilder::default()
            .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
                configured_scheme: scheme.to_string(),
                customized_credential_load: None,
            }))
            .load(
                "glue",
                HashMap::from([(
                    GLUE_CATALOG_PROP_WAREHOUSE.to_owned(),
                    settings.warehouse.clone(),
                )]),
            )
            .await
            .map_err(map_iceberg_error)?;

        Ok(Self {
            catalog,
            warehouse: settings.warehouse,
            namespace: settings.namespace,
        })
    }
}

#[async_trait]
impl CatalogCommitter for GlueCatalogCommitter {
    async fn commit_append(
        &self,
        manifest: &CommitManifest,
    ) -> std::result::Result<(), CommitError> {
        commit_append_to_catalog(&self.catalog, &self.namespace, manifest).await
    }

    async fn refresh_table(&self, table: &str) -> std::result::Result<(), CommitError> {
        refresh_table_from_catalog(&self.catalog, &self.namespace, table).await
    }

    async fn validate_startup(
        &self,
        tables: &TableMappings,
    ) -> std::result::Result<(), CommitError> {
        validate_catalog_tables(&self.catalog, &self.namespace, tables).await
    }
}

#[derive(Debug)]
pub struct RestCatalogCommitter {
    catalog: RestCatalog,
    pub uri: String,
    pub warehouse: Option<String>,
    pub namespace: String,
}

impl RestCatalogCommitter {
    pub async fn load(settings: RestCatalogSettings) -> std::result::Result<Self, CommitError> {
        let mut props = HashMap::from([(REST_CATALOG_PROP_URI.to_owned(), settings.uri.clone())]);
        if let Some(warehouse) = &settings.warehouse {
            props.insert(REST_CATALOG_PROP_WAREHOUSE.to_owned(), warehouse.clone());
        }
        insert_optional(&mut props, "credential", settings.credential.as_ref());
        insert_optional(&mut props, "token", settings.token.as_ref());
        insert_optional(
            &mut props,
            "oauth2-server-uri",
            settings.oauth2_server_uri.as_ref(),
        );
        insert_optional(&mut props, "prefix", settings.prefix.as_ref());
        insert_optional(&mut props, "scope", settings.scope.as_ref());

        let catalog = RestCatalogBuilder::default()
            .load("rest", props)
            .await
            .map_err(map_iceberg_error)?;

        Ok(Self {
            catalog,
            uri: settings.uri,
            warehouse: settings.warehouse,
            namespace: settings.namespace,
        })
    }
}

#[async_trait]
impl CatalogCommitter for RestCatalogCommitter {
    async fn commit_append(
        &self,
        manifest: &CommitManifest,
    ) -> std::result::Result<(), CommitError> {
        commit_append_to_catalog(&self.catalog, &self.namespace, manifest).await
    }

    async fn refresh_table(&self, table: &str) -> std::result::Result<(), CommitError> {
        refresh_table_from_catalog(&self.catalog, &self.namespace, table).await
    }

    async fn validate_startup(
        &self,
        tables: &TableMappings,
    ) -> std::result::Result<(), CommitError> {
        validate_catalog_tables(&self.catalog, &self.namespace, tables).await
    }
}

fn insert_optional(props: &mut HashMap<String, String>, key: &str, value: Option<&String>) {
    if let Some(value) = value {
        props.insert(key.to_owned(), value.clone());
    }
}

async fn commit_append_to_catalog(
    catalog: &dyn Catalog,
    namespace: &str,
    manifest: &CommitManifest,
) -> std::result::Result<(), CommitError> {
    let ident = table_ident(namespace, &manifest.table)?;
    let table = catalog
        .load_table(&ident)
        .await
        .map_err(map_iceberg_error)?;

    let data_files = manifest
        .files
        .iter()
        .map(|file| {
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(file.path.to_string_lossy().to_string())
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(file.byte_size)
                .record_count(file.row_count as u64)
                .partition_spec_id(table.metadata().default_partition_spec_id())
                .partition(Struct::empty())
                .build()
                .map_err(|error| CommitError::Permanent(error.to_string()))
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx).map_err(map_iceberg_error)?;
    tx.commit(catalog).await.map_err(map_iceberg_error)?;

    Ok(())
}

async fn refresh_table_from_catalog(
    catalog: &dyn Catalog,
    namespace: &str,
    table: &str,
) -> std::result::Result<(), CommitError> {
    let ident = table_ident(namespace, table)?;
    catalog
        .load_table(&ident)
        .await
        .map_err(map_iceberg_error)?;
    Ok(())
}

async fn validate_catalog_tables(
    catalog: &dyn Catalog,
    namespace: &str,
    tables: &TableMappings,
) -> std::result::Result<(), CommitError> {
    for expected in expected_table_schemas(tables) {
        let ident = table_ident(namespace, &expected.table)?;
        let table = catalog
            .load_table(&ident)
            .await
            .map_err(map_iceberg_error)?;
        let schema = table.metadata().current_schema();
        validate_table_schema(&expected, schema)?;
        info!(table = %expected.table, namespace, "validated Iceberg table schema");
    }

    Ok(())
}

#[derive(Debug)]
struct ExpectedTableSchema {
    table: String,
    fields: Vec<ExpectedField>,
}

#[derive(Debug)]
struct ExpectedField {
    name: String,
    data_type: ExpectedDataType,
    nullable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpectedDataType {
    Int,
    Long,
    Double,
    String,
    StringMap,
}

fn expected_table_schemas(tables: &TableMappings) -> Vec<ExpectedTableSchema> {
    vec![
        ExpectedTableSchema::from_arrow(
            tables.logs.name.clone(),
            log_schema_for_profile(tables.logs.schema_profile, &tables.logs.projections),
        ),
        ExpectedTableSchema::from_arrow(tables.metrics.name.clone(), metric_schema()),
        ExpectedTableSchema::from_arrow(tables.traces.name.clone(), trace_schema()),
    ]
}

impl ExpectedTableSchema {
    fn from_arrow(table: String, schema: SchemaRef) -> Self {
        Self {
            table,
            fields: schema
                .fields()
                .iter()
                .map(|field| ExpectedField::from_arrow(field))
                .collect(),
        }
    }
}

impl ExpectedField {
    fn from_arrow(field: &Field) -> Self {
        Self {
            name: field.name().clone(),
            data_type: ExpectedDataType::from_arrow(field.data_type()),
            nullable: field.is_nullable(),
        }
    }
}

impl ExpectedDataType {
    fn from_arrow(data_type: &DataType) -> Self {
        match data_type {
            DataType::Int32 => Self::Int,
            DataType::Int64 => Self::Long,
            DataType::Float64 => Self::Double,
            DataType::Utf8 => Self::String,
            DataType::Map(_, _) => Self::StringMap,
            other => {
                panic!("unsupported adapter Arrow schema type for Iceberg validation: {other}")
            }
        }
    }
}

fn validate_table_schema(
    expected: &ExpectedTableSchema,
    actual: &iceberg::spec::Schema,
) -> std::result::Result<(), CommitError> {
    let expected_names = expected
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<std::collections::HashSet<_>>();

    for expected_field in &expected.fields {
        let Some(actual_field) = actual.field_by_name(&expected_field.name) else {
            return Err(CommitError::Permanent(format!(
                "Iceberg table `{}` is missing required adapter column `{}`",
                expected.table, expected_field.name
            )));
        };

        if expected_field.nullable && actual_field.required {
            return Err(CommitError::Permanent(format!(
                "Iceberg table `{}` column `{}` is required but adapter may write nulls",
                expected.table, expected_field.name
            )));
        }

        if !iceberg_type_matches_expected(&actual_field.field_type, expected_field.data_type) {
            return Err(CommitError::Permanent(format!(
                "Iceberg table `{}` column `{}` has incompatible type `{}`; expected {}",
                expected.table,
                expected_field.name,
                actual_field.field_type,
                expected_field.data_type
            )));
        }
    }

    for actual_field in actual.as_struct().fields() {
        if actual_field.required && !expected_names.contains(actual_field.name.as_str()) {
            return Err(CommitError::Permanent(format!(
                "Iceberg table `{}` has required column `{}` that is not produced by the adapter",
                expected.table, actual_field.name
            )));
        }
    }

    Ok(())
}

fn iceberg_type_matches_expected(actual: &Type, expected: ExpectedDataType) -> bool {
    match (actual, expected) {
        (Type::Primitive(PrimitiveType::Int), ExpectedDataType::Int) => true,
        (Type::Primitive(PrimitiveType::Long), ExpectedDataType::Long) => true,
        (Type::Primitive(PrimitiveType::Double), ExpectedDataType::Double) => true,
        (Type::Primitive(PrimitiveType::String), ExpectedDataType::String) => true,
        (Type::Map(map), ExpectedDataType::StringMap) => {
            matches!(
                map.key_field.field_type.as_ref(),
                Type::Primitive(PrimitiveType::String)
            ) && matches!(
                map.value_field.field_type.as_ref(),
                Type::Primitive(PrimitiveType::String)
            )
        }
        _ => false,
    }
}

impl fmt::Display for ExpectedDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int => write!(f, "int"),
            Self::Long => write!(f, "long"),
            Self::Double => write!(f, "double"),
            Self::String => write!(f, "string"),
            Self::StringMap => write!(f, "map<string,string>"),
        }
    }
}

fn table_ident(namespace: &str, table: &str) -> std::result::Result<TableIdent, CommitError> {
    let mut parts = namespace.split('.').collect::<Vec<_>>();
    parts.push(table);
    TableIdent::from_strs(parts).map_err(map_iceberg_error)
}

fn map_iceberg_error(error: iceberg::Error) -> CommitError {
    if error.retryable() {
        CommitError::Conflict
    } else {
        CommitError::Permanent(error.to_string())
    }
}

#[derive(Debug)]
pub struct CommitCoordinator<C> {
    config: Arc<Config>,
    wal: Arc<dyn WalStore>,
    committer: Arc<C>,
}

impl<C> CommitCoordinator<C>
where
    C: CatalogCommitter,
{
    pub fn new(config: Arc<Config>, wal: Arc<dyn WalStore>, committer: Arc<C>) -> Self {
        Self {
            config,
            wal,
            committer,
        }
    }

    pub async fn run(self) {
        loop {
            match self.wal.pending_manifests(32).await {
                Ok(manifests) if manifests.is_empty() => sleep(Duration::from_secs(1)).await,
                Ok(manifests) => {
                    for manifest in manifests {
                        if let Err(error) = self.commit_with_retry(&manifest).await {
                            warn!(manifest_id = %manifest.id, error = %error, "commit remains pending");
                        }
                    }
                }
                Err(error) => {
                    warn!(error = %error, "failed to read commit manifests");
                    sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }

    async fn commit_with_retry(&self, manifest: &CommitManifest) -> Result<()> {
        let mut backoff = self.config.commit_retry_initial_backoff;

        for attempt in 1..=self.config.commit_retry_attempts {
            match self.committer.commit_append(manifest).await {
                Ok(()) => {
                    self.wal.mark_committed(manifest.id).await?;
                    info!(manifest_id = %manifest.id, attempt, "committed Iceberg manifest");
                    return Ok(());
                }
                Err(CommitError::Conflict) => {
                    warn!(manifest_id = %manifest.id, attempt, "Iceberg commit conflict; refreshing table");
                    self.committer
                        .refresh_table(&manifest.table)
                        .await
                        .map_err(anyhow::Error::new)?;
                }
                Err(CommitError::Retryable(error)) => {
                    warn!(manifest_id = %manifest.id, attempt, error, "retryable commit failure");
                }
                Err(CommitError::Permanent(error)) => {
                    anyhow::bail!("permanent commit failure for {}: {error}", manifest.id);
                }
            }

            sleep(with_jitter(backoff)).await;
            backoff = (backoff * 2).min(self.config.commit_retry_max_backoff);
        }

        anyhow::bail!(
            "commit retry attempts exhausted for manifest {}",
            manifest.id
        )
    }
}

fn with_jitter(base: Duration) -> Duration {
    let base_millis = base.as_millis().min(u64::MAX as u128) as u64;
    if base_millis == 0 {
        return base;
    }

    let jitter_millis = (uuid::Uuid::new_v4().as_u128() % (base_millis as u128 + 1)) as u64;
    base + Duration::from_millis(jitter_millis)
}

#[cfg(test)]
mod tests {
    use std::{
        net::SocketAddr,
        path::PathBuf,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::*;
    use crate::{
        config::{CatalogBackend, TableMappings, WalBackend},
        wal::{LocalWal, StagedDataFile},
    };
    use chrono::Utc;
    use uuid::Uuid;

    #[derive(Debug)]
    struct ConflictThenSuccess {
        commits: AtomicUsize,
        refreshes: AtomicUsize,
    }

    #[async_trait]
    impl CatalogCommitter for ConflictThenSuccess {
        async fn commit_append(
            &self,
            _manifest: &CommitManifest,
        ) -> std::result::Result<(), CommitError> {
            let attempt = self.commits.fetch_add(1, Ordering::SeqCst);
            if attempt < 2 {
                Err(CommitError::Conflict)
            } else {
                Ok(())
            }
        }

        async fn refresh_table(&self, _table: &str) -> std::result::Result<(), CommitError> {
            self.refreshes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn validate_startup(
            &self,
            _tables: &TableMappings,
        ) -> std::result::Result<(), CommitError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn coordinator_refreshes_and_retries_commit_conflicts() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(LocalWal::open(dir.path().join("wal")).await.unwrap());
        let config = Arc::new(Config {
            bind_addr: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
            grpc_bind_addr: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
            adapter_instance_id: "test".to_owned(),
            wal_dir: dir.path().join("wal"),
            parquet_dir: dir.path().join("parquet"),
            parquet_s3_base: None,
            max_http_body_bytes: 1024,
            max_pending_wal_bytes: 1024,
            batch_max_rows: 10,
            batch_max_bytes: 1024,
            batch_max_age: Duration::from_secs(1),
            log_worker_count: 1,
            commit_retry_attempts: 4,
            commit_retry_initial_backoff: Duration::from_millis(1),
            commit_retry_max_backoff: Duration::from_millis(2),
            catalog: CatalogBackend::Noop,
            wal_backend: WalBackend::Local,
            tables: TableMappings::default(),
        });
        let committer = Arc::new(ConflictThenSuccess {
            commits: AtomicUsize::new(0),
            refreshes: AtomicUsize::new(0),
        });
        let coordinator = CommitCoordinator::new(config, wal.clone(), committer.clone());
        let manifest = CommitManifest {
            id: Uuid::new_v4(),
            table: "logs".to_owned(),
            staged_at: Utc::now(),
            files: vec![StagedDataFile {
                path: PathBuf::from("s3://bucket/table/file.parquet"),
                source_wal_ids: vec![Uuid::new_v4()],
                row_count: 1,
                byte_size: 100,
            }],
        };

        wal.mark_parquet_written(&manifest).await.unwrap();
        coordinator.commit_with_retry(&manifest).await.unwrap();

        assert_eq!(committer.commits.load(Ordering::SeqCst), 3);
        assert_eq!(committer.refreshes.load(Ordering::SeqCst), 2);
        assert!(wal.pending_manifests(10).await.unwrap().is_empty());
    }
}
