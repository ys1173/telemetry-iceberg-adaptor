use std::sync::Arc;

use anyhow::Context;
use telemetry_iceberg_adapter::{
    commit::{
        CatalogCommitter, ConfiguredCommitter, GlueCatalogCommitter, NoopCatalogCommitter,
        RestCatalogCommitter,
    },
    config::{CatalogBackend, Config, WalBackend},
    grpc,
    http::router,
    parquet_writer::ParquetWriter,
    pipeline::{Pipeline, PipelineHandles},
    wal::{LocalWal, WalStore},
};
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_target(false).init();

    let config = Arc::new(Config::from_env()?);
    let wal = configured_wal(&config).await?;
    let writer = Arc::new(ParquetWriter::new(config.clone()).await?);
    let committer = Arc::new(configured_committer(&config).await?);
    committer
        .validate_startup(&config.tables)
        .await
        .context("validating Iceberg catalog tables")?;

    let PipelineHandles {
        batcher,
        committer: commit_worker,
    } = Pipeline::spawn(config.clone(), wal.clone(), writer, committer);

    let grpc_worker = tokio::spawn(grpc::serve(config.clone(), wal.clone()));

    let app = router(config.clone(), wal).layer(TraceLayer::new_for_http());

    let listener = TcpListener::bind(config.bind_addr)
        .await
        .with_context(|| format!("binding HTTP listener on {}", config.bind_addr))?;

    info!("listening on {}", config.bind_addr);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    batcher.abort();
    commit_worker.abort();
    grpc_worker.abort();
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

async fn configured_wal(config: &Config) -> anyhow::Result<Arc<dyn WalStore>> {
    match &config.wal_backend {
        WalBackend::Local => Ok(Arc::new(LocalWal::open(&config.wal_dir).await?)),
        WalBackend::Kafka(settings) => configured_kafka_wal(settings),
    }
}

async fn configured_committer(config: &Config) -> anyhow::Result<ConfiguredCommitter> {
    match &config.catalog {
        CatalogBackend::Noop => Ok(ConfiguredCommitter::Noop(NoopCatalogCommitter)),
        CatalogBackend::Glue(settings) => Ok(ConfiguredCommitter::Glue(
            GlueCatalogCommitter::load(settings.clone())
                .await
                .context("loading Glue Iceberg catalog")?,
        )),
        CatalogBackend::Rest(settings) => Ok(ConfiguredCommitter::Rest(
            RestCatalogCommitter::load(settings.clone())
                .await
                .context("loading Iceberg REST catalog")?,
        )),
        CatalogBackend::Hive(settings) => anyhow::bail!(
            "Hive catalog is configured for namespace `{}` but no Hive catalog implementation is wired yet",
            settings.namespace
        ),
    }
}

#[cfg(feature = "kafka")]
fn configured_kafka_wal(
    settings: &telemetry_iceberg_adapter::config::KafkaWalSettings,
) -> anyhow::Result<Arc<dyn WalStore>> {
    use telemetry_iceberg_adapter::wal::{KafkaWal, KafkaWalConfig};

    Ok(Arc::new(KafkaWal::open(KafkaWalConfig {
        bootstrap_servers: settings.bootstrap_servers.clone(),
        group_id: settings.group_id.clone(),
        records_topic: settings.records_topic.clone(),
        manifests_topic: settings.manifests_topic.clone(),
        produce_timeout: settings.produce_timeout,
    })?))
}

#[cfg(not(feature = "kafka"))]
fn configured_kafka_wal(
    _settings: &telemetry_iceberg_adapter::config::KafkaWalSettings,
) -> anyhow::Result<Arc<dyn WalStore>> {
    anyhow::bail!("Kafka WAL requested but binary was built without the `kafka` feature")
}
