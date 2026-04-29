use std::{fs::File, path::Path, sync::Arc};

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use aws_sdk_s3::{Client as S3Client, primitives::ByteStream};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use uuid::Uuid;

use crate::{
    config::{Config, TableMapping, ensure_parent},
    otlp::{LogRow, SpanRow},
    prometheus::PrometheusMetricRow,
    schema::{
        metric_rows_to_record_batch, rows_to_record_batch_for_profile, trace_rows_to_record_batch,
    },
    wal::{StagedDataFile, WalRecord},
};

struct S3Uploader {
    client: S3Client,
    bucket: String,
    key_prefix: String,
}

impl S3Uploader {
    async fn upload(&self, table_prefix: &str, local_path: &Path) -> Result<std::path::PathBuf> {
        let file_name = local_path
            .file_name()
            .context("parquet path has no file name")?
            .to_string_lossy();

        let key = if self.key_prefix.is_empty() {
            format!("{table_prefix}/{file_name}")
        } else {
            format!("{}/{table_prefix}/{file_name}", self.key_prefix)
        };

        let body = ByteStream::from_path(local_path)
            .await
            .context("reading parquet file for S3 upload")?;

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(body)
            .send()
            .await
            .with_context(|| format!("uploading {file_name} to s3://{}/{key}", self.bucket))?;

        Ok(std::path::PathBuf::from(format!(
            "s3://{}/{}",
            self.bucket, key
        )))
    }
}

impl std::fmt::Debug for S3Uploader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "S3Uploader(s3://{}/{})", self.bucket, self.key_prefix)
    }
}

fn parse_s3_base(base: &str) -> Result<(String, String)> {
    let path = base
        .strip_prefix("s3://")
        .or_else(|| base.strip_prefix("s3a://"))
        .context("OTLP_ADAPTER_PARQUET_S3_BASE must start with s3:// or s3a://")?;
    match path.split_once('/') {
        Some((bucket, prefix)) => Ok((bucket.to_owned(), prefix.trim_end_matches('/').to_owned())),
        None => Ok((path.to_owned(), String::new())),
    }
}

#[derive(Debug)]
pub struct ParquetWriter {
    config: Arc<Config>,
    properties: WriterProperties,
    s3: Option<S3Uploader>,
}

impl ParquetWriter {
    pub async fn new(config: Arc<Config>) -> Result<Self> {
        let s3 = match &config.parquet_s3_base {
            None => None,
            Some(base) => {
                let (bucket, key_prefix) = parse_s3_base(base)?;
                let aws_cfg =
                    aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
                let client = S3Client::new(&aws_cfg);
                Some(S3Uploader {
                    client,
                    bucket,
                    key_prefix,
                })
            }
        };

        Ok(Self {
            config,
            properties: WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build(),
            s3,
        })
    }

    pub async fn write_logs(
        &self,
        records: &[WalRecord],
        rows: &[LogRow],
    ) -> Result<StagedDataFile> {
        let batch = rows_to_record_batch_for_profile(
            rows,
            self.config.tables.logs.schema_profile,
            &self.config.tables.logs.projections,
        )
        .context("building Arrow log batch")?;
        self.write(
            &self.config.tables.logs.name,
            self.config.tables.logs.s3_prefix.as_deref(),
            records,
            rows.len(),
            batch,
        )
        .await
    }

    pub async fn write_metrics(
        &self,
        records: &[WalRecord],
        rows: &[PrometheusMetricRow],
    ) -> Result<StagedDataFile> {
        let batch = metric_rows_to_record_batch(rows).context("building Arrow metrics batch")?;
        self.write_table(&self.config.tables.metrics, records, rows.len(), batch)
            .await
    }

    pub async fn write_traces(
        &self,
        records: &[WalRecord],
        rows: &[SpanRow],
    ) -> Result<StagedDataFile> {
        let batch = trace_rows_to_record_batch(rows).context("building Arrow traces batch")?;
        self.write_table(&self.config.tables.traces, records, rows.len(), batch)
            .await
    }

    async fn write_table(
        &self,
        table: &TableMapping,
        records: &[WalRecord],
        row_count: usize,
        batch: RecordBatch,
    ) -> Result<StagedDataFile> {
        self.write(
            &table.name,
            table.s3_prefix.as_deref(),
            records,
            row_count,
            batch,
        )
        .await
    }

    async fn write(
        &self,
        table: &str,
        s3_prefix: Option<&str>,
        records: &[WalRecord],
        row_count: usize,
        batch: RecordBatch,
    ) -> Result<StagedDataFile> {
        let file_name = format!(
            "{}-{}-{}.parquet",
            self.config.adapter_instance_id,
            records.first().map(|r| r.id).unwrap_or_else(Uuid::nil),
            Uuid::new_v4()
        );
        let path = self.config.table_dir(table).join(&file_name);
        let source_wal_ids = records.iter().map(|r| r.id).collect::<Vec<_>>();
        let properties = self.properties.clone();

        let written_path = path.clone();
        let byte_size = tokio::task::spawn_blocking(move || {
            ensure_parent(&written_path)?;
            let file = File::create(&written_path)?;
            let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(properties))?;
            writer.write(&batch)?;
            writer.close()?;
            Ok::<u64, anyhow::Error>(std::fs::metadata(&written_path)?.len())
        })
        .await??;

        let final_path = match &self.s3 {
            Some(uploader) => uploader.upload(s3_prefix.unwrap_or(table), &path).await?,
            None => path,
        };

        Ok(StagedDataFile {
            path: final_path,
            source_wal_ids,
            row_count,
            byte_size,
        })
    }
}
