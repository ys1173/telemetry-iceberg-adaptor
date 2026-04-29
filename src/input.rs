use std::sync::Arc;

use anyhow::{Context, Result, bail};
use prost::Message;

use crate::{
    config::Config,
    otlp::{LogRow, OtlpEncoding, decode_logs, decode_traces},
    prometheus::{PROMETHEUS_REMOTE_WRITE_CONTENT_TYPE, decode_remote_write},
    wal::{Signal, WalRecord, WalStore},
};

pub const OTLP_PROTOBUF_CONTENT_TYPE: &str = "application/x-protobuf";
pub const OTLP_JSON_CONTENT_TYPE: &str = "application/json";
pub const NDJSON_CONTENT_TYPE: &str = "application/x-ndjson";

#[derive(Debug, Clone)]
pub struct LogIngestor {
    config: Arc<Config>,
    wal: Arc<dyn WalStore>,
}

impl LogIngestor {
    pub fn new(config: Arc<Config>, wal: Arc<dyn WalStore>) -> Self {
        Self { config, wal }
    }

    pub async fn ingest(&self, request: LogInputRequest) -> Result<WalRecord> {
        let pending_bytes = self
            .wal
            .pending_received_bytes()
            .await
            .context("failed to inspect WAL backlog")?;
        if pending_bytes >= self.config.max_pending_wal_bytes {
            bail!("adapter WAL backlog is full; retry later");
        }

        if request.payload.len() > self.config.max_http_body_bytes {
            bail!("input payload exceeds configured limit");
        }

        let _rows = decode_logs(&request.payload, request.encoding)
            .with_context(|| format!("decoding logs from {}", request.source))?;

        self.wal
            .append(Signal::Logs, request.content_type, request.payload)
            .await
    }

    pub async fn ingest_otlp_grpc(
        &self,
        request: opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest,
    ) -> Result<WalRecord> {
        let mut payload = Vec::new();
        request
            .encode(&mut payload)
            .context("encoding OTLP gRPC request for WAL replay")?;
        self.ingest(LogInputRequest {
            source: "otlp-grpc",
            content_type: OTLP_PROTOBUF_CONTENT_TYPE.to_owned(),
            encoding: OtlpEncoding::Protobuf,
            payload,
        })
        .await
    }

    pub async fn ingest_http_payload(
        &self,
        source: &'static str,
        content_type: String,
        encoding: OtlpEncoding,
        payload: Vec<u8>,
    ) -> Result<WalRecord> {
        self.ingest(LogInputRequest {
            source,
            content_type,
            encoding,
            payload,
        })
        .await
    }

    pub async fn ingest_prometheus_remote_write(&self, payload: Vec<u8>) -> Result<WalRecord> {
        let pending_bytes = self
            .wal
            .pending_received_bytes()
            .await
            .context("failed to inspect WAL backlog")?;
        if pending_bytes >= self.config.max_pending_wal_bytes {
            bail!("adapter WAL backlog is full; retry later");
        }

        if payload.len() > self.config.max_http_body_bytes {
            bail!("input payload exceeds configured limit");
        }

        let _rows = decode_remote_write(&payload).context("decoding Prometheus remote write")?;

        self.wal
            .append(
                Signal::Metrics,
                PROMETHEUS_REMOTE_WRITE_CONTENT_TYPE.to_owned(),
                payload,
            )
            .await
    }

    pub async fn ingest_otlp_grpc_traces(
        &self,
        request: opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest,
    ) -> Result<WalRecord> {
        let mut payload = Vec::new();
        request
            .encode(&mut payload)
            .context("encoding OTLP gRPC traces request for WAL replay")?;
        self.ingest_traces(TraceInputRequest {
            source: "otlp-grpc",
            content_type: OTLP_PROTOBUF_CONTENT_TYPE.to_owned(),
            encoding: OtlpEncoding::Protobuf,
            payload,
        })
        .await
    }

    pub async fn ingest_http_traces(
        &self,
        source: &'static str,
        content_type: String,
        encoding: OtlpEncoding,
        payload: Vec<u8>,
    ) -> Result<WalRecord> {
        self.ingest_traces(TraceInputRequest {
            source,
            content_type,
            encoding,
            payload,
        })
        .await
    }

    async fn ingest_traces(&self, request: TraceInputRequest) -> Result<WalRecord> {
        let pending_bytes = self
            .wal
            .pending_received_bytes()
            .await
            .context("failed to inspect WAL backlog")?;
        if pending_bytes >= self.config.max_pending_wal_bytes {
            bail!("adapter WAL backlog is full; retry later");
        }

        if request.payload.len() > self.config.max_http_body_bytes {
            bail!("input payload exceeds configured limit");
        }

        let _rows = decode_traces(&request.payload, request.encoding)
            .with_context(|| format!("decoding traces from {}", request.source))?;

        self.wal
            .append(Signal::Traces, request.content_type, request.payload)
            .await
    }
}

pub struct LogInputRequest {
    pub source: &'static str,
    pub content_type: String,
    pub encoding: OtlpEncoding,
    pub payload: Vec<u8>,
}

pub struct TraceInputRequest {
    pub source: &'static str,
    pub content_type: String,
    pub encoding: OtlpEncoding,
    pub payload: Vec<u8>,
}

pub trait LogInput {
    fn name(&self) -> &'static str;
}

pub fn ensure_non_empty_rows(rows: &[LogRow]) -> Result<()> {
    if rows.is_empty() {
        bail!("log input contained no records");
    }
    Ok(())
}
