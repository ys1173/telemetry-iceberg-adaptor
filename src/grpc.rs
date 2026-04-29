use std::sync::Arc;

use opentelemetry_proto::tonic::collector::{
    logs::v1::{
        ExportLogsServiceRequest, ExportLogsServiceResponse,
        logs_service_server::{LogsService, LogsServiceServer},
    },
    trace::v1::{
        ExportTraceServiceRequest, ExportTraceServiceResponse,
        trace_service_server::{TraceService, TraceServiceServer},
    },
};
use tonic::{Request, Response, Status, transport::Server};
use tracing::{info, warn};

use crate::{config::Config, input::LogIngestor, wal::WalStore};

#[derive(Debug)]
pub struct OtlpLogsGrpcService {
    ingestor: LogIngestor,
}

#[derive(Debug)]
pub struct OtlpTracesGrpcService {
    ingestor: LogIngestor,
}

impl OtlpTracesGrpcService {
    pub fn new(config: Arc<Config>, wal: Arc<dyn WalStore>) -> Self {
        Self {
            ingestor: LogIngestor::new(config, wal),
        }
    }
}

impl OtlpLogsGrpcService {
    pub fn new(config: Arc<Config>, wal: Arc<dyn WalStore>) -> Self {
        Self {
            ingestor: LogIngestor::new(config, wal),
        }
    }
}

#[tonic::async_trait]
impl LogsService for OtlpLogsGrpcService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let record = self
            .ingestor
            .ingest_otlp_grpc(request.into_inner())
            .await
            .map_err(status_from_ingest_error)?;

        info!(record_id = %record.id, "accepted OTLP gRPC logs request into WAL");
        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}

#[tonic::async_trait]
impl TraceService for OtlpTracesGrpcService {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let record = self
            .ingestor
            .ingest_otlp_grpc_traces(request.into_inner())
            .await
            .map_err(status_from_ingest_error)?;

        info!(record_id = %record.id, "accepted OTLP gRPC traces request into WAL");
        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

pub async fn serve(config: Arc<Config>, wal: Arc<dyn WalStore>) -> anyhow::Result<()> {
    let addr = config.grpc_bind_addr;
    let logs_service = OtlpLogsGrpcService::new(config.clone(), wal.clone());
    let traces_service = OtlpTracesGrpcService::new(config, wal);

    info!("listening for OTLP gRPC on {}", addr);
    Server::builder()
        .add_service(LogsServiceServer::new(logs_service))
        .add_service(TraceServiceServer::new(traces_service))
        .serve(addr)
        .await?;
    Ok(())
}

fn status_from_ingest_error(error: anyhow::Error) -> Status {
    let message = error.to_string();
    warn!(%message, "rejecting OTLP gRPC logs request");

    if message.contains("backlog is full") {
        Status::resource_exhausted(message)
    } else if message.contains("exceeds configured limit") {
        Status::out_of_range(message)
    } else if message.contains("decoding logs") || message.contains("decoding traces") {
        Status::invalid_argument(message)
    } else {
        Status::internal(message)
    }
}
