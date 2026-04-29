use std::{io::Read, sync::Arc};

use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};
use flate2::read::GzDecoder;
use tracing::{info, warn};

use crate::{config::Config, input::LogIngestor, otlp::encoding_from_content_type, wal::WalStore};

#[derive(Clone)]
pub struct AppState {
    config: Arc<Config>,
    ingestor: LogIngestor,
}

pub fn router(config: Arc<Config>, wal: Arc<dyn WalStore>) -> Router {
    let ingestor = LogIngestor::new(config.clone(), wal);
    Router::new()
        .route("/v1/logs", post(post_logs))
        .route("/v1/logs/ndjson", post(post_ndjson_logs))
        .route("/api/v1/write", post(post_prometheus_remote_write))
        .route("/v1/metrics", post(not_implemented))
        .route("/v1/traces", post(post_traces))
        .with_state(AppState { config, ingestor })
}

async fn post_traces(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    match handle_traces(state, headers, body).await {
        Ok(record_id) => {
            info!(%record_id, "accepted OTLP HTTP traces request into WAL");
            StatusCode::OK.into_response()
        }
        Err(HttpError { status, message }) => {
            warn!(%status, %message, "rejecting OTLP HTTP traces request");
            (status, message).into_response()
        }
    }
}

async fn post_logs(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    match handle_logs(state, headers, body, None, "otlp-http").await {
        Ok(record_id) => {
            info!(%record_id, "accepted HTTP logs request into WAL");
            StatusCode::OK.into_response()
        }
        Err(HttpError { status, message }) => {
            warn!(%status, %message, "rejecting HTTP logs request");
            (status, message).into_response()
        }
    }
}

async fn post_ndjson_logs(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    match handle_logs(
        state,
        headers,
        body,
        Some("application/x-ndjson".to_owned()),
        "http-ndjson",
    )
    .await
    {
        Ok(record_id) => {
            info!(%record_id, "accepted HTTP NDJSON logs request into WAL");
            StatusCode::OK.into_response()
        }
        Err(HttpError { status, message }) => {
            warn!(%status, %message, "rejecting HTTP NDJSON logs request");
            (status, message).into_response()
        }
    }
}

async fn post_prometheus_remote_write(State(state): State<AppState>, body: Bytes) -> Response {
    match state
        .ingestor
        .ingest_prometheus_remote_write(body.to_vec())
        .await
    {
        Ok(record) => {
            info!(record_id = %record.id, "accepted Prometheus remote write request into WAL");
            StatusCode::OK.into_response()
        }
        Err(error) => {
            let HttpError { status, message } = HttpError::from_ingest_error(error);
            warn!(%status, %message, "rejecting Prometheus remote write request");
            (status, message).into_response()
        }
    }
}

async fn not_implemented() -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        "only /v1/logs is implemented in this phase",
    )
        .into_response()
}

async fn handle_logs(
    state: AppState,
    headers: HeaderMap,
    body: Bytes,
    forced_content_type: Option<String>,
    source: &'static str,
) -> Result<String, HttpError> {
    if body.len() > state.config.max_http_body_bytes {
        return Err(HttpError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "OTLP request body exceeds configured limit",
        ));
    }

    let content_type = match forced_content_type {
        Some(content_type) => content_type,
        None => headers
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| {
                HttpError::new(StatusCode::UNSUPPORTED_MEDIA_TYPE, "missing Content-Type")
            })?
            .to_owned(),
    };

    let payload = decode_content_encoding(&headers, body)?;
    let encoding = encoding_from_content_type(&content_type)
        .map_err(|error| HttpError::new(StatusCode::UNSUPPORTED_MEDIA_TYPE, error.to_string()))?;

    let record = state
        .ingestor
        .ingest_http_payload(source, content_type, encoding, payload)
        .await
        .map_err(HttpError::from_ingest_error)?;

    Ok(record.id.to_string())
}

async fn handle_traces(
    state: AppState,
    headers: HeaderMap,
    body: Bytes,
) -> Result<String, HttpError> {
    if body.len() > state.config.max_http_body_bytes {
        return Err(HttpError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "OTLP traces request body exceeds configured limit",
        ));
    }

    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| HttpError::new(StatusCode::UNSUPPORTED_MEDIA_TYPE, "missing Content-Type"))?
        .to_owned();
    let payload = decode_content_encoding(&headers, body)?;
    let encoding = encoding_from_content_type(&content_type)
        .map_err(|error| HttpError::new(StatusCode::UNSUPPORTED_MEDIA_TYPE, error.to_string()))?;

    let record = state
        .ingestor
        .ingest_http_traces("otlp-http", content_type, encoding, payload)
        .await
        .map_err(HttpError::from_ingest_error)?;

    Ok(record.id.to_string())
}

fn decode_content_encoding(headers: &HeaderMap, body: Bytes) -> Result<Vec<u8>, HttpError> {
    match headers
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_ascii_lowercase())
        .as_deref()
    {
        None | Some("identity") => Ok(body.to_vec()),
        Some("gzip") => {
            let mut decoder = GzDecoder::new(body.as_ref());
            let mut decoded = Vec::new();
            decoder
                .read_to_end(&mut decoded)
                .map_err(|error| HttpError::new(StatusCode::BAD_REQUEST, error.to_string()))?;
            Ok(decoded)
        }
        Some(encoding) => Err(HttpError::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            format!("unsupported Content-Encoding: {encoding}"),
        )),
    }
}

struct HttpError {
    status: StatusCode,
    message: String,
}

impl HttpError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    fn internal(error: anyhow::Error, context: &str) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{context}: {error}"),
        )
    }

    fn from_ingest_error(error: anyhow::Error) -> Self {
        let message = error.to_string();
        if message.contains("backlog is full") {
            Self::new(StatusCode::TOO_MANY_REQUESTS, message)
        } else if message.contains("exceeds configured limit") {
            Self::new(StatusCode::PAYLOAD_TOO_LARGE, message)
        } else if message.contains("decoding logs") || message.contains("decoding traces") {
            Self::new(StatusCode::BAD_REQUEST, message)
        } else {
            Self::internal(error, "failed to ingest logs")
        }
    }
}
