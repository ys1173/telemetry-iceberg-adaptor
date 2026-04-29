use anyhow::{Context, Result, bail};
use base64::Engine;
use opentelemetry_proto::tonic::{
    collector::{logs::v1::ExportLogsServiceRequest, trace::v1::ExportTraceServiceRequest},
    common::v1::{AnyValue, KeyValue, any_value},
};
use prost::Message;
use serde_json::{Map, Number, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtlpEncoding {
    Protobuf,
    Json,
    Ndjson,
}

#[derive(Debug, Clone)]
pub struct LogRow {
    pub observed_timestamp_nanos: i64,
    pub timestamp_nanos: i64,
    pub severity_number: i32,
    pub severity_text: Option<String>,
    pub body: Option<String>,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub service_name: Option<String>,
    pub resource_attributes: Vec<(String, String)>,
    pub log_attributes: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct SpanRow {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub trace_state: Option<String>,
    pub name: String,
    pub kind: i32,
    pub start_time_unix_nano: i64,
    pub end_time_unix_nano: i64,
    pub duration_nanos: i64,
    pub status_code: i32,
    pub status_message: Option<String>,
    pub service_name: Option<String>,
    pub resource_attributes: Vec<(String, String)>,
    pub span_attributes: Vec<(String, String)>,
    pub event_count: i32,
    pub link_count: i32,
}

pub fn encoding_from_content_type(content_type: &str) -> Result<OtlpEncoding> {
    let media_type = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();

    match media_type.as_str() {
        "application/x-protobuf" => Ok(OtlpEncoding::Protobuf),
        "application/json" => Ok(OtlpEncoding::Json),
        "application/x-ndjson" | "application/ndjson" => Ok(OtlpEncoding::Ndjson),
        other => bail!("unsupported OTLP content type: {other}"),
    }
}

pub fn decode_logs(payload: &[u8], encoding: OtlpEncoding) -> Result<Vec<LogRow>> {
    if encoding == OtlpEncoding::Ndjson {
        return decode_ndjson_logs(payload);
    }

    let request = match encoding {
        OtlpEncoding::Protobuf => ExportLogsServiceRequest::decode(payload)
            .context("decoding OTLP protobuf logs request")?,
        OtlpEncoding::Json => {
            serde_json::from_slice(payload).context("decoding OTLP JSON logs request")?
        }
        OtlpEncoding::Ndjson => unreachable!("handled above"),
    };

    let mut rows = Vec::new();
    for resource_logs in request.resource_logs {
        let resource_attrs_json = resource_logs
            .resource
            .as_ref()
            .map(|resource| attrs_to_json(&resource.attributes))
            .unwrap_or_else(|| Value::Object(Map::new()));
        let resource_attributes = resource_logs
            .resource
            .as_ref()
            .map(|resource| attrs_to_pairs(&resource.attributes))
            .unwrap_or_default();
        let service_name = resource_attrs_json
            .get("service.name")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);

        for scope_logs in resource_logs.scope_logs {
            for record in scope_logs.log_records {
                let log_attributes = attrs_to_pairs(&record.attributes);
                rows.push(LogRow {
                    observed_timestamp_nanos: unsigned_nanos_to_i64(record.observed_time_unix_nano),
                    timestamp_nanos: unsigned_nanos_to_i64(record.time_unix_nano),
                    severity_number: record.severity_number,
                    severity_text: empty_to_none(record.severity_text),
                    body: record.body.as_ref().map(any_value_to_string),
                    trace_id: non_empty_hex(&record.trace_id),
                    span_id: non_empty_hex(&record.span_id),
                    service_name: service_name.clone(),
                    resource_attributes: resource_attributes.clone(),
                    log_attributes,
                });
            }
        }
    }

    Ok(rows)
}

pub fn decode_traces(payload: &[u8], encoding: OtlpEncoding) -> Result<Vec<SpanRow>> {
    let request = match encoding {
        OtlpEncoding::Protobuf => ExportTraceServiceRequest::decode(payload)
            .context("decoding OTLP protobuf traces request")?,
        OtlpEncoding::Json => {
            serde_json::from_slice(payload).context("decoding OTLP JSON traces request")?
        }
        OtlpEncoding::Ndjson => bail!("NDJSON traces are not supported"),
    };

    let mut rows = Vec::new();
    for resource_spans in request.resource_spans {
        let resource_attrs_json = resource_spans
            .resource
            .as_ref()
            .map(|resource| attrs_to_json(&resource.attributes))
            .unwrap_or_else(|| Value::Object(Map::new()));
        let resource_attributes = resource_spans
            .resource
            .as_ref()
            .map(|resource| attrs_to_pairs(&resource.attributes))
            .unwrap_or_default();
        let service_name = resource_attrs_json
            .get("service.name")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);

        for scope_spans in resource_spans.scope_spans {
            for span in scope_spans.spans {
                let start_time_unix_nano = unsigned_nanos_to_i64(span.start_time_unix_nano);
                let end_time_unix_nano = unsigned_nanos_to_i64(span.end_time_unix_nano);
                let status = span.status.unwrap_or_default();

                rows.push(SpanRow {
                    trace_id: non_empty_hex(&span.trace_id).unwrap_or_default(),
                    span_id: non_empty_hex(&span.span_id).unwrap_or_default(),
                    parent_span_id: non_empty_hex(&span.parent_span_id),
                    trace_state: empty_to_none(span.trace_state),
                    name: span.name,
                    kind: span.kind,
                    start_time_unix_nano,
                    end_time_unix_nano,
                    duration_nanos: end_time_unix_nano.saturating_sub(start_time_unix_nano),
                    status_code: status.code,
                    status_message: empty_to_none(status.message),
                    service_name: service_name.clone(),
                    resource_attributes: resource_attributes.clone(),
                    span_attributes: attrs_to_pairs(&span.attributes),
                    event_count: span.events.len() as i32,
                    link_count: span.links.len() as i32,
                });
            }
        }
    }

    Ok(rows)
}

fn decode_ndjson_logs(payload: &[u8]) -> Result<Vec<LogRow>> {
    let text = std::str::from_utf8(payload).context("NDJSON payload must be UTF-8")?;
    let mut rows = Vec::new();
    for (line_index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let value: Value = serde_json::from_str(line)
            .with_context(|| format!("decoding NDJSON log line {}", line_index + 1))?;
        rows.push(ndjson_value_to_row(value, line_index + 1)?);
    }
    Ok(rows)
}

fn ndjson_value_to_row(value: Value, line_number: usize) -> Result<LogRow> {
    let object = value
        .as_object()
        .with_context(|| format!("NDJSON log line {line_number} must be a JSON object"))?;

    let resource_attributes = object
        .get("resource_attributes")
        .or_else(|| object.get("resource"))
        .map(value_to_pairs)
        .transpose()?
        .unwrap_or_default();
    let log_attributes = object
        .get("log_attributes")
        .or_else(|| object.get("attributes"))
        .map(value_to_pairs)
        .transpose()?
        .unwrap_or_else(|| unknown_fields_to_pairs(object));

    let service_name = string_field(object, "service_name")
        .or_else(|| string_field(object, "service.name"))
        .or_else(|| {
            resource_attributes
                .iter()
                .find(|(key, _)| key == "service.name")
                .map(|(_, value)| value.clone())
        });

    Ok(LogRow {
        observed_timestamp_nanos: i64_field(object, "observed_timestamp_nanos")
            .or_else(|| i64_field(object, "observed_time_unix_nano"))
            .or_else(|| i64_field(object, "observed_timestamp"))
            .unwrap_or_default(),
        timestamp_nanos: i64_field(object, "timestamp_nanos")
            .or_else(|| i64_field(object, "time_unix_nano"))
            .or_else(|| i64_field(object, "timestamp"))
            .unwrap_or_default(),
        severity_number: i64_field(object, "severity_number").unwrap_or_default() as i32,
        severity_text: string_field(object, "severity_text")
            .or_else(|| string_field(object, "severity"))
            .or_else(|| string_field(object, "level")),
        body: object
            .get("body")
            .or_else(|| object.get("message"))
            .or_else(|| object.get("msg"))
            .or_else(|| object.get("log_message"))
            .map(value_to_log_string),
        trace_id: string_field(object, "trace_id").or_else(|| string_field(object, "traceId")),
        span_id: string_field(object, "span_id").or_else(|| string_field(object, "spanId")),
        service_name,
        resource_attributes,
        log_attributes,
    })
}

fn value_to_pairs(value: &Value) -> Result<Vec<(String, String)>> {
    let object = value
        .as_object()
        .context("attribute container must be a JSON object")?;
    Ok(object
        .iter()
        .map(|(key, value)| (key.clone(), value_to_log_string(value)))
        .collect())
}

fn unknown_fields_to_pairs(object: &Map<String, Value>) -> Vec<(String, String)> {
    object
        .iter()
        .filter(|(key, _)| !is_known_ndjson_field(key))
        .map(|(key, value)| (key.clone(), value_to_log_string(value)))
        .collect()
}

fn is_known_ndjson_field(key: &str) -> bool {
    matches!(
        key,
        "observed_timestamp_nanos"
            | "observed_time_unix_nano"
            | "observed_timestamp"
            | "timestamp_nanos"
            | "time_unix_nano"
            | "timestamp"
            | "severity_number"
            | "severity_text"
            | "severity"
            | "level"
            | "body"
            | "message"
            | "msg"
            | "log_message"
            | "trace_id"
            | "traceId"
            | "span_id"
            | "spanId"
            | "service_name"
            | "service.name"
            | "resource"
            | "resource_attributes"
            | "attributes"
            | "log_attributes"
    )
}

fn string_field(object: &Map<String, Value>, key: &str) -> Option<String> {
    object.get(key).and_then(|value| match value {
        Value::String(value) if !value.is_empty() => Some(value.clone()),
        _ => None,
    })
}

fn i64_field(object: &Map<String, Value>, key: &str) -> Option<i64> {
    object.get(key).and_then(|value| match value {
        Value::Number(value) => value.as_i64(),
        Value::String(value) => value.parse().ok(),
        _ => None,
    })
}

fn value_to_log_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        _ => value.to_string(),
    }
}

fn attrs_to_json(attrs: &[KeyValue]) -> Value {
    let mut map = Map::new();
    for attr in attrs {
        if let Some(value) = &attr.value {
            map.insert(attr.key.clone(), any_value_to_json(value));
        }
    }
    Value::Object(map)
}

fn attrs_to_pairs(attrs: &[KeyValue]) -> Vec<(String, String)> {
    attrs
        .iter()
        .filter_map(|attr| {
            attr.value.as_ref().map(|value| {
                let value = match &value.value {
                    Some(any_value::Value::StringValue(value)) => value.clone(),
                    _ => any_value_to_json(value).to_string(),
                };
                (attr.key.clone(), value)
            })
        })
        .collect()
}

fn any_value_to_json(value: &AnyValue) -> Value {
    match &value.value {
        Some(any_value::Value::StringValue(value)) => Value::String(value.clone()),
        Some(any_value::Value::BoolValue(value)) => Value::Bool(*value),
        Some(any_value::Value::IntValue(value)) => Value::Number(Number::from(*value)),
        Some(any_value::Value::DoubleValue(value)) => Number::from_f64(*value)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Some(any_value::Value::ArrayValue(values)) => {
            Value::Array(values.values.iter().map(any_value_to_json).collect())
        }
        Some(any_value::Value::KvlistValue(values)) => attrs_to_json(&values.values),
        Some(any_value::Value::BytesValue(values)) => {
            Value::String(base64::engine::general_purpose::STANDARD.encode(values))
        }
        None => Value::Null,
    }
}

fn any_value_to_string(value: &AnyValue) -> String {
    match &value.value {
        Some(any_value::Value::StringValue(value)) => value.clone(),
        _ => any_value_to_json(value).to_string(),
    }
}

fn unsigned_nanos_to_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

fn empty_to_none(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

fn non_empty_hex(bytes: &[u8]) -> Option<String> {
    if bytes.is_empty() {
        return None;
    }
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    Some(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::{
        collector::trace::v1::ExportTraceServiceRequest,
        common::v1::{AnyValue, KeyValue, any_value},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber},
        resource::v1::Resource,
        trace::v1::{ResourceSpans, ScopeSpans, Span, Status},
    };
    use prost::Message;

    #[test]
    fn accepts_standard_otlp_content_types() {
        assert_eq!(
            encoding_from_content_type("application/x-protobuf").unwrap(),
            OtlpEncoding::Protobuf
        );
        assert_eq!(
            encoding_from_content_type("application/json; charset=utf-8").unwrap(),
            OtlpEncoding::Json
        );
        assert_eq!(
            encoding_from_content_type("application/x-ndjson").unwrap(),
            OtlpEncoding::Ndjson
        );
    }

    #[test]
    fn decodes_protobuf_logs_with_arbitrary_attributes() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_owned(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("checkout".to_owned())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 42,
                        observed_time_unix_nano: 43,
                        severity_number: SeverityNumber::Info as i32,
                        severity_text: "INFO".to_owned(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("hello".to_owned())),
                        }),
                        attributes: vec![KeyValue {
                            key: "tenant".to_owned(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("acme".to_owned())),
                            }),
                        }],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        event_name: String::new(),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let mut payload = Vec::new();
        request.encode(&mut payload).unwrap();

        let rows = decode_logs(&payload, OtlpEncoding::Protobuf).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].service_name.as_deref(), Some("checkout"));
        assert_eq!(
            rows[0].log_attributes,
            vec![("tenant".to_owned(), "acme".to_owned())]
        );
        assert_eq!(
            rows[0].trace_id.as_deref(),
            Some("01010101010101010101010101010101")
        );
    }

    #[test]
    fn decodes_ndjson_logs_with_unknown_fields_as_attributes() {
        let payload = br#"{"timestamp_nanos":42,"severity_text":"INFO","body":"hello","service_name":"checkout","tenant":"acme"}
{"time_unix_nano":"43","body":{"message":"world"},"resource_attributes":{"service.name":"payments"},"attributes":{"region":"ap-southeast-2"}}"#;

        let rows = decode_logs(payload, OtlpEncoding::Ndjson).unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].timestamp_nanos, 42);
        assert_eq!(rows[0].service_name.as_deref(), Some("checkout"));
        assert_eq!(
            rows[0].log_attributes,
            vec![("tenant".to_owned(), "acme".to_owned())]
        );
        assert_eq!(rows[1].timestamp_nanos, 43);
        assert_eq!(rows[1].service_name.as_deref(), Some("payments"));
        assert_eq!(
            rows[1].log_attributes,
            vec![("region".to_owned(), "ap-southeast-2".to_owned())]
        );
    }

    #[test]
    fn decodes_generic_ndjson_aliases_for_event_logs() {
        let payload =
            br#"{"timestamp":"42","message":"denied","severity":"WARN","hostname":"fw01","action":"deny"}"#;

        let rows = decode_logs(payload, OtlpEncoding::Ndjson).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].timestamp_nanos, 42);
        assert_eq!(rows[0].body.as_deref(), Some("denied"));
        assert_eq!(rows[0].severity_text.as_deref(), Some("WARN"));
        assert_eq!(
            rows[0].log_attributes,
            vec![
                ("action".to_owned(), "deny".to_owned()),
                ("hostname".to_owned(), "fw01".to_owned())
            ]
        );
    }

    #[test]
    fn decodes_protobuf_traces_with_resource_and_span_attributes() {
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_owned(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("checkout".to_owned())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        parent_span_id: vec![],
                        trace_state: String::new(),
                        name: "GET /checkout".to_owned(),
                        kind: 2,
                        start_time_unix_nano: 100,
                        end_time_unix_nano: 150,
                        attributes: vec![KeyValue {
                            key: "http.method".to_owned(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("GET".to_owned())),
                            }),
                        }],
                        dropped_attributes_count: 0,
                        events: vec![],
                        dropped_events_count: 0,
                        links: vec![],
                        dropped_links_count: 0,
                        status: Some(Status {
                            message: String::new(),
                            code: 1,
                        }),
                        flags: 0,
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let mut payload = Vec::new();
        request.encode(&mut payload).unwrap();

        let rows = decode_traces(&payload, OtlpEncoding::Protobuf).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].service_name.as_deref(), Some("checkout"));
        assert_eq!(rows[0].duration_nanos, 50);
        assert_eq!(
            rows[0].span_attributes,
            vec![("http.method".to_owned(), "GET".to_owned())]
        );
        assert_eq!(rows[0].trace_id, "01010101010101010101010101010101");
    }
}
