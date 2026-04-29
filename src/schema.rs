use std::{collections::HashSet, sync::Arc};

use arrow_array::builder::{Float64Builder, Int32Builder, Int64Builder, MapBuilder, StringBuilder};
use arrow_array::{
    ArrayRef, Float64Array, Int32Array, Int64Array, MapArray, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{
    config::{
        LogBuiltinField, LogProjectionColumn, LogProjectionSource, LogSchemaProfile,
        ProjectionDataType,
    },
    otlp::{LogRow, SpanRow},
    prometheus::PrometheusMetricRow,
};

pub fn log_schema() -> SchemaRef {
    let attr_entry = attributes_entry_field();

    Arc::new(Schema::new(vec![
        Field::new("observed_timestamp_nanos", DataType::Int64, false),
        Field::new("timestamp_nanos", DataType::Int64, false),
        Field::new("severity_number", DataType::Int32, false),
        Field::new("severity_text", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        Field::new("service_name", DataType::Utf8, true),
        Field::new(
            "resource_attributes",
            DataType::Map(attr_entry.clone(), false),
            false,
        ),
        Field::new("log_attributes", DataType::Map(attr_entry, false), false),
    ]))
}

pub fn log_schema_with_projections(projections: &[LogProjectionColumn]) -> SchemaRef {
    log_schema_for_profile(LogSchemaProfile::Otel, projections)
}

pub fn log_schema_for_profile(
    profile: LogSchemaProfile,
    projections: &[LogProjectionColumn],
) -> SchemaRef {
    let mut fields = match profile {
        LogSchemaProfile::Otel => log_schema().fields().iter().cloned().collect::<Vec<_>>(),
        LogSchemaProfile::GenericEvent => generic_event_log_schema()
            .fields()
            .iter()
            .cloned()
            .collect::<Vec<_>>(),
    };
    for projection in projections {
        fields.push(Arc::new(Field::new(
            &projection.column,
            projection.data_type.arrow_type(),
            true,
        )));
    }
    Arc::new(Schema::new(fields))
}

fn generic_event_log_schema() -> SchemaRef {
    let attr_entry = attributes_entry_field();

    Arc::new(Schema::new(vec![
        Field::new("timestamp_nanos", DataType::Int64, false),
        Field::new("observed_timestamp_nanos", DataType::Int64, false),
        Field::new("message", DataType::Utf8, true),
        Field::new("severity", DataType::Utf8, true),
        Field::new("hostname", DataType::Utf8, true),
        Field::new("attributes", DataType::Map(attr_entry, false), false),
    ]))
}

pub fn metric_schema() -> SchemaRef {
    let attr_entry = attributes_entry_field();

    Arc::new(Schema::new(vec![
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("timestamp_millis", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("labels", DataType::Map(attr_entry, false), false),
    ]))
}

pub fn trace_schema() -> SchemaRef {
    let attr_entry = attributes_entry_field();

    Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("parent_span_id", DataType::Utf8, true),
        Field::new("trace_state", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, false),
        Field::new("kind", DataType::Int32, false),
        Field::new("start_time_unix_nano", DataType::Int64, false),
        Field::new("end_time_unix_nano", DataType::Int64, false),
        Field::new("duration_nanos", DataType::Int64, false),
        Field::new("status_code", DataType::Int32, false),
        Field::new("status_message", DataType::Utf8, true),
        Field::new("service_name", DataType::Utf8, true),
        Field::new(
            "resource_attributes",
            DataType::Map(attr_entry.clone(), false),
            false,
        ),
        Field::new("span_attributes", DataType::Map(attr_entry, false), false),
        Field::new("event_count", DataType::Int32, false),
        Field::new("link_count", DataType::Int32, false),
    ]))
}

fn attributes_entry_field() -> Arc<Field> {
    Arc::new(Field::new(
        "entries",
        DataType::Struct(
            vec![
                Field::new("keys", DataType::Utf8, false),
                Field::new("values", DataType::Utf8, true),
            ]
            .into(),
        ),
        false,
    ))
}

pub fn rows_to_record_batch(
    rows: &[LogRow],
) -> std::result::Result<RecordBatch, arrow_schema::ArrowError> {
    rows_to_record_batch_with_projections(rows, &[])
}

pub fn rows_to_record_batch_with_projections(
    rows: &[LogRow],
    projections: &[LogProjectionColumn],
) -> std::result::Result<RecordBatch, arrow_schema::ArrowError> {
    rows_to_record_batch_for_profile(rows, LogSchemaProfile::Otel, projections)
}

pub fn rows_to_record_batch_for_profile(
    rows: &[LogRow],
    profile: LogSchemaProfile,
    projections: &[LogProjectionColumn],
) -> std::result::Result<RecordBatch, arrow_schema::ArrowError> {
    match profile {
        LogSchemaProfile::Otel => rows_to_otel_record_batch(rows, projections),
        LogSchemaProfile::GenericEvent => rows_to_generic_event_record_batch(rows, projections),
    }
}

fn rows_to_otel_record_batch(
    rows: &[LogRow],
    projections: &[LogProjectionColumn],
) -> std::result::Result<RecordBatch, arrow_schema::ArrowError> {
    let observed_timestamp_nanos = Int64Array::from(
        rows.iter()
            .map(|row| row.observed_timestamp_nanos)
            .collect::<Vec<_>>(),
    );
    let timestamp_nanos = Int64Array::from(
        rows.iter()
            .map(|row| row.timestamp_nanos)
            .collect::<Vec<_>>(),
    );
    let severity_number = Int32Array::from(
        rows.iter()
            .map(|row| row.severity_number)
            .collect::<Vec<_>>(),
    );
    let severity_text = StringArray::from(
        rows.iter()
            .map(|row| row.severity_text.as_deref())
            .collect::<Vec<_>>(),
    );
    let body = StringArray::from(
        rows.iter()
            .map(|row| row.body.as_deref())
            .collect::<Vec<_>>(),
    );
    let trace_id = StringArray::from(
        rows.iter()
            .map(|row| row.trace_id.as_deref())
            .collect::<Vec<_>>(),
    );
    let span_id = StringArray::from(
        rows.iter()
            .map(|row| row.span_id.as_deref())
            .collect::<Vec<_>>(),
    );
    let service_name = StringArray::from(
        rows.iter()
            .map(|row| row.service_name.as_deref())
            .collect::<Vec<_>>(),
    );
    let promoted_log_keys = promoted_log_attribute_keys(projections);
    let promoted_resource_keys = promoted_resource_attribute_keys(projections);
    let resource_attributes = filter_attributes(
        &rows
            .iter()
            .map(|row| &row.resource_attributes)
            .collect::<Vec<_>>(),
        &promoted_resource_keys,
    );
    let log_attributes = filter_attributes(
        &rows
            .iter()
            .map(|row| &row.log_attributes)
            .collect::<Vec<_>>(),
        &promoted_log_keys,
    );
    let resource_attributes = build_attributes_map(resource_attributes.iter())?;
    let log_attributes = build_attributes_map(log_attributes.iter())?;

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(observed_timestamp_nanos),
        Arc::new(timestamp_nanos),
        Arc::new(severity_number),
        Arc::new(severity_text),
        Arc::new(body),
        Arc::new(trace_id),
        Arc::new(span_id),
        Arc::new(service_name),
        Arc::new(resource_attributes),
        Arc::new(log_attributes),
    ];
    for projection in projections {
        columns.push(projected_column(rows, projection));
    }

    RecordBatch::try_new(
        log_schema_for_profile(LogSchemaProfile::Otel, projections),
        columns,
    )
}

fn rows_to_generic_event_record_batch(
    rows: &[LogRow],
    projections: &[LogProjectionColumn],
) -> std::result::Result<RecordBatch, arrow_schema::ArrowError> {
    let timestamp_nanos = Int64Array::from(
        rows.iter()
            .map(|row| row.timestamp_nanos)
            .collect::<Vec<_>>(),
    );
    let observed_timestamp_nanos = Int64Array::from(
        rows.iter()
            .map(|row| row.observed_timestamp_nanos)
            .collect::<Vec<_>>(),
    );
    let message = StringArray::from(
        rows.iter()
            .map(|row| row.body.as_deref())
            .collect::<Vec<_>>(),
    );
    let severity = StringArray::from(
        rows.iter()
            .map(|row| row.severity_text.as_deref())
            .collect::<Vec<_>>(),
    );
    let hostname = StringArray::from(rows.iter().map(generic_hostname).collect::<Vec<_>>());

    let promoted_log_keys = promoted_log_attribute_keys(projections);
    let promoted_resource_keys = promoted_resource_attribute_keys(projections);
    let attributes = rows
        .iter()
        .map(|row| generic_attributes(row, &promoted_log_keys, &promoted_resource_keys))
        .collect::<Vec<_>>();
    let attributes = build_attributes_map(attributes.iter())?;

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(timestamp_nanos),
        Arc::new(observed_timestamp_nanos),
        Arc::new(message),
        Arc::new(severity),
        Arc::new(hostname),
        Arc::new(attributes),
    ];
    for projection in projections {
        columns.push(projected_column(rows, projection));
    }

    RecordBatch::try_new(
        log_schema_for_profile(LogSchemaProfile::GenericEvent, projections),
        columns,
    )
}

pub fn estimate_rows_size(rows: &[LogRow]) -> usize {
    rows.iter()
        .map(|row| {
            std::mem::size_of::<LogRow>()
                + row.severity_text.as_ref().map_or(0, String::len)
                + row.body.as_ref().map_or(0, String::len)
                + row.trace_id.as_ref().map_or(0, String::len)
                + row.span_id.as_ref().map_or(0, String::len)
                + row.service_name.as_ref().map_or(0, String::len)
                + row
                    .resource_attributes
                    .iter()
                    .map(|(key, value)| key.len() + value.len())
                    .sum::<usize>()
                + row
                    .log_attributes
                    .iter()
                    .map(|(key, value)| key.len() + value.len())
                    .sum::<usize>()
        })
        .sum()
}

pub fn metric_rows_to_record_batch(
    rows: &[PrometheusMetricRow],
) -> std::result::Result<RecordBatch, arrow_schema::ArrowError> {
    let metric_name = StringArray::from(
        rows.iter()
            .map(|row| row.metric_name.as_str())
            .collect::<Vec<_>>(),
    );
    let timestamp_millis = Int64Array::from(
        rows.iter()
            .map(|row| row.timestamp_millis)
            .collect::<Vec<_>>(),
    );
    let value = Float64Array::from(rows.iter().map(|row| row.value).collect::<Vec<_>>());
    let labels = build_attributes_map(rows.iter().map(|row| &row.labels))?;

    RecordBatch::try_new(
        metric_schema(),
        vec![
            Arc::new(metric_name),
            Arc::new(timestamp_millis),
            Arc::new(value),
            Arc::new(labels),
        ],
    )
}

pub fn trace_rows_to_record_batch(
    rows: &[SpanRow],
) -> std::result::Result<RecordBatch, arrow_schema::ArrowError> {
    let trace_id = StringArray::from(
        rows.iter()
            .map(|row| row.trace_id.as_str())
            .collect::<Vec<_>>(),
    );
    let span_id = StringArray::from(
        rows.iter()
            .map(|row| row.span_id.as_str())
            .collect::<Vec<_>>(),
    );
    let parent_span_id = StringArray::from(
        rows.iter()
            .map(|row| row.parent_span_id.as_deref())
            .collect::<Vec<_>>(),
    );
    let trace_state = StringArray::from(
        rows.iter()
            .map(|row| row.trace_state.as_deref())
            .collect::<Vec<_>>(),
    );
    let name = StringArray::from(rows.iter().map(|row| row.name.as_str()).collect::<Vec<_>>());
    let kind = Int32Array::from(rows.iter().map(|row| row.kind).collect::<Vec<_>>());
    let start_time_unix_nano = Int64Array::from(
        rows.iter()
            .map(|row| row.start_time_unix_nano)
            .collect::<Vec<_>>(),
    );
    let end_time_unix_nano = Int64Array::from(
        rows.iter()
            .map(|row| row.end_time_unix_nano)
            .collect::<Vec<_>>(),
    );
    let duration_nanos = Int64Array::from(
        rows.iter()
            .map(|row| row.duration_nanos)
            .collect::<Vec<_>>(),
    );
    let status_code = Int32Array::from(rows.iter().map(|row| row.status_code).collect::<Vec<_>>());
    let status_message = StringArray::from(
        rows.iter()
            .map(|row| row.status_message.as_deref())
            .collect::<Vec<_>>(),
    );
    let service_name = StringArray::from(
        rows.iter()
            .map(|row| row.service_name.as_deref())
            .collect::<Vec<_>>(),
    );
    let resource_attributes =
        build_attributes_map(rows.iter().map(|row| &row.resource_attributes))?;
    let span_attributes = build_attributes_map(rows.iter().map(|row| &row.span_attributes))?;
    let event_count = Int32Array::from(rows.iter().map(|row| row.event_count).collect::<Vec<_>>());
    let link_count = Int32Array::from(rows.iter().map(|row| row.link_count).collect::<Vec<_>>());

    RecordBatch::try_new(
        trace_schema(),
        vec![
            Arc::new(trace_id),
            Arc::new(span_id),
            Arc::new(parent_span_id),
            Arc::new(trace_state),
            Arc::new(name),
            Arc::new(kind),
            Arc::new(start_time_unix_nano),
            Arc::new(end_time_unix_nano),
            Arc::new(duration_nanos),
            Arc::new(status_code),
            Arc::new(status_message),
            Arc::new(service_name),
            Arc::new(resource_attributes),
            Arc::new(span_attributes),
            Arc::new(event_count),
            Arc::new(link_count),
        ],
    )
}

pub fn estimate_trace_rows_size(rows: &[SpanRow]) -> usize {
    rows.iter()
        .map(|row| {
            std::mem::size_of::<SpanRow>()
                + row.trace_id.len()
                + row.span_id.len()
                + row.parent_span_id.as_ref().map_or(0, String::len)
                + row.trace_state.as_ref().map_or(0, String::len)
                + row.name.len()
                + row.status_message.as_ref().map_or(0, String::len)
                + row.service_name.as_ref().map_or(0, String::len)
                + row
                    .resource_attributes
                    .iter()
                    .map(|(key, value)| key.len() + value.len())
                    .sum::<usize>()
                + row
                    .span_attributes
                    .iter()
                    .map(|(key, value)| key.len() + value.len())
                    .sum::<usize>()
        })
        .sum()
}

fn build_attributes_map<'a>(
    rows: impl Iterator<Item = &'a Vec<(String, String)>>,
) -> std::result::Result<MapArray, arrow_schema::ArrowError> {
    let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for attrs in rows {
        for (key, value) in attrs {
            builder.keys().append_value(key);
            builder.values().append_value(value);
        }
        builder.append(true)?;
    }
    Ok(builder.finish())
}

fn filter_attributes(
    rows: &[&Vec<(String, String)>],
    promoted_keys: &HashSet<String>,
) -> Vec<Vec<(String, String)>> {
    rows.iter()
        .map(|attrs| {
            attrs
                .iter()
                .filter(|(key, _)| !promoted_keys.contains(key))
                .cloned()
                .collect()
        })
        .collect()
}

fn promoted_log_attribute_keys(projections: &[LogProjectionColumn]) -> HashSet<String> {
    projections
        .iter()
        .filter_map(|projection| match &projection.source {
            LogProjectionSource::LogAttribute(key) => Some(key.clone()),
            _ => None,
        })
        .collect()
}

fn promoted_resource_attribute_keys(projections: &[LogProjectionColumn]) -> HashSet<String> {
    projections
        .iter()
        .filter_map(|projection| match &projection.source {
            LogProjectionSource::ResourceAttribute(key) => Some(key.clone()),
            _ => None,
        })
        .collect()
}

fn projected_column(rows: &[LogRow], projection: &LogProjectionColumn) -> ArrayRef {
    match projection.data_type {
        ProjectionDataType::String => {
            let mut builder = StringBuilder::new();
            for row in rows {
                append_optional_string(&mut builder, projection_value(row, projection));
            }
            Arc::new(builder.finish())
        }
        ProjectionDataType::Int => {
            let mut builder = Int32Builder::new();
            for row in rows {
                match projection_value(row, projection).and_then(|value| value.parse().ok()) {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        ProjectionDataType::Long => {
            let mut builder = Int64Builder::new();
            for row in rows {
                match projection_value(row, projection).and_then(|value| value.parse().ok()) {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        ProjectionDataType::Double => {
            let mut builder = Float64Builder::new();
            for row in rows {
                match projection_value(row, projection).and_then(|value| value.parse().ok()) {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
    }
}

fn append_optional_string(builder: &mut StringBuilder, value: Option<String>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

fn projection_value(row: &LogRow, projection: &LogProjectionColumn) -> Option<String> {
    match &projection.source {
        LogProjectionSource::Builtin(field) => builtin_projection_value(row, *field),
        LogProjectionSource::LogAttribute(key) => attribute_value(&row.log_attributes, key),
        LogProjectionSource::ResourceAttribute(key) => {
            attribute_value(&row.resource_attributes, key)
        }
    }
}

fn builtin_projection_value(row: &LogRow, field: LogBuiltinField) -> Option<String> {
    match field {
        LogBuiltinField::ObservedTimestampNanos => Some(row.observed_timestamp_nanos.to_string()),
        LogBuiltinField::TimestampNanos => Some(row.timestamp_nanos.to_string()),
        LogBuiltinField::SeverityNumber => Some(row.severity_number.to_string()),
        LogBuiltinField::SeverityText => row.severity_text.clone(),
        LogBuiltinField::Body => row.body.clone(),
        LogBuiltinField::TraceId => row.trace_id.clone(),
        LogBuiltinField::SpanId => row.span_id.clone(),
        LogBuiltinField::ServiceName => row.service_name.clone(),
    }
}

fn attribute_value(attrs: &[(String, String)], key: &str) -> Option<String> {
    attrs
        .iter()
        .find(|(attr_key, _)| attr_key == key)
        .map(|(_, value)| value.clone())
}

fn generic_hostname(row: &LogRow) -> Option<&str> {
    attribute_value_ref(&row.log_attributes, "hostname")
        .or_else(|| attribute_value_ref(&row.log_attributes, "host"))
        .or_else(|| attribute_value_ref(&row.log_attributes, "host.name"))
        .or_else(|| attribute_value_ref(&row.resource_attributes, "host.name"))
        .or_else(|| attribute_value_ref(&row.resource_attributes, "hostname"))
        .or_else(|| attribute_value_ref(&row.resource_attributes, "host"))
        .or(row.service_name.as_deref())
}

fn generic_attributes(
    row: &LogRow,
    promoted_log_keys: &HashSet<String>,
    promoted_resource_keys: &HashSet<String>,
) -> Vec<(String, String)> {
    const HOST_KEYS: &[&str] = &["hostname", "host", "host.name"];
    let mut attrs = Vec::new();
    attrs.extend(
        row.resource_attributes
            .iter()
            .filter(|(key, _)| {
                !promoted_resource_keys.contains(key) && !HOST_KEYS.contains(&key.as_str())
            })
            .cloned(),
    );
    attrs.extend(
        row.log_attributes
            .iter()
            .filter(|(key, _)| {
                !promoted_log_keys.contains(key) && !HOST_KEYS.contains(&key.as_str())
            })
            .cloned(),
    );
    attrs
}

fn attribute_value_ref<'a>(attrs: &'a [(String, String)], key: &str) -> Option<&'a str> {
    attrs
        .iter()
        .find(|(attr_key, _)| attr_key == key)
        .map(|(_, value)| value.as_str())
}

impl ProjectionDataType {
    fn arrow_type(self) -> DataType {
        match self {
            Self::String => DataType::Utf8,
            Self::Int => DataType::Int32,
            Self::Long => DataType::Int64,
            Self::Double => DataType::Float64,
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::DataType;

    use super::*;
    use crate::config::{
        LogProjectionColumn, LogProjectionSource, LogSchemaProfile, ProjectionDataType,
    };

    #[test]
    fn log_schema_keeps_arbitrary_attributes_as_maps() {
        let schema = log_schema();

        assert!(matches!(
            schema
                .field_with_name("resource_attributes")
                .unwrap()
                .data_type(),
            DataType::Map(_, false)
        ));
        assert!(matches!(
            schema
                .field_with_name("log_attributes")
                .unwrap()
                .data_type(),
            DataType::Map(_, false)
        ));
    }

    #[test]
    fn rows_convert_to_record_batch() {
        let rows = vec![LogRow {
            observed_timestamp_nanos: 2,
            timestamp_nanos: 1,
            severity_number: 9,
            severity_text: Some("INFO".to_owned()),
            body: Some("hello".to_owned()),
            trace_id: None,
            span_id: None,
            service_name: Some("checkout".to_owned()),
            resource_attributes: vec![("service.name".to_owned(), "checkout".to_owned())],
            log_attributes: vec![("tenant".to_owned(), "acme".to_owned())],
        }];

        let batch = rows_to_record_batch(&rows).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 10);
    }

    #[test]
    fn rows_convert_to_record_batch_with_log_projection() {
        let rows = vec![LogRow {
            observed_timestamp_nanos: 2,
            timestamp_nanos: 1,
            severity_number: 9,
            severity_text: Some("INFO".to_owned()),
            body: Some("hello".to_owned()),
            trace_id: None,
            span_id: None,
            service_name: Some("checkout".to_owned()),
            resource_attributes: vec![("service.name".to_owned(), "checkout".to_owned())],
            log_attributes: vec![
                ("src_ip".to_owned(), "10.0.0.1".to_owned()),
                ("bytes".to_owned(), "42".to_owned()),
                ("tenant".to_owned(), "acme".to_owned()),
            ],
        }];
        let projections = vec![
            LogProjectionColumn {
                column: "src_ip".to_owned(),
                source: LogProjectionSource::LogAttribute("src_ip".to_owned()),
                data_type: ProjectionDataType::String,
            },
            LogProjectionColumn {
                column: "bytes".to_owned(),
                source: LogProjectionSource::LogAttribute("bytes".to_owned()),
                data_type: ProjectionDataType::Long,
            },
        ];

        let batch = rows_to_record_batch_with_projections(&rows, &projections).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 12);
        assert_eq!(batch.schema().field(10).name(), "src_ip");
        assert_eq!(batch.schema().field(11).name(), "bytes");
        let src_ip = batch
            .column(10)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let bytes = batch
            .column(11)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(src_ip.value(0), "10.0.0.1");
        assert_eq!(bytes.value(0), 42);
    }

    #[test]
    fn rows_convert_to_generic_event_profile() {
        let rows = vec![LogRow {
            observed_timestamp_nanos: 2,
            timestamp_nanos: 1,
            severity_number: 9,
            severity_text: Some("INFO".to_owned()),
            body: Some("login failed".to_owned()),
            trace_id: None,
            span_id: None,
            service_name: Some("auth".to_owned()),
            resource_attributes: vec![("service.name".to_owned(), "auth".to_owned())],
            log_attributes: vec![
                ("hostname".to_owned(), "fw01".to_owned()),
                ("device".to_owned(), "firewall".to_owned()),
                ("event_type".to_owned(), "login_failed".to_owned()),
            ],
        }];
        let projections = vec![LogProjectionColumn {
            column: "event_type".to_owned(),
            source: LogProjectionSource::LogAttribute("event_type".to_owned()),
            data_type: ProjectionDataType::String,
        }];

        let batch =
            rows_to_record_batch_for_profile(&rows, LogSchemaProfile::GenericEvent, &projections)
                .unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 7);
        assert_eq!(batch.schema().field(0).name(), "timestamp_nanos");
        assert_eq!(batch.schema().field(2).name(), "message");
        assert_eq!(batch.schema().field(3).name(), "severity");
        assert_eq!(batch.schema().field(4).name(), "hostname");
        assert_eq!(batch.schema().field(5).name(), "attributes");
        assert_eq!(batch.schema().field(6).name(), "event_type");

        let hostname = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let event_type = batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(hostname.value(0), "fw01");
        assert_eq!(event_type.value(0), "login_failed");
    }

    #[test]
    fn metric_rows_convert_to_record_batch() {
        let rows = vec![PrometheusMetricRow {
            metric_name: "http_requests_total".to_owned(),
            timestamp_millis: 1234,
            value: 42.0,
            labels: vec![("job".to_owned(), "api".to_owned())],
        }];

        let batch = metric_rows_to_record_batch(&rows).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 4);
    }

    #[test]
    fn trace_rows_convert_to_record_batch() {
        let rows = vec![SpanRow {
            trace_id: "01".repeat(16),
            span_id: "02".repeat(8),
            parent_span_id: None,
            trace_state: None,
            name: "GET /checkout".to_owned(),
            kind: 2,
            start_time_unix_nano: 10,
            end_time_unix_nano: 20,
            duration_nanos: 10,
            status_code: 1,
            status_message: None,
            service_name: Some("checkout".to_owned()),
            resource_attributes: vec![("service.name".to_owned(), "checkout".to_owned())],
            span_attributes: vec![("http.method".to_owned(), "GET".to_owned())],
            event_count: 0,
            link_count: 0,
        }];

        let batch = trace_rows_to_record_batch(&rows).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 16);
    }
}
