use anyhow::{Context, Result};
use prost::Message;
use snap::raw::{Decoder, Encoder};

pub const PROMETHEUS_REMOTE_WRITE_CONTENT_TYPE: &str = "application/x-protobuf";

#[derive(Debug, Clone)]
pub struct PrometheusMetricRow {
    pub metric_name: String,
    pub timestamp_millis: i64,
    pub value: f64,
    pub labels: Vec<(String, String)>,
}

#[derive(Clone, PartialEq, Message)]
pub struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
    #[prost(message, repeated, tag = "3")]
    pub metadata: Vec<MetricMetadata>,
}

#[derive(Clone, PartialEq, Message)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

#[derive(Clone, PartialEq, Message)]
pub struct MetricMetadata {
    #[prost(enumeration = "MetricType", tag = "1")]
    pub r#type: i32,
    #[prost(string, tag = "2")]
    pub metric_family_name: String,
    #[prost(string, tag = "3")]
    pub help: String,
    #[prost(string, tag = "4")]
    pub unit: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum MetricType {
    Unknown = 0,
    Counter = 1,
    Gauge = 2,
    Histogram = 3,
    GaugeHistogram = 4,
    Summary = 5,
    Info = 6,
    Stateset = 7,
}

pub fn decode_remote_write(payload: &[u8]) -> Result<Vec<PrometheusMetricRow>> {
    let decompressed = Decoder::new()
        .decompress_vec(payload)
        .context("decompressing Prometheus remote write Snappy payload")?;
    decode_write_request(&decompressed)
}

pub fn encode_remote_write(request: &WriteRequest) -> Result<Vec<u8>> {
    let mut protobuf = Vec::new();
    request.encode(&mut protobuf)?;
    Encoder::new()
        .compress_vec(&protobuf)
        .context("compressing Prometheus remote write payload")
}

pub fn decode_write_request(payload: &[u8]) -> Result<Vec<PrometheusMetricRow>> {
    let request = WriteRequest::decode(payload).context("decoding Prometheus WriteRequest")?;
    let mut rows = Vec::new();

    for series in request.timeseries {
        let metric_name = series
            .labels
            .iter()
            .find(|label| label.name == "__name__")
            .map(|label| label.value.clone())
            .unwrap_or_else(|| "unknown".to_owned());
        let labels = series
            .labels
            .into_iter()
            .filter(|label| label.name != "__name__")
            .map(|label| (label.name, label.value))
            .collect::<Vec<_>>();

        for sample in series.samples {
            rows.push(PrometheusMetricRow {
                metric_name: metric_name.clone(),
                timestamp_millis: sample.timestamp,
                value: sample.value,
                labels: labels.clone(),
            });
        }
    }

    Ok(rows)
}

pub fn estimate_metric_rows_size(rows: &[PrometheusMetricRow]) -> usize {
    rows.iter()
        .map(|row| {
            std::mem::size_of::<PrometheusMetricRow>()
                + row.metric_name.len()
                + row
                    .labels
                    .iter()
                    .map(|(key, value)| key.len() + value.len())
                    .sum::<usize>()
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_snappy_remote_write_samples() {
        let payload = encode_remote_write(&WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    Label {
                        name: "__name__".to_owned(),
                        value: "http_requests_total".to_owned(),
                    },
                    Label {
                        name: "job".to_owned(),
                        value: "api".to_owned(),
                    },
                ],
                samples: vec![Sample {
                    value: 42.0,
                    timestamp: 1234,
                }],
            }],
            metadata: vec![],
        })
        .unwrap();

        let rows = decode_remote_write(&payload).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].metric_name, "http_requests_total");
        assert_eq!(rows[0].timestamp_millis, 1234);
        assert_eq!(rows[0].value, 42.0);
        assert_eq!(rows[0].labels, vec![("job".to_owned(), "api".to_owned())]);
    }
}
