use std::time::{Duration, Instant};

use crate::{
    otlp::LogRow,
    otlp::SpanRow,
    prometheus::{PrometheusMetricRow, estimate_metric_rows_size},
    schema::{estimate_rows_size, estimate_trace_rows_size},
    wal::WalRecord,
};

#[derive(Debug)]
pub struct PendingLogRequest {
    pub record: WalRecord,
    pub rows: Vec<LogRow>,
    pub estimated_bytes: usize,
}

impl PendingLogRequest {
    pub fn new(record: WalRecord, rows: Vec<LogRow>) -> Self {
        let estimated_bytes = estimate_rows_size(&rows);
        Self {
            record,
            rows,
            estimated_bytes,
        }
    }
}

#[derive(Debug)]
pub struct LogBatch {
    pub records: Vec<WalRecord>,
    pub rows: Vec<LogRow>,
}

#[derive(Debug)]
pub struct LogBatcher {
    max_rows: usize,
    max_bytes: usize,
    max_age: Duration,
    opened_at: Instant,
    records: Vec<WalRecord>,
    rows: Vec<LogRow>,
    estimated_bytes: usize,
}

impl LogBatcher {
    pub fn new(max_rows: usize, max_bytes: usize, max_age: Duration) -> Self {
        Self {
            max_rows,
            max_bytes,
            max_age,
            opened_at: Instant::now(),
            records: Vec::new(),
            rows: Vec::new(),
            estimated_bytes: 0,
        }
    }

    pub fn push(&mut self, request: PendingLogRequest) -> Option<LogBatch> {
        self.records.push(request.record);
        self.rows.extend(request.rows);
        self.estimated_bytes += request.estimated_bytes;

        if self.rows.len() >= self.max_rows || self.estimated_bytes >= self.max_bytes {
            self.take()
        } else {
            None
        }
    }

    pub fn flush_if_expired(&mut self) -> Option<LogBatch> {
        if !self.rows.is_empty() && self.opened_at.elapsed() >= self.max_age {
            self.take()
        } else {
            None
        }
    }

    pub fn take(&mut self) -> Option<LogBatch> {
        if self.rows.is_empty() {
            return None;
        }

        self.opened_at = Instant::now();
        self.estimated_bytes = 0;
        Some(LogBatch {
            records: std::mem::take(&mut self.records),
            rows: std::mem::take(&mut self.rows),
        })
    }
}

#[derive(Debug)]
pub struct PendingMetricRequest {
    pub record: WalRecord,
    pub rows: Vec<PrometheusMetricRow>,
    pub estimated_bytes: usize,
}

impl PendingMetricRequest {
    pub fn new(record: WalRecord, rows: Vec<PrometheusMetricRow>) -> Self {
        let estimated_bytes = estimate_metric_rows_size(&rows);
        Self {
            record,
            rows,
            estimated_bytes,
        }
    }
}

#[derive(Debug)]
pub struct MetricBatch {
    pub records: Vec<WalRecord>,
    pub rows: Vec<PrometheusMetricRow>,
}

#[derive(Debug)]
pub struct MetricBatcher {
    max_rows: usize,
    max_bytes: usize,
    max_age: Duration,
    opened_at: Instant,
    records: Vec<WalRecord>,
    rows: Vec<PrometheusMetricRow>,
    estimated_bytes: usize,
}

impl MetricBatcher {
    pub fn new(max_rows: usize, max_bytes: usize, max_age: Duration) -> Self {
        Self {
            max_rows,
            max_bytes,
            max_age,
            opened_at: Instant::now(),
            records: Vec::new(),
            rows: Vec::new(),
            estimated_bytes: 0,
        }
    }

    pub fn push(&mut self, request: PendingMetricRequest) -> Option<MetricBatch> {
        self.records.push(request.record);
        self.rows.extend(request.rows);
        self.estimated_bytes += request.estimated_bytes;

        if self.rows.len() >= self.max_rows || self.estimated_bytes >= self.max_bytes {
            self.take()
        } else {
            None
        }
    }

    pub fn flush_if_expired(&mut self) -> Option<MetricBatch> {
        if !self.rows.is_empty() && self.opened_at.elapsed() >= self.max_age {
            self.take()
        } else {
            None
        }
    }

    pub fn take(&mut self) -> Option<MetricBatch> {
        if self.rows.is_empty() {
            return None;
        }

        self.opened_at = Instant::now();
        self.estimated_bytes = 0;
        Some(MetricBatch {
            records: std::mem::take(&mut self.records),
            rows: std::mem::take(&mut self.rows),
        })
    }
}

#[derive(Debug)]
pub struct PendingTraceRequest {
    pub record: WalRecord,
    pub rows: Vec<SpanRow>,
    pub estimated_bytes: usize,
}

impl PendingTraceRequest {
    pub fn new(record: WalRecord, rows: Vec<SpanRow>) -> Self {
        let estimated_bytes = estimate_trace_rows_size(&rows);
        Self {
            record,
            rows,
            estimated_bytes,
        }
    }
}

#[derive(Debug)]
pub struct TraceBatch {
    pub records: Vec<WalRecord>,
    pub rows: Vec<SpanRow>,
}

#[derive(Debug)]
pub struct TraceBatcher {
    max_rows: usize,
    max_bytes: usize,
    max_age: Duration,
    opened_at: Instant,
    records: Vec<WalRecord>,
    rows: Vec<SpanRow>,
    estimated_bytes: usize,
}

impl TraceBatcher {
    pub fn new(max_rows: usize, max_bytes: usize, max_age: Duration) -> Self {
        Self {
            max_rows,
            max_bytes,
            max_age,
            opened_at: Instant::now(),
            records: Vec::new(),
            rows: Vec::new(),
            estimated_bytes: 0,
        }
    }

    pub fn push(&mut self, request: PendingTraceRequest) -> Option<TraceBatch> {
        self.records.push(request.record);
        self.rows.extend(request.rows);
        self.estimated_bytes += request.estimated_bytes;

        if self.rows.len() >= self.max_rows || self.estimated_bytes >= self.max_bytes {
            self.take()
        } else {
            None
        }
    }

    pub fn flush_if_expired(&mut self) -> Option<TraceBatch> {
        if !self.rows.is_empty() && self.opened_at.elapsed() >= self.max_age {
            self.take()
        } else {
            None
        }
    }

    pub fn take(&mut self) -> Option<TraceBatch> {
        if self.rows.is_empty() {
            return None;
        }

        self.opened_at = Instant::now();
        self.estimated_bytes = 0;
        Some(TraceBatch {
            records: std::mem::take(&mut self.records),
            rows: std::mem::take(&mut self.rows),
        })
    }
}
