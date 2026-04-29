use std::{collections::HashSet, sync::Arc, time::Duration};

use chrono::Utc;
use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{MissedTickBehavior, interval, sleep},
};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::{
    batching::{
        LogBatch, LogBatcher, MetricBatch, MetricBatcher, PendingLogRequest, PendingMetricRequest,
        PendingTraceRequest, TraceBatch, TraceBatcher,
    },
    commit::{CatalogCommitter, CommitCoordinator},
    config::Config,
    otlp::{decode_logs, decode_traces, encoding_from_content_type},
    parquet_writer::ParquetWriter,
    prometheus::decode_remote_write,
    wal::{CommitManifest, Signal, WalRecord, WalStore},
};

pub struct Pipeline;

pub struct PipelineHandles {
    pub batcher: JoinHandle<()>,
    pub committer: JoinHandle<()>,
}

impl Pipeline {
    pub fn spawn<C>(
        config: Arc<Config>,
        wal: Arc<dyn WalStore>,
        writer: Arc<ParquetWriter>,
        committer: Arc<C>,
    ) -> PipelineHandles
    where
        C: CatalogCommitter,
    {
        let batcher = {
            let config = config.clone();
            let wal = wal.clone();
            tokio::spawn(async move {
                run_batcher(config, wal, writer).await;
            })
        };

        let committer = tokio::spawn(async move {
            CommitCoordinator::new(config, wal, committer).run().await;
        });

        PipelineHandles { batcher, committer }
    }
}

async fn run_batcher(config: Arc<Config>, wal: Arc<dyn WalStore>, writer: Arc<ParquetWriter>) {
    let (completed_tx, mut completed_rx) =
        mpsc::channel::<Vec<Uuid>>(config.log_worker_count.max(1) * 4);
    let log_senders = spawn_log_workers(config.clone(), wal.clone(), writer.clone(), completed_tx);
    let mut metric_batcher = MetricBatcher::new(
        config.batch_max_rows,
        config.batch_max_bytes,
        config.batch_max_age,
    );
    let mut trace_batcher = TraceBatcher::new(
        config.batch_max_rows,
        config.batch_max_bytes,
        config.batch_max_age,
    );
    let mut in_memory = HashSet::new();

    loop {
        drain_completed_log_records(&mut completed_rx, &mut in_memory);

        match wal.pending_received(128).await {
            Ok(records) => {
                for record in records {
                    let record_id = record.id;
                    if !in_memory.insert(record_id) {
                        continue;
                    }

                    match record_to_pending(record).await {
                        Ok(Some(PendingRecord::Log(pending))) => {
                            let shard = log_worker_index(record_id, log_senders.len());
                            if let Err(error) = log_senders[shard].send(pending).await {
                                warn!(record_id = %error.0.record.id, "failed to dispatch log WAL record to worker");
                                in_memory.remove(&error.0.record.id);
                            }
                        }
                        Ok(Some(PendingRecord::Metric(pending))) => {
                            if let Some(batch) = metric_batcher.push(pending) {
                                flush_metric_batch(
                                    &config,
                                    wal.as_ref(),
                                    &writer,
                                    batch,
                                    &mut in_memory,
                                )
                                .await;
                            }
                        }
                        Ok(Some(PendingRecord::Trace(pending))) => {
                            if let Some(batch) = trace_batcher.push(pending) {
                                flush_trace_batch(
                                    &config,
                                    wal.as_ref(),
                                    &writer,
                                    batch,
                                    &mut in_memory,
                                )
                                .await;
                            }
                        }
                        Ok(None) => {
                            in_memory.remove(&record_id);
                        }
                        Err(error) => {
                            warn!(error = %error, "failed to decode WAL record; it remains pending");
                            in_memory.remove(&record_id);
                        }
                    }
                }

                if let Some(batch) = metric_batcher.flush_if_expired() {
                    flush_metric_batch(&config, wal.as_ref(), &writer, batch, &mut in_memory).await;
                }
                if let Some(batch) = trace_batcher.flush_if_expired() {
                    flush_trace_batch(&config, wal.as_ref(), &writer, batch, &mut in_memory).await;
                }
            }
            Err(error) => warn!(error = %error, "failed to scan WAL"),
        }

        sleep(Duration::from_secs(1)).await;
    }
}

fn spawn_log_workers(
    config: Arc<Config>,
    wal: Arc<dyn WalStore>,
    writer: Arc<ParquetWriter>,
    completed_tx: mpsc::Sender<Vec<Uuid>>,
) -> Vec<mpsc::Sender<PendingLogRequest>> {
    let worker_count = config.log_worker_count.max(1);
    (0..worker_count)
        .map(|worker_id| {
            let (tx, rx) = mpsc::channel::<PendingLogRequest>(256);
            tokio::spawn(log_worker_loop(
                worker_id,
                config.clone(),
                wal.clone(),
                writer.clone(),
                rx,
                completed_tx.clone(),
            ));
            tx
        })
        .collect()
}

async fn log_worker_loop(
    worker_id: usize,
    config: Arc<Config>,
    wal: Arc<dyn WalStore>,
    writer: Arc<ParquetWriter>,
    mut receiver: mpsc::Receiver<PendingLogRequest>,
    completed_tx: mpsc::Sender<Vec<Uuid>>,
) {
    let mut batcher = LogBatcher::new(
        config.batch_max_rows,
        config.batch_max_bytes,
        config.batch_max_age,
    );
    let mut tick = interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            maybe_pending = receiver.recv() => {
                let Some(pending) = maybe_pending else {
                    if let Some(batch) = batcher.take() {
                        flush_log_batch(&config, wal.as_ref(), &writer, batch, &completed_tx, worker_id).await;
                    }
                    return;
                };

                if let Some(batch) = batcher.push(pending) {
                    flush_log_batch(&config, wal.as_ref(), &writer, batch, &completed_tx, worker_id).await;
                }
            }
            _ = tick.tick() => {
                if let Some(batch) = batcher.flush_if_expired() {
                    flush_log_batch(&config, wal.as_ref(), &writer, batch, &completed_tx, worker_id).await;
                }
            }
        }
    }
}

fn drain_completed_log_records(
    completed_rx: &mut mpsc::Receiver<Vec<Uuid>>,
    in_memory: &mut HashSet<Uuid>,
) {
    while let Ok(ids) = completed_rx.try_recv() {
        for id in ids {
            in_memory.remove(&id);
        }
    }
}

fn log_worker_index(record_id: Uuid, worker_count: usize) -> usize {
    let bytes = record_id.as_bytes();
    let mut hash = 0usize;
    for byte in bytes {
        hash = hash.wrapping_mul(31).wrapping_add(*byte as usize);
    }
    hash % worker_count.max(1)
}

enum PendingRecord {
    Log(PendingLogRequest),
    Metric(PendingMetricRequest),
    Trace(PendingTraceRequest),
}

async fn record_to_pending(record: WalRecord) -> anyhow::Result<Option<PendingRecord>> {
    match record.signal {
        Signal::Logs => {
            let encoding = encoding_from_content_type(&record.content_type)?;
            let payload = record.payload()?;
            let rows = decode_logs(&payload, encoding)?;
            if rows.is_empty() {
                debug!(record_id = %record.id, "WAL record contained no log rows");
                return Ok(None);
            }
            Ok(Some(PendingRecord::Log(PendingLogRequest::new(
                record, rows,
            ))))
        }
        Signal::Metrics => {
            let payload = record.payload()?;
            let rows = decode_remote_write(&payload)?;
            if rows.is_empty() {
                debug!(record_id = %record.id, "WAL record contained no metric rows");
                return Ok(None);
            }
            Ok(Some(PendingRecord::Metric(PendingMetricRequest::new(
                record, rows,
            ))))
        }
        Signal::Traces => {
            let encoding = encoding_from_content_type(&record.content_type)?;
            let payload = record.payload()?;
            let rows = decode_traces(&payload, encoding)?;
            if rows.is_empty() {
                debug!(record_id = %record.id, "WAL record contained no trace rows");
                return Ok(None);
            }
            Ok(Some(PendingRecord::Trace(PendingTraceRequest::new(
                record, rows,
            ))))
        }
    }
}

async fn flush_log_batch(
    config: &Config,
    wal: &dyn WalStore,
    writer: &ParquetWriter,
    batch: LogBatch,
    completed_tx: &mpsc::Sender<Vec<Uuid>>,
    worker_id: usize,
) {
    let source_ids = batch
        .records
        .iter()
        .map(|record| record.id)
        .collect::<Vec<_>>();

    match writer.write_logs(&batch.records, &batch.rows).await {
        Ok(file) => {
            let manifest = CommitManifest {
                id: Uuid::new_v4(),
                table: config.tables.logs.name.clone(),
                staged_at: Utc::now(),
                files: vec![file],
            };
            if let Err(error) = wal.mark_parquet_written(&manifest).await {
                warn!(worker_id, manifest_id = %manifest.id, error = %error, "failed to stage commit manifest");
            }
        }
        Err(error) => warn!(worker_id, error = %error, "failed to write parquet batch"),
    }

    if let Err(error) = completed_tx.send(source_ids).await {
        warn!(
            worker_id,
            completed_count = error.0.len(),
            "failed to report completed log WAL records"
        );
    }
}

async fn flush_metric_batch(
    config: &Config,
    wal: &dyn WalStore,
    writer: &ParquetWriter,
    batch: MetricBatch,
    in_memory: &mut HashSet<Uuid>,
) {
    let source_ids = batch
        .records
        .iter()
        .map(|record| record.id)
        .collect::<Vec<_>>();

    match writer.write_metrics(&batch.records, &batch.rows).await {
        Ok(file) => {
            let manifest = CommitManifest {
                id: Uuid::new_v4(),
                table: config.tables.metrics.name.clone(),
                staged_at: Utc::now(),
                files: vec![file],
            };
            if let Err(error) = wal.mark_parquet_written(&manifest).await {
                warn!(manifest_id = %manifest.id, error = %error, "failed to stage metrics commit manifest");
            }
        }
        Err(error) => warn!(error = %error, "failed to write metrics parquet batch"),
    }

    for id in source_ids {
        in_memory.remove(&id);
    }
}

async fn flush_trace_batch(
    config: &Config,
    wal: &dyn WalStore,
    writer: &ParquetWriter,
    batch: TraceBatch,
    in_memory: &mut HashSet<Uuid>,
) {
    let source_ids = batch
        .records
        .iter()
        .map(|record| record.id)
        .collect::<Vec<_>>();

    match writer.write_traces(&batch.records, &batch.rows).await {
        Ok(file) => {
            let manifest = CommitManifest {
                id: Uuid::new_v4(),
                table: config.tables.traces.name.clone(),
                staged_at: Utc::now(),
                files: vec![file],
            };
            if let Err(error) = wal.mark_parquet_written(&manifest).await {
                warn!(manifest_id = %manifest.id, error = %error, "failed to stage traces commit manifest");
            }
        }
        Err(error) => warn!(error = %error, "failed to write traces parquet batch"),
    }

    for id in source_ids {
        in_memory.remove(&id);
    }
}
