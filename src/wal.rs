use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::task;
use uuid::Uuid;

#[cfg(feature = "kafka")]
use {
    anyhow::anyhow,
    rdkafka::{
        ClientConfig, Offset, TopicPartitionList,
        consumer::{CommitMode, Consumer, StreamConsumer},
        message::Message,
        producer::{FutureProducer, FutureRecord},
        util::Timeout,
    },
    std::{collections::HashMap, time::Duration},
    tokio::{sync::Mutex, time::timeout},
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Signal {
    Logs,
    Metrics,
    Traces,
}

impl Signal {
    pub fn table_name(self) -> &'static str {
        match self {
            Signal::Logs => "logs",
            Signal::Metrics => "metrics",
            Signal::Traces => "traces",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRecord {
    pub id: Uuid,
    pub signal: Signal,
    pub content_type: String,
    pub received_at: DateTime<Utc>,
    pub payload_base64: String,
}

impl WalRecord {
    pub fn payload(&self) -> Result<Vec<u8>> {
        STANDARD
            .decode(&self.payload_base64)
            .context("decoding WAL payload")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagedDataFile {
    pub path: PathBuf,
    pub source_wal_ids: Vec<Uuid>,
    pub row_count: usize,
    pub byte_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitManifest {
    pub id: Uuid,
    pub table: String,
    pub staged_at: DateTime<Utc>,
    pub files: Vec<StagedDataFile>,
}

#[async_trait]
pub trait WalStore: std::fmt::Debug + Send + Sync + 'static {
    async fn append(
        &self,
        signal: Signal,
        content_type: String,
        payload: Vec<u8>,
    ) -> Result<WalRecord>;

    async fn pending_received(&self, limit: usize) -> Result<Vec<WalRecord>>;

    async fn pending_manifests(&self, limit: usize) -> Result<Vec<CommitManifest>>;

    async fn pending_received_bytes(&self) -> Result<u64>;

    async fn mark_parquet_written(&self, manifest: &CommitManifest) -> Result<()>;

    async fn mark_committed(&self, manifest_id: Uuid) -> Result<()>;
}

#[derive(Debug)]
pub struct LocalWal {
    root: PathBuf,
}

impl LocalWal {
    pub async fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        let wal = Self { root };
        wal.ensure_layout().await?;
        Ok(wal)
    }
}

#[async_trait]
impl WalStore for LocalWal {
    async fn append(
        &self,
        signal: Signal,
        content_type: String,
        payload: Vec<u8>,
    ) -> Result<WalRecord> {
        let record = WalRecord {
            id: Uuid::new_v4(),
            signal,
            content_type,
            received_at: Utc::now(),
            payload_base64: STANDARD.encode(payload),
        };

        let path = self.received_path(record.id);
        let tmp_path = path.with_extension("tmp");
        let bytes = serde_json::to_vec(&record)?;

        task::spawn_blocking(move || durable_write_json(&tmp_path, &path, &bytes))
            .await?
            .context("writing WAL record")?;

        Ok(record)
    }

    async fn pending_received(&self, limit: usize) -> Result<Vec<WalRecord>> {
        let dir = self.received_dir();
        task::spawn_blocking(move || read_json_files::<WalRecord>(&dir, limit))
            .await?
            .context("reading pending WAL records")
    }

    async fn pending_manifests(&self, limit: usize) -> Result<Vec<CommitManifest>> {
        let dir = self.parquet_written_dir();
        task::spawn_blocking(move || read_json_files::<CommitManifest>(&dir, limit))
            .await?
            .context("reading staged commit manifests")
    }

    async fn pending_received_bytes(&self) -> Result<u64> {
        let dir = self.received_dir();
        task::spawn_blocking(move || dir_size(&dir))
            .await?
            .context("computing pending WAL bytes")
    }

    async fn mark_parquet_written(&self, manifest: &CommitManifest) -> Result<()> {
        let manifest_path = self.manifest_path(manifest.id);
        let tmp_path = manifest_path.with_extension("tmp");
        let manifest_bytes = serde_json::to_vec(manifest)?;
        let source_paths = manifest
            .files
            .iter()
            .flat_map(|file| file.source_wal_ids.iter().copied())
            .map(|id| self.received_path(id))
            .collect::<Vec<_>>();

        task::spawn_blocking(move || {
            durable_write_json(&tmp_path, &manifest_path, &manifest_bytes)?;
            for source in source_paths {
                match fs::remove_file(&source) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;

        Ok(())
    }

    async fn mark_committed(&self, manifest_id: Uuid) -> Result<()> {
        let source = self.manifest_path(manifest_id);
        let dest = self.committed_dir().join(format!("{manifest_id}.json"));

        task::spawn_blocking(move || {
            ensure_dir(dest.parent().expect("committed manifest has parent"))?;
            match fs::rename(&source, &dest) {
                Ok(()) => Ok::<(), anyhow::Error>(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    Ok::<(), anyhow::Error>(())
                }
                Err(error) => Err(error.into()),
            }
        })
        .await??;

        Ok(())
    }
}

impl LocalWal {
    async fn ensure_layout(&self) -> Result<()> {
        let dirs = [
            self.received_dir(),
            self.parquet_written_dir(),
            self.committed_dir(),
        ];
        task::spawn_blocking(move || {
            for dir in dirs {
                ensure_dir(&dir)?;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        Ok(())
    }

    fn received_dir(&self) -> PathBuf {
        self.root.join("received")
    }

    fn parquet_written_dir(&self) -> PathBuf {
        self.root.join("parquet_written")
    }

    fn committed_dir(&self) -> PathBuf {
        self.root.join("committed")
    }

    fn received_path(&self, id: Uuid) -> PathBuf {
        self.received_dir().join(format!("{id}.json"))
    }

    fn manifest_path(&self, id: Uuid) -> PathBuf {
        self.parquet_written_dir().join(format!("{id}.json"))
    }
}

#[cfg(feature = "kafka")]
#[derive(Debug, Clone)]
pub struct KafkaWalConfig {
    pub bootstrap_servers: String,
    pub group_id: String,
    pub records_topic: String,
    pub manifests_topic: String,
    pub produce_timeout: Duration,
}

#[cfg(feature = "kafka")]
#[derive(Debug, Clone)]
struct KafkaOffset {
    topic: String,
    partition: i32,
    next_offset: i64,
}

#[cfg(feature = "kafka")]
#[derive(Debug)]
pub struct KafkaWal {
    producer: FutureProducer,
    records_consumer: StreamConsumer,
    manifests_consumer: StreamConsumer,
    config: KafkaWalConfig,
    record_offsets: Mutex<HashMap<Uuid, KafkaOffset>>,
    manifest_offsets: Mutex<HashMap<Uuid, KafkaOffset>>,
}

#[cfg(feature = "kafka")]
impl KafkaWal {
    pub fn open(config: KafkaWalConfig) -> Result<Self> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("message.timeout.ms", "30000")
            .create()
            .context("creating Kafka WAL producer")?;

        let records_consumer = consumer(
            &config.bootstrap_servers,
            &format!("{}-records", config.group_id),
            &config.records_topic,
        )?;
        let manifests_consumer = consumer(
            &config.bootstrap_servers,
            &format!("{}-manifests", config.group_id),
            &config.manifests_topic,
        )?;

        Ok(Self {
            producer,
            records_consumer,
            manifests_consumer,
            config,
            record_offsets: Mutex::new(HashMap::new()),
            manifest_offsets: Mutex::new(HashMap::new()),
        })
    }

    async fn produce<T>(&self, topic: &str, key: Uuid, value: &T) -> Result<()>
    where
        T: Serialize + Sync,
    {
        let payload = serde_json::to_vec(value)?;
        let key = key.to_string();
        self.producer
            .send(
                FutureRecord::to(topic).key(&key).payload(&payload),
                Timeout::After(self.config.produce_timeout),
            )
            .await
            .map(|_| ())
            .map_err(|(error, _)| anyhow!("producing Kafka WAL message: {error}"))
    }
}

#[cfg(feature = "kafka")]
#[async_trait]
impl WalStore for KafkaWal {
    async fn append(
        &self,
        signal: Signal,
        content_type: String,
        payload: Vec<u8>,
    ) -> Result<WalRecord> {
        let record = WalRecord {
            id: Uuid::new_v4(),
            signal,
            content_type,
            received_at: Utc::now(),
            payload_base64: STANDARD.encode(payload),
        };
        self.produce(&self.config.records_topic, record.id, &record)
            .await?;
        Ok(record)
    }

    async fn pending_received(&self, limit: usize) -> Result<Vec<WalRecord>> {
        consume_records(
            &self.records_consumer,
            &self.record_offsets,
            limit,
            "received WAL record",
        )
        .await
    }

    async fn pending_manifests(&self, limit: usize) -> Result<Vec<CommitManifest>> {
        consume_records(
            &self.manifests_consumer,
            &self.manifest_offsets,
            limit,
            "commit manifest",
        )
        .await
    }

    async fn pending_received_bytes(&self) -> Result<u64> {
        // Kafka backlog bytes are broker-side state; use consumer lag metrics externally.
        Ok(0)
    }

    async fn mark_parquet_written(&self, manifest: &CommitManifest) -> Result<()> {
        self.produce(&self.config.manifests_topic, manifest.id, manifest)
            .await?;
        commit_offsets(
            &self.records_consumer,
            &self.record_offsets,
            manifest_source_ids(manifest),
        )
        .await
    }

    async fn mark_committed(&self, manifest_id: Uuid) -> Result<()> {
        commit_offsets(
            &self.manifests_consumer,
            &self.manifest_offsets,
            vec![manifest_id],
        )
        .await
    }
}

#[cfg(feature = "kafka")]
fn consumer(bootstrap_servers: &str, group_id: &str, topic: &str) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .context("creating Kafka WAL consumer")?;
    consumer
        .subscribe(&[topic])
        .with_context(|| format!("subscribing Kafka WAL consumer to {topic}"))?;
    Ok(consumer)
}

#[cfg(feature = "kafka")]
async fn consume_records<T>(
    consumer: &StreamConsumer,
    offsets: &Mutex<HashMap<Uuid, KafkaOffset>>,
    limit: usize,
    label: &str,
) -> Result<Vec<T>>
where
    T: for<'de> Deserialize<'de> + IdentifiedWalMessage,
{
    let mut records = Vec::new();
    for _ in 0..limit {
        let message = match timeout(Duration::from_millis(100), consumer.recv()).await {
            Ok(Ok(message)) => message,
            Ok(Err(error)) => return Err(anyhow!("consuming Kafka {label}: {error}")),
            Err(_) => break,
        };

        let payload = message
            .payload()
            .ok_or_else(|| anyhow!("Kafka {label} had empty payload"))?;
        let record: T = serde_json::from_slice(payload)
            .with_context(|| format!("decoding Kafka {label} JSON"))?;
        offsets.lock().await.insert(
            record.id(),
            KafkaOffset {
                topic: message.topic().to_owned(),
                partition: message.partition(),
                next_offset: message.offset() + 1,
            },
        );
        records.push(record);
    }
    Ok(records)
}

#[cfg(feature = "kafka")]
async fn commit_offsets(
    consumer: &StreamConsumer,
    offsets: &Mutex<HashMap<Uuid, KafkaOffset>>,
    ids: Vec<Uuid>,
) -> Result<()> {
    let mut offsets = offsets.lock().await;
    let mut tpl = TopicPartitionList::new();
    for id in ids {
        if let Some(offset) = offsets.remove(&id) {
            tpl.add_partition_offset(
                &offset.topic,
                offset.partition,
                Offset::Offset(offset.next_offset),
            )?;
        }
    }

    if tpl.count() > 0 {
        consumer
            .commit(&tpl, CommitMode::Sync)
            .context("committing Kafka WAL offsets")?;
    }
    Ok(())
}

#[cfg(feature = "kafka")]
fn manifest_source_ids(manifest: &CommitManifest) -> Vec<Uuid> {
    manifest
        .files
        .iter()
        .flat_map(|file| file.source_wal_ids.iter().copied())
        .collect()
}

#[cfg(feature = "kafka")]
trait IdentifiedWalMessage {
    fn id(&self) -> Uuid;
}

#[cfg(feature = "kafka")]
impl IdentifiedWalMessage for WalRecord {
    fn id(&self) -> Uuid {
        self.id
    }
}

#[cfg(feature = "kafka")]
impl IdentifiedWalMessage for CommitManifest {
    fn id(&self) -> Uuid {
        self.id
    }
}

fn durable_write_json(tmp_path: &Path, final_path: &Path, bytes: &[u8]) -> Result<()> {
    ensure_dir(final_path.parent().expect("WAL path has parent"))?;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(tmp_path)?;
    file.write_all(bytes)?;
    file.write_all(b"\n")?;
    file.sync_data()?;
    drop(file);
    fs::rename(tmp_path, final_path)?;
    sync_parent(final_path)?;
    Ok(())
}

fn read_json_files<T>(dir: &Path, limit: usize) -> Result<Vec<T>>
where
    T: for<'de> Deserialize<'de>,
{
    let mut entries = fs::read_dir(dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "json"))
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());

    let mut values = Vec::new();
    for entry in entries.into_iter().take(limit) {
        let mut bytes = Vec::new();
        match File::open(entry.path()).and_then(|mut file| file.read_to_end(&mut bytes)) {
            Ok(_) => values.push(serde_json::from_slice(&bytes).context("parsing WAL JSON")?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(values)
}

fn dir_size(dir: &Path) -> Result<u64> {
    let mut total = 0;
    for entry in fs::read_dir(dir)? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        if !file_type.is_file() {
            continue;
        }
        match entry.metadata() {
            Ok(metadata) => total += metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(total)
}

fn ensure_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    Ok(())
}

fn sync_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        let dir = File::open(parent)?;
        dir.sync_data()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn append_is_visible_to_replay_before_ack_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let wal = LocalWal::open(dir.path()).await.unwrap();

        let record = wal
            .append(
                Signal::Logs,
                "application/x-protobuf".to_owned(),
                b"payload".to_vec(),
            )
            .await
            .unwrap();

        let pending = wal.pending_received(10).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, record.id);
        assert_eq!(pending[0].payload().unwrap(), b"payload");
    }

    #[tokio::test]
    async fn committed_manifest_moves_out_of_pending_queue() {
        let dir = tempfile::tempdir().unwrap();
        let wal = LocalWal::open(dir.path()).await.unwrap();
        let manifest = CommitManifest {
            id: Uuid::new_v4(),
            table: "logs".to_owned(),
            staged_at: Utc::now(),
            files: vec![StagedDataFile {
                path: PathBuf::from("s3://bucket/table/file.parquet"),
                source_wal_ids: vec![Uuid::new_v4()],
                row_count: 1,
                byte_size: 100,
            }],
        };

        wal.mark_parquet_written(&manifest).await.unwrap();
        assert_eq!(wal.pending_manifests(10).await.unwrap().len(), 1);

        wal.mark_committed(manifest.id).await.unwrap();
        assert!(wal.pending_manifests(10).await.unwrap().is_empty());
    }
}
