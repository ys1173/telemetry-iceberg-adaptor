# Telemetry Iceberg Adapter

Receives live telemetry, writes it to durable Parquet files, and commits those files to an Apache Iceberg table — making logs, traces, and metrics queryable via Athena, Snowflake, Trino, or Spark within seconds of ingestion.

```
┌─────────────────────────────┐        ┌──────────────────────────────────────┐
│          INPUTS             │        │              OUTPUTS                 │
│                             │        │                                      │
│  OTLP/HTTP  logs, traces    │        │  Parquet files  (local or S3)        │
│  OTLP/gRPC  logs, traces    │──────▶ │  Iceberg tables (Glue / REST)        │
│  NDJSON     logs            │  WAL   │  Queryable via Athena, Snowflake,    │
│  Prometheus remote-write    │        │  Trino, or Spark                     │
└─────────────────────────────┘        └──────────────────────────────────────┘
```

**Inputs** accepted on the wire:

| Protocol | Endpoint | Signal |
|----------|----------|--------|
| OTLP/HTTP protobuf or JSON | `POST /v1/logs` | Logs |
| OTLP/HTTP protobuf or JSON | `POST /v1/traces` | Traces |
| OTLP/gRPC | `0.0.0.0:4317` | Logs, Traces |
| NDJSON (one JSON object per line) | `POST /v1/logs/ndjson` | Logs |
| Prometheus remote write | `POST /api/v1/write` | Metrics |

**Outputs** written per signal:

| Signal | Iceberg table | Key columns |
|--------|---------------|-------------|
| Logs (`otel` profile) | `logs` | `timestamp_nanos`, `severity_text`, `body`, `service_name`, `resource_attributes`, `log_attributes` |
| Logs (`generic_event` profile) | configurable | `timestamp_nanos`, `message`, `severity`, `hostname`, `attributes` |
| Traces | `traces` | `trace_id`, `span_id`, `name`, `duration_nanos`, `service_name`, `span_attributes` |
| Metrics | `metrics` | `metric_name`, `timestamp_millis`, `value`, `labels` |

Every accepted request is durably written to a WAL before `200 OK` is returned. Background workers drain the WAL into Parquet batches and commit them to the configured Iceberg catalog with OCC retry.

## Current Scope

- Implements OTLP/HTTP logs on `POST /v1/logs`.
- Implements OTLP/HTTP traces on `POST /v1/traces`.
- Implements OTLP/gRPC logs and traces on the standard OpenTelemetry `LogsService/Export` and `TraceService/Export` services.
- Implements HTTP NDJSON logs on `POST /v1/logs/ndjson`, and also accepts `application/x-ndjson` on `POST /v1/logs`.
- Implements Prometheus remote write metrics on `POST /api/v1/write`.
- Accepts standard OTLP/HTTP protobuf (`application/x-protobuf`) and JSON protobuf (`application/json`).
- Supports `Content-Encoding: gzip`.
- Returns `200 OK` only after the decoded request has been durably written to the configured WAL backend.
- Returns `429 Too Many Requests` when the pending WAL backlog exceeds `OTLP_ADAPTER_MAX_PENDING_WAL_BYTES`.
- Writes Parquet files locally and can upload them to S3 via `OTLP_ADAPTER_PARQUET_S3_BASE`.
- Keeps arbitrary OTEL attributes in `Map<Utf8, Utf8>` columns by default, with optional configured log projections into physical columns.
- Commits staged Parquet manifests through a serialized commit coordinator with bounded OCC retry.

`/v1/metrics` for native OTLP metrics intentionally returns `501 Not Implemented`; Prometheus remote write metrics are supported on `/api/v1/write`.

## Runtime Configuration

Configuration can come from a TOML file and environment variables. The loader uses this order:

1. `OTLP_ADAPTER_CONFIG=/path/to/config.toml`, if set.
2. `./telemetry-iceberg-adapter.toml`, if present.
3. Built-in defaults.

Environment variables override config-file values. See `telemetry-iceberg-adapter.example.toml` for a complete file.

| Variable | Default | Description |
| --- | --- | --- |
| `OTLP_ADAPTER_CONFIG` | unset | Optional path to a TOML config file. |
| `OTLP_ADAPTER_BIND` | `0.0.0.0:4318` | HTTP bind address. |
| `OTLP_ADAPTER_GRPC_BIND` | `0.0.0.0:4317` | OTLP/gRPC bind address. |
| `OTLP_ADAPTER_INSTANCE_ID` | random UUID | Instance ID used in deterministic Parquet file names. |
| `OTLP_ADAPTER_WAL_DIR` | `./data/wal` | Durable WAL directory. |
| `OTLP_ADAPTER_WAL_BACKEND` | `local` | WAL backend: `local` or `kafka`. |
| `OTLP_ADAPTER_KAFKA_BOOTSTRAP_SERVERS` | unset | Kafka bootstrap servers. Required when `OTLP_ADAPTER_WAL_BACKEND=kafka`. |
| `OTLP_ADAPTER_KAFKA_GROUP_ID` | `telemetry-iceberg-adapter` | Kafka consumer group prefix for WAL replay. |
| `OTLP_ADAPTER_KAFKA_RECORDS_TOPIC` | `telemetry-iceberg-wal-records` | Topic for accepted telemetry request records. |
| `OTLP_ADAPTER_KAFKA_MANIFESTS_TOPIC` | `telemetry-iceberg-wal-manifests` | Topic for Parquet commit manifests. |
| `OTLP_ADAPTER_KAFKA_PRODUCE_TIMEOUT_SECS` | `30` | Timeout for producing WAL records/manifests. |
| `OTLP_ADAPTER_PARQUET_DIR` | `./data/parquet` | Local/staged Parquet output directory. |
| `OTLP_ADAPTER_MAX_HTTP_BODY_BYTES` | `16777216` | Max accepted request body size. |
| `OTLP_ADAPTER_MAX_PENDING_WAL_BYTES` | `536870912` | Backpressure threshold for pending WAL bytes. |
| `OTLP_ADAPTER_BATCH_MAX_ROWS` | `100000` | Max rows per Parquet batch. |
| `OTLP_ADAPTER_BATCH_MAX_BYTES` | `134217728` | Estimated max uncompressed batch bytes. |
| `OTLP_ADAPTER_BATCH_MAX_AGE_SECS` | `60` | Time-based batch flush threshold. |
| `OTLP_ADAPTER_LOG_WORKERS` | `1` | Number of parallel log batch/write workers. Iceberg commits remain serialized. |
| `OTLP_ADAPTER_COMMIT_RETRY_ATTEMPTS` | `8` | Max OCC/catalog commit attempts. |
| `OTLP_ADAPTER_COMMIT_RETRY_INITIAL_BACKOFF_MS` | `200` | Initial commit retry backoff. |
| `OTLP_ADAPTER_COMMIT_RETRY_MAX_BACKOFF_SECS` | `10` | Max commit retry backoff. |
| `OTLP_ADAPTER_CATALOG_TYPE` | `noop` | Catalog backend: `noop`, `glue`, `rest`, or reserved `hive`. |
| `OTLP_ADAPTER_CATALOG_WAREHOUSE` | unset | Iceberg warehouse for catalog backends that require one. |
| `OTLP_ADAPTER_CATALOG_NAMESPACE` | `default` | Iceberg namespace for `logs`, `metrics`, and `traces` tables. |
| `OTLP_ADAPTER_REST_CATALOG_URI` | unset | Iceberg REST catalog URI for Lakekeeper, Polaris, or compatible catalogs. |
| `OTLP_ADAPTER_REST_CATALOG_CREDENTIAL` | unset | Optional REST catalog OAuth credential, usually `client_id:client_secret`. |
| `OTLP_ADAPTER_REST_CATALOG_TOKEN` | unset | Optional REST catalog bearer token. |
| `OTLP_ADAPTER_REST_CATALOG_OAUTH2_SERVER_URI` | unset | Optional OAuth token endpoint override. |
| `OTLP_ADAPTER_REST_CATALOG_PREFIX` | unset | Optional REST catalog prefix. |
| `OTLP_ADAPTER_REST_CATALOG_SCOPE` | unset | Optional OAuth scope. |
| `OTLP_ADAPTER_LOGS_TABLE` | `logs` | Iceberg table name for logs. |
| `OTLP_ADAPTER_LOGS_SCHEMA_PROFILE` | `otel` | Log table schema profile: `otel` or `generic_event`. |
| `OTLP_ADAPTER_METRICS_TABLE` | `metrics` | Iceberg table name for metrics. |
| `OTLP_ADAPTER_TRACES_TABLE` | `traces` | Iceberg table name for traces. |
| `OTLP_ADAPTER_LOGS_S3_PREFIX` | table name | Optional S3 prefix under `OTLP_ADAPTER_PARQUET_S3_BASE` for log Parquet files. |
| `OTLP_ADAPTER_METRICS_S3_PREFIX` | table name | Optional S3 prefix under `OTLP_ADAPTER_PARQUET_S3_BASE` for metric Parquet files. |
| `OTLP_ADAPTER_TRACES_S3_PREFIX` | table name | Optional S3 prefix under `OTLP_ADAPTER_PARQUET_S3_BASE` for trace Parquet files. |

When `OTLP_ADAPTER_CATALOG_TYPE=noop`, the service uses a no-op committer so ingestion, WAL replay, Parquet writing, and retry flow can be tested locally. Legacy `OTLP_ADAPTER_GLUE_WAREHOUSE` and `[glue]` config still select Glue for backwards compatibility.

Supported catalog backends:

- `noop`: local/test mode; does not update Iceberg metadata.
- `glue`: AWS Glue catalog via `iceberg-catalog-glue`.
- `rest`: Iceberg REST catalog via `iceberg-catalog-rest`, suitable for Lakekeeper, Polaris, and compatible REST catalogs.
- `hive`: reserved in config, but not wired yet because this project does not currently include a Hive catalog implementation.

For `glue` and `rest`, the adapter validates the configured logs, metrics, and traces Iceberg tables at startup. Each table must already exist in the configured namespace, and its columns must be compatible with the adapter schemas below. `noop` mode skips this validation.

Log tables can promote selected parsed JSON/log attributes into physical Iceberg columns while leaving all other fields in `log_attributes`:

```toml
[tables.logs]
name = "firewall_logs"
schema_profile = "generic_event"
s3_prefix = "security/firewall_logs"

[[tables.logs.projections]]
column = "src_ip"
source = "$.src_ip"
data_type = "string"

[[tables.logs.projections]]
column = "bytes"
source = "$.bytes"
data_type = "long"
```

Supported log schema profiles are:

- `otel`: OpenTelemetry-style log schema with `body`, `severity_text`, `resource_attributes`, and `log_attributes`.
- `generic_event`: smaller event schema with `timestamp_nanos`, `observed_timestamp_nanos`, `message`, `severity`, `hostname`, and `attributes`.

Supported projection sources are `$.field`, `log_attributes.field`, `resource_attributes.field`, and built-in fields such as `body`, `severity_text`, and `service_name`. Supported projection types are `string`, `int`, `long`, and `double`.

Example:

```sh
cp telemetry-iceberg-adapter.example.toml telemetry-iceberg-adapter.toml
cargo run
```

Or with an explicit path:

```sh
OTLP_ADAPTER_CONFIG=/etc/otlp-iceberg/config.toml cargo run
```

## Input Methods

All input methods feed the same `LogIngestor`, which validates the payload and appends it to the configured WAL before acknowledging the sender.

- `otlp-http`: `POST /v1/logs` with `application/x-protobuf` or `application/json`.
- `otlp-http-traces`: `POST /v1/traces` with `application/x-protobuf` or `application/json`.
- `otlp-grpc`: standard `opentelemetry.proto.collector.logs.v1.LogsService/Export` and `opentelemetry.proto.collector.trace.v1.TraceService/Export` on `OTLP_ADAPTER_GRPC_BIND`.
- `http-ndjson`: `POST /v1/logs/ndjson` or `POST /v1/logs` with `application/x-ndjson`.
- `prometheus-remote-write`: `POST /api/v1/write` with Snappy-compressed Prometheus `WriteRequest` protobuf payloads.

NDJSON expects one JSON object per line. Recognized top-level fields are `timestamp_nanos`, `time_unix_nano`, `observed_timestamp_nanos`, `severity_number`, `severity_text`, `body`, `trace_id`, `span_id`, `service_name`, `resource_attributes`, and `log_attributes`/`attributes`. Unknown top-level fields are preserved as log attributes.

Prometheus remote write samples are written to the `metrics` Iceberg table. The current schema stores sample-style time series data: metric name, timestamp in milliseconds, value, and labels as a map column. Histograms, exemplars, and metadata are accepted by the protobuf decoder but are not yet expanded into dedicated rows.

## Pipeline Parallelism

HTTP and gRPC ingestion run on Tokio's multi-threaded runtime. Accepted requests are first durably written to the configured WAL, then the background pipeline scans pending WAL records.

Logs can be sharded across multiple batch/write workers with `pipeline.log_workers` or `OTLP_ADAPTER_LOG_WORKERS`. Each log worker owns an independent batcher and Parquet/S3 write path, so multiple log files can be built and uploaded concurrently. The commit coordinator remains serialized and commits staged manifests to Iceberg with OCC retry, which avoids catalog contention while allowing the expensive file-writing path to run in parallel.

For high-rate streams, tune `log_workers`, `batch.max_rows`, `batch.max_bytes`, WAL backend, and S3/network capacity together. A 100k logs/sec target should be validated with a dedicated load test before using the result as a production sizing claim.

## WAL Backends

The default `local` WAL is a disk-backed buffer under `OTLP_ADAPTER_WAL_DIR`. It writes request records and commit manifests as JSON files and uses `sync_data` plus atomic rename before acknowledging HTTP requests.

The optional `kafka` WAL stores accepted OTLP requests in `OTLP_ADAPTER_KAFKA_RECORDS_TOPIC` and staged Parquet commit manifests in `OTLP_ADAPTER_KAFKA_MANIFESTS_TOPIC`. Kafka offsets are committed only after the next durable state exists:

1. A request record offset is committed after its Parquet file is written and the commit manifest has been produced.
2. A commit manifest offset is committed after the Iceberg commit succeeds.

This keeps the same at-least-once model while allowing Kafka to replace local disk as the durable WAL. Build with Kafka support using:

```sh
cargo build --features kafka
```

The Kafka implementation uses `rdkafka`. With the current vendored configuration, the build environment needs `cmake`; alternatively, adjust the dependency features for a system `librdkafka` installation.

## Data Schema

The default `otel` log profile uses stable typed columns for core fields:

- `observed_timestamp_nanos`
- `timestamp_nanos`
- `severity_number`
- `severity_text`
- `body`
- `trace_id`
- `span_id`
- `service_name`
- `resource_attributes`
- `log_attributes`

Arbitrary OTEL resource and log attributes are stored as Arrow map columns. Non-string OTEL values are JSON-encoded into the map value string. If `tables.logs.projections` is configured, projected log/resource attributes are written to additional physical columns and removed from the remaining map column.

The `generic_event` log profile is intended for heterogeneous syslog, device, and security events. It uses a smaller core:

- `timestamp_nanos`
- `observed_timestamp_nanos`
- `message`
- `severity`
- `hostname`
- `attributes`

In `generic_event`, resource and log attributes are merged into `attributes`, while promoted fields become physical columns and are removed from `attributes`.

The metrics table currently uses:

- `metric_name`
- `timestamp_millis`
- `value`
- `labels`

The traces table currently uses:

- `trace_id`
- `span_id`
- `parent_span_id`
- `trace_state`
- `name`
- `kind`
- `start_time_unix_nano`
- `end_time_unix_nano`
- `duration_nanos`
- `status_code`
- `status_message`
- `service_name`
- `resource_attributes`
- `span_attributes`
- `event_count`
- `link_count`

## AWS Smoke Test

For a first AWS validation, use Athena over uploaded Parquet files. This is simpler than Lambda and validates the adapter's decode, Arrow, Parquet, S3, Glue table, and query schema path.

Current note: `parquet_dir` is still a local staging directory. For Glue/REST Iceberg mode, set `storage.parquet_s3_base` or `OTLP_ADAPTER_PARQUET_S3_BASE` so the adapter uploads staged Parquet files to S3 before committing them.

Minimal config for the adapter:

```toml
[storage]
parquet_dir = "./data/parquet"

[batch]
max_age_secs = 5

[catalog]
type = "noop"
```

Run the adapter and your log generator, or let the script send a few NDJSON logs:

```sh
pip install boto3 requests
cargo run
```

In another shell:

```sh
python scripts/aws_log_smoke_test.py \
  --bucket my-telemetry-test-bucket \
  --prefix smoke/telemetry-iceberg-adapter \
  --athena-output s3://my-telemetry-test-bucket/athena-results/ \
  --region us-east-1
```

The script creates an Athena database/table over `s3://<bucket>/<prefix>/logs/`, queries the generated rows, and verifies map attributes such as `log_attributes['smoke.test_id']`.

Useful IAM permissions for the smoke test are `s3:PutObject`, `s3:GetObject`, `s3:ListBucket`, `athena:StartQueryExecution`, `athena:GetQueryExecution`, `athena:GetQueryResults`, and Glue database/table create/read/update/delete permissions for the smoke database.

## Delivery Semantics

The adapter is at-least-once:

1. Input request is decoded and validated.
2. The decompressed OTLP payload is written to the configured WAL backend.
3. Only then does the handler return `200 OK`.
4. Background workers replay pending WAL records into Parquet files.
5. Staged Parquet manifests remain pending until the Iceberg commit succeeds.
6. Commit conflicts refresh table metadata and retry without rewriting the Parquet data file.

If the process exits after WAL append but before commit, replay resumes from the WAL on restart.
