# E2E Test Findings: Glue Iceberg + Athena

## Overview

We built and ran end-to-end tests for the telemetry-iceberg-adapter covering all three signal types (logs, traces, metrics) in two modes:

- **athena-external**: adapter writes Parquet locally → test script uploads to S3 → Glue external table → Athena query
- **glue-iceberg**: adapter writes Parquet to S3 directly → commits to Glue Iceberg catalog → Athena queries Iceberg table

Both modes passed with 10 rows each for logs, traces, and metrics.

---

## Changes Made

### 1. S3 upload in the adapter (`src/parquet_writer.rs`)

The adapter previously only wrote Parquet to local disk. We added optional S3 upload via a new `S3Uploader` struct. When `OTLP_ADAPTER_PARQUET_S3_BASE` is set, the adapter uploads each Parquet file to S3 after writing it locally and commits the S3 URI to the Iceberg catalog instead of the local path.

Key details:
- `ParquetWriter::new` is now `async` (initializes the AWS SDK S3 client conditionally)
- `parse_s3_base()` accepts both `s3://` and `s3a://` input
- Data file paths returned to the commit layer always use `s3://` (matching the warehouse scheme)

### 2. Config: `parquet_s3_base` field (`src/config.rs`)

Added `parquet_s3_base: Option<String>` to `Config` and `StorageFileConfig`. Reads from:
- Environment variable: `OTLP_ADAPTER_PARQUET_S3_BASE`
- TOML key: `storage.parquet_s3_base`

### 3. S3 scheme fix for `iceberg-catalog-glue` (`src/commit.rs`)

**Root cause of the main blocker**: `iceberg-catalog-glue 0.9.0` hardcodes `configured_scheme: "s3a"` in its default `OpenDalStorageFactory::S3`. This means every file path the catalog reads or writes must start with `s3a://`. Athena creates Iceberg table metadata at `s3://` paths, so every commit attempt failed with:

```
DataInvalid => Invalid s3 url: s3://..., should start with s3a://&lt;your-bucket&gt;/
```

**Fix**: `GlueCatalogBuilder` exposes `with_storage_factory()` (via the `CatalogBuilder` trait). We inject a custom `OpenDalStorageFactory::S3` with `configured_scheme` derived from the warehouse URI:

```rust
let scheme = if settings.warehouse.starts_with("s3a://") { "s3a" } else { "s3" };

let catalog = GlueCatalogBuilder::default()
    .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
        configured_scheme: scheme.to_string(),
        customized_credential_load: None,
    }))
    .load("glue", props)
    .await?;
```

This added `iceberg-storage-opendal = { version = "0.9.0", features = ["opendal-s3"] }` to `Cargo.toml`.

With `s3://` warehouse: Athena-created metadata paths, new metadata written by the crate, and data files committed by the adapter all use `s3://` consistently.

### 4. `main.rs`

Changed `ParquetWriter::new(config.clone())?` → `ParquetWriter::new(config.clone()).await?` to match the now-async constructor.

### 5. E2E test script (`scripts/e2e_glue_athena_test.py`)

New Python script covering both test modes. Notable design decisions:

- **Table name defaults**: `logs`, `traces`, `metrics` — matching the adapter's hardcoded names in `pipeline.rs`
- **Database**: derived from `--namespace` in glue-iceberg mode; isolated `telemetry_iceberg_e2e` in athena-external mode
- **Stale file guard**: `snapshot_parquet()` captures existing files before sending; `wait_for_new_parquet()` waits only for files newer than the snapshot
- **Trace IDs**: single `uuid.uuid4().hex` = 32 hex chars (correct OTLP length)
- **Iceberg DDL**: uses `MAP<STRING, STRING>` (angle-bracket syntax required by Athena for Iceberg)
- **Table creation order** (glue-iceberg): tables created before signals are sent, so the first flush can commit immediately without retrying against missing tables
- **Metrics**: pure Python Prometheus remote-write encoder (snappy + protobuf); no external proto codegen dependency
- **Cleanup**: drops Glue tables and database, deletes S3 objects (athena-external only, since Iceberg metadata is managed by the catalog)

---

## Failure Sequence and Fixes

### Failure 1 — stale Parquet files (athena-external)

`wait_for_parquet()` returned any existing file under the table directory, including leftovers from a previous run. The test passed against old data.

**Fix**: Added `snapshot_parquet()` to record the set of files present before sending, then `wait_for_new_parquet()` polls until at least one file newer than the snapshot appears.

### Failure 2 — wrong table names and database

Script defaulted to `logs_e2e` / `traces_e2e` / `metrics_e2e` and `telemetry_iceberg_e2e`. The adapter hardcodes `logs` / `traces` / `metrics` and the Glue database must match the catalog namespace.

**Fix**: Default table names changed to match adapter; database defaults to `--namespace` in glue-iceberg mode.

### Failure 3 — MAP syntax rejected by Athena Iceberg DDL

`MAP(STRING, STRING)` syntax works for external tables but not for Iceberg `CREATE TABLE` in Athena.

**Fix**: Changed to `MAP<STRING, STRING>` in all Iceberg DDL templates.

### Failure 4 — signals sent before tables created (glue-iceberg)

The adapter tried to commit to a table that did not yet exist in Glue. Each commit attempt failed with "table not found", exhausted retries, and left the manifest permanently pending.

**Fix**: Moved `create_iceberg_tables()` to run before `send_logs / send_traces / send_metrics`.

### Failure 5 — `s3a://` scheme mismatch (main blocker, glue-iceberg)

Described in detail in §3 above. Every commit returned:
```
permanent commit failure: DataInvalid => Invalid s3 url: s3://..., should start with s3a://...
```

First hypothesis was to change data file paths to `s3a://`. This fixed the data file check but the same error then appeared for manifest (`.avro`) and metadata (`.json`) files that the crate derives from the table's internal `location` field — which Athena wrote as `s3://`.

**Root cause**: `configured_scheme: "s3a"` is hardcoded in `GlueCatalogBuilder`'s default storage factory. Any path the catalog touches must match that scheme, including paths read from the Athena-created `metadata.json`.

**Fix**: Inject a custom storage factory via `with_storage_factory()` that uses the scheme from the warehouse URI. With `s3://` warehouse, all paths are consistently `s3://`.

---

## How to Run

### Prerequisites

```bash
pip install boto3 requests python-snappy
cargo build
```

AWS credentials must be available (e.g., `AWS_PROFILE=&lt;your-aws-profile&gt;`).

### athena-external mode

```bash
# Start adapter (noop catalog, local Parquet)
OTLP_ADAPTER_BATCH_MAX_ROWS=10 \
OTLP_ADAPTER_BATCH_MAX_AGE_SECS=5 \
./target/debug/telemetry-iceberg-adapter

# Run test
AWS_PROFILE=&lt;your-aws-profile&gt; python3 scripts/e2e_glue_athena_test.py \
  --mode athena-external \
  --bucket &lt;your-bucket&gt; \
  --athena-output s3://&lt;your-bucket&gt;/athena-results/
```

### glue-iceberg mode

```bash
# Start adapter with Glue catalog and S3 upload
AWS_PROFILE=&lt;your-aws-profile&gt; \
OTLP_ADAPTER_PARQUET_S3_BASE=s3://&lt;your-bucket&gt;/iceberg \
OTLP_ADAPTER_CATALOG_TYPE=glue \
OTLP_ADAPTER_CATALOG_NAMESPACE=telemetry_e2e \
OTLP_ADAPTER_CATALOG_WAREHOUSE=s3://&lt;your-bucket&gt;/iceberg \
OTLP_ADAPTER_BATCH_MAX_ROWS=10 \
OTLP_ADAPTER_BATCH_MAX_AGE_SECS=5 \
./target/debug/telemetry-iceberg-adapter

# Run test
AWS_PROFILE=&lt;your-aws-profile&gt; python3 scripts/e2e_glue_athena_test.py \
  --mode glue-iceberg \
  --bucket &lt;your-bucket&gt; \
  --namespace telemetry_e2e \
  --warehouse s3://&lt;your-bucket&gt;/iceberg \
  --athena-output s3://&lt;your-bucket&gt;/athena-results/
```

---

## Test Results (final passing run)

```
test_id  = e2e-86963f60-85c9-426e-86e7-2d9d0bb1bee7
mode     = glue-iceberg
database = telemetry_e2e

creating Glue Iceberg tables ...
  created Glue Iceberg tables in database 'telemetry_e2e'

sending signals ...
  posted 10 NDJSON log(s)
  posted 10 OTLP trace span(s)
  posted 10 Prometheus metric sample(s)

waiting up to 120s for adapter commits ...
    telemetry_e2e.logs: 10 row(s) matching filter
    telemetry_e2e.traces: 10 row(s) matching filter
    telemetry_e2e.metrics: 10 row(s) matching filter

running assertions ...
  logs  : 10 row(s) ✓
  traces: 10 row(s) ✓
  metrics: 10 row(s) ✓

✓ test passed
```

---

## Notes and Constraints

- `iceberg-catalog-glue` requires the warehouse to use the same URI scheme as table metadata paths. Always use `s3://` (not `s3a://`) for both the warehouse config and `OTLP_ADAPTER_PARQUET_S3_BASE` when working with Athena-created tables. The adapter now handles this correctly via `with_storage_factory`.
- Athena Iceberg DDL rejects `s3a://` in `LOCATION`. Always normalize to `s3://` when constructing DDL.
- The adapter does not auto-create Iceberg tables. Tables must exist in Glue before the first flush. In production, create them via Athena DDL before starting the adapter against a new namespace.
- `batch_max_rows` and `batch_max_age_secs` must be small for fast test turnaround; use `10` rows and `5`s age in testing.

---

## Schema Profile Smoke Tests

Three smoke tests run after adding `schema_profile` to `[tables.logs]` and introducing the `generic_event` profile. All ran against a local noop catalog or Glue, using `cargo build --release`.

---

### Test 1 — Regression: `otel` profile (no projections)

**Purpose**: confirm nothing broke in the default otel log schema.

**Config**:
```toml
[tables.logs]
name = "logs"
schema_profile = "otel"
```

**Payload** (5 events via `POST /v1/logs/ndjson`):
```json
{"timestamp_nanos": 1777437647000000000, "observed_timestamp_nanos": 1777437647000000000,
 "severity_number": 9, "severity_text": "INFO", "body": "regression log 0",
 "service_name": "smoke-test", "resource_attributes": {"deployment.environment": "smoke"},
 "attributes": {"seq": "0", "host": "server-01"}}
```

**Parquet output**:

| Check | Result |
|-------|--------|
| Columns | `observed_timestamp_nanos, timestamp_nanos, severity_number, severity_text, body, trace_id, span_id, service_name, resource_attributes, log_attributes` ✓ |
| `body[0]` | `regression log 0` ✓ |
| `service_name[0]` | `smoke-test` ✓ |
| `severity_text[0]` | `INFO` ✓ |
| `log_attributes[0]` | `{'host': 'server-01', 'seq': '0'}` ✓ |

**Result**: passed. Existing otel schema and column order unchanged.

---

### Test 2 — New profile: `generic_event` with projections

**Purpose**: validate the smaller log schema, field aliases (`message`/`severity`/`hostname`), projections, and "keep the rest in `attributes`" semantics.

**Config**:
```toml
[tables.logs]
name = "generic_logs"
schema_profile = "generic_event"

[[tables.logs.projections]]
column    = "event_type"
source    = "$.event_type"
data_type = "string"

[[tables.logs.projections]]
column    = "src_ip"
source    = "$.src_ip"
data_type = "string"
```

**Payload** (5 events, excerpt of event 0):
```json
{"timestamp": "42", "message": "blocked connection", "severity": "WARN",
 "hostname": "fw01", "event_type": "deny", "src_ip": "10.0.0.5", "country": "AU"}
```

Note: no `attributes` object in the payload — unknown fields (`hostname`, `event_type`, `src_ip`, `country`) are captured as `log_attributes` by `unknown_fields_to_pairs()`, then the profile-specific writer extracts `hostname` to the `hostname` column, removes projected keys, and puts the remainder into `attributes`.

**Parquet output** (all 5 rows):

| message | severity | hostname | event_type | src_ip |
|---------|----------|----------|-----------|--------|
| blocked connection | WARN | fw01 | deny | 10.0.0.5 |
| allowed connection | INFO | fw02 | allow | 10.0.0.9 |
| port scan detected | ERROR | fw01 | deny | 1.2.3.4 |
| ssh login | INFO | web01 | allow | 192.168.1.10 |
| malware drop | ERROR | fw03 | drop | 5.6.7.8 |

**Column and promotion assertions**:

| Check | Result |
|-------|--------|
| Columns = 6 base + 2 projected | `[timestamp_nanos, observed_timestamp_nanos, message, severity, hostname, attributes, event_type, src_ip]` ✓ |
| `message` alias maps from JSON `"message"` | ✓ |
| `severity` alias maps from JSON `"severity"` | ✓ |
| `hostname` extracted from `log_attributes["hostname"]` | ✓ |
| `event_type` promoted to top-level column | ✓ |
| `src_ip` promoted to top-level column | ✓ |
| `attributes[0]` = `{'country': 'AU'}` only | ✓ |
| `event_type` absent from `attributes` | ✓ |
| `src_ip` absent from `attributes` | ✓ |
| `hostname` absent from `attributes` | ✓ |

**Result**: passed.

---

### Test 3 — Glue startup validation for `generic_event`

**Purpose**: confirm `validate_startup` accepts a correctly-shaped Glue Iceberg table and rejects one with a missing column.

#### Part A — correct schema

Glue Iceberg table `telemetry_e2e_generic.generic_logs` created with:

```sql
CREATE TABLE telemetry_e2e_generic.generic_logs (
  timestamp_nanos          BIGINT,
  observed_timestamp_nanos BIGINT,
  message                  STRING,
  severity                 STRING,
  hostname                 STRING,
  attributes               MAP<STRING, STRING>,
  event_type               STRING,
  src_ip                   STRING
)
LOCATION 's3://&lt;your-bucket&gt;/iceberg/telemetry_e2e_generic/generic_logs'
TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet')
```

Adapter startup log:
```
INFO validated Iceberg table schema  table=generic_logs  namespace="telemetry_e2e_generic"
INFO validated Iceberg table schema  table=metrics       namespace="telemetry_e2e_generic"
INFO validated Iceberg table schema  table=traces        namespace="telemetry_e2e_generic"
INFO listening for OTLP gRPC on 0.0.0.0:4317
INFO listening on 0.0.0.0:4318
```

**Result**: adapter started ✓

#### Part B — `message` column omitted

Table `telemetry_e2e_generic.generic_logs_bad` created identically but with `message STRING` removed. Config pointed `tables.logs.name = "generic_logs_bad"`.

Adapter output:
```
Error: validating Iceberg catalog tables

Caused by:
    permanent commit error: Iceberg table `generic_logs_bad` is missing required adapter column `message`
```

Exit code: 1

**Result**: adapter failed fast at startup with a clear schema mismatch error ✓

---

### Schema Profile Summary

| Profile | Base columns | Map column | Notes |
|---------|-------------|------------|-------|
| `otel` (default) | `observed_timestamp_nanos`, `timestamp_nanos`, `severity_number`, `severity_text`, `body`, `trace_id`, `span_id`, `service_name` | `resource_attributes`, `log_attributes` | Full OTLP log semantics |
| `generic_event` | `timestamp_nanos`, `observed_timestamp_nanos`, `message`, `severity`, `hostname` | `attributes` | Smaller schema; `message`/`severity`/`hostname` aliased from common field names; `hostname` also resolved from `resource_attributes["host.name"]` |

Both profiles support `[[tables.logs.projections]]`. Promoted keys are removed from the map column in both cases. `validate_startup` enforces the base columns for the configured profile against the live Iceberg table schema.
