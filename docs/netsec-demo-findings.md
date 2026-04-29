# Network Security Log Demo — Findings

End-to-end trace of 12 simulated firewall events through the adapter: NDJSON ingest → Parquet flush → S3 upload → Glue external table → Athena queries.

> **Mode**: `athena-external`. The test script uploads Parquet to S3 and creates a Glue external table. The adapter's glue-iceberg mode (direct S3 write + Iceberg commit) was **not** exercised here. See [Glue-Iceberg Projection Validation](#glue-iceberg-projection-validation) below for that result.

---

## Schema

### Projected columns (top-level Parquet / Athena columns)

These fields are extracted from `log_attributes` by the adapter and written as dedicated Parquet columns. They are **not** duplicated in the map.

| Column | Type | Source (TOML) | Description |
|--------|------|---------------|-------------|
| `src_ip` | STRING | `$.src_ip` | Source IP address |
| `dst_ip` | STRING | `$.dst_ip` | Destination IP address |
| `dst_port` | INT | `$.dst_port` | Destination port number |
| `protocol` | STRING | `$.protocol` | TCP / UDP / ICMP |
| `action` | STRING | `$.action` | Firewall verdict: ALLOW / DENY / DROP |
| `bytes_sent` | BIGINT | `$.bytes_sent` | Bytes sent by the initiator |

### Standard log columns (always present)

| Column | Type | Description |
|--------|------|-------------|
| `observed_timestamp_nanos` | BIGINT | Observed timestamp from the sender; for NDJSON the adapter uses the field if present, otherwise defaults to 0 (the adapter does not auto-stamp on NDJSON ingestion — the demo script sets this explicitly) |
| `timestamp_nanos` | BIGINT | Event timestamp from the source |
| `severity_number` | INT | OTLP severity number (9=INFO, 13=WARN, 17=ERROR) |
| `severity_text` | STRING | INFO / WARN / ERROR |
| `body` | STRING | Human-readable one-liner summary |
| `service_name` | STRING | `firewall-sensor` |
| `resource_attributes` | MAP\<STRING,STRING\> | `service.name`, `deployment.environment`, `sensor.id` |
| `log_attributes` | MAP\<STRING,STRING\> | Remaining attributes (see below) |

### Remaining in `log_attributes` (not projected)

| Key | Example values |
|-----|---------------|
| `src_port` | `54321`, `61234`, … |
| `bytes_recv` | `52480`, `0`, … |
| `duration_ms` | `142`, `34200`, `0`, … |
| `country` | `US`, `RU`, `CN`, `internal` |
| `threat_cat` | `brute_force`, `port_scan`, `c2_beacon`, `reconnaissance` |

### Adapter config (`/tmp/netsec-demo/adapter-netsec.toml`)

```toml
[tables.logs]
name = "logs"

[[tables.logs.projections]]
column    = "src_ip"
source    = "$.src_ip"
data_type = "string"

[[tables.logs.projections]]
column    = "dst_ip"
source    = "$.dst_ip"
data_type = "string"

[[tables.logs.projections]]
column    = "dst_port"
source    = "$.dst_port"
data_type = "int"

[[tables.logs.projections]]
column    = "protocol"
source    = "$.protocol"
data_type = "string"

[[tables.logs.projections]]
column    = "action"
source    = "$.action"
data_type = "string"

[[tables.logs.projections]]
column    = "bytes_sent"
source    = "$.bytes_sent"
data_type = "long"
```

---

## Input Logs (12 events sent)

Sent as NDJSON to `POST /v1/logs/ndjson`. Each line is one JSON object.

### Severity rule applied by the demo script

| Condition | severity_number | severity_text |
|-----------|----------------|---------------|
| action = ALLOW | 9 | INFO |
| action = DENY or DROP, no threat_cat | 13 | WARN |
| action = DENY or DROP, threat_cat present | 17 | ERROR |

### Event 01 — Normal outbound HTTPS

```json
{
  "timestamp_nanos": 1777434873000000000,
  "severity_number": 9,
  "severity_text": "INFO",
  "body": "ALLOW TCP 192.168.1.10:54321 -> 93.184.216.34:443",
  "service_name": "firewall-sensor",
  "resource_attributes": {
    "service.name": "firewall-sensor",
    "deployment.environment": "production",
    "sensor.id": "fw-edge-01"
  },
  "attributes": {
    "src_ip": "192.168.1.10",
    "dst_ip": "93.184.216.34",
    "src_port": "54321",
    "dst_port": "443",
    "protocol": "TCP",
    "action": "ALLOW",
    "bytes_sent": "1240",
    "bytes_recv": "52480",
    "duration_ms": "142",
    "country": "US"
  }
}
```

### Event 02 — Normal outbound HTTPS

```json
{
  "severity_text": "INFO",
  "body": "ALLOW TCP 192.168.1.15:61234 -> 104.26.10.72:443",
  "attributes": {
    "src_ip": "192.168.1.15", "dst_ip": "104.26.10.72",
    "src_port": "61234", "dst_port": "443", "protocol": "TCP",
    "action": "ALLOW", "bytes_sent": "800", "bytes_recv": "12300",
    "duration_ms": "89", "country": "US"
  }
}
```

### Event 03 — Internal SSH session

```json
{
  "severity_text": "INFO",
  "body": "ALLOW TCP 10.0.0.5:45000 -> 192.168.1.1:22",
  "attributes": {
    "src_ip": "10.0.0.5", "dst_ip": "192.168.1.1",
    "src_port": "45000", "dst_port": "22", "protocol": "TCP",
    "action": "ALLOW", "bytes_sent": "2048", "bytes_recv": "1024",
    "duration_ms": "5200", "country": "internal"
  }
}
```

### Event 04 — Brute-force SSH from external IP (DENY / ERROR)

```json
{
  "severity_text": "ERROR",
  "body": "DENY TCP 185.220.101.42:42001 -> 10.0.0.100:22",
  "attributes": {
    "src_ip": "185.220.101.42", "dst_ip": "10.0.0.100",
    "src_port": "42001", "dst_port": "22", "protocol": "TCP",
    "action": "DENY", "bytes_sent": "60", "bytes_recv": "0",
    "duration_ms": "1", "country": "RU", "threat_cat": "brute_force"
  }
}
```

### Event 05 — Port scan, Telnet probe (DENY / ERROR)

```json
{
  "severity_text": "ERROR",
  "body": "DENY TCP 185.220.101.42:42002 -> 10.0.0.100:23",
  "attributes": {
    "src_ip": "185.220.101.42", "dst_ip": "10.0.0.100",
    "src_port": "42002", "dst_port": "23", "protocol": "TCP",
    "action": "DENY", "bytes_sent": "60", "bytes_recv": "0",
    "duration_ms": "0", "country": "RU", "threat_cat": "port_scan"
  }
}
```

### Event 06 — Port scan, RDP probe (DENY / ERROR)

```json
{
  "severity_text": "ERROR",
  "body": "DENY TCP 185.220.101.42:42003 -> 10.0.0.100:3389",
  "attributes": {
    "src_ip": "185.220.101.42", "dst_ip": "10.0.0.100",
    "src_port": "42003", "dst_port": "3389", "protocol": "TCP",
    "action": "DENY", "bytes_sent": "60", "bytes_recv": "0",
    "duration_ms": "0", "country": "RU", "threat_cat": "port_scan"
  }
}
```

### Event 07 — Potential data exfiltration to CN, 98 MB (ALLOW / INFO)

```json
{
  "severity_text": "INFO",
  "body": "ALLOW TCP 192.168.1.20:55001 -> 203.0.113.50:443",
  "attributes": {
    "src_ip": "192.168.1.20", "dst_ip": "203.0.113.50",
    "src_port": "55001", "dst_port": "443", "protocol": "TCP",
    "action": "ALLOW", "bytes_sent": "98304000", "bytes_recv": "1200",
    "duration_ms": "34200", "country": "CN"
  }
}
```

### Event 08 — DNS query (ALLOW / INFO)

```json
{
  "severity_text": "INFO",
  "body": "ALLOW UDP 192.168.1.10:53241 -> 8.8.8.8:53",
  "attributes": {
    "src_ip": "192.168.1.10", "dst_ip": "8.8.8.8",
    "src_port": "53241", "dst_port": "53", "protocol": "UDP",
    "action": "ALLOW", "bytes_sent": "68", "bytes_recv": "120",
    "duration_ms": "12", "country": "US"
  }
}
```

### Event 09 — Internal Postgres connection (ALLOW / INFO)

```json
{
  "severity_text": "INFO",
  "body": "ALLOW TCP 10.0.0.5:49001 -> 10.0.0.20:5432",
  "attributes": {
    "src_ip": "10.0.0.5", "dst_ip": "10.0.0.20",
    "src_port": "49001", "dst_port": "5432", "protocol": "TCP",
    "action": "ALLOW", "bytes_sent": "4096", "bytes_recv": "16384",
    "duration_ms": "45", "country": "internal"
  }
}
```

### Event 10 — C2 beacon on port 4444 (DROP / ERROR)

```json
{
  "severity_text": "ERROR",
  "body": "DROP TCP 172.16.0.50:33444 -> 198.51.100.25:4444",
  "attributes": {
    "src_ip": "172.16.0.50", "dst_ip": "198.51.100.25",
    "src_port": "33444", "dst_port": "4444", "protocol": "TCP",
    "action": "DROP", "bytes_sent": "512", "bytes_recv": "0",
    "duration_ms": "2", "country": "US", "threat_cat": "c2_beacon"
  }
}
```

### Event 11 — Outbound HTTP (ALLOW / INFO)

```json
{
  "severity_text": "INFO",
  "body": "ALLOW TCP 192.168.1.30:61000 -> 91.108.4.1:80",
  "attributes": {
    "src_ip": "192.168.1.30", "dst_ip": "91.108.4.1",
    "src_port": "61000", "dst_port": "80", "protocol": "TCP",
    "action": "ALLOW", "bytes_sent": "340", "bytes_recv": "8200",
    "duration_ms": "67", "country": "NL"
  }
}
```

### Event 12 — ICMP ping sweep blocked (DENY / ERROR)

```json
{
  "severity_text": "ERROR",
  "body": "DENY ICMP 10.10.10.1:0 -> 10.0.0.0:0",
  "attributes": {
    "src_ip": "10.10.10.1", "dst_ip": "10.0.0.0",
    "src_port": "0", "dst_port": "0", "protocol": "ICMP",
    "action": "DENY", "bytes_sent": "32", "bytes_recv": "0",
    "duration_ms": "0", "country": "internal", "threat_cat": "reconnaissance"
  }
}
```

---

## Glue External Table DDL

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS telemetry_netsec_demo.firewall_logs (
  observed_timestamp_nanos BIGINT,
  timestamp_nanos          BIGINT,
  severity_number          INT,
  severity_text            STRING,
  body                     STRING,
  trace_id                 STRING,
  span_id                  STRING,
  service_name             STRING,
  resource_attributes      MAP<STRING, STRING>,
  log_attributes           MAP<STRING, STRING>,
  src_ip                   STRING,
  dst_ip                   STRING,
  dst_port                 INT,
  protocol                 STRING,
  action                   STRING,
  bytes_sent               BIGINT
)
STORED AS PARQUET
LOCATION 's3://&lt;your-bucket&gt;/netsec-demo/logs/'
```

---

## Queries and Results

### Query 1 — Traffic summary by action

```sql
SELECT
    action,
    count(*)                          AS connections,
    sum(bytes_sent)                   AS total_bytes_sent,
    count_if(severity_text = 'ERROR') AS high_severity
FROM telemetry_netsec_demo.firewall_logs
GROUP BY action
ORDER BY connections DESC
```

**Result:**

| action | connections | total_bytes_sent | high_severity |
|--------|-------------|-----------------|---------------|
| ALLOW  | 7           | 98,312,592      | 0             |
| DENY   | 4           | 212             | 4             |
| DROP   | 1           | 512             | 1             |

**Observations:**
- 7 connections were allowed, 5 blocked (4 DENY + 1 DROP).
- ALLOW traffic accounts for 98 MB of bytes sent, but nearly all of it (98,304,000 bytes) is the single suspicious upload to CN (event 07). Normal allowed traffic is only ~8 KB total.
- Every DENY and the DROP carry `severity_text = 'ERROR'`, meaning all had a `threat_cat` set — no "plain" blocks in this dataset.

---

### Query 2 — Blocked connections with threat context

```sql
SELECT
    src_ip,
    dst_ip,
    dst_port,
    protocol,
    action,
    severity_text,
    log_attributes['country']    AS country,
    log_attributes['threat_cat'] AS threat_cat,
    body
FROM telemetry_netsec_demo.firewall_logs
WHERE action IN ('DENY', 'DROP')
ORDER BY severity_text DESC, src_ip
```

**Result:**

| src_ip | dst_ip | dst_port | protocol | action | severity_text | country | threat_cat | body |
|--------|--------|----------|----------|--------|---------------|---------|------------|------|
| 10.10.10.1 | 10.0.0.0 | 0 | ICMP | DENY | ERROR | internal | reconnaissance | DENY ICMP 10.10.10.1:0 -> 10.0.0.0:0 |
| 172.16.0.50 | 198.51.100.25 | 4444 | TCP | DROP | ERROR | US | c2_beacon | DROP TCP 172.16.0.50:33444 -> 198.51.100.25:4444 |
| 185.220.101.42 | 10.0.0.100 | 22 | TCP | DENY | ERROR | RU | brute_force | DENY TCP 185.220.101.42:42001 -> 10.0.0.100:22 |
| 185.220.101.42 | 10.0.0.100 | 23 | TCP | DENY | ERROR | RU | port_scan | DENY TCP 185.220.101.42:42002 -> 10.0.0.100:23 |
| 185.220.101.42 | 10.0.0.100 | 3389 | TCP | DENY | ERROR | RU | port_scan | DENY TCP 185.220.101.42:42003 -> 10.0.0.100:3389 |

**Observations:**
- Three distinct threat actors / patterns: internal ICMP sweep, a C2 beacon from an internal host, and an external IP from RU probing SSH/Telnet/RDP.
- `threat_cat` is queryable even though it was not projected — it lives in `log_attributes` and is accessible via `log_attributes['threat_cat']`.
- The `body` column gives a readable one-liner of each event without needing to join back to a raw log store.

---

### Query 3 — Potential data exfiltration (bytes_sent > 1 MB)

```sql
SELECT
    src_ip,
    dst_ip,
    dst_port,
    protocol,
    bytes_sent,
    log_attributes['duration_ms'] AS duration_ms,
    log_attributes['country']     AS country,
    body
FROM telemetry_netsec_demo.firewall_logs
WHERE bytes_sent > 1000000
ORDER BY bytes_sent DESC
```

**Result:**

| src_ip | dst_ip | dst_port | protocol | bytes_sent | duration_ms | country | body |
|--------|--------|----------|----------|-----------|-------------|---------|------|
| 192.168.1.20 | 203.0.113.50 | 443 | TCP | 98,304,000 | 34200 | CN | ALLOW TCP 192.168.1.20:55001 -> 203.0.113.50:443 |

**Observations:**
- Only one event exceeds 1 MB — `192.168.1.20` sent ~94 MB to `203.0.113.50` (CN) in 34 seconds on port 443.
- The connection was **allowed** (no firewall block). This is the kind of event that would require a follow-up investigation — the firewall passed it, but the volume and destination are anomalous.
- `duration_ms` and `country` are read from `log_attributes` at query time, demonstrating that non-projected fields are still fully accessible.

---

### Query 4 — Repeated attacker: source IP with multiple denials

```sql
SELECT
    src_ip,
    log_attributes['country']    AS country,
    count(*)                     AS blocked_attempts,
    min(dst_port)                AS first_port,
    max(dst_port)                AS last_port,
    array_join(array_agg(CAST(dst_port AS VARCHAR)), ', ') AS ports_tried
FROM telemetry_netsec_demo.firewall_logs
WHERE action IN ('DENY', 'DROP')
GROUP BY src_ip, log_attributes['country']
HAVING count(*) > 1
ORDER BY blocked_attempts DESC
```

**Result:**

| src_ip | country | blocked_attempts | first_port | last_port | ports_tried |
|--------|---------|-----------------|-----------|----------|-------------|
| 185.220.101.42 | RU | 3 | 22 | 3389 | 22, 23, 3389 |

**Observations:**
- `185.220.101.42` (RU) is the only source with more than one block, hitting ports 22 (SSH), 23 (Telnet), and 3389 (RDP) — a classic sequential port scan targeting remote access services.
- The query uses `dst_port` as a first-class integer column (projected), enabling `min()`, `max()`, and `array_agg()`. If `dst_port` were still a string inside `log_attributes`, the `CAST` and aggregation would still work but range comparisons and sorting would be character-order not numeric.
- `array_join` + `array_agg` works in Athena (Trino engine) and produces the comma-separated port list inline.

---

### Query 5 — Confirm column promotion: projected vs log_attributes

```sql
SELECT
    src_ip,
    dst_port,
    action,
    log_attributes['src_ip']   AS src_ip_in_attrs,
    log_attributes['src_port'] AS src_port_in_attrs,
    log_attributes['country']  AS country
FROM telemetry_netsec_demo.firewall_logs
LIMIT 5
```

**Result:**

| src_ip | dst_port | action | src_ip_in_attrs | src_port_in_attrs | country |
|--------|----------|--------|-----------------|-------------------|---------|
| 192.168.1.15 | 443 | ALLOW | *(null)* | 61234 | US |
| 10.0.0.5 | 22 | ALLOW | *(null)* | 45000 | internal |
| 192.168.1.10 | 443 | ALLOW | *(null)* | 54321 | US |
| 185.220.101.42 | 22 | DENY | *(null)* | 42001 | RU |
| 185.220.101.42 | 23 | DENY | *(null)* | 42002 | RU |

**Observations:**
- `src_ip_in_attrs` is NULL for every row — the adapter removed `src_ip` from the map when it promoted it to a top-level column. No duplication.
- `src_port_in_attrs` has values — it was not projected so it remains in `log_attributes`.
- `country` has values — also not projected, also remains in `log_attributes`.
- This confirms the promotion semantics: projected keys are extracted and removed from the map; everything else is preserved as-is.

---

## Pipeline Summary

```
NDJSON (12 events, 7,120 bytes)
    │
    ▼  POST /v1/logs/ndjson
adapter (WAL → batch → Parquet writer with projections)
    │  flush at batch_max_rows=12 or batch_max_age_secs=5
    ▼
Parquet file (7,480 bytes, 16 columns = 10 base + 6 projected)
    │
    ▼  test script upload
S3 s3://&lt;your-bucket&gt;/netsec-demo/logs/
    │
    ▼  Athena CREATE EXTERNAL TABLE
Glue catalog (telemetry_netsec_demo.firewall_logs)
    │
    ▼  Athena SELECT queries
Results shown above
```

---

## Glue-Iceberg Projection Validation

Run separately to confirm that log column projections work through the full Iceberg commit path (adapter → S3 → Glue Iceberg catalog → Athena), not just the athena-external path used in the netsec demo above.

### Setup

**Adapter config** (`/tmp/adapter-glue-iceberg-proj.toml`):

```toml
[server]
http_bind = "0.0.0.0:4318"
grpc_bind = "0.0.0.0:4317"

[wal]
backend = "local"
dir = "/tmp/adapter-glue-proj/wal"

[storage]
parquet_dir = "/tmp/adapter-glue-proj/parquet"
parquet_s3_base = "s3://&lt;your-bucket&gt;/iceberg"

[batch]
max_rows = 10
max_age_secs = 15

[catalog]
type = "glue"
namespace = "telemetry_e2e_proj"
warehouse = "s3://&lt;your-bucket&gt;/iceberg"

[tables.logs]
name = "logs"

[[tables.logs.projections]]
column    = "src_ip"
source    = "$.src_ip"
data_type = "string"

[[tables.logs.projections]]
column    = "bytes"
source    = "$.bytes"
data_type = "long"
```

**Pre-create tables** (required before first adapter start — `validate_startup` checks that each configured table exists in Glue with the correct schema):

```sql
CREATE DATABASE IF NOT EXISTS telemetry_e2e_proj;

CREATE TABLE IF NOT EXISTS telemetry_e2e_proj.logs (
  observed_timestamp_nanos BIGINT,
  timestamp_nanos          BIGINT,
  severity_number          INT,
  severity_text            STRING,
  body                     STRING,
  trace_id                 STRING,
  span_id                  STRING,
  service_name             STRING,
  resource_attributes      MAP<STRING, STRING>,
  log_attributes           MAP<STRING, STRING>,
  src_ip                   STRING,
  bytes                    BIGINT
)
LOCATION 's3://&lt;your-bucket&gt;/iceberg/telemetry_e2e_proj/logs'
TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet');
```

**Adapter startup log** — schema validation passed for all three tables:

```
INFO validated Iceberg table schema  table=logs    namespace="telemetry_e2e_proj"
INFO validated Iceberg table schema  table=metrics namespace="telemetry_e2e_proj"
INFO validated Iceberg table schema  table=traces  namespace="telemetry_e2e_proj"
INFO listening for OTLP gRPC on 0.0.0.0:4317
INFO listening on 0.0.0.0:4318
```

**Test command**:

```bash
AWS_PROFILE=&lt;your-aws-profile&gt; python3 scripts/e2e_glue_athena_test.py \
  --mode glue-iceberg \
  --bucket &lt;your-bucket&gt; \
  --warehouse s3://&lt;your-bucket&gt;/iceberg \
  --namespace telemetry_e2e_proj \
  --athena-output s3://&lt;your-bucket&gt;/athena-results/ \
  --with-projections \
  --wait-seconds 120
```

### Result

10 logs, 10 traces, and 10 metrics committed and visible in Athena within ~20 seconds.

```
waiting up to 120s for adapter commits ...
    telemetry_e2e_proj.logs: 0 row(s) matching filter
    telemetry_e2e_proj.logs: 0 row(s) matching filter
    telemetry_e2e_proj.logs: 10 row(s) matching filter
    telemetry_e2e_proj.traces: 10 row(s) matching filter
    telemetry_e2e_proj.metrics: 10 row(s) matching filter

running assertions ...
  logs  : 10 row(s) ✓
  traces: 10 row(s) ✓
  metrics: 10 row(s) ✓
  projection `src_ip` is non-null ✓  (sample: 10.0.0.1)
  projection `bytes` is non-null ✓  (sample: 1024)
  `log_attributes['src_ip']` is NULL (promoted out) ✓
  `log_attributes['bytes']` is NULL (promoted out) ✓
  non-promoted `log_attributes['e2e.sequence']` still present ✓

✓ test passed  (test_id=e2e-a9ecdb7d-d2da-4233-8362-4e84868e5e97)
```

### Assertions

| Check | Result |
|-------|--------|
| `src_ip` top-level column is non-null | ✓ |
| `bytes` top-level column is non-null | ✓ |
| `log_attributes['src_ip']` is NULL (promoted out, not duplicated) | ✓ |
| `log_attributes['bytes']` is NULL (promoted out, not duplicated) | ✓ |
| Non-promoted key `log_attributes['e2e.sequence']` still present | ✓ |
| Iceberg commit visible in Athena within 120s | ✓ (~20s actual) |

### Notes

- Glue Iceberg tables must exist before the adapter starts. `validate_startup` loads each table from the catalog at boot and fails if any are missing or have a schema mismatch. Create them with the DDL above (or via `create_iceberg_tables()` in the test script) as part of the deployment runbook.
- The `s3://` vs `s3a://` scheme fix in `src/commit.rs` is what makes glue-iceberg mode work: a custom `OpenDalStorageFactory::S3 { configured_scheme: "s3" }` is injected so the adapter, Glue, and Athena all use `s3://` consistently.
