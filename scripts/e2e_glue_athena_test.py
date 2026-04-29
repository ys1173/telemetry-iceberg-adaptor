#!/usr/bin/env python3
"""
End-to-end AWS test for telemetry-iceberg-adapter: logs, traces, and metrics.

Two modes
---------
athena-external (default)
    Adapter runs with catalog.type=noop.  After the adapter flushes Parquet
    files to local disk, the script uploads them to S3 and creates Glue
    external tables so Athena can query them.  Good for quickly validating
    the decode / schema / Parquet path without needing a live Glue catalog.

glue-iceberg
    Adapter runs with catalog.type=glue and OTLP_ADAPTER_PARQUET_S3_BASE set.
    The adapter writes Parquet directly to S3 and commits each batch to the
    Glue Iceberg catalog.  The script pre-creates the Glue Iceberg tables via
    Athena DDL, then waits for the adapter commits and queries the results.

Prerequisites
-------------
    pip install boto3 requests python-snappy

For athena-external, start the adapter with a short flush interval:
    OTLP_ADAPTER_BATCH_MAX_AGE_SECS=10 cargo run

For glue-iceberg, start the adapter with:
    OTLP_ADAPTER_CATALOG_TYPE=glue \\
    OTLP_ADAPTER_CATALOG_WAREHOUSE=s3://my-bucket/iceberg \\
    OTLP_ADAPTER_CATALOG_NAMESPACE=telemetry_e2e \\
    OTLP_ADAPTER_PARQUET_S3_BASE=s3://my-bucket/iceberg \\
    OTLP_ADAPTER_BATCH_MAX_AGE_SECS=10 \\
    cargo run

Example – athena-external:
    python scripts/e2e_glue_athena_test.py \\
      --bucket my-bucket \\
      --athena-output s3://my-bucket/athena-results/

Example – glue-iceberg:
    python scripts/e2e_glue_athena_test.py \\
      --mode glue-iceberg \\
      --bucket my-bucket \\
      --warehouse s3://my-bucket/iceberg \\
      --namespace telemetry_e2e \\
      --athena-output s3://my-bucket/athena-results/
"""

from __future__ import annotations

import argparse
import json
import struct
import sys
import time
import uuid
from pathlib import Path

import boto3
import requests

try:
    import snappy
    HAS_SNAPPY = True
except ImportError:
    HAS_SNAPPY = False


# ---------------------------------------------------------------------------
# Athena DDL templates
# ---------------------------------------------------------------------------

_LOG_EXTERNAL_DDL_BASE = """
CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table} (
  observed_timestamp_nanos BIGINT,
  timestamp_nanos          BIGINT,
  severity_number          INT,
  severity_text            STRING,
  body                     STRING,
  trace_id                 STRING,
  span_id                  STRING,
  service_name             STRING,
  resource_attributes      MAP<STRING, STRING>,
  log_attributes           MAP<STRING, STRING>{extra_cols}
)
STORED AS PARQUET
LOCATION '{location}'
"""

_LOG_EXTERNAL_DDL = _LOG_EXTERNAL_DDL_BASE  # alias for the no-projection path

_TRACE_EXTERNAL_DDL = """
CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table} (
  trace_id             STRING,
  span_id              STRING,
  parent_span_id       STRING,
  trace_state          STRING,
  name                 STRING,
  kind                 INT,
  start_time_unix_nano BIGINT,
  end_time_unix_nano   BIGINT,
  duration_nanos       BIGINT,
  status_code          INT,
  status_message       STRING,
  service_name         STRING,
  resource_attributes  MAP<STRING, STRING>,
  span_attributes      MAP<STRING, STRING>,
  event_count          INT,
  link_count           INT
)
STORED AS PARQUET
LOCATION '{location}'
"""

_METRIC_EXTERNAL_DDL = """
CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table} (
  metric_name      STRING,
  timestamp_millis BIGINT,
  value            DOUBLE,
  labels           MAP<STRING, STRING>
)
STORED AS PARQUET
LOCATION '{location}'
"""

_LOG_ICEBERG_DDL_BASE = """
CREATE TABLE IF NOT EXISTS {db}.{table} (
  observed_timestamp_nanos BIGINT,
  timestamp_nanos          BIGINT,
  severity_number          INT,
  severity_text            STRING,
  body                     STRING,
  trace_id                 STRING,
  span_id                  STRING,
  service_name             STRING,
  resource_attributes      MAP<STRING, STRING>,
  log_attributes           MAP<STRING, STRING>{extra_cols}
)
LOCATION '{location}'
TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet')
"""

_LOG_ICEBERG_DDL = _LOG_ICEBERG_DDL_BASE  # alias for the no-projection path


# Maps ProjectionDataType names (from TOML) to Athena/Glue SQL types.
_PROJECTION_SQL_TYPES = {
    "string": "STRING",
    "int":    "INT",
    "long":   "BIGINT",
    "double": "DOUBLE",
}

# Hardcoded projections used by the projection smoke-test.
_SMOKE_PROJECTIONS = [
    {"column": "src_ip", "data_type": "string"},
    {"column": "bytes",  "data_type": "long"},
]


def _extra_cols_ddl(projections: list[dict]) -> str:
    """Return the extra column fragment to append inside the DDL column list."""
    if not projections:
        return ""
    parts = [
        f"  {p['column']} {_PROJECTION_SQL_TYPES[p['data_type']]}"
        for p in projections
    ]
    return ",\n" + ",\n".join(parts)

_TRACE_ICEBERG_DDL = """
CREATE TABLE IF NOT EXISTS {db}.{table} (
  trace_id             STRING,
  span_id              STRING,
  parent_span_id       STRING,
  trace_state          STRING,
  name                 STRING,
  kind                 INT,
  start_time_unix_nano BIGINT,
  end_time_unix_nano   BIGINT,
  duration_nanos       BIGINT,
  status_code          INT,
  status_message       STRING,
  service_name         STRING,
  resource_attributes  MAP<STRING, STRING>,
  span_attributes      MAP<STRING, STRING>,
  event_count          INT,
  link_count           INT
)
LOCATION '{location}'
TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet')
"""

_METRIC_ICEBERG_DDL = """
CREATE TABLE IF NOT EXISTS {db}.{table} (
  metric_name      STRING,
  timestamp_millis BIGINT,
  value            DOUBLE,
  labels           MAP<STRING, STRING>
)
LOCATION '{location}'
TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet')
"""


# ---------------------------------------------------------------------------
# Minimal pure-Python Prometheus WriteRequest protobuf encoder
# (matches the prost structs in prometheus.rs)
# ---------------------------------------------------------------------------

def _varint(n: int) -> bytes:
    result = b""
    while n > 0x7F:
        result += bytes([(n & 0x7F) | 0x80])
        n >>= 7
    return result + bytes([n & 0x7F])


def _len_field(number: int, data: bytes) -> bytes:
    return _varint((number << 3) | 2) + _varint(len(data)) + data


def _str_field(number: int, s: str) -> bytes:
    return _len_field(number, s.encode())


def _dbl_field(number: int, v: float) -> bytes:
    # wire type 1 = 64-bit little-endian
    return _varint((number << 3) | 1) + struct.pack("<d", v)


def _i64_field(number: int, v: int) -> bytes:
    if v < 0:
        v += 1 << 64
    return _varint((number << 3) | 0) + _varint(v)


def _build_write_request(series: list[dict]) -> bytes:
    """
    series: [{"labels": {"__name__": ..., key: val, ...},
              "samples": [(value, timestamp_ms), ...]}]
    """
    out = b""
    for s in series:
        ts = b""
        for name, val in s["labels"].items():
            ts += _len_field(1, _str_field(1, name) + _str_field(2, val))
        for value, ts_ms in s["samples"]:
            ts += _len_field(2, _dbl_field(1, value) + _i64_field(2, ts_ms))
        out += _len_field(1, ts)
    return out


# ---------------------------------------------------------------------------
# Signal senders
# ---------------------------------------------------------------------------

def send_logs(
    adapter_url: str,
    test_id: str,
    count: int,
    extra_attributes: dict | None = None,
) -> None:
    now_nanos = int(time.time() * 1_000_000_000)
    lines = [
        json.dumps({
            "timestamp_nanos": now_nanos + i,
            "observed_timestamp_nanos": now_nanos + i,
            "severity_number": 9,
            "severity_text": "INFO",
            "body": f"e2e test log {i}",
            "service_name": "telemetry-iceberg-e2e",
            "resource_attributes": {"deployment.environment": "e2e"},
            "attributes": {
                "e2e.test_id": test_id,
                "e2e.sequence": str(i),
                **(extra_attributes or {}),
            },
        })
        for i in range(count)
    ]
    resp = requests.post(
        f"{adapter_url.rstrip('/')}/v1/logs/ndjson",
        data="\n".join(lines).encode(),
        headers={"content-type": "application/x-ndjson"},
        timeout=15,
    )
    resp.raise_for_status()
    print(f"  posted {count} NDJSON log(s)")


def send_traces(adapter_url: str, test_id: str, count: int) -> None:
    now_nanos = int(time.time() * 1_000_000_000)
    spans = [
        {
            "traceId": uuid.uuid4().hex,   # 32 hex chars = 16 bytes
            "spanId": uuid.uuid4().hex[:16],  # 16 hex chars = 8 bytes
            "name": f"e2e-span-{i}",
            "kind": 1,
            "startTimeUnixNano": str(now_nanos + i * 1_000_000),
            "endTimeUnixNano": str(now_nanos + i * 1_000_000 + 500_000),
            "attributes": [
                {"key": "e2e.test_id", "value": {"stringValue": test_id}},
                {"key": "e2e.sequence", "value": {"stringValue": str(i)}},
            ],
            "status": {},
        }
        for i in range(count)
    ]
    payload = {
        "resourceSpans": [{
            "resource": {
                "attributes": [
                    {"key": "service.name", "value": {"stringValue": "telemetry-iceberg-e2e"}},
                    {"key": "deployment.environment", "value": {"stringValue": "e2e"}},
                ]
            },
            "scopeSpans": [{"scope": {"name": "e2e-test"}, "spans": spans}],
        }]
    }
    resp = requests.post(
        f"{adapter_url.rstrip('/')}/v1/traces",
        json=payload,
        headers={"content-type": "application/json"},
        timeout=15,
    )
    resp.raise_for_status()
    print(f"  posted {count} OTLP trace span(s)")


def send_metrics(adapter_url: str, test_id: str, count: int) -> bool:
    if not HAS_SNAPPY:
        print("  WARNING: python-snappy not installed – skipping metrics (pip install python-snappy)")
        return False

    now_ms = int(time.time() * 1000)
    series = [
        {
            "labels": {
                "__name__": "e2e_test_counter",
                "e2e_test_id": test_id,
                "instance": f"host-{i}",
            },
            "samples": [(float(i + 1), now_ms + i)],
        }
        for i in range(count)
    ]
    proto_bytes = _build_write_request(series)
    # snappy.compress() calls snappy::Compress() – raw format, compatible with
    # snap::raw::Decoder used by the adapter.
    compressed = snappy.compress(proto_bytes)
    resp = requests.post(
        f"{adapter_url.rstrip('/')}/api/v1/write",
        data=compressed,
        headers={"content-type": "application/x-protobuf"},
        timeout=15,
    )
    resp.raise_for_status()
    print(f"  posted {count} Prometheus metric sample(s)")
    return True


# ---------------------------------------------------------------------------
# Parquet file waiting (athena-external mode)
# ---------------------------------------------------------------------------

def snapshot_parquet(parquet_dir: Path, tables: list[str]) -> dict[str, set[Path]]:
    """Record which Parquet files already exist before the test run."""
    return {t: set((parquet_dir / t).glob("*.parquet")) for t in tables}


def wait_for_new_parquet(
    table_dir: Path, existing: set[Path], wait_seconds: int
) -> list[Path]:
    """Return only files created after the snapshot, blocking until at least one appears."""
    deadline = time.time() + wait_seconds
    while time.time() < deadline:
        new_files = sorted(set(table_dir.glob("*.parquet")) - existing)
        if new_files:
            return new_files
        time.sleep(2)
    raise TimeoutError(
        f"no new parquet files appeared in {table_dir} within {wait_seconds}s"
    )


# ---------------------------------------------------------------------------
# S3 upload (athena-external mode)
# ---------------------------------------------------------------------------

def upload_table(s3, bucket: str, s3_prefix: str, table: str, files: list[Path]) -> str:
    prefix = f"{s3_prefix.strip('/')}/{table}"
    for f in files:
        key = f"{prefix}/{f.name}"
        s3.upload_file(str(f), bucket, key)
        print(f"    uploaded {f.name} → s3://{bucket}/{key}")
    return f"s3://{bucket}/{prefix}/"


# ---------------------------------------------------------------------------
# Athena helpers
# ---------------------------------------------------------------------------

def run_athena(athena, query: str, args, fetch: bool = False) -> list[list[str]]:
    req: dict = {
        "QueryString": query,
        "ResultConfiguration": {"OutputLocation": args.athena_output},
    }
    if args.workgroup:
        req["WorkGroup"] = args.workgroup

    qid = athena.start_query_execution(**req)["QueryExecutionId"]
    while True:
        execution = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = execution["Status"]["State"]
        if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break
        time.sleep(2)

    if state != "SUCCEEDED":
        reason = execution["Status"].get("StateChangeReason", "unknown")
        raise RuntimeError(
            f"Athena query {qid} {state}: {reason}\nSQL: {query.strip()[:300]}"
        )

    if not fetch:
        return []

    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    return [
        [cell.get("VarCharValue", "") for cell in row.get("Data", [])]
        for row in rows
    ]


def athena_count(athena, db: str, table: str, where: str, args) -> int:
    rows = run_athena(
        athena,
        f"SELECT count(*) FROM {db}.{table} WHERE {where}",
        args,
        fetch=True,
    )
    return int(rows[1][0]) if len(rows) > 1 else 0


def wait_for_rows(
    athena, db: str, table: str, where: str, want: int, wait_seconds: int, args
) -> int:
    deadline = time.time() + wait_seconds
    while time.time() < deadline:
        try:
            n = athena_count(athena, db, table, where, args)
            print(f"    {db}.{table}: {n} row(s) matching filter")
            if n >= want:
                return n
        except Exception as exc:
            print(f"    (poll) {exc}")
        time.sleep(5)
    raise TimeoutError(
        f"{db}.{table} still has fewer than {want} rows after {wait_seconds}s"
    )


# ---------------------------------------------------------------------------
# Table creation
# ---------------------------------------------------------------------------

def create_external_tables(
    athena, db: str, bucket: str, run_prefix: str,
    log_table: str, trace_table: str, metric_table: str,
    with_metrics: bool, args,
    log_projections: list[dict] | None = None,
) -> None:
    run_athena(athena, f"CREATE DATABASE IF NOT EXISTS {db}", args)
    for t in [log_table, trace_table] + ([metric_table] if with_metrics else []):
        run_athena(athena, f"DROP TABLE IF EXISTS {db}.{t}", args)

    base = f"s3://{bucket}/{run_prefix.strip('/')}"
    log_ddl = _LOG_EXTERNAL_DDL_BASE.format(
        db=db, table=log_table,
        location=f"{base}/logs/",
        extra_cols=_extra_cols_ddl(log_projections or []),
    )
    run_athena(athena, log_ddl, args)
    run_athena(athena, _TRACE_EXTERNAL_DDL.format(db=db, table=trace_table, location=f"{base}/traces/"), args)
    if with_metrics:
        run_athena(athena, _METRIC_EXTERNAL_DDL.format(db=db, table=metric_table, location=f"{base}/metrics/"), args)
    print(f"  created external tables in Glue database '{db}'")


def create_iceberg_tables(
    athena, glue, db: str, warehouse: str, namespace: str,
    log_table: str, trace_table: str, metric_table: str,
    with_metrics: bool, args,
    log_projections: list[dict] | None = None,
) -> None:
    run_athena(athena, f"CREATE DATABASE IF NOT EXISTS {db}", args)

    wh = warehouse.rstrip("/")
    # Athena only accepts s3:// in LOCATION; normalize s3a:// if passed.
    if wh.startswith("s3a://"):
        wh = "s3://" + wh[6:]

    log_ddl = _LOG_ICEBERG_DDL_BASE.format(
        db=db, table=log_table,
        location=f"{wh}/{namespace}/logs",
        extra_cols=_extra_cols_ddl(log_projections or []),
    )
    run_athena(athena, log_ddl, args)
    run_athena(
        athena,
        _TRACE_ICEBERG_DDL.format(db=db, table=trace_table, location=f"{wh}/{namespace}/traces"),
        args,
    )
    if with_metrics:
        run_athena(
            athena,
            _METRIC_ICEBERG_DDL.format(db=db, table=metric_table, location=f"{wh}/{namespace}/metrics"),
            args,
        )
    print(f"  created Glue Iceberg tables in database '{db}'")


# ---------------------------------------------------------------------------
# Assertions
# ---------------------------------------------------------------------------

def assert_logs(athena, db: str, table: str, test_id: str, expected: int, args) -> None:
    where = f"log_attributes['e2e.test_id'] = '{test_id}'"
    n = athena_count(athena, db, table, where, args)
    assert n >= expected, f"logs: expected >= {expected} rows, got {n}"
    print(f"  logs  : {n} row(s) ✓")

    sample = run_athena(
        athena,
        f"SELECT service_name, severity_text, body, log_attributes['e2e.sequence'] AS seq "
        f"FROM {db}.{table} WHERE {where} ORDER BY log_attributes['e2e.sequence'] LIMIT 3",
        args,
        fetch=True,
    )
    for row in sample[1:]:
        print(f"    {row}")


def assert_traces(athena, db: str, table: str, test_id: str, expected: int, args) -> None:
    where = f"span_attributes['e2e.test_id'] = '{test_id}'"
    n = athena_count(athena, db, table, where, args)
    assert n >= expected, f"traces: expected >= {expected} rows, got {n}"
    print(f"  traces: {n} row(s) ✓")

    sample = run_athena(
        athena,
        f"SELECT trace_id, name, duration_nanos, service_name "
        f"FROM {db}.{table} WHERE {where} LIMIT 3",
        args,
        fetch=True,
    )
    for row in sample[1:]:
        print(f"    {row}")


def assert_log_projections(
    athena,
    db: str,
    table: str,
    test_id: str,
    projections: list[dict],
    args,
) -> None:
    """Verify promoted columns are queryable and absent from log_attributes."""
    where = f"log_attributes['e2e.test_id'] = '{test_id}'"
    col_names = [p["column"] for p in projections]

    # 1. Promoted columns must have non-null values.
    for col in col_names:
        rows = run_athena(
            athena,
            f"SELECT {col} FROM {db}.{table} WHERE {where} AND {col} IS NOT NULL LIMIT 1",
            args,
            fetch=True,
        )
        assert len(rows) > 1, (
            f"log projection `{col}` has no non-null rows in {db}.{table}"
        )
        print(f"  projection `{col}` is non-null ✓  (sample: {rows[1][0]})")

    # 2. Promoted attribute keys must NOT appear in log_attributes (promoted-out).
    for col in col_names:
        rows = run_athena(
            athena,
            f"SELECT log_attributes['{col}'] FROM {db}.{table} "
            f"WHERE {where} AND log_attributes['{col}'] IS NOT NULL LIMIT 1",
            args,
            fetch=True,
        )
        assert len(rows) == 1, (
            f"key `{col}` should have been promoted out of log_attributes but is still present"
        )
        print(f"  `log_attributes['{col}']` is NULL (promoted out) ✓")

    # 3. Non-promoted keys must still be in log_attributes.
    rows = run_athena(
        athena,
        f"SELECT log_attributes['e2e.sequence'] FROM {db}.{table} "
        f"WHERE {where} AND log_attributes['e2e.sequence'] IS NOT NULL LIMIT 1",
        args,
        fetch=True,
    )
    assert len(rows) > 1, "non-promoted key `e2e.sequence` is missing from log_attributes"
    print("  non-promoted `log_attributes['e2e.sequence']` still present ✓")


def assert_metrics(athena, db: str, table: str, test_id: str, expected: int, args) -> None:
    where = f"labels['e2e_test_id'] = '{test_id}'"
    n = athena_count(athena, db, table, where, args)
    assert n >= expected, f"metrics: expected >= {expected} rows, got {n}"
    print(f"  metrics: {n} row(s) ✓")

    sample = run_athena(
        athena,
        f"SELECT metric_name, timestamp_millis, value, labels['instance'] AS instance "
        f"FROM {db}.{table} WHERE {where} LIMIT 3",
        args,
        fetch=True,
    )
    for row in sample[1:]:
        print(f"    {row}")


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def cleanup(s3, athena, bucket: str, s3_prefix: str, db: str, tables: list[str], args) -> None:
    print("cleaning up ...")
    for t in tables:
        try:
            run_athena(athena, f"DROP TABLE IF EXISTS {db}.{t}", args)
        except Exception as exc:
            print(f"  warning: failed to drop {db}.{t}: {exc}")
    try:
        run_athena(athena, f"DROP DATABASE IF EXISTS {db}", args)
    except Exception as exc:
        print(f"  warning: failed to drop database {db}: {exc}")

    paginator = s3.get_paginator("list_objects_v2")
    deleted = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=s3_prefix.strip("/") + "/"):
        objects = page.get("Contents", [])
        if objects:
            s3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": o["Key"]} for o in objects]},
            )
            deleted += len(objects)
    if deleted:
        print(f"  deleted {deleted} S3 object(s) under s3://{bucket}/{s3_prefix}/")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="End-to-end Glue/Athena test for telemetry-iceberg-adapter"
    )
    p.add_argument("--mode", choices=["athena-external", "glue-iceberg"], default="athena-external")
    p.add_argument("--adapter-url", default="http://127.0.0.1:4318")
    p.add_argument("--parquet-dir", default="./data/parquet")
    p.add_argument("--bucket", required=True)
    p.add_argument("--prefix", default="telemetry-iceberg-e2e-test",
                   help="S3 key prefix for uploaded Parquet files")
    p.add_argument("--warehouse",
                   help="Iceberg warehouse URI, e.g. s3://bucket/prefix (glue-iceberg mode)")
    p.add_argument("--namespace", default="telemetry_e2e",
                   help="Glue/Iceberg namespace the adapter is configured with "
                        "(OTLP_ADAPTER_CATALOG_NAMESPACE). Used as the Glue database "
                        "name in glue-iceberg mode.")
    p.add_argument("--region", default=None)
    p.add_argument("--database", default=None,
                   help="Override the Glue database name. Defaults to --namespace in "
                        "glue-iceberg mode or 'telemetry_iceberg_e2e' in athena-external mode.")
    # The adapter hardcodes these table names in pipeline.rs; only override if you've
    # patched the adapter.
    p.add_argument("--log-table", default="logs")
    p.add_argument("--trace-table", default="traces")
    p.add_argument("--metric-table", default="metrics")
    p.add_argument("--athena-output", required=True,
                   help="S3 URI for Athena query results, e.g. s3://bucket/athena-results/")
    p.add_argument("--workgroup", default=None)
    p.add_argument("--test-id", default=None)
    p.add_argument("--log-count", type=int, default=10)
    p.add_argument("--trace-count", type=int, default=10)
    p.add_argument("--metric-count", type=int, default=10)
    p.add_argument("--wait-seconds", type=int, default=90)
    p.add_argument("--no-cleanup", action="store_true",
                   help="Skip S3/Glue cleanup after the test")
    p.add_argument("--with-projections", action="store_true",
                   help="Smoke-test log column projections (src_ip STRING, bytes BIGINT). "
                        "The adapter must be started with matching [tables.logs.projections] config.")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    test_id = args.test_id or f"e2e-{uuid.uuid4()}"
    # Isolate each run under its own S3 prefix so tests don't interfere.
    run_prefix = f"{args.prefix.strip('/')}/{test_id}"
    parquet_dir = Path(args.parquet_dir).expanduser().resolve()

    # In glue-iceberg mode the Glue database is the adapter's catalog namespace.
    # In athena-external mode we use an isolated test database by default.
    if args.database:
        db = args.database
    elif args.mode == "glue-iceberg":
        db = args.namespace
    else:
        db = "telemetry_iceberg_e2e"

    log_projections = _SMOKE_PROJECTIONS if args.with_projections else []
    log_extra_attrs = (
        {"src_ip": "10.0.0.1", "bytes": "1024"}
        if args.with_projections else None
    )

    print(f"test_id     = {test_id}")
    print(f"mode        = {args.mode}")
    print(f"database    = {db}")
    print(f"projections = {[p['column'] for p in log_projections] or 'none'}")

    s3 = boto3.client("s3", region_name=args.region)
    athena = boto3.client("athena", region_name=args.region)
    glue = boto3.client("glue", region_name=args.region)
    tables_created: list[str] = []

    try:
        # ── glue-iceberg: create tables before sending so the adapter can commit
        #    immediately on first flush rather than retrying against missing tables ──
        if args.mode == "glue-iceberg":
            if not args.warehouse:
                print("ERROR: --warehouse is required for glue-iceberg mode", file=sys.stderr)
                return 1
            print("\ncreating Glue Iceberg tables ...")
            # sent_metrics is not known yet; create all three tables up front.
            create_iceberg_tables(
                athena, glue, db, args.warehouse, args.namespace,
                args.log_table, args.trace_table, args.metric_table,
                with_metrics=True, args=args,
                log_projections=log_projections,
            )

        # ── snapshot existing Parquet before sending (athena-external only) ──
        existing_parquet = snapshot_parquet(
            parquet_dir, [args.log_table, args.trace_table, args.metric_table]
        )

        # ── send signals ──────────────────────────────────────────────────
        print("\nsending signals ...")
        send_logs(args.adapter_url, test_id, args.log_count, extra_attributes=log_extra_attrs)
        send_traces(args.adapter_url, test_id, args.trace_count)
        sent_metrics = send_metrics(args.adapter_url, test_id, args.metric_count)

        if args.mode == "athena-external":
            # ── wait for new local Parquet only ──────────────────────────
            print(f"\nwaiting up to {args.wait_seconds}s for new Parquet files ...")
            log_files = wait_for_new_parquet(
                parquet_dir / args.log_table,
                existing_parquet[args.log_table],
                args.wait_seconds,
            )
            trace_files = wait_for_new_parquet(
                parquet_dir / args.trace_table,
                existing_parquet[args.trace_table],
                args.wait_seconds,
            )
            metric_files = (
                wait_for_new_parquet(
                    parquet_dir / args.metric_table,
                    existing_parquet[args.metric_table],
                    args.wait_seconds,
                )
                if sent_metrics else []
            )
            print(
                f"  {len(log_files)} log, {len(trace_files)} trace, "
                f"{len(metric_files)} metric file(s)"
            )

            # ── upload to S3 ──────────────────────────────────────────────
            print("\nuploading to S3 ...")
            upload_table(s3, args.bucket, run_prefix, "logs", log_files)
            upload_table(s3, args.bucket, run_prefix, "traces", trace_files)
            if metric_files:
                upload_table(s3, args.bucket, run_prefix, "metrics", metric_files)

            # ── create Glue external tables ───────────────────────────────
            print("\ncreating external tables ...")
            create_external_tables(
                athena, db, args.bucket, run_prefix,
                args.log_table, args.trace_table, args.metric_table,
                bool(metric_files), args,
                log_projections=log_projections,
            )
            tables_created = [args.log_table, args.trace_table] + (
                [args.metric_table] if metric_files else []
            )

        else:  # glue-iceberg
            tables_created = [args.log_table, args.trace_table] + (
                [args.metric_table] if sent_metrics else []
            )

            # ── wait for adapter to commit ────────────────────────────────
            print(f"\nwaiting up to {args.wait_seconds}s for adapter commits ...")
            wait_for_rows(
                athena, db, args.log_table,
                f"log_attributes['e2e.test_id'] = '{test_id}'",
                args.log_count, args.wait_seconds, args,
            )
            wait_for_rows(
                athena, db, args.trace_table,
                f"span_attributes['e2e.test_id'] = '{test_id}'",
                args.trace_count, args.wait_seconds, args,
            )
            if sent_metrics:
                wait_for_rows(
                    athena, db, args.metric_table,
                    f"labels['e2e_test_id'] = '{test_id}'",
                    args.metric_count, args.wait_seconds, args,
                )

        # ── assert ────────────────────────────────────────────────────────
        print("\nrunning assertions ...")
        assert_logs(athena, db, args.log_table, test_id, args.log_count, args)
        assert_traces(athena, db, args.trace_table, test_id, args.trace_count, args)
        if sent_metrics:
            assert_metrics(athena, db, args.metric_table, test_id, args.metric_count, args)
        if log_projections:
            assert_log_projections(athena, db, args.log_table, test_id, log_projections, args)

        print(f"\n✓ test passed  (test_id={test_id})")
        return 0

    except AssertionError as exc:
        print(f"\nASSERTION FAILED: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1
    finally:
        if not args.no_cleanup and tables_created:
            cleanup(s3, athena, args.bucket, run_prefix, db, tables_created, args)


if __name__ == "__main__":
    raise SystemExit(main())
