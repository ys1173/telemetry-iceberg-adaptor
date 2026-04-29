#!/usr/bin/env python3
"""
Network-security firewall-log demo for telemetry-iceberg-adapter.

Sends 12 realistic firewall connection events (ALLOW / DENY / DROP),
waits for the adapter to flush Parquet, uploads to S3, creates a Glue
external table, then runs four named Athena queries and prints the results.

Schema
------
Projected (top-level Parquet columns):
  src_ip    STRING   source IP address
  dst_ip    STRING   destination IP address
  dst_port  INT      destination port
  protocol  STRING   TCP / UDP / ICMP
  action    STRING   ALLOW / DENY / DROP
  bytes_sent BIGINT  bytes sent by initiator

Remaining in log_attributes (not projected):
  src_port     source port (ephemeral, not useful to index)
  bytes_recv   bytes received
  duration_ms  connection duration
  country      geo-IP country of remote endpoint
  threat_cat   threat category when applicable (brute_force / port_scan / c2)

Adapter config required (see /tmp/adapter-netsec.toml written by this script):
  [tables.logs]
  [[tables.logs.projections]]  x6

Usage
-----
  # 1. Let the script write the adapter config and start the adapter
  python3 scripts/netsec_demo.py --bucket MY-BUCKET --athena-output s3://MY-BUCKET/athena/

  # 2. Or start the adapter yourself first, then run:
  python3 scripts/netsec_demo.py --bucket MY-BUCKET --athena-output s3://MY-BUCKET/athena/ \\
      --adapter-already-running
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import boto3
import requests

# ---------------------------------------------------------------------------
# Schema definition (single source of truth)
# ---------------------------------------------------------------------------

PROJECTIONS = [
    {"column": "src_ip",     "source": "$.src_ip",     "data_type": "string"},
    {"column": "dst_ip",     "source": "$.dst_ip",     "data_type": "string"},
    {"column": "dst_port",   "source": "$.dst_port",   "data_type": "int"},
    {"column": "protocol",   "source": "$.protocol",   "data_type": "string"},
    {"column": "action",     "source": "$.action",     "data_type": "string"},
    {"column": "bytes_sent", "source": "$.bytes_sent", "data_type": "long"},
]

_SQL_TYPE = {"string": "STRING", "int": "INT", "long": "BIGINT", "double": "DOUBLE"}

# ---------------------------------------------------------------------------
# Sample firewall events (what we will send)
# ---------------------------------------------------------------------------

EVENTS = [
    # Normal outbound HTTPS
    dict(src_ip="192.168.1.10", dst_ip="93.184.216.34",  src_port="54321", dst_port="443",  protocol="TCP",  action="ALLOW", bytes_sent="1240",    bytes_recv="52480",  duration_ms="142",  country="US"),
    dict(src_ip="192.168.1.15", dst_ip="104.26.10.72",   src_port="61234", dst_port="443",  protocol="TCP",  action="ALLOW", bytes_sent="800",     bytes_recv="12300",  duration_ms="89",   country="US"),
    # Internal SSH
    dict(src_ip="10.0.0.5",     dst_ip="192.168.1.1",    src_port="45000", dst_port="22",   protocol="TCP",  action="ALLOW", bytes_sent="2048",    bytes_recv="1024",   duration_ms="5200", country="internal"),
    # External brute-force attempt
    dict(src_ip="185.220.101.42", dst_ip="10.0.0.100",   src_port="42001", dst_port="22",   protocol="TCP",  action="DENY",  bytes_sent="60",      bytes_recv="0",      duration_ms="1",    country="RU",  threat_cat="brute_force"),
    # Port scan (same source, different ports)
    dict(src_ip="185.220.101.42", dst_ip="10.0.0.100",   src_port="42002", dst_port="23",   protocol="TCP",  action="DENY",  bytes_sent="60",      bytes_recv="0",      duration_ms="0",    country="RU",  threat_cat="port_scan"),
    dict(src_ip="185.220.101.42", dst_ip="10.0.0.100",   src_port="42003", dst_port="3389", protocol="TCP",  action="DENY",  bytes_sent="60",      bytes_recv="0",      duration_ms="0",    country="RU",  threat_cat="port_scan"),
    # Suspiciously large upload to foreign IP (potential exfiltration)
    dict(src_ip="192.168.1.20", dst_ip="203.0.113.50",   src_port="55001", dst_port="443",  protocol="TCP",  action="ALLOW", bytes_sent="98304000",bytes_recv="1200",   duration_ms="34200",country="CN"),
    # Normal DNS
    dict(src_ip="192.168.1.10", dst_ip="8.8.8.8",        src_port="53241", dst_port="53",   protocol="UDP",  action="ALLOW", bytes_sent="68",      bytes_recv="120",    duration_ms="12",   country="US"),
    # Internal database connection
    dict(src_ip="10.0.0.5",     dst_ip="10.0.0.20",      src_port="49001", dst_port="5432", protocol="TCP",  action="ALLOW", bytes_sent="4096",    bytes_recv="16384",  duration_ms="45",   country="internal"),
    # C2 beacon on unusual port
    dict(src_ip="172.16.0.50",  dst_ip="198.51.100.25",  src_port="33444", dst_port="4444", protocol="TCP",  action="DROP",  bytes_sent="512",     bytes_recv="0",      duration_ms="2",    country="US",  threat_cat="c2_beacon"),
    # Outbound HTTP (non-TLS)
    dict(src_ip="192.168.1.30", dst_ip="91.108.4.1",     src_port="61000", dst_port="80",   protocol="TCP",  action="ALLOW", bytes_sent="340",     bytes_recv="8200",   duration_ms="67",   country="NL"),
    # ICMP (ping sweep, blocked)
    dict(src_ip="10.10.10.1",   dst_ip="10.0.0.0",       src_port="0",     dst_port="0",    protocol="ICMP", action="DENY",  bytes_sent="32",      bytes_recv="0",      duration_ms="0",    country="internal", threat_cat="reconnaissance"),
]

# Severity logic: DENY/DROP with a threat_cat → ERROR; DENY/DROP → WARN; ALLOW → INFO
def _severity(event: dict) -> tuple[int, str]:
    if event["action"] in ("DENY", "DROP"):
        if "threat_cat" in event:
            return 17, "ERROR"
        return 13, "WARN"
    return 9, "INFO"

# Body: one-liner connection summary
def _body(event: dict) -> str:
    return (
        f"{event['action']} {event['protocol']} "
        f"{event['src_ip']}:{event['src_port']} -> "
        f"{event['dst_ip']}:{event['dst_port']}"
    )

# ---------------------------------------------------------------------------
# Print helpers
# ---------------------------------------------------------------------------

BOLD  = "\033[1m"
CYAN  = "\033[36m"
GREEN = "\033[32m"
YELLOW= "\033[33m"
RED   = "\033[31m"
RESET = "\033[0m"

def banner(title: str) -> None:
    width = 72
    print(f"\n{BOLD}{CYAN}{'─' * width}")
    print(f"  {title}")
    print(f"{'─' * width}{RESET}")

def print_table(headers: list[str], rows: list[list[str]], col_width: int = 18) -> None:
    fmt = "  " + "  ".join(f"{{:<{col_width}}}" for _ in headers)
    print(fmt.format(*headers))
    print("  " + "  ".join("─" * col_width for _ in headers))
    for row in rows:
        print(fmt.format(*[str(v)[:col_width] for v in row]))

# ---------------------------------------------------------------------------
# Adapter config writer
# ---------------------------------------------------------------------------

def write_adapter_config(path: str, parquet_dir: str) -> None:
    lines = [
        "[server]",
        'http_bind = "0.0.0.0:4318"',
        'grpc_bind = "0.0.0.0:4317"',
        "",
        "[wal]",
        'backend = "local"',
        f'dir = "{parquet_dir}/wal"',
        "",
        "[storage]",
        f'parquet_dir = "{parquet_dir}/parquet"',
        "",
        "[batch]",
        "max_rows = 12",
        "max_age_secs = 5",
        "",
        "[catalog]",
        'type = "noop"',
        "",
        "[tables.logs]",
        'name = "logs"',
        "",
    ]
    for p in PROJECTIONS:
        lines += [
            "[[tables.logs.projections]]",
            f'column    = "{p["column"]}"',
            f'source    = "{p["source"]}"',
            f'data_type = "{p["data_type"]}"',
            "",
        ]
    Path(path).write_text("\n".join(lines))
    print(f"  wrote adapter config → {path}")

# ---------------------------------------------------------------------------
# Athena helpers
# ---------------------------------------------------------------------------

def _athena_run(athena, sql: str, output: str, workgroup: str | None, region: str | None, fetch: bool = False):
    req = {"QueryString": sql, "ResultConfiguration": {"OutputLocation": output}}
    if workgroup:
        req["WorkGroup"] = workgroup
    qid = athena.start_query_execution(**req)["QueryExecutionId"]

    for _ in range(120):
        state = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"].get("StateChangeReason", "")
            raise RuntimeError(f"Athena query {state}: {reason}\nSQL: {sql[:300]}")
        time.sleep(1)
    else:
        raise TimeoutError(f"Athena query timed out: {sql[:100]}")

    if not fetch:
        return []

    pages, rows = athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid), []
    for page in pages:
        for result_row in page["ResultSet"]["Rows"]:
            rows.append([c.get("VarCharValue", "") for c in result_row["Data"]])
    return rows  # rows[0] is the header

# ---------------------------------------------------------------------------
# Parquet wait
# ---------------------------------------------------------------------------

def wait_for_parquet(directory: Path, timeout: int = 60) -> list[Path]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        files = list(directory.glob("*.parquet"))
        if files:
            return files
        time.sleep(2)
    raise TimeoutError(f"No Parquet files appeared in {directory} after {timeout}s")

# ---------------------------------------------------------------------------
# S3 upload
# ---------------------------------------------------------------------------

def upload_files(s3, bucket: str, prefix: str, files: list[Path]) -> None:
    for f in files:
        key = f"{prefix}/{f.name}"
        s3.upload_file(str(f), bucket, key)
        print(f"    {f.name}  →  s3://{bucket}/{key}")

# ---------------------------------------------------------------------------
# Main demo
# ---------------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(description="Network-security log demo")
    p.add_argument("--bucket", required=True)
    p.add_argument("--athena-output", required=True, help="s3://bucket/prefix/ for Athena results")
    p.add_argument("--adapter-url", default="http://127.0.0.1:4318")
    p.add_argument("--parquet-base", default="/tmp/netsec-demo",
                   help="Base dir; wal/ and parquet/ sub-dirs will be created here")
    p.add_argument("--region", default=None)
    p.add_argument("--workgroup", default=None)
    p.add_argument("--adapter-already-running", action="store_true",
                   help="Skip writing config and starting the adapter")
    p.add_argument("--no-cleanup", action="store_true")
    args = p.parse_args()

    db = "telemetry_netsec_demo"
    table = "firewall_logs"
    parquet_base = Path(args.parquet_base)
    parquet_dir = parquet_base / "parquet" / "logs"
    s3_prefix = "netsec-demo"

    # ── 0. Print schema ──────────────────────────────────────────────────────
    banner("SCHEMA  —  Iceberg / Parquet columns")
    print(f"\n  Table   : {db}.{table}")
    print(f"  Service : firewall-sensor\n")

    print(f"  {BOLD}Projected columns (top-level in Parquet):{RESET}")
    print_table(
        ["column", "type", "source", "description"],
        [
            ["src_ip",     "STRING", "$.src_ip",     "Source IP address"],
            ["dst_ip",     "STRING", "$.dst_ip",     "Destination IP address"],
            ["dst_port",   "INT",    "$.dst_port",   "Destination port number"],
            ["protocol",   "STRING", "$.protocol",   "TCP / UDP / ICMP"],
            ["action",     "STRING", "$.action",     "ALLOW / DENY / DROP"],
            ["bytes_sent", "BIGINT", "$.bytes_sent", "Bytes sent by initiator"],
        ],
    )
    print(f"\n  {BOLD}Remaining in log_attributes (map<string,string>):{RESET}")
    print_table(
        ["key", "example values"],
        [
            ["src_port",    "54321, 61234, …"],
            ["bytes_recv",  "52480, 0, …"],
            ["duration_ms", "142, 0, 34200, …"],
            ["country",     "US, RU, CN, internal"],
            ["threat_cat",  "brute_force, port_scan, c2_beacon, reconnaissance"],
        ],
        col_width=24,
    )

    # ── 1. Print sample events ───────────────────────────────────────────────
    banner("SAMPLE INPUT  —  12 firewall connection events")
    print()
    now_ns = int(time.time() * 1_000_000_000)
    ndjson_lines = []
    for i, event in enumerate(EVENTS):
        sev_num, sev_text = _severity(event)
        color = GREEN if event["action"] == "ALLOW" else (RED if sev_num == 17 else YELLOW)
        print(f"  [{i+1:02d}] {color}{event['action']:5s}{RESET}  {event['protocol']:4s}  "
              f"{event['src_ip']:16s}:{event['src_port']:5s}  →  "
              f"{event['dst_ip']:16s}:{event['dst_port']:4s}  "
              f"bytes_sent={event['bytes_sent']:>10s}  country={event.get('country','?')}"
              + (f"  {RED}threat_cat={event['threat_cat']}{RESET}" if "threat_cat" in event else ""))

        # Build the attributes dict — every event field except the projected ones
        # become log_attributes; projected ones also go in attributes (the adapter
        # will promote them to top-level columns and strip them from the map).
        attrs = {k: v for k, v in event.items()}
        ndjson_lines.append(json.dumps({
            "timestamp_nanos": now_ns + i * 1_000_000,
            "observed_timestamp_nanos": now_ns + i * 1_000_000,
            "severity_number": sev_num,
            "severity_text": sev_text,
            "body": _body(event),
            "service_name": "firewall-sensor",
            "resource_attributes": {
                "service.name": "firewall-sensor",
                "deployment.environment": "production",
                "sensor.id": "fw-edge-01",
            },
            "attributes": attrs,
        }))

    # ── 2. Start adapter (if needed) ─────────────────────────────────────────
    banner("ADAPTER  —  starting with projection config")
    config_path = str(parquet_base / "adapter-netsec.toml")
    parquet_base.mkdir(parents=True, exist_ok=True)
    (parquet_base / "wal").mkdir(exist_ok=True)
    (parquet_base / "parquet").mkdir(exist_ok=True)

    adapter_proc = None
    if not args.adapter_already_running:
        write_adapter_config(config_path, str(parquet_base))

        adapter_bin = Path(__file__).parent.parent / "target" / "debug" / "telemetry-iceberg-adapter"
        log_path = parquet_base / "adapter.log"
        print(f"  starting {adapter_bin}")
        adapter_proc = subprocess.Popen(
            [str(adapter_bin)],
            env={**os.environ, "OTLP_ADAPTER_CONFIG": config_path},
            stdout=open(log_path, "w"),
            stderr=subprocess.STDOUT,
        )
        time.sleep(3)
        if adapter_proc.poll() is not None:
            print(f"{RED}  adapter exited early — check {log_path}{RESET}")
            return 1
        print(f"  adapter running (PID {adapter_proc.pid}), log → {log_path}")
    else:
        print("  using already-running adapter")

    try:
        # ── 3. Send logs ─────────────────────────────────────────────────────
        banner("INGEST  —  posting logs to adapter")
        payload = "\n".join(ndjson_lines).encode()
        resp = requests.post(
            f"{args.adapter_url.rstrip('/')}/v1/logs/ndjson",
            data=payload,
            headers={"content-type": "application/x-ndjson"},
            timeout=15,
        )
        resp.raise_for_status()
        print(f"\n  posted {len(EVENTS)} events  ({len(payload)} bytes)")

        # ── 4. Wait for Parquet ───────────────────────────────────────────────
        banner("PARQUET  —  waiting for flush")
        parquet_files = wait_for_parquet(parquet_dir, timeout=30)
        print(f"\n  flushed {len(parquet_files)} file(s):")
        for f in parquet_files:
            print(f"    {f}  ({f.stat().st_size:,} bytes)")

        # ── 5. Upload to S3 ──────────────────────────────────────────────────
        banner("S3  —  uploading Parquet")
        s3 = boto3.client("s3", region_name=args.region)
        print()
        upload_files(s3, args.bucket, f"{s3_prefix}/logs", parquet_files)

        # ── 6. Create Glue external table ────────────────────────────────────
        banner("GLUE  —  creating external table")
        extra_cols = "\n".join(
            f"  {p['column']:<12} {_SQL_TYPE[p['data_type']]},"
            for p in PROJECTIONS
        )
        location = f"s3://{args.bucket}/{s3_prefix}/logs/"
        ddl = f"""
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
  log_attributes           MAP<STRING, STRING>,
{extra_cols.rstrip(',')}
)
STORED AS PARQUET
LOCATION '{location}'
"""
        athena = boto3.client("athena", region_name=args.region)
        _athena_run(athena, f"CREATE DATABASE IF NOT EXISTS {db}", args.athena_output, args.workgroup, args.region)
        _athena_run(athena, f"DROP TABLE IF EXISTS {db}.{table}", args.athena_output, args.workgroup, args.region)
        _athena_run(athena, ddl, args.athena_output, args.workgroup, args.region)
        print(f"\n  created {db}.{table}")
        print(f"  location: {location}")
        print(f"\n  DDL:\n{ddl}")

        def q(sql: str, fetch: bool = True):
            return _athena_run(athena, sql, args.athena_output, args.workgroup, args.region, fetch=fetch)

        # ── 7. Queries ────────────────────────────────────────────────────────
        banner("QUERY 1  —  Traffic summary by action")
        sql = f"""
SELECT
    action,
    count(*)                          AS connections,
    sum(bytes_sent)                   AS total_bytes_sent,
    count_if(severity_text = 'ERROR') AS high_severity
FROM {db}.{table}
GROUP BY action
ORDER BY connections DESC
"""
        print(f"\n  SQL:{sql}")
        rows = q(sql)
        print(f"  Result:")
        print_table(rows[0], rows[1:], col_width=20)

        banner("QUERY 2  —  Blocked connections with threat context")
        sql = f"""
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
FROM {db}.{table}
WHERE action IN ('DENY', 'DROP')
ORDER BY severity_text DESC, src_ip
"""
        print(f"\n  SQL:{sql}")
        rows = q(sql)
        print(f"  Result:")
        print_table(rows[0], rows[1:], col_width=16)

        banner("QUERY 3  —  Potential data exfiltration (bytes_sent > 1 MB)")
        sql = f"""
SELECT
    src_ip,
    dst_ip,
    dst_port,
    protocol,
    bytes_sent,
    log_attributes['duration_ms'] AS duration_ms,
    log_attributes['country']     AS country,
    body
FROM {db}.{table}
WHERE bytes_sent > 1000000
ORDER BY bytes_sent DESC
"""
        print(f"\n  SQL:{sql}")
        rows = q(sql)
        print(f"  Result:")
        print_table(rows[0], rows[1:], col_width=18)

        banner("QUERY 4  —  Repeated attacker: src_ip with multiple denials")
        sql = f"""
SELECT
    src_ip,
    log_attributes['country']    AS country,
    count(*)                     AS blocked_attempts,
    min(dst_port)                AS first_port,
    max(dst_port)                AS last_port,
    array_join(array_agg(CAST(dst_port AS VARCHAR)), ', ') AS ports_tried
FROM {db}.{table}
WHERE action IN ('DENY', 'DROP')
GROUP BY src_ip, log_attributes['country']
HAVING count(*) > 1
ORDER BY blocked_attempts DESC
"""
        print(f"\n  SQL:{sql}")
        rows = q(sql)
        print(f"  Result:")
        print_table(rows[0], rows[1:], col_width=18)

        banner("QUERY 5  —  Confirm promotions: projected vs log_attributes")
        sql = f"""
SELECT
    src_ip,
    dst_port,
    action,
    log_attributes['src_ip']   AS src_ip_in_attrs,
    log_attributes['src_port'] AS src_port_in_attrs,
    log_attributes['country']  AS country
FROM {db}.{table}
LIMIT 5
"""
        print(f"\n  SQL:{sql}")
        print(f"  (src_ip_in_attrs should be NULL — promoted out of the map)\n")
        rows = q(sql)
        print(f"  Result:")
        print_table(rows[0], rows[1:], col_width=18)

        print(f"\n{BOLD}{GREEN}✓  demo complete{RESET}\n")
        return 0

    finally:
        if adapter_proc:
            adapter_proc.terminate()
            print(f"\n  adapter stopped (PID {adapter_proc.pid})")

        if not args.no_cleanup:
            print("\ncleaning up ...")
            try:
                _athena_run(athena, f"DROP TABLE IF EXISTS {db}.{table}",
                            args.athena_output, args.workgroup, args.region)
                _athena_run(athena, f"DROP DATABASE IF EXISTS {db}",
                            args.athena_output, args.workgroup, args.region)
            except Exception:
                pass
            try:
                paginator = s3.get_paginator("list_objects_v2")
                keys = []
                for page in paginator.paginate(Bucket=args.bucket, Prefix=f"{s3_prefix}/"):
                    keys += [{"Key": o["Key"]} for o in page.get("Contents", [])]
                if keys:
                    s3.delete_objects(Bucket=args.bucket, Delete={"Objects": keys})
                    print(f"  deleted {len(keys)} S3 object(s)")
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
