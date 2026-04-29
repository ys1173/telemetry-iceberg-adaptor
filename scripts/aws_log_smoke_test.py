#!/usr/bin/env python3
"""
Small AWS smoke test for the telemetry-iceberg-adapter log path.

This intentionally keeps the AWS surface area small:
1. POST a few NDJSON logs to a running local adapter.
2. Wait for the adapter to write local Parquet files.
3. Upload those Parquet files to S3.
4. Create a Glue/Athena external table over the Parquet files.
5. Run a couple of Athena queries that validate row count and schema access.

Prerequisites:
  pip install boto3 requests

Example:
  python scripts/aws_log_smoke_test.py \
    --bucket my-telemetry-test-bucket \
    --prefix smoke/telemetry-iceberg-adapter \
    --athena-output s3://my-telemetry-test-bucket/athena-results/
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from pathlib import Path
from typing import Iterable

import boto3
import requests


LOG_TABLE_DDL = """
CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table} (
  observed_timestamp_nanos BIGINT,
  timestamp_nanos BIGINT,
  severity_number INT,
  severity_text STRING,
  body STRING,
  trace_id STRING,
  span_id STRING,
  service_name STRING,
  resource_attributes MAP<STRING, STRING>,
  log_attributes MAP<STRING, STRING>
)
STORED AS PARQUET
LOCATION '{location}'
"""


def main() -> int:
    args = parse_args()
    test_id = args.test_id or f"smoke-{uuid.uuid4()}"
    parquet_dir = Path(args.parquet_dir).expanduser().resolve() / "logs"

    print(f"test_id={test_id}")
    post_smoke_logs(args.adapter_url, test_id, args.log_count)
    files = wait_for_parquet_files(parquet_dir, args.wait_seconds)
    print(f"found {len(files)} local parquet file(s) in {parquet_dir}")

    s3 = boto3.client("s3", region_name=args.region)
    s3_location = upload_files(s3, args.bucket, args.prefix, files)
    print(f"uploaded parquet files to {s3_location}")

    athena = boto3.client("athena", region_name=args.region)
    run_athena(athena, f"CREATE DATABASE IF NOT EXISTS {args.database}", args)
    run_athena(athena, f"DROP TABLE IF EXISTS {args.database}.{args.table}", args)
    run_athena(
        athena,
        LOG_TABLE_DDL.format(
            database=args.database,
            table=args.table,
            location=s3_location,
        ),
        args,
    )

    count_rows = run_athena(
        athena,
        f"""
        SELECT count(*) AS rows
        FROM {args.database}.{args.table}
        WHERE log_attributes['smoke.test_id'] = '{test_id}'
        """,
        args,
        fetch=True,
    )
    rows = int(count_rows[1][0])
    print(f"athena row count for test_id={test_id}: {rows}")

    sample = run_athena(
        athena,
        f"""
        SELECT
          service_name,
          severity_text,
          body,
          resource_attributes['deployment.environment'] AS environment,
          log_attributes['smoke.sequence'] AS sequence
        FROM {args.database}.{args.table}
        WHERE log_attributes['smoke.test_id'] = '{test_id}'
        ORDER BY log_attributes['smoke.sequence']
        LIMIT 5
        """,
        args,
        fetch=True,
    )
    print("sample rows:")
    for row in sample:
        print(row)

    if rows < args.log_count:
        print(
            f"expected at least {args.log_count} rows for this test id, got {rows}",
            file=sys.stderr,
        )
        return 1

    print("smoke test passed")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--adapter-url", default="http://127.0.0.1:4318")
    parser.add_argument("--parquet-dir", default="./data/parquet")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", default="telemetry-iceberg-adapter-smoke")
    parser.add_argument("--region", default=None)
    parser.add_argument("--database", default="telemetry_iceberg_smoke")
    parser.add_argument("--table", default="logs_smoke")
    parser.add_argument("--athena-output", required=True)
    parser.add_argument("--workgroup", default=None)
    parser.add_argument("--test-id", default=None)
    parser.add_argument("--log-count", type=int, default=10)
    parser.add_argument("--wait-seconds", type=int, default=75)
    return parser.parse_args()


def post_smoke_logs(adapter_url: str, test_id: str, log_count: int) -> None:
    now_nanos = int(time.time() * 1_000_000_000)
    lines = []
    for index in range(log_count):
        lines.append(
            json.dumps(
                {
                    "timestamp_nanos": now_nanos + index,
                    "observed_timestamp_nanos": now_nanos + index,
                    "severity_number": 9,
                    "severity_text": "INFO",
                    "body": f"aws smoke test log {index}",
                    "service_name": "telemetry-iceberg-smoke",
                    "resource": {
                        "service.name": "telemetry-iceberg-smoke",
                        "deployment.environment": "aws-smoke",
                    },
                    "attributes": {
                        "smoke.test_id": test_id,
                        "smoke.sequence": str(index),
                    },
                }
            )
        )

    response = requests.post(
        f"{adapter_url.rstrip('/')}/v1/logs/ndjson",
        data="\n".join(lines).encode("utf-8"),
        headers={"content-type": "application/x-ndjson"},
        timeout=15,
    )
    response.raise_for_status()
    print(f"posted {log_count} NDJSON log(s) to {adapter_url}")


def wait_for_parquet_files(parquet_dir: Path, wait_seconds: int) -> list[Path]:
    deadline = time.time() + wait_seconds
    while time.time() < deadline:
        files = sorted(parquet_dir.glob("*.parquet"))
        if files:
            return files
        time.sleep(2)

    raise TimeoutError(
        f"no parquet files appeared in {parquet_dir} within {wait_seconds} seconds"
    )


def upload_files(s3, bucket: str, prefix: str, files: Iterable[Path]) -> str:
    cleaned_prefix = prefix.strip("/")
    for file in files:
        key = f"{cleaned_prefix}/logs/{file.name}"
        s3.upload_file(str(file), bucket, key)
    return f"s3://{bucket}/{cleaned_prefix}/logs/"


def run_athena(
    athena,
    query: str,
    args: argparse.Namespace,
    fetch: bool = False,
) -> list[list[str]]:
    request = {
        "QueryString": query,
        "ResultConfiguration": {"OutputLocation": args.athena_output},
    }
    if args.workgroup:
        request["WorkGroup"] = args.workgroup

    query_id = athena.start_query_execution(**request)["QueryExecutionId"]
    while True:
        execution = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        state = execution["Status"]["State"]
        if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break
        time.sleep(2)

    if state != "SUCCEEDED":
        reason = execution["Status"].get("StateChangeReason", "unknown reason")
        raise RuntimeError(f"Athena query {query_id} {state}: {reason}\n{query}")

    if not fetch:
        return []

    rows = athena.get_query_results(QueryExecutionId=query_id)["ResultSet"]["Rows"]
    return [
        [cell.get("VarCharValue", "") for cell in row.get("Data", [])]
        for row in rows
    ]


if __name__ == "__main__":
    raise SystemExit(main())
