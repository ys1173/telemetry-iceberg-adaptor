# Vector Local Scale Test Reference

This document captures the local Vector-to-adapter scale test setup used to validate
high-rate NDJSON log ingestion without S3 or Glue in the loop.

## Adapter Mode

The adapter was run locally with:

- `catalog.type = "noop"`
- local WAL under `./data/scale-test/wal`
- local Parquet under `./data/scale-test/parquet`
- `tables.logs.schema_profile = "generic_event"`
- `pipeline.log_workers = 4`
- `limits.max_http_body_bytes = 67108864` (64 MB)
- `limits.max_pending_wal_bytes = 2147483648` (2 GB)
- `batch.max_rows = 500000`
- `batch.max_bytes = 134217728` (128 MB)
- `batch.max_age_secs = 10`

This exercises:

```text
Vector -> HTTP NDJSON -> adapter WAL -> decode -> batch -> Arrow -> Parquet -> noop commit
```

It does not exercise S3 upload or Glue/Iceberg catalog commit latency.

## Vector Config

```toml
[sources.syslog_udp]
type = "socket"
address = "0.0.0.0:20514"
mode = "udp"

[transforms.parse_syslog]
type = "remap"
inputs = ["syslog_udp"]
source = '''
parsed, err = parse_syslog(string!(.message))
if err != null {
  abort
}
. = parsed
'''
drop_on_error = true

[sinks.http_ndjson]
type = "http"
inputs = ["parse_syslog"]
uri = "http://host.docker.internal:4318/v1/logs/ndjson"
method = "post"

[sinks.http_ndjson.buffer]
type = "memory"
max_events = 500000
when_full = "block"

[sinks.http_ndjson.request]
in_flight_limit = 4
timeout_secs = 30
retry_max_duration_secs = 600

[sinks.http_ndjson.request.headers]
Content-Type = "application/x-ndjson"

[sinks.http_ndjson.batch]
max_bytes = 1048576
timeout_secs = 1

[sinks.http_ndjson.encoding]
codec = "json"

[sinks.http_ndjson.framing]
method = "newline_delimited"
```

Notes:

- `host.docker.internal` was required because Vector ran in Docker and the adapter ran on the macOS host.
- `batch.max_bytes = 524288` (512 KB) was also tested successfully.
- `batch.max_bytes = 1048576` (1 MB) was used for the later 30k/sec, 60k/sec, and 100k/sec runs.
- Keep Vector `batch.max_bytes` below adapter `limits.max_http_body_bytes`.

## Observed Results

All tests below used local disk and `catalog.type = "noop"`.

| Rate | Vector batch size | Result |
|------|-------------------|--------|
| 30k logs/sec | 512 KB | Healthy after local WAL scan race fix |
| 30k logs/sec | 1 MB | Healthy after adapter restart with fixed binary |
| 60k logs/sec | 1 MB | Healthy; WAL backlog decreased during sample |
| 100k logs/sec | 1 MB | Healthy in sampled window; WAL backlog peaked then stabilized |

For the 100k/sec sampled window:

```text
time      parquet_files  wal_received  wal_size  parquet_size
16:48:26  379            156           212M      373M
16:48:36  382            186           253M      384M
16:48:46  386            180           245M      398M
16:48:56  390            147           200M      412M
16:49:06  394            148           202M      426M
16:49:16  398            150           204M      440M
```

No new adapter-side matches were found for:

```text
rejecting HTTP NDJSON
Status code: 500
failed to inspect WAL backlog
failed to scan WAL
Too Many
Payload Too Large
```

## Issue Found And Fixed

During the high-rate run, Vector reported intermittent `500 Internal Server Error`.
The adapter log showed:

```text
rejecting HTTP NDJSON logs request status=500 Internal Server Error
failed to ingest logs: failed to inspect WAL backlog
```

Root cause: the local WAL backlog-size check scans `wal/received` while worker tasks
may concurrently remove files after Parquet write. A file could disappear between
`read_dir`, `metadata`, or `open`, causing the backlog inspection to fail.

Fix: local WAL scans now tolerate `NotFound` during directory scanning and JSON
file reads. This removes the transient high-churn `500` behavior.

## Operational Signals

Useful commands while testing:

```sh
ls data/scale-test/parquet/logs | wc -l
du -sh data/scale-test/wal data/scale-test/parquet
ls data/scale-test/wal/received | wc -l
ls data/scale-test/wal/parquet_written | wc -l
ls data/scale-test/wal/committed | wc -l
```

Healthy pattern:

- `wal/received` may rise during bursts but should remain bounded.
- `wal/parquet_written` should usually stay near zero in noop mode.
- `wal/committed` and Parquet file count should continue increasing.
- After traffic stops, `wal/received` should drain to zero.

