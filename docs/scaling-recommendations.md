# Scaling Recommendations

This document captures scaling guidance from the local Vector-to-adapter load test
and expected production behavior when moving from local noop mode to S3 + Iceberg
catalog commits.

## Separate The Batching Layers

Vector and the adapter control different batch boundaries.

Vector controls HTTP request size:

```text
events -> Vector sink batch -> HTTP request
```

The adapter controls Parquet file size:

```text
many HTTP requests -> adapter batcher -> Arrow RecordBatch -> Parquet file
```

Therefore, final Parquet file size is primarily an adapter concern, not a Vector
concern. Vector can send 512 KB or 1 MB NDJSON requests, while the adapter can
accumulate many of those requests into 128 MB to 512 MB Parquet files.

## Adapter Flush Conditions

The adapter flushes a log batch when any of these conditions is met:

```text
rows >= batch.max_rows
estimated bytes >= batch.max_bytes
age >= batch.max_age_secs
```

This means `batch.max_age_secs` can force smaller files even when
`batch.max_bytes` is large.

With parallel log workers, each worker owns an independent batcher. If
`pipeline.log_workers = 4`, each worker sees roughly one quarter of the total log
volume. File-size tuning should account for this per-worker rate.

Example at 100k logs/sec, 500 bytes/log, and 4 log workers:

```text
total volume       ~= 50 MB/sec
per-worker volume  ~= 12.5 MB/sec
256 MB file target ~= 20 seconds per worker
```

If `batch.max_age_secs = 10`, the worker will flush by time before reaching a
256 MB target. Use a larger age threshold for high-volume production streams.

## Production Starting Point

For a high-volume log stream, a reasonable initial adapter config is:

```toml
[limits]
max_http_body_bytes = 67108864      # 64 MB
max_pending_wal_bytes = 2147483648  # 2 GB, tune per disk and retry budget

[batch]
max_rows = 1000000
max_bytes = 268435456               # 256 MB target
max_age_secs = 60

[pipeline]
log_workers = 4
```

For Vector, keep request batches large enough to amortize HTTP overhead but small
enough to avoid large retries and memory spikes:

```toml
[sinks.http_ndjson.batch]
max_bytes = 1048576   # 1 MB tested locally
timeout_secs = 1

[sinks.http_ndjson.request]
in_flight_limit = 4
timeout_secs = 30
retry_max_duration_secs = 600
```

For production, `2 MB` to `5 MB` Vector batches may be reasonable, but should be
validated with the chosen WAL backend and deployment environment. Keep Vector
`batch.max_bytes` below adapter `limits.max_http_body_bytes`.

## What Local Testing Proved

The local test validated:

```text
HTTP ingest -> WAL append -> decode -> batch -> Arrow -> local Parquet -> noop commit
```

It did not validate:

```text
S3 upload latency
Glue/Iceberg commit latency
Iceberg metadata growth
IAM or S3 throttling behavior
network path through VPC endpoints/NAT
```

The local test reached 100k logs/sec with Vector 1 MB batches, local WAL, local
Parquet, `catalog.type = "noop"`, and 4 log workers. WAL backlog remained bounded
in the sampled window after fixing a local WAL scan race.

## Moving To S3 And Glue

Running in AWS, same region, and same VPC is favorable:

- EC2/EKS to S3 in-region should have strong throughput.
- An S3 VPC endpoint avoids NAT gateway bottlenecks and transfer cost surprises.
- Glue in the same region avoids cross-region control-plane latency.
- IAM roles should be used instead of static credentials.

The expected new bottleneck is not HTTP ingest. More likely bottlenecks are:

- S3 upload latency and throughput.
- Glue/Iceberg commit latency.
- Too many small Parquet files.
- Too many Iceberg snapshots/metadata commits.
- Local WAL disk throughput if using disk-backed WAL.
- Pending commit manifests growing faster than the commit coordinator can drain.

## Recommended Next Production Hardening

### 1. Commit Coalescing

Currently, each flushed batch stages a manifest and the commit coordinator commits
manifests serially. In Glue Iceberg mode, committing every small file separately
can become the limiting factor.

Recommended improvement:

```text
group pending manifests by table
combine many staged data files
commit them in one Iceberg snapshot
```

This keeps Iceberg metadata healthier and reduces Glue catalog pressure.

### 2. Queue-Aware Backpressure

Current backpressure primarily considers pending received WAL bytes. Production
backpressure should also consider:

- pending manifest count
- pending manifest bytes
- commit lag
- worker queue depth
- oldest uncommitted age

This lets the adapter slow Vector before the commit path becomes dangerously
behind.

### 3. WAL Backend Choice

For local development, disk-backed WAL is fine. For production:

- Use persistent storage if using local WAL.
- Prefer Kafka WAL if containers can be rescheduled and local disk durability is
  not guaranteed.
- Avoid relying on ephemeral container filesystems for durable at-least-once
  delivery.

### 4. Metrics And Observability

Expose metrics for:

- HTTP requests accepted/rejected by status
- request body bytes
- pending WAL bytes
- pending WAL record count
- worker queue depth
- Parquet write latency
- S3 upload latency
- staged manifest count
- commit latency
- commit retry/conflict count
- rows/files committed per snapshot

These are required to make safe scaling decisions in production.

## Practical Sizing Guidance

At 100k logs/sec and 500 bytes/log:

```text
raw volume ~= 50 MB/sec
```

With 4 workers:

```text
per-worker raw volume ~= 12.5 MB/sec
```

Approximate flush interval by file target:

| Parquet target | Per-worker flush interval |
|----------------|---------------------------|
| 128 MB | ~10 seconds |
| 256 MB | ~20 seconds |
| 512 MB | ~40 seconds |

Set `batch.max_age_secs` higher than the expected interval if file size is more
important than low-latency visibility. Set it lower if freshness is more
important and smaller files are acceptable.

## Recommended AWS Test Plan

1. Run local noop test to validate decode/batch/Parquet CPU path.
2. Run S3 upload with `catalog.type = "noop"` to isolate S3 upload throughput.
3. Run Glue Iceberg at 30k/sec and inspect commit latency and snapshot count.
4. Increase to 60k/sec, then 100k/sec.
5. Confirm WAL received backlog, pending manifests, and commit lag remain bounded.
6. Tune `batch.max_bytes`, `batch.max_age_secs`, and `pipeline.log_workers`.
7. Add commit coalescing before making a production 100k/sec claim.

