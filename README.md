# oqbridge

[中文文档](README_zh.md)

A lightweight proxy for automatic hot/cold data tiering between OpenSearch and Quickwit.

## What is oqbridge?

oqbridge sits in front of OpenSearch and Quickwit, providing a unified query interface with transparent data lifecycle management:

- **Hot data** (last 30 days) → stored in **OpenSearch** for fast query performance.
- **Cold data** (older than 30 days) → automatically migrated to **Quickwit** for low storage cost and minimal system overhead.
- **Cold retention** — Quickwit data is automatically deleted after a configurable period (default: 365 days).

## Architecture

oqbridge consists of two separate binaries for clean separation of concerns:

```
OpenSearch node                         Remote
┌───────────────────────────┐     ┌──────────────┐
│  OpenSearch (localhost)   │     │   Quickwit   │
│          ↑                │     │      ↑       │
│  oqbridge-migrate ────────┼─gzip┼──────┘       │
│  (parallel sliced scroll) │     │              │
└───────────────────────────┘     └──────────────┘

Client ──► oqbridge (proxy) ──┬──► OpenSearch  (hot, <30d)
                              └──► Quickwit    (cold, ≥30d, auto-delete after 365d)
```

- **`oqbridge`** — Query proxy. Deploy anywhere. Routes searches to the correct backend based on time range, merges results transparently.
- **`oqbridge-migrate`** — Migration worker. Deploy on the OpenSearch node to read data locally and send to Quickwit over the network, minimizing data transfer path.

## Features

### Proxy (`oqbridge`)

- **Transparent proxy** — Full OpenSearch API compatibility via reverse proxy.
- **Smart query routing** — Automatically routes supported search requests to the correct backend based on time range.
- **Wildcard index support** — Queries like `logs-*/_search` and `*/_search` are correctly routed based on time range, with wildcard resolution for cold-tier queries.
- **Result merging** — Fan-out to both backends in parallel, merge results seamlessly.
- **Configurable retention** — Adjust the hot/cold threshold per index (default: 30 days).
- **Per-index timestamp field** — Different indices can use different timestamp fields.

### Migration (`oqbridge-migrate`)

- **Wildcard index patterns** — Configure `indices: ["*"]` or `["logs-*"]` to migrate matching indices. Wildcard patterns are resolved to concrete index names via the OpenSearch `_cat/indices` API.
- **System index filtering** — Internal OpenSearch indices (`.security`, `security-auditlog-*`, `top_queries-*`, etc.) are automatically excluded from migration.
- **Auto index creation** — Automatically creates Quickwit indices using dynamic (schemaless) mode before migration. No need to pre-define schemas.
- **Cold data retention** — Quickwit indices are created with a retention policy. Data older than `retention.cold_days` is automatically deleted by Quickwit.
- **Parallel sliced scroll** — Multiple workers read from OpenSearch concurrently using sliced scroll API.
- **Gzip compression** — Compress data over the network to Quickwit (significant savings for large volumes).
- **Checkpoint/resume** — Interrupted migrations automatically resume from the last completed slice.
- **Multi-instance safe** — Distributed locking (via OpenSearch) prevents multiple `oqbridge-migrate` instances from migrating the same index concurrently. Checkpoints and watermarks are stored in OpenSearch so all instances share migration progress.
- **Real-time progress** — Logs docs/sec, total migrated, and elapsed time every 10 seconds.
- **Two run modes** — One-shot (`--once`) for crontab, or built-in cron daemon mode.

## Quick Start

### Prerequisites

- Go 1.22+
- A running OpenSearch instance
- A running Quickwit instance

### Build

```bash
make build          # builds both binaries
make build-proxy    # builds oqbridge only
make build-migrate  # builds oqbridge-migrate only
```

### Configuration

Copy and edit the default configuration:

```bash
cp configs/oqbridge.yaml oqbridge.yaml
# Edit oqbridge.yaml with your connection details
```

### Run the Proxy

```bash
./bin/oqbridge -config oqbridge.yaml
```

### Run the Migration Worker

```bash
# One-shot mode (run once and exit, suitable for crontab)
./bin/oqbridge-migrate -config oqbridge.yaml --once

# Daemon mode (built-in cron scheduler)
./bin/oqbridge-migrate -config oqbridge.yaml
```

## Configuration

See [configs/oqbridge.yaml](configs/oqbridge.yaml) for the full configuration reference.

### Proxy Settings

| Parameter | Default | Description |
|-----------|---------|-------------|
| `server.listen` | `:9200` | Proxy listen address |
| `opensearch.url` | `http://localhost:9201` | OpenSearch endpoint |
| `quickwit.url` | `http://localhost:7280` | Quickwit endpoint |
| `retention.days` | `30` | Hot data retention period (days) |
| `retention.cold_days` | `365` | Cold data retention in Quickwit (days, 0 = forever) |
| `retention.timestamp_field` | `@timestamp` | Default timestamp field |
| `retention.index_fields` | — | Per-index timestamp field overrides |
| `retention.index_cold_days` | — | Per-index cold retention overrides (days). Supports exact names or glob patterns (e.g., `security-audit-*: 1095`) |

### Migration Settings

| Parameter | Default | Description |
|-----------|---------|-------------|
| `migration.schedule` | `0 * * * *` | Cron schedule (daemon mode) |
| `migration.migrate_after_days` | `retention.days - 5` | Migrate data older than this (must be < `retention.days`) |
| `migration.batch_size` | `5000` | Documents per scroll batch |
| `migration.workers` | `4` | Parallel sliced scroll workers |
| `migration.compress` | `true` | Gzip compress data to Quickwit |
| `migration.delete_after_migration` | `false` | Delete data from OpenSearch after migration |
| `migration.temp_dir` | — | Directory for staging data on disk during migration. When empty (default), data is buffered in memory. Useful for reducing memory usage with very large `batch_size` |
| `migration.indices` | — | Index patterns to migrate (supports wildcards: `*`, `logs-*`) |

## Data Lifecycle

```text
Day 0          Day 25              Day 30                    Day 395
  │              │                   │                         │
  ▼              ▼                   ▼                         ▼
 Ingest ──► OpenSearch (hot) ──► Migrate to Quickwit ──► Auto-delete from Quickwit
            ◄── retention.days ──►
            ◄── migrate_after_days ►
                                    ◄──── retention.cold_days (365) ────►
```

- **Day 0–25**: Data lives in OpenSearch only (hot tier).
- **Day 25–30**: Data is migrated to Quickwit by `oqbridge-migrate`. The exact boundary is controlled by `migration.migrate_after_days`.
- **Day 30+**: Data is queryable from Quickwit (cold tier). If `delete_after_migration: true`, it is also removed from OpenSearch.
- **Day 395+**: Quickwit automatically deletes data older than `retention.cold_days` (365 days).

## Authentication

oqbridge uses OpenSearch as the single source of truth for user authentication. Users only need OpenSearch credentials (e.g. via OpenSearch Dashboards). Quickwit is accessed internally by oqbridge using a dedicated service account — end users never interact with Quickwit directly.

```text
OpenSearch Dashboards
        │ (user's Authorization header)
        ▼
    oqbridge (proxy)
        │
        ├──► OpenSearch: forwards user's auth header, OpenSearch validates
        │
        └──► Quickwit:   uses service account from config, user is unaware
```

### How it works by query type

| Query type | Authentication flow |
| ---------- | ------------------- |
| **Hot only** (recent data) | Reverse proxy forwards the client's `Authorization` header to OpenSearch. OpenSearch validates the user. |
| **Cold only** (old data) | oqbridge first validates the client's credentials against OpenSearch (`_plugins/_security/authinfo`). Only after successful authentication does it query Quickwit. |
| **Both** (spans hot & cold) | OpenSearch leg validates auth implicitly. Both backends are queried in parallel. |
| **Non-search requests** | Forwarded directly to OpenSearch via reverse proxy. OpenSearch validates. |

## Search API support notes

oqbridge forwards all non-search requests to OpenSearch unchanged. For search interception/tiering it currently supports:

- `/{index}/_search`
- `/{index1,index2}/_search` (comma-separated explicit indices)
- `/{index-pattern*}/_search` (wildcard patterns, resolved for cold-tier queries)
- `/{index}/_msearch`
- `/_msearch` (requires each header line to include `"index"`)

`/_search` (no index in path) is forwarded to OpenSearch as-is.

Wildcard patterns (e.g., `logs-*/_search`) are fully supported for time-range routing. For hot-tier queries, the wildcard is passed to OpenSearch as-is (OpenSearch handles wildcards natively). For cold-tier queries, oqbridge resolves the wildcard against available Quickwit indices and queries only the matching ones.

### Cross-tier merge limitations

When a query spans hot+cold tiers (fan-out + merge), oqbridge currently supports only score-based ordering:

- Default ordering (no `sort`)
- Explicit `_score` sort

Queries using explicit non-`_score` sorts, `search_after`, or PIT are rejected with `400` for tiered (cross-tier) merging, because correct global ordering requires full sort-key merge semantics.

### Service accounts

- `opensearch.username` / `opensearch.password` — **Service account** for `oqbridge-migrate` background operations (scroll, delete). The proxy does NOT use these for user requests; it forwards the original client headers instead.
- `quickwit.username` / `quickwit.password` — **Service account** for all Quickwit access (both proxy and migrate). If Quickwit has no auth (e.g. network-isolated), leave empty.

### What you do NOT need to do

- No need to create users in Quickwit
- No need to sync user databases between OpenSearch and Quickwit
- No changes required to OpenSearch Dashboards

## Deployment

### Recommended Setup

1. **OpenSearch node**: Deploy `oqbridge-migrate` alongside OpenSearch. It reads locally (fast), compresses, and sends to remote Quickwit.
2. **Any node**: Deploy `oqbridge` proxy where your clients can reach it. It routes queries to both backends.

Both binaries share the same config file format.

### Multi-Instance Migration

You can safely run multiple `oqbridge-migrate` instances against the same OpenSearch cluster. Coordination is fully automatic:

1. **Distributed lock** — Before migrating an index, an instance acquires a lock stored in the `.oqbridge-locks` OpenSearch index (using atomic `op_type=create`). If another instance already holds the lock, the index is skipped.
2. **Shared checkpoint/watermark** — Migration progress is stored in the `.oqbridge-state` OpenSearch index, not local files. Every instance sees what time range has already been migrated and resumes from there.
3. **Stale lock cleanup** — Locks include a TTL (default: 2 hours). If an instance crashes, the lock expires and another instance can take over, resuming from the last shared checkpoint.

```text
Instance A: acquire lock → migrate [T0, T1) → save watermark T1 → release lock
Instance B: acquire lock → read watermark T1 → migrate [T1, T2) → save watermark T2 → release lock
```

No external coordination service (etcd, Consul, etc.) is required — OpenSearch itself is used as the coordination backend.

## License

[MIT](LICENSE)

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.
