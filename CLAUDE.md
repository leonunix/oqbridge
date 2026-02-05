# oqbridge - Agent Context

## Project Overview

**oqbridge** is an open-source proxy that sits in front of OpenSearch and Quickwit, providing a unified query interface with automatic hot/cold data tiering.

- **Repository**: git@github.com:leonunix/oqbridge.git
- **Language**: Go
- **License**: TBD (open source)

## Core Concept

```
Client ──► oqbridge (proxy) ──┬──► OpenSearch  (hot data, last 30 days)
                               └──► Quickwit    (cold data, older than 30 days)
```

- **OpenSearch**: Stores hot data (most recent 30 days). Optimized for fast query performance.
- **Quickwit**: Stores cold data (older than 30 days). Trades query performance for significantly lower storage usage and system overhead.

## Functional Requirements

### 1. Proxy Layer
- Accept incoming search/index requests (OpenSearch-compatible API).
- Route queries to the appropriate backend based on the time range of the query.
- Merge results from both backends when a query spans the 30-day boundary.

### 2. Data Tiering
- **Hot tier (OpenSearch)**: Data less than 30 days old.
- **Cold tier (Quickwit)**: Data 30 days or older.
- The retention threshold (default: 30 days) should be configurable.

### 3. Data Migration
- Automatic migration of data from OpenSearch to Quickwit when it exceeds the retention threshold.
- Migration should be performed in the background without impacting query performance.
- Support for index-level or time-based rollover strategies.

### 4. Query Routing
- Parse incoming queries to determine the time range.
- Route to OpenSearch only, Quickwit only, or both backends depending on the time range.
- Merge and deduplicate results when querying both backends.

### 5. Configuration
- YAML-based configuration file.
- Configurable parameters:
  - OpenSearch connection (host, port, auth)
  - Quickwit connection (host, port, auth)
  - Retention threshold (default: 30 days)
  - Migration schedule / batch size
  - Logging level

## Architecture

Two separate binaries for separation of concerns:

- **oqbridge** — Lightweight proxy (deploy anywhere).
- **oqbridge-migrate** — Migration worker (deploy on OpenSearch node to minimize data transfer path).

```
OpenSearch node                        Remote
┌──────────────────────────┐     ┌──────────────┐
│  OpenSearch (localhost)   │     │   Quickwit   │
│          ↑                │     │      ↑       │
│  oqbridge-migrate ───────┼─gzip┼──────┘       │
│  (parallel sliced scroll) │     │              │
└──────────────────────────┘     └──────────────┘

Client ──► oqbridge (proxy) ──┬──► OpenSearch
                               └──► Quickwit
```

```
oqbridge/
├── cmd/
│   ├── oqbridge/
│   │   └── main.go              # Proxy entry point
│   └── oqbridge-migrate/
│       └── main.go              # Migration worker entry point
├── internal/
│   ├── config/
│   │   └── config.go            # Configuration loading and validation
│   ├── proxy/
│   │   ├── proxy.go             # Core proxy logic, HTTP reverse proxy
│   │   ├── router.go            # Query routing (time-range analysis)
│   │   └── merger.go            # Result merging from multiple backends
│   ├── backend/
│   │   ├── backend.go           # Backend interface definition
│   │   ├── opensearch.go        # OpenSearch client (sliced scroll support)
│   │   └── quickwit.go          # Quickwit client (gzip ingest support)
│   ├── migration/
│   │   ├── migrator.go          # Parallel migration orchestrator
│   │   ├── checkpoint.go        # Checkpoint/resume persistence
│   │   └── transformer.go       # Document transformation (_source extraction)
│   └── util/
│       ├── timeutil.go          # Time range parsing utilities
│       └── logger.go            # Logging setup
├── configs/
│   └── oqbridge.yaml            # Default configuration file
├── CLAUDE.md                    # This file - project context for Claude
├── README.md                    # English README (default)
├── README_zh.md                 # Chinese README
├── go.mod
├── go.sum
├── .gitignore
└── Makefile
```

## Code Conventions

- **Language for comments and documentation**: English only.
- **README**: Dual-language — English (README.md, default) and Chinese (README_zh.md). English README links to Chinese version and vice versa.
- **Error handling**: Follow Go idioms — return errors, don't panic.
- **Logging**: Use structured logging (e.g., `slog` or `zerolog`).
- **Testing**: Table-driven tests following Go conventions.
- **Naming**: Follow Go naming conventions (camelCase for unexported, PascalCase for exported).

## Tech Stack

| Component       | Choice                     | Rationale                              |
|----------------|----------------------------|----------------------------------------|
| Language        | Go 1.22+                  | Performance, simplicity, single binary |
| HTTP framework  | `net/http` / `chi`         | Lightweight, stdlib-compatible         |
| Config          | `viper` or `koanf`        | YAML support, env var override         |
| Logging         | `log/slog`                 | Stdlib, structured logging             |
| OpenSearch SDK  | `opensearch-go`            | Official Go client                     |
| Build           | `make` + `go build`       | Standard Go toolchain                  |

## Non-Functional Requirements

- Low latency overhead for proxied requests.
- Graceful shutdown and signal handling.
- Health check endpoint (`/health`).
- Metrics endpoint (Prometheus-compatible, future).
- Docker support (future).

## Development Phases

### Phase 1 - Foundation
- Project scaffolding and configuration.
- Basic proxy that forwards all requests to OpenSearch.
- Health check endpoint.

### Phase 2 - Dual Backend
- Quickwit backend integration.
- Query time-range parsing and routing.
- Result merging.

### Phase 3 - Data Migration
- Background migration scheduler.
- OpenSearch → Quickwit data transfer.
- Migration status tracking.

### Phase 4 - Production Hardening
- Metrics and monitoring.
- Docker image.
- Documentation and examples.
