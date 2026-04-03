# Loki-VL-proxy

HTTP proxy that exposes a **Loki-compatible API** on the frontend and translates requests to **VictoriaLogs** on the backend. This allows using Grafana's native Loki datasource with VictoriaLogs — no custom datasource plugin needed.

## Architecture

```
Grafana (Loki datasource)
    │
    ▼
┌──────────────────┐
│  Loki-VL-proxy   │
│  :3100           │
│                  │
│  ┌─────────────┐ │
│  │  LogQL →    │ │
│  │  LogsQL     │ │
│  │  translator │ │
│  ├─────────────┤ │
│  │  Response   │ │
│  │  converter  │ │
│  ├─────────────┤ │
│  │  TTL cache  │ │
│  ├─────────────┤ │
│  │  /metrics   │ │
│  │  JSON logs  │ │
│  └─────────────┘ │
└────────┬─────────┘
         │
         ▼
   VictoriaLogs
    :9428
```

## Features

- Translates LogQL queries to LogsQL (stream selectors, line filters, label filters, parsers, metric queries)
- Converts VictoriaLogs responses to Loki-compatible format
- In-memory TTL cache for label names, label values, and metadata queries
- Prometheus metrics at `/metrics` for monitoring
- Structured JSON logs to stdout (slog)
- Single static binary, ~10MB Docker image

## API Coverage

| Loki Endpoint | Status | VL Backend |
|---|---|---|
| `/loki/api/v1/query_range` | Implemented | `/select/logsql/query` or `/select/logsql/stats_query_range` |
| `/loki/api/v1/query` | Implemented | `/select/logsql/query` or `/select/logsql/stats_query` |
| `/loki/api/v1/labels` | Implemented + cached | `/select/logsql/field_names` |
| `/loki/api/v1/label/{name}/values` | Implemented + cached | `/select/logsql/field_values` |
| `/loki/api/v1/series` | Implemented | `/select/logsql/streams` |
| `/loki/api/v1/index/stats` | Stub | — |
| `/loki/api/v1/index/volume` | Stub | — |
| `/loki/api/v1/index/volume_range` | Stub | — |
| `/loki/api/v1/detected_fields` | Implemented | `/select/logsql/field_names` |
| `/loki/api/v1/patterns` | Stub | — |
| `/loki/api/v1/tail` | Not yet | `/select/logsql/tail` |
| `/ready` | Implemented | `/health` |
| `/loki/api/v1/status/buildinfo` | Implemented | — |
| `/metrics` | Implemented | — |

## LogQL Translation Reference

| LogQL | LogsQL |
|---|---|
| `{app="nginx"}` | `{app="nginx"}` |
| `\|= "error"` | `"error"` |
| `!= "debug"` | `-"debug"` |
| `\|~ "err.*"` | `~"err.*"` |
| `!~ "debug.*"` | `NOT ~"debug.*"` |
| `\| json` | `\| unpack_json` |
| `\| logfmt` | `\| unpack_logfmt` |
| `\| pattern "<ip> ..."` | `\| extract "<ip> ..."` |
| `\| regexp "..."` | `\| extract_regexp "..."` |
| `\| line_format "{{.x}}"` | `\| format "<x>"` |
| `\| label_format x="{{.y}}"` | `\| format "<y>" as x` |
| `\| drop a, b` | `\| delete a, b` |
| `\| keep a, b` | `\| fields a, b` |
| `\| label == "val"` | `label:=val` |
| `\| label != "val"` | `-label:=val` |
| `rate({...}[5m])` | `{...} \| stats rate()` |
| `count_over_time({...}[5m])` | `{...} \| stats count()` |
| `sum(rate({...}[5m])) by (x)` | `{...} \| stats by (x) rate()` |

Full reference: https://docs.victoriametrics.com/victorialogs/logql-to-logsql/

## Quick Start

```bash
# Build and run locally
go build -o loki-vl-proxy ./cmd/proxy
./loki-vl-proxy -backend=http://your-victorialogs:9428

# Docker
docker build -t loki-vl-proxy .
docker run -p 3100:3100 loki-vl-proxy -backend=http://victorialogs:9428

# Docker Compose (with VictoriaLogs + Grafana)
docker-compose up -d
# Open Grafana at http://localhost:3000, Loki datasource pre-configured
```

## Configuration

| Flag | Env | Default | Description |
|---|---|---|---|
| `-listen` | `LISTEN_ADDR` | `:3100` | Listen address |
| `-backend` | `VL_BACKEND_URL` | `http://localhost:9428` | VictoriaLogs backend URL |
| `-cache-ttl` | — | `60s` | Cache TTL for metadata queries |
| `-cache-max` | — | `10000` | Maximum cache entries |
| `-log-level` | — | `info` | Log level: debug, info, warn, error |

## Observability

### Metrics (Prometheus)

Scrape `GET /metrics` — exposes:
- `loki_vl_proxy_requests_total{endpoint, status}` — request counter
- `loki_vl_proxy_request_duration_seconds{endpoint}` — latency histogram
- `loki_vl_proxy_cache_hits_total` / `cache_misses_total`
- `loki_vl_proxy_translations_total` / `translation_errors_total`
- `loki_vl_proxy_uptime_seconds`

### Logs

Structured JSON logs to stdout via Go's `slog`.

## E2E Compatibility Tests

```bash
cd test/e2e-compat
docker-compose up -d
# Wait for services to start
go test -v -timeout=120s ./test/e2e-compat/
```

Tests ingest identical logs into both Loki and VictoriaLogs, then compare API responses to measure compatibility.

## Development

```bash
go test ./...                    # Unit tests
go build ./cmd/proxy             # Build binary
docker-compose up -d             # Full stack test
```
