#!/usr/bin/env bash
# loki-vl-proxy — standalone local run script (flags-based configuration)
#
# Every flag is present; commented lines are at their default value.
# Uncomment and adjust what you need, then:
#
#   chmod +x examples/run-local.sh
#   ./examples/run-local.sh
#
# Build the binary first if you haven't already:
#   go build -o loki-vl-proxy ./cmd/proxy
#
# Alternative: env-file approach (see examples/run-local-env.sh)
#   source examples/loki-vl-proxy-full.env && ./loki-vl-proxy \
#     -emit-structured-metadata=true -patterns-enabled=true

set -euo pipefail

BINARY="${BINARY:-./loki-vl-proxy}"

args=()

# ── Backend ──────────────────────────────────────────────────────────────────
# VictoriaLogs HTTP API address.
args+=( -backend=http://127.0.0.1:9428 )

# Optional: alert/ruler backend for /rules passthrough (e.g., vmalert).
# args+=( -ruler-backend=http://127.0.0.1:8880 )

# Optional: alerts backend for /alerts passthrough.
# Defaults to -ruler-backend when unset.
# args+=( -alerts-backend=http://127.0.0.1:8881 )

# ── Proxy listen address ──────────────────────────────────────────────────────
# Grafana points its Loki datasource here.
args+=( -listen=:3100 )

# ── Loki compatibility — label translation ─────────────────────────────────────
# passthrough  — pass VL field names as-is (VL already stores underscores)
# underscores  — translate OTel dots: service.name → service_name
args+=( -label-style=underscores )

# Field exposure mode for detected_fields and structured metadata.
# translated  — only underscore aliases (matches real Loki; recommended)
# hybrid      — both native VL names and translated aliases (OTel trace correlation)
# native      — VL field names as-is
args+=( -metadata-field-mode=translated )

# Grafana 10+ requires 3-tuple [timestamp, line, metadata] stream values
# for per-line label context in the log details panel.
args+=( -emit-structured-metadata=true )

# ── Patterns (Grafana Logs Drilldown) ─────────────────────────────────────────
# Enable /loki/api/v1/patterns — the Patterns tab in Logs Drilldown.
args+=( -patterns-enabled=true )

# Warm the patterns cache from successful query/query_range responses.
# args+=( -patterns-autodetect-from-queries=true )

# Persist patterns snapshot across restarts.
# args+=( -patterns-persist-path=/tmp/loki-vl-proxy-patterns.json )
# args+=( -patterns-persist-interval=30s )

# ── Tenant routing ─────────────────────────────────────────────────────────────
# Single-tenant (default): X-Scope-OrgID "0", "fake", or "default" all resolve
# to VictoriaLogs 0:0 automatically — no flag needed.

# Named tenants — choose one option:

# Option A — inline JSON (small, static maps):
# args+=( -tenant-map='{"team-alpha":{"account_id":"1","project_id":"1"},"team-beta":{"account_id":"1","project_id":"2"}}' )

# Option B — YAML/JSON file with hot-reload on SIGHUP:
# args+=( -tenant-map-file=./examples/tenant-map.yaml )
# args+=( -tenant-map-reload-interval=30s )

# Label-based routing: inject {<field>="<orgID>"} into VL queries instead
# of AccountID/ProjectID headers.  Use when all data is under VL 0:0 and
# you segregate tenants via a label.  Explicit tenant-map entries take priority.
# args+=( -tenant-label=tenant )

# Reject requests missing X-Scope-OrgID with HTTP 401.
# args+=( -require-tenant-header=false )

# Allow X-Scope-OrgID "*" to bypass tenant scoping entirely.
# args+=( -tenant.allow-global=false )

# ── Tenant limits ──────────────────────────────────────────────────────────────
# Comma-separated fields exposed on /config/tenant/v1/limits.
# args+=( -tenant-limits-allow-publish=query_timeout,max_query_series )

# Default published limit overrides (JSON map).
# args+=( -tenant-default-limits='{"query_timeout":"2m","max_query_series":5000}' )

# Per-tenant published limit overrides keyed by X-Scope-OrgID (JSON map).
# args+=( -tenant-limits='{"team-alpha":{"max_query_series":10000}}' )

# ── In-memory cache (L1) ───────────────────────────────────────────────────────
# args+=( -cache-ttl=60s )
# args+=( -cache-max-bytes=104857600 )   # 100 MiB
# args+=( -cache-disabled=false )        # true = bypass cache (for testing)
# args+=( -coalescer-disabled=false )    # true = measure raw translation overhead

# ── Disk cache (L2) ────────────────────────────────────────────────────────────
# Persist hot cache entries across restarts.  Disabled by default.
# args+=( -disk-cache-path=/tmp/loki-vl-proxy-cache.db )
# args+=( -disk-cache-max-bytes=10737418240 )   # 10 GiB; 0 = unlimited
# args+=( -disk-cache-compress=true )

# ── query_range splitting & parallelism ────────────────────────────────────────
# Split long time-range queries into per-hour windows fetched in parallel.
# Adaptive parallelism adjusts concurrency based on backend latency feedback.
# args+=( -query-range-split-interval=1h )
# args+=( -query-range-adaptive-parallel=true )
# args+=( -query-range-adaptive-min-parallel=2 )
# args+=( -query-range-adaptive-max-parallel=8 )
# args+=( -query-range-history-cache-ttl=24h )

# ── Backend connection ─────────────────────────────────────────────────────────
# args+=( -backend-timeout=120s )
# args+=( -backend-compression=auto )          # auto | gzip | zstd | none
# args+=( -backend-tls-skip-verify=false )
# args+=( -backend-basic-auth=user:password )  # prefer env var in production

# Forward specific upstream headers from Grafana to VictoriaLogs.
# args+=( -forward-headers=X-Custom-Header )
# args+=( -forward-authorization=false )

# ── Custom field mapping ───────────────────────────────────────────────────────
# Override the default OTel→Loki field name translation rules.
# JSON array of {"vl_field":"<name>","loki_label":"<alias>"} objects.
# args+=( -field-mapping='[{"vl_field":"service.name","loki_label":"service_name"}]' )

# Additional VL field names exposed on /loki/api/v1/labels (comma-separated).
# args+=( -extra-label-fields=host.id,custom.pipeline.processing )

# Comma-separated VL _stream_fields labels for stream selector optimisation.
# args+=( -stream-fields=app,env,namespace )

# ── HTTP server hardening ──────────────────────────────────────────────────────
# args+=( -max-concurrent=100 )         # 0 = unlimited
# args+=( -rate-limit-per-second=50 )   # per-client; 0 = disabled
# args+=( -rate-limit-burst=100 )
# args+=( -http-read-timeout=30s )
# args+=( -http-write-timeout=120s )
# args+=( -http-idle-timeout=120s )

# ── TLS server ─────────────────────────────────────────────────────────────────
# Serve HTTPS.  Both cert and key required together.
# args+=( -tls-cert-file=/path/to/tls.crt )
# args+=( -tls-key-file=/path/to/tls.key )
# args+=( -tls-client-ca-file=/path/to/ca.crt )   # optional mTLS
# args+=( -tls-require-client-cert=false )

# ── OTLP metrics push ──────────────────────────────────────────────────────────
# Push proxy metrics to an OTLP collector.  Leave unset to use /metrics scrape.
# args+=( -otlp-endpoint=http://localhost:4318/v1/metrics )
# args+=( -otlp-interval=30s )
# args+=( -otlp-compression=none )         # none | gzip | zstd
# args+=( -otlp-headers=Authorization=Bearer\ tok )

# ── OpenTelemetry identity ─────────────────────────────────────────────────────
# Appears in OTLP metrics and structured log output.
# args+=( -otel-service-name=loki-vl-proxy )
# args+=( -otel-service-namespace=monitoring )
# args+=( -otel-service-instance-id=local )
# args+=( -deployment-environment=development )

# ── Admin / debug ──────────────────────────────────────────────────────────────
# /metrics is always enabled.  pprof and query analytics are opt-in.
# args+=( -server.enable-pprof=false )
# args+=( -server.enable-query-analytics=false )
# args+=( -server.admin-auth-token= )   # require Bearer token on debug endpoints

# ── Go runtime tuning ──────────────────────────────────────────────────────────
# Auto-detects container memory limit via cgroups and sets GOMEMLIMIT to 85%.
# args+=( -go-mem-limit-percent=85 )
# args+=( -go-gc-percent=200 )

# ── Logging ────────────────────────────────────────────────────────────────────
# debug = every translated query and VL response; info = normal operation.
args+=( -log-level=info )

exec "$BINARY" "${args[@]}"
