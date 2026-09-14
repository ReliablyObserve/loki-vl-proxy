---
sidebar_label: Reliability Manual Tests
description: Docker Compose manual test runbook for disk cache, shutdown, metrics, tail dedup, and security header fixes.
---

# Reliability Fixes — Manual Docker Compose Test Runbook

This guide covers manual validation of the five reliability fixes using the local compose stack. Run these after the automated unit/integration tests pass.

## Prerequisites

```bash
cd test/e2e-compat
docker compose up -d --build
# Wait for every service to answer its readiness endpoint
../../scripts/ci/wait_e2e_stack.sh 180
```

Endpoints used below:

- `http://localhost:13100` — main proxy (compose service `loki-vl-proxy`, container `e2e-proxy`)
- `http://localhost:13103` — synthetic-tail proxy (compose service `loki-vl-proxy-tail`, container `e2e-proxy-tail`)
- `http://localhost:19428` — VictoriaLogs direct (compose service `victorialogs`)

`docker compose` subcommands take the service name; `docker stats` takes the container name.

The main proxy already runs with the L2 disk cache (`-disk-cache-path=/cache/proxy-l2.bolt`, `-disk-cache-max-bytes=536870912`, `-disk-cache-min-ttl=1s`), `-cache-ttl=5s`, and an L3 static peer ring with `loki-vl-proxy-peer-a` (port 13150) and `loki-vl-proxy-peer-b` (port 13151). Requests use `X-Scope-OrgID: 0`; tenant IDs that are neither numeric, a default-tenant alias, nor mapped with `-tenant-map` are rejected with `403 unknown tenant`.

---

## 1 — Disk Cache Size Cap (overwrite accounting + lazy expiry eviction)

**What changed:** `Flush()` now uses `LeafInuse` (actual stored bytes) instead of the bbolt file size as the cap baseline. Overwritten keys deduct the old entry size before admission. Expired keys are lazily deleted from bolt on read.

### Test: Overwrite accounting does not shrink write budget

```bash
# Hit the proxy 50 times with the same query (repeated writes of the same cache key)
for i in $(seq 1 50); do
  curl -s "http://localhost:13100/loki/api/v1/labels" \
    -H "X-Scope-OrgID: 0" > /dev/null
done

# Scrape the cache tier metrics
curl -s http://localhost:13100/metrics | grep -E "loki_vl_proxy_cache_(bytes|objects|tier_[a-z_]+)\{tier=\"l2_disk\"\}"
```

**Expected:** `loki_vl_proxy_cache_bytes{tier="l2_disk"}` and `loki_vl_proxy_cache_objects{tier="l2_disk"}` stay flat across repeated runs of the loop instead of growing with every overwrite of the same key.

### Test: Expired entries release space for fresh writes

Use a small cap for this test. Temporarily change `-disk-cache-max-bytes=536870912` to `-disk-cache-max-bytes=1048576` and add `-labels-cache-ttl=5s` in the `loki-vl-proxy` service of `test/e2e-compat/docker-compose.yml` (do not commit the change), then:

```bash
# Recreate the main proxy with the small disk cache
docker compose up -d --force-recreate loki-vl-proxy
../../scripts/ci/wait_e2e_stack.sh 180

# Write several cacheable metadata responses (label values TTL is -labels-cache-ttl=5s;
# -cache-ttl does not apply to label endpoints)
for label in app level env service_name namespace; do
  curl -s "http://localhost:13100/loki/api/v1/label/${label}/values" \
    -H "X-Scope-OrgID: 0" > /dev/null
done

# Wait for the entries to expire
sleep 10

# Write a fresh batch, then confirm the disk tier still admits entries
for label in app level env service_name namespace; do
  curl -s "http://localhost:13100/loki/api/v1/label/${label}/values" \
    -H "X-Scope-OrgID: 0" > /dev/null
done
curl -s http://localhost:13100/metrics | grep -E "loki_vl_proxy_cache_(bytes|objects)\{tier=\"l2_disk\"\}"
```

**Expected:** After expiry, new writes are admitted: `loki_vl_proxy_cache_objects{tier="l2_disk"}` is non-zero and `loki_vl_proxy_cache_bytes{tier="l2_disk"}` stays below the 1 MiB cap instead of the tier refusing writes because of dead space from expired entries. Revert the compose change and recreate `loki-vl-proxy` when done.

---

## 2 — Shutdown Goroutine Cleanup

**What changed:** `Proxy.Shutdown` now calls `limiter.Stop()` and `peerCache.Close()` before flushing persistence state.

### Test: Clean shutdown leaves no leaked goroutines

```bash
# Trigger a graceful shutdown and observe the process exit cleanly
docker compose stop loki-vl-proxy

# Check logs for clean shutdown message (no goroutine leak warnings)
docker compose logs loki-vl-proxy | tail -20

# Start it again for the next tests
docker compose start loki-vl-proxy
```

**Expected:** Logs show orderly shutdown — persistence flush, no panics, exit 0.

### Test: Restart cycle works multiple times

```bash
for i in 1 2 3; do
  docker compose restart loki-vl-proxy
  sleep 5
  curl -sf http://localhost:13100/ready && echo "restart $i OK"
done
```

**Expected:** All three restarts succeed; `/ready` returns 200 each time. Before the fix, goroutine leaks from the rate-limiter cleanup and peer-cache loops could accumulate across restarts if the process was reused (e.g. in embedding scenarios).

---

## 3 — /metrics Streaming (no double-buffer)

**What changed:** `handleMetrics` streams directly to the client via `p.metrics.Handler(w, r)` + `io.WriteString`, eliminating the intermediate `httptest.NewRecorder()` buffer.

### Test: Metrics endpoint responds correctly

```bash
# Basic correctness check
curl -s http://localhost:13100/metrics | head -20

# Confirm Content-Type is set by the metrics handler
curl -s -D - -o /dev/null http://localhost:13100/metrics | grep -i content-type
```

**Expected:** `Content-Type: text/plain; version=0.0.4; charset=utf-8` (Prometheus exposition format).

### Test: Concurrent scrapes are handled correctly

```bash
# Fire 5 concurrent scrapes and print each status code
for i in $(seq 1 5); do
  curl -s -o /dev/null -w "%{http_code}\n" http://localhost:13100/metrics &
done
wait
```

**Expected:** Every request completes. `-server.metrics-max-concurrency` defaults to 1, so a scrape that overlaps an in-flight one returns `429` with `Retry-After: 1` immediately; the others return `200` with the full exposition (no deadlock or truncated responses).

### Test: Peer cache metrics present when peer cache configured

```bash
# The main compose proxy is a member of a 3-node static peer ring
curl -s http://localhost:13100/metrics | grep "loki_vl_proxy_peer_cache_"
```

**Expected:** `loki_vl_proxy_peer_cache_hits_total`, `loki_vl_proxy_peer_cache_misses_total`, `loki_vl_proxy_peer_cache_peers`, etc. are present in the output. Before the fix, peer metrics were appended to the recorder buffer and would be silently lost if the buffer copy path was broken.

---

## 4 — Tail Dedup Window Overflow (no allocation per overflow)

**What changed:** `syntheticTailSeen.Add` uses `copy`+reslice instead of `append([]string(nil), ...)`, eliminating a per-overflow heap allocation in the synthetic tail hot path.

`/loki/api/v1/tail` is a WebSocket endpoint, so plain `curl` cannot hold a tail session. The commands below use [`websocat`](https://github.com/vi/websocat) against the synthetic-tail proxy.

### Test: Tail endpoint works under sustained load

```bash
# Open a tail session in the background
websocat -H "X-Scope-OrgID: 0" \
  "ws://localhost:13103/loki/api/v1/tail?query=%7Bapp%3D%22nginx%22%7D" > /tmp/tail-frames.jsonl &
TAIL_PID=$!

# Inject log entries directly into VictoriaLogs to trigger tail emissions
for i in $(seq 1 200); do
  curl -s -X POST http://localhost:19428/insert/loki/api/v1/push \
    -H "Content-Type: application/json" \
    -d "{\"streams\":[{\"stream\":{\"app\":\"nginx\"},\"values\":[[\"$(date +%s)000000000\",\"line $i\"]]}]}" \
    > /dev/null
  sleep 0.05
done

# Give the synthetic tail a few polls to catch up, then close the session
sleep 5
kill $TAIL_PID
wc -l /tmp/tail-frames.jsonl
```

**Expected:** The tail session receives frames without memory growth visible in `docker stats e2e-proxy-tail`. The dedup window evicts old entries without allocating on each overflow.

### Test: Memory stays flat during sustained synthetic tail

```bash
# Record baseline memory
BEFORE=$(docker stats --no-stream e2e-proxy-tail --format "{{.MemUsage}}" | awk '{print $1}')

# Open 500 short tail sessions (triggers many dedup overflows)
for i in $(seq 1 500); do
  websocat -H "X-Scope-OrgID: 0" \
    "ws://localhost:13103/loki/api/v1/tail?query=%7Bapp%3D%22nginx%22%7D" \
    > /dev/null 2>&1 &
  WS_PID=$!
  sleep 0.1
  kill "$WS_PID" 2>/dev/null || true
done

AFTER=$(docker stats --no-stream e2e-proxy-tail --format "{{.MemUsage}}" | awk '{print $1}')
echo "memory before: $BEFORE  after: $AFTER"
```

**Expected:** Memory stays flat or grows only marginally (≤5%). Sustained growth would indicate the allocation-per-overflow regression is back.

---

## 5 — Security Headers Survive Backend Response

**What changed:** `copyBackendHeaders` (used on the client-response path) skips proxy-controlled headers, so `withSecurityHeaders`/`SecurityHeadersMiddleware` values cannot be overwritten by whatever the backend returns.

### Test: Security headers present on all endpoints

```bash
# Check multiple endpoint types (GET requests, headers only)
for path in \
  "/loki/api/v1/labels" \
  "/loki/api/v1/query_range?query=%7Bapp%3D%22nginx%22%7D&start=1&end=2&step=1" \
  "/loki/api/v1/query?query=%7Bapp%3D%22nginx%22%7D&time=1" \
  "/ready" \
  "/metrics"; do
  echo "--- $path"
  curl -s -D - -o /dev/null -H "X-Scope-OrgID: 0" "http://localhost:13100$path" \
    | grep -iE "x-content-type|x-frame|cross-origin|cache-control|pragma|expires"
done
```

**Expected output for each endpoint:**

```
X-Content-Type-Options: nosniff
X-Frame-Options: DENY
Cross-Origin-Resource-Policy: same-origin
Cache-Control: no-store, no-cache, must-revalidate, max-age=0
Pragma: no-cache
Expires: 0
```

### Test: Security headers present even when VL returns custom headers

```bash
# Query VL directly first to see what headers it returns
curl -s -D - -o /dev/null "http://localhost:19428/select/logsql/query?query=*&start=1&end=2" | \
  grep -iE "cache-control|x-frame|content-type"

# Query the proxy for the same range — proxy headers must win
curl -s -D - -o /dev/null -H "X-Scope-OrgID: 0" \
  "http://localhost:13100/loki/api/v1/query_range?query=%7Bapp%3D%22nginx%22%7D&start=1&end=2&step=1" | \
  grep -iE "cache-control|x-frame|content-type"
```

**Expected:** Even if VictoriaLogs sets `Cache-Control: max-age=3600`, the proxy response must have `Cache-Control: no-store, no-cache, must-revalidate, max-age=0`. `X-Frame-Options: DENY` must be present regardless of what VL returns.

---

## Cleanup

```bash
cd test/e2e-compat
docker compose down -v
```
