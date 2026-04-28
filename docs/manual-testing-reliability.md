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
# Wait for healthy state
docker compose ps
```

The proxy is reachable at `http://localhost:3100` (proxy) and `http://localhost:9428` (VictoriaLogs direct).

---

## 1 — Disk Cache Size Cap (overwrite accounting + lazy expiry eviction)

**What changed:** `Flush()` now uses `LeafInuse` (actual stored bytes) instead of the bbolt file size as the cap baseline. Overwritten keys deduct the old entry size before admission. Expired keys are lazily deleted from bolt on read.

### Test: Overwrite accounting does not shrink write budget

```bash
# Hit the proxy 50 times with the same query (forces compat-cache overwrites)
for i in $(seq 1 50); do
  curl -s "http://localhost:3100/loki/api/v1/labels" \
    -H "X-Scope-OrgID: tenant1" > /dev/null
done

# Scrape metrics — cache evictions should be 0 for repeated same-key writes
curl -s http://localhost:3100/metrics | grep "loki_vl_proxy_cache_"
```

**Expected:** `loki_vl_proxy_cache_evictions_total` does not climb with each overwrite cycle. If the disk cache is configured (`-l2-path`), `loki_vl_proxy_l2_evictions_total` stays at 0 for this working set.

### Test: Expired entries release space for fresh writes

This requires the disk cache to be configured. In the compose override add `-l2-path=/tmp/testcache.db -l2-max-bytes=1048576` to the proxy command, then:

```bash
# Restart proxy with small disk cache
docker compose restart e2e-proxy

# Write some queries that will be cached (they expire after the cache TTL)
for q in "nginx" "api" "auth" "frontend" "batch"; do
  curl -s "http://localhost:3100/loki/api/v1/query_range" \
    -H "X-Scope-OrgID: tenant1" \
    --data-urlencode "query={app=\"$q\"}" \
    --data-urlencode "start=1" \
    --data-urlencode "end=2" \
    --data-urlencode "step=1s" > /dev/null
done

# Wait for cache TTL to expire (default query_range TTL is 30s)
sleep 35

# Confirm metrics show disk cache admitted entries, not all evictions
curl -s http://localhost:3100/metrics | grep -E "l2_(hits|misses|evictions|writes)"
```

**Expected:** After expiry, new writes are admitted without evictions caused by dead space from expired entries.

---

## 2 — Shutdown Goroutine Cleanup

**What changed:** `Proxy.Shutdown` now calls `limiter.Stop()` and `peerCache.Close()` before flushing persistence state.

### Test: Clean shutdown leaves no leaked goroutines

```bash
# Trigger a graceful shutdown and observe the process exit cleanly
docker compose stop e2e-proxy

# Check logs for clean shutdown message (no goroutine leak warnings)
docker compose logs e2e-proxy | tail -20
```

**Expected:** Logs show orderly shutdown — persistence flush, no panics, exit 0.

### Test: Restart cycle works multiple times

```bash
for i in 1 2 3; do
  docker compose restart e2e-proxy
  sleep 2
  curl -sf http://localhost:3100/ready && echo "restart $i OK"
done
```

**Expected:** All three restarts succeed; `/ready` returns 200 each time. Before the fix, goroutine leaks from the rate-limiter cleanup and peer-cache loops could accumulate across restarts if the process was reused (e.g. in embedding scenarios).

---

## 3 — /metrics Streaming (no double-buffer)

**What changed:** `handleMetrics` streams directly to the client via `p.metrics.Handler(w, r)` + `io.WriteString`, eliminating the intermediate `httptest.NewRecorder()` buffer.

### Test: Metrics endpoint responds correctly

```bash
# Basic correctness check
curl -s http://localhost:3100/metrics | head -20

# Confirm Content-Type is set by the Prometheus handler (not the old recorder path)
curl -sI http://localhost:3100/metrics | grep -i content-type
```

**Expected:** `Content-Type: text/plain; version=0.0.4; charset=utf-8` (Prometheus exposition format).

### Test: Concurrent scrapes are handled correctly

```bash
# Fire 5 concurrent scrapes
for i in $(seq 1 5); do
  curl -s http://localhost:3100/metrics | wc -l &
done
wait
```

**Expected:** All 5 complete with similar line counts. The concurrency limiter allows one in-flight scrape; the others return 429 immediately (no deadlock or incomplete responses).

### Test: Peer cache metrics present when peer cache configured

```bash
# The e2e-compat stack does not have peer cache by default.
# If running the fleet stack (test/e2e-fleet):
curl -s http://localhost:3100/metrics | grep "loki_vl_proxy_peer_cache_"
```

**Expected (fleet stack only):** `loki_vl_proxy_peer_cache_hits_total`, `loki_vl_proxy_peer_cache_misses_total`, etc. are present in the output. Before the fix, peer metrics were appended to the recorder buffer and would be silently lost if the buffer copy path was broken.

---

## 4 — Tail Dedup Window Overflow (no allocation per overflow)

**What changed:** `syntheticTailSeen.Add` uses `copy`+reslice instead of `append([]string(nil), ...)`, eliminating a per-overflow heap allocation in the synthetic tail hot path.

### Test: Tail endpoint works under sustained load

```bash
# Open a tail session and verify it stays alive
curl -s -N --max-time 30 \
  "http://localhost:3100/loki/api/v1/tail?query={app%3D\"nginx\"}" &
TAIL_PID=$!

# Inject log entries to trigger tail emissions
for i in $(seq 1 200); do
  curl -s -X POST http://localhost:9428/insert/loki/api/v1/push \
    -H "Content-Type: application/json" \
    -d "{\"streams\":[{\"stream\":{\"app\":\"nginx\"},\"values\":[[\"$(date +%s)000000000\",\"line $i\"]]}]}" \
    > /dev/null
  sleep 0.05
done

wait $TAIL_PID
echo "tail session completed"
```

**Expected:** Tail session receives entries without memory growth visible in `docker stats e2e-proxy`. The dedup window evicts old entries without allocating on each overflow.

### Test: Memory stays flat during sustained synthetic tail

```bash
# Record baseline RSS
BEFORE=$(docker stats --no-stream e2e-proxy --format "{{.MemUsage}}" | awk '{print $1}')

# Run 500 iterations through the tail path (triggers many dedup overflows)
for i in $(seq 1 500); do
  curl -s --max-time 1 \
    "http://localhost:3100/loki/api/v1/tail?query={app%3D\"nginx\"}" \
    > /dev/null 2>&1 || true
done

AFTER=$(docker stats --no-stream e2e-proxy --format "{{.MemUsage}}" | awk '{print $1}')
echo "RSS before: $BEFORE  after: $AFTER"
```

**Expected:** RSS stays flat or grows only marginally (≤5%). Sustained growth would indicate the allocation-per-overflow regression is back.

---

## 5 — Security Headers Survive Backend Response

**What changed:** `copyBackendHeaders` (used on the client-response path) skips proxy-controlled headers, so `withSecurityHeaders`/`SecurityHeadersMiddleware` values cannot be overwritten by whatever the backend returns.

### Test: Security headers present on all endpoints

```bash
# Check multiple endpoint types
for path in \
  "/loki/api/v1/labels" \
  "/loki/api/v1/query_range?query={app%3D%22nginx%22}&start=1&end=2&step=1" \
  "/loki/api/v1/query?query={app%3D%22nginx%22}&time=1" \
  "/ready" \
  "/metrics"; do
  echo "--- $path"
  curl -sI "http://localhost:3100$path" | grep -iE "x-content-type|x-frame|cross-origin|cache-control|pragma|expires"
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
curl -sI "http://localhost:9428/select/logsql/query?query=*&start=1&end=2" | \
  grep -iE "cache-control|x-frame|content-type"

# Query the proxy for the same range — proxy headers must win
curl -sI "http://localhost:3100/loki/api/v1/query_range?query={app%3D%22nginx%22}&start=1&end=2&step=1" | \
  grep -iE "cache-control|x-frame|content-type"
```

**Expected:** Even if VictoriaLogs sets `Cache-Control: max-age=3600`, the proxy response must have `Cache-Control: no-store, no-cache, must-revalidate, max-age=0`. `X-Frame-Options: DENY` must be present regardless of what VL returns.

---

## Cleanup

```bash
cd test/e2e-compat
docker compose down -v
```
