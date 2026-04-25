#!/usr/bin/env python3
"""
Continuous realistic log generator for e2e testing.

Dual-writes to Loki + VictoriaLogs every INTERVAL seconds.
Produces 10 diverse services with realistic patterns, varied labels,
and message formats that exercise Explore, Drilldown pattern detection,
and proxy parity regression tests.

Services (all prod/data/ml/batch/ingress-nginx namespaces, no staging to avoid test conflicts):
  api-gateway      JSON HTTP access logs  (prod, us-east-1 + us-west-2)
  payment-service  logfmt transactions    (prod, us-east-1)
  auth-service     JSON auth events       (prod, us-east-1)
  nginx-ingress    nginx combined format  (ingress-nginx, us-east-1)
  worker-service   logfmt job queue       (prod, us-east-1)
  db-postgres      postgres log format    (data, us-east-1)
  cache-redis      logfmt cache ops       (data, us-east-1)
  frontend-ssr     JSON page events       (prod, us-east-1 + us-west-2)
  batch-etl        JSON batch jobs        (batch, us-east-1)
  ml-serving       JSON inference         (ml, us-east-1)
"""

import json
import random
import string
import time
import urllib.request
import urllib.error
import os
import sys
from datetime import datetime, timezone

LOKI_URL   = os.getenv("LOKI_URL",   "http://loki:3100")
VL_URL     = os.getenv("VL_URL",     "http://victorialogs:9428")
INTERVAL   = int(os.getenv("LOG_INTERVAL", "10"))   # seconds between batches
BATCH_SIZE = int(os.getenv("LOG_BATCH",    "8"))     # lines per service per batch

# ── Helpers ────────────────────────────────────────────────────────────────────

def ns() -> str:
    """Current time as nanosecond epoch string (Loki push format)."""
    return str(int(time.time() * 1_000_000_000))

def rand_id(length=8) -> str:
    return ''.join(random.choices(string.hexdigits[:16], k=length))

def rand_user() -> str:
    return random.choice([
        "usr_42", "usr_99", "usr_01", "usr_77", "usr_123",
        "usr_456", "usr_789", "usr_333", "usr_555", "usr_888",
    ])

def rand_ip() -> str:
    return f"10.{random.randint(0,5)}.{random.randint(1,254)}.{random.randint(1,254)}"

def rand_pod(app: str, suffix: str = "") -> str:
    return f"{app}-{rand_id(7)[:5]}-{rand_id(5)[:4]}{suffix}"

def push(payload: dict) -> bool:
    """Push to Loki. Returns True on success."""
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"{LOKI_URL}/loki/api/v1/push",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5):
            return True
    except Exception as e:
        print(f"[WARN] Loki push failed: {e}", file=sys.stderr)
        return False

def push_vl(payload: dict) -> bool:
    """Push to VictoriaLogs (same JSON format as Loki push)."""
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"{VL_URL}/insert/loki/api/v1/push",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5):
            return True
    except Exception as e:
        print(f"[WARN] VL push failed: {e}", file=sys.stderr)
        return False

def dual_push(streams: list):
    """Push a list of stream dicts to both Loki and VictoriaLogs."""
    payload = {"streams": streams}
    push(payload)
    push_vl(payload)

def stream(labels: dict, lines: list[str]) -> dict:
    """Build a Loki stream object from labels + log lines."""
    ts = ns()
    return {
        "stream": labels,
        "values": [[ts, line] for line in lines],
    }

# ── Log line generators ────────────────────────────────────────────────────────

METHODS  = ["GET", "POST", "PUT", "PATCH", "DELETE"]
API_PATHS = [
    "/api/v1/users", "/api/v1/users/{id}", "/api/v1/orders",
    "/api/v1/orders/{id}", "/api/v1/products", "/api/v1/cart",
    "/api/v1/payments", "/api/v1/subscriptions", "/api/v1/webhooks",
    "/api/v2/search", "/api/v2/recommendations", "/health", "/ready", "/metrics",
]
STATUS_WEIGHTS = [200]*60 + [201]*15 + [204]*5 + [400]*6 + [401]*4 + \
                 [403]*3 + [404]*4 + [429]*2 + [500]*2 + [502]*1 + [503]*1

def gen_api_gateway(n: int) -> list[str]:
    lines = []
    for _ in range(n):
        method = random.choice(METHODS)
        path   = random.choice(API_PATHS).replace("{id}", str(random.randint(1, 99999)))
        status = random.choice(STATUS_WEIGHTS)
        dur    = random.randint(1, 5000) if status >= 500 else random.randint(1, 300)
        level  = "error" if status >= 500 else ("warn" if status >= 400 else "info")
        entry  = {
            "service": {"name": "api-gateway"},
            "method": method, "path": path, "status": status,
            "duration_ms": dur, "trace_id": rand_id(12),
            "user_id": rand_user(), "level": level,
            "region": random.choice(["us-east-1", "us-west-2", "eu-west-1"]),
            "version": random.choice(["v1", "v2"]),
        }
        if status >= 400:
            entry["error"] = random.choice([
                "user not found", "rate limit exceeded", "invalid token",
                "connection refused", "gateway timeout", "service unavailable",
            ])
        lines.append(json.dumps(entry))
    return lines

def gen_payment_service(n: int) -> list[str]:
    lines = []
    for _ in range(n):
        evt = random.choices(
            ["processed", "failed", "refunded", "disputed", "retrying"],
            weights=[70, 10, 8, 2, 10],
        )[0]
        amount = round(random.uniform(0.99, 9999.99), 2)
        currency = random.choice(["USD", "EUR", "GBP", "JPY", "CAD"])
        provider = random.choice(["stripe", "paypal", "braintree", "adyen"])
        latency  = random.randint(50, 8000)
        if evt == "processed":
            lines.append(
                f'level=info msg="payment {evt}" tx_id=tx_{rand_id(8)} '
                f'amount={amount} currency={currency} provider={provider} '
                f'latency_ms={latency} user_id={rand_user()}'
            )
        elif evt == "failed":
            code = random.choice(["card_declined", "insufficient_funds",
                                   "expired_card", "fraud_detected", "provider_error"])
            lines.append(
                f'level=error msg="payment {evt}" tx_id=tx_{rand_id(8)} '
                f'amount={amount} currency={currency} provider={provider} '
                f'error_code={code} user_id={rand_user()}'
            )
        elif evt == "retrying":
            attempt = random.randint(1, 5)
            lines.append(
                f'level=warn msg="payment retry" tx_id=tx_{rand_id(8)} '
                f'attempt={attempt} max_retries=5 provider={provider} '
                f'reason=timeout latency_ms={latency}'
            )
        else:
            lines.append(
                f'level=info msg="payment {evt}" tx_id=tx_{rand_id(8)} '
                f'amount={amount} currency={currency} provider={provider} '
                f'user_id={rand_user()}'
            )
    return lines

def gen_auth_service(n: int) -> list[str]:
    lines = []
    events = ["login", "logout", "token_refresh", "password_reset",
              "mfa_challenge", "session_expired", "api_key_created"]
    for _ in range(n):
        evt    = random.choice(events)
        uid    = rand_user()
        ip     = rand_ip()
        method = random.choice(["password", "oauth", "sso", "api_key"])
        ok     = random.random() > 0.05
        entry  = {
            "service": {"name": "auth-service"},
            "event": evt, "user_id": uid, "ip": ip, "success": ok,
            "auth_method": method, "mfa": random.choice([True, False]),
            "session_id": rand_id(16), "trace_id": rand_id(12),
        }
        if not ok:
            entry["failure_reason"] = random.choice([
                "wrong_password", "account_locked", "mfa_timeout",
                "too_many_attempts", "invalid_token",
            ])
            entry["level"] = "warn"
        else:
            entry["level"] = "info"
        lines.append(json.dumps(entry))
    return lines

def gen_nginx_ingress(n: int) -> list[str]:
    """Nginx combined log format — classic regex extraction target."""
    lines = []
    uas = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'curl/8.4.0', 'python-requests/2.31.0', 'Go-http-client/1.1',
        'Datadog Agent/7.52.0', 'kube-probe/1.28', 'Prometheus/2.47',
        'sqlmap/1.7.8#stable (https://sqlmap.org)',
    ]
    for _ in range(n):
        ip     = rand_ip()
        user   = random.choice(["-", "admin", "svc-account"])
        method = random.choice(METHODS)
        path   = random.choice(API_PATHS).replace("{id}", str(random.randint(1, 9999)))
        status = random.choice(STATUS_WEIGHTS)
        size   = random.randint(0, 102400)
        lat    = random.randint(1, 5000) / 1000.0
        ua     = random.choice(uas)
        now    = datetime.now(timezone.utc).strftime("%d/%b/%Y:%H:%M:%S +0000")
        lines.append(
            f'{ip} - {user} [{now}] "{method} {path} HTTP/1.1" '
            f'{status} {size} "-" "{ua}" {lat:.3f}'
        )
    return lines

def gen_worker_service(n: int) -> list[str]:
    lines = []
    queues = ["email", "sms", "webhook", "export", "import",
              "notifications", "analytics", "cleanup"]
    for _ in range(n):
        queue  = random.choice(queues)
        job_id = f"job_{rand_id(8)}"
        worker = f"w-{random.randint(1, 8)}"
        dur    = random.randint(50, 30000)
        evt    = random.choices(
            ["started", "completed", "failed", "retry", "enqueued", "dequeued"],
            weights=[15, 60, 8, 7, 5, 5],
        )[0]
        if evt == "completed":
            lines.append(
                f'level=info msg="job {evt}" queue={queue} job_id={job_id} '
                f'duration_ms={dur} worker={worker} retries=0'
            )
        elif evt == "failed":
            err = random.choice([
                "provider_unavailable", "timeout", "rate_limited",
                "invalid_payload", "dependency_failed",
            ])
            lines.append(
                f'level=error msg="job {evt}" queue={queue} job_id={job_id} '
                f'worker={worker} error="{err}" attempt={random.randint(1,5)}'
            )
        elif evt == "retry":
            lines.append(
                f'level=warn msg="job {evt}" queue={queue} job_id={job_id} '
                f'attempt={random.randint(1, 4)} max_retries=5 '
                f'backoff_ms={random.choice([100, 500, 1000, 5000])}'
            )
        else:
            lines.append(
                f'level=info msg="job {evt}" queue={queue} job_id={job_id} '
                f'worker={worker}'
            )
    return lines

def gen_postgres(n: int) -> list[str]:
    lines = []
    tables = ["users", "orders", "products", "payments", "sessions",
              "audit_log", "events", "notifications"]
    for _ in range(n):
        pid  = random.randint(10000, 99999)
        db   = random.choice(["app_prod", "app_reporting", "app_analytics"])
        evt  = random.choices(
            ["slow_query", "connection", "checkpoint", "lock_wait",
             "autovacuum", "error"],
            weights=[30, 20, 15, 15, 10, 10],
        )[0]
        now  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        if evt == "slow_query":
            table = random.choice(tables)
            dur   = round(random.uniform(100, 30000), 3)
            lines.append(
                f'{now} [{pid}] {db} LOG: duration: {dur} ms  '
                f'statement: SELECT * FROM {table} WHERE id = $1 LIMIT $2'
            )
        elif evt == "connection":
            action = random.choice(["connection received", "connection authorized",
                                     "disconnection"])
            lines.append(
                f'{now} [{pid}] {db} LOG: {action}: '
                f'host={rand_ip()} port={random.randint(40000, 65000)}'
            )
        elif evt == "checkpoint":
            lines.append(
                f'{now} [{pid}] LOG: checkpoint complete: '
                f'wrote {random.randint(10, 5000)} buffers '
                f'({round(random.uniform(0.1, 50.0), 1)}%); '
                f'distance={random.randint(1000, 500000)} blks'
            )
        elif evt == "lock_wait":
            lines.append(
                f'{now} [{pid}] {db} LOG: process {pid} still waiting for '
                f'ShareLock on transaction {random.randint(1000000, 9999999)} '
                f'after {round(random.uniform(1000, 30000), 3)} ms'
            )
        elif evt == "autovacuum":
            table = random.choice(tables)
            lines.append(
                f'{now} [{pid}] {db} LOG: automatic vacuum of table '
                f'"{db}.public.{table}": index scans: {random.randint(0, 3)}, '
                f'pages: {random.randint(0, 500)} removed'
            )
        else:
            lines.append(
                f'{now} [{pid}] {db} ERROR: '
                + random.choice([
                    "deadlock detected",
                    "could not serialize access due to concurrent update",
                    "canceling statement due to conflict with recovery",
                    "SSL connection has been closed unexpectedly",
                ])
            )
    return lines

def gen_redis(n: int) -> list[str]:
    lines = []
    key_prefixes = ["user:", "session:", "product:", "cart:", "rate_limit:",
                    "feature_flag:", "cache:query:", "lock:"]
    for _ in range(n):
        prefix  = random.choice(key_prefixes)
        key_id  = rand_id(6)
        key     = f"{prefix}{key_id}"
        ttl     = random.randint(0, 86400)
        size    = random.randint(0, 65536)
        hit     = random.random() > 0.25
        op      = random.choices(
            ["get", "set", "del", "expire", "hget", "hset", "lpush", "zadd"],
            weights=[40, 20, 5, 10, 10, 5, 5, 5],
        )[0]
        level = "debug" if hit else "info"
        if op in ["get", "hget"]:
            result = "hit" if hit else "miss"
            lines.append(
                f'level={level} msg="cache {result}" key={key} '
                f'ttl={ttl}s size_bytes={size if hit else 0} op={op}'
            )
        elif op == "del":
            lines.append(f'level=debug msg="cache evict" key={key} reason=explicit')
        elif op == "expire":
            lines.append(
                f'level=debug msg="cache expire" key={key} '
                f'ttl={ttl}s reason={"ttl_reset" if hit else "expired"}'
            )
        else:
            lines.append(
                f'level=debug msg="cache write" key={key} op={op} '
                f'ttl={ttl}s size_bytes={size}'
            )
    return lines

def gen_frontend_ssr(n: int) -> list[str]:
    lines = []
    pages = [
        "/", "/dashboard", "/orders", "/products", "/profile",
        "/checkout", "/cart", "/search", "/settings", "/admin",
        "/api-docs", "/onboarding",
    ]
    events = ["page_view", "page_error", "api_call", "hydration",
              "navigation", "form_submit"]
    for _ in range(n):
        evt     = random.choices(events, weights=[40, 5, 25, 10, 15, 5])[0]
        path    = random.choice(pages)
        uid     = rand_user()
        sess    = rand_id(16)
        load_ms = random.randint(50, 5000)
        entry   = {
            "service": {"name": "frontend-ssr"},
            "event": evt, "path": path, "user_id": uid,
            "session_id": sess, "load_ms": load_ms,
            "region": random.choice(["us-east-1", "us-west-2"]),
        }
        if evt == "page_error":
            entry["error"]  = random.choice([
                "ChunkLoadError", "TypeError: Cannot read property",
                "Network request failed", "Hydration mismatch",
            ])
            entry["level"]  = "error"
        elif evt == "api_call":
            entry["api"]     = random.choice(API_PATHS).replace("{id}", "1")
            entry["status"]  = random.choice([200, 200, 200, 400, 500])
            entry["api_ms"]  = random.randint(5, 500)
            entry["level"]   = "warn" if entry["status"] >= 400 else "info"
        else:
            entry["level"]   = "info"
        lines.append(json.dumps(entry))
    return lines

def gen_batch_etl(n: int) -> list[str]:
    lines = []
    jobs = ["sync-users", "export-orders", "aggregate-metrics",
            "cleanup-sessions", "reindex-search", "backup-db",
            "compute-recommendations", "send-digests"]
    for _ in range(n):
        job      = random.choice(jobs)
        batch_id = f"batch_{rand_id(6)}"
        total    = random.randint(100, 100000)
        done     = random.randint(0, total)
        failed   = random.randint(0, max(1, total // 100))
        phase    = random.choice(["extract", "transform", "load", "validate",
                                  "complete", "failed"])
        dur      = round(random.uniform(0.5, 300.0), 2)
        entry    = {
            "service": {"name": "batch-etl"},
            "job": job, "batch_id": batch_id, "phase": phase,
            "total": total, "processed": done, "failed": failed,
            "duration_s": dur, "throughput_rps": round(done / max(dur, 0.1), 1),
        }
        if phase == "failed":
            entry["error"] = random.choice([
                "upstream_timeout", "schema_mismatch", "disk_full",
                "connection_pool_exhausted",
            ])
            entry["level"] = "error"
        elif failed > 0:
            entry["level"] = "warn"
        else:
            entry["level"] = "info"
        lines.append(json.dumps(entry))
    return lines

def gen_ml_serving(n: int) -> list[str]:
    lines = []
    models = ["rec-v3", "fraud-v2", "search-rank-v5",
              "sentiment-v1", "forecast-v4", "anomaly-v2"]
    for _ in range(n):
        model     = random.choice(models)
        req_id    = rand_id(12)
        lat_ms    = random.randint(2, 800)
        conf      = round(random.uniform(0.5, 0.99), 4)
        batch     = random.randint(1, 64)
        ok        = random.random() > 0.02
        entry     = {
            "service": {"name": "ml-serving"},
            "model": model, "request_id": req_id,
            "latency_ms": lat_ms, "confidence": conf,
            "batch_size": batch, "success": ok,
            "gpu_util_pct": random.randint(20, 98),
            "queue_depth": random.randint(0, 50),
        }
        if not ok:
            entry["error"] = random.choice([
                "model_timeout", "oom_killed", "input_shape_mismatch",
                "gpu_unavailable",
            ])
            entry["level"] = "error"
        elif lat_ms > 500:
            entry["level"] = "warn"
        else:
            entry["level"] = "info"
        lines.append(json.dumps(entry))
    return lines

# ── Service definitions ────────────────────────────────────────────────────────

def make_labels(app: str, namespace: str, cluster: str, level: str,
                pod_sfx: str = "", extra: dict | None = None) -> dict:
    """Build a full label set matching what Grafana Logs Drilldown expects."""
    labels = {
        # Both service_name AND app for full compatibility with all queries
        "service_name": app,
        "app":          app,
        "namespace":    namespace,
        "cluster":      cluster,
        "env":          "production" if namespace not in ("staging", "dev") else namespace,
        "level":        level,
        "pod":          rand_pod(app, pod_sfx),
        "container":    app.split("-")[0],
    }
    if extra:
        labels.update(extra)
    return labels

def services_batch(n: int) -> list[dict]:
    """Generate one batch of streams for all services."""
    streams = []

    # ── api-gateway us-east-1 ──────────────────────────────────────────────
    for level, lines in [
        ("info",  gen_api_gateway(max(1, n * 6 // 10))),
        ("error", gen_api_gateway(max(1, n * 2 // 10))),
        ("warn",  gen_api_gateway(max(1, n * 2 // 10))),
    ]:
        streams.append(stream(make_labels(
            "api-gateway", "prod", "us-east-1", level,
            extra={"version": "v2"},
        ), lines))

    # ── api-gateway us-west-2 (second region — tests multi-cluster queries) ─
    streams.append(stream(make_labels(
        "api-gateway", "prod", "us-west-2", "info",
        extra={"version": "v2"},
    ), gen_api_gateway(max(1, n // 3))))

    # ── payment-service ───────────────────────────────────────────────────
    streams.append(stream(make_labels(
        "payment-service", "prod", "us-east-1", "info",
    ), gen_payment_service(n)))

    # ── auth-service ──────────────────────────────────────────────────────
    streams.append(stream(make_labels(
        "auth-service", "prod", "us-east-1", "info",
    ), gen_auth_service(n)))

    # ── nginx-ingress ─────────────────────────────────────────────────────
    streams.append(stream(make_labels(
        "nginx-ingress", "ingress-nginx", "us-east-1", "info",
        extra={"component": "controller"},
    ), gen_nginx_ingress(n)))

    # ── worker-service ────────────────────────────────────────────────────
    for level, subset in [
        ("info",  gen_worker_service(max(1, n * 7 // 10))),
        ("error", gen_worker_service(max(1, n * 3 // 10))),
    ]:
        streams.append(stream(make_labels(
            "worker-service", "prod", "us-east-1", level,
        ), subset))

    # ── db-postgres ───────────────────────────────────────────────────────
    streams.append(stream(make_labels(
        "db-postgres", "data", "us-east-1", "info",
        extra={"role": "primary", "db_name": "app_prod"},
    ), gen_postgres(max(1, n // 2))))

    # ── cache-redis ───────────────────────────────────────────────────────
    streams.append(stream(make_labels(
        "cache-redis", "data", "us-east-1", "debug",
        extra={"role": "master", "db": "0"},
    ), gen_redis(n)))

    # ── frontend-ssr us-east-1 ────────────────────────────────────────────
    streams.append(stream(make_labels(
        "frontend-ssr", "prod", "us-east-1", "info",
    ), gen_frontend_ssr(n)))

    # ── frontend-ssr us-west-2 ────────────────────────────────────────────
    streams.append(stream(make_labels(
        "frontend-ssr", "prod", "us-west-2", "info",
    ), gen_frontend_ssr(max(1, n // 2))))

    # ── batch-etl ─────────────────────────────────────────────────────────
    streams.append(stream(make_labels(
        "batch-etl", "batch", "us-east-1", "info",
    ), gen_batch_etl(max(1, n // 2))))

    # ── ml-serving ────────────────────────────────────────────────────────
    streams.append(stream(make_labels(
        "ml-serving", "ml", "us-east-1", "info",
        extra={"gpu": "nvidia-a100"},
    ), gen_ml_serving(n)))

    return streams


# ── Main loop ─────────────────────────────────────────────────────────────────

def wait_for_backends(max_wait: int = 120):
    """Wait for Loki and VL to be reachable before starting."""
    print("[INFO] Waiting for Loki and VictoriaLogs…", flush=True)
    deadline = time.time() + max_wait
    while time.time() < deadline:
        loki_ok = vl_ok = False
        try:
            urllib.request.urlopen(f"{LOKI_URL}/ready", timeout=3)
            loki_ok = True
        except Exception:
            pass
        try:
            urllib.request.urlopen(f"{VL_URL}/health", timeout=3)
            vl_ok = True
        except Exception:
            pass
        if loki_ok and vl_ok:
            print(f"[INFO] Backends ready (Loki={loki_ok}, VL={vl_ok})", flush=True)
            return
        time.sleep(3)
    print("[WARN] Timed out waiting for backends — starting anyway", flush=True)


def main():
    print(f"[INFO] Log generator starting: interval={INTERVAL}s batch={BATCH_SIZE}", flush=True)
    wait_for_backends()

    cycle = 0
    while True:
        start = time.time()
        cycle += 1
        try:
            streams = services_batch(BATCH_SIZE)
            dual_push(streams)
            lines_total = sum(len(s["values"]) for s in streams)
            elapsed = time.time() - start
            print(
                f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] "
                f"cycle={cycle} streams={len(streams)} lines={lines_total} "
                f"elapsed={elapsed:.2f}s",
                flush=True,
            )
        except Exception as e:
            print(f"[ERROR] cycle={cycle} failed: {e}", file=sys.stderr, flush=True)

        sleep_time = max(0, INTERVAL - (time.time() - start))
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
