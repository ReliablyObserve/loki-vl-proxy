# LokiVLProxyBackendUnreachable

- Signal: sustained `502` responses from proxy to clients.
- Likely causes: VictoriaLogs outage, network/DNS path break, backend auth mismatch.
- Not every `502` means the backend is down: raw metric queries that exceed
  `-manual-range-metric-row-limit` or the `-max-stats-query-series` series cap
  during sample collection are rejected with `502` (`errorType: unavailable`).
  Check the error message before treating the backend as unreachable.

## Triage

1. `kubectl -n <ns> exec <proxy-pod> -- wget -qO- http://<victorialogs>:9428/health`
2. Validate `-backend` URL, DNS resolution, and network policy.
3. Validate backend auth headers/credentials used by proxy.
4. Check backend saturation and error logs.
5. Search proxy logs for `request error` entries with `limit exceeded`; if they
   dominate, the `502`s are execution-limit rejections for oversized queries,
   not backend unavailability. Narrow the offending queries (selector, range or
   grouping) rather than failing over the backend.

## Mitigation

- Restore backend service/network reachability.
- Fix backend auth credentials or forwarded headers.
- Fail over to healthy backend if your topology supports it.

## Recovery Criteria

- `502` rate returns to baseline.
- Proxy readiness remains stable.
- Circuit breaker closes and stays closed.

## Prevention

Apply [Deployment And Scaling Best Practices](deployment-best-practices.md) for backend health probes, network path hardening, and failover readiness.
