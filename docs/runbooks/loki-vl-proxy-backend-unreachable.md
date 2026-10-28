# LokiVLProxyBackendUnreachable

- Signal: sustained `500` responses from proxy to clients. Like Loki
  (`pkg/util/server/error.go`), the proxy answers every failed backend call
  with `500` and Loki's text (`errorType: internal`): VictoriaLogs refusing or
  resetting the connection, a VictoriaLogs or front-proxy error status, a
  truncated or malformed body. It never answers `502` or `503` for a query.
  A backend deadline is `504`, a client cancellation `499`, a full queue `429`.
- Likely causes: VictoriaLogs outage, network/DNS path break, backend auth mismatch.
- The client text carries no backend detail. The proxy's `request error` log
  line has it in `upstream_error`: the transport error, VictoriaLogs' message
  or the front proxy's status.
- Not every `500` means the backend is down: queries that exceed
  `-manual-range-metric-row-limit` or `-backend-max-buffered-response-bytes`
  are rejected with `500` and a message naming that flag. Check the error
  message before treating the backend as unreachable.

## Triage

1. `kubectl -n <ns> exec <proxy-pod> -- wget -qO- http://<victorialogs>:9428/health`
2. Validate `-backend` URL, DNS resolution, and network policy.
3. Validate backend auth headers/credentials used by proxy.
4. Check backend saturation and error logs.
5. Search proxy logs for `request error` entries and read `upstream_error`:
   `dial tcp`, `connection refused` or a VictoriaLogs message mean the backend
   failed; `limit exceeded` or `increase -<flag>` mean execution-limit
   rejections for oversized queries, not backend unavailability. Narrow the
   offending queries (selector, range or grouping) rather than failing over
   the backend.

## Mitigation

- Restore backend service/network reachability.
- Fix backend auth credentials or forwarded headers.
- Fail over to healthy backend if your topology supports it.

## Recovery Criteria

- `500` rate returns to baseline.
- Proxy readiness remains stable.
- Circuit breaker closes and stays closed.

## Prevention

Apply [Deployment And Scaling Best Practices](deployment-best-practices.md) for backend health probes, network path hardening, and failover readiness.
