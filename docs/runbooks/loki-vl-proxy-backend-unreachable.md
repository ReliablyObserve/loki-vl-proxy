# LokiVLProxyBackendUnreachable

- Signal: sustained `502` responses from proxy to clients.
- Likely causes: VictoriaLogs outage, network/DNS path break, backend auth mismatch.

## Triage

1. `kubectl -n <ns> exec <proxy-pod> -- wget -qO- http://<victorialogs>:9428/health`
2. Validate `-backend` URL, DNS resolution, and network policy.
3. Validate backend auth headers/credentials used by proxy.
4. Check backend saturation and error logs.

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
