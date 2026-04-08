# LokiVLProxyDown

- Signal: `up{job="<proxy-job>"} == 0` for 1m.
- Likely causes: pod crash, readiness failure, service misrouting, network policy.

## Triage

1. `kubectl -n <ns> get pods -l app.kubernetes.io/name=loki-vl-proxy -o wide`
2. `kubectl -n <ns> describe pod <pod>`
3. `kubectl -n <ns> logs <pod> --since=10m`
4. `kubectl -n <ns> get endpoints <service-name>`

## Mitigation

- Restart failed pod, roll back last deployment, or scale replicas.
- If readiness fails due to backend dependency, escalate as backend incident.

## Recovery Criteria

- `up == 1`
- `/ready` returns `200`
- dashboard query traffic recovers.

## Prevention

Apply [Deployment And Scaling Best Practices](deployment-best-practices.md) for multi-replica topology, rollout safety, and probe strategy.
