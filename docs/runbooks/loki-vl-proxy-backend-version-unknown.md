# LokiVLProxyBackendVersionUnknown

- Signal: `loki_vl_proxy_backend_version_known == 0` for 30m.
- Likely causes: `vmauth` (or another proxy) in front of VictoriaLogs answers `/metrics` itself; `-backend-version-url` is unreachable, refuses the credential or redirects; a probe contradicts `-backend-version` or the version URL.
- Impact: queries are answered, but the gates that need the release stay off.

## Triage

1. Read the `reason` of the `backend version compatibility check partial` or `unavailable` WARN line, or of a later `backend version read failed` line.
2. Check `GET /admin/capabilities`: a release marked `rejected` is one a feature probe contradicts.
3. Look for `named backend version contradicted by a feature probe` in the logs.

## Mitigation

- Behind vmauth: set `-httpInternalListenAddr` on vmauth, or set `-backend-version-url` (see [Backend version behind vmauth](../configuration.md#backend-version-behind-vmauth)).
- Fix the version URL's address or `-backend-version-basic-auth`.
- Correct `-backend-version` after a VictoriaLogs upgrade or rollback.

## Recovery Criteria

- `loki_vl_proxy_backend_version_known` is `1` on every replica.

## Prevention

Apply [Deployment And Scaling Best Practices](deployment-best-practices.md): include the version source in the review of any change to the proxy's path to VictoriaLogs.
