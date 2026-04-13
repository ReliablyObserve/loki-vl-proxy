# Release Info

## Auto Release Precedence

`Auto Release` on `main` uses this precedence order:

1. explicit chart version override: if `charts/loki-vl-proxy/Chart.yaml` `version` is ahead of the latest git tag, release exactly that chart version
2. explicit release labels on merged PR: `release:major`, `release:minor`, `release:patch`, `no-release`
3. semantic PR labels: `breaking-change`, `feature`, `bugfix`, `performance`
4. conventional commit subjects (`feat`, `fix`, `perf`, `refactor`, `revert`, breaking markers)
5. fallback patch bump for non-doc/non-ci changes when no explicit signal is present

## Skip Rules

Release is skipped when the change-set since the latest tag is docs/metadata/CI only:

- `README*`
- `CHANGELOG*`
- `docs/**`
- `.github/**`

Helm chart changes are treated as releasable. Metadata-sync commits are loop-protected with `[skip ci]`.

## GHCR Chart Visibility

Release workflows attempt to enforce public visibility for the OCI Helm chart package:

- package: `ghcr.io/reliablyobserve/charts/loki-vl-proxy`
- workflow secret: `GHCR_PACKAGE_ADMIN_TOKEN`

If `GHCR_PACKAGE_ADMIN_TOKEN` is not configured, release continues but visibility cannot be auto-enforced.
