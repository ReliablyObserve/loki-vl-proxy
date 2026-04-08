# Release Info

## Auto Release Precedence

`Auto Release` on `main` uses this precedence order:

1. explicit release labels on merged PR: `release:major`, `release:minor`, `release:patch`, `no-release`
2. semantic PR labels: `breaking-change`, `feature`, `bugfix`, `performance`
3. conventional commit subjects (`feat`, `fix`, `perf`, `refactor`, `revert`, breaking markers)
4. fallback patch bump for non-doc/non-ci changes when no explicit signal is present

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
