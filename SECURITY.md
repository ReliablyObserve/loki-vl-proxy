# Security Policy

## Supported Versions

| Version | Supported              |
|---------|------------------------|
| 2.x     | Yes (latest release)   |
| 1.x     | No                     |
| < 1.0   | No                     |

### Patch policy

Security fixes ship in a new release of the latest 2.x line. Releases are cut from `main` continuously, so the newest release is the patched one; fixes are not backported to older minor versions or to 1.x. A reachable vulnerability in the Go toolchain or in a dependency is fixed by moving to the fixed version as soon as one is published, and the release notes and `CHANGELOG.md` name the advisory IDs. Report vulnerabilities through the Security Advisories link below.

## Reporting a Vulnerability

Please report security vulnerabilities via GitHub Security Advisories:
https://github.com/ReliablyObserve/Loki-VL-proxy/security/advisories/new

Do NOT open a public issue for security vulnerabilities.

## Default Security Posture

`loki-vl-proxy` is designed as a read-focused compatibility layer. The project defaults are intentionally restrictive:

- read APIs enabled for Loki-compatible querying
- write ingestion API (`/loki/api/v1/push`) blocked with `405`
- delete API unsupported: the `/loki/api/v1/delete` handler validates requests (confirmation header, time bounds, selector, tenant scope, audit log) but forwards to a VictoriaLogs path that VictoriaLogs rejects, so no deletion is performed
- admin/debug endpoints disabled unless explicitly enabled; without `-server.admin-auth-token` they bind to a loopback-only `-admin-listen` (default `127.0.0.1:3101`)
- `/metrics` not served unless `-server.register-instrumentation=true` (optionally on a dedicated `-metrics-listen`)
- peer cache refuses to start without `-peer-auth-token` unless `-peer-insecure-ip-allowlist=true` is set explicitly
- runtime image runs as a non-root user
- runtime image keeps a read-only root filesystem
- Helm chart drops Linux capabilities and blocks privilege escalation

## Built-In Controls

- **Read-only by default**: `/loki/api/v1/push` blocked with 405
- **Delete not supported**: request safeguards (confirmation header, tenant scoping, 30-day time range limit, audit logging) run before the backend call, but the backend rejects the target path; see [Security hardening migration](docs/security-hardening-migration.md#remaining-delete-api-gap)
- **Rate limiting**: Per-client token bucket using `RemoteAddr` (not spoofable `X-Forwarded-For`)
- **Query length limit**: 64KB max to prevent abuse
- **Security headers**: `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Cache-Control: no-store`
- **Consistent response hardening**: the proxy now applies the same baseline security headers on success, `404`, and disabled admin/debug responses so scanners and browsers do not see a weaker edge path
- **HTTP hardening**: Configurable read/write/idle timeouts, max header/body bytes
- **TLS support**: Server-side HTTPS via `-tls-cert-file`/`-tls-key-file`
- **Tail browser-origin controls**: `/tail` can enforce explicit allowed origins
- **Admin token enforcement**: non-loopback admin/debug exposure requires `-server.admin-auth-token`; without a token, admin/debug routes are served only on `-admin-listen` (loopback by default)
- **Peer-cache protection**: `-peer-auth-token` is required when peer discovery is configured (constant-time `X-Peer-Token` check); the Helm chart generates a token Secret when `peerCache.authToken` is unset
- **Global tenant denial**: an unmapped `X-Scope-OrgID: *` returns `403` in both native and label-routing modes unless `-tenant.allow-global=true`
- **Tenant-scoped caches**: response, window, metadata, persisted and peer cache keys and coalescer keys include the resolved tenant routing and forwarded-identity fingerprint
- **Query redaction**: debug logs record queries as `sha256:<8hex> len=<n>` unless `-debug-log-raw-queries=true`; VictoriaLogs error bodies and transport errors are redacted before they are logged or returned to clients
- **Execution limits**: raw metric row/series limits, binary-expression budgets, `line_format` output limits and bounded coalesced bodies reject excessive work with an explicit error instead of partial results; see [Security hardening migration](docs/security-hardening-migration.md#execution-and-storage-limits)
- **Sensitive metrics off by default**: per-tenant and per-client identity labels are not exported unless explicitly enabled
- **No secrets in logs**: all log output passes through a redacting slog handler that detects and masks API keys, bearer tokens, passwords, AWS credentials, and URL-embedded credentials

## Known Security Considerations

- **`text/template` in `| line_format`**: Go templates are executed on query results. The template FuncMap is restricted to safe string functions, but this is still a surface to treat carefully in shared environments.
- **Disk cache**: No application-level encryption. Use cloud-provider or node-level disk encryption for data at rest.
- **Backend TLS skip**: `-backend-tls-skip-verify` disables certificate validation for VictoriaLogs connections.
- **Host `/proc` files for system metrics**: off by default (`systemMetrics.hostProc.enabled=false`), so the Helm chart renders no `hostPath` volume and passes the Pod Security `baseline` and `restricted` profiles. When you set `systemMetrics.hostProc.enabled=true`, the chart mounts five individual host files read-only (`/proc/stat`, `/proc/meminfo`, `/proc/pressure/{cpu,memory,io}`) and sets `-host-proc-root`; the whole host `/proc` directory is not mounted, but the namespace must allow `hostPath` volumes (Pod Security `privileged`, or an admission exemption) and security scanners will flag them.

## Security Testing And CI

The repository now keeps a dedicated layered security pipeline in addition to `CodeQL` and the normal compatibility suite.

- **Fast PR blockers** in `.github/workflows/security-pr.yaml`: `gitleaks`, `gosec`, `Trivy` filesystem scan, `actionlint`, `hadolint`, and `OpenSSF Scorecard`
- **Runtime PR lane** in `.github/workflows/security-pr.yaml`: custom Go regressions plus an OWASP ZAP baseline scan against the local compose stack
- **Heavy scheduled lane** in `.github/workflows/security-heavy.yaml`: image scanning, SBOM generation, longer fuzzing, `Semgrep`, OWASP ZAP active scan, and curated `Nuclei`

These lanes are intended to catch different classes of failures:

- source-level security mistakes
- workflow and supply-chain misconfigurations
- container hardening regressions
- tenant-isolation and auth-boundary regressions
- live HTTP surface issues in the running stack

The PR runtime lane targets a short allowlist of user-facing and admin/debug URLs. Local ZAP runs may still report `10049 Non-Storable Content` on intentional `404` discovery paths such as `/` or disabled `/debug/*` endpoints; CI treats that as report noise rather than an exploitable proxy-path failure.

## Recommended Production Baseline

- define an explicit `-tenant-map` for multi-tenant production
- keep `-tenant.allow-global=false` unless wildcard backend-default access is intentional
- set a strict `-tail.allowed-origins` list for browser clients
- keep request-size and timeout limits bounded
- require `-server.admin-auth-token` before enabling debug/admin surfaces
- set `-peer-auth-token` for peer cache and keep `-peer-insecure-ip-allowlist=false`
- validate representative wide queries against the execution limits before rollout
- keep `-metrics.export-sensitive-labels=false` on shared scrape paths
- enable TLS and validate upstream certificates in production

## Verifying Releases

Every release is built and published by GitHub Actions (`.github/workflows/auto-release.yaml`, or `.github/workflows/release.yaml` when a tag is republished by hand). The workflow signs and attests each artifact with short-lived Sigstore certificates issued to the workflow run through GitHub OIDC; no long-lived signing key exists. Each signature and attestation is recorded in Sigstore's public transparency log (Rekor). The release notes of each version list the image index digests and the chart digests, and repeat the commands below with the values filled in.

What is published for a release:

- container image `ghcr.io/reliablyobserve/loki-vl-proxy` (and `docker.io/reliablyobserve/loki-vl-proxy`): the multi-arch index is signed with cosign by digest and carries GitHub build provenance, plus BuildKit provenance (`mode=max`) and an SBOM as attestations inside the index
- Helm chart `oci://ghcr.io/reliablyobserve/charts/loki-vl-proxy` (the Docker Hub copy is pushed only when a release is republished with `release.yaml`): signed with cosign by digest; the chart `.tgz` has GitHub build provenance
- release binaries: `checksums.txt` with a keyless signature bundle (`checksums.txt.sigstore.json`), GitHub build provenance for every binary, and one SPDX JSON SBOM per binary (`loki-vl-proxy-<os>-<arch>.spdx.json`) attested to it
- evidence attached to the GitHub Release so it can be checked from files, without the GitHub attestation API (see [Verifying from the files attached to the release](#verifying-from-the-files-attached-to-the-release)): `loki-vl-proxy-<version>.intoto.jsonl` (every attestation above, one Sigstore bundle per line), `loki-vl-proxy-<version>.tgz.sigstore.json` (cosign signature of the chart archive), `loki-vl-proxy-image-<version>-linux-<arch>.spdx.json` (the SBOM of the image for each platform) and `image-digests.txt` (the image and chart references with their digests)

The commands need [cosign](https://docs.sigstore.dev/cosign/) 3.1.3 or newer (the version that signs the releases; cosign 3 stores signatures as OCI referrers, which older cosign releases do not look for by default) and the [GitHub CLI](https://cli.github.com/) for `gh attestation`.

### Signing identity

The certificate identity is the workflow file and the ref the run started from:

| Workflow | When it runs | Certificate identity |
|----------|--------------|----------------------|
| `auto-release.yaml` | push to `main`, or `workflow_dispatch` from `main` | `https://github.com/ReliablyObserve/loki-vl-proxy/.github/workflows/auto-release.yaml@refs/heads/main` |
| `release.yaml` | `workflow_dispatch` | `https://github.com/ReliablyObserve/loki-vl-proxy/.github/workflows/release.yaml@<ref the workflow was dispatched from>`, for example `@refs/heads/main` or `@refs/tags/vX.Y.Z` |

All examples use one regular expression that accepts both workflows, from `main` or from a version tag, and nothing else:

```sh
ID_RE='^https://github\.com/ReliablyObserve/loki-vl-proxy/\.github/workflows/(auto-release|release)\.yaml@refs/(heads/main|tags/v.+)$'
ISSUER=https://token.actions.githubusercontent.com
```

The certificate also records the commit the workflow run started from (GitHub Workflow SHA). To bind a verification to a tag, add `--certificate-github-workflow-sha "$(git rev-list -n1 vX.Y.Z)"` to any `cosign verify` or `cosign verify-blob` command below. The check holds in two cases: `release.yaml` dispatched from the tag ref itself, and an `auto-release.yaml` run that was started by the tagged commit. Runs of `auto-release.yaml` queue behind each other and release the head of `main` at the time they start, so a run queued behind a later push can be started by an earlier commit of `main` than the one it tags; the certificate then carries that earlier commit, and `release.yaml` dispatched from `main` carries the head of `main`. In those cases the identity check above is the one to rely on. Both workflows refuse to publish from any ref other than `main` or a `v*` tag.

### Container image

```sh
DIGEST=sha256:<digest from the release notes>

cosign verify ghcr.io/reliablyobserve/loki-vl-proxy@${DIGEST} \
  --certificate-oidc-issuer "$ISSUER" --certificate-identity-regexp "$ID_RE"

cosign verify docker.io/reliablyobserve/loki-vl-proxy@${DIGEST} \
  --certificate-oidc-issuer "$ISSUER" --certificate-identity-regexp "$ID_RE"

gh attestation verify oci://ghcr.io/reliablyobserve/loki-vl-proxy@${DIGEST} \
  --repo ReliablyObserve/loki-vl-proxy
```

### Helm chart

Use the chart digest from the release notes. The reference is the `oci://` URL without the `oci://` prefix. The Docker Hub command applies only to releases republished with `release.yaml`:

```sh
cosign verify ghcr.io/reliablyobserve/charts/loki-vl-proxy@sha256:<chart digest> \
  --certificate-oidc-issuer "$ISSUER" --certificate-identity-regexp "$ID_RE"

cosign verify docker.io/reliablyobserve/charts/loki-vl-proxy@sha256:<chart digest> \
  --certificate-oidc-issuer "$ISSUER" --certificate-identity-regexp "$ID_RE"
```

The chart `.tgz` attached to the release has GitHub build provenance: `gh attestation verify loki-vl-proxy-<version>.tgz --repo ReliablyObserve/loki-vl-proxy`.

### Release binaries

Download the binary, `checksums.txt` and `checksums.txt.sigstore.json` from the release page, then verify the signature on the checksums and the checksums on the files:

```sh
cosign verify-blob --bundle checksums.txt.sigstore.json \
  --certificate-oidc-issuer "$ISSUER" --certificate-identity-regexp "$ID_RE" \
  checksums.txt

sha256sum -c checksums.txt --ignore-missing    # macOS: shasum -a 256 -c checksums.txt --ignore-missing
gh attestation verify loki-vl-proxy-linux-amd64 --repo ReliablyObserve/loki-vl-proxy
```

The SPDX SBOM of a binary is attested to that binary and is also attached to the release as `loki-vl-proxy-<os>-<arch>.spdx.json`. To check the attestation: `gh attestation verify loki-vl-proxy-linux-amd64 --repo ReliablyObserve/loki-vl-proxy --predicate-type https://spdx.dev/Document/v2.3`.

### Verifying from the files attached to the release

Download `loki-vl-proxy-<version>.intoto.jsonl` and the artifact to check from the release page. The `.intoto.jsonl` file holds one Sigstore bundle per line: the build provenance of the four binaries, of the chart archive and of the image index (GHCR, and Docker Hub when it was pushed), and the SPDX SBOM attestation of each binary. `gh attestation verify --bundle` reads it instead of asking the GitHub attestation API, picks the bundles whose subject is the artifact, and verifies their signature, certificate and transparency-log entry against the repository and workflow:

```sh
VERSION=X.Y.Z    # without the leading v

gh attestation verify loki-vl-proxy-linux-amd64 \
  --bundle loki-vl-proxy-${VERSION}.intoto.jsonl --repo ReliablyObserve/loki-vl-proxy

gh attestation verify loki-vl-proxy-linux-amd64 \
  --bundle loki-vl-proxy-${VERSION}.intoto.jsonl --repo ReliablyObserve/loki-vl-proxy \
  --predicate-type https://spdx.dev/Document/v2.3          # the SBOM attestation

gh attestation verify loki-vl-proxy-${VERSION}.tgz \
  --bundle loki-vl-proxy-${VERSION}.intoto.jsonl --repo ReliablyObserve/loki-vl-proxy

gh attestation verify oci://ghcr.io/reliablyobserve/loki-vl-proxy@${DIGEST} \
  --bundle loki-vl-proxy-${VERSION}.intoto.jsonl --repo ReliablyObserve/loki-vl-proxy
```

An artifact that is not a subject of any bundle in the file fails verification. The `oci://` form still contacts the registry to read the image digest; use `${DIGEST}` from `image-digests.txt`, which lists `<repository>@<digest>` for the image and the chart.

The commands above still download the Sigstore trusted root (through TUF) the first time they run. To verify with no network access at all, save the trusted root once on a connected machine and pass it with every later run (checked with the network blocked: it verifies with the saved root and fails without it):

```sh
gh attestation trusted-root > trusted_root.jsonl
gh attestation verify loki-vl-proxy-linux-amd64 \
  --bundle loki-vl-proxy-${VERSION}.intoto.jsonl --repo ReliablyObserve/loki-vl-proxy \
  --custom-trusted-root trusted_root.jsonl
```

The chart archive also has its own cosign signature bundle, which `cosign verify-blob` checks against the same identity as the release checksums:

```sh
cosign verify-blob --bundle loki-vl-proxy-${VERSION}.tgz.sigstore.json \
  --certificate-oidc-issuer "$ISSUER" --certificate-identity-regexp "$ID_RE" \
  loki-vl-proxy-${VERSION}.tgz
```

The SBOM of the image is attached per platform as `loki-vl-proxy-image-<version>-linux-amd64.spdx.json` and `loki-vl-proxy-image-<version>-linux-arm64.spdx.json`. They are the SBOM documents BuildKit stored in the signed image index, read back by digest and written out as compact JSON; to compare one with the registry, read it again with `docker buildx imagetools inspect ghcr.io/reliablyobserve/loki-vl-proxy@${DIGEST} --format '{{ json .SBOM }}'`. The `.intoto.jsonl` suffix is also what OpenSSF Scorecard's `Signed-Releases` check looks for as release provenance.

### Reproducing a binary from a tag

The release binaries are built with `-trimpath`, and the build time embedded in them is the commit time of the release commit rather than the time of the build, so the same source and toolchain give the same bytes on any machine. From a clean checkout of the tag, with the Go toolchain named in `go.mod` (the `toolchain` line if there is one, otherwise the `go` line). CI installs the same version with `setup-go`; if the workflow and `go.mod` ever differ, use the one in `go.mod` at the tag.

Keep downloaded release files (`checksums.txt` and the binaries) outside the clone: Go records whether the working tree has untracked or modified files, and that changes the binary.

```sh
git clone https://github.com/ReliablyObserve/loki-vl-proxy.git && cd loki-vl-proxy
git checkout vX.Y.Z

VERSION=X.Y.Z
REVISION="$(git rev-parse HEAD)"
BUILD_TIME="$(TZ=UTC git log -1 --date=format-local:%Y-%m-%dT%H:%M:%SZ --format=%cd HEAD)"
TOOLCHAIN="$(awk '/^toolchain /{print $2}' go.mod)"
export GOTOOLCHAIN="${TOOLCHAIN:-go$(awk '/^go /{print $2}' go.mod)}"

mkdir -p dist    # dist/ is ignored by git, as in the release workflow
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath \
  -ldflags="-s -w -X main.version=${VERSION} -X main.revision=${REVISION} -X main.buildTime=${BUILD_TIME}" \
  -o dist/loki-vl-proxy-linux-amd64 ./cmd/proxy

sha256sum dist/loki-vl-proxy-linux-amd64    # compare with the line in the release's checksums.txt
```

`REVISION` is the commit the tag points to, for releases cut by `auto-release.yaml` and for releases republished with `release.yaml`. Container image digests are not reproducible byte for byte, because the image index also carries BuildKit attestations; the binaries inside the image are built with the same flags but carry no VCS stamp (`.dockerignore` excludes `.git`), so they do not match `checksums.txt` byte for byte.

### Pinning by digest

Tags can be moved; digests cannot. Reference the image by the digest listed in the release notes, for example `ghcr.io/reliablyobserve/loki-vl-proxy@sha256:<digest>`, and verify the digest with the commands above before rolling it out. The base images in the `Dockerfile` are pinned by digest as well and are updated by Dependabot.

## Related Docs

- [Extended security guide](docs/security.md)
- [Security hardening migration](docs/security-hardening-migration.md)
- [Testing and local validation](docs/testing.md)
- [Configuration](docs/configuration.md)
- [API Reference](docs/api-reference.md)
- [Observability](docs/observability.md)
- [Known Issues](docs/KNOWN_ISSUES.md)
