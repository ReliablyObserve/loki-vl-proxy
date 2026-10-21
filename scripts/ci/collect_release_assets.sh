#!/usr/bin/env bash
# Collect the public verification assets of a release into $DIST_DIR (default
# dist/) so the release workflows can attach them to the GitHub Release:
#
#   loki-vl-proxy-<version>.intoto.jsonl
#       every attestation the workflow created, one Sigstore bundle per line: the
#       binaries' build provenance, the chart's build provenance, the image's
#       build provenance (GHCR, and Docker Hub when it was pushed) and the SPDX
#       SBOM attestation of each binary. OpenSSF Scorecard (Signed-Releases)
#       recognises release provenance by this file suffix.
#   loki-vl-proxy-image-<version>-linux-<arch>.spdx.json
#       the SPDX SBOM BuildKit attached to the pushed image index, one per platform.
#   image-digests.txt
#       <repository>@<digest> for the image (GHCR, Docker Hub when pushed) and the
#       chart (GHCR, Docker Hub when pushed), the same values the release notes list.
#
# Everything comes in through the environment; nothing is read from the network.
# The caller writes the BuildKit SBOM (docker buildx imagetools inspect <ref>
# --format '{{ json .SBOM }}') to $IMAGE_SBOM_JSON first. Any missing or
# malformed input fails the run: a release must not ship half of its evidence.
#
# Required: VERSION IMAGE_SBOM_JSON GHCR_IMAGE IMAGE_DIGEST CHART_GHCR_REF
#   CHART_GHCR_DIGEST BUNDLE_BINARIES BUNDLE_CHART BUNDLE_IMAGE_GHCR
#   BUNDLE_SBOM_{LINUX,DARWIN}_{AMD64,ARM64}
# Conditional: HAS_DOCKERHUB=true requires BUNDLE_IMAGE_DH; CHART_DH_REF (set
#   only by the workflow that also pushes the chart to Docker Hub) requires
#   CHART_DH_DIGEST.
# Optional: DIST_DIR (default dist), DOCKERHUB_IMAGE.
set -euo pipefail

fail() {
  echo "::error::$*"
  exit 1
}

need() {
  local name="$1"
  [ -n "${!name:-}" ] || fail "release asset collection: ${name} is not set"
}

DIST="${DIST_DIR:-dist}"
DOCKERHUB_IMAGE="${DOCKERHUB_IMAGE:-docker.io/reliablyobserve/loki-vl-proxy}"
DIGEST_RE='^sha256:[0-9a-f]{64}$'
SEMVER_RE='^[0-9]+\.[0-9]+\.[0-9]+$'

need VERSION
need IMAGE_SBOM_JSON
need GHCR_IMAGE
need IMAGE_DIGEST
need CHART_GHCR_REF
need CHART_GHCR_DIGEST
[[ "${VERSION}" =~ ${SEMVER_RE} ]] || fail "release version '${VERSION}' is not MAJOR.MINOR.PATCH"
[[ "${IMAGE_DIGEST}" =~ ${DIGEST_RE} ]] || fail "image digest '${IMAGE_DIGEST}' is not sha256:<64 hex>"
[[ "${CHART_GHCR_DIGEST}" =~ ${DIGEST_RE} ]] || fail "chart digest '${CHART_GHCR_DIGEST}' is not sha256:<64 hex>"
[ -d "${DIST}" ] || fail "${DIST} does not exist"

# --- Attestation bundles -> one JSON Lines file -------------------------------
PROVENANCE="${DIST}/loki-vl-proxy-${VERSION}.intoto.jsonl"
: > "${PROVENANCE}"

add_bundle() {
  local var="$1" path
  need "${var}"
  path="${!var}"
  [ -s "${path}" ] || fail "attestation bundle ${var} is missing or empty: ${path}"
  # Exactly one Sigstore bundle with a DSSE envelope. jq -c writes it on one line,
  # whatever the action's file ends with.
  jq -e -s 'length == 1 and (.[0].dsseEnvelope.payload | type) == "string"' "${path}" > /dev/null \
    || fail "attestation bundle ${var} is not a single Sigstore bundle with a DSSE envelope: ${path}"
  jq -c . "${path}" >> "${PROVENANCE}"
}

add_bundle BUNDLE_BINARIES
add_bundle BUNDLE_CHART
add_bundle BUNDLE_IMAGE_GHCR
if [ "${HAS_DOCKERHUB:-}" = "true" ]; then
  add_bundle BUNDLE_IMAGE_DH
fi
add_bundle BUNDLE_SBOM_LINUX_AMD64
add_bundle BUNDLE_SBOM_LINUX_ARM64
add_bundle BUNDLE_SBOM_DARWIN_AMD64
add_bundle BUNDLE_SBOM_DARWIN_ARM64

# --- Image SBOMs, one SPDX document per platform ------------------------------
[ -s "${IMAGE_SBOM_JSON}" ] || fail "the image SBOM export is missing or empty: ${IMAGE_SBOM_JSON}"
for ARCH in amd64 arm64; do
  OUT="${DIST}/loki-vl-proxy-image-${VERSION}-linux-${ARCH}.spdx.json"
  # `docker buildx imagetools inspect --format '{{ json .SBOM }}'` prints
  # {"linux/<arch>": {"SPDX": {...}}, ...}. A platform without an SPDX document
  # selects nothing, which makes jq -e fail.
  jq -ce --arg platform "linux/${ARCH}" \
    '.[$platform].SPDX | select(type == "object" and ((.spdxVersion // "") | startswith("SPDX-")))' \
    "${IMAGE_SBOM_JSON}" > "${OUT}" \
    || fail "the image index has no SPDX SBOM for linux/${ARCH}"
done

# --- Digests ------------------------------------------------------------------
DIGESTS="${DIST}/image-digests.txt"
{
  echo "${GHCR_IMAGE}@${IMAGE_DIGEST}"
  if [ "${HAS_DOCKERHUB:-}" = "true" ]; then
    echo "${DOCKERHUB_IMAGE}@${IMAGE_DIGEST}"
  fi
  echo "${CHART_GHCR_REF}@${CHART_GHCR_DIGEST}"
  if [ -n "${CHART_DH_REF:-}" ]; then
    need CHART_DH_DIGEST
    [[ "${CHART_DH_DIGEST}" =~ ${DIGEST_RE} ]] || fail "Docker Hub chart digest '${CHART_DH_DIGEST}' is not sha256:<64 hex>"
    echo "${CHART_DH_REF}@${CHART_DH_DIGEST}"
  fi
} > "${DIGESTS}"

echo "Collected release assets in ${DIST}:"
ls -l "${PROVENANCE}" "${DIST}"/loki-vl-proxy-image-"${VERSION}"-linux-*.spdx.json "${DIGESTS}"
echo "Attestation bundles in ${PROVENANCE}: $(wc -l < "${PROVENANCE}" | tr -d ' ')"
