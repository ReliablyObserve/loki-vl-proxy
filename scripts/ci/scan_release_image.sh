#!/usr/bin/env bash
# Gate a release image on Trivy before anything is signed, tagged or published.
#
# IMAGE_REF is the image as it was pushed, by digest (repo@sha256:<64 hex>). Each
# platform of the index (linux/amd64 and linux/arm64) is scanned with the same
# Trivy image, severities and scanners as the pull request scan in
# security-pr.yaml: HIGH and CRITICAL findings that have a fix available fail the
# gate, and the entries of the ignore file are honoured. The image is read
# anonymously, like every user pulls it, so the gate also fails if the package is
# not public.
#
# A Trivy failure that is not a finding (for example the vulnerability database
# could not be downloaded) is retried; a finding is never retried.
#
# Required: IMAGE_REF
# Optional: IGNORE_FILE (default .trivyignore.yaml), TRIVY_CACHE_DIR
#   (default $RUNNER_TEMP/trivy-cache), PLATFORMS (default "linux/amd64 linux/arm64").
set -euo pipefail

TRIVY_IMAGE="aquasec/trivy:0.71.0@sha256:016eae51fdcf989332a5404af7e8f625cd5d95d7c0907a221d080a996f556500"

fail() {
  echo "::error::$*"
  exit 1
}

[ -n "${IMAGE_REF:-}" ] || fail "release image scan: IMAGE_REF is not set"
[[ "${IMAGE_REF}" =~ @sha256:[0-9a-f]{64}$ ]] || fail "release image scan: '${IMAGE_REF}' is not a digest reference"

IGNORE_FILE="${IGNORE_FILE:-.trivyignore.yaml}"
[ -f "${IGNORE_FILE}" ] || fail "release image scan: ignore file ${IGNORE_FILE} not found"
IGNORE_DIR="$(cd "$(dirname "${IGNORE_FILE}")" && pwd)"
IGNORE_NAME="$(basename "${IGNORE_FILE}")"

CACHE_DIR="${TRIVY_CACHE_DIR:-${RUNNER_TEMP:-/tmp}/trivy-cache}"
mkdir -p "${CACHE_DIR}"
LOG="$(mktemp)"
trap 'rm -f "${LOG}"' EXIT

scan() {
  local platform="$1"
  docker run --rm \
    --user "$(id -u):$(id -g)" \
    -e TRIVY_CACHE_DIR=/cache \
    -v "${CACHE_DIR}:/cache" \
    -v "${IGNORE_DIR}:/config:ro" \
    "${TRIVY_IMAGE}" \
    image "${IMAGE_REF}" \
    --platform "${platform}" \
    --ignorefile "/config/${IGNORE_NAME}" \
    --scanners vuln,misconfig,secret \
    --severity HIGH,CRITICAL \
    --ignore-unfixed \
    --exit-code 1 \
    --skip-version-check
}

STATUS=0
for PLATFORM in ${PLATFORMS:-linux/amd64 linux/arm64}; do
  echo "Trivy: scanning ${IMAGE_REF} for ${PLATFORM}"
  for ATTEMPT in 1 2 3; do
    RC=0
    scan "${PLATFORM}" > "${LOG}" 2>&1 || RC=$?
    cat "${LOG}"
    if [ "${RC}" -eq 0 ]; then
      break
    fi
    # Trivy exits 1 both for findings and for fatal errors; only a fatal error
    # (no report) is worth another attempt.
    if grep -q 'FATAL' "${LOG}" && [ "${ATTEMPT}" -lt 3 ]; then
      echo "::warning::Trivy failed to run for ${PLATFORM} (attempt ${ATTEMPT} of 3); retrying"
      sleep $((ATTEMPT * 10))
      continue
    fi
    break
  done
  if [ "${RC}" -ne 0 ]; then
    echo "::error::Trivy gate failed for ${IMAGE_REF} (${PLATFORM}, exit ${RC}): HIGH or CRITICAL findings with a fix available, or the scan could not run"
    STATUS=1
  fi
done

[ "${STATUS}" -eq 0 ] || exit 1
echo "Trivy gate passed for ${IMAGE_REF}"
