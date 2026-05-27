#!/usr/bin/env bash
set -euo pipefail
#
# Scrape GHCR package pages for total download counts.
# These pages are fully public — no token required.
#
# Usage: fetch_ghcr_pulls.sh <org> <repo>
# Output: image= and chart= written to $GITHUB_OUTPUT (CI)
#         or printed to stdout (local).

ORG="${1:?usage: fetch_ghcr_pulls.sh <org> <repo>}"
REPO="${2:?usage: fetch_ghcr_pulls.sh <org> <repo>}"

extract_total_downloads() {
  local url="$1"
  local page
  page="$(curl -fsSL --retry 3 --retry-delay 2 "$url" 2>/dev/null)" || { echo "0"; return; }

  # GHCR renders: <h3 title="2181">2.18K</h3> near "Total downloads".
  # Extract the raw number from the title attribute for precision,
  # and the formatted string (e.g. "2.18K") for display.
  local raw formatted
  raw="$(printf '%s' "$page" \
    | sed -n 's/.*Total downloads.*<h3[^>]*title="\([0-9]*\)">\([^<]*\)<.*/\1/p' \
    | head -1)"
  formatted="$(printf '%s' "$page" \
    | sed -n 's/.*Total downloads.*<h3[^>]*title="[0-9]*">\([^<]*\)<.*/\1/p' \
    | head -1)"

  if [ -z "$raw" ]; then
    # Fallback: look for the pattern without "Total downloads" context.
    raw="$(printf '%s' "$page" \
      | tr '\n' ' ' \
      | sed -n 's/.*Total downloads[^<]*<[^>]*>[^<]*<h3[^>]*title="\([0-9]*\)".*/\1/p' \
      | head -1)"
    formatted="$(printf '%s' "$page" \
      | tr '\n' ' ' \
      | sed -n 's/.*Total downloads[^<]*<[^>]*>[^<]*<h3[^>]*title="[0-9]*">\([^<]*\)<.*/\1/p' \
      | head -1)"
  fi

  if [ -n "$raw" ]; then
    echo "${raw}|${formatted}"
  else
    echo "0|0"
  fi
}

IMAGE_URL="https://github.com/${ORG}/${REPO}/pkgs/container/${REPO}"
CHART_URL="https://github.com/${ORG}/${REPO}/pkgs/container/charts%2F${REPO}"

IMAGE_RESULT="$(extract_total_downloads "$IMAGE_URL")"
CHART_RESULT="$(extract_total_downloads "$CHART_URL")"

IMAGE_RAW="${IMAGE_RESULT%%|*}"
IMAGE_FMT="${IMAGE_RESULT##*|}"
CHART_RAW="${CHART_RESULT%%|*}"
CHART_FMT="${CHART_RESULT##*|}"

echo "GHCR image pulls: ${IMAGE_FMT} (${IMAGE_RAW})"
echo "GHCR chart pulls: ${CHART_FMT} (${CHART_RAW})"

if [ -n "${GITHUB_OUTPUT:-}" ]; then
  {
    echo "image_raw=${IMAGE_RAW}"
    echo "image_fmt=${IMAGE_FMT}"
    echo "chart_raw=${CHART_RAW}"
    echo "chart_fmt=${CHART_FMT}"
  } >> "$GITHUB_OUTPUT"
fi
