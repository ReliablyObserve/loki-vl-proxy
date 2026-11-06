#!/usr/bin/env bash
# Run the VictoriaLogs parse oracle (test/oracle/vl) over a query corpus.
#
#   scripts/ci/vl_oracle.sh <corpus.jsonl.gz>   write <corpus>.vl-<version>.jsonl.gz (the verdicts) and
#                                               <corpus>.vl-<version>.constructs.json for every VictoriaLogs
#                                               version of the compatibility matrix
#   scripts/ci/vl_oracle.sh --pin <version>     pin a version: the matrix's pinned version in test/oracle/vl/go.mod
#                                               and go.sum, any other in go.<version>.mod and .sum
#
# The oracle is a module of its own. go.mod pins the matrix's pinned (newest) version, so the module builds as
# it is for every tool; each other version has a go.<version>.mod picked with -modfile. Each pins VictoriaLogs
# with a replace so no other module's requirement moves it. GOPRIVATE fetches it straight from its repository:
# the module proxy's tags of github.com/VictoriaMetrics/VictoriaLogs predate the repository split and hold
# VictoriaMetrics' tree.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ORACLE="$ROOT_DIR/test/oracle/vl"
MODULE=github.com/VictoriaMetrics/VictoriaLogs
export GOPRIVATE="$MODULE"

MATRIX="$ROOT_DIR/test/e2e-compat/compatibility-matrix.json"
PINNED="$(jq -r '.stack.victorialogs.pinned_version' "$MATRIX")"

# modfile names the module file of a version: go.mod for the pinned one, go.<version>.mod for any other.
modfile() {
  if [ "$1" = "$PINNED" ]; then echo go.mod; else echo "go.$1.mod"; fi
}

if [ "${1:-}" = "--pin" ]; then
  v="${2:?usage: $0 --pin <version>}"
  cd "$ORACLE"
  file="$(modfile "$v")"
  if [ "$file" != go.mod ]; then
    printf 'module github.com/ReliablyObserve/Loki-VL-proxy/test/oracle/vl\n\ngo %s\n' \
      "$(go mod edit -json | jq -r .Go)" > "$file"
  else
    go mod edit -dropreplace="$MODULE" -droprequire="$MODULE"
  fi
  go mod edit -modfile="$file" -require="$MODULE@$v" -replace="$MODULE=$MODULE@$v"
  GOFLAGS=-mod=mod go mod tidy -modfile="$file"
  echo "pinned $MODULE $v in test/oracle/vl/$file"
  exit 0
fi

corpus="$(cd "$(dirname "${1:?usage: $0 <corpus.jsonl.gz>}")" && pwd)/$(basename "$1")"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
# The matrix's VictoriaLogs versions, the pin first (internal/querycorpus goldenVersions reads them the same way).
mapfile -t versions < <(jq -r '.stack.victorialogs | [.pinned_version] + (.matrix_versions - [.pinned_version]) | .[]' "$MATRIX")

for v in "${versions[@]}"; do
  file="$(modfile "$v")"
  if ! grep -q "^replace $MODULE => $MODULE $v\$" "$ORACLE/$file" 2>/dev/null; then
    echo "test/oracle/vl/$file does not pin $MODULE $v (a matrix version): run $0 --pin $v" >&2
    exit 1
  fi
  (cd "$ORACLE" && go build -modfile="$file" -o "$work/vl-$v" .)
  src="$(cd "$ORACLE" && go list -modfile="$file" -m -f '{{if .Replace}}{{.Replace.Dir}}{{else}}{{.Dir}}{{end}}' "$MODULE")"
  "$work/vl-$v" cases -src "$src" -out "$work/cases-$v.json"
  # Its pipes, stats functions and filters: the report checks them against scripts/vl-ast-coverage.json.
  "$work/vl-$v" constructs -src "$src" -out "${corpus%.jsonl.gz}.vl-$v.constructs.json"
done

(cd "$ROOT_DIR" && LVP_QUERY_CORPUS_OUT="$corpus" LVP_ORACLE_DIR="$work" \
  go test ./internal/querycorpus -run '^TestOracleInputs$' -count=1 >&2)

for v in "${versions[@]}"; do
  "$work/vl-$v" check -in "$work/inputs-$v.jsonl" -out "$work/verdicts-$v.jsonl"
  gzip -c "$work/verdicts-$v.jsonl" > "${corpus%.jsonl.gz}.vl-$v.jsonl.gz"
  echo "VictoriaLogs $v: $(wc -l < "$work/verdicts-$v.jsonl") verdicts" >&2
done
