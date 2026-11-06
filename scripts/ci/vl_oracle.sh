#!/usr/bin/env bash
# Run the VictoriaLogs parse oracle (test/oracle/vl) over a query corpus.
#
#   scripts/ci/vl_oracle.sh <corpus.jsonl.gz>   write <corpus>.vl-<version>.jsonl.gz for every VictoriaLogs
#                                               version of the compatibility matrix
#   scripts/ci/vl_oracle.sh --pin <version>     write test/oracle/vl/go.<version>.mod and .sum for a new version
#
# The oracle is a module of its own: one go.<version>.mod per version, picked with -modfile, pins VictoriaLogs
# with a replace so no other module's requirement moves it. GOPRIVATE fetches it straight from its repository:
# the module proxy's tags of github.com/VictoriaMetrics/VictoriaLogs predate the repository split and hold
# VictoriaMetrics' tree.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ORACLE="$ROOT_DIR/test/oracle/vl"
MODULE=github.com/VictoriaMetrics/VictoriaLogs
export GOPRIVATE="$MODULE"

if [ "${1:-}" = "--pin" ]; then
  v="${2:?usage: $0 --pin <version>}"
  cd "$ORACLE"
  cp go.mod "go.$v.mod"
  go mod edit -modfile="go.$v.mod" -require="$MODULE@$v" -replace="$MODULE=$MODULE@$v"
  GOFLAGS=-mod=mod go mod tidy -modfile="go.$v.mod"
  echo "pinned $MODULE $v in test/oracle/vl/go.$v.mod"
  exit 0
fi

corpus="$(cd "$(dirname "${1:?usage: $0 <corpus.jsonl.gz>}")" && pwd)/$(basename "$1")"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
# The matrix's VictoriaLogs versions, the pin first (internal/querycorpus goldenVersions reads them the same way).
mapfile -t versions < <(jq -r '.stack.victorialogs | [.pinned_version] + (.matrix_versions - [.pinned_version]) | .[]' \
  "$ROOT_DIR/test/e2e-compat/compatibility-matrix.json")

for v in "${versions[@]}"; do
  if [ ! -f "$ORACLE/go.$v.mod" ]; then
    echo "test/oracle/vl has no go.$v.mod for matrix version $v: run $0 --pin $v" >&2
    exit 1
  fi
  (cd "$ORACLE" && go build -modfile="go.$v.mod" -o "$work/vl-$v" .)
  src="$(cd "$ORACLE" && go list -modfile="go.$v.mod" -m -f '{{if .Replace}}{{.Replace.Dir}}{{else}}{{.Dir}}{{end}}' "$MODULE")"
  "$work/vl-$v" cases -src "$src" -out "$work/cases-$v.json"
done

(cd "$ROOT_DIR" && LVP_QUERY_CORPUS_OUT="$corpus" LVP_ORACLE_DIR="$work" \
  go test ./internal/querycorpus -run '^TestOracleInputs$' -count=1 >&2)

for v in "${versions[@]}"; do
  "$work/vl-$v" check -in "$work/inputs-$v.jsonl" -out "$work/verdicts-$v.jsonl"
  gzip -c "$work/verdicts-$v.jsonl" > "${corpus%.jsonl.gz}.vl-$v.jsonl.gz"
  echo "VictoriaLogs $v: $(wc -l < "$work/verdicts-$v.jsonl") verdicts" >&2
done
