package proxy

import (
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// Every pattern the miner writes is a pattern line filter Loki accepts, as Logs Drilldown sends it back (|> to
// include it, !> to exclude it): its placeholders never stand two in a row, and it holds no other capture. A
// line's own `<name>` text reads as a capture in any pattern, Loki's included, so lines with '<' or '>' are left
// out.
//
// conformance: semantics/pattern-line-filter-invalid-captures, loki_api_v1_patterns
func FuzzMinedPatternsAreValidLineFilters(f *testing.F) {
	for _, seed := range [][3]string{
		{`1.2.3.4 - admin [x +0000] "GET /a HTTP/1.1" 200 1 "-" "curl/8" 3`, `5.6.7.8 - - [y +0000] "GET /b/c HTTP/1.1" 404 22 "-" "kube-probe/1.28" 0`, `9.9.9.9 - - [z +0000] "GET /d HTTP/1.1" 500 3 "-" "x" 1`},
		{"level=info msg=\"cache hit\" key=k1 ttl=5s", "level=info msg=\"cache miss\" key=k2 ttl=9s", "level=info msg=done key=k3 ttl=1s"},
		{`{"message":"user login failed for alice","id":"a1b2"}`, `{"message":"user login failed for bob","id":"c3d4"}`, `{"id":"e5"}`},
		{"id=abc123def456 x:1:2:3 7f3a9c2e-1b4d", "id=zzz999yyy888 x:4:5:6 0a1b2c3d-4e5f", "id=q x:7:8:9 9e8d7c6b-5a49"},
		{"a1b2c3d4 e5f6", "g7h8i9j0 k1l2", "m3n4o5p6 q7r8"},
		{"trace_id=4bf92f3577b34da6a3ce929d0e0e4736 span_id=00f067aa0ba902b7", "trace_id=5bf92f3577b34da6a3ce929d0e0e4737 span_id=10f067aa0ba902b8", "trace_id=x span_id=y"},
	} {
		f.Add(seed[0], seed[1], seed[2])
	}
	f.Fuzz(func(t *testing.T, a, b, c string) {
		lines := []string{a, b, c}
		for _, line := range lines {
			if strings.ContainsAny(line, "<>") {
				return
			}
		}
		miner := newPatternMiner()
		for _, line := range lines {
			miner.Observe("info", line, 0)
		}
		for _, e := range patternEntriesByLevelAndPattern(miner) {
			for _, op := range []string{"|>", "!>"} {
				q := `{app="a"} ` + op + " " + strconv.Quote(e.pattern)
				if err := logql.ParseAndValidate(q); err != nil {
					t.Fatalf("mined %q from %q: %s is rejected: %v", e.pattern, lines, q, err)
				}
			}
		}
	})
}
