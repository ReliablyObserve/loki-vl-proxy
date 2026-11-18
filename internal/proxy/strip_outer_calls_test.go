package proxy

import (
	"strings"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// stripOuterCallsRescanning is stripOuterCalls reading each call's first argument from its own text: the reference
// the linear read is held to.
func stripOuterCallsRescanning(logql string, prefixes ...string) string {
	for {
		logql = strings.TrimSpace(logql)
		inner, ok := "", false
		for _, prefix := range prefixes {
			if strings.HasPrefix(logql, prefix) {
				args := logql[len(prefix):]
				inner, ok = logqlpkg.FirstArgument(args)
				end := len(inner)
				for end < len(args) && args[end] == ',' {
					end += 1 + logqlpkg.FirstArgumentLen(args[end+1:])
				}
				ok = ok && end < len(args) && strings.TrimSpace(args[end+1:]) == ""
				break
			}
		}
		if !ok {
			return logql
		}
		logql = inner
	}
}

// stripOuterCallsTexts are label_replace nestings in the spellings the strip reads, and texts it leaves alone.
var stripOuterCallsTexts = []string{
	`label_replace(rate({app="a"}[1m]), "a", "b", "c", "d")`,
	`label_replace(label_replace(label_replace(sum by (a) (rate({app="a"}[1m])), "a", "b", "c", "d"), "a", "b", "c", "d"), "a", "b", "c", "d")`,
	` label_replace( label_replace( rate({app="a, ("}[1m]) , "a", "(", "c", "d") , "a", ")", "c", "d" ) `,
	"label_replace(\n\tlabel_replace(rate({app=\"a\"}[1m]), `a`, `b`, `c`, `d`),\n\t\"a\", \"b\", \"c\", \"d\")",
	`label_replace(label_replace(rate({app="a"}[1m]), "a", "b", "c", "d") / 2, "a", "b", "c", "d")`,
	`label_replace(label_replace(rate({app="a"}[1m]), "a", "b", "c", "d"), "a", "b", "c", "d") + 1`,
	`label_replace(label_replace(rate({app="a"}[1m], "a", "b", "c", "d"), "a", "b", "c", "d")`,
	`label_replace(label_replace(label_replace(rate({app="\""}[1m]), "a", "b", "c", "d"), "a", "b", "c", "d")`,
	`label_replace(label_replace(`,
	`sum(label_replace(rate({app="a"}[1m]), "a", "b", "c", "d"))`,
}

// The strip reads a nesting through one reader of the whole text and strips what reading each call from its own
// text strips.
//
// conformance: semantics/nested-calls-read-once
func TestStripOuterCallsMatchesRescanning(t *testing.T) {
	for _, q := range stripOuterCallsTexts {
		if got, want := stripOuterCalls(q, "label_replace("), stripOuterCallsRescanning(q, "label_replace("); got != want {
			t.Errorf("stripOuterCalls(%q) = %q, want %q", q, got, want)
		}
	}
}

// conformance: semantics/nested-calls-read-once
func FuzzStripOuterCallsMatchesRescanning(f *testing.F) {
	for _, q := range stripOuterCallsTexts {
		f.Add(q)
	}
	f.Fuzz(func(t *testing.T, q string) {
		if len(q) > 2048 {
			return
		}
		if got, want := stripOuterCalls(q, "label_replace("), stripOuterCallsRescanning(q, "label_replace("); got != want {
			t.Errorf("stripOuterCalls(%q) = %q, want %q", q, got, want)
		}
	})
}
