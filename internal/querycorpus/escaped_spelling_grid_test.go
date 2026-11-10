package querycorpus_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// escapedSpellingGrid is the generated grid of the spellings a double-quoted LogQL literal can take, in every
// position a filter-only log query holds a value: each escape kind (\x, \u, \U, octal and the simple ones), a
// control character written raw, the same value written plainly and in backticks, and the values the string
// translator copies as written (a regexp line filter, an exact matcher on a stream field) against the ones it
// quotes (the other positions). The lowering holds only the value, so it must write the translator's bytes for
// each row or delegate it (lower: "LogSelect escaped spelling").
func escapedSpellingGrid() []string {
	// The tool that wrote this file reads a backslash followed by u as an escape of its own: build those with bs.
	const bs = `\`
	literals := []string{
		bs + `x41b`, bs + `x41`, bs + `x4a` + bs + `x4B`, bs + `x61` + bs + `x62`, bs + `xff`, bs + `x00`, bs + `x7f`, bs + `x1f`,
		bs + `x5c`, bs + `x22`, bs + `x2e`, bs + `x7c`, bs + `x27`,
		bs + `u0041b`, bs + `u00e9`, bs + `u00ad`, bs + `u2028`, bs + `ufeff`, bs + `u20ac`,
		bs + `U00000041b`, bs + `U0001F600`, bs + `U0000007f`,
		bs + `101b`, bs + `101`, bs + `000`, bs + `377`, bs + `7`,
		bs + `t`, bs + `n`, bs + `r`, bs + `a`, bs + `b`, bs + `f`, bs + `v`, bs + `"`, bs + bs, bs + bs + `.`, bs + bs + `d+`, `a` + bs + `"b`, `a` + bs + bs + `b`,
		`a` + bs + `x41` + bs + bs + `b`, bs + `x41` + bs + `t` + bs + `"` + bs + bs,
		`error`, `a|b`, `é`, `日本`, `a b`, `a.b`, ``,
		"\t", "a\tb", "\x00", "\x7f", "\x1b", "\x01" + bs + `x41`, "é" + bs + `t`, bs + `x41é`,
		// not valid Go escapes: the parser rejects the query, so no row.
		bs + `d`, bs + `/`, bs + `'`, bs + `x4`, bs + `u12`,
	}
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	templates := []string{
		// line filters: the value is copied as written for |~ and !~ (no or chain), quoted for the rest.
		`{app="a"} |= %s`, `{app="a"} != %s`, `{app="a"} |~ %s`, `{app="a"} !~ %s`, `{app="a"} |> %s`, `{app="a"} !> %s`,
		`{app="a"} |= ip(%s)`, `{app="a"} != ip(%s)`,
		`{app="a"} |= %s or "z"`, `{app="a"} |= "z" or %s`, `{app="a"} |~ %s or "z"`, `{app="a"} |~ "z" or %s`,
		`{app="a"} !~ %s or "z"`, `{app="a"} !~ "z" or %s`, `{app="a"} != %s or "z"`, `{app="a"} |> %s or "<_>"`,
		`{app="a"} |~ %s |~ %s`, `{app="a"} |= "k" |~ %s != "j"`, `{app="a"} |~ %s | b="1"`, `{app="a"} | b="1" !~ %s`,
		// stream selectors: copied as written on a stream field, quoted on any other label.
		`{app=%s}`, `{app!=%s}`, `{app=~%s}`, `{app!~%s}`, `{b=%s}`, `{b!=%s}`, `{b=~%s}`, `{b!~%s}`, `{service_name=%s}`,
		`{app=%s, env="prod"}`, `{env="prod", app=%s}`, `{app=%s, env=%s}`, `{app="a", b=%s, env=%s}`, `{detected_level=%s}`, `{level=%s}`,
		`{app=%s} |= "x"`, `{app=%s} |~ %s`, `{b=%s} |~ %s`,
		// label filters: the value is quoted in every operator.
		`{app="a"} | b=%s`, `{app="a"} | b!=%s`, `{app="a"} | b=~%s`, `{app="a"} | b!~%s`, `{app="a"} | b==%s`, `{app="a"} | b=ip(%s)`, `{app="a"} | b!=ip(%s)`,
		`{app="a"} | b>%s`, `{app="a"} | b=%s and c=%s`, `{app="a"} | (b=%s or c=~%s)`, `{app="a"} | b=%s, c!~%s`,
	}
	quotes := func(l string) []string { return []string{`"` + l + `"`, "`" + l + "`"} }
	for _, l := range literals {
		for _, q := range quotes(l) {
			if strings.Contains(l, "`") {
				continue
			}
			for _, tpl := range templates {
				add(fmt.Sprintf(tpl, repeat(q, strings.Count(tpl, "%s"))...))
			}
		}
	}
	return out
}

func repeat(s string, n int) []any {
	out := make([]any, n)
	for i := range out {
		out[i] = s
	}
	return out
}

// TestEscapedSpellingGridShadow holds the lowering to the string translator on the escaped-spelling grid under
// every profile: 0 mismatches (a row the lowering writes natively is the translator's bytes), the rows that keep
// the client's spelling are delegated by name, and the rows that need no spelling stay native.
// conformance: architecture-syntax-package-layering
func TestEscapedSpellingGridShadow(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	grid := escapedSpellingGrid()
	rows, native, delegated, problems := labelFilterShadow(t, grid)
	t.Logf("%d queries x %d profiles: %d rows, %d lowered, delegated %v", len(grid), len(planProfiles()), rows, native, delegated)
	if len(problems) > 0 {
		t.Fatalf("%d rows lower unlike the string translator:\n%s", len(problems), joinFirst(problems, 20))
	}
	if delegated["LogSelect escaped spelling"] == 0 {
		t.Fatal("no row is delegated for its spelling: the grid does not reach the positions that keep it")
	}
	if native < rows/2 {
		t.Fatalf("only %d of %d rows lowered", native, rows)
	}
}
