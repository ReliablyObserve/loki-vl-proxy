package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// diffRow is one line of the differential fixture: its body and the structured
// metadata stored with it.
type diffRow struct {
	ts     int64
	line   string
	stored map[string]string
}

// diffPostFilter is a label filter after the unwrap in the reference's own
// model: Loki's StringLabelFilter.
type diffPostFilter struct {
	text, name, op, value string
}

// referenceParse is an independent model of Loki v3.7.7's parsers
// (pkg/logql/log/parser.go) for the fixture's lines, written apart from the
// proxy's: `| json` keeps strings, numbers and booleans and flattens objects
// with `_`, skipping arrays and null; `| logfmt` follows referenceLogfmt;
// `| unpack` keeps the strings of a line holding _entry, the last value of a
// key winning. A key named like a label the line already has gets
// `_extracted`; for json and logfmt the first value of a key wins.
func referenceParse(parser, line string, labels map[string]string) {
	stored := map[string]bool{}
	for k := range labels {
		stored[k] = true
	}
	name := func(k string) string {
		if stored[k] {
			return k + "_extracted"
		}
		return k
	}
	switch parser {
	case "logfmt":
		for _, kv := range referenceLogfmt(line) {
			if n := name(kv[0]); !stored[n] {
				if _, done := labels[n]; !done {
					labels[n] = kv[1]
				}
			}
		}
	case "unpack":
		dec := json.NewDecoder(strings.NewReader(line))
		if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
			return
		}
		var pairs [][2]string
		packed := false
		for dec.More() {
			keyTok, err := dec.Token()
			if err != nil {
				return
			}
			var raw json.RawMessage
			if dec.Decode(&raw) != nil {
				return
			}
			var str string
			if json.Unmarshal(raw, &str) != nil {
				continue // not a string
			}
			if keyTok.(string) == "_entry" {
				packed = true
				continue
			}
			pairs = append(pairs, [2]string{name(keyTok.(string)), strings.ReplaceAll(str, "�", " ")})
		}
		if packed {
			for _, kv := range pairs {
				labels[kv[0]] = kv[1]
			}
		}
	case "json":
		dec := json.NewDecoder(strings.NewReader(line))
		dec.UseNumber()
		var obj map[string]any
		if dec.Decode(&obj) != nil {
			return
		}
		keys := make([]string, 0, len(obj))
		for k := range obj {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		set := func(k, v string) {
			if n := name(k); !stored[n] {
				if _, done := labels[n]; !done {
					labels[n] = v
				}
			}
		}
		for _, k := range keys {
			switch v := obj[k].(type) {
			case string:
				set(k, v)
			case json.Number:
				set(k, v.String())
			case bool:
				set(k, strconv.FormatBool(v))
			case map[string]any:
				for nk, nv := range v {
					if n, ok := nv.(json.Number); ok {
						set(k+"_"+nk, n.String())
					}
				}
			}
		}
	}
}

// referenceLogfmt lists the (key, value) pairs Loki's non-strict logfmt
// parser keeps from line, in order: separators are bytes <= ' '; a key is the
// bytes up to '=' or a separator; `k=` or a bare `k` has an empty value, which
// is not kept; a value is either quoted (JSON-like escapes plus \') or runs to
// a separator and holds neither '=' nor '"'; a malformed pair is dropped up to
// the next separator; U+FFFD in a value becomes a space; keys are sanitised.
func referenceLogfmt(line string) [][2]string {
	var out [][2]string
	b := []byte(line)
	i := 0
	for i < len(b) {
		if b[i] <= ' ' {
			i++
			continue
		}
		keyStart := i
		for i < len(b) && b[i] > ' ' && b[i] != '=' && b[i] != '"' {
			i++
		}
		key := string(b[keyStart:i])
		if i < len(b) && b[i] == '"' || (i < len(b) && b[i] == '=' && key == "") || strings.ContainsRune(key, utf8.RuneError) {
			for i < len(b) && b[i] > ' ' {
				i++
			}
			continue
		}
		if i >= len(b) || b[i] <= ' ' {
			continue // bare key: an empty value, not kept
		}
		i++ // '='
		value, ok := "", true
		if i < len(b) && b[i] == '"' {
			j := i + 1
			for ; j < len(b); j++ {
				if b[j] == '\\' {
					j++
					continue
				}
				if b[j] == '"' {
					break
				}
			}
			if j >= len(b) {
				i, ok = len(b), false
			} else {
				value, ok = referenceUnquote(string(b[i+1 : j]))
				i = j + 1
			}
		} else {
			start := i
			for i < len(b) && b[i] > ' ' {
				if b[i] == '=' || b[i] == '"' {
					ok = false
				}
				i++
			}
			value = string(b[start:i])
		}
		value = strings.Map(func(r rune) rune {
			if r == utf8.RuneError {
				return ' '
			}
			return r
		}, value)
		if !ok || value == "" {
			continue
		}
		if k := orderedJSONSanitizeForReference(key); k != "" {
			out = append(out, [2]string{k, value})
		}
	}
	// The first value of a key wins.
	seen := map[string]bool{}
	first := out[:0]
	for _, kv := range out {
		if !seen[kv[0]] {
			seen[kv[0]] = true
			first = append(first, kv)
		}
	}
	return first
}

func referenceUnquote(s string) (string, bool) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' {
			b.WriteByte(s[i])
			continue
		}
		i++
		if i >= len(s) {
			return "", false
		}
		switch s[i] {
		case '"', '\\', '/', '\'':
			b.WriteByte(s[i])
		case 'n':
			b.WriteByte('\n')
		case 't':
			b.WriteByte('\t')
		case 'r':
			b.WriteByte('\r')
		case 'b':
			b.WriteByte('\b')
		case 'f':
			b.WriteByte('\f')
		case 'u':
			if i+4 >= len(s) {
				return "", false
			}
			n, err := strconv.ParseUint(s[i+1:i+5], 16, 32)
			if err != nil {
				return "", false
			}
			b.WriteRune(rune(n))
			i += 4
		default:
			return "", false
		}
	}
	return b.String(), true
}

// orderedJSONSanitizeForReference is Loki's sanitizeLabelKey, written again.
func orderedJSONSanitizeForReference(k string) string {
	k = strings.TrimSpace(k)
	if k == "" {
		return ""
	}
	var b strings.Builder
	if k[0] >= '0' && k[0] <= '9' {
		b.WriteByte('_')
	}
	for _, r := range k {
		if r == '_' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

// vlUnpack models VictoriaLogs' unpack_json / unpack_logfmt over a stored row:
// every key becomes a field holding its text (an array or a boolean as its JSON
// text, null as empty, a nested object as dotted keys), overwriting a stored
// field of the same name; for logfmt the last value of a key wins and a quoted
// value is unquoted. It is a model of the row VictoriaLogs counts; a row Loki
// fails on that it does not flag is accepted only where its value differs from
// Loki's (registered).
func vlUnpack(parser, line string, fields map[string]string) {
	switch parser {
	case "logfmt":
		for _, token := range splitOutsideQuotes(line) {
			k, v, _ := strings.Cut(token, "=")
			if k == "" {
				continue
			}
			if strings.HasPrefix(v, `"`) {
				if u, err := strconv.Unquote(v); err == nil {
					v = u
				}
			}
			fields[k] = v
		}
	case "json", "unpack":
		var obj map[string]json.RawMessage
		if json.Unmarshal([]byte(line), &obj) != nil {
			return
		}
		for k, raw := range obj {
			var s string
			switch {
			case json.Unmarshal(raw, &s) == nil:
				fields[k] = s
			case string(raw) == "null":
				fields[k] = ""
			case bytes.HasPrefix(raw, []byte("{")):
				var nested map[string]json.RawMessage
				if json.Unmarshal(raw, &nested) == nil {
					for nk, nv := range nested {
						fields[k+"."+nk] = string(nv)
					}
				}
			default:
				fields[k] = string(raw)
			}
		}
	}
}

func splitOutsideQuotes(line string) []string {
	var out []string
	start, quoted := -1, false
	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case c == '\\' && quoted:
			i++
		case c == '"':
			quoted = !quoted
			if start < 0 {
				start = i
			}
		case c <= ' ' && !quoted:
			if start >= 0 {
				out = append(out, line[start:i])
				start = -1
			}
		default:
			if start < 0 {
				start = i
			}
		}
	}
	if start >= 0 {
		out = append(out, line[start:])
	}
	return out
}

func diffPostFilterPasses(f diffPostFilter, labels map[string]string, err string) bool {
	actual := labels[f.name]
	switch f.name {
	case "__error__":
		actual = err
	case "__error_details__":
		actual = ""
	}
	switch f.op {
	case "=":
		return actual == f.value
	case "!=":
		return actual != f.value
	case "=~":
		return regexp.MustCompile("^(?:" + f.value + ")$").MatchString(actual)
	default:
		return !regexp.MustCompile("^(?:" + f.value + ")$").MatchString(actual)
	}
}

func diffVisible(ts, start, end, step, window int64, instant bool) bool {
	if instant {
		return ts > start-window && ts <= start
	}
	for t := start; t <= end; t += step {
		if ts > t-window && ts <= t {
			return true
		}
	}
	return false
}

// lokiDiffFails is the reference: Loki v3.7.7's rule for whether the
// aggregation fails with SampleExtractionErr over rows.
func lokiDiffFails(rows []diffRow, parser, label, conv string, post []diffPostFilter, start, end, step, window int64, instant bool) (fails, vlSees bool) {
	for _, row := range rows {
		if !diffVisible(row.ts, start, end, step, window, instant) {
			continue
		}
		labels := map[string]string{"app": "x"}
		for k, v := range row.stored {
			labels[k] = v
		}
		referenceParse(parser, row.line, labels)
		value := labels[label]
		if value == "" {
			continue
		}
		if _, err := convertUnwrap(value, conv); err == nil {
			continue
		}
		passes := true
		for _, f := range post {
			passes = passes && diffPostFilterPasses(f, labels, "SampleExtractionErr")
		}
		if !passes {
			continue
		}
		fails = true
		// Does VictoriaLogs read the same value, so that the counter flags it?
		fields := map[string]string{"app": "x"}
		for k, v := range row.stored {
			fields[k] = v
		}
		vlUnpack(parser, row.line, fields)
		vlPasses := true
		for _, f := range post {
			vlPasses = vlPasses && diffPostFilterPasses(f, fields, "")
		}
		if fields[label] == value && vlPasses {
			vlSees = true
		}
	}
	return fails, vlSees
}

// proxyDiffFails simulates the proxy: the aggregations it checks, the rows its
// counted stats query flags (VictoriaLogs' unpacked value outside the accepted
// forms, the post filters VictoriaLogs applies, the evaluated windows), and the
// confirmation that re-derives Loki's value from the stored row.
func proxyDiffFails(t *testing.T, rows []diffRow, query, parser, label string, post []diffPostFilter, start, end, step int64, instant bool) (checked, fails bool) {
	expr, err := logqlpkg.Parse(query)
	if err != nil {
		t.Fatalf("parse %s: %v", query, err)
	}
	probes := unwrapErrorProbes(expr)
	if len(probes) == 0 {
		return false, false
	}
	probe := probes[0]
	window := int64(probe.window)
	accepted := regexp.MustCompile(unwrapAcceptablePattern(probe.conv))
	for _, row := range rows {
		if !diffVisible(row.ts, start, end, step, window, instant) {
			continue
		}
		fields := map[string]string{"app": "x"}
		for k, v := range row.stored {
			fields[k] = v
		}
		vlUnpack(parser, row.line, fields)
		value := fields[label]
		if value == "" || accepted.MatchString(value) {
			continue
		}
		vlPasses := true
		for _, f := range post {
			vlPasses = vlPasses && diffPostFilterPasses(f, fields, "")
		}
		if !vlPasses {
			continue
		}
		base := map[string]string{"app": "x"}
		for k, v := range row.stored {
			base[k] = v
		}
		if unwrapLokiError(probe, base, row.line, row.line) != nil {
			return true, true
		}
	}
	return true, false
}

// Over generated rows (JSON and logfmt bodies holding numbers, text, empty and
// missing values, durations, byte sizes, arrays, booleans, null, nested
// objects, a key named like stored structured metadata) and generated queries
// (`| json`, `| logfmt`, `| unpack`; every interception form before and after
// the unwrap; string filters on other labels; every conversion; range and
// instant windows; gapped grids), the proxy fails the query exactly when Loki's
// rule does. Two classes of difference are registered and counted, never
// silently accepted: a post filter the detection does not evaluate
// (semantics/unwrap-conversion-error-postfilter-forms) and a stored value Loki
// reads that VictoriaLogs' parser overwrote (semantics/unwrap-conversion-error-stored-value-overwritten).
// conformance: semantics/unwrap-conversion-error, semantics/unwrap-conversion-error-postfilter-forms, semantics/unwrap-conversion-error-stored-value-overwritten, parser-error-and-label-collision
func TestUnwrapConversionErrorDifferential(t *testing.T) {
	values := []string{
		"3", "-1.5", "1e3", "0x1p4", "1_000", ".5", // numbers
		"abc", "x7", "1.2.3", "0x1", // text
		"1.5s", "250ms", "1d", // durations, and a Go reject
		"2KiB", "1kb", "lots", // byte sizes, and a reject
	}
	jsonValues := []string{`[1,2]`, `null`, `true`, `{"x":1}`, `"[1,2]"`, `5`, `"7"`, `"abc"`, `""`}
	pres := []string{"", ` | __error__=""`, ` | drop __error__`, ` | k!="zz"`}
	posts := []diffPostFilter{
		{text: `__error__=""`, name: "__error__", op: "=", value: ""},
		{text: `__error__!="SampleExtractionErr"`, name: "__error__", op: "!=", value: "SampleExtractionErr"},
		{text: `__error__=~".*"`, name: "__error__", op: "=~", value: ".*"},
		{text: `__error__="SampleExtractionErr"`, name: "__error__", op: "=", value: "SampleExtractionErr"},
		{text: `__error_details__=""`, name: "__error_details__", op: "=", value: ""},
		{text: `__error_details__!=""`, name: "__error_details__", op: "!=", value: ""},
		{text: `k="a"`, name: "k", op: "=", value: "a"},
		{text: `k!="a"`, name: "k", op: "!=", value: "a"},
	}
	convs := []struct{ wrap, conv string }{{"%s", ""}, {"duration(%s)", "duration"}, {"bytes(%s)", "bytes"}}
	ranges := []string{"30s", "1m", "2m", "5m"}
	steps := []time.Duration{10 * time.Second, 30 * time.Second, time.Minute, 2 * time.Minute, 5 * time.Minute}
	rng := rand.New(rand.NewSource(42))
	base := time.Unix(1700000000, 0).UnixNano()
	var compared, failing, unevaluatedFailing, overwritten, diverged int
	for iter := 0; iter < 20000; iter++ {
		parser := []string{"logfmt", "json", "unpack"}[rng.Intn(3)]
		label := []string{"v", "v", "x"}[rng.Intn(3)]
		rows := make([]diffRow, rng.Intn(5)+1)
		for i := range rows {
			ts := base + int64(rng.Intn(1800))*int64(time.Second)
			if rng.Intn(3) == 0 {
				ts += int64(rng.Intn(1000)) * int64(time.Millisecond)
			}
			row := diffRow{ts: ts}
			if rng.Intn(3) == 0 {
				row.stored = map[string]string{"x": values[rng.Intn(len(values))]}
			}
			k := []string{"a", "b", ""}[rng.Intn(3)]
			if parser == "logfmt" {
				parts := []string{fmt.Sprintf("n=%d", rng.Intn(5))}
				for _, name := range []string{"v", "x"} {
					// Plain values plus the forms Loki's decoder and VictoriaLogs'
					// unpack_logfmt read differently: a duplicate key (Loki keeps the
					// first), an empty value then another, a bare key, quoted and
					// escaped values, U+FFFD and invalid UTF-8, unicode, a value
					// holding '=', an unterminated quote.
					for n := rng.Intn(3); n >= 0; n-- {
						if rng.Intn(4) == 0 {
							continue
						}
						v := values[rng.Intn(len(values))]
						switch rng.Intn(10) {
						case 0:
							parts = append(parts, name+"=")
							continue
						case 1:
							parts = append(parts, name)
							continue
						case 2:
							v = strconv.Quote(v)
						case 3:
							v = `"a\"` + v + `"`
						case 4:
							v += "\uFFFD"
						case 5:
							v += "\xff"
						case 6:
							v = "ä" + v
						case 7:
							v += "=1"
						case 8:
							v = `"` + v
						}
						parts = append(parts, name+"="+v)
					}
				}
				if k != "" {
					parts = append(parts, "k="+k)
				}
				row.line = strings.Join(parts, " ")
			} else {
				obj := []string{fmt.Sprintf(`"n":%d`, rng.Intn(5))}
				if parser == "unpack" && rng.Intn(2) == 0 {
					obj = append(obj, `"_entry":"line"`)
				}
				for _, name := range []string{"v", "x"} {
					// unpack: a key may repeat (Loki keeps the last value).
					repeats := 0
					if parser == "unpack" {
						repeats = rng.Intn(2)
					}
					for n := repeats; n >= 0; n-- {
						if rng.Intn(4) == 0 {
							continue
						}
						v := jsonValues[rng.Intn(len(jsonValues))]
						if rng.Intn(2) == 0 {
							v = strconv.Quote(values[rng.Intn(len(values))])
						}
						obj = append(obj, strconv.Quote(name)+":"+v)
					}
				}
				if k != "" {
					obj = append(obj, `"k":`+strconv.Quote(k))
				}
				row.line = "{" + strings.Join(obj, ",") + "}"
			}
			rows[i] = row
		}
		c := convs[rng.Intn(len(convs))]
		rangeText := ranges[rng.Intn(len(ranges))]
		window, _ := time.ParseDuration(rangeText)
		var chosen []diffPostFilter
		var postText strings.Builder
		for n := rng.Intn(3); n > 0; n-- {
			f := posts[rng.Intn(len(posts))]
			chosen = append(chosen, f)
			postText.WriteString(" | " + f.text)
		}
		pre := pres[rng.Intn(len(pres))]
		query := fmt.Sprintf(`max_over_time({app="x"} | %s%s | unwrap %s%s [%s])`, parser, pre, fmt.Sprintf(c.wrap, label), postText.String(), rangeText)
		instant := rng.Intn(4) == 0
		start := base + int64(rng.Intn(1500))*int64(time.Second)
		step := steps[rng.Intn(len(steps))]
		end := start + int64(rng.Intn(20))*int64(step) + int64(rng.Intn(int(step/time.Second)))*int64(time.Second)
		if instant {
			end = start
		}
		if strings.Contains(pre, `k!="zz"`) {
			// A filter before the unwrap that Loki and VictoriaLogs both apply.
			kept := rows[:0]
			for _, row := range rows {
				if !strings.Contains(row.line, "zz") {
					kept = append(kept, row)
				}
			}
			rows = kept
		}
		want, vlSees := lokiDiffFails(rows, parser, label, c.conv, chosen, start, end, int64(step), int64(window), instant)
		checked, got := proxyDiffFails(t, rows, query, parser, label, chosen, start, end, int64(step), instant)
		if !checked && !want {
			continue
		}
		if !checked {
			// Either the query intercepts the error (Loki must not fail then) or a
			// post filter the detection does not evaluate.
			intercepted := false
			for _, f := range chosen {
				if (f.name == "__error__" && !diffPostFilterPasses(f, nil, "SampleExtractionErr")) ||
					(f.name == "__error_details__" && !diffPostFilterPasses(f, nil, "SampleExtractionErr")) {
					intercepted = true
				}
			}
			if intercepted {
				t.Fatalf("%s over %+v: Loki fails a query whose post filter drops the error", query, rows)
			}
			unevaluatedFailing++
			continue
		}
		compared++
		if got != want {
			// A false 400 never; a false 200 only where VictoriaLogs reads a
			// value other than Loki's (registered).
			if want && !got && !vlSees {
				if label == "x" && storedValueOverwritten(rows, parser) {
					overwritten++
				} else {
					diverged++
				}
				continue
			}
			t.Fatalf("%s [start=%d end=%d step=%s instant=%v] over %+v: proxy fails=%v, Loki fails=%v", query, start, end, step, instant, rows, got, want)
		}
		if want {
			failing++
		}
	}
	if compared < 10000 || failing < 1000 {
		t.Fatalf("weak generation: %d compared, %d failing", compared, failing)
	}
	t.Logf("%d queries compared (%d fail in Loki); failing in Loki and answered 200: %d with a post filter the detection does not evaluate, %d on a stored value VictoriaLogs' parser overwrote, %d where VictoriaLogs' parser reads another value (duplicate logfmt key, quoting)",
		compared, failing, unevaluatedFailing, overwritten, diverged)
}

// storedValueOverwritten reports whether some row stores x as metadata and its
// line holds a key x too: Loki reads the stored value (the parsed one is
// x_extracted), VictoriaLogs' unpack overwrites the field.
func storedValueOverwritten(rows []diffRow, parser string) bool {
	for _, row := range rows {
		if row.stored["x"] == "" {
			continue
		}
		fields := map[string]string{}
		vlUnpack(parser, row.line, fields)
		if _, ok := fields["x"]; ok {
			return true
		}
	}
	return false
}
