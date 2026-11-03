package proxy

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// A `| detected_level <op> "<value>"` filter that no parser precedes selects
// the lines whose ingest-time level, as Loki v3.7.9 assigns it, the matcher
// accepts. The proxy has no ingest path, so the filter derives the value from
// what VictoriaLogs stored: the reference below is written from Loki's source
// alone and calls nothing of the proxy, and the plans are run over the rows
// with the fake VictoriaLogs' filter semantics and the chain emulation that
// the e2e tests pin against VictoriaLogs.

// lokiRefAllowedLevelFields is validation.DefaultAllowedLevelFields
// (Loki v3.7.9 pkg/validation/limits.go:70-85).
var lokiRefAllowedLevelFields = []string{
	"level", "LEVEL", "Level", "log.level", "severity", "SEVERITY", "Severity", "SeverityText",
	"lvl", "LVL", "Lvl", "severity_text", "Severity_Text", "SEVERITY_TEXT",
}

// lokiRefSpellings is normalizeLogLevel's table (pkg/distributor/
// field_detection.go:156-177): a case-insensitive spelling and its level.
var lokiRefSpellings = map[string]string{
	"trace": "trace", "trc": "trace", "debug": "debug", "dbg": "debug",
	"info": "info", "inf": "info", "information": "info",
	"warn": "warn", "wrn": "warn", "warning": "warn",
	"error": "error", "err": "error", "critical": "critical", "fatal": "fatal",
}

// lokiRefKeywords is levelPatterns (field_detection.go:42-55), in its order.
var lokiRefKeywords = []struct{ word, level string }{
	{"trace", "trace"}, {"debug", "debug"}, {"fatal", "fatal"}, {"critical", "critical"},
	{"error", "error"}, {"err", "error"}, {"warning", "warn"}, {"warn", "warn"}, {"info", "info"},
}

// lokiRefNormalize is normalizeLogLevel: a known spelling becomes its level,
// any other value stays as it is.
func lokiRefNormalize(v string) string {
	if level, ok := lokiRefSpellings[strings.ToLower(v)]; ok {
		return level
	}
	return v
}

// lokiRefLevel is extractLogLevel (field_detection.go:96-127) over a line's
// stream labels, structured metadata and text: the stream labels in the
// allowed order, then the structured metadata, then the OTLP severity number
// (:182-203), then the line.
func lokiRefLevel(stream, sm map[string]string, line string) string {
	if v, ok := sm["detected_level"]; ok {
		return lokiRefNormalize(v) // :98-107
	}
	for _, labels := range []map[string]string{stream, sm} { // :110-116
		for _, name := range lokiRefAllowedLevelFields {
			if v, ok := labels[name]; ok {
				return lokiRefNormalize(v)
			}
		}
	}
	if number, ok := sm["severity_number"]; ok {
		n, err := strconv.Atoi(number)
		switch {
		case err != nil:
			return "info"
		case n == 0:
			return "unknown"
		case n <= 4:
			return "trace"
		case n <= 8:
			return "debug"
		case n <= 12:
			return "info"
		case n <= 16:
			return "warn"
		case n <= 20:
			return "error"
		case n <= 24:
			return "fatal"
		}
		return "unknown"
	}
	return lokiRefLineLevel(line)
}

// lokiRefLineLevel is extractLogLevelFromLogLine (:219-248): a JSON line's
// first allowed key holding a string (document order, two levels deep), a
// logfmt line's allowed key (case-insensitive, the first name of the list
// wins), mapped through the spellings; anything else is the keyword scan.
func lokiRefLineLevel(line string) string {
	trimmed := strings.TrimFunc(line, unicode.IsSpace)
	var v string
	switch {
	case strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}"): // isJSON :327-346
		v = lokiRefJSONLevel(trimmed)
	case strings.Contains(line, "="): // isLogFmt :319-325
		best := len(lokiRefAllowedLevelFields)
		for _, token := range strings.Fields(line) {
			key, value, _ := strings.Cut(token, "=")
			for i, name := range lokiRefAllowedLevelFields {
				if strings.EqualFold(key, name) && i < best {
					best, v = i, strings.Trim(value, `"`)
				}
			}
		}
	default:
		return lokiRefKeywordLevel(line)
	}
	if level, ok := lokiRefSpellings[strings.ToLower(v)]; ok {
		return level
	}
	return lokiRefKeywordLevel(line)
}

// lokiRefJSONLevel is getLevelUsingJSONParser (:288-317) with the default
// depth of 2 (limits.go:349): the first allowed key holding a string, in
// document order, in the object or an object nested in it.
func lokiRefJSONLevel(line string) string {
	var walk func(raw json.RawMessage, depth int) (string, bool)
	walk = func(raw json.RawMessage, depth int) (string, bool) {
		dec := json.NewDecoder(strings.NewReader(string(raw)))
		if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
			return "", false
		}
		for dec.More() {
			tok, err := dec.Token()
			if err != nil {
				return "", false
			}
			key, _ := tok.(string)
			var value json.RawMessage
			if err := dec.Decode(&value); err != nil {
				return "", false
			}
			switch {
			case len(value) > 0 && value[0] == '"':
				for _, name := range lokiRefAllowedLevelFields {
					if key == name {
						s, _ := strconv.Unquote(string(value))
						return s, true
					}
				}
			case len(value) > 0 && value[0] == '{' && depth+1 < 2:
				if v, ok := walk(value, depth+1); ok {
					return v, true
				}
			}
		}
		return "", false
	}
	v, _ := walk(json.RawMessage(line), 0)
	return v
}

// lokiRefKeywordLevel is detectLevelFromLogLine (:388-403): the lowercased
// line, the earliest bounded occurrence of each keyword, the smallest position
// winning (the earlier keyword on a tie); unknown when none.
func lokiRefKeywordLevel(line string) string {
	lower := strings.ToLower(line)
	left := func(pos int) bool { // isLeftWordBoundary :352-358
		return pos < 0 || strings.IndexByte(" \t\n[({\"=", lower[pos]) >= 0
	}
	right := func(pos int) bool { // isRightWordBoundary :364-370
		return pos >= len(lower) || strings.IndexByte(" \t\n[](){}:,!\"=", lower[pos]) >= 0
	}
	best, level := len(lower), "unknown"
	for _, k := range lokiRefKeywords {
		for offset := 0; ; {
			pos := strings.Index(lower[offset:], k.word)
			if pos < 0 {
				break
			}
			pos += offset
			if left(pos-1) && right(pos+len(k.word)) {
				if pos < best {
					best, level = pos, k.level
				}
				break
			}
			offset = pos + 1
		}
	}
	return level
}

// lokiRefMatches is a Loki label filter on a value: = and != compare; =~ and
// !~ match the regexp after Loki's simplifier (pkg/logql/log/filter.go
// parseRegexpFilter, RegexSimplifier.Simplify: a literal alternation such as
// `w|wA` is a substring check per branch, `=~""` keeps every value), which
// translator.NewLabelFilterCondition models and the label filter e2e tests pin
// against Loki.
func lokiRefMatches(op, value, v string) bool {
	switch op {
	case "=":
		return v == value
	case "!=":
		return v != value
	}
	dc, err := translator.NewLabelFilterCondition("detected_level", op, value)
	if err != nil {
		panic(err)
	}
	return dc.Matches(v)
}

// dlFilterRow is one stored line: its stream labels, its structured metadata
// (VictoriaLogs stores both as fields) and its text.
type dlFilterRow struct {
	stream, sm map[string]string
	line       string
}

func (r dlFilterRow) values() map[string]string {
	values := map[string]string{"_msg": r.line}
	for _, m := range []map[string]string{r.stream, r.sm} {
		for k, v := range m {
			values[k] = v
		}
	}
	return values
}

// planKeeps runs a filter plan over one row as VictoriaLogs does: the
// prefilter on the stored row, the filter pipes before the chain, then the
// chain (emulateDetectedLevelChainFor, pinned against VictoriaLogs by the e2e
// tests) and the filters after it.
func planKeeps(t testing.TB, plan translator.DetectedLevelFilterPlan, values map[string]string) bool {
	t.Helper()
	if plan.Never {
		return false
	}
	if plan.Prefilter != "" && !fakeLogsQLMatch(t, plan.Prefilter, values) {
		return false
	}
	if plan.Pipes == "" {
		return true
	}
	vals := cloneStringMap(values)
	derived := false
	for _, pipe := range splitLogsQLPipes(strings.TrimPrefix(plan.Pipes, "| ")) {
		if !derived && chainStart(pipe) {
			level := emulateDetectedLevelChainFor(vals, nil)
			vals["__dl"], vals["detected_level"] = level, level
			derived = true
			continue
		}
		if filter, ok := strings.CutPrefix(pipe, "filter "); ok && !fakeLogsQLMatch(t, filter, vals) {
			return false
		}
	}
	if !derived {
		t.Fatalf("plan pipes hold no chain: %s", plan.Pipes)
	}
	return true
}

// dlFilterMatchers covers every operator, unknown, a multi-select and words
// Loki keeps raw.
var dlFilterMatchers = []struct{ op, value string }{
	{"=", "error"}, {"=", "unknown"}, {"=", "info"}, {"=", "warn"}, {"=", "debug"}, {"=", "trace"},
	{"=", "critical"}, {"=", "fatal"}, {"!=", "info"}, {"!=", "unknown"}, {"!=", ""}, {"=", ""},
	{"=~", "info|error"}, {"=~", "unknown|error"}, {"=~", "warn|error"}, {"!~", "warn|error"}, {"!~", "info|debug"},
	{"=~", "Notice|warn"}, {"=", "Notice"}, {"!=", "Notice"}, {"=", "WARN"}, {"=~", "unknown"},
	// Loki's simplified label filter regexps: a substring check, every value.
	{"=~", "otice.*"}, {"=~", ""}, {"!~", ""},
}

var dlFilterModes = []struct {
	name string
	mode translator.DetectedLevelFilterMode
}{
	{"logs", translator.DetectedLevelFilterLogs},
	{"metric", translator.DetectedLevelFilterMetric},
	{"stored only", translator.DetectedLevelFilterStored},
}

// checkDetectedLevelFilter asserts that every matcher, in every mode, keeps
// exactly the rows whose Loki level it accepts; the stored-only mode is the
// label with -detected-level-body-scan=false.
func checkDetectedLevelFilter(t testing.TB, name string, row dlFilterRow) {
	t.Helper()
	want, storedOnly := lokiRefLevel(row.stream, row.sm, row.line), lokiRefLevel(row.stream, row.sm, "")
	for _, m := range dlFilterMatchers {
		for _, mode := range dlFilterModes {
			plan, err := translator.BuildDetectedLevelFilter(m.op, m.value, translator.DetectedLevelFieldNames, mode.mode)
			if err != nil {
				t.Fatal(err)
			}
			level := want
			if mode.mode == translator.DetectedLevelFilterStored {
				level = storedOnly
			}
			if got, ok := planKeeps(t, plan, row.values()), lokiRefMatches(m.op, m.value, level); got != ok {
				t.Fatalf("%s, %s: detected_level%s%q keeps the row = %v, Loki's level %q gives %v\nrow %+v\nplan %+v", name, mode.name, m.op, m.value, got, level, ok, row, plan)
			}
		}
	}
}

// The edge cases, each with the level Loki assigns.
// conformance: severity/detected-level-filter-json-level, severity-detected-level-derivation, loki_api_v1_query_range
func TestDetectedLevelFilterAnswersLikeLoki(t *testing.T) {
	cases := []struct {
		name string
		row  dlFilterRow
		want string
	}{
		{"a stream label decides over the line's logfmt key", dlFilterRow{stream: map[string]string{"level": "info"}, line: `level=error msg="job failed"`}, "info"},
		{"a stream label decides over a cache-hit line", dlFilterRow{stream: map[string]string{"level": "debug"}, line: `level=info msg="cache miss"`}, "debug"},
		{"a stream label decides over a keyword", dlFilterRow{stream: map[string]string{"level": "info"}, line: `2026 [1] db ERROR: deadlock detected`}, "info"},
		{"a line with no level is unknown", dlFilterRow{line: "ordinary visual sample 7"}, "unknown"},
		{"an empty line is unknown", dlFilterRow{line: ""}, "unknown"},
		{"json level key", dlFilterRow{line: `{"level":"warn","msg":"slow"}`}, "warn"},
		{"json spelling", dlFilterRow{line: `{"level":"WARNING"}`}, "warn"},
		{"json abbreviation", dlFilterRow{line: `{"lvl":"dbg","msg":"x"}`}, "debug"},
		{"json level that is a number reads the keywords", dlFilterRow{line: `{"level":3,"msg":"request error"}`}, "error"},
		{"json raw level word reads the keywords", dlFilterRow{line: `{"level":"notice","msg":"fine"}`}, "unknown"},
		{"json message keyword", dlFilterRow{line: `{"msg":"request error"}`}, "error"},
		{"logfmt level key", dlFilterRow{line: `level=ERR msg=boom`}, "error"},
		{"logfmt hint priority", dlFilterRow{line: `severity=warn level=error`}, "error"},
		{"logfmt empty value reads the keywords", dlFilterRow{line: `level= msg=quiet`}, "unknown"},
		{"plain keyword, upper case", dlFilterRow{line: `2026 [1] db ERROR: deadlock`}, "error"},
		{"plain keyword after a colon is no word", dlFilterRow{line: `misc:error happened`}, "unknown"},
		{"plain warning", dlFilterRow{line: `the warning: disk full`}, "warn"},
		{"the earliest keyword wins", dlFilterRow{line: `info then error`}, "info"},
		{"a stored word Loki does not know stays as stored", dlFilterRow{stream: map[string]string{"level": "Notice"}, line: "error"}, "Notice"},
		{"structured metadata level", dlFilterRow{sm: map[string]string{"level": "WARN"}, line: "info"}, "warn"},
		{"stream label over structured metadata of the same name list", dlFilterRow{stream: map[string]string{"level": "error"}, sm: map[string]string{"lvl": "info"}}, "error"},
		{"OTLP severity number", dlFilterRow{sm: map[string]string{"severity_number": "17"}, line: "fine"}, "error"},
		{"OTLP severity number zero", dlFilterRow{sm: map[string]string{"severity_number": "0"}, line: "error"}, "unknown"},
		{"OTLP severity number out of range", dlFilterRow{sm: map[string]string{"severity_number": "30"}}, "unknown"},
		{"a level text over the number", dlFilterRow{sm: map[string]string{"severity_number": "17", "severity_text": "warning"}}, "warn"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := lokiRefLevel(tc.row.stream, tc.row.sm, tc.row.line); got != tc.want {
				t.Fatalf("reference level %q, want %q", got, tc.want)
			}
			checkDetectedLevelFilter(t, tc.name, tc.row)
		})
	}
}

// Two consecutive level filters share one chain and keep the rows both
// matchers accept, in the log and metric forms.
// conformance: severity/detected-level-filter-json-level, severity-detected-level-derivation
func TestDetectedLevelFilterChainedAnswersLikeLoki(t *testing.T) {
	rng := rand.New(rand.NewSource(1103))
	pairs := [][2]struct{ op, value string }{
		{{"!=", "debug"}, {"!=", "info"}},
		{{"=~", "warn|error"}, {"!=", "warn"}},
		{{"!=", "unknown"}, {"=~", "info|error"}},
		{{"!=", ""}, {"=", "unknown"}},
	}
	for _, mode := range dlFilterModes[:2] {
		for _, pair := range pairs {
			var plans []translator.DetectedLevelFilterPlan
			var pre []string
			for _, m := range pair {
				plan, err := translator.BuildDetectedLevelFilter(m.op, m.value, translator.DetectedLevelFieldNames, mode.mode)
				if err != nil {
					t.Fatal(err)
				}
				plans = append(plans, plan)
				if plan.Prefilter != "" {
					pre = append(pre, "("+plan.Prefilter+")")
				}
			}
			joined := translator.DetectedLevelFilterPlan{
				Prefilter: strings.Join(pre, " "),
				Pipes:     translator.JoinDetectedLevelFilters(plans, translator.DetectedLevelFieldNames, mode.mode),
			}
			if strings.Count(joined.Pipes, translatorChainMarker) > 1 {
				t.Fatalf("%s %v: the chain runs twice", mode.name, pair)
			}
			for i := 0; i < 300; i++ {
				row := dlRandomRow(rng)
				level := lokiRefLevel(row.stream, row.sm, row.line)
				want := lokiRefMatches(pair[0].op, pair[0].value, level) && lokiRefMatches(pair[1].op, pair[1].value, level)
				got := want
				if joined.Pipes != "" || joined.Prefilter != "" {
					got = planKeeps(t, joined, row.values())
				}
				if got != want {
					t.Fatalf("%s %v: keeps %v, Loki's level %q gives %v\nrow %+v", mode.name, pair, got, level, want, row)
				}
			}
		}
	}
}

// The chain's known differences from Loki's ingest, inherent to the pipes
// (translator/detected_level_logsql.go): the random rows below leave them
// out, and these pin what the filter does on them.
// conformance: severity-detected-level-derivation
func TestDetectedLevelFilterKnownChainDifferences(t *testing.T) {
	cases := []struct {
		name       string
		row        dlFilterRow
		loki, vl   string
		matcherVal string
	}{
		// unpack_json reads top-level keys; Loki reads one nested level too.
		{"a nested JSON level key", dlFilterRow{line: `{"a":{"level":"dbg"}}`}, "debug", "unknown", "debug"},
		// Two level fields of different names on the stream and in metadata:
		// Loki reads the stream label first, VictoriaLogs stores both as
		// fields and the chain reads Loki's list order.
		{"stream lvl and metadata level", dlFilterRow{stream: map[string]string{"lvl": "info"}, sm: map[string]string{"level": "error"}}, "info", "error", "info"},
	}
	for _, tc := range cases {
		if got := lokiRefLevel(tc.row.stream, tc.row.sm, tc.row.line); got != tc.loki {
			t.Fatalf("%s: reference level %q, want %q", tc.name, got, tc.loki)
		}
		plan, err := translator.BuildDetectedLevelFilter("=", tc.vl, translator.DetectedLevelFieldNames, translator.DetectedLevelFilterLogs)
		if err != nil {
			t.Fatal(err)
		}
		if !planKeeps(t, plan, tc.row.values()) {
			t.Fatalf("%s: the chain's level is no longer %q", tc.name, tc.vl)
		}
	}
}

// dlRandomRow builds a row from the shapes the stack and real collectors
// send, without the known chain differences: one level field name at most
// (on the stream or in metadata), top-level JSON keys, plain logfmt.
func dlRandomRow(rng *rand.Rand) dlFilterRow {
	pick := func(list ...string) string { return list[rng.Intn(len(list))] }
	levelWords := []string{"info", "INFO", "warn", "Warning", "error", "ERR", "debug", "dbg", "trace", "fatal", "critical", "Notice", "information", "wrn"}
	row := dlFilterRow{stream: map[string]string{"app": "a"}, sm: map[string]string{}}
	switch rng.Intn(6) {
	case 0:
		row.stream[pick("level", "lvl", "severity")] = pick(levelWords...)
	case 1:
		row.sm[pick("level", "severity_text", "Level")] = pick(levelWords...)
	case 2:
		row.sm["severity_number"] = strconv.Itoa(rng.Intn(27))
	}
	words := []string{"ok", "request", "done", "user", "err", "error", "warn", "info", "debug", "misc:error", "foo-error", "[ERROR]", "Info:", "warning!", "notice"}
	text := func() string {
		n := 1 + rng.Intn(4)
		parts := make([]string, n)
		for i := range parts {
			parts[i] = pick(words...)
		}
		return strings.Join(parts, " ")
	}
	switch rng.Intn(4) {
	case 0:
		row.line = text()
	case 1:
		obj := map[string]any{"msg": text()}
		if rng.Intn(2) == 0 {
			obj[pick("level", "lvl", "severity")] = pick(levelWords...)
		}
		b, _ := json.Marshal(obj)
		row.line = string(b)
	case 2:
		kv := []string{"msg=" + strconv.Quote(text())}
		if rng.Intn(2) == 0 {
			kv = append(kv, pick("level", "lvl", "severity")+"="+pick(levelWords...))
		}
		rng.Shuffle(len(kv), func(i, j int) { kv[i], kv[j] = kv[j], kv[i] })
		row.line = strings.Join(kv, " ")
	default:
		row.line = ""
	}
	return row
}

// The plans keep exactly the rows Loki's level accepts, in every mode, over
// seeded random rows.
// conformance: severity/detected-level-filter-json-level, severity-detected-level-derivation
func TestDetectedLevelFilterDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(20261103))
	n := 1500
	if testing.Short() {
		n = 300
	}
	for i := 0; i < n; i++ {
		checkDetectedLevelFilter(t, fmt.Sprintf("row %d", i), dlRandomRow(rng))
	}
}

// fuzzLogfmtKeyRE finds a logfmt key of a line.
var fuzzLogfmtKeyRE = regexp.MustCompile(`(?:^|\s)([^\s=]+)=`)

// FuzzDetectedLevelFilterLikeLoki drives the filter plans with arbitrary
// stored levels, lines and matchers, against Loki's level. Inputs on the
// chain's known differences (a line starting with a brace that is not one
// JSON object, a nested or escaped JSON level, a logfmt key Loki reads by
// case folding that is not one of the spellings the chain unpacks) and
// non-ASCII input (VictoriaLogs' folding of i() is not modelled by the fake)
// are skipped.
func FuzzDetectedLevelFilterLikeLoki(f *testing.F) {
	f.Add("info", `level=error msg="job failed"`, uint8(0), "error")
	f.Add("", "ordinary visual sample 7", uint8(0), "unknown")
	f.Add("", `{"level":"WARNING"}`, uint8(2), "warn|error")
	f.Add("Notice", "error", uint8(1), "Notice")
	f.Add("", "misc:error happened", uint8(3), "info|debug")
	f.Fuzz(func(t *testing.T, stored, line string, op uint8, value string) {
		for _, s := range []string{stored, line, value} {
			for _, r := range s {
				if r > unicode.MaxASCII || r < ' ' && r != '\t' {
					return
				}
			}
		}
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "{") {
			var obj map[string]any
			if json.Unmarshal([]byte(trimmed), &obj) != nil || strings.Contains(trimmed, `\`) || strings.Count(trimmed, "{") > 1 {
				return
			}
		} else if strings.Contains(line, "=") {
			for _, m := range fuzzLogfmtKeyRE.FindAllStringSubmatch(line, -1) {
				for _, name := range lokiRefAllowedLevelFields {
					if strings.EqualFold(m[1], name) && m[1] != name {
						return
					}
				}
				if strings.ContainsAny(m[1], `"'`) {
					return
				}
			}
			if strings.ContainsAny(line, `"'\`) {
				return // quoted logfmt values: the fake's splitter is not Loki's decoder
			}
		}
		ops := []string{"=", "!=", "=~", "!~"}
		o := ops[int(op)%len(ops)]
		if o == "=~" || o == "!~" {
			if _, err := translator.NewLabelFilterCondition("detected_level", o, value); err != nil {
				return // Loki rejects the query
			}
		}
		row := dlFilterRow{stream: map[string]string{"app": "a"}, line: line}
		if stored = strings.TrimSpace(stored); stored != "" {
			row.stream["level"] = stored
		}
		want := lokiRefLevel(row.stream, nil, line)
		for _, mode := range dlFilterModes[:2] {
			plan, err := translator.BuildDetectedLevelFilter(o, value, translator.DetectedLevelFieldNames, mode.mode)
			if err != nil {
				return
			}
			if got, ok := planKeeps(t, plan, row.values()), lokiRefMatches(o, value, want); got != ok {
				t.Fatalf("%s: detected_level%s%q keeps %v, Loki's level %q gives %v\nrow %+v\nplan prefilter %s", mode.name, o, value, got, want, ok, row, plan.Prefilter)
			}
		}
	})
}

// dlStackRows mirrors the stack's services: a logfmt service whose stream
// label disagrees with its lines (worker-service), a cache whose stream label
// is debug and whose lines say info (cache-redis), JSON lines whose stream
// label agrees (api-gateway), and plain lines with no level anywhere
// (visual-detected-level).
func dlStackRows(s0 time.Time) []pushdownRow {
	var rows []pushdownRow
	add := func(i int, stream map[string]string, msg string) {
		stream["env"] = "production"
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(i)*time.Second + 500*time.Millisecond), stream: stream, msg: msg})
	}
	for i := 0; i < 600; i += 6 {
		add(i, map[string]string{"service_name": "worker-service", "level": "info"}, []string{`level=info msg="job completed"`, `level=error msg="job failed"`, `level=warn msg="job retry"`}[i%3])
		add(i+1, map[string]string{"service_name": "cache-redis", "level": "debug"}, []string{`level=debug msg="cache hit"`, `level=info msg="cache miss"`}[i%2])
		lvl := []string{"info", "warn", "error"}[i%3]
		add(i+2, map[string]string{"service_name": "api-gateway", "level": lvl}, `{"level":"`+lvl+`","msg":"request"}`)
		add(i+3, map[string]string{"service_name": "visual-detected-level"}, "ordinary visual sample "+strconv.Itoa(i))
		add(i+4, map[string]string{"service_name": "db-postgres", "level": "info"}, "2026 [1] app_prod ERROR: deadlock detected")
	}
	return rows
}

// dlStackReference is Loki's answer over dlStackRows: per service and level,
// the lines in (start, end] the matcher accepts.
func dlStackReference(rows []pushdownRow, op, value string, start, end time.Time) map[string]int {
	out := map[string]int{}
	for _, row := range rows {
		if !row.ts.After(start) || row.ts.After(end) {
			continue
		}
		level := lokiRefLevel(row.stream, nil, row.msg)
		if lokiRefMatches(op, value, level) {
			out[row.stream["service_name"]+"/"+level]++
		}
	}
	return out
}

// Through the proxy's handlers, against a recording fake VictoriaLogs: a log
// query and a metric query with a detected_level filter answer what Loki
// answers over the stack's services, with one VictoriaLogs query per log
// window, no logfmt unpack of every line, and a metric request that reads
// the presence of the selector's level fields once.
// conformance: severity/detected-level-filter-json-level, severity-detected-level-derivation, loki_api_v1_query_range
func TestDetectedLevelFilterThroughHandlersLikeLoki(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	rows := dlStackRows(s0)
	start, end := s0, s0.Add(10*time.Minute)
	matchers := []struct{ op, value string }{
		{"!=", "info"}, {"=~", "unknown|error"}, {"=", "unknown"}, {"=~", "error|warn"}, {"=", "error"}, {"!~", "info|debug"},
	}
	for _, m := range matchers {
		want := dlStackReference(rows, m.op, m.value, start, end)
		t.Run("logs "+m.op+m.value, func(t *testing.T) {
			srv, fake := newPushdownFakeVL(t, rows, nil)
			p := newFilterPushdownProxy(t, srv.URL)
			query := `{env="production"} | detected_level` + m.op + strconv.Quote(m.value)
			got := runDetectedLevelLogQuery(t, p, query, start, end)
			if fmt.Sprint(sortedCounts(got)) != fmt.Sprint(sortedCounts(want)) {
				t.Fatalf("%s:\n got %v\nwant %v", query, sortedCounts(got), sortedCounts(want))
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			for _, q := range fake.rawQueries {
				if strings.Contains(q.query, "| unpack_logfmt | filter") || strings.Count(q.query, translatorChainMarker) != 1 {
					t.Fatalf("log query sent %q", q.query)
				}
			}
		})
		t.Run("metric "+m.op+m.value, func(t *testing.T) {
			srv, fake := newPushdownFakeVL(t, rows, nil)
			p := newFilterPushdownProxy(t, srv.URL)
			query := `sum by (service_name, detected_level) (count_over_time({env="production"} | detected_level` + m.op + strconv.Quote(m.value) + ` [10m]))`
			got := map[string]int{}
			for key, points := range runJSONVolumeQueryRange(t, p, query, end, end, 10*time.Minute) {
				labels := map[string]string{}
				for _, m := range regexp.MustCompile(`(\w+)="([^"]*)"`).FindAllStringSubmatch(key, -1) {
					labels[m[1]] = m[2]
				}
				for _, v := range points {
					n, _ := strconv.ParseFloat(v, 64)
					got[labels["service_name"]+"/"+labels["detected_level"]] += int(n)
				}
			}
			if fmt.Sprint(sortedCounts(got)) != fmt.Sprint(sortedCounts(want)) {
				t.Fatalf("%s:\n got %v\nwant %v", query, sortedCounts(got), sortedCounts(want))
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			for _, q := range fake.stats {
				if strings.Contains(q, "| unpack_logfmt | filter") || strings.Count(q, translatorChainMarker) != 1 {
					t.Fatalf("metric query sent %q", q)
				}
			}
			if len(fake.presenceCalls) > 1 {
				t.Fatalf("the presence of the level fields was probed %d times: %q", len(fake.presenceCalls), fake.presenceCalls)
			}
			for _, q := range fake.presenceCalls {
				if strings.Contains(q, "i(") {
					t.Fatalf("the presence probe reads the filter's prefilter, not the selector: %q", q)
				}
			}
		})
	}
}

// translatorChainMarker is the first pipe of the detected_level chain.
const translatorChainMarker = `format if (detected_level:*) "<detected_level>" as __dl_s`

func sortedCounts(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		if v != 0 {
			out = append(out, k+"="+strconv.Itoa(v))
		}
	}
	sort.Strings(out)
	return out
}

// runDetectedLevelLogQuery runs a log query over (start, end] and counts its
// lines per service and detected_level.
func runDetectedLevelLogQuery(t *testing.T, p *Proxy, query string, start, end time.Time) map[string]int {
	t.Helper()
	params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano()+1, 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "limit": {"5000"}, "direction": {"backward"}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
	p.handleQueryRange(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: status %d: %s", query, rec.Code, rec.Body)
	}
	var resp struct {
		Data struct {
			Result []struct {
				Stream map[string]string `json:"stream"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("%s: %v: %s", query, err, rec.Body)
	}
	got := map[string]int{}
	for _, s := range resp.Data.Result {
		for _, v := range s.Values {
			level := s.Stream["detected_level"]
			if len(v) > 2 {
				if meta, ok := v[2].(map[string]any); ok {
					if sm, ok := meta["structuredMetadata"].(map[string]any); ok {
						if l, ok := sm["detected_level"].(string); ok {
							level = l
						}
					}
				}
			}
			for _, field := range []string{"__dl", "__dl_s", "__dl_h"} {
				if strings.Contains(fmt.Sprint(v), field) {
					t.Fatalf("%s: a scratch field reached the response: %v", query, v)
				}
			}
			got[s.Stream["service_name"]+"/"+level]++
		}
	}
	return got
}

// A bare metric over two level filters still names detected_level in its
// series (one chain, so the heuristics that skip it see no parser), and a
// log query over two level filters does not read as one that unpacks _msg.
// conformance: severity/detected-level-filter-json-level, severity-detected-level-derivation
func TestDetectedLevelChainedFiltersKeepTheirShape(t *testing.T) {
	metric, err := translator.TranslateLogQLWithStreamFields(`count_over_time({app="x"} | detected_level!="debug" | detected_level!="info" [5m])`, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := addStatsByStreamClause(metric, nil); !strings.Contains(got, "| stats by (_stream, level, detected_level)") {
		t.Fatalf("the series lose detected_level: %s", got[strings.LastIndex(got, "| stats"):])
	}
	logs, err := translator.TranslateLogQL(`{app="x"} | detected_level!="debug" | detected_level!="info"`)
	if err != nil {
		t.Fatal(err)
	}
	if unpacksMessage(logs) {
		t.Fatalf("two level filters read as a query that unpacks _msg: %s", logs)
	}
}

// The witness cannot ride a stats query whose rows a fields pipe (keep) has
// narrowed: the pipe would drop it, and a flagged row would be counted with
// no second request. Such a request keeps the full chain.
// conformance: severity-detected-level-derivation
func TestDetectedLevelWitnessRefusesAFieldsPipe(t *testing.T) {
	if _, ok := withWitnessGroup(` | filter (detected_level:="error") OR __lw:* | fields _time, _msg, _stream, app | stats count()`); ok {
		t.Fatal("the witness was added behind a fields pipe")
	}
	if got, ok := withWitnessGroup(` | filter (detected_level:="error") OR __lw:* | stats by (app) count()`); !ok || !strings.Contains(got, "by (app, __lw)") {
		t.Fatalf("the witness was not added: %q %v", got, ok)
	}
}
