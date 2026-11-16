package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// The unwrap conversion, Loki's parsers as the conversion-error check re-reads a line, and the error a metric
// answers, held to Loki's own answers for the same lines and queries (recorded with the engine and pipeline oracles
// of test/oracle/loki; re-record with LVP_RUN_ORACLES=1). Every input here is this file's own.

// unwrapRecordedT is the time of every recorded line; requests evaluate at unwrapRecordedT plus a second.
const unwrapRecordedT = int64(1_800_000_000) * 1e9

// unwrapEdgeValues are the unwrapped values recorded per conversion: number spellings, units, overflow, NaN and
// infinities, signs, blanks and spaces.
var unwrapEdgeValues = map[string][]string{
	"": {"0", "-0", "+5", "5.", ".5", "1e3", "1E-3", "1_000", "1__0", "_1", "0x1p4", "0x1P-2", "0x10", "0b101", "1e309", "-1e309",
		"1e-400", "1.7976931348623157e308", "NaN", "nan", "-nan", "+NaN", "Inf", "+inf", "-Infinity", "infinity", "infin",
		" 5", "5 ", "12ms", "abc", "1.2.3", "٣", "５", "1e+5", "1e", "-"},
	"duration": {"0", "-0", "+0", "1h", "1.5h", "-1s", "+2m", "01m", "5.s", ".5s", "100us", "100µs", "100μs", "1d", "42",
		"2562047h", "2562048h", "-2562048h", "9223372036854775807ns", "9223372036854775808ns", "1h-1m", "1h30m15s",
		" 1s", "1s ", "1S", "1H", "abc", "1e3s", "1_0s", "0.000000001s", "0.0000000001s"},
	"bytes": {"0", "1", "1B", "1b", "1KB", "1kb", "1KiB", "1 KiB", "1,000", ",5", "1.5KB", "1.5B", "16EiB", "15EiB", "19EB",
		"18446744073709551615", "18446744073709551616", "1PB", "1ZB", "5 MB ", "5MBs", "-1KB", "1e3", "1.2.3", "KB", "",
		"1\u00a0KB", "1\u212aB", "1kİB", "٣KB"},
}

// unwrapEncode writes value as the line of an encoding: logfmt (quoted when it is not a bare value) or json.
func unwrapEncode(encoding, value string) string {
	if encoding == "json" {
		b, _ := json.Marshal(map[string]string{"v": value})
		return string(b)
	}
	if value != "" && !strings.ContainsAny(value, " \"=\\") && !strings.ContainsFunc(value, func(r rune) bool { return r <= ' ' }) {
		return "v=" + value + " w=1"
	}
	b, _ := json.Marshal(value)
	return "v=" + string(b) + " w=1"
}

// unwrapConversionSets is one data set per encoding, conversion and value: a stream holding one line, and an
// instant sum_over_time over it.
func unwrapConversionSets() (sets []oraclerun.EngineDataSet, cases [][3]string) {
	convs := []string{"", "duration", "bytes"}
	for _, enc := range []string{"logfmt", "json"} {
		for _, conv := range convs {
			for i, value := range unwrapEdgeValues[conv] {
				unwrap := "v"
				if conv != "" {
					unwrap = conv + "(v)"
				}
				query := `sum_over_time({app="x"} | ` + enc + ` | unwrap ` + unwrap + ` [1m])`
				id := fmt.Sprintf("%s/%s/%d", enc, conv, i)
				form := url.Values{"query": {query}, "time": {strconv.FormatInt(unwrapRecordedT+int64(time.Second), 10)}}
				sets = append(sets, oraclerun.EngineDataSet{
					Streams:  []oraclerun.EngineStream{{Labels: map[string]string{"app": "x"}, Entries: []oraclerun.EngineEntry{{TS: unwrapRecordedT, Line: unwrapEncode(enc, value)}}}},
					Requests: []oraclerun.EngineRequest{{ID: id, Instant: true, Form: form.Encode()}},
				})
				cases = append(cases, [3]string{id, conv, value})
			}
		}
	}
	return sets, cases
}

// lokiErrorDetails reads the value of __error_details__ out of Loki's pipeline error text.
func lokiErrorDetails(text string) (string, bool) {
	const key = `__error_details__="`
	at := strings.Index(text, key)
	if at < 0 {
		return "", false
	}
	rest := text[at+len(key)-1:]
	quoted, err := strconv.QuotedPrefix(rest)
	if err != nil {
		return "", false
	}
	v, err := strconv.Unquote(quoted)
	return v, err == nil
}

// lokiInstantValue reads the one sample of an instant answer (ok false: no sample).
func lokiInstantValue(t *testing.T, body json.RawMessage) (float64, bool) {
	t.Helper()
	var resp struct {
		Data struct {
			Result []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	if len(resp.Data.Result) == 0 {
		return 0, false
	}
	s, _ := resp.Data.Result[0].Value[1].(string)
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		t.Fatalf("sample %q: %v", s, err)
	}
	return v, true
}

// sameUnwrapValue compares two converted values: NaN and the infinities exactly, finite ones to the last bits.
func sameUnwrapValue(a, b float64) bool {
	if math.IsNaN(a) || math.IsNaN(b) {
		return math.IsNaN(a) && math.IsNaN(b)
	}
	if math.IsInf(a, 0) || math.IsInf(b, 0) {
		return a == b
	}
	return math.Abs(a-b) <= 1e-12*math.Max(1, math.Abs(b))
}

// TestUnwrapConversionMatchesRecordedLoki: for every edge value, encoding and conversion, the conversion accepts a
// value exactly when Loki makes its sample (with Loki's value), rejects it with the error text Loki puts in
// __error_details__ when Loki fails the query with SampleExtractionErr, and a blank value makes no sample.
//
// conformance: semantics/unwrap-conversion-error, semantics/unwrap-sample-validity, semantics/unwrap-duration-seconds
func TestUnwrapConversionMatchesRecordedLoki(t *testing.T) {
	sets, cases := unwrapConversionSets()
	answers := lokiEngineAnswers(t, "unwrap_conversion", sets)
	for _, c := range cases {
		id, conv, value := c[0], c[1], c[2]
		loki, ok := answers[id]
		if !ok {
			t.Fatalf("%s: no recorded answer", id)
		}
		got, err := convertUnwrap(value, conv)
		switch loki.Status {
		case 400:
			details, found := lokiErrorDetails(loki.Error)
			if !found || !strings.Contains(loki.Error, "SampleExtractionErr") {
				t.Fatalf("%s %q: Loki's 400 is not a conversion error: %s", id, value, loki.Error)
			}
			if err == nil || err.Error() != details {
				t.Errorf("%s %q: conversion error %v, Loki's __error_details__ %q", id, value, err, details)
			}
		case 200:
			want, sample := lokiInstantValue(t, loki.Body)
			if !sample {
				if value != "" {
					t.Errorf("%s %q: Loki makes no sample; the proxy reads one (%v, %v)", id, value, got, err)
				}
				continue
			}
			if err != nil || !sameUnwrapValue(got, want) {
				t.Errorf("%s %q: converts to (%v, %v), Loki's sample %v", id, value, got, err, want)
			}
		default:
			t.Fatalf("%s: Loki answered %d %s", id, loki.Status, loki.Error)
		}
	}
}

// unwrapParserLines generates lines for a parser: keys and values from small alphabets (duplicates, empty and bare
// values, quotes, escapes, dots, invalid UTF-8, U+FFFD, a key named like a stream label or metadata).
func unwrapParserLines(parser string, r *rand.Rand, n int) []string {
	keys := []string{"v", "w", "app", "meta", "a.b", "a-b", "1k", "_x", "é", "v"}
	values := []string{"5", "", "abc", "1.5s", `a"b`, "x y", "\ufffd", "5\xff", "é", "a=b", `\u00e9`, "-1", "[1,2]", "null", "true"}
	var out []string
	for len(out) < n {
		var b strings.Builder
		switch parser {
		case "json", "unpack":
			b.WriteByte('{')
			if parser == "unpack" && r.Intn(2) == 0 {
				b.WriteString(`"_entry":"line",`)
			}
			for i, m := 0, r.Intn(4)+1; i < m; i++ {
				if i > 0 {
					b.WriteByte(',')
				}
				k, _ := json.Marshal(keys[r.Intn(len(keys))])
				b.Write(k)
				b.WriteByte(':')
				switch v := values[r.Intn(len(values))]; {
				case r.Intn(5) == 0:
					b.WriteString(`{"x":` + strconv.Itoa(r.Intn(9)) + `}`)
				case v == "[1,2]" || v == "null" || v == "true" || v == "-1":
					b.WriteString(v)
				default:
					q, _ := json.Marshal(v)
					b.Write(q)
				}
			}
			b.WriteByte('}')
			if r.Intn(10) == 0 {
				b.WriteString("x") // broken JSON
			}
		default:
			for i, m := 0, r.Intn(5)+1; i < m; i++ {
				if i > 0 {
					b.WriteString([]string{" ", "  ", "\t"}[r.Intn(3)])
				}
				k := keys[r.Intn(len(keys))]
				v := values[r.Intn(len(values))]
				switch r.Intn(6) {
				case 0:
					b.WriteString(k)
				case 1:
					q, _ := json.Marshal(v)
					b.WriteString(k + "=" + string(q))
				case 2:
					b.WriteString(k + `="` + v)
				default:
					b.WriteString(k + "=" + v)
				}
			}
		}
		out = append(out, b.String())
	}
	return out
}

// TestUnwrapParsersMatchRecordedLoki: the labels the conversion-error check derives from a stored line (stream
// labels, structured metadata, the parser's labels) are the labels Loki's pipeline gives the line, over generated
// lines of each parser the check reproduces, with and without structured metadata that collides with a parsed key.
//
// conformance: semantics/unwrap-conversion-error, parser-error-and-label-collision
func TestUnwrapParsersMatchRecordedLoki(t *testing.T) {
	r := rand.New(rand.NewSource(20261116))
	stream := map[string]string{"app": "x"}
	var cases []oraclerun.ProcessCase
	var parsers []string
	for _, parser := range []string{"logfmt", "json", "unpack", `regexp "v=(?P<v>[^ ]*)"`} {
		kind := strings.Fields(parser)[0]
		for i, line := range unwrapParserLines(kind, r, 150) {
			c := oraclerun.ProcessCase{ID: fmt.Sprintf("%s/%d", kind, i), Query: `{app="x"} | ` + parser, Labels: stream, Line: line}
			if i%3 == 0 {
				c.Metadata = map[string]string{"meta": "m", "v": "7"}
			}
			cases = append(cases, c)
			parsers = append(parsers, parser)
		}
	}
	answers := lokiProcessAnswers(t, "unwrap_parsers", cases)
	for i, c := range cases {
		loki := answers[i]
		if loki.Error != "" || !loki.Keep {
			t.Fatalf("%s: Loki %+v", c.ID, loki)
		}
		expr, err := logqlpkg.Parse(`sum_over_time({app="x"} | ` + parsers[i] + ` | unwrap v [1m])`)
		if err != nil {
			t.Fatal(err)
		}
		probes := unwrapErrorProbes(expr)
		if len(probes) != 1 {
			t.Fatalf("%s: %d probes", c.ID, len(probes))
		}
		base := cloneStringMap(stream)
		for k, v := range c.Metadata {
			base[k] = v
		}
		got, _, ok := unwrapLokiLabels(probes[0], base, c.Line, c.Line)
		if !ok {
			t.Fatalf("%s: the check does not reproduce %s", c.ID, parsers[i])
		}
		delete(got, "__preserve_error__")
		// The oracle's answer is JSON, which writes an invalid UTF-8 byte as U+FFFD: the proxy's labels are compared
		// as JSON writes them.
		raw, _ := json.Marshal(got)
		got = map[string]string{}
		_ = json.Unmarshal(raw, &got)
		want := cloneStringMap(loki.Labels)
		// The JSON parser's error text is the parser library's; only the error is compared.
		for _, m := range []map[string]string{got, want} {
			if m[errorLabel] != "" {
				m[errorDetailsLabel] = "-"
			}
		}
		if g, w := unwrapLabelsText(got), unwrapLabelsText(want); g != w {
			t.Errorf("%s %q (metadata %v):\n proxy %s\n Loki  %s", c.ID, c.Line, c.Metadata, g, w)
		}
	}
}

func unwrapLabelsText(m map[string]string) string {
	var parts []string
	for k, v := range m {
		parts = append(parts, k+"="+strconv.Quote(v))
	}
	sort.Strings(parts)
	return strings.Join(parts, " ")
}

// unwrapErrorShapes are the metric queries over one failing line: groupings (the parser hints), wrappers, post
// filters, and conversions. %s is the parser.
var unwrapErrorShapes = []string{
	`sum_over_time({app="x"} | %s | unwrap v [1m])`,
	`sum by (app) (sum_over_time({app="x"} | %s | unwrap v [1m]))`,
	`sum(sum_over_time({app="x"} | %s | unwrap v [1m]))`,
	`sum without (n) (sum_over_time({app="x"} | %s | unwrap v [1m]))`,
	`sum by (n) (rate({app="x"} | %s | unwrap v [1m]))`,
	`sum by (app) (max_over_time({app="x"} | %s | unwrap v [1m]))`,
	`max_over_time({app="x"} | %s | unwrap v [1m]) by (n)`,
	`avg_over_time({app="x"} | %s | unwrap v [1m]) by (app, n)`,
	`max by (n) (max_over_time({app="x"} | %s | unwrap v [1m]))`,
	`sum by (__error__) (sum_over_time({app="x"} | %s | unwrap v [1m]))`,
	`absent_over_time({app="x"} | %s | unwrap v [1m])`,
	`sum(absent_over_time({app="x"} | %s | unwrap v [1m]))`,
	`sum(last_over_time({app="x"} | %s | unwrap v [1m]))`,
	`quantile_over_time(0.5, {app="x"} | %s | unwrap v [1m])`,
	`sum_over_time({app="x"} | %s | unwrap v [1m]) * 2`,
	`topk(1, sum_over_time({app="x"} | %s | unwrap v [1m]))`,
	`sum_over_time({app="x"} | %s | unwrap v | __error__="" [1m])`,
	`sum_over_time({app="x"} | %s | unwrap v | __error__!="SampleExtractionErr" [1m])`,
	`sum_over_time({app="x"} | %s | unwrap v | __error__=~".*" [1m])`,
	`sum_over_time({app="x"} | %s | unwrap v | __error_details__="" [1m])`,
	`sum_over_time({app="x"} | %s | unwrap v | __error_details__!="" [1m])`,
	`sum_over_time({app="x"} | %s | unwrap v | n="1" [1m])`,
	`sum_over_time({app="x"} | %s | unwrap v | n!="1" [1m])`,
	`sum_over_time({app="x"} | %s | __error__="" | unwrap v [1m])`,
	`sum_over_time({app="x"} | %s | drop __error__ | unwrap v [1m])`,
	`sum_over_time({app="x"} | %s | drop n | unwrap v [1m])`,
	`sum_over_time({app="x"} | %s | keep v, app | unwrap v [1m])`,
	`sum_over_time({app="x"} | %s | unwrap duration(v) [1m])`,
	`sum_over_time({app="x"} | %s | unwrap bytes(v) [1m])`,
	`sum_over_time({app="x"} | %s | unwrap app [1m])`,
}

// TestUnwrapConversionErrorMatchesRecordedLoki: over one stream with one line whose unwrapped value Loki rejects,
// every shape the check evaluates answers Loki's status and, for Loki's 400, Loki's text (the error, its details and
// the series the error names: the labels the parser hints keep); a shape the check does not evaluate is one Loki
// answers 200, or a registered form (semantics/unwrap-conversion-error-postfilter-forms).
//
// conformance: semantics/unwrap-conversion-error, semantics/unwrap-conversion-error-postfilter-forms, parser-error-and-label-collision
func TestUnwrapConversionErrorMatchesRecordedLoki(t *testing.T) {
	lines := map[string]string{"logfmt": `n=1 v=x7 w=abc`, "json": `{"n":"1","v":"x7","w":"abc","o":{"k":"2"}}`}
	stream := map[string]string{"app": "x", "service_name": "x"}
	metadata := map[string]string{"detected_level": "info"}
	var sets []oraclerun.EngineDataSet
	type shape struct{ id, query, parser string }
	var shapes []shape
	for _, parser := range []string{"logfmt", "json"} {
		for i, s := range unwrapErrorShapes {
			q := fmt.Sprintf(s, parser)
			id := fmt.Sprintf("%s/%d", parser, i)
			form := url.Values{"query": {q}, "time": {strconv.FormatInt(unwrapRecordedT+int64(time.Second), 10)}}
			sets = append(sets, oraclerun.EngineDataSet{
				Streams:  []oraclerun.EngineStream{{Labels: stream, Entries: []oraclerun.EngineEntry{{TS: unwrapRecordedT, Line: lines[parser], Metadata: metadata}}}},
				Requests: []oraclerun.EngineRequest{{ID: id, Instant: true, Form: form.Encode()}},
			})
			shapes = append(shapes, shape{id, q, parser})
		}
	}
	answers := lokiEngineAnswers(t, "unwrap_conversion_error", sets)
	base := cloneStringMap(stream)
	for k, v := range metadata {
		base[k] = v
	}
	for _, s := range shapes {
		loki := answers[s.id]
		expr, err := logqlpkg.Parse(s.query)
		if err != nil {
			t.Fatal(err)
		}
		probes := unwrapErrorProbes(expr)
		if len(probes) == 0 {
			if loki.Status == 400 && !unwrapRegisteredUnevaluated(s.query) {
				t.Errorf("%s: Loki fails it, the check does not evaluate it: %s", s.query, loki.Error)
			}
			continue
		}
		perr := unwrapLokiError(probes[0], base, lines[s.parser], lines[s.parser])
		switch {
		case loki.Status == 400 && perr == nil:
			t.Errorf("%s: Loki 400 %q, the check finds no error", s.query, loki.Error)
		case loki.Status != 400 && perr != nil:
			t.Errorf("%s: Loki %d, the check answers %q", s.query, loki.Status, perr.Error())
		case loki.Status == 400 && perr.Error() != loki.Error:
			t.Errorf("%s:\n proxy %q\n Loki  %q", s.query, perr.Error(), loki.Error)
		}
	}
}

// unwrapRegisteredUnevaluated reports the post-filter forms the check leaves to the metric's answer
// (semantics/unwrap-conversion-error-postfilter-forms).
func unwrapRegisteredUnevaluated(query string) bool {
	return strings.Contains(query, `__error_details__!=""`) || strings.Contains(query, `__error__!=""`) ||
		strings.Contains(query, `__error__="SampleExtractionErr"`)
}
