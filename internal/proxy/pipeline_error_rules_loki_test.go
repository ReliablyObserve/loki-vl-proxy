package proxy

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// pipelineErrorRulesAnswers holds Loki's answers (test/oracle/loki engine) to the requests of pipelineErrorRules
// over pipelineErrorRulesStreams; LVP_RUN_ORACLES=1 records them again.
var pipelineErrorRulesAnswers = filepath.Join("testdata", "pipeline_error_rules_loki_answers.jsonl.gz")

// pipelineErrorRulesBase is the first entry's time, in seconds; every stream holds a line every 10 s for 15 minutes,
// and every request reads the last 10 of them.
const pipelineErrorRulesBase = int64(1794500000)

// pipelineErrorRulesStreams is the data set: a JSON stream with lines that are not one JSON object, a logfmt
// stream whose `n` is not a number, a duration or a size on some lines, a logfmt stream with unterminated quoted
// values, and a plain stream.
func pipelineErrorRulesStreams() []oraclerun.EngineStream {
	lines := map[string][]string{
		"j": {`{"level":"info","n":5,"s":"x"}`, `{"level":"warn","n":7,"s":"y"`, `GET /x 200`, `{"level":"info","n":2,"s":"z"}`},
		"l": {`level=info n=5 msg=ok`, `level=info n=abc msg=ok`, `level=warn n=10s msg=ok`, `level=info n=2KB msg=ok`},
		"q": {`level=info n=5 msg="ok`, `level=info n=6 msg=ok`},
		"p": {`GET /x 200`, `POST /y 500`},
	}
	var out []oraclerun.EngineStream
	for _, app := range []string{"j", "l", "q", "p"} {
		var entries []oraclerun.EngineEntry
		for i := int64(0); i < 90; i++ {
			ls := lines[app]
			entries = append(entries, oraclerun.EngineEntry{TS: (pipelineErrorRulesBase + i*10) * 1e9, Line: ls[i%int64(len(ls))]})
		}
		out = append(out, oraclerun.EngineStream{Labels: map[string]string{"app": app, "env": "prod"}, Entries: entries})
	}
	return out
}

// pipelineErrorRule is one query of the grid and the rule it records.
type pipelineErrorRule struct {
	rule, query string
	// maxSeries is the tenant's series limit for the query; 0 for none.
	maxSeries int
	// fails is what Loki answers on query_range and query alike: the pipeline error type of its 400, the status of
	// another error, or "" for a 200.
	fails string
}

// pipelineErrorRules are the queries whose Loki answers decide which metric queries fail on a pipeline error: the
// error producers not recorded elsewhere (a numeric, duration or size label filter on a value that is not one, a
// template that fails to run, an extraction list, unpack, plain logfmt), the aggregations directly over the range
// aggregation that decide whether the parser runs at all, grouping by __error__, and a pipeline error meeting a
// vector matching failure or the series limit in one query.
var pipelineErrorRules = []pipelineErrorRule{
	{"label-filter-number", `count_over_time({app="l"} | logfmt | n > 3 [5m])`, 0, "LabelFilterErr"},
	{"label-filter-number-ungrouped-sum", `sum(count_over_time({app="l"} | logfmt | n > 3 [5m]))`, 0, "LabelFilterErr"},
	{"label-filter-number-grouped-sum", `sum by (app) (count_over_time({app="l"} | logfmt | n > 3 [5m]))`, 0, "LabelFilterErr"},
	{"label-filter-duration", `count_over_time({app="l"} | logfmt | n > 3s [5m])`, 0, "LabelFilterErr"},
	{"label-filter-bytes", `count_over_time({app="l"} | logfmt | n > 1KB [5m])`, 0, "LabelFilterErr"},
	{"label-filter-number-equal", `count_over_time({app="l"} | logfmt | n == 5 [5m])`, 0, "LabelFilterErr"},
	{"label-filter-or", `count_over_time({app="l"} | logfmt | n > 3 or msg="ok" [5m])`, 0, "LabelFilterErr"},
	{"label-filter-intercepted", `count_over_time({app="l"} | logfmt | n > 3 | __error__="" [5m])`, 0, ""},
	{"label-filter-dropped", `count_over_time({app="l"} | logfmt | n > 3 | drop __error__, __error_details__ [5m])`, 0, ""},
	{"label-filter-under-unwrap", `sum_over_time({app="l"} | logfmt | n > 3 | unwrap n [5m])`, 0, "SampleExtractionErr"},
	{"template-unread", `count_over_time({app="l"} | logfmt | line_format "{{ .n.x }}" [5m])`, 0, ""},
	{"template-read-by-line-filter", `count_over_time({app="l"} | logfmt | line_format "{{ .n.x }}" |= "a" [5m])`, 0, "TemplateFormatErr"},
	{"template-label", `count_over_time({app="l"} | logfmt | label_format y="{{ .n.x }}" [5m])`, 0, "TemplateFormatErr"},
	{"template-label-grouped", `sum by (y) (count_over_time({app="l"} | logfmt | label_format y="{{ .n.x }}" [5m]))`, 0, "TemplateFormatErr"},
	{"template-label-ungrouped-sum", `sum(count_over_time({app="l"} | logfmt | label_format y="{{ .n.x }}" [5m]))`, 0, "TemplateFormatErr"},
	{"logfmt-plain-unterminated", `count_over_time({app="q"} | logfmt [5m])`, 0, ""},
	{"logfmt-strict-unterminated", `count_over_time({app="q"} | logfmt --strict [5m])`, 0, "LogfmtParserErr"},
	{"unpack-not-json", `count_over_time({app="j"} | unpack [5m])`, 0, "JSONParserErr"},
	{"json-extraction-list", `count_over_time({app="j"} | json level="level" [5m])`, 0, "JSONParserErr"},
	{"json-extraction-list-two", `count_over_time({app="j"} | json level="level", n="n" [5m])`, 0, "JSONParserErr"},
	{"json-extraction-list-grouped", `sum by (level) (count_over_time({app="j"} | json level="level" [5m]))`, 0, "JSONParserErr"},
	{"json-bare", `count_over_time({app="j"} | json [5m])`, 0, "JSONParserErr"},
	{"json-sum", `sum(count_over_time({app="j"} | json [5m]))`, 0, ""},
	{"json-sum-without", `sum without (app) (count_over_time({app="j"} | json [5m]))`, 0, "JSONParserErr"},
	{"json-sum-without-empty", `sum without () (count_over_time({app="j"} | json [5m]))`, 0, "JSONParserErr"},
	{"json-sum-by-empty", `sum by () (count_over_time({app="j"} | json [5m]))`, 0, ""},
	{"json-count", `count(count_over_time({app="j"} | json [5m]))`, 0, "JSONParserErr"},
	{"json-max", `max(count_over_time({app="j"} | json [5m]))`, 0, "JSONParserErr"},
	{"json-min", `min(count_over_time({app="j"} | json [5m]))`, 0, "JSONParserErr"},
	{"json-avg", `avg(count_over_time({app="j"} | json [5m]))`, 0, "JSONParserErr"},
	{"json-stddev", `stddev(count_over_time({app="j"} | json [5m]))`, 0, "JSONParserErr"},
	{"json-topk", `topk(1, count_over_time({app="j"} | json [5m]))`, 0, "JSONParserErr"},
	{"json-sort", `sort(count_over_time({app="j"} | json [5m]))`, 0, "JSONParserErr"},
	{"json-sum-label-filter", `sum(count_over_time({app="j"} | json | level="info" [5m]))`, 0, ""},
	{"json-sum-label-filter-keeps-errors", `sum(count_over_time({app="j"} | json | level!="none" [5m]))`, 0, "JSONParserErr"},
	{"json-sum-by-error", `sum by (__error__) (count_over_time({app="j"} | json [5m]))`, 0, ""},
	{"label-filter-sum-by-error", `sum by (__error__) (count_over_time({app="l"} | logfmt | n > 3 [5m]))`, 0, "LabelFilterErr"},
	{"template-label-sum-by-error", `sum by (__error__) (count_over_time({app="l"} | logfmt | label_format y="{{ .n.x }}" [5m]))`, 0, "TemplateFormatErr"},
	{"json-sum-by-level", `sum by (level) (count_over_time({app="j"} | json [5m]))`, 0, "JSONParserErr"},
	{"json-sum-by-stream-label", `sum by (env) (count_over_time({app="j"} | json [5m]))`, 0, "JSONParserErr"},
	{"json-rate-sum", `sum(rate({app="j"} | json [5m]))`, 0, ""},
	{"json-bytes-sum", `sum(bytes_over_time({app="j"} | json [5m]))`, 0, ""},
	{"json-matching-many-to-many-right", `sum by (app) (count_over_time({app="j"} | json [5m])) + on () count_over_time({app=~"p|l"}[5m])`, 0, "JSONParserErr"},
	{"json-matching-many-to-one-left", `count_over_time({app=~"p|l"}[5m]) + on () sum by (app) (count_over_time({app="j"} | json [5m]))`, 0, "JSONParserErr"},
	{"matching-control", `count_over_time({app=~"p|l"}[5m]) + on () count_over_time({app="p"}[5m])`, 0, "500"},
	{"json-series-limit", `count_over_time({app=~"j|l|p|q"} | json [5m])`, 2, "JSONParserErr"},
	{"series-limit-control", `count_over_time({app=~"j|l|p|q"}[5m])`, 2, "400"},
	{"json-series-limit-sum", `sum by (app) (count_over_time({app=~"j|l|p|q"} | json [5m]))`, 2, "JSONParserErr"},
}

// pipelineErrorRuleAnswer is Loki's answer to one rule's query_range and query requests.
type pipelineErrorRuleAnswer struct {
	Rule         string `json:"rule"`
	Query        string `json:"q"`
	RangeStatus  int    `json:"range_status"`
	RangeError   string `json:"range_error,omitempty"`
	InstantStat  int    `json:"instant_status"`
	InstantError string `json:"instant_error,omitempty"`
}

func recordPipelineErrorRules(t *testing.T) {
	t.Helper()
	streams := pipelineErrorRulesStreams()
	end := pipelineErrorRulesBase + 890
	sets := map[int]*oraclerun.EngineDataSet{}
	var order []int
	for i, r := range pipelineErrorRules {
		set := sets[r.maxSeries]
		if set == nil {
			set = &oraclerun.EngineDataSet{Streams: streams, MaxSeries: r.maxSeries}
			sets[r.maxSeries] = set
			order = append(order, r.maxSeries)
		}
		id := strconv.Itoa(i)
		set.Requests = append(set.Requests,
			oraclerun.EngineRequest{ID: "r" + id, Form: url.Values{"query": {r.query}, "start": {strconv.FormatInt(end-300, 10)}, "end": {strconv.FormatInt(end, 10)}, "step": {"60"}}.Encode()},
			oraclerun.EngineRequest{ID: "i" + id, Instant: true, Form: url.Values{"query": {r.query}, "time": {strconv.FormatInt(end, 10)}}.Encode()})
	}
	var all []oraclerun.EngineDataSet
	for _, k := range order {
		all = append(all, *sets[k])
	}
	answers, err := oraclerun.LokiEngine(filepath.Join("..", "..", "test", "oracle", "loki"), all)
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	for i, r := range pipelineErrorRules {
		id := strconv.Itoa(i)
		ra, ia := answers["r"+id], answers["i"+id]
		if err := enc.Encode(pipelineErrorRuleAnswer{Rule: r.rule, Query: r.query, RangeStatus: ra.Status, RangeError: ra.Error, InstantStat: ia.Status, InstantError: ia.Error}); err != nil {
			t.Fatal(err)
		}
	}
	var zipped bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&zipped, gzip.BestCompression)
	if _, err := zw.Write(b.Bytes()); err != nil || zw.Close() != nil {
		t.Fatal("compress the answers")
	}
	if err := os.WriteFile(pipelineErrorRulesAnswers, zipped.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readPipelineErrorRules(t *testing.T) []pipelineErrorRuleAnswer {
	t.Helper()
	f, err := os.Open(pipelineErrorRulesAnswers)
	if err != nil {
		t.Fatalf("%v: record the answers with LVP_RUN_ORACLES=1", err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	var out []pipelineErrorRuleAnswer
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var a pipelineErrorRuleAnswer
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			t.Fatal(err)
		}
		out = append(out, a)
	}
	if len(out) != len(pipelineErrorRules) {
		t.Fatalf("%d answers for %d rules: record them again with LVP_RUN_ORACLES=1", len(out), len(pipelineErrorRules))
	}
	for i, a := range out {
		if a.Query != pipelineErrorRules[i].query || a.Rule != pipelineErrorRules[i].rule {
			t.Fatalf("answer %d is for %q: record them again with LVP_RUN_ORACLES=1", i, a.Query)
		}
	}
	return out
}

// pipelineErrorType is the error type a Loki pipeline error answer names, "" for any other answer.
func pipelineErrorType(text string) string {
	rest, ok := strings.CutPrefix(text, "pipeline error: '")
	if !ok {
		return ""
	}
	typ, _, _ := strings.Cut(rest, "'")
	return typ
}

// outcome is what an answer is in the terms of pipelineErrorRule.fails.
func outcome(status int, text string) string {
	switch {
	case status == 200:
		return ""
	case status == 400 && pipelineErrorType(text) != "":
		return pipelineErrorType(text)
	}
	return strconv.Itoa(status)
}

// Loki fails a metric query on a pipeline error exactly as these rules say, on query_range and on query: a numeric,
// duration or size label filter on a value that is not one (LabelFilterErr), a template that fails to run on a
// line some later stage reads or in a label_format (TemplateFormatErr; an unread line_format is not built),
// logfmt --strict on an unterminated value (plain logfmt skips it), unpack or json, with or without an extraction
// list, on a line that is not one JSON object (JSONParserErr), and an unwrap of a value that is not a number
// (SampleExtractionErr, also after a label filter that failed on it). The parser runs only when some label is
// needed: a sum with no grouping or `by ()` directly over the range aggregation needs none, so it never fails;
// every other aggregation (`sum without`, `sum by` a stream or parsed label, count, max, min, avg, stddev, topk,
// sort) and a label filter keeping the line do. A filter after the error on `__error__=""` or a drop of the
// error labels removes it, and a sum grouped by __error__ keeps a parser's error (json) as a series, but not a
// label filter's or a template's. A pipeline error answers ahead
// of a vector matching failure (500) and of the series limit (400) in the same query.
//
// conformance: semantics/loki-pipeline-error-rules
func TestPipelineErrorRulesRecorded(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordPipelineErrorRules(t)
	}
	for i, a := range readPipelineErrorRules(t) {
		want := pipelineErrorRules[i].fails
		if got := outcome(a.RangeStatus, a.RangeError); got != want {
			t.Errorf("%s: query_range answers %q (%d %s), the rule says %q", a.Rule, got, a.RangeStatus, firstLine(a.RangeError), want)
		}
		if got := outcome(a.InstantStat, a.InstantError); got != want {
			t.Errorf("%s: query answers %q (%d %s), the rule says %q", a.Rule, got, a.InstantStat, firstLine(a.InstantError), want)
		}
	}
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(s, "\n")
	return line
}
