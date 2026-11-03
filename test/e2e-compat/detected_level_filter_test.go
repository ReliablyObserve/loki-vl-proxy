//go:build e2e

package e2e_compat

import (
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"testing"
	"time"
)

// `| detected_level <op> "<value>"` before any parser, against live Loki.
// Loki assigns the label at ingest (v3.7.9 pkg/distributor/field_detection.go
// extractLogLevel): the stream labels first, then structured metadata, then
// the line's JSON or logfmt level key, then its first bounded keyword, else
// unknown. The proxy derives it on the read path. The fixture holds the
// stack's disagreements: a stream label that a logfmt or keyword line
// contradicts (worker-service, cache-redis, db-postgres), lines with no level
// anywhere (unknown), and lines whose level is only in the text. Each filter
// must select the lines whose label in Loki's own unfiltered answer the
// matcher accepts, and the metrics over the filter must count them as Loki
// does, grouped by detected_level and by another label.

type dlFilterLine struct {
	name   string
	labels map[string]string // extra stream labels
	sm     map[string]string // structured metadata, fields in VictoriaLogs
	line   string
}

var dlFilterLines = []dlFilterLine{
	// The stream label decides over the line.
	{name: "label-info-logfmt-error", labels: map[string]string{"level": "info"}, line: `level=error msg="job failed" queue=email`},
	{name: "label-info-logfmt-warn", labels: map[string]string{"level": "info"}, line: `level=warn msg="job retry" attempt=2`},
	{name: "label-debug-logfmt-info", labels: map[string]string{"level": "debug"}, line: `level=info msg="cache miss" key=user:1`},
	{name: "label-info-keyword-error", labels: map[string]string{"level": "info"}, line: "2026-11-03 12:00:00 UTC [12345] app_prod ERROR: deadlock detected"},
	{name: "label-debug-json-info", labels: map[string]string{"level": "debug"}, line: `{"level":"info","msg":"x"}`},
	{name: "label-unknown-word", labels: map[string]string{"level": "notice"}, line: "error"},
	// No level anywhere: unknown.
	{name: "plain-nolevel-1", line: "ordinary visual sample 1"},
	{name: "plain-nolevel-2", line: "ordinary visual sample 2"},
	{name: "json-nolevel", line: `{"msg":"hello"}`},
	{name: "logfmt-nolevel", line: "v=3 msg=a"},
	{name: "plain-inside-word", line: "the terror"},
	{name: "plain-after-colon", line: "misc:error happened"},
	// The level is only in the text.
	{name: "plain-error", line: "2026-11-03 12:00:00 UTC [1] app_prod ERROR: canceling statement"},
	{name: "plain-earliest", line: "info then error"},
	{name: "plain-warning", line: "WARNING disk almost full"},
	{name: "json-level", line: `{"level":"ERR","msg":"x"}`},
	{name: "json-warning", line: `{"level":"Warning","msg":"x"}`},
	{name: "logfmt-level", line: "ts=1 level=Information msg=hi"},
	{name: "logfmt-warn", line: "level=warn msg=x"},
	// Structured metadata.
	{name: "sm-level", sm: map[string]string{"level": "WARN"}, line: "info"},
}

var dlFilterE2EMatchers = []struct{ op, value string }{
	{"!=", "info"}, {"=~", "warn|error"}, {"=", "unknown"}, {"=~", "unknown|error"}, {"=", "error"},
	{"=~", "info|error"}, {"!~", "info|debug"}, {"=", "debug"}, {"=", "notice"}, {"!=", ""},
}

// dlFilterMatches is Loki's label filter for the matchers above, whose
// regexps are alternations of words without a common prefix (Loki's
// simplifier keeps them as equality checks).
func dlFilterMatches(op, value, level string) bool {
	switch op {
	case "=":
		return level == value
	case "!=":
		return level != value
	}
	return regexp.MustCompile("^(?:"+value+")$").MatchString(level) == (op == "=~")
}

// ensureDetectedLevelFilterFixture pushes dlFilterLines to both backends under
// a unique app label (VictoriaLogs keeps each line byte for byte, its stream
// labels and structured metadata as fields) and waits until both hold all of
// them.
func ensureDetectedLevelFilterFixture(t *testing.T) dlFixture {
	t.Helper()
	now := time.Now().UTC()
	f := dlFixture{app: fmt.Sprintf("dlfilter%d", now.UnixNano()), start: now.Add(-2 * time.Minute), byTS: map[string]dlCase{}}
	var streams []map[string]interface{}
	for i, l := range dlFilterLines {
		at := f.start.Add(time.Duration(i) * time.Millisecond)
		f.byTS[strconv.FormatInt(at.UnixNano(), 10)] = dlCase{name: l.name}
		labels := map[string]string{"app": f.app, "case": l.name}
		for k, v := range l.labels {
			labels[k] = v
		}
		value := []interface{}{strconv.FormatInt(at.UnixNano(), 10), l.line}
		if len(l.sm) > 0 {
			value = append(value, l.sm)
		}
		streams = append(streams, map[string]interface{}{"stream": labels, "values": []interface{}{value}})
	}
	f.end = f.start.Add(time.Duration(len(dlFilterLines))*time.Millisecond + time.Second)
	body := dlJSON(map[string]interface{}{"streams": streams})
	dlPost(t, lokiURL+"/loki/api/v1/push", "application/json", body)
	dlPost(t, vlURL+"/insert/loki/api/v1/push?disable_message_parsing=1", "application/json", body)
	dlWaitBothBackends(t, f, f.appSelector(), f.appSelector(), len(dlFilterLines))
	return f
}

// conformance: severity/detected-level-filter-json-level, severity-detected-level-derivation, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_DetectedLevelFilterJSONLevel(t *testing.T) {
	f := ensureDetectedLevelFilterFixture(t)
	lokiAll := dlEntries(t, dlQueryRange(t, lokiURL, f.appSelector(), f, nil))
	if len(lokiAll) != len(dlFilterLines) {
		t.Fatalf("Loki returned %d of %d fixture lines; refusing to compare", len(lokiAll), len(dlFilterLines))
	}
	lokiLevel := map[string]string{}
	for stamp, e := range lokiAll {
		if lokiLevel[stamp] = dlLevel(e, false); lokiLevel[stamp] == "" {
			t.Fatalf("%s: Loki returned no detected_level: %+v", f.byTS[stamp].name, e)
		}
	}
	for _, m := range dlFilterE2EMatchers {
		query := fmt.Sprintf("%s | detected_level%s%q", f.appSelector(), m.op, m.value)
		t.Run(query, func(t *testing.T) {
			loki := dlEntries(t, dlQueryRange(t, lokiURL, query, f, nil))
			proxy := dlEntries(t, dlQueryRange(t, proxyURL, query, f, nil))
			if len(loki) == 0 && m.value != "notice" {
				t.Fatalf("Loki selected no line; refusing an empty comparison")
			}
			for stamp, level := range lokiLevel {
				want := dlFilterMatches(m.op, m.value, level)
				if _, ok := loki[stamp]; ok != want {
					t.Fatalf("%s: Loki's filter disagrees with its own label %q (selected=%v)", f.byTS[stamp].name, level, ok)
				}
				e, ok := proxy[stamp]
				if ok != want {
					t.Errorf("%s: proxy selected=%v, Loki %v (level %q)", f.byTS[stamp].name, ok, want, level)
				}
				if ok && dlLevel(e, false) != level {
					t.Errorf("%s: proxy detected_level %q, Loki %q", f.byTS[stamp].name, dlLevel(e, false), level)
				}
			}
			if len(proxy) != len(loki) {
				t.Errorf("proxy returned %d lines, Loki %d", len(proxy), len(loki))
			}
		})
	}
	waitForLokiMetricDataSelector(t, f.appSelector())
	for _, grouping := range []string{"detected_level", "case"} {
		t.Run("metric by "+grouping, func(t *testing.T) {
			for _, m := range dlFilterE2EMatchers {
				query := fmt.Sprintf("sum by (%s) (count_over_time(%s | detected_level%s%q [10m]))", grouping, f.appSelector(), m.op, m.value)
				loki := dlFilterInstant(t, lokiURL, query, f.end, grouping)
				proxy := dlFilterInstant(t, proxyURL, query, f.end, grouping)
				if fmt.Sprint(loki) != fmt.Sprint(proxy) {
					t.Errorf("%s\n loki  %v\n proxy %v", query, loki, proxy)
				}
			}
		})
	}
}

// dlFilterInstant returns an instant vector as label value -> count, sorted.
func dlFilterInstant(t *testing.T, base, query string, at time.Time, label string) []string {
	t.Helper()
	// Whole seconds after every fixture line.
	params := url.Values{"query": {query}, "time": {strconv.FormatInt(at.Unix()+1, 10)}}
	status, body, resp := doJSONGET(t, base+"/loki/api/v1/query?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if status != http.StatusOK || resp["status"] != "success" || extractMap(resp, "data")["resultType"] != "vector" {
		t.Fatalf("%s %s: %d %s", base, query, status, body)
	}
	var out []string
	for _, item := range extractArray(extractMap(resp, "data"), "result") {
		sample, _ := item.(map[string]any)
		metric, _ := sample["metric"].(map[string]any)
		value, _ := sample["value"].([]any)
		if len(value) != 2 {
			t.Fatalf("%s %s: unexpected sample in %s", base, query, body)
		}
		out = append(out, fmt.Sprintf("%v=%v", metric[label], value[1]))
	}
	sort.Strings(out)
	return out
}
