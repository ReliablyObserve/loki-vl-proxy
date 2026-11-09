package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// oldOrderedJSONLabelFilter is what compileOrderedJSONStage made of a label
// filter before the stage carried its parsed filter: the text read again as a
// stream selector (oldSelectorReading), which keeps string comparisons only.
func oldOrderedJSONLabelFilter(raw string) (translator.DropCondition, bool) {
	parsed, err := oldSelectorReading(raw)
	if err != nil || len(parsed.Selector.Matchers) != 1 {
		return translator.DropCondition{}, false
	}
	m := parsed.Selector.Matchers[0]
	condition, err := translator.NewLabelFilterCondition(m.Name, []string{"=", "!=", "=~", "!~"}[int(m.Op)], m.Value)
	return condition, err == nil
}

// A label filter stage compiles to the same condition as before for every
// shape but ip(), which now compiles to an address condition (a string one
// holding the text `ip(...)` matched nothing) and, on detected_level, no longer
// compiles.
// conformance: semantics/ip-filter-json-stats-pushdown
func TestCompileOrderedJSONLabelFilterShapes(t *testing.T) {
	probes := []string{"", "b", "B", "5", "1m", "a\nb", "é", "b.x", "10.2.3.4", "10.3.0.1", "2001:db8::1", "error"}
	for _, tc := range []struct {
		stage   string
		changed bool // ip(): compiled differently now
	}{
		{`a="b"`, false}, {`a!="b"`, false}, {`a=~"b.*"`, false}, {`a!~"b"`, false}, {"a=`b\\n`", false},
		{`a="b\"c"`, false}, {`a = "b"`, false}, {`a=""`, false}, {`a!=""`, false}, {`a=~""`, false}, {`a!~".*"`, false},
		{`a="é"`, false}, {`a="\x41"`, false}, {`_x="y"`, false}, {`__error__=""`, false}, {`detected_level="error"`, false},
		{`a.b="c"`, false}, {`a=~"("`, false}, {`a="b", c="d"`, false}, {`a="b" and c="d"`, false}, {`a="b" or c="d"`, false},
		{`(a="b")`, false}, {`((a="b"))`, false}, {`a=5`, false}, {`a!=5`, false}, {`a==5`, false}, {`a>5`, false},
		{`a>=1m`, false}, {`a<1KB`, false}, {`a="\q"`, false}, {`a=ip("1.2.3.4")`, true}, {`a!=ip("1.2.3.0/24")`, true},
		{`a = ip ( "1.2.3.4-1.2.3.9" )`, true}, {"a=ip(`2001:db8::/32`)", true}, {`(a=ip("1.2.3.4"))`, false},
		{`a=ip("1.2.3.4"), b="c"`, false}, {`a=ip("1.2.3.4") or b="c"`, false}, {`a=ip("999.1.1.1")`, true},
		{`detected_level=ip("1.2.3.4")`, true},
	} {
		t.Run(tc.stage, func(t *testing.T) {
			lq, err := logqlpkg.ParseLogQuery(`{app="a"} | json | ` + tc.stage)
			if err != nil {
				t.Skipf("not a LogQL query: %v", err)
			}
			stage := lq.Pipeline[len(lq.Pipeline)-1]
			oldCond, oldOK := oldOrderedJSONLabelFilter(stage.(*logqlpkg.LabelFilterStage).Raw)
			compiled, newOK := compileOrderedJSONStage(stage)
			if tc.changed {
				if !strings.Contains(tc.stage, "ip") {
					t.Fatal("bad table row")
				}
				if wantOK := !strings.Contains(tc.stage, "detected_level") && !strings.Contains(tc.stage, "999"); newOK != wantOK {
					t.Fatalf("ip() filter compiles: %v, want %v", newOK, wantOK)
				}
				if newOK && compiled.filter.Matches("") {
					t.Error("an ip() condition keeps an absent label")
				}
				return
			}
			if oldOK != newOK {
				t.Fatalf("compiled before: %v, now: %v", oldOK, newOK)
			}
			if !newOK {
				return
			}
			got := *compiled.filter
			if got.Field != oldCond.Field || got.Op != oldCond.Op || got.Value != oldCond.Value {
				t.Fatalf("condition %+v, before %+v", got, oldCond)
			}
			for _, v := range probes {
				if got.Matches(v) != oldCond.Matches(v) {
					t.Errorf("Matches(%q) = %v, before %v", v, got.Matches(v), oldCond.Matches(v))
				}
			}
		})
	}
}

// ipFilterJSONRows is a JSON stream with an ip key: addresses inside and
// outside 10.2.0.0/16, an IPv6 address, a leading-zero address, lines without
// the key, a truncated line and plain text.
func ipFilterJSONRows(s0 time.Time) []pushdownRow {
	lines := []string{
		`{"ip":"10.2.3.4","method":"GET"}`, `{"ip":"10.3.0.1","method":"POST"}`, `{"ip":"10.2.255.255","method":"GET"}`,
		`{"ip":"2001:db8::1","method":"PUT"}`, `{"ip":"010.2.3.4","method":"GET"}`, `{"method":"GET"}`,
		`{"ip":"10.2.1.77","method":"DELETE"}`, `{"msg": "truncated`, "plain text line",
	}
	rows := make([]pushdownRow, 0, 120)
	for i := 0; i < 120; i++ {
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(i) * 5 * time.Second), stream: map[string]string{"app": "api"}, msg: lines[i%len(lines)]})
	}
	return rows
}

var ipFilterJSONQueries = []string{
	`sum(count_over_time({app="api"} | json | ip=ip("10.2.0.0/16") [1m]))`,
	`sum(count_over_time({app="api"} | json | ip!=ip("10.2.0.0/16") [1m]))`,
	`sum(count_over_time({app="api"} | json | ip=ip("10.2.1.0-10.2.3.255") [1m]))`,
	`sum(count_over_time({app="api"} | json | ip=ip("10.2.3.4") [1m]))`,
	`sum(count_over_time({app="api"} | json | ip=ip("2001:db8::/32") [1m]))`,
	`sum(count_over_time({app="api"} | json | ip!=ip("2001:db8::/32") [1m]))`,
	`sum by (method) (count_over_time({app="api"} | json | ip=ip("10.2.0.0/16") [1m]))`,
	`sum by (method) (count_over_time({app="api"} | json | ip!=ip("10.2.0.0/16") | method="GET" [1m]))`,
	`sum by (method) (rate({app="api"} | json | ip=ip("10.2.0.0/16") | drop __error__ [2m]))`,
	`sum(bytes_over_time({app="api"} | json | ip=ip("10.2.0.0/16") [1m]))`,
}

// The ordered-JSON plan keeps an ip() label filter: it is pushed down as the
// address filter the log query uses, never as a string comparison, and the
// stats buckets equal the row evaluator (the Loki model of the plan) on a
// stream with addresses in and out of the pattern, lines without the key and
// lines Loki's parser rejects.
// conformance: semantics/ip-filter-json-stats-pushdown
func TestOrderedJSONIPFilterPushdownLikeRowEvaluator(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	rows := ipFilterJSONRows(s0)
	start, end := s0.Add(time.Minute), s0.Add(10*time.Minute)
	for _, query := range ipFilterJSONQueries {
		t.Run(query, func(t *testing.T) {
			srv, fake := newPushdownFakeVL(t, rows, nil)
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			plan, ok := compileOrderedJSONMetric(query)
			if !ok || !plan.pushdown {
				t.Fatalf("expected a pushdown plan, got ok=%v", ok)
			}
			want := lokiPushdownReference(t, plan, rows, start, end, time.Minute)
			if len(want) == 0 {
				t.Fatal("fixture yields no series")
			}
			got := runJSONVolumeQueryRange(t, p, query, start, end, time.Minute)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("result differs from the row evaluator\n got: %v\nwant: %v", got, want)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if fake.raw != 0 || len(fake.stats) != 1 {
				t.Fatalf("expected one stats call and no raw read: raw=%d stats=%d %v %q", fake.raw, len(fake.stats), fake.rawQueries, fake.guards)
			}
			stats := fake.stats[0]
			if strings.Contains(stats, `ip:="ip(`) || strings.Contains(stats, `ip:=\"ip(`) {
				t.Errorf("the filter compares the text ip(...): %s", stats)
			}
			if strings.Contains(query, `10.2.0.0/16`) && (!strings.Contains(stats, `ip:~"`) || !strings.Contains(stats, "ip:ipv4_range(10.2.0.0, 10.2.255.255)")) {
				t.Errorf("no address range and regexp filter in %s", stats)
			}
		})
	}
}

// A line Loki's parser rejects after reading the ip key keeps the raw
// evaluator, whose ip() condition answers like the pushed-down filter: with
// the error dropped the line is counted, without the drop it is Loki's
// pipeline error.
// conformance: semantics/ip-filter-json-stats-pushdown
func TestOrderedJSONIPFilterRowFallback(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	rows := ipFilterJSONRows(s0)[:6]
	risky := pushdownRow{ts: s0.Add(35 * time.Second), stream: map[string]string{"app": "api"}, msg: `{"ip":"10.2.9.9","msg": truncated`}
	rows = append(rows, risky)
	at := s0.Add(time.Minute)
	for _, tc := range []struct {
		query         string
		pipelineError bool
	}{
		{`sum by (method) (count_over_time({app="api"} | json | ip=ip("10.2.0.0/16") | drop __error__ [1m]))`, false},
		{`sum by (method) (count_over_time({app="api"} | json | ip!=ip("10.2.0.0/16") | drop __error__ [1m]))`, false},
		{`sum by (method) (count_over_time({app="api"} | json | ip=ip("10.2.0.0/16") [1m]))`, true},
	} {
		t.Run(tc.query, func(t *testing.T) {
			srv, fake := newPushdownFakeVL(t, rows, nil)
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			plan, ok := compileOrderedJSONMetric(tc.query)
			if !ok || !plan.pushdown {
				t.Fatalf("expected a pushdown plan, got ok=%v", ok)
			}
			if tc.pipelineError {
				params := url.Values{"query": {tc.query}, "start": {strconv.FormatInt(at.UnixNano(), 10)}, "end": {strconv.FormatInt(at.UnixNano(), 10)}, "step": {"60"}}
				rec := httptest.NewRecorder()
				p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
				if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "pipeline error") {
					t.Fatalf("expected Loki's pipeline error, got %d %s", rec.Code, rec.Body)
				}
			} else {
				want := lokiPushdownReference(t, plan, rows, at, at, time.Minute)
				if len(want) == 0 {
					t.Fatal("fixture yields no series")
				}
				if got := runJSONVolumeQueryRange(t, p, tc.query, at, at, time.Minute); !reflect.DeepEqual(got, want) {
					t.Fatalf("result differs from the row evaluator\n got: %v\nwant: %v", got, want)
				}
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if fake.raw != 1 {
				t.Fatalf("expected the probe to keep the raw evaluator: raw=%d guards=%q stats=%q", fake.raw, fake.guards, fake.stats)
			}
		})
	}
}

// An instant sum over an ip() filtered `| json` stream is one stats bucket,
// with the address filter in it and no raw read of the stream's rows.
// conformance: semantics/ip-filter-json-stats-pushdown
func TestOrderedJSONIPFilterInstantReadsNoRawRows(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	rows := ipFilterJSONRows(s0)
	for _, query := range []string{
		`sum(count_over_time({app="api"} | json | ip=ip("10.2.0.0/16") [1h]))`,
		`sum(count_over_time({app="api"} | json | ip!=ip("10.2.0.0/16") [1h]))`,
		`sum by (method) (count_over_time({app="api"} | json | ip=ip("10.2.0.0/16") [1h]))`,
	} {
		t.Run(query, func(t *testing.T) {
			at := s0.Add(10 * time.Minute)
			srv, fake := newPushdownFakeVL(t, rows, nil)
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			plan, ok := compileOrderedJSONMetric(query)
			if !ok || !plan.pushdown {
				t.Fatalf("expected a pushdown plan, got ok=%v", ok)
			}
			want := map[string]string{}
			for key, byTime := range lokiPushdownReference(t, plan, rows, at, at, time.Second) {
				want[key] = byTime[at.Unix()]
			}
			if len(want) == 0 {
				t.Fatal("fixture yields no series")
			}
			got, rec := runJSONVolumeQueryInstant(t, p, query, at)
			if rec.Code != http.StatusOK {
				t.Fatalf("status %d: %s", rec.Code, rec.Body)
			}
			if !reflect.DeepEqual(got.values, want) {
				t.Fatalf("result differs from the row evaluator\n got: %v\nwant: %v", got.values, want)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if fake.raw != 0 || len(fake.stats) != 1 {
				t.Fatalf("expected one stats call and no raw read: stats=%d raw=%d", len(fake.stats), fake.raw)
			}
			for _, raw := range fake.rawQueries {
				if strings.Contains(raw.query, "limit 1000001") {
					t.Errorf("raw read %q", raw.query)
				}
			}
			if !strings.Contains(fake.stats[0], `ip:~"`) || !strings.Contains(fake.stats[0], "ip:ipv4_range(") {
				t.Errorf("no address range and regexp filter in %s", fake.stats[0])
			}
		})
	}
}
