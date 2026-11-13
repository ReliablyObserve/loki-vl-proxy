package querycorpus_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// The Loki metric differential's own checks: its fixtures spread lines over the window as it says, its requests
// sit on the step's grid, its front server keys a request whatever the form's order, and its comparison sees a
// wrong answer.

// conformance: architecture-syntax-package-layering
func TestLokiMetricDifferentialFixtures(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	scope := metricDiffQueries(t)
	if len(scope.queries) == 0 {
		t.Fatal("no metric query in scope")
	}
	for _, mq := range scope.queries {
		f := mq.fix
		if f.hash() != buildMetricFixture(mq.plan).hash() {
			t.Errorf("%s: the fixture is not the same from one build to the next", mq.q)
		}
		if len(f.Entries) > metricMaxEntries {
			t.Errorf("%s: %d entries, more than %d", mq.q, len(f.Entries), metricMaxEntries)
		}
		namesLevel := false
		for _, sel := range logSelects(mq.plan.Root) {
			for _, m := range sel.Matchers {
				namesLevel = namesLevel || m.Name == "level"
			}
		}
		seen := map[int64]bool{}
		for _, e := range f.Entries {
			if seen[e.TS] {
				t.Errorf("%s: two lines at %d", mq.q, e.TS)
			}
			seen[e.TS] = true
			if e.TS >= metricEnd.UnixNano() {
				t.Errorf("%s: a line at %d, not before the window end", mq.q, e.TS)
			}
			if off := e.TS % int64(time.Millisecond); off < int64(100*time.Microsecond) || off > int64(900*time.Microsecond) {
				t.Errorf("%s: a line %d ns past a millisecond, near a window edge", mq.q, off)
			}
			if e.Metadata["detected_level"] == "" {
				t.Errorf("%s: a line without its detected_level", mq.q)
			}
			if _, ok := f.Streams[e.Stream]["level"]; ok && !namesLevel {
				t.Errorf("%s: a stream with a level label no selector names", mq.q)
			}
		}
		for _, r := range mq.reqs {
			if r.instant {
				continue
			}
			ms := int64(0)
			if v, ok := strings.CutSuffix(r.form.Get("step"), "ms"); ok {
				ms, _ = strconv.ParseInt(v, 10, 64)
			} else if sec, err := strconv.ParseInt(r.form.Get("step"), 10, 64); err == nil {
				ms = sec * 1000
			}
			if ms <= 0 {
				t.Fatalf("%s: step %q", mq.q, r.form.Get("step"))
			}
			for _, bound := range []string{"start", "end"} {
				ts, err := time.Parse(time.RFC3339Nano, r.form.Get(bound))
				if err != nil {
					t.Fatalf("%s: %s %q: %v", mq.q, bound, r.form.Get(bound), err)
				}
				if ts.UnixMilli()%ms != 0 || ts.UnixNano()%int64(time.Millisecond) != 0 {
					t.Errorf("%s %s: %s %s is not on the step's grid", mq.q, r.kind, bound, ts)
				}
			}
		}
	}
}

// The front server keys a request by its path and its sorted form, from the query string and a form body alike.
func TestLokiMetricDifferentialRequestKey(t *testing.T) {
	get := httptest.NewRequest(http.MethodGet, "/select/logsql/stats_query?step=60s&query=%2A&start=1", nil)
	post := httptest.NewRequest(http.MethodPost, "/select/logsql/stats_query", nil)
	body := url.Values{"start": {"1"}, "query": {"*"}, "step": {"60s"}}.Encode()
	if a, b := metricRequestKey(get, nil), metricRequestKey(post, []byte(body)); a != b {
		t.Errorf("keys differ: %q, %q", a, b)
	}
	other := httptest.NewRequest(http.MethodGet, "/select/logsql/stats_query_range?step=60s&query=%2A&start=1", nil)
	if metricRequestKey(get, nil) == metricRequestKey(other, nil) {
		t.Error("two paths share a key")
	}
}

// The comparison sees a wrong answer: on runs that answer as Loki does, a dropped series, an added series, a
// changed value, a dropped point, a label renamed, another status and another error text each show as their class,
// and a value within the float tolerance does not.
//
// conformance: architecture-syntax-package-layering
func TestLokiMetricDifferentialCatchesAWrongAnswer(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	answers := readMetricAnswers(t)
	prof := diffProfiles()[0]
	type body struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string           `json:"resultType"`
			Result     []map[string]any `json:"result"`
		} `json:"data"`
	}
	mutations := map[string]func(b *body) bool{
		"misses a series Loki has": func(b *body) bool {
			if len(b.Data.Result) == 0 {
				return false
			}
			b.Data.Result = b.Data.Result[1:]
			return true
		},
		"adds a series Loki does not have": func(b *body) bool {
			if len(b.Data.Result) == 0 {
				return false
			}
			extra := map[string]any{}
			for k, v := range b.Data.Result[0] {
				extra[k] = v
			}
			extra["metric"] = map[string]string{"zz_extra": "1"}
			b.Data.Result = append(b.Data.Result, extra)
			return true
		},
		"series: misses a label": func(b *body) bool {
			for _, s := range b.Data.Result {
				// A series of two labels or more keeps one, so the nearest series is found.
				if m, _ := s["metric"].(map[string]any); len(m) > 1 {
					for k := range m {
						delete(m, k)
						return true
					}
				}
			}
			return false
		},
		"writes another value": func(b *body) bool {
			return mutatePoint(b.Data.ResultType, b.Data.Result, func(v string) string {
				f, _ := strconv.ParseFloat(v, 64)
				return strconv.FormatFloat(f*2+1, 'f', -1, 64)
			})
		},
		"misses a point Loki has": func(b *body) bool {
			if b.Data.ResultType != "matrix" {
				return false
			}
			for _, s := range b.Data.Result {
				if vs, _ := s["values"].([]any); len(vs) > 1 {
					s["values"] = vs[1:]
					return true
				}
			}
			return false
		},
	}
	tried := map[string]int{}
	tolerated := 0
	for _, mq := range metricDiffQueries(t).queries {
		rec := answers[mq.q]
		if rec == nil || rec.Inputs != mq.inputsHash() {
			continue
		}
		for _, mr := range mq.reqs {
			loki, ok := rec.Loki[mr.kind]
			if !ok || loki.Status != 200 {
				continue
			}
			run := runMetricHandler(t, prof, mr, &metricFront{version: prof.version, answers: rec.VL[prof.version]})
			if len(run.missing) > 0 || run.status != 200 || len(compareMetric(loki, run).classes) != 0 {
				continue
			}
			for class, mutate := range mutations {
				if tried[class] >= 3 {
					continue
				}
				var b body
				if err := json.Unmarshal([]byte(run.body), &b); err != nil {
					t.Fatal(err)
				}
				if !mutate(&b) {
					continue
				}
				out, _ := json.Marshal(b)
				tried[class]++
				if d := compareMetric(loki, metricHandlerRun{status: 200, body: string(out)}); !hasClass(d.classes, class) {
					t.Errorf("%s %s: %q is not seen: %v", mq.q, mr.kind, class, d.classes)
				}
			}
			if tolerated < 3 {
				var b body
				_ = json.Unmarshal([]byte(run.body), &b)
				if mutatePoint(b.Data.ResultType, b.Data.Result, func(v string) string {
					f, err := strconv.ParseFloat(v, 64)
					if err != nil || f == 0 {
						return v
					}
					return strconv.FormatFloat(f*(1+1e-12), 'g', 17, 64)
				}) {
					out, _ := json.Marshal(b)
					d := compareMetric(loki, metricHandlerRun{status: 200, body: string(out)})
					if len(d.classes) != 0 {
						t.Errorf("%s %s: a value within the float tolerance differs: %v", mq.q, mr.kind, d.classes)
					}
					tolerated++
				}
			}
		}
		if len(tried) == len(mutations) && minCount(tried) >= 3 && tolerated >= 3 {
			break
		}
	}
	for class := range mutations {
		if tried[class] == 0 {
			t.Errorf("no run to try %q on", class)
		}
	}
	status := compareMetric(metricLokiAnswer{Status: 400, Error: "parse error"}, metricHandlerRun{status: 200, body: `{"status":"success"}`})
	if status.classes["answers status 200 where Loki answers 400"] == "" {
		t.Errorf("another status is not seen: %v", status.classes)
	}
	text := compareMetric(metricLokiAnswer{Status: 400, Error: "parse error"}, metricHandlerRun{status: 400, body: "another error"})
	if text.classes["writes another error text"] == "" {
		t.Errorf("another error text is not seen: %v", text.classes)
	}
}

// mutatePoint changes the first non-zero value of a matrix or vector answer.
func mutatePoint(resultType string, result []map[string]any, change func(string) string) bool {
	for _, s := range result {
		var points []any
		switch resultType {
		case "matrix":
			points, _ = s["values"].([]any)
		case "vector":
			points = []any{s["value"]}
		}
		for _, p := range points {
			pair, _ := p.([]any)
			if len(pair) != 2 {
				continue
			}
			v, _ := pair[1].(string)
			if v == "" || v == "0" || strings.ContainsAny(v, "NnIi") {
				continue
			}
			if c := change(v); c != v {
				pair[1] = c
				return true
			}
		}
	}
	return false
}

// hasClass is whether the classes hold class; "series: misses a label" is any label kind's class of it.
func hasClass(classes map[string]string, class string) bool {
	if class == "series: misses a label" {
		for c := range classes {
			if strings.HasPrefix(c, "series: misses ") {
				return true
			}
		}
		return false
	}
	return classes[class] != ""
}

func minCount(m map[string]int) int {
	lowest := -1
	for _, n := range m {
		if lowest < 0 || n < lowest {
			lowest = n
		}
	}
	return lowest
}
