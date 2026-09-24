package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestOrderedJSONMetricPipelineState(t *testing.T) {
	for _, tc := range []struct {
		pipeline string
		line     string
		keep     bool
		errLabel string
		wantKind string
	}{
		{`| json`, `bad`, true, "JSONParserErr", "base"},
		{`| json | __error__=""`, `bad`, false, "", ""},
		{`| json | __error__!="" | drop __error__`, `bad`, true, "", "base"},
		{`| json | drop __error__ | __error__!=""`, `bad`, false, "", ""},
		{`| json | keep kind`, `bad`, true, "JSONParserErr", "base"},
		{`| json | drop __error_details__`, `bad`, true, "JSONParserErr", "base"},
		{`| json | drop kind="base"`, `{}`, true, "", ""},
		{`| json |= "missing"`, `bad`, false, "", ""},
		{`| json | kind_extracted="nested"`, `{"kind":"nested"}`, true, "", "base"},
		{`| json | object_child="nested"`, `{"object":{"child":"nested"}}`, true, "", "base"},
	} {
		t.Run(tc.pipeline+tc.line, func(t *testing.T) {
			plan, ok := compileOrderedJSONMetric(`rate({app="test"} ` + tc.pipeline + ` [5m])`)
			if !ok {
				t.Fatal("not compiled")
			}
			base := map[string]string{"app": "test", "kind": "base"}
			labels, keep, err := plan.process(tc.line, base)
			if err != nil || keep != tc.keep || labels["__error__"] != tc.errLabel || labels["kind"] != tc.wantKind {
				t.Fatalf("labels=%v keep=%v err=%v", labels, keep, err)
			}
			if len(base) != 2 || base["kind"] != "base" {
				t.Fatal("mutated original stream identity")
			}
		})
	}
}

func TestOrderedJSONMetricHintsAndEligibility(t *testing.T) {
	for _, tc := range []struct {
		query, line, wantError       string
		eligible, noLabels, preserve bool
	}{
		{`sum(rate({app="test"}|json[5m]))`, `bad`, "", true, true, false},
		{`sum by(app)(rate({app="test"}|json[5m]))`, `bad`, "JSONParserErr", true, false, false},
		{`sum by(__error__)(rate({app="test"}|json[5m]))`, `bad`, "JSONParserErr", true, false, true},
		{`sum by(value)(rate({app="test"}|json[5m]))`, `{"value":"ready","bad":INVALID}`, "", true, false, false},
		{`rate({app="test"}|json[5m])`, `{"value":"ready","bad":INVALID}`, "JSONParserErr", true, false, false},
		{`rate({app="test"}|json value="nested.value"[5m])`, `{}`, "", false, false, false},
		{`rate({app="test"}|json|logfmt[5m])`, `{}`, "", false, false, false},
		{`rate({app="test"}|json|status>=400[5m])`, `{}`, "", false, false, false},
		{`rate({app="test"}|= "| json field"|json[5m])`, `{}`, "", true, false, false},
	} {
		t.Run(tc.query, func(t *testing.T) {
			plan, ok := compileOrderedJSONMetric(tc.query)
			if ok != tc.eligible {
				t.Fatalf("eligible=%v", ok)
			}
			if !ok {
				return
			}
			if plan.noLabels != tc.noLabels || plan.preserveError != tc.preserve {
				t.Fatalf("hints=%+v", plan)
			}
			labels, _, err := plan.process(tc.line, map[string]string{"app": "test"})
			if err != nil || labels["__error__"] != tc.wantError {
				t.Fatalf("labels=%v err=%v", labels, err)
			}
		})
	}
}

func TestOrderedJSONMetricParserComplexity(t *testing.T) {
	plan, _ := compileOrderedJSONMetric(`rate({app="test"}|json[5m])`)
	deep := strings.Repeat(`{"nested":`, 66) + `1` + strings.Repeat(`}`, 66)
	wide := make(map[string]int)
	for i := 0; i < 1025; i++ {
		wide[fmt.Sprintf("field%d", i)] = i
	}
	encoded, _ := json.Marshal(wide)
	for _, line := range []string{deep, string(encoded), `{"value":"` + strings.Repeat("x", 64<<10+1) + `"}`, `{"` + strings.Repeat("x", 1025) + `":"value"}`} {
		_, _, err := plan.process(line, map[string]string{"app": "test"})
		if !errors.Is(err, errOrderedJSONComplexity) {
			t.Fatalf("wanted resource error, got %v", err)
		}
	}
}

func TestOrderedJSONMetricHintSuffixExpandedOnce(t *testing.T) {
	for i := 0; i < 100; i++ {
		plan, ok := compileOrderedJSONMetric(`sum by(foo_extracted_extracted)(count_over_time({app="test"}|json[5m]))`)
		if !ok || len(plan.required) != 2 || plan.required["foo"] {
			t.Fatalf("hint suffix must be expanded exactly once: %+v", plan)
		}
		labels, keep, err := plan.process(`{"foo_extracted":"one","foo_extracted_extracted":"two","bad":INVALID}`, map[string]string{"app": "test"})
		if err != nil || !keep || labels["__error__"] != "" || labels["foo_extracted_extracted"] != "two" {
			t.Fatalf("complete required hints must skip malformed tail: labels=%v keep=%v err=%v", labels, keep, err)
		}
	}
}

func TestOrderedJSONMetricPreserveErrorState(t *testing.T) {
	for _, tc := range []struct{ pipeline, want string }{
		{`|json`, "true"},
		{`|json|keep __error__`, "true"},
		{`|json|drop __preserve_error__`, ""},
	} {
		plan, _ := compileOrderedJSONMetric(`sum by(__error__,__preserve_error__)(count_over_time({app="test"}` + tc.pipeline + `[5m]))`)
		labels, keep, err := plan.process("bad", map[string]string{"app": "test"})
		if err != nil || !keep || labels["__preserve_error__"] != tc.want || labels["__error__"] != "JSONParserErr" {
			t.Fatalf("pipeline=%s labels=%v keep=%v err=%v", tc.pipeline, labels, keep, err)
		}
	}
}

func TestOrderedJSONMetricEmptyLabelAggregation(t *testing.T) {
	for _, tc := range []struct {
		query        string
		emptyPresent bool
	}{
		{`count_over_time({app="test"}|json[5m])`, true},
		{`count_over_time({app="test"}|drop unused|json[5m])`, false},
		{`sum by(value)(count_over_time({app="test"}|json[5m]))`, false},
	} {
		plan, _ := compileOrderedJSONMetric(tc.query)
		labels, _, err := plan.process(`{"value":""}`, map[string]string{"app": "test"})
		_, present := plan.groupLabels(labels)["value"]
		if err != nil || present != tc.emptyPresent {
			t.Fatalf("query=%s labels=%v err=%v", tc.query, labels, err)
		}
	}
}

func TestOrderedJSONMetricDroppedCollisionProvenance(t *testing.T) {
	plan, _ := compileOrderedJSONMetric(`count_over_time({app="test"}|drop value|json[5m])`)
	base := map[string]string{"app": "test", "value": "stored"}
	for _, metadata := range []bool{false, true} {
		stream := base
		want := "value_extracted"
		if metadata {
			stream, want = map[string]string{"app": "test"}, "value"
		}
		labels, keep, err := plan.processWithStreamLabels(`{"value":"parsed"}`, base, stream)
		if err != nil || !keep || labels[want] != "parsed" || len(labels) != 2 {
			t.Fatalf("metadata=%v labels=%v keep=%v err=%v", metadata, labels, keep, err)
		}
	}
}

func TestOrderedJSONMetricUnicodeReplacement(t *testing.T) {
	plan, _ := compileOrderedJSONMetric(`count_over_time({app="test"}|json[5m])`)
	for _, tc := range []struct{ line, want string }{
		{`{"value":"a\ufffdb"}`, "a b"},
		{`{"value":"bad\q"}`, ""},
		{`{" ":{"value":"nested"}}`, "nested"},
		{`{"value":{" ":"nested"}}`, "nested"},
	} {
		labels, keep, err := plan.process(tc.line, map[string]string{"app": "test"})
		if err != nil || !keep || labels["value"] != tc.want || labels["__error__"] != "" {
			t.Fatalf("line=%s labels=%v keep=%v err=%v", tc.line, labels, keep, err)
		}
	}
}

func TestOrderedJSONMetricEarlyFilterHints(t *testing.T) {
	inner := `count_over_time({app="test"}|json|drop value|value=""[5m])`
	for _, tc := range []struct {
		query string
		keep  bool
	}{{inner, false}, {"sum(" + inner + ")", true}, {"sum by(app)(" + inner + ")", true}, {"sum without(app)(" + inner + ")", false}, {`count_over_time({app="test"}|json|value="bad"|drop value|value=""[5m])`, true}} {
		plan, ok := compileOrderedJSONMetric(tc.query)
		if !ok {
			t.Fatal(tc.query)
		}
		labels, keep, err := plan.process(`{"value":"bad"}`, map[string]string{"app": "test"})
		if err != nil || keep != tc.keep || labels["value"] != "" {
			t.Fatalf("query=%s labels=%v keep=%v err=%v", tc.query, labels, keep, err)
		}
	}
}

func TestOrderedJSONMetricSlidingWindowRetainsRealZeroAndExcludesBoundary(t *testing.T) {
	plan, _ := compileOrderedJSONMetric(`bytes_over_time({app="test"}|json|drop __error__[10s])`)
	stamp := time.Unix(100, 0)
	series := map[string]manualSeriesSamples{"one": {
		Metric: map[string]string{"app": "test"},
		Samples: []rangeMetricSample{
			{ts: time.Unix(100, 0).UnixNano(), value: 0},
			{ts: time.Unix(90, 0).UnixNano(), value: 100},
		},
	}}
	body, err := buildOrderedJSONMetric(t.Context(), plan, series, stamp, stamp.Add(20*time.Second), 10*time.Second, true, defaultOrderedJSONMetricMaxBytes)
	if err != nil {
		t.Fatal(err)
	}
	var response struct {
		Data struct {
			Result []struct{ Values [][]json.RawMessage }
		}
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Data.Result) != 1 || len(response.Data.Result[0].Values) != 1 || string(response.Data.Result[0].Values[0][0]) != "100" || string(response.Data.Result[0].Values[0][1]) != `"0"` {
		t.Fatalf("expected one real zero point, no lower-bound or empty samples: %s", body)
	}
}

func TestOrderedJSONMetricVisibilityBoundaries(t *testing.T) {
	for _, tc := range []struct {
		ts   int64
		want bool
	}{{89, false}, {90, false}, {91, true}, {100, true}, {101, false}, {190, false}, {191, true}, {200, true}, {201, false}} {
		if got := orderedJSONSampleVisible(tc.ts, 100, 200, 100, 10); got != tc.want {
			t.Errorf("ts=%d got=%v", tc.ts, got)
		}
	}
}

func TestOrderedJSONMetricRowsCancellationAndTenant(t *testing.T) {
	stamp := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var requestedLimit, requestedQuery, account string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			fmt.Fprintln(w, `{}`)
			return
		}
		_ = r.ParseForm()
		requestedLimit, requestedQuery, account = r.FormValue("limit"), r.FormValue("query"), r.Header.Get("AccountID")
		for i := 0; i < 2; i++ {
			_ = json.NewEncoder(w).Encode(map[string]string{"_time": stamp.Add(time.Second).Format(time.RFC3339Nano), "_msg": `{"value":"row"}`, "_stream": `{app="test",kind="original"}`})
		}
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	p.ReloadTenantMap(map[string]TenantMapping{"tenant-a": {AccountID: "91", ProjectID: "0"}})
	p.rangeMetricRowLimit = 1
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Scope-OrgID", "tenant-a")
	req = p.withRequestScope(req)
	plan, _ := compileOrderedJSONMetric(`rate({app="test"}|json[5m])`)
	_, err := p.collectOrderedJSONMetric(req.Context(), plan, stamp.Add(2*time.Second), stamp.Add(2*time.Second), time.Second)
	if err == nil || !strings.Contains(err.Error(), "row limit") || requestedLimit != "" || !strings.HasSuffix(requestedQuery, " | limit 2") || account != "91" {
		t.Fatalf("limit=%s query=%s account=%s err=%v", requestedLimit, requestedQuery, account, err)
	}
	ctx, cancel := context.WithCancel(req.Context())
	cancel()
	_, _, err = plan.processWithContext(ctx, `{}`, map[string]string{"app": "test"}, map[string]string{"app": "test"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("pipeline cancellation=%v", err)
	}
	_, err = p.collectOrderedJSONMetric(ctx, plan, stamp, stamp, time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
	_, err = buildOrderedJSONMetric(ctx, plan, map[string]manualSeriesSamples{"x": {Samples: []rangeMetricSample{{ts: stamp.UnixNano(), value: 1}}}}, stamp, stamp, time.Second, true, defaultOrderedJSONMetricMaxBytes)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("evaluation cancellation=%v", err)
	}
}

func TestOrderedJSONMetricBodyBudgetRejectsPartialResponse(t *testing.T) {
	stamp := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	row, _ := json.Marshal(map[string]string{"_time": stamp.Format(time.RFC3339Nano), "_msg": strings.Repeat("x", 1<<20), "_stream": `{app="test"}`})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			fmt.Fprintln(w, `{}`)
			return
		}
		for i := 0; i < 65; i++ {
			if _, err := w.Write(append(row, '\n')); err != nil {
				return
			}
		}
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	p.orderedJSONMaxBytes = 64 << 20
	plan, _ := compileOrderedJSONMetric(`sum(rate({app="test"}|json[5m]))`)
	series, err := p.collectOrderedJSONMetric(t.Context(), plan, stamp.Add(time.Second), stamp.Add(time.Second), time.Second)
	if err == nil || series != nil {
		t.Fatalf("oversized response returned partial samples: series=%v err=%v", series, err)
	}
}

func TestOrderedJSONMetricHandlerRejectsSurvivingErrors(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			fmt.Fprintln(w, `{}`)
			return
		}
		fmt.Fprintln(w, `{"_time":"2026-01-01T00:00:01Z","_msg":"invalid JSON","_stream":"{app=\"test\"}"}`)
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	for _, mode := range []string{"query", "query_range"} {
		params := url.Values{"query": {`rate({app="test"}|json|__error__!=""[5m])`}, "time": {"2026-01-01T00:00:02Z"}, "start": {"2026-01-01T00:00:02Z"}, "end": {"2026-01-01T00:00:02Z"}, "step": {"60"}}
		result := doCompatProxyRequest(p, "/loki/api/v1/"+mode+"?"+params.Encode(), nil)
		if result.Code != 400 || !strings.Contains(result.Body.String(), "JSONParserErr") {
			t.Fatalf("%s status=%d body=%s", mode, result.Code, result.Body)
		}
	}
}

func TestOrderedJSONMetricCollectorUpperInclusiveLowerExclusive(t *testing.T) {
	evaluation := time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)
	for _, upperError := range []bool{false, true} {
		t.Run(fmt.Sprint(upperError), func(t *testing.T) {
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/select/logsql/query" {
					fmt.Fprintln(w, `{}`)
					return
				}
				_ = r.ParseForm()
				end, err := time.Parse(time.RFC3339Nano, r.FormValue("end"))
				if err != nil {
					t.Error(err)
					return
				}
				if end != evaluation.Add(time.Nanosecond) {
					t.Errorf("VL end=%s want=%s", end, evaluation.Add(time.Nanosecond))
				}
				start, err := time.Parse(time.RFC3339Nano, r.FormValue("start"))
				if err != nil {
					t.Error(err)
					return
				}
				for _, ts := range []time.Time{evaluation.Add(-5 * time.Minute), evaluation} {
					if !ts.Before(end) || ts.Before(start) {
						continue
					}
					line := `{"value":"ok"}`
					if upperError || ts.Before(evaluation) {
						line = "invalid"
					}
					_ = json.NewEncoder(w).Encode(map[string]string{"_time": ts.Format(time.RFC3339Nano), "_msg": line, "_stream": `{app="test"}`})
				}
			}))
			defer backend.Close()
			p := newTestProxy(t, backend.URL)
			plan, _ := compileOrderedJSONMetric(`rate({app="test"}|json[5m])`)
			series, err := p.collectOrderedJSONMetric(t.Context(), plan, evaluation, evaluation, time.Second)
			if upperError {
				var pipelineErr *orderedJSONPipelineError
				if !errors.As(err, &pipelineErr) {
					t.Fatalf("upper boundary parser error lost: %v", err)
				}
			} else if err != nil || len(series) != 1 {
				t.Fatalf("upper valid row lost or lower error retained: series=%v err=%v", series, err)
			}
		})
	}
}

func TestOrderedJSONMetricNoLabelsRewriteAndRequestIsolation(t *testing.T) {
	plan, ok := compileOrderedJSONMetric(`sum(rate({app="test"} |= "original" | json | drop kind [5m]))`)
	if !ok || !plan.noLabels || plan.withoutJSON != `sum(rate({app="test"} |= "original"[5m]))` {
		t.Fatalf("invalid NoLabels rewrite: %+v", plan)
	}
	parent := httptest.NewRequest(http.MethodPost, "/loki/api/v1/query_range?start=100&step=60", strings.NewReader("query=original&end=200"))
	parent.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	parent.Header.Set("X-Scope-OrgID", "tenant-a")
	if err := parent.ParseForm(); err != nil {
		t.Fatal(err)
	}
	child := cloneMetricQueryRequest(parent, "replacement")
	if child.FormValue("query") != "replacement" || child.FormValue("start") != "100" || child.FormValue("end") != "200" || child.Context() != parent.Context() || child.Header.Get("X-Scope-OrgID") != "tenant-a" {
		t.Fatalf("child lost request scope/parameters: %+v", child)
	}
	child.Form["start"][0] = "999"
	child.PostForm["end"][0] = "888"
	child.Header.Set("X-Scope-OrgID", "tenant-b")
	if parent.FormValue("query") != "original" || parent.FormValue("start") != "100" || parent.PostForm.Get("end") != "200" || parent.Header.Get("X-Scope-OrgID") != "tenant-a" || parent.URL.Query().Has("query") {
		t.Fatal("child mutated parent request")
	}
}

func TestOrderedJSONMetricNativeRewriteRequiresGuaranteedGroupAndClearedErrors(t *testing.T) {
	for _, tc := range []struct {
		query  string
		native bool
	}{
		{`sum by(app)(rate({app="test"}|json|drop __error__[5m]))`, true},
		{`sum by(app)(rate({app="test"}|json|drop __error_details__,__error__[5m]))`, true},
		{`sum by(app)(rate({app="test"}|json|drop __error__|json[5m]))`, false},
		{`sum by(app)(rate({app="test"}|json|__error__=""[5m]))`, false},
		{`sum by(app)(rate({app="test"}|json|drop __error_details__[5m]))`, false},
		{`sum by(app)(rate({app="test"}|json|drop __error__,app[5m]))`, false},
		{`sum by(kind)(rate({app="test"}|json|drop __error__[5m]))`, false},
	} {
		t.Run(tc.query, func(t *testing.T) {
			plan, ok := compileOrderedJSONMetric(tc.query)
			if !ok || (plan.withoutJSON != "") != tc.native {
				t.Fatalf("plan=%+v compiled=%v", plan, ok)
			}
		})
	}
}

func TestOrderedJSONMetricStructuredMetadataAndParserCollisions(t *testing.T) {
	p := newTestProxy(t, "http://127.0.0.1:1")
	row := map[string]string{"_stream": `{app="test"}`, "_msg": `{"trace_id":"json-value","same":"shared"}`, "trace_id": "metadata", "span_id": "span-value", "same": "shared"}
	desc := p.logQueryStreamDescriptor(row["_stream"], "", make(map[string]map[string]string), make(map[string]cachedLogQueryStreamDescriptor))
	base, err := p.orderedJSONBaseLabels(row, desc, make(map[string][]metadataFieldExposure))
	if err != nil {
		t.Fatal(err)
	}
	plan, _ := compileOrderedJSONMetric(`rate({app="test"}|json|trace_id="metadata"|trace_id_extracted="json-value"|span_id="span-value"[5m])`)
	labels, keep, err := plan.process(row["_msg"], base)
	if err != nil || !keep || labels["same"] != "shared" || labels["same_extracted"] != "shared" || labels["trace_id"] != "metadata" || labels["trace_id_extracted"] != "json-value" {
		t.Fatalf("labels=%v keep=%v err=%v", labels, keep, err)
	}
	if _, exists := desc.translatedLabels["trace_id"]; exists {
		t.Fatal("metadata mutated cached stream identity")
	}
}

// conformance: semantics/pipeline-error-text-parity
func TestLokiPipelineErrorTypeIsAlwaysALokiConstant(t *testing.T) {
	for value, want := range map[string]string{
		"JSONParserErr":   "JSONParserErr",
		"LogfmtParserErr": "LogfmtParserErr",
		"x'y\"z":          "JSONParserErr",
		"":                "JSONParserErr",
	} {
		if got := lokiPipelineErrorType(value); got != want {
			t.Fatalf("lokiPipelineErrorType(%q) = %q, want %q", value, got, want)
		}
	}
	msg := lokiPipelineError(map[string]string{"__error__": "JSONParserErr", "app": "a'b"}).Error()
	if !strings.HasPrefix(msg, `pipeline error: 'JSONParserErr' for series: '{__error__="JSONParserErr", app="a'b"}'.`) {
		t.Fatalf("unexpected message: %s", msg)
	}
}
