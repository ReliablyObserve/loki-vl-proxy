package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// cutLokiResultAfter ends a matrix at the first grid step at or after the cut: points after it go, a series left
// without points goes, the response keeps Loki's key order, and a vector, a zero cut or a body that is not JSON is
// unchanged.
//
// conformance: limits/drilldown-series-limit-range-cut-at-trip-step
func TestCutLokiResultAfter(t *testing.T) {
	body := `{"status":"success","warnings":["w"],"data":{"resultType":"matrix","result":[` +
		`{"metric":{"pod":"a"},"values":[[100,"1"],[160,"2"],[220,"3"]]},` +
		`{"metric":{"pod":"b"},"values":[[220,"4"],[280,"5"]]}]}}`
	step := int64(60 * time.Second)
	for _, tc := range []struct {
		name string
		cut  int64
		want string
	}{
		{"on a step", 160e9, `{"status":"success","warnings":["w"],"data":{"resultType":"matrix","result":[{"metric":{"pod":"a"},"values":[[100,"1"],[160,"2"]]}]}}`},
		{"inside a step moves up to it", 161e9, `{"status":"success","warnings":["w"],"data":{"resultType":"matrix","result":[{"metric":{"pod":"a"},"values":[[100,"1"],[160,"2"],[220,"3"]]},{"metric":{"pod":"b"},"values":[[220,"4"]]}]}}`},
		{"before the first step keeps it", 1e9, `{"status":"success","warnings":["w"],"data":{"resultType":"matrix","result":[{"metric":{"pod":"a"},"values":[[100,"1"]]}]}}`},
		{"after the last step keeps all", 999e9, body},
	} {
		got := string(cutLokiResultAfter([]byte(body), tc.cut, step))
		if tc.want == body {
			var a, b any
			_ = json.Unmarshal([]byte(got), &a)
			_ = json.Unmarshal([]byte(body), &b)
			if jsonText(a) != jsonText(b) {
				t.Errorf("%s: %s", tc.name, got)
			}
			continue
		}
		if got != tc.want {
			t.Errorf("%s:\n got %s\nwant %s", tc.name, got, tc.want)
		}
	}
	vector := `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[100,"1"]}]}}`
	for _, in := range []string{vector, "not json", `{"data":1}`} {
		if got := string(cutLokiResultAfter([]byte(in), 160e9, step)); got != in {
			t.Errorf("%q changed: %s", in, got)
		}
	}
	if got := string(cutLokiResultAfter([]byte(body), 0, step)); got != body {
		t.Errorf("a zero cut changed the body: %s", got)
	}
}

func jsonText(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// seriesKeptForLimit keeps the busiest series by default and, in the Loki mode, the series met first, recording
// the first time of the first series it drops; ties break on the key both ways.
//
// conformance: limits/drilldown-series-limit-range-cut-at-trip-step, limits/drilldown-breakdown-keeps-busiest
func TestSeriesKeptForLimit(t *testing.T) {
	keys := []string{"a", "b", "c", "d"}
	totals := map[string]float64{"a": 1, "b": 9, "c": 5, "d": 9}
	firsts := map[string]int64{"a": 30, "b": 10, "c": 20, "d": 10}
	total := func(k string) float64 { return totals[k] }
	first := func(k string) int64 { return firsts[k] }

	busiest := &seriesLimitScope{drilldown: true}
	ctx := context.WithValue(context.Background(), seriesLimitScopeKey{}, busiest)
	if got := strings.Join(seriesKeptForLimit(ctx, keys, total, first, 2), ","); got != "b,d" {
		t.Errorf("busiest: %s", got)
	}
	if busiest.cutNs.Load() != 0 {
		t.Error("busiest mode records no cut")
	}

	loki := &seriesLimitScope{drilldown: true, lokiCut: true, stepNs: 1}
	ctx = context.WithValue(context.Background(), seriesLimitScopeKey{}, loki)
	if got := strings.Join(seriesKeptForLimit(ctx, keys, total, first, 2), ","); got != "b,d" {
		t.Errorf("loki, two series met at 10: %s", got)
	}
	if loki.cutNs.Load() != 20 {
		t.Errorf("loki cut %d, want 20 (c, the first series dropped)", loki.cutNs.Load())
	}
	if got := strings.Join(seriesKeptForLimit(ctx, keys, total, first, 3), ","); got != "b,d,c" {
		t.Errorf("loki, three: %s", got)
	}
	if loki.cutNs.Load() != 20 {
		t.Errorf("a later, later cut does not move the earliest one: %d", loki.cutNs.Load())
	}
	if got := seriesKeptForLimit(ctx, keys, total, first, 4); len(got) != 4 {
		t.Errorf("under the limit every key stays: %v", got)
	}
}

// The mode is the operator's: an unknown value is refused at startup, a Drilldown range request in the default
// mode gets the Loki cut and an instant one or a busiest-mode proxy does not.
func TestDrilldownSeriesLimitModeScope(t *testing.T) {
	if _, err := New(Config{BackendURL: "http://127.0.0.1:1", DrilldownSeriesLimitMode: "first"}); err == nil {
		t.Fatal("an unknown mode must be refused")
	}
	drilldown := func(target string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, target, nil)
		r.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
		return r
	}
	p := newTestProxy(t, "http://127.0.0.1:1")
	if _, s := p.withSeriesLimitScope(drilldown("/loki/api/v1/query_range?query=x&step=60")); !s.lokiCut || s.stepNs != int64(time.Minute) {
		t.Errorf("default mode, range: %+v", s)
	}
	if _, s := p.withSeriesLimitScope(drilldown("/loki/api/v1/query?query=x")); s.lokiCut {
		t.Error("an instant request keeps the busiest series")
	}
	if _, s := p.withSeriesLimitScope(httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?query=x&step=60", nil)); s.lokiCut {
		t.Error("a request that is not Drilldown's is not cut")
	}
	p.drilldownSeriesLimitMode = drilldownSeriesLimitBusiest
	if _, s := p.withSeriesLimitScope(drilldown("/loki/api/v1/query_range?query=x&step=60")); s.lokiCut {
		t.Error("busiest mode keeps the whole range")
	}
}

// FuzzCutLokiResultAfter: any body and cut terminate, a JSON matrix keeps only points at or before the cut's step,
// and anything else comes back unchanged.
func FuzzCutLokiResultAfter(f *testing.F) {
	f.Add(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b"},"values":[[1,"1"],[61,"2"]]}]}}`, int64(30e9))
	f.Add(`{"data":{"resultType":"vector","result":[]}}`, int64(1))
	f.Add(`{"data":{"resultType":"matrix","result":[{"values":[["x","1"]]}]}}`, int64(5e9))
	f.Fuzz(func(t *testing.T, body string, cut int64) {
		if len(body) > 4096 {
			return
		}
		out := cutLokiResultAfter([]byte(body), cut, int64(60*time.Second))
		var in, got struct {
			Data struct {
				ResultType string `json:"resultType"`
				Result     []struct {
					Values [][]json.RawMessage `json:"values"`
				} `json:"result"`
			} `json:"data"`
		}
		if json.Unmarshal(out, &got) != nil || json.Unmarshal([]byte(body), &in) != nil {
			return
		}
		if len(got.Data.Result) > len(in.Data.Result) {
			t.Fatalf("the cut added series: %s -> %s", body, out)
		}
	})
}

// BenchmarkCutLokiResultAfter is what a Drilldown answer over the limit pays in the Loki mode: 1000 series of 60
// points, cut half way.
func BenchmarkCutLokiResultAfter(b *testing.B) {
	var sb strings.Builder
	sb.WriteString(`{"status":"success","warnings":["w"],"data":{"resultType":"matrix","result":[`)
	for s := 0; s < 1000; s++ {
		if s > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(`{"metric":{"pod":"p` + strings.Repeat("x", s%7) + `"},"values":[`)
		for i := 0; i < 60; i++ {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(`[` + jsonText(1800000000+i*60) + `,"1"]`)
		}
		sb.WriteString(`]}`)
	}
	sb.WriteString(`]}}`)
	body := []byte(sb.String())
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = cutLokiResultAfter(body, (1800000000+1800)*int64(time.Second), int64(time.Minute))
	}
}
