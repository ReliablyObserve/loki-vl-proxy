package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
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

// cutLokiResultAfterReference is the cut written plainly over decoded JSON: the grid origin is the earliest first
// point, the end the first grid time at or after the cut, every point after the end and every series left empty go.
// It returns the kept series (metric and points), nil when the body is not a matrix with a positive cut and step.
func cutLokiResultAfterReference(body []byte, cutNs, stepNs int64) []any {
	if cutNs <= 0 || stepNs <= 0 {
		return nil
	}
	var resp struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Values [][]json.Number   `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &resp) != nil || resp.Data.ResultType != "matrix" {
		return nil
	}
	ns := func(p []json.Number) int64 {
		sec, _ := strconv.ParseFloat(string(p[0]), 64)
		return int64(math.Round(sec*1e3)) * int64(time.Millisecond)
	}
	origin := int64(math.MaxInt64)
	for _, s := range resp.Data.Result {
		if len(s.Values) > 0 && ns(s.Values[0]) < origin {
			origin = ns(s.Values[0])
		}
	}
	end := origin
	if cutNs > origin {
		end = origin + (cutNs-origin+stepNs-1)/stepNs*stepNs
	}
	var out []any
	for _, s := range resp.Data.Result {
		var kept []string
		for _, p := range s.Values {
			if ns(p) <= end {
				kept = append(kept, string(p[0])+"="+string(p[1]))
			}
		}
		if kept != nil {
			out = append(out, fmt.Sprint(s.Metric, kept))
		}
	}
	return out
}

// randomCutMatrix is a matrix answer of random series and steps, as the proxy writes one (whole and fractional
// second times, ascending points, a warning first).
func randomCutMatrix(rng *rand.Rand) ([]byte, int64, int64) {
	step := int64(1+rng.Intn(120)) * int64(time.Second) / int64(1+rng.Intn(4))
	step -= step % int64(time.Millisecond)
	origin := int64(1700000000+rng.Intn(1000))*int64(time.Second) + int64(rng.Intn(4))*250*int64(time.Millisecond)
	var sb strings.Builder
	sb.WriteString(`{"warnings":["w"],"status":"success","data":{"resultType":"matrix","result":[`)
	series := rng.Intn(8)
	for s := 0; s < series; s++ {
		if s > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(`{"metric":{"pod":"p` + strconv.Itoa(s) + `"},"values":[`)
		k := rng.Intn(5)
		for i, n := 0, rng.Intn(12); i < n; i++ {
			if i > 0 {
				sb.WriteByte(',')
			}
			at := origin + int64(k)*step
			sb.WriteString(`[` + strconv.FormatFloat(float64(at)/1e9, 'f', -1, 64) + `,"` + strconv.Itoa(rng.Intn(9)) + `"]`)
			k += 1 + rng.Intn(3)
		}
		sb.WriteString(`]}`)
	}
	sb.WriteString(`],"stats":{}}}`)
	cut := origin + rng.Int63n(20*step+1) - step
	return []byte(sb.String()), cut, step
}

// The in-place cut keeps exactly the series and points the plain cut over decoded JSON keeps, over random answers,
// cuts and steps, and leaves the bytes around data.result as they came.
//
// conformance: limits/drilldown-series-limit-range-cut-at-trip-step
func TestCutLokiResultAfterMatchesReference(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 5000; i++ {
		body, cut, step := randomCutMatrix(rng)
		checkCutMatchesReference(t, body, cut, step)
	}
}

func checkCutMatchesReference(t *testing.T, body []byte, cut, step int64) {
	t.Helper()
	out := cutLokiResultAfter(body, cut, step)
	want := cutLokiResultAfterReference(body, cut, step)
	got := cutLokiResultAfterReference(out, math.MaxInt64/2, 1) // the output read back whole
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("cut %d step %d:\n in %s\nout %s\ngot  %v\nwant %v", cut, step, body, out, got, want)
	}
	if !strings.HasPrefix(string(out), `{"warnings":["w"],"status":"success","data":{"resultType":"matrix","result":[`) || !strings.HasSuffix(string(out), `],"stats":{}}}`) {
		t.Fatalf("the bytes around data.result changed: %s", out)
	}
}

// lokiPointTimeNs reads a point's time as the float reading rounded to the millisecond does, exactly for the
// seconds-with-up-to-three-decimals form the proxy writes and through the float for every other number; a string,
// another value or no time is not a time.
//
// conformance: limits/drilldown-series-limit-range-cut-at-trip-step
func TestLokiPointTimeNs(t *testing.T) {
	float := func(text string) int64 {
		sec, _ := strconv.ParseFloat(text, 64)
		return int64(math.Round(sec*1e3)) * int64(time.Millisecond)
	}
	for _, text := range []string{"0", "1700000000", "1700000000.5", "1700000000.25", "1700000000.125", "1789383657.6",
		"1700000000.1234", "1700000000.0005", "1.7e9", "-5", "17000000000000", "1700000000."} {
		if got, ok := lokiPointTimeNs([]byte("[" + text + `,"1"]`)); !ok || got != float(text) {
			t.Errorf("%s: %d %v, want %d", text, got, ok, float(text))
		}
	}
	for _, point := range []string{`["1700000000","1"]`, `[]`, `[null,"1"]`, `1700000000`, ``, `[,"1"]`, `[1.2.3,"1"]`} {
		if got, ok := lokiPointTimeNs([]byte(point)); ok {
			t.Errorf("%s read as %d", point, got)
		}
	}
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 20000; i++ {
		text := strconv.FormatInt(rng.Int63n(4e9), 10)
		if d := rng.Intn(4); d > 0 {
			text += "." + fmt.Sprintf("%0*d", d, rng.Intn(int(math.Pow10(d))))
		}
		if got, ok := lokiPointTimeNs([]byte("[ " + text + ` ,"1"]`)); !ok || got != float(text) {
			t.Fatalf("%s: %d %v, want %d", text, got, ok, float(text))
		}
	}
}

// FuzzCutLokiResultAfterMatchesReference is TestCutLokiResultAfterMatchesReference over fuzzed answers.
func FuzzCutLokiResultAfterMatchesReference(f *testing.F) {
	f.Add(int64(1))
	f.Add(int64(42))
	f.Fuzz(func(t *testing.T, seed int64) {
		body, cut, step := randomCutMatrix(rand.New(rand.NewSource(seed)))
		checkCutMatchesReference(t, body, cut, step)
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

// The Loki mode's ranking subquery, written from the LogsQL AST, is the text the VictoriaLogs oracle checked (v1.49.0,
// v1.53.0, v1.54.0: min(_time) sorts by time and the query survives the clone round trip), for a plain, a parsed
// and a quoted field; the top-value read ranks the same way.
//
// conformance: limits/drilldown-series-limit-range-cut-at-trip-step
func TestRankedSingleFieldQueryFirstMet(t *testing.T) {
	for _, tc := range []struct{ query, want string }{
		{`app:="x" pod:!"" | stats by (pod) count()`,
			`app:="x" pod:!"" | filter pod:in(app:="x" pod:!"" | stats by (pod) min(_time) as __lvp_rank | sort by (__lvp_rank, pod) | limit 11 | fields pod) | stats by (pod) count()`},
		{`app:="x" | unpack_logfmt | filter user_id:!"" | stats by (user_id) count()`,
			`app:="x" | unpack_logfmt | filter user_id:!"" | filter user_id:in(app:="x" | unpack_logfmt | filter user_id:!"" | stats by (user_id) min(_time) as __lvp_rank | sort by (__lvp_rank, user_id) | limit 11 | fields user_id) | stats by (user_id) count()`},
		{`app:="x" | stats by ("service.name") count()`,
			`app:="x" | filter "service.name":in(app:="x" | stats by ("service.name") min(_time) as __lvp_rank | sort by (__lvp_rank, "service.name") | limit 11 | fields "service.name") | stats by ("service.name") count()`},
	} {
		if got, ok := rankedSingleFieldQueryFirstMet(tc.query, 10); !ok || got != tc.want {
			t.Errorf("rankedSingleFieldQueryFirstMet(%q)\n got %v %q\nwant %q", tc.query, ok, got, tc.want)
		}
	}
	if got := firstMetRanking("pod", "_c", false, 4); got != ` | stats by (pod) min(_time) as _c | sort by (_c) | limit 4` {
		t.Errorf("top-value ranking: %q", got)
	}
}
