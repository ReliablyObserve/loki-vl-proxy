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
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The split grid is Loki's query frontend's: multiples of the interval since the epoch strictly inside (start, end),
// a step on a boundary starting the later split; no interval is one split.
//
// conformance: limits/drilldown-series-limit-range-cut-at-trip-step
func TestSeriesLimitScopeSplits(t *testing.T) {
	h := int64(time.Hour)
	s := &seriesLimitScope{startNs: 1000*h + 600e9, endNs: 1003 * h, stepNs: 60e9, splitNs: h}
	for _, tc := range []struct {
		t    int64
		want int
	}{{1000*h + 600e9, 0}, {1001*h - 60e9, 0}, {1001 * h, 1}, {1002*h + 1, 2}, {1003 * h, 2}} {
		if got := s.splitOf(tc.t); got != tc.want {
			t.Errorf("splitOf(%d) = %d, want %d", tc.t-1000*h, got, tc.want)
		}
	}
	if aligned := (&seriesLimitScope{startNs: 1000 * h, endNs: 1001 * h, stepNs: 60e9, splitNs: h}); aligned.splitOf(1001*h) != 0 {
		t.Error("an end on a boundary is not a split: the last step stays in the one split")
	}
	if none := (&seriesLimitScope{startNs: 0, endNs: 10 * h, stepNs: 60e9}); none.splitOf(5*h) != 0 {
		t.Error("no split interval: one split")
	}
	if got := s.stepOf(1000*h + 600e9 + 1); got != 1000*h+660e9 {
		t.Errorf("a line is met at the first step at or after it: %d", got)
	}
	var firsts []int64
	s.firstStepsInSplits(1000*h+900e9, 1002*h+300e9, func(t int64) { firsts = append(firsts, t-1000*h) })
	if fmt.Sprint(firsts) != fmt.Sprint([]int64{900e9, h, 2 * h}) {
		t.Errorf("a run's first step in every split: %v", firsts)
	}
}

// cutLokiSplits keeps, in each split, the series met first up to the step where the series met so far pass the
// limit, filling the limit from those new at that step by labels; a split within the limit stays whole; the
// answer keeps Loki's key order and the bytes around data.result; a vector or a body that is not JSON is unchanged.
//
// conformance: limits/drilldown-series-limit-range-cut-at-trip-step
func TestCutLokiSplits(t *testing.T) {
	body := `{"status":"success","warnings":["w"],"data":{"resultType":"matrix","result":[` +
		`{"metric":{"pod":"a"},"values":[[100,"1"],[160,"2"],[220,"3"],[280,"4"]]},` +
		`{"metric":{"pod":"b"},"values":[[160,"5"],[220,"6"],[280,"7"]]},` +
		`{"metric":{"pod":"c"},"values":[[220,"8"],[280,"9"]]}],"stats":{}}}`
	one := &seriesLimitScope{startNs: 100e9, endNs: 280e9, stepNs: 60e9}
	split := &seriesLimitScope{startNs: 100e9, endNs: 280e9, stepNs: 60e9, splitNs: 200e9} // a boundary at 200
	head, tail := `{"status":"success","warnings":["w"],"data":{"resultType":"matrix","result":[`, `],"stats":{}}}`
	for _, tc := range []struct {
		name    string
		scope   *seriesLimitScope
		limit   int
		want    string
		tripped bool
	}{
		{"one split: c past the limit at 220", one, 2, `{"metric":{"pod":"a"},"values":[[100,"1"],[160,"2"],[220,"3"]]},{"metric":{"pod":"b"},"values":[[160,"5"],[220,"6"]]}`, true},
		{"one split: b past the limit at 160", one, 1, `{"metric":{"pod":"a"},"values":[[100,"1"],[160,"2"]]}`, true},
		{"one split within the limit", one, 3, "", false},
		// Split 0 holds a (100) and b (160): within a limit of 2. Split 1 meets a, b and c at 220: past it there,
		// so a and b (by labels) are kept up to 220.
		{"two splits: only the second trips", split, 2, `{"metric":{"pod":"a"},"values":[[100,"1"],[160,"2"],[220,"3"]]},{"metric":{"pod":"b"},"values":[[160,"5"],[220,"6"]]}`, true},
		// A limit of 1: split 0 trips at 160 (keeps a), split 1 trips at 220 (keeps a).
		{"two splits, both trip", split, 1, `{"metric":{"pod":"a"},"values":[[100,"1"],[160,"2"],[220,"3"]]}`, true},
	} {
		got, tripped := cutLokiSplits([]byte(body), tc.scope, tc.limit)
		want := body
		if tc.want != "" {
			want = head + tc.want + tail
		}
		if string(got) != want || tripped != tc.tripped {
			t.Errorf("%s:\n got %v %s\nwant %v %s", tc.name, tripped, got, tc.tripped, want)
		}
	}
	vector := `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[100,"1"]}]}}`
	for _, in := range []string{vector, "not json", `{"data":1}`} {
		if got, tripped := cutLokiSplits([]byte(in), one, 1); string(got) != in || tripped {
			t.Errorf("%q changed: %s", in, got)
		}
	}
}

func jsonText(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// seriesKeptForLimit keeps the busiest limit series by default and, in the Loki mode, the limit+1 series met first in
// every split (a superset cutLokiSplits cuts on the finished answer); ties break on the key both ways.
//
// conformance: limits/drilldown-series-limit-range-cut-at-trip-step, limits/drilldown-breakdown-keeps-busiest
func TestSeriesKeptForLimit(t *testing.T) {
	keys := []string{"a", "b", "c", "d", "e"}
	totals := map[string]float64{"a": 1, "b": 9, "c": 5, "d": 9, "e": 2}
	// Sample times: a and b in the first split only, c and d in the second only, e in both.
	times := map[string][]int64{"a": {30}, "b": {10}, "c": {120, 130}, "d": {110}, "e": {20, 140}}
	total := func(k string) float64 { return totals[k] }
	each := func(k string, yield func(int64)) {
		for _, t := range times[k] {
			yield(t)
		}
	}
	busiest := &seriesLimitScope{drilldown: true}
	ctx := context.WithValue(context.Background(), seriesLimitScopeKey{}, busiest)
	if got := strings.Join(seriesKeptForLimit(ctx, keys, total, each, 2), ","); got != "b,d" {
		t.Errorf("busiest: %s", got)
	}
	one := &seriesLimitScope{drilldown: true, lokiCut: true, startNs: 0, endNs: 200, stepNs: 10}
	ctx = context.WithValue(context.Background(), seriesLimitScopeKey{}, one)
	if got := strings.Join(seriesKeptForLimit(ctx, keys, total, each, 2), ","); got != "a,b,e" {
		t.Errorf("loki, one split: the three met first (b 10, e 20, a 30), in the keys' order: %s", got)
	}
	split := &seriesLimitScope{drilldown: true, lokiCut: true, startNs: 0, endNs: 200, stepNs: 10, splitNs: 100}
	ctx = context.WithValue(context.Background(), seriesLimitScopeKey{}, split)
	if got := strings.Join(seriesKeptForLimit(ctx, keys, total, each, 1), ","); got != "b,c,d,e" {
		t.Errorf("loki, two splits: the two met first in each (b 10, e 20; d 110, c 120), in the keys' order: %s", got)
	}
	if got := seriesKeptForLimit(ctx, keys, total, each, 5); len(got) != 5 {
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
	p.drilldownSeriesLimitSplit = time.Hour
	if _, s := p.withSeriesLimitScope(drilldown("/loki/api/v1/query_range?query=x&start=100&end=7300&step=60")); !s.lokiCut || s.stepNs != int64(time.Minute) ||
		s.startNs != 100e9 || s.endNs != 7300e9 || s.splitNs != int64(time.Hour) {
		t.Errorf("default mode, range: %+v", s)
	}
	if _, err := New(Config{BackendURL: "http://127.0.0.1:1", DrilldownSeriesLimitSplitInterval: -time.Second}); err == nil {
		t.Fatal("a negative split interval must be refused")
	}
	if _, s := p.withSeriesLimitScope(drilldown("/loki/api/v1/query?query=x")); s.lokiCut {
		t.Error("an instant request keeps the busiest series")
	}
	if _, s := p.withSeriesLimitScope(httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?query=x&start=100&end=7300&step=60", nil)); s.lokiCut {
		t.Error("a request that is not Drilldown's is not cut")
	}
	p.drilldownSeriesLimitMode = drilldownSeriesLimitBusiest
	if _, s := p.withSeriesLimitScope(drilldown("/loki/api/v1/query_range?query=x&start=100&end=7300&step=60")); s.lokiCut {
		t.Error("busiest mode keeps the whole range")
	}
}

// FuzzCutLokiSplits: any body, limit and split terminate and never add a series or a point.
func FuzzCutLokiSplits(f *testing.F) {
	f.Add(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b"},"values":[[1,"1"],[61,"2"]]},{"metric":{"a":"c"},"values":[[61,"1"]]}]}}`, 1, int64(60e9))
	f.Add(`{"data":{"resultType":"vector","result":[]}}`, 1, int64(0))
	f.Add(`{"data":{"resultType":"matrix","result":[{"values":[["x","1"]]}]}}`, 2, int64(1))
	f.Fuzz(func(t *testing.T, body string, limit int, splitNs int64) {
		if len(body) > 4096 || splitNs < 0 {
			return
		}
		scope := &seriesLimitScope{startNs: 0, endNs: 200e9, stepNs: 60e9, splitNs: splitNs}
		out, _ := cutLokiSplits([]byte(body), scope, limit)
		count := func(b []byte) (series, points int, ok bool) {
			var r struct {
				Data struct {
					Result []struct {
						Values [][]json.RawMessage `json:"values"`
					} `json:"result"`
				} `json:"data"`
			}
			if json.Unmarshal(b, &r) != nil {
				return 0, 0, false
			}
			for _, s := range r.Data.Result {
				points += len(s.Values)
			}
			return len(r.Data.Result), points, true
		}
		gs, gp, okOut := count(out)
		is, ip, okIn := count([]byte(body))
		if okIn && okOut && (gs > is || gp > ip) {
			t.Fatalf("the cut added series or points: %s -> %s", body, out)
		}
	})
}

// cutLokiSplitsReference is the cut written plainly over decoded JSON: per split, the series by their first point
// in it (ties by labels), the first limit kept up to the (limit+1)th's first point, the rest dropped there. It
// returns the kept series (labels and points), nil when no split passes the limit.
func cutLokiSplitsReference(body []byte, scope *seriesLimitScope, limit int) []any {
	var resp struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric json.RawMessage `json:"metric"`
				Values [][]json.Number `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &resp) != nil || resp.Data.ResultType != "matrix" || limit <= 0 {
		return nil
	}
	ns := func(p []json.Number) int64 {
		sec, _ := strconv.ParseFloat(string(p[0]), 64)
		return int64(math.Round(sec*1e3)) * int64(time.Millisecond)
	}
	type met struct {
		first int64
		idx   int
	}
	bySplit := map[int][]met{}
	for i, s := range resp.Data.Result {
		firsts := map[int]int64{}
		for _, p := range s.Values {
			t := ns(p)
			if cur, ok := firsts[scope.splitOf(t)]; !ok || t < cur {
				firsts[scope.splitOf(t)] = t
			}
		}
		for split, first := range firsts {
			bySplit[split] = append(bySplit[split], met{first, i})
		}
	}
	until := map[[2]int]int64{}
	tripped := false
	for split, list := range bySplit {
		sort.Slice(list, func(a, b int) bool {
			if list[a].first != list[b].first {
				return list[a].first < list[b].first
			}
			return string(resp.Data.Result[list[a].idx].Metric) < string(resp.Data.Result[list[b].idx].Metric)
		})
		end := int64(math.MaxInt64)
		if len(list) > limit {
			end, list, tripped = list[limit].first, list[:limit], true
		}
		for _, m := range list {
			until[[2]int{m.idx, split}] = end
		}
	}
	if !tripped {
		return nil
	}
	var out []any
	for i, s := range resp.Data.Result {
		var kept []string
		for _, p := range s.Values {
			t := ns(p)
			if end, ok := until[[2]int{i, scope.splitOf(t)}]; ok && t <= end {
				kept = append(kept, string(p[0])+"="+string(p[1]))
			}
		}
		if kept != nil {
			out = append(out, string(s.Metric)+fmt.Sprint(kept))
		}
	}
	return out
}

// randomSplitMatrix is a matrix answer of random series over a random number of splits, as the proxy writes one
// (whole and fractional second times, ascending points, a warning first), with its scope and a limit.
func randomSplitMatrix(rng *rand.Rand) ([]byte, *seriesLimitScope, int) {
	step := int64(1+rng.Intn(120)) * int64(time.Second) / int64(1+rng.Intn(4))
	step -= step % int64(time.Millisecond)
	start := int64(1700000000+rng.Intn(1000))*int64(time.Second) + int64(rng.Intn(4))*250*int64(time.Millisecond)
	steps := 1 + rng.Intn(80)
	split := int64(0)
	if rng.Intn(3) > 0 {
		split = step * int64(1+rng.Intn(20))
	}
	scope := &seriesLimitScope{startNs: start, endNs: start + int64(steps)*step, stepNs: step, splitNs: split}
	var sb strings.Builder
	sb.WriteString(`{"warnings":["w"],"status":"success","data":{"resultType":"matrix","result":[`)
	series := rng.Intn(9)
	for s := 0; s < series; s++ {
		if s > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(`{"metric":{"pod":"p` + strconv.Itoa(s) + `"},"values":[`)
		n := 0
		for k := rng.Intn(steps); k <= steps; k += 1 + rng.Intn(4) {
			if n > 0 {
				sb.WriteByte(',')
			}
			at := start + int64(k)*step
			sb.WriteString(`[` + strconv.FormatFloat(float64(at)/1e9, 'f', -1, 64) + `,"` + strconv.Itoa(rng.Intn(9)) + `"]`)
			n++
		}
		sb.WriteString(`]}`)
	}
	sb.WriteString(`],"stats":{}}}`)
	return []byte(sb.String()), scope, 1 + rng.Intn(4)
}

// The in-place cut keeps exactly the series and points the plain cut over decoded JSON keeps, over random answers,
// limits and splits, and leaves the bytes around data.result as they came.
//
// conformance: limits/drilldown-series-limit-range-cut-at-trip-step
func TestCutLokiSplitsMatchesReference(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	trips := 0
	for i := 0; i < 5000; i++ {
		body, scope, limit := randomSplitMatrix(rng)
		if checkCutMatchesReference(t, body, scope, limit) {
			trips++
		}
	}
	if trips < 1000 {
		t.Fatalf("only %d answers were cut: the run proves little", trips)
	}
}

func checkCutMatchesReference(t *testing.T, body []byte, scope *seriesLimitScope, limit int) bool {
	t.Helper()
	out, tripped := cutLokiSplits(body, scope, limit)
	want := cutLokiSplitsReference(body, scope, limit)
	if !tripped {
		if want != nil || string(out) != string(body) {
			t.Fatalf("no split passes the limit, the reference cut %v / the body changed: %s", want, out)
		}
		return false
	}
	whole := &seriesLimitScope{startNs: scope.startNs, endNs: scope.endNs, stepNs: scope.stepNs}
	got := cutLokiSplitsReference(out, whole, math.MaxInt32) // nil: within the limit; read the output whole
	if got == nil {
		got = keptWhole(t, out)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("limit %d split %d:\n in %s\nout %s\ngot  %v\nwant %v", limit, scope.splitNs, body, out, got, want)
	}
	if !strings.HasPrefix(string(out), `{"warnings":["w"],"status":"success","data":{"resultType":"matrix","result":[`) || !strings.HasSuffix(string(out), `],"stats":{}}}`) {
		t.Fatalf("the bytes around data.result changed: %s", out)
	}
	return true
}

// keptWhole lists a matrix answer's series as cutLokiSplitsReference does.
func keptWhole(t *testing.T, body []byte) []any {
	t.Helper()
	var resp struct {
		Data struct {
			Result []struct {
				Metric json.RawMessage `json:"metric"`
				Values [][]json.Number `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	var out []any
	for _, s := range resp.Data.Result {
		var kept []string
		for _, p := range s.Values {
			kept = append(kept, string(p[0])+"="+string(p[1]))
		}
		out = append(out, string(s.Metric)+fmt.Sprint(kept))
	}
	return out
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

// FuzzCutLokiSplitsMatchesReference is TestCutLokiSplitsMatchesReference over fuzzed answers.
func FuzzCutLokiSplitsMatchesReference(f *testing.F) {
	f.Add(int64(1))
	f.Add(int64(42))
	f.Fuzz(func(t *testing.T, seed int64) {
		body, scope, limit := randomSplitMatrix(rand.New(rand.NewSource(seed)))
		checkCutMatchesReference(t, body, scope, limit)
	})
}

// BenchmarkCutLokiSplits is what a Drilldown answer over the limit pays in the Loki mode: 1000 series of 60 points
// (one new series every 3.6 s of the hour), at a limit of 500, as one split and over hours of 15 minutes.
func BenchmarkCutLokiSplits(b *testing.B) {
	var sb strings.Builder
	sb.WriteString(`{"status":"success","warnings":["w"],"data":{"resultType":"matrix","result":[`)
	for s := 0; s < 1000; s++ {
		if s > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(`{"metric":{"pod":"p` + strconv.Itoa(s) + `"},"values":[`)
		for i := 0; i < 60; i++ {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(`[` + jsonText(1800000000+(s*36/600+i)*60) + `,"1"]`)
		}
		sb.WriteString(`]}`)
	}
	sb.WriteString(`]}}`)
	body := []byte(sb.String())
	for _, split := range []time.Duration{0, 15 * time.Minute} {
		b.Run("split="+split.String(), func(b *testing.B) {
			scope := &seriesLimitScope{startNs: 1800000000e9, endNs: 1800000000e9 + int64(2*time.Hour), stepNs: int64(time.Minute), splitNs: int64(split)}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = cutLokiSplits(body, scope, 500)
			}
		})
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
		if got, ok := rankedSingleFieldQueryFirstMet(tc.query, 10, nil); !ok || got != tc.want {
			t.Errorf("rankedSingleFieldQueryFirstMet(%q)\n got %v %q\nwant %q", tc.query, ok, got, tc.want)
		}
	}
	if got := (*seriesLimitScope)(nil).firstMetRanking("pod", "_c", false, 4, 0); got != ` | stats by (pod) min(_time) as _c | sort by (_c) | limit 4` {
		t.Errorf("top-value ranking: %q", got)
	}
}
