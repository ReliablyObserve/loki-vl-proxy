package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// The ranked reads of the Loki series-limit mode (firstMetRanking): a Drilldown breakdown over the limit asks
// VictoriaLogs for the values met first in every bucket of the request's ranking grid (rankingGrid) and reads only
// their series; cutLokiSplits then cuts the answer per split. These tests hold the ranked answer to Loki's: the
// series every split keeps, its trip step and its points.

// rankingEnd is an hour boundary in the past (VictoriaLogs' query server stores no future rows): the data of the
// ranking tests lies in the hours before it.
const rankingEnd = int64(1_779_998_400)

// rankingCase is one Drilldown request over its own rows: pods logging in bursts, some near an hour boundary, and
// sometimes lines without the pod label (the series {} Loki counts like any other).
type rankingCase struct {
	app                      string
	rows                     []vlemu.Row
	step, rangeS, start, end int64 // seconds
	// bodyBound, when set, is the stats body bound (maxStatsBucketResponseBytes) of the request: a full read over
	// it fails, so a range longer than the step takes the ranked top-value read.
	bodyBound int64
}

func (c rankingCase) query() string {
	return fmt.Sprintf(`sum by (pod) (count_over_time({app=%q}[%ds]))`, c.app, c.rangeS)
}

func (c rankingCase) form() string {
	return url.Values{"query": {c.query()}, "start": {strconv.FormatInt(c.start, 10)}, "end": {strconv.FormatInt(c.end, 10)}, "step": {strconv.FormatInt(c.step, 10)}}.Encode()
}

// randomRankingCase draws a case. topValue makes the range longer (or shorter) than the step and the stats body bound
// small, so the request takes the ranked top-value read; otherwise the range is the step (the ranked in() read).
func randomRankingCase(rng *rand.Rand, i int, topValue bool) rankingCase {
	c := rankingCase{app: "rk" + strconv.Itoa(i)}
	base := (rankingEnd - 4*3600) * int64(time.Second)
	pods := 4 + rng.Intn(8)
	if topValue {
		pods = 60 + rng.Intn(100)
	}
	for p := 0; p < pods; p++ {
		stream := fmt.Sprintf(`{app=%q,pod="p%03d"}`, c.app, p)
		for b := 0; b < 1+rng.Intn(4); b++ {
			from := base + rng.Int63n(int64(4*time.Hour))
			if rng.Intn(2) == 0 { // a burst that begins near an hour boundary
				from = base + int64(1+rng.Intn(3))*int64(time.Hour) - rng.Int63n(int64(3*time.Minute)) + rng.Int63n(int64(time.Minute))
			}
			gap := int64(3+rng.Intn(40)) * int64(time.Second)
			for at, n := from, 0; n < 1+rng.Intn(30); at, n = at+gap, n+1 {
				c.rows = append(c.rows, vlemu.Row{TS: at, Stream: stream, Value: 1, Msg: "ranking line"})
			}
		}
	}
	if rng.Intn(3) == 0 {
		for k := 0; k < 1+rng.Intn(20); k++ {
			c.rows = append(c.rows, vlemu.Row{TS: base + rng.Int63n(int64(4*time.Hour)), Stream: fmt.Sprintf(`{app=%q}`, c.app), Value: 1, Msg: "ranking line"})
		}
	}
	sort.Slice(c.rows, func(a, b int) bool { return c.rows[a].TS < c.rows[b].TS })
	steps := []int64{5, 7, 15, 30, 60, 120}
	c.step = steps[rng.Intn(len(steps))]
	c.rangeS = c.step // Drilldown's [$__auto]
	if topValue {
		c.rangeS = c.step * int64(2+rng.Intn(4))
		if rng.Intn(4) == 0 { // a range shorter than the step: only its windows' lines are read
			c.rangeS = max(1, c.step/2)
		}
		c.bodyBound = int64(math.Exp(math.Log(2000) + rng.Float64()*math.Log(500)))
	}
	// Any second into an hour: the first step of a split lies anywhere within a step after its boundary.
	c.start = rankingEnd - 4*3600 + 600 + int64(rng.Intn(3600))
	c.end = c.start + 3600 + int64(rng.Intn(2*3600))
	return c
}

// rankingBackend is a VictoriaLogs for the ranking tests and the queries it was sent.
type rankingBackend struct {
	name string
	url  func(c rankingCase) string
	mu   sync.Mutex
	sent []string
	// lastBodySize is the size of the last stats_query_range answer without a ranking.
	lastBodySize int64
}

func (b *rankingBackend) record(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		form, _ := url.ParseQuery(string(body))
		q := form.Get("query")
		if q == "" {
			q = r.URL.Query().Get("query")
		}
		if q != "" {
			b.mu.Lock()
			b.sent = append(b.sent, r.URL.Path+" "+q)
			b.mu.Unlock()
		}
		// The answer is recorded before the client reads any of it.
		rec := httptest.NewRecorder()
		next.ServeHTTP(rec, r)
		if r.URL.Path == "/select/logsql/stats_query_range" && !strings.Contains(q, ":in(") {
			b.mu.Lock()
			b.lastBodySize = int64(rec.Body.Len())
			b.mu.Unlock()
		}
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
	})
}

// wholeReadSize is the size of the last stats_query_range answer without a ranking.
func (b *rankingBackend) wholeReadSize() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastBodySize
}

func (b *rankingBackend) take() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.sent
	b.sent = nil
	return out
}

// rankingBackends are the emulator at v1.49.0 and v1.54.0 and, with LVP_RUN_ORACLES=1, VictoriaLogs v1.49.0 and
// v1.54.0 themselves (their own query handlers through the serve oracle, holding every case).
func rankingBackends(t *testing.T, cases []rankingCase) []*rankingBackend {
	t.Helper()
	var out []*rankingBackend
	for _, version := range []string{"v1.49.0", "v1.54.0"} {
		b := &rankingBackend{name: "emulator " + version}
		servers := map[string]*httptest.Server{}
		b.url = func(c rankingCase) string {
			if s, ok := servers[c.app]; ok {
				return s.URL
			}
			emu := &vlemu.Emulator{T: t, Rows: c.rows, Field: "amount", Version: version, AlignStatsRange: true}
			s := httptest.NewServer(b.record(emu.Handler()))
			t.Cleanup(s.Close)
			servers[c.app] = s
			return s.URL
		}
		out = append(out, b)
	}
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		return out
	}
	var set oraclerun.ServeDataSet
	for _, c := range cases {
		for _, r := range c.rows {
			var stream [][2]string
			for _, l := range vlemu.StreamLabelRE.FindAllStringSubmatch(r.Stream, -1) {
				stream = append(stream, [2]string{l[1], l[2]})
			}
			set.Rows = append(set.Rows, oraclerun.ServeRow{TS: r.TS, Stream: stream, Fields: [][2]string{{"_msg", r.Msg}}})
		}
	}
	for _, v := range []struct{ version, modfile string }{{"v1.49.0", "go.v1.49.0.mod"}, {"v1.54.0", ""}} {
		srv, err := oraclerun.StartVLServe(filepath.Join("..", "..", "test", "oracle", "vl"), v.modfile, []oraclerun.ServeDataSet{set})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(srv.Stop)
		b := &rankingBackend{name: "VictoriaLogs " + v.version}
		front := httptest.NewServer(b.record(levelSweepVLFront(v.version, srv.URL)))
		t.Cleanup(front.Close)
		b.url = func(rankingCase) string { return front.URL }
		out = append(out, b)
	}
	return out
}

// askRanked sends c's request to a fresh proxy (Loki mode, split interval 1h, limit 3) over backend and returns the
// answer and the queries it sent.
func askRanked(t *testing.T, b *rankingBackend, c rankingCase) (*httptest.ResponseRecorder, []string) {
	t.Helper()
	if c.bodyBound > 0 {
		old := maxStatsBucketResponseBytes
		maxStatsBucketResponseBytes = c.bodyBound
		defer func() { maxStatsBucketResponseBytes = old }()
	}
	p, err := New(Config{BackendURL: b.url(c), Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0",
		BackendVersionCheckTimeout: time.Second, MaxStatsQuerySeries: drilldownSeriesLimit, DrilldownSeriesLimitSplitInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	b.take()
	res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+c.form(), map[string]string{"X-Scope-OrgID": "0", "X-Query-Tags": drilldownSeriesLimitTags})
	return res, b.take()
}

func sentRanked(sent []string) bool {
	for _, q := range sent {
		if strings.Contains(q, ":in(") && strings.Contains(q, " min(_time) as __lvp_rank") {
			return true
		}
	}
	return false
}

// Over random data, steps that divide the split interval and steps that do not, starts anywhere in an hour, ranges
// equal to and longer than the step and lines without the label, a Drilldown request answered from a ranked read
// keeps, in every split, what Loki's cumulative rule keeps over all the data: the series met before the split's trip
// step, as many met at it as fill the limit (Loki's order among those is not fixed), and their points up to the trip
// step. The ranked in() read (range == step) and the ranked top-value read (a range longer than the step whose
// whole answer passes the stats body bound) both run, on the emulator at v1.49.0 and v1.54.0 and, with
// LVP_RUN_ORACLES=1, on VictoriaLogs v1.49.0 and v1.54.0. A ranking per split of the lines from a range before each
// boundary (the grid before rankingGrid) fails it whenever the step grid does not start on the boundary.
//
// conformance: limits/drilldown-series-limit-range-cut-at-trip-step, limits/drilldown-partial-with-warning
// conformance: loki_api_v1_query_range
func TestDrilldownSeriesLimitRankedReadsKeepLokisSeries(t *testing.T) {
	n := 300
	if testing.Short() && os.Getenv("LVP_RUN_ORACLES") != "1" {
		n = 120
	}
	rng := rand.New(rand.NewSource(1))
	var cases []rankingCase
	for i := 0; i < n; i++ {
		cases = append(cases, randomRankingCase(rng, i, i%2 == 1))
	}
	for _, b := range rankingBackends(t, cases) {
		checked := map[string]int{}
		for _, c := range cases {
			res, sent := askRanked(t, b, c)
			path := "in"
			if c.bodyBound > 0 {
				path = "top-value"
			}
			if res.Code != http.StatusOK {
				if c.bodyBound > 0 {
					continue // the ranked read passed the bound too (or the whole answer fitted it): the bound decides
				}
				t.Fatalf("%s %s: proxy answered %d %s", b.name, c.query(), res.Code, res.Body.String())
			}
			if !sentRanked(sent) {
				continue
			}
			checked[path]++
			scope := &seriesLimitScope{startNs: c.start * 1e9, endNs: c.end * 1e9, stepNs: c.step * 1e9, splitNs: int64(time.Hour), rangeNs: c.rangeS * 1e9}
			if msg := lokiRuleViolations(c.rows, scope, drilldownSeriesLimit, res.Body.Bytes()); msg != "" {
				t.Errorf("%s: step %ds range %ds start %d (%ds into its hour) end %d: %s\nsent:\n%s", b.name, c.step, c.rangeS, c.start, c.start%3600, c.end, msg, strings.Join(sent, "\n"))
			}
		}
		if checked["in"] < n/4 || checked["top-value"] < n/20 {
			t.Fatalf("%s: only %v answers came from a ranked read: the run proves little", b.name, checked)
		}
		t.Logf("%s: ranked answers checked %v", b.name, checked)
	}
}

// lokiRuleViolations lists where a range answer differs from what Loki's query frontend and engine keep over rows
// (count_over_time grouped by pod): in every split, the series in the order they are first met, cut at the step
// where they pass the limit; the series met at that step are interchangeable (Loki's order among them is not fixed).
func lokiRuleViolations(rows []vlemu.Row, s *seriesLimitScope, limit int, body []byte) string {
	full := map[string]map[int64]int{}
	for at := s.startNs; at <= s.endNs; at += s.stepNs {
		for _, r := range rows {
			if r.TS > at-s.rangeNs && r.TS <= at {
				pod := ""
				if i := strings.Index(r.Stream, `pod="`); i >= 0 {
					pod = r.Stream[i+5 : len(r.Stream)-2]
				}
				if full[pod] == nil {
					full[pod] = map[int64]int{}
				}
				full[pod][at]++
			}
		}
	}
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]json.Number   `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return err.Error()
	}
	got := map[string]map[int64]int{}
	for _, r := range resp.Data.Result {
		points := map[int64]int{}
		for _, p := range r.Values {
			sec, _ := strconv.ParseFloat(string(p[0]), 64)
			v, _ := strconv.Atoi(string(p[1]))
			points[int64(math.Round(sec*1e3))*1e6] = v
		}
		got[r.Metric["pod"]] = points
	}
	var problems []string
	for k := 0; k <= s.splitOf(s.endNs); k++ {
		type met struct {
			pod   string
			first int64
		}
		var order []met
		for pod, points := range full {
			first := int64(-1)
			for at := range points {
				if s.splitOf(at) == k && (first < 0 || at < first) {
					first = at
				}
			}
			if first >= 0 {
				order = append(order, met{pod, first})
			}
		}
		sort.Slice(order, func(a, b int) bool { return order[a].first < order[b].first })
		trip := int64(math.MaxInt64)
		if len(order) > limit {
			trip = order[limit].first
		}
		kept := 0
		for _, m := range order {
			inSplit := false
			for at := range got[m.pod] {
				if s.splitOf(at) == k {
					inSplit = true
				}
			}
			switch {
			case m.first < trip && !inSplit:
				problems = append(problems, fmt.Sprintf("split %d: {pod=%q} met at %+ds before the trip is missing", k, m.pod, (m.first-s.startNs)/1e9))
			case m.first > trip && inSplit:
				problems = append(problems, fmt.Sprintf("split %d: {pod=%q} met at %+ds after the trip is kept", k, m.pod, (m.first-s.startNs)/1e9))
			}
			if !inSplit {
				continue
			}
			kept++
			for at, v := range full[m.pod] {
				if s.splitOf(at) == k && at <= trip && got[m.pod][at] != v {
					problems = append(problems, fmt.Sprintf("split %d: {pod=%q} at %+ds is %d, want %d", k, m.pod, (at-s.startNs)/1e9, got[m.pod][at], v))
				}
			}
			for at := range got[m.pod] {
				if s.splitOf(at) == k && (at > trip || full[m.pod][at] == 0) {
					problems = append(problems, fmt.Sprintf("split %d: {pod=%q} has a point at %+ds", k, m.pod, (at-s.startNs)/1e9))
				}
			}
		}
		if len(order) > limit && kept != limit {
			problems = append(problems, fmt.Sprintf("split %d: %d series kept, want the limit", k, kept))
		}
		for pod, points := range got {
			if _, ok := full[pod]; ok {
				continue
			}
			for at := range points {
				if s.splitOf(at) == k {
					problems = append(problems, fmt.Sprintf("split %d: {pod=%q} has no line but a point at %+ds", k, pod, (at-s.startNs)/1e9))
				}
			}
		}
	}
	return strings.Join(problems, "; ")
}

// rankingCornerCase is a data set where the split ranking of lines from a range before each boundary leaves out
// what Loki keeps: the request's steps lie 25 s past each hour, so four pods whose only lines lie in the last range
// before an hour but before that split's first step reads it (in split 0's last steps, after its trip) rank first in
// the second split's bucket and push out pod h, met at that split's first step only through a line in the range
// before the hour, and the pods l1-l3 met after it. rangeS is the range (the step is 60 s); noise adds 30 pods to
// the third split, one new every other step, so a read of the whole answer passes a body bound the ranked read
// fits.
func rankingCornerCase(app string, rangeS int64, noise bool) rankingCase {
	h := (rankingEnd - 3*3600) * int64(time.Second)
	b1, b2 := h+int64(time.Hour), h+2*int64(time.Hour)
	c := rankingCase{app: app, step: 60, rangeS: rangeS, start: h/1e9 + 25, end: h/1e9 + 10225}
	add := func(pod string, from, to, every int64) {
		for at := from; at <= to; at += every {
			c.rows = append(c.rows, vlemu.Row{TS: at, Stream: fmt.Sprintf(`{app=%q,pod=%q}`, app, pod), Value: 1, Msg: "corner line"})
		}
	}
	sec := int64(time.Second)
	for i := int64(1); i <= 4; i++ { // split 0 trips at e4, met at its own step
		add("e"+strconv.FormatInt(i, 10), h+600*i*sec-5*sec, b1-600*sec, 7*sec)
	}
	for i, off := range []int64{-2, 4, 10, 16} { // read only by split 0's last steps
		at := b1 - rangeS*sec + off*sec + 2*sec
		add("c"+strconv.Itoa(i+1), at, at, sec)
	}
	add("h", b1-rangeS*sec/3, b1-rangeS*sec/3, sec) // read by the second split's first step only
	add("h", b1+600*sec, b1+1800*sec, 7*sec)
	for i, from := range []int64{900, 1500, 2100} { // the second split trips at l3
		add("l"+strconv.Itoa(i+1), b1+from*sec, b1+(from+900)*sec, 7*sec)
	}
	if noise {
		for j := int64(0); j < 30; j++ {
			add(fmt.Sprintf("n%02d", j), b2+(120*j+30)*sec, b2+(120*j+1230)*sec, 7*sec)
		}
	} else {
		add("m1", b2+300*sec, b2+2400*sec, 7*sec)
		add("m2", b2+400*sec, b2+2400*sec, 7*sec)
	}
	sort.Slice(c.rows, func(a, b int) bool { return c.rows[a].TS < c.rows[b].TS })
	return c
}

// Loki's answers per split (each split of the query frontend's grid asked alone, Loki 3.7.10's engine), concatenated,
// equal the proxy's answer from its ranked reads where the ranking per split of the lines from a range before each
// boundary leaves out what Loki keeps (rankingCornerCase): range == step through the ranked in() read, a range
// longer than the step through the ranked top-value read (its whole answer over the stats body bound), on the
// emulator at v1.49.0 and v1.54.0 and, with LVP_RUN_ORACLES=1, on VictoriaLogs v1.49.0 and v1.54.0.
//
// conformance: limits/drilldown-series-limit-range-cut-at-trip-step, limits/drilldown-partial-with-warning
// conformance: loki_api_v1_query_range
func TestDrilldownSeriesLimitRankingCornerMatchesLoki(t *testing.T) {
	cases := []rankingCase{rankingCornerCase("corner-in", 60, false), rankingCornerCase("corner-topvalue", 300, true)}
	pieces := func(c rankingCase) [][2]int64 { // Loki's splits on the request's grid: a step on a boundary starts the later one
		var out [][2]int64
		from := c.start
		for b := c.start - c.start%3600 + 3600; b < c.end; b += 3600 {
			first := c.start + (b-c.start+c.step-1)/c.step*c.step
			out = append(out, [2]int64{from, first - c.step})
			from = first
		}
		return append(out, [2]int64{from, c.end})
	}
	var sets []oraclerun.EngineDataSet
	for _, c := range cases {
		set := oraclerun.EngineDataSet{MaxSeries: drilldownSeriesLimit, Streams: engineStreams(c.rows, func(row vlemu.Row) (string, map[string]string) { return row.Msg, nil })}
		for i, p := range pieces(c) {
			piece := c
			piece.start, piece.end = p[0], p[1]
			set.Requests = append(set.Requests, oraclerun.EngineRequest{ID: fmt.Sprintf("%s-split%d", c.app, i), Unaligned: true, QueryTags: drilldownSeriesLimitTags, Form: piece.form()})
		}
		sets = append(sets, set)
	}
	loki := lokiEngineAnswers(t, "drilldown_series_limit_ranking_corner", sets)
	for _, b := range rankingBackends(t, cases) {
		for _, c := range cases {
			want := lokiSeries{Points: map[string]map[int64]string{}}
			for i := range pieces(c) {
				a := loki[fmt.Sprintf("%s-split%d", c.app, i)]
				if a.Status != http.StatusOK {
					t.Fatalf("%s split %d: Loki answered %d %s", c.app, i, a.Status, a.Error)
				}
				piece := decodeDrilldownSeriesLimitAnswer(t, "Loki", a.Body)
				if i < 2 && len(piece.warnings) == 0 {
					t.Fatalf("%s: Loki's split %d is within the limit: the data proves little", c.app, i)
				}
				for key, points := range piece.series.Points {
					if want.Points[key] == nil {
						want.Points[key] = map[int64]string{}
					}
					for ms, v := range points {
						want.Points[key][ms] = v
					}
				}
			}
			if c.bodyBound = 0; c.rangeS > c.step {
				// The whole answer over the bound, the ranked read within it.
				c.bodyBound = 1 << 40
				if res, sent := askRanked(t, b, c); res.Code != http.StatusOK || sentRanked(sent) {
					t.Fatalf("%s %s: the whole read answered %d, ranked %v", b.name, c.app, res.Code, sentRanked(sent))
				}
				if c.bodyBound = b.wholeReadSize() - 1; c.bodyBound <= 0 {
					t.Fatalf("%s %s: no whole read was recorded", b.name, c.app)
				}
			}
			res, sent := askRanked(t, b, c)
			if res.Code != http.StatusOK || !sentRanked(sent) {
				t.Fatalf("%s %s: proxy answered %d without a ranked read (%v): %s", b.name, c.app, res.Code, sent, res.Body.String())
			}
			got := decodeDrilldownSeriesLimitAnswer(t, "proxy", res.Body.Bytes())
			if len(got.warnings) != 1 {
				t.Errorf("%s %s: warnings %v, want Loki's", b.name, c.app, got.warnings)
			}
			if len(got.series.Points) != len(want.Points) {
				t.Errorf("%s %s: proxy %d series, Loki's splits %d", b.name, c.app, len(got.series.Points), len(want.Points))
			}
			for key, wp := range want.Points {
				if gp := got.series.Points[key]; fmt.Sprint(gp) != fmt.Sprint(wp) {
					t.Errorf("%s %s: %s\n proxy %v\n  Loki %v", b.name, c.app, key, gp, wp)
				}
			}
		}
	}
}

// The ranking grid has a bucket edge at every split's first step less the lookback (VictoriaLogs' buckets begin one
// nanosecond after it: t+offset is a multiple of the width), one bucket per split when the split interval is a
// multiple of the step, and its bucket count is the number of buckets the read (start-lookback, end] reaches, over
// random starts, ends, steps (dividing the split interval and not), lookbacks below and above the split interval and
// negative times.
//
// conformance: limits/drilldown-series-limit-range-cut-at-trip-step
func TestRankingGrid(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	sec := int64(time.Second)
	steps := []int64{1, 5, 7, 15, 30, 60, 120, 900, 3600, 5400, 7200}
	for i := 0; i < 20000; i++ {
		split := []int64{900, 1800, 3600}[rng.Intn(3)] * sec
		step := steps[rng.Intn(len(steps))] * sec
		start := (int64(rng.Intn(2*86400)) - 86400) * sec
		if rng.Intn(4) == 0 {
			start += int64(rng.Intn(1000)) * int64(time.Millisecond)
		}
		end := start + int64(rng.Intn(4*86400/int(max(1, step/sec/30))+1))*sec
		lookback := max(step, int64(rng.Intn(3))*step+int64(rng.Intn(7200))*sec*int64(rng.Intn(2)))
		s := &seriesLimitScope{startNs: start, endNs: end, stepNs: step, splitNs: split}
		width, offset, buckets, ok := s.rankingGrid(lookback)
		if ok != (s.splitOf(end) > 0) {
			t.Fatalf("start %d end %d split %d: ok %v with %d splits", start, end, split, ok, s.splitOf(end)+1)
		}
		if !ok {
			continue
		}
		if split%step == 0 && width != split || split%step != 0 && width != step || offset < 0 || offset >= width {
			t.Fatalf("step %d split %d: width %d offset %d", step, split, width, offset)
		}
		bucketOf := func(ts int64) int64 { return floorDiv(ts+offset, width) }
		var firsts []int64
		s.firstStepsInSplits(start, start+stepCount(start, end, step)*step-step, func(first int64) { firsts = append(firsts, first) })
		for _, first := range firsts[1:] {
			if edge := first - lookback; bucketOf(edge) == bucketOf(edge+1) {
				t.Fatalf("start %d step %d split %d lookback %d: no bucket begins after %d (a split's first step %d)", start, step, split, lookback, edge, first)
			}
		}
		if want := bucketOf(end) - bucketOf(start-lookback+1) + 1; int64(buckets) != want {
			t.Fatalf("start %d end %d step %d split %d lookback %d: %d buckets, the read reaches %d", start, end, step, split, lookback, buckets, want)
		}
		if split%step == 0 && int64(buckets) > int64(s.splitOf(end))+1+lookback/split+1 {
			t.Fatalf("step %d split %d lookback %d: %d buckets over %d splits", step, split, lookback, buckets, s.splitOf(end)+1)
		}
	}
}
