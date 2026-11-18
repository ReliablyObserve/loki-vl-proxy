package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// A Logs Drilldown metric query over the series limit gets a partial result and Loki's warning, not the error a
// plain client gets (registry limits/drilldown-partial-with-warning). Loki 3.7.10 (grafana/loki#26433) keeps
// exactly max_query_series series, every one labelled: 3.7.9 added a series without labels for every further new
// series after the first one over the limit. These checks ask the proxy, with -max-stats-query-series equal to the
// tenant's max_query_series, for Drilldown requests at the limit and one, two and five series over it, range and
// instant, and compare each answer with Loki's recorded answer to the same request (the Loki engine oracle with the
// request's X-Query-Tags in the query context; LVP_RUN_ORACLES=1 records again).
//
// In the default -drilldown-series-limit-mode=loki the proxy answers as Loki does (registry
// limits/drilldown-series-limit-range-cut-at-trip-step): a range answer keeps the series met first and ends at the
// step where the first series past the limit appears (Loki stops evaluating there), the same points as Loki's. Loki
// orders the series new in one step at random, so which of those it keeps may differ; where every series is new at a
// different step (staggered) the kept set is Loki's. In -drilldown-series-limit-mode=busiest the proxy keeps the
// busiest series over the whole range (registry limits/drilldown-breakdown-keeps-busiest): every point Loki answers
// for a series both keep is the proxy's. Either way the test pins Loki's cut to the trip step, so a Loki that changes
// it fails here.

const (
	drilldownSeriesLimit     = 3
	drilldownSeriesLimitEnd  = int64(1_800_000_000) // unix s, a multiple of every step below
	drilldownSeriesLimitTags = "Source=grafana-lokiexplore-app"
)

// drilldownSeriesLimitRows is a data set of pods streams over the last hour: pod i logs a line every (pods-i)*7 s,
// so the pods are ranked by volume and the busiest is the last. Staggered, pod i starts i*5 minutes into the hour
// (the busiest start last, one new series per step); otherwise every pod starts at once, so the first step meets
// every series, and every series past the limit is new in the same step (the case grafana/loki#26433 fixed).
func drilldownSeriesLimitRows(pods int, staggered bool) []vlemu.Row {
	var rows []vlemu.Row
	end := drilldownSeriesLimitEnd * 1e9
	for i := 0; i < pods; i++ {
		stream := fmt.Sprintf(`{app="sl",pod="p%02d"}`, i)
		every := int64(time.Duration(pods-i) * 7 * time.Second)
		start := end - int64(time.Hour) + 3e8
		if staggered {
			start += int64(i) * int64(5*time.Minute)
		}
		for at := start; at < end; at += every {
			rows = append(rows, vlemu.Row{TS: at, Stream: stream, Value: 1, Msg: "series limit line"})
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].TS < rows[j].TS })
	return rows
}

// drilldownSeriesLimitQueries are the request shapes: a breakdown by a stream label (Drilldown's labels tab), the raw
// per-stream series, and a breakdown by the stream label of a rate.
var drilldownSeriesLimitQueries = []string{
	`sum by (pod) (count_over_time({app="sl"}[1m]))`,
	`count_over_time({app="sl"}[1m])`,
	`sum by (pod) (rate({app="sl"}[5m]))`,
}

// drilldownSeriesLimitCase is one request: over pods streams, a range (step 60 s over the last hour) or an instant
// request at the end.
type drilldownSeriesLimitCase struct {
	id        string
	pods      int
	staggered bool
	query     string
	instant   bool
}

// dataSet names the case's data set.
func (c drilldownSeriesLimitCase) dataSet() string { return fmt.Sprintf("%d-%v", c.pods, c.staggered) }

func (c drilldownSeriesLimitCase) form() url.Values {
	if c.instant {
		return url.Values{"query": {c.query}, "time": {strconv.FormatInt(drilldownSeriesLimitEnd, 10)}}
	}
	return url.Values{"query": {c.query}, "start": {strconv.FormatInt(drilldownSeriesLimitEnd-3600, 10)},
		"end": {strconv.FormatInt(drilldownSeriesLimitEnd, 10)}, "step": {"60"}}
}

func drilldownSeriesLimitCases() []drilldownSeriesLimitCase {
	var out []drilldownSeriesLimitCase
	for _, staggered := range []bool{true, false} {
		for _, pods := range []int{drilldownSeriesLimit, drilldownSeriesLimit + 1, drilldownSeriesLimit + 2, drilldownSeriesLimit + 5} {
			for qi, q := range drilldownSeriesLimitQueries {
				for _, instant := range []bool{false, true} {
					kind := "range"
					if instant {
						kind = "instant"
					}
					start := "together"
					if staggered {
						start = "staggered"
					}
					out = append(out, drilldownSeriesLimitCase{id: fmt.Sprintf("%s-pods%d-q%d-%s", start, pods, qi, kind), pods: pods, staggered: staggered, query: q, instant: instant})
				}
			}
		}
	}
	return out
}

// drilldownSeriesLimitRange is the range of the query's range function (every query reads one).
func drilldownSeriesLimitRange(query string) int64 {
	if strings.Contains(query, "[5m]") {
		return 300
	}
	return 60
}

// drilldownSeriesLimitTripStep is the first step (unix s) of a range request at which the series met so far
// outnumber the limit: a pod is a series at step t when it has a line in (t-range, t]. 0 when no step does.
func drilldownSeriesLimitTripStep(c drilldownSeriesLimitCase) int64 {
	rows := drilldownSeriesLimitRows(c.pods, c.staggered)
	window := drilldownSeriesLimitRange(c.query) * 1e9
	seen := map[string]bool{}
	for t := drilldownSeriesLimitEnd - 3600; t <= drilldownSeriesLimitEnd; t += 60 {
		for _, r := range rows {
			if r.TS > t*1e9-window && r.TS <= t*1e9 {
				seen[r.Stream] = true
			}
		}
		if len(seen) > drilldownSeriesLimit {
			return t
		}
	}
	return 0
}

// drilldownSeriesLimitAnswer is a decoded answer with its warnings.
type drilldownSeriesLimitAnswer struct {
	warnings []string
	series   lokiSeries
	// unlabelled counts the series with no labels.
	unlabelled int
}

func decodeDrilldownSeriesLimitAnswer(t *testing.T, label string, body []byte) drilldownSeriesLimitAnswer {
	t.Helper()
	var head struct {
		Warnings []string `json:"warnings"`
		Data     struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &head); err != nil {
		t.Fatalf("%s: %v: %s", label, err, body)
	}
	a := drilldownSeriesLimitAnswer{warnings: head.Warnings}
	for _, s := range head.Data.Result {
		if len(s.Metric) == 0 {
			a.unlabelled++
		}
	}
	if a.unlabelled > 1 {
		// decodeLokiSeries refuses a label set twice: count them here, read the rest below.
		return a
	}
	s, err := decodeLokiSeries(body)
	if err != nil {
		t.Fatalf("%s: %v: %s", label, err, body)
	}
	a.series = s
	return a
}

// conformance: limits/drilldown-series-limit-skips-every-new-series, limits/drilldown-partial-with-warning, loki_api_v1_query_range, loki_api_v1_query
func TestDrilldownSeriesLimitMatchesLoki(t *testing.T) {
	cases := drilldownSeriesLimitCases()
	byData := map[string]*oraclerun.EngineDataSet{}
	var order []drilldownSeriesLimitCase
	for _, c := range cases {
		set := byData[c.dataSet()]
		if set == nil {
			rows := drilldownSeriesLimitRows(c.pods, c.staggered)
			set = &oraclerun.EngineDataSet{MaxSeries: drilldownSeriesLimit, Streams: engineStreams(rows, func(r vlemu.Row) (string, map[string]string) { return r.Msg, nil })}
			byData[c.dataSet()] = set
			order = append(order, c)
		}
		set.Requests = append(set.Requests, oraclerun.EngineRequest{ID: c.id, Instant: c.instant, Unaligned: true, Form: c.form().Encode(), QueryTags: drilldownSeriesLimitTags})
	}
	var sets []oraclerun.EngineDataSet
	for _, c := range order {
		sets = append(sets, *byData[c.dataSet()])
	}
	loki := lokiEngineAnswers(t, "drilldown_series_limit", sets)

	wantWarning := fmt.Sprintf("maximum number of series (%d) reached for a single query; returning partial results", drilldownSeriesLimit)
	truncated, sharedSeries, cut, sameSet := 0, 0, 0, 0
	for _, run := range []struct{ version, mode string }{{"v1.49.0", "loki"}, {"v1.54.0", "loki"}, {"v1.49.0", "busiest"}, {"v1.54.0", "busiest"}} {
		version, mode := run.version, run.mode
		proxies := map[string]*Proxy{}
		for _, c := range order {
			emu := &vlemu.Emulator{T: t, Rows: drilldownSeriesLimitRows(c.pods, c.staggered), Field: "amount", Version: version}
			backend := httptest.NewServer(emu.Handler())
			t.Cleanup(backend.Close)
			p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0",
				BackendVersionCheckTimeout: time.Second, MaxStatsQuerySeries: drilldownSeriesLimit, DrilldownSeriesLimitMode: mode})
			if err != nil {
				t.Fatal(err)
			}
			if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
				t.Fatal(err)
			}
			proxies[c.dataSet()] = p
		}
		for _, c := range cases {
			t.Run(mode+"/"+version+"/"+c.id, func(t *testing.T) {
				la := loki[c.id]
				if la.Status != http.StatusOK {
					t.Fatalf("Loki answered %d %s", la.Status, la.Error)
				}
				want := decodeDrilldownSeriesLimitAnswer(t, "Loki", la.Body)
				if want.unlabelled != 0 {
					t.Fatalf("Loki's recorded answer holds %d series without labels: recorded with a Loki before 3.7.10 (grafana/loki#26433)", want.unlabelled)
				}
				path := "/loki/api/v1/query_range?"
				if c.instant {
					path = "/loki/api/v1/query?"
				}
				res := doCompatProxyRequest(proxies[c.dataSet()], path+c.form().Encode(), map[string]string{
					"X-Scope-OrgID": "0", "X-Query-Tags": drilldownSeriesLimitTags, "User-Agent": "Grafana/13.3.1"})
				if res.Code != http.StatusOK {
					t.Fatalf("proxy answered %d: %s", res.Code, res.Body.String())
				}
				got := decodeDrilldownSeriesLimitAnswer(t, "proxy", res.Body.Bytes())
				if got.unlabelled != 0 {
					t.Fatalf("proxy answered %d series without labels", got.unlabelled)
				}
				if strings.Join(got.warnings, "\n") != strings.Join(want.warnings, "\n") {
					t.Fatalf("warnings: proxy %q, Loki %q", got.warnings, want.warnings)
				}
				if got.series.ResultType != want.series.ResultType {
					t.Fatalf("resultType: proxy %q, Loki %q", got.series.ResultType, want.series.ResultType)
				}
				if len(got.series.Points) != len(want.series.Points) {
					t.Fatalf("proxy answered %d series, Loki %d", len(got.series.Points), len(want.series.Points))
				}
				partial := len(want.warnings) > 0
				if partial {
					if want.warnings[0] != wantWarning || len(want.series.Points) != drilldownSeriesLimit {
						t.Fatalf("Loki's partial answer: %d series, warnings %q", len(want.series.Points), want.warnings)
					}
					truncated++
				}
				if c.pods > drilldownSeriesLimit != partial {
					t.Fatalf("%d series over a limit of %d: Loki's answer partial = %v", c.pods, drilldownSeriesLimit, partial)
				}
				// Loki's cut: a partial range answer ends at the trip step (limits/drilldown-series-limit-range-cut-at-trip-step).
				lastLoki := int64(0)
				for _, points := range want.series.Points {
					for ms := range points {
						lastLoki = max(lastLoki, ms/1000)
					}
				}
				if partial && !c.instant {
					if trip := drilldownSeriesLimitTripStep(c); lastLoki != trip {
						t.Fatalf("Loki's partial range answer ends at %d, the trip step is %d", lastLoki, trip)
					}
					cut++
				}
				lokiMode := mode == "loki" && !c.instant
				if lokiMode && partial {
					// Loki's cut, on the proxy's answer too.
					lastProxy := int64(0)
					for _, points := range got.series.Points {
						for ms := range points {
							lastProxy = max(lastProxy, ms/1000)
						}
					}
					if lastProxy != lastLoki {
						t.Errorf("proxy's partial range answer ends at %d, Loki's at %d", lastProxy, lastLoki)
					}
				}
				if lokiMode && c.staggered && partial && strings.HasPrefix(c.query, "sum by") {
					for key := range want.series.Points {
						if _, ok := got.series.Points[key]; !ok {
							t.Errorf("Loki keeps %s, the proxy does not (series met at different steps keep Loki's set)", key)
						}
					}
					sameSet++
				}
				for key, points := range got.series.Points {
					wp, ok := want.series.Points[key]
					if !ok {
						continue // a kept set that differs: busiest mode, or series new in Loki's trip step
					}
					sharedSeries++
					if (!partial || lokiMode) && len(points) != len(wp) {
						t.Errorf("series %s: proxy %d points, Loki %d", key, len(points), len(wp))
					}
					for ms, v := range wp {
						if pv, ok := points[ms]; !ok || pv != v {
							t.Errorf("series %s at %d: proxy %q, Loki %q", key, ms/1000, pv, v)
						}
					}
				}
			})
		}
	}
	if truncated == 0 || sharedSeries == 0 || cut == 0 || sameSet == 0 {
		t.Fatalf("the cases must cover partial answers, cut ranges, Loki's kept sets and shared series: %d partial, %d cut, %d sets, %d shared", truncated, cut, sameSet, sharedSeries)
	}
}
