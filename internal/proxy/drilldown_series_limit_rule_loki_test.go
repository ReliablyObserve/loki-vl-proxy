package proxy

import (
	"context"
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

// drilldownSeriesLimitRotation is a data set of pods that come and go over the hour: pod i logs every 7 s only from
// first(i) to last(i) minutes into the hour. Series met so far and series in one step's vector then differ, which
// tells apart the two rules Loki's evaluation could stop by (the monotone data sets of
// TestDrilldownSeriesLimitMatchesLoki cannot): the series met so far past the limit (cumulative), or one step holding
// more series than the limit (per step).
type drilldownSeriesLimitRotation struct {
	name string
	pods [][2]int // first and last minute of each pod's lines
}

var drilldownSeriesLimitRotations = []drilldownSeriesLimitRotation{
	// One pod at a time, ten minutes each: a step never holds more than two, six are met in all.
	{"one-at-a-time", [][2]int{{0, 10}, {10, 20}, {20, 30}, {30, 40}, {40, 50}, {50, 60}}},
	// Two at a time, overlapping: a step holds at most three.
	{"two-at-a-time", [][2]int{{0, 16}, {8, 24}, {16, 32}, {24, 40}, {32, 48}, {40, 56}, {48, 60}}},
	// Three for the whole hour, then two more at once from minute 40: per step past the limit only at 40.
	{"late-burst", [][2]int{{0, 60}, {0, 60}, {0, 60}, {40, 60}, {40, 60}}},
	// Three for the whole hour and one more pod at a time from minute 10: the series met so far pass the limit at
	// minute 10, a step's vector only when the visitors overlap at minute 30.
	{"visitors", [][2]int{{0, 60}, {0, 60}, {0, 60}, {10, 15}, {20, 25}, {30, 40}, {32, 36}}},
}

func (r drilldownSeriesLimitRotation) rows() []vlemu.Row {
	var rows []vlemu.Row
	start := drilldownSeriesLimitEnd*1e9 - int64(time.Hour)
	for i, p := range r.pods {
		stream := fmt.Sprintf(`{app="rot",pod="p%02d"}`, i)
		for at := start + int64(p[0])*int64(time.Minute) + 3e8; at < start+int64(p[1])*int64(time.Minute); at += int64(7 * time.Second) {
			rows = append(rows, vlemu.Row{TS: at, Stream: stream, Value: 1, Msg: "rotation line"})
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].TS < rows[j].TS })
	return rows
}

// Loki stops a Drilldown range evaluation where the series met so far pass the limit, not where one step's vector
// does (Loki 3.7.10's engine, recorded over pods that come and go: one at a time it stops at minute 30 although no
// step holds more than two series). The proxy, in the default Loki mode, answers each request as Loki does: the same
// warning, series, last step and points, on VictoriaLogs v1.49.0 and v1.54.0.
//
// conformance: limits/drilldown-series-limit-range-cut-at-trip-step, limits/drilldown-partial-with-warning
// conformance: loki_api_v1_query_range, loki_api_v1_query
func TestDrilldownSeriesLimitRuleMatchesLoki(t *testing.T) {
	query := `sum by (pod) (count_over_time({app="rot"}[1m]))`
	var sets []oraclerun.EngineDataSet
	for _, r := range drilldownSeriesLimitRotations {
		set := oraclerun.EngineDataSet{MaxSeries: drilldownSeriesLimit, Streams: engineStreams(r.rows(), func(row vlemu.Row) (string, map[string]string) { return row.Msg, nil })}
		set.Requests = append(set.Requests, oraclerun.EngineRequest{ID: r.name + "-range", Unaligned: true, QueryTags: drilldownSeriesLimitTags,
			Form: drilldownSeriesLimitCase{query: query}.form().Encode()})
		set.Requests = append(set.Requests, oraclerun.EngineRequest{ID: r.name + "-instant", Instant: true, QueryTags: drilldownSeriesLimitTags,
			Form: drilldownSeriesLimitCase{query: query, instant: true}.form().Encode()})
		sets = append(sets, set)
	}
	loki := lokiEngineAnswers(t, "drilldown_series_limit_rotation", sets)
	// What the recording shows, pinned so a Loki that changes it fails here: the last step of each range answer
	// (seconds into the hour) is where the series met so far first pass the limit.
	wantLast := map[string]int64{"one-at-a-time": 1800, "two-at-a-time": 1500, "late-burst": 2460, "visitors": 660}
	for _, version := range []string{"v1.49.0", "v1.54.0"} {
		for _, r := range drilldownSeriesLimitRotations {
			emu := &vlemu.Emulator{T: t, Rows: r.rows(), Field: "amount", Version: version}
			backend := httptest.NewServer(emu.Handler())
			t.Cleanup(backend.Close)
			p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0",
				BackendVersionCheckTimeout: time.Second, MaxStatsQuerySeries: drilldownSeriesLimit})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
				t.Fatal(err)
			}
			for _, kind := range []string{"range", "instant"} {
				a := loki[r.name+"-"+kind]
				if a.Status != http.StatusOK {
					t.Fatalf("%s %s: Loki answered %d %s", r.name, kind, a.Status, a.Error)
				}
				want := decodeDrilldownSeriesLimitAnswer(t, "Loki", a.Body)
				if kind == "range" {
					if last := lastPointSecond(want.series) - (drilldownSeriesLimitEnd - 3600); last != wantLast[r.name] || len(want.warnings) != 1 {
						t.Fatalf("%s: Loki's range answer ends %+ds with warnings %v, the recording said %+ds", r.name, last, want.warnings, wantLast[r.name])
					}
				}
				path := "/loki/api/v1/query_range?"
				form := drilldownSeriesLimitCase{query: query, instant: kind == "instant"}.form()
				if kind == "instant" {
					path = "/loki/api/v1/query?"
				}
				res := doCompatProxyRequest(p, path+form.Encode(), map[string]string{"X-Scope-OrgID": "0", "X-Query-Tags": drilldownSeriesLimitTags})
				if res.Code != http.StatusOK {
					t.Fatalf("%s %s %s: proxy answered %d %s", version, r.name, kind, res.Code, res.Body.String())
				}
				got := decodeDrilldownSeriesLimitAnswer(t, "proxy", res.Body.Bytes())
				if strings.Join(got.warnings, "\n") != strings.Join(want.warnings, "\n") || len(got.series.Points) != len(want.series.Points) {
					t.Fatalf("%s %s %s: proxy %d series warnings %v, Loki %d series warnings %v", version, r.name, kind, len(got.series.Points), got.warnings, len(want.series.Points), want.warnings)
				}
				for key, wp := range want.series.Points {
					gp, ok := got.series.Points[key]
					if !ok {
						if kind == "range" || len(want.warnings) == 0 {
							t.Errorf("%s %s %s: Loki keeps %s, the proxy does not", version, r.name, kind, key)
						}
						continue // an instant over the limit: Loki keeps an arbitrary set, the proxy the busiest
					}
					if fmt.Sprint(gp) != fmt.Sprint(wp) {
						t.Errorf("%s %s %s: %s proxy %v, Loki %v", version, r.name, kind, key, gp, wp)
					}
				}
			}
		}
	}
}

func lastPointSecond(s lokiSeries) int64 {
	last := int64(0)
	for _, points := range s.Points {
		for ms := range points {
			last = max(last, ms/1000)
		}
	}
	return last
}

// drilldownSeriesLimitSplitRows is a data set over three and a half hours of pods that come and go: pod i logs
// every 7 s for 15 minutes from 30 s past minute 7*i. At most three log at once, so a split's first step meets at
// most the limit and every trip step meets one new series (no tie Loki would break at random).
func drilldownSeriesLimitSplitRows(start int64) []vlemu.Row {
	var rows []vlemu.Row
	for i := 0; i*7 < 210; i++ {
		stream := fmt.Sprintf(`{app="split",pod="p%02d"}`, i)
		from := start + int64(i)*int64(7*time.Minute) + int64(30*time.Second)
		for at := from; at < from+int64(15*time.Minute); at += int64(7 * time.Second) {
			rows = append(rows, vlemu.Row{TS: at, Stream: stream, Value: 1, Msg: "split line"})
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].TS < rows[j].TS })
	return rows
}

// Loki's query frontend splits a range request at the hours inside it and cuts each split's answer on its own
// (the stack's answers: each split asked alone is its fragment of the whole answer). The proxy, with
// -drilldown-series-limit-split-interval=1h, answers a Drilldown request over three hours, crossing three hour
// boundaries, with Loki's answers for each split concatenated: the same series, steps and values, for range ==
// step (Drilldown's [$__auto]) and a range longer than the step, on VictoriaLogs v1.49.0 and v1.54.0.
//
// conformance: limits/drilldown-series-limit-range-cut-at-trip-step, limits/drilldown-partial-with-warning
// conformance: loki_api_v1_query_range
func TestDrilldownSeriesLimitSplitsMatchLoki(t *testing.T) {
	const split = int64(3600)
	dataStart := drilldownSeriesLimitEnd*1e9 - int64(3*time.Hour+30*time.Minute)
	rows := drilldownSeriesLimitSplitRows(dataStart)
	start, end := drilldownSeriesLimitEnd-3*3600-1200, drilldownSeriesLimitEnd-1200 // 20 minutes before an hour
	queries := []struct {
		name, query string
		step        int64
	}{
		{"range-eq-step", `sum by (pod) (count_over_time({app="split"}[1m]))`, 60},
		{"range-gt-step", `sum by (pod) (count_over_time({app="split"}[5m]))`, 60},
		{"step-30", `sum by (pod) (count_over_time({app="split"}[30s]))`, 30},
	}
	set := oraclerun.EngineDataSet{MaxSeries: drilldownSeriesLimit, Streams: engineStreams(rows, func(row vlemu.Row) (string, map[string]string) { return row.Msg, nil })}
	form := func(q string, from, to, step int64) string {
		return url.Values{"query": {q}, "start": {strconv.FormatInt(from, 10)}, "end": {strconv.FormatInt(to, 10)}, "step": {strconv.FormatInt(step, 10)}}.Encode()
	}
	// Loki's splits: [start, b1 - step], [b1, b2 - step], ..., [bn, end].
	pieces := func(step int64) [][2]int64 {
		var out [][2]int64
		from := start
		for b := start - start%split + split; b < end; b += split {
			out = append(out, [2]int64{from, b - step})
			from = b
		}
		return append(out, [2]int64{from, end})
	}
	for _, q := range queries {
		for i, piece := range pieces(q.step) {
			set.Requests = append(set.Requests, oraclerun.EngineRequest{ID: fmt.Sprintf("%s-split%d", q.name, i), Unaligned: true,
				QueryTags: drilldownSeriesLimitTags, Form: form(q.query, piece[0], piece[1], q.step)})
		}
	}
	loki := lokiEngineAnswers(t, "drilldown_series_limit_splits", []oraclerun.EngineDataSet{set})
	for _, version := range []string{"v1.49.0", "v1.54.0"} {
		for _, q := range queries {
			want := lokiSeries{Points: map[string]map[int64]string{}}
			cutSplits := 0
			for i := range pieces(q.step) {
				a := loki[fmt.Sprintf("%s-split%d", q.name, i)]
				if a.Status != http.StatusOK {
					t.Fatalf("%s split %d: Loki answered %d %s", q.name, i, a.Status, a.Error)
				}
				piece := decodeDrilldownSeriesLimitAnswer(t, "Loki", a.Body)
				if len(piece.warnings) > 0 {
					cutSplits++
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
			if cutSplits < 3 {
				t.Fatalf("%s: only %d of Loki's splits passed the limit: the data proves little", q.name, cutSplits)
			}
			emu := &vlemu.Emulator{T: t, Rows: rows, Field: "amount", Version: version}
			backend := httptest.NewServer(emu.Handler())
			t.Cleanup(backend.Close)
			p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0",
				BackendVersionCheckTimeout: time.Second, MaxStatsQuerySeries: drilldownSeriesLimit, DrilldownSeriesLimitSplitInterval: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
				t.Fatal(err)
			}
			res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+form(q.query, start, end, q.step), map[string]string{"X-Scope-OrgID": "0", "X-Query-Tags": drilldownSeriesLimitTags})
			if res.Code != http.StatusOK {
				t.Fatalf("%s %s: proxy answered %d %s", version, q.name, res.Code, res.Body.String())
			}
			got := decodeDrilldownSeriesLimitAnswer(t, "proxy", res.Body.Bytes())
			if len(got.warnings) != 1 {
				t.Errorf("%s %s: warnings %v, want Loki's", version, q.name, got.warnings)
			}
			if len(got.series.Points) != len(want.Points) {
				t.Errorf("%s %s: proxy %d series, Loki's splits %d", version, q.name, len(got.series.Points), len(want.Points))
			}
			for key, wp := range want.Points {
				if gp := got.series.Points[key]; fmt.Sprint(gp) != fmt.Sprint(wp) {
					t.Errorf("%s %s: %s\n proxy %v\n  Loki %v", version, q.name, key, gp, wp)
				}
			}
		}
	}
}
