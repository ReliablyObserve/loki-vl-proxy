package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
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
