package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// A client that loads a long metric range in pieces (Grafana does, for ranges over a day) merges the pieces' answers
// into one chart. The merged chart equals Loki's when every piece's answer equals Loki's answer for the same
// request, whatever the pieces are and however the client merges them. These checks ask the proxy for the pieces
// such a client can ask for (day-long pieces with unaligned starts, the whole range, and the short last or first
// piece, shorter than a step, one step, or just over) and compare each answer with Loki's recorded answer to the
// same request (the Loki engine oracle, with a query frontend's step alignment, LVP_RUN_ORACLES=1 records again).
// Logs Drilldown's piece shorter than a step is the one documented exception (TestLock_LeftoverChunkSuppressedInHits).

const chunkedWindowsQuery = `sum by (pod) (count_over_time({app="cm",namespace="prod",pod!=""}[2m]))`

// chunkedWindowsEnd is the end of the data and of the whole ranges (unix s): a multiple of the step, not of a day.
const chunkedWindowsEnd = int64(1_800_000_000)

// chunkedWindowsRows: two pods over 60 hours, one line every 5 and every 7 minutes, off the step grid.
func chunkedWindowsRows() []vlemu.Row {
	var rows []vlemu.Row
	end := chunkedWindowsEnd * 1e9
	for _, pod := range []struct {
		name  string
		every time.Duration
	}{{"cm-a", 5 * time.Minute}, {"cm-b", 7 * time.Minute}} {
		stream := fmt.Sprintf(`{app="cm",namespace="prod",pod=%q}`, pod.name)
		for at := end - int64(60*time.Hour) + 3e8; at < end; at += int64(pod.every) {
			rows = append(rows, vlemu.Row{TS: at, Stream: stream, Value: 1, Msg: "chunk window line"})
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].TS < rows[j].TS })
	return rows
}

// chunkedWindow is one piece: its bounds (unix s) and step.
type chunkedWindow struct {
	name       string
	start, end int64
	step       int64
}

func chunkedWindows() []chunkedWindow {
	const hour, day, step = int64(3600), int64(86400), int64(120)
	e := chunkedWindowsEnd
	var w []chunkedWindow
	for _, h := range []int64{24, 25, 48, 168} {
		w = append(w, chunkedWindow{fmt.Sprintf("whole-%dh", h), e - h*hour, e, step})
	}
	// Day-long pieces of a 48h range: the start is not on the step grid, each piece ends one step before the next.
	for _, off := range []int64{0, 37, 3*hour + 11} {
		start := e - 2*day + off
		for i := int64(0); i < 2; i++ {
			s := start + i*day
			end := s + day - step
			if end > e {
				end = e
			}
			w = append(w, chunkedWindow{fmt.Sprintf("day-piece-%d-off%d", i, off), s, end, step})
		}
		w = append(w, chunkedWindow{fmt.Sprintf("last-piece-off%d", off), start + 2*day, e + off, step})
	}
	// Pieces shorter than a step, one step, just over one step, at the end and at the start of a range.
	for _, n := range []int64{1, 30, 119, 120, 121, 239} {
		w = append(w, chunkedWindow{fmt.Sprintf("short-end-%ds", n), e - n, e, step})
		w = append(w, chunkedWindow{fmt.Sprintf("short-start-%ds", n), e - day, e - day + n, step})
	}
	// A piece ending inside a step, and other steps.
	w = append(w, chunkedWindow{"mid-step-end", e - 6*hour, e - 3*hour + 59, step})
	w = append(w, chunkedWindow{"step-60-day", e - day, e, 60})
	w = append(w, chunkedWindow{"step-300-week", e - 7*day, e, 300})
	return w
}

func chunkedWindowsRequest(w chunkedWindow) url.Values {
	return url.Values{
		"query": {chunkedWindowsQuery},
		"start": {strconv.FormatInt(w.start*1e9, 10)},
		"end":   {strconv.FormatInt(w.end*1e9, 10)},
		"step":  {strconv.FormatInt(w.step, 10)},
	}
}

// conformance: drilldown/long-range-pieces-answer-like-loki, loki_api_v1_query_range
func TestChunkedRangeWindowsMatchLoki(t *testing.T) {
	rows := chunkedWindowsRows()
	windows := chunkedWindows()
	set := oraclerun.EngineDataSet{Streams: engineStreams(rows, func(r vlemu.Row) (string, map[string]string) { return r.Msg, nil })}
	for _, w := range windows {
		set.Requests = append(set.Requests, oraclerun.EngineRequest{ID: w.name, Form: chunkedWindowsRequest(w).Encode()})
	}
	loki := lokiEngineAnswers(t, "chunked_range_windows", []oraclerun.EngineDataSet{set})

	for _, version := range []string{"v1.49.0", "v1.53.0"} {
		for _, client := range []struct {
			name    string
			headers map[string]string
		}{
			{"api", map[string]string{"X-Scope-OrgID": "0"}},
			{"grafana", map[string]string{"X-Scope-OrgID": "0", "User-Agent": "Grafana/13.3.0", "X-Grafana-Org-Id": "1"}},
		} {
			// A proxy and a cache of its own per client: no client's piece is answered from another's.
			emu := &vlemu.Emulator{T: t, Rows: rows, Field: "amount", Version: version}
			backend := httptest.NewServer(emu.Handler())
			t.Cleanup(backend.Close)
			p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second, AlignQueriesWithStep: true})
			if err != nil {
				t.Fatal(err)
			}
			if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
				t.Fatal(err)
			}
			for _, w := range windows {
				t.Run(fmt.Sprintf("%s/%s/%s", version, client.name, w.name), func(t *testing.T) {
					want, wantLabels := lokiMetricPoints(t, w.name, loki[w.name])
					res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+chunkedWindowsRequest(w).Encode(), client.headers)
					if res.Code != http.StatusOK {
						t.Fatalf("proxy answered %d: %s", res.Code, res.Body.String())
					}
					got, err := decodeLokiSeries(res.Body.Bytes())
					if err != nil {
						t.Fatal(err)
					}
					if got.ResultType != "matrix" {
						t.Fatalf("proxy resultType %q, Loki matrix", got.ResultType)
					}
					chunkedWindowsCompare(t, want, wantLabels, got)
				})
			}
		}
	}
}

// chunkedWindowsCompare holds the proxy's series and points to Loki's: the same series, the same times, the same
// value text.
func chunkedWindowsCompare(t *testing.T, want map[string]map[int64]float64, wantLabels map[string]map[string]string, got lokiSeries) {
	t.Helper()
	if len(got.Points) != len(want) {
		t.Errorf("proxy answered %d series, Loki %d (%v)", len(got.Points), len(want), wantLabels)
	}
	for key, points := range want {
		gp, ok := got.Points[key]
		if !ok {
			t.Errorf("series %s: Loki answers it, the proxy does not", key)
			continue
		}
		if len(gp) != len(points) {
			t.Errorf("series %s: proxy %d points, Loki %d", key, len(gp), len(points))
		}
		for ns, v := range points {
			text, ok := gp[ns/int64(time.Millisecond)]
			if !ok {
				t.Errorf("series %s: no proxy point at %d, Loki %v", key, ns/1e9, v)
				continue
			}
			if pv, err := strconv.ParseFloat(text, 64); err != nil || pv != v {
				t.Errorf("series %s at %d: proxy %s, Loki %v", key, ns/1e9, text, v)
			}
		}
	}
}
