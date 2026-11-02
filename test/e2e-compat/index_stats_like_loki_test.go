//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Loki's /index/stats reads chunk metadata from its index: streams with a
// chunk in the window, and the chunks' entries pro rata by time
// (pkg/storage/stores/shipper/indexshipper/tsdb/index/chunk.go:262-267,
// tsdb/single_file_index.go:340-376). On the single-binary stack the querier
// reads only the store (pkg/querier/querier.go:529-557; no AsyncStore for
// target all, pkg/loki/modules.go:1078-1101), so it counts flushed chunks only.
// For chunks that lie wholly inside the window and are counted once, those
// numbers are exact: the window's streams and lines. The proxy counts them
// exactly from VictoriaLogs. A missing start is the hour before end
// (pkg/loghttp/params.go:91-120).

type indexStatsFixture struct {
	probe        string
	s0           time.Time // start of the hour the old part lies in
	oldLines     int       // part="old": pods a, b, c, inside [s0+5m, s0+25m]
	recentLines  int       // part="recent", pod r: inside the last 25 minutes
	outsideLines int       // part="recent", pod o: 3h ago, outside Loki's default hour
	subLines     int       // old lines in (s0+10m, s0+20m]
	subStreams   int
}

var indexStatsFx *indexStatsFixture

type indexStatsAnswerE2E struct {
	Streams int `json:"streams"`
	Chunks  int `json:"chunks"`
	Entries int `json:"entries"`
	Bytes   int `json:"bytes"`
}

func ensureIndexStatsFixture(t *testing.T) *indexStatsFixture {
	t.Helper()
	if indexStatsFx != nil {
		return indexStatsFx
	}
	now := time.Now()
	fx := &indexStatsFixture{probe: fmt.Sprintf("idxstats%d", now.UnixNano()), s0: now.Add(-9 * time.Hour).Truncate(time.Hour)}
	type stream struct {
		part, pod     string
		from, to      time.Time
		every, offset time.Duration
		count, inSub  *int
	}
	streams := []stream{
		{"old", "a", fx.s0.Add(5 * time.Minute), fx.s0.Add(25 * time.Minute), 7 * time.Second, 370 * time.Millisecond, &fx.oldLines, &fx.subLines},
		{"old", "b", fx.s0.Add(5 * time.Minute), fx.s0.Add(25 * time.Minute), 11 * time.Second, 500 * time.Millisecond, &fx.oldLines, &fx.subLines},
		{"old", "c", fx.s0.Add(5 * time.Minute), fx.s0.Add(25 * time.Minute), 13 * time.Second, 630 * time.Millisecond, &fx.oldLines, &fx.subLines},
		{"recent", "r", now.Add(-25 * time.Minute), now.Add(-15 * time.Minute), 10 * time.Second, 250 * time.Millisecond, &fx.recentLines, nil},
		{"recent", "o", now.Add(-3 * time.Hour), now.Add(-170 * time.Minute), 30 * time.Second, 410 * time.Millisecond, &fx.outsideLines, nil},
	}
	subLo, subHi := fx.s0.Add(10*time.Minute), fx.s0.Add(20*time.Minute)
	var vlRows strings.Builder
	var lokiStreams []any
	for _, s := range streams {
		labels := map[string]string{"probe": fx.probe, "part": s.part, "pod": s.pod}
		var values [][]string
		inSub := false
		for at := s.from.Add(s.offset); at.Before(s.to); at = at.Add(s.every) {
			line := fmt.Sprintf("idx %s %d", s.pod, at.UnixNano())
			values = append(values, []string{strconv.FormatInt(at.UnixNano(), 10), line})
			row := map[string]string{"_time": at.UTC().Format(time.RFC3339Nano), "_msg": line}
			for k, v := range labels {
				row[k] = v
			}
			encoded, _ := json.Marshal(row)
			vlRows.Write(encoded)
			vlRows.WriteByte('\n')
			*s.count++
			if s.inSub != nil && at.After(subLo) && !at.After(subHi) {
				*s.inSub++
				inSub = true
			}
		}
		if inSub {
			fx.subStreams++
		}
		lokiStreams = append(lokiStreams, map[string]any{"stream": labels, "values": values})
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields="+url.QueryEscape("probe,part,pod"), vlRows.String(), map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VL ingest: %d %s", status, body)
	}
	payload, _ := json.Marshal(map[string]any{"streams": lokiStreams})
	if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json", "X-Scope-OrgID": "0"}); status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)
	// The single-binary querier counts flushed chunks only.
	if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/flush", "", nil); status >= 300 {
		t.Fatalf("Loki flush: %d %s", status, body)
	}

	// Ready when Loki's own query path counts every old line (they are older
	// than query_ingesters_within, so they come from the flushed chunks),
	// Loki's index answers the recent part (an end at now is never served from
	// Loki's index-stats results cache: max_stats_cache_freshness 10m) and
	// Loki's index answers the old part exactly.
	//
	// Loki's index lags the flush (2 of 3 streams, then 3 of 3), and an old
	// window's answer is stored in Loki's index-stats results cache, keyed by
	// tenant, query text and the split interval of the start, with overlapping
	// windows served from the stored extent. A metric query also asks that
	// cache for its selector's stats (the shard resolver sends an
	// IndexStatsRequest through the cached stats tripperware:
	// pkg/querier/queryrange/shard_resolver.go:106-111, roundtrip.go:196-203),
	// so a readiness probe that used the asserted selector would store the
	// lagging answer under the very key the assertion reads. Every probe of the old window therefore
	// uses its own selector text (a no-op pod alternative) and its own end,
	// and the asserted selector is first asked by the assertion itself.
	oldWindowEnd := fx.s0.Add(30 * time.Minute)
	probes := 0
	deadline := time.Now().Add(120 * time.Second)
	for {
		probes++
		at := oldWindowEnd.Add(time.Duration(probes) * time.Millisecond)
		probeSelector := fmt.Sprintf(`{probe=%q, part="old", pod=~"a|b|c|ready%d"}`, fx.probe, probes)
		lokiOld := nonSumInstant(t, lokiURL, fmt.Sprintf(`sum(count_over_time(%s[30m]))`, probeSelector), at)[""]
		proxyOld := nonSumInstant(t, proxyURL, fmt.Sprintf(`sum(count_over_time({probe=%q, part="old"}[30m]))`, fx.probe), at)[""]
		oldIndex := indexStatsGet(t, lokiURL, url.Values{"query": {probeSelector}, "start": {strconv.FormatInt(fx.s0.UnixNano(), 10)}, "end": {strconv.FormatInt(at.UnixNano(), 10)}})
		recent := indexStatsGet(t, lokiURL, url.Values{"query": {fmt.Sprintf(`{probe=%q, part="recent"}`, fx.probe)}, "since": {"4h"}})
		if lokiOld == float64(fx.oldLines) && proxyOld == float64(fx.oldLines) && recent.Entries == fx.recentLines+fx.outsideLines &&
			oldIndex.Streams == 3 && oldIndex.Entries == fx.oldLines {
			break
		}
		if time.Now().After(deadline) {
			t.Skipf("Loki's index never became exact within 120s (refusing to compare): old part %d lines in 3 streams (Loki counts %v, proxy %v, Loki's index %+v), recent part %d lines (Loki's index %d)",
				fx.oldLines, lokiOld, proxyOld, oldIndex, fx.recentLines+fx.outsideLines, recent.Entries)
		}
		time.Sleep(2 * time.Second)
	}
	t.Logf("fixture ready after %d probes", probes)
	indexStatsFx = fx
	return fx
}

func indexStatsGet(t *testing.T, base string, params url.Values) indexStatsAnswerE2E {
	t.Helper()
	status, body, _ := doJSONGET(t, base+"/loki/api/v1/index/stats?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	var out indexStatsAnswerE2E
	if status != http.StatusOK || json.Unmarshal([]byte(body), &out) != nil {
		t.Fatalf("%s index/stats %s: %d %s", base, params.Encode(), status, body)
	}
	return out
}

// lokiSeriesCount is the number of streams Loki's /series lists for the window.
func lokiSeriesCount(t *testing.T, selector string, start, end time.Time) int {
	t.Helper()
	params := url.Values{"match[]": {selector}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
	status, body, resp := doJSONGET(t, lokiURL+"/loki/api/v1/series?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if status != http.StatusOK || resp["status"] != "success" {
		t.Fatalf("Loki /series %s: %d %s", selector, status, body)
	}
	return len(extractArray(resp, "data"))
}

// TestDrilldown_IndexStatsLikeLoki: the proxy's index/stats streams and entries
// equal Loki's where Loki's chunk estimate is exact (every chunk inside the
// window, flushed, read once), equal Loki's own /series and count_over_time on
// a window that cuts the chunks (where Loki's index/stats pro-rates), and a
// request without start reads Loki's default hour (or `since`).
//
// conformance: loki_api_v1_index_stats, quality/index-stats-default-window, quality/index-stats-streams-differ, quality/index-stats-loki-chunk-accounting
func TestDrilldown_IndexStatsLikeLoki(t *testing.T) {
	fx := ensureIndexStatsFixture(t)
	old := fmt.Sprintf(`{probe=%q, part="old"}`, fx.probe)
	recent := fmt.Sprintf(`{probe=%q, part="recent"}`, fx.probe)
	ns := func(at time.Time) string { return strconv.FormatInt(at.UnixNano(), 10) }

	t.Run("whole chunks in the window: equal to Loki's index", func(t *testing.T) {
		// One request per side for this window: a second Loki request could be
		// answered from its index-stats results cache.
		params := url.Values{"query": {old}, "start": {ns(fx.s0)}, "end": {ns(fx.s0.Add(30 * time.Minute))}}
		loki, proxy := indexStatsGet(t, lokiURL, params), indexStatsGet(t, proxyURL, params)
		if loki.Entries != fx.oldLines || loki.Streams != 3 {
			t.Fatalf("Loki's index is not exact for whole flushed chunks (refusing to compare): %+v, fixture %d lines in 3 streams", loki, fx.oldLines)
		}
		if proxy.Streams != loki.Streams || proxy.Entries != loki.Entries {
			t.Fatalf("proxy %+v, Loki %+v", proxy, loki)
		}
	})

	t.Run("window cutting the chunks: equal to Loki's own counts", func(t *testing.T) {
		start, end := fx.s0.Add(10*time.Minute), fx.s0.Add(20*time.Minute)
		params := url.Values{"query": {old}, "start": {ns(start)}, "end": {ns(end)}}
		proxy := indexStatsGet(t, proxyURL, params)
		lokiIndex := indexStatsGet(t, lokiURL, params)
		lines := nonSumInstant(t, lokiURL, fmt.Sprintf(`sum(count_over_time(%s[10m]))`, old), end)[""]
		series := lokiSeriesCount(t, old, start, end)
		if lines == 0 || series == 0 {
			t.Fatalf("Loki answered no sample for the sub-window (refusing to compare): lines %v, series %d", lines, series)
		}
		if proxy.Entries != int(lines) || proxy.Entries != fx.subLines || proxy.Streams != series || proxy.Streams != fx.subStreams {
			t.Fatalf("proxy %+v; Loki's count_over_time %v, /series %d; fixture %d lines in %d streams (Loki's index estimate %+v)",
				proxy, lines, series, fx.subLines, fx.subStreams, lokiIndex)
		}
		t.Logf("Loki's index estimate for the cut window: %+v (pro rata), exact: %d lines in %d streams", lokiIndex, fx.subLines, fx.subStreams)
	})

	t.Run("no start: Loki's default hour", func(t *testing.T) {
		params := url.Values{"query": {recent}}
		loki, proxy := indexStatsGet(t, lokiURL, params), indexStatsGet(t, proxyURL, params)
		if loki.Entries != fx.recentLines || loki.Streams != 1 {
			t.Fatalf("Loki's default hour is not the fixture's recent stream (refusing to compare): %+v, fixture %d lines", loki, fx.recentLines)
		}
		if proxy.Streams != loki.Streams || proxy.Entries != loki.Entries {
			t.Fatalf("proxy %+v, Loki %+v", proxy, loki)
		}
	})

	t.Run("no start, since 4h", func(t *testing.T) {
		params := url.Values{"query": {recent}, "since": {"4h"}}
		loki, proxy := indexStatsGet(t, lokiURL, params), indexStatsGet(t, proxyURL, params)
		if loki.Entries != fx.recentLines+fx.outsideLines || loki.Streams != 2 {
			t.Fatalf("Loki's 4h window is not both recent streams (refusing to compare): %+v", loki)
		}
		if proxy.Streams != loki.Streams || proxy.Entries != loki.Entries {
			t.Fatalf("proxy %+v, Loki %+v", proxy, loki)
		}
	})
}
