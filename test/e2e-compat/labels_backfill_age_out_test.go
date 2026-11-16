//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The core stack's proxy runs with the defaults of the label freshness flags
// (none is set in docker-compose.yml): -max-metadata-cache-freshness 24h,
// -recent-tail-refresh-max-staleness 15s (a near-now metadata answer and an
// inventory row count are reused that long) and -metadata-max-staleness 10s
// (a refresh may answer from the last read that long). Both tests below use
// windows that end a few hours back: inside the freshness window, so Loki
// reads them live, and fully sealed for the proxy's inventory. A proxy
// answer is polled until it matches Loki or labelsFreshnessBound passes; the
// polls take the time the proxy needs, never a tuned sleep.
const labelsFreshnessBound = 15*time.Second + 10*time.Second + 35*time.Second

// labelsFixtureIngest writes one line with labels at ts to VictoriaLogs (as a
// stream with those fields) and to Loki, and flushes both: Loki serves lines
// older than query_ingesters_within from its store only.
func labelsFixtureIngest(t *testing.T, labels map[string]string, ts time.Time, msg string) {
	t.Helper()
	fields := make([]string, 0, len(labels))
	row := map[string]string{"_time": ts.Format(time.RFC3339Nano), "_msg": msg}
	for k, v := range labels {
		row[k] = v
		fields = append(fields, k)
	}
	sort.Strings(fields)
	encoded, _ := json.Marshal(row)
	status, body := hardeningRequest(t, http.MethodPost,
		vlURL+"/insert/jsonline?_stream_fields="+url.QueryEscape(strings.Join(fields, ",")), string(encoded)+"\n",
		map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VictoriaLogs ingest: %d %s", status, body)
	}
	payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{
		"stream": labels,
		"values": [][]string{{strconv.FormatInt(ts.UnixNano(), 10), msg}},
	}}})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	if status, body := hardeningRequest(t, http.MethodPost, vlURL+"/internal/force_flush", "", nil); status != http.StatusOK {
		t.Fatalf("VictoriaLogs flush: %d %s", status, body)
	}
	if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/flush", "", nil); status >= 300 {
		t.Fatalf("Loki flush: %d %s", status, body)
	}
}

// labelsWindow is the label request parameters of [start, end] scoped to
// selector.
func labelsWindow(selector string, start, end time.Time) string {
	q := url.Values{}
	q.Set("start", strconv.FormatInt(start.UnixNano(), 10))
	q.Set("end", strconv.FormatInt(end.UnixNano(), 10))
	q.Set("query", selector)
	return q.Encode()
}

// waitBothHold waits until Loki's /labels over params and VictoriaLogs'
// native stream field names over the same window list every name in want and
// none in absent: both sides hold the fixture before any comparison.
func waitBothHold(t *testing.T, selector string, start, end time.Time, want, absent []string) {
	t.Helper()
	params := labelsWindow(selector, start, end)
	native := url.Values{"query": {selector}, "start": {start.Format(time.RFC3339Nano)}, "end": {end.Format(time.RFC3339Nano)}}
	deadline := time.Now().Add(90 * time.Second)
	for {
		loki := labelsFullRangeStrings(t, lokiURL+"/loki/api/v1/labels?"+params)
		vl := labelsFullRangeNativeStrings(t, vlURL+"/select/logsql/stream_field_names?"+native.Encode())
		ok := true
		for _, name := range want {
			ok = ok && slices.Contains(loki, name) && slices.Contains(vl, name)
		}
		for _, name := range absent {
			ok = ok && !slices.Contains(loki, name) && !slices.Contains(vl, name)
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the backends do not hold the fixture: want %v, absent %v; Loki %v, VictoriaLogs %v", want, absent, loki, vl)
		}
		time.Sleep(2 * time.Second)
	}
}

// fixtureNames keeps the names of labels that belong to the fixture.
func fixtureNames(labels []string, names ...string) []string {
	var out []string
	for _, l := range labels {
		if slices.Contains(names, l) {
			out = append(out, l)
		}
	}
	return out
}

// pollProxyLikeLoki asks the proxy for params until its fixture labels equal
// Loki's or labelsFreshnessBound passes, and returns how long it took.
func pollProxyLikeLoki(t *testing.T, what, params string, names ...string) time.Duration {
	t.Helper()
	loki := fixtureNames(labelsFullRangeStrings(t, lokiURL+"/loki/api/v1/labels?"+params), names...)
	started := time.Now()
	for {
		proxy := fixtureNames(labelsFullRangeStrings(t, proxyURL+"/loki/api/v1/labels?"+params), names...)
		if slices.Equal(proxy, loki) {
			return time.Since(started)
		}
		if time.Since(started) > labelsFreshnessBound {
			t.Fatalf("%s: the proxy lists %v after %s, Loki %v", what, proxy, labelsFreshnessBound, loki)
		}
		time.Sleep(2 * time.Second)
	}
}

// TestCompat_LabelsBackfillIntoCachedBucket: a stream pushed with an old
// timestamp into an hour the proxy's label inventory already holds with data
// is listed by /labels on the next request past the staleness bounds, as Loki
// lists it. The hour lies 7h back (inside -max-metadata-cache-freshness, more
// than an hour old); the request window is the hour with half an hour on each
// side, so the plan holds the sealed hour bucket and two uncached edges.
//
// conformance: quality/labels-backfill-into-cached-bucket, semantics/metadata-answers-include-last-24h-like-loki
func TestCompat_LabelsBackfillIntoCachedBucket(t *testing.T) {
	id := strconv.FormatInt(time.Now().UnixNano(), 36)
	app := "labels-backfill-" + id
	uniq := "lbf_" + id
	hour := time.Now().UTC().Truncate(time.Hour).Add(-7 * time.Hour)
	start, end := hour.Add(-30*time.Minute), hour.Add(90*time.Minute)
	selector := fmt.Sprintf(`{app=%q}`, app)
	params := labelsWindow(selector, start, end)

	// The hour holds data: the proxy caches it with its row count.
	labelsFixtureIngest(t, map[string]string{"app": app, "env": "base"}, hour.Add(10*time.Minute), "labels backfill base "+id)
	waitBothHold(t, selector, start, end, []string{"app", "env"}, []string{uniq})
	pollProxyLikeLoki(t, "before the backfill", params, "app", "env", uniq)

	// A stream with a new label backfilled into the cached hour.
	labelsFixtureIngest(t, map[string]string{"app": app, uniq: "v1"}, hour.Add(40*time.Minute), "labels backfill late "+id)
	waitBothHold(t, selector, start, end, []string{"app", "env", uniq}, nil)
	took := pollProxyLikeLoki(t, "after the backfill", params, "app", "env", uniq)
	t.Logf("the proxy listed the backfilled label %s after Loki did", took)
}

// TestE2E_LabelsAgeOutOfWindow: a label whose only stream has data on an
// earlier UTC day than the requested window is not listed by /labels, as Loki
// does not list it, while a window that reaches back to the stream lists it
// on both sides; the two windows ask one after the other on the same proxy.
// Loki lists label names per index day (TSDB, 24h tables) for data it reads
// from its store, not per stream time: a stream earlier on the same UTC day
// as the window is listed by Loki (seen on the stack: a narrow window that
// starts a minute after the stream), so the window is put on the next day.
// The fixture is more than query_ingesters_within (30m on the stack) old, so
// Loki reads it from the store only.
//
// conformance: quality/label-cache-aged-labels-kept
func TestE2E_LabelsAgeOutOfWindow(t *testing.T) {
	id := strconv.FormatInt(time.Now().UnixNano(), 36)
	app := "labels-age-out-" + id
	gone := "lgone_" + id
	day := time.Now().UTC().Truncate(24 * time.Hour)
	if time.Since(day) < 2*time.Hour {
		day = day.Add(-24 * time.Hour) // the whole fixture stays more than 30m old
	}
	oldAt := day.Add(-30 * time.Minute) // the day before
	newAt := day.Add(30 * time.Minute)
	selector := fmt.Sprintf(`{app=%q}`, app)
	end := day.Add(time.Hour)
	wideStart, narrowStart := day.Add(-time.Hour), day.Add(10*time.Minute)
	wide := labelsWindow(selector, wideStart, end)
	narrow := labelsWindow(selector, narrowStart, end)

	labelsFixtureIngest(t, map[string]string{"app": app, gone: "v1"}, oldAt, "labels age out old "+id)
	labelsFixtureIngest(t, map[string]string{"app": app, "env": "new"}, newAt, "labels age out new "+id)
	// Both backends hold both streams over the wide window, and neither lists
	// the old one over the narrow window.
	waitBothHold(t, selector, wideStart, end, []string{"app", "env", gone}, nil)
	waitBothHold(t, selector, narrowStart, end, []string{"app", "env"}, []string{gone})

	// The wide window lists the old stream's label, on both sides.
	pollProxyLikeLoki(t, "the wide window", wide, "app", "env", gone)
	if !slices.Contains(labelsFullRangeStrings(t, proxyURL+"/loki/api/v1/labels?"+wide), gone) {
		t.Fatalf("the proxy does not list %s for the window that holds its stream", gone)
	}
	// The narrow window starts on the next day: like Loki, no gone label.
	took := pollProxyLikeLoki(t, "the narrow window", narrow, "app", "env", gone)
	t.Logf("the proxy listed the narrow window like Loki after %s", took)
}
