package proxy

import (
	"net/http"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// scopedRows are lines of one app in one window: clean ones, and the extra rows
// given (a line, or a line with a stored field).
func scopedRows(s0 time.Time, app string, extra ...pushdownRow) []pushdownRow {
	var rows []pushdownRow
	for i := 0; i < 20; i++ {
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(i+1) * 10 * time.Second), stream: map[string]string{"app": app, "env": "prod"},
			msg: `{"pipeline":"logs/loki","n":` + strconv.Itoa(i) + `}`})
	}
	for i := range extra {
		extra[i].ts = s0.Add(time.Duration(100+i) * time.Second)
		extra[i].stream = map[string]string{"app": app, "env": "prod"}
	}
	return append(rows, extra...)
}

// askScoped runs a query on a fresh proxy over rows, after the queries of
// before (same proxy, same window, same tenant), and returns the answer of the
// last and whether the raw-row evaluator served it.
func askScoped(t *testing.T, rows []pushdownRow, end time.Time, before []string, query string) (map[string]map[int64]string, bool) {
	t.Helper()
	srv, fake := newPushdownFakeVL(t, rows, nil)
	fake.presence = true
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	for _, q := range append(append([]string(nil), before...), query) {
		fake.mu.Lock()
		fake.raw = 0
		fake.mu.Unlock()
		if code, got, body := detectedLevelQuery(t, p, q, end, end, time.Minute); code != http.StatusOK {
			t.Fatalf("status %d %.200s", code, body)
		} else if q == query {
			fake.mu.Lock()
			defer fake.mu.Unlock()
			return got, fake.raw > 0
		}
	}
	return nil, false
}

// A verdict that a window holds no risky line is remembered per tenant, query and
// base. Two bases in one window: the first is clean, the second holds a line the
// pipes read differently from Loki (two level keys; the grouping has no level
// label, so only the level probes see it). Asking the clean one first
// must not clear the second: it goes to the raw-row evaluator, with the answer it
// gets when it is asked alone.
// conformance: semantics/json-metric-detected-level-derivation
func TestRiskVerdictOfOneBaseDoesNotClearAnother(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	end := s0.Add(6 * time.Minute)
	rows := append(scopedRows(s0, "api"), scopedRows(s0, "web", pushdownRow{msg: `{"level":"info","level":"warn"}`})...)
	const clean = `sum by (detected_level) (count_over_time({app="api"} | json | drop __error__ [5m]))`
	const risky = `sum by (detected_level) (count_over_time({app="web"} | json | drop __error__ [5m]))`
	alone, aloneRaw := askScoped(t, rows, end, nil, risky)
	after, afterRaw := askScoped(t, rows, end, []string{clean}, risky)
	if !aloneRaw || !afterRaw || !reflect.DeepEqual(alone, after) {
		t.Fatalf("risky base asked alone: raw=%v %v; after the clean base: raw=%v %v", aloneRaw, alone, afterRaw, after)
	}
}

// The same for the filters that reject unparsed lines: a window is clean for a
// filter on pipeline and risky for one on service_version (a row stores
// service.version while its body has no such key), over the same fields.
// conformance: semantics/json-metric-detected-level-derivation
func TestRiskVerdictOfOneFilterDoesNotClearAnother(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	end := s0.Add(6 * time.Minute)
	rows := scopedRows(s0, "api", pushdownRow{msg: `{"pipeline":"logs/loki"}`, vl: map[string]string{"service.version": "0.95.0"}, loki: map[string]string{"service_version": "0.95.0"}})
	const clean = `sum by (pipeline, service_version) (count_over_time({app="api"} | json | pipeline!="" [5m]))`
	const risky = `sum by (pipeline, service_version) (count_over_time({app="api"} | json | service_version!="" [5m]))`
	alone, aloneRaw := askScoped(t, rows, end, nil, risky)
	after, afterRaw := askScoped(t, rows, end, []string{clean}, risky)
	if !aloneRaw || !afterRaw || !reflect.DeepEqual(alone, after) {
		t.Fatalf("risky filter asked alone: raw=%v %v; after the clean one: raw=%v %v", aloneRaw, alone, afterRaw, after)
	}
}
