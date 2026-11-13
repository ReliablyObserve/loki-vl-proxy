package proxy

import (
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// The mixed-failure gate. TestErrorLayer_NoVictoriaLogsLeak fails every call of
// a route, so it cannot see a route whose first reads succeed and a later one
// fails: the route answers from what it has read and the failed call is lost.
// Here each distinct call of a route (its path, query and field) fails in turn
// while every other call gets a valid answer, with rows or without, in the
// modes that matter to a read that already has an answer: a 500, a body that is
// not VictoriaLogs JSON and a body cut off. Loki answers a failed backend call
// of any part of a query with an error, so a 200 here is a leak, unless the
// call is a documented best-effort extra (errLayerBestEffort).
//
// Cells that still differ are listed in conformance/registry/error-layer-known-leaks.json
// under "<route> mixed:<path>#<n>:<rows|empty>:<mode>", n being the call's
// place among the calls of that path in query order. ERROR_LAYER_RECORD_MIXED=<file>
// writes the current list of this gate (merge it into the shared list).

var errLayerMixedModes = []string{"vl500", "malformed", "truncated"}

// errLayerBestEffortCall is a call whose failure the route answers 200 on purpose, with the reason. Each is an
// alternate source for the same answer, a shortcut to a read the route makes next, or an extra read next to a
// required read that already fails the request on any error: what the call adds is lost. The gate holds each entry to a cell that still answers 200.
type errLayerBestEffortCall struct {
	routes []string
	// prefix and contains select the call by its signature (path, query and field).
	prefix, contains string
	reason           string
	// optional: the entry may stop matching a cell, because another fix closes it.
	optional bool
}

var errLayerBestEffort = []errLayerBestEffortCall{
	{
		routes: []string{"query_range_rate"},
		prefix: "stats_query_range ",
		reason: "a failed stats read falls back to the per-row evaluator, which answers the same query from the rows",
	},
	{
		routes: []string{"detected_labels", "detected_field_values", "detected_field_values_parser", "detected_field_values_field"},
		prefix: "streams ",
		reason: "the stored stream lookup falls back to the sampled scan, which reads the same labels",
	},
	{
		routes: []string{"detected_labels", "detected_field_values", "detected_field_values_parser", "detected_field_values_field"},
		prefix: "query ", contains: "| sort by (_time desc)",
		reason: "the sampled label scan is the fallback of the stored lookup, its relaxed candidate answers when the strict one fails, and it supplies level and service_name when no stored label holds them (a caller asking for those fails on it)",
	},
}

func (e errLayerBestEffortCall) matches(route, sig string) bool {
	if !strings.HasPrefix(sig, e.prefix) || !strings.Contains(sig, e.contains) {
		return false
	}
	for _, r := range e.routes {
		if r == route {
			return true
		}
	}
	return false
}

// errLayerAnswer is a response body without its query statistics, which differ from run to run.
func errLayerAnswer(body string) string {
	var doc map[string]any
	if json.Unmarshal([]byte(body), &doc) != nil {
		return body
	}
	if data, ok := doc["data"].(map[string]any); ok {
		delete(data, "stats")
	}
	out, err := json.Marshal(doc)
	if err != nil {
		return body
	}
	return string(out)
}

// errLayerCallSig names one call of a route: its path, query and field, without
// the time range.
func errLayerCallSig(r *http.Request, raw []byte) string {
	v := r.URL.Query()
	if form, err := url.ParseQuery(string(raw)); err == nil {
		for k, vs := range form {
			v[k] = append(v[k], vs...)
		}
	}
	sig := strings.TrimPrefix(r.URL.Path, "/select/logsql/") + " " + v.Get("query")
	if f := v.Get("field"); f != "" {
		sig += " field=" + f
	}
	return sig
}

// recordCall notes the call and reports whether it must be answered with a
// valid body: in a mixed-failure cell every call but the failing one succeeds.
func (b *errLayerBackend) recordCall(r *http.Request, raw []byte) bool {
	sig := errLayerCallSig(r, raw)
	b.mu.Lock()
	if b.seen != nil {
		b.seen[sig] = struct{}{}
		b.order = append(b.order, sig)
	}
	b.mu.Unlock()
	if b.failSig == "" && len(b.failContains) == 0 {
		return false
	}
	fails := b.failSig != "" && sig == b.failSig
	for _, part := range b.failContains {
		fails = fails || strings.Contains(sig, part)
	}
	if fails {
		b.failHits.Add(1)
		return false
	}
	return true
}

// serveOK answers a call with the endpoint's valid body, with entries or without.
func (b *errLayerBackend) serveOK(w http.ResponseWriter, r *http.Request) {
	n := 3
	if b.okBody == "empty" {
		n = 0
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(errLayerShapedBody(r.URL.Path, n)))
}

// errLayerCallsOf returns the distinct calls route rt makes against a backend
// that answers every call with rows, with none, or fails every call in a mode:
// the union covers the calls only a fallback makes.
func errLayerCallsOf(t *testing.T, rt errLayerRoute) []string {
	t.Helper()
	union := map[string]struct{}{}
	for _, b := range []*errLayerBackend{
		{failSig: "\x00none", okBody: "rows"},
		{failSig: "\x00none", okBody: "empty"},
		{mode: "vl500"},
		{mode: "malformed"},
	} {
		b.seen = map[string]struct{}{}
		runErrLayerBackendCell(t, rt, b)
		// A call that reaches the backend after the route has answered records under b.mu.
		b.mu.Lock()
		for sig := range b.seen {
			union[sig] = struct{}{}
		}
		b.mu.Unlock()
	}
	calls := make([]string, 0, len(union))
	for sig := range union {
		calls = append(calls, sig)
	}
	sort.Strings(calls)
	return calls
}

// conformance: semantics/error-layer-silent-success, semantics/error-layer-transport-text, semantics/error-layer-backend-text, semantics/error-layer-internal-go-text, semantics/error-layer-backend-failure-status
func TestErrorLayer_NoVictoriaLogsLeakMixed(t *testing.T) {
	known := errLayerKnownLeaks(t)
	recorded := map[string][]string{}
	var recordMu, usedMu sync.Mutex
	used := map[int]bool{}
	now := time.Now().Truncate(time.Minute)
	for _, rt := range errLayerRoutes(now) {
		rt := rt
		if rt.name == "tail" {
			continue
		}
		t.Run(rt.name, func(t *testing.T) {
			calls := errLayerCallsOf(t, rt)
			if len(calls) < 2 {
				t.Skip("one call: the all-fail gate covers it")
			}
			// What the route answers when every call works: a failed call whose route answers exactly this
			// had an alternate source for it, and nothing was lost.
			whole := map[string]string{}
			for _, body := range []string{"rows", "empty"} {
				o, _ := runErrLayerBackendCell(t, rt, &errLayerBackend{failSig: "\x00none", okBody: body})
				whole[body] = errLayerAnswer(o.body)
			}
			perPath := map[string]int{}
			for _, sig := range calls {
				path, _, _ := strings.Cut(sig, " ")
				name := path + "#" + string(rune('0'+perPath[path]))
				perPath[path]++
				t.Logf("%s = %.150q", name, sig)
				for _, body := range []string{"rows", "empty"} {
					for _, mode := range errLayerMixedModes {
						sig, name, body, mode := sig, name, body, mode
						cell := "mixed:" + name + ":" + body + ":" + mode
						t.Run(cell, func(t *testing.T) {
							backend := &errLayerBackend{mode: mode, failSig: sig, okBody: body, seen: map[string]struct{}{}}
							outcome, host := runErrLayerBackendCell(t, rt, backend)
							if backend.failHits.Load() == 0 {
								t.Skip("this combination never makes the call")
							}
							got := errLayerClasses(rt, mode, outcome, host)
							if outcome.status == http.StatusOK && errLayerAnswer(outcome.body) == whole[body] {
								// The same answer as with every call working: an alternate source served it.
								return
							}
							key := rt.name + " " + cell
							for i, e := range errLayerBestEffort {
								if !e.matches(rt.name, sig) {
									continue
								}
								// An entry stands for the cells where the call's failure is answered 200; a
								// cell where the route fails anyway is clean.
								if strings.Join(got, ",") == leakSilentSuccess {
									usedMu.Lock()
									used[i] = true
									usedMu.Unlock()
									return
								}
							}
							if len(got) > 0 {
								recordMu.Lock()
								recorded[key] = got
								recordMu.Unlock()
							}
							want := known[key]
							switch {
							case len(got) == 0 && len(want) == 0:
							case strings.Join(got, ",") == strings.Join(want, ","):
								t.Skipf("known leak %s: status %d, text %.160q", strings.Join(want, ", "), outcome.status, errLayerText(outcome))
							case len(want) == 0:
								t.Errorf("new error-layer leak %v: status %d, body %.300q", got, outcome.status, outcome.body)
							default:
								t.Errorf("known leak list for %q is stale: listed %v, now %v (status %d, text %.200q)", key, want, got, outcome.status, errLayerText(outcome))
							}
						})
					}
				}
			}
		})
	}
	for i, e := range errLayerBestEffort {
		if !used[i] && !e.optional {
			t.Errorf("best-effort call %q (%s) is stale: no cell answers 200 when it fails any more", e.prefix+e.contains, e.reason)
		}
	}
	if path := os.Getenv("ERROR_LAYER_RECORD_MIXED"); path != "" {
		writeErrLayerRecord(t, path, recorded)
	}
}
