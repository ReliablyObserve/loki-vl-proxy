package proxy

import (
	"context"
	"encoding/json"
	"fmt"
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

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// fakeDeleteVL is a VictoriaLogs stand-in for the delete task API: it records every call, answers run_task with
// a task id, lists the tasks it was given, and stops one.
type fakeDeleteVL struct {
	mu     sync.Mutex
	calls  []deleteVLCall
	tasks  string // the active_tasks answer
	status int    // a status for every call, 0 for success
}

type deleteVLCall struct {
	path, method, filter, taskID, account, project string
}

func (f *fakeDeleteVL) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	f.mu.Lock()
	f.calls = append(f.calls, deleteVLCall{path: r.URL.Path, method: r.Method, filter: r.FormValue("filter"), taskID: r.FormValue("task_id"),
		account: r.Header.Get("AccountID"), project: r.Header.Get("ProjectID")})
	tasks, status := f.tasks, f.status
	f.mu.Unlock()
	if status != 0 {
		http.Error(w, "requests to /delete/* are disabled; pass -delete.enable command-line flag", status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case vlDeleteRunTaskPath:
		_, _ = w.Write([]byte(`{"task_id":"1794790000000000000"}`))
	case vlDeleteActiveTasksPath:
		if tasks == "" {
			tasks = "[]"
		}
		_, _ = w.Write([]byte(tasks))
	case vlDeleteStopTaskPath:
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	default:
		_, _ = w.Write([]byte(`{}`))
	}
}

func (f *fakeDeleteVL) taken() []deleteVLCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.calls
	f.calls = nil
	return out
}

func newDeleteAPIProxy(t *testing.T, vl *fakeDeleteVL, opts ...testProxyOption) (*Proxy, string) {
	t.Helper()
	backend := httptest.NewServer(vl)
	t.Cleanup(backend.Close)
	base := []testProxyOption{withBackendURL(backend.URL), func(c *Config) {
		c.DeleteAPIEnabled = true
		c.DeleteAPIAuthToken = testDeleteToken
		c.TenantMap = map[string]TenantMapping{"team-a": {AccountID: "100", ProjectID: "1"}, "team-b": {AccountID: "200", ProjectID: "0"}}
	}}
	p := newTestProxyWithOptions(t, append(base, opts...)...)
	mux := http.NewServeMux()
	p.RegisterProxyRoutes(mux)
	front := httptest.NewServer(shippedChain(mux))
	t.Cleanup(front.Close)
	return p, strings.TrimPrefix(front.URL, "http://")
}

// deleteAPIRowsServedOtherwise are the recorded rows Loki answers and the proxy answers otherwise, each with the
// reason: a query Loki accepts whose rows VictoriaLogs cannot delete exactly, and the list, which VictoriaLogs keeps
// no record for. The proxy answers Loki's 500 for them.
var deleteAPIRowsServedOtherwise = map[string]string{
	"r1705": "| json: a parser is a pipe, and a delete task takes a filter",
	"r1711": "| logfmt | level=...: a filter on a parsed label needs the parser's pipe",
	"r1737": "| json | level=...: a filter on a parsed label needs the parser's pipe",
	"r1761": "| json: a parser is a pipe",
	"r1736": "|~ \"(\": Loki accepts a line filter regexp that does not compile, which cannot be sent to VictoriaLogs",
	"r1715": "the list: VictoriaLogs keeps no record of a request's query and range",
	"r1716": "the list: VictoriaLogs keeps no record of a request's query and range",
	"r1735": "the list: VictoriaLogs keeps no record of a request's query and range",
	"r1763": "a start of 9999999999 wraps to 1702 in nanoseconds: Loki deletes back to then, the proxy serves no such request",
	"r1768": "a start of 9999999999 wraps to 1702 in nanoseconds: Loki deletes back to then, the proxy serves no such request",
	"r1765": "a start before 1678, which VictoriaLogs' nanosecond times cannot hold",
}

// TestDeleteAPIAnswersLikeLoki replays every recorded delete API row (r1201 and r1700 and later, recorded from a
// Loki that serves the API) against the proxy with the delete API on and its token added: a Loki error is answered
// byte for byte (status, Content-Type, nosniff, body) with no VictoriaLogs call, a Loki success is a 204, and the
// rows of deleteAPIRowsServedOtherwise are Loki's 500.
//
// conformance: semantics/delete-api-via-victorialogs, loki_api_v1_delete
func TestDeleteAPIAnswersLikeLoki(t *testing.T) {
	vl := &fakeDeleteVL{}
	_, addr := newDeleteAPIProxy(t, vl)
	rows := 0
	for _, row := range readLokiErrorAnswers(t) {
		if row.Route != "/loki/api/v1/delete" {
			continue
		}
		rows++
		ask := row
		ask.Headers = append(append([]string(nil), row.Headers...), "X-Delete-Token: "+testDeleteToken)
		got := ask.replay(t, addr)
		calls := vl.taken()
		switch reason, otherwise := deleteAPIRowsServedOtherwise[row.ID]; {
		case otherwise:
			if got.Status != http.StatusInternalServerError || got.Body != "Internal Server Error\n" {
				t.Errorf("%s (%s): %s, want Loki's 500", row.ID, reason, summarizeReply(got))
			}
		case row.Loki.Status == http.StatusNoContent:
			if got.Status != http.StatusNoContent || got.Body != "" || got.ContentType != "" {
				t.Errorf("%s %s %s: %s, want Loki's 204", row.ID, row.Method, row.Target, summarizeReply(got))
			}
		case row.Loki.Status >= 400:
			want := row.Loki
			if want.Nosniff == "" && got.Nosniff == "nosniff" && want.Status == http.StatusMethodNotAllowed {
				want.Nosniff = "nosniff" // the proxy's security headers on Loki's bodyless 405
			}
			if got != want {
				t.Errorf("%s %s %s:\n got  %s\n want %s (Loki)", row.ID, row.Method, row.Target, summarizeReply(got), summarizeReply(row.Loki))
			}
			for _, c := range calls {
				if c.path == vlDeleteRunTaskPath || c.path == vlDeleteStopTaskPath {
					t.Errorf("%s: an error Loki answers reached VictoriaLogs' %s", row.ID, c.path)
				}
			}
		}
	}
	if rows < 40 {
		t.Fatalf("%d recorded delete rows, want the recorded set", rows)
	}
}

// A create request starts one VictoriaLogs delete task with the tenant's headers and a filter of the request's
// window (both ends included) and of the rows the same log query reads on the read path.
//
// conformance: semantics/delete-api-via-victorialogs
func TestDeleteCreateStartsTheTenantsTask(t *testing.T) {
	vl := &fakeDeleteVL{}
	p, addr := newDeleteAPIProxy(t, vl)
	for _, tc := range []struct{ query, start, end, contains string }{
		{`{app="nginx"}`, "1794567075", "1794570675", `_time:[2026-11-13T10:51:15Z, 2026-11-13T11:51:15Z]`},
		{`{app="nginx"} |= "error" != "debug"`, "2026-11-13T10:11:15.5Z", "2026-11-13T12:11:15+01:00", `_time:[2026-11-13T10:11:15.5Z, 2026-11-13T11:11:15Z]`},
		{`{}`, "1794567075", "1794570675", `_time:[2026-11-13T10:51:15Z, 2026-11-13T11:51:15Z]`},
	} {
		target := "/loki/api/v1/delete?" + url.Values{"query": {tc.query}, "start": {tc.start}, "end": {tc.end}}.Encode()
		row := lokiErrorAnswer{ID: tc.query, Method: http.MethodPost, Target: target, Headers: []string{"X-Scope-OrgID: team-a", "X-Delete-Token: " + testDeleteToken}}
		got := row.replay(t, addr)
		calls := vl.taken()
		if got.Status != http.StatusNoContent {
			t.Errorf("%s: %s, want 204", tc.query, summarizeReply(got))
			continue
		}
		var run *deleteVLCall
		for i := range calls {
			if calls[i].path == vlDeleteRunTaskPath {
				run = &calls[i]
			}
		}
		if run == nil || run.method != http.MethodPost || run.account != "100" || run.project != "1" || !strings.Contains(run.filter, tc.contains) {
			t.Errorf("%s: VictoriaLogs calls %+v, want one POST run_task for 100:1 with %s", tc.query, calls, tc.contains)
			continue
		}
		// The rows a delete removes are the rows the read path reads: the filter after the window is the query's
		// read translation.
		if tc.query != `{}` {
			read, err := p.translateQueryWithContext(context.Background(), tc.query)
			if err != nil || !strings.Contains(run.filter, "("+read+")") {
				t.Errorf("%s: filter %q, read translation %q (%v)", tc.query, run.filter, read, err)
			}
		}
	}
}

// A cancel stops a running task only when it belongs to the request's tenant alone; another tenant's task, an
// unknown id and an id that is not a task id answer Loki's 404 and stop nothing; no id is Loki's 204 with no
// VictoriaLogs call.
//
// conformance: semantics/delete-api-via-victorialogs
func TestDeleteCancelStopsOnlyTheTenantsTask(t *testing.T) {
	vl := &fakeDeleteVL{tasks: `[{"task_id":"111","tenant_ids":[{"account_id":100,"project_id":1}],"filter":"x","start_time":"2026-11-16T00:00:00Z"},` +
		`{"task_id":"222","tenant_ids":[{"account_id":200,"project_id":0}],"filter":"x","start_time":"2026-11-16T00:00:00Z"},` +
		`{"task_id":"333","tenant_ids":[{"account_id":100,"project_id":1},{"account_id":200,"project_id":0}],"filter":"x","start_time":"2026-11-16T00:00:00Z"}]`}
	_, addr := newDeleteAPIProxy(t, vl)
	notFound := lokiErrorReply{Status: 404, ContentType: "text/plain; charset=utf-8", Nosniff: "nosniff", Body: "could not find delete request with given id\n"}
	for _, tc := range []struct {
		id       string
		status   int
		stopped  string
		vlCalled bool
	}{
		{"111", 204, "111", true},
		{"222", 404, "", true},
		{"333", 404, "", true},
		{"444", 404, "", true},
		{"11a", 404, "", false},
		{"", 204, "", false},
	} {
		target := "/loki/api/v1/delete"
		if tc.id != "" {
			target += "?request_id=" + tc.id
		}
		row := lokiErrorAnswer{ID: tc.id, Method: http.MethodDelete, Target: target, Headers: []string{"X-Scope-OrgID: team-a", "X-Delete-Token: " + testDeleteToken}}
		got := row.replay(t, addr)
		calls := vl.taken()
		if got.Status != tc.status || (tc.status == 404 && got != notFound) {
			t.Errorf("cancel %q: %s, want %d", tc.id, summarizeReply(got), tc.status)
		}
		stopped := ""
		for _, c := range calls {
			if c.path == vlDeleteStopTaskPath {
				stopped = c.taskID
			}
		}
		if stopped != tc.stopped || (len(calls) > 0) != tc.vlCalled {
			t.Errorf("cancel %q: VictoriaLogs calls %+v, want stop %q (called %v)", tc.id, calls, tc.stopped, tc.vlCalled)
		}
	}
}

// What the proxy cannot scope or serve answers Loki's 500 and starts no task: a tenant-label proxy (a delete task
// takes no tenant stream filter), the global tenant, and VictoriaLogs refusing the call (its text stays in the log).
//
// conformance: semantics/delete-api-via-victorialogs
func TestDeleteRefusalsStartNoTask(t *testing.T) {
	target := "/loki/api/v1/delete?" + url.Values{"query": {`{app="nginx"}`}, "start": {"1794567075"}, "end": {"1794570675"}}.Encode()
	internal := lokiErrorReply{Status: 500, ContentType: "text/plain; charset=utf-8", Nosniff: "nosniff", Body: "Internal Server Error\n"}
	ask := func(addr, org string) lokiErrorReply {
		row := lokiErrorAnswer{ID: org, Method: http.MethodPost, Target: target, Headers: []string{"X-Scope-OrgID: " + org, "X-Delete-Token: " + testDeleteToken}}
		return row.replay(t, addr)
	}
	// Tenant-label mode: a tenant the label confines is refused; a tenant mapped to its own account is deleted from.
	vl := &fakeDeleteVL{}
	_, addr := newDeleteAPIProxy(t, vl, withTenantLabel("tenant"))
	if got := ask(addr, "production"); got != internal {
		t.Errorf("tenant-label mode: %s, want Loki's 500", summarizeReply(got))
	}
	for _, c := range vl.taken() {
		if c.path == vlDeleteRunTaskPath {
			t.Errorf("tenant-label mode started a task: %+v", c)
		}
	}
	if got := ask(addr, "team-a"); got.Status != http.StatusNoContent {
		t.Errorf("tenant-label mode, a mapped tenant: %s, want 204", summarizeReply(got))
	}
	if calls := vl.taken(); len(calls) != 1 || calls[0].account != "100" || calls[0].project != "1" {
		t.Errorf("tenant-label mode, a mapped tenant: VictoriaLogs calls %+v, want one task for 100:1", calls)
	}
	vl = &fakeDeleteVL{}
	_, addr = newDeleteAPIProxy(t, vl, func(c *Config) { c.AllowGlobalTenant = true })
	if got := ask(addr, "*"); got != internal {
		t.Errorf("global tenant: %s, want Loki's 500", summarizeReply(got))
	}
	for _, c := range vl.taken() {
		if c.path == vlDeleteRunTaskPath {
			t.Errorf("the global tenant started a task: %+v", c)
		}
	}
	vl = &fakeDeleteVL{status: http.StatusBadRequest}
	_, addr = newDeleteAPIProxy(t, vl)
	if got := ask(addr, "team-a"); got != internal || strings.Contains(got.Body, "delete.enable") {
		t.Errorf("VictoriaLogs refusing: %s, want Loki's 500 without VictoriaLogs' text", summarizeReply(got))
	}
}

// deleteRequestOf's checks in Loki's order, the time forms Loki reads and the ones it refuses, and max_interval,
// checked for a query with a filter only.
//
// conformance: semantics/delete-api-via-victorialogs
func TestDeleteRequestChecksInLokisOrder(t *testing.T) {
	now := time.Unix(1794790000, 0)
	for _, tc := range []struct {
		form url.Values
		msg  lokierr.Msg
	}{
		{url.Values{}, lokierr.MsgDeleteQueryNotSet},
		{url.Values{"query": {"{"}}, lokierr.MsgDeleteQueryInvalid},
		{url.Values{"query": {`sum(rate({a="b"}[1m]))`}, "start": {"x"}}, lokierr.MsgDeleteQueryInvalid},
		{url.Values{"query": {`{a="b"}`}}, lokierr.MsgDeleteStartNotSet},
		{url.Values{"query": {`{a="b"}`}, "start": {"abc"}, "end": {"abc"}}, lokierr.MsgDeleteTimeInvalid},
		{url.Values{"query": {`{a="b"}`}, "start": {"1794567075000"}}, lokierr.MsgDeleteTimeInvalid},
		{url.Values{"query": {`{a="b"}`}, "start": {"01794567075"}}, lokierr.MsgDeleteTimeInvalid},
		{url.Values{"query": {`{a="b"}`}, "start": {"+179456707"}}, lokierr.MsgDeleteTimeInvalid},
		{url.Values{"query": {`{a="b"}`}, "start": {"2026-11-13 10:11:15"}}, lokierr.MsgDeleteTimeInvalid},
		{url.Values{"query": {`{a="b"}`}, "start": {"1794567075"}, "end": {"0"}}, lokierr.MsgDeleteTimeInvalid},
		{url.Values{"query": {`{a="b"}`}, "start": {"1794567075"}, "end": {"1794567075"}}, lokierr.MsgDeleteStartNotBeforeEnd},
		{url.Values{"query": {`{a="b"}`}, "start": {"1794567075"}, "end": {"9999999999"}}, lokierr.MsgDeleteStartNotBeforeEnd},
		{url.Values{"query": {`{a="b"}`}, "start": {"2147483647"}}, lokierr.MsgDeleteStartNotBeforeEnd},
		// An RFC3339 time is not wrapped: a start in 3000 is after the end (r1764), an end in 3000 is in the future (r1767).
		{url.Values{"query": {`{a="b"}`}, "start": {"3000-01-01T00:00:00Z"}, "end": {"1794570675"}}, lokierr.MsgDeleteStartNotBeforeEnd},
		{url.Values{"query": {`{a="b"}`}, "start": {"1794567075"}, "end": {"3000-01-01T00:00:00Z"}}, lokierr.MsgDeleteEndInFuture},
		{url.Values{"query": {`{a="b"}`}, "start": {"1794567075"}, "end": {"1794800000"}}, lokierr.MsgDeleteEndInFuture},
		// A start of 9999999999 wraps to 1702, and Loki deletes back to then (r1763): the proxy serves no such request,
		// nor one that starts before 1678 (r1765).
		{url.Values{"query": {`{a="b"}`}, "start": {"9999999999"}}, lokierr.MsgInternal},
		{url.Values{"query": {`{a="b"}`}, "start": {"9999999999"}, "end": {"1794570675"}}, lokierr.MsgInternal},
		{url.Values{"query": {`{a="b"}`}, "start": {"1600-01-01T00:00:00Z"}, "end": {"1794570675"}}, lokierr.MsgInternal},
		{url.Values{"query": {`{a="b"} |= "x"`}, "start": {"1794567075"}, "end": {"1794570675"}, "max_interval": {"1d"}}, lokierr.MsgDeleteMaxIntervalInvalid},
		{url.Values{"query": {`{a="b"} | c="d"`}, "start": {"1794567075"}, "end": {"1794570675"}, "max_interval": {"0"}}, lokierr.MsgDeleteMaxIntervalInvalid},
		{url.Values{"query": {`{a="b"} |= "x"`}, "start": {"1794567075"}, "end": {"1794570675"}, "max_interval": {"1ms"}}, lokierr.MsgDeleteMaxIntervalInvalid},
		{url.Values{"query": {`{a="b"} |= "x"`}, "start": {"1794567075"}, "end": {"1794570675"}, "max_interval": {"-1h"}}, lokierr.MsgDeleteMaxIntervalInvalid},
		{url.Values{"query": {`{a="b"} |= "x"`}, "start": {"1794567075"}, "end": {"1794570675"}, "max_interval": {"1h30m"}}, lokierr.MsgDeleteMaxIntervalTooLong},
	} {
		_, err := deleteRequestOf(tc.form, now)
		e, ok := lokierr.As(err)
		if !ok || e.Msg() != tc.msg {
			t.Errorf("%v: %v, want %s", tc.form, err, tc.msg.ID())
		}
	}
	for _, form := range []url.Values{
		{"query": {`{a="b"}`}, "start": {"1794567075"}},
		{"query": {`{a="b"}`}, "start": {"2026-11-13T10:11:15.123Z"}, "end": {"2026-11-13T12:11:15+01:00"}},
		{"query": {`{a="b"} | json`}, "start": {"1794567075"}, "end": {"1794570675"}, "max_interval": {"abc"}},
		{"query": {`{a="b"}`}, "start": {"1794567075"}, "end": {"1794570675"}, "max_interval": {"abc"}},
		{"query": {`{a="b"} |= "x"`}, "start": {"1794567075"}, "end": {"1794570675"}, "max_interval": {"90s"}},
		{"query": {`{}`}, "start": {"1794567075"}, "end": {"1794570675"}},
		{"query": {`{a="b"}`}, "start": {"1969-12-31T00:00:00Z"}, "end": {"1794570675"}},
	} {
		if _, err := deleteRequestOf(form, now); err != nil {
			t.Errorf("%v: %v, want Loki's acceptance", form, err)
		}
	}
	if d, ok := deleteMaxInterval("1h30m"); !ok || d != 90*time.Minute {
		t.Errorf("1h30m: %v %v", d, ok)
	}
}

// FuzzDeleteRequest: reading a create request terminates with Loki's typed error or a request whose start is before
// its end, whatever its parameters.
func FuzzDeleteRequest(f *testing.F) {
	for _, seed := range [][4]string{
		{`{app="a"}`, "1794567075", "1794570675", ""}, {`{app="a"} |= "x"`, "1794567075", "1794570675", "1h30m"},
		{"{", "abc", "", ""}, {`{}`, "2026-11-13T10:11:15Z", "9999999999", "90s"}, {"", "", "", ""},
		{`{a="b"} | json`, "0000000000", "2147483647", "1h1h1h"}, {`rate({a="b"}[1m])`, "1", "2", "-1h"},
	} {
		f.Add(seed[0], seed[1], seed[2], seed[3])
	}
	now := time.Unix(1794790000, 0)
	f.Fuzz(func(t *testing.T, query, start, end, maxInterval string) {
		form := url.Values{"query": {query}, "start": {start}, "end": {end}, "max_interval": {maxInterval}}
		req, err := deleteRequestOf(form, now)
		if err != nil {
			// Loki's 400, or Loki's 500 for a time the proxy serves no request with.
			if e, ok := lokierr.As(err); !ok || e.Status() != http.StatusBadRequest && constructOf(e.Cause()) == "" {
				t.Fatalf("%v: %v is not Loki's 400 nor a construct the proxy does not serve", form, err)
			}
			return
		}
		// An accepted request deletes a window that ends after it starts and not after now, from 1678 on: never a
		// start that wrapped.
		if req.query == nil || !req.start.Before(req.end) || req.end.After(now) || req.start.Before(minNanosTime) || secondsWrap(start) {
			t.Fatalf("%v: accepted %+v", form, req)
		}
	})
}

// deleteFilterQueries are delete queries whose filter VictoriaLogs' parser is asked about.
var deleteFilterQueries = []string{
	`{app="nginx"}`, `{app="nginx", env=~"prod|staging"}`, `{app!="x", pod=~".+"}`, `{}`,
	`{app="nginx"} |= "error"`, `{app="nginx"} != "debug" |~ "time(out)?"`, `{app="nginx"} !~ "^GET"`,
	`{app="nginx"} |= "a" or "b"`, `{app="nginx"} |= ip("10.0.0.0/8")`, `{app="nginx"} |> "<_> error <_>"`,
	`{app="nginx"} | trace_id="abc"`, `{app="nginx"} | status >= 500`, `{app="nginx"} | _time="x"`,
	`{app="nginx", _TIME="x"}`, `{service_name="api"} |= "x"`,
}

// deleteFilterVLVerdicts holds VictoriaLogs' verdicts on the delete filters of deleteFilterQueries (test/oracle/vl
// roundtrip, v1.49.0 and v1.53.0): the delete task parses the filter and prints it again, so a filter must parse,
// and print and parse back to itself. LVP_RUN_ORACLES=1 records them again.
var deleteFilterVLVerdicts = filepath.Join("testdata", "delete_filter_vl_verdicts.jsonl")

type deleteFilterVerdict struct {
	Filter string                    `json:"filter"`
	V      map[string]map[string]any `json:"v"`
}

func deleteFiltersOf(t *testing.T) map[string]string {
	t.Helper()
	p := newTestProxy(t, "http://unused")
	out := map[string]string{}
	for _, q := range deleteFilterQueries {
		req, err := deleteRequestOf(url.Values{"query": {q}, "start": {"1794567075"}, "end": {"1794570675"}}, time.Now())
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		f, err := p.deleteFilter(context.Background(), req)
		if err != nil {
			continue // a query the delete API does not serve sends nothing
		}
		out[q] = f
	}
	// A window from before 1970, which Loki accepts (r1766).
	req, err := deleteRequestOf(url.Values{"query": {`{app="nginx"}`}, "start": {"1969-12-31T00:00:00Z"}, "end": {"1794570675"}}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if out["before 1970"], err = p.deleteFilter(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	return out
}

func recordDeleteFilterVerdicts(t *testing.T, filters map[string]string) {
	t.Helper()
	dir := filepath.Join("..", "..", "test", "oracle", "vl")
	work := t.TempDir()
	in := filepath.Join(work, "in.jsonl")
	var texts []string
	for _, f := range filters {
		texts = append(texts, f)
	}
	sort.Strings(texts)
	var b strings.Builder
	for i, f := range texts {
		line, _ := json.Marshal(map[string]string{"id": strconv.Itoa(i), "q": f})
		b.Write(line)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(in, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	rows := make([]deleteFilterVerdict, len(texts))
	for i, f := range texts {
		rows[i] = deleteFilterVerdict{Filter: f, V: map[string]map[string]any{}}
	}
	for _, v := range []string{"v1.49.0", "v1.53.0"} {
		modfile := ""
		if v == "v1.49.0" {
			modfile = "go.v1.49.0.mod"
		}
		bin, err := oraclerun.Build(dir, modfile, work, "vl-"+v)
		if err != nil {
			t.Fatal(err)
		}
		out := filepath.Join(work, v+".jsonl")
		if err := oraclerun.Run(bin, "roundtrip", "-in", in, "-out", out); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(out)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
			var x map[string]any
			if err := json.Unmarshal([]byte(line), &x); err != nil {
				t.Fatal(err)
			}
			i, _ := strconv.Atoi(fmt.Sprint(x["id"]))
			delete(x, "id")
			rows[i].V[v] = x
		}
	}
	var w strings.Builder
	for _, r := range rows {
		line, _ := json.Marshal(r)
		w.Write(line)
		w.WriteByte('\n')
	}
	if err := os.WriteFile(deleteFilterVLVerdicts, []byte(w.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestDeleteFiltersParseInVictoriaLogs: every filter the delete API sends is accepted by VictoriaLogs' parser on
// v1.49.0 and v1.53.0 and prints and parses back to itself (the delete task stores the printed filter), recorded with
// the VictoriaLogs oracle; a filter not in the recording fails until it is recorded.
//
// conformance: semantics/delete-api-via-victorialogs, vl-delete-tasks-tenant-and-time
func TestDeleteFiltersParseInVictoriaLogs(t *testing.T) {
	filters := deleteFiltersOf(t)
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordDeleteFilterVerdicts(t, filters)
	}
	raw, err := os.ReadFile(deleteFilterVLVerdicts)
	if err != nil {
		t.Fatalf("%v: record the verdicts with LVP_RUN_ORACLES=1", err)
	}
	recorded := map[string]deleteFilterVerdict{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var r deleteFilterVerdict
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatal(err)
		}
		recorded[r.Filter] = r
	}
	if len(filters) < len(deleteFilterQueries)-3 {
		t.Fatalf("only %d of %d delete queries wrote a filter", len(filters), len(deleteFilterQueries))
	}
	for q, f := range filters {
		r, ok := recorded[f]
		if !ok {
			t.Errorf("%s: filter %q has no recorded VictoriaLogs verdict: record them with LVP_RUN_ORACLES=1", q, f)
			continue
		}
		for _, v := range []string{"v1.49.0", "v1.53.0"} {
			if r.V[v]["accept"] != true || r.V[v]["ok"] != true {
				t.Errorf("%s: VictoriaLogs %s on %q: %v", q, v, f, r.V[v])
			}
		}
	}
}

// A multi-tenant header is Loki's 400 on create and cancel, with and without a tenant map, and reaches no delete
// task: a delete never runs over two tenants, nor over the default tenant a joined org could fall back to.
//
// conformance: semantics/delete-api-via-victorialogs
func TestDeleteMultiTenantHeaderStartsNoTask(t *testing.T) {
	want := lokiErrorReply{Status: 400, ContentType: "text/plain; charset=utf-8", Nosniff: "nosniff", Body: "multiple org IDs present\n"}
	for _, withMap := range []bool{true, false} {
		vl := &fakeDeleteVL{tasks: `[{"task_id":"111","tenant_ids":[{"account_id":0,"project_id":0}]}]`}
		var opts []testProxyOption
		if !withMap {
			opts = append(opts, func(c *Config) { c.TenantMap = nil })
		}
		_, addr := newDeleteAPIProxy(t, vl, opts...)
		for _, org := range []string{"team-a|team-b", "team-a|", "|team-b", "0|1"} {
			for _, target := range []string{
				"/loki/api/v1/delete?" + url.Values{"query": {`{app="x"}`}, "start": {"1794567075"}, "end": {"1794570675"}}.Encode(),
				"/loki/api/v1/delete?request_id=111",
			} {
				method := http.MethodPost
				if strings.Contains(target, "request_id") {
					method = http.MethodDelete
				}
				row := lokiErrorAnswer{ID: org, Method: method, Target: target, Headers: []string{"X-Scope-OrgID: " + org, "X-Delete-Token: " + testDeleteToken}}
				if got := row.replay(t, addr); got != want {
					t.Errorf("map %v, %q, %s: %s, want Loki's 400", withMap, org, method, summarizeReply(got))
				}
				if calls := vl.taken(); len(calls) != 0 {
					t.Errorf("map %v, %q, %s: VictoriaLogs calls %+v", withMap, org, method, calls)
				}
			}
		}
	}
}

// The rows a delete removes are the rows the read path reads: for every query of deleteFilterQueries the delete API
// serves, the filter after the time window is the query's read translation under the same request.
//
// conformance: semantics/delete-api-via-victorialogs
func TestDeleteFilterIsTheReadTranslation(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	served := 0
	for _, q := range deleteFilterQueries {
		req, err := deleteRequestOf(url.Values{"query": {q}, "start": {"1794567075"}, "end": {"1794570675"}}, time.Now())
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		ctx := context.WithValue(context.Background(), origRequestKey, httptest.NewRequest(http.MethodPost, "/loki/api/v1/delete", nil))
		f, err := p.deleteFilter(ctx, req)
		if err != nil {
			continue
		}
		served++
		read, err := p.translateQueryWithContext(ctx, q)
		if err != nil {
			t.Errorf("%s: the read path refuses the query (%v) the delete serves", q, err)
			continue
		}
		window := "_time:[" + time.Unix(1794567075, 0).UTC().Format(time.RFC3339Nano) + ", " + time.Unix(1794570675, 0).UTC().Format(time.RFC3339Nano) + "] "
		if want := window + "(" + read + ")"; f != want {
			t.Errorf("%s:\n delete %s\n read   %s", q, f, want)
		}
	}
	if served < len(deleteFilterQueries)-3 {
		t.Fatalf("the delete API served %d of %d queries", served, len(deleteFilterQueries))
	}
}
