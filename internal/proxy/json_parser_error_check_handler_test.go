package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// jsonMarkFakeVL plays VictoriaLogs for the JSON parser error check: a marked stats query answers its group and,
// when the fixture holds a line VictoriaLogs cannot vouch for, a marked group in that line's bucket; an unmarked one
// answers the group alone; a marked raw-row read carries the mark on that line; the lookup answers that line, the way
// the fixture says (Loki fails on it, Loki parses it, VictoriaLogs fails).
type jsonMarkFakeVL struct {
	mu      sync.Mutex
	line    string // the marked line's _msg; "" for none
	at      time.Time
	lookup  int // 0 answer, 1 VictoriaLogs fails, 2 answer a broken row, a row outside the window and then the line
	buckets int // marked buckets, one a minute from at; 0 is 1
	calls   []string
	lookups int
}

func newJSONMarkFakeVL(t *testing.T, fake *jsonMarkFakeVL) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		query := r.Form.Get("query")
		fake.mu.Lock()
		defer fake.mu.Unlock()
		if r.URL.Path == "/health" || r.URL.Path == "/metrics" {
			return
		}
		fake.calls = append(fake.calls, r.URL.Path+" "+query)
		marked := strings.Contains(query, jsonValidField)
		row := func(extra map[string]string) []byte {
			out := map[string]string{"_time": fake.at.UTC().Format(time.RFC3339Nano), "_stream": `{app="pay"}`, "app": "pay", "_msg": fake.line}
			for k, v := range extra {
				out[k] = v
			}
			b, _ := json.Marshal(out)
			return append(b, '\n')
		}
		switch r.URL.Path {
		case "/select/logsql/stats_query_range", "/select/logsql/stats_query":
			kind, point := "matrix", `"values":[[%d,"%s"]]`
			if r.URL.Path == "/select/logsql/stats_query" {
				kind, point = "vector", `"value":[%d,"%s"]`
			}
			pt := func(ts int64, v string) string {
				return strings.Replace(strings.Replace(point, "%d", strconv.FormatInt(ts, 10), 1), "%s", v, 1)
			}
			bucket := fake.at.Truncate(time.Minute).Unix()
			group := `{"metric":{"__name__":"c","_stream":"{app=\"pay\"}"},` + pt(bucket, "2") + `}`
			if marked {
				group = `{"metric":{"__name__":"c","_stream":"{app=\"pay\"}","` + jsonValidField + `":"1"},` + pt(bucket, "2") + `}`
				for b := 0; fake.line != "" && (b == 0 || b < fake.buckets); b++ {
					group += `,{"metric":{"__name__":"c","_stream":"{app=\"pay\"}","` + jsonValidField + `":""},` + pt(bucket+int64(60*b), "1") + `}`
				}
			}
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"` + kind + `","result":[` + group + `]}}`))
		case "/select/logsql/query":
			if strings.Contains(query, "| filter NOT "+jsonValidField+":*") {
				fake.lookups++
				if fake.lookup == 1 {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				if fake.lookup == 2 {
					_, _ = w.Write([]byte("not json\n"))
					_, _ = w.Write(row(map[string]string{"_time": fake.at.Add(-24 * time.Hour).UTC().Format(time.RFC3339Nano)}))
				}
				if fake.line != "" {
					_, _ = w.Write(row(nil))
				}
				return
			}
			if strings.Contains(query, "| limit ") && fake.line != "" {
				extra := map[string]string{}
				if strings.Contains(query, jsonErrorField) {
					extra[jsonErrorField] = "1"
				}
				_, _ = w.Write(row(extra))
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func (f *jsonMarkFakeVL) snapshot() (calls []string, lookups int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...), f.lookups
}

// End to end through the handlers, on the stats route (step equal to the range) and the raw-row route (an instant
// query): a metric whose lines all parse sends its reads with the mark and nothing else, and a repeat sends the same
// reads; a line Loki's JSON parser rejects costs one lookup and answers
// Loki's 400 text, again on a repeat (an error is never cached); a mark Loki parses (`{ }`) costs a
// lookup and the stats answer read again unmarked, with the metric's own values; a lookup VictoriaLogs does not
// answer answers Loki's failed-backend 500, never a cached 200.
//
// conformance: semantics/json-parser-error-under-wrappers, semantics/pipeline-error-text-parity, status-400, loki_api_v1_query_range, loki_api_v1_query
func TestJSONParserErrorCheckHandlers(t *testing.T) {
	at := time.Unix(1700000000, 0)
	bad := at.Add(3*time.Minute + 7*time.Second)
	const query = `sort(rate({app="pay"} | json [1m]))`
	rangeReq := func() *http.Request {
		v := url.Values{"query": {query}, "start": {strconv.FormatInt(at.UnixNano(), 10)}, "end": {strconv.FormatInt(at.Add(10*time.Minute).UnixNano(), 10)}, "step": {"60"}}
		return httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+v.Encode(), nil)
	}
	instantReq := func() *http.Request {
		v := url.Values{"query": {query}, "time": {strconv.FormatInt(bad.Add(30*time.Second).UnixNano(), 10)}}
		return httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+v.Encode(), nil)
	}
	serve := func(p *Proxy, req *http.Request) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		if strings.Contains(req.URL.Path, "query_range") {
			p.handleQueryRange(rec, req)
		} else {
			p.handleQuery(rec, req)
		}
		return rec
	}
	for _, route := range []struct {
		name string
		req  func() *http.Request
	}{{"stats", rangeReq}, {"raw rows", instantReq}} {
		t.Run(route.name+" clean", func(t *testing.T) {
			fake := &jsonMarkFakeVL{at: bad}
			p := newTestProxy(t, newJSONMarkFakeVL(t, fake).URL)
			first := serve(p, route.req())
			if first.Code != http.StatusOK {
				t.Fatalf("status %d: %s", first.Code, first.Body.String())
			}
			calls, lookups := fake.snapshot()
			if lookups != 0 || len(calls) == 0 {
				t.Fatalf("%d lookups, calls %q", lookups, calls)
			}
			for _, c := range calls {
				// Every read that unpacks the line carries the mark; the level-presence probe reads stored fields
				// and the line's text, not its JSON.
				if strings.Contains(c, "unpack_json") && !strings.Contains(c, jsonValidField) {
					t.Errorf("an unmarked read: %s", c)
				}
			}
			if again := serve(p, route.req()); again.Code != http.StatusOK || again.Body.String() != first.Body.String() {
				t.Errorf("repeat: %d %s", again.Code, again.Body.String())
			}
			// The repeat sends what the first request sent and nothing more (the route keeps no response for it
			// on main either): the mark adds no request on either.
			repeat, lookups := fake.snapshot()
			if lookups != 0 || len(repeat) > 2*len(calls) {
				t.Fatalf("repeat: %d lookups, calls %q", lookups, repeat)
			}
			for i, c := range repeat[len(calls):] {
				if c != calls[i] {
					t.Errorf("the repeat sent %s, the first request %s", c, calls[i])
				}
			}
		})
		t.Run(route.name+" Loki fails", func(t *testing.T) {
			fake := &jsonMarkFakeVL{at: bad, line: "plain text line"}
			p := newTestProxy(t, newJSONMarkFakeVL(t, fake).URL)
			for i := 1; i <= 2; i++ {
				rec := serve(p, route.req())
				if rec.Code != http.StatusBadRequest {
					t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
				}
				text := rec.Body.String()
				if !strings.HasPrefix(text, `pipeline error: 'JSONParserErr' for series: '{__error__="JSONParserErr", __error_details__="`) || !strings.Contains(text, `app="pay"`) {
					t.Errorf("text %q", text)
				}
				if _, lookups := fake.snapshot(); lookups != i {
					t.Errorf("request %d: %d lookups, want %d (an error is not cached)", i, lookups, i)
				}
			}
		})
		t.Run(route.name+" VictoriaLogs fails the lookup", func(t *testing.T) {
			fake := &jsonMarkFakeVL{at: bad, line: "plain text line", lookup: 1}
			p := newTestProxy(t, newJSONMarkFakeVL(t, fake).URL)
			for i := 1; i <= 2; i++ {
				if rec := serve(p, route.req()); rec.Code != http.StatusInternalServerError {
					t.Fatalf("request %d: status %d: %s", i, rec.Code, rec.Body.String())
				}
			}
			if _, lookups := fake.snapshot(); lookups < 2 {
				t.Errorf("%d lookups: the failed answer was cached", lookups)
			}
		})
	}
	t.Run("stats mark Loki parses", func(t *testing.T) {
		clean := &jsonMarkFakeVL{at: bad}
		want := serve(newTestProxy(t, newJSONMarkFakeVL(t, clean).URL), rangeReq())
		fake := &jsonMarkFakeVL{at: bad, line: `{ }`}
		p := newTestProxy(t, newJSONMarkFakeVL(t, fake).URL)
		got := serve(p, rangeReq())
		if got.Code != http.StatusOK || got.Body.String() != want.Body.String() {
			t.Fatalf("status %d: %s\nwant %s", got.Code, got.Body.String(), want.Body.String())
		}
		calls, lookups := fake.snapshot()
		marked, unmarked := 0, 0
		for _, c := range calls {
			if !strings.HasPrefix(c, "/select/logsql/stats_query") {
				continue
			}
			if strings.Contains(c, jsonValidField) {
				marked++
			} else {
				unmarked++
			}
		}
		// Each marked stats read (the metric's, and the level derivation's read of a stream without a level label)
		// is read again unmarked once; the lookup of their shared span is sent once.
		if lookups != 1 || marked == 0 || unmarked != marked {
			t.Errorf("%d lookups, %d marked and %d unmarked reads, want 1 lookup and one unmarked read per marked one: %q", lookups, marked, unmarked, calls)
		}
	})
}

// The lookup budget is the request's: marks in many buckets that Loki parses cost eight lookups, and the answer is
// sent without being kept. A lookup that answers rows the window does not hold reads past them. A binary operation
// of two checked operands, an unwrap operand beside a JSON one and a nan-only unwrap check join one check.
//
// conformance: semantics/json-parser-error-under-wrappers, loki_api_v1_query_range
func TestJSONParserErrorCheckBudgetAndShapes(t *testing.T) {
	at := time.Unix(1700000000, 0)
	bad := at.Add(3*time.Minute + 7*time.Second)
	rangeReq := func(q string) *http.Request {
		v := url.Values{"query": {q}, "start": {strconv.FormatInt(at.UnixNano(), 10)}, "end": {strconv.FormatInt(at.Add(20*time.Minute).UnixNano(), 10)}, "step": {"60"}}
		return httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+v.Encode(), nil)
	}
	serve := func(p *Proxy, req *http.Request) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		p.handleQueryRange(rec, req)
		return rec
	}
	t.Run("budget", func(t *testing.T) {
		fake := &jsonMarkFakeVL{at: bad, line: `{ }`, buckets: 12}
		p := newTestProxy(t, newJSONMarkFakeVL(t, fake).URL)
		for i := 1; i <= 2; i++ {
			if rec := serve(p, rangeReq(`sort(rate({app="pay"} | json [1m]))`)); rec.Code != http.StatusOK {
				t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}
			if _, lookups := fake.snapshot(); lookups != i*jsonMaxLookups {
				t.Fatalf("request %d: %d lookups, want the budget %d per request (an uncertain answer is not cached)", i, lookups, i*jsonMaxLookups)
			}
		}
	})
	t.Run("lookup reads past rows outside the window", func(t *testing.T) {
		fake := &jsonMarkFakeVL{at: bad, line: "plain text line", lookup: 2}
		p := newTestProxy(t, newJSONMarkFakeVL(t, fake).URL)
		if rec := serve(p, rangeReq(`sort(rate({app="pay"} | json [1m]))`)); rec.Code != http.StatusBadRequest {
			t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
	})
	for _, q := range []string{
		`sort(rate({app="pay"} | JSON [1m]))`,
		`sort(sum by (detected_level) (count_over_time({app="pay"} | json [1m])))`,
		`max by (detected_level) (rate({app="pay"} | json [1m]))`,
		`rate({app="pay"} | json [1m]) + rate({app="pay"} | json [1m])`,
		`sum by (app) (sum_over_time({app="pay"} | logfmt | unwrap n [1m])) + sum by (app) (rate({app="pay"} | json [1m]))`,
		`sum by (app) (sum_over_time({app="pay"} | logfmt | unwrap n | __error__="" [1m])) + sum by (app) (rate({app="pay"} | json [1m]))`,
	} {
		t.Run(q, func(t *testing.T) {
			fake := &jsonMarkFakeVL{at: bad, line: "plain text line"}
			p := newTestProxy(t, newJSONMarkFakeVL(t, fake).URL)
			rec := serve(p, rangeReq(q))
			if rec.Code != http.StatusBadRequest || !strings.HasPrefix(rec.Body.String(), `pipeline error: 'JSONParserErr'`) {
				t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}
		})
	}
}

// The memo of marked texts starts over when full, and keeps the stats and raw-row forms apart.
//
// conformance: semantics/json-parser-error-under-wrappers
func TestJSONMarkMemo(t *testing.T) {
	m := &jsonMarkMemo{entries: map[string]jsonMarkedText{}, max: 2}
	calls := 0
	mark := func(q string) (string, bool) { calls++; return q + "!", true }
	for _, q := range []string{"a", "b", "a", "c", "a"} {
		if got, ok := m.mark(q, false, mark); !ok || got != q+"!" {
			t.Fatalf("%s: %q %v", q, got, ok)
		}
	}
	if calls != 4 {
		t.Errorf("%d marks, want 4 (a, b, c, then a again after the memo started over)", calls)
	}
	if _, _ = m.mark("a", true, mark); calls != 5 {
		t.Errorf("the raw-row form shared the stats form's entry")
	}
}
