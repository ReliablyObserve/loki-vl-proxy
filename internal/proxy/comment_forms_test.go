package proxy

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

const commentFormsLokiAnswers = "testdata/comment_forms_loki_answers.jsonl"

// commentFormQueries are a comment between each kind of function name and its parenthesis, the subquery-like range,
// the negative zero offsets and the query they mean.
var commentFormQueries = []string{
	"rate # c\n({app=\"a\"}[5m])",
	"RATE#c\n({app=\"a\"}[5m])",
	"count_over_time#c\n({app=\"a\"}[5m])",
	"quantile_over_time # c\n(0.5, {app=\"a\"} | logfmt | unwrap c [5m])",
	"vector # c\n(1)",
	"label_replace # c\n(rate({app=\"a\"}[5m]), \"d\", \"$1\", \"app\", \"(.*)\")",
	"{app=\"a\"} |= ip # c\n(\"1.2.3.4\")",
	"{app=\"a\"} | x = ip # c\n(\"1.2.3.4\")",
	"sum_over_time({app=\"a\"} | logfmt | unwrap bytes # c\n(c) [5m])",
	"sum_over_time({app=\"a\"} | logfmt | unwrap duration # c\n(c) [5m])",
	"sum_over_time({app=\"a\"} | logfmt | unwrap duration_seconds # c\n(c) [5m])",
	"sum without # c\n(app) (rate({app=\"a\"}[5m]))",
	"avg # c\n(rate({app=\"a\"}[5m]))",
	"rate({app=\"a\"}[0:1m])",
	"rate({app=\"a\"}[5m] offset -0s)",
	"rate({app=\"a\"}[5m] offset -0)",
	"rate({app=\"a\"}[5m] offset 0s)",
	"rate({app=\"a\"}[5m])",
}

// commentFormAnswer is Loki's answer to a query of commentFormQueries on query_range and query: the status, the error
// text of a rejection and the body of an answer.
type commentFormAnswer struct {
	Query         string          `json:"q"`
	RangeStatus   int             `json:"range_status"`
	RangeError    string          `json:"range_error,omitempty"`
	RangeBody     json.RawMessage `json:"range_body,omitempty"`
	InstantStatus int             `json:"instant_status"`
	InstantError  string          `json:"instant_error,omitempty"`
	InstantBody   json.RawMessage `json:"instant_body,omitempty"`
}

func commentFormForms(q string) (rangeForm, instantForm url.Values) {
	return url.Values{"query": {q}, "start": {"1700000600"}, "end": {"1700003600"}, "step": {"600"}},
		url.Values{"query": {q}, "time": {"1700003000"}}
}

func recordCommentFormAnswers(t *testing.T) {
	t.Helper()
	const base = int64(1700000000) * 1e9
	var entries []oraclerun.EngineEntry
	for i := int64(0); i < 600; i++ {
		entries = append(entries, oraclerun.EngineEntry{TS: base + i*7e9, Line: `level=info c=` + string(rune('1'+i%9)) + ` x=1.2.3.4 msg="x"`})
	}
	set := oraclerun.EngineDataSet{Streams: []oraclerun.EngineStream{{Labels: map[string]string{"app": "a", "pod": "p1"}, Entries: entries}}}
	for i, q := range commentFormQueries {
		id := string(rune('A' + i))
		rangeForm, instantForm := commentFormForms(q)
		set.Requests = append(set.Requests,
			oraclerun.EngineRequest{ID: "r" + id, Form: rangeForm.Encode()},
			oraclerun.EngineRequest{ID: "i" + id, Instant: true, Form: instantForm.Encode()})
	}
	answers, err := oraclerun.LokiEngine(filepath.Join("..", "..", "test", "oracle", "loki"), []oraclerun.EngineDataSet{set})
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for i, q := range commentFormQueries {
		id := string(rune('A' + i))
		r, in := answers["r"+id], answers["i"+id]
		line, _ := json.Marshal(commentFormAnswer{
			Query: q, RangeStatus: r.Status, RangeError: r.Error, RangeBody: r.Body,
			InstantStatus: in.Status, InstantError: in.Error, InstantBody: in.Body,
		})
		b.Write(line)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(commentFormsLokiAnswers, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func loadCommentFormAnswers(t *testing.T) map[string]commentFormAnswer {
	t.Helper()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordCommentFormAnswers(t)
	}
	f, err := os.Open(commentFormsLokiAnswers)
	if err != nil {
		t.Fatalf("%v: record the answers with LVP_RUN_ORACLES=1", err)
	}
	defer func() { _ = f.Close() }()
	loki := map[string]commentFormAnswer{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var a commentFormAnswer
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			t.Fatal(err)
		}
		loki[a.Query] = a
	}
	if len(loki) != len(commentFormQueries) {
		t.Fatalf("%d recorded answers for %d queries: record them again with LVP_RUN_ORACLES=1", len(loki), len(commentFormQueries))
	}
	return loki
}

// A comment between a function's name and its parenthesis (a range aggregation in any case, vector, label_replace,
// ip, an unwrap conversion, a vector aggregation and its grouping) and a subquery-like range get Loki's 400 with its
// exact text on query_range and query, before any VictoriaLogs call, also on the second request. Loki's answers are
// recorded with its engine (test/oracle/loki engine; LVP_RUN_ORACLES=1 records them again).
//
// conformance: semantics/comment-between-function-and-parenthesis-accepted
func TestCommentFormsAnswerLikeLoki(t *testing.T) {
	loki := loadCommentFormAnswers(t)
	var upstream atomic.Int64
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstream.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer vl.Close()
	rejected := 0
	for _, q := range commentFormQueries {
		want := loki[q]
		if want.RangeStatus == http.StatusOK {
			continue
		}
		rejected++
		rangeForm, instantForm := commentFormForms(q)
		for _, rt := range []struct {
			path, lokiErr string
			status        int
			form          url.Values
		}{
			{"/loki/api/v1/query_range", want.RangeError, want.RangeStatus, rangeForm},
			{"/loki/api/v1/query", want.InstantError, want.InstantStatus, instantForm},
		} {
			if rt.status != http.StatusBadRequest || rt.lokiErr == "" {
				t.Fatalf("%s %q: Loki's recorded answer %d %q, want a 400 with its text", rt.path, q, rt.status, rt.lokiErr)
			}
			p := newCompatTestProxy(t, vl.URL)
			before := upstream.Load()
			for i := 0; i < 2; i++ {
				rec := doCompatProxyRequest(p, rt.path+"?"+rt.form.Encode(), map[string]string{"X-Scope-OrgID": "team-a"})
				if rec.Code != rt.status || strings.TrimSpace(rec.Body.String()) != rt.lokiErr {
					t.Errorf("%s %q, request %d: %d %q, want Loki's %d %q", rt.path, q, i+1, rec.Code, rec.Body, rt.status, rt.lokiErr)
				}
			}
			if calls := upstream.Load() - before; calls != 0 {
				t.Errorf("%s %q: %d requests reached VictoriaLogs", rt.path, q, calls)
			}
		}
	}
	if rejected == 0 {
		t.Fatal("no recorded rejection")
	}
}

// A negative zero offset (`offset -0s`, `offset -0`) is no offset to Loki, whose answers for them are its answer for
// the query without one (recorded with its engine); the proxy sends VictoriaLogs the requests of the query without
// an offset and answers what it answers.
//
// conformance: semantics/range-duration-grammar-wider-than-loki
func TestNegativeZeroOffsetAnswersLikeNoOffset(t *testing.T) {
	loki := loadCommentFormAnswers(t)
	const plain = "rate({app=\"a\"}[5m])"
	ref := loki[plain]
	if ref.RangeStatus != http.StatusOK || ref.InstantStatus != http.StatusOK || len(ref.RangeBody) == 0 {
		t.Fatalf("Loki's answer to %s: %d / %d", plain, ref.RangeStatus, ref.InstantStatus)
	}
	var mu sync.Mutex
	var seen []string
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		mu.Lock()
		seen = append(seen, r.URL.Path+"?"+r.Form.Encode())
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[]}`))
	}))
	defer vl.Close()
	answer := func(path string) (int, string, []string) {
		mu.Lock()
		seen = nil
		mu.Unlock()
		// A fresh proxy, so no answer is served from another spelling's cache.
		w := doCompatProxyRequest(newTestProxy(t, vl.URL), path, map[string]string{"X-Scope-OrgID": "0"})
		mu.Lock()
		defer mu.Unlock()
		return w.Code, w.Body.String(), append([]string(nil), seen...)
	}
	for _, q := range []string{"rate({app=\"a\"}[5m] offset -0s)", "rate({app=\"a\"}[5m] offset -0)", "rate({app=\"a\"}[5m] offset 0s)"} {
		got := loki[q]
		if got.RangeStatus != http.StatusOK || !bytes.Equal(got.RangeBody, ref.RangeBody) || !bytes.Equal(got.InstantBody, ref.InstantBody) {
			t.Errorf("%s: Loki's answer is not its answer without the offset: %d %s / %d %s", q, got.RangeStatus, got.RangeBody, got.InstantStatus, got.InstantBody)
		}
		rangeForm, instantForm := commentFormForms(q)
		plainRange, plainInstant := commentFormForms(plain)
		for _, pair := range [][2]string{
			{"/loki/api/v1/query_range?" + rangeForm.Encode(), "/loki/api/v1/query_range?" + plainRange.Encode()},
			{"/loki/api/v1/query?" + instantForm.Encode(), "/loki/api/v1/query?" + plainInstant.Encode()},
		} {
			code, body, calls := answer(pair[0])
			wantCode, wantBody, wantCalls := answer(pair[1])
			if code != http.StatusOK || code != wantCode || body != wantBody {
				t.Errorf("%s: %d %q, without the offset %d %q", pair[0], code, body, wantCode, wantBody)
			}
			if strings.Join(calls, "\n") != strings.Join(wantCalls, "\n") || len(calls) == 0 {
				t.Errorf("%s: VictoriaLogs requests\n%s\nwithout the offset\n%s", pair[0], strings.Join(calls, "\n"), strings.Join(wantCalls, "\n"))
			}
		}
	}
}
