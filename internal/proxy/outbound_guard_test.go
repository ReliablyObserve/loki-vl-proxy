package proxy

import (
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// Every function of the package that sends a query to VictoriaLogs records it for the corpus capture
// (querycorpus.RecordBackend), and each of them passes its params through guardOutboundQuery first: the hot
// backend's GET and POST, the live tail and the cold tier.
//
// conformance: semantics/reserved-field-name-case-outbound
func TestEveryBackendSenderGuardsItsQuery(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	senders := 0
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range file.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			body := string(src[fset.Position(fn.Body.Pos()).Offset:fset.Position(fn.Body.End()).Offset])
			if !strings.Contains(body, "querycorpus.RecordBackend(") {
				continue
			}
			senders++
			record := strings.Index(body, "querycorpus.RecordBackend(")
			if guard := strings.Index(body, "guardOutboundQuery("); guard < 0 || guard > record {
				t.Errorf("%s: %s sends a query to VictoriaLogs without guardOutboundQuery ahead of it", path, fn.Name.Name)
			}
		}
	}
	if senders < 5 {
		t.Fatalf("only %d senders found", senders)
	}
}

// A query is guarded on its way out and passed on unchanged otherwise: the same params, with no copy.
//
// conformance: semantics/reserved-field-name-case-outbound
func TestGuardOutboundQuery(t *testing.T) {
	plain := url.Values{"query": {`app:="a" _time:5m`}, "limit": {"10"}}
	if got := guardOutboundQuery(plain); got.Get("query") != plain.Get("query") {
		t.Errorf("changed a plain query: %v", got)
	}
	if allocs := testing.AllocsPerRun(100, func() { _ = guardOutboundQuery(plain) }); allocs != 0 {
		t.Errorf("a plain query allocates %v times", allocs)
	}
	in := url.Values{"query": {`app:="a" "_TIME":="up"`}, "limit": {"10"}}
	got := guardOutboundQuery(in)
	if got.Get("query") != `app:="a" | copy _TIME as __lxrf_TIME | filter __lxrf_TIME:="up" | delete __lxrf_TIME` || got.Get("limit") != "10" {
		t.Errorf("guarded %v", got)
	}
	if in.Get("query") != `app:="a" "_TIME":="up"` {
		t.Error("the caller's params were changed")
	}
}

// Through the handlers: a log query and a metric query on a name in another case reach VictoriaLogs in the guarded
// form, whatever path wrote them (the query plan for the log query, the string translator for the metric).
//
// conformance: semantics/reserved-field-name-case-outbound
func TestOutboundGuardOnTheServedPath(t *testing.T) {
	for _, c := range []struct{ path, query string }{
		{"/loki/api/v1/query_range", `{app="a"} | _TIME="up"`},
		{"/loki/api/v1/query_range", `sum(sum_over_time({app="a"} | logfmt | unwrap _TIME [1m]))`},
		{"/loki/api/v1/query", `sum(count_over_time({app="a"} | json | _Stream_Id="x" [5m]))`},
	} {
		var mu sync.Mutex
		var sent []string
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = r.ParseForm()
			mu.Lock()
			sent = append(sent, r.Form.Get("query"))
			mu.Unlock()
			w.Header().Set("Content-Type", "application/x-ndjson")
		}))
		p := newCompatTestProxy(t, backend.URL)
		target := c.path + "?" + url.Values{"query": {c.query}, "start": {"1700000000"}, "end": {"1700003600"}, "step": {"60"}, "time": {"1700003600"}, "limit": {"10"}}.Encode()
		_ = doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "team-a"})
		backend.Close()
		if len(sent) == 0 {
			t.Errorf("%s: nothing sent", c.query)
		}
		for _, q := range sent {
			if strings.Contains(q, `"_TIME":`) || strings.Contains(q, ` _TIME:`) || strings.Contains(q, `"_Stream_Id":`) || strings.Contains(q, ` _Stream_Id:`) {
				t.Errorf("%s: VictoriaLogs got %s", c.query, q)
			}
		}
	}
}
