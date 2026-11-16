package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

func newJSONLineKeysProxy(t *testing.T, on bool, defaultMsg string) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL:               "http://127.0.0.1:1",
		Cache:                    cache.New(time.Minute, 100),
		LogLevel:                 "error",
		LabelStyle:               LabelStyleUnderscores,
		MetadataFieldMode:        MetadataFieldModeTranslated,
		BackendDefaultMsgValue:   defaultMsg,
		JSONLineFieldsAsLineKeys: on,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

// The guard of a pipeline: whether Loki keeps nothing from a flat JSON line without structured metadata, and the label
// that keeps it as a stream label.
//
// conformance: profiles/json-line-stored-without-line
func TestPlaceholderExclusionDecision(t *testing.T) {
	p := newJSONLineKeysProxy(t, true, "")
	const notPlaceholder = `NOT _msg:="missing _msg field; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field"`
	cases := []struct {
		query string
		guard string // "" when no exclusion
	}{
		{`max_over_time({a="b"} | unwrap amount [5m])`, "amount"},
		{`max_over_time({a="b"} | logfmt amount | unwrap amount [5m])`, "amount"},
		{`max_over_time({a="b"} | logfmt | unwrap duration(took) [5m])`, "took"},
		{`max_over_time({a="b"} |= "x" | logfmt | unwrap amount [5m])`, "amount"},
		{`max_over_time({a="b"} | json | unwrap amount [5m])`, ""},
		{`max_over_time({a="b"} | json amount | unwrap amount [5m])`, ""},
		{`max_over_time({a="b"} | logfmt | json | unwrap amount [5m])`, ""},
		{`max_over_time({a="b"} | regexp "(?P<amount>\\d+)" | unwrap amount [5m])`, ""},
		{`max_over_time({a="b"} | label_format amount=a | unwrap amount [5m])`, ""},
		{`max_over_time({a="b"} | drop x | unwrap amount [5m])`, ""},
		{`max_over_time({a="b"} | unwrap detected_level [5m])`, ""},
		{`count_over_time({a="b"} | status="200" [5m])`, "status"},
		{`count_over_time({a="b"} | status=~"2.." [5m])`, "status"},
		{`count_over_time({a="b"} | amount > 5 [5m])`, "amount"},
		{`count_over_time({a="b"} | took > 1s [5m])`, "took"},
		{`count_over_time({a="b"} | status!="200" [5m])`, ""},
		{`count_over_time({a="b"} | status=~".*" [5m])`, ""},
		{`count_over_time({a="b"} | status="200" or status="500" [5m])`, ""},
		{`count_over_time({a="b"} | detected_level="error" [5m])`, ""},
		{`count_over_time({a="b"} | level="error" [5m])`, ""},
		{`count_over_time({a="b"} | service_name="x" [5m])`, ""},
		{`count_over_time({a="b"} | __error__="" [5m])`, ""},
		{`count_over_time({a="b"} | line_format "{{.a}}" | status="200" [5m])`, "status"},
		{`count_over_time({a="b"} | decolorize | status="200" [5m])`, "status"},
		{`count_over_time({a="b"} | logfmt status | status="200" [5m])`, "status"},
		{`count_over_time({a="b"} | logfmt | status="" [5m])`, ""},
		{`count_over_time({a="b"} [5m])`, ""},
		{`count_over_time({a="b"} | logfmt [5m])`, ""},
		{`count_over_time({a="b"} | status="200" [5m]) / count_over_time({a="b"} | status="200" [5m])`, "status"},
		{`count_over_time({a="b"} | status="200" [5m]) / count_over_time({a="b"} | code="200" [5m])`, ""},
		{`count_over_time({a="b"} | status="200" [5m]) / count_over_time({a="b"} [5m])`, ""},
		{`sum by (a) (count_over_time({a="b"} | status="200" [5m])) * 2`, "status"},
		{`topk(2, sum by (a) (count_over_time({a="b"} | status="200" [5m])))`, "status"},
		{`label_replace(count_over_time({a="b"} | status="200" [5m]), "x", "$1", "a", "(.*)")`, "status"},
		{`{a="b"} | status="200"`, "status"},
		{`{a="b"} | logfmt | unwrap_x="1"`, "unwrap_x"},
		{`{a="b"}`, ""},
		{`{a="b"} | json | status="200"`, ""},
		{`vector(1)`, ""},
	}
	for _, c := range cases {
		plan := resolvedPlan(c.query)
		if plan == nil {
			t.Fatalf("%s: no plan", c.query)
		}
		f, ok := p.placeholderExclusion(plan)
		if c.guard == "" {
			if ok {
				t.Errorf("%s: excludes placeholder rows (%s), want none", c.query, f)
			}
			continue
		}
		want := notPlaceholder + ` OR {` + c.guard + `!=""}`
		if !ok || f != want {
			t.Errorf("%s: exclusion %q (%v), want %q", c.query, f, ok, want)
		}
		if _, err := logsql.ParseFilter(f); err != nil {
			t.Errorf("%s: the exclusion %q does not parse: %v", c.query, f, err)
		}
	}
}

// The exclusion names the configured placeholder as well, and the stream label in its VictoriaLogs spelling.
func TestPlaceholderExclusionCustomPlaceholder(t *testing.T) {
	p := newJSONLineKeysProxy(t, true, "no message")
	f, ok := p.placeholderExclusion(resolvedPlan(`max_over_time({a="b"} | unwrap amount [5m])`))
	want := `NOT (_msg:="missing _msg field; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field" OR _msg:="no message") OR {amount!=""}`
	if !ok || f != want {
		t.Fatalf("exclusion %q, want %q", f, want)
	}
	if got := p.placeholderRiskFilter(); !strings.Contains(got, `"no message"`) {
		t.Fatalf("risk filter %q does not name the configured placeholder", got)
	}
}

// With the flag off nothing the proxy sends or keys changes: no exclusion, the probes' and the split's filters as
// before, the routing namespace of every cache key as before.
//
// conformance: profiles/json-line-stored-without-line
func TestJSONLineKeysOffChangesNothing(t *testing.T) {
	off := newJSONLineKeysProxy(t, false, "")
	on := newJSONLineKeysProxy(t, true, "")
	q := `max_over_time({a="b"} | unwrap amount [5m])`
	r := httptest.NewRequest("GET", "/loki/api/v1/query?query="+url.QueryEscape(q), nil)
	if got := off.withPlaceholderExclusion(r, q); got != r {
		t.Fatal("the flag off attached an exclusion")
	}
	if got := on.withPlaceholderExclusion(r, q); placeholderExclusionFrom(got.Context()) == "" {
		t.Fatal("the flag on attached no exclusion")
	}
	if off.placeholderRiskFilter() != "" || off.placeholderValidPipe() != "" || off.placeholderJSONStartFilter() != jsonObjectStartFilter {
		t.Fatal("the flag off changed a | json split or probe")
	}
	if off.lineFieldExposure(`{a="b"} | json`).placeholderLine {
		t.Fatal("the flag off reads placeholder rows as lines")
	}
	if f := off.rowLineFieldsOf(`{a="b"} | json amount`, nil); f.active {
		t.Fatal("the flag off rebuilds placeholder lines otherwise")
	}
	// The namespace as it was before the flag existed.
	routing := off.routingForContext(context.Background())
	var style LabelStyle
	var fields map[string]string
	var otel bool
	if routing.translator != nil {
		style, fields, otel = routing.translator.style, routing.translator.vlToLoki, routing.translator.translateOTel
	}
	data, _ := json.Marshal([]any{"scope-v3", off.backend.String(), "", off.backendHeaders, routing.tenants, routing.label, style, fields, otel, off.forwardTenantHeader})
	sum := sha256.Sum256(data)
	if got := off.buildRoutingNamespace(routing); got != hex.EncodeToString(sum[:]) {
		t.Fatal("the flag off changed the routing namespace of every cache key")
	}
	if on.buildRoutingNamespace(on.routingForContext(context.Background())) == off.buildRoutingNamespace(routing) {
		t.Fatal("the flag on shares the cache identities of the flag off")
	}
	// A request's reads with an exclusion are keyed apart from the same reads without one.
	ctx := context.Background()
	plain := on.contextScopeFingerprint(ctx)
	excluded := on.contextScopeFingerprint(context.WithValue(ctx, placeholderExclusionKey{}, "x"))
	if plain == excluded || !strings.HasPrefix(excluded, plain) {
		t.Fatalf("scope fingerprints %q and %q", plain, excluded)
	}
}

// The exclusion reaches every row read of the request and nothing else.
func TestWithExclusionParams(t *testing.T) {
	ctx := context.WithValue(context.Background(), placeholderExclusionKey{}, "F")
	params := url.Values{"query": {"*"}, "extra_filters": {"G"}}
	got := withExclusionParams(ctx, "/select/logsql/stats_query_range", params)
	if strings.Join(got["extra_filters"], ",") != "G,F" || strings.Join(params["extra_filters"], ",") != "G" {
		t.Fatalf("extra_filters %v (the caller's %v)", got["extra_filters"], params["extra_filters"])
	}
	if got := withExclusionParams(ctx, "/health", params); len(got["extra_filters"]) != 1 {
		t.Fatalf("a non-select path got %v", got["extra_filters"])
	}
	if got := withExclusionParams(context.Background(), "/select/logsql/query", params); len(got["extra_filters"]) != 1 {
		t.Fatalf("a request without an exclusion got %v", got["extra_filters"])
	}
}

// Placeholder rows, by placeholder and pipeline.
func TestPlaceholderRowReading(t *testing.T) {
	const def = "missing _msg field; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field"
	for _, c := range []struct {
		msg, custom string
		want        bool
	}{
		{def, "", true},
		{"no line", "no line", true},
		{"no line", "", false},
		{"", "", false},
		{"", "x", false},
		{`{"a":"b"}`, "", false},
		{def + " ", "", false},
	} {
		if got := isVLPlaceholderMsg(c.msg, c.custom); got != c.want {
			t.Errorf("isVLPlaceholderMsg(%q, %q) = %v", c.msg, c.custom, got)
		}
		if got := isVLPlaceholderMsg([]byte(c.msg), c.custom); got != c.want {
			t.Errorf("isVLPlaceholderMsg([]byte(%q), %q) = %v", c.msg, c.custom, got)
		}
	}
	p := newJSONLineKeysProxy(t, true, "")
	logfmt := p.lineFieldExposure(`{a="b"} | logfmt amount`)
	plain := p.lineFieldExposure(`{a="b"}`)
	// unpack_logfmt reads the placeholder's _msg word as a key and blanks _msg.
	if !readsPlaceholderAsLine(logfmt, "") || readsPlaceholderAsLine(plain, "") || !readsPlaceholderAsLine(plain, def) {
		t.Fatal("blank _msg is the placeholder only after | logfmt")
	}
	if !logfmt.logfmtOnly["amount"] {
		t.Fatalf("logfmtOnly %v", logfmt.logfmtOnly)
	}
	if got := placeholderLogfmtOnly(parsePipeline(t, `{a="b"} | logfmt amount, b | json b | label_format c=a | logfmt c`)); len(got) != 1 || !got["amount"] {
		t.Fatalf("logfmtOnly %v", got)
	}
	if got := placeholderLogfmtOnly(parsePipeline(t, `{a="b"} | json`)); got != nil {
		t.Fatalf("logfmtOnly %v", got)
	}
	// A placeholder row's rebuilt line keeps the json and logfmt extraction names, never another stage's target.
	got := placeholderPipelineLineFields(parsePipeline(t, `{a="b"} | json amount | logfmt b | regexp "(?P<c>x)" | label_format d=a | pattern "<e> <_>"`))
	for name, want := range map[string]bool{"amount": false, "b": false, "c": true, "d": true, "e": true} {
		if got[name] != want {
			t.Errorf("placeholder line field %s: %v, want %v (%v)", name, got[name], want, got)
		}
	}
	lf := p.rowLineFieldsOf(`{a="b"} | logfmt amount`, map[string]bool{"amount": true})
	if forRowMsg(lf, def)["amount"] || !forRowMsg(lf, "x=1")["amount"] || forRowMsg(lf, "")["amount"] {
		t.Fatal("rowLineFields picks the placeholder set for placeholder rows only")
	}
}

func parsePipeline(t *testing.T, q string) []logqlpkg.Stage {
	t.Helper()
	lq, err := parseLogQLQuery(noFlowCtx, q)
	if err != nil {
		t.Fatal(err)
	}
	return lq.Pipeline
}

// The | json row evaluator reads a placeholder row as the JSON object of the fields VictoriaLogs returned.
func TestPlaceholderRowLine(t *testing.T) {
	const def = "missing _msg field; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field"
	on := newJSONLineKeysProxy(t, true, "")
	off := newJSONLineKeysProxy(t, false, "")
	stream := map[string]string{"app": "x"}
	row := map[string]string{"_msg": def, "_time": "t", "_stream": `{app="x"}`, "app": "x", "status": "200", "amount": "3"}
	if got := on.placeholderRowLine(row, stream); got != `{"amount":"3","status":"200"}` {
		t.Fatalf("line %q", got)
	}
	if got := off.placeholderRowLine(row, stream); got != def {
		t.Fatalf("flag off line %q", got)
	}
	if got := on.placeholderRowLine(map[string]string{"_msg": def, "app": "x"}, stream); got != "{}" {
		t.Fatalf("a row without a read key: %q", got)
	}
	if got := on.placeholderRowLine(map[string]string{"_msg": "a=1"}, stream); got != "a=1" {
		t.Fatalf("a stored line: %q", got)
	}
}

// FuzzPlaceholderExclusion: on any query the decision terminates without a panic, and an exclusion it writes is a
// LogsQL filter that parses, on a guard that is a Loki label name Loki gives no placeholder line without a parser.
func FuzzPlaceholderExclusion(f *testing.F) {
	for _, seed := range []string{
		`max_over_time({a="b"} | logfmt amount | unwrap amount [5m])`,
		`count_over_time({a="b"} | status="200" [5m]) / count_over_time({a="b"} | status="200" [5m])`,
		`{a="b"} | status=~"2.." | logfmt`,
		`sum by (x) (rate({a="b"} | json | unwrap y [1m]))`,
		`{a="b"} | amount > 5 and b="c"`,
		`{a="b"} | detected_level="error"`,
		`vector(1) + count_over_time({a="b"} | x="1" [1m])`,
	} {
		f.Add(seed)
	}
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: cache.New(time.Minute, 10), LogLevel: "error",
		LabelStyle: LabelStyleUnderscores, MetadataFieldMode: MetadataFieldModeTranslated, JSONLineFieldsAsLineKeys: true})
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	f.Fuzz(func(t *testing.T, q string) {
		if len(q) > 4096 {
			return
		}
		plan := resolvedPlan(q)
		if plan == nil {
			return
		}
		filter, ok := p.placeholderExclusion(plan)
		if !ok {
			return
		}
		if _, err := logsql.ParseFilter(filter); err != nil {
			t.Fatalf("%q: exclusion %q does not parse: %v", q, filter, err)
		}
		if !strings.HasPrefix(filter, "NOT _msg:=") {
			t.Fatalf("%q: exclusion %q", q, filter)
		}
	})
}
