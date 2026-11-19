package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// upstreamRecorder is a fake VictoriaLogs that records every request it gets, whole, and answers a log row.
type upstreamRecorder struct {
	mu   sync.Mutex
	reqs []string
}

func (u *upstreamRecorder) handler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	if r.URL.Path == "/select/logsql/stream_field_names" {
		// The stream-label discovery of the Loki-compatible profile runs in the background, on its own clock.
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"values":[]}`))
		return
	}
	u.mu.Lock()
	u.reqs = append(u.reqs, r.Method+" "+r.URL.Path+"?"+r.URL.RawQuery+" "+string(body))
	u.mu.Unlock()
	switch r.URL.Path {
	case "/select/logsql/stats_query_range", "/select/logsql/stats_query":
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
	case "/select/logsql/hits":
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"hits":[]}`))
	default:
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(`{"_time":"2026-04-04T10:00:00Z","_msg":"GET /a 200","_stream":"{app=\"a\"}","app":"a","status":"200"}` + "\n"))
	}
}

func (u *upstreamRecorder) take() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := u.reqs
	u.reqs = nil
	return out
}

// loweringParityQueries are the queries the route parity test sends: native says whether the typed lowering
// translates it (and so it is counted as flipped), the others go to the string translator in both runs.
var loweringParityQueries = []struct {
	name, query string
	native      bool
	// rejectedIn names a profile in which the proxy rejects the query before translating it.
	rejectedIn string
	// construct names the metric constructs that serve the query (or the operands the proxy translates on their own)
	// from the lowering once one of them is flipped (QueryPlanFlippedConstructs), or the log stage families that serve a
	// log query once all are flipped (QueryPlanFlippedLogStages), comma separated; empty for a query no flip serves.
	construct string
}{
	// Stream selectors.
	{"selector", `{app="a"}`, true, "", ""},
	{"selector regexp and not", `{app=~"a|b", env!="x", job!~"j.*"}`, true, "", ""},
	{"selector with dotted and service_name", `{service_name="a", k8s_pod_name="p"}`, true, "", ""},
	// Line filters.
	{"contains", `{app="a"} |= "error"`, true, "", ""},
	{"chain of line filters", `{app="a"} |= "error" != "debug" |~ "e.r+" !~ "to.*"`, true, "", ""},
	{"case insensitive", `{app="a"} |~ "(?i)error"`, true, "", ""},
	{"line filter or", `{app="a"} |= "a" or "b" or "c"`, true, "", ""},
	{"ip line filter", `{app="a"} |= ip("10.0.0.0/8")`, true, "", ""},
	{"not ip line filter", `{app="a"} != ip("1.2.3.4")`, true, "", ""},
	{"quotes and unicode", "{app=\"a\"} |= `a\"b` |= \"żółć\\n\"", true, "", ""},
	// A regexp the client wrote with an escape is sent as written (the lowering holds only the value), so the
	// translator serves it; the substring filter quotes its value, so the lowering still does.
	{"escaped regexp filter", `{app="a"} |~ "\x41b"`, false, "", ""},
	{"escaped negated regexp filter", `{app="a"} !~ "\101b"`, false, "", ""},
	{"escaped regexp or chain", `{app="a"} |~ "\x41" or "b"`, true, "", ""},
	{"escaped substring filter", `{app="a"} |= "\x41b"`, true, "", ""},
	{"escaped label filter", `{app="a"} | b=~"\x41"`, true, "", ""},
	{"quote and backslash regexp filter", `{app="a"} |~ "a\"b\\d+"`, true, "", ""},
	// Pattern filters.
	{"pattern", `{app="a"} |> "<_> GET <_> <_>"`, true, "", ""},
	{"not pattern", `{app="a"} !> "<_> POST <_>"`, true, "", ""},
	{"pattern or chain", `{app="a"} |> "a<_>" or "b<_>" or "<_> c"`, true, "", ""},
	{"pattern literal", `{app="a"} |> "error"`, true, "", ""},
	{"pattern then line filter", `{app="a"} |> "<_> GET <_>" |= "200"`, true, "", ""},
	{"line filter then pattern", `{app="a"} |= "200" |> "<_> GET <_>" != "x"`, true, "", ""},
	// Label filters with no parser.
	{"label filter string", `{app="a"} | status="200"`, true, "", ""},
	{"label filter numeric", `{app="a"} | status >= 500`, true, "", ""},
	{"label filter and or", `{app="a"} | status >= 500 and status < 600 or code="x"`, true, "", ""},
	{"label filter regexp", `{app="a"} | path =~ "/api/.*" | path !~ "/api/v0"`, true, "", ""},
	{"label filter duration bytes", `{app="a"} | latency > 250ms | size < 1MB`, true, "", ""},
	{"repeated label filter", `{app="a"} | status="200" | status!="500" | status="200"`, true, "", ""},
	{"ip label filter", `{app="a"} | addr = ip("10.0.0.0/8")`, true, "", ""},
	{"not ip label filter", `{app="a"} | addr != ip("192.168.0.0/16") | addr = ip("1.2.3.4-1.2.3.9")`, true, "", ""},
	{"label filters among line filters", `{app="a"} |= "x" | status="200" |> "<_> y" | code != "z" != "w"`, true, "", ""},
	{"dotted label filter", `{app="a"} | k8s.pod.name = "p"`, true, "loki compatible", ""},
	// Not covered by the lowering: the string translator answers in both runs.
	// A filter on detected_level: served once its stage family (LevelFilter) is flipped. One after a parser and one on
	// service_name are not lowered: the string translator answers in both runs.
	{"detected_level filter", `{app="a"} | detected_level="error"`, false, "", "LevelFilter"},
	{"detected_level filters among line filters", `{app="a"} |= "x" | detected_level=~"error|warn" != "y" | status >= 500`, false, "", "LevelFilter"},
	{"detected_level filter after a parser", `{app="a"} | logfmt | detected_level="error"`, false, "", ""},
	{"service_name label filter", `{app="a"} | service_name="x"`, false, "", ""},
	{"json", `{app="a"} | json | status="200"`, false, "", "JSON"},
	{"logfmt", `{app="a"} | logfmt | level="error"`, false, "", "Logfmt"},
	// The Loki-compatible profile drops a line_format nothing reads: the query is the selector, translated before.
	{"line_format", `{app="a"} | line_format "{{.status}}"`, false, "loki compatible", "LineFormat"},
	// The lowering writes these natively (the corpus shadow holds it to the translator), but the flip's screen turns
	// every parser and formatting stage away: they stay on the string translator.
	{"decolorize", `{app="a"} | decolorize`, false, "", ""},
	{"decolorize then filter", `{app="a"} | decolorize | status="200" |= "x"`, false, "", ""},
	{"regexp", "{app=\"a\"} | regexp `(?P<s>\\d+)` | s=\"200\"", false, "", "RegexpPattern"},
	{"pattern", "{app=\"a\"} | pattern `<_> <s> <_>` | s=\"200\"", false, "", "RegexpPattern"},
	{"label_format", `{app="a"} | label_format s=status | s="200"`, false, "", "LabelFormat"},
	{"label_format template", `{app="a"} | label_format s="{{.status}}-x"`, false, "", "LabelFormat"},
	{"drop", `{app="a"} | drop status`, false, "", "DropKeep"},
	{"keep", `{app="a"} | keep status`, false, "", "DropKeep"},
	// Two line_format stages keep the stored line aside in the Loki-compatible profile (translateQueryOpts keepLine).
	{"keep line", `{app="a"} | line_format "{{.status}}" | line_format "{{.app}}: {{.__line__}}"`, false, "", "LineFormat"},
	{"keep line with filters", `{app="a"} |= "x" | line_format "{{.a}}" | status="200" | line_format "{{.b}}"`, false, "", "LineFormat"},
	{"detected_level and service_name filters", `{service_name="a"} |= "x" | detected_level="error" | service_name="a"`, false, "", ""},
	{"decolorize", `{app="a"} | decolorize |= "x"`, false, "", ""},
	// Asked again: answered from the translation cache, with no translation.
	{"drop", `{app="a"} | drop status`, false, "", ""},
	// Metric queries: served from the lowering once their construct is flipped, with the string translator's text.
	{"unwrap metric", `sum(rate({app="a"} | json | unwrap bytes [5m]))`, false, "", "VectorAgg"},
	{"rate", `rate({app="a"} |= "x" [5m])`, false, "", "RangeAgg"},
	{"sum by", `sum by (level) (count_over_time({app="a"} | status="200" [1m]))`, false, "", "VectorAgg"},
	{"sum by rate", `sum by (app) (rate({app="a"} | json | status >= 500 [5m]))`, false, "", "VectorAgg"},
	{"avg without", `avg without (pod) (bytes_over_time({app="a"} |= "x" [5m]))`, false, "", "VectorAgg"},
	{"max over a sum", `max(sum by (app, pod) (count_over_time({app="a"} | logfmt [5m])))`, false, "", "VectorAgg"},
	{"sum unwrap quantile", `sum by (app) (quantile_over_time(0.95, {app="a"} | json | unwrap latency [5m]))`, false, "", "VectorAgg"},
	{"count over a range", `count(count_over_time({app="a"} | drop pod [5m]))`, false, "", "VectorAgg"},
	{"binary", `rate({app="a"}[5m]) / rate({app="b"}[5m])`, false, "", "BinOp,RangeAgg"},
	{"count_over_time", `count_over_time({app="a"} |> "<_> GET <_>" [5m])`, false, "", "RangeAgg"},
	{"label_replace", `label_replace(rate({app="a"}[5m]), "x", "$1", "app", "(.*)")`, false, "", "LabelReplace"},
	// The proxy evaluates these itself and translates their operand alone: the operand is served from the lowering
	// once its construct is flipped, and the whole query never is.
	{"stddev", `stddev by (app) (count_over_time({app="a"}[5m]))`, false, "", "RangeAgg"},
	{"topk", `topk(2, sum by (app) (count_over_time({app="a"}[5m])))`, false, "", "VectorAgg"},
	{"binary with a without operand", `sum without (pod) (rate({app="a"}[5m])) / 2`, false, "", "VectorAgg"},
	{"binary with a vector operand", `vector(1) + sum(rate({app="a"}[5m]))`, false, "", "VectorAgg"},
	// Metric queries no flip serves: the proxy sends the string translator's text for the whole query, and the plan
	// holds an evaluated level, or writes Loki's reading where the translator writes another text
	// (writesTranslatorText).
	{"sum over a stddev", `sum(stddev by (app) (count_over_time({app="a"}[5m])))`, false, "", ""},
	{"label_replace around topk", `label_replace(topk(2, sum by (app) (count_over_time({app="a"}[5m]))), "x", "$1", "app", "(.*)")`, false, "", ""},
	{"numeric equality after a parser", `sum(count_over_time({app="a"} | json | status == 500 [5m]))`, false, "", ""},
	{"unwrap of a reserved field in another case", `sum(sum_over_time({app="a"} | logfmt | unwrap _TIME [5m]))`, false, "", ""},
	{"keep before unwrap under a label_replace", `label_replace(first_over_time({app="a"} | json | keep app, n | unwrap n [5m]), "x", "$1", "app", "(.*)")`, false, "", ""},
	{"vector", `vector(1)`, false, "", ""},
	{"invalid", `{app="a"} |= `, false, "", ""},
	{"invalid regexp", `{app="a"} | path =~ "("`, false, "", ""},
}

// loweringParityProfiles are the translator profiles the flag-on and flag-off comparisons run under: the label style,
// stream fields, a pinned older VictoriaLogs, the body scan, the Loki-compatible profile and the multi-tenant label
// mode.
var loweringParityProfiles = []struct {
	name    string
	config  func(*Config)
	version string
}{
	{"passthrough", func(c *Config) { c.LabelStyle = LabelStylePassthrough }, ""},
	{"underscores", func(c *Config) { c.LabelStyle = LabelStyleUnderscores }, ""},
	{"loki compatible", func(c *Config) {
		c.LabelStyle, c.MetadataFieldMode = LabelStyleUnderscores, MetadataFieldModeTranslated
	}, ""},
	{"stream fields", func(c *Config) {
		c.LabelStyle, c.StreamFields = LabelStyleUnderscores, []string{"app", "env", "namespace", "service_name"}
	}, ""},
	{"stream fields v1.49", func(c *Config) {
		c.LabelStyle, c.StreamFields = LabelStyleUnderscores, []string{"app", "job"}
	}, "v1.49.0"},
	{"no body scan", func(c *Config) { c.LabelStyle, c.DisableDetectedLevelBodyScan = LabelStyleUnderscores, true }, ""},
	{"native metadata", func(c *Config) { c.LabelStyle, c.MetadataFieldMode = LabelStylePassthrough, MetadataFieldModeNative }, ""},
}

// metricFlipCandidates are the metric constructs the tests serve from the lowering on a proxy of their own
// (withFlipped), to prove a flip of them ready: the corpus text parity, the route parity and the gate tests run with
// them flipped as well as with the constructs served today (QueryPlanFlippedConstructs).
var metricFlipCandidates = []string{"VectorAgg", "RangeAgg", "BinOp", "LabelReplace"}

// withFlipped makes p serve the constructs it serves today and the given ones from the lowering.
func withFlipped(p *Proxy, constructs ...string) {
	p.flippedConstructs = append(append([]string(nil), QueryPlanFlippedConstructs...), constructs...)
}

// A native query is served from the lowering with the flag on and from the string translator with it off, and the
// client cannot tell: the same status and body, and the same requests to VictoriaLogs, under the translator
// profiles that change the translation (label style, stream fields, a pinned older VictoriaLogs, the body scan,
// the Loki-compatible profile and the multi-tenant label mode).
// conformance: architecture-syntax-package-layering, loki_api_v1_query_range
func TestQueryPlanLowering_RouteParity(t *testing.T) {
	for _, flips := range []struct {
		name  string
		extra []string
		logs  []string
	}{{"served today", nil, nil}, {"metric and log stage flips", metricFlipCandidates, logStageFlipCandidates}} {
		t.Run(flips.name, func(t *testing.T) {
			testLoweringRouteParity(t, flips.extra, flips.logs)
		})
	}
}

func testLoweringRouteParity(t *testing.T, extra, logs []string) {
	for _, prof := range loweringParityProfiles {
		t.Run(prof.name, func(t *testing.T) {
			type run struct {
				p  *Proxy
				up *upstreamRecorder
			}
			start := func(disable bool) run {
				up := &upstreamRecorder{}
				vl := httptest.NewServer(http.HandlerFunc(up.handler))
				t.Cleanup(vl.Close)
				cfg := Config{BackendURL: vl.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", DisableQueryPlanLowering: disable}
				prof.config(&cfg)
				p, err := New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
				if prof.version != "" {
					p.setReleaseForTest(prof.version)
				}
				withFlipped(p, extra...)
				withFlippedLogStages(p, logs...)
				up.take() // the version probe, if any
				return run{p, up}
			}
			on, off := start(false), start(true)
			if on.p.queryPlanLowering == off.p.queryPlanLowering {
				t.Fatal("the two proxies translate the same way")
			}
			flipped := func(r run) int { return shadowTotal(shadowCounts(t, r.p), "flipped") }
			for _, q := range loweringParityQueries {
				t.Run(q.name, func(t *testing.T) {
					target := "/loki/api/v1/query_range?start=1609459200&end=1609462800&limit=100&query=" + url.QueryEscape(q.query)
					headers := map[string]string{"X-Scope-OrgID": "0"}
					before := flipped(on)
					got := doCompatProxyRequest(on.p, target, headers)
					want := doCompatProxyRequest(off.p, target, headers)
					if got.Code != want.Code || !sameJSON(got.Body.String(), want.Body.String()) {
						t.Fatalf("answers differ\nflag on  %d %s\nflag off %d %s", got.Code, got.Body, want.Code, want.Body)
					}
					if reqs, wantReqs := on.up.take(), off.up.take(); !reflect.DeepEqual(reqs, wantReqs) {
						t.Fatalf("VictoriaLogs requests differ\nflag on  %q\nflag off %q", reqs, wantReqs)
					}
					native := (q.native || slices.ContainsFunc(strings.Split(q.construct, ","), func(c string) bool {
						return slices.Contains(on.p.flippedConstructs, c) || lower.LogStagesOf([]string{c})&on.p.flippedLogStages != 0
					})) && q.rejectedIn != prof.name
					if n := flipped(on) - before; native && n == 0 {
						t.Fatalf("%s was not served from the lowering", q.query)
					} else if !native && n != 0 {
						t.Fatalf("%s was served from the lowering %d times", q.query, n)
					}
					if n := flipped(off); n != 0 {
						t.Fatalf("flag off: %d flipped translations", n)
					}
				})
			}
		})
	}
}

// With the flag off nothing is translated by the lowering, and the shadow still compares the constructs it covers;
// with it on a flipped query has nothing to compare, and the constructs still on the translator are compared.
// conformance: architecture-syntax-package-layering
func TestQueryPlanLowering_ShadowCountsOnlyWhatStaysOnTheTranslator(t *testing.T) {
	for _, tc := range []struct {
		name    string
		disable bool
		want    map[string]int
	}{
		// `| json` is served from the lowering (the JSON stage family): nothing is left for the shadow to compare.
		{"flag on", false, map[string]int{"flipped LogSelect": 3, "not_lowered RangeAgg grouping": 1}},
		{"flag off", true, map[string]int{"match LogSelect": 3, "not_lowered RangeAgg grouping": 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := New(Config{
				BackendURL: "http://unused", Cache: cache.New(60*time.Second, 1000), LogLevel: "error",
				LabelStyle: LabelStyleUnderscores, TranslateShadowSample: 1, DisableQueryPlanLowering: tc.disable,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			for _, q := range []string{`{app="a"}`, `{app="a"} |> "<_> x" | status="200"`, `avg_over_time({app="a"} | unwrap n [5m]) without (pod)`, `{app="a"} | json`} {
				if _, err := translateAndSettle(t, p, q); err != nil {
					t.Fatal(err)
				}
			}
			if got := shadowCounts(t, p); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("shadow counts = %v, want %v", got, tc.want)
			}
		})
	}
}

// A log query with a filter on detected_level (the LevelFilter family) is served from the plan, with the string
// translator's text, with and without the body scan; one after a parser, and one on service_name, stay with the
// string translator (the lowering does not write them).
// conformance: architecture-syntax-package-layering
func TestQueryPlanLowering_ServesDetectedLevelFilters(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	for _, caps := range []logsql.Capabilities{{}, {NoDetectedLevelBodyScan: true}} {
		prof := loweringProfile(nil, nil, caps, false)
		for _, q := range []string{
			`{a="b"} | detected_level="error"`, `{a="b"} | detected_level!="info"`, `{a="b"} | detected_level=~"error|warn"`,
			`{a="b"} |= "x" | detected_level="unknown" | c="d"`, `{a="b"} | (detected_level="warn")`,
		} {
			want, err := translator.TranslateLogQLWithCapabilities(q, nil, nil, caps)
			if err != nil {
				t.Fatal(err)
			}
			if got, ok := p.lowerLogSelect(context.Background(), q, prof); !ok || got != want {
				t.Errorf("lowerLogSelect(%q) = %q, %v; want the translator's text %q", q, got, ok, want)
			}
		}
		for _, q := range []string{`{a="b"} | json | detected_level="error"`, `{a="b"} | service_name="x"`, `{a="b"} | detected_level`} {
			if got, ok := p.lowerLogSelect(context.Background(), q, prof); ok {
				t.Errorf("lowerLogSelect(%q) = %q, want the translator to answer", q, got)
			}
		}
	}
}

// A query the lowering cannot translate, or that panics in it, is translated by the string translator.
// conformance: architecture-syntax-package-layering
func TestQueryPlanLowering_FallsBackToTheTranslator(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	prof := loweringProfile(nil, nil, logsql.Capabilities{}, false)
	for _, q := range []string{"", "*", `rate({a="b"}[5m])`, `{a="b"} | unpack`, `{a="b" |= `, ` {a="b"}`} {
		if got, ok := p.lowerLogSelect(context.Background(), q, prof); ok {
			t.Errorf("lowerLogSelect(%q) = %q, want the translator to answer", q, got)
		}
	}
	if got, ok := p.lowerLogSelect(context.Background(), `{a="b"} |= "x"`, prof); !ok || got != `a:="b" ~"x"` {
		t.Fatalf("lowerLogSelect = %q, %v", got, ok)
	}
	// A stage the parser reads in any letter case is a stage: not served from the lowering, whether the screen or
	// the typed check turns it away.
	for _, q := range []string{`{a="b"} | JSON`, `{a="b"} | Logfmt | level="x"`, `{a="b"} | DROP c`, `{a="b"} | Keep c`} {
		if got, ok := p.lowerLogSelect(context.Background(), q, prof); ok {
			t.Errorf("lowerLogSelect(%q) = %q, want the translator to answer", q, got)
		}
	}
	// A profile whose label function panics: recovered, counted, and the translator's answer is used.
	boom := loweringProfile(func(string) string { panic("boom") }, nil, logsql.Capabilities{}, false)
	if got, ok := p.lowerLogSelect(context.Background(), `{a="b"}`, boom); ok {
		t.Fatalf("a panicking lowering answered %q", got)
	}
	if n := shadowCounts(t, p)["panic LogSelect"]; n != 1 {
		t.Fatalf("panic LogSelect = %d, want 1", n)
	}
}

// The live shadow and the flip build the lowering's profile from the translator's options with one helper.
// conformance: architecture-syntax-package-layering
func TestLoweringProfile_IsOneMappingForTheShadowAndTheFlip(t *testing.T) {
	label := func(s string) string { return "x_" + s }
	fields := map[string]bool{"app": true}
	caps := logsql.CapabilitiesFor("v1.49.0")
	caps.NoDetectedLevelBodyScan = true
	in := shadowInput{keepLine: true, label: label, streamFields: fields, caps: caps}
	got, want := in.profile(), loweringProfile(label, fields, caps, true)
	if got.Caps != want.Caps || got.KeepLine != want.KeepLine || !reflect.DeepEqual(got.StreamFields, want.StreamFields) || got.Label("a") != want.Label("a") {
		t.Fatalf("shadow profile %+v differs from %+v", got, want)
	}
}

// BenchmarkTranslateQueryMissShapesTranslator is BenchmarkTranslateQueryMissShapes with -query-plan-lowering=false:
// the same-code control for the lowering.
func BenchmarkTranslateQueryMissShapesTranslator(b *testing.B) {
	benchTranslateMissShapes(b, Config{DisableQueryPlanLowering: true})
}

// A log query that keeps its stored line aside (translateQueryOpts keepLine, in the Loki-compatible profile) is
// served from the lowering (the LineFormat stage family) and translated alike with the flag on and off.
// conformance: architecture-syntax-package-layering
func TestQueryPlanLowering_KeepLineIsTranslatedAlike(t *testing.T) {
	newProxy := func(disable bool) *Proxy {
		p, err := New(Config{
			BackendURL: "http://unused", Cache: cache.New(60*time.Second, 1000), LogLevel: "error",
			LabelStyle: LabelStyleUnderscores, MetadataFieldMode: MetadataFieldModeTranslated, DisableQueryPlanLowering: disable,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
		return p
	}
	on, off := newProxy(false), newProxy(true)
	for i, q := range []string{
		`{app="a"} | line_format "{{.status}}" | line_format "{{.app}}: {{.__line__}}"`,
		`{app="a"} |= "x" | line_format "{{.a}}" | status="200" | line_format "{{.b}}"`,
	} {
		if _, keep := on.backendLogQuery(q); !keep {
			t.Fatalf("%s does not keep the line aside: the case tests nothing", q)
		}
		got, err := on.translateLogResponseQuery(context.Background(), q)
		if err != nil {
			t.Fatal(err)
		}
		want, err := off.translateLogResponseQuery(context.Background(), q)
		if err != nil {
			t.Fatal(err)
		}
		if got != want || !strings.Contains(got, translator.StoredLineField) {
			t.Fatalf("keep-line translation differs or lost the stored line\n on  %s\n off %s", got, want)
		}
		// The LineFormat stage family serves it from the plan, with the line kept aside as the translator keeps it.
		if n := shadowCounts(t, on)["flipped LogSelect"]; n != i+1 {
			t.Fatalf("a keep-line query was not served from the lowering (%d)", n)
		}
	}
}

// The typed check behind the text screen: only a log query of line and label filters is served from the lowering.
// conformance: architecture-syntax-package-layering
func TestQueryPlanLowering_ServesFilterOnlyPlans(t *testing.T) {
	for q, want := range map[string]bool{
		`{a="b"}`: true, `{a="b"} |= "x" | c="d"`: true, `{a="b"} |> "<_> x"`: true,
		`{a="b"} | JSON`: false, `{a="b"} | logfmt`: false, `{a="b"} | drop c`: false, `{a="b"} | line_format "x"`: false,
		`rate({a="b"}[5m])`: false,
	} {
		plan, err := loweringPlan(context.Background(), q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if got := filterOnly(plan.Root); got != want {
			t.Errorf("filterOnly(%s) = %v, want %v", q, got, want)
		}
	}
}

// A stream selector or a regexp line filter written with an escape (\x41) is translated by the string translator
// with the flag on: it copies the client's spelling where the lowering, which holds the value, would write its own.
// The query is counted as not lowered, never as flipped, and the text is the translator's. A plain spelling of the
// same query is still served from the lowering.
// conformance: architecture-syntax-package-layering
func TestQueryPlanLowering_EscapedSpellingStaysOnTheTranslator(t *testing.T) {
	for _, tc := range []struct {
		name         string
		streamFields []string
		query, want  string
		plain, wantP string
	}{
		{"regexp line filter", nil, `{app="a"} |~ "\x41b"`, `app:="a" ~"\x41b"`, `{app="a"} |~ "Ab"`, `app:="a" ~"Ab"`},
		{"negated regexp line filter", nil, `{app="a"} !~ "\x41b"`, `app:="a" NOT ~"\x41b"`, `{app="a"} !~ "Ab"`, `app:="a" NOT ~"Ab"`},
		{"stream field selector", []string{"app"}, `{app="\x41"}`, `{app="\x41"}`, `{app="A"}`, `{app="A"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			translate := func(disable bool, q string) (string, map[string]int) {
				p, err := New(Config{
					BackendURL: "http://unused", Cache: cache.New(60*time.Second, 1000), LogLevel: "error",
					LabelStyle: LabelStyleUnderscores, StreamFields: tc.streamFields, TranslateShadowSample: 1, DisableQueryPlanLowering: disable,
				})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
				out, err := translateAndSettle(t, p, q)
				if err != nil {
					t.Fatal(err)
				}
				return out, shadowCounts(t, p)
			}
			got, counts := translate(false, tc.query)
			if want, _ := translate(true, tc.query); got != want || got != tc.want {
				t.Fatalf("flag on translated %q\nflag off translated %q\nwant %q", got, want, tc.want)
			}
			if counts["flipped LogSelect"] != 0 || counts["not_lowered LogSelect escaped spelling"] != 1 || shadowTotal(counts, "mismatch") != 0 {
				t.Fatalf("shadow counts = %v, want the query counted as not lowered", counts)
			}
			got, counts = translate(false, tc.plain)
			if got != tc.wantP || counts["flipped LogSelect"] != 1 {
				t.Fatalf("plain spelling translated %q, counts %v; want %q served from the lowering", got, counts, tc.wantP)
			}
		})
	}
}

// A metric query is served from the lowering when its plan is a flipped construct whose lowered text is the string
// translator's (writesTranslatorText), with that text; the plans the corpus shadow holds to anything else stay with
// the string translator: an evaluated level (a ranking, stddev or stdvar at any depth), a binary the binary route
// evaluates operand by operand, and the recorded divergences the plan writes as Loki reads them.
// conformance: architecture-syntax-package-layering
func TestQueryPlanLowering_ServesMetricConstructs(t *testing.T) {
	newProxy := func(disable bool) *Proxy {
		p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(60*time.Second, 1000), LogLevel: "error",
			LabelStyle: LabelStyleUnderscores, DisableQueryPlanLowering: disable})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
		return p
	}
	on, off := newProxy(false), newProxy(true)
	withFlipped(on, metricFlipCandidates...)
	prof := loweringProfile(translator.WithStreamLabels(on.labelTranslator.ToVL, nil), nil, on.logsqlCapabilities(), false)
	for _, tc := range []struct {
		query, construct string // construct is the flipped construct that serves the query; empty for none
	}{
		{`sum by (app) (rate({app="a"} |= "x" [5m]))`, "VectorAgg"},
		{`avg without (pod) (count_over_time({app="a"} | json [5m]))`, "VectorAgg"},
		{`max(sum by (app, pod) (bytes_over_time({app="a"}[5m])))`, "VectorAgg"},
		{`sum(quantile_over_time(0.9, {app="a"} | logfmt | unwrap d [5m]))`, "VectorAgg"},
		// An evaluated level at the top, nested, under a label_replace and in a binary operand.
		{`stddev(count_over_time({app="a"}[5m]))`, ""},
		{`stdvar by (app) (rate({app="a"}[5m]))`, ""},
		{`sum(stddev by (app) (count_over_time({app="a"}[5m])))`, ""},
		{`sum(topk(2, count_over_time({app="a"}[5m])))`, ""},
		{`topk(2, sum by (app) (count_over_time({app="a"}[5m])))`, ""},
		{`label_replace(stddev(count_over_time({app="a"}[5m])), "x", "$1", "app", "(.*)")`, ""},
		{`sum(rate({app="a"}[5m])) / stddev(rate({app="a"}[5m]))`, ""},
		// A binary the route evaluates operand by operand.
		{`sum without (pod) (rate({app="a"}[5m])) / 2`, ""},
		{`vector(1) + sum(rate({app="a"}[5m]))`, ""},
		// The plan writes Loki's reading, the translator another text.
		{`sum(count_over_time({app="a"} | json | status == 500 [5m]))`, ""},
		{`sum(sum_over_time({app="a"} | logfmt | unwrap _TIME [5m]))`, ""},
		{`sum by (app) (max_over_time({app="a"} | json | keep app, n | unwrap n [5m])) / 2`, ""},
		{`rate({app="a"}[5m])`, "RangeAgg"},
		{`count_over_time({app="a"} |> "<_> GET <_>" [5m])`, "RangeAgg"},
		{`sum(rate({app="a"}[5m])) / sum(rate({app="b"}[5m]))`, "BinOp"},
		{`sum(rate({app="a"}[5m])) * 2`, "BinOp"},
		{`label_replace(rate({app="a"}[5m]), "x", "$1", "app", "(.*)")`, "LabelReplace"},
		// Constructs the lowering does not cover, or does not flip.
		{`sum without () (count_over_time({app="a"}[5m]))`, ""},
		{`vector(1)`, ""},
		{`{app="a"} | json`, ""},
		{``, ""},
		{`sum(`, ""},
	} {
		got, ok := on.lowerMetric(context.Background(), tc.query, prof)
		if want := tc.construct != ""; ok != want {
			t.Errorf("%q: served from the lowering %v, want %v (%s)", tc.query, ok, want, got)
			continue
		}
		if !ok {
			continue
		}
		if want, err := off.translateQuery(tc.query); err != nil || got != want {
			t.Errorf("%q: served %q, the string translator writes %q (%v)", tc.query, got, want, err)
		}
	}
	// A query the lowering serves is counted as flipped under its construct, once.
	counts := shadowCounts(t, on)
	for c, want := range map[string]int{"VectorAgg": 4, "RangeAgg": 2, "BinOp": 2, "LabelReplace": 1} {
		if n := counts["flipped "+c]; n != want {
			t.Errorf("flipped %s = %d, want %d", c, n, want)
		}
	}
	// With the constructs served today (QueryPlanFlippedConstructs), a query is served from the lowering exactly when
	// its construct is one of them.
	today := newProxy(false)
	for q, c := range map[string]string{
		`sum by (app) (rate({app="a"} |= "x" [5m]))`: "VectorAgg", `rate({app="a"}[5m])`: "RangeAgg",
		`sum(rate({app="a"}[5m])) * 2`: "BinOp", `label_replace(rate({app="a"}[5m]), "x", "$1", "app", "(.*)")`: "LabelReplace",
	} {
		if got, ok := today.lowerMetric(context.Background(), q, prof); ok != slices.Contains(QueryPlanFlippedConstructs, c) {
			t.Errorf("%q: served from the lowering today %v (%s), flipped %v", q, ok, got, QueryPlanFlippedConstructs)
		}
	}
}

// The gate of the metric flip on typed plans: every branch of holdsEvaluatedLayer, and writesTranslatorText for each
// kind of plan it turns away.
// conformance: architecture-syntax-package-layering
func TestQueryPlanLowering_WritesTranslatorTextGate(t *testing.T) {
	prof := loweringProfile(nil, nil, logsql.Capabilities{}, false)
	for q, want := range map[string]bool{
		`rate({a="b"}[5m])`:      true,
		`sum(rate({a="b"}[5m]))`: true,
		`label_replace(sum(rate({a="b"}[5m])), "x", "$1", "a", "(.*)")`:                                 true,
		`sum(rate({a="b"}[5m])) / sum(rate({a="c"}[5m]))`:                                               true,
		`sort(sum(rate({a="b"}[5m])))`:                                                                  false,
		`sum(stdvar(rate({a="b"}[5m])))`:                                                                false,
		`label_replace(topk(1, rate({a="b"}[5m])), "x", "$1", "a", "(.*)")`:                             false,
		`sum(rate({a="b"}[5m])) / bottomk(1, sum(rate({a="c"}[5m])))`:                                   false,
		`stddev(rate({a="b"}[5m])) / sum(rate({a="c"}[5m]))`:                                            false,
		`sum without (x) (rate({a="b"}[5m])) > 1`:                                                       false,
		`sum(rate({a="b"}[5m])) * vector(2)`:                                                            false,
		`count_over_time({a="b"} | logfmt | n == 2 [5m])`:                                               false,
		`count_over_time({a="b"} | _stream="x" [5m])`:                                                   true,
		`sum(max_over_time({a="b"} | json | unwrap _Stream_ID [5m]))`:                                   false,
		`label_replace(max_over_time({a="b"} | json | keep n | unwrap n [5m]), "x", "$1", "a", "(.*)")`: false,
		"count_over_time({a=\"b\"} | s=`a detected_level b` [5m])":                                      false,
		`sum(count_over_time({a="b"} | s!~"service_name.*" [5m]))`:                                      false,
		`sum(count_over_time({a="b"} | o="\x02\x00\x02" | json | k="v" [5m]))`:                          false,
		`sum(count_over_time({a="b"} | json | (s="x" or o="\x01") [5m]))`:                               false,
		`sum by (a) (count_over_time({a="b"} | n > - .5 [5m]))`:                                         false,
		`count_over_time({a="b"} | d >= -5ms [5m]) / count_over_time({a="b"} | size < 1KB [5m])`:        false,
		`sum(count_over_time({a="b"} | n > 5 and s="x" [5m]))`:                                          true,
		`stddev_over_time({a="b"} | logfmt | unwrap n | s="detected" [5m])`:                             true,
	} {
		plan, err := loweringPlan(context.Background(), q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if got := writesTranslatorText(plan, prof); got != want {
			t.Errorf("writesTranslatorText(%s) = %v, want %v", q, got, want)
		}
	}
}

// While no metric construct is flipped, a metric query without both words of the keep identity is turned away before
// any plan is built, and one with them is still served with the keep's identity; a lowering that panics is recovered
// and counted under the construct it was planning.
// conformance: architecture-syntax-package-layering, semantics/keep-before-unwrap-series-identity
func TestQueryPlanLowering_MetricFlipScreenAndPanic(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	prof := loweringProfile(nil, nil, logsql.Capabilities{}, false)
	const kept = `first_over_time({app="a"} | json | keep app, n | unwrap n [5m])`
	p.flippedConstructs = []string{"LogSelect"}
	if lower.MetricSetOf(p.flippedConstructs) != 0 {
		t.Fatal("a metric construct is flipped")
	}
	if got, ok := p.lowerMetric(context.Background(), `sum(rate({a="b"}[5m]))`, prof); ok {
		t.Errorf("an unflipped metric construct was served: %s", got)
	}
	if _, ok := p.lowerMetric(context.Background(), kept, prof); !ok {
		t.Error("the keep identity is not served while no metric construct is flipped")
	}
	if got, ok := p.lowerMetric(context.Background(), `sum by (app) (max_over_time({a="b"} | json | keep app, n | unwrap n [5m])) / 2`, prof); ok {
		t.Errorf("a keep the proxy does not serve was served: %s", got)
	}
	withFlipped(p, metricFlipCandidates...)
	if lower.MetricSetOf(p.flippedConstructs) == 0 {
		t.Fatal("no metric construct is flipped")
	}
	boom := loweringProfile(func(string) string { panic("boom") }, nil, logsql.Capabilities{}, false)
	if got, ok := p.lowerMetric(context.Background(), `sum(rate({a="b"}[5m]))`, boom); ok {
		t.Fatalf("a panicking lowering answered %q", got)
	}
	if n := shadowCounts(t, p)["panic VectorAgg"]; n != 1 {
		t.Fatalf("panic VectorAgg = %d, want 1", n)
	}
}

// The recorded known divergences the proxy serves from the plan, where the string translator writes another text: a
// metric query with a label filter or a selector matcher on a name VictoriaLogs reserves (the plan filters the stream
// tag, the translator a field VictoriaLogs rejects or reads as its own filter), and a log query with a stage and a
// label filter value that holds the translator's marker bytes (the plan keeps the value, the translator cuts it). The
// requests to VictoriaLogs differ from the translator's by design; the Loki differentials hold the answers
// (TestReservedFieldLabelsMatchLoki, TestPlanMatchesLokiOnMetricQueries, TestMarkerBytesInLabelValueMeanWhatLokiMeans).
// The other kinds stay with the translator: a label filter value that holds detected_level or service_name or a signed
// number, after a stage of a log query or in a metric query, and a marker byte in a metric query.
//
// conformance: semantics/translator-writes-reserved-field-names, semantics/translator-reads-marker-bytes-in-quoted-value
func TestQueryPlanLowering_ServesTranslatorOnlyDivergences(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	prof := routeLoweringProfile()
	for _, tc := range []struct {
		query, served string // served is the plan's text the proxy sends; empty for the translator's
	}{
		{`count_over_time({app="a"} | _time="x" [5m])`, `app:="a" {_time="x"} | stats count()`},
		{`sum(count_over_time({app="a", _msg="x"}[5m]))`, `app:="a" {_msg="x"} | stats count()`},
		{`sum(rate({app="a"} | _TIME > 5 [5m]))`, `| copy _TIME as __lxrf_TIME | filter __lxrf_TIME:>5 | delete __lxrf_TIME |`},
		{`{app="a"} | o="\x02\x00\x02" | json`, "app:=\"a\" o:=\"\x02\x00\x02\" | unpack_json"},
		// Turned away: a reserved name in a clause, a drop, a log query with a stage; the other misread kinds.
		{`sum by (_time) (count_over_time({app="a"}[5m]))`, ""},
		{`count_over_time({app="a"} | drop _time [5m])`, ""},
		{`count_over_time({app="a"} | drop a | _time="x" [5m])`, ""},
		{`count_over_time({app="a"} | pattern "<_time>" | _time="x" [5m])`, ""},
		{`sum(count_over_time({app="a"} | keep a | _msg="x" [5m])) / sum(count_over_time({app="c"}[5m]))`, ""},
		{`{app="a"} | _time="x" | json`, ""},
		{`{app="a"} | s="detected_level" | json`, ""},
		{`{app="a"} | n > - 5 | json`, ""},
		{`count_over_time({app="a"} | o="\x02\x00\x02" | json [5m])`, ""},
	} {
		t.Run(tc.query, func(t *testing.T) {
			got, ok := p.PlanServedText(tc.query, prof)
			if want := tc.served != ""; ok != want {
				t.Fatalf("served from the plan %v (%s), want %v", ok, got, want)
			}
			if ok && !strings.Contains(got, tc.served) {
				t.Errorf("served %q, want %q", got, tc.served)
			}
		})
	}
}

// The label filter values the string translator reads as syntax, at every node of a plan: the gate reads the decoded
// value of each leaf, and a value that is only text is left to the flip.
// conformance: semantics/translator-reads-quoted-text-as-syntax, semantics/translator-reads-marker-bytes-in-quoted-value, semantics/translator-writes-signed-number-with-blank
func TestQueryPlanLowering_MisreadFilterValue(t *testing.T) {
	for q, want := range map[string]bool{
		`{a="b"} | s="detected_level"`:                                                         true,
		`topk(1, count_over_time({a="b"} | s=~"x|service_name" [5m]))`:                         true,
		`label_replace(rate({a="b"} | v="\x00" [5m]), "x", "$1", "a", "(.*)")`:                 true,
		`rate({a="b"}[5m]) + rate({a="c"} | n >= +1 [5m])`:                                     true,
		`sum(rate({a="b"} | (x="1" or (y="2" and z=-3)) [5m]))`:                                true,
		`topk(1, count_over_time({a="b"} | s="x" | n > 1.5 | d < 5ms | b >= 1KB [5m]))`:        false,
		`rate({a="b"} | addr = ip("10.0.0.0/8") [5m]) + rate({a="c"} | json | m=~"\x03" [5m])`: false,
		`vector(1)`: false,
	} {
		plan, err := loweringPlan(context.Background(), q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if got := misreadFilterValue(plan.Root, misreadAny); got != want {
			t.Errorf("misreadFilterValue(%s) = %v, want %v", q, got, want)
		}
	}
	if misreadLeaf(nil) != 0 {
		t.Error("a nil filter is misread")
	}
	// Each kind on its own: the log gate serves the marker kind and turns the other two away.
	for q, want := range map[string]misreadKind{
		`{a="b"} | s="detected_level"`:             misreadText,
		`{a="b"} | v="\x02\x00\x02" | json`:        misreadMarker,
		`{a="b"} | n >= - 1`:                       misreadSign,
		`{a="b"} | s="service_name\x01" or n < +2`: misreadText | misreadMarker | misreadSign,
		`{a="b"} | s="x" | n > 1`:                  0,
	} {
		plan, err := loweringPlan(context.Background(), q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		var got misreadKind
		for _, k := range []misreadKind{misreadText, misreadMarker, misreadSign} {
			if misreadFilterValue(plan.Root, k) {
				got |= k
			}
		}
		if got != want {
			t.Errorf("%s: misread kinds %b, want %b", q, got, want)
		}
	}
}

// The lowering reads the plan a handler already made for the query (the plan memo) instead of parsing it again: the
// handlers' resolved plan is used as it is, a query they only parsed is built from the shared parse, and only a query
// they never saw is parsed, counted in the request's flow, and left out of the memo (a translation's plan is not one
// the handlers read).
//
// conformance: architecture-syntax-package-layering, observability-query-flow
func TestQueryPlanLowering_ReadsTheHandlersPlan(t *testing.T) {
	planned := `sum(quantile_over_time(0.9, {app="memo-planned"} | json | keep app, d | unwrap d [5m]))`
	want := resolvedPlan(planned)
	if want == nil {
		t.Fatal("no plan")
	}
	ctx, _ := flowCtx()
	got, err := loweringPlan(ctx, planned)
	if err != nil || got != want {
		t.Fatalf("the handlers' plan is not read: %p %v, want %p", got, err, want)
	}
	parsed := `sum by (app) (rate({app="memo-parsed"}[5m]))`
	if _, ok := sharedLogQLExpr(parsed); !ok {
		t.Fatal("no parse")
	}
	built, err := loweringPlan(ctx, parsed)
	if err != nil || lower.Construct(built.Root) != "VectorAgg" || !built.Sem.Resolved {
		t.Fatalf("a shared parse is not built and resolved: %v %v", built, err)
	}
	if f := flowOf(ctx); f.parseCount != 0 {
		t.Fatalf("a query the handlers saw was parsed again: %d parses", f.parseCount)
	}
	unseen := `sum by (app) (rate({app="memo-unseen"}[5m]))`
	if _, err := loweringPlan(ctx, unseen); err != nil {
		t.Fatal(err)
	}
	if f := flowOf(ctx); f.parseCount != 1 {
		t.Fatalf("a query the handlers never saw: %d parses, want 1", f.parseCount)
	}
	if _, stored := planMemoLoad(unseen); stored {
		t.Fatal("the lowering stored its plan in the handlers' memo")
	}
	if _, err := loweringPlan(ctx, `sum(rate(`); err == nil {
		t.Fatal("a query that does not parse has no plan")
	}
}
