package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
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
}{
	// Stream selectors.
	{"selector", `{app="a"}`, true, ""},
	{"selector regexp and not", `{app=~"a|b", env!="x", job!~"j.*"}`, true, ""},
	{"selector with dotted and service_name", `{service_name="a", k8s_pod_name="p"}`, true, ""},
	// Line filters.
	{"contains", `{app="a"} |= "error"`, true, ""},
	{"chain of line filters", `{app="a"} |= "error" != "debug" |~ "e.r+" !~ "to.*"`, true, ""},
	{"case insensitive", `{app="a"} |~ "(?i)error"`, true, ""},
	{"line filter or", `{app="a"} |= "a" or "b" or "c"`, true, ""},
	{"ip line filter", `{app="a"} |= ip("10.0.0.0/8")`, true, ""},
	{"not ip line filter", `{app="a"} != ip("1.2.3.4")`, true, ""},
	{"quotes and unicode", "{app=\"a\"} |= `a\"b` |= \"żółć\\n\"", true, ""},
	// A regexp the client wrote with an escape is sent as written (the lowering holds only the value), so the
	// translator serves it; the substring filter quotes its value, so the lowering still does.
	{"escaped regexp filter", `{app="a"} |~ "\x41b"`, false, ""},
	{"escaped negated regexp filter", `{app="a"} !~ "\101b"`, false, ""},
	{"escaped regexp or chain", `{app="a"} |~ "\x41" or "b"`, true, ""},
	{"escaped substring filter", `{app="a"} |= "\x41b"`, true, ""},
	{"escaped label filter", `{app="a"} | b=~"\x41"`, true, ""},
	{"quote and backslash regexp filter", `{app="a"} |~ "a\"b\\d+"`, true, ""},
	// Pattern filters.
	{"pattern", `{app="a"} |> "<_> GET <_> <_>"`, true, ""},
	{"not pattern", `{app="a"} !> "<_> POST <_>"`, true, ""},
	{"pattern or chain", `{app="a"} |> "a<_>" or "b<_>" or "<_> c"`, true, ""},
	{"pattern literal", `{app="a"} |> "error"`, true, ""},
	{"pattern then line filter", `{app="a"} |> "<_> GET <_>" |= "200"`, true, ""},
	{"line filter then pattern", `{app="a"} |= "200" |> "<_> GET <_>" != "x"`, true, ""},
	// Label filters with no parser.
	{"label filter string", `{app="a"} | status="200"`, true, ""},
	{"label filter numeric", `{app="a"} | status >= 500`, true, ""},
	{"label filter and or", `{app="a"} | status >= 500 and status < 600 or code="x"`, true, ""},
	{"label filter regexp", `{app="a"} | path =~ "/api/.*" | path !~ "/api/v0"`, true, ""},
	{"label filter duration bytes", `{app="a"} | latency > 250ms | size < 1MB`, true, ""},
	{"repeated label filter", `{app="a"} | status="200" | status!="500" | status="200"`, true, ""},
	{"ip label filter", `{app="a"} | addr = ip("10.0.0.0/8")`, true, ""},
	{"not ip label filter", `{app="a"} | addr != ip("192.168.0.0/16") | addr = ip("1.2.3.4-1.2.3.9")`, true, ""},
	{"label filters among line filters", `{app="a"} |= "x" | status="200" |> "<_> y" | code != "z" != "w"`, true, ""},
	{"dotted label filter", `{app="a"} | k8s.pod.name = "p"`, true, "loki compatible"},
	// Not covered by the lowering: the string translator answers in both runs.
	{"detected_level filter", `{app="a"} | detected_level="error"`, false, ""},
	{"service_name label filter", `{app="a"} | service_name="x"`, false, ""},
	{"json", `{app="a"} | json | status="200"`, false, ""},
	{"logfmt", `{app="a"} | logfmt | level="error"`, false, ""},
	{"line_format", `{app="a"} | line_format "{{.status}}"`, false, ""},
	// Two line_format stages keep the stored line aside in the Loki-compatible profile (translateQueryOpts keepLine).
	{"keep line", `{app="a"} | line_format "{{.status}}" | line_format "{{.app}}: {{.__line__}}"`, false, ""},
	{"keep line with filters", `{app="a"} |= "x" | line_format "{{.a}}" | status="200" | line_format "{{.b}}"`, false, ""},
	{"detected_level and service_name filters", `{service_name="a"} |= "x" | detected_level="error" | service_name="a"`, false, ""},
	{"decolorize", `{app="a"} | decolorize |= "x"`, false, ""},
	{"drop", `{app="a"} | drop status`, false, ""},
	{"unwrap metric", `sum(rate({app="a"} | json | unwrap bytes [5m]))`, false, ""},
	{"rate", `rate({app="a"} |= "x" [5m])`, false, ""},
	{"sum by", `sum by (level) (count_over_time({app="a"} | status="200" [1m]))`, false, ""},
	{"binary", `rate({app="a"}[5m]) / rate({app="b"}[5m])`, false, ""},
	{"count_over_time", `count_over_time({app="a"} |> "<_> GET <_>" [5m])`, false, ""},
	{"label_replace", `label_replace(rate({app="a"}[5m]), "x", "$1", "app", "(.*)")`, false, ""},
	{"invalid", `{app="a"} |= `, false, ""},
	{"invalid regexp", `{app="a"} | path =~ "("`, false, ""},
}

// A native query is served from the lowering with the flag on and from the string translator with it off, and the
// client cannot tell: the same status and body, and the same requests to VictoriaLogs, under the translator
// profiles that change the translation (label style, stream fields, a pinned older VictoriaLogs, the body scan,
// the Loki-compatible profile and the multi-tenant label mode).
// conformance: architecture-syntax-package-layering, loki_api_v1_query_range
func TestQueryPlanLowering_RouteParity(t *testing.T) {
	profiles := []struct {
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
	for _, prof := range profiles {
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
					p.backendVersionMu.Lock()
					p.backendVersionSemver = prof.version
					p.backendVersionMu.Unlock()
				}
				up.take() // the version probe, if any
				return run{p, up}
			}
			on, off := start(false), start(true)
			if on.p.queryPlanLowering == off.p.queryPlanLowering {
				t.Fatal("the two proxies translate the same way")
			}
			flipped := func(r run) int { return shadowCounts(t, r.p)["flipped LogSelect"] }
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
					native := q.native && q.rejectedIn != prof.name
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
		// `| json` is lowered natively but not served from the lowering (a parser stage): the shadow compares it.
		{"flag on", false, map[string]int{"flipped LogSelect": 2, "match LogSelect": 1, "not_lowered RangeAgg": 1}},
		{"flag off", true, map[string]int{"match LogSelect": 3, "not_lowered RangeAgg": 1}},
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
			for _, q := range []string{`{app="a"}`, `{app="a"} |> "<_> x" | status="200"`, `rate({app="a"}[5m])`, `{app="a"} | json`} {
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

// A query the lowering cannot translate, or that panics in it, is translated by the string translator.
// conformance: architecture-syntax-package-layering
func TestQueryPlanLowering_FallsBackToTheTranslator(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	prof := loweringProfile(nil, nil, logsql.Capabilities{}, false)
	for _, q := range []string{"", "*", `rate({a="b"}[5m])`, `{a="b"} | json`, `{a="b" |= `, ` {a="b"}`} {
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

// A log query that keeps its stored line aside (translateQueryOpts keepLine, in the Loki-compatible profile) is not
// covered by the lowering, and is translated alike with the flag on and off.
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
	for _, q := range []string{
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
		if n := shadowCounts(t, on)["flipped LogSelect"]; n != 0 {
			t.Fatalf("a keep-line query was served from the lowering (%d)", n)
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
