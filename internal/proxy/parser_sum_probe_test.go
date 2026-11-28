package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// parserSumRequest is the instant or the range request of q.
func parserSumRequest(q string, instant bool) string {
	if instant {
		return "/loki/api/v1/query?" + url.Values{"query": {q}, "time": {strconv.FormatInt(parserProbeAt/1e9, 10)}}.Encode()
	}
	return "/loki/api/v1/query_range?" + url.Values{"query": {q}, "start": {strconv.FormatInt(parserProbeAt/1e9-3600, 10)}, "end": {strconv.FormatInt(parserProbeAt/1e9, 10)}, "step": {"60"}}.Encode()
}

// A sum with no label and no filter after its json and logfmt parsers runs no parser in Loki: the count Logs
// Drilldown sends on every service page, `sum(count_over_time({…} | json | logfmt | drop __error__,
// __error_details__ [3h]))`, is the number of selected lines (#1821, item 4). The proxy read 25 lines to choose one of
// the two parsers (the probe) and made VictoriaLogs run `unpack_logfmt` on every selected line; it now sends one
// stats read with no parser pipe and no probe, at an instant and over a range. A filter or a grouping after the
// parsers keeps today's probe and pipes, and -query-plan-lowering=false is today's path.
//
// conformance: semantics/ungrouped-sum-skips-parsers, semantics/instant-parser-probe-window, semantics/two-parsers-keep-one, loki_api_v1_query, loki_api_v1_query_range
func TestUngroupedParserSumSendsNoProbe(t *testing.T) {
	const pipe = `{app="x"} | json | logfmt | drop __error__, __error_details__`
	skipped := []string{
		`sum(count_over_time(` + pipe + ` [3h]))`,
		`sum(count_over_time({app="x"} | json | logfmt [3h]))`,
		`sum(count_over_time({app="x"} | logfmt | json [3h]))`,
		`sum(count_over_time({app="x"} | logfmt [3h]))`,
		`sum(count_over_time({app="x"} |= "k" | json | logfmt [3h] offset 1h))`,
		`sum by () (count_over_time(` + pipe + ` [3h]))`,
		`sum(rate(` + pipe + ` [3h]))`,
		`sum(count_over_time(` + pipe + ` | keep a [3h]))`,
		`sum(count_over_time(` + pipe + ` [3h])) * 2`,
		`label_replace(sum(count_over_time(` + pipe + ` [3h])), "a", "b", "", "")`,
		`sort(sum(count_over_time(` + pipe + ` [3h])))`,
	}
	probed := []string{
		`sum(count_over_time(` + pipe + ` | a!="" [3h]))`,
		`sum(rate(` + pipe + ` | keep a [3h]))`,
		`count_over_time(` + pipe + ` [3h])`,
	}
	for _, instant := range []bool{true, false} {
		for _, q := range skipped {
			b := &parserProbeBackend{lines: func(string, string) []string { return []string{"level=info msg=hi"} }}
			p := newParserProbeProxy(t, b)
			res := doCompatProxyRequest(p, parserSumRequest(q, instant), map[string]string{"X-Scope-OrgID": "0"})
			if res.Code != http.StatusOK {
				t.Fatalf("instant %v %s: %d %s", instant, q, res.Code, res.Body)
			}
			if len(b.probes) != 0 || len(b.reads) != 1 {
				t.Errorf("instant %v %s: %d probes, reads %q; want no probe and one read", instant, q, len(b.probes), b.reads)
				continue
			}
			if strings.Contains(b.reads[0], "unpack_") || !strings.HasPrefix(b.reads[0], "/select/logsql/stats_query") {
				t.Errorf("instant %v %s: read %q, want stats with no parser pipe", instant, q, b.reads[0])
			}
		}
		for _, q := range probed {
			b := &parserProbeBackend{lines: func(string, string) []string { return []string{"level=info msg=hi"} }}
			p := newParserProbeProxy(t, b)
			doCompatProxyRequest(p, parserSumRequest(q, instant), map[string]string{"X-Scope-OrgID": "0"})
			if len(b.probes) != 1 {
				t.Errorf("instant %v %s: %d probes, want the probe (today's path)", instant, q, len(b.probes))
			}
		}
	}

	t.Run("query plan lowering off", func(t *testing.T) {
		b := &parserProbeBackend{lines: func(string, string) []string { return []string{"level=info msg=hi"} }}
		backend := httptest.NewServer(http.HandlerFunc(b.handler))
		t.Cleanup(backend.Close)
		p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second, DisableQueryPlanLowering: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
		if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
			t.Fatal(err)
		}
		res := doCompatProxyRequest(p, parserSumRequest(skipped[0], true), map[string]string{"X-Scope-OrgID": "0"})
		if res.Code != http.StatusOK || len(b.probes) != 1 || len(b.reads) != 1 || !strings.Contains(b.reads[0], "unpack_logfmt") {
			t.Fatalf("%d, %d probes, reads %q: want today's probe and logfmt pipe", res.Code, len(b.probes), b.reads)
		}
	})
}

// BenchmarkParserProbeDecision is the proxy's cost of the decision that skips the probe, on the Drilldown count it
// skips and on a grouped field breakdown it leaves to the probe (the plan is read from the shared memo).
func BenchmarkParserProbeDecision(b *testing.B) {
	p := &Proxy{queryPlanLowering: true, flippedConstructs: QueryPlanFlippedConstructs}
	for _, tc := range []struct{ name, q string }{
		{"skipped", `sum(count_over_time({cluster="c1", namespace="ns1", container="ingress"} | json | logfmt | drop __error__, __error_details__ [3h]))`},
		{"grouped", `sum by (status) (count_over_time({cluster="c1", namespace="ns1", container="ingress"} | json | logfmt | drop __error__, __error_details__ | status!="" [3h]))`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			want := tc.name == "skipped"
			b.ReportAllocs()
			for b.Loop() {
				if p.planSkipsParsers(tc.q) != want {
					b.Fatal("decision")
				}
			}
		})
	}
}

// BenchmarkUngroupedParserSumRequest is one cold request through the handlers to an in-process VictoriaLogs stub (the
// response cache is off, so every iteration plans, probes where it does and reads): the Drilldown count the plan
// skips, and a grouped field breakdown it leaves to the probe and the parsers.
func BenchmarkUngroupedParserSumRequest(b *testing.B) {
	const pipe = `{cluster="c1", namespace="ns1", container="ingress"} | json | logfmt | drop __error__, __error_details__`
	for _, tc := range []struct{ name, q string }{
		{"skipped", `sum(count_over_time(` + pipe + ` [3h]))`},
		{"grouped", `sum by (status) (count_over_time(` + pipe + ` | status!="" [3h]))`},
	} {
		for _, instant := range []bool{true, false} {
			name := tc.name + "/range"
			if instant {
				name = tc.name + "/instant"
			}
			b.Run(name, func(b *testing.B) {
				backend := &parserProbeBackend{lines: func(string, string) []string { return []string{"level=info status=200 msg=hi"} }}
				srv := httptest.NewServer(http.HandlerFunc(backend.handler))
				defer srv.Close()
				p, err := New(Config{BackendURL: srv.URL, Cache: cache.New(time.Nanosecond, 1), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
				if err != nil {
					b.Fatal(err)
				}
				defer func() { _ = p.Shutdown(context.Background()) }()
				if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
					b.Fatal(err)
				}
				target := parserSumRequest(tc.q, instant)
				b.ReportAllocs()
				for b.Loop() {
					if res := doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "0"}); res.Code != http.StatusOK {
						b.Fatal(res.Code)
					}
					backend.mu.Lock()
					backend.probes, backend.reads = backend.probes[:0], backend.reads[:0]
					backend.mu.Unlock()
				}
			})
		}
	}
}

// The probe is skipped only where the request's own translation runs no parser, under every label, field and tenant
// profile: a profile the lowering turns away keeps today's probe and pipes, never both parsers without the probe
// (which the handlers would read as a mixed-parser metric and answer from raw rows). The issue's selector names
// dotted stream fields through field mappings.
//
// conformance: semantics/ungrouped-sum-skips-parsers, semantics/two-parsers-keep-one
func TestUngroupedParserSumProbeSkipFollowsTheTranslation(t *testing.T) {
	mappings := []FieldMapping{{VLField: "k8s.cluster.name", LokiLabel: "cluster"}, {VLField: "k8s.namespace.name", LokiLabel: "namespace"}}
	profiles := []struct {
		name string
		cfg  func(*Config)
	}{
		{"default", func(*Config) {}},
		{"underscores translated", func(c *Config) { c.LabelStyle, c.MetadataFieldMode = LabelStyleUnderscores, MetadataFieldModeTranslated }},
		{"passthrough native", func(c *Config) { c.LabelStyle, c.MetadataFieldMode = LabelStylePassthrough, MetadataFieldModeNative }},
		{"hybrid", func(c *Config) { c.LabelStyle, c.MetadataFieldMode = LabelStyleUnderscores, MetadataFieldModeHybrid }},
		{"field mappings original", func(c *Config) { c.FieldMappings = mappings }},
		{"field mappings alias", func(c *Config) { c.FieldMappings, c.FieldMappingLabelMode = mappings, FieldMappingLabelModeAlias }},
		{"field mappings both", func(c *Config) { c.FieldMappings, c.FieldMappingLabelMode = mappings, FieldMappingLabelModeBoth }},
		{"stream fields", func(c *Config) { c.StreamFields = []string{"cluster", "namespace", "container"} }},
		{"tenant label", func(c *Config) { c.TenantLabel = "tenant" }},
	}
	const pipe = `{cluster="c1", namespace="ns1", container="ingress"} | json | logfmt | drop __error__, __error_details__`
	queries := []string{
		`sum(count_over_time(` + pipe + ` [3h]))`,
		`sum(rate(` + pipe + ` [3h]))`,
		`sort(sum(count_over_time(` + pipe + ` [3h])))`,
	}
	skipped := 0
	for _, prof := range profiles {
		for _, q := range queries {
			for _, instant := range []bool{true, false} {
				b := &parserProbeBackend{lines: func(string, string) []string { return []string{"level=info msg=hi"} }}
				backend := httptest.NewServer(http.HandlerFunc(b.handler))
				cfg := Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second}
				prof.cfg(&cfg)
				p, err := New(cfg)
				if err != nil {
					backend.Close()
					t.Fatalf("%s: %v", prof.name, err)
				}
				if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
					t.Fatal(err)
				}
				tenant := "0"
				if cfg.TenantLabel != "" {
					tenant = "team-a"
				}
				res := doCompatProxyRequest(p, parserSumRequest(q, instant), map[string]string{"X-Scope-OrgID": tenant})
				_ = p.Shutdown(context.Background())
				backend.Close()
				if res.Code != http.StatusOK {
					t.Errorf("%s instant %v %s: %d %s", prof.name, instant, q, res.Code, res.Body)
					continue
				}
				if len(b.probes) != 0 {
					t.Logf("%s instant %v %s: probed (today's path)", prof.name, instant, q)
					continue // today's path: the probe and the parser it keeps
				}
				skipped++
				for _, read := range b.reads {
					if strings.Contains(read, "unpack_") || strings.HasPrefix(read, "/select/logsql/query ") {
						t.Errorf("%s instant %v %s: no probe, but read %q", prof.name, instant, q, read)
					}
				}
			}
		}
	}
	if skipped < len(queries)*2 {
		t.Errorf("only %d runs skipped the probe", skipped)
	}
}

// Answers cached before the sum ran no parser are fenced, for every spelling of the shape and for no other query: the
// count over one parser answered other values on main when a key was named like a VictoriaLogs field
// (TestParserSumMatchesLoki), and the key now carries skippedParsersResponseVersion.
//
// conformance: semantics/ungrouped-sum-skips-parsers, cache-response-key-versions
func TestSkippedParsersResponseFence(t *testing.T) {
	for _, q := range []string{
		`sum(count_over_time({app="x"} | logfmt [5m]))`,
		`SUM(count_over_time({app="x"} | LOGFMT [5m]))`,
		`sum(count_over_time({app="x"} # a comment
			| json [5m]))`,
		`sum by () (rate({app="x"} | json | logfmt | drop __error__, __error_details__ [5m]))`,
		`sum(count_over_time({app="x"} | logfmt [5m])) * 2`,
		`sort(sum(count_over_time({app="x"} | logfmt [5m])))`,
	} {
		if got := responseKeyFences(httptest.NewRequest(http.MethodGet, parserSumRequest(q, false), nil), q, responseKeyInnerQueryRange); !strings.Contains(got, skippedParsersResponseVersion) {
			t.Errorf("%q: fences %q, want %s", q, got, skippedParsersResponseVersion)
		}
	}
	for _, q := range []string{
		`sum by (a) (count_over_time({app="x"} | logfmt [5m]))`,
		`sum(count_over_time({app="x"} | logfmt | a="b" [5m]))`,
		`sum(count_over_time({app="x"} | unpack [5m]))`,
		`sum(count_over_time({app="x"} [5m]))`,
		`count_over_time({app="x"} | json [5m])`,
	} {
		if got := responseKeyFences(httptest.NewRequest(http.MethodGet, parserSumRequest(q, false), nil), q, responseKeyInnerQueryRange); strings.Contains(got, skippedParsersResponseVersion) {
			t.Errorf("%q: fences %q, want no %s", q, got, skippedParsersResponseVersion)
		}
	}
}
