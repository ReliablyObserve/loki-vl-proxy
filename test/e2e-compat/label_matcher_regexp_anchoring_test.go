//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// A stream-selector regexp matches the whole label value in Loki: Prometheus
// compiles {app=~"api|web"} as ^(?s:api|web)$
// (vendor/github.com/prometheus/prometheus/model/labels/regexp.go:76), so it
// selects api and web, never api-gateway, my-api or webhook. VictoriaLogs'
// field:~"re" is a substring match, and the proxy passed the pattern through,
// which is the widening a Grafana multi-value variable hits
// (profiles/stream-selector-regexp-anchoring). A label filter after a parser
// is matched after Loki's regexp simplifications instead
// (pkg/logql/log/filter.go parseRegexpFilter, RegexSimplifier.Simplify), which
// keeps some substring checks (profiles/label-filter-regexp-anchoring).
//
// The fixture's values are substrings of each other: app api / api-gateway /
// my-api / web / webhook, env prod / prod-eu / nonprod.

type anchorFixtures struct {
	probe      string
	start, end time.Time
	lines      int
}

var (
	anchorOnce      sync.Once
	anchorFixtures_ *anchorFixtures
)

var anchorStreams = []struct{ app, env string }{
	{"api", "prod"},
	{"api-gateway", "prod-eu"},
	{"my-api", "nonprod"},
	{"web", "prod"},
	{"webhook", "nonprod"},
}

const anchorLinesPerStream = 20

func ensureAnchorFixtures(t *testing.T) *anchorFixtures {
	t.Helper()
	anchorOnce.Do(func() {
		now := time.Now()
		fx := &anchorFixtures{
			probe: fmt.Sprintf("anchor%d", now.UnixNano()),
			start: now.Add(-12 * time.Minute).Truncate(time.Second),
			end:   now.Add(time.Minute).Truncate(time.Second),
		}
		var vlRows strings.Builder
		var lokiStreams []any
		for i, s := range anchorStreams {
			labels := map[string]string{"probe": fx.probe, "app": s.app, "env": s.env}
			var values [][]string
			for n := 0; n < anchorLinesPerStream; n++ {
				at := fx.start.Add(time.Minute + time.Duration(n)*30*time.Second + time.Duration(i+1)*111*time.Millisecond)
				line := fmt.Sprintf(`{"svc":%q,"stage":%q,"n":%d}`, s.app, s.env, n)
				values = append(values, []string{strconv.FormatInt(at.UnixNano(), 10), line})
				row := map[string]string{"_time": at.UTC().Format(time.RFC3339Nano), "_msg": line}
				for k, v := range labels {
					row[k] = v
				}
				encoded, _ := json.Marshal(row)
				vlRows.Write(encoded)
				vlRows.WriteByte('\n')
			}
			fx.lines += len(values)
			lokiStreams = append(lokiStreams, map[string]any{"stream": labels, "values": values})
		}
		status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields="+url.QueryEscape("probe,app,env"), vlRows.String(), map[string]string{"Content-Type": "application/stream+json"})
		if status != http.StatusOK {
			t.Fatalf("VL ingest: %d %s", status, body)
		}
		payload, _ := json.Marshal(map[string]any{"streams": lokiStreams})
		if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json", "X-Scope-OrgID": "0"}); status != http.StatusNoContent {
			t.Fatalf("Loki ingest: %d %s", status, body)
		}
		forceVLFlush(t)
		// Both backends hold every line, and Loki's metric path answers (it is
		// empty for a few minutes after a fresh stack starts).
		query := fmt.Sprintf(`sum(count_over_time({probe=%q}[15m]))`, fx.probe)
		deadline := time.Now().Add(240 * time.Second)
		for {
			lokiTotal := anchorInstantByApp(t, lokiURL, query, fx.end)[""]
			proxyTotal := anchorInstantByApp(t, proxyURL, query, fx.end)[""]
			if lokiTotal == float64(fx.lines) && proxyTotal == float64(fx.lines) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("fixture not ready: %d lines, Loki counts %v, proxy %v", fx.lines, lokiTotal, proxyTotal)
			}
			time.Sleep(2 * time.Second)
		}
		anchorFixtures_ = fx
	})
	if anchorFixtures_ == nil {
		t.Fatal("the anchoring fixtures were not ingested; see the first test that ran")
	}
	return anchorFixtures_
}

func anchorGet(t *testing.T, base, path string, params url.Values, headers map[string]string) map[string]any {
	t.Helper()
	if headers == nil {
		headers = map[string]string{"X-Scope-OrgID": "0"}
	}
	status, body, resp := doJSONGET(t, base+path+"?"+params.Encode(), headers)
	if status != http.StatusOK || resp == nil {
		t.Fatalf("%s%s %v: %d %s", base, path, params, status, body)
	}
	if s, ok := resp["status"]; ok && s != "success" {
		t.Fatalf("%s%s %v: %s", base, path, params, body)
	}
	if resp["warnings"] != nil {
		t.Fatalf("%s%s %v: warnings in %s", base, path, params, body)
	}
	return resp
}

// anchorInstantByApp returns an instant vector as app label -> value.
func anchorInstantByApp(t *testing.T, base, query string, at time.Time) map[string]float64 {
	t.Helper()
	resp := anchorGet(t, base, "/loki/api/v1/query", url.Values{"query": {query}, "time": {strconv.FormatInt(at.Unix(), 10)}}, nil)
	out := map[string]float64{}
	for _, item := range extractArray(extractMap(resp, "data"), "result") {
		sample, _ := item.(map[string]any)
		metric, _ := sample["metric"].(map[string]any)
		value, _ := sample["value"].([]any)
		if len(value) != 2 {
			t.Fatalf("%s %s: unexpected sample %v", base, query, sample)
		}
		v, err := strconv.ParseFloat(fmt.Sprint(value[1]), 64)
		if err != nil {
			t.Fatalf("%s %s: invalid value %v", base, query, value)
		}
		app, _ := metric["app"].(string)
		out[app] = v
	}
	return out
}

// anchorLogLines returns, per app label, the lines a log query answers.
func anchorLogLines(t *testing.T, base string, fx *anchorFixtures, query string, headers map[string]string) map[string][]string {
	t.Helper()
	resp := anchorGet(t, base, "/loki/api/v1/query_range", url.Values{
		"query":     {query},
		"start":     {strconv.FormatInt(fx.start.UnixNano(), 10)},
		"end":       {strconv.FormatInt(fx.end.UnixNano(), 10)},
		"limit":     {"5000"},
		"direction": {"forward"},
	}, headers)
	out := map[string][]string{}
	for _, item := range extractArray(extractMap(resp, "data"), "result") {
		stream, _ := item.(map[string]any)
		labels, _ := stream["stream"].(map[string]any)
		key := fmt.Sprint(labels["app"])
		if tenant, ok := labels["__tenant_id__"]; ok {
			key += "@" + fmt.Sprint(tenant)
		}
		values, _ := stream["values"].([]any)
		for _, raw := range values {
			pair, _ := raw.([]any)
			if len(pair) == 2 {
				out[key] = append(out[key], fmt.Sprint(pair[0])+" "+fmt.Sprint(pair[1]))
			}
		}
	}
	for k := range out {
		sort.Strings(out[k])
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func anchorStringSet(values []any) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		out = append(out, fmt.Sprint(v))
	}
	sort.Strings(out)
	return out
}

// assertAnchorSet fails when Loki answered nothing, answered a value the
// unanchored widening would add, or the proxy answered another set.
func assertAnchorSet(t *testing.T, what string, loki, proxy, want []string) {
	t.Helper()
	if len(loki) == 0 {
		t.Fatalf("%s: Loki answered nothing; parity would be empty-vs-empty", what)
	}
	if fmt.Sprint(loki) != fmt.Sprint(want) {
		t.Fatalf("%s: Loki answered %v, the fixture expects %v (unhealthy comparison)", what, loki, want)
	}
	if fmt.Sprint(proxy) != fmt.Sprint(loki) {
		t.Errorf("%s: proxy %v, Loki %v", what, proxy, loki)
	}
}

// TestCompat_LabelMatcherRegexpAnchoring compares Loki and the proxy on
// selector regexps whose pattern is a substring of other values, over a log
// query, a metric query, label values, series, volume and detected labels;
// label filters after a parser, including Loki's substring simplification of
// a factored alternation; and the multi-tenant __tenant_id__ matcher.
//
// conformance: loki_api_v1_detected_labels, loki_api_v1_query_range, loki_api_v1_query, loki_api_v1_label_name_values, loki_api_v1_series, loki_api_v1_index_volume, profiles/stream-selector-regexp-anchoring, profiles/label-filter-regexp-anchoring
func TestCompat_LabelMatcherRegexpAnchoring(t *testing.T) {
	fx := ensureAnchorFixtures(t)
	sel := func(matchers string) string { return fmt.Sprintf(`{probe=%q, %s}`, fx.probe, matchers) }

	t.Run("log query", func(t *testing.T) {
		for _, tc := range []struct {
			query string
			want  []string
		}{
			{sel(`app=~"api|web"`), []string{"api", "web"}},
			{sel(`app=~"api"`), []string{"api"}},
			{sel(`app!~"api|web"`), []string{"api-gateway", "my-api", "webhook"}},
			{sel(`env=~"prod"`), []string{"api", "web"}},
			{sel(`env=~"prod|prod-eu"`), []string{"api", "api-gateway", "web"}},
			{sel(`app=~"(?i)API|WEB"`), []string{"api", "web"}},
			{sel(`app=~".*api"`), []string{"api", "my-api"}},
			{sel(`app=~"api.+"`), []string{"api-gateway"}},
			// Label filters after a parser: literal alternations compare
			// whole values; a common prefix makes Loki compare by substring.
			{fmt.Sprintf(`{probe=%q} | json | svc=~"api|web"`, fx.probe), []string{"api", "web"}},
			{fmt.Sprintf(`{probe=%q} | json | stage!~"prod"`, fx.probe), []string{"api-gateway", "my-api", "webhook"}},
			{fmt.Sprintf(`{probe=%q} | json | svc=~"api|api-gateway"`, fx.probe), []string{"api", "api-gateway", "my-api"}},
		} {
			t.Run(tc.query, func(t *testing.T) {
				loki := anchorLogLines(t, lokiURL, fx, tc.query, nil)
				proxy := anchorLogLines(t, proxyURL, fx, tc.query, nil)
				assertAnchorSet(t, tc.query, sortedKeys(loki), sortedKeys(proxy), tc.want)
				for app, lines := range loki {
					if fmt.Sprint(proxy[app]) != fmt.Sprint(lines) {
						t.Errorf("%s app=%s: proxy %d lines, Loki %d", tc.query, app, len(proxy[app]), len(lines))
					}
				}
			})
		}
	})

	t.Run("metric query", func(t *testing.T) {
		for _, tc := range []struct {
			query string
			want  []string
		}{
			{fmt.Sprintf(`sum by (app) (count_over_time(%s[15m]))`, sel(`app=~"api|web"`)), []string{"api", "web"}},
			{fmt.Sprintf(`sum by (app) (count_over_time(%s[15m]))`, sel(`env=~"prod"`)), []string{"api", "web"}},
			{fmt.Sprintf(`sum by (app) (count_over_time({probe=%q} | json | svc=~"web" [15m]))`, fx.probe), []string{"web"}},
			{fmt.Sprintf(`sum by (app) (count_over_time({probe=%q} | json | svc=~"api|web" [15m]))`, fx.probe), []string{"api", "web"}},
			{fmt.Sprintf(`sum by (app) (count_over_time({probe=%q} | json | svc=~"api|api-gateway" [15m]))`, fx.probe), []string{"api", "api-gateway", "my-api"}},
		} {
			t.Run(tc.query, func(t *testing.T) {
				loki := anchorInstantByApp(t, lokiURL, tc.query, fx.end)
				proxy := anchorInstantByApp(t, proxyURL, tc.query, fx.end)
				assertAnchorSet(t, tc.query, sortedKeys(loki), sortedKeys(proxy), tc.want)
				for app, v := range loki {
					if proxy[app] != v {
						t.Errorf("%s app=%s: proxy %v, Loki %v", tc.query, app, proxy[app], v)
					}
				}
			})
		}
	})

	window := url.Values{
		"start": {strconv.FormatInt(fx.start.UnixNano(), 10)},
		"end":   {strconv.FormatInt(fx.end.UnixNano(), 10)},
	}
	withQuery := func(key, query string) url.Values {
		v := url.Values{key: {query}}
		for k, vs := range window {
			v[k] = vs
		}
		return v
	}

	t.Run("label values", func(t *testing.T) {
		query := sel(`env=~"prod"`)
		loki := anchorStringSet(extractArray(anchorGet(t, lokiURL, "/loki/api/v1/label/app/values", withQuery("query", query), nil), "data"))
		proxy := anchorStringSet(extractArray(anchorGet(t, proxyURL, "/loki/api/v1/label/app/values", withQuery("query", query), nil), "data"))
		assertAnchorSet(t, query, loki, proxy, []string{"api", "web"})
	})

	t.Run("series", func(t *testing.T) {
		query := sel(`app=~"api|web"`)
		apps := func(base string) []string {
			var out []string
			for _, item := range extractArray(anchorGet(t, base, "/loki/api/v1/series", withQuery("match[]", query), nil), "data") {
				labels, _ := item.(map[string]any)
				out = append(out, fmt.Sprint(labels["app"]))
			}
			sort.Strings(out)
			return out
		}
		assertAnchorSet(t, query, apps(lokiURL), apps(proxyURL), []string{"api", "web"})
	})

	t.Run("volume", func(t *testing.T) {
		query := sel(`app=~"api|web"`)
		apps := func(base string) []string {
			params := withQuery("query", query)
			params.Set("targetLabels", "app")
			var out []string
			for _, item := range extractArray(extractMap(anchorGet(t, base, "/loki/api/v1/index/volume", params, nil), "data"), "result") {
				sample, _ := item.(map[string]any)
				metric, _ := sample["metric"].(map[string]any)
				out = append(out, fmt.Sprint(metric["app"]))
			}
			sort.Strings(out)
			return out
		}
		assertAnchorSet(t, query, apps(lokiURL), apps(proxyURL), []string{"api", "web"})
	})

	t.Run("detected labels", func(t *testing.T) {
		query := sel(`env=~"prod|nonprod"`)
		cardinality := func(base string) map[string]float64 {
			resp := anchorGet(t, base, "/loki/api/v1/detected_labels", withQuery("query", query), nil)
			out := map[string]float64{}
			for _, item := range extractArray(resp, "detectedLabels") {
				entry, _ := item.(map[string]any)
				c, _ := entry["cardinality"].(float64)
				out[fmt.Sprint(entry["label"])] = c
			}
			return out
		}
		loki, proxy := cardinality(lokiURL), cardinality(proxyURL)
		// env=~"prod|nonprod" selects api, my-api, web and webhook (not
		// api-gateway, whose env is prod-eu) in two env values.
		if loki["app"] != 4 || loki["env"] != 2 {
			t.Fatalf("%s: Loki detected labels %v, the fixture expects app=4 env=2", query, loki)
		}
		for _, label := range []string{"app", "env"} {
			if proxy[label] != loki[label] {
				t.Errorf("%s: %s cardinality proxy %v, Loki %v", query, label, proxy[label], loki[label])
			}
		}
	})

	// Loki runs single-tenant in this stack, so the multi-tenant request is
	// checked against Loki's semantics: the __tenant_id__ matcher selects
	// tenant ids with the selector's anchored matcher (filterValuesByMatchers,
	// pkg/querier/multi_tenant_querier.go:421). Unanchored, `=~"fak"` reached
	// tenant fake.
	t.Run("multi-tenant", func(t *testing.T) {
		headers := map[string]string{"X-Scope-OrgID": "0|fake"}
		for _, tc := range []struct {
			tenant string
			want   []string
		}{
			{`__tenant_id__=~"fake"`, []string{"api@fake"}},
			{`__tenant_id__=~"fake|0"`, []string{"api@0", "api@fake"}},
			{`__tenant_id__!~"fak"`, []string{"api@0", "api@fake"}},
		} {
			query := sel(`app=~"api", ` + tc.tenant)
			got := sortedKeys(anchorLogLines(t, proxyURL, fx, query, headers))
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Errorf("%s: proxy answered %v, Loki's matcher selects %v", query, got, tc.want)
			}
		}
		query := sel(`app=~"api", __tenant_id__=~"fak"`)
		params := url.Values{"query": {query}, "limit": {"10"}}
		for k, vs := range window {
			params[k] = vs
		}
		status, body, _ := doJSONGET(t, proxyURL+"/loki/api/v1/query_range?"+params.Encode(), headers)
		if status != http.StatusOK || strings.Contains(body, `"fake"`) {
			t.Errorf("%s: no tenant matches, proxy answered %d %s", query, status, body)
		}
	})
}
