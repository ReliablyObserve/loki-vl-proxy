package proxy

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The reference functions below are the fence chains as the key builders wrote
// them before responseKeyFences: any difference means a response cached by an
// older binary is no longer reachable, a cold start across the fleet.

func oldCompatQueryFences(r *http.Request, query string) string {
	return orderedJSONVectorResponseKeyVersion(r, query) +
		bareJSONMetricResponseKeyVersion(r, query) +
		detectedLevelMutationResponseKeyVersion(query) +
		keepMetadataProjectionResponseKeyVersion(query) +
		ipLabelFilterResponseKeyVersion(query) +
		repeatedLabelFilterResponseKeyVersion(query)
}

func oldCompatQueryRangeFences(r *http.Request, query string) string {
	return oldCompatQueryFences(r, query) + logfmtPresenceResponseKeyVersion(r, query)
}

func oldInnerQueryRangeFences(r *http.Request, query string) string {
	var key strings.Builder
	key.WriteString(orderedJSONVectorResponseKeyVersion(r, query))
	key.WriteString(bareJSONMetricResponseKeyVersion(r, query))
	key.WriteString(logfmtPresenceResponseKeyVersion(r, query))
	key.WriteString(detectedLevelMutationResponseKeyVersion(query))
	key.WriteString(keepMetadataProjectionResponseKeyVersion(query))
	key.WriteString(ipLabelFilterResponseKeyVersion(query))
	key.WriteString(repeatedLabelFilterResponseKeyVersion(query))
	return key.String()
}

// fenceCorpusYAMLQueries collects the query and expr values of the registry.
func fenceCorpusYAMLQueries(t *testing.T) []string {
	t.Helper()
	var out []string
	unparsed := 0
	var walk func(v any, key string)
	walk = func(v any, key string) {
		switch v := v.(type) {
		case string:
			if (key == "query" || key == "expr") && strings.Contains(v, "{") {
				out = append(out, v)
			}
		case []any:
			for _, e := range v {
				walk(e, key)
			}
		case map[string]any:
			for k, e := range v {
				walk(e, k)
			}
		}
	}
	err := filepath.WalkDir("../../conformance/registry", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".yaml") {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var doc any
		if err := yaml.Unmarshal(data, &doc); err != nil {
			// A few prose-only rows are not strict YAML (Python reads the same
			// two as invalid); they hold no query.
			unparsed++
			return nil
		}
		walk(doc, "")
		return nil
	})
	if err != nil {
		t.Fatalf("walk registry: %v", err)
	}
	if unparsed > 3 {
		t.Fatalf("%d registry files do not parse as YAML", unparsed)
	}
	return out
}

// fenceSyntheticQueries fire each fence alone and in combination.
var fenceSyntheticQueries = []string{
	`{app="a"}`,
	`{app="a"} | keep level`,
	`{app="a"} |keep   level, msg`,
	`{app="a"} | json | keep detected_level`,
	`{app="a"} | status = ip("10.0.0.0/8")`,
	`{app="a"} | addr != ip("10.0.0.1")`,
	`{app="a"} |= ip("10.0.0.1")`,
	`{app="a"} | drop detected_level`,
	`{app="a"} | keep detected_level`,
	`{app="a"} | detected_level != ""`,
	`{app="a"} | drop level`,
	`{app="a"} | DROP Severity_Text`,
	`{app="a"} | json`,
	`sum by (level) (count_over_time({app="a"} | json [$__auto]))`,
	`sum by (a) (count_over_time({app="a"} | json | drop __error__ [1m]))`,
	`sum(rate({app="a"} | json | a="b" [$__interval]))`,
	`sum by (x) (sum_over_time({app="a"} | json | unwrap n [5m]))`,
	`topk(3, sum by (x) (count_over_time({app="a"} | json x="x" [5m])))`,
	`count_over_time({app="a"} | json [5m])`,
	`quantile_over_time(0.9, {app="a"} | json | unwrap d [5m])`,
	`sum(count_over_time({app="a"} | logfmt | msg != "" [5m]))`,
	`sum(count_over_time({app="a"} | logfmt | msg != "" [$__auto]))`,
	`sum(count_over_time({app="a"} | logfmt [5m]))`,
	`sum by (l) (count_over_time({app="a"} | logfmt | msg != "" [5m]))`,
	`count_over_time({app="a"} | logfmt | msg != "" [5m])`,
	`sum(rate({app="a"} | logfmt | msg != "" [5m]))`,
	`sum(count_over_time({app="a"} | json | logfmt | msg != "" [5m]))`,
	`sum(count_over_time({app="a"} | logfmt | msg != "" | keep msg [5m]))`,
	`sum by (level) (count_over_time({app="a"} | json | keep level | status = ip("1.2.3.4") | drop detected_level [5m]))`,
	`sum(count_over_time({app="a"} | json | logfmt | keep level | detected_level != "" | code = ip("::1") [5m]))`,
	`count_over_time({app="a"} | json | keep level | drop level [5m])`,
	`sum(count_over_time({app="a"} | json [5m] offset 1h))`,
	`{app="a"} | json | line_format "{{.msg}}"`,
	`{app="a"} | logfmt | keep msg | detected_level = ""`,
	``,
	`*`,
	`not a logql | json keep x logfmt != ""`,
}

type fenceRequestShape struct {
	name, path string
	params     url.Values
}

func fenceRequestShapes(query string) []fenceRequestShape {
	rangeParams := url.Values{"query": {query}, "start": {"1700000000"}, "end": {"1700003600"}, "step": {"15"}}
	return []fenceRequestShape{
		{"query", "/loki/api/v1/query", url.Values{"query": {query}, "time": {"1700003600"}}},
		{"query_range", "/loki/api/v1/query_range", rangeParams},
		{"query_range_nostep", "/loki/api/v1/query_range", url.Values{"query": {query}}},
	}
}

// conformance: cache-response-key-versions
func TestResponseKeyFencesByteIdentical(t *testing.T) {
	seen := map[string]bool{}
	var corpus []string
	add := func(qs ...string) {
		for _, q := range qs {
			if !seen[q] {
				seen[q] = true
				corpus = append(corpus, q)
			}
		}
	}
	add(fenceSyntheticQueries...)
	for _, path := range []string{"../../bench/ab/shapes.json", "../../test/e2e-compat/query-semantics-matrix.json"} {
		for _, s := range jsonStrings(t, path) {
			if strings.Contains(s, "{") {
				add(s)
			}
		}
	}
	add(fenceCorpusYAMLQueries(t)...)
	sort.Strings(corpus)
	if len(corpus) < 300 {
		t.Fatalf("query corpus has %d entries, expected the shapes, the matrix and the registry", len(corpus))
	}

	p := newCompatTestProxy(t, "http://unused")
	fired := map[string]bool{}
	for _, query := range corpus {
		for _, shape := range fenceRequestShapes(query) {
			r := httptest.NewRequest(http.MethodGet, shape.path+"?"+shape.params.Encode(), nil)
			endpoint := strings.TrimSuffix(strings.TrimPrefix(shape.path, "/loki/api/v1/"), "_nostep")

			oldCompatQuery, oldCompatRange, oldInner := oldCompatQueryFences(r, query), oldCompatQueryRangeFences(r, query), oldInnerQueryRangeFences(r, query)
			if got := responseKeyFences(r, query, responseKeyCompatQuery); got != oldCompatQuery {
				t.Errorf("compat query fences differ for %q (%s): got %q want %q", query, shape.name, got, oldCompatQuery)
			}
			if got := responseKeyFences(r, query, responseKeyCompatQueryRange); got != oldCompatRange {
				t.Errorf("compat query_range fences differ for %q (%s): got %q want %q", query, shape.name, got, oldCompatRange)
			}
			if got := responseKeyFences(r, query, responseKeyInnerQueryRange); got != oldInner {
				t.Errorf("inner query_range fences differ for %q (%s): got %q want %q", query, shape.name, got, oldInner)
			}
			if got, want := responseKeyFences(nil, query, responseKeyLogWindowShape), keepMetadataProjectionResponseKeyVersion(query); got != want {
				t.Errorf("window shape fences differ for %q: got %q want %q", query, got, want)
			}
			for _, f := range []string{
				orderedJSONVectorResponseVersion, bareJSONMetricResponseVersion, logfmtPresenceResponseVersion,
				detectedLevelMutationResponseVersion, keepMetadataProjectionResponseVersion, ipLabelFilterResponseVersion, repeatedLabelFilterResponseVersion,
			} {
				if strings.Contains(oldInner, f) {
					fired[f] = true
				}
			}

			// The full keys, built by the real key builders, keep the old chain.
			if query == "" || shape.name == "query_range_nostep" {
				continue
			}
			if key, ok := p.compatCacheKey(endpoint, r); !ok {
				t.Fatalf("compat cache key refused for %q", query)
			} else {
				want := oldCompatQuery
				if endpoint == "query_range" {
					want = oldCompatRange
				}
				if prefix := "compat:v2:" + endpoint + ":" + logLineCacheKeyVersion + ":" + want; !strings.HasPrefix(key, prefix) {
					t.Errorf("compat key for %q (%s): %q lacks prefix %q", query, endpoint, key, prefix)
				}
			}
			if endpoint == "query_range" {
				if key, prefix := p.queryRangeCacheKey(r, query), "query_range:v3:"+logLineCacheKeyVersion+":"+oldInner; !strings.HasPrefix(key, prefix) {
					t.Errorf("inner key for %q: %q lacks prefix %q", query, key, prefix)
				}
			}
		}
	}
	for _, f := range []string{
		orderedJSONVectorResponseVersion, bareJSONMetricResponseVersion, logfmtPresenceResponseVersion,
		detectedLevelMutationResponseVersion, keepMetadataProjectionResponseVersion, ipLabelFilterResponseVersion, repeatedLabelFilterResponseVersion,
	} {
		if !fired[f] {
			t.Errorf("no corpus query fires %s: the differential does not cover it", f)
		}
	}
	t.Logf("%d queries x 3 request shapes, all fences fired", len(corpus))
}

// TestResponseKeyFencesSiteOrder pins what differs between the sites: the
// logfmt presence fence is absent from the compat instant key, last in the
// compat range key and right after the JSON fences in the inner key.
//
// conformance: cache-response-key-versions
func TestResponseKeyFencesSiteOrder(t *testing.T) {
	query := `sum(count_over_time({app="a"} | logfmt | msg != "" | keep msg | status = ip("1.2.3.4") | drop detected_level [5m]))`
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?query="+url.QueryEscape(query)+"&start=1&end=2&step=1", nil)
	keep, ip, dl, lf, rl := keepMetadataProjectionResponseVersion, ipLabelFilterResponseVersion, detectedLevelMutationResponseVersion, logfmtPresenceResponseVersion, repeatedLabelFilterResponseVersion
	for site, want := range map[responseKeySite]string{
		responseKeyCompatQuery:      dl + keep + ip + rl,
		responseKeyCompatQueryRange: dl + keep + ip + rl + lf,
		responseKeyInnerQueryRange:  lf + dl + keep + ip + rl,
		responseKeyLogWindowShape:   keep,
	} {
		if got := responseKeyFences(r, query, site); got != want {
			t.Errorf("site %d: got %q want %q", site, got, want)
		}
	}
}
