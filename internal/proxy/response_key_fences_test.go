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

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
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
		repeatedLabelFilterResponseKeyVersion(query) +
		keywordLabelResponseKeyVersion(query) +
		reservedFieldCaseResponseKeyVersion(query) +
		reservedFieldLabelResponseKeyVersion(query)
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
	key.WriteString(keywordLabelResponseKeyVersion(query))
	key.WriteString(reservedFieldCaseResponseKeyVersion(query))
	key.WriteString(reservedFieldLabelResponseKeyVersion(query))
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

			// The rune fence (rune_response_fence.go) ends the compat chains and is empty for a valid query.
			oldCompatQuery, oldCompatRange, oldInner := oldCompatQueryFences(r, query)+runeResponseKeyVersion(query), oldCompatQueryRangeFences(r, query)+runeResponseKeyVersion(query), oldInnerQueryRangeFences(r, query)
			if got := responseKeyFences(r, query, responseKeyCompatQuery); got != oldCompatQuery {
				t.Errorf("compat query fences differ for %q (%s): got %q want %q", query, shape.name, got, oldCompatQuery)
			}
			if got := responseKeyFences(r, query, responseKeyCompatQueryRange); got != oldCompatRange {
				t.Errorf("compat query_range fences differ for %q (%s): got %q want %q", query, shape.name, got, oldCompatRange)
			}
			if got := responseKeyFences(r, query, responseKeyInnerQueryRange); got != oldInner {
				t.Errorf("inner query_range fences differ for %q (%s): got %q want %q", query, shape.name, got, oldInner)
			}
			if got, want := responseKeyFences(nil, query, responseKeyLogWindowShape), keepMetadataProjectionResponseKeyVersion(query)+keywordLabelResponseKeyVersion(query)+reservedFieldCaseResponseKeyVersion(query)+reservedFieldLabelResponseKeyVersion(query); got != want {
				t.Errorf("window shape fences differ for %q: got %q want %q", query, got, want)
			}
			for _, f := range []string{
				orderedJSONVectorResponseVersion, bareJSONMetricResponseVersion, logfmtPresenceResponseVersion,
				detectedLevelMutationResponseVersion, keepMetadataProjectionResponseVersion, ipLabelFilterResponseVersion, repeatedLabelFilterResponseVersion, keywordLabelResponseVersion,
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
		detectedLevelMutationResponseVersion, keepMetadataProjectionResponseVersion, ipLabelFilterResponseVersion, repeatedLabelFilterResponseVersion, keywordLabelResponseVersion,
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

// TestIPFilterResponseKeyVersionFencesLineAndLabelFilters pins which queries carry the ip() fence: a label filter
// and, since v4, a line filter, both operators, any blank before the parenthesis; no other query.
//
// conformance: cache-response-key-versions
func TestIPFilterResponseKeyVersionFencesLineAndLabelFilters(t *testing.T) {
	for query, fenced := range map[string]bool{
		`{app="a"} | addr = ip("10.0.0.1")`:             true,
		`{app="a"} | logfmt | addr != ip("10.0.0.0/8")`: true,
		`{app="a"} |= ip("10.0.0.1")`:                   true,
		`{app="a"} != ip("10.0.0.1")`:                   true,
		`{app="a"} |=ip ("10.0.0.1-10.0.0.5")`:          true,
		`rate({app="a"} |= ip("0.0.0.0/0") [5m])`:       true,
		`{app="a"} |= "ip(10.0.0.1)"`:                   false,
		`{app="a"} | json | ip_addr="10.0.0.1"`:         false,
		`sum by (ip) (count_over_time({app="a"}[5m]))`:  false,
		`{app="a"} |~ "zip"`:                            false,
	} {
		if got := ipLabelFilterResponseKeyVersion(query) == ipLabelFilterResponseVersion; got != fenced {
			t.Errorf("%s: fenced %v, want %v", query, got, fenced)
		}
	}
}

// TestKeywordLabelResponseKeyVersionFencesTheChangedReadings pins which queries carry the keyword label fence: a label
// that is not all lower case ip or label_replace with no parenthesis after it, and a query now rejected for naming a
// label after a word of Loki's lexer; no other query, the stages and ranges that use the same words included.
//
// conformance: cache-response-key-versions
func TestKeywordLabelResponseKeyVersionFencesTheChangedReadings(t *testing.T) {
	for query, fenced := range map[string]bool{
		`{Ip="a"}`:                                       true,
		`{app="a"} | IP="x"`:                             true,
		`{app="a"} | drop Label_replace`:                 true,
		`sum by (Ip) (count_over_time({app="a"}[5m]))`:   true,
		`{OF="a"}`:                                       true,
		`{app="a"} | keep OF`:                            true,
		`{app="a"} | json a, drop`:                       true,
		`sum by (json) (count_over_time({app="a"}[5m]))`: true,
		`{app="a"} | label_format x=Offset`:              true,
		`{app="a"} | json`:                               false,
		`{app="a"} | logfmt | drop a | keep b | line_format "{{.a}}"`: false,
		`{app="a"} | json | unwrap n`:                                 false,
		`sum(count_over_time({app="a"}[5m] offset 1h))`:               false,
		`sum(rate({app="a"}[5m])) > bool 1`:                           false,
		`{app="a"} | addr = ip("10.0.0.1")`:                           false,
		`{app="a"} | addr = IP("10.0.0.1")`:                           false,
		`{app="a"} | x = ip ("10.0.0.1")`:                             false,
		`{app="a"} |= "Ip" |~ "of json"`:                              false,
		`{app="a"} | ip_addr="x"`:                                     false,
		`{app="a"} | Zip="x"`:                                         false,
		`label_replace(rate({app="a"}[5m]), "a", "b", "c", "d")`:      false,
		`{app="a"} | not="x" | in="y"`:                                false,
		``:                                                            false,
	} {
		got := keywordLabelResponseKeyVersion(query)
		if (got == keywordLabelResponseVersion) != fenced {
			t.Errorf("%s: fence %q, want fenced %v", query, got, fenced)
		}
	}
	if allocs := testing.AllocsPerRun(100, func() {
		_ = keywordLabelResponseKeyVersion(`{app="a",env=~"p.*"} | json | status="500" |= "error" | keep level`)
	}); allocs != 0 {
		t.Errorf("an unaffected query allocates %v times", allocs)
	}
}

// TestKeywordLabelFenceMovesOnlyAffectedKeys: on the registry and matrix corpus the fence changes the response keys of
// the queries the parser now reads differently or rejects, and no other.
//
// conformance: cache-response-key-versions
func TestKeywordLabelFenceMovesOnlyAffectedKeys(t *testing.T) {
	corpus := append([]string{}, fenceSyntheticQueries...)
	corpus = append(corpus, fenceCorpusYAMLQueries(t)...)
	for _, path := range []string{"../../bench/ab/shapes.json", "../../test/e2e-compat/query-semantics-matrix.json"} {
		for _, s := range jsonStrings(t, path) {
			if strings.Contains(s, "{") {
				corpus = append(corpus, s)
			}
		}
	}
	moved := map[string]bool{}
	for _, query := range corpus {
		if keywordLabelResponseKeyVersion(query) != "" {
			moved[query] = true
		}
	}
	if len(moved) == 0 {
		t.Fatal("no corpus query is fenced: the registry cases of the change are missing")
	}
	for query := range moved {
		if !strings.Contains(query, `of="a"`) && !strings.Contains(query, "OF") && !strings.Contains(query, "Ip") && query != `not a logql | json keep x logfmt != ""` {
			t.Errorf("an unaffected corpus query is fenced: %s", query)
		}
	}
}

// A reply cached under the key an older binary built is not served: the edge cache is looked up before the handler
// validates the query, and the fenced key differs from the old one.
//
// conformance: cache-response-key-versions
func TestKeywordLabelFenceDoesNotServeAnOldCachedAnswer(t *testing.T) {
	const stale = `{"status":"success","data":{"resultType":"streams","result":[{"stream":{"stale":"yes"},"values":[["1700000000000000000","old"]]}],"stats":{}}}`
	for _, c := range []struct {
		query    string
		wantCode int
	}{
		{`{OF="a"}`, http.StatusBadRequest}, // Loki's 400, not the 200 cached before
		{`{Ip="a"}`, http.StatusOK},         // another field now: the backend answers, not the cache
	} {
		backendCalls := 0
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			backendCalls++
			w.Header().Set("Content-Type", "application/x-ndjson")
		}))
		p := newCompatTestProxy(t, backend.URL)
		target := "/loki/api/v1/query_range?" + url.Values{"query": {c.query}, "start": {"1700000000"}, "end": {"1700003600"}, "step": {"15"}}.Encode()
		// The route rewrites the keywords of the query (lowercaseQueryKeywords) before the cache key is built.
		rewritten := logqlpkg.LowercaseKeywords(logqlpkg.StripParserFlags(logqlpkg.StripComments(c.query)))
		keyTarget := "/loki/api/v1/query_range?" + url.Values{"query": {rewritten}, "start": {"1700000000"}, "end": {"1700003600"}, "step": {"15"}}.Encode()
		req := httptest.NewRequest(http.MethodGet, keyTarget, nil)
		req.Header.Set("X-Scope-OrgID", "team-a")
		key, ok := p.compatCacheKey("query_range", req)
		if !ok || !strings.Contains(key, keywordLabelResponseVersion) {
			t.Fatalf("%s: compat key %q lacks the fence", c.query, key)
		}
		oldKey := strings.Replace(key, keywordLabelResponseVersion, "", 1)
		if oldKey == key {
			t.Fatalf("%s: the fence does not move the key", c.query)
		}
		p.compatCache.Set(oldKey, []byte(stale))
		rec := doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "team-a"})
		if rec.Code != c.wantCode || strings.Contains(rec.Body.String(), "stale") {
			t.Errorf("%s: status %d, body %s; want %d and not the cached answer", c.query, rec.Code, rec.Body.String(), c.wantCode)
		}
		// Control: the same cached answer under the key the proxy builds now is served.
		p.compatCache.Set(key, []byte(stale))
		before := backendCalls
		if rec := doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "team-a"}); !strings.Contains(rec.Body.String(), "stale") || backendCalls != before {
			t.Errorf("%s: the control did not hit the cache: %d %s", c.query, rec.Code, rec.Body.String())
		}
		backend.Close()
	}
}
