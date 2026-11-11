package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// TestReservedFieldCaseResponseKeyVersionFencesTheChangedReadings pins which queries carry the fence: a matcher or a
// comparison on _time, _stream or _stream_id in another letter case; no lower-case name, quoted text, longer name,
// pipe stage or range function.
//
// conformance: cache-response-key-versions, semantics/reserved-field-name-case-outbound
func TestReservedFieldCaseResponseKeyVersionFencesTheChangedReadings(t *testing.T) {
	for query, fenced := range map[string]bool{
		`{app="a"} | _TIME>=5s`:                                      true,
		`{app="a", _Stream != "x"}`:                                  true,
		`{app="a"} | logfmt | _Stream_ID="si"`:                       true,
		`sum(sum_over_time({app="a"} | _STREAM_ID=~"x" [5m]))`:       true,
		`{app="a"} | _time="x"`:                                      false,
		`{app="a"} | _msg="x"`:                                       false,
		`sum(sum_over_time({app="a"} | logfmt | unwrap _TIME [1m]))`: false, // the same reading, now clone-safe
		`{app="a"} | drop _TIME | keep _Stream`:                      false,
		`{app="a"} |= "_TIME=1"`:                                     false,
		`{app="a"} | _TIMES="x"`:                                     false,
		`{app="a"} | x_TIME="x"`:                                     false,
		`sum(count_over_time({app="a"}[5m])) > 1`:                    false,
		``: false,
	} {
		if got := reservedFieldCaseResponseKeyVersion(query); (got == reservedFieldCaseResponseVersion) != fenced {
			t.Errorf("%s: fence %q, want fenced %v", query, got, fenced)
		}
	}
	if allocs := testing.AllocsPerRun(100, func() {
		_ = reservedFieldCaseResponseKeyVersion(`{app="a",env=~"p.*"} | json | status="500" |= "error" | keep level`)
	}); allocs != 0 {
		t.Errorf("an unaffected query allocates %v times", allocs)
	}
}

// reservedCaseFilterText is a name in another case as a whole word before a matcher or a comparison: what the fence
// looks for, read independently of its scanner.
var reservedCaseFilterText = regexp.MustCompile(`(?:^|[^A-Za-z0-9_])(_[A-Za-z]+(?:_[A-Za-z]+)?)\s*[=!<>]`)

// TestReservedFieldCaseFenceMovesOnlyAffectedKeys: on the registry and matrix corpus the fence changes the response
// keys of the queries that filter such a name, and no other.
//
// conformance: cache-response-key-versions
func TestReservedFieldCaseFenceMovesOnlyAffectedKeys(t *testing.T) {
	corpus := append([]string{}, fenceSyntheticQueries...)
	corpus = append(corpus, fenceCorpusYAMLQueries(t)...)
	for _, path := range []string{"../../bench/ab/shapes.json", "../../test/e2e-compat/query-semantics-matrix.json"} {
		for _, s := range jsonStrings(t, path) {
			if strings.Contains(s, "{") {
				corpus = append(corpus, s)
			}
		}
	}
	moved := 0
	for _, query := range corpus {
		if reservedFieldCaseResponseKeyVersion(query) == "" {
			continue
		}
		moved++
		ok := false
		for _, m := range reservedCaseFilterText.FindAllStringSubmatch(query, -1) {
			ok = ok || strings.EqualFold(m[1], "_time") || strings.EqualFold(m[1], "_stream") || strings.EqualFold(m[1], "_stream_id")
		}
		if !ok {
			t.Errorf("an unaffected corpus query is fenced: %s", query)
		}
	}
	if moved == 0 {
		t.Fatal("no corpus query is fenced: the registry case of the change is missing")
	}
}

// A reply cached under the key an older binary built is not served: `| _TIME>=5s` was VictoriaLogs' time range (a 200
// with every line), and the compat edge looks its cache up before the handler builds the query.
//
// conformance: cache-response-key-versions
func TestReservedFieldCaseFenceDoesNotServeAnOldCachedAnswer(t *testing.T) {
	const stale = `{"status":"success","data":{"resultType":"streams","result":[{"stream":{"stale":"yes"},"values":[["1700000000000000000","old"]]}],"stats":{}}}`
	query := `{app="a"} | _TIME>=5s`
	backendCalls := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalls++
		w.Header().Set("Content-Type", "application/x-ndjson")
	}))
	defer backend.Close()
	p := newCompatTestProxy(t, backend.URL)
	target := "/loki/api/v1/query_range?" + url.Values{"query": {query}, "start": {"1700000000"}, "end": {"1700003600"}, "step": {"15"}}.Encode()
	rewritten := logqlpkg.LowercaseKeywords(logqlpkg.StripParserFlags(logqlpkg.StripComments(query)))
	keyTarget := "/loki/api/v1/query_range?" + url.Values{"query": {rewritten}, "start": {"1700000000"}, "end": {"1700003600"}, "step": {"15"}}.Encode()
	req := httptest.NewRequest(http.MethodGet, keyTarget, nil)
	req.Header.Set("X-Scope-OrgID", "team-a")
	key, ok := p.compatCacheKey("query_range", req)
	if !ok || !strings.Contains(key, reservedFieldCaseResponseVersion) {
		t.Fatalf("compat key %q lacks the fence", key)
	}
	p.compatCache.Set(strings.Replace(key, reservedFieldCaseResponseVersion, "", 1), []byte(stale))
	rec := doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "team-a"})
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), "stale") || backendCalls == 0 {
		t.Errorf("status %d, body %s, %d backend calls; want the backend's 200, not the cached answer", rec.Code, rec.Body.String(), backendCalls)
	}
	p.compatCache.Set(key, []byte(stale))
	before := backendCalls
	if rec := doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "team-a"}); !strings.Contains(rec.Body.String(), "stale") || backendCalls != before {
		t.Errorf("the control did not hit the cache: %d %s", rec.Code, rec.Body.String())
	}
}
