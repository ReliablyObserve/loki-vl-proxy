package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// TestReservedFieldLabelResponseKeyVersionFencesTheChangedReadings pins which queries carry the reserved field label
// fence: a matcher or a comparison on a label called _time, _stream or _stream_id in any case, or _msg; no other
// query, quoted text, a longer name and the proxy's own words included.
//
// conformance: cache-response-key-versions, semantics/translator-writes-reserved-field-names
func TestReservedFieldLabelResponseKeyVersionFencesTheChangedReadings(t *testing.T) {
	for query, fenced := range map[string]bool{
		`{_time="a"}`:                                       true,
		`{app="a", _stream != "x"}`:                         true,
		`{app="a"} | _time>=5s`:                             true,
		`{app="a"} | _stream_id=~"x"`:                       true,
		`{app="a"} | _msg="x"`:                              true,
		`{app="a"} | _TIME="x"`:                             true,
		`{app="a"} | _Stream_Id<5`:                          true,
		`count_over_time({_msg="a"}[5m])`:                   true,
		`{app="a"} | json _time="t"`:                        true, // a json expression: the key is unchanged, the fence costs one miss
		`{app="a"} | _MSG="x"`:                              false,
		`{app="a"} | _times="x"`:                            false,
		`{app="a"} | x_time="x"`:                            false,
		`{app="a"} |= "_time=1"`:                            false,
		"{app=\"a\"} |~ `_msg!=`":                           false,
		`{app="a"} | line_format "{{._time}}"`:              false,
		`sum by (_stream) (count_over_time({app="a"}[5m]))`: false,
		`{app="a"} | drop _time | keep _msg`:                false,
		`{app="a"} | label_format x=_time`:                  false,
		`sum(count_over_time({app="a"}[5m])) > 1`:           false,
		`bytes_over_time({app="a"}[5m]) < 10`:               false,
		``:                                                  false,
	} {
		got := reservedFieldLabelResponseKeyVersion(query)
		if (got == reservedFieldLabelResponseVersion) != fenced {
			t.Errorf("%s: fence %q, want fenced %v", query, got, fenced)
		}
	}
	if allocs := testing.AllocsPerRun(100, func() {
		_ = reservedFieldLabelResponseKeyVersion(`{app="a",env=~"p.*"} | json | status="500" |= "error" | keep level`)
	}); allocs != 0 {
		t.Errorf("an unaffected query allocates %v times", allocs)
	}
}

// reservedFilterText is a reserved name as a whole word (not the end of count_over_time) before a matcher or a
// comparison: what the fence looks for, read independently of its scanner.
var reservedFilterText = regexp.MustCompile(`(?:^|[^A-Za-z0-9_])(?:(?i:_time|_stream_id|_stream)|_msg)\s*[=!<>]`)

// TestReservedFieldLabelFenceMovesOnlyAffectedKeys: on the registry and matrix corpus the fence changes the response
// keys of the queries that filter a reserved field name, and no other.
//
// conformance: cache-response-key-versions
func TestReservedFieldLabelFenceMovesOnlyAffectedKeys(t *testing.T) {
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
		if reservedFieldLabelResponseKeyVersion(query) == "" {
			continue
		}
		moved++
		if !reservedFilterText.MatchString(query) {
			t.Errorf("an unaffected corpus query is fenced: %s", query)
		}
	}
	if moved == 0 {
		t.Fatal("no corpus query is fenced: the registry cases of the change are missing")
	}
}

// A reply cached under the key an older binary built is not served: `| _time>=5s` was VictoriaLogs' time range (a
// 200 with every line), and the compat edge looks its cache up before the handler builds the query.
//
// conformance: cache-response-key-versions
func TestReservedFieldLabelFenceDoesNotServeAnOldCachedAnswer(t *testing.T) {
	const stale = `{"status":"success","data":{"resultType":"streams","result":[{"stream":{"stale":"yes"},"values":[["1700000000000000000","old"]]}],"stats":{}}}`
	for _, query := range []string{`{app="a"} | _time>=5s`, `{app="a", _msg="x"}`} {
		backendCalls := 0
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			backendCalls++
			w.Header().Set("Content-Type", "application/x-ndjson")
		}))
		p := newCompatTestProxy(t, backend.URL)
		target := "/loki/api/v1/query_range?" + url.Values{"query": {query}, "start": {"1700000000"}, "end": {"1700003600"}, "step": {"15"}}.Encode()
		rewritten := logqlpkg.LowercaseKeywords(logqlpkg.StripParserFlags(logqlpkg.StripComments(query)))
		keyTarget := "/loki/api/v1/query_range?" + url.Values{"query": {rewritten}, "start": {"1700000000"}, "end": {"1700003600"}, "step": {"15"}}.Encode()
		req := httptest.NewRequest(http.MethodGet, keyTarget, nil)
		req.Header.Set("X-Scope-OrgID", "team-a")
		key, ok := p.compatCacheKey("query_range", req)
		if !ok || !strings.Contains(key, reservedFieldLabelResponseVersion) {
			t.Fatalf("%s: compat key %q lacks the fence", query, key)
		}
		oldKey := strings.Replace(key, reservedFieldLabelResponseVersion, "", 1)
		p.compatCache.Set(oldKey, []byte(stale))
		rec := doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "team-a"})
		if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), "stale") || backendCalls == 0 {
			t.Errorf("%s: status %d, body %s, %d backend calls; want the backend's 200, not the cached answer", query, rec.Code, rec.Body.String(), backendCalls)
		}
		// Control: the same cached answer under the key the proxy builds now is served.
		p.compatCache.Set(key, []byte(stale))
		before := backendCalls
		if rec := doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "team-a"}); !strings.Contains(rec.Body.String(), "stale") || backendCalls != before {
			t.Errorf("%s: the control did not hit the cache: %d %s", query, rec.Code, rec.Body.String())
		}
		backend.Close()
	}
}

// A filter-only log query on a label called _time, _stream_id or _msg is served from the query plan: the request
// VictoriaLogs gets filters the stream tag (or quotes the field of another case), and the stream of a row keeps the
// label, as Loki's stream does.
//
// conformance: semantics/translator-writes-reserved-field-names
func TestReservedFieldLabelServedFromThePlan(t *testing.T) {
	for _, c := range []struct {
		query, stream, wantFilter string
		wantLabel                 [2]string
	}{
		{`{app="a"} | _time="st"`, `{_time="st",app="a"}`, `{_time="st"}`, [2]string{"_time", "st"}},
		{`{app="a", _stream_id!="x"}`, `{_stream_id="si",app="a"}`, `{_stream_id!="x"}`, [2]string{"_stream_id", "si"}},
		{`{app="a"} | _msg=~"m.*"`, `{_msg="m1",app="a"}`, `{_msg=~"(?-s:(?s:.*)m(?s:.*))"}`, [2]string{"_msg", "m1"}},
		{`{app="a"} | _time>=5s`, `{_time="7s",app="a"}`, `| filter __lxrf_time:>=5s`, [2]string{"_time", "7s"}},
		{`{app="a"} | _TIME="up"`, `{app="a"}`, `"_TIME":="up"`, [2]string{"app", "a"}},
	} {
		var sent []string
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = r.ParseForm()
			sent = append(sent, r.Form.Get("query"))
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = w.Write([]byte(`{"_time":"2023-11-14T22:13:21Z","_msg":"line","_stream":` + strconv.Quote(c.stream) + `,"_stream_id":"0000","app":"a"}` + "\n"))
		}))
		p := newCompatTestProxy(t, backend.URL)
		target := "/loki/api/v1/query_range?" + url.Values{"query": {c.query}, "start": {"1700000000"}, "end": {"1700003600"}, "limit": {"10"}}.Encode()
		rec := doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "team-a"})
		backend.Close()
		if rec.Code != http.StatusOK {
			t.Errorf("%s: status %d %s", c.query, rec.Code, rec.Body.String())
			continue
		}
		found := false
		for _, q := range sent {
			found = found || strings.Contains(q, c.wantFilter)
		}
		if !found {
			t.Errorf("%s: VictoriaLogs got %q, want a request holding %s", c.query, sent, c.wantFilter)
		}
		var resp struct {
			Data struct {
				Result []struct {
					Stream map[string]string `json:"stream"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || len(resp.Data.Result) != 1 {
			t.Errorf("%s: answer %s (%v)", c.query, rec.Body.String(), err)
			continue
		}
		if got := resp.Data.Result[0].Stream[c.wantLabel[0]]; got != c.wantLabel[1] {
			t.Errorf("%s: the stream %v lacks %s=%q", c.query, resp.Data.Result[0].Stream, c.wantLabel[0], c.wantLabel[1])
		}
	}
}
