package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// TestLexerResponseKeyVersionFencesTheChangedReadings pins which queries carry the lexer fence: those the LogQL lexer
// answered before and rejects now, and any other query the validation rejects; no query that is valid, the
// words, brackets and strings that look alike included.
//
// conformance: cache-response-key-versions
func TestLexerResponseKeyVersionFencesTheChangedReadings(t *testing.T) {
	for query, fenced := range map[string]bool{
		"{app=\"a\"}\v":                                    true,
		"\v{app=\"a\"}":                                    true,
		"{app=\"a\"} |\v= \"x\"":                           true,
		"{app=\"a\"}\f| json":                              true,
		"{app=\"a\"} | json":                               true,
		"{app=\"a\"} |= \"x\" ":                            true,
		"rate\v({app=\"a\"}[5m])":                          true,
		"{app=\"a\"}\x01":                                  true,
		"rate({app=\"a\"}[ 5m])":                           true,
		"rate({app=\"a\"}[5m ])":                           true,
		"rate({app=\"a\"}[5m\n])":                          true,
		"sum by (a b) (rate({app=\"a\"}[5m]))":             true,
		"sum by (a,) (rate({app=\"a\"}[5m]))":              true,
		"sum(rate({app=\"a\"}[5m])) without (a b)":         true,
		"rate({a=\"b\"}[5m]) / on(a,) rate({a=\"b\"}[5m])": true,
		"rate({a=\"b\"}[5m]) / on rate({a=\"b\"}[5m])":     true,
		"{app=\"a\"} | x=\"a\nb\"":                         true,
		"{app=\"a\"} | json | x=\"a\nb\" | json":           true,
		"label_replace(rate({app=\"a\"}[$__auto]), \"d\", \"$1\", \"a\", \"(.*)\")": true,
		"{app=\"a\"}":                                                   false,
		"{app=\"a\"} |= \"x\" | json":                                   false,
		"{app=\"a\"} |= \"a\vb\"":                                       false,
		"{app=\"a\"} |= `a\nb`":                                         false,
		"{app=\"a\"} | line_format \"{{.a}}\"":                          false,
		"rate({app=\"a\"}[5m])":                                         false,
		"sum by (a, b) (rate({app=\"a\"}[5m]))":                         false,
		"sum without (a) (rate({app=\"a\"}[5m]))":                       false,
		"sum by ( a , b ) (rate({app=\"a\"}[5m]))":                      false,
		"sum by () (rate({app=\"a\"}[5m]))":                             false,
		"sum by (a,b,) (rate({app=\"a\"}[5m]))":                         true,
		"sum by (,a) (rate({app=\"a\"}[5m]))":                           true,
		"sum by (a b ) (rate({app=\"a\"}[5m]))":                         true,
		"sum by (a) rate({app=\"a\"}[5m])":                              true, // rejected before as well: nothing cached to fence, the version is harmless
		"rate({a=\"b\"}[5m]) / on(a) group_right rate({a=\"b\"}[5m])":   false,
		"rate({a=\"b\"}[5m]) / on(a) group_left(c) rate({a=\"b\"}[5m])": false,
		"sum(rate({app=\"a\"}[5m])) by (a)":                             false,
		"{app=\"a\"} # a\vb\n| json":                                    false,
		"{app=\"a\"} |= \"by  (a b\"":                                   false,
		"rate({app=\"a\"} |= \"[ \" [5m])":                              false,
		"":                                                              true,
	} {
		if got := lexerResponseKeyVersion(query) != ""; got != fenced {
			t.Errorf("lexerResponseKeyVersion(%q) fenced = %v, want %v", query, got, fenced)
		}
	}
}

// A query answered by an older binary and rejected now is not served its cached answer at the compat edge: the
// key an older binary built holds the 200, the key of this binary does not, and the handler answers Loki's 400.
//
// conformance: cache-response-key-versions
func TestCompatEdgeDoesNotServeAnAnswerCachedBeforeTheLexerFix(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"streams","result":[]}}`))
	}))
	defer backend.Close()
	cached := `{"status":"success","data":{"resultType":"streams","result":[{"stream":{"app":"a"},"values":[["1700000000000000000","old"]]}]}}`

	for _, endpoint := range []string{"query", "query_range"} {
		for _, tc := range []struct {
			query    string
			rejected bool
		}{
			{"{app=\"a\"}\v|= \"x\"", true},
			{"rate({app=\"a\"}[ 5m])", true},
			{"sum by (a b) (rate({app=\"a\"}[5m]))", true},
			{"{app=\"a\"} |= \"x\"", false},
		} {
			p := newCompatTestProxy(t, backend.URL)
			params := url.Values{"query": {tc.query}, "start": {"1700000000"}, "end": {"1700003600"}, "step": {"60"}, "time": {"1700003600"}}
			target := "/loki/api/v1/" + endpoint + "?" + params.Encode()
			key, ok := p.compatCacheKey(endpoint, httptest.NewRequest(http.MethodGet, target, nil))
			if !ok {
				t.Fatalf("%s %q: no compat key", endpoint, tc.query)
			}
			if got := strings.Contains(key, lexerResponseVersion); got != tc.rejected {
				t.Fatalf("%s %q: key %q carries the fence = %v, want %v", endpoint, tc.query, key, got, tc.rejected)
			}
			// The key a binary without the fence built: the same key with the fence taken out.
			p.compatCache.SetWithTTL(strings.Replace(key, lexerResponseVersion, "", 1), []byte(cached), 5*time.Minute)
			rec := doCompatProxyRequest(p, target, nil)
			if tc.rejected {
				if rec.Code != http.StatusBadRequest || strings.Contains(rec.Body.String(), "old") {
					t.Errorf("%s %q: got %d %s, want Loki's 400 and no cached answer", endpoint, tc.query, rec.Code, rec.Body.String())
				}
			} else if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "old") {
				t.Errorf("%s %q: got %d %s, want the cached answer", endpoint, tc.query, rec.Code, rec.Body.String())
			}
		}
	}
}
