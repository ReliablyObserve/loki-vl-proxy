package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// ipFenceUnknownNames are the classes of characters that start no token and that Loki's lexer and the parser read
// as a name of their own (logql.startsUnknownName): control characters, the ASCII symbols $ & ; ? \ ~, a char
// literal, and characters outside ASCII that are no letters (blanks Go names white space, format characters,
// symbols, superscript digits). Loki accepts each as the name of a label filter after another filter
// (`A=1 and ;=1`, testdata/label_filter_number_literals_loki.jsonl.gz in internal/logql).
var ipFenceUnknownNames = []string{"\v", "\f", "\x01", "\x7f", "\u00a0", "\u200b", "€", "²", "$", "&", ";", "?", "\\", "~", "'a'"}

// ipFenceUnknownNameQueries is every way a label filter named by such a character follows another filter of its
// stage (a blank, a comma, and, or, a line break, a comment, a group), with = and != and ip in two cases, in a log
// query and in a metric query.
func ipFenceUnknownNameQueries() []string {
	var out []string
	for _, name := range ipFenceUnknownNames {
		for _, join := range []string{" ", ", ", " and ", " or ", "\n", " #c\n"} {
			for _, op := range []string{"=", "!=", " = "} {
				for _, word := range []string{"ip", "IP"} {
					filter := `A="x"` + join + name + op + word + `("10.0.0.0/8")`
					out = append(out,
						`{a="b"} | json | `+filter,
						`{a="b"} | json | (`+filter+`)`,
						`sum by (A) (count_over_time({a="b"} | json | `+filter+` [5m]))`,
					)
				}
			}
		}
	}
	return out
}

// TestIPFenceReadsALabelNamedByAnUnknownCharacter pins the ip() label filter fence on the spellings its text screen
// did not read: a label filter whose name is one character that starts no token, after another filter of the
// stage (FuzzResponseKeyFencesMatchParsedTree found `A=""#c` + line break + `\v=` + line break + `IP("")`). The
// parser keeps the ip() leaf in the tree, so every key the query builds carries the fence, read as sent and in the
// spelling the handlers read (RequestQueryText). The same filters with no ip() call carry no fence.
//
// conformance: cache-response-key-versions
func TestIPFenceReadsALabelNamedByAnUnknownCharacter(t *testing.T) {
	// These tests check the fences against the parser, not the translation: their spellings stay out of the query corpus.
	t.Cleanup(querycorpus.PauseRecording())
	for _, q := range ipFenceUnknownNameQueries() {
		for _, text := range []string{q, RequestQueryText(q)} {
			f, _, ok := factsOf(text)
			if !ok || !f.ip {
				t.Fatalf("%+q: the validation must accept it with an ip() filter in its tree (ok=%v ip=%v)", text, ok, f.ip)
			}
			if got := ipLabelFilterResponseKeyVersion(text); got != ipLabelFilterResponseVersion {
				t.Errorf("%+q: ip() fence %q, want %q", text, got, ipLabelFilterResponseVersion)
			}
			r := fenceShapeRequest(text)
			for _, site := range []responseKeySite{responseKeyCompatQuery, responseKeyCompatQueryRange, responseKeyInnerQueryRange} {
				if !strings.Contains(responseKeyFences(r, text, site), ipLabelFilterResponseVersion) {
					t.Errorf("%+q: the key chain at site %d lacks the ip() fence", text, site)
				}
			}
		}
	}
	for _, name := range ipFenceUnknownNames {
		for _, q := range []string{
			`{a="b"} | json | A="x" ` + name + `="10.0.0.1"`,
			`{a="b"} | json | A="x" or ` + name + `!="ip"`,
			`sum by (A) (count_over_time({a="b"} | json | A="x", ` + name + `="zip" [5m]))`,
		} {
			if _, _, ok := factsOf(q); !ok {
				t.Fatalf("%+q: the validation must accept it", q)
			}
			if got := ipLabelFilterResponseKeyVersion(q); got != "" {
				t.Errorf("%+q: no ip() call, yet the fence %q fires", q, got)
			}
		}
	}
}

// TestIPFenceNeverServesAnAnswerCachedUnderTheUnfencedKey is the cache side of the fence: an answer an earlier build
// cached for such a query, under the key with no ip() fence, is not served; the handler answers again. The control
// row seeds the key this build writes and is served from it, which proves the seeded key is the edge's own.
//
// conformance: cache-response-key-versions
func TestIPFenceNeverServesAnAnswerCachedUnderTheUnfencedKey(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	const stale = `{"status":"success","data":{"resultType":"streams","result":[{"stream":{"a":"stale"},"values":[["1","stale"]]}]}}`
	query := `{a="b"} | json | A="x" ;=ip("10.0.0.0/8")`
	if RequestQueryText(query) != query {
		t.Fatalf("the query must be in the spelling the handlers read: %q", RequestQueryText(query))
	}
	target := "/loki/api/v1/query_range?" + url.Values{
		"query": {query}, "start": {"1600000000"}, "end": {"1600003600"}, "limit": {"10"}, "direction": {"backward"},
	}.Encode()
	headers := map[string]string{"X-Scope-OrgID": "team-a"}
	for _, tc := range []struct {
		name      string
		seedKey   func(key string) string
		wantStale bool
	}{
		{"this build's key is served", func(key string) string { return key }, true},
		{"the unfenced key is not", func(key string) string { return strings.Replace(key, ipLabelFilterResponseVersion, "", 1) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int64
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
			}))
			defer backend.Close()
			p := newCompatTestProxy(t, backend.URL)
			req := httptest.NewRequest(http.MethodGet, target, nil)
			req.Header.Set("X-Scope-OrgID", "team-a")
			key, ok := p.compatCacheKey("query_range", req)
			if !ok || !strings.Contains(key, ipLabelFilterResponseVersion) {
				t.Fatalf("compat key %q must carry the ip() fence", key)
			}
			seed := tc.seedKey(key)
			p.compatCache.Set(seed, []byte(stale))
			rec := doCompatProxyRequest(p, target, headers)
			if rec.Code != http.StatusOK {
				t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}
			served := strings.Contains(rec.Body.String(), `"stale"`)
			if served != tc.wantStale {
				t.Fatalf("stale answer served = %v, want %v (backend calls %d): %s", served, tc.wantStale, calls.Load(), rec.Body.String())
			}
			if !tc.wantStale && calls.Load() == 0 {
				t.Fatal("the handler must answer again from VictoriaLogs")
			}
		})
	}
}
