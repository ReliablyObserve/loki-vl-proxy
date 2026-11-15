package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
)

// A step is read as Loki reads it: whole-number units from ms up to y in descending order, no fractions, no
// microseconds or nanoseconds (Prometheus' duration grammar), so Go's wider duration syntax is a 400 with
// `cannot parse "<step>" to a valid duration`, and a step of zero or less is a 400 with Loki's resolution text,
// before any VictoriaLogs call. Loki 3.7.9 answered `1.5s` and `0` as listed in the live differential of
// 2026-11-14 (build 726dfb48f); the other spellings follow the same text, and the recorded request rows
// (loki_request_param_answers.jsonl, TestRequestParamAnswers) hold `abc`, `5x`, `-1` and `0`.
//
// conformance: semantics/step-duration-grammar-wider-than-loki, semantics/step-nonpositive-or-unparsable-is-400, loki_api_v1_query_range
func TestStepGrammarIsLokis(t *testing.T) {
	var calls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "unexpected backend call", http.StatusTeapot)
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	const zeroText = "zero or negative query resolution step widths are not accepted. Try a positive integer"
	for _, tc := range []struct{ step, want string }{
		{"1.5s", `cannot parse "1.5s" to a valid duration`},
		{"500us", `cannot parse "500us" to a valid duration`},
		{"1h30m5.5s", `cannot parse "1h30m5.5s" to a valid duration`},
		{"abc", `cannot parse "abc" to a valid duration`},
		{"5x", `cannot parse "5x" to a valid duration`},
		{"0", zeroText},
		{"-1", zeroText},
	} {
		t.Run(tc.step, func(t *testing.T) {
			params := url.Values{
				"query": {`sum(count_over_time({app="api-gateway"}[1m]))`},
				"start": {"1800000000"}, "end": {"1800000600"}, "step": {tc.step},
			}
			w := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
			if body := readErrorRec(t, w); w.Code != http.StatusBadRequest || body.Error != tc.want {
				t.Fatalf("status=%d body=%s, want 400 %q", w.Code, w.Body, tc.want)
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("rejected steps reached the backend %d times", calls.Load())
	}
	// What Loki accepts stays accepted: seconds, a whole-number duration, a decimal number of seconds.
	for _, step := range []string{"15", "15s", "1m30s", "0.5"} {
		params := url.Values{"query": {`sum(count_over_time({app="api-gateway"}[1m]))`}, "start": {"1800000000"}, "end": {"1800000600"}, "step": {step}}
		w := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
		if w.Code == http.StatusBadRequest {
			t.Errorf("step %s: %d %s, Loki accepts it", step, w.Code, w.Body)
		}
	}
}
