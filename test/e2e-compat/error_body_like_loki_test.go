//go:build e2e

package e2e_compat

import (
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// errBodyRaw sends a request and returns the status, headers and body without
// following redirects.
func errBodyRaw(t *testing.T, method, target string, form url.Values) (int, http.Header, []byte) {
	t.Helper()
	var body io.Reader
	if form != nil {
		body = strings.NewReader(form.Encode())
	}
	req, err := http.NewRequestWithContext(t.Context(), method, target, body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Scope-OrgID", "0")
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	client := &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, target, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header, data
}

// TestCompat_ErrorBodyLikeLoki sends the same failing request to the stack
// Loki and to the proxy and compares what a client reads: the status, the
// Content-Type, nosniff and the body. Where the proxy's text is Loki's text the
// bytes are compared exactly; where Loki's parser words an error differently
// (open gap semantics/parse-error-text-parity) the shape is compared: the
// same status, Content-Type and framing, the message alone with no JSON
// envelope. The proxy's own hardening headers (Cache-Control, X-Frame-Options,
// Cross-Origin-Resource-Policy, Expires, Pragma) are the one allowed
// difference.
// conformance: semantics/error-layer-body-shape, status-400, status-404
func TestCompat_ErrorBodyLikeLoki(t *testing.T) {
	ensureDataIngested(t)
	waitForLokiMetricData(t)
	end := time.Now().Truncate(time.Second)
	window := func(extra url.Values) url.Values {
		v := url.Values{"start": {strconv.FormatInt(end.Add(-time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
		for k, vs := range extra {
			v[k] = vs
		}
		return v
	}
	// Whole seconds, so both sides print the same "query length".
	long := func(extra url.Values) url.Values {
		v := url.Values{"start": {strconv.FormatInt(end.Add(-800*time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
		for k, vs := range extra {
			v[k] = vs
		}
		return v
	}
	const sel = `{app="api-gateway"}`
	cases := []struct {
		name, method, path string
		params             url.Values
		form               bool
		exact              bool
	}{
		// The proxy's text is Loki's: byte for byte.
		{name: "query_range/missing_query", path: "/loki/api/v1/query_range", params: window(url.Values{"step": {"60"}}), exact: true},
		{name: "query/missing_query", path: "/loki/api/v1/query", params: url.Values{"time": {strconv.FormatInt(end.UnixNano(), 10)}}, exact: true},
		{name: "query/log_query_as_instant", path: "/loki/api/v1/query", params: url.Values{"query": {sel}, "time": {strconv.FormatInt(end.UnixNano(), 10)}}, exact: true},
		{name: "index_stats/missing_query", path: "/loki/api/v1/index/stats", params: window(nil), exact: true},
		{name: "index_volume/missing_query", path: "/loki/api/v1/index/volume", params: window(nil), exact: true},
		{name: "index_volume_range/missing_query", path: "/loki/api/v1/index/volume_range", params: window(url.Values{"step": {"60"}}), exact: true},
		{name: "patterns/missing_query", path: "/loki/api/v1/patterns", params: window(url.Values{"step": {"60"}}), exact: true},
		{name: "detected_fields/missing_query", path: "/loki/api/v1/detected_fields", params: window(nil), exact: true},
		{name: "detected_field_values/missing_query", path: "/loki/api/v1/detected_field/level/values", params: window(nil), exact: true},
		{name: "tail/missing_query", path: "/loki/api/v1/tail", exact: true},
		{name: "tail/no_upgrade", path: "/loki/api/v1/tail", params: url.Values{"query": {sel}}, exact: true},
		{name: "query_range/points_per_series", path: "/loki/api/v1/query_range", params: long(url.Values{"query": {sel}, "step": {"1"}}), exact: true},
		{name: "labels/max_query_length", path: "/loki/api/v1/labels", params: long(nil), exact: true},
		{name: "label_values/max_query_length", path: "/loki/api/v1/label/app/values", params: long(nil), exact: true},
		{name: "series/max_query_length", path: "/loki/api/v1/series", params: long(url.Values{"match[]": {sel}}), exact: true},
		{name: "detected_labels/max_query_length", path: "/loki/api/v1/detected_labels", params: long(url.Values{"query": {sel}}), exact: true},
		{name: "unknown/404", path: "/loki/api/v1/nope", exact: true},
		{name: "unknown/root_404", path: "/nope", exact: true},
		// The ruler's JSON with no nosniff and a sniffed text/plain (api.go:166-179).
		{name: "prometheus_rules/unknown_type", path: "/prometheus/api/v1/rules", params: url.Values{"type": {"xyz"}}, exact: true},
		// Request parameters, checked before the query: Loki's text, byte for byte
		// (recorded rows r0535 ff.; internal/proxy/testdata/loki_request_param_answers.jsonl).
		{name: "query_range/bad_start", path: "/loki/api/v1/query_range", params: url.Values{"query": {sel}, "start": {"abc"}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}, exact: true},
		{name: "labels/bad_start", path: "/loki/api/v1/labels", params: url.Values{"start": {"abc"}}, exact: true},
		{name: "query_range/bad_since", path: "/loki/api/v1/query_range", params: window(url.Values{"query": {sel}, "step": {"60"}, "since": {"abc"}}), exact: true},
		{name: "query_range/step_zero", path: "/loki/api/v1/query_range", params: window(url.Values{"query": {sel}, "step": {"0"}}), exact: true},
		{name: "query_range/step_not_a_duration", path: "/loki/api/v1/query_range", params: window(url.Values{"query": {sel}, "step": {"1h1h"}}), exact: true},
		{name: "query_range/end_before_start", path: "/loki/api/v1/query_range", params: url.Values{"query": {sel}, "start": {strconv.FormatInt(end.UnixNano(), 10)}, "end": {strconv.FormatInt(end.Add(-time.Hour).UnixNano(), 10)}, "step": {"60"}}, exact: true},
		{name: "query_range/bad_limit", path: "/loki/api/v1/query_range", params: window(url.Values{"query": {sel}, "step": {"60"}, "limit": {"abc"}}), exact: true},
		{name: "query_range/zero_limit", path: "/loki/api/v1/query_range", params: window(url.Values{"query": {sel}, "step": {"60"}, "limit": {"0"}}), exact: true},
		{name: "query_range/bad_direction", path: "/loki/api/v1/query_range", params: window(url.Values{"query": {sel}, "step": {"60"}, "direction": {"bad"}}), exact: true},
		{name: "query_range/bad_interval", path: "/loki/api/v1/query_range", params: window(url.Values{"query": {sel}, "step": {"60"}, "interval": {"-1"}}), exact: true},
		{name: "query_range/bad_start_and_bad_query", path: "/loki/api/v1/query_range", params: url.Values{"query": {"{"}, "start": {"abc"}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {"60"}}, exact: true},
		{name: "query_range/bad_limit_and_bad_step", path: "/loki/api/v1/query_range", params: window(url.Values{"query": {sel}, "step": {"abc"}, "limit": {"abc"}}), exact: true},
		{name: "query/bad_time", path: "/loki/api/v1/query", params: url.Values{"query": {sel}, "time": {"abc"}}, exact: true},
		{name: "query/bad_limit_before_log_query", path: "/loki/api/v1/query", params: url.Values{"query": {sel}, "time": {strconv.FormatInt(end.UnixNano(), 10)}, "limit": {"abc"}}, exact: true},
		{name: "index_stats/end_before_start", path: "/loki/api/v1/index/stats", params: url.Values{"query": {sel}, "start": {strconv.FormatInt(end.UnixNano(), 10)}, "end": {strconv.FormatInt(end.Add(-time.Hour).UnixNano(), 10)}}, exact: true},
		{name: "index_volume/max_query_length", path: "/loki/api/v1/index/volume", params: long(url.Values{"query": {sel}}), exact: true},
		{name: "index_volume_range/bad_step", path: "/loki/api/v1/index/volume_range", params: window(url.Values{"query": {sel}, "step": {"abc"}}), exact: true},
		{name: "index_volume/bad_aggregate_by", path: "/loki/api/v1/index/volume", params: window(url.Values{"query": {sel}, "aggregateBy": {"foo"}}), exact: true},
		{name: "patterns/bad_since", path: "/loki/api/v1/patterns", params: window(url.Values{"query": {sel}, "step": {"60"}, "since": {"abc"}}), exact: true},
		{name: "detected_fields/max_query_length", path: "/loki/api/v1/detected_fields", params: long(url.Values{"query": {sel}}), exact: true},
		{name: "detected_fields/zero_limit", path: "/loki/api/v1/detected_fields", params: window(url.Values{"query": {sel}, "limit": {"0"}}), exact: true},
		{name: "detected_field_values/bad_limit", path: "/loki/api/v1/detected_field/level/values", params: window(url.Values{"query": {sel}, "limit": {"abc"}}), exact: true},
		{name: "tail/bad_limit", path: "/loki/api/v1/tail", params: url.Values{"query": {sel}, "limit": {"abc"}}, exact: true},
		{name: "tail/delay_for_over_max", path: "/loki/api/v1/tail", params: url.Values{"query": {sel}, "delay_for": {"10"}}, exact: true},
		{name: "tail/bad_start", path: "/loki/api/v1/tail", params: url.Values{"query": {sel}, "start": {"abc"}}, exact: true},
		{name: "labels/bad_url_escape", path: "/loki/api/v1/labels?query=%zz", exact: true},
		{name: "labels/semicolon", path: "/loki/api/v1/labels?start=1;end=2", exact: true},
		// Loki's parser words these differently: the shape is compared.
		{name: "query_range/bad_query", path: "/loki/api/v1/query_range", params: window(url.Values{"query": {"{"}, "step": {"60"}})},
		{name: "query/bad_query", path: "/loki/api/v1/query", params: url.Values{"query": {"{"}, "time": {strconv.FormatInt(end.UnixNano(), 10)}}},
		{name: "series/bad_match", path: "/loki/api/v1/series", params: window(url.Values{"match[]": {"{"}})},
		{name: "index_stats/bad_query", path: "/loki/api/v1/index/stats", params: window(url.Values{"query": {"{"}})},
		{name: "index_volume/bad_query", path: "/loki/api/v1/index/volume", params: window(url.Values{"query": {"{"}})},
		{name: "patterns/bad_query", path: "/loki/api/v1/patterns", params: window(url.Values{"query": {"{"}, "step": {"60"}})},
		{name: "detected_fields/bad_query", path: "/loki/api/v1/detected_fields", params: window(url.Values{"query": {"{"}})},
		{name: "detected_labels/bad_query", path: "/loki/api/v1/detected_labels", params: window(url.Values{"query": {"{"}})},
		{name: "tail/bad_query", path: "/loki/api/v1/tail", params: url.Values{"query": {"{"}}},
		{name: "query_range/post_form_bad_query", method: http.MethodPost, path: "/loki/api/v1/query_range", params: window(url.Values{"query": {"{"}, "step": {"60"}}), form: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			method := tc.method
			if method == "" {
				method = http.MethodGet
			}
			send := func(base string) (int, http.Header, []byte) {
				if tc.form {
					return errBodyRaw(t, method, base+tc.path, tc.params)
				}
				target := base + tc.path
				if len(tc.params) > 0 {
					target += "?" + tc.params.Encode()
				}
				return errBodyRaw(t, method, target, nil)
			}
			lokiStatus, lokiHeader, lokiBody := send(lokiURL)
			if lokiStatus < 400 {
				t.Fatalf("Loki fixture drifted: want an error, got %d %.200s", lokiStatus, lokiBody)
			}
			status, header, body := send(proxyURL)
			if status != lokiStatus {
				t.Fatalf("status: proxy %d, Loki %d; proxy body %q, Loki body %q", status, lokiStatus, body, lokiBody)
			}
			if got, want := header.Get("Content-Type"), lokiHeader.Get("Content-Type"); got != want {
				t.Fatalf("Content-Type: proxy %q, Loki %q", got, want)
			}
			if got, want := header.Get("X-Content-Type-Options"), lokiHeader.Get("X-Content-Type-Options"); got != want {
				t.Fatalf("X-Content-Type-Options: proxy %q, Loki %q", got, want)
			}
			if tc.exact {
				if string(body) != string(lokiBody) {
					t.Fatalf("body: proxy %q, Loki %q", body, lokiBody)
				}
				return
			}
			// Shape: the message alone (no JSON envelope), with the same framing.
			if len(body) == 0 || body[0] == '{' || strings.HasSuffix(string(body), "\n") != strings.HasSuffix(string(lokiBody), "\n") {
				t.Fatalf("proxy body %q is not Loki's shape %q", body, lokiBody)
			}
		})
	}
}
