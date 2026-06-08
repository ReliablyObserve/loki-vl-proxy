package proxy

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// TestSanitizeUpstreamError_RedactsQuery covers finding 1: transport errors are
// *url.Error whose message embeds the full backend URL including the LogQL/LogsQL
// query in RawQuery, leaking it into logs and handler error responses.
func TestSanitizeUpstreamError_RedactsQuery(t *testing.T) {
	underlying := errors.New("dial tcp 127.0.0.1:9428: connect: connection refused")
	raw := &url.Error{
		Op:  "Get",
		URL: `http://victorialogs:9428/select/logsql/query?query=%7Bapp%3D%22secret%22%7D+%7C%3D+%22password%3Dhunter2%22&start=1`,
		Err: underlying,
	}

	// Redaction on by default.
	p := &Proxy{}
	got := p.sanitizeUpstreamError(raw)
	msg := got.Error()
	for _, leak := range []string{"secret", "password", "hunter2", "query="} {
		if strings.Contains(msg, leak) {
			t.Errorf("sanitized error still leaks %q: %s", leak, msg)
		}
	}
	if !strings.Contains(msg, "redacted") {
		t.Errorf("expected redacted marker, got: %s", msg)
	}
	// Underlying cause preserved so statusFromUpstreamErr / breaker still classify.
	if !errors.Is(got, underlying) {
		t.Errorf("sanitized error lost the underlying cause: %v", got)
	}
	// Path/host kept for diagnostics.
	if !strings.Contains(msg, "victorialogs:9428") || !strings.Contains(msg, "/select/logsql/query") {
		t.Errorf("expected host+path retained, got: %s", msg)
	}

	// Raw-query debug mode opts out of redaction.
	pRaw := &Proxy{debugLogRawQueries: true}
	if pRaw.sanitizeUpstreamError(raw).Error() != raw.Error() {
		t.Error("debugLogRawQueries=true should not redact")
	}
	// Non-url errors and nil pass through.
	plain := errors.New("boom")
	if p.sanitizeUpstreamError(plain) != plain {
		t.Error("non-url error should pass through unchanged")
	}
	if p.sanitizeUpstreamError(nil) != nil {
		t.Error("nil should pass through")
	}
}

// TestWindowedHits_GatedToGrafana covers finding 3: the window-sampled /hits
// rewrite (lossy top-N sampling) must only apply to Grafana-sourced requests so
// direct API clients keep exact stats semantics.
func TestWindowedHits_GatedToGrafana(t *testing.T) {
	p := &Proxy{}
	q := `sum by (pod) (count_over_time({env="production"} | pod!="" [2m]))`
	mk := func(grafana bool) *http.Request {
		// 24h range so the >=2h gate would otherwise pass.
		r := httptest.NewRequest(http.MethodGet,
			"/loki/api/v1/query_range?start=1700000000000000000&end=1700086400000000000&step=120s&query="+url.QueryEscape(q), nil)
		if grafana {
			r.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
		}
		return r
	}

	// Non-Grafana → must return false (falls through to exact path) without writing.
	w := httptest.NewRecorder()
	if p.tryHighCardCountByWindowedHits(w, mk(false), `pod!="" | stats by (pod) count()`) {
		t.Error("windowed-hits ran for a non-Grafana request; API clients must get exact stats")
	}
	if w.Body.Len() != 0 {
		t.Errorf("non-Grafana path wrote a body: %s", w.Body.String())
	}

	// Sanity: a Grafana request is NOT rejected by the source gate (it proceeds
	// past it; later steps may still no-op without a backend, but it must not be
	// short-circuited by isGrafanaSourcedRequest).
	if !isGrafanaSourcedRequest(mk(true)) {
		t.Error("expected the drilldown-tagged request to be Grafana-sourced")
	}
}
