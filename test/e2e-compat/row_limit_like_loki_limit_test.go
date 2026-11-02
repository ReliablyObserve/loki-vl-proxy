//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// TestCompat_RowLimitAnswersLikeLokiLimit compares the proxy's answer to a
// metric that passes -manual-range-metric-row-limit with live Loki's answer to
// a query over one of its own limits. Loki answers such a query 400 with the
// raw message (pkg/util/server/error.go ClientHTTPStatusAndError, ErrLimit and
// user errors; WriteError); the stack's max_query_series is 1,000,000, which a
// query cannot reach, so the Loki side is max_query_length (default 721h), a
// limit the stack Loki enforces with the same status. The proxy side is an
// in-process proxy on the stack's VictoriaLogs with a row limit of 1, reading
// a metric that always scans raw rows (rate_counter), on query_range and query.
// The proxy keeps its JSON error envelope (loki/errors/status-400), so the
// status, errorType and the absence of backend text are compared, and the text
// must name the proxy's flag since Loki has none.
// conformance: heavy-metric-fetch-bounds, limits/row-limit-error-names-flag, status-400
func TestCompat_RowLimitAnswersLikeLokiLimit(t *testing.T) {
	ensureDataIngested(t)
	waitForLokiMetricData(t)
	end := time.Now()
	start := end.Add(-time.Hour)
	const rawRows = `sum(rate_counter({app="api-gateway"} | json | __error__="" | duration_ms!="" | unwrap duration_ms [5m]))`

	// Loki's answer to a query over its max_query_length.
	tooLong := url.Values{
		"query": {`sum(count_over_time({app="api-gateway"}[5m]))`},
		"start": {strconv.FormatInt(end.Add(-800*time.Hour).UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
		"step":  {"3600"},
	}
	lokiStatus, lokiBody := rejectedQueryGet(t, lokiURL, "/loki/api/v1/query_range", tooLong, "0", nil)
	if lokiStatus != http.StatusBadRequest || !strings.Contains(string(lokiBody), "the query time range exceeds the limit") {
		t.Fatalf("Loki fixture drifted: want 400 for a query over max_query_length, got %d %.300s", lokiStatus, lokiBody)
	}

	// Baseline: Loki answers the raw-row metric with data, so the proxy's limit
	// is the only reason for its refusal.
	rangeParams := url.Values{
		"query": {rawRows},
		"start": {strconv.FormatInt(start.UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
		"step":  {"60"},
	}
	if status, body := rejectedQueryGet(t, lokiURL, "/loki/api/v1/query_range", rangeParams, "0", nil); status != http.StatusOK || !strings.Contains(string(body), `"values"`) {
		t.Fatalf("Loki fixture drifted: want 200 with samples for %s, got %d %.300s", rawRows, status, body)
	}

	route := newErrLayerProxy(t, errLayerFaultBackend(t, "pass"), func(cfg *proxy.Config) { cfg.RangeMetricRowLimit = 1 })
	instantParams := url.Values{"query": {rawRows}, "time": {strconv.FormatInt(end.UnixNano(), 10)}}
	for _, tc := range []struct {
		name, path string
		params     url.Values
	}{
		{"query_range", "/loki/api/v1/query_range", rangeParams},
		{"query", "/loki/api/v1/query", instantParams},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := rejectedQueryGet(t, route, tc.path, tc.params, "0", nil)
			if status == http.StatusOK {
				t.Fatalf("the row limit of 1 was not reached; the case proves nothing: %.300s", body)
			}
			if status != lokiStatus {
				t.Fatalf("status %d, want Loki's %d for a query over a limit: %.300s", status, lokiStatus, body)
			}
			var envelope struct {
				Status    string `json:"status"`
				ErrorType string `json:"errorType"`
				Error     string `json:"error"`
				Message   string `json:"message"`
			}
			if err := json.Unmarshal(body, &envelope); err != nil {
				t.Fatalf("body is not the proxy's error envelope: %v: %.300s", err, body)
			}
			if envelope.Status != "error" || envelope.ErrorType != "bad_data" {
				t.Fatalf("envelope status %q errorType %q, want error and bad_data: %.300s", envelope.Status, envelope.ErrorType, body)
			}
			if !strings.HasPrefix(envelope.Error, "manual range metric row limit exceeded (1") || !strings.HasSuffix(envelope.Error, "; narrow the query or increase -manual-range-metric-row-limit") {
				t.Fatalf("error %q does not name the row limit flag", envelope.Error)
			}
			if envelope.Message != "" && envelope.Message != envelope.Error {
				t.Fatalf("message %q differs from error %q", envelope.Message, envelope.Error)
			}
			got := errLayerE2EClasses(status, body, nil, lokiStatus, "", false, true)
			errLayerE2EVerdict(t, got, nil, status, body)
		})
	}
}
