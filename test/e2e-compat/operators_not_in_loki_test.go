//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Loki has no group(), no label_join() and none of the PromQL math, time or
// quantile functions: a name outside the lexer's function tokens is an
// IDENTIFIER, and the query is a 400 parse error at the column of that name.
// Every Loki-compatible proxy answers the same status and the same bytes, on
// the range and the instant endpoint, wherever the name stands in the query.
//
// conformance: semantics/proxy-accepts-operators-not-in-loki
func TestCompat_OperatorsNotInLokiRejected(t *testing.T) {
	ensureDataIngested(t)
	end := time.Now()
	window := func(params url.Values) url.Values {
		out := cloneQueryValues(params)
		out.Set("start", strconv.FormatInt(end.Add(-time.Hour).UnixNano(), 10))
		out.Set("end", strconv.FormatInt(end.UnixNano(), 10))
		out.Set("time", strconv.FormatInt(end.UnixNano(), 10))
		out.Set("step", "60")
		return out
	}
	queries := []string{
		`group(count_over_time({app="api-gateway"}[5m]))`,
		`GROUP(count_over_time({app="api-gateway"}[5m]))`,
		`group by (app) (count_over_time({app="api-gateway"}[5m]))`,
		`group(sum by (app) (count_over_time({env="production"}[5m])))`,
		`sum(group(rate({app="api-gateway"}[5m])))`,
		`label_join(rate({app="api-gateway"}[5m]), "d", ",", "app", "level")`,
		`LABEL_JOIN(rate({app="api-gateway"}[5m]), "d", ",", "app")`,
		`label_join(sum by (app) (count_over_time({env="production"}[5m])), "app_copy", "", "app")`,
		`label_replace(label_join(rate({app="api-gateway"}[5m]), "d", ",", "app"), "a", "b", "c", "d")`,
		`sum(rate({app="api-gateway"}[5m])) + clamp_min(rate({app="api-gateway"}[5m]), 1)`,
		`abs(rate({app="api-gateway"}[5m]))`,
		`round(rate({app="api-gateway"}[5m]))`,
		`ln(rate({app="api-gateway"}[5m]))`,
		`scalar(rate({app="api-gateway"}[5m]))`,
		`quantile(0.5, rate({app="api-gateway"}[5m]))`,
		`count_values("v", count_over_time({app="api-gateway"}[5m]))`,
		`time()`,
	}
	for _, path := range []string{"/loki/api/v1/query_range", "/loki/api/v1/query"} {
		for _, query := range queries {
			t.Run(path+" "+query, func(t *testing.T) {
				params := window(url.Values{"query": {query}})
				lokiStatus, lokiBody := rejectedQueryGet(t, lokiURL, path, params, "0", nil)
				lokiMsg := strings.TrimSpace(string(lokiBody))
				if lokiStatus != http.StatusBadRequest || !strings.HasPrefix(lokiMsg, "parse error at line ") {
					t.Fatalf("Loki fixture drifted: want a 400 parse error, got %d %s", lokiStatus, lokiBody)
				}
				for name, base := range lokiProfileProxies() {
					status, body := rejectedQueryGet(t, base, path, params, "0", nil)
					var envelope struct {
						Status, ErrorType, Error string
					}
					if status != http.StatusBadRequest || json.Unmarshal(body, &envelope) != nil ||
						envelope.Status != "error" || envelope.ErrorType != "bad_data" || envelope.Error != lokiMsg {
						t.Errorf("%s proxy: %d %s, want 400 bad_data %q", name, status, body, lokiMsg)
					}
				}
			})
		}
	}
}

// Loki lower-cases a word before it looks it up, so SUM, TOPK(3, ...), RATE,
// BY, OFFSET, UNWRAP, BOOL, ON and GROUP_LEFT are the keywords they spell. The
// proxy answers 200 with the series Loki answers.
//
// conformance: semantics/proxy-accepts-operators-not-in-loki
func TestCompat_UppercaseKeywordsMatchLoki(t *testing.T) {
	ensureDataIngested(t)
	end := time.Now()
	queries := []string{
		`TOPK(3, SUM BY (app) (RATE({env="production"}[5m])))`,
		`SUM BY (app) (COUNT_OVER_TIME({env="production"}[5m]))`,
		`SUM(RATE({env="production"}[5m])) BY (app)`,
		`SUM WITHOUT (level) (COUNT_OVER_TIME({env="production"}[5m]))`,
		`MAX BY (app) (BYTES_OVER_TIME({env="production"}[5m]))`,
		`SUM BY (app) (COUNT_OVER_TIME({env="production"}[5m])) / ON (app) SUM BY (app) (COUNT_OVER_TIME({env="production"}[5m]))`,
		`SUM BY (app) (COUNT_OVER_TIME({env="production"}[5m])) > BOOL 0`,
		`SUM BY (app) (COUNT_OVER_TIME({env="production"}[5m] OFFSET 1m))`,
		`SUM BY (app) (SUM_OVER_TIME({env="production"} | JSON | UNWRAP duration_ms [5m]))`,
	}
	type result struct {
		status     int
		resultType string
		series     []string
	}
	run := func(base, path string, params url.Values) result {
		status, body := rejectedQueryGet(t, base, path, params, "0", nil)
		out := result{status: status}
		var resp struct {
			Data struct {
				ResultType string `json:"resultType"`
				Result     []struct {
					Metric map[string]string `json:"metric"`
				} `json:"result"`
			} `json:"data"`
		}
		if status == http.StatusOK && json.Unmarshal(body, &resp) == nil {
			out.resultType = resp.Data.ResultType
			for _, s := range resp.Data.Result {
				keys := make([]string, 0, len(s.Metric))
				for k, v := range s.Metric {
					keys = append(keys, k+"="+v)
				}
				sort.Strings(keys)
				out.series = append(out.series, strings.Join(keys, ","))
			}
			sort.Strings(out.series)
		}
		return out
	}
	for _, path := range []string{"/loki/api/v1/query_range", "/loki/api/v1/query"} {
		for _, query := range queries {
			t.Run(path+" "+query, func(t *testing.T) {
				params := url.Values{
					"query": {query},
					"start": {strconv.FormatInt(end.Add(-time.Hour).UnixNano(), 10)},
					"end":   {strconv.FormatInt(end.UnixNano(), 10)},
					"time":  {strconv.FormatInt(end.UnixNano(), 10)},
					"step":  {"60"},
				}
				loki := run(lokiURL, path, params)
				if loki.status != http.StatusOK {
					t.Fatalf("Loki fixture drifted: want 200, got %d", loki.status)
				}
				for name, base := range lokiProfileProxies() {
					got := run(base, path, params)
					if got.status != loki.status || got.resultType != loki.resultType || strings.Join(got.series, "|") != strings.Join(loki.series, "|") {
						t.Errorf("%s proxy: status %d %s series %q, Loki %d %s series %q",
							name, got.status, got.resultType, got.series, loki.status, loki.resultType, loki.series)
					}
				}
			})
		}
	}
}
