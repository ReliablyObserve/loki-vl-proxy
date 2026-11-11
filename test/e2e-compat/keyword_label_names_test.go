//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// keywordLabelFixture is two streams with the labels Loki accepts and LogsQL reads as keywords: service_name and
// in=<svc> on one, service_name and not=<svc> on the other, one line each, ingested alike into Loki and VictoriaLogs.
// The value is the run's service name, so a selector on in alone finds no line of an earlier run. It returns the
// service name, the lines by label and the query window around them.
func keywordLabelFixture(t *testing.T) (svc string, lines map[string]string, start, end time.Time) {
	t.Helper()
	svc = fmt.Sprintf("kw-labels-%d", time.Now().UnixNano())
	base := time.Now().Add(-time.Minute).Truncate(time.Second)
	lines = map[string]string{"in": "kw in line " + svc, "not": "kw not line " + svc}
	for i, label := range []string{"in", "not"} {
		ts := base.Add(time.Duration(i+1) * time.Second)
		row, _ := json.Marshal(map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": lines[label], "service_name": svc, label: svc})
		status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=service_name,"+label,
			string(row)+"\n", map[string]string{"Content-Type": "application/stream+json"})
		if status != http.StatusOK {
			t.Fatalf("VictoriaLogs ingest: %d %s", status, body)
		}
		push, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{
			"stream": map[string]string{"service_name": svc, label: svc},
			"values": [][]string{{strconv.FormatInt(ts.UnixNano(), 10), lines[label]}},
		}}})
		status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(push), map[string]string{"Content-Type": "application/json"})
		if status != http.StatusNoContent {
			t.Fatalf("Loki ingest: %d %s", status, body)
		}
	}
	forceVLFlush(t)
	return svc, lines, base.Add(-time.Minute), base.Add(30 * time.Second)
}

// logStreamLines is the stream count and the sorted lines of a query_range log answer.
func logStreamLines(payload map[string]interface{}) (streams int, lines []string) {
	result := extractArray(extractMap(payload, "data"), "result")
	for _, item := range result {
		stream, _ := item.(map[string]interface{})
		values, _ := stream["values"].([]interface{})
		for _, v := range values {
			if pair, _ := v.([]interface{}); len(pair) == 2 {
				lines = append(lines, fmt.Sprint(pair[1]))
			}
		}
	}
	sort.Strings(lines)
	return len(result), lines
}

// A label Loki accepts and LogsQL reads as a keyword (in, not) is filtered on as the field it is: the proxy answers
// the selector and the label filters like Loki, where VictoriaLogs rejected the bare name. A label named after a
// word of Loki's lexer (of, and keep OF) is Loki's 400, with Loki's text. Both sides hold the fixture before anything
// is compared.
//
// conformance: semantics/translator-writes-keyword-field-names-bare, semantics/loki-keyword-label-names, loki_api_v1_query_range
func TestCompat_KeywordLabelNamesLikeLoki(t *testing.T) {
	headers := map[string]string{"X-Scope-OrgID": "0"}
	svc, lines, start, end := keywordLabelFixture(t)
	params := func(query string) url.Values {
		return url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)},
			"end": {strconv.FormatInt(end.UnixNano(), 10)}, "limit": {"1000"}}
	}

	// Both sides hold both lines.
	selector := fmt.Sprintf(`{service_name=%q}`, svc)
	deadline := time.Now().Add(2 * time.Minute)
	for {
		_, ll := logStreamLines(mustQueryRange(t, lokiURL, params(selector), headers))
		_, pl := logStreamLines(mustQueryRange(t, proxyURL, params(selector), headers))
		if len(ll) == 2 && len(pl) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fixture not visible on both sides: Loki %v, proxy %v", ll, pl)
		}
		time.Sleep(time.Second)
	}

	for _, tc := range []struct {
		query string
		want  []string
	}{
		{fmt.Sprintf(`{in=%q}`, svc), []string{lines["in"]}},
		{selector + fmt.Sprintf(` | in=%q`, svc), []string{lines["in"]}},
		{selector + fmt.Sprintf(` | not=%q`, svc), []string{lines["not"]}},
	} {
		ls, lb, lp := queryRangeGET(t, lokiURL, params(tc.query), headers)
		ps, pb, pp := queryRangeGET(t, proxyURL, params(tc.query), headers)
		if ls != http.StatusOK {
			t.Fatalf("%s: Loki answered %d %s", tc.query, ls, lb)
		}
		lStreams, lLines := logStreamLines(lp)
		pStreams, pLines := logStreamLines(pp)
		if strings.Join(lLines, "\n") != strings.Join(tc.want, "\n") {
			t.Fatalf("%s: Loki answered the lines %q, the test expects %q", tc.query, lLines, tc.want)
		}
		if ps != ls || pStreams != lStreams || strings.Join(pLines, "\n") != strings.Join(lLines, "\n") {
			t.Errorf("%s:\n proxy %d, %d streams, %q (%s)\n Loki  %d, %d streams, %q", tc.query, ps, pStreams, pLines, pb, ls, lStreams, lLines)
		}
	}

	for _, query := range []string{
		fmt.Sprintf(`{service_name=%q, of="a"}`, svc),
		fmt.Sprintf(`{service_name=%q} | keep OF`, svc),
	} {
		ls, lb := hardeningRequest(t, http.MethodGet, lokiURL+"/loki/api/v1/query_range?"+params(query).Encode(), "", headers)
		ps, pb := hardeningRequest(t, http.MethodGet, proxyURL+"/loki/api/v1/query_range?"+params(query).Encode(), "", headers)
		if ls != http.StatusBadRequest {
			t.Fatalf("%s: Loki answered %d %s, the test pins 400", query, ls, lb)
		}
		if ps != ls || string(pb) != string(lb) {
			t.Errorf("%s:\n proxy %d %q\n Loki  %d %q", query, ps, pb, ls, lb)
		}
	}
}

// mustQueryRange is queryRangeGET for an answer that must be a 200; a poll treats anything else as no data yet.
func mustQueryRange(t *testing.T, base string, params url.Values, headers map[string]string) map[string]interface{} {
	t.Helper()
	status, _, payload := queryRangeGET(t, base, params, headers)
	if status != http.StatusOK {
		return nil
	}
	return payload
}
