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

// TestCompat_DetectedFieldsEmptyAnswer: a detected_fields query that matches
// no line answers like Loki, with no fields and no limit. Loki lists only the
// fields of the lines the query returned and sets the limit only when it
// lists one (pkg/querier/queryrange/detected_fields.go:42-89), so its body is
// `{}`. The proxy used to list the native fields of the bare selector instead
// (the Logs Drilldown page of a JSON service with the error and warn level
// pills, whose escaped pipe matches no level) and to send the limit with no
// fields. The proxy keeps its status and data mirror.
//
// conformance: semantics/detected-fields-empty-answer, loki_api_v1_detected_fields
func TestCompat_DetectedFieldsEmptyAnswer(t *testing.T) {
	svc := fmt.Sprintf("dd-empty-%d", time.Now().UnixNano())
	base := time.Now().Add(-90 * time.Second)
	lines := map[string][]string{}
	for i := 0; i < 20; i++ {
		lines["json"] = append(lines["json"], fmt.Sprintf(`{"method": "%s", "path": "/api/v1/users/%d", "status": %d, "region": "us-east-1"}`,
			[]string{"GET", "POST"}[i%2], i, []int{500, 502}[i%2]))
		lines["logfmt"] = append(lines["logfmt"], fmt.Sprintf(`level=error msg="payment retry" tx_id=tx%d provider=stripe`, i))
		lines["plain"] = append(lines["plain"], fmt.Sprintf(`2026-11-03 14:00:%02d UTC [%d] ERROR:  deadlock detected`, i%60, 1000+i))
	}
	for kind, values := range lines {
		labels := map[string]string{"service_name": svc + "-" + kind, "level": "error", "env": "dd-empty"}
		var lokiValues, vlValues []interface{}
		for i, line := range values {
			ts := strconv.FormatInt(base.Add(time.Duration(i)*time.Second).UnixNano(), 10)
			lokiValues = append(lokiValues, []interface{}{ts, line})
			vlLine := line
			if strings.HasPrefix(line, "{") {
				var obj map[string]interface{}
				_ = json.Unmarshal([]byte(line), &obj)
				obj["_msg"] = line
				encoded, _ := json.Marshal(obj)
				vlLine = string(encoded)
			}
			vlValues = append(vlValues, []interface{}{ts, vlLine})
		}
		for _, target := range []struct {
			url    string
			values []interface{}
		}{
			{lokiURL + "/loki/api/v1/push", lokiValues},
			{vlURL + "/insert/loki/api/v1/push", vlValues},
		} {
			body, _ := json.Marshal(map[string]interface{}{"streams": []map[string]interface{}{{"stream": labels, "values": target.values}}})
			resp, err := http.Post(target.url, "application/json", strings.NewReader(string(body)))
			if err != nil {
				t.Fatalf("push %s: %v", target.url, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode/100 != 2 {
				t.Fatalf("push %s: status %d", target.url, resp.StatusCode)
			}
		}
	}
	forceVLFlush(t)

	const tail = `    | json  | logfmt | drop __error__, __error_details__ `
	window := url.Values{
		"start": {strconv.FormatInt(base.Add(-time.Minute).UnixNano(), 10)},
		"end":   {strconv.FormatInt(time.Now().Add(time.Minute).UnixNano(), 10)},
	}
	type answer struct {
		fields int
		labels string // sorted, without Loki's <key>_extracted names (semantics/detected-fields-extracted-suffix)
		limit  bool
		raw    string
	}
	ask := func(t *testing.T, target, query string) answer {
		t.Helper()
		params := url.Values{"query": {query}, "start": window["start"], "end": window["end"]}
		status, body := rejectedQueryGet(t, target, "/loki/api/v1/detected_fields", params, "0", nil)
		var resp map[string]json.RawMessage
		if status != http.StatusOK || json.Unmarshal(body, &resp) != nil {
			t.Fatalf("detected_fields %s %s: %d %.300s", target, query, status, body)
		}
		var fields []detectedFieldEntry
		if raw, ok := resp["fields"]; ok {
			_ = json.Unmarshal(raw, &fields)
		}
		_, limit := resp["limit"]
		var labels []string
		for _, f := range fields {
			if !strings.HasSuffix(f.Label, "_extracted") {
				labels = append(labels, f.Label)
			}
		}
		sort.Strings(labels)
		return answer{fields: len(fields), labels: strings.Join(labels, ","), limit: limit, raw: string(body)}
	}

	// Both sides must hold the lines first: Loki lists the JSON lines' fields
	// with the limit (it answers from its ingesters, so wait for them). Then
	// the matching control query answers alike on both sides, field names and
	// whether a limit is sent, for every kind of line.
	jsonControl := fmt.Sprintf(`{service_name="%s"} | detected_level="error"`+tail, svc+"-json")
	deadline := time.Now().Add(60 * time.Second)
	for {
		loki := ask(t, lokiURL, jsonControl)
		if loki.fields > 0 && loki.limit {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Loki never listed the fields of %s: %s", jsonControl, loki.raw)
		}
		time.Sleep(2 * time.Second)
	}
	for _, kind := range []string{"json", "logfmt", "plain"} {
		control := fmt.Sprintf(`{service_name="%s"} | detected_level="error"`+tail, svc+"-"+kind)
		loki := ask(t, lokiURL, control)
		if got := ask(t, proxyURL, control); got.labels != loki.labels || got.limit != loki.limit {
			t.Errorf("control %s:\nproxy %s\nloki  %s", control, got.raw, loki.raw)
		}
	}

	cases := []struct{ kind, query string }{
		// The Drilldown level pills error and warn: the escaped pipe is a
		// literal '|' in the regexp, so no level matches.
		{"json", `{service_name="%s"} | detected_level=~"error\\|warn"` + tail},
		{"json", `{service_name="%s"} | detected_level="debug"` + tail},
		{"json", `{service_name="%s"}` + tail + `| method="DELETE"`},
		{"json", `{service_name="%s"} |= "no-such-text"` + tail},
		{"logfmt", `{service_name="%s"} | detected_level="debug"` + tail},
		{"plain", `{service_name="%s"} | detected_level="debug"` + tail},
	}
	for _, tc := range cases {
		query := fmt.Sprintf(tc.query, svc+"-"+tc.kind)
		loki := ask(t, lokiURL, query)
		if loki.fields != 0 || loki.limit {
			t.Fatalf("Loki listed fields for a query that matches no line %s: %s", query, loki.raw)
		}
		for _, target := range []struct{ name, url string }{
			{"parity (13100)", proxyURL},
			{"drilldown default (13110)", patternsAutodetectProxyURL},
		} {
			if got := ask(t, target.url, query); got.fields != 0 || got.limit {
				t.Errorf("%s %s: want Loki's empty answer %s, got %s", target.name, query, loki.raw, got.raw)
			}
		}
	}
}
