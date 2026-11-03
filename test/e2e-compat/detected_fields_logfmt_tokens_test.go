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

// TestCompat_DetectedFieldsNoLogfmtTokenFields: the Logs Drilldown Fields
// tab with a level pill (and a field filter behind `| json | logfmt`) lists
// the fields Loki lists. VictoriaLogs' unpack_logfmt makes a field of every
// word of a JSON or plain-text line written with spaces; Loki's logfmt parser
// gives such a line no keys (pkg/logql/log/parser.go:423,
// pkg/logql/log/logfmt/decode.go:78-81), so none of those words is a field.
// Loki names a key that clashes with a stream label <key>_extracted, which
// the proxy omits (semantics/detected-fields-extracted-suffix); the
// comparison leaves those names out.
//
// conformance: semantics/detected-fields-no-logfmt-token-fields, loki_api_v1_detected_fields
func TestCompat_DetectedFieldsNoLogfmtTokenFields(t *testing.T) {
	svc := fmt.Sprintf("dd-tokens-%d", time.Now().UnixNano())
	base := time.Now().Add(-90 * time.Second)
	lines := map[string][]string{}
	for i := 0; i < 30; i++ {
		// json.dumps spacing, as the Drilldown e2e generator writes it.
		lines["json"] = append(lines["json"], fmt.Sprintf(`{"method": "%s", "path": "/api/v1/users/%d", "status": %d, "duration_ms": %d, "region": "%s", "error": "gateway timeout"}`,
			[]string{"GET", "POST"}[i%2], i, []int{500, 502, 503}[i%3], 100+i, []string{"us-east-1", "eu-west-1"}[i%2]))
		lines["logfmt"] = append(lines["logfmt"], fmt.Sprintf(`level=error msg="payment retry scheduled" tx_id=tx%d provider=stripe attempt=%d`, i, i%3))
		lines["plain"] = append(lines["plain"], fmt.Sprintf(`2026-11-03 14:00:%02d UTC [%d] ERROR:  deadlock detected on relation "orders", 500, {retry}`, i%60, 1000+i))
	}
	for kind, values := range lines {
		labels := map[string]string{"service_name": svc + "-" + kind, "level": "error", "env": "dd-tokens"}
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
	cases := []struct {
		kind, query string
	}{
		{"json", `{service_name="%s"} | detected_level="error"` + tail},
		{"json", `{service_name="%s"} | detected_level=~"error|warn"` + tail},
		{"json", `{service_name="%s"}` + tail},
		{"json", `{service_name="%s"}` + tail + `| method="GET"`},
		{"json", `{service_name="%s"}` + tail + `| status!="200"`},
		{"logfmt", `{service_name="%s"} | detected_level="error"` + tail},
		{"logfmt", `{service_name="%s"}` + tail + `| provider="stripe"`},
		{"plain", `{service_name="%s"} | detected_level="error"` + tail},
		{"plain", `{service_name="%s"} | detected_level="error"`},
	}
	window := url.Values{
		"start": {strconv.FormatInt(base.Add(-time.Minute).UnixNano(), 10)},
		"end":   {strconv.FormatInt(time.Now().Add(time.Minute).UnixNano(), 10)},
	}
	labelsOf := func(t *testing.T, target, query string) []string {
		t.Helper()
		params := url.Values{"query": {query}, "start": window["start"], "end": window["end"]}
		status, body := rejectedQueryGet(t, target, "/loki/api/v1/detected_fields", params, "0", nil)
		var resp struct {
			Fields []detectedFieldEntry `json:"fields"`
		}
		if status != http.StatusOK || json.Unmarshal(body, &resp) != nil {
			t.Fatalf("detected_fields %s %s: %d %.300s", target, query, status, body)
		}
		var out []string
		for _, f := range resp.Fields {
			if !strings.HasSuffix(f.Label, "_extracted") {
				out = append(out, f.Label)
			}
		}
		sort.Strings(out)
		return out
	}
	for _, tc := range cases {
		query := fmt.Sprintf(tc.query, svc+"-"+tc.kind)
		// Loki answers from its ingesters; wait until it lists the stream's fields.
		var want []string
		deadline := time.Now().Add(60 * time.Second)
		for {
			want = labelsOf(t, lokiURL, query)
			if len(want) > 1 || tc.kind == "plain" || time.Now().After(deadline) {
				break
			}
			time.Sleep(2 * time.Second)
		}
		for _, target := range []struct{ name, url string }{
			{"parity (13100)", proxyURL},
			{"drilldown default (13110)", patternsAutodetectProxyURL},
		} {
			if got := labelsOf(t, target.url, query); strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("%s %s:\nproxy %v\nloki  %v", target.name, query, got, want)
			}
		}
	}
}
