//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"
)

func TestHardeningLive_BinaryLiteralRoundTrip(t *testing.T) {
	service := fmt.Sprintf("hardening-binary-literals-%d", time.Now().UnixNano())
	evaluation := time.Now().Add(-4 * time.Minute).Truncate(time.Minute)
	path := "quote\"slash\\"
	unicodeLiteral := "raw\rseparator\u2028"
	ingest := func(labels map[string]string, line string, stamp time.Time) {
		t.Helper()
		record := make(map[string]string, len(labels)+2)
		for name, value := range labels {
			record[name] = value
		}
		record["_time"], record["_msg"] = stamp.UTC().Format(time.RFC3339Nano), line
		row, _ := json.Marshal(record)
		status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=service_name,path,kind,unicode", string(row)+"\n", map[string]string{"Content-Type": "application/stream+json"})
		if status != 200 {
			t.Fatalf("VL ingest: %d %s", status, body)
		}
		payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{"stream": labels, "values": [][]string{{strconv.FormatInt(stamp.UnixNano(), 10), line}}}}})
		status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json"})
		if status != 204 {
			t.Fatalf("Loki ingest: %d %s", status, body)
		}
	}
	for i, suffix := range []string{"address=10.0.0.1", "ip(10.0.0.1)"} {
		stamp := evaluation.Add(time.Duration(-30+i) * time.Second)
		kind := strconv.Itoa(i)
		line := unicodeLiteral + " say \"hello\" C:\\logs\\ " + suffix + " tick`value"
		labels := map[string]string{"service_name": service, "path": path, "kind": kind, "unicode": unicodeLiteral}
		ingest(labels, line, stamp)
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/internal/force_flush", "", nil)
	if status != 200 {
		t.Fatalf("VL flush: %d %s", status, body)
	}
	selector := `{service_name="` + service + `"}`
	waitForParserErrorCanaryMetrics(t, selector, evaluation)
	metric := func(selector, pipeline string) string {
		return "sum(count_over_time(" + selector + pipeline + "[5m])) + on() vector(0)"
	}
	without := "sum without()(count_over_time(" + selector + "[5m]))"
	for _, tc := range []struct {
		name, query string
		want        float64
		grouped     bool
	}{
		{"selector_escapes", metric(`{service_name="`+service+`",path=`+strconv.Quote(path)+`}`, ""), 2, false},
		{"quoted_literal", metric(selector, `|= "say \"hello\""`), 2, false},
		{"backslash_literal", metric(selector, `|= "C:\\logs\\"`), 2, false},
		{"ip_call", metric(selector, `|= ip("10.0.0.1")`), 2, false},
		{"ip_literal", metric(selector, `|= "ip(10.0.0.1)"`), 1, false},
		{"ip_shaped_label_literal", metric(selector, `|label_format cap="ip(\"10.0.0.1\")"|cap="ip(\"10.0.0.1\")"`), 2, false},
		{"raw_unicode_literal", metric(selector, "|= `"+unicodeLiteral+"`"), 2, false},
		{"escaped_unicode_literal", metric(selector, "|= "+strconv.Quote(unicodeLiteral)), 2, false},
		{"raw_unicode_selector", metric(`{service_name="`+service+"\",unicode=`"+unicodeLiteral+"`}", ""), 2, false},
		{"regexp_backtick", metric(selector, "|regexp "+strconv.Quote("(?P<cap>tick`value)")+"|cap="+strconv.Quote("tick`value")), 2, false},
		{"regexp_quoted_filter", metric(selector, "|regexp "+strconv.Quote(`(?P<cap>say "hello")`)+"|cap="+strconv.Quote(`say "hello"`)), 2, false},
		{"label_template", metric(selector, "|label_format cap="+strconv.Quote("say \"hello\", tick`value")+"|cap="+strconv.Quote("say \"hello\", tick`value")), 2, false},
		{"pattern_backtick", metric(selector, "|pattern "+strconv.Quote("<_>tick`<cap>")+`|cap="value"`), 2, false},
		{"empty_by", "sum by()(count_over_time(" + selector + "[5m])) + on() vector(0)", 2, false},
		{"empty_without", without + " + on(kind) " + without, 2, true},
	} {
		for _, mode := range []string{"query", "query_range"} {
			for _, backend := range []struct{ name, base string }{{"loki", lokiURL}, {"proxy", proxyURL}} {
				t.Run(tc.name+"/"+mode+"/"+backend.name, func(t *testing.T) {
					params := parserErrorQueryParams(tc.query, evaluation, mode)
					status, body := hardeningRequest(t, http.MethodGet, backend.base+"/loki/api/v1/"+mode+"?"+params.Encode(), "", nil)
					t.Logf("params=%s status=%d body=%s", params.Encode(), status, body)
					var response parserErrorMetricResponse
					wantSeries := 1
					if tc.grouped {
						wantSeries = 2
					}
					if status != 200 || json.Unmarshal(body, &response) != nil || response.Status != "success" || len(response.Data.Result) != wantSeries {
						t.Fatalf("want %d successful series: %d %s", wantSeries, status, body)
					}
					seen := make(map[string]bool)
					for _, series := range response.Data.Result {
						if tc.grouped {
							kind := series.Metric["kind"]
							if len(series.Metric) != 1 || (kind != "0" && kind != "1") || seen[kind] {
								t.Fatalf("wrong grouped labels: %s", body)
							}
							seen[kind] = true
						} else if len(series.Metric) != 0 {
							t.Fatalf("unexpected labels: %s", body)
						}
						points, wantPoints := series.Values, 2
						if mode == "query" {
							points, wantPoints = [][]json.RawMessage{series.Value}, 1
						}
						if len(points) != wantPoints {
							t.Fatalf("wrong samples: %s", body)
						}
						for i, point := range points {
							assertParserErrorMetricPoint(t, point, evaluation.Add(time.Duration(i)*time.Minute).Unix(), tc.want)
						}
					}
				})
			}
		}
	}
}
