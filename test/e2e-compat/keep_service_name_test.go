//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// conformance: profiles/keep-removes-derived-service-name, loki_api_v1_query_range
func TestCompat_KeepRemovesDerivedServiceName(t *testing.T) {
	probe := fmt.Sprintf("keep-service-%d", time.Now().UnixNano())
	stamp := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	line := `{"level":"info","message":"keep marker"}`
	labels := map[string]string{"app": "checkout", "probe": probe}
	row, _ := json.Marshal(map[string]string{
		"_time": stamp.UTC().Format(time.RFC3339Nano), "_msg": line,
		"app": "checkout", "probe": probe,
	})
	status, body := hardeningRequest(t, http.MethodPost,
		vlURL+"/insert/jsonline?_stream_fields=app,probe", string(row)+"\n",
		map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VictoriaLogs ingest: %d %s", status, body)
	}
	push, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{
		"stream": labels, "values": [][]string{{strconv.FormatInt(stamp.UnixNano(), 10), line}},
	}}})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(push),
		map[string]string{"Content-Type": "application/json"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)

	params := url.Values{
		"query": {fmt.Sprintf(`{probe=%q} | json | keep level`, probe)},
		"start": {strconv.FormatInt(stamp.Add(-time.Minute).UnixNano(), 10)},
		"end":   {strconv.FormatInt(stamp.Add(time.Minute).UnixNano(), 10)},
		"limit": {"10"},
	}
	for _, categorized := range []bool{false, true} {
		headers := map[string]string{"X-Scope-OrgID": "0"}
		if categorized {
			headers["X-Loki-Response-Encoding-Flags"] = "categorize-labels"
		}
		var lokiResult, proxyResult []interface{}
		deadline := time.Now().Add(60 * time.Second)
		for {
			ls, lb, lp := queryRangeGET(t, lokiURL, params, headers)
			ps, pb, pp := queryRangeGET(t, proxyURL, params, headers)
			if ls != http.StatusOK || ps != http.StatusOK {
				t.Fatalf("query_range Loki=%d %s proxy=%d %s", ls, lb, ps, pb)
			}
			lokiResult = extractArray(extractMap(lp, "data"), "result")
			proxyResult = extractArray(extractMap(pp, "data"), "result")
			if len(lokiResult) == 1 && len(proxyResult) == 1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("fixture not visible on both sides: Loki=%s proxy=%s", lb, pb)
			}
			time.Sleep(time.Second)
		}
		want := map[string]interface{}{"level": "info"}
		if categorized {
			want = map[string]interface{}{}
		}
		loki := lokiResult[0].(map[string]interface{})
		proxy := proxyResult[0].(map[string]interface{})
		if !reflect.DeepEqual(loki["stream"], want) {
			t.Fatalf("categorized=%v Loki labels=%v, expected fixture labels=%v", categorized, loki["stream"], want)
		}
		if !reflect.DeepEqual(proxy["stream"], loki["stream"]) || !reflect.DeepEqual(proxy["values"], loki["values"]) {
			t.Fatalf("categorized=%v proxy=%v Loki=%v", categorized, proxy, loki)
		}
	}
}

// TestCompat_KeepDropsUnlistedLabels: Loki's keep stage keeps only the labels
// it names (pkg/logql/log/keep_labels.go:15-45). A JSON or logfmt key named
// like a stream label is name_extracted and is gone after `| keep name`; under
// a vector aggregation every stream keeps the same labels, so `count(rate(...
// | keep foo [5m]))` is the number of kept label sets (1 when no line has
// foo). The fixture is three streams with a level label, JSON and logfmt
// lines each holding level, ingested alike into Loki and VictoriaLogs; the
// metrics read the JSON streams only (a logfmt line is a JSON parser error,
// which keep never removes and which fails a Loki metric).
// conformance: semantics/keep-leaves-labels-loki-drops, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_KeepDropsUnlistedLabels(t *testing.T) {
	probe := fmt.Sprintf("keep-drops-%d", time.Now().UnixNano())
	stamp := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	type fixture struct {
		pod, line string
	}
	fixtures := []fixture{
		{"p1", `{"level":"error","msg":"boom"}`},
		{"p2", `level=error msg="payment failed"`},
		{"p3", `{"level":"warn","foo":"x"}`},
	}
	var vlRows []string
	var lokiStreams []any
	for i, f := range fixtures {
		ts := stamp.Add(time.Duration(i) * time.Second)
		row, _ := json.Marshal(map[string]string{
			"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": f.line,
			"app": "keep-drops", "probe": probe, "pod": f.pod, "level": "error",
		})
		vlRows = append(vlRows, string(row))
		lokiStreams = append(lokiStreams, map[string]any{
			"stream": map[string]string{"app": "keep-drops", "probe": probe, "pod": f.pod, "level": "error"},
			"values": [][]string{{strconv.FormatInt(ts.UnixNano(), 10), f.line}},
		})
	}
	status, body := hardeningRequest(t, http.MethodPost,
		vlURL+"/insert/jsonline?_stream_fields=app,probe,pod,level", strings.Join(vlRows, "\n")+"\n",
		map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VictoriaLogs ingest: %d %s", status, body)
	}
	push, _ := json.Marshal(map[string]any{"streams": lokiStreams})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(push),
		map[string]string{"Content-Type": "application/json"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)
	window := url.Values{
		"start": {strconv.FormatInt(stamp.Add(-time.Minute).UnixNano(), 10)},
		"end":   {strconv.FormatInt(stamp.Add(time.Minute).UnixNano(), 10)},
	}

	logQueries := []struct {
		query string
		want  int
	}{
		{fmt.Sprintf(`{probe=%q, pod=~"p1|p3"} | json | keep level`, probe), 2},
		{fmt.Sprintf(`{probe=%q, pod="p2"} | logfmt | keep level, msg`, probe), 1},
		{fmt.Sprintf(`{probe=%q, pod="p2"} | logfmt | level="error" | keep level, msg`, probe), 1},
	}
	for _, lq := range logQueries {
		params := url.Values{"query": {lq.query}, "start": window["start"], "end": window["end"], "limit": {"10"}}
		for _, categorized := range []bool{false, true} {
			headers := map[string]string{"X-Scope-OrgID": "0"}
			if categorized {
				headers["X-Loki-Response-Encoding-Flags"] = "categorize-labels"
			}
			var lokiResult, proxyResult []interface{}
			deadline := time.Now().Add(60 * time.Second)
			for {
				ls, lb, lp := queryRangeGET(t, lokiURL, params, headers)
				ps, pb, pp := queryRangeGET(t, proxyURL, params, headers)
				if ls != http.StatusOK || ps != http.StatusOK {
					t.Fatalf("%s: Loki=%d %s proxy=%d %s", lq.query, ls, lb, ps, pb)
				}
				lokiResult = extractArray(extractMap(lp, "data"), "result")
				proxyResult = extractArray(extractMap(pp, "data"), "result")
				if countStreamEntries(lokiResult) >= lq.want && countStreamEntries(proxyResult) >= lq.want {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("%s: fixture not visible on both sides: Loki=%s proxy=%s", lq.query, lb, pb)
				}
				time.Sleep(time.Second)
			}
			if !reflect.DeepEqual(entryLabelSets(proxyResult), entryLabelSets(lokiResult)) {
				t.Errorf("%s categorized=%v:\nproxy %v\n Loki %v", lq.query, categorized, entryLabelSets(proxyResult), entryLabelSets(lokiResult))
			}
		}
	}

	waitForLokiMetricDataSelector(t, fmt.Sprintf(`{probe=%q}`, probe))
	for _, keep := range []string{"foo", "pod", "level", "zzz"} {
		query := fmt.Sprintf(`count(rate({probe=%q, pod=~"p1|p3"} | json | keep %s [5m]))`, probe, keep)
		rangeParams := url.Values{"query": {query}, "start": window["start"], "end": {strconv.FormatInt(stamp.Add(4*time.Minute).UnixNano(), 10)}, "step": {"60"}}
		_, lb, lp := queryRangeGET(t, lokiURL, rangeParams, map[string]string{"X-Scope-OrgID": "0"})
		_, pb, pp := queryRangeGET(t, proxyURL, rangeParams, map[string]string{"X-Scope-OrgID": "0"})
		lokiSeries := extractArray(extractMap(lp, "data"), "result")
		if len(lokiSeries) == 0 {
			t.Fatalf("%s: Loki answered no series: %s", query, lb)
		}
		if !reflect.DeepEqual(extractArray(extractMap(pp, "data"), "result"), lokiSeries) {
			t.Errorf("query_range %s:\nproxy %s\n Loki %s", query, pb, lb)
		}
		instant := url.Values{"query": {query}, "time": {strconv.FormatInt(stamp.Add(time.Minute).UnixNano(), 10)}}
		ls, lb, lpi := doJSONGET(t, lokiURL+"/loki/api/v1/query?"+instant.Encode(), map[string]string{"X-Scope-OrgID": "0"})
		ps, pb, ppi := doJSONGET(t, proxyURL+"/loki/api/v1/query?"+instant.Encode(), map[string]string{"X-Scope-OrgID": "0"})
		if ls != http.StatusOK || ps != http.StatusOK {
			t.Fatalf("query %s: Loki=%d %s proxy=%d %s", query, ls, lb, ps, pb)
		}
		if !reflect.DeepEqual(extractArray(extractMap(ppi, "data"), "result"), extractArray(extractMap(lpi, "data"), "result")) {
			t.Errorf("query %s:\nproxy %s\n Loki %s", query, pb, lb)
		}
	}
}

// countStreamEntries is the number of entries of a streams result.
func countStreamEntries(result []interface{}) int {
	n := 0
	for _, s := range result {
		values, _ := s.(map[string]interface{})["values"].([]interface{})
		n += len(values)
	}
	return n
}

// entryLabelSets is, per entry, its stream labels and the categorized
// metadata it carries (the third tuple element), sorted.
func entryLabelSets(result []interface{}) []string {
	var out []string
	for _, s := range result {
		stream := s.(map[string]interface{})
		labels, _ := json.Marshal(stream["stream"])
		values, _ := stream["values"].([]interface{})
		for _, v := range values {
			tuple, _ := v.([]interface{})
			meta := ""
			if len(tuple) > 2 {
				b, _ := json.Marshal(tuple[2])
				meta = string(b)
			}
			out = append(out, string(labels)+" "+meta)
		}
	}
	sort.Strings(out)
	return out
}
