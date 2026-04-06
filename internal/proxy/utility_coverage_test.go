package proxy

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"
)

func TestParseInstantVectorTimeVariants(t *testing.T) {
	rfc := "2026-04-06T20:21:22.123456789Z"
	if got := parseInstantVectorTime(rfc); got != time.Date(2026, 4, 6, 20, 21, 22, 123456789, time.UTC).UnixNano() {
		t.Fatalf("unexpected RFC3339Nano timestamp: %d", got)
	}
	if got := parseInstantVectorTime("1712434882"); got != 1712434882*int64(time.Second) {
		t.Fatalf("unexpected integer seconds timestamp: %d", got)
	}
	if got := parseInstantVectorTime("1712434882.5"); got != int64(1712434882.5*float64(time.Second)) {
		t.Fatalf("unexpected float seconds timestamp: %d", got)
	}
	if got := parseInstantVectorTime("1712434882123456789"); got != 1712434882123456789 {
		t.Fatalf("unexpected nanoseconds timestamp: %d", got)
	}
}

func TestApplyScalarOpReverse(t *testing.T) {
	body := []byte(`{"results":[{"metric":{"app":"api"},"values":[[1,"2"],[2,"4"]]}]}`)
	out := applyScalarOpReverse(body, "-", 10, "matrix")

	var resp struct {
		Data struct {
			Results []struct {
				Values [][]interface{} `json:"values"`
			} `json:"results"`
		} `json:"data"`
	}
	if err := json.Unmarshal(out, &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got := resp.Data.Results[0].Values[0][1]; got != "8" {
		t.Fatalf("unexpected first reversed value: %#v", got)
	}
	if got := resp.Data.Results[0].Values[1][1]; got != "6" {
		t.Fatalf("unexpected second reversed value: %#v", got)
	}
}

func TestStreamStringMapVariants(t *testing.T) {
	if got := streamStringMap(map[string]string{"app": "api"}); got["app"] != "api" {
		t.Fatalf("unexpected direct map conversion: %#v", got)
	}
	got := streamStringMap(map[string]interface{}{"app": "api", "count": 3})
	if got["app"] != "api" {
		t.Fatalf("unexpected interface map conversion: %#v", got)
	}
	if _, ok := got["count"]; ok {
		t.Fatalf("expected non-string field to be dropped: %#v", got)
	}
	if got := streamStringMap(42); got != nil {
		t.Fatalf("expected nil for unsupported type, got %#v", got)
	}
}

func TestFormatEntryTimestampVariants(t *testing.T) {
	if got, ok := formatEntryTimestamp("2026-04-06T20:21:22Z"); !ok || got != strconv.FormatInt(time.Date(2026, 4, 6, 20, 21, 22, 0, time.UTC).UnixNano(), 10) {
		t.Fatalf("unexpected RFC3339 timestamp result: %q %v", got, ok)
	}
	if got, ok := formatEntryTimestamp("2026-04-06T20:21:22.123456789Z"); !ok || got != strconv.FormatInt(time.Date(2026, 4, 6, 20, 21, 22, 123456789, time.UTC).UnixNano(), 10) {
		t.Fatalf("unexpected RFC3339Nano timestamp result: %q %v", got, ok)
	}
	if got, ok := formatEntryTimestamp("1712434882123456789"); !ok || got != "1712434882123456789" {
		t.Fatalf("unexpected raw numeric timestamp result: %q %v", got, ok)
	}
	if _, ok := formatEntryTimestamp("not-a-time"); ok {
		t.Fatal("expected invalid timestamp to fail")
	}
}

func TestApplyWithoutMatrix(t *testing.T) {
	body := []byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"api","pod":"a"},"values":[[1,"2"]]}]}}`)
	out := applyWithoutMatrix(body, map[string]bool{"pod": true})

	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(out, &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if _, ok := resp.Data.Result[0].Metric["pod"]; ok {
		t.Fatalf("expected excluded label to be removed: %#v", resp.Data.Result[0].Metric)
	}
}

func TestPostprocessHelpers(t *testing.T) {
	if got := titleCase("hello world"); got != "Hello World" {
		t.Fatalf("unexpected titleCase result: %q", got)
	}
	if !isIPLike("10.20.30.40") {
		t.Fatal("expected dotted quad to be recognized")
	}
	if isIPLike("10.20.30") {
		t.Fatal("expected incomplete IP to be rejected")
	}
}

func TestCombineMetricResults(t *testing.T) {
	left := []byte(`{"results":[{"metric":{"app":"api"},"values":[["1","4"],["2","8"]]}]}`)
	right := []byte(`{"results":[{"metric":{"app":"api"},"values":[["1","2"],["3","10"]]}]}`)
	out := combineMetricResults(left, right, "/", "matrix")

	var resp struct {
		Data struct {
			Results []struct {
				Values [][]interface{} `json:"values"`
			} `json:"results"`
		} `json:"data"`
	}
	if err := json.Unmarshal(out, &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got := resp.Data.Results[0].Values[0][1]; got != "2" {
		t.Fatalf("expected matched point to be divided, got %#v", got)
	}
	if got := resp.Data.Results[0].Values[1][1]; got != "8" {
		t.Fatalf("expected unmatched point to stay unchanged, got %#v", got)
	}
}

func TestDrilldownHelpers(t *testing.T) {
	if _, ok := parseVolumeBoundary(""); ok {
		t.Fatal("expected empty boundary to fail")
	}
	if got, ok := parseVolumeBoundary("1712434882123456789"); !ok || got.UnixNano() != 1712434882123456789 {
		t.Fatalf("unexpected absolute boundary: %v %v", got, ok)
	}
	if got, ok := parseVolumeBoundary("now-1m"); !ok || time.Since(got) < 50*time.Second || time.Since(got) > 70*time.Second {
		t.Fatalf("unexpected relative boundary: %v %v", got, ok)
	}
	if got, ok := parseEntryTime("2026-04-06T20:21:22Z"); !ok || got.UTC().Format(time.RFC3339) != "2026-04-06T20:21:22Z" {
		t.Fatalf("unexpected parsed entry time: %v %v", got, ok)
	}
	floatTS := float64(1712434882123456789)
	if got, ok := parseEntryTime(floatTS); !ok || got.UnixNano() != int64(floatTS) {
		t.Fatalf("unexpected numeric entry time: %v %v", got, ok)
	}
	if got := unifyDetectedType("", "int"); got != "int" {
		t.Fatalf("unexpected unified type: %q", got)
	}
	if got := unifyDetectedType("int", "float"); got != "float" {
		t.Fatalf("unexpected float promotion: %q", got)
	}
	if got := unifyDetectedType("boolean", "string"); got != "string" {
		t.Fatalf("unexpected fallback to string: %q", got)
	}
	if got := formatDetectedValue("ok"); got != "ok" {
		t.Fatalf("unexpected formatted string value: %q", got)
	}
	if got := formatDetectedValue(12.5); got != "12.5" {
		t.Fatalf("unexpected formatted float value: %q", got)
	}
	if got := formatDetectedValue(true); got != "true" {
		t.Fatalf("unexpected formatted bool value: %q", got)
	}
	if got := formatDetectedValue(map[string]interface{}{"path": "/ready"}); got != `{"path":"/ready"}` {
		t.Fatalf("unexpected formatted object value: %q", got)
	}
	if string(mustJSON(map[string]string{"app": "api"})) != `{"app":"api"}` {
		t.Fatalf("unexpected mustJSON output: %s", string(mustJSON(map[string]string{"app": "api"})))
	}
}
