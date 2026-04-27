package proxy

import (
	"encoding/json"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

func matrixBody(t *testing.T, series []map[string]interface{}) []byte {
	t.Helper()
	body := map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "matrix",
			"result":     series,
		},
	}
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func parseMatrix(t *testing.T, body []byte) []map[string]string {
	t.Helper()
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("parseMatrix: %v", err)
	}
	out := make([]map[string]string, len(resp.Data.Result))
	for i, r := range resp.Data.Result {
		out[i] = r.Metric
	}
	return out
}

func parseMatrixValues(t *testing.T, body []byte) [][]interface{} {
	t.Helper()
	var resp struct {
		Data struct {
			Result []struct {
				Values [][]interface{} `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("parseMatrixValues: %v", err)
	}
	if len(resp.Data.Result) == 0 {
		return nil
	}
	return resp.Data.Result[0].Values
}

func TestApplyGroupNormalization(t *testing.T) {
	body := matrixBody(t, []map[string]interface{}{
		{
			"metric": map[string]string{"level": "error"},
			"values": [][]interface{}{{"1234567890", "42"}, {"1234567891", "7"}},
		},
		{
			"metric": map[string]string{"level": "warn"},
			"values": [][]interface{}{{"1234567890", "3"}},
		},
	})

	result := applyGroupNormalization(body)

	vals := parseMatrixValues(t, result)
	for _, v := range vals {
		if len(v) < 2 || v[1] != "1" {
			t.Errorf("expected value 1, got %v", v)
		}
	}
}

func TestApplyGroupNormalization_NonMatrix(t *testing.T) {
	body := []byte(`{"status":"success","data":{"resultType":"streams","result":[]}}`)
	out := applyGroupNormalization(body)
	if string(out) != string(body) {
		t.Error("non-matrix body should be returned unchanged")
	}
}

func TestApplyLabelReplace_BasicMatch(t *testing.T) {
	body := matrixBody(t, []map[string]interface{}{
		{"metric": map[string]string{"instance": "host1:9090"}, "values": [][]interface{}{{"1", "1"}}},
		{"metric": map[string]string{"instance": "host2:8080"}, "values": [][]interface{}{{"1", "1"}}},
	})

	spec := translator.LabelReplaceSpec{
		DstLabel:    "host",
		Replacement: "$1",
		SrcLabel:    "instance",
		Regex:       `(.+):\d+`,
	}
	result := applyLabelReplace(body, spec)
	metrics := parseMatrix(t, result)

	if metrics[0]["host"] != "host1" {
		t.Errorf("series 0: host=%q want host1", metrics[0]["host"])
	}
	if metrics[1]["host"] != "host2" {
		t.Errorf("series 1: host=%q want host2", metrics[1]["host"])
	}
}

func TestApplyLabelReplace_NoMatch_KeepsDst(t *testing.T) {
	// Prometheus semantics: if regex doesn't match, dst_label is left unchanged.
	body := matrixBody(t, []map[string]interface{}{
		{"metric": map[string]string{"instance": "no-port", "host": "old"}, "values": [][]interface{}{{"1", "1"}}},
	})

	spec := translator.LabelReplaceSpec{
		DstLabel:    "host",
		Replacement: "$1",
		SrcLabel:    "instance",
		Regex:       `(.+):\d+`, // requires a colon — won't match "no-port"
	}
	result := applyLabelReplace(body, spec)
	metrics := parseMatrix(t, result)

	if metrics[0]["host"] != "old" {
		t.Errorf("host label should be unchanged on no-match, got %q", metrics[0]["host"])
	}
}

func TestApplyLabelJoin(t *testing.T) {
	body := matrixBody(t, []map[string]interface{}{
		{"metric": map[string]string{"service": "api", "host": "node1"}, "values": [][]interface{}{{"1", "1"}}},
	})

	spec := translator.LabelJoinSpec{
		DstLabel:  "service_host",
		Separator: "/",
		SrcLabels: []string{"service", "host"},
	}
	result := applyLabelJoin(body, spec)
	metrics := parseMatrix(t, result)

	if metrics[0]["service_host"] != "api/node1" {
		t.Errorf("service_host=%q want api/node1", metrics[0]["service_host"])
	}
}

func TestApplyLabelJoin_MissingSrc(t *testing.T) {
	body := matrixBody(t, []map[string]interface{}{
		{"metric": map[string]string{"service": "api"}, "values": [][]interface{}{{"1", "1"}}},
	})

	spec := translator.LabelJoinSpec{
		DstLabel:  "service_host",
		Separator: "/",
		SrcLabels: []string{"service", "host"}, // host is missing
	}
	result := applyLabelJoin(body, spec)
	metrics := parseMatrix(t, result)

	// Missing labels are skipped, so only "api" appears (no trailing separator)
	if metrics[0]["service_host"] != "api" {
		t.Errorf("service_host=%q want api", metrics[0]["service_host"])
	}
}
