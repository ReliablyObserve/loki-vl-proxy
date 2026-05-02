package proxy

import (
	"encoding/json"
	"testing"
)

// hitsJSON builds a minimal VL /hits response with the given fields.
func hitsJSON(fields map[string]string) []byte {
	type hitsPayload struct {
		Hits []struct {
			Fields     map[string]string `json:"fields"`
			Timestamps []string          `json:"timestamps"`
			Values     []int             `json:"values"`
		} `json:"hits"`
	}
	p := hitsPayload{}
	p.Hits = append(p.Hits, struct {
		Fields     map[string]string `json:"fields"`
		Timestamps []string          `json:"timestamps"`
		Values     []int             `json:"values"`
	}{
		Fields:     fields,
		Timestamps: []string{"2026-05-01T00:00:00Z"},
		Values:     []int{42},
	})
	b, _ := json.Marshal(p)
	return b
}

func extractVectorMetric(t *testing.T, result map[string]interface{}) map[string]string {
	t.Helper()
	data := result["data"].(map[string]interface{})
	rows := data["result"].([]map[string]interface{})
	if len(rows) == 0 {
		t.Fatal("empty result rows")
	}
	return rows[0]["metric"].(map[string]string)
}

func TestHitsToVolumeVector_FiltersToTargetLabels(t *testing.T) {
	p := newTestProxy(t, "http://unused")

	body := hitsJSON(map[string]string{
		"container":    "api",
		"service_name": "my-service",
		"namespace":    "prod",
	})

	result := p.hitsToVolumeVector(body, "container")
	metric := extractVectorMetric(t, result)

	if _, ok := metric["container"]; !ok {
		t.Error("expected container key in metric")
	}
	if _, ok := metric["service_name"]; ok {
		t.Error("service_name must not leak into container-only metric")
	}
	if _, ok := metric["namespace"]; ok {
		t.Error("namespace must not leak into container-only metric")
	}
}

func TestHitsToVolumeVector_NoTargetLabels_ReturnsAll(t *testing.T) {
	p := newTestProxy(t, "http://unused")

	body := hitsJSON(map[string]string{
		"container":    "api",
		"service_name": "my-service",
	})

	result := p.hitsToVolumeVector(body, "")
	metric := extractVectorMetric(t, result)

	if _, ok := metric["container"]; !ok {
		t.Error("expected container when no targetLabels filter")
	}
	if _, ok := metric["service_name"]; !ok {
		t.Error("expected service_name when no targetLabels filter")
	}
}

func TestHitsToVolumeMatrix_FiltersToTargetLabels(t *testing.T) {
	p := newTestProxy(t, "http://unused")

	body := hitsJSON(map[string]string{
		"container":    "api",
		"service_name": "my-service",
	})

	result := p.hitsToVolumeMatrix(body, "container", "1746057600", "1746061200", "1h")
	data := result["data"].(map[string]interface{})
	rows := data["result"].([]map[string]interface{})
	if len(rows) == 0 {
		t.Fatal("empty matrix result")
	}
	metric := rows[0]["metric"].(map[string]string)
	if _, ok := metric["container"]; !ok {
		t.Error("expected container key in matrix metric")
	}
	if _, ok := metric["service_name"]; ok {
		t.Error("service_name must not leak into container-only matrix metric")
	}
}
