//go:build e2e

package e2e_pipeline

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// ingestPipelineData pushes ~10k log lines across 5 services into VictoriaLogs
// via the proxy's Loki push endpoint. Data spans 30 minutes so query_range
// returns meaningful sample series across multiple steps.
func ingestPipelineData(t *testing.T) {
	t.Helper()

	// Anchor 30 minutes in the past so all entries are fully indexed.
	base := time.Now().Add(-32 * time.Minute)

	services := []struct {
		name   string
		format string // "json" or "logfmt"
		count  int
	}{
		{"api-gw", "json", 2500},
		{"auth", "json", 2000},
		{"worker", "logfmt", 2000},
		{"cache", "logfmt", 1500},
		{"db", "json", 2000},
	}

	for _, svc := range services {
		pushServiceLogs(t, base, svc.name, svc.format, svc.count)
	}

	// Allow VL to index before queries run.
	time.Sleep(3 * time.Second)
	t.Logf("pipeline test data ingested: ~10k lines across 5 services")
}

func pushServiceLogs(t *testing.T, base time.Time, service, format string, count int) {
	t.Helper()

	labels := map[string]string{
		"service_name": service,
		"namespace":    "prod",
		"env":          "e2e",
	}

	// Spread entries across 30 minutes so query_range has non-trivial sample data.
	interval := 30 * time.Minute / time.Duration(count)
	levels := []string{"info", "info", "info", "warn", "error"}
	statuses := []int{200, 200, 201, 400, 500}
	latencies := []float64{12.5, 45.3, 8.1, 320.7, 5023.4}

	// Build lines in batches of 500 to avoid huge single pushes.
	const batchSize = 500
	for batchStart := 0; batchStart < count; batchStart += batchSize {
		end := batchStart + batchSize
		if end > count {
			end = count
		}

		var lines []string
		var timestamps []string

		for i := batchStart; i < end; i++ {
			ts := base.Add(time.Duration(i) * interval)
			timestamps = append(timestamps, fmt.Sprintf("%d", ts.UnixNano()))

			idx := i % len(levels)
			switch format {
			case "json":
				lines = append(lines, fmt.Sprintf(
					`{"level":%q,"method":"GET","path":"/api/v%d/items","status":%d,"duration_ms":%.1f,"trace_id":"tr%08d","service":%q}`,
					levels[idx], 1+(i%3), statuses[idx], latencies[idx], i, service,
				))
			default: // logfmt
				lines = append(lines, fmt.Sprintf(
					`level=%s method=GET path=/api/v%d/items status=%d duration_ms=%.1f trace_id=tr%08d service=%s`,
					levels[idx], 1+(i%3), statuses[idx], latencies[idx], i, service,
				))
			}
		}

		pushViaProxy(t, labels, timestamps, lines)
	}
}

func pushViaProxy(t *testing.T, labels map[string]string, timestamps, lines []string) {
	t.Helper()

	values := make([]interface{}, len(lines))
	for i, line := range lines {
		values[i] = []string{timestamps[i], line}
	}

	payload := map[string]interface{}{
		"streams": []map[string]interface{}{
			{"stream": labels, "values": values},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal push payload: %v", err)
	}

	resp, err := http.Post(proxyURL+"/loki/api/v1/push", "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("push to proxy: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		t.Fatalf("push returned %d", resp.StatusCode)
	}
}
