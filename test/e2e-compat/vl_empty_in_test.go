//go:build e2e

package e2e_compat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"
)

// The proxy's match-no-rows filter (`_stream_id:in()`, an empty in() list) matches no row of a stream that holds
// lines and is not an error, for log and stats queries alike. The proxy answers queries Loki answers with no line
// with it (a detected_level matcher no stream satisfies, a |= text that is not UTF-8), so a VictoriaLogs release
// that matched rows or rejected it must fail here.
//
// conformance: vl-empty-in-matches-nothing, semantics/line-filter-invalid-utf8
func TestVLCapability_EmptyInMatchesNothing(t *testing.T) {
	unique := fmt.Sprintf("emptyin%d", time.Now().UnixNano())
	stamp := time.Now().Add(-2 * time.Minute).UTC()
	for i := 0; i < 2; i++ {
		tenantLabelIngest(t, map[string]string{"_time": stamp.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano), "_msg": fmt.Sprintf("%s-%d", unique, i), "app": unique}, "app", "0")
	}
	forceVLFlush(t)
	selector := `app:="` + unique + `"`
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, msgs := vlLogsqlQuery(t, selector, ""); len(msgs) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ingested rows did not become searchable")
		}
		time.Sleep(500 * time.Millisecond)
	}
	if status, msgs := vlLogsqlQuery(t, selector+" _stream_id:in()", ""); status != http.StatusOK || len(msgs) != 0 {
		t.Fatalf("log query: status %d, lines %v, want 200 and no line", status, msgs)
	}
	form := url.Values{"query": {selector + " _stream_id:in() | stats count() as c"}}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/select/logsql/query", form.Encode(), map[string]string{"Content-Type": "application/x-www-form-urlencoded"})
	var row map[string]string
	if status != http.StatusOK || json.Unmarshal(bytes.TrimSpace(body), &row) != nil || row["c"] != "0" {
		t.Fatalf("stats query: status %d, body %q, want 200 and c=0", status, body)
	}
}
