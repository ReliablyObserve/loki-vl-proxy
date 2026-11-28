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

// The checks of the stream grouped sum rule read two VictoriaLogs behaviours (vl-stream-grouped-label-checks): a
// stream filter `{k=~".+"}` selects the streams that hold k with a value, so `NOT {k=~".+"}` finds a row of a stream
// without it; and a row whose stream holds k="v" reads k:="v" as its field, unless the row carries a regular field
// of the same name (structured metadata here), which the field filter then reads. A release that changed either would
// let the rule serve a cut query's answer where Loki groups otherwise, so it must fail here, on every version of the
// matrix.
//
// conformance: vl-stream-grouped-label-checks, semantics/stream-grouped-sum-skips-parsers
func TestVLCapability_StreamGroupedLabelChecks(t *testing.T) {
	app := fmt.Sprintf("vlsgcheck%d", time.Now().UnixNano())
	stamp := time.Now().Add(-2 * time.Minute).UTC()
	ts := func(i int) string { return strconv.FormatInt(stamp.Add(time.Duration(i)*time.Second).UnixNano(), 10) }
	push := map[string]any{"streams": []map[string]any{
		{"stream": map[string]string{"app": app, "job": "labelled", "level": "info"}, "values": [][]any{{ts(0), "a"}}},
		{"stream": map[string]string{"app": app, "job": "unlabelled"}, "values": [][]any{{ts(1), "b"}}},
		{"stream": map[string]string{"app": app, "job": "shadowed", "level": "info"}, "values": [][]any{{ts(2), "c", map[string]string{"level": "meta"}}}},
	}}
	body, _ := json.Marshal(push)
	if status, out := hardeningRequest(t, http.MethodPost, vlURL+"/insert/loki/api/v1/push?disable_message_parsing=1", string(body), map[string]string{"Content-Type": "application/json"}); status/100 != 2 {
		t.Fatalf("VictoriaLogs push: %d %s", status, out)
	}
	forceVLFlush(t)
	sel := `app:="` + app + `"`
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, msgs := vlLogsqlQuery(t, sel, ""); len(msgs) == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pushed rows did not become searchable")
		}
		time.Sleep(500 * time.Millisecond)
	}
	for _, c := range []struct {
		job, filter string
		want        []string
	}{
		{"labelled", `NOT {level=~".+"}`, nil},
		{"unlabelled", `NOT {level=~".+"}`, []string{"b"}},
		{"shadowed", `NOT {level=~".+"}`, nil},
		{"labelled", `NOT (({level="info"} AND level:="info"))`, nil},
		{"shadowed", `NOT (({level="info"} AND level:="info"))`, []string{"c"}},
		{"unlabelled", `NOT (({level="info"} AND level:="info"))`, []string{"b"}},
	} {
		query := `(` + sel + ` job:="` + c.job + `") AND (` + c.filter + `)`
		status, msgs := vlLogsqlQuery(t, query, "")
		if status != http.StatusOK || fmt.Sprint(msgs) != fmt.Sprint(c.want) {
			t.Errorf("%s: status %d, lines %v, want 200 and %v", query, status, msgs, c.want)
		}
	}
}
