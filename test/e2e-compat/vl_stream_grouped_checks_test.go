//go:build e2e

package e2e_compat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"
)

// The checks of the stream grouped sum rule read two VictoriaLogs behaviours (vl-stream-grouped-label-checks): a
// stream filter `{k=~".+"}` selects the streams that hold k with a value, so `NOT {k=~".+"}` finds a row of a stream
// without it; and a row whose stream holds k="v" reads k:="v" as its field, unless the row carries a regular field
// of the same name (here a key VictoriaLogs' Loki push API unpacked from a JSON line at ingest), which the field
// filter then reads. A release that changed either would let the rule serve a cut query's answer where Loki groups
// otherwise, so it must fail here, on every version of the matrix.
//
// conformance: vl-stream-grouped-label-checks, semantics/stream-grouped-sum-skips-parsers
func TestVLCapability_StreamGroupedLabelChecks(t *testing.T) {
	app := fmt.Sprintf("vlsgcheck%d", time.Now().UnixNano())
	stamp := time.Now().Add(-2 * time.Minute).UTC()
	ts := func(i int) string { return strconv.FormatInt(stamp.Add(time.Duration(i)*time.Second).UnixNano(), 10) }
	push := func(parse bool, streams ...map[string]any) {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"streams": streams})
		target := vlURL + "/insert/loki/api/v1/push"
		if !parse {
			target += "?disable_message_parsing=1"
		}
		if status, out := hardeningRequest(t, http.MethodPost, target, string(body), map[string]string{"Content-Type": "application/json"}); status/100 != 2 {
			t.Fatalf("VictoriaLogs push: %d %s", status, out)
		}
	}
	push(false,
		map[string]any{"stream": map[string]string{"app": app, "job": "labelled", "level": "info"}, "values": [][]any{{ts(0), "a"}}},
		map[string]any{"stream": map[string]string{"app": app, "job": "unlabelled"}, "values": [][]any{{ts(1), "b"}}})
	// Message parsing on: the line's level key is stored over the stream label's value.
	push(true, map[string]any{"stream": map[string]string{"app": app, "job": "shadowed", "level": "info"}, "values": [][]any{{ts(2), `{"level":"meta","m":"c"}`}}})
	forceVLFlush(t)
	sel := `app:="` + app + `"`
	count := func(query string) (int, string) {
		t.Helper()
		form := url.Values{"query": {query + " | stats count() as c"}}
		status, body := hardeningRequest(t, http.MethodPost, vlURL+"/select/logsql/query", form.Encode(), map[string]string{"Content-Type": "application/x-www-form-urlencoded"})
		var row map[string]string
		if status != http.StatusOK || json.Unmarshal(bytes.TrimSpace(body), &row) != nil {
			return -1, fmt.Sprintf("%d %s", status, body)
		}
		n, _ := strconv.Atoi(row["c"])
		return n, ""
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if n, _ := count(sel); n == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pushed rows did not become searchable")
		}
		time.Sleep(500 * time.Millisecond)
	}
	for _, c := range []struct {
		job, filter string
		want        int
	}{
		{"labelled", `NOT {level=~".+"}`, 0},
		{"unlabelled", `NOT {level=~".+"}`, 1},
		{"shadowed", `NOT {level=~".+"}`, 0},
		{"labelled", `NOT (({level="info"} AND level:="info"))`, 0},
		{"shadowed", `NOT (({level="info"} AND level:="info"))`, 1},
		{"unlabelled", `NOT (({level="info"} AND level:="info"))`, 1},
	} {
		query := `(` + sel + ` job:="` + c.job + `") AND (` + c.filter + `)`
		if n, failure := count(query); n != c.want {
			t.Errorf("%s: %d rows (%s), want %d", query, n, failure, c.want)
		}
	}
}
