//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"
)

// patternMinableLinesFilter is the proxy's patternMinableLines as LogsQL (TestPatternQueriesLeaveOutJSONLinesWithoutAMessage
// pins the same text on the proxy's side).
const patternMinableLinesFilter = `-(_msg:="{"* _msg:~"\\}$" -("\"log\"" OR "\"message\"" OR "\"msg\"" OR "\"msg_\"" OR "\"_msg\"" OR "\"content\""))`

// The patterns flow reads a selector's lines with two VictoriaLogs behaviours it relies on: a phrase filter of a
// quoted name ("\"msg\"") matches the name with its quotes anywhere in the line, so the filter that leaves out
// JSON lines without a message field keeps every line holding a message field's key and drops one without; and a
// sort by stream, time and text returns a stream's lines that share a time in the same order on every read, so
// the lines are mined alike on every read, rebuild and peer. The name puts it in the vl-pinned check (every pull
// request) and the VictoriaLogs version matrix.
//
// conformance: vl-phrase-filter-quoted-names, vl-sort-ties-by-text
func TestVLCapability_PatternLinesFilterAndTieOrder(t *testing.T) {
	unique := fmt.Sprintf("patlines%d", time.Now().UnixNano())
	// A tenant of its own keeps the rows out of every parity query.
	tenant := map[string]string{"AccountID": "9910023", "ProjectID": "0"}
	stamp := time.Now().Add(-2 * time.Minute).UTC().Truncate(time.Second)

	kept := []string{
		`{"msg":"order shipped"}`,
		`{"msg" : "spaced colon"}`,
		`{"message":"a message"}`,
		`{"log":"a log line\n","stream":"stdout"}`,
		`{"_msg":"underscore key"}`,
		`{"msg_":"trailing underscore key"}`,
		`{"content":"content key"}`,
		`{"data":{"msg":"nested key"}}`,
		`{"kind":"msg"}`, // the name as a value: kept, and mined to nothing by the proxy
		`plain text line with {"a":1} inside`,
		`{"msg":"no closing brace"`,
	}
	dropped := []string{
		`{"a":1}`,
		`{"msgs":"a longer key"}`,
		`{"xmsg":"a key ending in msg"}`,
		`{"Msg":"another case"}`,
	}
	var ties []string
	for i := 0; i < 60; i++ {
		ties = append(ties, fmt.Sprintf("tie %02d at one time", (i*37)%60))
	}

	ingest := func(stream string, lines []string, at func(int) time.Time) {
		var batch strings.Builder
		for i, line := range lines {
			fmt.Fprintf(&batch, `{"_time":%q,"_msg":%q,"app":%q,"kind":%q}`+"\n", at(i).Format(time.RFC3339Nano), line, unique, stream)
		}
		headers := map[string]string{"Content-Type": "application/stream+json"}
		for k, v := range tenant {
			headers[k] = v
		}
		target := vlURL + "/insert/jsonline?_stream_fields=app,kind&_msg_field=_msg&_time_field=_time"
		if status, body := hardeningRequest(t, "POST", target, batch.String(), headers); status < 200 || status >= 300 {
			t.Fatalf("VL ingest %d: %s", status, body)
		}
	}
	all := append(slices.Clone(kept), dropped...)
	ingest("filter", all, func(i int) time.Time { return stamp.Add(time.Duration(i) * time.Millisecond) })
	// The lines that share a time come in three pushes, so they lie in separate parts.
	for part := 0; part < 3; part++ {
		ingest("ties", ties[part*20:(part+1)*20], func(int) time.Time { return stamp })
		if status, body := hardeningRequest(t, "POST", vlURL+"/internal/force_flush", "", nil); status != 200 {
			t.Fatalf("flush %d: %s", status, body)
		}
	}

	read := func(query string) []string {
		t.Helper()
		headers := map[string]string{"Content-Type": "application/x-www-form-urlencoded"}
		for k, v := range tenant {
			headers[k] = v
		}
		form := url.Values{"query": {query}, "start": {stamp.Add(-time.Minute).Format(time.RFC3339Nano)}, "end": {stamp.Add(time.Minute).Format(time.RFC3339Nano)}}.Encode()
		status, body := hardeningRequest(t, "POST", vlURL+"/select/logsql/query", form, headers)
		if status != 200 {
			t.Fatalf("query %d: %.300s", status, body)
		}
		var msgs []string
		for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
			if line == "" {
				continue
			}
			var row map[string]string
			if err := json.Unmarshal([]byte(line), &row); err != nil {
				t.Fatalf("row %q: %v", line, err)
			}
			msgs = append(msgs, row["_msg"])
		}
		return msgs
	}
	for deadline := time.Now().Add(30 * time.Second); len(read(`app:="`+unique+`"`)) < len(all)+len(ties); time.Sleep(500 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("ingested rows did not become searchable")
		}
	}

	got := read(`app:="` + unique + `" kind:="filter" ` + patternMinableLinesFilter + ` | sort by (_msg)`)
	want := slices.Clone(kept)
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("the minable-lines filter kept\n %q\nwant\n %q", got, want)
	}

	sorted := `app:="` + unique + `" kind:="ties" | fields _time, _msg, _stream_id | sort by (_stream_id, _time, _msg)`
	first := read(sorted)
	if len(first) != len(ties) {
		t.Fatalf("read %d of the %d lines that share a time", len(first), len(ties))
	}
	for i := 0; i < 10; i++ {
		if again := read(sorted); !slices.Equal(again, first) {
			t.Fatalf("lines that share a time came in another order on read %d\n %q\nfirst\n %q", i+2, again, first)
		}
	}
}
