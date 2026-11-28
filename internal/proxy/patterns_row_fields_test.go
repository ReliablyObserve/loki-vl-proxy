package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// patternRowsKept is a row as VictoriaLogs answers it after the fields pipe of patternRowsQuery.
func patternRowsKept(t *testing.T, row map[string]string) map[string]string {
	t.Helper()
	keep := map[string]bool{}
	for _, f := range patternRowFields {
		keep[strings.Trim(f, `"`)] = true
	}
	out := map[string]string{}
	for k, v := range row {
		if keep[k] {
			out[k] = v
		}
	}
	return out
}

func patternRowsBody(t *testing.T, rows []map[string]string) []byte {
	t.Helper()
	var b bytes.Buffer
	for _, row := range rows {
		line, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		b.Write(line)
		b.WriteByte('\n')
	}
	return b.Bytes()
}

// patternWideRows are rows as wide as the issue's (#1821): an ingress access log with dozens of OTel attributes
// stored as fields, its level in a field, in a stream label, in the line only, or nowhere; messages under _msg or
// only under a fallback field; time under _time or only under a fallback field.
func patternWideRows() []map[string]string {
	base := time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC)
	var rows []map[string]string
	for i := 0; i < 240; i++ {
		ts := base.Add(time.Duration(i) * 7 * time.Second)
		row := map[string]string{
			"_time":      ts.Format(time.RFC3339Nano),
			"_stream":    fmt.Sprintf(`{cluster="c1",container="ingress",namespace="ns1",pod="p%d"}`, i%3),
			"_stream_id": fmt.Sprintf("stream-%d", i%3),
			"_msg":       fmt.Sprintf("GET /api/v%d/orders/%d status=%d took %dms user=u%d", i%2, 1000+i, []int{200, 404, 500}[i%3], 10+i%90, i%7),
		}
		for a := 0; a < 40; a++ {
			row[fmt.Sprintf("attributes.http.attr_%02d", a)] = fmt.Sprintf("value-%d-%d with error words warn", a, i%5)
		}
		switch i % 6 {
		case 0:
			row["level"] = "error"
		case 1:
			row["severity_text"], row["severity_number"] = "Warn", "13"
		case 2:
			row["log.level"] = "debug"
		case 3:
			row["_msg"] = "level=info " + row["_msg"]
		case 4:
			row["_stream"] = fmt.Sprintf(`{cluster="c1",container="ingress",level="warn",pod="p%d"}`, i%3)
			row["level"] = "warn"
		}
		switch i % 40 {
		case 7: // a message under a fallback field only
			delete(row, "_msg")
			row["message"] = fmt.Sprintf("fallback message %d", i%4)
		case 13: // a time under a fallback field only
			row["ts"] = row["_time"]
			delete(row, "_time")
		}
		rows = append(rows, row)
	}
	return rows
}

// The fields pipe of a sampled pattern read keeps every field the row decoder reads: the patterns mined from the
// kept rows are the patterns mined from the whole rows, byte for byte, with the line scan for a level on and off.
//
// conformance: drilldown/patterns-rows-fields, loki_api_v1_patterns
func TestPatternRowFieldsMineAsWholeRows(t *testing.T) {
	rows := patternWideRows()
	var kept []map[string]string
	for _, row := range rows {
		kept = append(kept, patternRowsKept(t, row))
	}
	whole, keptBody := patternRowsBody(t, rows), patternRowsBody(t, kept)
	if len(keptBody)*4 > len(whole) {
		t.Fatalf("kept rows are %d of %d bytes: the fixture is not wide", len(keptBody), len(whole))
	}
	for _, bodyScan := range []bool{true, false} {
		for _, step := range []string{"10s", "60s"} {
			for _, fill := range []bool{false, true} {
				levels := func() logRowLevels { l := defaultLogRowLevels(); l.bodyScan = bodyScan; return l }
				startNs := rows[0]["_time"]
				_ = startNs
				from, to := int64(1795773600e9), int64(1795775700e9)
				want, wantStats, err := extractLogPatternsStreamFilled(bytes.NewReader(whole), step, 1000, levels(), fill, from, to)
				if err != nil {
					t.Fatal(err)
				}
				got, gotStats, err := extractLogPatternsStreamFilled(bytes.NewReader(keptBody), step, 1000, levels(), fill, from, to)
				if err != nil {
					t.Fatal(err)
				}
				if len(want) == 0 {
					t.Fatal("no patterns mined from the fixture")
				}
				wantJSON, _ := json.Marshal(want)
				gotJSON, _ := json.Marshal(got)
				if !bytes.Equal(wantJSON, gotJSON) || wantStats.observedLines != gotStats.observedLines {
					t.Fatalf("bodyScan=%v step=%s fill=%v: kept rows mine\n%s\nwhole rows\n%s", bodyScan, step, fill, gotJSON, wantJSON)
				}
			}
		}
	}
}

// A row VictoriaLogs stores without a message is answered with its placeholder _msg; with the line scan on and no
// level in a kept field, its level is read from the line rebuilt from its fields, and the kept fields rebuild
// another line. That row is the one the fields pipe can mine at another level (the exact tier, which has always
// kept these fields, reads it the same way).
func TestPatternRowFieldsMissingMessageLevel(t *testing.T) {
	row := map[string]string{
		"_time": "2026-11-27T10:00:00Z", "_stream": `{app="a"}`, "_stream_id": "s", "_msg": vlDefaultMsgValue,
		"note": "an error happened",
	}
	levels := defaultLogRowLevels()
	whole, _, _ := extractLogPatternsStreamFilled(bytes.NewReader(patternRowsBody(t, []map[string]string{row})), "60s", 10, levels, false, 0, 0)
	levels = defaultLogRowLevels()
	kept, _, _ := extractLogPatternsStreamFilled(bytes.NewReader(patternRowsBody(t, []map[string]string{patternRowsKept(t, row)})), "60s", 10, levels, false, 0, 0)
	if len(whole) != 1 || len(kept) != 1 {
		t.Fatalf("whole %v kept %v", whole, kept)
	}
	if whole[0]["pattern"] != kept[0]["pattern"] {
		t.Fatalf("pattern %v vs %v", whole[0]["pattern"], kept[0]["pattern"])
	}
	if whole[0]["level"] == kept[0]["level"] {
		t.Fatalf("level %v on both: the case this test names no longer differs", whole[0]["level"])
	}
}

// patternRowsQuery appends the fields pipe to the parsed query and keeps its filter: the filter VictoriaLogs reads
// is the translated query's, node for node, under every profile the translator writes.
//
// conformance: drilldown/patterns-rows-fields, loki_api_v1_patterns
func TestPatternRowsQueryKeepsTheFilter(t *testing.T) {
	queries := []string{
		`{service_name="ingress"}`, `{cluster="c1", namespace="ns1", container="ingress"}`, `{app=~"a|b", env!="dev"}`,
		`{app="x"} |= "error"`, `{a!~"b.*"}`, `{service_name="x", detected_level="error"}`, `{k8s_pod_name="p"}`,
	}
	for _, labelStyle := range []string{"passthrough", "underscores"} {
		p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: cache.New(time.Minute, 100), LogLevel: "error", LabelStyle: LabelStyle(labelStyle)})
		if err != nil {
			t.Fatal(err)
		}
		for _, query := range queries {
			text, err := p.translatePatternQuery(context.Background(), query)
			if err != nil {
				t.Fatalf("%s: %v", query, err)
			}
			want, err := logsql.Parse(text)
			if err != nil {
				t.Fatalf("%s: the translated query %q does not parse: %v", query, text, err)
			}
			got, err := logsql.Parse(patternRowsQuery(text))
			if err != nil {
				t.Fatalf("%s: %v", query, err)
			}
			if !reflect.DeepEqual(got.Filter, want.Filter) || len(got.Pipes) != len(want.Pipes)+1 {
				t.Fatalf("%s: %q read back as %q", query, text, patternRowsQuery(text))
			}
			if last, ok := got.Pipes[len(got.Pipes)-1].(logsql.PipeFields); !ok || !reflect.DeepEqual(last.Labels, patternRowFields) {
				t.Fatalf("%s: last pipe %#v", query, got.Pipes[len(got.Pipes)-1])
			}
		}
		_ = p.Shutdown(context.Background())
	}
	if got := patternRowsQuery("*"); !strings.HasPrefix(got, "* | fields _time, _msg, _stream, _stream_id") {
		t.Fatalf("* read as %q", got)
	}
	if got := patternRowsQuery(`a:="b" |`); got != `a:="b" |` {
		t.Fatalf("an unparsable query is sent as it is, got %q", got)
	}
}
