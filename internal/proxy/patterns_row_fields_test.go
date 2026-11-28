package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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

// patternWideRows are rows as VictoriaLogs answers them, as wide as the issue's (#1821): an ingress access log with
// dozens of OTel attributes stored as fields, its level in a field, in a stream label, in the line only, or nowhere,
// and some with fields named like the decoder's fallback message and time fields (every VictoriaLogs row has _time
// and a non-empty _msg).
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
		if i%40 == 7 { // fields named like the decoder's fallbacks, next to _msg and _time
			row["message"], row["ts"], row["log"] = fmt.Sprintf("fallback message %d", i%4), row["_time"], "x"
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

// The read's query is the stream selector's filter as the translator writes it, byte for byte, then the fields
// pipe, under every label profile; a query that is not a bare stream selector is read as translated.
//
// conformance: drilldown/patterns-rows-fields, loki_api_v1_patterns
func TestPatternRowsQueryKeepsTheFilter(t *testing.T) {
	queries := []string{
		`{service_name="ingress"}`, `{cluster="c1", namespace="ns1", container="ingress"}`, `{app=~"a|b", env!="dev"}`,
		`{a!~"b.*"}`, `{service_name="x", detected_level="error"}`, `{k8s_pod_name="p"}`,
	}
	pipe := " | fields " + strings.Join(patternRowFields, ", ")
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
			if got := p.patternRowsQuery(query, text); got != text+pipe {
				t.Fatalf("%s %s: read %q, want the translated %q and the fields pipe", labelStyle, query, got, text)
			}
			if _, err := logsql.Parse(p.patternRowsQuery(query, text)); err != nil {
				t.Fatalf("%s: %v", query, err)
			}
		}
		for _, query := range []string{"*", `app:="x"`} {
			if got := p.patternRowsQuery(query, query); got != query {
				t.Fatalf("%q read as %q, want it as translated", query, got)
			}
		}
		_ = p.Shutdown(context.Background())
	}
}

// BenchmarkPatternRowsDecode is the proxy's cost of mining a sampled read's answer: whole rows (base) and the rows
// the fields pipe keeps (this change), 240 wide rows.
func BenchmarkPatternRowsDecode(b *testing.B) {
	t := &testing.T{}
	rows := patternWideRows()
	var kept []map[string]string
	for _, row := range rows {
		kept = append(kept, patternRowsKept(t, row))
	}
	for name, body := range map[string][]byte{"whole": patternRowsBody(t, rows), "kept": patternRowsBody(t, kept)} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(body)))
			for b.Loop() {
				if _, _, err := extractLogPatternsStreamFilled(bytes.NewReader(body), "10s", 1000, defaultLogRowLevels(), true, 1795773600e9, 1795775700e9); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkPatternRowsQuery is the cost of building the read's query: the selector's filter from its parsed tree
// (memoised) with the fields pipe.
func BenchmarkPatternRowsQuery(b *testing.B) {
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: cache.New(time.Minute, 100), LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	const query = `{service_name="ingress"}`
	text, _ := p.translatePatternQuery(context.Background(), query)
	b.ReportAllocs()
	for b.Loop() {
		_ = p.patternRowsQuery(query, text)
	}
}
