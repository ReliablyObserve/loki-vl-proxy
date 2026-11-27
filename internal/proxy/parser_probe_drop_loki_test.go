package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// parserProbeDropRows are the data sets of the parser probe differential: lines only `| json` reads, lines only
// `| logfmt` reads, both kinds in one stream, and lines neither reads as its own format.
func parserProbeDropRows(at time.Time) map[string][]pushdownRow {
	stream := map[string]string{"app": "pp"}
	// Loki's ingester stores the level it detects in each line as structured metadata.
	level := func(l string) map[string]string { return map[string]string{"detected_level": l} }
	jsonLine := func(i int) string {
		return fmt.Sprintf(`{"route":"/r%d","level":"info","x":%d}`, i%3, i%5+1)
	}
	logfmtLine := func(i int) string {
		return fmt.Sprintf(`route=/r%d level=warn x=%d`, i%3, i%5+1)
	}
	sets := map[string][]pushdownRow{}
	for i := 0; i < 24; i++ {
		ts := at.Add(-time.Duration(24-i) * 20 * time.Second)
		sets["json"] = append(sets["json"], pushdownRow{ts: ts, msg: jsonLine(i), stream: stream, loki: level("info")})
		sets["logfmt"] = append(sets["logfmt"], pushdownRow{ts: ts, msg: logfmtLine(i), stream: stream, loki: level("warn")})
		mixed := pushdownRow{ts: ts, msg: jsonLine(i), stream: stream, loki: level("info")}
		if i%3 == 0 {
			mixed = pushdownRow{ts: ts, msg: logfmtLine(i), stream: stream, loki: level("warn")}
		}
		sets["mixed"] = append(sets["mixed"], mixed)
		sets["plain"] = append(sets["plain"], pushdownRow{ts: ts, msg: fmt.Sprintf("plain text line %d", i), stream: stream, loki: level("unknown")})
	}
	return sets
}

// parserProbeDropQueries are the queries naming both `| json` and `| logfmt` whose second parser is the last stage
// before the range, in both orders, with and without a drop of the error labels after them: every range function
// over the lines and three over an unwrap, under every wrapper Loki keeps a parser error and an answer through.
func parserProbeDropQueries() []string {
	var out []string
	// The one-parser pipelines are the controls: the queries the probe leaves, which it never reads.
	for _, pipe := range []string{`| json | logfmt`, `| logfmt | json`, `| json | logfmt | drop __error__, __error_details__`, `| logfmt | json | drop __error__, __error_details__`,
		`| json`, `| logfmt`, `| json | drop __error__, __error_details__`, `| logfmt | drop __error__, __error_details__`} {
		sel := `{app="pp"} ` + pipe
		count := `count_over_time(` + sel + ` [5m])`
		for _, wrap := range []string{"%s", "sum(%s)", "sum by (route) (%s)", "max(%s)", "min(%s)", "avg(%s)", "count(%s)",
			"topk(2, sum by (route) (%s))", "bottomk(2, sum by (route) (%s))", "sort(sum by (route) (%s))", "sort_desc(sum by (route) (%s))",
			`label_replace(sum(%s), "copy", "$1", "route", "(.*)")`, "sum(%s) + 1", "1 + sum(%s)"} {
			out = append(out, fmt.Sprintf(wrap, count))
		}
		out = append(out, `sum(count_over_time(`+sel+` [5m] offset 2m))`)
		for _, fn := range []string{"rate", "bytes_over_time", "bytes_rate"} {
			r := fn + `(` + sel + ` [5m])`
			out = append(out, r, `sum(`+r+`)`)
		}
		out = append(out, `absent_over_time(`+sel+` [5m])`)
		unwrap := strings.Replace(sel, " | drop", " | unwrap x | drop", 1)
		if !strings.Contains(unwrap, "unwrap") {
			unwrap += " | unwrap x"
		}
		for _, fn := range []string{"quantile_over_time(0.9, "} {
			r := fn + unwrap + ` [5m])`
			out = append(out, r, `sum(`+r+`)`)
		}
	}
	return out
}

// The parser probe keeps one of `| json` and `| logfmt` (preferWorkingParser). It removed the other with a text
// pattern that, for a metric query, took everything after the stage up to the next `|`: a stage that was the last
// one before its range took the range and the closing parentheses with it (#1821). An instant query then answered
// "log queries are not supported as an instant query type", a range query answered log streams read with the cut
// text as a phrase, a bare range function answered 500. Its probe query was read from the text too, and took a
// quantile_over_time's parameter into the log query. Both are read from the parsed tree now.
//
// Every request here, instant and range, over four data sets, is held to two things. No answer shows the cut: no
// VictoriaLogs read carries LogQL text, the probe is well formed and bounded, a metric query answers a vector or a
// matrix and never a 500. And the answer is compared with what Loki answers for the same data and request
// (recorded engine answers): the rows that differ are listed in testdata/parser_probe_drop_known_differences.txt,
// a ratchet that may only shrink (LVP_PARSER_PROBE_DROP_UPDATE=1 rewrites it). The listed rows differ for reasons
// this test does not change: the probe keeps one parser where Loki runs both, and the one-parser forms of the same
// shapes differ on this fake backend too.
//
// conformance: semantics/parser-probe-drop-keeps-range, semantics/two-parsers-keep-one, loki_api_v1_query, loki_api_v1_query_range
func TestParserProbeDropMatchesRecordedLoki(t *testing.T) {
	at := time.Unix(1700000400, 0).UTC()
	rows := parserProbeDropRows(at)
	names := []string{"json", "logfmt", "mixed", "plain"}
	queries := parserProbeDropQueries()
	sets := make([]oraclerun.EngineDataSet, 0, len(names))
	for _, name := range names {
		set := oraclerun.EngineDataSet{Streams: pushdownEngineStreams(rows[name])}
		for _, step := range []time.Duration{0, time.Minute} {
			for _, q := range queries {
				set.Requests = append(set.Requests, lokiMetricRequest(fmt.Sprintf("%s/%v/%s", name, step, q), q, at.Add(-5*time.Minute).UnixNano(), at.UnixNano(), step))
			}
		}
		sets = append(sets, set)
	}
	answers := lokiEngineAnswers(t, "parser_probe_drop", sets)
	var differs []string
	for i, name := range names {
		backend, fake := newPushdownFakeVL(t, rows[name], func(s string) string { return s })
		fake.projectRaw = true
		for _, req := range sets[i].Requests {
			route := "query_range"
			if req.Instant {
				route = "query"
			}
			p := newTestProxy(t, backend.URL)
			fake.mu.Lock()
			raw := len(fake.rawQueries)
			fake.mu.Unlock()
			got := doCompatProxyRequest(p, "/loki/api/v1/"+route+"?"+req.Form, nil)
			fake.mu.Lock()
			reads := append([]fakeRawQuery(nil), fake.rawQueries[raw:]...)
			fake.mu.Unlock()
			for _, read := range reads {
				// The probe and every raw read carry LogsQL, never LogQL text read as a phrase.
				if strings.Contains(read.query, "_over_time") || strings.Contains(read.query, "rate(") {
					t.Errorf("%s: a raw read of the LogQL text: %q", req.ID, read.query)
				}
				if strings.HasSuffix(read.query, "| sort by (_time desc)") && (!strings.HasPrefix(read.query, `app:="pp" `) || read.start == math.MinInt64 || read.end == math.MaxInt64) {
					t.Errorf("%s: malformed or unbounded parser probe: %+v", req.ID, read)
				}
			}
			body := got.Body.String()
			if got.Code >= 500 || strings.Contains(body, "log queries are not supported") || strings.Contains(body, `"streams"`) {
				t.Errorf("%s: the query was cut: %d %s", req.ID, got.Code, body)
			}
			if err := parserProbeLokiDifference(answers[req.ID], got.Code, got.Body.Bytes()); err != nil {
				differs = append(differs, req.ID)
				t.Logf("%s: %v", req.ID, err)
			}
		}
	}
	holdKnownDifferences(t, filepath.Join("testdata", "parser_probe_drop_known_differences.txt"), "LVP_PARSER_PROBE_DROP_UPDATE", differs)
}

// parserProbeLokiDifference is how the proxy's answer differs from Loki's recorded one, or nil: the status, a
// pipeline error's diagnostic (the failing series' text apart), or the series and values.
func parserProbeLokiDifference(want lokiEngineAnswer, code int, body []byte) error {
	if code != want.Status {
		return fmt.Errorf("status %d, Loki %d", code, want.Status)
	}
	if want.Status != 200 {
		if recordedParserDiagnostic(string(body)) != recordedParserDiagnostic(want.Error) {
			return fmt.Errorf("error %q, Loki %q", body, want.Error)
		}
		return nil
	}
	if parserProbeEmptyAnswer(want.Body) && parserProbeEmptyAnswer(body) {
		return nil // absent_over_time over present lines: neither answers a series
	}
	return recordedParserDifference(recordedParserCase{Status: 200, Response: want.Body}, code, body)
}

// holdKnownDifferences holds the ids that differ from Loki to the list in path: a row not listed that differs
// fails, and so does a listed row that no longer differs (the list only shrinks). With env set to 1 it writes the
// list instead.
func holdKnownDifferences(t *testing.T, path, env string, differs []string) {
	t.Helper()
	sort.Strings(differs)
	if os.Getenv(env) == "1" {
		text := "# Rows of " + t.Name() + " whose answer differs from Loki's recorded one, one request id per line.\n" +
			"# Regenerate with " + env + "=1; the list may only shrink.\n" + strings.Join(differs, "\n") + "\n"
		if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	known := map[string]bool{}
	for _, line := range strings.Split(string(data), "\n") {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
			known[line] = true
		}
	}
	seen := map[string]bool{}
	for _, id := range differs {
		seen[id] = true
		if !known[id] {
			t.Errorf("%s: differs from Loki and is not a known difference", id)
		}
	}
	for id := range known {
		if !seen[id] {
			t.Errorf("%s: now answers as Loki does: remove it from %s (%s=1)", id, path, env)
		}
	}
}

// parserProbeEmptyAnswer reports a success answer with no series.
func parserProbeEmptyAnswer(body []byte) bool {
	var reply struct {
		Status string `json:"status"`
		Data   struct {
			Result []json.RawMessage `json:"result"`
		} `json:"data"`
	}
	return json.Unmarshal(body, &reply) == nil && reply.Status == "success" && len(reply.Data.Result) == 0
}

