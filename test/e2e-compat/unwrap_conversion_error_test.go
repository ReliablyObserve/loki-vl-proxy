//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// unwrapErrorAnswer is one raw HTTP answer: Loki writes a pipeline error as
// text/plain, so the body is compared as text.
type unwrapErrorAnswer struct {
	status      int
	contentType string
	body        string
}

func unwrapErrorGet(t *testing.T, base, path string, params url.Values) unwrapErrorAnswer {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+path+"?"+params.Encode(), nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Scope-OrgID", "0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s: %v", base, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return unwrapErrorAnswer{resp.StatusCode, resp.Header.Get("Content-Type"), string(body)}
}

// unwrapErrorSeriesRE reads the series and the conversion's message out of
// logqlmodel.PipelineError's text.
var unwrapErrorSeriesRE = regexp.MustCompile(`^pipeline error: 'SampleExtractionErr' for series: '\{(.*)\}'\.\n`)

// unwrapErrorShape is what two pipeline errors must share: Loki names the
// first failing line it meets, which depends on its shards, so the values may
// differ; the label names, the conversion that failed and the rest of the text
// may not.
func unwrapErrorShape(t *testing.T, body string) string {
	t.Helper()
	m := unwrapErrorSeriesRE.FindStringSubmatch(body)
	if m == nil {
		t.Fatalf("not a SampleExtractionErr pipeline error: %q", body)
	}
	var names []string
	detail := ""
	for _, pair := range regexp.MustCompile(`(?:^|, )([A-Za-z_][A-Za-z0-9_]*)="((?:[^"\\]|\\.)*)"`).FindAllStringSubmatch(m[1], -1) {
		names = append(names, pair[1])
		if pair[1] == "__error_details__" {
			value, _ := strconv.Unquote(`"` + pair[2] + `"`)
			detail, _, _ = strings.Cut(value, ":")
		}
	}
	sort.Strings(names)
	return fmt.Sprintf("labels=%v conversion=%s rest=%q", names, detail, body[len(m[0]):])
}

// unwrapResultCount returns the number of series of a 200 answer.
func unwrapResultCount(t *testing.T, a unwrapErrorAnswer) int {
	t.Helper()
	var resp struct {
		Status   string          `json:"status"`
		Warnings []string        `json:"warnings"`
		Data     json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal([]byte(a.body), &resp); err != nil || resp.Status != "success" || len(resp.Warnings) != 0 {
		t.Fatalf("unhealthy 200 answer: %s", a.body)
	}
	var data struct {
		Result []json.RawMessage `json:"result"`
	}
	_ = json.Unmarshal(resp.Data, &data)
	return len(data.Result)
}

// TestCompat_UnwrapConversionErrorParity: a range or instant metric whose
// unwrap meets a value its conversion rejects (strconv.ParseFloat,
// time.ParseDuration, humanize.ParseBytes) fails like Loki: 400, text/plain,
// logqlmodel.PipelineError over the line's labels, unless a label filter after
// the unwrap drops the error. Missing and empty labels make no sample and no
// error, and a value outside every evaluated window does not count. The value
// is Loki's parser's: a JSON array or null and a non-packed `| unpack` make no
// sample (VictoriaLogs renders the array as text), a JSON boolean fails, and a
// key named like stored structured metadata leaves the stored value to unwrap.
// Shapes the detection leaves out are registered and not asserted here
// (semantics/unwrap-conversion-error-postfilter-forms, -stored-value-overwritten,
// -undetected-shapes).
// conformance: semantics/unwrap-conversion-error, semantics/unwrap-conversion-error-stored-value-overwritten, status-400, parser-error-and-label-collision, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_UnwrapConversionErrorParity(t *testing.T) {
	now := time.Now()
	fx := slidingLiveFixture{app: fmt.Sprintf("unwrap-converr-%d", now.UnixNano()), service: true}
	s0 := now.Add(-6 * time.Hour).Truncate(time.Hour)
	// One logfmt line every 10s for 30 minutes, 7s past each 10s mark:
	//   n      a number on every line          t     "abc" on every line
	//   v      a number on even lines, "x<k>" on odd lines
	//   dur    1.5s, or "1d" every third line   size  2KiB, or "lots" every third line
	//   okdur  250ms (a duration, not a number) w     a number, "bad" on line 72 only (s0+12m07s)
	//   e      present and empty every fifth line
	for i := 0; i < 180; i++ {
		parts := []string{fmt.Sprintf("n=%d", i%7+1), "t=abc"}
		if i%2 == 0 {
			parts = append(parts, fmt.Sprintf("v=%d", i%9+1))
		} else {
			parts = append(parts, fmt.Sprintf("v=x%d", i%9+1))
		}
		dur, size := "1.5s", "2KiB"
		if i%3 == 0 {
			dur, size = "1d", "lots"
		}
		parts = append(parts, "dur="+dur, "size="+size, "okdur=250ms")
		w := strconv.Itoa(i%5 + 1)
		if i == 72 {
			w = "bad"
		}
		parts = append(parts, "w="+w)
		if i%5 == 0 {
			parts = append(parts, "e=")
		}
		fx.lines = append(fx.lines, slidingLiveLine{ts: s0.Add(7*time.Second + time.Duration(i)*10*time.Second), msg: strings.Join(parts, " ")})
	}
	// One JSON line every 10s: an array, null, a boolean, a number, an object,
	// strings that look like an array or a number, and text. VictoriaLogs'
	// unpack_json renders the array and the boolean as text; Loki's | json skips
	// the array and keeps the boolean, and | unpack (no _entry) adds nothing.
	jsonFx := slidingLiveFixture{app: fmt.Sprintf("unwrap-converr-json-%d", now.UnixNano()), service: true}
	for i := 0; i < 180; i++ {
		line := fmt.Sprintf(`{"a":[1,2],"b":null,"c":true,"d":%d,"e":{"x":1},"f":"[1,2]","g":"%d","s":"abc"}`, i%5+1, i%3+1)
		jsonFx.lines = append(jsonFx.lines, slidingLiveLine{ts: s0.Add(7*time.Second + time.Duration(i)*10*time.Second), msg: line})
	}
	// One logfmt line every 10s whose keys Loki's decoder and VictoriaLogs'
	// unpack_logfmt read differently: a duplicate key (Loki keeps the first
	// value, VictoriaLogs the last), a byte size followed by U+FFFD (Loki makes
	// it a space, which humanize.ParseBytes accepts) and an escaped quote.
	dupFx := slidingLiveFixture{app: fmt.Sprintf("unwrap-converr-dup-%d", now.UnixNano()), service: true}
	for i := 0; i < 180; i++ {
		line := fmt.Sprintf("d=%d d=abc b=5KB\uFFFD q=\"a\\\"b\" n=%d", i%5+1, i%3+1)
		dupFx.lines = append(dupFx.lines, slidingLiveLine{ts: s0.Add(7*time.Second + time.Duration(i)*10*time.Second), msg: line})
	}
	ingestSlidingFixtures(t, fx, jsonFx, dupFx)
	sel, jsonSel, dupSel := fx.selector(), jsonFx.selector(), dupFx.selector()
	collSel := ingestUnwrapCollisionFixture(t, now, s0)
	unpackedSel := ingestUnwrapIngestUnpackedFixture(t, now, s0)

	rangeParams := func(q string, start, end time.Duration, step int) url.Values {
		// Starts are multiples of the step: the stack's Loki aligns queries with
		// the step (align_queries_with_step), the proxy evaluates at start+k*step
		// as a default Loki does.
		return url.Values{"query": {q}, "start": {strconv.FormatInt(s0.Add(start).UnixNano(), 10)},
			"end": {strconv.FormatInt(s0.Add(end).UnixNano(), 10)}, "step": {strconv.Itoa(step)}}
	}
	window := func(q string) url.Values { return rangeParams(q, 10*time.Minute, 25*time.Minute, 60) }
	instant := func(q string, at time.Duration) url.Values {
		return url.Values{"query": {q}, "time": {strconv.FormatInt(s0.Add(at).UnixNano(), 10)}}
	}

	// Loki answers range metrics of a new stream only once its ingester cut the
	// chunks (the fresh-stack blank window): wait for the numeric control.
	control := `sum by (service_name) (sum_over_time(` + sel + ` | logfmt | unwrap n [5m]))`
	deadline := time.Now().Add(180 * time.Second)
	for {
		a := unwrapErrorGet(t, lokiURL, "/loki/api/v1/query_range", window(control))
		if a.status == http.StatusOK && unwrapResultCount(t, a) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Loki does not answer the numeric control: %d %s", a.status, a.body)
		}
		time.Sleep(5 * time.Second)
	}

	const rangePath, instantPath = "/loki/api/v1/query_range", "/loki/api/v1/query"
	for _, tc := range []struct {
		name, path string
		params     url.Values
		want       int // Loki 3.7.7's status, captured on this fixture
	}{
		{"sum over text", rangePath, window(`sum_over_time(` + sel + ` | logfmt | unwrap t [5m])`), 400},
		{"rate over mixed values", rangePath, window(`rate(` + sel + ` | logfmt | unwrap v [5m])`), 400},
		{"avg over mixed values", rangePath, window(`avg_over_time(` + sel + ` | logfmt | unwrap v [5m])`), 400},
		{"quantile over mixed values", rangePath, window(`quantile_over_time(0.9, ` + sel + ` | logfmt | unwrap v [5m])`), 400},
		{"grouped sum, parser hints", rangePath, window(`sum by (service_name) (sum_over_time(` + sel + ` | logfmt | unwrap v [5m]))`), 400},
		{"ungrouped sum, parser hints", rangePath, window(`sum(sum_over_time(` + sel + ` | logfmt | unwrap v [5m]))`), 400},
		{"max by keeps every label", rangePath, window(`max by (n) (max_over_time(` + sel + ` | logfmt | unwrap v [5m]))`), 400},
		{"binary operand", rangePath, window(`sum_over_time(` + sel + ` | logfmt | unwrap v [5m]) * 2`), 400},
		{"duration()", rangePath, window(`sum_over_time(` + sel + ` | logfmt | unwrap duration(dur) [5m])`), 400},
		{"bytes()", rangePath, window(`sum_over_time(` + sel + ` | logfmt | unwrap bytes(size) [5m])`), 400},
		{"a duration is not a number", rangePath, window(`sum_over_time(` + sel + ` | logfmt | unwrap okdur [5m])`), 400},
		{"structured metadata without a parser", rangePath, window(`sum_over_time(` + sel + ` | unwrap detected_level [5m])`), 400},
		{"error filter before the unwrap does not drop it", rangePath, window(`sum_over_time(` + sel + ` | logfmt | __error__="" | unwrap v [5m])`), 400},
		{"drop __error__ before the unwrap does not drop it", rangePath, window(`sum_over_time(` + sel + ` | logfmt | drop __error__ | unwrap v [5m])`), 400},
		{"grouping by __error__ does not preserve it", rangePath, window(`sum by (__error__) (sum_over_time(` + sel + ` | logfmt | unwrap v [5m]))`), 400},
		{"details filter compares the empty value", rangePath, window(`sum_over_time(` + sel + ` | logfmt | unwrap v | __error_details__="" [5m])`), 400},
		{"string filter on another label", rangePath, window(`sum_over_time(` + sel + ` | logfmt | unwrap v | n!="99" [5m])`), 400},
		{"the value inside a tumbling window", rangePath, rangeParams(`sum by (service_name) (sum_over_time(`+sel+` | logfmt | unwrap w [1m]))`, 10*time.Minute, 15*time.Minute, 60), 400},
		{"the value inside a tumbling window, ungrouped", rangePath, rangeParams(`max_over_time(`+sel+` | logfmt | unwrap w [1m])`, 10*time.Minute, 15*time.Minute, 60), 400},
		{"the value inside a gapped window", rangePath, rangeParams(`max_over_time(`+sel+` | logfmt | unwrap w [3m])`, 5*time.Minute, 25*time.Minute, 300), 400},
		{"instant", instantPath, instant(`sum_over_time(`+sel+` | logfmt | unwrap t [5m])`, 20*time.Minute), 400},
		{"instant quantile", instantPath, instant(`quantile_over_time(0.5, `+sel+` | logfmt | unwrap v [5m])`, 20*time.Minute), 400},
		{"instant window holding the value", instantPath, instant(`max_over_time(`+sel+` | logfmt | unwrap w [1m])`, 12*time.Minute+30*time.Second), 400},

		{"json boolean", rangePath, window(`sum by (service_name) (sum_over_time(` + jsonSel + ` | json | unwrap c [5m]))`), 400},
		{"json string that looks like an array", rangePath, window(`sum by (service_name) (sum_over_time(` + jsonSel + ` | json | unwrap f [5m]))`), 400},
		{"json text", rangePath, window(`sum by (service_name) (sum_over_time(` + jsonSel + ` | json | unwrap s [5m]))`), 400},

		{"quantile under sum", rangePath, window(`sum(quantile_over_time(0.5, ` + sel + ` | logfmt | unwrap t [5m]))`), 400},
		{"quantile under max, instant", instantPath, instant(`max(quantile_over_time(0.5, `+sel+` | logfmt | unwrap t [5m]))`, 20*time.Minute), 400},
		{"parser hints keep no name_extracted of a grouping label", rangePath, window(`sum by (x) (sum_over_time(` + collSel + ` | logfmt | unwrap z [5m]))`), 400},
		{"a JSON line VictoriaLogs also unpacked at ingest", rangePath, window(`sum(quantile_over_time(0.5, ` + unpackedSel + ` | json | unwrap duration(ms) [5m]))`), 400},

		{"logfmt duplicate key, Loki keeps the first value", rangePath, window(`sum by (service_name) (max_over_time(` + dupSel + ` | logfmt | unwrap d [5m]))`), 200},
		{"logfmt U+FFFD becomes a space", rangePath, window(`sum by (service_name) (max_over_time(` + dupSel + ` | logfmt | unwrap bytes(b) [5m]))`), 200},
		{"json array is skipped", rangePath, window(`sum by (service_name) (sum_over_time(` + jsonSel + ` | json | unwrap a [5m]))`), 200},
		{"json null is skipped", rangePath, window(`sum by (service_name) (sum_over_time(` + jsonSel + ` | json | unwrap b [5m]))`), 200},
		{"json number", rangePath, window(`sum by (service_name) (sum_over_time(` + jsonSel + ` | json | unwrap d [5m]))`), 200},
		{"json object is flattened", rangePath, window(`sum by (service_name) (sum_over_time(` + jsonSel + ` | json | unwrap e [5m]))`), 200},
		{"json nested number", rangePath, window(`sum by (service_name) (sum_over_time(` + jsonSel + ` | json | unwrap e_x [5m]))`), 200},
		{"unpack adds nothing without _entry", rangePath, window(`sum by (service_name) (sum_over_time(` + jsonSel + ` | unpack | unwrap s [5m]))`), 200},
		{"unpack skips an array", rangePath, window(`sum by (service_name) (sum_over_time(` + jsonSel + ` | unpack | unwrap a [5m]))`), 200},
		{"line_format replaces the parsed line", rangePath, window(`sum by (service_name) (sum_over_time(` + jsonSel + ` | line_format "{{.service_name}}" | logfmt | unwrap s [5m]))`), 200},
		{"a parsed key named like stored metadata reads the stored value", rangePath, window(`sum by (service_name) (sum_over_time(` + collSel + ` | logfmt | unwrap x [5m]))`), 200},
		{"json on a logfmt line extracts nothing", rangePath, window(`sum by (service_name) (sum_over_time(` + collSel + ` | json | unwrap y [5m]))`), 200},
		{"error dropped after the unwrap", rangePath, window(`sum by (service_name) (sum_over_time(` + sel + ` | logfmt | unwrap v | __error__="" [5m]))`), 200},
		{"other errors dropped after the unwrap", rangePath, window(`sum by (service_name) (sum_over_time(` + sel + ` | logfmt | unwrap v | __error__!="SampleExtractionErr" [5m]))`), 200},
		{"string filter no line passes", rangePath, window(`sum_over_time(` + sel + ` | logfmt | unwrap v | n="99" [5m])`), 200},
		{"empty label", rangePath, window(`sum_over_time(` + sel + ` | logfmt | unwrap e [5m])`), 200},
		{"missing label", rangePath, window(`sum_over_time(` + sel + ` | logfmt | unwrap nosuch [5m])`), 200},
		{"numeric control", rangePath, window(control), 200},
		{"duration() of durations", rangePath, window(`sum by (service_name) (sum_over_time(` + sel + ` | logfmt | unwrap duration(okdur) [5m]))`), 200},
		{"the value between gapped windows", rangePath, rangeParams(`max by (service_name) (max_over_time(`+sel+` | logfmt | unwrap w [1m]))`, 5*time.Minute, 25*time.Minute, 300), 200},
		{"the value after the last evaluation", rangePath, rangeParams(`max by (service_name) (max_over_time(`+sel+` | logfmt | unwrap w [1m]))`, 5*time.Minute, 12*time.Minute+6*time.Second, 60), 200},
		{"the value before the first window", rangePath, rangeParams(`max by (service_name) (max_over_time(`+sel+` | logfmt | unwrap w [1m]))`, 14*time.Minute, 20*time.Minute, 60), 200},
		{"instant window without the value", instantPath, instant(`max by (service_name) (max_over_time(`+sel+` | logfmt | unwrap w [1m]))`, 14*time.Minute), 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loki := unwrapErrorGet(t, lokiURL, tc.path, tc.params)
			if loki.status != tc.want {
				t.Fatalf("Loki answered %d (%s), the captured answer is %d: Loki is not healthy for this case", loki.status, loki.body, tc.want)
			}
			proxy := unwrapErrorGet(t, proxyURL, tc.path, tc.params)
			if proxy.status != loki.status {
				t.Fatalf("proxy %d %s, Loki %d %s", proxy.status, proxy.body, loki.status, loki.body)
			}
			if loki.status == http.StatusBadRequest {
				if proxy.contentType != loki.contentType {
					t.Fatalf("content type: proxy %q, Loki %q", proxy.contentType, loki.contentType)
				}
				if p, l := unwrapErrorShape(t, proxy.body), unwrapErrorShape(t, loki.body); p != l {
					t.Fatalf("pipeline error differs\nproxy %s\nloki  %s\nproxy body %q\nloki body  %q", p, l, proxy.body, loki.body)
				}
				return
			}
			if strings.Contains(tc.params.Get("query"), collSel) || strings.Contains(tc.params.Get("query"), "| unpack") || strings.Contains(tc.params.Get("query"), dupSel) {
				// Status only: the series of these shapes differ from Loki's for
				// reasons registered elsewhere (profiles/parsed-key-colliding-with-structured-metadata,
				// semantics/unpack-non-string-values).
				return
			}
			if p, l := unwrapResultCount(t, proxy), unwrapResultCount(t, loki); (p == 0) != (l == 0) || (strings.Contains(tc.params.Get("query"), " by (") && p != l) {
				t.Fatalf("proxy %d series, Loki %d series", p, l)
			}
		})
	}

	// Shapes Loki fails that the detection leaves out (registered, open): the
	// test checks Loki still fails them and skips with the case named.
	for _, tc := range []struct{ name, query, open string }{
		{"absent_over_time", `absent_over_time(` + sel + ` | logfmt | unwrap t [5m])`, "semantics/unwrap-conversion-error-undetected-shapes"},
		{"label_replace around the metric", `label_replace(sum by (service_name) (sum_over_time(` + sel + ` | logfmt | unwrap t [5m])), "x", "$1", "service_name", "(.*)")`, "semantics/unwrap-conversion-error-undetected-shapes"},
		{"a comparison after the unwrap", `sum_over_time(` + sel + ` | logfmt | unwrap v | v > 3 [5m])`, "semantics/unwrap-conversion-error-postfilter-forms"},
	} {
		t.Run("open/"+tc.name, func(t *testing.T) {
			if loki := unwrapErrorGet(t, lokiURL, rangePath, window(tc.query)); loki.status != http.StatusBadRequest {
				t.Fatalf("Loki answered %d (%s); the open case %s expects 400", loki.status, loki.body, tc.open)
			}
			if proxy := unwrapErrorGet(t, proxyURL, rangePath, window(tc.query)); proxy.status == http.StatusBadRequest {
				t.Fatalf("the proxy now answers 400 like Loki: close %s and move this query to the cases above", tc.open)
			}
			t.Skipf("Loki fails this query; the proxy does not detect it yet (%s)", tc.open)
		})
	}
}

// ingestUnwrapIngestUnpackedFixture ingests JSON lines the way VictoriaLogs'
// Loki push API stores them: the whole line in _msg and its keys unpacked as
// fields beside it (nested ones dotted). Loki keeps the line only. It returns
// the selector.
func ingestUnwrapIngestUnpackedFixture(t *testing.T, now, s0 time.Time) string {
	t.Helper()
	app := fmt.Sprintf("unwrap-converr-unpacked-%d", now.UnixNano())
	var vlRows strings.Builder
	values := make([][]any, 0, 180)
	for i := 0; i < 180; i++ {
		ts := s0.Add(7*time.Second + time.Duration(i)*10*time.Second)
		line := fmt.Sprintf(`{"service":{"name":"web"},"event":"load","ms":%d,"path":"/p%d"}`, 4000+i, i%3)
		row, _ := json.Marshal(map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": line, "service_name": app, "detected_level": "unknown",
			"service.name": "web", "event": "load", "ms": strconv.Itoa(4000 + i), "path": fmt.Sprintf("/p%d", i%3)})
		vlRows.Write(row)
		vlRows.WriteByte('\n')
		values = append(values, []any{strconv.FormatInt(ts.UnixNano(), 10), line, map[string]string{"detected_level": "unknown"}})
	}
	return ingestUnwrapRawFixture(t, app, vlRows.String(), values, s0)
}

// ingestUnwrapCollisionFixture ingests one logfmt stream whose lines hold
// x=abc while every line stores the structured metadata x="5": Loki renames the
// parsed key x_extracted and `unwrap x` reads the stored 5; VictoriaLogs'
// unpack_logfmt overwrites the stored field. It returns the selector.
func ingestUnwrapCollisionFixture(t *testing.T, now, s0 time.Time) string {
	t.Helper()
	app := fmt.Sprintf("unwrap-converr-coll-%d", now.UnixNano())
	var vlRows strings.Builder
	values := make([][]any, 0, 180)
	for i := 0; i < 180; i++ {
		ts := s0.Add(7*time.Second + time.Duration(i)*10*time.Second)
		line := fmt.Sprintf("x=abc y=%d z=bad", i%4+1)
		row, _ := json.Marshal(map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": line, "service_name": app, "detected_level": "unknown", "x": "5"})
		vlRows.Write(row)
		vlRows.WriteByte('\n')
		values = append(values, []any{strconv.FormatInt(ts.UnixNano(), 10), line, map[string]string{"detected_level": "unknown", "x": "5"}})
	}
	return ingestUnwrapRawFixture(t, app, vlRows.String(), values, s0)
}

// ingestUnwrapRawFixture writes rows to VictoriaLogs (jsonline) and values to
// Loki for one service_name stream of 180 lines, and waits until both count them.
func ingestUnwrapRawFixture(t *testing.T, app, vlRows string, values [][]any, s0 time.Time) string {
	t.Helper()
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=service_name,detected_level", vlRows, map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VL ingest: %d %s", status, body)
	}
	payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{"stream": map[string]string{"service_name": app}, "values": values}}})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json", "X-Scope-OrgID": "0"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)
	if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/flush", "", nil); status >= 300 {
		t.Fatalf("Loki flush: %d %s", status, body)
	}
	fx := slidingLiveFixture{app: app, service: true, lines: make([]slidingLiveLine, len(values))}
	for i := range fx.lines {
		fx.lines[i].ts = s0.Add(7*time.Second + time.Duration(i)*10*time.Second)
	}
	deadline := time.Now().Add(180 * time.Second)
	for {
		lokiCount, vlCount := slidingFixtureCounts(t, fx)
		if lokiCount == len(values) && vlCount == len(values) {
			return fx.selector()
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s fixture: loki=%d victorialogs=%d, want %d", app, lokiCount, vlCount, len(values))
		}
		time.Sleep(time.Second)
	}
}
