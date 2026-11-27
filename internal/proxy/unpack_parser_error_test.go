package proxy

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// The proxy reads a line an unpack parses as Loki does: the same verdict and, for an error, Loki's details, on the
// recorded lines.
//
// conformance: semantics/unpack-parser-error-not-raised
func TestUnpackLineErrorMatchesRecordedLoki(t *testing.T) {
	b, err := os.ReadFile("testdata/unpack_error_lines.json")
	if err != nil {
		t.Fatal(err)
	}
	var rows []struct{ Line, Error, Details string }
	if err := json.Unmarshal(b, &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) < 20 {
		t.Fatalf("%d recorded lines", len(rows))
	}
	for _, row := range rows {
		got := unpackLineError(row.Line)
		switch {
		case (got != nil) != (row.Error != ""):
			t.Errorf("%q: error %v, Loki %q %q", row.Line, got, row.Error, row.Details)
		case got == nil:
		case strings.Contains(row.Line, `\q`):
			// An invalid escape: both fail the line, with other details. VictoriaLogs reads the line as JSON, so no
			// metric reaches this reading for it (semantics/json-parser-error-hazard-lines-not-marked).
		case got.Error() != row.Details:
			t.Errorf("%q: details %q, Loki %q", row.Line, got, row.Details)
		}
	}
}

// The probe of a count over an unpack exists exactly where Loki can fail the query.
//
// conformance: semantics/unpack-parser-error-not-raised
func TestUnpackErrorProbes(t *testing.T) {
	for q, want := range map[string]int{
		`count_over_time({a="b"} | unpack [1m])`:                                     1,
		`rate({a="b"} |= "k" | unpack | drop x [1m] offset 5m)`:                      1,
		`bytes_over_time({a="b"} | x="1" | unpack [1m])`:                             1,
		`bytes_rate({a="b"} | unpack |= "k" [1m])`:                                   1,
		`sum by (x) (count_over_time({a="b"} | unpack [1m]))`:                        1,
		`max(count_over_time({a="b"} | unpack [1m]))`:                                1,
		`count_over_time({a="b"} | unpack [1m]) + rate({a="b"} | unpack [1m])`:       2,
		`label_replace(count_over_time({a="b"} | unpack [1m]), "d", "x", "d", ".*")`: 1,
		`sum(count_over_time({a="b"} | unpack [1m]))`:                                0,
		`sum(rate({a="b"} | unpack | keep x [1m]))`:                                  0,
		`count_over_time({a="b"} | unpack | drop __error__, __error_details__ [1m])`: 0,
		`count_over_time({a="b"} | unpack | __error__="" [1m])`:                      0,
		`absent_over_time({a="b"} | unpack [1m])`:                                    0,
		// Another parser beside the unpack: not covered yet (semantics/translator-writes-unpack-as-unpack-json).
		`count_over_time({a="b"} | unpack | json [1m])`:   0,
		`count_over_time({a="b"}[1m])`:                    0,
		`sum_over_time({a="b"} | unpack | unwrap x [1m])`: 0,
	} {
		expr, ok := sharedLogQLExpr(RequestQueryText(q))
		if !ok {
			t.Fatalf("%s does not parse", q)
		}
		if got := len(jsonErrorProbes(expr)); got != want {
			t.Errorf("%s: %d probes, want %d", q, got, want)
		}
	}
}

// FuzzUnpackLineError holds the reading to the contract on any line: an empty line and a valid JSON object set no
// error, a line that opens with anything but a brace sets Loki's not-an-object error.
//
// conformance: semantics/unpack-parser-error-not-raised
func FuzzUnpackLineError(f *testing.F) {
	for _, s := range []string{``, `{}`, `{"a":"1","_entry":"k"}`, `{"a":"1","_entry":"k"`, `[1]`, `x`, `{"a":"é"}`, `{"a":{"b":[1,"\n"]}}`} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, line string) {
		err := unpackLineError(line)
		switch {
		case line == "":
			if err != nil {
				t.Fatalf("empty line: %v", err)
			}
		case line[0] != '{':
			if !errors.Is(err, errUnpackNotObject) {
				t.Fatalf("%q: %v", line, err)
			}
		case json.Valid([]byte(line)) && !strings.Contains(line, `\`):
			if err != nil {
				t.Fatalf("%q is a JSON object: %v", line, err)
			}
		}
	})
}

// The unwrap check reads from a line an unpack parses the labels Loki reads, and its error, on the recorded lines of
// the lowering's grid (internal/queryplan/lower/testdata/unpack-oracles.json, the plain unpack).
//
// conformance: semantics/unwrap-conversion-check-reads-unpack-unlike-loki
func TestUnwrapPackedLabelsMatchRecordedLoki(t *testing.T) {
	b, err := os.ReadFile("../queryplan/lower/testdata/unpack-oracles.json")
	if err != nil {
		t.Fatal(err)
	}
	var families []struct {
		Query string `json:"q"`
		Loki  []struct {
			Labels map[string]string `json:"labels"`
		} `json:"loki"`
	}
	if err := json.Unmarshal(b, &families); err != nil {
		t.Fatal(err)
	}
	// The grid's lines (internal/queryplan/lower unpack_test.go unpackLines).
	lines := []string{`{"a":"1","b":"x","_entry":"hello world"}`, `{"a":"1","b":"x"}`, `{"a":"1","_entry":""}`,
		`{"a":1,"b":true,"c":null,"d":{"e":"f"},"_entry":"L"}`, `not json`, `{"a":"1","_entry":5}`, `{"a":"1","_entry":{"x":"y"}}`,
		`{"_entry":"only"}`, `{"a":"1","n":{"_entry":"z"}}`, `{}`, ``, `{"a":"1","_entry":"say \"hi\" {x}"}`,
		`{"a":"2","_entry":"hello again"}`, `{"a":"hello","_entry":"bye"}`, `{"a":"1","_entry":"{\"z\":\"2\"}"}`}
	if len(families) == 0 || families[0].Query != `{app="s"} | unpack` || len(families[0].Loki) != len(lines) {
		t.Fatalf("the recorded grid changed: %d families", len(families))
	}
	for i, line := range lines {
		labels := map[string]string{"app": "s"}
		if !unwrapParseLine(&logqlpkg.ParserStage{Type: logqlpkg.ParserUnpack}, line, labels, map[string]bool{}, false, false, nil) {
			t.Fatalf("%q: not read", line)
		}
		want := families[0].Loki[i].Labels
		if len(labels) != len(want) {
			t.Errorf("%q: labels %v, Loki %v", line, labels, want)
			continue
		}
		for name, value := range want {
			if labels[name] != value {
				t.Errorf("%q: %s=%q, Loki %q", line, name, labels[name], value)
			}
		}
	}
}

// End to end through the handlers for an unpack: Loki's error names the failing sample's labels, the ingest level
// included under a sum by other labels (detected_level="unknown" on a line naming none); the reads mark an empty
// line and VictoriaLogs' -defaultMsgValue as parsed, and a lookup that answers such a row (a JSON object VictoriaLogs
// unpacked at ingest) answers the metric, as Loki's unpack fails on neither. (VictoriaLogs itself, the format the reads
// send, is held by TestCompat_UnpackSumsLikeLoki over its Loki push route.)
//
// conformance: semantics/unpack-parser-error-not-raised, semantics/pipeline-error-text-parity, loki_api_v1_query_range
func TestUnpackErrorCheckHandlers(t *testing.T) {
	at := time.Unix(1700000000, 0)
	bad := at.Add(3*time.Minute + 7*time.Second)
	const query = `sum by (app) (count_over_time({app="pay"} | unpack [1m]))`
	req := func() *http.Request {
		v := url.Values{"query": {query}, "start": {strconv.FormatInt(at.UnixNano(), 10)}, "end": {strconv.FormatInt(at.Add(10*time.Minute).UnixNano(), 10)}, "step": {"60"}}
		return httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+v.Encode(), nil)
	}
	serve := func(p *Proxy) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		p.handleQueryRange(rec, req())
		return rec
	}
	t.Run("Loki fails", func(t *testing.T) {
		fake := &jsonMarkFakeVL{at: bad, line: "plain text line"}
		rec := serve(newTestProxy(t, newJSONMarkFakeVL(t, fake).URL))
		const want = `pipeline error: 'JSONParserErr' for series: '{__error__="JSONParserErr", __error_details__="expecting json object(6), but it is not", app="pay", detected_level="unknown", service_name="pay"}'.`
		if rec.Code != http.StatusBadRequest || !strings.HasPrefix(rec.Body.String(), want) {
			t.Fatalf("status %d: %q\nwant %q", rec.Code, rec.Body.String(), want)
		}
		calls, _ := fake.snapshot()
		parsed := func(field string) string {
			return `format if (` + field + `:="" OR ` + field + `:=` + strconv.Quote(vlDefaultMsgValue) + `) "1" as ` + jsonValidField
		}
		for _, c := range calls {
			if strings.Contains(c, "unpack_json") && !strings.Contains(c, parsed("_msg")) && !strings.Contains(c, parsed("__vlm")) {
				t.Errorf("a read that does not count the stored empty lines as parsed: %s", c)
			}
		}
	})
	t.Run("no line stored", func(t *testing.T) {
		want := serve(newTestProxy(t, newJSONMarkFakeVL(t, &jsonMarkFakeVL{at: bad}).URL))
		fake := &jsonMarkFakeVL{at: bad, line: vlDefaultMsgValue}
		got := serve(newTestProxy(t, newJSONMarkFakeVL(t, fake).URL))
		var g, w any
		if got.Code != http.StatusOK || json.Unmarshal(got.Body.Bytes(), &g) != nil || json.Unmarshal(want.Body.Bytes(), &w) != nil || !reflect.DeepEqual(g, w) {
			t.Fatalf("status %d: %s\nwant %s", got.Code, got.Body.String(), want.Body.String())
		}
	})
}
