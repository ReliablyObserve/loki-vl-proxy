package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"text/template"
)

// Loki's error details for {{.a.b}} on an entry without a map-valued a
// (measured on Loki v3.7.7).
const lokiTemplateErrDetails = `template: line:1:4: executing "line" at <.a.b>: can't evaluate field b in type string`

// A template that fails on an entry keeps the entry and its line, with
// Loki's error labels: in the stream labels without categorize-labels (the
// entry moves to a stream of its own), in the parsed labels with it.
// conformance: semantics/line-format-template-error-keeps-entry
func TestLineFormatTemplateErrorKeepsEntry(t *testing.T) {
	tmpl := `{{if .x}}{{.x.y}}{{else}}ok {{.app}}{{end}}`
	// Loki executes the template as written, named "line": the details must
	// read as text/template reports them for that template.
	var want strings.Builder
	plain := template.Must(template.New("line").Option("missingkey=zero").Parse(tmpl))
	details := plain.Execute(&want, map[string]string{"x": "v"}).Error()

	streams := []map[string]interface{}{{
		"stream": map[string]string{"app": "web"},
		"values": []interface{}{[]interface{}{"1", "plain"}, []interface{}{"2", "with x"}},
	}}
	// The second entry fails: give its stream an x label through a stream of
	// its own.
	streams = append(streams, map[string]interface{}{
		"stream": map[string]string{"app": "web", "x": "v"},
		"values": []interface{}{[]interface{}{"3", "fails"}},
	})
	got, err := applyLineFormatTemplate(streams, tmpl)
	if err != nil {
		t.Fatal(err)
	}
	byTS := map[string][2]interface{}{}
	for _, s := range got {
		for _, v := range s["values"].([]interface{}) {
			tuple := v.([]interface{})
			byTS[tuple[0].(string)] = [2]interface{}{tuple[1], s["stream"]}
		}
	}
	if len(byTS) != 3 {
		t.Fatalf("entries %v", byTS)
	}
	if byTS["1"][0] != "ok web" || byTS["2"][0] != "ok web" {
		t.Errorf("formatted lines: %v", byTS)
	}
	failed := byTS["3"]
	labels := failed[1].(map[string]string)
	if failed[0] != "fails" || labels["__error__"] != "TemplateFormatErr" || labels["__error_details__"] != details || labels["x"] != "v" {
		t.Errorf("failed entry: line %q labels %v", failed[0], labels)
	}

	// Categorized: the error labels are parsed labels; shared maps stay as
	// they were.
	shared := map[string]string{"level": "info"}
	meta := map[string]interface{}{"structuredMetadata": shared}
	cat := []map[string]interface{}{{
		"stream": map[string]string{"app": "web"},
		"values": []interface{}{[]interface{}{"1", "line", meta}},
	}}
	got, err = applyLineFormatTemplate(cat, `{{.a.b}}`)
	if err != nil {
		t.Fatal(err)
	}
	tuple := got[0]["values"].([]interface{})[0].([]interface{})
	md := tuple[2].(map[string]interface{})
	parsed := md["parsed"].(map[string]string)
	if tuple[1] != "line" || parsed["__error__"] != "TemplateFormatErr" || parsed["__error_details__"] != lokiTemplateErrDetails {
		t.Errorf("categorized failed entry: %v", tuple)
	}
	if fmt.Sprint(got[0]["stream"]) != fmt.Sprint(map[string]string{"app": "web"}) || len(meta) != 1 || len(shared) != 1 {
		t.Errorf("stream or shared metadata changed: %v %v %v", got[0]["stream"], meta, shared)
	}
}

// The stages after line_format see the error labels: | __error__="" drops
// a failed entry, | drop removes the labels.
// conformance: semantics/line-format-template-error-keeps-entry
func TestLineFormatTemplateErrorLaterStages(t *testing.T) {
	for _, tc := range []struct {
		query   string
		entries int
		labels  map[string]string
	}{
		{`{app="web"} | line_format "{{.a.b}}"`, 1, map[string]string{"app": "web", "__error__": "TemplateFormatErr", "__error_details__": lokiTemplateErrDetails}},
		{`{app="web"} | line_format "{{.a.b}}" | __error__=""`, 0, nil},
		{`{app="web"} | line_format "{{.a.b}}" | drop __error__`, 1, map[string]string{"app": "web", "__error_details__": lokiTemplateErrDetails}},
		{`{app="web"} | line_format "{{.a.b}}" | drop __error__, __error_details__`, 1, map[string]string{"app": "web"}},
		{`{app="web"} | line_format "{{.a.b}}" | drop __error__, __error_details__ | __error__=""`, 1, map[string]string{"app": "web"}},
		{`{app="web"} | __error__="" | line_format "{{.a.b}}"`, 1, map[string]string{"app": "web", "__error__": "TemplateFormatErr", "__error_details__": lokiTemplateErrDetails}},
	} {
		streams := []map[string]interface{}{{
			"stream": map[string]string{"app": "web"},
			"values": [][]string{{"1", "line"}},
		}}
		got, err := applyQueryLineFormat(t.Context(), streams, tc.query)
		if err != nil {
			t.Fatalf("%s: %v", tc.query, err)
		}
		n := 0
		for _, s := range got {
			for _, v := range s["values"].([][]string) {
				n++
				if v[1] != "line" || fmt.Sprint(s["stream"]) != fmt.Sprint(tc.labels) {
					t.Errorf("%s: entry %v labels %v, want labels %v", tc.query, v, s["stream"], tc.labels)
				}
			}
		}
		if n != tc.entries {
			t.Errorf("%s: %d entries, want %d", tc.query, n, tc.entries)
		}
	}
}

// A template the parser rejects gets Loki's parse error.
// conformance: profiles/tail-line-format-parse-error-status
func TestLineFormatParseErrorText(t *testing.T) {
	_, err := applyLineFormatTemplate(nil, `{{`)
	want := `parse error : stage '| line_format "{{"' : invalid line template: template: line:1: unclosed action`
	if err == nil || err.Error() != want {
		t.Fatalf("got %v\nwant %s", err, want)
	}
}

// query_range answers a template error per entry, as Loki, where it
// answered 400; tail sends the entry. A proxy budget still fails the request.
// conformance: semantics/line-format-template-error-keeps-entry
func TestLineFormatTemplateErrorResponses(t *testing.T) {
	row := `{"_time":"2026-01-01T00:00:01Z","_msg":"a=x msg=hello","_stream":"{app=\"web\"}","app":"web","a":"x","msg":"hello"}`
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		fmt.Fprintln(w, row)
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	q := url.Values{"query": {`{app="web"} | logfmt | line_format "{{.a.b}}"`}, "start": {"1767225600"}, "end": {"1767225602"}}
	res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+q.Encode(), nil)
	if res.Code != http.StatusOK {
		t.Fatalf("query_range status %d: %s", res.Code, res.Body)
	}
	var body struct {
		Data struct {
			Result []struct {
				Stream map[string]string `json:"stream"`
				Values [][]string        `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Data.Result) != 1 || body.Data.Result[0].Values[0][1] != "a=x msg=hello" ||
		body.Data.Result[0].Stream["__error__"] != "TemplateFormatErr" || body.Data.Result[0].Stream["__error_details__"] != lokiTemplateErrDetails {
		t.Fatalf("query_range result: %s", res.Body)
	}

	q.Set("query", "{app=\"web\"} | line_format `{{printf \"%100000000s\" .app}}`")
	if res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+q.Encode(), nil); res.Code != http.StatusBadRequest || !strings.Contains(res.Body.String(), "limit") {
		t.Fatalf("budget: status %d %s", res.Code, res.Body)
	}

	tp := p.newTailPipeline(`{app="web"} | logfmt | line_format "{{.a.b}}"`, false)
	entries, err := p.tailEntries(t.Context(), tp, []byte(row+"\n"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("tail entries %v, %v", entries, err)
	}
	if entries[0].stream["__error__"] != "TemplateFormatErr" || entries[0].value.([]interface{})[1] != "a=x msg=hello" {
		t.Fatalf("tail entry %+v", entries[0])
	}
}
