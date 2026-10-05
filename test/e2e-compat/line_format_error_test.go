//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// lfeEntries runs a log query_range and indexes the entries by timestamp.
func lfeEntries(t *testing.T, base, query string, start, end time.Time, categorized bool) (int, map[string]tailPipelineEntry) {
	t.Helper()
	params := url.Values{}
	params.Set("query", query)
	params.Set("start", strconv.FormatInt(start.UnixNano(), 10))
	params.Set("end", strconv.FormatInt(end.UnixNano(), 10))
	params.Set("limit", "100")
	req, _ := http.NewRequest(http.MethodGet, base+"/loki/api/v1/query_range?"+params.Encode(), nil)
	req.Header.Set("X-Scope-OrgID", "0")
	if categorized {
		req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
	}
	resp, err := dlHTTP.Do(req)
	if err != nil {
		t.Fatalf("query_range %s: %v", base, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	out := map[string]tailPipelineEntry{}
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, out
	}
	var r dlStreamsResponse
	if err := json.Unmarshal(body, &r); err != nil || len(r.Warnings) > 0 {
		t.Fatalf("query_range %s: %v %s", base, err, body)
	}
	for _, s := range r.Data.Result {
		for _, v := range s.Values {
			var ts string
			e := tailPipelineEntry{stream: s.Stream}
			_ = json.Unmarshal(v[0], &ts)
			_ = json.Unmarshal(v[1], &e.line)
			if len(v) > 2 {
				var meta struct {
					SM map[string]string `json:"structuredMetadata"`
					P  map[string]string `json:"parsed"`
				}
				if err := json.Unmarshal(v[2], &meta); err != nil {
					t.Fatalf("decode entry metadata %s: %v", v[2], err)
				}
				e.metadata, e.parsed = meta.SM, meta.P
			}
			out[ts] = e
		}
	}
	return resp.StatusCode, out
}

// A line_format template that fails on an entry: Loki keeps the entry and
// its line with __error__="TemplateFormatErr" and __error_details__, and the
// stages after it see those labels.
// conformance: semantics/line-format-template-error-keeps-entry
func TestCompat_LineFormatTemplateErrorParity(t *testing.T) {
	app := fmt.Sprintf("lferr%d", time.Now().UnixNano())
	start := time.Now()
	lines := []string{`a=x msg=hello`, `msg=two`, `a=y msg=three level=warn`}
	for i, line := range lines {
		at := start.Add(time.Duration(i) * time.Millisecond)
		payload := dlJSON(map[string]interface{}{"streams": []interface{}{map[string]interface{}{
			"stream": map[string]string{"app": app}, "values": []interface{}{[]interface{}{strconv.FormatInt(at.UnixNano(), 10), line}},
		}}})
		dlPost(t, vlURL+"/insert/loki/api/v1/push?disable_message_parsing=1", "application/json", payload)
		dlPost(t, lokiURL+"/loki/api/v1/push", "application/json", payload)
	}
	end := start.Add(time.Second)
	selector := fmt.Sprintf(`{app=%q}`, app)

	// Both sides hold the fixture before any comparison.
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, l := lfeEntries(t, lokiURL, selector, start, end, false)
		_, v := lfeEntries(t, proxyURL, selector, start, end, false)
		if len(l) == len(lines) && len(v) == len(lines) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fixture not queryable: loki=%d proxy=%d", len(l), len(v))
		}
		time.Sleep(500 * time.Millisecond)
	}

	for _, target := range []struct{ name, url string }{{"loki-profile", patternsAutodetectProxyURL}, {"default-profile", proxyURL}} {
		for _, categorized := range []bool{false, true} {
			for _, pipeline := range []string{
				`| logfmt | line_format "{{.a.b}}"`,
				`| logfmt | line_format "{{if .a}}{{.a.b}}{{else}}{{.msg}}{{end}}"`,
				`| logfmt | line_format "{{.a.b}}" | __error__=""`,
				`| logfmt | line_format "{{.a.b}}" | drop __error__`,
				`| logfmt | line_format "{{.a.b}}" | drop __error__, __error_details__`,
			} {
				query := selector + " " + pipeline
				ls, lokiEntries := lfeEntries(t, lokiURL, query, start, end, categorized)
				ps, proxyEntries := lfeEntries(t, target.url, query, start, end, categorized)
				name := fmt.Sprintf("%s categorized=%v %s", target.name, categorized, pipeline)
				if ls != http.StatusOK || ps != ls {
					t.Errorf("%s: status loki=%d proxy=%d", name, ls, ps)
					continue
				}
				if len(lokiEntries) != len(proxyEntries) {
					t.Errorf("%s: entries loki=%d proxy=%d", name, len(lokiEntries), len(proxyEntries))
				}
				for ts, le := range lokiEntries {
					if pe := proxyEntries[ts]; !reflect.DeepEqual(le, pe) {
						t.Errorf("%s: entry\n loki  %q %v sm=%v parsed=%v\n proxy %q %v sm=%v parsed=%v", name,
							le.line, le.stream, le.metadata, le.parsed, pe.line, pe.stream, pe.metadata, pe.parsed)
					}
				}
			}
		}
	}
}
