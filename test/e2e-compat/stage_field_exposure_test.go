//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	stageFixtureOnce sync.Once
	stageFixtureApp  string
	stageFixtureEnd  time.Time
)

// ensureStageFixture pushes the same events to Loki and to VictoriaLogs'
// Loki push endpoint as two streams: <app>-json holds JSON lines (one nested
// object) with OTel structured metadata, the VictoriaLogs copy carrying the
// line under _msg as the UI log generator sends it, so VictoriaLogs holds the
// line and its keys as fields; <app>-logfmt holds logfmt lines with
// structured metadata; <app>-mix holds older plain lines with user as
// structured metadata and newer JSON lines holding user as a key. Both
// backends are flushed and Loki's index must count
// every entry before a comparison runs.
func ensureStageFixture(t *testing.T) (string, time.Time) {
	t.Helper()
	stageFixtureOnce.Do(func() {
		app := fmt.Sprintf("stage-fields-%d", time.Now().UnixNano())
		base := time.Now().Add(-40 * time.Second)
		meta := map[string]string{"trace_id": "4bf92f3577b34da6a3ce929d0e0e4736", "k8s.pod.name": "stage-pod-1"}
		streams := map[string][]string{
			app + "-json": {
				`{"msg":"login ok","user":"u1","status":200,"svc":{"name":"api"},"tags":["a","b"]}`,
				`{"msg":"login failed","user":"u2","status":401,"svc":{"name":"api"},"tags":["a"]}`,
				`{"msg":"logout","user":"u1","status":200,"svc":{"name":"web"},"tags":[]}`,
			},
			app + "-logfmt": {
				`msg="login ok" user=u1 status=200`,
				`msg="login failed" user=u2 status=401`,
				`msg=logout user=u1 status=200`,
			},
		}
		// <app>-mix: three older plain lines with user as structured metadata,
		// three newer JSON lines holding user as a key.
		mix := map[string][]interface{}{}
		for i := 0; i < 6; i++ {
			ts := strconv.FormatInt(base.Add(time.Duration(i)*time.Second).UnixNano(), 10)
			if i < 3 {
				v := []interface{}{ts, fmt.Sprintf("plain line %d", i), map[string]string{"user": "u1"}}
				mix["loki"], mix["vl"] = append(mix["loki"], v), append(mix["vl"], v)
				continue
			}
			line := fmt.Sprintf(`{"msg":"json line %d","user":"u1"}`, i)
			mix["loki"] = append(mix["loki"], []interface{}{ts, line})
			mix["vl"] = append(mix["vl"], []interface{}{ts, fmt.Sprintf(`{"msg":"json line %d","user":"u1","_msg":%q}`, i, line)})
		}
		for _, target := range []struct{ url, side string }{{lokiURL + "/loki/api/v1/push", "loki"}, {vlURL + "/insert/loki/api/v1/push", "vl"}} {
			body, _ := json.Marshal(map[string]interface{}{"streams": []map[string]interface{}{{"stream": map[string]string{"app": app + "-mix", "env": "stage-fields"}, "values": mix[target.side]}}})
			resp, err := http.Post(target.url, "application/json", strings.NewReader(string(body)))
			if err != nil {
				t.Fatalf("push %s: %v", target.url, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode/100 != 2 {
				t.Fatalf("push %s: status %d", target.url, resp.StatusCode)
			}
		}
		for name, lines := range streams {
			var lokiValues, vlValues []interface{}
			for i, line := range lines {
				ts := strconv.FormatInt(base.Add(time.Duration(i)*time.Second).UnixNano(), 10)
				lokiValues = append(lokiValues, []interface{}{ts, line, meta})
				vlLine := line
				if strings.HasPrefix(line, "{") {
					var obj map[string]interface{}
					_ = json.Unmarshal([]byte(line), &obj)
					obj["_msg"] = line
					encoded, _ := json.Marshal(obj)
					vlLine = string(encoded)
				}
				vlValues = append(vlValues, []interface{}{ts, vlLine, meta})
			}
			labels := map[string]string{"app": name, "env": "stage-fields"}
			for _, target := range []struct {
				url    string
				values []interface{}
			}{
				{lokiURL + "/loki/api/v1/push", lokiValues},
				{vlURL + "/insert/loki/api/v1/push", vlValues},
			} {
				body, _ := json.Marshal(map[string]interface{}{"streams": []map[string]interface{}{{"stream": labels, "values": target.values}}})
				resp, err := http.Post(target.url, "application/json", strings.NewReader(string(body)))
				if err != nil {
					t.Fatalf("push %s: %v", target.url, err)
				}
				_ = resp.Body.Close()
				if resp.StatusCode/100 != 2 {
					t.Fatalf("push %s: status %d", target.url, resp.StatusCode)
				}
			}
		}
		forceVLFlush(t)
		if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/flush", "", nil); status >= 300 {
			t.Fatalf("Loki flush: %d %s", status, body)
		}
		end := base.Add(10 * time.Second)
		params := lineFieldsWindow(end)
		params.Set("query", fmt.Sprintf(`{app=~%q}`, app+"-.*"))
		deadline := time.Now().Add(90 * time.Second)
		for {
			var stats struct{ Entries int }
			status, body := rejectedQueryGet(t, lokiURL, "/loki/api/v1/index/stats", params, "0", nil)
			if status == http.StatusOK && json.Unmarshal(body, &stats) == nil && stats.Entries == 12 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("Loki index/stats never counted the flushed fixture: %d %s", status, body)
			}
			time.Sleep(2 * time.Second)
		}
		stageFixtureApp, stageFixtureEnd = app, end
	})
	if stageFixtureApp == "" {
		t.Fatal("stage-fields fixture not ingested")
	}
	return stageFixtureApp, stageFixtureEnd
}

// stageEntry is one log entry of a query_range answer: its line, stream
// labels and, with categorize-labels, its structuredMetadata and parsed
// labels, each as sorted name=value pairs (detected_level and the fixture's
// env label left out).
type stageEntry struct {
	Line   string
	Stream []string
	SM     []string
	Parsed []string
}

func stageEntries(t *testing.T, base, query string, end time.Time, categorize bool) []stageEntry {
	t.Helper()
	params := lineFieldsWindow(end)
	params.Set("query", query)
	params.Set("limit", "100")
	headers := map[string]string{}
	if categorize {
		headers["X-Loki-Response-Encoding-Flags"] = "categorize-labels"
	}
	status, body := rejectedQueryGet(t, base, "/loki/api/v1/query_range", params, "0", headers)
	var resp struct {
		Status string
		Data   struct {
			Result []struct {
				Stream map[string]string   `json:"stream"`
				Values [][]json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &resp) != nil || resp.Status != "success" {
		t.Fatalf("%s %s: %d %.300s", base, query, status, body)
	}
	pairs := func(m map[string]string) []string {
		out := []string{}
		for k, v := range m {
			if k != "detected_level" && k != "env" {
				out = append(out, k+"="+v)
			}
		}
		sort.Strings(out)
		return out
	}
	var entries []stageEntry
	for _, r := range resp.Data.Result {
		for _, v := range r.Values {
			e := stageEntry{Stream: pairs(r.Stream)}
			_ = json.Unmarshal(v[1], &e.Line)
			if len(v) > 2 {
				var meta struct {
					StructuredMetadata map[string]string `json:"structuredMetadata"`
					Parsed             map[string]string `json:"parsed"`
				}
				_ = json.Unmarshal(v[2], &meta)
				e.SM, e.Parsed = pairs(meta.StructuredMetadata), pairs(meta.Parsed)
			}
			entries = append(entries, e)
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Line < entries[j].Line })
	return entries
}

// sameEntries reports whether two answers hold the same entries (an empty
// answer equals another empty one).
func sameEntries(a, b []stageEntry) bool {
	return (len(a) == 0 && len(b) == 0) || reflect.DeepEqual(a, b)
}

// TestCompat_StageFieldExposureLikeLoki compares the Loki-compatible proxies
// with Loki, entry by entry, for each LogQL stage that adds labels or not, on
// data both hold identically: each entry's line, its stream labels, and with
// categorize-labels (as Grafana sends it) its structured metadata and parsed
// labels with their values; without categorize-labels the stream labels,
// which hold structured metadata and parsed labels in both. An extraction
// list gives each of its labels a value, empty when the line lacks the key;
// a line_format a later stage reads, and label_format reading a key no stage
// exposed, are covered too.
//
// conformance: profiles/stage-field-exposure, profiles/parsed-fields-without-parser, profiles/label-filter-on-line-field, profiles/uncategorized-streams-carry-structured-metadata, loki_api_v1_query_range
func TestCompat_StageFieldExposureLikeLoki(t *testing.T) {
	app, end := ensureStageFixture(t)
	j := fmt.Sprintf(`{app="%s-json"}`, app)
	l := fmt.Sprintf(`{app="%s-logfmt"}`, app)
	queries := []string{
		j,
		j + ` | json`,
		j + ` | json user`,
		j + ` | json u="user", n="svc.name"`,
		j + ` | json nosuch, user`,
		j + ` | json s="svc", m="msg"`,
		j + ` | logfmt nosuch`,
		j + ` | logfmt`,
		j + " | regexp `\"user\":\"(?P<who>[^\"]+)\"`",
		j + " | pattern `{\"msg\":\"<m>\",<_>`",
		j + ` | label_format x="{{.app}}"`,
		j + ` | unpack`,
		j + ` | line_format "{{.msg}}"`,
		j + ` | json | line_format "{{.msg}}"`,
		j + ` | json | user="u1"`,
		j + ` | json | label_format who=user`,
		j + ` | json | label_format user=nosuch`,
		j + ` | label_format u=user`,
		j + ` | label_format x="{{.user}}-{{.app}}"`,
		j + ` | json | line_format "{{.user}}" |= "u"`,
		j + ` | line_format "{{.msg}}" | decolorize`,
		j + ` | json user | line_format "{{.user}}" |= "u" | label_format l="{{.user}}"`,
		j + ` | user="u1"`,
		j + ` | user=~"u.*"`,
		j + ` | status > 100`,
		j + ` | json user | status="200"`,
		j + ` | user="u1" | json`,
		j + ` | k8s_pod_name="stage-pod-1"`,
		l,
		l + ` | logfmt`,
		l + ` | logfmt user`,
		l + ` | logfmt nosuch, code="status"`,
		l + ` | logfmt | user="u1"`,
		l + ` | logfmt user | status="200"`,
		l + ` | logfmt | label_format code=status`,
		l + ` | user="u1"`,
	}
	// Loki answers these with no entries: each filters on a label no stage
	// before it adds. Every other query must return entries on Loki.
	empty := map[string]bool{
		j + ` | user="u1"`:                  true,
		j + ` | user=~"u.*"`:                true,
		j + ` | status > 100`:               true,
		j + ` | json user | status="200"`:   true,
		j + ` | user="u1" | json`:           true,
		l + ` | logfmt user | status="200"`: true,
		l + ` | user="u1"`:                  true,
	}
	for _, query := range queries {
		if got := stageEntries(t, lokiURL, query, end, true); (len(got) == 0) != empty[query] {
			t.Fatalf("Loki fixture drifted: %s returned %d entries", query, len(got))
		}
	}
	for _, target := range []struct{ name, url string }{
		{"parity (13100)", proxyURL},
		{"drilldown default (13110)", patternsAutodetectProxyURL},
	} {
		t.Run(target.name, func(t *testing.T) {
			for _, query := range queries {
				loki := stageEntries(t, lokiURL, query, end, true)
				if got := stageEntries(t, target.url, query, end, true); !sameEntries(got, loki) {
					t.Errorf("%s (categorize-labels):\nproxy %+v\nloki  %+v", query, got, loki)
				}
				lokiPlain := stageEntries(t, lokiURL, query, end, false)
				if got := stageEntries(t, target.url, query, end, false); !sameEntries(got, lokiPlain) {
					t.Errorf("%s:\nproxy %+v\nloki  %+v", query, got, lokiPlain)
				}
			}
		})
	}
}

// TestCompat_DetectedFieldsNestedJSONPathLikeLoki: detected_fields names a
// nested JSON key by its underscore-joined path and reports the path in
// jsonPath, as Loki does.
//
// conformance: profiles/detected-fields-dotted-json-keys, loki_api_v1_detected_fields
func TestCompat_DetectedFieldsNestedJSONPathLikeLoki(t *testing.T) {
	app, _ := ensureStageFixture(t)
	selector := fmt.Sprintf(`{app="%s-json"}`, app)
	loki := matrixDetectedFields(t, lokiURL, selector)
	if want := fmt.Sprint([]string{"svc", "name"}); fmt.Sprint(loki["svc_name"].JSONPath) != want {
		t.Fatalf("Loki fixture drifted: svc_name jsonPath %v", loki["svc_name"].JSONPath)
	}
	for _, target := range []struct{ name, url string }{
		{"parity (13100)", proxyURL},
		{"drilldown default (13110)", patternsAutodetectProxyURL},
	} {
		got := matrixDetectedFields(t, target.url, selector)
		for label, want := range loki {
			if label == "detected_level" {
				continue
			}
			if g := got[label]; fmt.Sprint(g.Parsers, g.JSONPath) != fmt.Sprint(want.Parsers, want.JSONPath) {
				t.Errorf("%s detected_fields %s: proxy %+v, loki %+v", target.name, label, g, want)
			}
		}
	}
}

// TestCompat_LabelFilterOnLineFieldFillsLimitLikeLoki: a label filter whose
// label is structured metadata on older plain lines and a key of newer JSON
// lines returns Loki's lines up to the limit: the rows the proxy drops after
// VictoriaLogs applied the limit are replaced from further pages, in both
// directions.
//
// conformance: profiles/label-filter-on-line-field, loki_api_v1_query_range
func TestCompat_LabelFilterOnLineFieldFillsLimitLikeLoki(t *testing.T) {
	app, end := ensureStageFixture(t)
	query := fmt.Sprintf(`{app="%s-mix"} | user="u1"`, app)
	lines := func(base, direction string, categorize bool) []string {
		params := lineFieldsWindow(end)
		params.Set("query", query)
		params.Set("limit", "3")
		params.Set("direction", direction)
		headers := map[string]string{}
		if categorize {
			headers["X-Loki-Response-Encoding-Flags"] = "categorize-labels"
		}
		status, body := rejectedQueryGet(t, base, "/loki/api/v1/query_range", params, "0", headers)
		var resp struct {
			Data struct {
				Result []struct {
					Values [][]json.RawMessage `json:"values"`
				} `json:"result"`
			} `json:"data"`
		}
		if status != http.StatusOK || json.Unmarshal(body, &resp) != nil {
			t.Fatalf("%s: %d %.300s", base, status, body)
		}
		var out []string
		for _, r := range resp.Data.Result {
			for _, v := range r.Values {
				var line string
				_ = json.Unmarshal(v[1], &line)
				out = append(out, line)
			}
		}
		sort.Strings(out)
		return out
	}
	for _, direction := range []string{"backward", "forward"} {
		want := lines(lokiURL, direction, true)
		if fmt.Sprint(want) != "[plain line 0 plain line 1 plain line 2]" {
			t.Fatalf("Loki fixture drifted: %s %v", direction, want)
		}
		for _, target := range []struct{ name, url string }{
			{"parity (13100)", proxyURL},
			{"drilldown default (13110)", patternsAutodetectProxyURL},
		} {
			for _, categorize := range []bool{true, false} {
				if got := lines(target.url, direction, categorize); !reflect.DeepEqual(got, want) {
					t.Errorf("%s %s categorize-labels=%v: %v, Loki %v", target.name, direction, categorize, got, want)
				}
			}
		}
	}
}
