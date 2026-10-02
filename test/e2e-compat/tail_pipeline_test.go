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
	"strings"
	"testing"
	"time"
)

// Live tail runs the query's pipeline on every entry in Loki (the
// ingester's tailer), with the stream labels of a query response: the index
// labels only for a query without stages, every label of the entry (structured
// metadata, parsed labels, detected_level) once a stage runs, and the
// categorize-labels split with the encoding flag. These tests push one
// fixture to Loki and VictoriaLogs and compare the entries both tails
// deliver, line and labels, for queries with stages of every kind.

// tailPipelineFixture is pushed one entry at a time, after every tail is
// live. Lines carry no key that collides with a structured metadata name:
// the _extracted suffix Loki gives such a key is a query response
// difference of its own.
var tailPipelineFixture = []struct {
	line string
	sm   map[string]string
}{
	{`{"level":"info","msg":"json one","user":"u1"}`, map[string]string{"service.version": "1.2", "foo": "bar"}},
	{`{"level":"warn","msg":"json two","user":"u2"}`, map[string]string{"service.version": "1.3"}},
	{`{"level":"error","msg":"json three","status":500}`, nil},
	{`level=warn msg="logfmt four" user=u4`, nil},
	{`level=info msg="logfmt five" status=200`, map[string]string{"trace_id": "abc"}},
	{"plain six error happened", map[string]string{"trace_id": "def"}},
	{"\x1b[31mred seven\x1b[0m level=debug", nil},
}

// tailPipelineSentinels: every query below keeps at least one of them, so a
// tail is known to be live once it delivers one.
var tailPipelineSentinels = []struct {
	line string
	sm   map[string]string
}{
	{`{"level":"warn","msg":"json sentinel","user":"u1","status":500}`, map[string]string{"service.version": "s"}},
	{`level=warn msg="logfmt sentinel" user=u1 status=500`, nil},
}

var tailPipelineQueries = []string{
	``,
	`|= "json"`,
	`!= "json"`,
	`| logfmt`,
	`| logfmt | level="warn"`,
	`| logfmt | line_format "{{.msg}}"`,
	`| logfmt | drop __error__, __error_details__`,
	`|= "json" | json`,
	`|= "json" | json | service_version!=""`,
	`|= "json" | json | user="u1"`,
	`|= "json" | json | status>=500`,
	`|= "json" | json | line_format "{{.user}}: {{.msg}}"`,
	`|= "json" | json | label_format who=user`,
	`|= "json" | json | drop user`,
	`| decolorize`,
}

type tailPipelineEntry struct {
	line                     string
	stream, metadata, parsed map[string]string
}

// tailPipelineTail is a tail websocket whose frames are decoded by timestamp.
type tailPipelineTail struct {
	*dlTail
	entries map[string]tailPipelineEntry
	flags   []string
}

func (s *tailPipelineTail) read(t *testing.T, until func() bool, deadline time.Time) {
	t.Helper()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	for !until() {
		select {
		case msg, ok := <-s.frames:
			if !ok {
				t.Fatalf("%s tail closed", s.url)
			}
			var frame struct {
				Streams []struct {
					Stream map[string]string   `json:"stream"`
					Values [][]json.RawMessage `json:"values"`
				} `json:"streams"`
				EncodingFlags []string `json:"encodingFlags"`
			}
			if err := json.Unmarshal(msg, &frame); err != nil {
				t.Fatalf("decode tail frame: %v: %s", err, msg)
			}
			if len(frame.Streams) > 0 {
				s.flags = frame.EncodingFlags
			}
			for _, st := range frame.Streams {
				for _, v := range st.Values {
					var ts string
					e := tailPipelineEntry{stream: st.Stream}
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
					s.entries[ts] = e
				}
			}
		case <-timer.C:
			return
		}
	}
}

// tailPipelineLokiCount is the number of fixture entries Loki's query_range
// returns for query: what each tail must deliver.
func tailPipelineLokiCount(t *testing.T, query string, start, end time.Time) int {
	t.Helper()
	params := url.Values{}
	params.Set("query", query)
	params.Set("start", strconv.FormatInt(start.UnixNano(), 10))
	params.Set("end", strconv.FormatInt(end.UnixNano()+1, 10))
	params.Set("limit", "1000")
	req, _ := http.NewRequest(http.MethodGet, lokiURL+"/loki/api/v1/query_range?"+params.Encode(), nil)
	req.Header.Set("X-Scope-OrgID", "0")
	resp, err := dlHTTP.Do(req)
	if err != nil {
		t.Fatalf("Loki query_range: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var out dlStreamsResponse
	if resp.StatusCode != http.StatusOK || json.Unmarshal(body, &out) != nil || len(out.Warnings) > 0 {
		t.Fatalf("Loki query_range %s: %d %s", query, resp.StatusCode, body)
	}
	n := 0
	for _, s := range out.Data.Result {
		n += len(s.Values)
	}
	return n
}

// conformance: loki_api_v1_tail
func TestFeature_Tail_PipelineParity(t *testing.T) {
	for _, target := range []struct{ name, url string }{
		{"loki-profile", patternsAutodetectProxyURL},
		{"default-profile", proxyURL},
		{"synthetic", tailProxyURL},
	} {
		for _, categorized := range []bool{false, true} {
			name := target.name + "/default"
			if categorized {
				name = target.name + "/categorize-labels"
			}
			t.Run(name, func(t *testing.T) {
				// Loki allows a few concurrent tails per tenant; the queries
				// run in groups.
				for start := 0; start < len(tailPipelineQueries); start += 4 {
					group := tailPipelineQueries[start:min(start+4, len(tailPipelineQueries))]
					tailPipelineGroup(t, target.url, categorized, group)
				}
			})
		}
	}
}

func tailPipelineGroup(t *testing.T, proxy string, categorized bool, pipelines []string) {
	t.Helper()
	app := fmt.Sprintf("tailpipe%d", time.Now().UnixNano())
	header := http.Header{}
	header.Set("X-Scope-OrgID", "0")
	if categorized {
		header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
	}
	type pair struct {
		query       string
		loki, proxy *tailPipelineTail
	}
	var pairs []pair
	var all []*dlTail
	// Loki allows a few concurrent tails per tenant: close this group's
	// before the next group dials.
	defer func() {
		for _, tl := range all {
			if tl.conn != nil {
				_ = tl.conn.Close()
			}
		}
	}()
	for _, p := range pipelines {
		q := strings.TrimSpace(fmt.Sprintf(`{app=%q} %s`, app, p))
		params := url.Values{"query": {q}, "start": {strconv.FormatInt(time.Now().UnixNano(), 10)}}
		pr := pair{
			query: q,
			loki:  &tailPipelineTail{dlTail: dlTailDial(t, lokiURL, params, header), entries: map[string]tailPipelineEntry{}},
			proxy: &tailPipelineTail{dlTail: dlTailDial(t, proxy, params, header), entries: map[string]tailPipelineEntry{}},
		}
		pairs = append(pairs, pr)
		all = append(all, pr.loki.dlTail, pr.proxy.dlTail)
	}

	// Push sentinels until every tail delivered one.
	sentinels := map[string]bool{}
	deadline := time.Now().Add(dlTailSubscribeWait)
	var pending []string
	for attempt := 0; ; attempt++ {
		if time.Now().After(deadline) {
			t.Fatalf("tails not live after %s: %v", dlTailSubscribeWait, pending)
		}
		pending = pending[:0]
		for _, sentinel := range tailPipelineSentinels {
			at := strconv.FormatInt(time.Now().UnixNano(), 10)
			sentinels[at] = true
			value := []interface{}{at, sentinel.line}
			if sentinel.sm != nil {
				value = append(value, sentinel.sm)
			}
			payload := dlJSON(map[string]interface{}{"streams": []interface{}{map[string]interface{}{
				"stream": map[string]string{"app": app, "case": "sentinel"},
				"values": []interface{}{value},
			}}})
			dlPost(t, vlURL+"/insert/loki/api/v1/push?disable_message_parsing=1", "application/json", payload)
			dlPost(t, lokiURL+"/loki/api/v1/push", "application/json", payload)
			time.Sleep(5 * time.Millisecond)
		}
		live := true
		attemptEnd := time.Now().Add(2 * time.Second)
		for _, pr := range pairs {
			for _, tl := range []*tailPipelineTail{pr.loki, pr.proxy} {
				tl.read(t, func() bool {
					for ts := range tl.entries {
						if sentinels[ts] {
							return true
						}
					}
					return false
				}, attemptEnd)
				got := false
				for ts := range tl.entries {
					got = got || sentinels[ts]
				}
				if !got {
					pending = append(pending, tl.url+" "+pr.query)
				}
				live = live && got
			}
		}
		if live {
			break
		}
	}

	// Loki's tail drops an entry older than one it already sent, so the
	// entries go out one push at a time in timestamp order.
	var first, last time.Time
	for _, f := range tailPipelineFixture {
		at := time.Now()
		if first.IsZero() {
			first = at
		}
		last = at
		value := []interface{}{strconv.FormatInt(at.UnixNano(), 10), f.line}
		if f.sm != nil {
			value = append(value, f.sm)
		}
		payload := dlJSON(map[string]interface{}{"streams": []interface{}{map[string]interface{}{
			"stream": map[string]string{"app": app, "env": "tail"}, "values": []interface{}{value},
		}}})
		dlPost(t, vlURL+"/insert/loki/api/v1/push?disable_message_parsing=1", "application/json", payload)
		dlPost(t, lokiURL+"/loki/api/v1/push", "application/json", payload)
		time.Sleep(150 * time.Millisecond)
	}

	readDeadline := time.Now().Add(dlTailFramesWait)
	for _, pr := range pairs {
		want := tailPipelineLokiCount(t, pr.query, first, last)
		fixtureEntries := func(tl *tailPipelineTail) map[string]tailPipelineEntry {
			out := map[string]tailPipelineEntry{}
			for ts, e := range tl.entries {
				if !sentinels[ts] {
					out[ts] = e
				}
			}
			return out
		}
		for _, tl := range []*tailPipelineTail{pr.loki, pr.proxy} {
			tl.read(t, func() bool { return len(fixtureEntries(tl)) >= want }, readDeadline)
		}
		lokiEntries, proxyEntries := fixtureEntries(pr.loki), fixtureEntries(pr.proxy)
		if len(lokiEntries) != want {
			t.Fatalf("%s: Loki tail delivered %d entries, its query_range %d", pr.query, len(lokiEntries), want)
		}
		if len(proxyEntries) != want {
			t.Errorf("%s: proxy tail delivered %d entries, Loki %d", pr.query, len(proxyEntries), want)
		}
		if categorized && !reflect.DeepEqual(pr.loki.flags, pr.proxy.flags) {
			t.Errorf("%s: encodingFlags loki=%v proxy=%v", pr.query, pr.loki.flags, pr.proxy.flags)
		}
		for ts, le := range lokiEntries {
			pe, ok := proxyEntries[ts]
			if !ok {
				t.Errorf("%s: proxy tail lacks %q", pr.query, le.line)
				continue
			}
			if !reflect.DeepEqual(le, pe) {
				t.Errorf("%s: tail entry\n loki  %q %v sm=%v parsed=%v\n proxy %q %v sm=%v parsed=%v", pr.query,
					le.line, le.stream, le.metadata, le.parsed, pe.line, pe.stream, pe.metadata, pe.parsed)
			}
		}
	}
}
