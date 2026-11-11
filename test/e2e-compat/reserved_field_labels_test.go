//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// reservedFieldStreams are the streams of the reserved field fixture: service_name is the run's own name, and each
// stream has one stream label called after a field VictoriaLogs keeps for itself (_time, _stream_id, _msg) or after
// one of them in another case (_TIME), or none. The label is the key, its value the value.
var reservedFieldStreams = []struct{ label, value string }{
	{"_time", "st"}, {"_time", "10"}, {"_time", "7s"}, {"_stream_id", "si"}, {"_TIME", "up"}, {"_msg", "m"}, {"", ""},
}

// reservedFieldFixture pushes the streams alike to Loki and to VictoriaLogs' Loki push API, one line each, and returns
// the service name and the query window around them. VictoriaLogs keeps each reserved label as a tag of the stream
// only, and the _msg label replaces the line (lib/logstorage), so the answers are compared by their streams and, but
// for the _msg stream, their lines.
func reservedFieldFixture(t *testing.T) (svc string, start, end time.Time) {
	t.Helper()
	svc = fmt.Sprintf("rf-labels-%d", time.Now().UnixNano())
	base := time.Now().Add(-time.Minute).Truncate(time.Second)
	var streams []any
	for i, s := range reservedFieldStreams {
		labels := map[string]string{"service_name": svc}
		if s.label != "" {
			labels[s.label] = s.value
		}
		ts := base.Add(time.Duration(i+1) * time.Second)
		streams = append(streams, map[string]any{"stream": labels, "values": [][]string{{strconv.FormatInt(ts.UnixNano(), 10), fmt.Sprintf("rf line %d %s", i, svc)}}})
	}
	push, _ := json.Marshal(map[string]any{"streams": streams})
	for _, target := range []string{lokiURL + "/loki/api/v1/push", vlURL + "/insert/loki/api/v1/push"} {
		status, body := hardeningRequest(t, http.MethodPost, target, string(push), map[string]string{"Content-Type": "application/json"})
		if status/100 != 2 {
			t.Fatalf("push %s: %d %s", target, status, body)
		}
	}
	forceVLFlush(t)
	return svc, base.Add(-time.Minute), base.Add(30 * time.Second)
}

// logStreamsByLabels is, for each stream of a query_range log answer, the fixture label it carries (or "none") and
// its lines, sorted. Other labels (detected_level, the service name) are left out: the fixture label is what the
// queries select on.
func logStreamsByLabels(payload map[string]interface{}) []string {
	var out []string
	for _, item := range extractArray(extractMap(payload, "data"), "result") {
		stream, _ := item.(map[string]interface{})
		labels, _ := stream["stream"].(map[string]interface{})
		label := "none"
		for _, s := range reservedFieldStreams {
			if v, ok := labels[s.label]; ok && s.label != "" {
				label = fmt.Sprintf("%s=%v", s.label, v)
			}
		}
		var lines []string
		values, _ := stream["values"].([]interface{})
		for _, v := range values {
			if pair, _ := v.([]interface{}); len(pair) == 2 {
				lines = append(lines, fmt.Sprint(pair[1]))
			}
		}
		if _, msg := labels["_msg"]; msg {
			lines = []string{fmt.Sprintf("%d line(s)", len(lines))} // VictoriaLogs stored the label as the line
		}
		sort.Strings(lines)
		out = append(out, label+" "+strings.Join(lines, " | "))
	}
	sort.Strings(out)
	return out
}

// A label called _time, _stream_id or _msg is filtered as the stream tag VictoriaLogs keeps it as, and _TIME as the
// field it is: the proxy answers the selector matchers, the label filters and the comparisons on them like Loki,
// where it answered VictoriaLogs' parse error (_time:="st"), a time range (`| _time>=5s` was every line of the last
// 5s) or the line filter (_msg:="m"). Both sides hold the fixture before anything is compared, and Loki's answer is
// pinned to the streams the fixture has.
//
// conformance: semantics/translator-writes-reserved-field-names, loki_api_v1_query_range
func TestCompat_ReservedFieldLabelsLikeLoki(t *testing.T) {
	headers := map[string]string{"X-Scope-OrgID": "0"}
	svc, start, end := reservedFieldFixture(t)
	params := func(query string) url.Values {
		return url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)},
			"end": {strconv.FormatInt(end.UnixNano(), 10)}, "limit": {"1000"}}
	}
	selector := fmt.Sprintf(`{service_name=%q`, svc)

	// Both sides hold every stream.
	deadline := time.Now().Add(2 * time.Minute)
	for {
		ls := logStreamsByLabels(mustQueryRange(t, lokiURL, params(selector+"}"), headers))
		ps := logStreamsByLabels(mustQueryRange(t, proxyURL, params(selector+"}"), headers))
		if len(ls) == len(reservedFieldStreams) && len(ps) == len(reservedFieldStreams) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fixture not visible on both sides: Loki %v, proxy %v", ls, ps)
		}
		time.Sleep(time.Second)
	}

	for _, tc := range []struct {
		query   string
		streams int // the streams Loki answers with
	}{
		{selector + `, _time="st"}`, 1},
		{selector + `, _time!="st"}`, 6},
		{selector + `, _time=~"1.*|7s"}`, 2},
		{selector + `, _time=""}`, 4},
		{selector + `} | _time="10"`, 1},
		{selector + `} | _time!~"s.*"`, 5},
		{selector + `, _time=~"[0-9]+"} | _time > 5`, 1},
		{selector + `, _time=~".*s"} | _time >= 5s`, 1},
		{selector + `} | _stream_id="si"`, 1},
		{selector + `, _stream_id!="si"}`, 6},
		{selector + `, _TIME="up"}`, 1},
		{selector + `} | _TIME=~"u.*" or _time="st"`, 2},
		{selector + `, _msg="m"}`, 1},
		{selector + `} | _msg!="m"`, 6},
	} {
		ls, lb, lp := queryRangeGET(t, lokiURL, params(tc.query), headers)
		ps, pb, pp := queryRangeGET(t, proxyURL, params(tc.query), headers)
		if ls != http.StatusOK {
			t.Fatalf("%s: Loki answered %d %s", tc.query, ls, lb)
		}
		lStreams, pStreams := logStreamsByLabels(lp), logStreamsByLabels(pp)
		if len(lStreams) != tc.streams {
			t.Fatalf("%s: Loki answered the streams %q, the test expects %d", tc.query, lStreams, tc.streams)
		}
		if ps != ls || strings.Join(pStreams, "\n") != strings.Join(lStreams, "\n") {
			t.Errorf("%s:\n proxy %d %q (%s)\n Loki  %d %q", tc.query, ps, pStreams, pb, ls, lStreams)
		}
	}
}
