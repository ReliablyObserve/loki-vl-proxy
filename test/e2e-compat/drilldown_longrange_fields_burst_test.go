//go:build e2e

package e2e_compat

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

type longrangeFieldsCase struct {
	name, query string
	want        map[string]map[string]string
	large       bool
}

type longrangeFieldsReply struct {
	series map[string]map[string]string
	bytes  int
	err    error
}

func longrangeFieldsDifference(want, got map[string]map[string]string) string {
	for key, expected := range want {
		actual, ok := got[key]
		if !ok {
			return "missing series {" + key + "}"
		}
		for ts, value := range expected {
			if actual[ts] != value {
				return fmt.Sprintf("{%s} t=%s: got %q, want %q", key, ts, actual[ts], value)
			}
		}
		if len(actual) != len(expected) {
			return fmt.Sprintf("{%s}: got %d points, want %d", key, len(actual), len(expected))
		}
	}
	return fmt.Sprintf("got %d series, want %d", len(got), len(want))
}

// longrangeFieldsFetch deliberately keeps the Grafana duration spelling and
// fractional end bound. It also rejects duplicate series/points and
// unordered values before canonicalising the backend's unspecified series order.
func longrangeFieldsFetch(ctx context.Context, client *http.Client, base, query string, start, end time.Time) longrangeFieldsReply {
	params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)},
		"end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {"60000ms"}, "direction": {"backward"}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/loki/api/v1/query_range?"+params.Encode(), nil)
	if err != nil {
		return longrangeFieldsReply{err: err}
	}
	req.Header.Set("X-Scope-OrgID", "0")
	req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
	resp, err := client.Do(req)
	if err != nil {
		return longrangeFieldsReply{err: err}
	}
	defer resp.Body.Close()
	const maxBody = 8 << 20
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return longrangeFieldsReply{err: err}
	}
	if len(body) > maxBody {
		return longrangeFieldsReply{err: fmt.Errorf("response exceeds fixture's %d-byte bound", maxBody)}
	}
	var envelope struct {
		Status   string   `json:"status"`
		Warnings []string `json:"warnings"`
		Error    string   `json:"error"`
		Data     struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string   `json:"metric"`
				Values [][]json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return longrangeFieldsReply{err: fmt.Errorf("HTTP %d: invalid response: %w", resp.StatusCode, err)}
	}
	if resp.StatusCode != http.StatusOK || envelope.Status != "success" || len(envelope.Warnings) != 0 || envelope.Error != "" || envelope.Data.ResultType != "matrix" {
		return longrangeFieldsReply{err: fmt.Errorf("unhealthy response HTTP %d: %.1000s", resp.StatusCode, body)}
	}
	out := map[string]map[string]string{}
	for _, series := range envelope.Data.Result {
		var labels []string
		for name, value := range series.Metric {
			labels = append(labels, name+"="+value)
		}
		sort.Strings(labels)
		key := strings.Join(labels, ",")
		if _, duplicate := out[key]; duplicate {
			return longrangeFieldsReply{err: fmt.Errorf("duplicate series {%s}", key)}
		}
		points := map[string]string{}
		previous := -1.0
		for _, pair := range series.Values {
			if len(pair) != 2 {
				return longrangeFieldsReply{err: fmt.Errorf("malformed point in {%s}", key)}
			}
			var ts float64
			var value string
			if err := json.Unmarshal(pair[0], &ts); err != nil {
				return longrangeFieldsReply{err: fmt.Errorf("invalid timestamp in {%s}: %w", key, err)}
			}
			if err := json.Unmarshal(pair[1], &value); err != nil {
				return longrangeFieldsReply{err: fmt.Errorf("invalid value in {%s}: %w", key, err)}
			}
			if ts <= previous {
				return longrangeFieldsReply{err: fmt.Errorf("unordered or duplicate point %.3f in {%s}", ts, key)}
			}
			previous = ts
			points[strconv.FormatFloat(ts, 'f', -1, 64)] = value
		}
		out[key] = points
	}
	return longrangeFieldsReply{series: out, bytes: len(body)}
}

func longrangeFieldsFixture(app string, start time.Time, span time.Duration) ([]jsonVolumeStreamLine, []longrangeFieldsCase) {
	const cardinality = 288 // below Loki's and the proxy's default 500-series cap
	anchors := []time.Duration{0, time.Minute, 2 * time.Minute, 3 * time.Minute,
		span / 6, span / 4, span / 3, span / 2, 2 * span / 3, 3 * span / 4,
		span - 2*time.Minute, span - time.Minute}
	var lines []jsonVolumeStreamLine
	for _, offset := range anchors {
		for i := range cardinality {
			id := fmt.Sprintf("%032x", i+1)
			meta := map[string]string{"trace_id": id, "span_id": fmt.Sprintf("%016x", i+1)}
			lines = append(lines, jsonVolumeStreamLine{
				ts:   start.Add(offset + 10*time.Second + time.Duration(i)*time.Millisecond),
				msg:  fmt.Sprintf(`{"request_id":%q,"user_id":%q,"total":%q}`, id, id, id),
				meta: meta, vlMeta: meta, labels: map[string]string{"env": "production", "version": fmt.Sprintf("v%d", i%3)},
			})
		}
		for i, ttl := range []string{"250ms", "1750ms"} {
			lines = append(lines, jsonVolumeStreamLine{ts: start.Add(offset + time.Duration(12+i)*time.Second), msg: "ttl=" + ttl, labels: map[string]string{"env": "production"}})
		}
	}
	// ingestJSONVolumeFixture groups these into streams. Keeping the complete
	// input sorted also keeps each resulting Loki push stream oldest-first.
	sort.Slice(lines, func(i, j int) bool { return lines[i].ts.Before(lines[j].ts) })
	selector := fmt.Sprintf(`{app=%q,env="production"}`, app)
	var cases []longrangeFieldsCase
	for _, field := range []string{"trace_id", "span_id", "version", "request_id", "user_id", "total"} {
		pipeline := ""
		if field == "request_id" || field == "user_id" || field == "total" {
			pipeline = fmt.Sprintf(` | json %s="[\"%s\"]" | drop __error__, __error_details__`, field, field)
		}
		query := fmt.Sprintf(`sum by (%s) (count_over_time(%s%s | %s!="" [1m]))`, field, selector, pipeline, field)
		want := map[string]map[string]string{}
		count, value := cardinality, "1"
		if field == "version" {
			count, value = 3, strconv.Itoa(cardinality/3)
		}
		for i := range count {
			id := fmt.Sprintf("%032x", i+1)
			switch field {
			case "span_id":
				id = fmt.Sprintf("%016x", i+1)
			case "version":
				id = fmt.Sprintf("v%d", i)
			}
			points := map[string]string{}
			for _, offset := range anchors {
				ts := float64(start.Add(offset+time.Minute).UnixMilli()) / 1000
				points[strconv.FormatFloat(ts, 'f', -1, 64)] = value
			}
			want[field+"="+id] = points
		}
		cases = append(cases, longrangeFieldsCase{name: field, query: query, want: want, large: field != "version"})
	}
	ttlPoints := map[string]string{}
	for _, offset := range anchors {
		ts := float64(start.Add(offset+time.Minute).UnixMilli()) / 1000
		ttlPoints[strconv.FormatFloat(ts, 'f', -1, 64)] = "1"
	}
	cases = append(cases, longrangeFieldsCase{name: "ttl", query: `avg_over_time(` + selector + ` | logfmt | ttl!="" | unwrap duration(ttl) | __error__="" [1m]) by ()`, want: map[string]map[string]string{"": ttlPoints}})
	return lines, cases
}

// conformance: loki_api_v1_query_range, limits/drilldown-breakdown-exact
func TestDrilldown_LongRangeFieldsBurst(t *testing.T) {
	client := &http.Client{Timeout: 30 * time.Second}
	// Leave retention headroom inside the stack's 3-day VL retention;
	// the 48h fixture still spans two days and ends at least one hour ago.
	base := time.Now().UTC().Add(-49 * time.Hour).Truncate(time.Hour)
	fixture := map[string][]jsonVolumeStreamLine{}
	type workload struct {
		start, end time.Time
		cases      []longrangeFieldsCase
	}
	workloads := map[string]workload{}
	for _, span := range []time.Duration{12 * time.Hour, 48 * time.Hour} {
		app := fmt.Sprintf("drilldown-fields-burst-%d-%dh", time.Now().UnixNano(), int(span.Hours()))
		lines, cases := longrangeFieldsFixture(app, base, span)
		fixture[app] = lines
		workloads[span.String()] = workload{base, base.Add(span + 17431*time.Millisecond), cases}
		t.Logf("fixture app=%s lines=%d range=%s: high-card=288x12, version=3x12, ttl=1x12", app, len(lines), span)
	}
	ingestJSONVolumeFixture(t, fixture)
	for name, workload := range workloads {
		t.Run(name, func(t *testing.T) {
			// Loki may have indexed a stream without exposing its newest head
			// blocks to range queries yet. Gate on the complete known answer of
			// every shape, never on a nonempty or HTTP-200 response alone.
			deadline := time.Now().Add(180 * time.Second)
			for _, shape := range workload.cases {
				for {
					reply := longrangeFieldsFetch(t.Context(), client, lokiURL, shape.query, workload.start, workload.end)
					if reply.err != nil {
						t.Fatalf("Loki %s reference: %v", shape.name, reply.err)
					}
					if reflect.DeepEqual(reply.series, shape.want) {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("Loki %s never exposed the complete reference: %s", shape.name, longrangeFieldsDifference(shape.want, reply.series))
					}
					select {
					case <-time.After(time.Second):
					case <-t.Context().Done():
						t.Fatal(t.Context().Err())
					}
				}
			}
			for _, phase := range []string{"cold", "repeat"} {
				t.Run(phase, func(t *testing.T) {
					type result struct {
						shape longrangeFieldsCase
						reply longrangeFieldsReply
					}
					results := make(chan result, len(workload.cases))
					start := make(chan struct{})
					for _, shape := range workload.cases {
						go func() {
							<-start
							results <- result{shape, longrangeFieldsFetch(t.Context(), client, proxyURL, shape.query, workload.start, workload.end)}
						}()
					}
					close(start)
					for range workload.cases {
						result := <-results
						if result.reply.err != nil {
							t.Errorf("%s: %v", result.shape.name, result.reply.err)
							continue
						}
						if !reflect.DeepEqual(result.reply.series, result.shape.want) {
							t.Errorf("%s differs from the full Loki reference: %s", result.shape.name, longrangeFieldsDifference(result.shape.want, result.reply.series))
						}
						if result.shape.large && result.reply.bytes <= 64<<10 {
							t.Errorf("%s fixture response did not exceed 64 KiB: %d bytes", result.shape.name, result.reply.bytes)
						}
					}
				})
			}
			status, body := hardeningRequest(t, http.MethodGet, proxyURL+"/ready", "", map[string]string{"X-Scope-OrgID": "0"})
			if status != http.StatusOK {
				t.Fatalf("readiness after concurrent Fields: %d %s", status, body)
			}
		})
	}
}
