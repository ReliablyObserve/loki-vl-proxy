package querycorpus_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// A folded read (internal/proxy/series_fold_read.go) is a stats_query_range read sent to /select/logsql/query, whose
// stats pipes group by the bucket themselves: the proxy sends the bounds stats_query_range reads for its parameters
// (proxy.SeriesFoldQueryParams). Real VictoriaLogs (the serve oracle, test/oracle/vl, v1.53.0 and v1.49.0) answers the
// same buckets both ways: the points of a stats query grouped by a key on stats_query_range, and the rows of the same
// stats grouped by the bucket (_time:<step> offset <offset>) and the key on /select/logsql/query.
//
// The reads (seriesFoldBoundsReads) are on the minute's grid, 17 s off it, on a grid of 20 s offset by 17 s and 1 ns,
// on 1 s buckets, and over a range the step does not divide; the lines lie on the first and the last nanosecond of
// buckets and of the reads, and between them. The answers are recorded in seriesFoldBoundsPath (LVP_RUN_ORACLES=1
// records them again); a change of the bounds the proxy sends makes the recording stale.

const seriesFoldBoundsPath = "testdata/series-fold-bounds.json"

// seriesFoldBoundsStart is the first bucket edge of the reads.
var seriesFoldBoundsStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// seriesFoldBoundsReads are the reads: name, from and to (after seriesFoldBoundsStart), and step.
var seriesFoldBoundsReads = []struct {
	name     string
	from, to time.Duration
	step     time.Duration
}{
	{"minute", 0, 6 * time.Minute, time.Minute},
	{"minute off the grid", 17 * time.Second, 6*time.Minute + 17*time.Second, time.Minute},
	{"20s off the grid", 17*time.Second + time.Nanosecond, 3 * time.Minute, 20 * time.Second},
	{"1s", time.Minute - 3*time.Second, time.Minute + 4*time.Second, time.Second},
	{"not divided", 0, 5*time.Minute + 30*time.Second, 2 * time.Minute},
}

// seriesFoldBoundsRecord is the recording of one read on one version: the parameters of both endpoints and their
// answers.
type seriesFoldBoundsRecord struct {
	Range, Query       url.Values
	RangeBody, RowBody string
}

// seriesFoldBoundsParams are a read's stats_query_range parameters, as the window evaluator sets them
// (setSlidingStatsRangeParams: the end one nanosecond on, the offset the shift of the start past the grid).
func seriesFoldBoundsParams(from, to, step time.Duration) url.Values {
	start := seriesFoldBoundsStart.Add(from)
	shift := ((start.UnixNano()+1)%int64(step) + int64(step)) % int64(step)
	return url.Values{"query": {`app:="bounds" | stats by (k) sum(n) as s, count() as c`}, "start": {start.Format(time.RFC3339Nano)},
		"end": {seriesFoldBoundsStart.Add(to).Add(time.Nanosecond).Format(time.RFC3339Nano)}, "step": {strconv.FormatInt(int64(step/time.Second), 10) + "s"},
		"offset": {"-" + strconv.FormatInt(shift, 10) + "ns"}}
	// A step under a second does not occur: the proxy's buckets are whole seconds here.
}

// seriesFoldBoundsQuery is the read's /select/logsql/query form.
func seriesFoldBoundsQuery(t *testing.T, rangeParams url.Values) url.Values {
	t.Helper()
	q, err := proxy.SeriesFoldQueryParams(rangeParams)
	if err != nil {
		t.Fatal(err)
	}
	q.Set("query", `app:="bounds" | stats by (_time:`+rangeParams.Get("step")+` offset `+rangeParams.Get("offset")+`, k) sum(n) as s, count() as c`)
	return q
}

// TestSeriesFoldBoundsMatchStatsQueryRange checks the recorded answers: the /select/logsql/query rows of each read hold
// the stats_query_range points, bucket by bucket, value by value, and the bounds sent are the proxy's.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func TestSeriesFoldBoundsMatchStatsQueryRange(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordSeriesFoldBounds(t)
	}
	raw, err := os.ReadFile(seriesFoldBoundsPath)
	if err != nil {
		t.Fatal(err)
	}
	var recorded map[string]map[string]seriesFoldBoundsRecord
	if err := json.Unmarshal(raw, &recorded); err != nil {
		t.Fatal(err)
	}
	for _, prof := range diffProfiles() {
		for _, read := range seriesFoldBoundsReads {
			rec, ok := recorded[prof.version][read.name]
			params := seriesFoldBoundsParams(read.from, read.to, read.step)
			if !ok || rec.Range.Encode() != params.Encode() || rec.Query.Encode() != seriesFoldBoundsQuery(t, params).Encode() {
				t.Fatalf("%s %s: not recorded, or stale: record again with LVP_RUN_ORACLES=1", prof.version, read.name)
			}
			points := seriesFoldRangePoints(t, rec.RangeBody)
			rows := seriesFoldRowPoints(t, rec.RowBody)
			if len(points) == 0 || !equalPoints(points, rows) {
				t.Errorf("%s %s: stats_query_range %v, /select/logsql/query %v", prof.version, read.name, points, rows)
			}
		}
	}
}

// seriesFoldRangePoints are the points of a stats_query_range answer: "<name> <k> <time in seconds>" to the value.
func seriesFoldRangePoints(t *testing.T, body string) map[string]string {
	t.Helper()
	var answer struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][2]any          `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &answer); err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, s := range answer.Data.Result {
		for _, v := range s.Values {
			out[s.Metric["__name__"]+" "+s.Metric["k"]+" "+strconv.FormatFloat(v[0].(float64), 'f', -1, 64)] = v[1].(string)
		}
	}
	return out
}

// seriesFoldRowPoints are the rows of a /select/logsql/query answer in seriesFoldRangePoints' form.
func seriesFoldRowPoints(t *testing.T, body string) map[string]string {
	t.Helper()
	out := map[string]string{}
	sc := bufio.NewScanner(bytes.NewReader([]byte(body)))
	for sc.Scan() {
		var row map[string]string
		if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
			t.Fatalf("%v: %s", err, sc.Text())
		}
		ts, err := time.Parse(time.RFC3339Nano, row["_time"])
		if err != nil {
			t.Fatal(err)
		}
		at := strconv.FormatFloat(float64(ts.UnixNano())/1e9, 'f', -1, 64)
		for _, name := range []string{"s", "c"} {
			out[name+" "+row["k"]+" "+at] = row[name]
		}
	}
	return out
}

// equalPoints reports whether two point sets are the same.
func equalPoints(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// recordSeriesFoldBounds records VictoriaLogs' answers to the reads, per version.
func recordSeriesFoldBounds(t *testing.T) {
	t.Helper()
	var rows []oraclerun.ServeRow
	at := []time.Duration{0, time.Nanosecond, time.Minute - time.Nanosecond, time.Minute, time.Minute + time.Nanosecond, 17 * time.Second,
		17*time.Second + time.Nanosecond, 77*time.Second + time.Nanosecond, 90 * time.Second, 2*time.Minute + 30*time.Second,
		3*time.Minute - time.Nanosecond, 3 * time.Minute, 4*time.Minute + 17*time.Second, 5*time.Minute + 30*time.Second,
		5*time.Minute + 30*time.Second + time.Nanosecond, 6 * time.Minute, 6*time.Minute + 17*time.Second, 6*time.Minute + 17*time.Second + time.Nanosecond}
	sort.Slice(at, func(i, j int) bool { return at[i] < at[j] })
	for i, d := range at {
		rows = append(rows, oraclerun.ServeRow{TS: seriesFoldBoundsStart.Add(d).UnixNano(), Stream: [][2]string{{"app", "bounds"}},
			Fields: [][2]string{{"n", strconv.Itoa(i + 1)}, {"k", []string{"a", "b"}[i%2]}, {"_msg", "line"}}})
	}
	post := func(url string, form url.Values) string {
		resp, err := http.PostForm(url, form)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: %d %s", url, resp.StatusCode, body)
		}
		return string(body)
	}
	out := map[string]map[string]seriesFoldBoundsRecord{}
	for _, prof := range diffProfiles() {
		srv, err := oraclerun.StartVLServe(filepath.Join("..", "..", "test", "oracle", "vl"), prof.modfile, []oraclerun.ServeDataSet{{Tenant: 0, Rows: rows}})
		if err != nil {
			t.Fatal(err)
		}
		out[prof.version] = map[string]seriesFoldBoundsRecord{}
		for _, read := range seriesFoldBoundsReads {
			params := seriesFoldBoundsParams(read.from, read.to, read.step)
			query := seriesFoldBoundsQuery(t, params)
			out[prof.version][read.name] = seriesFoldBoundsRecord{Range: params, Query: query,
				RangeBody: post(srv.URL+"/select/logsql/stats_query_range", params), RowBody: post(srv.URL+"/select/logsql/query", query)}
		}
		srv.Stop()
	}
	b, _ := json.MarshalIndent(out, "", " ")
	if err := os.WriteFile(seriesFoldBoundsPath, append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}
