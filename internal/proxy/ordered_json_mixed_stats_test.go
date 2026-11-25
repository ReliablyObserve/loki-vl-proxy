package proxy

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/capability"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// An old scratch flag must remain ordinary original data in poison fixtures.
const mixedStatsSafeField = "__vlmc_safe"

type mixedReal48Request struct {
	ID     string            `json:"id"`
	Params map[string]string `json:"params"`
}
type mixedReal48Fixture struct {
	Rows          int                  `json:"rows"`
	DecodedBytes  int64                `json:"decoded_bytes"`
	DecodedSHA    string               `json:"decoded_sha256"`
	CompressedSHA string               `json:"compressed_sha256"`
	StartNS       int64                `json:"source_start_ns"`
	EndNS         int64                `json:"source_end_ns"`
	Metadata      map[string]string    `json:"metadata_map"`
	Full          []mixedReal48Request `json:"full_requests"`
	Overlap       []mixedReal48Request `json:"overlap_requests"`
	Candidate     string               `json:"candidate"`
	Port          string               `json:"candidate_port"`
}

func mixedReal48Source(t *testing.T) (mixedReal48Fixture, []oraclerun.EngineStream) {
	t.Helper()
	var spec mixedReal48Fixture
	data, err := os.ReadFile(filepath.Join(lokiEngineDir(), "ordered_mixed_real48_source.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &spec); err != nil {
		t.Fatal(err)
	}
	compressed, err := os.ReadFile(filepath.Join(lokiEngineDir(), "ordered_mixed_real48_source.jsonl.gz"))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(compressed)
	if hex.EncodeToString(sum[:]) != spec.CompressedSHA {
		t.Fatal("captured gzip bytes changed")
	}
	f, err := os.Open(filepath.Join(lokiEngineDir(), "ordered_mixed_real48_source.jsonl.gz"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()
	h := sha256.New()
	sc := bufio.NewScanner(io.TeeReader(io.LimitReader(zr, (1<<28)+1), h))
	sc.Buffer(make([]byte, 65536), 1<<20)
	byStream := make(map[string]int)
	var streams []oraclerun.EngineStream
	rows := 0
	var bytes int64
	for sc.Scan() {
		rows++
		bytes += int64(len(sc.Bytes()) + 1)
		if rows > 200000 || bytes > 1<<28 {
			t.Fatal("source fixture budget exceeded")
		}
		var row map[string]string
		if err = json.Unmarshal(sc.Bytes(), &row); err != nil {
			t.Fatal(err)
		}
		labels := parseStreamLabels(row["_stream"])
		if labels["service_name"] == "" || labels["env"] != "production" || labels["workload_scope"] != "scope-01" {
			t.Fatal("unsupported captured source profile")
		}
		level := labels["level"]
		detected, ok := spec.Metadata[level]
		if !ok || detected != level {
			t.Fatal("unvalidated ingest level mapping")
		}
		if _, exists := labels["detected_level"]; exists {
			t.Fatal("ambiguous source category")
		}
		for name, value := range row {
			if !isVLInternalField(name) {
				stored, present := labels[name]
				if !present || stored != value {
					t.Fatalf("unexpected physical field %q", name)
				}
			}
		}
		ts, ok := parseFlexibleUnixNanos(row["_time"])
		if !ok || ts < spec.StartNS || ts > spec.EndNS {
			t.Fatal("source timestamp outside exact capture")
		}
		key := canonicalLabelsKey(labels)
		index, found := byStream[key]
		if !found {
			index = len(streams)
			byStream[key] = index
			streams = append(streams, oraclerun.EngineStream{Labels: cloneStringMap(labels)})
		}
		streams[index].Entries = append(streams[index].Entries, oraclerun.EngineEntry{TS: ts, Line: row["_msg"], Metadata: map[string]string{"detected_level": detected}})
	}
	if err = sc.Err(); err != nil {
		t.Fatal(err)
	}
	if rows != spec.Rows || bytes != spec.DecodedBytes || hex.EncodeToString(h.Sum(nil)) != spec.DecodedSHA {
		t.Fatal("source rows/bytes/digest changed")
	}
	return spec, streams
}

func mixedReal48Set(streams []oraclerun.EngineStream, requests []mixedReal48Request) oraclerun.EngineDataSet {
	set := oraclerun.EngineDataSet{Streams: streams, MaxSeries: 1_000_000}
	for _, request := range requests {
		form := url.Values{}
		for key, value := range request.Params {
			form.Set(key, value)
		}
		// The recorded stack profile aligns start/end to the step. Preserve
		// its actual forms; do not substitute the unaligned engine profile.
		set.Requests = append(set.Requests, oraclerun.EngineRequest{ID: request.ID, Form: form.Encode()})
	}
	return set
}

func mixedReal48ReadGzip(t *testing.T, path, wantSHA string) []byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	z, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	body, err := io.ReadAll(io.LimitReader(z, (1<<28)+1))
	if err != nil || len(body) > 1<<28 {
		t.Fatalf("invalid bounded reference: %v", err)
	}
	if wantSHA != "" {
		sum := sha256.Sum256(body)
		if hex.EncodeToString(sum[:]) != wantSHA {
			t.Fatal("reference response digest changed")
		}
	}
	return body
}

// conformance: semantics/two-parsers-keep-one
func testOrderedMixedReal48CapturedSource(t *testing.T, reference, overlap string) {
	t.Helper()
	if overlap == "" {
		t.Fatal("LVP_MIXED_REAL48_OVERLAP_DIR must name the complete fixed24h source proof")
	}
	spec, streams := mixedReal48Source(t)
	var live struct {
		Status       string `json:"status"`
		SourceSHA    string `json:"source_capture_sha256"`
		SourceBefore struct {
			Result string `json:"result"`
			Rows   int    `json:"rows"`
			SHA    string `json:"identity_counter_sha256"`
		} `json:"source_before"`
		SourceAfter struct {
			Result string `json:"result"`
			Rows   int    `json:"rows"`
			SHA    string `json:"identity_counter_sha256"`
		} `json:"source_after"`
		Requests []struct {
			ID     string            `json:"id"`
			Params map[string]string `json:"params"`
			File   string            `json:"body_file"`
			SHA    string            `json:"body_sha256"`
			Before int               `json:"source_count_before"`
			After  int               `json:"source_count_after"`
			Status int               `json:"status"`
		} `json:"requests"`
	}
	data, err := os.ReadFile(filepath.Join(overlap, "receipt.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &live); err != nil {
		t.Fatal(err)
	}
	if live.Status != "PASS_COMPLETE_FIXED_24H_SOURCE_AND_LIVE_REPLIES" || live.SourceSHA != spec.DecodedSHA || live.SourceBefore.Result != "PASS_COMPLETE_SAME_INPUT" || live.SourceAfter.Result != "PASS_COMPLETE_SAME_INPUT" || live.SourceBefore.Rows != live.SourceAfter.Rows || live.SourceBefore.SHA != live.SourceAfter.SHA {
		t.Fatal("live overlap source proof is incomplete")
	}
	sets := []oraclerun.EngineDataSet{mixedReal48Set(streams, spec.Overlap), mixedReal48Set(streams, spec.Full)}
	for _, set := range sets {
		encoded, err := json.Marshal(set)
		if err != nil || len(encoded) > 1<<28 {
			t.Fatal("engine dataset exceeds existing256MiB record cap")
		}
	}
	all := make(map[string]oraclerun.EngineAnswer)
	first, err := oraclerun.LokiEngine(filepath.Join("..", "..", "test", "oracle", "loki"), sets[:1])
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range spec.Overlap {
		found := false
		for _, reply := range live.Requests {
			if reply.ID == request.ID {
				if !reflect.DeepEqual(reply.Params, request.Params) || reply.Status != 200 || reply.Before != live.SourceBefore.Rows || reply.After != reply.Before {
					t.Fatal("overlap request/count changed")
				}
				body := mixedReal48ReadGzip(t, filepath.Join(overlap, reply.File), reply.SHA)
				answer := first[request.ID]
				if err = recordedParserDifference(recordedParserCase{Status: reply.Status, Response: body}, answer.Status, answer.Body); err != nil {
					t.Fatalf("pinned engine vs complete live overlap %s: %v", request.ID, err)
				}
				all[request.ID] = answer
				found = true
			}
		}
		if !found {
			t.Fatalf("missing live overlap reply %s", request.ID)
		}
	}
	t.Logf("PASS pinned engine vs complete live24h source: %d requests, %d source rows", len(spec.Overlap), live.SourceBefore.Rows)
	var saved []struct {
		URL    string `json:"url"`
		Status int    `json:"status"`
		File   string `json:"body_file"`
	}
	data, err = os.ReadFile(filepath.Join(reference, "all-full-answers.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	build, err := os.ReadFile(filepath.Join(reference, "build-proof.json"))
	if err != nil || !strings.Contains(string(build), spec.Candidate[:12]) {
		t.Fatal("retained candidate build identity missing")
	}
	full, err := oraclerun.LokiEngine(filepath.Join("..", "..", "test", "oracle", "loki"), sets[1:])
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range spec.Full {
		found := false
		for _, reply := range saved {
			u, err := url.Parse(reply.URL)
			if err != nil {
				t.Fatal(err)
			}
			if u.Port() != spec.Port {
				continue
			}
			want := url.Values{}
			for k, v := range request.Params {
				want.Set(k, v)
			}
			if u.Query().Encode() != want.Encode() {
				continue
			}
			body := mixedReal48ReadGzip(t, filepath.Join(reference, reply.File), "")
			answer := full[request.ID]
			if err = recordedParserDifference(recordedParserCase{Status: answer.Status, Response: answer.Body}, reply.Status, body); err != nil {
				t.Fatalf("full48h candidate vs identical-source pinned engine %s: %v", request.ID, err)
			}
			all[request.ID] = answer
			found = true
			break
		}
		if !found {
			t.Fatalf("missing retained candidate answer %s", request.ID)
		}
	}
	t.Logf("PASS full48h identical VL-source oracle: %d count requests, %d source rows; original live48h retention mismatch remains recorded", len(spec.Full), spec.Rows)
	if os.Getenv("LVP_MIXED_REAL48_APPEND_GOLDEN") == "1" {
		appendMixedReal48Golden(t, sets, all)
	}
}

// Append a gzip member in the existing catalog, retaining every old byte and
// oracle case. The normal gzip reader already supports concatenated members.
func appendMixedReal48Golden(t *testing.T, sets []oraclerun.EngineDataSet, answers map[string]oraclerun.EngineAnswer) {
	t.Helper()
	path := filepath.Join(lokiEngineDir(), "ordered_mixed_stats_ranges.jsonl.gz")
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	z, err := gzip.NewReader(f)
	if err != nil {
		f.Close()
		t.Fatal(err)
	}
	existing := make(map[string]string)
	sc := bufio.NewScanner(z)
	sc.Buffer(make([]byte, 65536), 1<<28)
	for sc.Scan() {
		var a lokiEngineAnswer
		if err = json.Unmarshal(sc.Bytes(), &a); err != nil {
			t.Fatal(err)
		}
		existing[a.ID] = a.Key
	}
	if err = sc.Err(); err != nil {
		t.Fatal(err)
	}
	z.Close()
	f.Close()
	var records []lokiEngineAnswer
	for _, set := range sets {
		for _, request := range set.Requests {
			key := lokiEngineKey(set, request)
			if old, found := existing[request.ID]; found {
				if old != key {
					t.Fatal("existing golden ID has different source digest")
				}
				continue
			}
			a := answers[request.ID]
			records = append(records, lokiEngineAnswer{ID: request.ID, Key: key, Status: a.Status, Body: a.Body, Error: a.Error})
		}
	}
	if len(records) == 0 {
		return
	}
	f, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	zw, err := gzip.NewWriterLevel(f, gzip.BestCompression)
	if err != nil {
		f.Close()
		t.Fatal(err)
	}
	enc := json.NewEncoder(zw)
	enc.SetEscapeHTML(false)
	for _, record := range records {
		if err = enc.Encode(record); err != nil {
			t.Fatal(err)
		}
	}
	if err = zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("appended%d real-source records to existing catalog without rewriting old cases", len(records))
}

// conformance: semantics/two-parsers-keep-one
func TestOrderedMixedStatsExpandedQueryBudgetBeforeBackend(t *testing.T) {
	at := time.Unix(1700002800, 0).UTC()
	for _, stages := range []string{`| json k="[\"k\"]" | logfmt`, `| logfmt | json`} {
		t.Run(stages, func(t *testing.T) {
			var requests atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/select/logsql/field_names" {
					// The field-name certificate is not under test here; without a listing the full class runs.
					http.Error(w, "no listing", http.StatusServiceUnavailable)
					return
				}
				requests.Add(1)
				if err := r.ParseForm(); err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				query := r.FormValue("query")
				if len(query) > vlDefaultMaxQueryLen {
					http.Error(w, "query exceeds VictoriaLogs maxQueryLen", http.StatusBadRequest)
					return
				}
				if r.URL.Path != statsQueryRangePath {
					t.Errorf("unexpected backend work:%s", r.URL.Path)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				fmt.Fprintf(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"c","%s":"blue"},"values":[[%d,"1"]]}]}}`, mixedStatsGroupField, at.Add(-30*time.Second).Unix())
			}))
			t.Cleanup(srv.Close)
			p, err := New(Config{BackendURL: srv.URL, Cache: cache.NewDisabled(), LogLevel: "error", MetadataInventoryParallelism: -1})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			p.setReleaseForTest("v1.54.0")
			build := func(n int) (*orderedJSONMetricPlan, int, int, int) {
				t.Helper()
				selector := `{app="api",padding="` + strings.Repeat("x", n) + `"}`
				plan, ok := compileOrderedJSONMetric(`sum by(k)(count_over_time(` + selector + ` ` + stages + ` | drop __error__, __error_details__ | k!="" [1m]))`)
				if !ok {
					t.Fatal("long valid typed mixed plan rejected")
				}
				class, ok := plan.mixedStatsClass()
				if !ok {
					t.Fatal("ordinary mixed class rejected")
				}
				base, err := p.translateQueryWithContext(context.Background(), plan.fetchQuery)
				if err != nil {
					t.Fatal(err)
				}
				query, _ := mixedJSONStatsQuery(base, class, "k")
				stats := logsql.PipeStats{By: []logsql.GroupKey{{Field: mixedStatsGroupField}}, Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}, Alias: "c"}}}
				fullStats := appendMixedClassPipes(query, []logsql.Pipe{stats})
				rawPipes := class.rawPipes()
				rawPipes = append(rawPipes, logsql.PipeFields{Labels: []string{class.originalField}})
				fullRaw := appendMixedClassPipes(base, rawPipes)
				return plan, len(base), len(fullStats), len(fullRaw)
			}
			_, _, statsOne, rawOne := build(1)
			overhead := max(statsOne, rawOne) - 1
			for _, length := range []int{1, vlDefaultMaxQueryLen - 1 - overhead, vlDefaultMaxQueryLen - overhead, vlDefaultMaxQueryLen + 1 - overhead} {
				if length < 1 {
					t.Fatal("classifier overhead alone exceeds backend budget")
				}
				plan, baseLen, statsLen, rawLen := build(length)
				if baseLen >= vlDefaultMaxQueryLen {
					t.Fatal("raw selector must still fit the backend budget")
				}
				if length != 1 && max(statsLen, rawLen) != length+overhead {
					t.Fatal("query-length boundary construction drifted")
				}
				before := requests.Load()
				series, handled, err := p.collectOrderedMixedJSONStats(context.Background(), plan, at, at.Add(30*time.Second), 30*time.Second)
				oversize := statsLen > vlDefaultMaxQueryLen || rawLen > vlDefaultMaxQueryLen
				if oversize {
					if handled || err != nil || series != nil || requests.Load() != before {
						t.Fatalf("expanded native/raw %d/%d must fall back before ALL backend work: handled=%v error=%v backendDelta=%d", statsLen, rawLen, handled, err, requests.Load()-before)
					}
					continue
				}
				if !handled || err != nil || len(series) != 1 || requests.Load() != before+1 {
					t.Fatalf("supported native/raw %d/%d must remain handled: handled=%v error=%v series=%d backendDelta=%d", statsLen, rawLen, handled, err, len(series), requests.Load()-before)
				}
			}
		})
	}
}

// conformance: mixed-json-row-stats
func TestOrderedMixedStatsCapabilityAdmission(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "test backend intentionally refuses evaluation", http.StatusBadRequest)
	}))
	defer srv.Close()
	plan, ok := compileOrderedJSONMetric(`sum by(k)(count_over_time({app="api"} | logfmt | json | drop __error__, __error_details__ | k!="" [1m]))`)
	if !ok {
		t.Fatal("typed mixed plan missing")
	}
	at := time.Unix(1700002800, 0).UTC()
	for _, release := range []string{"", "v1.48.9", "v1.49.0", "v1.54.0"} {
		t.Run(release, func(t *testing.T) {
			p, err := New(Config{BackendURL: srv.URL, Cache: cache.NewDisabled(), LogLevel: "error", MetadataInventoryParallelism: -1})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			p.setReleaseForTest(release)
			want := release == "v1.49.0" || release == "v1.54.0"
			if p.caps.Backend().On(capability.MixedJSONRowStats) != want {
				t.Fatal("stored backend version did not update the capability gate")
			}
			_, handled, err := p.collectOrderedMixedJSONStats(context.Background(), plan, at, at.Add(30*time.Second), 30*time.Second)
			if handled != want || (err != nil) != want {
				t.Fatalf("release %q: handled=%v error=%v, capability=%v", release, handled, err, want)
			}
		})
	}
}

func TestOrderedMixedStatsAdmission(t *testing.T) {
	for _, tc := range []struct {
		pipeline, function string
		want               bool
	}{
		{`| json k="[\"k\"]" | logfmt | drop __error__, __error_details__ | k!=""`, "count_over_time", true},
		{`| logfmt | json | drop __error__, __error_details__ | k!=""`, "rate", true},
		{`| json | logfmt | drop __error__, __error_details__ | k!=""`, "count_over_time", false},
		{`| json k="[\"k\"]" | logfmt | drop __error__ | k!=""`, "count_over_time", false},
		{`| json k="[\"k\"]" | logfmt | drop __error__, __error_details__ | k!=""`, "bytes_over_time", false},
	} {
		query := `sum by(k)(` + tc.function + `({app="api"} ` + tc.pipeline + ` [1m]))`
		plan, ok := compileOrderedJSONMetric(query)
		if !ok {
			if tc.want {
				t.Fatalf("supported mixed plan not compiled: %s", query)
			}
			continue
		}
		_, got := plan.mixedStatsClass()
		if got != tc.want {
			t.Errorf("admitted=%v want=%v: %s", got, tc.want, query)
		}
	}
}

// conformance: semantics/two-parsers-keep-one
func TestOrderedMixedStatsActualRangeMatchesRecordedLoki(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("requires existing VictoriaLogs storage/HTTP and recorded Loki oracles")
	}
	if reference := os.Getenv("LVP_MIXED_REAL48_REFERENCE_DIR"); reference != "" {
		testOrderedMixedReal48CapturedSource(t, reference, os.Getenv("LVP_MIXED_REAL48_OVERLAP_DIR"))
		return
	}
	at := time.Unix(1700002800, 0).UTC()
	rows := []pushdownRow{
		{ts: at.Add(-2*time.Minute - time.Nanosecond), stream: map[string]string{"app": "mixed-class-range"}, msg: `k=outside`},
		{ts: at.Add(-2 * time.Minute), stream: map[string]string{"app": "mixed-class-range"}, msg: `k=lower_two`},
		{ts: at.Add(-2*time.Minute + time.Nanosecond), stream: map[string]string{"app": "mixed-class-range"}, msg: `k=body`},
		{ts: at.Add(-time.Minute), stream: map[string]string{"app": "mixed-class-range"}, msg: `k=lower_one`},
		{ts: at.Add(-time.Minute), stream: map[string]string{"app": "mixed-class-range"}, msg: `{"k":"body"}`},
		{ts: at.Add(-time.Minute + time.Nanosecond), stream: map[string]string{"app": "mixed-class-range"}, msg: `k=body`},
		{ts: at.Add(-30 * time.Second), stream: map[string]string{"app": "mixed-class-range"}, msg: `{"k":"body","service":{"name":"api"}}`},
		{ts: at.Add(-time.Nanosecond), stream: map[string]string{"app": "mixed-class-range"}, msg: `{"k":"body","k":"later"}`},
		{ts: at, stream: map[string]string{"app": "mixed-class-range"}, msg: `{"k":"body"}`},
		{ts: at.Add(time.Nanosecond), stream: map[string]string{"app": "mixed-class-range"}, msg: `k=body`},
		{ts: at.Add(time.Nanosecond), stream: map[string]string{"app": "mixed-class-range"}, msg: `{"k":"body"}`},
		{ts: at.Add(30 * time.Second), stream: map[string]string{"app": "mixed-class-range"}, msg: `{"k":"blue"}`},
		{ts: at.Add(time.Minute), stream: map[string]string{"app": "mixed-class-range"}, msg: `{"k":"-0"}`},
		{ts: at.Add(15 * time.Second), stream: map[string]string{"app": "mixed-class-range"}, msg: `{"k":"\"quote"}`},
		{ts: at.Add(15 * time.Second), stream: map[string]string{"app": "mixed-class-range"}, msg: `{"k":"\\slash"}`},
		{ts: at.Add(15 * time.Second), stream: map[string]string{"app": "mixed-class-range"}, msg: `{"k":"\nline"}`},
	}
	engine := oraclerun.EngineDataSet{Streams: pushdownEngineStreams(rows)}
	for _, window := range []time.Duration{time.Minute, 2 * time.Minute} {
		for _, step := range []time.Duration{15 * time.Second, 30 * time.Second, time.Minute} {
			for _, function := range []string{"count_over_time", "rate"} {
				for _, parser := range []struct{ name, stages string }{
					{"forward", `| json k="[\"k\"]" | logfmt`}, {"reverse", `| logfmt | json`},
				} {
					query := fmt.Sprintf(`sum by(k)(%s({app="mixed-class-range"} %s | drop __error__, __error_details__ | k!="" [%s]))`, function, parser.stages, window)
					id := fmt.Sprintf("%s/%s/window%s/step%s", function, parser.name, window, step)
					for _, phase := range []time.Duration{0, time.Millisecond, 7*time.Second + 123*time.Millisecond} {
						phaseID := id
						if phase != 0 {
							phaseID += "/phase" + phase.String()
						}
						request := lokiMetricRequest(phaseID, query, at.Add(phase).UnixNano(), at.Add(2*time.Minute+phase).UnixNano(), step)
						if !request.Unaligned {
							t.Fatal("phase oracle must evaluate the original unaligned request bounds")
						}
						engine.Requests = append(engine.Requests, request)
					}
				}
			}
		}
	}
	spec, capturedStreams := mixedReal48Source(t)
	capturedSet := mixedReal48Set(capturedStreams, append(append([]mixedReal48Request(nil), spec.Overlap...), spec.Full...))
	sets := []oraclerun.EngineDataSet{engine, capturedSet}
	answers := mixedStatsSavedAnswers(t, sets)
	fresh, err := oraclerun.LokiEngine(filepath.Join("..", "..", "test", "oracle", "loki"), sets)
	if err != nil {
		t.Fatal(err)
	}
	for _, set := range sets {
		for _, request := range set.Requests {
			want := answers[request.ID]
			got, exists := fresh[request.ID]
			if !exists {
				t.Fatalf("pinned engine omitted request %s", request.ID)
			}
			if want.Error != got.Error {
				t.Fatalf("fresh pinned engine error differs for %s: recorded=%q fresh=%q", request.ID, want.Error, got.Error)
			}
			if err := recordedParserDifference(recordedParserCase{Status: want.Status, Response: want.Body}, got.Status, got.Body); err != nil {
				t.Fatalf("fresh pinned engine vs immutable catalog %s: %v", request.ID, err)
			}
		}
	}
	var store oraclerun.ServeDataSet
	for _, row := range rows {
		store.Rows = append(store.Rows, oraclerun.ServeRow{TS: row.ts.UnixNano(), Stream: [][2]string{{"app", "mixed-class-range"}}, Fields: [][2]string{{"_msg", row.msg}}})
	}
	for _, version := range []struct{ name, modfile string }{{"v1.49.0", "go.v1.49.0.mod"}, {"v1.54.0", ""}} {
		t.Run(version.name, func(t *testing.T) {
			backend, err := oraclerun.StartVLServe(filepath.Join("..", "..", "test", "oracle", "vl"), version.modfile, []oraclerun.ServeDataSet{store})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(backend.Stop)
			p, err := New(Config{BackendURL: backend.URL, Cache: cache.NewDisabled(), LogLevel: "error", MetadataInventoryParallelism: -1})
			if err != nil {
				t.Fatal(err)
			}
			p.storeBackendVersion(version.name, version.name)
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			for _, request := range engine.Requests {
				form, err := url.ParseQuery(request.Form)
				if err != nil {
					t.Fatal(err)
				}
				plan, ok := compileOrderedJSONMetric(form.Get("query"))
				if !ok {
					t.Fatal("typed mixed plan missing")
				}
				bounds := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+request.Form, nil)
				start, end, step, err := orderedJSONMetricTimes(bounds, true)
				if err != nil {
					t.Fatalf("invalid recorded bounds: %v", err)
				}
				series, served, err := p.collectOrderedMixedJSONStats(context.Background(), plan, start, end, step)
				if err != nil || !served {
					t.Fatalf("%s: served=%v error=%v", request.ID, served, err)
				}
				body, err := buildMixedJSONStatsMatrix(plan, series, start, end, step, p.limits().BufferedBackendBodyBytes)
				if err != nil {
					t.Fatal(err)
				}
				want := answers[request.ID]
				if err := recordedParserDifference(recordedParserCase{Status: want.Status, Response: want.Body}, http.StatusOK, body); err != nil {
					t.Fatalf("%s: %v", request.ID, err)
				}
				httpAnswer := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+request.Form, nil)
				if err := recordedParserDifference(recordedParserCase{Status: want.Status, Response: want.Body}, httpAnswer.Code, httpAnswer.Body.Bytes()); err != nil {
					t.Fatalf("%s full HTTP route: %v", request.ID, err)
				}
			}
		})
	}
	t.Run("captured_full48", func(t *testing.T) {
		testOrderedMixedReal48RecordedSource(t, capturedStreams, capturedSet, answers)
	})
}

// The live storage replay reads the existing catalog. Re-recording only a
// subset here would discard unrelated recorded requests in the same catalog.
func mixedStatsSavedAnswers(t *testing.T, sets []oraclerun.EngineDataSet) map[string]lokiEngineAnswer {
	t.Helper()
	previous := os.Getenv("LVP_RUN_ORACLES")
	t.Setenv("LVP_RUN_ORACLES", "0")
	defer t.Setenv("LVP_RUN_ORACLES", previous)
	return lokiEngineAnswers(t, "ordered_mixed_stats_ranges", sets)
}

// conformance: semantics/two-parsers-keep-one
func testOrderedMixedReal48RecordedSource(t *testing.T, streams []oraclerun.EngineStream, set oraclerun.EngineDataSet, answers map[string]lokiEngineAnswer) {
	t.Helper()
	var store oraclerun.ServeDataSet
	for _, stream := range streams {
		labels := make([][2]string, 0, len(stream.Labels))
		for name, value := range stream.Labels {
			labels = append(labels, [2]string{name, value})
		}
		sort.Slice(labels, func(i, j int) bool { return labels[i][0] < labels[j][0] })
		for _, entry := range stream.Entries {
			store.Rows = append(store.Rows, oraclerun.ServeRow{TS: entry.TS, Stream: labels, Fields: [][2]string{{"_msg", entry.Line}}})
		}
	}
	for _, version := range []struct{ name, modfile string }{{"v1.49.0", "go.v1.49.0.mod"}, {"v1.54.0", ""}} {
		t.Run(version.name, func(t *testing.T) {
			backend, err := oraclerun.StartVLServe(filepath.Join("..", "..", "test", "oracle", "vl"), version.modfile, []oraclerun.ServeDataSet{store})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(backend.Stop)
			p, err := New(Config{BackendURL: backend.URL, Cache: cache.NewDisabled(), LogLevel: "error", MetadataInventoryParallelism: -1})
			if err != nil {
				t.Fatal(err)
			}
			p.storeBackendVersion(version.name, version.name)
			p.maxStatsQuerySeries = set.MaxSeries
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			for _, request := range set.Requests {
				form, err := url.ParseQuery(request.Form)
				if err != nil {
					t.Fatal(err)
				}
				bounds := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+request.Form, nil)
				start, end, step, err := orderedJSONMetricTimes(bounds, true)
				if err != nil || step.Milliseconds() <= 0 {
					t.Fatalf("invalid captured request bounds: %v", err)
				}
				// Replay the recorded frontend's step-aligned engine axis.
				stepMS := step.Milliseconds()
				form.Set("start", time.UnixMilli(start.UnixMilli()/stepMS*stepMS).Format(time.RFC3339Nano))
				form.Set("end", time.UnixMilli(end.UnixMilli()/stepMS*stepMS).Format(time.RFC3339Nano))
				got := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+form.Encode(), nil)
				want := answers[request.ID]
				if err := recordedParserDifference(recordedParserCase{Status: want.Status, Response: want.Body}, got.Code, got.Body.Bytes()); err != nil {
					t.Fatalf("%s: %v", request.ID, err)
				}
			}
		})
	}
}

// conformance: semantics/two-parsers-keep-one
func TestOrderedMixedStatsActualSourceAliasesMatchCanonicalLoki(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("requires existing VictoriaLogs storage/HTTP and recorded Loki oracles")
	}
	at := time.Unix(1700002800, 0).UTC()
	var engine oraclerun.EngineDataSet
	var store oraclerun.ServeDataSet
	keys := make(map[string]string)
	for _, alias := range []struct{ physical, canonical string }{{"k_", "k"}, {"user__id", "user_id"}, {"___", "key_empty"}} {
		for _, stream := range []bool{false, true} {
			app := fmt.Sprintf("mixed-alias-%s-%t", alias.canonical, stream)
			line := fmt.Sprintf(`{"%s":"blue"}`, alias.canonical)
			labels := map[string]string{"app": app}
			entry := oraclerun.EngineEntry{TS: at.UnixNano(), Line: line}
			row := oraclerun.ServeRow{TS: at.UnixNano(), Stream: [][2]string{{"app", app}}, Fields: [][2]string{{"_msg", line}}}
			if stream {
				labels[alias.canonical] = "stored"
				row.Stream = append(row.Stream, [2]string{alias.physical, "stored"})
			} else {
				entry.Metadata = map[string]string{alias.canonical: "stored"}
				row.Fields = append(row.Fields, [2]string{alias.physical, "stored"})
			}
			engine.Streams = append(engine.Streams, oraclerun.EngineStream{Labels: labels, Entries: []oraclerun.EngineEntry{entry}})
			store.Rows = append(store.Rows, row)
			for _, function := range []string{"count_over_time", "rate"} {
				for _, parser := range []struct{ name, stages string }{
					{"forward", fmt.Sprintf(`| json %s="[\"%s\"]" | logfmt`, alias.canonical, alias.canonical)}, {"reverse", `| logfmt | json`},
				} {
					query := fmt.Sprintf(`sum by(%s)(%s({app=%q} %s | drop __error__, __error_details__ | %s!="" [1m]))`, alias.canonical, function, app, parser.stages, alias.canonical)
					id := fmt.Sprintf("%s/%s/%s", app, function, parser.name)
					engine.Requests = append(engine.Requests, lokiMetricRequest(id, query, at.UnixNano(), at.Add(30*time.Second).UnixNano(), 30*time.Second))
					keys[id] = alias.canonical
				}
			}
		}
	}
	answers := lokiEngineAnswers(t, "ordered_mixed_stats_source_aliases", []oraclerun.EngineDataSet{engine})
	for _, version := range []struct{ name, modfile string }{{"v1.49.0", "go.v1.49.0.mod"}, {"v1.54.0", ""}} {
		t.Run(version.name, func(t *testing.T) {
			backend, err := oraclerun.StartVLServe(filepath.Join("..", "..", "test", "oracle", "vl"), version.modfile, []oraclerun.ServeDataSet{store})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(backend.Stop)
			for _, mode := range []MetadataFieldMode{MetadataFieldModeTranslated, MetadataFieldModeHybrid} {
				t.Run(string(mode), func(t *testing.T) {
					p, err := New(Config{BackendURL: backend.URL, Cache: cache.NewDisabled(), LogLevel: "error", MetadataInventoryParallelism: -1, LabelStyle: LabelStyleUnderscores, MetadataFieldMode: mode})
					if err != nil {
						t.Fatal(err)
					}
					p.storeBackendVersion(version.name, version.name)
					t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
					for _, request := range engine.Requests {
						form, _ := url.ParseQuery(request.Form)
						plan, ok := compileOrderedJSONMetric(form.Get("query"))
						if !ok {
							t.Fatalf("%s: typed mixed plan missing", request.ID)
						}
						series, served, err := p.collectOrderedMixedJSONStats(context.Background(), plan, at, at.Add(30*time.Second), 30*time.Second)
						if err != nil {
							t.Fatalf("%s: %v", request.ID, err)
						}
						var body []byte
						if served {
							body, err = buildMixedJSONStatsMatrix(plan, series, at, at.Add(30*time.Second), 30*time.Second, 1<<20)
						} else {
							if keys[request.ID] != "key_empty" {
								t.Fatalf("%s: ordinary alias case bypassed classified collector", request.ID)
							}
							series, err = p.collectOrderedJSONMetric(context.Background(), plan, at, at.Add(30*time.Second), 30*time.Second)
							if err == nil {
								body, err = buildOrderedJSONMetric(context.Background(), plan, series, at, at.Add(30*time.Second), 30*time.Second, true, 1<<20)
							}
						}
						if err != nil {
							t.Fatalf("%s: %v", request.ID, err)
						}
						want := answers[request.ID]
						if !strings.Contains(string(want.Body), `"stored"`) || strings.Contains(string(want.Body), `"blue"`) {
							t.Fatalf("%s: canonical Loki control did not retain source value: %s", request.ID, want.Body)
						}
						if err := recordedParserDifference(recordedParserCase{Status: want.Status, Response: want.Body}, http.StatusOK, body); err != nil {
							t.Fatalf("%s: %v", request.ID, err)
						}
					}
				})
			}
		})
	}
}

func TestOrderedMixedStatsMatrixRetainsMixedLabelOrder(t *testing.T) {
	at := time.Unix(1700002800, 0)
	plan, _ := compileOrderedJSONMetric(`sum by(k)(count_over_time({app="api"} | logfmt | json | drop __error__, __error_details__ | k!="" [1m]))`)
	stats, raw := make(map[string]manualSeriesSamples), make(map[string]manualSeriesSamples)
	for _, value := range []string{"body", "-0", "\nline", `"quote`, `\slash`} {
		metric := map[string]string{"k": value}
		key := canonicalLabelsKey(metric)
		stats[key] = manualSeriesSamples{Metric: metric, Samples: []rangeMetricSample{{ts: at.Add(-30 * time.Second).UnixNano(), value: 1}}}
		raw[key] = manualSeriesSamples{Metric: metric, Samples: []rangeMetricSample{{ts: at.UnixNano(), value: 1}}}
	}
	want, err := buildOrderedJSONMetric(context.Background(), plan, raw, at, at, time.Minute, true, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	got, err := buildMixedJSONStatsMatrix(plan, stats, at, at, time.Minute, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	var expected, actual any
	if err := json.Unmarshal(want, &expected); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(got, &actual); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("mixed series order or points changed:\n%s\nwant:\n%s", got, want)
	}
}

func TestOrderedMixedStatsMatrixPreservesGlobalPointBudget(t *testing.T) {
	at := time.Unix(1700002800, 0)
	plan := &orderedJSONMetricPlan{function: "count_over_time", window: 10000 * time.Second}
	series := make(map[string]manualSeriesSamples)
	for i := range 101 {
		metric := map[string]string{"k": fmt.Sprintf("group_%03d", i)}
		series[canonicalLabelsKey(metric)] = manualSeriesSamples{Metric: metric, Samples: []rangeMetricSample{{ts: at.Add(-time.Second).UnixNano(), value: 1}}}
	}
	// The budget is global across series (101 series of 10,000 points are 1,010,000 samples) and follows the cap
	// of the request (-ordered-json-metric-max-samples; 1,000,000 was the fixed one before it was a flag).
	capped := withMetricSampleLimit(context.Background(), maxMetricEvalSamples)
	_, err := buildMixedJSONStatsMatrixContext(capped, plan, series, at, at.Add(9999*time.Second), time.Second, 512<<20)
	want := fmt.Sprintf("ordered JSON metric output exceeds %d samples", maxMetricEvalSamples)
	if err == nil || err.Error() != want {
		t.Fatalf("mixed point budget changed: got %v want %s", err, want)
	}
	if _, err := buildMixedJSONStatsMatrixContext(withMetricSampleLimit(context.Background(), 2*maxMetricEvalSamples), plan, series, at, at.Add(9999*time.Second), time.Second, 512<<20); err != nil {
		t.Fatalf("a cap of two million refused 1,010,000 samples: %v", err)
	}
}

type mixedStatsNotifyingContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *mixedStatsNotifyingContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func TestOrderedMixedStatsMatrixHonorsCancellationDuringEvaluation(t *testing.T) {
	at := time.Unix(1700002800, 0)
	base, cancel := context.WithCancel(context.Background())
	ctx := &mixedStatsNotifyingContext{Context: base, entered: make(chan struct{})}
	finished := make(chan struct{})
	go func() {
		select {
		case <-ctx.entered:
			cancel()
		case <-base.Done():
		}
		close(finished)
	}()
	t.Cleanup(func() { cancel(); <-finished })
	metric := map[string]string{"k": "body"}
	series := map[string]manualSeriesSamples{"one": {Metric: metric, Samples: []rangeMetricSample{{ts: at.Add(-time.Second).UnixNano(), value: 1}}}}
	// Large real work gives the cancellation goroutine time to run. There
	// are no sleeps and no mock Err() changing while Done remains open.
	rows, err := hitsRangeMetricSeriesBounded("count_over_time", series, at, at.Add(999999*time.Millisecond), time.Millisecond, time.Hour, 1<<30, nil, ctx, maxMetricEvalSamples)
	if !errors.Is(err, context.Canceled) || rows != nil {
		t.Fatalf("cancelled reduction returned partial success: rows=%d error=%v", len(rows), err)
	}
}

// Capturing Done can expose a cancellation before a short/empty final batch;
// the reducer must observe it even when no per-point poll follows.
type mixedStatsCancelOnDoneContext struct {
	context.Context
	cancel context.CancelFunc
}

func (c mixedStatsCancelOnDoneContext) Done() <-chan struct{} { c.cancel(); return c.Context.Done() }

func TestOrderedMixedStatsMatrixHonorsCancellationAtEmptyExit(t *testing.T) {
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := mixedStatsCancelOnDoneContext{Context: base, cancel: cancel}
	at := time.Unix(1700002800, 0)
	rows, err := hitsRangeMetricSeriesBounded("count_over_time", nil, at, at, time.Second, time.Minute, 1<<20, nil, ctx, maxMetricEvalSamples)
	if !errors.Is(err, context.Canceled) || rows != nil {
		t.Fatalf("empty exit lost cancellation: rows=%v error=%v", rows, err)
	}
}

func TestOrderedMixedStatsMatrixPreservesExactEscapedByteLimit(t *testing.T) {
	at := time.Unix(1700002800, 0)
	plan := &orderedJSONMetricPlan{function: "count_over_time", window: time.Minute, mixedParsers: true}
	metric := map[string]string{"k": strings.Repeat("\x01", 30)}
	key := canonicalLabelsKey(metric)
	raw := map[string]manualSeriesSamples{key: {Metric: metric, Samples: []rangeMetricSample{{ts: at.UnixNano(), value: 1}}}}
	stats := map[string]manualSeriesSamples{key: {Metric: metric, Samples: []rangeMetricSample{{ts: at.Add(-30 * time.Second).UnixNano(), value: 1}}}}
	full, err := buildOrderedJSONMetric(context.Background(), plan, raw, at, at, time.Minute, true, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	limit := len(full) - 1
	_, wantErr := buildOrderedJSONMetric(context.Background(), plan, raw, at, at, time.Minute, true, int64(limit))
	body, gotErr := buildMixedJSONStatsMatrix(plan, stats, at, at, time.Minute, limit)
	if body != nil || gotErr == nil || fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
		t.Fatalf("exact mixed byte-limit policy changed: bytes=%d got=%v want=%v", len(body), gotErr, wantErr)
	}
}

func TestOrderedMixedStatsMatrixAcceptsExactManyPointByteLimit(t *testing.T) {
	at := time.Unix(1700002800, 0)
	plan := &orderedJSONMetricPlan{function: "count_over_time", window: 1000 * time.Second, mixedParsers: true}
	metric := map[string]string{"k": "body"}
	key := canonicalLabelsKey(metric)
	raw := map[string]manualSeriesSamples{key: {Metric: metric, Samples: []rangeMetricSample{{ts: at.Add(-time.Second + time.Nanosecond).UnixNano(), value: 1}}}}
	stats := map[string]manualSeriesSamples{key: {Metric: metric, Samples: []rangeMetricSample{{ts: at.Add(-time.Second).UnixNano(), value: 1}}}}
	end := at.Add(999 * time.Second)
	want, err := buildOrderedJSONMetric(context.Background(), plan, raw, at, end, time.Second, true, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	got, err := buildMixedJSONStatsMatrix(plan, stats, at, end, time.Second, len(want))
	if err != nil || len(got) != len(want) {
		t.Fatalf("estimated bytes rejected a permitted1000-point response: got%d want%d error=%v", len(got), len(want), err)
	}
	if err := recordedParserDifference(recordedParserCase{Status: http.StatusOK, Response: want}, http.StatusOK, got); err != nil {
		t.Fatal(err)
	}
	if body, err := buildMixedJSONStatsMatrix(plan, stats, at, end, time.Second, len(want)-1); err == nil || body != nil {
		t.Fatalf("exact serialized byte limit was not enforced: bytes=%d error=%v", len(body), err)
	}
}

func TestOrderedMixedStatsRawBudgetSpansAndCancellation(t *testing.T) {
	at := time.Unix(1700002800, 0).UTC()
	var encoded [][]byte
	for _, ts := range []time.Time{at, at.Add(time.Hour)} {
		original, err := json.Marshal(map[string]string{"_time": ts.Format(time.RFC3339Nano), "_stream": `{app="api"}`, "_msg": `k=body`})
		if err != nil {
			t.Fatal(err)
		}
		outer, err := json.Marshal(map[string]string{mixedClassOriginalField: string(original)})
		if err != nil {
			t.Fatal(err)
		}
		encoded = append(encoded, append(outer, '\n'))
	}
	for _, mode := range []string{"rows", "bytes", "cancel", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = r.ParseForm()
				calls.Add(1)
				if mode == "malformed" {
					_, _ = w.Write([]byte(`{"__vlmc_original":"{"}` + "\n"))
					return
				}
				from, _ := parseFlexibleUnixNanos(r.Form.Get("start"))
				index := 0
				if from >= at.Add(time.Hour).UnixNano() {
					index = 1
				}
				_, _ = w.Write(encoded[index])
			}))
			t.Cleanup(srv.Close)
			p, err := New(Config{BackendURL: srv.URL, Cache: cache.NewDisabled(), LogLevel: "error", MetadataInventoryParallelism: -1})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			plan, _ := compileOrderedJSONMetric(`sum by(k)(count_over_time({app="api"} | logfmt | json | drop __error__, __error_details__ | k!="" [1m]))`)
			if mode == "rows" {
				p.rangeMetricRowLimit = 1
			}
			if mode == "bytes" {
				p.orderedJSONMaxBytes = int64(len(encoded[0]) + len(encoded[1]) - 1)
			}
			ctx := context.Background()
			if mode == "cancel" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			spans := [][2]time.Time{{at, at}, {at.Add(time.Hour), at.Add(time.Hour)}}
			class, _ := newMixedJSONRowClass("k")
			series, err := p.collectMixedClassRowSpans(ctx, plan, "*", class, spans, at, at.Add(time.Hour), time.Minute)
			if err == nil || series != nil {
				t.Fatalf("unsafe partial result escaped: mode=%s series=%v error=%v", mode, series, err)
			}
			if mode == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation lost: %v", err)
			}
			if (mode == "rows" || mode == "bytes") && calls.Load() != 2 {
				t.Fatalf("did not exercise shared budget across spans: calls=%d", calls.Load())
			}
		})
	}
}

// conformance: quality/query-range-extent-cache, semantics/two-parsers-keep-one
func TestOrderedMixedStatsAuxSurvivesSmallDiskRestart(t *testing.T) {
	at := time.Unix(1700002800, 0).UTC()
	original, err := json.Marshal(map[string]string{"_time": at.Add(time.Nanosecond).Format(time.RFC3339Nano), "_stream": `{app="api"}`, "_msg": `k=body`})
	if err != nil {
		t.Fatal(err)
	}
	outer, err := json.Marshal(map[string]string{mixedClassOriginalField: string(original)})
	if err != nil {
		t.Fatal(err)
	}
	var statsCalls, rawCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case statsQueryRangePath:
			statsCalls.Add(1)
			fmt.Fprintf(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"c","%s":"body"},"values":[[%d,"1"]]},{"metric":{"__name__":"%s","%s":""},"values":[[%d,"1"]]}]}}`, mixedStatsGroupField, at.Add(-30*time.Second).Unix(), "c", mixedStatsGroupField, at.Unix())
		case "/select/logsql/query":
			rawCalls.Add(1)
			_, _ = w.Write(append(outer, '\n'))
		case "/select/logsql/field_names":
			// The field-name certificate is not under test here; without a listing the full class runs.
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			t.Errorf("unexpected backend read %s", r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	t.Cleanup(srv.Close)
	diskConfig := cache.DiskCacheConfig{Path: filepath.Join(t.TempDir(), "small.db"), MaxBytes: 4 << 20, Compression: true, Codec: "s2", FlushInterval: time.Hour}
	open := func() (*Proxy, *cache.Cache, *cache.DiskCache, func()) {
		t.Helper()
		disk, err := cache.NewDiskCache(diskConfig)
		if err != nil {
			t.Fatal(err)
		}
		c := cache.NewWithMaxBytes(time.Hour, 1000, 4<<20)
		c.SetL2(disk)
		p, err := New(Config{BackendURL: srv.URL, Cache: c, LogLevel: "error", MetadataInventoryParallelism: -1, QueryRangeExtentCache: true, QueryRangeExtentCacheTTL: time.Hour})
		if err != nil {
			c.Close()
			_ = disk.Close()
			t.Fatal(err)
		}
		p.storeBackendVersion("v1.54.0", "v1.54.0")
		var once sync.Once
		closeThis := func() {
			once.Do(func() {
				if err := p.Shutdown(context.Background()); err != nil {
					t.Error(err)
				}
				c.Close()
				if err := disk.Close(); err != nil {
					t.Error(err)
				}
			})
		}
		t.Cleanup(closeThis)
		return p, c, disk, closeThis
	}
	forward, _ := compileOrderedJSONMetric(`sum by(k)(count_over_time({app="api"} | json k="[\"k\"]" | logfmt | drop __error__, __error_details__ | k!="" [1m]))`)
	reverse, _ := compileOrderedJSONMetric(`sum by(k)(count_over_time({app="api"} | logfmt | json | drop __error__, __error_details__ | k!="" [1m]))`)
	ctx := context.WithValue(context.Background(), statsExtentRequestKey{}, true)
	p, _, _, closeFirst := open()
	read := func(p *Proxy, plan *orderedJSONMetricPlan) map[string]manualSeriesSamples {
		t.Helper()
		series, served, err := p.collectOrderedMixedJSONStats(ctx, plan, at, at.Add(time.Minute), 30*time.Second)
		if err != nil || !served {
			t.Fatalf("served=%v error=%v", served, err)
		}
		body, err := buildMixedJSONStatsMatrix(plan, series, at, at.Add(time.Minute), 30*time.Second, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		var response struct {
			Data struct {
				Result []struct {
					Metric map[string]string `json:"metric"`
					Values [][]any           `json:"values"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &response); err != nil {
			t.Fatal(err)
		}
		if len(response.Data.Result) != 1 || !reflect.DeepEqual(response.Data.Result[0].Metric, map[string]string{"k": "body"}) {
			t.Fatalf("native/raw merged labels changed: %s", body)
		}
		want := [][]any{{float64(at.Unix()), "1"}, {float64(at.Add(30 * time.Second).Unix()), "2"}, {float64(at.Add(time.Minute).Unix()), "1"}}
		if !reflect.DeepEqual(response.Data.Result[0].Values, want) {
			t.Fatalf("native/raw full grid changed: %s", body)
		}
		return series
	}
	cold := read(p, forward)
	for _, plan := range []*orderedJSONMetricPlan{reverse, forward, reverse} {
		if warm := read(p, plan); !reflect.DeepEqual(warm, cold) {
			t.Fatal("cross-order warm stats/aux reuse changed the merged answer")
		}
	}
	closeFirst()
	p2, c2, _, _ := open()
	for _, plan := range []*orderedJSONMetricPlan{reverse, forward} {
		if restarted := read(p2, plan); !reflect.DeepEqual(restarted, cold) {
			t.Fatal("cross-order SSD restart lost auxiliary buckets or native values")
		}
	}
	if statsCalls.Load() != 1 || rawCalls.Load() != 6 || c2.Stats().L2.Hits == 0 {
		t.Fatalf("expected one shared stats read, six exact raw reads and SSD reuse across both orders: stats=%d raw=%d L2=%d", statsCalls.Load(), rawCalls.Load(), c2.Stats().L2.Hits)
	}
}

// conformance: semantics/two-parsers-keep-one, quality/query-range-settled-steps
func TestOrderedMixedStatsHTTPRepeatAndShiftReuseOriginalSteps(t *testing.T) {
	at := time.Unix(1700002800, 0).UTC()
	for _, pipeline := range []string{`| json k="[\"k\"]" | logfmt`, `| logfmt | json`} {
		t.Run(pipeline, func(t *testing.T) {
			safe := map[string]string{"_time": at.Format(time.RFC3339Nano), "_stream": `{app="api"}`, "_msg": `{"k":"body"}`}
			unsafe := map[string]string{"_time": at.Add(time.Nanosecond).Format(time.RFC3339Nano), "_stream": `{app="api"}`, "_msg": `k=body`}
			var statsCalls, rawCalls atomic.Int32
			var mu sync.Mutex
			var rawStarts []int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = r.ParseForm()
				switch r.URL.Path {
				case statsQueryRangePath:
					if strings.Contains(r.FormValue("query"), "pack_json as "+mixedClassOriginalField) {
						statsCalls.Add(1)
					}
					fmt.Fprintf(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"c","%s":"body"},"values":[[%d,"1"]]},{"metric":{"__name__":"%s","%s":""},"values":[[%d,"1"]]}]}}`, mixedStatsGroupField, at.Add(-30*time.Second).Unix(), "c", mixedStatsGroupField, at.Unix())
				case "/select/logsql/query":
					rawCalls.Add(1)
					from, _ := parseFlexibleUnixNanos(r.Form.Get("start"))
					to, _ := parseFlexibleUnixNanos(r.Form.Get("end"))
					mu.Lock()
					rawStarts = append(rawStarts, from)
					mu.Unlock()
					for _, row := range []map[string]string{safe, unsafe} {
						ts, _ := parseFlexibleUnixNanos(row["_time"])
						if ts < from || ts >= to {
							continue
						}
						if strings.Contains(r.FormValue("query"), "pack_json as "+mixedClassOriginalField) {
							if row["_msg"] == safe["_msg"] {
								continue
							}
							packed, _ := json.Marshal(row)
							_ = json.NewEncoder(w).Encode(map[string]string{mixedClassOriginalField: string(packed)})
						} else {
							_ = json.NewEncoder(w).Encode(row)
						}
					}
				case "/select/logsql/field_names":
					// The field-name certificate is not under test here; without a listing the full class runs.
					w.WriteHeader(http.StatusServiceUnavailable)
				default:
					t.Errorf("unexpected backend path %s", r.URL.Path)
					w.WriteHeader(http.StatusInternalServerError)
				}
			}))
			t.Cleanup(srv.Close)
			c := cache.NewWithMaxBytes(time.Hour, 1000, 4<<20)
			t.Cleanup(c.Close)
			p, err := New(Config{BackendURL: srv.URL, Cache: c, LogLevel: "error", MetadataInventoryParallelism: -1, QueryRangeExtentCache: true, QueryRangeExtentCacheTTL: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			p.storeBackendVersion("v1.54.0", "v1.54.0")
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			query := `sum by(k)(count_over_time({app="api"} ` + pipeline + ` | drop __error__, __error_details__ | k!="" [1m]))`
			plan, _ := compileOrderedJSONMetric(query)
			read := func(from, to time.Time) *httptest.ResponseRecorder {
				t.Helper()
				form := url.Values{"query": {query}, "start": {from.Format(time.RFC3339Nano)}, "end": {to.Format(time.RFC3339Nano)}, "step": {"30"}}
				got := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+form.Encode(), nil)
				metric := map[string]string{"k": "body"}
				rows := map[string]manualSeriesSamples{canonicalLabelsKey(metric): {Metric: metric, Samples: []rangeMetricSample{{ts: at.UnixNano(), value: 1}, {ts: at.Add(time.Nanosecond).UnixNano(), value: 1}}}}
				want, err := buildOrderedJSONMetric(context.Background(), plan, rows, from, to, 30*time.Second, true, 1<<20)
				if err != nil {
					t.Fatal(err)
				}
				if err := recordedParserDifference(recordedParserCase{Status: http.StatusOK, Response: want}, got.Code, got.Body.Bytes()); err != nil {
					t.Fatalf("HTTP cache answer changed: %v", err)
				}
				p.statsExtentStores.wait()
				return got
			}
			read(at, at.Add(time.Minute))
			if statsCalls.Load() == 0 || rawCalls.Load() == 0 {
				t.Fatal("cold HTTP request did not exercise native plus exact raw route")
			}
			mu.Lock()
			rawStarts = nil
			mu.Unlock()
			read(at.Add(30*time.Second), at.Add(90*time.Second))
			mu.Lock()
			for _, from := range rawStarts {
				if from < at.Add(30*time.Second).UnixNano() {
					t.Errorf("shift reread settled raw history from %s", time.Unix(0, from).Sub(at))
				}
			}
			mu.Unlock()
			beforeStats, beforeRaw := statsCalls.Load(), rawCalls.Load()
			read(at.Add(30*time.Second), at.Add(90*time.Second))
			if statsCalls.Load() != beforeStats || rawCalls.Load() != beforeRaw {
				t.Fatalf("exact repeat reached backend: stats %d→%d raw %d→%d", beforeStats, statsCalls.Load(), beforeRaw, rawCalls.Load())
			}
		})
	}
}

func TestOrderedMixedStatsOriginalEnvelopeRestoration(t *testing.T) {
	original := map[string]string{
		"_time": "2026-11-22T10:00:00Z", "_stream": `{app="api",k=""}`, "_msg": "k=body",
		mixedClassOriginalField: "original snapshot collision", mixedStatsSafeField: "1", "__vlp": "original sentinel",
	}
	packed, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	outer := map[string]string{mixedClassOriginalField: string(packed), "stale": "must disappear", mixedStatsSafeField: "wrong"}
	got, err := restoreMixedClassRow(outer, mixedClassOriginalField)
	if err != nil || !reflect.DeepEqual(got, original) {
		t.Fatalf("original source changed: %v %v", got, err)
	}
	for _, envelope := range []string{"", "null", "[1]", `{"k":7}`, "{"} {
		if _, err := restoreMixedClassRow(map[string]string{mixedClassOriginalField: envelope}, mixedClassOriginalField); err == nil {
			t.Errorf("accepted malformed/missing original envelope %q", envelope)
		}
	}
}

func TestOrderedMixedStatsDoesNotOverwriteSourceBeforeSnapshot(t *testing.T) {
	plan, _ := compileOrderedJSONMetric(`sum by(k)(count_over_time({app="api"} | logfmt | json | drop __error__, __error_details__ | k!="" [1m]))`)
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: cache.NewDisabled(), LogLevel: "error", MetadataInventoryParallelism: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	at := time.Unix(1700002800, 0)
	for _, step := range []time.Duration{0, 2 * time.Minute} {
		if _, served, err := p.collectOrderedMixedJSONStats(context.Background(), plan, at, at.Add(time.Hour), step); served || err != nil {
			t.Fatalf("unprotected grid read backend: step=%s served=%v err=%v", step, served, err)
		}
	}
}

func TestOrderedMixedStatsQueryClassifiesBeforeClearingWithSingleCount(t *testing.T) {
	c, _ := newMixedJSONRowClass("k")
	query, agg := mixedJSONStatsQuery(`app:="api"`, c, "k")
	pack := strings.Index(query, "pack_json as "+c.originalField)
	blank := strings.Index(query, `"" as `+mixedClassTargetPrefix+"k")
	if pack < 0 || blank <= pack || strings.Contains(query, "as "+mixedStatsSafeField) {
		t.Fatalf("original row was changed before classification: %s", query)
	}
	if agg != "count() as c" {
		t.Fatalf("stats must count both disjoint groups without zero auxiliary series: %s", agg)
	}
	if strings.Contains(query, "| filter ") {
		t.Fatal("pre-stats class filter discarded complementary rows")
	}
}

func TestOrderedMixedStatsSingleCountRemovesOnlyEmptyGroup(t *testing.T) {
	series := map[string]manualSeriesSamples{
		"empty":  {Metric: map[string]string{}, Samples: []rangeMetricSample{{ts: 1, value: 0}, {ts: 2, value: 3}}},
		"native": {Metric: map[string]string{"k": "body"}, Samples: []rangeMetricSample{{ts: 2, value: 4}}},
	}
	edges := mixedClassRawCountEdges(series)
	if !reflect.DeepEqual(edges, []int64{2}) || len(series) != 1 || series["native"].Samples[0].value != 4 {
		t.Fatalf("single-count native/unsafe groups overlapped or disappeared: %v %v", edges, series)
	}
	series["empty"] = manualSeriesSamples{Metric: map[string]string{}, Samples: []rangeMetricSample{{ts: 3, value: 0}}}
	if edges := mixedClassRawCountEdges(series); len(edges) != 0 || len(series) != 1 {
		t.Fatalf("zero unsafe counts triggered a raw read or left a user group: %v %v", edges, series)
	}
}

// This fake supplies a controlled stats response and preserved raw envelope;
// it does not implement another JSON/logfmt parser or emulate VL classification.
func TestOrderedMixedStatsRangeUnionUsesOriginalReaderAndReducer(t *testing.T) {
	at := time.Unix(1700002800, 0).UTC()
	for _, function := range []string{"count_over_time", "rate"} {
		for _, pipeline := range []string{
			`| json k="[\"k\"]" | logfmt`, `| logfmt | json`,
		} {
			t.Run(function+pipeline, func(t *testing.T) {
				original := map[string]string{
					"_time":   at.Add(time.Nanosecond).Format(time.RFC3339Nano),
					"_stream": `{app="api"}`, "_msg": `k=body`,
					mixedClassOriginalField: "original scratch collision", "__vlp": "original sentinel",
				}
				envelope, err := json.Marshal(original)
				if err != nil {
					t.Fatal(err)
				}
				outer, err := json.Marshal(map[string]string{mixedClassOriginalField: string(envelope)})
				if err != nil {
					t.Fatal(err)
				}
				var mu sync.Mutex
				var rawCalls, statsCalls int
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_ = r.ParseForm()
					mu.Lock()
					defer mu.Unlock()
					switch r.URL.Path {
					case statsQueryRangePath:
						statsCalls++
						fmt.Fprintf(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"c","%s":"body"},"values":[[%d,"1"]]},{"metric":{"__name__":"%s","%s":""},"values":[[%d,"1"]]}]}}`, mixedStatsGroupField, at.Add(-30*time.Second).Unix(), "c", mixedStatsGroupField, at.Unix())
					case "/select/logsql/query":
						rawCalls++
						if !strings.Contains(r.FormValue("query"), "NOT (") {
							t.Error("raw request lacks full static complement")
						}
						_, _ = w.Write(append(outer, '\n'))
					case "/select/logsql/field_names":
						// The field-name certificate is not under test here; without a listing the full class runs.
						w.WriteHeader(http.StatusServiceUnavailable)
					default:
						t.Errorf("unexpected backend read %s", r.URL.Path)
						w.WriteHeader(http.StatusInternalServerError)
					}
				}))
				t.Cleanup(srv.Close)
				p, err := New(Config{BackendURL: srv.URL, Cache: cache.NewDisabled(), LogLevel: "error", MetadataInventoryParallelism: -1})
				if err != nil {
					t.Fatal(err)
				}
				p.storeBackendVersion("v1.54.0", "v1.54.0")
				t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
				// One user group plus its internal unsafe sentinel must fit a
				// one-series limit after the native/raw group is merged.
				p.maxStatsQuerySeries = 1
				plan, ok := compileOrderedJSONMetric(`sum by(k)(` + function + `({app="api"} ` + pipeline + ` | drop __error__, __error_details__ | k!="" [1m]))`)
				if !ok {
					t.Fatal("mixed plan missing")
				}
				series, served, err := p.collectOrderedMixedJSONStats(context.Background(), plan, at, at.Add(time.Minute), 30*time.Second)
				if err != nil || !served {
					t.Fatalf("served=%v error=%v", served, err)
				}
				body, err := buildHitsRangeMetricMatrix(function, series, at, at.Add(time.Minute), 30*time.Second, time.Minute, 1<<20)
				if err != nil {
					t.Fatal(err)
				}
				var response struct {
					Data struct {
						Result []struct {
							Metric map[string]string `json:"metric"`
							Values [][]any           `json:"values"`
						} `json:"result"`
					} `json:"data"`
				}
				if err := json.Unmarshal(body, &response); err != nil {
					t.Fatal(err)
				}
				if len(response.Data.Result) != 1 || response.Data.Result[0].Metric["k"] != "body" {
					t.Fatalf("native/raw labels not merged: %s", body)
				}
				values := response.Data.Result[0].Values
				if len(values) != 3 {
					t.Fatalf("missing grid points: %s", body)
				}
				for i, count := range []float64{1, 2, 1} {
					if function == "rate" {
						count /= 60
					}
					if fmt.Sprint(values[i][1]) != formatMetricSampleValue(count) {
						t.Fatalf("point%d: got%s expected%v; %s", i, values[i][1], count, body)
					}
				}
				mu.Lock()
				defer mu.Unlock()
				if statsCalls != 1 || rawCalls != 1 {
					t.Fatalf("stats/raw calls=%d/%d, expected1/1", statsCalls, rawCalls)
				}
			})
		}
	}
}

// The class envelope is an implementation detail. A request whose complete
// original raw input and final output fit the unchanged ordered JSON byte cap
// must not fail merely because wrapping that input crosses the same cap.
func TestOrderedMixedStatsEnvelopeExpansionRetainsOriginalByteBudget(t *testing.T) {
	at := time.Unix(1700000000, 0).UTC()
	original, err := json.Marshal(map[string]string{
		"_time":   at.Format(time.RFC3339Nano),
		"_stream": `{app="api"}`,
		"_msg":    "k=body " + strings.Repeat("x", 200),
	})
	if err != nil {
		t.Fatal(err)
	}
	wrapped, err := json.Marshal(map[string]string{mixedClassOriginalField: string(original)})
	if err != nil {
		t.Fatal(err)
	}
	original = append(original, '\n')
	wrapped = append(wrapped, '\n')
	byteCap := int64((len(original) + len(wrapped)) / 2)
	if int64(len(original)) >= byteCap || int64(len(wrapped)) <= byteCap {
		t.Fatal("fixture does not straddle the original byte cap")
	}
	for _, stages := range []string{
		`| json k="[\"k\"]" | logfmt`,
		`| logfmt | json`,
	} {
		t.Run(stages, func(t *testing.T) {
			query := `sum by(k)(count_over_time({app="api"} ` + stages + ` | drop __error__, __error_details__ | k!="" [1m]))`
			for _, byteCap := range []int64{byteCap, int64(len(original) - 1)} {
				var baseline []byte
				for _, known := range []bool{false, true} {
					var statsCalls, envelopeCalls, originalCalls atomic.Int32
					srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						_ = r.ParseForm()
						switch r.URL.Path {
						case statsQueryRangePath:
							statsCalls.Add(1)
							fmt.Fprintf(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"c","%s":""},"values":[[%d,"1"]]}]}}`, mixedStatsGroupField, at.Add(-30*time.Second).Unix())
						case "/select/logsql/query":
							if strings.Contains(r.FormValue("query"), "pack_json as "+mixedClassOriginalField) {
								envelopeCalls.Add(1)
								_, _ = w.Write(wrapped)
							} else {
								originalCalls.Add(1)
								_, _ = w.Write(original)
							}
						default:
							w.WriteHeader(http.StatusNotFound)
						}
					}))
					t.Cleanup(srv.Close)
					p, err := New(Config{BackendURL: srv.URL, Cache: cache.NewDisabled(), LogLevel: "error", MetadataInventoryParallelism: -1})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
					p.orderedJSONMaxBytes = byteCap
					if known {
						p.storeBackendVersion("v1.54.0", "v1.54.0")
					}
					form := fmt.Sprintf("/loki/api/v1/query_range?query=%s&start=%s&end=%s&step=30", url.QueryEscape(query), at.Format(time.RFC3339Nano), at.Add(30*time.Second).Format(time.RFC3339Nano))
					got := doCompatProxyRequest(p, form, nil)
					if byteCap < int64(len(original)) {
						if got.Code != http.StatusInternalServerError || !strings.Contains(got.Body.String(), "ResourceExhausted") || originalCalls.Load() != 1 {
							t.Fatalf("original raw limit was bypassed: known=%v status=%d original=%d body=%s", known, got.Code, originalCalls.Load(), got.Body.String())
						}
						continue
					}
					if got.Code != http.StatusOK {
						t.Fatalf("known=%v cap=%d original=%d envelope=%d: status=%d body=%s", known, byteCap, len(original), len(wrapped), got.Code, got.Body.Bytes())
					}
					if int64(got.Body.Len()) > byteCap {
						t.Fatal("fixture final output exceeds the original byte budget")
					}
					if !known {
						baseline = append([]byte(nil), got.Body.Bytes()...)
						if statsCalls.Load() != 0 || envelopeCalls.Load() != 0 || originalCalls.Load() != 1 {
							t.Fatal("control did not use exactly one original raw read")
						}
					} else {
						if err := recordedParserDifference(recordedParserCase{Status: http.StatusOK, Response: baseline}, got.Code, got.Body.Bytes()); err != nil {
							t.Fatal(err)
						}
						if statsCalls.Load() != 1 || envelopeCalls.Load() != 1 || originalCalls.Load() != 1 {
							t.Fatalf("expected native attempt and original bounded fallback: stats=%d envelope=%d original=%d", statsCalls.Load(), envelopeCalls.Load(), originalCalls.Load())
						}
					}
				}
			}
		})
	}
}
