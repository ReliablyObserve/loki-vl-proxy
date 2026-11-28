package proxy

import (
	"bufio"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// keepBlankEngineAnswers holds Loki's engine answers (test/oracle/loki engine) for every query of keepBlankQueries
// and for the query keepSeriesGrouping rewrites it to.
const keepBlankEngineAnswers = "testdata/keep_blank_list_loki_engine.jsonl"

// keepBlankRecord is one recorded pair: a query, its rewrite, and Loki's answer to each.
type keepBlankRecord struct {
	Q         string          `json:"q"`
	Rewritten string          `json:"rewritten"`
	Status    int             `json:"status"`
	Body      json.RawMessage `json:"body,omitempty"`
	Error     string          `json:"error,omitempty"`
	RStatus   int             `json:"rewritten_status"`
	RBody     json.RawMessage `json:"rewritten_body,omitempty"`
	RError    string          `json:"rewritten_error,omitempty"`
}

// keepBlankQueries are range aggregations over a keep whose list holds only a character Loki reads as a label
// name no stream has (a vertical tab, a form feed, U+0085, U+00A0, U+2028, two of them), with and without a parser
// and an outer aggregation, and the fuzz input that found the printer gap.
func keepBlankQueries() []string {
	out := []string{
		"rAte({}|keep\v[0y])",
		// Stages after the keep: line filters read no label, a label filter reads one the keep deleted.
		"count_over_time({app=\"api\"} | keep\v |= \"foo\" | decolorize [1m])",
		"rate({app=\"api\"} | json | keep\u00a0 | foo=\"1\" [1m])",
		"rate({app=\"api\"} | json | keep\u00a0 | line_format \"{{.foo}}\" [1m])",
		// Two keeps, and a drop before the keep.
		"count_over_time({app=\"api\"} | keep pod | keep\v [1m])",
		"count_over_time({app=\"api\"} | drop pod | keep\v [1m])",
	}
	for _, keep := range []string{"keep\v", "keep \u00a0", "keep\f, \u2028", "keep\u0085"} {
		for _, fn := range []string{"count_over_time", "rate", "bytes_over_time", "bytes_rate"} {
			for _, parser := range []string{"", " | json"} {
				for _, outer := range []string{"", "count", "sum", "sort"} {
					q := fn + `({app="api"}` + parser + " | " + keep + " [1m])"
					if outer != "" {
						q = outer + "(" + q + ")"
					}
					out = append(out, q)
				}
			}
		}
	}
	return out
}

// keepBlankStreams is the data set the engine answers over: two streams of different labels, with JSON lines.
func keepBlankStreams(s0 time.Time) []oraclerun.EngineStream {
	var streams []oraclerun.EngineStream
	for i, pod := range []string{"p1", "p2"} {
		st := oraclerun.EngineStream{Labels: map[string]string{"app": "api", "pod": pod}}
		for j := 0; j < 12; j++ {
			st.Entries = append(st.Entries, oraclerun.EngineEntry{TS: s0.Add(time.Duration(j*(10+5*i)) * time.Second).UnixNano(), Line: `{"foo":"` + strconv.Itoa(j%3) + `","n":` + strconv.Itoa(j) + `}`})
		}
		streams = append(streams, st)
	}
	return streams
}

func keepBlankForm(q string, s0 time.Time) string {
	return url.Values{"query": {q}, "start": {strconv.FormatInt(s0.Unix(), 10)}, "end": {strconv.FormatInt(s0.Add(4*time.Minute).Unix(), 10)}, "step": {"60"}}.Encode()
}

// recordKeepBlankAnswers runs Loki's engine over every query and its rewrite and writes the answers.
func recordKeepBlankAnswers(t *testing.T, queries []string) {
	t.Helper()
	s0 := time.Unix(1700000400, 0).UTC()
	set := oraclerun.EngineDataSet{Streams: keepBlankStreams(s0)}
	rewritten := make([]string, len(queries))
	for i, q := range queries {
		rewritten[i], _ = keepSeriesGrouping(q)
		set.Requests = append(set.Requests,
			oraclerun.EngineRequest{ID: strconv.Itoa(i) + "/q", Form: keepBlankForm(q, s0)},
			oraclerun.EngineRequest{ID: strconv.Itoa(i) + "/r", Form: keepBlankForm(rewritten[i], s0)})
	}
	answers, err := oraclerun.LokiEngine(filepath.Join("..", "..", "test", "oracle", "loki"), []oraclerun.EngineDataSet{set})
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(keepBlankEngineAnswers)
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	for i, q := range queries {
		a, r := answers[strconv.Itoa(i)+"/q"], answers[strconv.Itoa(i)+"/r"]
		rec := keepBlankRecord{Q: q, Rewritten: rewritten[i], Status: a.Status, Body: a.Body, Error: a.Error, RStatus: r.Status, RBody: r.Body, RError: r.Error}
		if err := enc.Encode(rec); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// A range aggregation over a keep that lists only a character Loki reads as a name (`rate({...} | keep\v [1m])`)
// is one series in Loki: the keep deletes every label, as for any label no stream has. keepSeriesGrouping rewrites
// it to `sum by () (...)` and prints the keep with its character, so the rewrite parses (the CI fuzz input
// `rAte({}|keep\v[0y])` printed `| keep ` before) and is not rewritten again, and Loki's engine gives the rewrite
// the answer it gives the query, status and body. Loki's answers are recorded with the engine oracle
// (LVP_RUN_ORACLES=1 records them again).
//
// conformance: semantics/keep-drop-blank-name-prints-back, semantics/keep-leaves-labels-loki-drops
func TestKeepSeriesGrouping_BlankKeepListMatchesLokiEngine(t *testing.T) {
	// The spellings stay out of the query corpus, whose registry case row stands for them (see
	// TestKeepDropBlankMatchesLoki).
	t.Cleanup(querycorpus.PauseRecording())
	queries := keepBlankQueries()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordKeepBlankAnswers(t, queries)
	}
	f, err := os.Open(keepBlankEngineAnswers)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var recs []keepBlankRecord
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var r keepBlankRecord
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		recs = append(recs, r)
	}
	if len(recs) != len(queries) {
		t.Fatalf("%d recorded answers for %d queries: record them again with LVP_RUN_ORACLES=1", len(recs), len(queries))
	}
	oneSeries := 0
	for i, r := range recs {
		if r.Q != queries[i] {
			t.Fatalf("answer %d is for %q, not %q: record them again with LVP_RUN_ORACLES=1", i, r.Q, queries[i])
		}
		got, changed := keepSeriesGrouping(r.Q)
		if !changed || got != r.Rewritten {
			t.Errorf("%q rewritten to %q (changed=%v), recorded %q: record them again with LVP_RUN_ORACLES=1", r.Q, got, changed, r.Rewritten)
			continue
		}
		// The rewrite is handed on in the request's canonical spelling (logql.CanonicalSpelling).
		if e, err := logqlpkg.Parse(got); err != nil || logqlpkg.CanonicalSpellingOfValid(e.String()) != got {
			t.Errorf("%q: the rewrite %q does not read back: %v", r.Q, got, err)
		}
		if again, changed := keepSeriesGrouping(got); changed {
			t.Errorf("%q rewritten twice: %q then %q", r.Q, got, again)
		}
		if r.Status != r.RStatus || r.Error != r.RError || string(r.Body) != string(r.RBody) {
			t.Errorf("%q: Loki answers %d %s %s, and its rewrite %q %d %s %s", r.Q, r.Status, r.Body, r.Error, got, r.RStatus, r.RBody, r.RError)
		}
		var body struct {
			Data struct {
				Result []struct {
					Metric map[string]string `json:"metric"`
				} `json:"result"`
			} `json:"data"`
		}
		if r.Status == http.StatusOK && json.Unmarshal(r.Body, &body) == nil && len(body.Data.Result) == 1 && len(body.Data.Result[0].Metric) == 0 {
			oneSeries++
		}
	}
	// Every query but the fuzz input (an empty selector, which Loki rejects for both) and the label filter on a
	// deleted label (no line passes it) is one {} series.
	if oneSeries != len(queries)-2 {
		t.Errorf("%d of %d queries are one {} series in Loki", oneSeries, len(queries)-2)
	}
}

// Through the query_range handler, a keep of only a character Loki reads as a name answers like a keep of a label
// no line has (one {} series, Loki's sum of every stream), a repeat makes no VictoriaLogs call, and a keep of that
// character beside a name is Loki's 400 with no VictoriaLogs call.
//
// conformance: semantics/keep-drop-blank-name-prints-back, semantics/keep-leaves-labels-loki-drops, loki_api_v1_query_range
func TestKeepSeriesGrouping_BlankKeepListHandler(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(10*time.Minute)
	rows := keepRows(rand.New(rand.NewSource(5)), s0, 220, false)
	srv, fake, counts := newKeepFakeVL(t, rows)
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	run := func(query string) *httptest.ResponseRecorder {
		params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {"60"}}
		rec := httptest.NewRecorder()
		p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
		return rec
	}
	for _, tc := range []struct{ query, parser, fn string }{
		{"count_over_time({app=\"api\"} | keep\v [1m])", "", "count_over_time"},
		{"rate({app=\"api\"} | json | keep \u00a0 [1m])", "json", "rate"},
		{"bytes_over_time({app=\"api\"} | keep\f, \u2028 [1m])", "", "bytes_over_time"},
		{"bytes_over_time({app=\"api\"} | json | keep\u0085 [1m])", "json", "bytes_over_time"},
	} {
		want := lokiPushdownMatrix(t, "keep_blank_list_handler", rows, tc.query, start, end, time.Minute)
		rec := run(tc.query)
		if rec.Code != http.StatusOK {
			t.Errorf("%q: status %d: %s", tc.query, rec.Code, rec.Body)
			continue
		}
		if got := decodeExtractedMatrix(t, rec.Body.Bytes()); !sameMatrixValues(got, want) {
			t.Errorf("%q\n got: %v\nwant: %v", tc.query, got, want)
		}
		// A repeat is answered from the cache: no VictoriaLogs call, the same answer.
		before := counts.stats.Load() + counts.raw.Load()
		if again := run(tc.query); again.Code != http.StatusOK || again.Body.String() != rec.Body.String() {
			t.Errorf("%q: the repeat answers %d %s", tc.query, again.Code, again.Body)
		}
		if after := counts.stats.Load() + counts.raw.Load(); after != before {
			t.Errorf("%q: the repeat made %d VictoriaLogs calls", tc.query, after-before)
		}
	}
	calls := counts.stats.Load() + counts.raw.Load()
	for _, q := range []string{"count_over_time({app=\"api\"} | keep\v\v [1m])", "rate({app=\"api\"} | keep\u00a0a [1m])", "rate({app=\"api\"} | drop\fa [1m])"} {
		rec := run(q)
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "syntax error: unexpected IDENTIFIER") {
			t.Errorf("%q: %d %s, want Loki's 400 unexpected IDENTIFIER", q, rec.Code, rec.Body)
		}
	}
	if after := counts.stats.Load() + counts.raw.Load(); after != calls {
		t.Errorf("rejected queries made %d VictoriaLogs calls", after-calls)
	}
}
