package querycorpus_test

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// The Loki differential of metric queries: plan-corpus rows whose plan is a metric query the plan routes natively,
// run by real Loki and by real VictoriaLogs over the same data, through the proxy's real query and query_range
// handlers.
//
//   - Data: buildMetricFixture, the streams and lines of the log differential for each log selector of the query,
//     spread over the request window (plan_loki_vl_metric_diff_fixture_test.go).
//   - Requests: an instant query at metricEnd, and query_range over the hour before it with the step equal to the
//     query's first range, a fifth of it and three times it (the route parity request profiles), bounds on the
//     step's grid.
//   - Loki: the Loki oracle's engine command (test/oracle/loki/engine): Loki's HTTP parameter parser, the engine
//     over an in-memory querier, Loki's response encoder and error status.
//   - VictoriaLogs: the VictoriaLogs oracle's serve command (test/oracle/vl/serve, v1.53.0 and v1.49.0): the data
//     stored as the Loki push API hands it over, answered by VictoriaLogs' own query handlers.
//   - Proxy: a fresh proxy's query or query_range handler (proxy.New, the Loki-compatible profile of the log
//     differential) sends its requests to VictoriaLogs through a recording front server
//     (plan_loki_vl_metric_diff_proxy_test.go), and its answer is compared with Loki's.
//
// Plan and translator: the handlers serve metric queries from the string translator and route them with the
// handlers' own rules, which the plan's route rule (lower.RouteQuery) is held to on every corpus row by the route
// parity test (internal/proxy TestPlanRouteParity). A run counts for the plan when the route rule routes the query
// natively for the request, and every stats text the route lowers is the filter part of a request the handler sent:
// its answer is then the plan's. Every run counts for the translator (the handler as it serves today, reported).
//
// Comparison, per request: the same status (and, for an error, Loki's text); the same result type; the same series
// by label set; per series the same timestamps and, per point, the same value. A value equal only within a relative
// 1e-9 (float summation order) matches, counted apart. Series are matched by their whole label set; a series of one
// side that has no equal on the other is classed by the labels it differs in from the nearest series.
//
// The oracle answers are recorded (testdata/plan-loki-vl-metric-diff.jsonl.gz): Loki's answer per request, and
// VictoriaLogs' answer per request the handler sent, keyed by path and form. CI replays them (the fast path); a
// fixture, a request or a VictoriaLogs request that was not recorded fails as stale until LVP_RUN_ORACLES=1 records
// them again (with LVP_LOKI_DIFF_CHECK=1, the live answers are compared with the committed ones instead: CI's oracle
// step). The counts are the loki_metric_differential section of conformance/registry/query-plan.json
// (LVP_QUERY_PLAN_UPDATE=1 rewrites it): plan_matches_loki may only rise, and every query where the plan differs from
// Loki is listed with its registry case in testdata/loki-metric-differential-known.json.
//
// Not compared (counted): metric rows the route rule does not route natively for any request (their delegation
// reasons), a request kind it does not route for a row, queries Loki rejects at parse (the parse oracles hold
// them), and queries whose answer depends on the wall clock.

const (
	metricAnswersPath = "testdata/plan-loki-vl-metric-diff.jsonl.gz"
	metricKnownPath   = "testdata/loki-metric-differential-known.json"
	// metricFloatTolerance is the relative difference two float values may have and still match: the order of a
	// float sum is not the same in two engines.
	metricFloatTolerance = 1e-9
)

// metricKind is one request shape.
type metricKind struct {
	name    string
	instant bool
	step    func(r time.Duration) time.Duration
}

func metricKinds() []metricKind {
	return []metricKind{
		{name: "instant", instant: true},
		{name: "step=range", step: func(r time.Duration) time.Duration { return r }},
		{name: "step<range", step: func(r time.Duration) time.Duration { return max(r/5, time.Second) }},
		{name: "step>range", step: func(r time.Duration) time.Duration { return 3 * r }},
	}
}

// metricRequest is one request of a query.
type metricRequest struct {
	kind    string
	instant bool
	form    url.Values
	route   lower.RouteRequest
}

// metricQuery is one corpus row of the differential.
type metricQuery struct {
	q, id string
	plan  *queryplan.Plan
	fix   metricFixture
	reqs  []metricRequest
	// routes are, per profile and request kind, the plan's route (nil when the rule delegates the request).
	routes map[string]map[string]*lower.QueryRoute
}

// metricRequests are a query's requests: instant at metricEnd, and query_range over metricWindow before it, both
// bounds moved down to the step's grid (as Loki's query frontend aligns them, so neither side moves them).
func metricRequests(q string, r time.Duration) []metricRequest {
	var out []metricRequest
	for _, k := range metricKinds() {
		form := url.Values{"query": {q}}
		mr := metricRequest{kind: k.name, instant: k.instant}
		if k.instant {
			form.Set("time", strconv.FormatInt(metricEnd.Unix(), 10))
			mr.route = lower.RouteRequest{Instant: true, End: metricEnd}
		} else {
			// Loki keeps a step in whole milliseconds; the bounds are written to the millisecond, so a step that is
			// not whole seconds keeps them on its grid. Such a step is written as a duration in milliseconds: Loki
			// reads float seconds through float64 nanoseconds and truncates them (1.037 is 1036.999999 ms), the
			// difference semantics/sub-second-step-grid holds, which would move every point of the run.
			step := k.step(r).Truncate(time.Millisecond)
			end := alignDown(metricEnd, step)
			start := alignDown(metricEnd.Add(-metricWindow), step)
			form.Set("start", start.Format(time.RFC3339Nano))
			form.Set("end", end.Format(time.RFC3339Nano))
			if step%time.Second == 0 {
				form.Set("step", strconv.FormatInt(int64(step/time.Second), 10))
			} else {
				form.Set("step", strconv.FormatInt(step.Milliseconds(), 10)+"ms")
			}
			mr.route = lower.RouteRequest{Step: step, Span: end.Sub(start), End: end}
		}
		mr.form = form
		out = append(out, mr)
	}
	return out
}

// alignDown moves t down to a multiple of step since the Unix epoch, in milliseconds, as Loki's query frontend
// aligns a range request's bounds.
func alignDown(t time.Time, step time.Duration) time.Time {
	ms := step.Milliseconds()
	if ms <= 0 {
		return t
	}
	return time.UnixMilli(t.UnixMilli() / ms * ms).UTC()
}

// metricRoot is whether a plan root is a metric query.
func metricRoot(n queryplan.Node) bool {
	switch n.(type) {
	case *queryplan.RangeAgg, *queryplan.VectorAgg, *queryplan.BinOp, *queryplan.LabelReplace, *queryplan.Rank:
		return true
	}
	return false
}

// metricProfile is the lowering profile of the handler for a diff profile: its VictoriaLogs capabilities and its
// upstream label names.
func metricProfile(prof diffProfile) lower.Profile {
	return lower.Profile{Caps: prof.caps(), Label: translator.WithStreamLabels(prof.toVL(), nil)}
}

// metricScope is the differential's rows and why the other metric rows are left out.
type metricScope struct {
	queries  []metricQuery
	excluded map[string]int
}

// metricDiffQueries are the plan-corpus metric rows the plan routes natively for at least one request under the
// first profile.
func metricDiffQueries(t *testing.T) metricScope {
	t.Helper()
	sc := metricScope{excluded: map[string]int{}}
	limit, _ := strconv.Atoi(os.Getenv("LVP_METRIC_DIFF_LIMIT"))
	for _, q := range planCorpus(t) {
		expr, msg := logql.ValidateLogQLExpr(q)
		if msg != "" || expr == nil {
			continue
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil || !metricRoot(p.Root) {
			continue
		}
		queryplan.Resolve(p)
		if metricNowDependent(p.Root) {
			sc.excluded["the answer depends on the wall clock"]++
			continue
		}
		mq := metricQuery{q: q, id: queryID("logql", q), plan: p, routes: map[string]map[string]*lower.QueryRoute{}}
		mq.reqs = metricRequests(q, firstRangeOf(p.Root))
		native := false
		reason := ""
		for _, prof := range diffProfiles() {
			mq.routes[prof.name] = map[string]*lower.QueryRoute{}
			if err := metricPlanGate(q, p, metricProfile(prof)); err != nil {
				if reason == "" {
					reason = delegationReason(err)
				}
				continue
			}
			for _, mr := range mq.reqs {
				r, err := lower.RouteQuery(p, metricProfile(prof), mr.route)
				if err != nil {
					if reason == "" {
						reason = delegationReason(err)
					}
					continue
				}
				mq.routes[prof.name][mr.kind] = &r
				if prof.name == diffProfiles()[0].name {
					native = true
				}
			}
		}
		if !native {
			sc.excluded["the plan does not route it: "+reason]++
			continue
		}
		mq.fix = buildMetricFixture(p)
		sc.queries = append(sc.queries, mq)
		if limit > 0 && len(sc.queries) == limit {
			break
		}
	}
	return sc
}

// metricPlanGate is the plan's delegation of a query the handlers read by its text another way, as the route parity
// prediction has it (internal/proxy predictMetricRoute): a range aggregation at the root spelled otherwise than
// "<op>(", a text the handlers' request rewrite changes (a comment, a keyword in upper case), a filter on a field
// VictoriaLogs keeps for itself, or a plan the lowering does not write (an operand it turns away by its spelling).
func metricPlanGate(q string, p *queryplan.Plan, prof lower.Profile) error {
	if ra, ok := p.Root.(*queryplan.RangeAgg); ok && !strings.HasPrefix(strings.TrimSpace(q), string(ra.Op)+"(") {
		return &lower.NotLoweredError{Construct: "RangeAgg route spelling"}
	}
	if logql.LowercaseKeywords(logql.StripParserFlags(logql.StripComments(q))) != q {
		return &lower.NotLoweredError{Construct: lower.Construct(p.Root) + " route rewrite"}
	}
	if lower.ReadsReservedField(p, prof) {
		return &lower.NotLoweredError{Construct: lower.Construct(p.Root) + " route reserved field"}
	}
	if err := lower.ScreenText(q, p); err != nil {
		return err
	}
	_, err := lower.Plan(p, prof)
	return err
}

// delegationReason names why the route rule delegates a request: the construct of its NotLoweredError.
func delegationReason(err error) string {
	var nl *lower.NotLoweredError
	if errors.As(err, &nl) {
		return nl.Construct
	}
	return err.Error()
}

// metricNowDependent is whether a plan's answer depends on the time it is evaluated at beyond the request (its
// resolved semantics say so).
func metricNowDependent(n queryplan.Node) bool {
	dep := false
	var walk func(queryplan.Node)
	walk = func(n queryplan.Node) {
		switch v := n.(type) {
		case *queryplan.RangeAgg:
			if v.Sem != nil && v.Sem.NowDep {
				dep = true
			}
		case *queryplan.VectorAgg:
			walk(v.In)
		case *queryplan.Rank:
			walk(v.In)
		case *queryplan.LabelReplace:
			walk(v.In)
		case *queryplan.BinOp:
			walk(v.L)
			walk(v.R)
		}
	}
	walk(n)
	return dep
}

// firstRangeOf is the range of the plan's first range aggregation, 5m when it has none (the route parity rule).
func firstRangeOf(n queryplan.Node) time.Duration {
	switch v := n.(type) {
	case *queryplan.RangeAgg:
		return v.Range
	case *queryplan.VectorAgg:
		return firstRangeOf(v.In)
	case *queryplan.Rank:
		return firstRangeOf(v.In)
	case *queryplan.LabelReplace:
		return firstRangeOf(v.In)
	case *queryplan.BinOp:
		if d := firstRangeOf(v.L); d != 5*time.Minute {
			return d
		}
		return firstRangeOf(v.R)
	}
	return 5 * time.Minute
}

// planSent is whether every stats text the route lowers is the filter part of a request the handler sent: the
// handler's answer is then the plan's.
func planSent(r *lower.QueryRoute, sent []string) bool {
	queries := sentStatsQueries(sent)
	for _, op := range r.Operands() {
		want := filterPart(op.Text)
		found := false
		for _, q := range queries {
			if filterPart(q) == want {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// filterPart is a LogsQL text before its first pipe, whitespace folded.
func filterPart(q string) string {
	if i := translator.FirstTopLevelPipe(q); i >= 0 {
		q = q[:i]
	}
	return strings.Join(strings.Fields(q), " ")
}

// ---- recorded answers ----

// metricLokiAnswer is Loki's answer to one request.
type metricLokiAnswer struct {
	Status int             `json:"status"`
	Body   json.RawMessage `json:"body,omitempty"`
	Error  string          `json:"error,omitempty"`
}

type metricRecord struct {
	Q string `json:"q"`
	// Inputs is the hash of the fixture and the request forms the answers were recorded for.
	Inputs string                      `json:"inputs"`
	Loki   map[string]metricLokiAnswer `json:"loki"`
	// VL are VictoriaLogs' answers per version, by the hash of the request key (shortHash of metricRequestKey).
	VL map[string]map[string]metricVLAnswer `json:"-"`
}

// metricStoredRecord is a record as the file holds it: each distinct VictoriaLogs body once (the versions mostly
// answer alike), each answer its status and the index of its body.
type metricStoredRecord struct {
	Q      string                                   `json:"q"`
	Inputs string                                   `json:"inputs"`
	Loki   map[string]metricLokiAnswer              `json:"loki"`
	Bodies []string                                 `json:"bodies"`
	VL     map[string]map[string]metricStoredAnswer `json:"vl"`
}

type metricStoredAnswer struct {
	Status int `json:"s"`
	Body   int `json:"b"`
}

func (r *metricRecord) stored() metricStoredRecord {
	out := metricStoredRecord{Q: r.Q, Inputs: r.Inputs, Loki: r.Loki, Bodies: []string{}, VL: map[string]map[string]metricStoredAnswer{}}
	index := map[string]int{}
	for _, version := range sortedKeysOf(r.VL) {
		out.VL[version] = map[string]metricStoredAnswer{}
		answers := r.VL[version]
		for _, key := range sortedKeysOf(answers) {
			a := answers[key]
			i, ok := index[a.Body]
			if !ok {
				i = len(out.Bodies)
				index[a.Body] = i
				out.Bodies = append(out.Bodies, a.Body)
			}
			out.VL[version][key] = metricStoredAnswer{Status: a.Status, Body: i}
		}
	}
	return out
}

func (s metricStoredRecord) record() (*metricRecord, error) {
	r := &metricRecord{Q: s.Q, Inputs: s.Inputs, Loki: s.Loki, VL: map[string]map[string]metricVLAnswer{}}
	for version, answers := range s.VL {
		r.VL[version] = map[string]metricVLAnswer{}
		for key, a := range answers {
			if a.Body < 0 || a.Body >= len(s.Bodies) {
				return nil, fmt.Errorf("%s: body %d of %d", s.Q, a.Body, len(s.Bodies))
			}
			r.VL[version][key] = metricVLAnswer{Status: a.Status, Body: s.Bodies[a.Body]}
		}
	}
	return r, nil
}

func sortedKeysOf[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// inputsHash is the identity of a query's data and requests.
func (mq metricQuery) inputsHash() string {
	var b strings.Builder
	b.WriteString(mq.fix.hash())
	for _, r := range mq.reqs {
		b.WriteString("\x00" + r.kind + "\x00" + r.form.Encode())
	}
	return shortHash(b.String())
}

func readMetricAnswers(t *testing.T) map[string]*metricRecord {
	t.Helper()
	return readMetricAnswersFrom(t, metricAnswersPath)
}

// readMetricAnswersFrom reads the recorded answers of path.
func readMetricAnswersFrom(t *testing.T, path string) map[string]*metricRecord {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("%s: %v: LVP_RUN_ORACLES=1 records it", path, err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]*metricRecord{}
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<28)
	for sc.Scan() {
		var stored metricStoredRecord
		if err := json.Unmarshal(sc.Bytes(), &stored); err != nil {
			t.Fatal(err)
		}
		r, err := stored.record()
		if err != nil {
			t.Fatal(err)
		}
		out[r.Q] = r
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func writeMetricAnswers(t *testing.T, path string, recs []*metricRecord) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	zw, err := gzip.NewWriterLevel(f, gzip.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	// The header carries no time, so the same answers write the same bytes (encoding/json sorts map keys).
	zw.ModTime = time.Time{}
	enc := json.NewEncoder(zw)
	enc.SetEscapeHTML(false)
	for _, r := range recs {
		if err := enc.Encode(r.stored()); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// recordMetricAnswers runs the oracles into path: Loki's engine over every query's requests, and every request the handler
// sends, under every profile, through VictoriaLogs' query server of each version.
func recordMetricAnswers(t *testing.T, path string, queries []metricQuery) {
	t.Helper()
	root := filepath.Join("..", "..")
	recs := make([]*metricRecord, len(queries))
	sets := make([]oraclerun.EngineDataSet, len(queries))
	for i, mq := range queries {
		recs[i] = &metricRecord{Q: mq.q, Inputs: mq.inputsHash(), Loki: map[string]metricLokiAnswer{}, VL: map[string]map[string]metricVLAnswer{}}
		sets[i].Streams = mq.fix.lokiStreams()
		for _, r := range mq.reqs {
			sets[i].Requests = append(sets[i].Requests, oraclerun.EngineRequest{ID: strconv.Itoa(i) + "/" + r.kind, Instant: r.instant, Form: r.form.Encode()})
		}
	}
	loki, err := oraclerun.LokiEngine(filepath.Join(root, "test", "oracle", "loki"), sets)
	if err != nil {
		t.Fatal(err)
	}
	for i, mq := range queries {
		for _, r := range mq.reqs {
			a := loki[strconv.Itoa(i)+"/"+r.kind]
			recs[i].Loki[r.kind] = metricLokiAnswer{Status: a.Status, Body: a.Body, Error: a.Error}
		}
	}
	byVersion := map[string][]diffProfile{}
	var versions []string
	for _, prof := range diffProfiles() {
		if _, ok := byVersion[prof.version]; !ok {
			versions = append(versions, prof.version)
		}
		byVersion[prof.version] = append(byVersion[prof.version], prof)
	}
	for _, version := range versions {
		profs := byVersion[version]
		data := make([]oraclerun.ServeDataSet, len(queries))
		for i, mq := range queries {
			data[i] = oraclerun.ServeDataSet{Tenant: uint32(i + 1), Rows: mq.fix.vlRows(profs[0].toVL())}
		}
		srv, err := oraclerun.StartVLServe(filepath.Join(root, "test", "oracle", "vl"), profs[0].modfile, data)
		if err != nil {
			t.Fatal(err)
		}
		for i, mq := range queries {
			front := &metricFront{version: version, upstream: srv.URL, tenant: uint32(i + 1), recorded: map[string]metricVLAnswer{}}
			for _, prof := range profs {
				for _, r := range mq.reqs {
					// Twice: a probe a sibling's answer cancels in one run may be sent in another.
					runMetricHandler(t, prof, r, front)
					runMetricHandler(t, prof, r, front)
				}
			}
			recs[i].VL[version] = front.recorded
			// LVP_RECORD_PACE (a duration) waits after each query: every run is a fresh proxy with its own
			// connections, and a recording of many queries can use up the host's ephemeral ports while the closed
			// ones wait out TIME_WAIT (a run that cannot dial records no answer).
			if pace, err := time.ParseDuration(os.Getenv("LVP_RECORD_PACE")); err == nil && pace > 0 {
				time.Sleep(pace)
			}
		}
		srv.Stop()
	}
	if os.Getenv("LVP_LOKI_DIFF_CHECK") == "1" {
		committed := readMetricAnswersFrom(t, path)
		changed := 0
		for _, r := range recs {
			got, _ := json.Marshal(r.stored())
			var want []byte
			if c := committed[r.Q]; c != nil {
				want, _ = json.Marshal(c.stored())
			}
			if string(got) != string(want) {
				if changed++; changed <= 5 {
					t.Errorf("the oracles answer %s otherwise than %s records", r.Q, path)
				}
			}
		}
		if changed > 0 || len(committed) != len(recs) {
			t.Fatalf("%d of %d queries answered otherwise (%d recorded): record them again with LVP_RUN_ORACLES=1", changed, len(recs), len(committed))
		}
		t.Logf("the oracles answer the %d queries as %s records", len(recs), path)
		return
	}
	writeMetricAnswers(t, path, recs)
	t.Logf("recorded %s: %d queries", path, len(recs))
}

// ---- comparison ----

// metricSeries is one series of an answer: its labels and its points (timestamp text to value text).
type metricSeries struct {
	labels map[string]string
	points map[string]string
}

// metricResult is a decoded answer.
type metricResult struct {
	resultType string
	series     map[string]metricSeries
}

func labelsKey(m map[string]string) string {
	var b strings.Builder
	for _, k := range sortedKeys(m) {
		b.WriteString(k + "=" + strconv.Quote(m[k]) + ",")
	}
	return b.String()
}

// decodeMetricBody reads a Loki-shaped query answer.
func decodeMetricBody(body []byte) (metricResult, error) {
	var resp struct {
		Data struct {
			ResultType string          `json:"resultType"`
			Result     json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return metricResult{}, err
	}
	out := metricResult{resultType: resp.Data.ResultType, series: map[string]metricSeries{}}
	point := func(p []json.RawMessage) (string, string, error) {
		if len(p) != 2 {
			return "", "", fmt.Errorf("point of %d elements", len(p))
		}
		var ts json.Number
		if err := json.Unmarshal(p[0], &ts); err != nil {
			return "", "", err
		}
		var v string
		if err := json.Unmarshal(p[1], &v); err != nil {
			return "", "", err
		}
		f, err := ts.Float64()
		if err != nil {
			return "", "", err
		}
		return strconv.FormatFloat(f, 'f', 3, 64), v, nil
	}
	switch out.resultType {
	case "matrix", "vector":
		var series []struct {
			Metric map[string]string   `json:"metric"`
			Values [][]json.RawMessage `json:"values"`
			Value  []json.RawMessage   `json:"value"`
		}
		if err := json.Unmarshal(resp.Data.Result, &series); err != nil {
			return metricResult{}, err
		}
		for _, s := range series {
			ms := metricSeries{labels: s.Metric, points: map[string]string{}}
			if ms.labels == nil {
				ms.labels = map[string]string{}
			}
			values := s.Values
			if out.resultType == "vector" {
				values = [][]json.RawMessage{s.Value}
			}
			for _, p := range values {
				ts, v, err := point(p)
				if err != nil {
					return metricResult{}, err
				}
				ms.points[ts] = v
			}
			key := labelsKey(ms.labels)
			if prev, dup := out.series[key]; dup {
				// A series written twice keeps every point; a repeated point is a difference of its own.
				for ts, v := range ms.points {
					if _, ok := prev.points[ts]; ok {
						prev.points[ts+" (twice)"] = v
					} else {
						prev.points[ts] = v
					}
				}
				continue
			}
			out.series[key] = ms
		}
	case "scalar":
		var p []json.RawMessage
		if err := json.Unmarshal(resp.Data.Result, &p); err != nil {
			return metricResult{}, err
		}
		ts, v, err := point(p)
		if err != nil {
			return metricResult{}, err
		}
		out.series[""] = metricSeries{labels: map[string]string{}, points: map[string]string{ts: v}}
	default:
		return metricResult{}, fmt.Errorf("result type %q", out.resultType)
	}
	return out, nil
}

// proxyErrorText is the error text of a proxy error answer: Loki's error body is the text itself; a JSON body's
// error or message field is read too.
func proxyErrorText(body string) string {
	var j struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	if json.Unmarshal([]byte(body), &j) == nil {
		if j.Error != "" {
			return j.Error
		}
		if j.Message != "" {
			return j.Message
		}
	}
	return body
}

// metricLabelClass names the kind of a series label for the difference classes.
func metricLabelClass(name string) string {
	switch {
	case name == "__error__" || name == "__error_details__":
		return name
	case name == "detected_level":
		return "detected_level"
	case strings.HasSuffix(name, "_extracted"):
		return "an _extracted label"
	}
	return "a label"
}

// metricDiff is one run's comparison: its difference classes, an example of each, and whether it matched only
// within the float tolerance.
type metricDiff struct {
	classes map[string]string
	float   bool
}

func (d *metricDiff) add(class, example string) {
	if _, ok := d.classes[class]; !ok {
		d.classes[class] = example
	}
}

// compareMetric holds the handler's answer to Loki's.
func compareMetric(loki metricLokiAnswer, run metricHandlerRun) metricDiff {
	d := metricDiff{classes: map[string]string{}}
	if loki.Status != run.status {
		lokiText, proxyText := loki.Error, proxyErrorText(run.body)
		if loki.Status == 200 {
			lokiText = "an answer"
		}
		if run.status == 200 {
			proxyText = "an answer"
		}
		d.add(fmt.Sprintf("answers status %d where Loki answers %d", run.status, loki.Status), fmt.Sprintf("proxy %.200q, Loki %.200q", proxyText, lokiText))
		return d
	}
	if loki.Status != 200 {
		if got := strings.TrimSpace(proxyErrorText(run.body)); got != strings.TrimSpace(loki.Error) {
			d.add("writes another error text", fmt.Sprintf("proxy %.200q, Loki %.200q", got, loki.Error))
		}
		return d
	}
	want, err := decodeMetricBody(loki.Body)
	if err != nil {
		d.add("Loki's answer does not decode", err.Error())
		return d
	}
	got, err := decodeMetricBody([]byte(run.body))
	if err != nil {
		d.add("the answer does not decode", err.Error())
		return d
	}
	if got.resultType != want.resultType {
		d.add("answers another result type", fmt.Sprintf("%s, Loki %s", got.resultType, want.resultType))
		return d
	}
	var missing, extra []metricSeries
	for k, w := range want.series {
		g, ok := got.series[k]
		if !ok {
			missing = append(missing, w)
			continue
		}
		comparePoints(&d, k, g, w)
	}
	for k, g := range got.series {
		if _, ok := want.series[k]; !ok {
			extra = append(extra, g)
		}
	}
	classifySeries(&d, missing, extra)
	return d
}

// comparePoints compares the points of a series both sides answer.
func comparePoints(d *metricDiff, key string, got, want metricSeries) {
	for ts, w := range want.points {
		g, ok := got.points[ts]
		switch {
		case !ok:
			d.add("misses a point Loki has", fmt.Sprintf("{%s} at %s: Loki %s", key, ts, w))
		case g != w:
			gf, gerr := strconv.ParseFloat(g, 64)
			wf, werr := strconv.ParseFloat(w, 64)
			if gerr == nil && werr == nil && math.Abs(gf-wf) <= metricFloatTolerance*math.Max(math.Abs(gf), math.Abs(wf)) {
				d.float = true
				continue
			}
			d.add("writes another value", fmt.Sprintf("{%s} at %s: %s, Loki %s", key, ts, g, w))
		}
	}
	for ts, g := range got.points {
		if _, ok := want.points[ts]; !ok {
			d.add("adds a point Loki does not have", fmt.Sprintf("{%s} at %s: %s", key, ts, g))
		}
	}
}

// classifySeries classes the series one side has and the other does not: by the labels a missing series differs
// in from the nearest extra one (one class per kind of label), or as a series missed or added outright.
func classifySeries(d *metricDiff, missing, extra []metricSeries) {
	used := make([]bool, len(extra))
	sort.Slice(missing, func(i, j int) bool { return labelsKey(missing[i].labels) < labelsKey(missing[j].labels) })
	sort.Slice(extra, func(i, j int) bool { return labelsKey(extra[i].labels) < labelsKey(extra[j].labels) })
	for _, w := range missing {
		best, bestShared := -1, -1
		for i, g := range extra {
			shared := 0
			for k, v := range w.labels {
				if g.labels[k] == v {
					shared++
				}
			}
			if shared > bestShared {
				best, bestShared = i, shared
			}
		}
		if best < 0 || (bestShared == 0 && len(w.labels) > 0) {
			d.add("misses a series Loki has", fmt.Sprintf("{%s}", labelsKey(w.labels)))
			continue
		}
		used[best] = true
		g := extra[best]
		example := fmt.Sprintf("{%s}, Loki {%s}", labelsKey(g.labels), labelsKey(w.labels))
		for k, v := range w.labels {
			gv, ok := g.labels[k]
			switch {
			case !ok:
				d.add("series: misses "+metricLabelClass(k), example)
			case gv != v:
				d.add("series: writes another value of "+metricLabelClass(k), example)
			}
		}
		for k := range g.labels {
			if _, ok := w.labels[k]; !ok {
				d.add("series: adds "+metricLabelClass(k), example)
			}
		}
	}
	for i, g := range extra {
		if !used[i] {
			d.add("adds a series Loki does not have", fmt.Sprintf("{%s}", labelsKey(g.labels)))
		}
	}
}

// ---- the section ----

// metricKindCounts are the counts of one profile and request kind.
type metricKindCounts struct {
	// Runs are the requests compared; NotComparable the others, by reason.
	Runs          int            `json:"runs"`
	NotComparable map[string]int `json:"not_comparable,omitempty"`
	// PlanRuns are the runs the plan's route answers (the route rule routes them and the handler sent its texts);
	// NotPlan the others, by reason.
	PlanRuns int            `json:"plan_runs"`
	NotPlan  map[string]int `json:"not_plan,omitempty"`
	// PlanMatchesLoki are the plan's runs that answer as Loki does; TranslatorMatchesLoki every run that does
	// (reported); FloatTolerance the matching runs with a value equal only within the float tolerance.
	PlanMatchesLoki int `json:"plan_matches_loki"`
	// PlanMatchesLokiEmpty are the plan's matching runs where Loki answers no series (or an error): both sides
	// empty is a match the data says little about, counted apart.
	PlanMatchesLokiEmpty  int `json:"plan_matches_loki_empty"`
	TranslatorMatchesLoki int `json:"translator_matches_loki"`
	FloatTolerance        int `json:"float_tolerance"`
	// PlanDiffersByClass counts, by difference class, the plan's runs that differ from Loki.
	PlanDiffersByClass       map[string]int `json:"plan_differs_by_class,omitempty"`
	TranslatorDiffersByClass map[string]int `json:"translator_differs_by_class,omitempty"`
}

type metricSection struct {
	Queries int `json:"queries"`
	Entries int `json:"entries"`
	// Excluded counts the metric corpus rows left out, by reason.
	Excluded map[string]int                          `json:"excluded"`
	Profiles map[string]map[string]*metricKindCounts `json:"profiles"`
}

// TestPlanMatchesLokiOnMetricQueries runs the metric differential over the recorded answers (LVP_RUN_ORACLES=1
// records them first) and holds its counts to the loki_metric_differential section of query-plan.json.
//
// conformance: architecture-syntax-package-layering
func TestPlanMatchesLokiOnMetricQueries(t *testing.T) {
	if testing.Short() && os.Getenv("CI") == "" && os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("the metric differential runs every routed metric row on a fresh proxy; run without -short")
	}
	t.Cleanup(querycorpus.PauseRecording())
	scope := metricDiffQueries(t)
	queries := scope.queries
	if len(queries) < 100 && os.Getenv("LVP_METRIC_DIFF_LIMIT") == "" {
		t.Fatalf("%d metric queries the plan routes, expected the curated rows and the capture sample", len(queries))
	}
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordMetricAnswers(t, metricAnswersPath, queries)
	}
	answers := readMetricAnswers(t)
	known := readMetricKnown(t)
	if raceDetectorEnabled {
		// A sample for the race detector (every eighth row): its differences must be listed; the counts are held
		// without -race.
		var sample []metricQuery
		for i := 0; i < len(queries); i += 8 {
			sample = append(sample, queries[i])
		}
		_, stale, _, differs := runMetricDiff(t, sample, answers)
		if len(stale) > 0 {
			t.Fatalf("%d recorded answers are stale (first: %s): record them again with LVP_RUN_ORACLES=1", len(stale), stale[0])
		}
		for id, classes := range differs {
			for c := range classes {
				if known[id].Classes[c] == "" {
					t.Errorf("the plan differs from Loki on %s: %q, and %s names no registry case for it", id, c, metricKnownPath)
				}
			}
		}
		return
	}
	sec, stale, examples, differs := runMetricDiff(t, queries, answers)
	sec.Excluded = scope.excluded
	if len(stale) > 0 {
		t.Fatalf("%d recorded answers are stale for the current corpus, fixtures, requests or handler (first: %s): record them again with "+
			"LVP_RUN_ORACLES=1 go test ./internal/querycorpus -run '^TestPlanMatchesLokiOnMetricQueries$' (a module cache whose "+
			"VictoriaLogs checksum matches, see test/oraclerun) and commit %s", len(stale), stale[0], metricAnswersPath)
	}
	if len(answers) != len(queries) && os.Getenv("LVP_METRIC_DIFF_LIMIT") == "" {
		t.Errorf("%d recorded answers for %d queries: record them again", len(answers), len(queries))
	}
	reportMetricDiff(t, sec, examples)
	if os.Getenv("LVP_METRIC_DIFF_LIMIT") != "" {
		return
	}
	checkMetricKnown(t, known, differs)
	checkMetricSection(t, sec)
}

// metricExample is one run of a difference class, for the report.
type metricExample struct {
	Profile, Kind, Query, Class, Example string
}

// runMetricDiff replays every query's requests through the handler and compares them with Loki's answers.
func runMetricDiff(t *testing.T, queries []metricQuery, answers map[string]*metricRecord) (metricSection, []string, []metricExample, map[string]map[string]bool) {
	t.Helper()
	sec := metricSection{Queries: len(queries), Profiles: map[string]map[string]*metricKindCounts{}}
	for _, prof := range diffProfiles() {
		sec.Profiles[prof.name] = map[string]*metricKindCounts{}
		for _, k := range metricKinds() {
			sec.Profiles[prof.name][k.name] = &metricKindCounts{NotComparable: map[string]int{}, NotPlan: map[string]int{},
				PlanDiffersByClass: map[string]int{}, TranslatorDiffersByClass: map[string]int{}}
		}
	}
	var stale []string
	var examples []metricExample
	differs := map[string]map[string]bool{}
	for _, mq := range queries {
		rec := answers[mq.q]
		if rec == nil || rec.Inputs != mq.inputsHash() {
			stale = append(stale, "inputs of "+mq.q)
			continue
		}
		sec.Entries += len(mq.fix.Entries)
		for _, prof := range diffProfiles() {
			recorded := rec.VL[prof.version]
			for _, mr := range mq.reqs {
				c := sec.Profiles[prof.name][mr.kind]
				loki, ok := rec.Loki[mr.kind]
				if !ok {
					stale = append(stale, mr.kind+" of "+mq.q)
					continue
				}
				if loki.Status == 400 && strings.HasPrefix(loki.Error, "parse error") {
					c.NotComparable["Loki rejects the query at parse"]++
					continue
				}
				run := runMetricHandler(t, prof, mr, &metricFront{version: prof.version, answers: recorded})
				if len(run.missing) > 0 {
					stale = append(stale, fmt.Sprintf("%s %s VictoriaLogs request %s of %s", prof.name, mr.kind, run.missing[0], mq.q))
					continue
				}
				c.Runs++
				d := compareMetric(loki, run)
				matches := len(d.classes) == 0
				if matches {
					c.TranslatorMatchesLoki++
					if d.float {
						c.FloatTolerance++
					}
				} else {
					for cl := range d.classes {
						c.TranslatorDiffersByClass[cl]++
					}
				}
				route := mq.routes[prof.name][mr.kind]
				switch {
				case route == nil:
					c.NotPlan["the route rule delegates the request"]++
					continue
				case len(run.sent) == 0:
					// Answered before any request to VictoriaLogs (Loki's resolution limit): the route rule predicts
					// the rejection, and no text decides the answer.
				case !planSent(route, run.sent):
					c.NotPlan["the handler sends another text than the plan's route"]++
					continue
				}
				c.PlanRuns++
				if matches {
					c.PlanMatchesLoki++
					if lokiEmpty(loki) {
						c.PlanMatchesLokiEmpty++
					}
					continue
				}
				if differs[mq.id] == nil {
					differs[mq.id] = map[string]bool{}
				}
				for cl, ex := range d.classes {
					c.PlanDiffersByClass[cl]++
					differs[mq.id][cl] = true
					examples = append(examples, metricExample{prof.name, mr.kind, mq.q, cl, ex})
				}
			}
		}
	}
	return sec, stale, examples, differs
}

// lokiEmpty is whether Loki's answer holds no series: an error, or a result without a series.
func lokiEmpty(a metricLokiAnswer) bool {
	if a.Status != 200 {
		return true
	}
	r, err := decodeMetricBody(a.Body)
	return err != nil || len(r.series) == 0
}

// ---- known differences, the section and the report ----

// metricKnown are the queries where the plan differs from Loki: per query id, its text and, per difference class,
// the registry case that holds it.
type metricKnown map[string]struct {
	Query   string            `json:"q"`
	Classes map[string]string `json:"classes"`
}

func readMetricKnown(t *testing.T) metricKnown {
	t.Helper()
	k := metricKnown{}
	b, err := os.ReadFile(metricKnownPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return k
		}
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &k); err != nil {
		t.Fatal(err)
	}
	return k
}

// checkMetricKnown holds the plan's differences from Loki to the known list, both ways.
func checkMetricKnown(t *testing.T, known metricKnown, differs map[string]map[string]bool) {
	t.Helper()
	for id, classes := range differs {
		for _, c := range sortedSet(classes) {
			if known[id].Classes[c] == "" {
				t.Errorf("the plan differs from Loki on %s: %q, and %s names no registry case for it", id, c, metricKnownPath)
			}
		}
	}
	for id, k := range known {
		for c, caseID := range k.Classes {
			if !differs[id][c] {
				t.Errorf("%s %q (%s) is listed as a difference, but the plan no longer shows it: drop it from %s", id, c, caseID, metricKnownPath)
			}
		}
	}
}

func checkMetricSection(t *testing.T, got metricSection) {
	t.Helper()
	file, readErr := readPlanFile()
	if os.Getenv("LVP_QUERY_PLAN_UPDATE") == "1" {
		writePlanSection(t, file, "loki_metric_differential", got)
		return
	}
	if readErr != nil {
		t.Fatalf("read %s: %v", planPath, readErr)
	}
	var want metricSection
	if err := json.Unmarshal(file["loki_metric_differential"], &want); err != nil {
		t.Fatalf("%s has no readable loki_metric_differential section: %v", planPath, err)
	}
	for name, kinds := range got.Profiles {
		for kind, g := range kinds {
			w := want.Profiles[name][kind]
			if w == nil {
				t.Errorf("%s %s: no recorded counts", name, kind)
				continue
			}
			if g.PlanMatchesLoki < w.PlanMatchesLoki {
				t.Errorf("%s %s: the plan matches Loki on %d runs, %d before", name, kind, g.PlanMatchesLoki, w.PlanMatchesLoki)
			}
		}
	}
	// Compared as written: an empty count map and a missing one are the same counts.
	g, _ := json.MarshalIndent(got, "", "  ")
	if w, _ := json.MarshalIndent(want, "", "  "); string(g) != string(w) {
		t.Errorf("the loki_metric_differential counts changed: run LVP_QUERY_PLAN_UPDATE=1 go test ./internal/querycorpus -run '^TestPlanMatchesLokiOnMetricQueries$' and commit %s\n%s",
			strings.TrimPrefix(planPath, "../../"), g)
	}
}

// reportMetricDiff logs the headline per profile and request kind; LVP_LOKI_METRIC_DIFF_REPORT names a file to write the
// section and the examples to as JSON.
func reportMetricDiff(t *testing.T, sec metricSection, examples []metricExample) {
	t.Helper()
	pct := func(n, of int) string {
		if of == 0 {
			return "-"
		}
		return strconv.FormatFloat(100*float64(n)/float64(of), 'f', 1, 64) + "%"
	}
	for _, prof := range diffProfiles() {
		for _, k := range metricKinds() {
			c := sec.Profiles[prof.name][k.name]
			t.Logf("%s %s: %d runs, plan routes %d, plan matches Loki %d (%s; %d on non-empty answers), translator %d (%s), float tolerance %d, not compared %v, not the plan's %v",
				prof.name, k.name, c.Runs, c.PlanRuns, c.PlanMatchesLoki, pct(c.PlanMatchesLoki, c.PlanRuns), c.PlanMatchesLoki-c.PlanMatchesLokiEmpty, c.TranslatorMatchesLoki,
				pct(c.TranslatorMatchesLoki, c.Runs), c.FloatTolerance, c.NotComparable, c.NotPlan)
			for _, cl := range sortedKeysInt(c.PlanDiffersByClass) {
				t.Logf("  plan differs: %s: %d", cl, c.PlanDiffersByClass[cl])
			}
		}
	}
	if path := os.Getenv("LVP_LOKI_METRIC_DIFF_REPORT"); path != "" {
		sort.SliceStable(examples, func(i, j int) bool { return examples[i].Class < examples[j].Class })
		b, _ := json.MarshalIndent(map[string]any{"section": sec, "examples": examples}, "", "  ")
		if err := os.WriteFile(path, b, 0o600); err != nil {
			t.Error(err)
		}
	}
}
