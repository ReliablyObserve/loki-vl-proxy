// Command engine evaluates LogQL metric queries with Loki's own query engine (pkg/logql) over a fixed data set, as
// an oracle for the proxy's metric answers (internal/querycorpus plan_loki_vl_metric_diff_test.go).
//
//	go run ./engine -in datasets.jsonl -out answers.jsonl
//
// Each input line is a data set (streams, each its labels and its entries) and the requests over it, each a Loki
// query or query_range request form, URL-encoded. Each output line is one request's answer: the HTTP status and
// the body Loki's HTTP layer writes for it. The request is read by Loki's HTTP parameter parser
// (loghttp.ParseInstantQuery, loghttp.ParseRangeQuery), a range request's bounds are aligned to its step as a
// query frontend with align_queries_with_step does, the query runs on logql.NewEngine over an in-memory querier,
// the result is written by Loki's response encoder (marshal.WriteQueryResponseJSON) and an error gets Loki's
// client status and text (server.ClientHTTPStatusAndError).
//
// The querier hands the engine, for each stream the selector selects, every entry of the stream inside the time
// range the engine asks for, through the query's own sample extractor (SelectSampleParams.Expr, Extractors,
// ForStream, Process): the engine does the windowing, the range functions, the aggregations and the binary
// operations. Query splitting, sharding and the results cache of a query frontend are not modelled: the answer is
// the un-split engine result, the semantic reference.
//
// It is a command of its own, like ./align, so the parse oracle (the module's root command), which every product
// pull request builds, does not compile Loki's engine and what it imports. Like the rest of this module it is used
// only in tests (see ../main.go).
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/iter"
	"github.com/grafana/loki/v3/pkg/loghttp"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logqlmodel/stats"
	"github.com/grafana/loki/v3/pkg/util/httpreq"
	"github.com/grafana/loki/v3/pkg/util/marshal"
	"github.com/grafana/loki/v3/pkg/util/server"
	"github.com/grafana/loki/v3/pkg/util/validation"
	"github.com/prometheus/prometheus/model/labels"
)

// Entry is one log line of a stream: its time in nanoseconds, the line and its structured metadata.
type Entry struct {
	TS       int64             `json:"ts"`
	Line     string            `json:"line"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Stream is a stream's labels and its entries (in any order).
type Stream struct {
	Labels  map[string]string `json:"labels"`
	Entries []Entry           `json:"entries"`
}

// Request is a Loki request: Instant is a /loki/api/v1/query request, else /loki/api/v1/query_range; Form is its
// form, URL-encoded.
type Request struct {
	ID      string `json:"id"`
	Instant bool   `json:"instant,omitempty"`
	Form    string `json:"form"`
}

// Input is one data set and the requests over it.
type Input struct {
	Streams  []Stream  `json:"streams"`
	Requests []Request `json:"requests"`
}

// Answer is Loki's answer to one request: the HTTP status and the body (the response JSON for a 200, the error
// text otherwise).
type Answer struct {
	ID     string          `json:"id"`
	Status int             `json:"status"`
	Body   json.RawMessage `json:"body,omitempty"`
	Error  string          `json:"error,omitempty"`
}

func main() {
	in := flag.String("in", "", "inputs, one data set per line")
	out := flag.String("out", "", "output file, one answer per request")
	flag.Parse()
	if err := run(*in, *out); err != nil {
		fmt.Fprintln(os.Stderr, "engine:", err)
		os.Exit(1)
	}
}

func run(in, out string) error {
	src, err := os.Open(in)
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }() // read only: a close error loses nothing
	dst, err := os.Create(out)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(dst)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	sc := bufio.NewScanner(src)
	sc.Buffer(make([]byte, 1<<20), 1<<28)
	for sc.Scan() {
		var input Input
		if err := json.Unmarshal(sc.Bytes(), &input); err != nil {
			return err
		}
		q := newQuerier(input.Streams)
		for _, r := range input.Requests {
			if err := enc.Encode(answer(q, r)); err != nil {
				return err
			}
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return dst.Close()
}

// limits are the engine's per-tenant limits: none that a test data set reaches.
type limits struct{}

func (limits) MaxQuerySeries(context.Context, string) int                        { return 1 << 30 }
func (limits) MaxQueryRange(context.Context, string) time.Duration               { return 0 }
func (limits) QueryTimeout(context.Context, string) time.Duration                { return time.Hour }
func (limits) BlockedQueries(context.Context, string) []*validation.BlockedQuery { return nil }
func (limits) EnableMultiVariantQueries(string) bool                             { return false }
func (limits) MaxScanTaskParallelism(string) int                                 { return 1 }
func (limits) DebugEngineTasks(string) bool                                      { return false }
func (limits) DebugEngineStreams(string) bool                                    { return false }

// answer runs one request. A panic of Loki's code is an error answer.
func answer(q *querier, r Request) (a Answer) {
	a.ID = r.ID
	defer func() {
		if p := recover(); p != nil {
			a = Answer{ID: r.ID, Status: -1, Error: fmt.Sprintf("panic: %v", p)}
		}
	}()
	path := "/loki/api/v1/query_range"
	if r.Instant {
		path = "/loki/api/v1/query"
	}
	httpReq := httptest.NewRequest(http.MethodGet, path+"?"+r.Form, nil)
	if err := httpReq.ParseForm(); err != nil {
		return badRequest(a, err)
	}
	var params logql.LiteralParams
	var err error
	if r.Instant {
		var iq *loghttp.InstantQuery
		if iq, err = loghttp.ParseInstantQuery(httpReq); err != nil {
			return badRequest(a, err)
		}
		params, err = logql.NewLiteralParams(iq.Query, iq.Ts, iq.Ts, 0, 0, iq.Direction, iq.Limit, nil, nil)
	} else {
		var rq *loghttp.RangeQuery
		if rq, err = loghttp.ParseRangeQuery(httpReq); err != nil {
			return badRequest(a, err)
		}
		start, end := rq.Start, rq.End
		// A query frontend with align_queries_with_step moves both bounds down to a multiple of the step, in
		// milliseconds.
		if stepMs := rq.Step.Milliseconds(); stepMs > 0 {
			start = time.UnixMilli(start.UnixMilli() / stepMs * stepMs)
			end = time.UnixMilli(end.UnixMilli() / stepMs * stepMs)
		}
		params, err = logql.NewLiteralParams(rq.Query, start, end, rq.Step, rq.Interval, rq.Direction, rq.Limit, nil, nil)
	}
	if err != nil {
		return errAnswer(a, err)
	}
	engine := logql.NewEngine(logql.EngineOpts{}, q, limits{}, log.NewNopLogger())
	ctx := user.InjectOrgID(context.Background(), "fake")
	res, err := engine.Query(params).Exec(ctx)
	if err != nil {
		return errAnswer(a, err)
	}
	var b bytes.Buffer
	if err := marshal.WriteQueryResponseJSON(res.Data, nil, stats.Result{}, &b, httpreq.EncodingFlags{}); err != nil {
		return errAnswer(a, err)
	}
	// The answer keeps the result; the statistics of an in-memory run say nothing.
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string          `json:"resultType"`
			Result     json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(b.Bytes(), &resp); err != nil {
		return errAnswer(a, err)
	}
	body, err := json.Marshal(resp)
	if err != nil {
		return errAnswer(a, err)
	}
	a.Status = http.StatusOK
	a.Body = body
	return a
}

// badRequest is the answer to a request Loki's HTTP parameter parser rejects: the query frontend answers it 400
// with the parser's text before any query runs.
func badRequest(a Answer, err error) Answer {
	a.Status = http.StatusBadRequest
	a.Error = err.Error()
	return a
}

// errAnswer is the answer to an error of the engine: Loki's client status and text for it.
func errAnswer(a Answer, err error) Answer {
	status, clientErr := server.ClientHTTPStatusAndError(err)
	a.Status = status
	if clientErr != nil {
		a.Error = clientErr.Error()
	} else {
		a.Error = err.Error()
	}
	return a
}

// querier is an in-memory logql.Querier over a data set's streams.
type querier struct {
	streams []stream
}

type stream struct {
	labels  labels.Labels
	hash    uint64
	entries []Entry
}

func newQuerier(in []Stream) *querier {
	q := &querier{}
	for _, s := range in {
		entries := append([]Entry(nil), s.Entries...)
		sort.SliceStable(entries, func(i, j int) bool { return entries[i].TS < entries[j].TS })
		ls := labels.FromMap(s.Labels)
		q.streams = append(q.streams, stream{labels: ls, hash: ls.Hash(), entries: entries})
	}
	return q
}

var errNoLogs = errors.New("the engine oracle answers metric queries only")

func (q *querier) SelectLogs(context.Context, logql.SelectLogParams) (iter.EntryIterator, error) {
	return nil, errNoLogs
}

// SelectSamples extracts the samples of every selected stream's entries in [start, end] with the query's sample
// extractors; each series is the labels the extractor gives a sample, its samples in time order.
func (q *querier) SelectSamples(_ context.Context, p logql.SelectSampleParams) (iter.SampleIterator, error) {
	expr, err := p.Expr()
	if err != nil {
		return nil, err
	}
	sel, err := expr.Selector()
	if err != nil {
		return nil, err
	}
	extractors, err := expr.Extractors()
	if err != nil {
		return nil, err
	}
	from, through := p.Start.UnixNano(), p.End.UnixNano()
	bySeries := map[string]*logproto.Series{}
	var order []string
	for si, s := range q.streams {
		if !selects(sel.Matchers(), s.labels) {
			continue
		}
		for _, ex := range extractors {
			sx := ex.ForStream(s.labels)
			for ei, e := range s.entries {
				if e.TS < from || e.TS > through {
					continue
				}
				metadata := labels.EmptyLabels()
				if len(e.Metadata) > 0 {
					metadata = labels.FromMap(e.Metadata)
				}
				samples, ok := sx.ProcessString(e.TS, e.Line, metadata)
				if !ok {
					continue
				}
				for _, smp := range samples {
					key := smp.Labels.String()
					sr := bySeries[key]
					if sr == nil {
						sr = &logproto.Series{Labels: key, StreamHash: s.hash}
						bySeries[key] = sr
						order = append(order, key)
					}
					sr.Samples = append(sr.Samples, logproto.Sample{Timestamp: e.TS, Value: smp.Value, Hash: uint64(si)<<32 | uint64(ei+1)})
				}
			}
		}
	}
	series := make([]logproto.Series, 0, len(order))
	for _, k := range order {
		sr := bySeries[k]
		sort.SliceStable(sr.Samples, func(i, j int) bool { return sr.Samples[i].Timestamp < sr.Samples[j].Timestamp })
		series = append(series, *sr)
	}
	return iter.NewMultiSeriesIterator(series), nil
}

func selects(ms []*labels.Matcher, ls labels.Labels) bool {
	for _, m := range ms {
		if !m.Matches(ls.Get(m.Name)) {
			return false
		}
	}
	return true
}
