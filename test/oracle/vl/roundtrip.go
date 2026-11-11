package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// The roundtrip command answers, for each input query, whether VictoriaLogs' /select/logsql/query handler can run
// it without the panic of Query.Clone. The handler (app/vlselect/logsql/logsql.go ProcessQueryRequest and
// parseCommonArgs) parses the query (ParseQueryAtTimestamp), adds the request's _time:[start, end] filter
// (AddTimeFilter) and, for a limit, `| sort by (_time desc)` when the query can return the last N results and
// `| offset 0 limit <limit>`; vlstorage.RunQuery (app/vlstorage/main.go) then asks GetLastNResultsQuery for the
// optimized query, which clones the query (lib/logstorage/parser.go Query.Clone): the query is printed with String
// and parsed again, and a text that does not parse again is logger.Panicf ("BUG: cannot parse"), which ends the
// VictoriaLogs process. Every other clone (the binary search of the last N rows, CloneWithTimeFilter of
// hits and stats_query_range, the cluster's remote query) prints and parses the same way, so the command also
// clones the query before and after the limit pipes, and checks that the printed text reads back to itself.
//
// The steps are the handler's, in its order, through lib/logstorage's exported API; a panic of VictoriaLogs' code is
// recovered and recorded, never passed on.

// RoundtripInput is a query as the proxy sends it in the query arg of /select/logsql/query.
type RoundtripInput struct {
	ID    string `json:"id"`
	Query string `json:"q"`
}

// RoundtripVerdict is VictoriaLogs' answer: Accept is whether the handler's first parse accepts the query (a
// rejected query is a 400, not a crash); OK is whether every clone parsed again and printed the same text; Panic
// is the recovered panic or the error of the parse that would have panicked; Printed and Reprinted the text before
// and after the clone that differed.
type RoundtripVerdict struct {
	ID        string `json:"id"`
	Accept    bool   `json:"accept"`
	Error     string `json:"error,omitempty"`
	OK        bool   `json:"ok"`
	Panic     string `json:"panic,omitempty"`
	Printed   string `json:"printed,omitempty"`
	Reprinted string `json:"reprinted,omitempty"`
}

// roundtripLimit is the limit the proxy sends with a log query.
const roundtripLimit = 1000

func roundtrip(in, out string) error {
	f, err := os.Open(in)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	var verdicts []RoundtripVerdict
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		var x RoundtripInput
		if err := json.Unmarshal(sc.Bytes(), &x); err != nil {
			return err
		}
		verdicts = append(verdicts, roundtripOne(x))
	}
	if err := sc.Err(); err != nil {
		return err
	}
	dst, err := os.Create(out)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(dst)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for _, v := range verdicts {
		if err := enc.Encode(v); err != nil {
			_ = dst.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		_ = dst.Close()
		return err
	}
	return dst.Close()
}

// roundtripOne runs the handler's steps on x.
func roundtripOne(x RoundtripInput) (v RoundtripVerdict) {
	v.ID = x.ID
	defer func() {
		if p := recover(); p != nil {
			v.OK = false
			v.Panic = fmt.Sprint(p)
		}
	}()
	timestamp := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	start, end := timestamp-int64(time.Hour), timestamp
	q, err := logstorage.ParseQueryAtTimestamp(x.Query, timestamp)
	if err != nil {
		v.Error = err.Error()
		return v
	}
	v.Accept = true
	q.AddTimeFilter(start, end)
	if !cloneReadsBack(q, timestamp, &v) {
		return v
	}
	if q.CanReturnLastNResults() {
		q.AddPipeSortByTimeDesc()
	}
	q.AddPipeOffsetLimit(0, roundtripLimit)
	if !cloneReadsBack(q, timestamp, &v) {
		return v
	}
	if qOpt, _, _ := q.GetLastNResultsQuery(); qOpt != nil && !cloneReadsBack(qOpt, timestamp, &v) {
		return v
	}
	v.OK = true
	return v
}

// sameBytes reports two texts made of the same bytes, as a reordering of the same filters is.
func sameBytes(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	x, y := []byte(a), []byte(b)
	slices.Sort(x)
	slices.Sort(y)
	return string(x) == string(y)
}

// cloneReadsBack is Query.Clone without its panic, twice over: the printed query parses again (else v.Panic is the
// error Clone panics with), and the clone's own print parses again and prints the same text (else v.Printed and
// v.Reprinted are the two prints). VictoriaLogs' parser may put the filters of the first print in another order
// (a stream filter goes first), which means the same query: the first reprint may differ from the print by the
// order of its bytes only (any other difference is a print that reads back as another query, such as a quoted
// field read back as VictoriaLogs' own filter), and the second must be a fixed point, so a clone of a clone, as the
// binary search of the last N rows makes, reads the same query.
func cloneReadsBack(q *logstorage.Query, timestamp int64, v *RoundtripVerdict) bool {
	printed := q.String()
	for range 2 {
		again, err := logstorage.ParseQueryAtTimestamp(printed, timestamp)
		if err != nil {
			v.Panic = fmt.Sprintf("BUG: cannot parse %q: %s", printed, err)
			return false
		}
		reprinted := again.String()
		if reprinted == printed {
			break
		}
		v.Printed, v.Reprinted = printed, reprinted
		if !sameBytes(printed, reprinted) {
			// Not a reordering of the same filters: the print reads back as another query.
			return false
		}
		printed = reprinted
	}
	if again, err := logstorage.ParseQueryAtTimestamp(printed, timestamp); err != nil || again.String() != printed {
		return false
	}
	v.Printed, v.Reprinted = "", ""
	// The clone itself, as VictoriaLogs runs it: a panic is recovered by roundtripOne.
	_ = q.Clone(timestamp)
	return true
}
