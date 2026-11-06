// Command align decodes Loki range query requests as a Loki query frontend with align_queries_with_step does, as
// an oracle for the proxy's step alignment (internal/proxy step_align_diff_test.go).
//
//	go run ./align -in forms.jsonl -out answers.jsonl
//
// Each input line is {"id", "form"}, the request's form URL-encoded; each output line is the input with Loki's
// answer. It is a command of its own so the parse oracle (the module's root command), which every product pull
// request builds, does not compile Loki's HTTP parameter package and what it imports. Like the rest of this module
// it is used only in tests (see ../main.go).
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"time"

	"github.com/grafana/loki/v3/pkg/loghttp"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
)

// AlignInput is a range query request's form, URL-encoded.
type AlignInput struct {
	ID   string `json:"id"`
	Form string `json:"form"`
}

// AlignAnswer is what a Loki query frontend with align_queries_with_step makes of the request: whether it decodes
// (or the error it answers 400 with), the step it keeps in milliseconds, and the bounds it passes on after
// aligning, in nanoseconds. A step under a millisecond is kept as 0 and has no aligned bounds: the alignment would
// divide by it.
type AlignAnswer struct {
	ID       string `json:"id"`
	Form     string `json:"form"`
	Accepted bool   `json:"accepted"`
	Error    string `json:"error,omitempty"`
	StepMs   int64  `json:"step_ms"`
	StartNs  int64  `json:"start_ns"`
	EndNs    int64  `json:"end_ns"`
}

func main() {
	in := flag.String("in", "", "inputs, one JSON object per line")
	out := flag.String("out", "", "output file")
	flag.Parse()
	if err := align(*in, *out); err != nil {
		fmt.Fprintln(os.Stderr, "align:", err)
		os.Exit(1)
	}
}

func align(in, out string) error {
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
	sc.Buffer(make([]byte, 1<<16), 1<<20)
	for sc.Scan() {
		var input AlignInput
		if err := json.Unmarshal(sc.Bytes(), &input); err != nil {
			return err
		}
		if err := enc.Encode(alignOne(input)); err != nil {
			return err
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

func alignOne(input AlignInput) (a AlignAnswer) {
	a = AlignAnswer{ID: input.ID, Form: input.Form}
	defer func() {
		if p := recover(); p != nil {
			a = AlignAnswer{ID: input.ID, Form: input.Form, Error: fmt.Sprintf("panic: %v", p)}
		}
	}()
	httpReq := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+input.Form, nil)
	if err := httpReq.ParseForm(); err != nil {
		a.Error = err.Error()
		return a
	}
	// The query frontend's decode of a range query (pkg/querier/queryrange/codec.go parseRangeQuery): Loki's
	// range query parser, then the query must parse; the step is kept in milliseconds.
	decoded, err := loghttp.ParseRangeQuery(httpReq)
	if err == nil {
		_, err = syntax.ParseExpr(decoded.Query)
	}
	if err != nil {
		a.Error = err.Error()
		return a
	}
	a.Accepted, a.StepMs = true, decoded.Step.Milliseconds()
	if a.StepMs == 0 {
		return a
	}
	// Both bounds move down to a multiple of the step, in milliseconds
	// (pkg/querier/queryrange/queryrangebase/step_align.go).
	a.StartNs = time.UnixMilli(decoded.Start.UnixMilli() / a.StepMs * a.StepMs).UnixNano()
	a.EndNs = time.UnixMilli(decoded.End.UnixMilli() / a.StepMs * a.StepMs).UnixNano()
	return a
}
