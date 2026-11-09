package main

import (
	"bufio"
	"encoding/json"
	"os"

	"github.com/grafana/loki/v3/pkg/logql/syntax"
)

// The pipeline command answers, for each log query, whether Loki builds its pipeline: syntax.ParseLogSelector with
// validation, then Pipeline(), read through Loki's exported syntax package only. A stage Loki parses but refuses to
// build (a pattern line filter with two captures in a row or a named capture) fails there; the proxy's tests
// compare its own rejection and its text with these answers.

// PipelineInput is one log query.
type PipelineInput struct {
	ID    string `json:"id"`
	Query string `json:"q"`
}

// PipelineVerdict is Loki's answer for one query: Accept when it parses and builds, else the error.
type PipelineVerdict struct {
	ID     string `json:"id"`
	Accept bool   `json:"accept"`
	Error  string `json:"error,omitempty"`
}

func pipeline(in, out string) error {
	f, err := os.Open(in)
	if err != nil {
		return err
	}
	defer f.Close()
	o, err := os.Create(out)
	if err != nil {
		return err
	}
	defer o.Close()
	w := bufio.NewWriter(o)
	enc := json.NewEncoder(w)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var x PipelineInput
		if err := json.Unmarshal(sc.Bytes(), &x); err != nil {
			return err
		}
		v := PipelineVerdict{ID: x.ID}
		if sel, err := syntax.ParseLogSelector(x.Query, true); err != nil {
			v.Error = err.Error()
		} else if _, err := sel.Pipeline(); err != nil {
			v.Error = err.Error()
		} else {
			v.Accept = true
		}
		if err := enc.Encode(v); err != nil {
			return err
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return w.Flush()
}
