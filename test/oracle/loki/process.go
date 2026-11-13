package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/prometheus/prometheus/model/labels"
)

// The process command answers, for each input, what Loki's pipeline of a log query makes of one line of one
// stream: syntax.ParseLogSelector with validation, Pipeline(), ForStream(labels) and Process(line), read through
// Loki's exported packages only. Keep is whether the line passes every stage; Line and Labels are the line and
// the labels after the stages, so a stage that keeps a line with an __error__ label shows as keep with the label.
// The proxy's tests compare its label filters with these answers. Metadata, when set, is the line's structured
// metadata, handed to Process as Loki's ingester hands it; Stream, StructuredMetadata and Parsed are Loki's
// categories of the labels after the stages (LabelsResult.Stream, StructuredMetadata and Parsed).

// ProcessInput is one log query, the labels of the stream and one line.
type ProcessInput struct {
	ID     string            `json:"id"`
	Query  string            `json:"q"`
	Labels map[string]string `json:"labels"`
	Line   string            `json:"line"`
	// Metadata is the structured metadata of the line, none when empty.
	Metadata map[string]string `json:"metadata,omitempty"`
	// WithCategories asks for the categories of the labels of a kept line.
	WithCategories bool `json:"categories,omitempty"`
}

// ProcessVerdict is Loki's answer for one input; Error is set when Loki rejects the query or cannot build its
// pipeline. Selected is whether the stream selector's matchers select the stream (a missing label is the empty
// value): Loki's index chooses the streams, the pipeline never sees one it does not select, so Keep alone says
// nothing about the selector.
type ProcessVerdict struct {
	ID       string            `json:"id"`
	Selected bool              `json:"selected,omitempty"`
	Keep     bool              `json:"keep"`
	Line     string            `json:"line,omitempty"`
	Labels   map[string]string `json:"labels,omitempty"`
	// Stream, StructuredMetadata and Parsed split Labels into Loki's categories, for a kept line of an input with
	// WithCategories.
	Stream             map[string]string `json:"stream,omitempty"`
	StructuredMetadata map[string]string `json:"structured_metadata,omitempty"`
	Parsed             map[string]string `json:"parsed,omitempty"`
	Error              string            `json:"error,omitempty"`
}

func process(in, out string) error {
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
		var x ProcessInput
		if err := json.Unmarshal(sc.Bytes(), &x); err != nil {
			return err
		}
		if err := enc.Encode(processOne(x)); err != nil {
			return err
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return w.Flush()
}

// processOne is Loki's answer for x. A panic of Loki's code is an error.
func processOne(x ProcessInput) (v ProcessVerdict) {
	v.ID = x.ID
	defer func() {
		if p := recover(); p != nil {
			v = ProcessVerdict{ID: x.ID, Error: fmt.Sprintf("panic: %v", p)}
		}
	}()
	sel, err := syntax.ParseLogSelector(x.Query, true)
	if err != nil {
		v.Error = err.Error()
		return v
	}
	pipe, err := sel.Pipeline()
	if err != nil {
		v.Error = err.Error()
		return v
	}
	v.Selected = true
	for _, m := range sel.Matchers() {
		if !m.Matches(x.Labels[m.Name]) {
			v.Selected = false
		}
	}
	metadata := labels.EmptyLabels()
	if len(x.Metadata) > 0 {
		metadata = labels.FromMap(x.Metadata)
	}
	line, res, keep := pipe.ForStream(labels.FromMap(x.Labels)).Process(0, []byte(x.Line), metadata)
	v.Keep = keep
	if keep {
		v.Line = string(line)
		v.Labels = res.Labels().Map()
		if x.WithCategories {
			v.Stream = res.Stream().Map()
			v.StructuredMetadata = res.StructuredMetadata().Map()
			v.Parsed = res.Parsed().Map()
		}
	}
	return v
}
