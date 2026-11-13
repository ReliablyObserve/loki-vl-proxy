package oraclerun

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// ProcessCase is one log query, the labels of a stream and one line of it.
type ProcessCase struct {
	ID     string            `json:"id"`
	Query  string            `json:"q"`
	Labels map[string]string `json:"labels"`
	Line   string            `json:"line"`
	// Metadata is the line's structured metadata; WithCategories asks for the categories of the labels of a kept line.
	Metadata       map[string]string `json:"metadata,omitempty"`
	WithCategories bool              `json:"categories,omitempty"`
}

// ProcessAnswer is what Loki's pipeline of the query makes of the line (test/oracle/loki process): Keep is whether
// the line passes every stage, Line and Labels are the line and the labels after the stages (an __error__ label
// shows there), Error is set when Loki rejects the query.
type ProcessAnswer struct {
	ID string `json:"id"`
	// Selected is whether the stream selector's matchers select the stream.
	Selected bool              `json:"selected,omitempty"`
	Keep     bool              `json:"keep"`
	Line     string            `json:"line,omitempty"`
	Labels   map[string]string `json:"labels,omitempty"`
	// Stream, StructuredMetadata and Parsed are Loki's categories of Labels, for a case with WithCategories.
	Stream             map[string]string `json:"stream,omitempty"`
	StructuredMetadata map[string]string `json:"structured_metadata,omitempty"`
	Parsed             map[string]string `json:"parsed,omitempty"`
	Error              string            `json:"error,omitempty"`
}

// LokiProcess runs the Loki oracle in oracleDir (test/oracle/loki) over cases and returns its answers, in the order
// of cases.
func LokiProcess(oracleDir string, cases []ProcessCase) ([]ProcessAnswer, error) {
	work, err := os.MkdirTemp("", "loki-process")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(work) }()
	bin, err := Build(oracleDir, "", work, "loki")
	if err != nil {
		return nil, err
	}
	in, out := filepath.Join(work, "in.jsonl"), filepath.Join(work, "out.jsonl")
	f, err := os.Create(in)
	if err != nil {
		return nil, err
	}
	enc := json.NewEncoder(f)
	for i, c := range cases {
		c.ID = fmt.Sprint(i)
		if err := enc.Encode(c); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	if err := Run(bin, "process", "-in", in, "-out", out); err != nil {
		return nil, err
	}
	r, err := os.Open(out)
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.Close() }()
	var answers []ProcessAnswer
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var a ProcessAnswer
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			return nil, err
		}
		answers = append(answers, a)
	}
	if len(answers) != len(cases) {
		return nil, fmt.Errorf("%d answers for %d cases", len(answers), len(cases))
	}
	return answers, sc.Err()
}
