package oraclerun

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// EngineEntry is one log line of a stream: its time in nanoseconds, the line and its structured metadata.
type EngineEntry struct {
	TS       int64             `json:"ts"`
	Line     string            `json:"line"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// EngineStream is a stream's labels and its entries.
type EngineStream struct {
	Labels  map[string]string `json:"labels"`
	Entries []EngineEntry     `json:"entries"`
}

// EngineRequest is a Loki query (Instant) or query_range request, its form URL-encoded.
type EngineRequest struct {
	ID      string `json:"id"`
	Instant bool   `json:"instant,omitempty"`
	Form    string `json:"form"`
}

// EngineDataSet is a data set and the requests over it.
type EngineDataSet struct {
	Streams  []EngineStream  `json:"streams"`
	Requests []EngineRequest `json:"requests"`
}

// EngineAnswer is Loki's answer to one request (test/oracle/loki engine): the HTTP status, and the body Loki's HTTP
// layer writes (status and data, without the statistics) or the error text.
type EngineAnswer struct {
	ID     string          `json:"id"`
	Status int             `json:"status"`
	Body   json.RawMessage `json:"body,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// LokiEngine runs the Loki engine oracle (the engine command of the module in oracleDir, test/oracle/loki) over
// the data sets and returns its answers, by request id.
func LokiEngine(oracleDir string, sets []EngineDataSet) (map[string]EngineAnswer, error) {
	work, err := os.MkdirTemp("", "loki-engine")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(work) }()
	bin, err := Build(filepath.Join(oracleDir, "engine"), "", work, "engine")
	if err != nil {
		return nil, err
	}
	in, out := filepath.Join(work, "in.jsonl"), filepath.Join(work, "out.jsonl")
	f, err := os.Create(in)
	if err != nil {
		return nil, err
	}
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	requests := 0
	for _, s := range sets {
		requests += len(s.Requests)
		if err := enc.Encode(s); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	if err := Run(bin, "-in", in, "-out", out); err != nil {
		return nil, err
	}
	r, err := os.Open(out)
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.Close() }()
	answers := map[string]EngineAnswer{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<28)
	for sc.Scan() {
		var a EngineAnswer
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			return nil, err
		}
		answers[a.ID] = a
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(answers) != requests {
		return nil, fmt.Errorf("%d answers for %d requests", len(answers), requests)
	}
	return answers, nil
}
