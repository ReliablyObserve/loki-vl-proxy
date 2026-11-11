package oraclerun

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// StoreRow is one log entry as an ingestion route hands it to VictoriaLogs (test/oracle/vl store): Stream are the
// stream labels the Loki push API and OpenTelemetry pass first, Fields the rest (structured metadata, attributes,
// _msg).
type StoreRow struct {
	Stream [][2]string `json:"stream,omitempty"`
	Fields [][2]string `json:"fields"`
}

// StoreAnswer is what VictoriaLogs returns for one query over the stored rows: each row its fields as name=value,
// sorted, _time left out; Error is set when VictoriaLogs rejects the query.
type StoreAnswer struct {
	ID    string     `json:"id"`
	Error string     `json:"error,omitempty"`
	Rows  [][]string `json:"rows,omitempty"`
}

// VLStore runs the VictoriaLogs oracle in oracleDir (test/oracle/vl) with modfile ("" for its go.mod) over rows
// and queries: it stores the rows in a VictoriaLogs storage of the pinned version and returns its answer for each
// query, in the order of queries.
func VLStore(oracleDir, modfile string, rows []StoreRow, queries []string) ([]StoreAnswer, error) {
	work, err := os.MkdirTemp("", "vl-store")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(work) }()
	bin, err := Build(oracleDir, modfile, work, "vl")
	if err != nil {
		return nil, err
	}
	in, out := filepath.Join(work, "in.jsonl"), filepath.Join(work, "out.jsonl")
	f, err := os.Create(in)
	if err != nil {
		return nil, err
	}
	enc := json.NewEncoder(f)
	inputs := []any{map[string]any{"id": "stored", "rows": rows}}
	for i, q := range queries {
		inputs = append(inputs, map[string]string{"id": fmt.Sprint(i), "q": q})
	}
	for _, x := range inputs {
		if err := enc.Encode(x); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	if err := Run(bin, "store", "-in", in, "-out", out); err != nil {
		return nil, err
	}
	r, err := os.Open(out)
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.Close() }()
	var answers []StoreAnswer
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var a StoreAnswer
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			return nil, err
		}
		if a.ID != "stored" {
			answers = append(answers, a)
		}
	}
	if len(answers) != len(queries) {
		return nil, fmt.Errorf("%d answers for %d queries", len(answers), len(queries))
	}
	return answers, sc.Err()
}
