package querycorpus_test

import (
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

type captured struct {
	Kind   string `json:"kind"`
	Query  string `json:"q"`
	Source string `json:"src"`
	Via    string `json:"via"`
	Direct bool   `json:"direct"`
	Path   string `json:"path"`
}

// Row is one corpus entry: a query a test or client sent in, or, with Path
// set, a LogsQL query the proxy sent to that VictoriaLogs endpoint.
type Row struct {
	ID      string   `json:"id"`
	Lang    string   `json:"lang"`
	Path    string   `json:"path,omitempty"`
	Valid   bool     `json:"valid"`
	Query   string   `json:"q"`
	Sources []string `json:"sources"`
}

func queryID(lang, q string) string {
	sum := sha256.Sum256([]byte(lang + "\x00" + q))
	return "q-" + hex.EncodeToString(sum[:6])
}

// TestBuildQueryCorpus rebuilds the corpus from the capture files of a
// `go test -tags corpuscapture` run with LVP_QUERY_CORPUS_DIR set:
//
//	LVP_QUERY_CORPUS_DIR=$dir go test -tags corpuscapture -short -p 1 ./internal/...
//	LVP_QUERY_CORPUS_DIR=$dir LVP_QUERY_CORPUS_OUT=corpus.jsonl.gz go test ./internal/querycorpus -run TestBuildQueryCorpus
//
// The corpus is a CI artifact, not a committed file.
func TestBuildQueryCorpus(t *testing.T) {
	dir, outPath := os.Getenv("LVP_QUERY_CORPUS_DIR"), os.Getenv("LVP_QUERY_CORPUS_OUT")
	if dir == "" || outPath == "" {
		t.Skip("LVP_QUERY_CORPUS_DIR and LVP_QUERY_CORPUS_OUT are not set")
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no capture files in %s: %v", dir, err)
	}
	rows := map[string]*Row{}
	internal := map[string]int{}
	for _, f := range files {
		fh, err := os.Open(f)
		if err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(fh)
		sc.Buffer(make([]byte, 1<<20), 1<<26)
		for sc.Scan() {
			var c captured
			if err := json.Unmarshal(sc.Bytes(), &c); err != nil {
				t.Fatalf("%s: %v", f, err)
			}
			if !c.Direct && c.Kind != "vl" {
				internal[c.Via]++
				continue
			}
			lang := "logql"
			if c.Kind == "logsql" || c.Kind == "vl" {
				lang = "logsql"
			}
			key := lang
			if c.Path != "" { // a client row keeps the id it had before sent rows were added
				key += "\x00" + c.Path
			}
			id := queryID(key, c.Query)
			r := rows[id]
			if r == nil {
				r = &Row{ID: id, Lang: lang, Path: c.Path, Query: c.Query}
				rows[id] = r
			}
			if c.Source != "" {
				r.Sources = append(r.Sources, c.Source)
			}
		}
		fh.Close()
	}

	out := make([]*Row, 0, len(rows))
	duplicated, sent, sentValid := 0, 0, 0
	for _, r := range rows {
		if r.Lang == "logsql" {
			_, err := logsql.Parse(r.Query)
			r.Valid = err == nil
		} else {
			_, err := logql.Parse(r.Query)
			r.Valid = err == nil
		}
		if r.Path != "" {
			sent++
			if r.Valid {
				sentValid++
			}
		}
		sort.Strings(r.Sources)
		r.Sources = compact(r.Sources)
		if len(r.Sources) > 1 {
			duplicated++
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	fh, err := os.Create(outPath)
	if err != nil {
		t.Fatal(err)
	}
	zw := gzip.NewWriter(fh)
	enc := json.NewEncoder(zw)
	enc.SetEscapeHTML(false)
	for _, r := range out {
		if err := enc.Encode(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fh.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("corpus rows=%d (sent to VictoriaLogs %d, our LogsQL parser reads %d) defined in more than one test=%d internal parse call sites=%d",
		len(out), sent, sentValid, duplicated, len(internal))
}

func compact(s []string) []string {
	out := s[:0]
	for i, v := range s {
		if i == 0 || v != s[i-1] {
			out = append(out, v)
		}
	}
	return out
}
