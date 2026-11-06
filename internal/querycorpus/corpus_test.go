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
	"strings"
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
	// Golden is the LogsQL of a valid LogQL client row, per VictoriaLogs version:
	// the newest always, an older one only where it differs (annotate).
	Golden map[string]string `json:"golden,omitempty"`
	// GoldenReads is whether our LogsQL parser reads the newest golden.
	GoldenReads bool `json:"golden_reads,omitempty"`
	// Untyped lists the kinds of node of the query that hold text, not typed fields.
	Untyped []string `json:"untyped,omitempty"`
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
// The corpus is a CI artifact, not a committed file. Its counts go to
// <corpus>.summary.json beside it, which TestQueryCorpusReport reads.
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

	versions := goldenVersions(t)
	out := make([]*Row, 0, len(rows))
	duplicated := 0
	for _, r := range rows {
		if r.Lang == "logsql" {
			_, err := logsql.Parse(r.Query)
			r.Valid = err == nil
		} else {
			_, err := logql.Parse(r.Query)
			r.Valid = err == nil
		}
		annotate(r, versions)
		sort.Strings(r.Sources)
		r.Sources = compact(r.Sources)
		// A query many tests send to VictoriaLogs is shared traffic, not a case
		// defined twice: only client rows count as duplicates.
		if r.Path == "" && len(r.Sources) > 1 {
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
	summary := summarize(out, duplicated, len(internal), versions)
	b, err := json.MarshalIndent(summary, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(summaryPath(outPath), append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("corpus summary: %s", b)
}

// summaryPath is where the summary of the corpus at outPath goes.
func summaryPath(outPath string) string {
	return strings.TrimSuffix(outPath, ".jsonl.gz") + ".summary.json"
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
