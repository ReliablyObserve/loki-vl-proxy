package proxy

import (
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// lokiOracleInput is one query for the Loki parse oracle (test/oracle/loki Input): its LogQL and what the proxy's
// validation, the one every query route runs first, made of it.
type lokiOracleInput struct {
	ID      string `json:"id"`
	Kind    string `json:"kind"` // client or case
	Query   string `json:"q"`
	Ours    bool   `json:"ours"`
	Error   string `json:"error,omitempty"`
	Printed string `json:"printed,omitempty"`
}

// lokiOracleDir is the Loki parse oracle: a test-only module of its own that imports Loki's parser (AGPL-3.0)
// and is never part of the proxy build.
const lokiOracleDir = "../../test/oracle/loki"

// TestLokiOracle runs the Loki parse oracle over the corpus at LVP_QUERY_CORPUS_OUT when LVP_RUN_ORACLES=1. It
// builds the oracle (it pins the compatibility matrix's Loki version), collects the queries of Loki's syntax
// tests, writes the inputs (every LogQL client query of the corpus and those queries, each with the proxy's
// answer through validateQuery: accepted or the error text a client gets, and the printed form of the parsed
// query) and writes Loki's verdicts to <corpus>.loki-<version>.jsonl.gz and the share of the proxy's Go files
// that matches Loki's sources to <corpus>.loki-similar.json.
func TestLokiOracle(t *testing.T) {
	corpus := os.Getenv("LVP_QUERY_CORPUS_OUT")
	if corpus == "" || os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("LVP_QUERY_CORPUS_OUT and LVP_RUN_ORACLES=1 are not set")
	}
	corpus, err := filepath.Abs(corpus)
	if err != nil {
		t.Fatal(err)
	}
	version := lokiMatrixVersion(t)
	if mod := readText(t, filepath.Join(lokiOracleDir, "go.mod")); !strings.Contains(mod, "github.com/grafana/loki/v3 v"+version+"\n") {
		t.Fatalf("test/oracle/loki/go.mod does not require Loki v%s, the compatibility matrix's pinned version", version)
	}
	work := t.TempDir()
	bin, err := oraclerun.Build(lokiOracleDir, "", work, "loki")
	if err != nil {
		t.Fatal(err)
	}
	src, err := oraclerun.SourceDir(lokiOracleDir, "", "github.com/grafana/loki/v3")
	if err != nil {
		t.Fatal(err)
	}
	casesPath := filepath.Join(work, "loki-cases.json")
	if err := oraclerun.Run(bin, "cases", "-src", src, "-out", casesPath); err != nil {
		t.Fatal(err)
	}
	inputs := filepath.Join(work, "loki-inputs.jsonl")
	writeLokiInputs(t, inputs, casesPath, corpus)
	verdicts := filepath.Join(work, "loki-verdicts.jsonl")
	if err := oraclerun.Run(bin, "check", "-in", inputs, "-out", verdicts); err != nil {
		t.Fatal(err)
	}
	base := strings.TrimSuffix(corpus, ".jsonl.gz")
	if err := oraclerun.Gzip(verdicts, base+".loki-"+version+".jsonl.gz"); err != nil {
		t.Fatal(err)
	}
	if err := oraclerun.Run(bin, "similar", "-src", src, "-ours", strings.Join(productDirs(t), ","), "-out", base+".loki-similar.json"); err != nil {
		t.Fatal(err)
	}
}

// lokiMatrixVersion is the compatibility matrix's pinned Loki version, such as 3.7.9.
func lokiMatrixVersion(t *testing.T) string {
	t.Helper()
	var m struct {
		Stack struct {
			Loki struct {
				Pinned string `json:"pinned_version"`
			} `json:"loki"`
		} `json:"stack"`
	}
	if err := json.Unmarshal([]byte(readText(t, "../../test/e2e-compat/compatibility-matrix.json")), &m); err != nil {
		t.Fatal(err)
	}
	return strings.TrimPrefix(m.Stack.Loki.Pinned, "v")
}

// productDirs are the directories of the repository's own Go code: cmd/, internal/ and test/, except the oracles,
// which import Loki and VictoriaLogs on purpose, and vendored JavaScript dependencies.
func productDirs(t *testing.T) []string {
	t.Helper()
	var dirs []string
	for _, top := range []string{"cmd", "internal", "test"} {
		root, err := filepath.Abs(filepath.Join("../..", top))
		if err != nil {
			t.Fatal(err)
		}
		err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil || !d.IsDir() {
				return err
			}
			if d.Name() == "node_modules" || strings.HasSuffix(filepath.ToSlash(path), "/test/oracle") {
				return filepath.SkipDir
			}
			dirs = append(dirs, path)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return dirs
}

func readText(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// writeLokiInputs writes the oracle's inputs: the corpus's LogQL client queries and Loki's syntax test queries.
func writeLokiInputs(t *testing.T, path, casesPath, corpus string) {
	t.Helper()
	p := newTestProxy(t, "http://127.0.0.1:1") // validation never calls the backend
	out, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := bufio.NewWriter(out)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	emit := func(id, kind, q string) {
		in := lokiOracleInput{ID: id, Kind: kind, Query: q}
		rec := httptest.NewRecorder()
		_, in.Ours = p.validateQuery(rec, q, "query_range")
		if !in.Ours {
			if rec.Code != http.StatusBadRequest {
				t.Errorf("validateQuery(%q) answered %d, want 400", q, rec.Code)
			}
			in.Error = rec.Body.String()
		} else if expr, err := logqlpkg.Parse(q); err == nil {
			in.Printed = expr.String()
		}
		if err := enc.Encode(in); err != nil {
			t.Fatal(err)
		}
	}
	for _, r := range corpusLogQL(t, corpus) {
		emit(r.ID, "client", r.Query)
	}
	var cases []string
	if err := json.Unmarshal([]byte(readText(t, casesPath)), &cases); err != nil {
		t.Fatal(err)
	}
	for _, q := range cases {
		sum := sha256.Sum256([]byte(q))
		emit("c-"+hex.EncodeToString(sum[:6]), "case", q)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
}

type corpusQuery struct {
	ID    string `json:"id"`
	Lang  string `json:"lang"`
	Path  string `json:"path"`
	Query string `json:"q"`
}

// corpusLogQL reads the LogQL client queries of a corpus file (internal/querycorpus Row), valid or not.
func corpusLogQL(t *testing.T, path string) []corpusQuery {
	t.Helper()
	fh, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fh.Close() }()
	zr, err := gzip.NewReader(fh)
	if err != nil {
		t.Fatal(err)
	}
	var out []corpusQuery
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		var r corpusQuery
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		if r.Lang == "logql" && r.Path == "" && strings.TrimSpace(r.Query) != "" {
			out = append(out, r)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
