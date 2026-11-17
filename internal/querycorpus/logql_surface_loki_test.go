package querycorpus_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// logqlSurface is the generated LogQL surface (conformance/scripts/sync_ast.py): the constructs Loki's public
// documentation lists, each with a probe query.
type logqlSurface struct {
	Version    string `json:"version"`
	Constructs []struct {
		Name    string `json:"name"`
		Literal string `json:"literal"`
		Probe   string `json:"probe"`
		Source  string `json:"source"`
	} `json:"constructs"`
}

// The LogQL surface the registry records is the one Loki's parser accepts: every construct's probe query parses
// with Loki's own parser (test/oracle/loki check, LVP_RUN_ORACLES=1). The surface comes from Loki's public
// documentation, so this is the check that it has not drifted from Loki: a construct Loki stops accepting, or a
// documentation page that names one Loki does not parse, fails here.
//
// conformance: architecture-syntax-package-layering
func TestLogQLSurfaceMatchesLokiParser(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("LVP_RUN_ORACLES=1 is not set")
	}
	paths, err := filepath.Glob("../../conformance/registry/generated/loki/*/logql.json")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no generated LogQL surface: %v", err)
	}
	work := t.TempDir()
	bin, err := oraclerun.Build(lokiFactsDir, "", work, "loki")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		var surface logqlSurface
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, &surface); err != nil {
			t.Fatal(err)
		}
		in := filepath.Join(work, "in.jsonl")
		f, err := os.Create(in)
		if err != nil {
			t.Fatal(err)
		}
		enc := json.NewEncoder(f)
		enc.SetEscapeHTML(false)
		for i, c := range surface.Constructs {
			if c.Probe == "" {
				t.Fatalf("%s: %s has no probe query", path, c.Name)
			}
			if err := enc.Encode(oracleInput{ID: strconv.Itoa(i), Kind: "surface", Query: c.Probe, Ours: true}); err != nil {
				t.Fatal(err)
			}
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		out := filepath.Join(work, "out.jsonl")
		if err := oraclerun.Run(bin, "check", "-in", in, "-out", out); err != nil {
			t.Fatal(err)
		}
		verdicts := readVerdictsWithError(t, out)
		for i, c := range surface.Constructs {
			if v := verdicts[strconv.Itoa(i)]; !v.Accept {
				t.Errorf("%s: %s (%s): Loki's parser rejects its probe %q: %s", path, c.Name, c.Source, c.Probe, v.Error)
			}
		}
	}
}
