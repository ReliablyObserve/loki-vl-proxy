package logql

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// patternFilterGrid is the grid the pattern line filter's validation is held to Loki on: patterns with every kind
// of capture (named, `<_>`, in a row, malformed) under |> and !>, quoted and in backticks, in `or` chains on either
// side, and next to other line filters and stages.
func patternFilterGrid() []string {
	patterns := []string{"<_><_>", "<a>", "<_a>", "<A>", "<1>", "<>", "<_", "< _>", "<_>x<_><_>", "<a><b>", "", "<_> <foo>",
		"<_>", "x<_>y", "<a> x", "<_> <_>", "<__>", "<_1>", "<a_b>", "<_><a>", "<a><_>", "x<_><_>y", "<a><b><c>", "<_><_><_>",
		"x <a> <b>", "q\"<a>", "é<a>", "\t<a>", "<_> x <_> y"}
	var out []string
	for _, op := range []string{"|>", "!>"} {
		for _, p := range patterns {
			out = append(out,
				fmt.Sprintf(`{app="a"} %s %s`, op, strconv.Quote(p)),
				fmt.Sprintf(`{app="a"} %s "ok" or %s`, op, strconv.Quote(p)),
				fmt.Sprintf(`{app="a"} %s %s or "<_><_>"`, op, strconv.Quote(p)),
				fmt.Sprintf(`{app="a"} %s %s or "<b>" or "ok"`, op, strconv.Quote(p)),
				fmt.Sprintf(`{app="a"} | json %s %s`, op, strconv.Quote(p)),
				fmt.Sprintf(`{app="a"} |= "x" %s %s`, op, strconv.Quote(p)),
			)
			if !strings.Contains(p, "`") {
				out = append(out, fmt.Sprintf("{app=\"a\"} %s `%s`", op, p))
			}
		}
	}
	return append(out,
		`{app="a"} | pattern "<a> <b>"`, `{app="a"} | pattern "<_> <b>"`,
		`{app="a"} |> "<a>" |> "<_><_>"`, `{app="a"} !> "<a>" !> "<_><_>"`, `{app="a"} |~ "(" |> "<a>"`,
		`{app="a"} |> "<a>" |~ "("`, `{app="a"} |> "<a>" | json`, `{app="a"} |> "<a>" | line_format "{{.x}"`,
		`{app="a"} | line_format "{{.x}" |> "<a>"`, `{app="a"} |> "<a>" | x="1" |> "<_><_>"`,
		`{app="a"} |> "<a>" | json |~ "("`, `{app="a"} |= "a" or "b" |> "<_><_>" or "<a>"`,
	)
}

const patternFilterLokiAnswers = "testdata/pattern-line-filter-loki.jsonl"

// patternFilterAnswer is Loki's answer for one query of the grid.
type patternFilterAnswer struct {
	Accept bool   `json:"accept"`
	Error  string `json:"error,omitempty"`
}

// recordPipelineAnswers runs the Loki oracle (test/oracle/loki pipeline: Loki's exported pipeline build) over the
// grid and writes its answers to path, one JSON line per query.
func recordPipelineAnswers(t *testing.T, grid []string, path string) {
	t.Helper()
	work := t.TempDir()
	bin, err := oraclerun.Build(filepath.Join("..", "..", "test", "oracle", "loki"), "", work, "loki")
	if err != nil {
		t.Fatal(err)
	}
	in, out := filepath.Join(work, "in.jsonl"), filepath.Join(work, "out.jsonl")
	f, err := os.Create(in)
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(f)
	for i, q := range grid {
		if err := enc.Encode(map[string]string{"id": fmt.Sprint(i), "q": q}); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := oraclerun.Run(bin, "pipeline", "-in", in, "-out", out); err != nil {
		t.Fatal(err)
	}
	r, err := os.Open(out)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	var b strings.Builder
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		var v patternFilterAnswer
		if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		line, _ := json.Marshal(v)
		b.Write(line)
		b.WriteByte('\n')
	}
	if err := os.MkdirAll("testdata", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// lineFilterRunTextDifferences is the number of grid queries the proxy rejects like Loki but with another text:
// Loki quotes every line filter of the run it builds together (adjacent ones, and those after a parser, up to a
// label filter) and reports the run from its last filter, where the proxy quotes the stage alone; see
// semantics/line-filter-error-quotes-the-run. The count may only fall.
const lineFilterRunTextDifferences = 46

// A pattern line filter is rejected exactly where Loki rejects it, with Loki's text: two captures in a row
// ("found consecutive capture") and any capture but `<_>` ("named captures are not allowed"), for |> and !>, alone,
// in `or` chains and next to other stages. Loki's answers are recorded with the Loki oracle (LVP_RUN_ORACLES=1
// records them again). The `| pattern` parser keeps its named captures.
//
// conformance: semantics/pattern-line-filter-invalid-captures, semantics/line-filter-error-quotes-the-run
func TestPatternLineFilterValidationMatchesLoki(t *testing.T) {
	grid := patternFilterGrid()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordPipelineAnswers(t, grid, patternFilterLokiAnswers)
	}
	b, err := os.ReadFile(patternFilterLokiAnswers)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != len(grid) {
		t.Fatalf("%d recorded answers for %d queries: record them again with LVP_RUN_ORACLES=1", len(lines), len(grid))
	}
	runText := 0
	for i, q := range grid {
		var loki patternFilterAnswer
		if err := json.Unmarshal([]byte(lines[i]), &loki); err != nil {
			t.Fatal(err)
		}
		err := ParseAndValidate(q)
		if (err == nil) != loki.Accept {
			t.Errorf("%s: proxy error %v, Loki accepts=%v %s", q, err, loki.Accept, loki.Error)
			continue
		}
		if err == nil || err.Error() == loki.Error {
			continue
		}
		if lineFilterStages(t, q) > 1 {
			runText++
			t.Logf("run text: %s:\n proxy %s\n Loki  %s", q, err, loki.Error)
			continue
		}
		t.Errorf("%s:\n proxy %s\n Loki  %s", q, err, loki.Error)
	}
	switch {
	case runText > lineFilterRunTextDifferences:
		t.Errorf("queries rejected with another text than Loki's run grew from %d to %d", lineFilterRunTextDifferences, runText)
	case runText < lineFilterRunTextDifferences:
		t.Errorf("queries rejected with another text than Loki's run fell from %d to %d: lower lineFilterRunTextDifferences", lineFilterRunTextDifferences, runText)
	}
}

// lineFilterStages is the number of line filter stages of a log query.
func lineFilterStages(t *testing.T, q string) int {
	t.Helper()
	lq, err := ParseLogQuery(q)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, st := range lq.Pipeline {
		if _, ok := st.(*LineFilterStage); ok {
			n++
		}
	}
	return n
}
