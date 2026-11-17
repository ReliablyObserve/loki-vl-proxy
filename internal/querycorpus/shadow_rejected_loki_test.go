package querycorpus_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// shadowRejectedAnswers are Loki's answers (the Loki oracle, test/oracle/loki check) on the plan-corpus rows the
// proxy's request validation refuses (rejectedByValidation): the rows the shadow leaves out as rejected.
const shadowRejectedAnswers = "testdata/shadow-rejected-loki.jsonl"

// shadowRejectedAnswer is Loki's verdict on one rejected row.
type shadowRejectedAnswer struct {
	Query  string `json:"q"`
	Accept bool   `json:"accept"`
	Error  string `json:"error,omitempty"`
}

// rejectedCorpusRows are the plan-corpus rows the plan builds and the proxy's validation refuses, each once.
func rejectedCorpusRows(t *testing.T) []string {
	t.Helper()
	var out []string
	seen := map[string]bool{}
	for _, q := range planCorpus(t) {
		if seen[q] {
			continue
		}
		seen[q] = true
		expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
		if err != nil {
			continue
		}
		if _, err := queryplan.Build(expr, queryplan.Inputs{}); err != nil {
			continue
		}
		if rejectedByValidation(q) {
			out = append(out, q)
		}
	}
	return out
}

func recordShadowRejected(t *testing.T, rows []string) {
	t.Helper()
	work := t.TempDir()
	bin, err := oraclerun.Build(lokiFactsDir, "", work, "loki")
	if err != nil {
		t.Fatal(err)
	}
	in := filepath.Join(work, "in.jsonl")
	f, err := os.Create(in)
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	for i, q := range rows {
		if err := enc.Encode(oracleInput{ID: strconv.Itoa(i), Kind: "shadow-rejected", Query: q}); err != nil {
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
	o, err := os.Create(shadowRejectedAnswers)
	if err != nil {
		t.Fatal(err)
	}
	oenc := json.NewEncoder(o)
	oenc.SetEscapeHTML(false)
	for i, q := range rows {
		v, ok := verdicts[strconv.Itoa(i)]
		if !ok {
			t.Fatalf("no verdict for %q", q)
		}
		if err := oenc.Encode(shadowRejectedAnswer{Query: q, Accept: v.Accept, Error: v.Error}); err != nil {
			t.Fatal(err)
		}
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
}

// rejectionTextDiffers are the rejected rows whose error text is a recorded difference from Loki's, with its case.
var rejectionTextDiffers = map[string]string{
	// Loki's lexer reads no sign before the parameter; the proxy reads -0.1 and checks its range
	// (conformance/registry/grammar-sweep.json, `syntax error at - || invalid parameter for quantile_over_time`).
	`quantile_over_time(-0.1, {app="api"} | unwrap lat [5m])`: "semantics/parse-error-text-parity",
}

// TestShadowRejectedRowsAreLokiRejections holds the rows the plan shadow leaves out as rejected (the proxy's request
// validation refuses them before anything is translated, so neither the plan nor the string translator sees them)
// to Loki's answers: Loki rejects every one of them, with the text the proxy answers. The malformed label_replace
// calls and label_join the logical plan keeps as Opaque nodes are among them: they are parse errors to Loki, so no
// lowering is owed. LVP_RUN_ORACLES=1 records the answers again.
//
// conformance: architecture-syntax-package-layering, semantics/parse-error-text-parity
func TestShadowRejectedRowsAreLokiRejections(t *testing.T) {
	rows := rejectedCorpusRows(t)
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordShadowRejected(t, rows)
	}
	answers := map[string]shadowRejectedAnswer{}
	for _, a := range readJSONLines[shadowRejectedAnswer](t, shadowRejectedAnswers) {
		answers[a.Query] = a
	}
	if len(rows) == 0 {
		t.Fatal("no corpus row is rejected by the validation")
	}
	for _, q := range rows {
		a, ok := answers[q]
		if !ok {
			t.Errorf("%q: the validation rejects it and no Loki answer is recorded: run LVP_RUN_ORACLES=1 go test ./internal/querycorpus -run TestShadowRejectedRowsAreLokiRejections", q)
			continue
		}
		if a.Accept {
			t.Errorf("%q: the proxy rejects it before translation, Loki accepts it", q)
			continue
		}
		if got := validationText(q); got != a.Error && rejectionTextDiffers[q] == "" {
			t.Errorf("%q: the proxy answers %q, Loki %q", q, got, a.Error)
		}
	}
	t.Logf("%d corpus rows rejected by the validation, each a Loki rejection", len(rows))
}

// validationText is the proxy's error text for a query its validation refuses (validateQueryTree): the parser's,
// or a tree check's as a parse error with no position.
func validationText(q string) string {
	expr, msg := logql.ValidateLogQLExpr(q)
	if msg != "" || expr == nil {
		return msg
	}
	if r := queryplan.Validate(expr); r != nil {
		return "parse error : " + r.Detail
	}
	return ""
}
