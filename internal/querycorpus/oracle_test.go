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

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// oracleDir is the VictoriaLogs parse oracle (test/oracle/vl): a module of its own whose go.mod pins the matrix's
// pinned VictoriaLogs version, with a go.v<version>.mod for each other matrix version.
const oracleDir = "../../test/oracle/vl"

// oracleInput is one query for the oracle: its LogsQL and what our LogsQL parser makes of it.
type oracleInput struct {
	ID      string `json:"id"`
	Kind    string `json:"kind"` // golden, sent, case-accept or case-reject
	Query   string `json:"q"`
	Ours    bool   `json:"ours"`              // our parser reads it
	Printed string `json:"printed,omitempty"` // our parser's printed form
}

// oracleVerdict is the oracle's answer for one input (test/oracle/vl Verdict), with our parser's verdict.
type oracleVerdict struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	Accept      bool   `json:"accept"`
	Ours        bool   `json:"ours"`
	SameMeaning *bool  `json:"same_meaning,omitempty"`
}

type upstreamCase struct {
	Query  string `json:"q"`
	Accept bool   `json:"accept"`
}

func caseID(q string) string {
	sum := sha256.Sum256([]byte(q))
	return "c-" + hex.EncodeToString(sum[:6])
}

func ours(q string) (bool, string) {
	pq, err := logsql.Parse(q)
	if err != nil {
		return false, ""
	}
	return true, pq.String()
}

// verdictsPath is where the oracle's verdicts on version v for the corpus at corpus go.
func verdictsPath(corpus, v string) string {
	return strings.TrimSuffix(corpus, ".jsonl.gz") + ".vl-" + v + ".jsonl.gz"
}

// TestOracleInputs writes, for every VictoriaLogs version of the compatibility matrix, the oracle's inputs into
// LVP_ORACLE_DIR: the corpus's goldens of that version, its sent LogsQL (for the newest version only), and
// VictoriaLogs' own parser test cases
// (cases-<version>.json there, from the oracle's cases command), each with our LogsQL parser's verdict and printed
// form. It fails when the oracle does not pin a matrix version.
func TestOracleInputs(t *testing.T) {
	corpus, dir := os.Getenv("LVP_QUERY_CORPUS_OUT"), os.Getenv("LVP_ORACLE_DIR")
	if corpus == "" || dir == "" {
		t.Skip("LVP_QUERY_CORPUS_OUT and LVP_ORACLE_DIR are not set")
	}
	versions := goldenVersions(t)
	rows := allRows(t, corpus)
	for _, v := range versions {
		file := "go." + v + ".mod"
		if v == versions[0] {
			file = "go.mod"
		}
		b, err := os.ReadFile(filepath.Join(oracleDir, file))
		pin := "replace github.com/VictoriaMetrics/VictoriaLogs => github.com/VictoriaMetrics/VictoriaLogs " + v + "\n"
		if err != nil || !strings.Contains(string(b), pin) {
			t.Fatalf("test/oracle/vl/%s does not pin VictoriaLogs %s, a matrix version: run scripts/ci/vl_oracle.sh --pin %s", file, v, v)
		}
		var cases []upstreamCase
		readJSON(t, filepath.Join(dir, "cases-"+v+".json"), &cases)
		f, err := os.Create(filepath.Join(dir, "inputs-"+v+".jsonl"))
		if err != nil {
			t.Fatal(err)
		}
		w := bufio.NewWriter(f)
		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		emit := func(id, kind, q string) {
			in := oracleInput{ID: id, Kind: kind, Query: q}
			in.Ours, in.Printed = ours(q)
			if err := enc.Encode(in); err != nil {
				t.Fatal(err)
			}
		}
		for _, r := range rows {
			switch {
			case r.Golden != nil:
				emit(r.ID, "golden", at(r.Golden, v, versions[0]))
			case r.Path != "" && v == versions[0]:
				// Tests send with the newest version's capabilities: checking that LogsQL against an older
				// VictoriaLogs only finds features it is too old for.
				emit(r.ID, "sent", r.Query)
			}
		}
		seen := map[string]bool{}
		for _, c := range cases {
			id := caseID(c.Query)
			if seen[id] {
				continue
			}
			seen[id] = true
			kind := "case-reject"
			if c.Accept {
				kind = "case-accept"
			}
			emit(id, kind, c.Query)
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// allRows reads every row of a corpus file in file order.
func allRows(t *testing.T, path string) []Row {
	t.Helper()
	fh, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()
	zr, err := gzip.NewReader(fh)
	if err != nil {
		t.Fatal(err)
	}
	var out []Row
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		var r Row
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// readVerdicts reads the oracle's verdicts on one version, by kind and id. A missing file is no verdicts.
func readVerdicts(t *testing.T, path string) map[string]map[string]oracleVerdict {
	t.Helper()
	fh, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()
	zr, err := gzip.NewReader(fh)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]map[string]oracleVerdict{}
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		var v oracleVerdict
		if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		if out[v.Kind] == nil {
			out[v.Kind] = map[string]oracleVerdict{}
		}
		out[v.Kind][v.ID] = v
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// OracleSummary is the oracle's counts on one VictoriaLogs version.
type OracleSummary struct {
	Version string `json:"version"`
	// GoldensRejected are goldens VictoriaLogs' parser rejects: LogsQL the translator produces that
	// VictoriaLogs cannot run.
	Goldens         int `json:"goldens"`
	GoldensRejected int `json:"goldens_rejected"`
	// GoldensSameMeaning are goldens our parser reads that VictoriaLogs reads the same as our printed form.
	GoldensRead        int `json:"goldens_read"`
	GoldensSameMeaning int `json:"goldens_same_meaning"`
	Sent               int `json:"sent"`
	SentRejected       int `json:"sent_rejected"`
	// CasesRead are VictoriaLogs' accepted test cases our parser reads with the same meaning.
	CasesAccepted int `json:"cases_accepted"`
	CasesRead     int `json:"cases_read"`
	// CasesWronglyAccepted are VictoriaLogs' rejected test cases our parser accepts.
	CasesRejected        int `json:"cases_rejected"`
	CasesWronglyAccepted int `json:"cases_wrongly_accepted"`
}

func summarizeOracle(v string, verdicts map[string]map[string]oracleVerdict) OracleSummary {
	s := OracleSummary{Version: v}
	same := func(x oracleVerdict) bool { return x.SameMeaning != nil && *x.SameMeaning }
	for _, x := range verdicts["golden"] {
		s.Goldens++
		if !x.Accept {
			s.GoldensRejected++
		}
		if x.Ours {
			s.GoldensRead++
			if same(x) {
				s.GoldensSameMeaning++
			}
		}
	}
	for _, x := range verdicts["sent"] {
		s.Sent++
		if !x.Accept {
			s.SentRejected++
		}
	}
	for _, x := range verdicts["case-accept"] {
		s.CasesAccepted++
		if same(x) {
			s.CasesRead++
		}
	}
	for _, x := range verdicts["case-reject"] {
		s.CasesRejected++
		if x.Ours {
			s.CasesWronglyAccepted++
		}
	}
	return s
}

// OracleRegressions are, on one version, the queries on both the base and the PR that the oracle now judges worse.
type OracleRegressions struct {
	Version string `json:"version"`
	// GoldensRejected: VictoriaLogs accepted the base's golden and rejects the PR's.
	GoldensRejected []string `json:"goldens_rejected"`
	// GoldensMeaningLost: our printed form meant the same to VictoriaLogs on the base and does not now.
	GoldensMeaningLost []string `json:"goldens_meaning_lost"`
	// CasesLost: an accepted VictoriaLogs test case our parser read with the same meaning on the base and does not now.
	CasesLost []string `json:"cases_lost"`
}

func (r OracleRegressions) any() bool {
	return len(r.GoldensRejected)+len(r.GoldensMeaningLost)+len(r.CasesLost) > 0
}

func oracleRegressions(v string, base, head map[string]map[string]oracleVerdict) OracleRegressions {
	reg := OracleRegressions{Version: v, GoldensRejected: []string{}, GoldensMeaningLost: []string{}, CasesLost: []string{}}
	same := func(x oracleVerdict) bool { return x.SameMeaning != nil && *x.SameMeaning }
	for id, h := range head["golden"] {
		b, ok := base["golden"][id]
		if !ok {
			continue
		}
		if b.Accept && !h.Accept {
			reg.GoldensRejected = append(reg.GoldensRejected, id)
		}
		if same(b) && !same(h) {
			reg.GoldensMeaningLost = append(reg.GoldensMeaningLost, id)
		}
	}
	for id, h := range head["case-accept"] {
		if b, ok := base["case-accept"][id]; ok && same(b) && !same(h) {
			reg.CasesLost = append(reg.CasesLost, id)
		}
	}
	sort.Strings(reg.GoldensRejected)
	sort.Strings(reg.GoldensMeaningLost)
	sort.Strings(reg.CasesLost)
	return reg
}

func boolp(b bool) *bool { return &b }

func TestOracleSummaryAndRegressions(t *testing.T) {
	base := map[string]map[string]oracleVerdict{
		"golden": {
			"g1": {Accept: true, Ours: true, SameMeaning: boolp(true)},
			"g2": {Accept: true},
			"g3": {Accept: true, Ours: true, SameMeaning: boolp(true)},
		},
		"case-accept": {"c1": {Accept: true, Ours: true, SameMeaning: boolp(true)}},
	}
	head := map[string]map[string]oracleVerdict{
		"golden": {
			"g1": {Accept: true, Ours: true, SameMeaning: boolp(true)},  // held
			"g2": {Accept: false},                                       // VictoriaLogs now rejects it
			"g3": {Accept: true, Ours: true, SameMeaning: boolp(false)}, // our printed form lost its meaning
			"g4": {Accept: false},                                       // new: never a regression
		},
		"case-accept": {"c1": {Accept: true}},
		"case-reject": {"c9": {Accept: false, Ours: true}},
		"sent":        {"s1": {Accept: false}},
	}
	got := oracleRegressions("v1.53.0", base, head)
	if strings.Join(got.GoldensRejected, ",") != "g2" || strings.Join(got.GoldensMeaningLost, ",") != "g3" ||
		strings.Join(got.CasesLost, ",") != "c1" || !got.any() {
		t.Errorf("regressions = %+v", got)
	}
	s := summarizeOracle("v1.53.0", head)
	want := OracleSummary{Version: "v1.53.0", Goldens: 4, GoldensRejected: 2, GoldensRead: 2, GoldensSameMeaning: 1,
		Sent: 1, SentRejected: 1, CasesAccepted: 1, CasesRejected: 1, CasesWronglyAccepted: 1}
	if s != want {
		t.Errorf("summary = %+v, want %+v", s, want)
	}
}

// The oracle's VictoriaLogs dependency stays in its own module: the proxy's go.mod and go.sum never name it, and
// no package of the proxy imports it.
func TestProductModuleHasNoOracleDependency(t *testing.T) {
	for _, name := range []string{"go.mod", "go.sum"} {
		b, err := os.ReadFile(filepath.Join("../..", name))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), "github.com/VictoriaMetrics/VictoriaLogs") {
			t.Errorf("%s names github.com/VictoriaMetrics/VictoriaLogs; only test/oracle/vl may depend on it", name)
		}
	}
	if _, err := os.Stat(filepath.Join(oracleDir, "go.mod")); err != nil {
		t.Errorf("test/oracle/vl must stay a module of its own: %v", err)
	}
}
