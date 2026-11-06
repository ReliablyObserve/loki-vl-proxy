package querycorpus_test

import (
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
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

// oracleModfile is the module file that pins VictoriaLogs version v in the oracle: go.mod for the matrix's pinned
// version, go.v<version>.mod for any other.
func oracleModfile(v, pinned string) string {
	if v == pinned {
		return "go.mod"
	}
	return "go." + v + ".mod"
}

// TestVLOracle runs the VictoriaLogs parse oracle over the corpus at LVP_QUERY_CORPUS_OUT when LVP_RUN_ORACLES=1.
// For every VictoriaLogs version of the compatibility matrix it builds the oracle with that version's module file,
// lists its pipes, stats functions and filters (<corpus>.vl-<version>.constructs.json) and its own parser test
// cases, writes the inputs (the goldens of that version, the LogsQL the tests sent for the newest version only,
// and the test cases, each with our LogsQL parser's verdict and printed form) and writes VictoriaLogs' verdicts
// (<corpus>.vl-<version>.jsonl.gz). It fails when the oracle does not pin a matrix version.
func TestVLOracle(t *testing.T) {
	corpus := os.Getenv("LVP_QUERY_CORPUS_OUT")
	if corpus == "" || os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("LVP_QUERY_CORPUS_OUT and LVP_RUN_ORACLES=1 are not set")
	}
	corpus, err := filepath.Abs(corpus)
	if err != nil {
		t.Fatal(err)
	}
	work := t.TempDir()
	versions := goldenVersions(t)
	rows := allRows(t, corpus)
	for _, v := range versions {
		file := oracleModfile(v, versions[0])
		b, err := os.ReadFile(filepath.Join(oracleDir, file))
		pin := "replace github.com/VictoriaMetrics/VictoriaLogs => github.com/VictoriaMetrics/VictoriaLogs " + v + "\n"
		if err != nil || !strings.Contains(string(b), pin) {
			t.Fatalf("test/oracle/vl/%s does not pin VictoriaLogs %s, a matrix version: run LVP_ORACLE_PIN=%s go test ./internal/querycorpus -run TestPinVLOracle", file, v, v)
		}
		modfile := ""
		if file != "go.mod" {
			modfile = file
		}
		bin, err := oraclerun.Build(oracleDir, modfile, work, "vl-"+v)
		if err != nil {
			t.Fatal(err)
		}
		src, err := oraclerun.SourceDir(oracleDir, modfile, "github.com/VictoriaMetrics/VictoriaLogs")
		if err != nil {
			t.Fatal(err)
		}
		casesPath := filepath.Join(work, "cases-"+v+".json")
		if err := oraclerun.Run(bin, "cases", "-src", src, "-out", casesPath); err != nil {
			t.Fatal(err)
		}
		if err := oraclerun.Run(bin, "constructs", "-src", src, "-out", constructsPath(corpus, v)); err != nil {
			t.Fatal(err)
		}
		inputs := filepath.Join(work, "inputs-"+v+".jsonl")
		writeVLInputs(t, inputs, casesPath, v, versions[0], rows)
		verdicts := filepath.Join(work, "verdicts-"+v+".jsonl")
		if err := oraclerun.Run(bin, "check", "-in", inputs, "-out", verdicts); err != nil {
			t.Fatal(err)
		}
		if err := oraclerun.Gzip(verdicts, verdictsPath(corpus, v)); err != nil {
			t.Fatal(err)
		}
	}
}

// writeVLInputs writes the oracle's inputs for VictoriaLogs version v.
func writeVLInputs(t *testing.T, path, casesPath, v, newest string, rows []Row) {
	t.Helper()
	var cases []upstreamCase
	readJSON(t, casesPath, &cases)
	f, err := os.Create(path)
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
			emit(r.ID, "golden", at(r.Golden, v, newest))
		case r.Path != "" && v == newest:
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

// TestPinVLOracle pins VictoriaLogs version LVP_ORACLE_PIN in the oracle: in go.mod when it is the matrix's pinned
// version, in go.v<version>.mod otherwise. It needs the network.
func TestPinVLOracle(t *testing.T) {
	v := os.Getenv("LVP_ORACLE_PIN")
	if v == "" {
		t.Skip("LVP_ORACLE_PIN is not set")
	}
	const module = "github.com/VictoriaMetrics/VictoriaLogs"
	file := oracleModfile(v, goldenVersions(t)[0])
	if file == "go.mod" {
		if err := oraclerun.Go(oracleDir, "mod", "edit", "-dropreplace="+module, "-droprequire="+module); err != nil {
			t.Fatal(err)
		}
	} else {
		goVersion := strings.TrimPrefix(regexp.MustCompile(`(?m)^go \S+$`).FindString(readFile(t, filepath.Join(oracleDir, "go.mod"))), "go ")
		header := "module github.com/ReliablyObserve/Loki-VL-proxy/test/oracle/vl\n\ngo " + goVersion + "\n"
		if err := os.WriteFile(filepath.Join(oracleDir, file), []byte(header), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := oraclerun.Go(oracleDir, "mod", "edit", "-modfile="+file, "-require="+module+"@"+v, "-replace="+module+"="+module+"@"+v); err != nil {
		t.Fatal(err)
	}
	if err := oraclerun.Go(oracleDir, "mod", "tidy", "-modfile="+file); err != nil {
		t.Fatal(err)
	}
	t.Logf("pinned %s %s in test/oracle/vl/%s", module, v, file)
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
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

// The oracles' dependencies stay in their own modules. Loki is AGPL-3.0 and its parser is used only by the test
// oracle under test/oracle/loki: the proxy's go.mod and go.sum never name it (nor VictoriaLogs, the other
// oracle's), so no package of the proxy can import it and no release binary or its SBOM can contain it.
func TestProductModuleHasNoOracleDependency(t *testing.T) {
	for _, name := range []string{"go.mod", "go.sum"} {
		b, err := os.ReadFile(filepath.Join("../..", name))
		if err != nil {
			t.Fatal(err)
		}
		for _, module := range []string{"github.com/grafana/loki", "github.com/VictoriaMetrics/VictoriaLogs"} {
			if strings.Contains(string(b), module) {
				t.Errorf("%s names %s; only the test oracles under test/oracle may depend on it", name, module)
			}
		}
	}
	for _, dir := range []string{oracleDir, "../../test/oracle/loki"} {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err != nil {
			t.Errorf("%s must stay a module of its own: %v", dir, err)
		}
	}
}

// coveragePath is the LogsQL constructs internal/logsql covers, and the lib/logstorage source files that are no
// user-facing construct.
const coveragePath = "../../scripts/vl-ast-coverage.json"

// ConstructCoverage is, on one VictoriaLogs version, its pipes, stats functions and filters that
// scripts/vl-ast-coverage.json neither covers nor ignores.
type ConstructCoverage struct {
	Version string   `json:"version"`
	Missing []string `json:"missing"`
}

// constructsPath is where the oracle's list of version v's constructs for the corpus at corpus goes.
func constructsPath(corpus, v string) string {
	return strings.TrimSuffix(corpus, ".jsonl.gz") + ".vl-" + v + ".constructs.json"
}

// missingConstructs lists, as "<category>/<name>", the constructs of upstream that coverage neither covers nor ignores.
func missingConstructs(upstream map[string][]string, coverage map[string]json.RawMessage) ([]string, error) {
	missing := []string{}
	var ignored map[string]map[string]string
	if raw, ok := coverage["ignored"]; ok {
		if err := json.Unmarshal(raw, &ignored); err != nil {
			return nil, err
		}
	}
	for _, category := range []string{"pipes", "stats", "filters"} {
		var covered []string
		if raw, ok := coverage[category]; ok {
			if err := json.Unmarshal(raw, &covered); err != nil {
				return nil, err
			}
		}
		known := map[string]bool{}
		for _, c := range covered {
			known[c] = true
		}
		for _, name := range upstream[category] {
			if !known[name] && ignored[category][name] == "" {
				missing = append(missing, category+"/"+name)
			}
		}
	}
	sort.Strings(missing)
	return missing, nil
}

func TestMissingConstructsAreThoseNeitherCoveredNorIgnored(t *testing.T) {
	coverage := map[string]json.RawMessage{
		"pipes":   json.RawMessage(`["stats", "filter"]`),
		"stats":   json.RawMessage(`["count"]`),
		"ignored": json.RawMessage(`{"pipes": {"local": "internal base"}}`),
	}
	got, err := missingConstructs(map[string][]string{
		"pipes": {"filter", "local", "stats", "unroll"}, "stats": {"count", "rate"}, "filters": {"phrase"},
	}, coverage)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "filters/phrase,pipes/unroll,stats/rate" {
		t.Errorf("missing = %v", got)
	}
}

// The committed coverage file parses, and every construct of every matrix version with a constructs list beside
// LVP_QUERY_CORPUS_OUT is covered or ignored.
func TestConstructCoverageFileParses(t *testing.T) {
	var coverage map[string]json.RawMessage
	readJSON(t, coveragePath, &coverage)
	if _, err := missingConstructs(map[string][]string{}, coverage); err != nil {
		t.Fatalf("%s: %v", coveragePath, err)
	}
}

// The oracles pin the compatibility matrix's versions: test/oracle/vl each VictoriaLogs matrix version (go.mod the
// pinned one), test/oracle/loki the pinned Loki version. A version bump that forgets an oracle fails here, in the
// unit tests, and not later as a report that quietly leaves the oracle's section out.
func TestOraclePinsMatchTheMatrix(t *testing.T) {
	versions := goldenVersions(t)
	for _, v := range versions {
		file := oracleModfile(v, versions[0])
		pin := "replace github.com/VictoriaMetrics/VictoriaLogs => github.com/VictoriaMetrics/VictoriaLogs " + v + "\n"
		if !strings.Contains(readFile(t, filepath.Join(oracleDir, file)), pin) {
			t.Errorf("test/oracle/vl/%s does not pin VictoriaLogs %s, a matrix version: run LVP_ORACLE_PIN=%s go test ./internal/querycorpus -run TestPinVLOracle", file, v, v)
		}
	}
	matches, err := filepath.Glob(filepath.Join(oracleDir, "go.v*.mod"))
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range matches {
		v := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(m), "go."), ".mod")
		if !slices.Contains(versions, v) {
			t.Errorf("test/oracle/vl/%s pins %s, which is no longer a matrix version: remove it and its .sum", filepath.Base(m), v)
		}
	}
	loki := lokiMatrixVersion(t)
	if !strings.Contains(readFile(t, "../../test/oracle/loki/go.mod"), "github.com/grafana/loki/v3 v"+loki+"\n") {
		t.Errorf("test/oracle/loki/go.mod does not require Loki v%s, the matrix's pinned version", loki)
	}
}
