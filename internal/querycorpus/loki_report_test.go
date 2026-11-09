package querycorpus_test

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
)

// lokiAllowlistPath holds the proxy files that may share token runs with Loki's LogQL sources, with why.
const lokiAllowlistPath = "../../test/oracle/loki/similarity-allowlist.json"

// lokiVerdict is the Loki oracle's answer for one input (test/oracle/loki Verdict).
type lokiVerdict struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"` // client or case
	Accept      bool   `json:"accept"`
	Ours        bool   `json:"ours"`
	SameError   *bool  `json:"same_error,omitempty"`
	SameMeaning *bool  `json:"same_meaning,omitempty"`
}

func (v lokiVerdict) agree() bool       { return v.Accept == v.Ours }
func (v lokiVerdict) sameError() bool   { return v.SameError != nil && *v.SameError }
func (v lokiVerdict) sameMeaning() bool { return v.SameMeaning != nil && *v.SameMeaning }

// LokiCounts are Loki's parser and the proxy's validation on one kind of query.
type LokiCounts struct {
	Queries     int `json:"queries"`
	BothAccept  int `json:"both_accept"`
	SameMeaning int `json:"same_meaning"`
	BothReject  int `json:"both_reject"`
	SameError   int `json:"same_error"`
	// OursOnly are queries the proxy accepts and Loki rejects; LokiOnly the other way round.
	OursOnly int `json:"ours_only"`
	LokiOnly int `json:"loki_only"`
}

// LokiSummary is the Loki oracle on the corpus's client queries and on Loki's syntax test queries.
type LokiSummary struct {
	Version string     `json:"version"`
	Client  LokiCounts `json:"client"`
	Case    LokiCounts `json:"case"`
}

// LokiRegressions are the queries on both the base and the PR that the Loki oracle now judges worse.
type LokiRegressions struct {
	// Disagree: the proxy and Loki agreed on accepting or rejecting it on the base, and do not now.
	Disagree []string `json:"disagree"`
	// ErrorLost: both rejected it with Loki's error text on the base; the proxy's text differs now.
	ErrorLost []string `json:"error_lost"`
	// MeaningLost: Loki read the proxy's printed form the same on the base, and does not now.
	MeaningLost []string `json:"meaning_lost"`
}

// SimilarFile is a proxy file that shares token runs with Loki's LogQL sources (test/oracle/loki Similarity).
type SimilarFile struct {
	File   string `json:"file"`
	Shared int    `json:"shared"`
	Best   string `json:"best,omitempty"`
	// Allowed is the allowlist's max_shared for the file, 0 when it is not listed; PendingRemoval marks a copy of
	// Loki code held at its size until it is rewritten.
	Allowed        int  `json:"allowed"`
	PendingRemoval bool `json:"pending_removal,omitempty"`
}

func readLokiVerdicts(t *testing.T, path string) map[string]lokiVerdict {
	t.Helper()
	fh, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fh.Close() }()
	zr, err := gzip.NewReader(fh)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]lokiVerdict{}
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		var v lokiVerdict
		if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		out[v.Kind+"/"+v.ID] = v
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func summarizeLoki(version string, verdicts map[string]lokiVerdict) LokiSummary {
	s := LokiSummary{Version: version}
	for _, v := range verdicts {
		c := &s.Client
		if v.Kind == "case" {
			c = &s.Case
		}
		c.Queries++
		switch {
		case v.Accept && v.Ours:
			c.BothAccept++
			if v.sameMeaning() {
				c.SameMeaning++
			}
		case !v.Accept && !v.Ours:
			c.BothReject++
			if v.sameError() {
				c.SameError++
			}
		case v.Ours:
			c.OursOnly++
		default:
			c.LokiOnly++
		}
	}
	return s
}

func lokiRegressions(base, head map[string]lokiVerdict) LokiRegressions {
	reg := LokiRegressions{Disagree: []string{}, ErrorLost: []string{}, MeaningLost: []string{}}
	for key, h := range head {
		b, ok := base[key]
		if !ok {
			continue
		}
		switch {
		case b.agree() && !h.agree():
			reg.Disagree = append(reg.Disagree, key)
		case b.sameError() && !h.sameError():
			reg.ErrorLost = append(reg.ErrorLost, key)
		case b.sameMeaning() && h.Accept && h.Ours && !h.sameMeaning():
			// A query both now reject (Loki's rejection, matched) has no printed form to compare.
			reg.MeaningLost = append(reg.MeaningLost, key)
		}
	}
	sort.Strings(reg.Disagree)
	sort.Strings(reg.ErrorLost)
	sort.Strings(reg.MeaningLost)
	return reg
}

// similarFiles reads the oracle's similar output and the allowlist: the files that share any fingerprint, with
// paths relative to the repository.
func similarFiles(t *testing.T, path string) []SimilarFile {
	t.Helper()
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var rows []SimilarFile
	if err := json.Unmarshal(b, &rows); err != nil {
		t.Fatal(err)
	}
	var allow struct {
		Files map[string]struct {
			MaxShared      int    `json:"max_shared"`
			PendingRemoval bool   `json:"pending_removal"`
			Reason         string `json:"reason"`
		} `json:"files"`
	}
	readJSON(t, lokiAllowlistPath, &allow)
	out := []SimilarFile{}
	for _, r := range rows {
		if r.Shared == 0 {
			continue
		}
		for _, top := range []string{"/internal/", "/cmd/", "/test/"} {
			if i := strings.LastIndex(r.File, top); i >= 0 {
				r.File = r.File[i+1:]
				break
			}
		}
		r.Allowed, r.PendingRemoval = allow.Files[r.File].MaxShared, allow.Files[r.File].PendingRemoval
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].File < out[j].File })
	return out
}

// lokiFailures are the gate's findings from the Loki oracle.
func lokiFailures(reg *LokiRegressions, similar []SimilarFile) []string {
	var out []string
	if reg != nil {
		for _, r := range []struct {
			ids  []string
			what string
		}{
			{reg.Disagree, "queries the proxy and Loki no longer agree on accepting"},
			{reg.ErrorLost, "queries whose error text is no longer Loki's"},
			{reg.MeaningLost, "queries whose printed form no longer means the same to Loki"},
		} {
			if len(r.ids) > 0 {
				out = append(out, fmt.Sprintf("Loki: %d %s: %s", len(r.ids), r.what, strings.Join(first(r.ids), ", ")))
			}
		}
	}
	for _, s := range similar {
		if s.Shared > s.Allowed {
			out = append(out, fmt.Sprintf("%s shares %d token runs with Loki's %s (allowed %d, test/oracle/loki/similarity-allowlist.json): the repository's code is written without Loki's",
				s.File, s.Shared, s.Best, s.Allowed))
		}
	}
	return out
}

func TestLokiSummaryRegressionsAndSimilarity(t *testing.T) {
	yes, no := true, false
	base := map[string]lokiVerdict{
		"client/a": {Kind: "client", Accept: true, Ours: true, SameMeaning: &yes},
		"client/b": {Kind: "client", Accept: false, Ours: false, SameError: &yes},
		"client/c": {Kind: "client", Accept: true, Ours: true, SameMeaning: &yes},
		"case/d":   {Kind: "case", Accept: false, Ours: false, SameError: &no},
		"client/f": {Kind: "client", Accept: true, Ours: true, SameMeaning: &yes},
	}
	head := map[string]lokiVerdict{
		"client/a": {Kind: "client", Accept: true, Ours: false},                   // now disagree
		"client/b": {Kind: "client", Accept: false, Ours: false, SameError: &no},  // error text lost
		"client/c": {Kind: "client", Accept: true, Ours: true, SameMeaning: &no},  // meaning lost
		"case/d":   {Kind: "case", Accept: false, Ours: false, SameError: &no},    // held
		"client/e": {Kind: "client", Accept: false, Ours: true},                   // new: never a regression
		"client/f": {Kind: "client", Accept: false, Ours: false, SameError: &yes}, // both now reject alike
	}
	reg := lokiRegressions(base, head)
	if strings.Join(reg.Disagree, ",") != "client/a" || strings.Join(reg.ErrorLost, ",") != "client/b" ||
		strings.Join(reg.MeaningLost, ",") != "client/c" {
		t.Errorf("regressions = %+v", reg)
	}
	s := summarizeLoki("3.7.9", head)
	if s.Client != (LokiCounts{Queries: 5, BothAccept: 1, BothReject: 2, SameError: 1, OursOnly: 1, LokiOnly: 1}) ||
		s.Case != (LokiCounts{Queries: 1, BothReject: 1}) {
		t.Errorf("summary = %+v", s)
	}
	got := lokiFailures(&reg, []SimilarFile{
		{File: "internal/logql/stage_validate.go", Shared: 9, Allowed: 9},
		{File: "internal/proxy/new.go", Shared: 3, Best: "pkg/logql/log/util.go"},
	})
	if len(got) != 4 || !strings.HasPrefix(got[3], "internal/proxy/new.go shares 3 token runs") {
		t.Errorf("failures = %q", got)
	}
}

func TestLokiSimilarityAllowlistParses(t *testing.T) {
	var allow struct {
		Files map[string]struct {
			MaxShared int    `json:"max_shared"`
			Reason    string `json:"reason"`
		} `json:"files"`
	}
	readJSON(t, lokiAllowlistPath, &allow)
	for f, e := range allow.Files {
		if e.MaxShared <= 0 || strings.TrimSpace(e.Reason) == "" {
			t.Errorf("%s: an allowlist entry needs a positive max_shared and a reason", f)
		}
		if _, err := os.Stat("../../" + f); err != nil {
			t.Errorf("%s: listed but not in the tree", f)
		}
	}
}
