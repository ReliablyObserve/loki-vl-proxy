package main

import (
	"fmt"
	"sort"
	"strings"
)

// Coverage: how each registry case is proven, in three states, per VictoriaLogs version.
//
//	proven live        Loki and the proxy answered the case on equal, non-empty data (verdict matches or differs)
//	proven by a test   not proven live, and wired to a named test (conformance/registry/generated/wiring.json) while
//	                   the last ci.yaml run on main is green: the tests are named, and the run that passed them is cited
//	not proven         neither, with the reason the live run gave (empty on both sides, blocked and its class, not
//	                   executable, data that differs, ...), or the wired tests' CI state when it is not green
//
// Only a regression is red (a fixed case that differs, handled where the verdicts are read): a case that is not proven
// is a number to work down, never a red reason.

// Coverage states of one case.
const (
	coverageLive  = "proven_live"
	coverageTest  = "proven_by_test"
	coverageNone  = "not_proven"
	ciWorkflow    = "ci.yaml"
	reasonClipped = 160
)

// CoveragePart is one VictoriaLogs version's coverage.
type CoveragePart struct {
	Cases        int            `json:"cases"`
	ProvenLive   int            `json:"proven_live"`
	ProvenByTest int            `json:"proven_by_test"`
	NotProven    int            `json:"not_proven"`
	NotProvenBy  map[string]int `json:"not_proven_by"`
	// CI is the run "passing" is read from: the last ci.yaml run on main. A case is proven by a test only while it
	// concluded success.
	CI *CIRun `json:"ci,omitempty"`
}

// CIRun is the last run of a workflow on main.
type CIRun struct {
	Workflow   string `json:"workflow"`
	Conclusion string `json:"conclusion"`
	Date       string `json:"date,omitempty"`
	URL        string `json:"url,omitempty"`
}

// Green reports whether the run passed its tests.
func (c *CIRun) Green() bool { return c != nil && c.Conclusion == "success" }

// caseOutcome is what one run said about one case: what coverage needs of it.
type caseOutcome struct{ verdict, class, reason string }

func (s *Summary) coverage(in Inputs, merged map[string]any) error {
	if len(s.outcomes) == 0 {
		return nil
	}
	var wiring struct {
		ItemToTests map[string][]string `json:"item_to_tests"`
	}
	if in.Wiring != "" {
		if _, err := s.optional("registry wiring", in.Wiring, &wiring); err != nil {
			return err
		}
	}
	var ci *CIRun
	if in.Runs != "" {
		var runs struct {
			Workflows map[string]struct {
				Main *CIRun `json:"main"`
			} `json:"workflows"`
		}
		if err := readJSON(in.Runs, &runs); err == nil {
			if ci = runs.Workflows[ciWorkflow].Main; ci != nil {
				ci.Workflow = ciWorkflow
			}
		}
	}
	byCase, _ := merged["cases"].(map[string]map[string]any)
	s.Coverage = map[string]*CoveragePart{}
	for _, v := range s.VictoriaLogs {
		part := &CoveragePart{NotProvenBy: map[string]int{}, CI: ci}
		ids := make([]string, 0, len(s.outcomes[v]))
		for id := range s.outcomes[v] {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			o := s.outcomes[v][id]
			part.Cases++
			tests := wiring.ItemToTests[id]
			state, why := coverageOf(o, tests, ci)
			switch state {
			case coverageLive:
				part.ProvenLive++
			case coverageTest:
				part.ProvenByTest++
			default:
				part.NotProven++
				part.NotProvenBy[why.class]++
			}
			if entry, ok := byCase[id][v].(map[string]any); ok && state != coverageLive {
				proof := map[string]any{"state": state}
				if state == coverageTest {
					proof["tests"], proof["ci_run"] = tests, ci.URL
				} else {
					proof["reason"] = why.reason
				}
				entry["coverage"] = proof
			}
		}
		s.Coverage[v] = part
	}
	return nil
}

type notProven struct{ class, reason string }

// coverageOf is the state of one case on one version.
func coverageOf(o caseOutcome, tests []string, ci *CIRun) (string, notProven) {
	if o.verdict == "matches" || o.verdict == "differs" {
		return coverageLive, notProven{}
	}
	why := notProven{class: o.verdict, reason: strings.TrimSpace(o.reason)}
	if o.verdict == "blocked" && o.class != "" {
		why.class = "blocked: " + o.class
	}
	if len(why.reason) > reasonClipped {
		why.reason = why.reason[:reasonClipped] + "..."
	}
	switch {
	case len(tests) == 0:
		why.reason = join(why.reason, "no named test is wired to it")
	case !ci.Green():
		state := "no run recorded"
		if ci != nil {
			state = "ended " + ci.Conclusion
		}
		why.reason = join(why.reason, fmt.Sprintf("%d wired test file(s), but the last %s run on main %s", len(tests), ciWorkflow, state))
	default:
		return coverageTest, why
	}
	return coverageNone, why
}

func join(a, b string) string {
	if a == "" {
		return b
	}
	return a + "; " + b
}

// coverageDetail is the report's text for one version: the three states and why the rest is not proven.
func coverageDetail(v string, c *CoveragePart) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Coverage, VictoriaLogs %s: %d of %d cases proven (live %d, by a named passing test %d); not proven %d",
		v, c.ProvenLive+c.ProvenByTest, c.Cases, c.ProvenLive, c.ProvenByTest, c.NotProven)
	if len(c.NotProvenBy) > 0 {
		keys := make([]string, 0, len(c.NotProvenBy))
		for k := range c.NotProvenBy {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, fmt.Sprintf("%s %d", k, c.NotProvenBy[k]))
		}
		fmt.Fprintf(&b, " (%s)", strings.Join(parts, ", "))
	}
	switch {
	case c.CI.Green():
		fmt.Fprintf(&b, "; tests passing: %s run of %s on main, %s.", c.CI.Workflow, c.CI.Date, c.CI.URL)
	case c.CI != nil:
		fmt.Fprintf(&b, "; wired tests are not counted: the last %s run on main ended %s.", c.CI.Workflow, c.CI.Conclusion)
	default:
		fmt.Fprintf(&b, "; wired tests are not counted: no %s run on main is recorded, so their state is unknown.", ciWorkflow)
	}
	return b.String()
}
