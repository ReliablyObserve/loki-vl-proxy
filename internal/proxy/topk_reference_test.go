package proxy

import (
	"fmt"
	"math"
	"math/rand"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// rankTopK is the reference for topk and bottomk over one instant vector, written from LogQL's documented
// meaning: topk selects the k largest samples by value and bottomk the k smallest, within each group of the
// by()/without() labels (group gives a series' group key). A NaN sample ranks after every number for both, and
// among equal values the series met first (input order) is kept. TestTopKReferenceMatchesRecordedLoki holds this
// to Loki's recorded answers over the fuzz target's whole value domain. k below 1 keeps nothing.
//
// The result lists the groups in the order their first series was met, each group in rank order.
func rankTopK(vec []topKSeries, k int, largest bool, group func(topKSeries) string) []topKSeries {
	if k < 1 {
		return nil
	}
	var order []string
	members := map[string][]topKSeries{}
	for _, s := range vec {
		g := group(s)
		if _, seen := members[g]; !seen {
			order = append(order, g)
		}
		members[g] = append(members[g], s)
	}
	var out []topKSeries
	for _, g := range order {
		m := members[g]
		sort.SliceStable(m, func(i, j int) bool { return ranksBefore(m[i].value, m[j].value, largest) })
		if len(m) > k {
			m = m[:k]
		}
		out = append(out, m...)
	}
	return out
}

// ranksBefore reports whether a sample of value a ranks before one of value b: larger first for topk, smaller
// first for bottomk, every number before NaN.
func ranksBefore(a, b float64, largest bool) bool {
	if math.IsNaN(a) || math.IsNaN(b) {
		return !math.IsNaN(a) && math.IsNaN(b)
	}
	if largest {
		return a > b
	}
	return a < b
}

// topKGroupingText is the clause of a fuzz grouping as LogQL writes it.
func topKGroupingText(g *logqlpkg.Grouping) string {
	if g == nil {
		return ""
	}
	word := "by"
	if g.Without {
		word = "without"
	}
	return " " + word + " (" + strings.Join(g.Labels, ", ") + ")"
}

// topKRecordedCases are the seeded fuzz cases whose answers are recorded from Loki: every grid, k from 1 to 6,
// topk and bottomk, every grouping of decodeTopKFuzz.
func topKRecordedCases() []topKFuzzCase {
	rng := rand.New(rand.NewSource(11))
	var out []topKFuzzCase
	for i := 0; i < 240; i++ {
		data := make([]byte, 4+9*5)
		rng.Read(data)
		c := decodeTopKFuzz(data)
		if c.k < 1 {
			continue
		}
		out = append(out, c)
	}
	return out
}

var topKRecordStart = time.Unix(1700000000, 0)

// topKCaseDataSet is a fuzz case as Loki data: a stream per series whose line at each present step (30s before it)
// carries the cell's value, read back by last_over_time over an unwrap, so NaN and +Inf are samples too.
func topKCaseDataSet(id int, c topKFuzzCase) oraclerun.EngineDataSet {
	set := oraclerun.EngineDataSet{}
	for i := 0; i < c.series; i++ {
		st := oraclerun.EngineStream{Labels: map[string]string{"app": c.app(i), "env": c.env(i)}}
		for j := 0; j < c.steps; j++ {
			if c.present[i][j] {
				ts := topKRecordStart.Add(time.Duration(j)*time.Minute - 30*time.Second)
				st.Entries = append(st.Entries, oraclerun.EngineEntry{TS: ts.UnixNano(), Line: "v=" + strconv.FormatFloat(c.cells[i][j], 'f', -1, 64)})
			}
		}
		if len(st.Entries) > 0 {
			set.Streams = append(set.Streams, st)
		}
	}
	name := "bottomk"
	if c.descending {
		name = "topk"
	}
	query := fmt.Sprintf(`%s%s (%d, sum by (app, env) (last_over_time({app=~".+"} | logfmt | unwrap v [1m])))`, name, topKGroupingText(c.group), c.k)
	end := topKRecordStart.Add(time.Duration(c.steps-1) * time.Minute)
	set.Requests = []oraclerun.EngineRequest{{ID: strconv.Itoa(id), Unaligned: true, Form: url.Values{
		"query": {query}, "start": {strconv.FormatInt(topKRecordStart.Unix(), 10)}, "end": {strconv.FormatInt(end.Unix(), 10)}, "step": {"60"},
	}.Encode()}}
	return set
}

// The reference ranks as Loki does on every recorded case: per step and group the same kept values, and the same
// series wherever no tie sits across the k-th place.
//
// conformance: semantics/topk-bottomk-range-series, semantics/topk-grouped-spellings
func TestTopKReferenceMatchesRecordedLoki(t *testing.T) {
	cases := topKRecordedCases()
	var sets []oraclerun.EngineDataSet
	for i, c := range cases {
		sets = append(sets, topKCaseDataSet(i, c))
	}
	answers := lokiEngineAnswers(t, "topk_fuzz_cases", sets)
	checked := 0
	for i, c := range cases {
		a := answers[strconv.Itoa(i)]
		if a.Status != 200 {
			t.Fatalf("case %d: Loki answered %d %s", i, a.Status, a.Error)
		}
		s, err := decodeLokiSeries(a.Body)
		if err != nil {
			t.Fatal(err)
		}
		for j := 0; j < c.steps; j++ {
			lokiStep := topKFuzzStep{}
			ts := topKRecordStart.Add(time.Duration(j)*time.Minute).Unix() * 1000
			for key, points := range s.Points {
				if text, ok := points[ts]; ok {
					v, _ := strconv.ParseFloat(text, 64)
					lokiStep[s.Labels[key]["app"]+"/"+s.Labels[key]["env"]] = v
				}
			}
			c.checkStep(t, fmt.Sprintf("Loki (case %d)", i), j, lokiStep)
			checked++
		}
	}
	if checked < 300 {
		t.Fatalf("only %d steps checked", checked)
	}
}
