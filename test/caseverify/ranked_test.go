package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"net/url"
	"os"
	"regexp"
	"strings"
	"testing"
)

// m is a matrix answer: a series per label set, each with the same two steps.
func m(series ...string) Answer {
	return ok(`{"status":"success","data":{"resultType":"matrix","result":[` + strings.Join(series, ",") + `]}}`)
}

func s(labels string, v1, v2 string) string {
	return `{"metric":` + labels + `,"values":[[1800000000,"` + v1 + `"],[1800000060,"` + v2 + `"]]}`
}

func TestRankOf(t *testing.T) {
	for _, c := range []struct {
		query      string
		k          int
		descending bool
		grouping   string
		nilSpec    bool
	}{
		{query: `topk(3, sum by (app) (rate({a="b"}[5m])))`, k: 3, descending: true},
		{query: `bottomk(2, sum by (app) (rate({a="b"}[5m])))`, k: 2},
		{query: `topk by (app) (5, sum by (app, pod) (rate({a="b"}[5m])))`, k: 5, descending: true, grouping: "by(app)"},
		{query: `bottomk without (pod) (1, sum by (app, pod) (rate({a="b"}[5m])))`, k: 1, grouping: "without(pod)"},
		{query: `sum(topk(3, rate({a="b"}[5m])))`, nilSpec: true},
		{query: `sum(rate({a="b"}[5m]))`, nilSpec: true},
		{query: `topk(`, nilSpec: true},
	} {
		got := RankOf(c.query)
		if c.nilSpec {
			if got != nil {
				t.Errorf("%s: %+v, want none", c.query, got)
			}
			continue
		}
		if got == nil || got.K != c.k || got.Descending != c.descending {
			t.Errorf("%s: %+v", c.query, got)
			continue
		}
		g := ""
		if got.Grouping != nil {
			g = map[bool]string{true: "without", false: "by"}[got.Grouping.Without] + "(" + strings.Join(got.Grouping.Labels, ",") + ")"
		}
		if g != c.grouping {
			t.Errorf("%s: grouping %q, want %q", c.query, g, c.grouping)
		}
	}
}

func TestRankedDifference(t *testing.T) {
	top2 := RankOf(`topk(2, rate({a="b"}[5m]))`)
	bottom2 := RankOf(`bottomk(2, rate({a="b"}[5m]))`)
	byG := RankOf(`topk by (g) (1, rate({a="b"}[5m]))`)
	without := RankOf(`topk without (pod) (1, rate({a="b"}[5m]))`)
	top5 := RankOf(`topk(5, rate({a="b"}[5m]))`)
	cases := []struct {
		name        string
		spec        *RankSpec
		loki, proxy Answer
		differ      bool
	}{
		{"the same answer", top2, m(s(`{"a":"1"}`, "9", "9"), s(`{"a":"2"}`, "5", "5")), m(s(`{"a":"2"}`, "5", "5"), s(`{"a":"1"}`, "9", "9")), false},
		{"another series tied at the cutoff", top2, m(s(`{"a":"1"}`, "9", "9"), s(`{"a":"2"}`, "5", "5")), m(s(`{"a":"1"}`, "9", "9"), s(`{"a":"3"}`, "5", "5")), false},
		{"a series above the cutoff swapped", top2, m(s(`{"a":"1"}`, "9", "9"), s(`{"a":"2"}`, "5", "5")), m(s(`{"a":"3"}`, "9", "9"), s(`{"a":"2"}`, "5", "5")), true},
		{"a value of the ranking differs", top2, m(s(`{"a":"1"}`, "9", "9"), s(`{"a":"2"}`, "5", "5")), m(s(`{"a":"1"}`, "9", "9"), s(`{"a":"2"}`, "4", "5")), true},
		{"a step with a series fewer", top2, m(s(`{"a":"1"}`, "9", "9"), s(`{"a":"2"}`, "5", "5")), m(s(`{"a":"1"}`, "9", "9")), true},
		{"fewer series than k: they must be the same", top5, m(s(`{"a":"1"}`, "9", "9"), s(`{"a":"2"}`, "5", "5")), m(s(`{"a":"1"}`, "9", "9"), s(`{"a":"3"}`, "5", "5")), true},
		{"fewer series than k, the same ones", top5, m(s(`{"a":"1"}`, "9", "9"), s(`{"a":"2"}`, "5", "5")), m(s(`{"a":"2"}`, "5", "5"), s(`{"a":"1"}`, "9", "9")), false},
		{"bottomk: a tie at the largest of the kept", bottom2, m(s(`{"a":"1"}`, "1", "1"), s(`{"a":"2"}`, "3", "3")), m(s(`{"a":"1"}`, "1", "1"), s(`{"a":"9"}`, "3", "3")), false},
		{"bottomk: the smallest swapped", bottom2, m(s(`{"a":"1"}`, "1", "1"), s(`{"a":"2"}`, "3", "3")), m(s(`{"a":"7"}`, "1", "1"), s(`{"a":"2"}`, "3", "3")), true},
		{"by(): a tie inside a group", byG,
			m(s(`{"g":"x","p":"1"}`, "4", "4"), s(`{"g":"y","p":"1"}`, "2", "2")), m(s(`{"g":"x","p":"2"}`, "4", "4"), s(`{"g":"y","p":"3"}`, "2", "2")), false},
		{"by(): one group's ranked value differs", byG,
			m(s(`{"g":"x","p":"1"}`, "4", "4"), s(`{"g":"y","p":"1"}`, "2", "2")), m(s(`{"g":"x","p":"1"}`, "4", "4"), s(`{"g":"y","p":"1"}`, "3", "2")), true},
		{"by(): a group the proxy lacks", byG, m(s(`{"g":"x"}`, "4", "4"), s(`{"g":"y"}`, "2", "2")), m(s(`{"g":"x"}`, "4", "4")), true},
		{"by(): a value another group holds does not stand in", byG,
			m(s(`{"g":"x"}`, "4", "4"), s(`{"g":"y"}`, "2", "2")), m(s(`{"g":"x"}`, "2", "4"), s(`{"g":"y"}`, "4", "2")), true},
		{"without(): groups by the other labels", without,
			m(s(`{"app":"a","pod":"1"}`, "4", "4"), s(`{"app":"b","pod":"1"}`, "2", "2")), m(s(`{"app":"a","pod":"2"}`, "4", "4"), s(`{"app":"b","pod":"9"}`, "2", "2")), false},
		{"a sample within the value tolerance", top2, m(s(`{"a":"1"}`, "9", "9"), s(`{"a":"2"}`, "5", "5")), m(s(`{"a":"1"}`, "9.0000000001", "9"), s(`{"a":"2"}`, "5", "5")), false},
		{"a log answer is no metric answer", top2, ok(`{"data":{"resultType":"streams","result":[]}}`), m(), true},
		{"not JSON", top2, ok("x"), m(), true},
	}
	for _, c := range cases {
		msg := RankedDifference(c.loki, c.proxy, c.spec)
		if (msg != "") != c.differ {
			t.Errorf("%s: %q", c.name, msg)
		}
		// The ranked_values facet gives the same answer through the comparison.
		d := CompareWith([]string{facetRanked}, c.loki, c.proxy, Options{Rank: c.spec})
		if (len(d) > 0) != c.differ || (len(d) > 0 && d[0].Facet != facetRanked) {
			t.Errorf("%s: facet %+v", c.name, d)
		}
	}
	// Without a ranking query nothing is tied: the values compare as they are.
	if d := CompareWith([]string{facetRanked}, m(s(`{"a":"1"}`, "9", "9")), m(s(`{"a":"1"}`, "9", "8")), Options{}); len(d) != 1 || d[0].Facet != facetValues {
		t.Errorf("no ranking: %+v", d)
	}
	if d := CompareWith([]string{facetRanked}, m(s(`{"a":"1"}`, "9", "9")), m(s(`{"a":"1"}`, "9", "9")), Options{}); len(d) != 0 {
		t.Errorf("no ranking, equal: %+v", d)
	}
}

func TestRankedInstantVector(t *testing.T) {
	vec := func(a string, v string) string {
		return `{"metric":{"app":"` + a + `"},"value":[1800000000,"` + v + `"]}`
	}
	ans := func(items ...string) Answer {
		return ok(`{"data":{"resultType":"vector","result":[` + strings.Join(items, ",") + `]}}`)
	}
	spec := RankOf(`topk(1, rate({a="b"}[5m]))`)
	if msg := RankedDifference(ans(vec("a", "3")), ans(vec("b", "3")), spec); msg != "" {
		t.Errorf("a tie of one: %s", msg)
	}
	if msg := RankedDifference(ans(vec("a", "3")), ans(vec("b", "2")), spec); msg == "" {
		t.Error("another value is a difference")
	}
}

// The probe's options carry the ranking of the query it sends.
func TestOptionsOfReadsTheRanking(t *testing.T) {
	q := url.Values{"query": {`topk by (container) (5, sum by (container, pod) (count_over_time({env="production"}[5m])))`}}
	o := OptionsOf(Probe{Path: "/loki/api/v1/query_range"}, q)
	if o.Rank == nil || o.Rank.K != 5 || !o.Rank.Descending {
		t.Errorf("rank: %+v", o.Rank)
	}
	if o := OptionsOf(Probe{Path: "/loki/api/v1/labels"}, q); o.Rank != nil {
		t.Errorf("labels: %+v", o.Rank)
	}
}

var sampleRE = regexp.MustCompile(`\[\d+,"\d+"\]`)

type recordedPair struct {
	Case    string `json:"case"`
	Request struct {
		Query string `json:"query"`
	} `json:"request"`
	Loki, Proxy Answer
}

func loadTopKPairs(t *testing.T) map[string]recordedPair {
	t.Helper()
	f, err := os.Open("testdata/topk_ties.jsonl.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]recordedPair{}
	sc := bufio.NewReaderSize(zr, 1<<20)
	for {
		line, err := sc.ReadBytes('\n')
		if len(strings.TrimSpace(string(line))) > 0 {
			var p recordedPair
			if jerr := json.Unmarshal(line, &p); jerr != nil {
				t.Fatal(jerr)
			}
			out[p.Case] = p
		}
		if err != nil {
			break
		}
	}
	return out
}

// The two recorded pairs of the topk cases (Loki 3.7.9 and the proxy, one stack, 2026-11-14): the only
// differences are at ties for the last place. Compared exactly they differ; as ranked values they match.
func TestRecordedTopKPairsDifferExactlyAndMatchRanked(t *testing.T) {
	pairs := loadTopKPairs(t)
	for _, id := range []string{"semantics/topk-bottomk-range-series", "semantics/topk-grouped-spellings"} {
		p, ok := pairs[id]
		if !ok {
			t.Fatalf("%s: no recorded pair", id)
		}
		query, _ := url.QueryUnescape(strings.NewReplacer("+", " ").Replace(strings.SplitN(strings.SplitN(p.Request.Query, "query=", 2)[1], "&", 2)[0]))
		spec := RankOf(query)
		if spec == nil {
			t.Fatalf("%s: %q is no ranking", id, query)
		}
		if p.Loki.Status != 200 || p.Proxy.Status != 200 {
			t.Fatalf("%s: statuses %d %d", id, p.Loki.Status, p.Proxy.Status)
		}
		exact := CompareWith([]string{facetLabels, facetValues}, p.Loki, p.Proxy, Options{})
		if len(exact) == 0 {
			t.Errorf("%s: an exact comparison must differ at the ties", id)
		}
		if d := CompareWith([]string{facetRanked}, p.Loki, p.Proxy, Options{Rank: spec}); len(d) != 0 {
			t.Errorf("%s: ranked_values must match: %+v", id, d)
		}
		// The ranking is checked, not skipped: another value anywhere is still a difference.
		changed := p.Proxy
		first := true
		changed.Body = sampleRE.ReplaceAllStringFunc(changed.Body, func(m string) string {
			if !first {
				return m
			}
			first = false
			return strings.TrimSuffix(m, `"]`) + `7"]`
		})
		if changed.Body == p.Proxy.Body {
			t.Fatalf("%s: nothing to change", id)
		}
		if d := CompareWith([]string{facetRanked}, p.Loki, changed, Options{Rank: spec}); len(d) == 0 {
			t.Errorf("%s: a changed value must differ", id)
		}
	}
}
