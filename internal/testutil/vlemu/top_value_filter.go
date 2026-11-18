package vlemu

import (
	"net/http"
	"regexp"
	"sort"
	"strconv"
)

// The top-value filter of a series-limited breakdown (the proxy's fitTopValueFilter with a ranking subquery): the
// read keeps the rows whose field is one of the n values with the most lines over the read's rows, ties by value.
// The emulator runs the subquery's meaning, without its pipes, over the rows the request selects (VictoriaLogs
// scopes a subquery to the read's time range).

// topValueFilterRE is `| filter <f>:in(<selector> | stats by (<f>) count() as __lvp_rank | sort by (__lvp_rank
// desc, <f>) | limit <n> | fields <f>)`.
var topValueFilterRE = regexp.MustCompile(`\| filter (\w+):in\([^|()]*(?:\([^()]*\)[^|()]*)* \| stats by \((\w+)\) count\(\) as __lvp_rank \| sort by \(__lvp_rank desc, (\w+)\) \| limit (\d+) \| fields (\w+)\) `)

// emuTopValueFilter is a read's top-value filter: its field and how many values it keeps.
type emuTopValueFilter struct {
	field string
	n     int
}

// cutTopValueFilter cuts a top-value filter out of a read's query; nil when the query has none.
func cutTopValueFilter(e *Emulator, query string) (string, *emuTopValueFilter) {
	loc := topValueFilterRE.FindStringSubmatchIndex(query)
	if loc == nil {
		return query, nil
	}
	m := topValueFilterRE.FindStringSubmatch(query)
	if m[1] != m[2] || m[1] != m[3] || m[1] != m[5] {
		e.T.Fatalf("emulator: top-value filter over several fields: %s", m[0])
	}
	n, err := strconv.Atoi(m[4])
	if err != nil || n <= 0 {
		e.T.Fatalf("emulator: top-value filter limit %q", m[4])
	}
	return query[:loc[0]] + query[loc[1]:], &emuTopValueFilter{field: m[1], n: n}
}

// keep is recs without the rows whose field is not a top value: values ranked by their lines among the rows the
// request selects (most first, then by value), the first n kept.
func (f *emuTopValueFilter) keep(e *Emulator, r *http.Request, recs []emuRec) []emuRec {
	counts := map[string]int{}
	for _, row := range e.SelectRows(r) {
		fields := map[string]string{}
		for _, m := range StreamLabelRE.FindAllStringSubmatch(row.Stream, -1) {
			fields[m[1]] = m[2]
		}
		for k, v := range row.Labels {
			fields[k] = v
		}
		if v, ok := fields[f.field]; ok {
			counts[v]++
		}
	}
	values := make([]string, 0, len(counts))
	for v := range counts {
		values = append(values, v)
	}
	sort.Slice(values, func(i, j int) bool {
		if counts[values[i]] != counts[values[j]] {
			return counts[values[i]] > counts[values[j]]
		}
		return values[i] < values[j]
	})
	top := map[string]bool{}
	for i := 0; i < len(values) && i < f.n; i++ {
		top[values[i]] = true
	}
	kept := recs[:0]
	for _, rec := range recs {
		if top[rec.f[f.field]] {
			kept = append(kept, rec)
		}
	}
	return kept
}
