package vlemu

import (
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"time"
)

// The top-value filter of a series-limited breakdown (the proxy's rankedSingleFieldQuery): the read keeps the rows
// whose field is one of the n values with the most lines over the read's rows, or, ranked by min(_time), the n
// values whose first line is earliest; ties by value.
// The emulator runs the subquery's meaning, without its pipes, over the rows the request selects (VictoriaLogs
// scopes a subquery to the read's time range).

// topValueFilterRE is `| filter <f>:in(<selector> | stats by (<f>) count() as __lvp_rank | sort by (__lvp_rank
// desc, <f>) | limit <n> | fields <f>)`.
var topValueFilterRE = regexp.MustCompile(`\| filter (\w+):in\([^|()]*(?:\([^()]*\)[^|()]*)* \| stats by \((\w+)\) (count\(\)|min\(_time\)) as __lvp_rank \| sort by \(__lvp_rank( desc)?, (\w+)\) \| limit (\d+) \| fields (\w+)\) `)

// splitTopValueFilterRE is the Loki series-limit mode's top-value filter over several splits: `| filter <f>:in(
// <selector> | stats by (_time:<bucket> offset <offset>, <f>) min(_time) as __lvp_rank | sort by (__lvp_rank, <f>)
// partition by (_time) limit <n> | fields <f>)`: the n values met first in every time bucket.
var splitTopValueFilterRE = regexp.MustCompile(`\| filter (\w+):in\([^|()]*(?:\([^()]*\)[^|()]*)* \| stats by \(_time:(\w+) offset (\w+), (\w+)\) min\(_time\) as __lvp_rank \| sort by \(__lvp_rank, (\w+)\) partition by \(_time\) limit (\d+) \| fields (\w+)\) `)

// emuTopValueFilter is a read's top-value filter: its field and how many values it keeps.
type emuTopValueFilter struct {
	field   string
	n       int
	byFirst bool // ranked by min(_time) ascending, not by count
	// bucket and offset rank the values in every time bucket (VictoriaLogs' _time:<bucket> offset <offset>).
	bucket, offset int64
}

// cutTopValueFilter cuts a top-value filter out of a read's query; nil when the query has none.
func cutTopValueFilter(e *Emulator, query string) (string, *emuTopValueFilter) {
	if loc := splitTopValueFilterRE.FindStringSubmatchIndex(query); loc != nil {
		m := splitTopValueFilterRE.FindStringSubmatch(query)
		if m[1] != m[4] || m[1] != m[5] || m[1] != m[7] {
			e.T.Fatalf("emulator: top-value filter over several fields: %s", m[0])
		}
		bucket, errB := time.ParseDuration(m[2])
		offset, errO := time.ParseDuration(m[3])
		n, errN := strconv.Atoi(m[6])
		if errB != nil || errO != nil || errN != nil || bucket <= 0 || n <= 0 {
			e.T.Fatalf("emulator: split top-value filter %q", m[0])
		}
		return query[:loc[0]] + query[loc[1]:], &emuTopValueFilter{field: m[1], n: n, byFirst: true, bucket: int64(bucket), offset: int64(offset)}
	}
	loc := topValueFilterRE.FindStringSubmatchIndex(query)
	if loc == nil {
		return query, nil
	}
	m := topValueFilterRE.FindStringSubmatch(query)
	if m[1] != m[2] || m[1] != m[5] || m[1] != m[7] {
		e.T.Fatalf("emulator: top-value filter over several fields: %s", m[0])
	}
	byFirst := m[3] == "min(_time)"
	if byFirst == (m[4] == " desc") {
		e.T.Fatalf("emulator: top-value ranking %q sorted %q", m[3], m[4])
	}
	n, err := strconv.Atoi(m[6])
	if err != nil || n <= 0 {
		e.T.Fatalf("emulator: top-value filter limit %q", m[6])
	}
	return query[:loc[0]] + query[loc[1]:], &emuTopValueFilter{field: m[1], n: n, byFirst: byFirst}
}

// keep is recs without the rows whose field is not a top value: values ranked by their lines among the rows the
// request selects (most first, then by value), the first n kept; with a bucket, the first n of every bucket.
func (f *emuTopValueFilter) keep(e *Emulator, r *http.Request, recs []emuRec) []emuRec {
	if f.bucket > 0 {
		return f.keepPerBucket(e, r, recs)
	}
	counts := map[string]int{}
	firsts := map[string]int64{}
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
			if first, seen := firsts[v]; !seen || row.TS < first {
				firsts[v] = row.TS
			}
		}
	}
	values := make([]string, 0, len(counts))
	for v := range counts {
		values = append(values, v)
	}
	sort.Slice(values, func(i, j int) bool {
		if f.byFirst && firsts[values[i]] != firsts[values[j]] {
			return firsts[values[i]] < firsts[values[j]]
		}
		if !f.byFirst && counts[values[i]] != counts[values[j]] {
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

// keepPerBucket is keep for a ranking per time bucket: in every bucket (VictoriaLogs truncates t+offset to the
// bucket and takes the offset back), the n values whose first line in it is earliest, ties by value.
func (f *emuTopValueFilter) keepPerBucket(e *Emulator, r *http.Request, recs []emuRec) []emuRec {
	firsts := map[int64]map[string]int64{}
	for _, row := range e.SelectRows(r) {
		fields := map[string]string{}
		for _, m := range StreamLabelRE.FindAllStringSubmatch(row.Stream, -1) {
			fields[m[1]] = m[2]
		}
		for k, v := range row.Labels {
			fields[k] = v
		}
		v, ok := fields[f.field]
		if !ok {
			continue
		}
		t := row.TS + f.offset
		rem := t % f.bucket
		if rem < 0 {
			rem += f.bucket
		}
		b := t - rem - f.offset
		if firsts[b] == nil {
			firsts[b] = map[string]int64{}
		}
		if first, seen := firsts[b][v]; !seen || row.TS < first {
			firsts[b][v] = row.TS
		}
	}
	top := map[string]bool{}
	for _, byValue := range firsts {
		values := make([]string, 0, len(byValue))
		for v := range byValue {
			values = append(values, v)
		}
		sort.Slice(values, func(i, j int) bool {
			if byValue[values[i]] != byValue[values[j]] {
				return byValue[values[i]] < byValue[values[j]]
			}
			return values[i] < values[j]
		})
		for i := 0; i < len(values) && i < f.n; i++ {
			top[values[i]] = true
		}
	}
	kept := recs[:0]
	for _, rec := range recs {
		if top[rec.f[f.field]] {
			kept = append(kept, rec)
		}
	}
	return kept
}
