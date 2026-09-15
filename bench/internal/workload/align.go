package workload

import (
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// StepOf returns the step of a range request (`step` query arg) as a duration.
// Loki accepts a float number of seconds or a Prometheus-style duration; the
// VictoriaLogs native API accepts durations. It returns 0 when there is no
// usable step.
func StepOf(params url.Values) time.Duration {
	raw := strings.TrimSpace(params.Get("step"))
	if raw == "" {
		return 0
	}
	if secs, err := strconv.ParseFloat(raw, 64); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs * float64(time.Second))
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0
	}
	return d
}

// FloorToStep floors a Unix-nanosecond timestamp to a multiple of step.
func FloorToStep(ns int64, step time.Duration) int64 {
	s := int64(step)
	if s <= 0 {
		return ns
	}
	r := ns % s
	if r < 0 {
		r += s
	}
	return ns - r
}

// AlignToStep floors the start and end of every step-bearing request to the
// step, as Grafana does before issuing a range query. Loki (with
// align_queries_with_step) and the proxy (epoch-aligned buckets) then evaluate
// exactly the same axis. Instant queries (`time`) and step-less requests are
// returned unchanged. The input is not modified.
func AlignToStep(workloads []Workload) []Workload {
	out := make([]Workload, len(workloads))
	for i, w := range workloads {
		aligned := Workload{Name: w.Name, Queries: make([]Query, len(w.Queries))}
		for j, q := range w.Queries {
			aligned.Queries[j] = alignQuery(q)
		}
		out[i] = aligned
	}
	return out
}

func alignQuery(q Query) Query {
	step := StepOf(q.Params)
	if step <= 0 {
		return q
	}
	params := make(url.Values, len(q.Params))
	for k, v := range q.Params {
		params[k] = append([]string(nil), v...)
	}
	for _, key := range []string{"start", "end"} {
		v := params.Get(key)
		if v == "" {
			continue
		}
		ns, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			continue
		}
		params.Set(key, strconv.FormatInt(FloorToStep(ns, step), 10))
	}
	q.Params = params
	return q
}

// EarliestEvaluated returns the earliest timestamp (Unix ns) a request reads:
// its earliest start/time bound minus the query's range-vector lookback.
func EarliestEvaluated(params url.Values) (int64, bool) {
	earliest, ok := EarliestTime(params)
	if !ok {
		return 0, false
	}
	return earliest - int64(Lookback(params.Get("query"))), true
}

var (
	// rangeRe matches a range selector `[5m]` or a subquery `[1h:1m]`.
	rangeRe = regexp.MustCompile(`\[\s*([0-9][0-9a-zA-Z.]*)\s*(:\s*[0-9a-zA-Z.]*\s*)?\]`)
	// offsetRe matches an `offset 1h` modifier.
	offsetRe = regexp.MustCompile(`\boffset\s+(-?[0-9][0-9a-zA-Z.]*)`)
)

// Lookback returns how far before a sample's timestamp a LogQL query reads: the
// largest range selector, plus every subquery range (subqueries nest on top of
// their inner range), plus the largest positive `offset`. It is an upper bound
// for queries that combine several selectors. String literals are ignored, so
// regex character classes such as "[0-9]" are not mistaken for ranges.
func Lookback(query string) time.Duration {
	q := stripStringLiterals(query)
	var maxRange, subqueries, maxOffset time.Duration
	for _, m := range rangeRe.FindAllStringSubmatch(q, -1) {
		d, ok := parsePromDuration(m[1])
		if !ok {
			continue
		}
		if m[2] != "" {
			subqueries += d
		} else if d > maxRange {
			maxRange = d
		}
	}
	for _, m := range offsetRe.FindAllStringSubmatch(q, -1) {
		if d, ok := parsePromDuration(m[1]); ok && d > maxOffset {
			maxOffset = d
		}
	}
	return maxRange + subqueries + maxOffset
}

// stripStringLiterals blanks out "double-quoted" and `backtick` strings.
func stripStringLiterals(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	var quote rune
	escaped := false
	for _, r := range s {
		switch {
		case quote == 0 && (r == '"' || r == '`'):
			quote = r
			b.WriteRune(' ')
		case quote != 0:
			if quote == '"' && !escaped && r == '\\' {
				escaped = true
			} else {
				if r == quote && !escaped {
					quote = 0
				}
				escaped = false
			}
			b.WriteRune(' ')
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

var promUnits = map[string]time.Duration{
	"ms": time.Millisecond,
	"s":  time.Second,
	"m":  time.Minute,
	"h":  time.Hour,
	"d":  24 * time.Hour,
	"w":  7 * 24 * time.Hour,
	"y":  365 * 24 * time.Hour,
}

var promDurationPart = regexp.MustCompile(`([0-9]+)(ms|s|m|h|d|w|y)`)

// parsePromDuration parses Prometheus/LogQL durations such as 5m, 1h30m or 2d.
func parsePromDuration(s string) (time.Duration, bool) {
	if s == "" {
		return 0, false
	}
	if promDurationPart.ReplaceAllString(s, "") != "" {
		return 0, false
	}
	var total time.Duration
	for _, m := range promDurationPart.FindAllStringSubmatch(s, -1) {
		n, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil {
			return 0, false
		}
		total += time.Duration(n) * promUnits[m[2]]
	}
	return total, total > 0
}

// EarliestTime returns the earliest start/time timestamp (Unix ns) a request
// covers, and false when it carries no parseable time bound.
func EarliestTime(params url.Values) (int64, bool) {
	var earliest int64
	found := false
	for _, key := range []string{"start", "time", "end"} {
		v := params.Get(key)
		if v == "" {
			continue
		}
		ns, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			continue
		}
		if !found || ns < earliest {
			earliest = ns
			found = true
		}
	}
	return earliest, found
}
