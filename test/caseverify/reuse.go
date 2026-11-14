package main

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// Cache reuse: after a case's answers are compared, the proxy is asked the same request again. A repeat
// must give the first answer and ask VictoriaLogs nothing; the proxy's own metrics, read before and after
// each ask, say what each ask cost and which cache layer and tier answered. A repeat that differs or calls
// VictoriaLogs again is `cache_not_reused`: a finding about the cache, never a regression of the case.
//
// A snapshot is the proxy's counters by key:
//
//	vl_calls, vl_calls:<endpoint>   VictoriaLogs requests (loki_vl_proxy_backend_duration_seconds_count),
//	                                in all and per VictoriaLogs endpoint (query, stats_query_range, hits, ...)
//	vl_bytes                        decoded VictoriaLogs body bytes (loki_vl_proxy_backend_read_bytes_total)
//	<layer>:<tier>:requests|hits    cache lookups and hits per semantic layer (loki: Loki-facing answers, vl:
//	                                VictoriaLogs-facing results) and tier (l1_memory, l2_disk, l3_peer)
//	cpu_seconds                     the proxy's CPU (process_cpu_seconds_total)
//	translations                    LogQL translations the proxy ran (loki_vl_proxy_translations_total)
//
// Not measured, for want of a metric: allocations per request (no cumulative allocation counter is exported),
// plan and translation cache hits, reuse of proxy-evaluated operators, and attribution of each counter to one
// request (background work of the proxy counts too).

// Snapshot is the proxy's counters at one instant.
type Snapshot map[string]float64

// Meter reads the proxy's counters around an ask; false when it cannot.
type Meter interface {
	Measure(caseID, variant, phase string) (Snapshot, bool)
}

// Reuse is what a repeat of a case cost against its first ask.
type Reuse struct {
	Verdict string `json:"verdict"`
	Reason  string `json:"reason,omitempty"`
	// Background is the repeat's VictoriaLogs calls on endpoints the first ask did not use: counted by the
	// process-wide metrics, attributed to the proxy's background work rather than the repeat.
	Background float64  `json:"background_vl_calls,omitempty"`
	Cold       Snapshot `json:"cold,omitempty"`
	Repeat     Snapshot `json:"repeat,omitempty"`
}

// Reuse verdicts.
const (
	reuseReused      = "reused"
	reuseNotReused   = "cache_not_reused"
	reuseNotMeasured = "not_measured"
)

// metricKeys maps a metric line to its snapshot keys.
func metricKeys(name string, labels map[string]string) []string {
	switch name {
	case "loki_vl_proxy_backend_duration_seconds_count":
		return []string{"vl_calls", "vl_calls:" + labels["endpoint"]}
	case "loki_vl_proxy_backend_read_bytes_total":
		return []string{"vl_bytes"}
	case "loki_vl_proxy_cache_layer_requests_total":
		return []string{labels["layer"] + ":" + labels["tier"] + ":requests"}
	case "loki_vl_proxy_cache_layer_hits_total":
		return []string{labels["layer"] + ":" + labels["tier"] + ":hits"}
	case "process_cpu_seconds_total":
		return []string{"cpu_seconds"}
	case "loki_vl_proxy_translations_total":
		return []string{"translations"}
	}
	return nil
}

// ParseMetrics reads the Prometheus text exposition into a snapshot (counters summed per key).
func ParseMetrics(r io.Reader) Snapshot {
	out := Snapshot{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<16), 4<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		name, rest := line, ""
		labels := map[string]string{}
		if i := strings.IndexByte(line, '{'); i >= 0 {
			j := strings.LastIndexByte(line, '}')
			if j < i {
				continue
			}
			name, rest = line[:i], line[j+1:]
			for _, pair := range splitLabels(line[i+1 : j]) {
				if k, v, ok := strings.Cut(pair, "="); ok {
					labels[k] = strings.Trim(v, `"`)
				}
			}
		} else if k, v, ok := strings.Cut(line, " "); ok {
			name, rest = k, " "+v
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			continue
		}
		value, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			continue
		}
		for _, key := range metricKeys(name, labels) {
			out[key] += value
		}
	}
	return out
}

// splitLabels splits `a="1",b="x,y"` at the commas outside quotes.
func splitLabels(s string) []string {
	var out []string
	quoted, start := false, 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			i++
		case '"':
			quoted = !quoted
		case ',':
			if !quoted {
				out = append(out, s[start:i])
				start = i + 1
			}
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}

// Delta is after less before, the keys that moved only.
func Delta(before, after Snapshot) Snapshot {
	out := Snapshot{}
	for k, v := range after {
		if d := v - before[k]; d != 0 {
			out[k] = round6(d)
		}
	}
	return out
}

func round6(v float64) float64 {
	f, _ := strconv.ParseFloat(strconv.FormatFloat(v, 'f', 6, 64), 64)
	return f
}

// HitRatio is a delta's cache hits over its lookups, per layer and tier ("loki:l1_memory": 1).
func HitRatio(d Snapshot) map[string]float64 {
	out := map[string]float64{}
	for k, req := range d {
		prefix, ok := strings.CutSuffix(k, ":requests")
		if !ok || req <= 0 {
			continue
		}
		out[prefix] = round6(d[prefix+":hits"] / req)
	}
	return out
}

// LiveMeter scrapes each proxy's /metrics and records every snapshot.
type LiveMeter struct {
	Bases  map[string]string // "proxy" and "proxy@<variant>"
	Client *http.Client
	rec    *Recorder
}

// Measure scrapes the variant's proxy.
func (m *LiveMeter) Measure(caseID, variant, phase string) (Snapshot, bool) {
	base := m.Bases["proxy"]
	if variant != "" {
		base = m.Bases["proxy@"+variant]
	}
	if base == "" {
		return nil, false
	}
	resp, err := m.Client.Get(strings.TrimRight(base, "/") + "/metrics")
	if err != nil {
		return nil, false
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, false
	}
	s := ParseMetrics(io.LimitReader(resp.Body, 64<<20))
	if m.rec != nil {
		m.rec.addMetrics(caseID, phase, s)
	}
	return s, true
}

// repeatAttempt is the attempt number a repeat is recorded under (after the two comparison attempts).
const repeatAttempt = 9

// measureReuse asks the proxy the request again and judges the repeat against the first answer.
func measureReuse(p Probe, req Request, first Answer, firstCost Snapshot, src Source, meter Meter, facets []string, o Options) *Reuse {
	r := &Reuse{Verdict: reuseNotMeasured, Cold: firstCost}
	if meter == nil || firstCost == nil {
		r.Reason = "the proxy's metrics could not be read"
		return r
	}
	before, ok1 := meter.Measure(p.ID, p.Variant, "repeat_before")
	repeat := src.Ask("proxy", p.ID, repeatAttempt, req)
	after, ok2 := meter.Measure(p.ID, p.Variant, "repeat_after")
	if !ok1 || !ok2 || repeat.Err != "" {
		r.Reason = "the repeat or its metrics could not be read"
		return r
	}
	r.Repeat = Delta(before, after)
	var why []string
	if d := CompareWith(facets, first, repeat, o); len(d) > 0 {
		names := make([]string, 0, len(d))
		for _, x := range d {
			names = append(names, x.Facet)
		}
		why = append(why, "the repeat answers differently ("+strings.Join(names, ", ")+")")
	}
	own, background := attribute(firstCost, r.Repeat)
	if own > 0 {
		why = append(why, fmt.Sprintf("the repeat called VictoriaLogs %.0f time(s) on the endpoints of the first ask", own))
	}
	if background > 0 {
		// Counters are process-wide: a call to an endpoint the first ask never used (a health check, the stream
		// field inventory) is the proxy's background work, not this request's.
		r.Background = background
	}
	if len(why) > 0 {
		r.Verdict, r.Reason = reuseNotReused, strings.Join(why, "; ")
		return r
	}
	r.Verdict = reuseReused
	return r
}

// attribute splits a repeat's VictoriaLogs calls into the calls on endpoints the first ask used (the
// request's own) and the rest (background work the process-wide counters also count).
func attribute(cold, repeat Snapshot) (own, background float64) {
	for k, n := range repeat {
		endpoint, ok := strings.CutPrefix(k, "vl_calls:")
		if !ok || n <= 0 {
			continue
		}
		if cold["vl_calls:"+endpoint] > 0 {
			own += n
		} else {
			background += n
		}
	}
	return own, background
}

// ReuseSummary counts a run's reuse verdicts and totals the cold and repeat costs.
type ReuseSummary struct {
	Counts      map[string]int     `json:"counts"`
	NotReused   []string           `json:"not_reused"`
	ColdVLCalls float64            `json:"cold_vl_calls"`
	RepeatCalls float64            `json:"repeat_vl_calls"`
	ColdBytes   float64            `json:"cold_vl_bytes"`
	RepeatBytes float64            `json:"repeat_vl_bytes"`
	RepeatHits  map[string]float64 `json:"repeat_hit_ratio"`
	Background  float64            `json:"background_vl_calls"`
}

// SummarizeReuse totals the reuse of every case that measured one.
func SummarizeReuse(cases map[string]Result) ReuseSummary {
	s := ReuseSummary{Counts: map[string]int{}, NotReused: []string{}, RepeatHits: map[string]float64{}}
	repeat := Snapshot{}
	for id, r := range cases {
		if r.Reuse == nil {
			continue
		}
		s.Counts[r.Reuse.Verdict]++
		if r.Reuse.Verdict == reuseNotReused {
			s.NotReused = append(s.NotReused, id)
		}
		s.ColdVLCalls += r.Reuse.Cold["vl_calls"]
		s.ColdBytes += r.Reuse.Cold["vl_bytes"]
		s.Background += r.Reuse.Background
		for k, v := range r.Reuse.Repeat {
			repeat[k] += v
		}
	}
	s.RepeatCalls, s.RepeatBytes, s.RepeatHits = repeat["vl_calls"], repeat["vl_bytes"], HitRatio(repeat)
	sort.Strings(s.NotReused)
	return s
}
