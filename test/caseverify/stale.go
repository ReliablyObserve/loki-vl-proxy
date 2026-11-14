package main

import (
	"fmt"
	"strings"
)

// A Loki that answers from its results cache can serve an extent an earlier run stored before the data existed:
// the answer then lacks a block of lines or samples that Loki's own count of the window says it holds. The request
// cannot be made unique per run without changing what the case asks: Loki ignores no-cache request headers and
// reuses an extent by the overlap of time ranges, so a sub-second offset of the window still overlaps (and a metric
// query's window is aligned to its step besides); only another query text or another step would miss the cache, and
// those are other questions. The answer says it instead: stats.cache.result.entriesFound is above zero when an
// extent was reused.
//
// An answer is judged stale only on all of the evidence: Loki's answer reports a reused extent; both answers hold
// the same series; every entry or sample of Loki's is in the proxy's; the entries the proxy holds beyond them form
// one contiguous block of time with none of Loki's own entries inside it (a block missing at an edge or in the
// middle, which is what a stale extent leaves; extra lines interleaved with Loki's are the proxy's difference,
// never a cache's). The data precheck has already proven the selector's lines equal on Loki and VictoriaLogs, so
// the missing block is not data Loki lacks; it says nothing about the filtered answer's size. A stale answer is a
// non-verdict: the case stays unproven until a run asks a window no earlier run did.

// lokiCacheHit reports whether Loki's answer was assembled from a reused results-cache extent.
func lokiCacheHit(a Answer) bool {
	if !a.OK() {
		return false
	}
	v := parse(a)
	found, _ := field(v.doc, "data", "stats", "cache", "result", "entriesFound").(float64)
	return found > 0
}

// staleFacets are the facets that read the data of an answer: a difference on any other facet (the status, an
// error, the result type) is never explained by a missing extent.
var staleFacets = map[string]bool{facetLabels: true, facetCount: true, facetValues: true, facetEntries: true, facetData: true,
	facetResult: true, facetBody: true, facetTimestamps: true, facetRanked: true}

// entrySet is every entry or sample of an answer's series, by series: a log entry is "timestamp line", a
// sample "timestamp value".
func (p parsed) entrySet() map[string]map[string]bool {
	out := map[string]map[string]bool{}
	for _, item := range p.result() {
		values, _ := field(item, "values").([]any)
		set := map[string]bool{}
		for _, v := range values {
			if e, _ := v.([]any); len(e) >= 2 {
				set[tsText(e[0])+"\x00"+canonical(e[1])] = true
			}
		}
		if value, _ := field(item, "value").([]any); len(value) == 2 {
			set[tsText(value[0])+"\x00"+canonical(value[1])] = true
		}
		out[canonical(labelsOf(item))] = set
	}
	return out
}

// lokiMissingOnly reports whether Loki's answer lacks one block of the proxy's: both hold the same series, every
// entry or sample of Loki's is in the proxy's series, the proxy holds more, and the entries only the proxy holds
// lie in one block of time that none of Loki's entries is in. A series only one side holds is a difference of
// series, never a missing extent. It returns what is missing, for the reason.
func lokiMissingOnly(loki, proxy Answer) (string, bool) {
	lp, pp := parse(loki), parse(proxy)
	if lp.facet(facetResultType, loki.Status) != pp.facet(facetResultType, proxy.Status) {
		return "", false
	}
	l, p := lp.entrySet(), pp.entrySet()
	if len(l) != len(p) {
		return "", false
	}
	// Loki's own timestamps, and the timestamps of what only the proxy holds.
	var held, missing []string
	lokiN, proxyN := 0, 0
	for key, set := range l {
		other, ok := p[key]
		if !ok {
			return "", false
		}
		for e := range set {
			if !other[e] {
				return "", false
			}
			held = append(held, entryTime(e))
		}
		for e := range other {
			if !set[e] {
				missing = append(missing, entryTime(e))
			}
		}
		lokiN += len(set)
		proxyN += len(other)
	}
	if len(missing) == 0 || proxyN <= lokiN {
		return "", false
	}
	low, high := missing[0], missing[0]
	for _, ts := range missing {
		if tsLess(ts, low) {
			low = ts
		}
		if tsLess(high, ts) {
			high = ts
		}
	}
	for _, ts := range held {
		if !tsLess(ts, low) && !tsLess(high, ts) {
			return "", false
		}
	}
	return fmt.Sprintf("Loki holds %d of the proxy's %d entries or samples, the %d it lacks all between %s and %s with none of its own among them", lokiN, proxyN, len(missing), low, high), true
}

// entryTime is the timestamp of an entrySet element ("timestamp\x00value").
func entryTime(e string) string {
	ts, _, _ := strings.Cut(e, "\x00")
	return ts
}

// lokiCachedStale is why an answer that differs is Loki's stale cache and not the proxy: "" when it is not.
func lokiCachedStale(diffs []Difference, loki, proxy Answer) string {
	if len(diffs) == 0 || !lokiCacheHit(loki) {
		return ""
	}
	for _, d := range diffs {
		if !staleFacets[d.Facet] {
			return ""
		}
	}
	missing, ok := lokiMissingOnly(loki, proxy)
	if !ok {
		return ""
	}
	return "Loki answered from reused results-cache extents (stats.cache.result.entriesFound above zero): " + missing +
		", on data its own count proved equal to VictoriaLogs'; rerun with a window no earlier run asked for"
}
