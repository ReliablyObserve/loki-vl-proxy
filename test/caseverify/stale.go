package main

import "fmt"

// A Loki that answers from its results cache can serve extents an earlier run stored before the data existed:
// the answer then lacks lines or samples that Loki's own count of the window (the data precheck) says it holds.
// Loki ignores no-cache request headers, and its step alignment removes a per-run offset of a window, so the
// request cannot be made unique per run without changing what the case asks. The answer says it instead:
// stats.cache.result.entriesFound is above zero when an extent was reused.
//
// An answer is judged stale only on all of the evidence: Loki's answer reports a reused extent, the data precheck
// proved Loki and VictoriaLogs hold the same lines (the comparison never runs otherwise), and every difference is
// on answer data where Loki holds a strict subset of what the proxy answers (each of Loki's entries and samples is
// in the proxy's, with the same text or value, and the proxy has more). Anything else stays a difference.

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

// lokiMissingOnly reports whether Loki's answer is a strict subset of the proxy's: both hold the same series, every
// entry or sample of Loki's is in the proxy's series, and the proxy holds more. A series only one side holds is
// a difference of series, never a missing extent. It returns what is missing, for the reason.
func lokiMissingOnly(loki, proxy Answer) (string, bool) {
	lp, pp := parse(loki), parse(proxy)
	if lp.facet(facetResultType, loki.Status) != pp.facet(facetResultType, proxy.Status) {
		return "", false
	}
	l, p := lp.entrySet(), pp.entrySet()
	if len(l) != len(p) {
		return "", false
	}
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
		}
		lokiN += len(set)
	}
	for _, set := range p {
		proxyN += len(set)
	}
	if proxyN <= lokiN {
		return "", false
	}
	return fmt.Sprintf("Loki holds %d of the proxy's %d entries or samples", lokiN, proxyN), true
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
