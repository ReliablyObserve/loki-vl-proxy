package main

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/report"
	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/workload"
)

func allURLs() targetURLs {
	return targetURLs{
		loki: "http://loki", proxy: "http://proxy", proxyNoCache: "http://nocache", proxyCoalescer: "http://coalescer",
		proxyPartial: "http://partial", vlDirect: "http://vl",
	}
}

func names(targets []benchTarget) string {
	var out []string
	for _, t := range targets {
		out = append(out, t.name)
	}
	return strings.Join(out, ",")
}

func TestTimedTargetsPerCacheMode(t *testing.T) {
	warm := timedTargets(cacheModeWarm, allURLs())
	if got := names(warm); got != "loki,proxy,proxy_partial,vl_direct" {
		t.Fatalf("warm targets %s", got)
	}
	for _, tgt := range warm {
		if !tgt.warmup {
			t.Errorf("warm %s must get the same warm-up as Loki", tgt.name)
		}
	}
	cold := timedTargets(cacheModeCold, allURLs())
	if got := names(cold); got != "loki,proxy_coalescer,proxy_nocache,vl_direct" {
		t.Fatalf("cold targets %s", got)
	}
	for _, tgt := range cold {
		if tgt.warmup {
			t.Errorf("cold %s must not warm up", tgt.name)
		}
	}
	u := allURLs()
	u.proxyCoalescer, u.skipVLDirect, u.skipProxyNoCache = "", true, true
	if got := names(timedTargets(cacheModeCold, u)); got != "loki" {
		t.Fatalf("skipped and unset targets still timed: %s", got)
	}
}

func TestEveryTimedProxyTargetIsVerified(t *testing.T) {
	for _, mode := range []string{cacheModeWarm, cacheModeCold} {
		targets := timedTargets(mode, allURLs())
		verified := map[string]string{}
		for _, v := range verifyTargets(targets) {
			verified[v.Name] = v.URL
		}
		for _, tgt := range targets {
			if strings.HasPrefix(tgt.name, "proxy") && verified[tgt.name] != tgt.url {
				t.Errorf("%s: timed proxy target %s is not verified against Loki", mode, tgt.name)
			}
			if !strings.HasPrefix(tgt.name, "proxy") && verified[tgt.name] != "" {
				t.Errorf("%s: %s is not a proxy target", mode, tgt.name)
			}
		}
	}
}

func TestNotPublishable(t *testing.T) {
	ok := runSettings{dataEnd: "auto", waitIngested: time.Minute, verifyStrict: true, targets: timedTargets(cacheModeWarm, allURLs())}
	if r := notPublishable(ok); len(r) != 0 {
		t.Fatalf("a verified, pinned run is publishable, got %v", r)
	}
	skip := ok
	skip.skipLoki = true
	r := notPublishable(skip)
	if len(r) != 1 || !strings.Contains(r[0], "--skip-loki") {
		t.Fatalf("--skip-loki reasons %v", r)
	}
	banner := notPublishableBanner(r)
	if !strings.Contains(banner, "NOT PUBLISHABLE") || !strings.Contains(banner, "--skip-loki") {
		t.Fatalf("banner:\n%s", banner)
	}
	loose := runSettings{targets: nil}
	if r := notPublishable(loose); len(r) != 4 {
		t.Fatalf("unverified, unpinned run without proxies: %v", r)
	}
}

func TestSkipLokiReportIsMarked(t *testing.T) {
	dir := t.TempDir()
	meta := report.Meta{CacheMode: cacheModeWarm, NotPublishable: notPublishable(runSettings{dataEnd: "auto", waitIngested: time.Minute, verifyStrict: true, skipLoki: true, targets: timedTargets(cacheModeWarm, allURLs())})}
	jsonPath, mdPath := writeOutputs(dir, time.Date(2026, 9, 15, 1, 2, 3, 0, time.UTC), meta, []report.RunRecord{{Target: "proxy", WorkloadName: "small", Concurrency: 10}})
	if !strings.HasSuffix(jsonPath, "-NOT-PUBLISHABLE.json") || !strings.HasSuffix(mdPath, "-NOT-PUBLISHABLE.md") {
		t.Fatalf("paths %s %s", jsonPath, mdPath)
	}
	md, err := os.ReadFile(mdPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(md), "NOT PUBLISHABLE") || !strings.Contains(string(md), "--skip-loki") {
		t.Fatalf("markdown not marked:\n%s", md)
	}
	js, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(js), `"Publishable": false`) || !strings.Contains(string(js), "--skip-loki") {
		t.Fatalf("json not marked:\n%s", js)
	}
	if p, _ := outputPaths(dir, time.Unix(0, 0), report.Meta{}); filepath.Base(p) != "bench-"+time.Unix(0, 0).Format("2006-01-02T15-04-05")+".json" {
		t.Fatalf("publishable path %s", p)
	}
}

const lokiConfigSnippet = `server:
  http_listen_port: 3100
query_range:
  align_queries_with_step: true
  results_cache:
    cache:
      default_validity: 1h0m0s
  cache_results: %v
  max_retries: 5
  cache_index_stats_results: %v
  index_stats_results_cache:
    cache:
      enabled: true
  cache_volume_results: %v
  cache_instant_metric_results: false
  cache_series_results: %v
  cache_label_results: %v
ruler:
  cache_results: true
`

func lokiConfig(on bool) string {
	return strings.ReplaceAll(lokiConfigSnippet, "%v", map[bool]string{true: "true", false: "false"}[on])
}

func TestLokiCacheModeCheck(t *testing.T) {
	warmFlags, err := parseLokiCacheFlags(strings.NewReader(lokiConfig(true)))
	if err != nil {
		t.Fatal(err)
	}
	coldFlags, _ := parseLokiCacheFlags(strings.NewReader(lokiConfig(false)))
	if len(coldFlags) != 7 || coldFlags["cache_results"] || !warmFlags["cache_label_results"] {
		t.Fatalf("parsed %v / %v", warmFlags, coldFlags)
	}
	if err := checkLokiCacheMode(warmFlags, cacheModeWarm); err != nil {
		t.Fatal(err)
	}
	if err := checkLokiCacheMode(coldFlags, cacheModeCold); err != nil {
		t.Fatal(err)
	}
	if err := checkLokiCacheMode(coldFlags, cacheModeWarm); err == nil {
		t.Fatal("warm mode against a cache-less Loki must fail")
	}
	if err := checkLokiCacheMode(warmFlags, cacheModeCold); err == nil || !strings.Contains(err.Error(), "cache_results") {
		t.Fatalf("cold mode against a cached Loki: %v", err)
	}
	labelsOff := map[string]bool{}
	for k, v := range warmFlags {
		labelsOff[k] = v
	}
	labelsOff["cache_label_results"] = false
	if err := checkLokiCacheMode(labelsOff, cacheModeWarm); err == nil || !strings.Contains(err.Error(), "cache_label_results") {
		t.Fatalf("warm mode must confirm every results cache Loki serves from: %v", err)
	}
	partial := map[string]bool{"cache_results": false}
	if err := checkLokiCacheMode(partial, cacheModeCold); err == nil {
		t.Fatal("cold mode must confirm every results cache switch")
	}
}

func TestCacheModeExclusions(t *testing.T) {
	ref := time.Date(2026, 9, 14, 19, 47, 0, 0, time.UTC)
	names := []string{"small", "heavy", "long_range", "compute", "unindexed_scan", "high_cardinality", "machinery"}
	excluded := func(mode string) map[string]string {
		out := map[string]string{}
		for _, w := range withExclusions(workload.ByName(names, ref), func(q workload.Query) string { return cacheModeExclusion(q, mode) }) {
			for _, q := range w.Queries {
				out[w.Name+"/"+q.Name] = q.Excluded
			}
		}
		return out
	}
	warm, cold := excluded(cacheModeWarm), excluded(cacheModeCold)
	cases := []struct {
		query      string
		warm, cold bool
	}{
		{"small/query_range_simple_1m", true, false}, // log lines
		{"heavy/json_line_format", true, false},      // log lines with a pipeline
		{"heavy/regex_filter", true, false},          // "[0-9]" is not a range
		{"small/detected_fields_small", true, false}, // not cached by Loki
		{"heavy/patterns_prod", true, false},         // not cached by Loki
		{"small/labels", false, true},                // always-on proxy cache
		{"small/label_values_app", false, true},      // always-on proxy cache
		{"small/series", false, false},               // cached on both / on neither
		{"heavy/metric_rate_by_app", false, false},   // metric range query
		{"small/query_instant_rate", false, false},   // instant metric query
		{"small/index_stats", true, true},            // excluded in every mode
		{"long_range/count_by_detected_level_48h", false, false},
	}
	for _, tc := range cases {
		if got := warm[tc.query] != ""; got != tc.warm {
			t.Errorf("warm %s excluded=%v (%q)", tc.query, got, warm[tc.query])
		}
		if got := cold[tc.query] != ""; got != tc.cold {
			t.Errorf("cold %s excluded=%v (%q)", tc.query, got, cold[tc.query])
		}
	}
	// Every query is compared in at least one mode unless excluded everywhere.
	for name, reason := range warm {
		if reason != "" && cold[name] != "" && !strings.Contains(reason, "not comparable") {
			t.Errorf("%s is compared in neither mode: %q / %q", name, reason, cold[name])
		}
	}
}

func TestUniqueWindowsExclusion(t *testing.T) {
	end := time.Date(2026, 9, 14, 19, 0, 0, 0, time.UTC)
	start := end.Add(-96 * time.Hour)
	q := func(window, step time.Duration, query string) workload.Query {
		return workload.Query{Params: url.Values{
			"query": {query},
			"start": {strconv64(end.Add(-window))}, "end": {strconv64(end)}, "step": {strconv64s(step)},
		}}
	}
	long := q(72*time.Hour, time.Hour, `sum(bytes_rate({a="b"}[1h]))`)
	if r := uniqueWindowsExclusion(long, start, 50); !strings.Contains(r, "distinct whole-step windows") {
		t.Fatalf("72h window at 1h steps in 96h of data must be excluded at 50 clients: %q", r)
	}
	if r := uniqueWindowsExclusion(long, start, 10); r != "" {
		t.Fatalf("enough windows for 10 clients: %q", r)
	}
	short := q(15*time.Minute, time.Minute, `sum(rate({a="b"}[1m]))`)
	if r := uniqueWindowsExclusion(short, start, 500); r != "" {
		t.Fatalf("short window: %q", r)
	}
	if r := notPublishable(runSettings{dataEnd: "x", waitIngested: 1, verifyStrict: true, targets: timedTargets(cacheModeCold, allURLs()), uniqueWindows: true}); len(r) != 1 || !strings.Contains(r[0], "--unique-windows") {
		t.Fatalf("unique windows without a data start: %v", r)
	}
}

func strconv64(t time.Time) string      { return fmt.Sprint(t.UnixNano()) }
func strconv64s(d time.Duration) string { return fmt.Sprint(int64(d / time.Second)) }
