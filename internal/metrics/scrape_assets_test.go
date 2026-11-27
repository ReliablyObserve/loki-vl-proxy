package metrics

import (
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// The chart's ServiceMonitor sets honorLabels, so a label the proxy exposes replaces the scrape target's label of
// the same name. "endpoint" is the only one meant to; any of these would break the job= and instance selectors of
// the shipped alerts and dashboard.
var scrapeTargetLabels = []string{"job", "instance", "namespace", "pod", "service", "container"}

// linuxExposition renders /metrics as a Linux replica writes it: the system families, pressure included, are
// written only there.
func linuxExposition(t *testing.T) string {
	t.Helper()
	old := systemGOOS
	systemGOOS = "linux"
	t.Cleanup(func() { systemGOOS = old })
	m := NewMetricsWithOptions(256, 256, true)
	recordEveryHistogram(m, 0)
	return metricsText(m)
}

// conformance: observability-metrics-histograms-and-labels
func TestNoMetricLabelTakesOverATargetLabel(t *testing.T) {
	for name, labels := range exposedLabels(linuxExposition(t)) {
		for label := range labels {
			for _, reserved := range scrapeTargetLabels {
				if label == reserved {
					t.Errorf("%s exposes the scrape target label %q", name, label)
				}
			}
		}
	}
	// Families other packages append to /metrics, found by their format strings.
	written := regexp.MustCompile(`(?:loki_vl_proxy_|process_|go_)[a-zA-Z0-9_%]*\{(?:[^}\n]*,)?(` + strings.Join(scrapeTargetLabels, "|") + `)=`)
	err := filepath.WalkDir("..", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || strings.Contains(path, "testutil") {
			return err
		}
		source, err := os.ReadFile(path)
		if err == nil {
			if found := written.FindString(string(source)); found != "" {
				t.Errorf("%s writes a scrape target label: %s", path, found)
			}
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

var (
	seriesLine = regexp.MustCompile(`^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})? `)
	labelPair  = regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"`)
	selector   = regexp.MustCompile(`\b((?:loki_vl_proxy|process|go)_[a-zA-Z0-9_]+)\{((?:[^{}"]|"(?:[^"\\]|\\.)*")*)\}`)
	matcher    = regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)\s*(=~|!=|!~|=)\s*"((?:[^"\\]|\\.)*)"`)
	groupBy    = regexp.MustCompile(`\bby\s*\(([^)]*)\)`)
	metricName = regexp.MustCompile(`\bloki_vl_proxy_[a-z0-9_]+`)
	helmAction = regexp.MustCompile(`\{\{[^{}]*\}\}`)
)

// exposedLabels maps every series name of an exposition to its label names and their values.
func exposedLabels(body string) map[string]map[string]map[string]bool {
	out := map[string]map[string]map[string]bool{}
	for _, line := range strings.Split(body, "\n") {
		series := seriesLine.FindStringSubmatch(line)
		if series == nil {
			continue
		}
		if out[series[1]] == nil {
			out[series[1]] = map[string]map[string]bool{}
		}
		for _, pair := range labelPair.FindAllStringSubmatch(series[2], -1) {
			if out[series[1]][pair[1]] == nil {
				out[series[1]][pair[1]] = map[string]bool{}
			}
			out[series[1]][pair[1]][pair[2]] = true
		}
	}
	return out
}

func shippedExpressions(t *testing.T) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	raw, err := os.ReadFile("../../dashboard/loki-vl-proxy.json")
	if err != nil {
		t.Fatal(err)
	}
	var dashboard any
	if err := json.Unmarshal(raw, &dashboard); err != nil {
		t.Fatal(err)
	}
	var walk func(any)
	walk = func(v any) {
		switch node := v.(type) {
		case map[string]any:
			if expr, ok := node["expr"].(string); ok {
				out["dashboard"] = append(out["dashboard"], expr)
			}
			for _, child := range node {
				walk(child)
			}
		case []any:
			for _, child := range node {
				walk(child)
			}
		}
	}
	walk(dashboard)
	for _, file := range []string{"loki-vl-proxy-prometheusrule.yaml", "loki-vl-proxy-alerting-rules.yaml"} {
		raw, err := os.ReadFile("../../alerting/" + file)
		if err != nil {
			t.Fatal(err)
		}
		// An expression ends where the next key of its rule starts.
		for _, block := range regexp.MustCompile(`(?s)\bexpr:(.*?)\n\s+(?:for|labels|annotations):`).FindAllStringSubmatch(string(raw), -1) {
			out[file] = append(out[file], helmAction.ReplaceAllString(block[1], "x"))
		}
	}
	return out
}

// Every label the shipped dashboard and alerts select or group a proxy metric by must be one the proxy exposes on
// it, and a fixed value of a fixed set (the pressure windows) must be one it writes.
// conformance: observability-metrics-histograms-and-labels
func TestShippedQueriesMatchTheExposition(t *testing.T) {
	exposed := exposedLabels(linuxExposition(t))
	selectors, endpointGroups := 0, 0
	for file, exprs := range shippedExpressions(t) {
		if len(exprs) == 0 {
			t.Fatalf("%s: no expressions found", file)
		}
		for _, expr := range exprs {
			for _, sel := range selector.FindAllStringSubmatch(expr, -1) {
				labels, ok := exposed[sel[1]]
				if !ok {
					continue // a family another package or a configured feature writes
				}
				for _, match := range matcher.FindAllStringSubmatch(sel[2], -1) {
					name, op, value := match[1], match[2], match[3]
					if name == "job" || name == "instance" {
						continue
					}
					selectors++
					if labels[name] == nil {
						t.Errorf("%s: %s has no label %q: %s", file, sel[1], name, expr)
					} else if name == "window" && op == "=" && !labels[name][value] {
						t.Errorf("%s: %s is never window=%q: %s", file, sel[1], value, expr)
					}
				}
			}
			for _, by := range groupBy.FindAllStringSubmatch(expr, -1) {
				if !slices.Contains(strings.Split(strings.ReplaceAll(by[1], " ", ""), ","), "endpoint") {
					continue
				}
				for _, name := range metricName.FindAllString(expr, -1) {
					if labels, ok := exposed[name]; ok {
						endpointGroups++
						if labels["endpoint"] == nil {
							t.Errorf("%s: grouped by endpoint, which %s lacks: %s", file, name, expr)
						}
					}
				}
			}
		}
	}
	if selectors < 10 || endpointGroups < 3 {
		t.Fatalf("checked %d selectors and %d endpoint groupings: the expressions are not being read", selectors, endpointGroups)
	}
}
