// Package config holds the registry behind the generated configuration and
// limits reference documentation.
//
// The flags themselves stay declared once, in cmd/proxy/main.go: ParseFlags
// reads them from there, so a flag cannot drift from its documentation. The
// tables in this file add what a flag declaration cannot carry — which Helm
// value sets it, what it bounds, the error a client gets when it is hit, the
// metric and alert that show it, whether a tenant can override it, and how to
// size it up or down.
//
// Regenerate the documents with `go run ./cmd/configdoc`; TestGeneratedDocsAreUpToDate
// fails when they drift.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// Flag is one flag as declared by the binary.
type Flag struct {
	Name    string
	Type    string // Int, Int64, Duration, Bool, String, Float64
	Default string // resolved value, not the Go expression
	Usage   string
}

// Limit is the operator-facing description of a bound on work.
type Limit struct {
	Flag       string // flag name without the leading dash
	Unit       string // bytes, rows, series, buckets, tenants, queries, duration
	Bounds     string // what the limit bounds
	Error      string // what a client receives when the limit is hit
	Metric     string // metric that counts or exposes hits, or "none"
	Alert      string // alert in alerting/ that fires for it, or "none"
	LokiParity string // Loki limit this mirrors, or ""
	PerTenant  bool   // settable per tenant through -tenant-limits
	Sizing     string // how to size it up or down, with the tradeoff
}

// Category groups flags in the generated reference.
type Category struct {
	Name     string
	Prefixes []string
}

// categories are matched in order; the first matching prefix wins.
var categories = []Category{
	{"limits", []string{"http-conn-", "max-", "backend-max-", "backend-heavy-", "manual-range-metric-row-limit", "ordered-json-metric-max-bytes", "binary-metric-", "multi-tenant-max-", "detected-fields-max-", "patterns-max-", "patterns-second-pass-", "drilldown-max-", "stats-query-range-concurrency", "stats-query-range-inter-query-delay-ms", "default-max-query-length", "rate-limit-", "http-max-"}},
	{"timeouts", []string{"backend-timeout", "backend-version-check-timeout", "drilldown-scan-timeout", "query-range-window-timeout", "http-read", "http-write", "http-idle", "shutdown-"}},
	{"cache", []string{"cache-", "compat-cache-", "disk-cache-", "labels-cache-ttl", "query-range-", "recent-tail-", "label-values-"}},
	{"tenancy", []string{"tenant", "require-tenant-header", "allow-global-tenant", "forward-tenant-header", "auth"}},
	{"compatibility", []string{"label-style", "metadata-", "emit-structured-metadata", "extra-label-fields", "stream-fields", "field-mapping", "derived-fields", "patterns", "translate-otel", "backend-min-version", "backend-allow-unsupported-version", "backend-version-strict", "tail", "drilldown-", "align-queries-with-step", "detected-level-body-scan"}},
	{"security", []string{"server.", "tls-", "client-ca", "cb-", "coalescer-disabled", "debug-log-raw-queries", "forward-authorization", "forward-cookies", "forward-headers"}},
	{"observability", []string{"metrics-", "metrics.", "otlp-", "otel-", "log-", "enable-query-analytics", "systemMetrics", "host-proc-root", "proc-root", "deployment-environment"}},
	{"peer cache", []string{"peer-"}},
	{"cold storage", []string{"cold-backend", "cold-"}},
	{"server", []string{"listen", "admin-listen", "backend", "ruler-backend", "alerts-backend", "response-compression", "response-gzip", "enable-gzip", "stream-response", "warmup-max-jitter", "go-"}},
}

var flagDefRe = regexp.MustCompile(`fs\.(\w+)\("([a-zA-Z][a-zA-Z0-9._-]*)",\s*([^,]+),`)

// ParseFlags reads the flag declarations of cmd/proxy/main.go and resolves
// each default through the constants of internal/proxy.
func ParseFlags(mainGoPath string) ([]Flag, error) {
	data, err := os.ReadFile(mainGoPath)
	if err != nil {
		return nil, err
	}
	consts, err := proxyConstants(filepath.Join(filepath.Dir(mainGoPath), "..", ".."))
	if err != nil {
		return nil, err
	}
	source := string(data)
	var flags []Flag
	for _, match := range flagDefRe.FindAllStringSubmatchIndex(source, -1) {
		kind := source[match[2]:match[3]]
		name := source[match[4]:match[5]]
		def := strings.TrimSpace(source[match[6]:match[7]])
		usage, ok := usageAfter(source[match[1]:])
		if !ok {
			return nil, fmt.Errorf("flag %q: cannot read its usage string", name)
		}
		flags = append(flags, Flag{Name: name, Type: kind, Default: ResolveDefault(def, consts), Usage: usage})
	}
	sort.Slice(flags, func(i, j int) bool { return flags[i].Name < flags[j].Name })
	return flags, nil
}

// usageAfter returns the first Go string literal of a flag declaration.
func usageAfter(rest string) (string, bool) {
	start := strings.Index(rest, `"`)
	if start < 0 {
		return "", false
	}
	var out strings.Builder
	escaped := false
	for i := start + 1; i < len(rest); i++ {
		c := rest[i]
		switch {
		case escaped:
			if c == 'n' {
				out.WriteByte(' ')
			} else {
				out.WriteByte(c)
			}
			escaped = false
		case c == '\\':
			escaped = true
		case c == '"':
			// A usage string may be split across adjacent literals ("a" + "b").
			tail := strings.TrimLeft(rest[i+1:], " \t\n")
			if strings.HasPrefix(tail, "+") {
				tail = strings.TrimLeft(tail[1:], " \t\n")
				if strings.HasPrefix(tail, `"`) {
					i = len(rest) - len(tail)
					continue
				}
			}
			return out.String(), true
		default:
			out.WriteByte(c)
		}
	}
	return "", false
}

// CategoryOf returns the category a flag belongs to.
func CategoryOf(name string) string {
	for _, category := range categories {
		for _, prefix := range category.Prefixes {
			if strings.HasPrefix(name, prefix) {
				return category.Name
			}
		}
	}
	return "other"
}

// HelmKeyOf returns the Helm value that sets a flag.
func HelmKeyOf(name string) string {
	switch name {
	case "peer-self", "peer-discovery", "peer-dns", "peer-static", "peer-srv", "peer-http-url":
		return "peerCache.* (chart-managed)"
	case "disk-cache-path":
		return "persistence.* (chart-managed)"
	}
	return "extraArgs." + name
}

// LimitByFlag indexes the limit table.
func LimitByFlag() map[string]Limit {
	out := make(map[string]Limit, len(Limits))
	for _, limit := range Limits {
		out[limit.Flag] = limit
	}
	return out
}
