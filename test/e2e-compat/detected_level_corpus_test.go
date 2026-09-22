//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// The hand-written dlCases cover the shapes we reasoned about. This generates
// the combinations around them: every level-like key Loki reads, crossed with
// values that exercise its normalisation, in JSON, logfmt and plain form, plus
// the structural shapes where the two parsers can disagree.
//
// Every generated case goes in on the raw route, where VictoriaLogs stores the
// line byte for byte, so Loki and the proxy see identical input and any
// difference in the answer is the proxy's. The expected value is not written
// down: the comparison is against whatever live Loki returns, so the corpus
// cannot encode a wrong belief about Loki.
//
// conformance: severity-detected-level-derivation, severity/normalisation-table
func generatedDetectedLevelCases() []dlCase {
	keys := []string{"level", "LEVEL", "Level", "severity", "SEVERITY", "levelname", "severity_text", "lvl", "loglevel"}
	values := []string{
		"error", "ERROR", "Error", "err", "ERR",
		"warn", "WARN", "warning", "Warning", "wrn",
		"info", "INFO", "inf", "information",
		"debug", "dbg", "trace", "trc",
		"critical", "fatal", "notice", "alert", "emerg",
		"30", "", " warn ", "warn\t", "wärn", "x",
	}
	var cases []dlCase
	add := func(name, line string) {
		cases = append(cases, dlCase{
			name:  "gen-" + name,
			route: dlRouteRaw,
			line:  line,
			exact: true,
		})
	}
	for _, key := range keys {
		for _, value := range values {
			slug := fmt.Sprintf("%s-%s", strings.ToLower(key), levelSlug(value))
			body, err := json.Marshal(map[string]string{key: value, "msg": "generated"})
			if err != nil {
				continue
			}
			add("json-"+slug, string(body))
			add("logfmt-"+slug, fmt.Sprintf("%s=%q msg=generated", key, value))
		}
	}
	// Shapes where Loki's decoder and VictoriaLogs' unpacking can disagree, or
	// where the level is not where the parser looks first.
	for name, line := range map[string]string{
		"json-nested-level":        `{"a":{"level":"error"},"msg":"nested"}`,
		"json-array-level":         `{"level":["error"],"msg":"array"}`,
		"json-number-level":        `{"level":40,"msg":"number"}`,
		"json-null-level":          `{"level":null,"msg":"null"}`,
		"json-repeated-level":      `{"level":"info","level":"error","msg":"repeated"}`,
		"json-leading-space":       ` {"level":"error","msg":"space"}`,
		"json-trailing-bytes":      `{"level":"error","msg":"trailing"} and more`,
		"json-truncated":           `{"level":"error","msg":"truncated`,
		"json-escaped-key":         `{"level":"error","msg":"escaped"}`,
		"json-padded-key":          `{" level ":"error","msg":"padded"}`,
		"json-unicode-value":       `{"level":"érror","msg":"unicode"}`,
		"json-deep-only":           `{"a":{"b":{"level":"error"}},"msg":"deep"}`,
		"logfmt-tab-separated":     "n=1\tlevel=warn msg=tab",
		"logfmt-repeated-key":      "level=info level=error msg=repeated",
		"logfmt-quoted-value":      `level="error" msg=quoted`,
		"logfmt-backtick-value":    "level=`error` msg=backtick",
		"logfmt-no-space":          "level=errormsg=joined",
		"logfmt-trailing-equals":   "level= msg=empty",
		"logfmt-bare-key":          "level msg=bare",
		"plain-level-word-only":    "error",
		"plain-level-in-url":       "GET /api/v1/error?x=1 200",
		"plain-level-substring":    "the terror of the deep",
		"plain-two-levels":         "info: something warn: other",
		"plain-bracket-lowercase":  "[warn] something happened",
		"plain-json-like":          `level="error" but not really logfmt`,
		"plain-long-line":          strings.Repeat("filler ", 300) + "level=error",
		"plain-leading-newline":    "\nlevel=error after newline",
		"plain-only-whitespace":    "   ",
		"plain-empty":              "",
		"plain-control-characters": "level=\x01error",
	} {
		add(name, line)
	}
	return cases
}

// levelSlug makes a case name from a value that may be empty or padded.
func levelSlug(value string) string {
	switch {
	case value == "":
		return "empty"
	case strings.TrimSpace(value) != value:
		return "padded-" + strings.TrimSpace(value)
	}
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			return r
		}
		if r >= 'A' && r <= 'Z' {
			return r + 32
		}
		return '-'
	}, value)
}

// TestCompat_DetectedLevelCorpus compares the generated corpus line by line
// against live Loki. Every line goes into both backends through the Loki push
// API with message parsing disabled, so VictoriaLogs stores exactly the bytes
// Loki ingested and the derivation is the only variable.
//
// conformance: severity-detected-level-derivation, severity/normalisation-table
func TestCompat_DetectedLevelCorpus(t *testing.T) {
	f := ensureDetectedLevelFixture(t)
	generated := generatedDetectedLevelCases()
	for _, categorized := range []bool{false, true} {
		name := "default"
		var headers map[string]string
		if categorized {
			name = "categorize-labels"
			headers = map[string]string{"X-Loki-Response-Encoding-Flags": "categorize-labels"}
		}
		t.Run(name, func(t *testing.T) {
			loki := dlQueryRange(t, lokiURL, f.corpusSelector(), f, headers)
			proxy := dlQueryRange(t, proxyURL, f.corpusSelector(), f, headers)
			lokiEntries, proxyEntries := dlEntries(t, loki), dlEntries(t, proxy)
			if len(lokiEntries) < len(generated) {
				t.Fatalf("Loki returned %d of %d corpus entries; the fixture did not ingest",
					len(lokiEntries), len(generated))
			}
			agree, differ := 0, map[string][3]string{}
			for stamp, le := range lokiEntries {
				pe, ok := proxyEntries[stamp]
				if !ok {
					t.Fatalf("proxy lacks corpus entry %s", stamp)
				}
				c := f.byTS[stamp]
				lokiLevel, proxyLevel := dlLevel(le, categorized), dlLevel(pe, categorized)
				if lokiLevel == proxyLevel {
					agree++
					continue
				}
				differ[c.name] = [3]string{c.line, lokiLevel, proxyLevel}
			}
			t.Logf("corpus: %d lines, %d agree with Loki, %d differ", len(lokiEntries), agree, len(differ))
			for name, detail := range differ {
				t.Errorf("%s: line %q — Loki %q, proxy %q", name, detail[0], detail[1], detail[2])
			}
		})
	}
}
