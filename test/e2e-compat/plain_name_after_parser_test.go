//go:build e2e

package e2e_compat

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

// TestCompat_PlainNameAfterParserReadsStreamLabel: after a parser stage whose
// key is also a stream label's name, Loki renames the parsed label name_extracted
// and the plain name still reads the stream label (pkg/logql/log/labels.go
// getWithCategory, parser.go duplicateSuffix), so a filter, a grouping, an
// unwrap, label_format and line_format on the plain name see the stream value (keep and
// drop of the plain name in a log query act on the entry's labels, a separate gap).
// The fixture's stream has level="info" and its JSON and logfmt lines carry
// other levels. Compared name=value with Loki for json and logfmt, the
// filter operators, line_format and metric grouping, in the
// default and categorize-labels encodings, over one window and a range split
// into windows.
//
// conformance: semantics/plain-name-after-parser-reads-stream-label, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_PlainNameAfterParserReadsStreamLabel(t *testing.T) {
	app, end := ensureExtractedSuffixFixture(t)
	keys := fmt.Sprintf(`{app=%q,env="keys"}`, app)

	// The proxy learns the tenant's stream label names in the background and
	// translates with the plain semantics until it has them (up to a minute
	// after a new stream label appears): wait until it answers like Loki.
	warm := keys + ` | json | level="info"`
	for deadline := time.Now().Add(150 * time.Second); len(extractedLogEntries(t, proxyURL, warm, end, 5*time.Minute, false, 5)) != 5; {
		if time.Now().After(deadline) {
			t.Fatalf("proxy never learned the stream label names: %s", warm)
		}
	}

	logQueries := []struct {
		name, query string
		lines       int
		span        time.Duration
	}{
		{"json equals the parsed value", keys + ` | json | level="debug"`, 0, 5 * time.Minute},
		{"json equals the stream value", keys + ` | json | level="info"`, 5, 5 * time.Minute},
		{"json equals the stream value over windows", keys + ` | json | level="info"`, 5, 30 * time.Hour},
		{"json not equal", keys + ` | json | level!="info"`, 0, 5 * time.Minute},
		{"json not equal to the parsed value", keys + ` | json | level!="debug"`, 5, 5 * time.Minute},
		{"json regexp", keys + ` | json | level=~"debug|error"`, 0, 5 * time.Minute},
		{"json negated regexp", keys + ` | json | level!~"debug|error"`, 5, 5 * time.Minute},
		{"json existence", keys + ` | json | level!=""`, 5, 5 * time.Minute},
		{"json and with another label", keys + ` | json | level="info" and status="500"`, 1, 5 * time.Minute},
		{"json or", keys + ` | json | level="debug" or status="500"`, 1, 5 * time.Minute},
		{"json extraction list", keys + ` | json level | level="debug"`, 0, 5 * time.Minute},
		{"logfmt equals the parsed value", keys + ` | logfmt | level="warn"`, 0, 5 * time.Minute},
		{"logfmt equals the stream value", keys + ` | logfmt | level="info"`, 5, 5 * time.Minute},
		{"regexp capture", keys + ` | regexp "level=(?P<level>\\w+)" | level="warn"`, 0, 5 * time.Minute},
		{"line_format reads the stream value", keys + ` | json | line_format "{{.level}}-{{.msg}}"`, 4, 5 * time.Minute},
		{"label_format template of the plain name", keys + ` | json | label_format x="{{.level}}" | x="info"`, 5, 5 * time.Minute},
		{"label_format template", keys + ` | json | label_format x="{{.level}}-a" | x="info-a"`, 5, 5 * time.Minute},
		{"line_format template then a line filter", keys + ` | json | line_format "{{.level}}-{{.msg}}" |= "info"`, 4, 5 * time.Minute},
		{"renamed label beside the plain one", keys + ` | json | level_extracted="debug" and level="info"`, 1, 5 * time.Minute},
		{"a name that is no stream label", keys + ` | json | user="u1"`, 1, 5 * time.Minute},
	}
	for _, categorize := range []bool{false, true} {
		for _, q := range logQueries {
			name := q.name + map[bool]string{false: "/default", true: "/categorize-labels"}[categorize]
			t.Run(name, func(t *testing.T) {
				loki := extractedLogEntries(t, lokiURL, q.query, end, q.span, categorize, q.lines)
				if len(loki) != q.lines {
					t.Fatalf("Loki fixture: %d entries for %s, want %d", len(loki), q.query, q.lines)
				}
				proxy := extractedLogEntries(t, proxyURL, q.query, end, q.span, categorize, q.lines)
				if !reflect.DeepEqual(proxy, loki) {
					t.Errorf("%s\nproxy %+v\nloki  %+v", q.query, proxy, loki)
				}
			})
		}
	}

	for _, q := range []string{
		fmt.Sprintf(`sum by (level) (count_over_time(%s | logfmt | __error__="" [5m]))`, keys),
		fmt.Sprintf(`sum by (level) (count_over_time(%s | json | __error__="" [5m]))`, keys),
		fmt.Sprintf(`sum by (level, level_extracted) (count_over_time(%s | json | __error__="" [5m]))`, keys),
		fmt.Sprintf(`sum by (level) (count_over_time(%s | json | __error__="" | level="info" [5m]))`, keys),
		fmt.Sprintf(`sum by (level) (count_over_time(%s | logfmt | __error__="" | level!="warn" [5m]))`, keys),
		fmt.Sprintf(`sum(count_over_time(%s | json | __error__="" | level="info" [5m]))`, keys),
		fmt.Sprintf(`sum by (level) (sum_over_time(%s | json | __error__="" | unwrap status [5m]))`, keys),
		fmt.Sprintf(`sum without (level) (count_over_time(%s | json | __error__="" [5m]))`, keys),
		fmt.Sprintf(`sum by (level) (count_over_time(%s | json | __error__="" | keep level [5m]))`, keys),
		fmt.Sprintf(`count_over_time(%s | json | __error__="" | level="info" [5m])`, keys),
		fmt.Sprintf(`sum by (user) (count_over_time(%s | json | __error__="" [5m]))`, keys),
	} {
		t.Run(q, func(t *testing.T) {
			var loki map[string]string
			deadline := time.Now().Add(150 * time.Second)
			for {
				if loki = extractedSeries(t, lokiURL, q, end); len(loki) > 0 || time.Now().After(deadline) {
					break
				}
				time.Sleep(2 * time.Second)
			}
			if len(loki) == 0 {
				t.Fatalf("Loki answered no series for %s", q)
			}
			if proxy := extractedSeries(t, proxyURL, q, end); !reflect.DeepEqual(proxy, loki) {
				t.Errorf("%s\nproxy %v\nloki  %v", q, proxy, loki)
			}
		})
	}
}
