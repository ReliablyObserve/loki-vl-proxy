package proxy

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// extractedPushdownFixture is a JSON stream in which the body's level either
// collides with the stream label (Loki exposes the parsed value as
// level_extracted), has no stream label to collide with (the parsed value is
// level), or is missing; one line in eight is unparseable and one is plain text.
func extractedPushdownFixture(s0 time.Time) []pushdownRow {
	var rows []pushdownRow
	for i := 0; i < 120; i++ {
		ts := s0.Add(time.Duration(i) * 5 * time.Second)
		row := pushdownRow{ts: ts, stream: map[string]string{"app": "api"}}
		switch i % 8 {
		case 0: // collision: level_extracted=error
			row.stream["level"] = "warn"
			row.msg = `{"level":"error","pipeline":"logs/loki","n":` + strconv.Itoa(i) + `}`
		case 1: // collision, same value on both sides
			row.stream["level"] = "info"
			row.msg = `{"level":"info","pipeline":"logs/loki"}`
		case 2: // no stream label: the body's level stays level
			row.msg = `{"level":"debug","pipeline":"traces/otlp"}`
		case 3: // the literal key level_extracted, no collision
			row.msg = `{"level_extracted":"literal","pipeline":"logs/loki"}`
		case 4: // stream label, body without the key
			row.stream["level"] = "error"
			row.msg = `{"pipeline":"logs/loki"}`
		case 5: // unparseable, no keys before the error
			row.stream["level"] = "warn"
			row.msg = `{"msg": "truncated ` + strconv.Itoa(i)
		case 6:
			row.stream["level"] = "info"
			row.msg = "plain text line " + strconv.Itoa(i)
		default: // collision, another parsed value
			row.stream["level"] = "warn"
			row.msg = `{"level":"fatal","pipeline":"logs/loki"}`
		}
		rows = append(rows, row)
	}
	return rows
}

// Metrics filtered or grouped on a name_extracted label after `| json` are
// answered from VictoriaLogs stats buckets, never from raw rows, and equal
// Loki's: the parsed value of name where the stream has the label name, the
// key name_extracted otherwise. A raw-row answer fails these cases.
// conformance: parser-error-and-label-collision, semantics/json-extracted-metric-pushdown
func TestOrderedJSONExtractedMetricPushdownMatchesLoki(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	rows := extractedPushdownFixture(s0)
	start, end := s0.Add(time.Minute), s0.Add(10*time.Minute)
	for _, tc := range []struct {
		query string
		empty bool // Loki's answer is empty (no line carries the label)
	}{
		{query: `sum by (level) (count_over_time({app="api"} | json | level_extracted!="" [1m]))`},
		{query: `sum by (level_extracted) (count_over_time({app="api"} | json | drop __error__ [1m]))`},
		{query: `sum by (level, level_extracted) (count_over_time({app="api"} | json | drop __error__ [1m]))`},
		{query: `sum(rate({app="api"} | json | level_extracted="error" [1m]))`},
		{query: `sum by (level) (rate({app="api"} | json | level_extracted=~"err.*|literal" [2m]))`},
		{query: `sum by (level) (bytes_over_time({app="api"} | json | level_extracted!~"error" | level_extracted!="" [1m]))`},
		{query: `sum by (level) (bytes_rate({app="api"} | json | drop __error__ | level_extracted!="error" [1m]))`},
		{query: `sum by (level_extracted) (count_over_time({app="api"} | json | level_extracted!="" | pipeline="logs/loki" [1m]))`},
		{query: `sum(count_over_time({app="api"} | json | level_extracted!="" [1m]))`},
		// No stream label named pipeline, so there is no pipeline_extracted.
		{query: `sum by (level) (count_over_time({app="api"} | json | pipeline_extracted!="" [1m]))`, empty: true},
	} {
		t.Run(tc.query, func(t *testing.T) {
			srv, fake := newPushdownFakeVL(t, rows, nil)
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			plan, ok := compileOrderedJSONMetric(tc.query)
			if !ok || !plan.pushdown {
				t.Fatalf("a name_extracted metric must compile to a stats pushdown, got ok=%v", ok)
			}
			want := lokiPushdownReference(t, plan, rows, start, end, time.Minute)
			if (len(want) == 0) != tc.empty {
				t.Fatalf("fixture answer has %d series, empty expected: %v", len(want), tc.empty)
			}
			got := runJSONVolumeQueryRange(t, p, tc.query, start, end, time.Minute)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("result differs from the Loki reference\n got: %v\nwant: %v", got, want)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if fake.raw != 0 || len(fake.stats) != 1 {
				t.Fatalf("expected one stats call and no raw rows: raw=%d stats=%q guards=%q", fake.raw, fake.stats, fake.guards)
			}
		})
	}
}

// The same answers when the body also has the key under the dotted or hyphen
// spelling Loki sanitizes, and a line the two parsers read differently keeps
// the exact raw evaluator (the probes read the stored value of name too).
// conformance: parser-error-and-label-collision, semantics/json-extracted-metric-pushdown
func TestOrderedJSONExtractedMetricProbeKeepsRawEvaluator(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	query := `sum by (level) (count_over_time({app="api"} | json | level_extracted!="" | drop __error__ [1m]))`
	base := func(i int) pushdownRow {
		return pushdownRow{ts: s0.Add(time.Duration(i) * 10 * time.Second), stream: map[string]string{"app": "api", "level": "info"}, msg: `{"level":"error"}`}
	}
	// Loki keeps the keys it parsed before the syntax error: level_extracted=error.
	risky := base(3)
	risky.msg = `{"level":"error","msg": truncated`
	rows := []pushdownRow{base(0), base(1), base(2), risky}
	srv, fake := newPushdownFakeVL(t, rows, nil)
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	plan, ok := compileOrderedJSONMetric(query)
	if !ok || !plan.pushdown {
		t.Fatalf("expected a pushdown plan, got ok=%v", ok)
	}
	at := s0.Add(time.Minute)
	want := lokiPushdownReference(t, plan, rows, at, at, time.Minute)
	got := runJSONVolumeQueryRange(t, p, query, at, at, time.Minute)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("result differs from the Loki reference\n got: %v\nwant: %v", got, want)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.raw != 1 {
		t.Fatalf("expected the probe to keep the raw evaluator: raw=%d guards=%q stats=%q", fake.raw, fake.guards, fake.stats)
	}
}

// A `| logfmt` volume filtered on name_extracted stays on stats buckets: the
// parsed level where the stream has a level label, the key level_extracted
// otherwise. Only the first of four lines (stream level warn, body level
// error) passes the filter: three samples per one-minute window.
// conformance: parser-error-and-label-collision, semantics/json-extracted-metric-pushdown
func TestLogfmtExtractedMetricPushdownMatchesLoki(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	var rows []pushdownRow
	for i := 0; i < 120; i++ {
		row := pushdownRow{ts: s0.Add(time.Duration(i) * 5 * time.Second), stream: map[string]string{"app": "api"}}
		switch i % 4 {
		case 0:
			row.stream["level"] = "warn"
			row.msg = "level=error op=update"
		case 1: // stream label, no level in the body
			row.stream["level"] = "info"
			row.msg = "op=select"
		case 2: // no stream label: level, not level_extracted
			row.msg = "level=debug op=select"
		default: // a literal level_extracted key does not pass level_extracted="" either way
			row.msg = "level_extracted= op=select"
		}
		rows = append(rows, row)
	}
	query := `sum by (level, detected_level) (count_over_time({app="api"} | logfmt | level_extracted!="" [1m]))`
	srv, fake := newPushdownFakeVL(t, rows, nil)
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	plan, ok := compileOrderedJSONMetric(query)
	if !ok || !plan.pushdown {
		t.Fatalf("expected a pushdown plan, got ok=%v", ok)
	}
	start, end := s0.Add(time.Minute), s0.Add(9*time.Minute)
	want := map[string]map[int64]string{canonicalLabelsKey(map[string]string{"level": "warn", "detected_level": "warn"}): {}}
	for at := start; !at.After(end); at = at.Add(time.Minute) {
		want[canonicalLabelsKey(map[string]string{"level": "warn", "detected_level": "warn"})][at.Unix()] = "3"
	}
	got := runJSONVolumeQueryRange(t, p, query, start, end, time.Minute)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("result differs from Loki's\n got: %v\nwant: %v", got, want)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.raw != 0 || len(fake.stats) != 1 || !strings.Contains(fake.stats[0], "unpack_logfmt from _msg fields (level)") {
		t.Fatalf("expected one stats call with the scratch unpack and no raw rows: raw=%d stats=%q", fake.raw, fake.stats)
	}
}

// A parsed key named like structured metadata (stored by VictoriaLogs as
// service.version, outside _stream) is renamed service_version_extracted too.
// conformance: parser-error-and-label-collision, semantics/json-extracted-metric-pushdown, profiles/parsed-key-colliding-with-structured-metadata
func TestOrderedJSONExtractedMetricStructuredMetadataCollision(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	var rows []pushdownRow
	for i := 0; i < 60; i++ {
		row := pushdownRow{ts: s0.Add(time.Duration(i) * 10 * time.Second), stream: map[string]string{"app": "api", "level": "info"}}
		if i%2 == 0 { // structured metadata and a body key of the same name
			row.vl, row.loki = map[string]string{"service.version": "0.96.0"}, map[string]string{"service_version": "0.96.0"}
			row.msg = `{"service_version":"9.9.9","pipeline":"logs/loki"}`
		} else { // only the body key: no collision
			row.msg = `{"service_version":"1.0.0","pipeline":"logs/loki"}`
		}
		rows = append(rows, row)
	}
	query := `sum by (service_version, service_version_extracted) (count_over_time({app="api"} | json | drop __error__ [1m]))`
	srv, fake := newPushdownFakeVL(t, rows, nil)
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	plan, ok := compileOrderedJSONMetric(query)
	if !ok || !plan.pushdown {
		t.Fatalf("expected a pushdown plan, got ok=%v", ok)
	}
	start, end := s0.Add(time.Minute), s0.Add(9*time.Minute)
	want := lokiPushdownReference(t, plan, rows, start, end, time.Minute)
	got := runJSONVolumeQueryRange(t, p, query, start, end, time.Minute)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("result differs from the Loki reference\n got: %v\nwant: %v", got, want)
	}
}

// The name_extracted pushdown needs the stats range offset arg (VictoriaLogs
// v1.45+) for its bucket edges; an older backend keeps the raw-row evaluator
// these shapes always had, with the same answer, and an unknown version
// stays on the pushdown.
// conformance: parser-error-and-label-collision, semantics/json-extracted-metric-pushdown, versions/stats-bucket-label-v1.45
func TestOrderedJSONExtractedMetricPushdownNeedsStatsRangeOffset(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	rows := extractedPushdownFixture(s0)
	start, end := s0.Add(time.Minute), s0.Add(10*time.Minute)
	query := `sum by (level) (count_over_time({app="api"} | json | level_extracted!="" [1m]))`
	for _, tc := range []struct {
		version string
		pushed  bool
	}{{"", true}, {"v1.40.0", false}, {"v1.44.2", false}, {"v1.45.0", true}, {"v1.52.0", true}} {
		t.Run("backend "+tc.version, func(t *testing.T) {
			srv, fake := newPushdownFakeVL(t, rows, nil)
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			// newSlidingTestProxy stored v1.50.0 first and the first version wins.
			p.backendVersionMu.Lock()
			p.backendVersionSemver, p.backendSupportsStatsRangeOffset = tc.version, tc.version != "" && semverAtLeast(tc.version, 1, 45, 0)
			p.backendVersionMu.Unlock()
			plan, ok := compileOrderedJSONMetric(query)
			if !ok || !plan.pushdown {
				t.Fatalf("expected a pushdown plan, got ok=%v", ok)
			}
			want := lokiPushdownReference(t, plan, rows, start, end, time.Minute)
			got := runJSONVolumeQueryRange(t, p, query, start, end, time.Minute)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("result differs from the Loki reference\n got: %v\nwant: %v", got, want)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if tc.pushed && (fake.raw != 0 || len(fake.stats) != 1) {
				t.Fatalf("expected the stats pushdown: raw=%d stats=%d", fake.raw, len(fake.stats))
			}
			if !tc.pushed && (fake.raw != 1 || len(fake.stats) != 0) {
				t.Fatalf("expected the raw evaluator below v1.45: raw=%d stats=%d", fake.raw, len(fake.stats))
			}
		})
	}
}
