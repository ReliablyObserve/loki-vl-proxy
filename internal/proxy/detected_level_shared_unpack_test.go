package proxy

import (
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

// sharedUnpackProbe is a stored line and the level and detected_level labels
// Loki gives it for `sum by (level, detected_level) (... | json | drop
// __error__ ...)`. The answers follow Loki v3.7.7: detected_level is set at
// ingest from the first level label of the stream, then of the structured
// metadata, then the OTLP severity number, then the line
// (pkg/distributor/field_detection.go), and the level label is the stream's or
// the metadata's `level`, else the parser's `level` key (a body key that
// collides with a stored label is renamed level_extracted). They are pinned
// against real Loki by the e2e test
// TestRangeMetricCompatibilityJSONDetectedLevelSharedUnpack.
type sharedUnpackProbe struct {
	name          string
	stream        map[string]string
	vl            map[string]string
	msg           string
	level, detect string // the labels Loki gives the line ("" level: no label)
}

var sharedUnpackProbes = []sharedUnpackProbe{
	{name: "stored severity, body level", vl: map[string]string{"severity": "warn"}, msg: `{"level":"error","n":1}`, level: "error", detect: "warn"},
	{name: "stored lvl, body level", vl: map[string]string{"lvl": "info"}, msg: `{"level":"error","n":1}`, level: "error", detect: "info"},
	{name: "stored severity number, body level", vl: map[string]string{"severity_number": "17"}, msg: `{"level":"debug","n":1}`, level: "debug", detect: "error"},
	{name: "stored Level, body level", vl: map[string]string{"Level": "info"}, msg: `{"level":"debug","n":1}`, level: "debug", detect: "info"},
	{name: "stored LEVEL, body level of another spelling", vl: map[string]string{"LEVEL": "WARN"}, msg: `{"level":"Information"}`, level: "Information", detect: "warn"},
	{name: "stream level, body level", stream: map[string]string{"level": "warn"}, msg: `{"level":"error","n":1}`, level: "warn", detect: "warn"},
	{name: "stored level, body level", vl: map[string]string{"level": "WARN"}, msg: `{"level":"info"}`, level: "WARN", detect: "warn"},
	{name: "stored severity, no body level", vl: map[string]string{"severity": "warn"}, msg: `{"n":1}`, level: "", detect: "warn"},
	{name: "body level only", msg: `{"level":"debug","n":1}`, level: "debug", detect: "debug"},
	{name: "body level of an unknown spelling, keyword in the line", msg: `{"level":"notice","msg":"error connecting"}`, level: "notice", detect: "error"},
}

// A plan that groups by `level` unpacks it from the body, and the chain that
// derives detected_level reads the same line: one unpack_json serves both. The
// label must be what Loki gives it wherever a level is stored (a stored field
// of another name decides the row's detected_level, and the parsed `level`
// still counts), also when the presence probe finds no body key the chain
// would read (the plan's label is no body key of the chain).
// conformance: semantics/json-metric-detected-level-derivation
func TestSharedUnpackKeepsThePlansLabelsLikeLoki(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(2*time.Minute)
	for _, pruned := range []bool{false, true} {
		for _, probe := range sharedUnpackProbes {
			name := probe.name
			if pruned {
				name += " pruned"
			}
			t.Run(name, func(t *testing.T) {
				// A second line no stored level decides keeps the chain route in play.
				rows := []pushdownRow{
					probe.row(s0.Add(100 * time.Second)),
					{ts: s0.Add(101 * time.Second), stream: map[string]string{"app": "api", "env": "prod"}, msg: `{"n":2}`},
				}
				srv, fake := newPushdownFakeVL(t, rows, nil)
				fake.presence = pruned
				p := newFilterPushdownProxy(t, srv.URL)
				fake.stored = p.labelTranslator.ToVL
				labels := map[string]string{"detected_level": probe.detect}
				if probe.level != "" {
					labels["level"] = probe.level
				}
				want := map[string]map[int64]string{
					canonicalLabelsKey(labels): {end.Unix(): "1"},
					canonicalLabelsKey(map[string]string{"detected_level": "unknown"}): {end.Unix(): "1"},
				}
				query := `sum by (level, detected_level) (count_over_time({env="prod"} | json | drop __error__ [5m]))`
				code, got, body := detectedLevelQuery(t, p, query, start, end, time.Minute)
				if code != http.StatusOK || !reflect.DeepEqual(got, want) {
					t.Fatalf("status %d %.200s, got %v, want %v", code, body, got, want)
				}
				fake.mu.Lock()
				defer fake.mu.Unlock()
				for _, q := range fake.stats {
					i := strings.Index(q, "unpack_json")
					println("DBG", q[max(0, i-80):min(len(q), i+500)])
					j := strings.Index(q, "as detected_level")
					if j >= 0 {
						println("DBGEND", q[j:])
					}
				}
				chain := chainStatsCalls(fake.stats)
				if len(chain) != 1 || fake.raw != 0 {
					t.Fatalf("expected one chain stats query without raw rows: chain=%d raw=%d", len(chain), fake.raw)
				}
				// The query carries one parse of the line, from the chain, and the
				// plan's own unpack is gone.
				if n := strings.Count(chain[0], "unpack_json"); n != 1 {
					t.Fatalf("%d unpack_json pipes, want 1: %s", n, chain[0])
				}
				if !strings.Contains(chain[0], `result_prefix "__j_"`) || strings.Contains(chain[0], "fields (level) keep_original_fields") {
					t.Fatalf("the plan keeps its own unpack: %s", chain[0])
				}
				if pruned && len(chain[0]) >= len(levelChain) {
					t.Fatalf("the query carries the full chain (%d bytes)", len(chain[0]))
				}
				// The chain that holds the plan's unpack is found and narrowed like any
				// other: the witness rides on it.
				if pruned && !strings.Contains(chain[0], levelWitnessField) {
					t.Fatalf("the pruned chain carries no witness: %s", chain[0])
				}
			})
		}
	}
}

func (c sharedUnpackProbe) row(ts time.Time) pushdownRow {
	labels := map[string]string{"app": "api", "env": "prod"}
	for k, v := range c.stream {
		labels[k] = v
	}
	return pushdownRow{ts: ts, stream: labels, vl: c.vl, msg: c.msg}
}

// Only a JSON plan that groups by plain labels hands its unpack to the chain:
// logfmt keeps its own (the chain does not read a line Loki reads as JSON as
// logfmt, the plan's parser does), a name_extracted label has a scratch unpack
// of its own, the stored-level plan has no chain, a key that is no plain name
// would be quoted or flattened, and a plan that groups by detected_level alone
// has no label to hand over.
// conformance: semantics/json-metric-detected-level-derivation
func TestSharedLevelUnpackEligibility(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		storedLevel bool
		want        []string
	}{
		{"level and detected_level", `sum by (level, detected_level) (count_over_time({app="x"} | json | drop __error__ [1m]))`, false, []string{"level"}},
		{"two labels", `sum by (app_name, level, detected_level) (count_over_time({app="x"} | json | drop __error__ [1m]))`, false, []string{"app_name", "level"}},
		{"a filter on a label", `sum by (level, detected_level) (count_over_time({app="x"} | json | status="500" | drop __error__ [1m]))`, false, []string{"level", "status"}},
		{"detected_level alone", `sum by (detected_level) (count_over_time({app="x"} | json | drop __error__ [1m]))`, false, nil},
		{"the stored-level plan", `sum by (level, detected_level) (count_over_time({app="x"} | json | drop __error__ [1m]))`, true, nil},
		{"logfmt", `sum by (level, detected_level) (count_over_time({app="x"} | logfmt | drop __error__ [1m]))`, false, nil},
		{"a name_extracted label", `sum by (level, level_extracted, detected_level) (count_over_time({app="x"} | json | drop __error__ [1m]))`, false, nil},
		{"no level derived", `sum by (level) (count_over_time({app="x"} | json | drop __error__ [1m]))`, false, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, ok := compileOrderedJSONMetric(tc.query)
			if !ok {
				t.Fatalf("no plan for %s", tc.query)
			}
			_, allKeys, extracted, _ := splitPushdownKeys(plan.unpackKeys())
			got := plan.sharedLevelUnpack(tc.storedLevel, allKeys, extracted, nil)
			if tc.want == nil {
				if got != nil {
					t.Fatalf("shared %v, want none", got)
				}
				return
			}
			// the unpack parses a row only where a label is missing as a stored field
			cond := orderedJSONUnpackCond(tc.want, nil)
			if got == nil || !reflect.DeepEqual(got.Keys, tc.want) || got.Cond != cond {
				t.Fatalf("shared %+v, want keys %v with condition %q", got, tc.want, cond)
			}
		})
	}
	// A key that is no plain name is never handed over.
	plan, _ := compileOrderedJSONMetric(`sum by (level, detected_level) (count_over_time({app="x"} | json | drop __error__ [1m]))`)
	for _, key := range []string{"log.level", "a-b", "9a", "__dl_x", ""} {
		if got := plan.sharedLevelUnpack(false, []string{"level", key}, nil, nil); got != nil {
			t.Errorf("key %q was shared", key)
		}
	}
}
