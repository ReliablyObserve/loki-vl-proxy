package proxy

import (
	"context"
	"strings"
	"testing"
	"time"
)

// The parse-risk probes of a name_extracted label run over every line of the
// range with the stored label deleted. Their filters on _msg come first, where
// VictoriaLogs merges them into the query and skips blocks through its token
// index, instead of behind the delete, where they ran row by row over every
// line; the regexps of the partial-parse probe run only on lines holding the
// key as a word. The lines they find are the same: the probe answers of every
// risky line kind are pinned against the Loki reference by
// TestOrderedJSONExtractedMetricDifferential.
// conformance: quality/extracted-label-probe-cost, semantics/json-extracted-metric-pushdown
func TestExtractedLabelProbesFilterAheadOfDelete(t *testing.T) {
	for _, tc := range []struct{ base, filter, want string }{
		{`app:="api" | delete level`, `_msg:"x"`, `app:="api" | filter _msg:"x" | delete level`},
		{`app:="api" | delete level, service.version`, `_msg:"x"`, `app:="api" | filter _msg:"x" | delete level, service.version`},
		{`app:="api"`, `_msg:"x"`, `app:="api"`},
		{`app:="api" | delete _msg`, `_msg:"x"`, `app:="api" | delete _msg`},
		{`app:="api" | delete lev*`, `_msg:"x"`, `app:="api" | delete lev*`},
		{`app:="api" | delete level | unpack_json`, `_msg:"x"`, `app:="api" | delete level | unpack_json`},
		{`app:="api" | delete level`, ``, `app:="api" | delete level`},
	} {
		if got := filterAheadOfDelete(tc.base, tc.filter); got != tc.want {
			t.Errorf("filterAheadOfDelete(%q, %q) = %q, want %q", tc.base, tc.filter, got, tc.want)
		}
	}
	long := strings.Repeat("x", vlDefaultMaxQueryLen+1)
	if got := fasterRiskQuery(context.Background(), "short", long); got != "short" {
		t.Errorf("a faster probe over VictoriaLogs' query length must give way to the one that fits")
	}
	if got := fasterRiskQuery(context.Background(), long, long+"y"); got != long+"y" {
		t.Errorf("when neither fits, the faster probe is sent")
	}

	s0 := time.Unix(1700000400, 0).UTC()
	srv, fake := newPushdownFakeVL(t, extractedPushdownFixture(s0), nil)
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	runJSONVolumeQueryRange(t, p, `sum by (level) (count_over_time({app="api"} | json | level_extracted!="" [1m]))`, s0.Add(time.Minute), s0.Add(10*time.Minute), time.Minute)
	fake.mu.Lock()
	defer fake.mu.Unlock()
	deleted := 0
	for _, guard := range fake.guards {
		head, _, ok := strings.Cut(guard, " | delete level")
		if !ok {
			continue
		}
		deleted++
		lead, filter, ok := strings.Cut(head, " | filter ")
		if !ok || strings.Contains(lead, "|") || strings.Contains(filter, " | ") || strings.Contains(filter, "level:") {
			t.Errorf("the probe's filter on _msg must be its first pipe, ahead of the delete: %s", guard)
		}
	}
	if deleted != 3 {
		t.Fatalf("expected the three name_extracted probes, got %d: %q", deleted, fake.guards)
	}
}
