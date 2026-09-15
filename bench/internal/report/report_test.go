package report

import (
	"bytes"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/histogram"
)

func TestViolations(t *testing.T) {
	clean := histogram.Stats{Count: 100}
	if v := Violations(clean, 0); len(v) != 0 {
		t.Fatalf("clean run: %v", v)
	}
	if v := Violations(histogram.Stats{}, 0); len(v) != 1 || !strings.Contains(v[0], "no request") {
		t.Fatalf("empty run: %v", v)
	}
	oneError := histogram.Stats{Count: 100, Errors: 1, ErrorRate: 0.01, Status5xx: 1}
	if v := Violations(oneError, 0); len(v) != 1 || !strings.Contains(v[0], "error rate 1.00%") || !strings.Contains(v[0], "5xx=1") {
		t.Fatalf("one error at threshold 0: %v", v)
	}
	if v := Violations(oneError, 0.01); len(v) != 0 {
		t.Fatalf("one error within 1%%: %v", v)
	}
	degraded := histogram.Stats{Count: 50, Degraded: 2, DegradedRate: 0.04}
	if v := Violations(degraded, 0.01); len(v) != 1 || !strings.Contains(v[0], "degraded-answer rate 4.00%") {
		t.Fatalf("degraded answers: %v", v)
	}
}

func TestReportHeaderShowsThresholdAndPublishability(t *testing.T) {
	var b bytes.Buffer
	WriteText(&b, Meta{CacheMode: "cold", MaxErrorRate: 0.005}, nil)
	if !strings.Contains(b.String(), "Max error and degraded-answer rate: 0.50%") || !strings.Contains(b.String(), "Publishable: yes") || !strings.Contains(b.String(), "cold (Loki results caches off") {
		t.Fatalf("header:\n%s", b.String())
	}
	b.Reset()
	WriteText(&b, Meta{CacheMode: "warm", NotPublishable: []string{"proxy_nocache: error rate"}}, nil)
	if !strings.Contains(b.String(), "NOT PUBLISHABLE") || !strings.Contains(b.String(), "proxy_nocache: error rate") {
		t.Fatalf("header:\n%s", b.String())
	}
	records := []RunRecord{{Target: "loki"}, {Target: "proxy"}}
	Stamp(records, Meta{CacheMode: "warm", MaxErrorRate: 0.01, NotPublishable: []string{"x"}})
	if records[1].Publishable || records[1].CacheMode != "warm" || records[1].MaxErrorRate != 0.01 || len(records[1].NotPublishable) != 1 {
		t.Fatalf("stamp %+v", records[1])
	}
}
