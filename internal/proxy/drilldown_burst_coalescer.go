package proxy

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

var (
	// drilldownFieldFilterRE matches "| filter <field> != \"\"" in a baseQuery.
	drilldownFieldFilterRE = regexp.MustCompile(`\|\s*filter\s+([\w.]+)\s*!=\s*""`)
	// drilldownParserPipeRE matches parser-stage pipes to strip before building
	// fused conditional-stats queries. VL's field:* existence check works on
	// pre-indexed columns WITHOUT | json / | logfmt. Including them returns empty.
	drilldownParserPipeRE = regexp.MustCompile(`\|\s*(?:unpack_json|unpack_logfmt|json|logfmt)\b[^|]*`)
)

// extractCommonBase strips the per-field filter and parser-stage pipes from a
// Drilldown Fields baseQuery, returning the pure stream selector and field name.
// Returns ("", "", false) if baseQuery does not match the Drilldown presence pattern.
func extractCommonBase(baseQuery string) (base, field string, ok bool) {
	m := drilldownFieldFilterRE.FindStringSubmatchIndex(baseQuery)
	if m == nil {
		return "", "", false
	}
	field = baseQuery[m[2]:m[3]]
	prefix := baseQuery[:m[0]]
	prefix = drilldownParserPipeRE.ReplaceAllString(prefix, "")
	base = strings.TrimSpace(prefix)
	return base, field, true
}

type burstKey struct {
	orgID    string
	base     string
	startSec int64
	endSec   int64
	stepNs   int64
}

type fieldResult struct {
	series map[string]manualSeriesSamples
	err    error
}

type burstGroup struct {
	fields []string
	chans  []chan fieldResult
}

// DrilldownBurstCoalescer groups concurrent per-field count_over_time queries from
// Grafana Drilldown Fields into a single fused VL conditional-stats call, reducing
// ~30 VL round-trips to 1 per page refresh.
type DrilldownBurstCoalescer struct {
	mu        sync.Mutex
	pending   map[burstKey]*burstGroup
	window    time.Duration
	maxFields int
}

func newDrilldownBurstCoalescer(windowMs, maxFields int) *DrilldownBurstCoalescer {
	if windowMs <= 0 {
		windowMs = 50
	}
	if maxFields <= 0 {
		maxFields = 30
	}
	return &DrilldownBurstCoalescer{
		pending:   make(map[burstKey]*burstGroup),
		window:    time.Duration(windowMs) * time.Millisecond,
		maxFields: maxFields,
	}
}

// Submit registers field in the burst group for key, waits for the window to
// close, and returns the per-field result from the fused fireFn call.
func (c *DrilldownBurstCoalescer) Submit(
	ctx context.Context,
	key burstKey,
	field string,
	fireFn func(ctx context.Context, fields []string) (map[string]fieldResult, error),
) (map[string]manualSeriesSamples, error) {
	ch := make(chan fieldResult, 1)

	c.mu.Lock()
	g := c.pending[key]
	if g != nil && len(g.fields) >= c.maxFields {
		delete(c.pending, key)
		g = nil
	}
	if g == nil {
		g = &burstGroup{
			fields: []string{field},
			chans:  []chan fieldResult{ch},
		}
		c.pending[key] = g
		window := c.window
		time.AfterFunc(window, func() { c.fire(key, g, fireFn) })
	} else {
		g.fields = append(g.fields, field)
		g.chans = append(g.chans, ch)
	}
	c.mu.Unlock()

	select {
	case res := <-ch:
		return res.series, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *DrilldownBurstCoalescer) fire(
	key burstKey,
	g *burstGroup,
	fireFn func(ctx context.Context, fields []string) (map[string]fieldResult, error),
) {
	c.mu.Lock()
	if c.pending[key] == g {
		delete(c.pending, key)
	}
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := fireFn(ctx, g.fields)
	for i, f := range g.fields {
		var r fieldResult
		if err != nil {
			r.err = err
		} else if res, ok := results[f]; ok {
			r = res
		} else {
			r.err = fmt.Errorf("burst coalescer: no result for field %q", f)
		}
		g.chans[i] <- r
	}
}
