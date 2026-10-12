//go:build e2e

package e2e_compat

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Loki attaches detected_level to every entry at ingest and falls back to
// "unknown" (pkg/distributor/field_detection.go), so a metric over lines that
// carry no level is still labelled detected_level="unknown", and a line whose
// level only the body gives (logfmt, JSON, a keyword) is labelled with it
// (pkg/logql/log/metrics_extraction.go streamLineSampleExtractor.Process adds
// the structured metadata; labels.go GroupedLabels returns it for an ungrouped
// or without() grouping). The proxy answered the instant and the tumbling route
// of a bare or without() metric without the label for a stream with no level
// field, and merged the lines of different body levels into one series
// (semantics/detected-level-missing-from-stats-answers).

type dlUnknownFixture struct {
	fx       slidingLiveFixture
	s0       time.Time
	selector string
}

var (
	dlUnknownOnce sync.Once
	dlUnknownFx   *dlUnknownFixture
)

// ensureDLUnknownFixture ingests one stream of lines whose level only the body
// gives, a line every 10s for 30 minutes: plain lines (unknown), logfmt, JSON
// and keyword lines, none with a stored level field, so each backend derives
// detected_level itself (Loki at ingest, the proxy on read).
func ensureDLUnknownFixture(t *testing.T) *dlUnknownFixture {
	t.Helper()
	dlUnknownOnce.Do(func() {
		now := time.Now()
		fx := &dlUnknownFixture{
			fx: slidingLiveFixture{app: fmt.Sprintf("dl-unknown-%d", now.UnixNano()), service: true, derivedLevel: true},
			s0: now.Add(-9 * time.Hour).Truncate(time.Hour),
		}
		fx.selector = fx.fx.selector()
		bodies := []string{
			"tick %d",
			"level=warn msg=slow n=%d",
			`{"level":"error","n":%d}`,
			"ERROR: upstream failed %d",
			"tick again %d",
			"level=info n=%d",
		}
		for i := 0; i < 180; i++ {
			ts := fx.s0.Add(3*time.Second + time.Duration(i)*10*time.Second)
			fx.fx.lines = append(fx.fx.lines, slidingLiveLine{ts: ts, msg: fmt.Sprintf(bodies[i%len(bodies)], i)})
		}
		ingestSlidingFixtures(t, fx.fx)
		dlUnknownFx = fx
	})
	if dlUnknownFx == nil {
		t.Fatal("the detected_level fixture was not ingested; see the first test that ran")
	}
	return dlUnknownFx
}

// waitForLoki polls until Loki's range metric path answers for the fixture
// (right after a fresh stack starts it answers empty for a few minutes).
func (fx *dlUnknownFixture) waitForLoki(t *testing.T) {
	t.Helper()
	at := fx.s0.Add(20 * time.Minute)
	query := fmt.Sprintf("sum(count_over_time(%s[5m]))", fx.selector)
	deadline := time.Now().Add(240 * time.Second)
	for {
		if len(slidingRangeSeries(t, lokiURL, query, at.Add(-time.Minute), at, time.Minute, nil)) > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Loki's range metric path answered nothing for %s after 240s", fx.selector)
		}
		time.Sleep(2 * time.Second)
	}
}

type dlUnknownShape struct {
	name        string
	window      string
	step        time.Duration // 0: instant
	start, stop time.Duration // offsets from the fixture start; an instant query evaluates at stop
}

var dlUnknownShapes = []dlUnknownShape{
	{"instant", "5m", 0, 0, 20 * time.Minute},
	{"tumbling", "5m", 5 * time.Minute, 5 * time.Minute, 25 * time.Minute},
	{"sliding", "5m", time.Minute, 6 * time.Minute, 26 * time.Minute},
}

func (s dlUnknownShape) run(t *testing.T, fx *dlUnknownFixture, base, query string) map[string]map[string]string {
	t.Helper()
	if s.step == 0 {
		at := fx.s0.Add(s.stop)
		out := map[string]map[string]string{}
		for key, value := range unwrapInstant(t, base, query, at) {
			out[key] = map[string]string{strconv.FormatInt(at.Unix(), 10): value}
		}
		return out
	}
	return slidingRangeSeries(t, base, query, fx.s0.Add(s.start), fx.s0.Add(s.stop), s.step, nil)
}

// TestCompat_DetectedLevelUnknownInStatsAnswers: a bare count_over_time and
// rate, and the same under a without() grouping, a grouping by detected_level
// and a series count by it, are Loki's series (the labels with
// detected_level="unknown" for the lines no rule gives a level, and the level
// the body gives for the others) with Loki's values, at an instant, over a
// tumbling window and over a sliding window.
// conformance: semantics/detected-level-missing-from-stats-answers, severity-exposure-surfaces, metric-series-identity, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_DetectedLevelUnknownInStatsAnswers(t *testing.T) {
	fx := ensureDLUnknownFixture(t)
	fx.waitForLoki(t)
	for _, shape := range dlUnknownShapes {
		for _, outer := range []string{
			`count_over_time(%s[%s])`,
			`rate(%s[%s])`,
			`sum without (service_name) (count_over_time(%s[%s]))`,
			`sum without (detected_level) (count_over_time(%s[%s]))`,
			`sum by (detected_level) (count_over_time(%s[%s]))`,
			`count by (detected_level) (count_over_time(%s[%s]))`,
		} {
			if strings.HasPrefix(outer, "count by") && shape.name == "sliding" {
				// The sliding route does not reduce count, min, max and avg yet
				// (semantics/non-sum-vector-aggregation-unreduced).
				continue
			}
			query := fmt.Sprintf(outer, fx.selector, shape.window)
			t.Run(shape.name+"/"+query, func(t *testing.T) {
				loki := shape.run(t, fx, lokiURL, query)
				proxy := shape.run(t, fx, proxyURL, query)
				if strings.Contains(query, "without (detected_level)") {
					for key := range loki {
						if strings.Contains(key, "detected_level=") {
							t.Fatalf("%s: Loki sanity: the series keeps detected_level: %s", query, key)
						}
					}
				} else {
					unknown := false
					for key := range loki {
						unknown = unknown || strings.Contains(key, "detected_level=unknown")
					}
					if !unknown {
						t.Fatalf("%s: Loki sanity: no detected_level=unknown series in %v", query, loki)
					}
				}
				compareUnwrapSumSeries(t, query, loki, proxy)
			})
		}
	}
}
