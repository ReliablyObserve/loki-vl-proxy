package metrics

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

const (
	sinceStartRatioName = "loki_vl_proxy_cache_hit_ratio_since_start"
	sinceStartRatioHelp = "Share of a response cache tier's lookups that hit, from the start up to the given minutes after it: live until they have passed, then frozen. Absent for a tier without lookups in that time."
)

type sinceStartRatio struct {
	tier, minutes string
	value         float64
}

// sinceStartRatios lists the ratios there are: a tier that was not read in a window has none.
func sinceStartRatios(h cache.HandoverStats) []sinceStartRatio {
	if !h.StartupTracked {
		return nil
	}
	var out []sinceStartRatio
	for t, tier := range cacheLookupTier[:cacheTiers] {
		for w, window := range cache.StartupRatioWindows {
			if c := h.SinceStart[w][t]; c.Requests > 0 {
				out = append(out, sinceStartRatio{tier, strconv.Itoa(int(window / time.Minute)), float64(c.Hits) / float64(c.Requests)})
			}
		}
	}
	return out
}

func writeSinceStartRatios(sb *strings.Builder, h cache.HandoverStats) {
	ratios := sinceStartRatios(h)
	if len(ratios) == 0 {
		return
	}
	sb.WriteString("# HELP " + sinceStartRatioName + " " + sinceStartRatioHelp + "\n")
	sb.WriteString("# TYPE " + sinceStartRatioName + " gauge\n")
	for _, r := range ratios {
		fmt.Fprintf(sb, "%s{tier=%q,minutes=%q} %g\n", sinceStartRatioName, r.tier, r.minutes, r.value)
	}
}

// sinceStartRatioMetrics mirrors writeSinceStartRatios.
func (p *OTLPPusher) sinceStartRatioMetrics(h cache.HandoverStats, now int64) []map[string]interface{} {
	ratios := sinceStartRatios(h)
	if len(ratios) == 0 {
		return nil
	}
	points := make([]map[string]interface{}, 0, len(ratios))
	for _, r := range ratios {
		points = append(points, p.gaugeDP(sinceStartRatioName, r.value, now, attr("tier", r.tier), attr("minutes", r.minutes)))
	}
	return []map[string]interface{}{p.gaugeMetric(sinceStartRatioName, sinceStartRatioHelp, "1", points...)}
}
