package metrics

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// CacheComponentSignals is one physical cache instance's eviction and admission
// reasons, eviction histograms, promotions and L2 record reads. Components are
// summed on export: the reason families carry layer, tier and reason, not
// component, so their series stay a small fixed set.
type CacheComponentSignals struct {
	Component string
	Layers    [cache.LayerCount]cache.LayerSignalsSnapshot
}

// CacheTTL is the base TTL the proxy stores one answer class with.
type CacheTTL struct {
	Class string
	TTL   time.Duration
}

// CacheTTLClasses are the only classes loki_vl_proxy_cache_ttl_seconds exports;
// anything else a provider returns is dropped, so the label set stays fixed.
var CacheTTLClasses = [...]string{
	"query_range", "query", "labels", "label_values", "series", "detected_fields", "detected_field_values",
	"detected_labels", "patterns", "index_stats", "volume", "volume_range",
	"labels_handler", "label_values_handler", "label_inventory", "metadata_negative",
	"query_range_window_recent", "query_range_window_history", "query_range_extent", "parse_risk_probe",
}

// SetCacheSignalStatsProvider attaches the reason, eviction and promotion
// snapshots. The provider must read atomics only (cache.Cache.SignalStats):
// it runs on every scrape and OTLP push.
func (m *Metrics) SetCacheSignalStatsProvider(fn func() []CacheComponentSignals) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.cacheSignalStatsProvider = fn
	m.mu.Unlock()
}

// SetCacheTTLProvider attaches the base TTL of each answer class.
func (m *Metrics) SetCacheTTLProvider(fn func() []CacheTTL) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.cacheTTLProvider = fn
	m.mu.Unlock()
}

// The reasons each tier can produce. Others stay unexported rather than
// exported as permanent zeros.
var (
	cacheSignalTiers          = [...]string{"l1_memory", "l2_disk"}
	cacheSignalEvictionByTier = [len(cacheSignalTiers)][]cache.EvictionReason{
		{cache.EvictionExpired, cache.EvictionCapacity, cache.EvictionReplaced, cache.EvictionInvalidated, cache.EvictionFlush},
		{cache.EvictionExpired, cache.EvictionCapacity, cache.EvictionReplaced, cache.EvictionInvalidated, cache.EvictionFlush},
	}
	cacheSignalRejectionByTier = [len(cacheSignalTiers)][]cache.RejectionReason{
		{cache.RejectionSingleEntryBudget, cache.RejectionSourceExpired, cache.RejectionShadowBudget},
		{cache.RejectionTooLarge, cache.RejectionQueueFull, cache.RejectionTTLBelowMinimum, cache.RejectionCapacity, cache.RejectionInvalidKey, cache.RejectionEncodeFailed, cache.RejectionClosed, cache.RejectionMemoryOnly, cache.RejectionNotReplica},
	}
)

type signalLabel struct{ name, value string }

type signalHistogram struct {
	bounds []float64 // upper bounds in the exported unit
	counts []int64   // per bucket, not cumulative; the last is above every bound
	sum    float64
	count  int64
}

type signalPoint struct {
	labels []signalLabel
	value  int64
	gauge  float64
	hist   *signalHistogram
}

type signalMetric struct {
	name, help, unit string
	kind             string // counter, gauge or histogram
	points           []signalPoint
}

func durationBoundsSeconds(bounds []time.Duration) []float64 {
	out := make([]float64, len(bounds))
	for i, b := range bounds {
		out[i] = b.Seconds()
	}
	return out
}

func countBounds(bounds []int64) []float64 {
	out := make([]float64, len(bounds))
	for i, b := range bounds {
		out[i] = float64(b)
	}
	return out
}

// addHistogram sums snapshot s into h. A snapshot that was never initialised
// has no counts and adds nothing.
func (h *signalHistogram) add(s cache.HistogramSnapshot, sumScale float64) {
	counts := s.Counts()
	for i := 0; i < len(counts) && i < len(h.counts); i++ {
		h.counts[i] += counts[i]
	}
	h.sum += float64(s.Sum) * sumScale
	h.count += s.Count
}

func newSignalHistogram(bounds []float64) *signalHistogram {
	return &signalHistogram{bounds: bounds, counts: make([]int64, len(bounds)+1)}
}

func (m *Metrics) cacheSignalMetricSnapshots() []signalMetric {
	m.mu.RLock()
	signalsFn := m.cacheSignalStatsProvider
	ttlFn := m.cacheTTLProvider
	m.mu.RUnlock()
	var out []signalMetric
	if signalsFn != nil {
		var sum [cache.LayerCount]cache.LayerSignalsSnapshot
		var age, hits [cache.LayerCount][len(cacheSignalTiers)][len(cache.ObservedEvictionReasons)]*signalHistogram
		var readSeconds, readBytes [cache.LayerCount]*signalHistogram
		ageBounds, hitBounds := durationBoundsSeconds(cache.EvictionAgeBounds[:]), countBounds(cache.EvictionHitsBounds[:])
		for l := range sum {
			for t := range cacheSignalTiers {
				for r := range cache.ObservedEvictionReasons {
					age[l][t][r], hits[l][t][r] = newSignalHistogram(ageBounds), newSignalHistogram(hitBounds)
				}
			}
			readSeconds[l] = newSignalHistogram(durationBoundsSeconds(cache.DiskReadSecondsBounds[:]))
			readBytes[l] = newSignalHistogram(countBounds(cache.DiskReadBytesBounds[:]))
		}
		seen := map[string]bool{}
		for _, component := range signalsFn() {
			if seen[component.Component] {
				continue
			}
			seen[component.Component] = true
			for l := range component.Layers {
				c := component.Layers[l]
				for t, tier := range [...]cache.TierSignalsSnapshot{c.L1, c.L2} {
					dst := &sum[l].L1
					if t == 1 {
						dst = &sum[l].L2
					}
					for r := range tier.Evictions {
						dst.Evictions[r] += tier.Evictions[r]
					}
					for r := range tier.Rejections {
						dst.Rejections[r] += tier.Rejections[r]
					}
					for r := range cache.ObservedEvictionReasons {
						age[l][t][r].add(tier.AgeAtEviction[r], 1e-9)
						hits[l][t][r].add(tier.HitsAtEviction[r], 1)
					}
				}
				for p := range c.Promotions {
					sum[l].Promotions[p] += c.Promotions[p]
				}
				readSeconds[l].add(c.DiskReadSeconds, 1e-9)
				readBytes[l].add(c.DiskReadBytes, 1)
			}
		}
		evictions := signalMetric{name: "loki_vl_proxy_cache_evictions_total", kind: "counter",
			help: "Cache entries that left a tier, by semantic layer, tier and reason: expired, capacity (least recently used under the entry or byte budget), replaced (a newer value for the key), invalidated (L1 explicit invalidation; L2 a record whose newer write could not be stored or that could not be read back), flush (a purge). expired + capacity equals loki_vl_proxy_cache_layer_evictions_total summed over components."}
		rejections := signalMetric{name: "loki_vl_proxy_cache_admission_rejections_total", kind: "counter",
			help: "Values a tier did not store, by semantic layer, tier and reason. memory_only and not_replica are L2 placement skips (memory-only answers, non-owner shadow copies), not refused writes; the other reasons sum to loki_vl_proxy_cache_layer_rejections_total."}
		promotions := signalMetric{name: "loki_vl_proxy_cache_promotions_total", kind: "counter",
			help: "Lower-tier hits copied to a higher tier, by semantic layer: from l2_disk or l3_peer to l1_memory, and l3_peer hits handed to the local l2_disk write queue."}
		ageMetric := signalMetric{name: "loki_vl_proxy_cache_entry_age_at_eviction_seconds", kind: "histogram", unit: "s",
			help: "Time an entry spent in a tier before it was evicted for expiry or capacity, by semantic layer, tier and reason. L2 records found by the start-up scan have no known admission time and are not observed."}
		hitsMetric := signalMetric{name: "loki_vl_proxy_cache_entry_hits_at_eviction", kind: "histogram",
			help: "Lookups an entry answered from its tier before it was evicted for expiry or capacity, by semantic layer, tier and reason. L1 counts deferred LRU promotions, which drop under bursts, so it is a lower bound."}
		for l := cache.LayerUnknown; l < cache.LayerCount; l++ {
			layer := signalLabel{"layer", l.String()}
			for t, tierName := range cacheSignalTiers {
				tier := signalLabel{"tier", tierName}
				snap := sum[l].L1
				if t == 1 {
					snap = sum[l].L2
				}
				for _, r := range cacheSignalEvictionByTier[t] {
					evictions.points = append(evictions.points, signalPoint{labels: []signalLabel{layer, tier, {"reason", r.String()}}, value: snap.Evictions[r]})
				}
				for _, r := range cacheSignalRejectionByTier[t] {
					rejections.points = append(rejections.points, signalPoint{labels: []signalLabel{layer, tier, {"reason", r.String()}}, value: snap.Rejections[r]})
				}
				for r, reason := range cache.ObservedEvictionReasons {
					labels := []signalLabel{layer, tier, {"reason", reason.String()}}
					ageMetric.points = append(ageMetric.points, signalPoint{labels: labels, hist: age[l][t][r]})
					hitsMetric.points = append(hitsMetric.points, signalPoint{labels: labels, hist: hits[l][t][r]})
				}
			}
			for p := cache.Promotion(0); p < cache.PromotionCount; p++ {
				from, to := p.Tiers()
				promotions.points = append(promotions.points, signalPoint{labels: []signalLabel{layer, {"from", from}, {"to", to}}, value: sum[l].Promotions[p]})
			}
		}
		readSecondsMetric := signalMetric{name: "loki_vl_proxy_cache_l2_read_seconds", kind: "histogram", unit: "s",
			help: "Time of one L2 lookup that read a record from bbolt (the index lock, the read and the decode), by semantic layer. L2 hits served from the pending write queue are not reads; the request-level cache lookup time is loki_vl_proxy_cache_lookup_seconds."}
		readBytesMetric := signalMetric{name: "loki_vl_proxy_cache_l2_read_bytes", kind: "histogram", unit: "By",
			help: "Encoded bytes of one L2 record read from bbolt, by semantic layer."}
		for l := cache.LayerUnknown; l < cache.LayerCount; l++ {
			layer := []signalLabel{{"layer", l.String()}}
			readSecondsMetric.points = append(readSecondsMetric.points, signalPoint{labels: layer, hist: readSeconds[l]})
			readBytesMetric.points = append(readBytesMetric.points, signalPoint{labels: layer, hist: readBytes[l]})
		}
		out = append(out, evictions, rejections, ageMetric, hitsMetric, promotions, readSecondsMetric, readBytesMetric)
	}
	if ttlFn != nil {
		ttl := signalMetric{name: "loki_vl_proxy_cache_ttl_seconds", kind: "gauge", unit: "s",
			help: "Base TTL the proxy stores each answer class with. The label, label value and detected field and label handler caches scale it with the request window: x3 up to 6h, x10 up to 24h, x20 up to 7d, never above 1h, and 1h beyond 7d."}
		values := map[string]time.Duration{}
		for _, entry := range ttlFn() {
			values[entry.Class] = entry.TTL
		}
		for _, class := range CacheTTLClasses {
			if v, ok := values[class]; ok {
				ttl.points = append(ttl.points, signalPoint{labels: []signalLabel{{"class", class}}, gauge: v.Seconds()})
			}
		}
		if len(ttl.points) > 0 {
			out = append(out, ttl)
		}
	}
	return out
}

func writeSignalLabels(sb *strings.Builder, labels []signalLabel, extra string) {
	sb.WriteByte('{')
	for i, l := range labels {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(sb, "%s=%q", l.name, l.value)
	}
	if extra != "" {
		if len(labels) > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(extra)
	}
	sb.WriteByte('}')
}

func (m *Metrics) writeCacheSignalMetrics(sb *strings.Builder) {
	for _, metric := range m.cacheSignalMetricSnapshots() {
		fmt.Fprintf(sb, "# HELP %s %s\n# TYPE %s %s\n", metric.name, metric.help, metric.name, metric.kind)
		for _, point := range metric.points {
			switch metric.kind {
			case "histogram":
				cumulative := int64(0)
				for i, bound := range point.hist.bounds {
					cumulative += point.hist.counts[i]
					sb.WriteString(metric.name + "_bucket")
					writeSignalLabels(sb, point.labels, `le="`+strconv.FormatFloat(bound, 'g', -1, 64)+`"`)
					fmt.Fprintf(sb, " %d\n", cumulative)
				}
				sb.WriteString(metric.name + "_bucket")
				writeSignalLabels(sb, point.labels, `le="+Inf"`)
				fmt.Fprintf(sb, " %d\n", point.hist.count)
				sb.WriteString(metric.name + "_sum")
				writeSignalLabels(sb, point.labels, "")
				fmt.Fprintf(sb, " %s\n", strconv.FormatFloat(point.hist.sum, 'g', -1, 64))
				sb.WriteString(metric.name + "_count")
				writeSignalLabels(sb, point.labels, "")
				fmt.Fprintf(sb, " %d\n", point.hist.count)
			case "gauge":
				sb.WriteString(metric.name)
				writeSignalLabels(sb, point.labels, "")
				fmt.Fprintf(sb, " %s\n", strconv.FormatFloat(point.gauge, 'g', -1, 64))
			default:
				sb.WriteString(metric.name)
				writeSignalLabels(sb, point.labels, "")
				fmt.Fprintf(sb, " %d\n", point.value)
			}
		}
	}
}

func (p *OTLPPusher) cacheSignalMetrics(now int64) []map[string]interface{} {
	var out []map[string]interface{}
	for _, metric := range p.metrics.cacheSignalMetricSnapshots() {
		points := make([]map[string]interface{}, 0, len(metric.points))
		for _, point := range metric.points {
			attributes := make([]map[string]interface{}, 0, len(point.labels))
			for _, l := range point.labels {
				attributes = append(attributes, attr(l.name, l.value))
			}
			switch metric.kind {
			case "histogram":
				points = append(points, map[string]interface{}{
					"timeUnixNano":   fmt.Sprintf("%d", now),
					"count":          fmt.Sprintf("%d", point.hist.count),
					"sum":            point.hist.sum,
					"explicitBounds": append([]float64(nil), point.hist.bounds...),
					"bucketCounts":   append([]int64(nil), point.hist.counts...),
					"attributes":     attributes,
				})
			case "gauge":
				points = append(points, p.gaugeDP(metric.name, point.gauge, now, attributes...))
			default:
				points = append(points, p.counterDP(metric.name, point.value, now, attributes...))
			}
		}
		switch metric.kind {
		case "histogram":
			out = append(out, p.histogramMetric(metric.name, metric.help, metric.unit, points...))
		case "gauge":
			out = append(out, p.gaugeMetric(metric.name, metric.help, metric.unit, points...))
		default:
			out = append(out, p.sumMetric(metric.name, metric.help, metric.unit, points...))
		}
	}
	return out
}
