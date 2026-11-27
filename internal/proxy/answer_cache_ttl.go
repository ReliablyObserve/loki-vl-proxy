package proxy

import (
	"fmt"
	"time"
)

// AnswerCacheTTLClasses are the Loki answer classes whose built-in cache TTL an
// operator may shorten (Config.AnswerCacheTTLs). Each class is one row of
// loki_vl_proxy_cache_ttl_seconds{class}. A setting can never lengthen a TTL:
// answers must not become staler than the built-in TTLs allow. patterns is not
// one of them (its TTL keeps mined state until it is replaced, it is no
// freshness bound), nor are the classes with their own flags (labels_handler,
// label_values_handler, the query_range windows and extents, the parse-risk
// probe) or derived ones (label_inventory, metadata_negative).
var AnswerCacheTTLClasses = [...]string{
	"query_range", "query", "labels", "label_values", "series",
	"detected_fields", "detected_field_values", "detected_labels",
	"index_stats", "volume", "volume_range",
}

// compatAnswerFixedTTL is the Loki answer cache's TTL of query_range and query:
// a fixed 5 minutes so the entry outlives the bucket used to generate the key
// (max(5min, step)) while keeping staleness imperceptible: on a 2-day chart a
// 5-min gap is 0.17% of width. Using step (up to 1h) as TTL caused visible
// empty spaces at the chart right edge.
const compatAnswerFixedTTL = 5 * time.Minute

// AnswerCacheTTLMax is the built-in TTL of an answer class, the longest a
// setting may give it, and false for a name that is not such a class.
func AnswerCacheTTLMax(class string) (time.Duration, bool) {
	for _, c := range AnswerCacheTTLClasses {
		if c != class {
			continue
		}
		if class == "query_range" || class == "query" {
			return compatAnswerFixedTTL, true
		}
		return CacheTTLs[class], true
	}
	return 0, false
}

// ValidateAnswerCacheTTL checks one setting: 0 keeps the built-in TTL, a
// positive value up to the built-in one shortens it, anything else is refused.
func ValidateAnswerCacheTTL(class string, ttl time.Duration) error {
	limit, ok := AnswerCacheTTLMax(class)
	if !ok {
		return fmt.Errorf("unknown answer cache TTL class %q", class)
	}
	if ttl < 0 {
		return fmt.Errorf("answer cache TTL of %s is %s: must be >= 0 (0 keeps the built-in %s)", class, ttl, limit)
	}
	if ttl > limit {
		return fmt.Errorf("answer cache TTL of %s is %s, longer than the built-in %s: it can only be shortened, so answers never become staler", class, ttl, limit)
	}
	return nil
}

// newAnswerCacheTTLs validates the settings and keeps the ones that shorten a
// TTL; a class left at 0 or at its built-in TTL is not kept, so its TTLs and
// cache keys stay exactly those of a proxy without the setting.
func newAnswerCacheTTLs(settings map[string]time.Duration) (map[string]time.Duration, error) {
	var out map[string]time.Duration
	for class, ttl := range settings {
		if err := ValidateAnswerCacheTTL(class, ttl); err != nil {
			return nil, err
		}
		if limit, _ := AnswerCacheTTLMax(class); ttl == 0 || ttl == limit {
			continue
		}
		if out == nil {
			out = make(map[string]time.Duration, len(settings))
		}
		out[class] = ttl
	}
	return out, nil
}

// shortenedTTL caps a TTL of an endpoint's entries at the class's setting: its
// built-in TTL, or one that is not the class's own (the negative TTL of an
// empty label list). While the class keeps its built-in TTL it is unchanged.
func (p *Proxy) shortenedTTL(endpoint string, ttl time.Duration) time.Duration {
	if p == nil {
		return ttl
	}
	if short, ok := p.answerCacheTTLs[endpoint]; ok && short < ttl {
		return short
	}
	return ttl
}

// endpointTTL is the TTL the proxy's read caches store an endpoint's answer
// with: CacheTTLs, shortened by the class's setting.
func (p *Proxy) endpointTTL(endpoint string) time.Duration {
	return p.shortenedTTL(endpoint, CacheTTLs[endpoint])
}

// compatAnswerTTL is the Loki answer cache's TTL for an endpoint: the fixed
// 5 minutes of query_range and query, CacheTTLs for the others, shortened by
// the class's setting.
func (p *Proxy) compatAnswerTTL(endpoint string) time.Duration {
	if endpoint == "query_range" || endpoint == "query" {
		return p.shortenedTTL(endpoint, compatAnswerFixedTTL)
	}
	return p.endpointTTL(endpoint)
}

// answerTTLKeyFence is appended to the cache keys of entries stored with
// endpointTTL in the shared tiers (memory, disk, peers): "" while the class
// keeps its built-in TTL, so those keys are unchanged, and "@ttl=<ttl>" once
// a setting shortens it. Entries carry an expiry, not their age, so a copy a
// replica with a longer TTL wrote (a peer, or this replica's disk before a
// restart with a shorter setting) could otherwise be served past the
// shorter TTL. '@' never appears in an encoded query string.
func (p *Proxy) answerTTLKeyFence(endpoint string) string {
	if p == nil || len(p.answerCacheTTLs) == 0 {
		return ""
	}
	builtin := CacheTTLs[endpoint]
	if ttl := p.endpointTTL(endpoint); ttl < builtin {
		return "@ttl=" + ttl.String()
	}
	return ""
}

// detectedLabelsDetectionTTL is the TTL of the label detection cache under
// /detected_labels: 30 s, or the class's shorter setting.
const detectedLabelsDetectionTTL = 30 * time.Second

// detectLabelsTTLKeyFence fences the label detection cache's key once the
// detected_labels setting is below its 30 s.
func (p *Proxy) detectLabelsTTLKeyFence() string {
	if p == nil || len(p.answerCacheTTLs) == 0 {
		return ""
	}
	if ttl := p.shortenedTTL("detected_labels", detectedLabelsDetectionTTL); ttl < detectedLabelsDetectionTTL {
		return "@ttl=" + ttl.String()
	}
	return ""
}

// readCacheKeyTTLFence is the fence of an endpoint's canonical read cache key.
func (p *Proxy) readCacheKeyTTLFence(endpoint string) string {
	if p == nil || len(p.answerCacheTTLs) == 0 || !readCacheKeyFencedByTTL(endpoint) {
		return ""
	}
	return p.answerTTLKeyFence(endpoint)
}

// readCacheKeyFencedByTTL names the endpoints whose canonical read cache key
// addresses an entry stored with endpointTTL. The label list and value
// handlers store theirs with -labels-cache-ttl, a class of its own, so their
// keys are fenced only where the multi-tenant merge stores them (mt: keys).
func readCacheKeyFencedByTTL(endpoint string) bool {
	switch endpoint {
	case "detected_fields", "detected_field_values", "detected_labels", "index_stats", "volume", "volume_range":
		return true
	}
	return false
}
