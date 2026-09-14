package proxy

import (
	"regexp"
	"strconv"
	"strings"
)

// parseDuration converts a Loki-style duration string to seconds.
// Supports: ns, us, ms, s, m, h, d
// Examples: "100ms" → 0.1, "1.5s" → 1.5, "2m30s" → 150, "1h" → 3600
func parseDuration(s string) (float64, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}

	// Try simple numeric (already seconds)
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f, true
	}

	// Parse compound duration like "1h2m3s"
	re := regexp.MustCompile(`(\d+\.?\d*)(ns|us|µs|ms|s|m|h|d)`)
	matches := re.FindAllStringSubmatch(s, -1)
	if len(matches) == 0 {
		return 0, false
	}

	var total float64
	for _, m := range matches {
		val, _ := strconv.ParseFloat(m[1], 64)
		switch m[2] {
		case "ns":
			total += val / 1e9
		case "us", "µs":
			total += val / 1e6
		case "ms":
			total += val / 1e3
		case "s":
			total += val
		case "m":
			total += val * 60
		case "h":
			total += val * 3600
		case "d":
			total += val * 86400
		}
	}
	return total, true
}

// convertUnwrapValue applies the LogQL unwrap conversion function to a raw
// field value: duration() and bytes() parse Loki-style units, an absent
// conversion parses a plain number. Shared by every raw-sample path so all of
// them accept the same inputs as Loki.
func convertUnwrapValue(value, conv string) (float64, bool) {
	switch conv {
	case "duration":
		return parseDuration(value)
	case "bytes":
		return parseBytes(value)
	default:
		f, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		return f, err == nil
	}
}

// parseBytes converts a Loki-style byte string to bytes.
// Supports: B, KB, KiB, MB, MiB, GB, GiB, TB, TiB
// Examples: "1.5KiB" → 1536, "100MB" → 100000000
func parseBytes(s string) (float64, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}

	// Try simple numeric (already bytes)
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f, true
	}

	re := regexp.MustCompile(`^(\d+\.?\d*)\s*(B|KB|KiB|MB|MiB|GB|GiB|TB|TiB)$`)
	m := re.FindStringSubmatch(s)
	if m == nil {
		return 0, false
	}

	val, _ := strconv.ParseFloat(m[1], 64)
	switch m[2] {
	case "B":
		return val, true
	case "KB":
		return val * 1000, true
	case "KiB":
		return val * 1024, true
	case "MB":
		return val * 1e6, true
	case "MiB":
		return val * 1024 * 1024, true
	case "GB":
		return val * 1e9, true
	case "GiB":
		return val * 1024 * 1024 * 1024, true
	case "TB":
		return val * 1e12, true
	case "TiB":
		return val * 1024 * 1024 * 1024 * 1024, true
	}
	return val, true
}
