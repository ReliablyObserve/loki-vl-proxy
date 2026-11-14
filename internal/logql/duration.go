package logql

import (
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var durationPartRE = regexp.MustCompile(`(?i)([0-9]*\.?[0-9]+)(ns|us|µs|ms|s|m|h|d|w|y)`)

// ParseUnitDuration reads a duration written as a sum of number-unit parts
// ("1d", "1.5h", "2w3d", "1y"; units ns, us, µs, ms, s, m, h, d, w, y in any
// case), which time.ParseDuration does not accept for d, w and y. ok is false
// for anything else, a part that is not positive, or an overflow.
func ParseUnitDuration(value string) (time.Duration, bool) {
	parts := durationPartRE.FindAllStringSubmatch(value, -1)
	if len(parts) == 0 {
		return 0, false
	}
	totalSeconds := 0.0
	var consumed strings.Builder
	for _, part := range parts {
		numeric, err := strconv.ParseFloat(part[1], 64)
		if err != nil || numeric <= 0 {
			return 0, false
		}
		switch strings.ToLower(part[2]) {
		case "ns":
			totalSeconds += numeric / 1e9
		case "us", "µs":
			totalSeconds += numeric / 1e6
		case "ms":
			totalSeconds += numeric / 1e3
		case "s":
			totalSeconds += numeric
		case "m":
			totalSeconds += numeric * 60
		case "h":
			totalSeconds += numeric * 3600
		case "d":
			totalSeconds += numeric * 86400
		case "w":
			totalSeconds += numeric * 7 * 86400
		case "y":
			totalSeconds += numeric * 365 * 86400
		}
		consumed.WriteString(part[0])
	}
	if consumed.String() != value {
		return 0, false
	}
	nanos := totalSeconds * float64(time.Second)
	if nanos <= 0 || nanos > float64(math.MaxInt64) {
		return 0, false
	}
	return time.Duration(nanos), true
}
