package proxy

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
)

// The flags behind the proxy's limit answers. A client reads Loki's text for
// the limit's class and never a flag: the flag and its value go to the request
// error log line (limit_flag, limit_value), through the typed error's Limit.
const (
	flagBackendMaxBufferedResponseBytes = "backend-max-buffered-response-bytes"
	flagLabelValuesMaxResponseBytes     = "label-values-max-response-bytes"
	flagManualRangeMetricRowLimit       = "manual-range-metric-row-limit"
	flagMaxStatsQuerySeries             = "max-stats-query-series"
	flagOrderedJSONMetricMaxBytes       = "ordered-json-metric-max-bytes"
	flagMaxQueryLengthBytes             = "max-query-length-bytes"
	flagMultiTenantMaxFanout            = "multi-tenant-max-fanout"
	flagMultiTenantMaxMergedBytes       = "multi-tenant-max-merged-response-bytes"
	flagBinaryMetricMaxArrays           = "binary-metric-max-arrays"
	flagBinaryMetricMaxOperandBytes     = "binary-metric-max-operand-bytes"
	flagMaxConcurrent                   = "max-concurrent"
	flagRateLimitPerSecond              = "rate-limit-per-second"
)

// errManualMetricResponseTooLarge marks the typed error of a proxy-evaluated
// metric whose response outgrew -backend-max-buffered-response-bytes, for the
// callers that retry such a request over smaller parts (errors.Is).
var errManualMetricResponseTooLarge = errors.New("manual metric response exceeds the buffered response limit")

// limitCause is what a limit error counted, for the request error log line
// only: operator wording that names the flag. It reports Is for one sentinel
// so callers can still tell the limit apart without reading the text.
type limitCause struct {
	msg  string
	kind error
}

func (c *limitCause) Error() string { return c.msg }

func (c *limitCause) Is(target error) bool { return c.kind != nil && target == c.kind }

// newBufferLimitError is a response the proxy would buffer above limit bytes:
// Loki's answer to a response over its message size limit (500 ResourceExhausted),
// the flag and the operator wording in the log line only. read is the size seen
// when the read stopped, at least limit+1.
func newBufferLimitError(flag string, read, limit int64, what string, kind error) error {
	return lokierr.ResponseTooLarge(flag, read, limit,
		&limitCause{msg: fmt.Sprintf("%s exceeds %d bytes; narrow the query or increase -%s", what, limit, flag), kind: kind})
}

// newManualMetricResponseTooLarge is a proxy-evaluated metric response above
// the bytes the proxy buffers for it.
func newManualMetricResponseTooLarge(limit int64) error {
	return newBufferLimitError(flagBackendMaxBufferedResponseBytes, limit+1, limit, "manual metric response", errManualMetricResponseTooLarge)
}

// newBackendBodyTooLarge is a backend response above the bytes the proxy
// buffers for it. It matches errBodyTooLarge (errors.Is), so the paths that
// degrade on that sentinel keep doing so.
func newBackendBodyTooLarge(limit int64) error {
	return lokierr.ResponseTooLarge(flagBackendMaxBufferedResponseBytes, limit+1, limit, &backendBodyTooLargeError{limit: limit})
}

// newLabelValuesTooLarge is a label values response above the cap read from
// VictoriaLogs; read is the size seen when the read stopped.
func newLabelValuesTooLarge(read, limit int64) error {
	return lokierr.ResponseTooLarge(flagLabelValuesMaxResponseBytes, read, limit, &labelValuesResponseTooLargeError{read: read, limit: limit})
}

// newRowLimitError is a metric evaluation past -manual-range-metric-row-limit.
func newRowLimitError(limit int, unit string, matches int64) error {
	return lokierr.LimitReached(flagManualRangeMetricRowLimit, int64(limit), &rowLimitError{limit: limit, unit: unit, matches: matches})
}

// newBinaryBudgetError is a binary expression past one of its evaluation
// budgets; flag names the budget's flag.
func newBinaryBudgetError(flag string) error {
	return lokierr.LimitReached(flag, 0, &binaryBudgetError{flag: flag})
}

// newHeavyQueueFull is Loki's queue-full answer for a backend call that
// waited its whole queue time; the typed cause says which queue and why.
func newHeavyQueueFull(cause *heavyQueryQueueFullError) error {
	flag := cause.limitFlag
	if flag == "" {
		flag = heavyQueryLimitFlag
	}
	return lokierr.QueueFull(cause).WithLimit(trimFlagDash(flag), int64(cause.maxConcurrent))
}

// trimFlagDash is flag without its leading dash.
func trimFlagDash(flag string) string {
	if len(flag) > 0 && flag[0] == '-' {
		return flag[1:]
	}
	return flag
}

// logGate lets one log line through per second. The zero value is ready.
type logGate struct{ next atomic.Int64 }

// allow reports whether a line may be written at now, and reserves the next
// second when it may.
func (g *logGate) allow(now time.Time) bool {
	t := now.UnixNano()
	for {
		next := g.next.Load()
		if t < next {
			return false
		}
		if g.next.CompareAndSwap(next, t+int64(time.Second)) {
			return true
		}
	}
}
