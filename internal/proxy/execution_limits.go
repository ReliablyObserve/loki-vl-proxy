package proxy

import "context"

// Execution limits bound the work a single request may do in the proxy and the
// bytes it may pull out of VictoriaLogs. Every one of them is an operator flag
// with a Helm value; the defaults here are the built-in values used when a flag
// is left at 0. Hitting a limit is an error naming its flag, never a truncated
// result. See docs/reference/limits-registry.md.
const (
	DefaultBackendMaxBufferedResponseBytes   = 64 << 20  // -backend-max-buffered-response-bytes
	DefaultBinaryMetricMaxOperandBytes       = 256 << 20 // -binary-metric-max-operand-bytes
	DefaultBinaryMetricMaxArrays             = 2_000_000 // -binary-metric-max-arrays
	DefaultMultiTenantMaxFanout              = 64        // -multi-tenant-max-fanout
	DefaultMultiTenantMaxMergedResponseBytes = 32 << 20  // -multi-tenant-max-merged-response-bytes
	DefaultMaxEntriesLimitPerQuery           = 10_000    // -max-entries-limit-per-query
	DefaultDetectedFieldsMaxScanLines        = 2_000     // -detected-fields-max-scan-lines
	DefaultPatternsMaxBackendRows            = 20_000    // -patterns-max-backend-rows
	DefaultPatternsSecondPassMaxRows         = 8_000     // -patterns-second-pass-max-rows
	DefaultPatternsSecondPassMaxWindows      = 8         // -patterns-second-pass-max-windows
	DefaultDrilldownMaxStatsBuckets          = 120       // -drilldown-max-stats-buckets
	DefaultMaxZeroFillBuckets                = 32_768    // -max-zero-fill-buckets
	// DefaultLabelValuesMaxResponseBytes matches the per-response budget of
	// -backend-max-buffered-response-bytes: 16x Loki's default gRPC message
	// limit (4 MiB), about 1.4 million values of a high-churn pod label.
	DefaultLabelValuesMaxResponseBytes = 64 << 20 // -label-values-max-response-bytes
	// DefaultMaxQueryLengthBytes is Loki's syntax.maxInputSize: Loki parses any
	// shorter query, so the proxy must not reject one either.
	DefaultMaxQueryLengthBytes = 128 << 10 // -max-query-length-bytes
)

// executionLimits carries the resolved limits of one proxy instance.
type executionLimits struct {
	BufferedBackendBodyBytes  int
	BinaryOperandBytes        int
	BinaryArrays              int
	MultiTenantFanout         int
	MultiTenantMergedBytes    int
	EntriesPerQuery           int
	DetectedScanLines         int
	PatternsBackendRows       int
	PatternsSecondPassRows    int
	PatternsSecondPassWindows int
	DrilldownStatsBuckets     int
	ZeroFillBuckets           int
	QueryLengthBytes          int
	LabelValuesResponseBytes  int
}

// ExecutionLimitsConfig holds the configured values; 0 selects the default.
type ExecutionLimitsConfig struct {
	BackendMaxBufferedResponseBytes   int
	BinaryMetricMaxOperandBytes       int
	BinaryMetricMaxArrays             int
	MultiTenantMaxFanout              int
	MultiTenantMaxMergedResponseBytes int
	MaxEntriesLimitPerQuery           int
	DetectedFieldsMaxScanLines        int
	PatternsMaxBackendRows            int
	PatternsSecondPassMaxRows         int
	PatternsSecondPassMaxWindows      int
	DrilldownMaxStatsBuckets          int
	MaxZeroFillBuckets                int
	MaxQueryLengthBytes               int
	LabelValuesMaxResponseBytes       int
}

func defaultExecutionLimits() executionLimits {
	return executionLimits{
		BufferedBackendBodyBytes:  DefaultBackendMaxBufferedResponseBytes,
		BinaryOperandBytes:        DefaultBinaryMetricMaxOperandBytes,
		BinaryArrays:              DefaultBinaryMetricMaxArrays,
		MultiTenantFanout:         DefaultMultiTenantMaxFanout,
		MultiTenantMergedBytes:    DefaultMultiTenantMaxMergedResponseBytes,
		EntriesPerQuery:           DefaultMaxEntriesLimitPerQuery,
		DetectedScanLines:         DefaultDetectedFieldsMaxScanLines,
		PatternsBackendRows:       DefaultPatternsMaxBackendRows,
		PatternsSecondPassRows:    DefaultPatternsSecondPassMaxRows,
		PatternsSecondPassWindows: DefaultPatternsSecondPassMaxWindows,
		DrilldownStatsBuckets:     DefaultDrilldownMaxStatsBuckets,
		ZeroFillBuckets:           DefaultMaxZeroFillBuckets,
		QueryLengthBytes:          DefaultMaxQueryLengthBytes,
		LabelValuesResponseBytes:  DefaultLabelValuesMaxResponseBytes,
	}
}

func resolveExecutionLimits(cfg ExecutionLimitsConfig) executionLimits {
	limits := defaultExecutionLimits()
	set := func(dst *int, configured int) {
		if configured > 0 {
			*dst = configured
		}
	}
	set(&limits.BufferedBackendBodyBytes, cfg.BackendMaxBufferedResponseBytes)
	set(&limits.BinaryOperandBytes, cfg.BinaryMetricMaxOperandBytes)
	set(&limits.BinaryArrays, cfg.BinaryMetricMaxArrays)
	set(&limits.MultiTenantFanout, cfg.MultiTenantMaxFanout)
	set(&limits.MultiTenantMergedBytes, cfg.MultiTenantMaxMergedResponseBytes)
	set(&limits.EntriesPerQuery, cfg.MaxEntriesLimitPerQuery)
	set(&limits.DetectedScanLines, cfg.DetectedFieldsMaxScanLines)
	set(&limits.PatternsBackendRows, cfg.PatternsMaxBackendRows)
	set(&limits.PatternsSecondPassRows, cfg.PatternsSecondPassMaxRows)
	set(&limits.PatternsSecondPassWindows, cfg.PatternsSecondPassMaxWindows)
	set(&limits.DrilldownStatsBuckets, cfg.DrilldownMaxStatsBuckets)
	set(&limits.ZeroFillBuckets, cfg.MaxZeroFillBuckets)
	set(&limits.QueryLengthBytes, cfg.MaxQueryLengthBytes)
	set(&limits.LabelValuesResponseBytes, cfg.LabelValuesMaxResponseBytes)
	return limits
}

// limits returns this proxy's resolved limits. Proxy values built directly in
// tests, without New, fall back to the built-in defaults.
func (p *Proxy) limits() executionLimits {
	if p == nil || p.execLimits.EntriesPerQuery <= 0 {
		return defaultExecutionLimits()
	}
	return p.execLimits
}

type executionLimitsKey struct{}

// withExecutionLimits attaches the instance limits to a request context so
// evaluation helpers that take only a context stay bounded by the same values.
func withExecutionLimits(ctx context.Context, limits executionLimits) context.Context {
	return context.WithValue(ctx, executionLimitsKey{}, limits)
}

// executionLimitsFrom returns the limits of the request's proxy instance, or
// the built-in defaults for contexts created outside a request.
func executionLimitsFrom(ctx context.Context) executionLimits {
	if ctx != nil {
		if limits, ok := ctx.Value(executionLimitsKey{}).(executionLimits); ok {
			return limits
		}
	}
	return defaultExecutionLimits()
}
