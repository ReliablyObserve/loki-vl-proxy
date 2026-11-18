package proxy

import (
	"context"
	"time"
)

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
	DefaultMaxLines                          = 100       // -max-lines: Loki's default limit
	DefaultDetectedFieldsMaxScanLines        = 2_000     // -detected-fields-max-scan-lines
	DefaultPatternsMaxBackendRows            = 20_000    // -patterns-max-backend-rows
	DefaultPatternsSecondPassMaxRows         = 8_000     // -patterns-second-pass-max-rows
	DefaultPatternsSecondPassMaxWindows      = 8         // -patterns-second-pass-max-windows
	DefaultPatternsMaxCountsRows             = 250_000   // -patterns-max-counts-rows
	DefaultDrilldownMaxStatsBuckets          = 120       // -drilldown-max-stats-buckets
	DefaultMaxZeroFillBuckets                = 32_768    // -max-zero-fill-buckets
	// DefaultLabelValuesMaxResponseBytes matches the per-response budget of
	// -backend-max-buffered-response-bytes: 16x Loki's default gRPC message
	// limit (4 MiB), about 1.4 million values of a high-churn pod label.
	DefaultLabelValuesMaxResponseBytes = 64 << 20 // -label-values-max-response-bytes
	// DefaultMaxQueryLengthBytes is Loki's syntax.maxInputSize: Loki parses any
	// shorter query, so the proxy must not reject one either.
	DefaultMaxQueryLengthBytes = 128 << 10 // -max-query-length-bytes
	// DefaultMaxQueryPlanTime is -max-query-plan-time. The slowest planning work measured (as of 2026-11-18, commit
	// 8e80a32b7; registry decision query-complexity-limits) is the string translator on 26,000 nested sum( calls:
	// 3.3 s on a quiet host, 6.2 s under load, 5.0 s under -race; the slowest plan corpus row takes 0.4 ms. The default
	// is above that and comes down once nested vector aggregations are translated by the query plan.
	DefaultMaxQueryPlanTime = 10 * time.Second // -max-query-plan-time
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
	PatternsCountsRows        int
	DrilldownStatsBuckets     int
	ZeroFillBuckets           int
	QueryLengthBytes          int
	LabelValuesResponseBytes  int
	// QueryASTNodes and QueryNestingDepth are the operator limits on the size of a parsed query
	// (-max-query-ast-nodes, -max-query-nesting-depth); unset they are QueryLengthBytes, which no query that passed the
	// length limit can exceed (logql.Complexity). QueryPlanTime is -max-query-plan-time; 0 turns the budget off.
	QueryASTNodes     int
	QueryNestingDepth int
	QueryPlanTime     time.Duration
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
	PatternsMaxCountsRows             int
	DrilldownMaxStatsBuckets          int
	MaxZeroFillBuckets                int
	MaxQueryLengthBytes               int
	LabelValuesMaxResponseBytes       int
	// MaxQueryASTNodes and MaxQueryNestingDepth: 0 derives the limit from MaxQueryLengthBytes.
	MaxQueryASTNodes     int
	MaxQueryNestingDepth int
	// MaxQueryPlanTime: 0 selects the default, a negative value turns the budget off.
	MaxQueryPlanTime time.Duration
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
		PatternsCountsRows:        DefaultPatternsMaxCountsRows,
		DrilldownStatsBuckets:     DefaultDrilldownMaxStatsBuckets,
		ZeroFillBuckets:           DefaultMaxZeroFillBuckets,
		QueryLengthBytes:          DefaultMaxQueryLengthBytes,
		LabelValuesResponseBytes:  DefaultLabelValuesMaxResponseBytes,
		QueryASTNodes:             DefaultMaxQueryLengthBytes,
		QueryNestingDepth:         DefaultMaxQueryLengthBytes,
		QueryPlanTime:             DefaultMaxQueryPlanTime,
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
	set(&limits.PatternsCountsRows, cfg.PatternsMaxCountsRows)
	set(&limits.DrilldownStatsBuckets, cfg.DrilldownMaxStatsBuckets)
	set(&limits.ZeroFillBuckets, cfg.MaxZeroFillBuckets)
	set(&limits.QueryLengthBytes, cfg.MaxQueryLengthBytes)
	set(&limits.LabelValuesResponseBytes, cfg.LabelValuesMaxResponseBytes)
	// Unset, the node and depth limits are the query length limit in effect: a query is at most that many bytes and
	// has at most as many nodes and levels as bytes (logql.Complexity), so the derived limits never refuse a query.
	limits.QueryASTNodes, limits.QueryNestingDepth = limits.QueryLengthBytes, limits.QueryLengthBytes
	set(&limits.QueryASTNodes, cfg.MaxQueryASTNodes)
	set(&limits.QueryNestingDepth, cfg.MaxQueryNestingDepth)
	switch {
	case cfg.MaxQueryPlanTime > 0:
		limits.QueryPlanTime = cfg.MaxQueryPlanTime
	case cfg.MaxQueryPlanTime < 0:
		limits.QueryPlanTime = 0
	}
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
