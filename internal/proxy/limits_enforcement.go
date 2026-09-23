package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// Loki's per-tenant query limits (limits_config, v3.7.7) that the proxy
// enforces. One resolver answers both enforcement and the published limits
// (/config/tenant/v1/limits, /loki/api/v1/drilldown-limits), so what a client
// reads is what the proxy applies. Each value resolves, highest first:
// -tenant-limits[tenant] -> -tenant-default-limits -> the proxy flag -> Loki's
// default.
const (
	limitMaxQuerySeries          = "max_query_series"
	limitMaxEntriesLimitPerQuery = "max_entries_limit_per_query"
	limitMaxQueryLength          = "max_query_length"
	limitMaxQueryLookback        = "max_query_lookback"
	limitMaxQueryRange           = "max_query_range"
	limitQueryTimeout            = "query_timeout"
)

// Loki limits the proxy publishes but cannot enforce: VictoriaLogs reports no
// bytes read before a query runs, and the volume endpoints bound their answer
// by the request limit instead of failing above a series count. An override
// would publish a limit nothing applies, so only Loki's disabled or default
// value is accepted (validateTenantLimitOverrides).
var unenforcedTenantLimits = map[string]any{
	"max_query_bytes_read":   "0B",
	"max_querier_bytes_read": "0B",
	"volume_max_series":      1000,
}

// queryLimits are the Loki limits one request is held to.
type queryLimits struct {
	MaxQuerySeries          int           // max_query_series
	MaxEntriesLimitPerQuery int           // max_entries_limit_per_query; 0 = unlimited
	MaxQueryLength          time.Duration // max_query_length; 0 = unlimited
	MaxQueryLookback        time.Duration // max_query_lookback; 0 = unlimited
	MaxQueryRange           time.Duration // max_query_range; 0 = unlimited
	QueryTimeout            time.Duration // query_timeout; 0 = none
	// QueryTimeoutOverride is set when -tenant-limits or -tenant-default-limits
	// sets query_timeout: the whole request then runs under that deadline.
	// Otherwise query_timeout is -backend-timeout, which bounds each
	// VictoriaLogs call.
	QueryTimeoutOverride bool
}

// tenantQueryLimits resolves the limits of one tenant.
func (p *Proxy) tenantQueryLimits(orgID string) queryLimits {
	limits := queryLimits{
		MaxQuerySeries:          p.flagMaxQuerySeries(),
		MaxEntriesLimitPerQuery: p.limits().EntriesPerQuery,
		QueryTimeout:            p.backendTimeout(),
	}
	if p == nil {
		return limits
	}
	limits.MaxQueryLength = p.defaultMaxQueryLength
	p.configMu.RLock()
	tenant := p.tenantLimits[orgID]
	defaults := p.tenantDefaultLimits
	p.configMu.RUnlock()
	if len(tenant) == 0 && len(defaults) == 0 {
		return limits
	}
	lookup := func(key string) (any, bool) {
		if v, ok := tenant[key]; ok {
			return v, true
		}
		v, ok := defaults[key]
		return v, ok
	}
	// Values were validated at startup (validateTenantLimitOverrides); a value
	// set directly on a Proxy that does not parse keeps the flag's value.
	if v, ok := lookup(limitMaxQuerySeries); ok {
		if n, err := tenantLimitInt(v); err == nil && n > 0 {
			limits.MaxQuerySeries = n
		}
	}
	if v, ok := lookup(limitMaxEntriesLimitPerQuery); ok {
		// 0 is unlimited, as in Loki.
		if n, err := tenantLimitInt(v); err == nil && n >= 0 {
			limits.MaxEntriesLimitPerQuery = n
		}
	}
	duration := func(key string, dst *time.Duration) {
		if v, ok := lookup(key); ok {
			if d, err := tenantLimitDuration(v); err == nil {
				*dst = d
			}
		}
	}
	duration(limitMaxQueryLength, &limits.MaxQueryLength)
	duration(limitMaxQueryLookback, &limits.MaxQueryLookback)
	duration(limitMaxQueryRange, &limits.MaxQueryRange)
	if v, ok := lookup(limitQueryTimeout); ok {
		if d, err := tenantLimitDuration(v); err == nil && d > 0 {
			limits.QueryTimeout = d
			limits.QueryTimeoutOverride = true
		}
	}
	return limits
}

// queryLimitsFor resolves the limits of an X-Scope-OrgID. A multi-tenant
// header combines its tenants the way Loki does: the smallest max_query_series
// (validation.SmallestPositiveIntPerTenant) and the smallest non-zero value of
// every other limit (SmallestPositiveNonZeroIntPerTenant and
// SmallestPositiveNonZeroDurationPerTenant), zero meaning unlimited.
func (p *Proxy) queryLimitsFor(orgID string) queryLimits {
	orgID = strings.TrimSpace(orgID)
	if !hasMultiTenantOrgID(orgID) {
		return p.tenantQueryLimits(orgID)
	}
	tenants := splitMultiTenantOrgIDs(orgID)
	if len(tenants) == 0 {
		return p.tenantQueryLimits("")
	}
	combined := p.tenantQueryLimits(tenants[0])
	for _, tenant := range tenants[1:] {
		limits := p.tenantQueryLimits(tenant)
		if limits.MaxQuerySeries < combined.MaxQuerySeries {
			combined.MaxQuerySeries = limits.MaxQuerySeries
		}
		combined.MaxEntriesLimitPerQuery = smallestNonZero(combined.MaxEntriesLimitPerQuery, limits.MaxEntriesLimitPerQuery)
		combined.MaxQueryLength = smallestNonZero(combined.MaxQueryLength, limits.MaxQueryLength)
		combined.MaxQueryLookback = smallestNonZero(combined.MaxQueryLookback, limits.MaxQueryLookback)
		combined.MaxQueryRange = smallestNonZero(combined.MaxQueryRange, limits.MaxQueryRange)
		combined.QueryTimeout = smallestNonZero(combined.QueryTimeout, limits.QueryTimeout)
		combined.QueryTimeoutOverride = combined.QueryTimeoutOverride || limits.QueryTimeoutOverride
	}
	return combined
}

func smallestNonZero[T int | time.Duration](a, b T) T {
	switch {
	case a <= 0:
		return b
	case b <= 0 || a < b:
		return a
	}
	return b
}

// requestQueryLimits returns the limits of the request's tenant.
func (p *Proxy) requestQueryLimits(ctx context.Context) queryLimits {
	return p.queryLimitsFor(getOrgID(ctx))
}

// flagMaxQuerySeries is -max-stats-query-series, or Loki's default 500.
func (p *Proxy) flagMaxQuerySeries() int {
	if p != nil && p.maxStatsQuerySeries > 0 {
		return p.maxStatsQuerySeries
	}
	return defaultStatsQuerySeries
}

// backendTimeout is -backend-timeout, the proxy's query_timeout unless a
// tenant limit sets one.
func (p *Proxy) backendTimeout() time.Duration {
	if p == nil || p.client == nil || p.client.Timeout <= 0 {
		return 0
	}
	return p.client.Timeout
}

// effectiveMaxQueryLength returns the max_query_length of orgID; 0 = unlimited.
func (p *Proxy) effectiveMaxQueryLength(orgID string) time.Duration {
	return p.queryLimitsFor(orgID).MaxQueryLength
}

// lokiQueryTooLongError is Loki's validation.ErrQueryTooLong: the query
// length is a Go duration, the limit a Prometheus model.Duration.
func lokiQueryTooLongError(length, limit time.Duration) string {
	return fmt.Sprintf("the query time range exceeds the limit (query length: %s, limit: %s)", length, formatLokiDuration(limit))
}

// checkQueryRangeLength returns Loki's error when the requested range exceeds
// the request tenant's max_query_length, and "" otherwise.
func (p *Proxy) checkQueryRangeLength(ctx context.Context, startNs, endNs int64) string {
	return queryLengthLimitError(p.requestQueryLimits(ctx).MaxQueryLength, startNs, endNs)
}

func queryLengthLimitError(maxLen time.Duration, startNs, endNs int64) string {
	if maxLen <= 0 || endNs <= startNs {
		return ""
	}
	// Loki compares millisecond timestamps.
	length := time.Duration(endNs/int64(time.Millisecond)-startNs/int64(time.Millisecond)) * time.Millisecond
	if length > maxLen {
		return lokiQueryTooLongError(length, maxLen)
	}
	return ""
}

// formatLokiDuration renders d the way Loki publishes a limit
// (prometheus/common model.Duration.String): "30d1h", "5m", "0s".
func formatLokiDuration(d time.Duration) string {
	ms := int64(d / time.Millisecond)
	if ms == 0 {
		return "0s"
	}
	var b strings.Builder
	if ms < 0 {
		b.WriteByte('-')
		ms = -ms
	}
	unit := func(name string, size int64, exact bool) {
		if exact && ms%size != 0 {
			return
		}
		if v := ms / size; v > 0 {
			b.WriteString(strconv.FormatInt(v, 10))
			b.WriteString(name)
			ms -= v * size
		}
	}
	// Years and weeks only when exact, as Prometheus does (90d, not 12w6d).
	unit("y", 1000*60*60*24*365, true)
	unit("w", 1000*60*60*24*7, true)
	unit("d", 1000*60*60*24, false)
	unit("h", 1000*60*60, false)
	unit("m", 1000*60, false)
	unit("s", 1000, false)
	unit("ms", 1, false)
	return b.String()
}

// publishEnforcedLimits writes the enforced limits of orgID into a published
// limits payload.
func (p *Proxy) publishEnforcedLimits(published map[string]any, orgID string) {
	limits := p.queryLimitsFor(orgID)
	published[limitMaxQuerySeries] = limits.MaxQuerySeries
	published[limitMaxEntriesLimitPerQuery] = limits.MaxEntriesLimitPerQuery
	published[limitMaxQueryLength] = formatLokiDuration(limits.MaxQueryLength)
	published[limitMaxQueryLookback] = formatLokiDuration(limits.MaxQueryLookback)
	published[limitMaxQueryRange] = formatLokiDuration(limits.MaxQueryRange)
	published[limitQueryTimeout] = formatLokiDuration(limits.QueryTimeout)
	for key, value := range unenforcedTenantLimits {
		published[key] = value
	}
}

// tenantLimitInt reads an integer limit from a JSON override.
func tenantLimitInt(v any) (int, error) {
	switch n := v.(type) {
	case int:
		return n, nil
	case int64:
		return int(n), nil
	case float64:
		if n != math.Trunc(n) || n > math.MaxInt32 || n < math.MinInt32 {
			return 0, fmt.Errorf("%v is not an integer", n)
		}
		return int(n), nil
	case json.Number:
		i, err := n.Int64()
		return int(i), err
	case string:
		return strconv.Atoi(strings.TrimSpace(n))
	}
	return 0, fmt.Errorf("%v (%T) is not an integer", v, v)
}

// tenantLimitDuration reads a Loki duration ("5m", "30d1h", "0s") from a JSON
// override.
func tenantLimitDuration(v any) (time.Duration, error) {
	s, ok := v.(string)
	if !ok {
		return 0, fmt.Errorf("%v (%T) is not a duration string such as \"5m\"", v, v)
	}
	s = strings.TrimSpace(s)
	if s == "0" || s == "0s" {
		return 0, nil
	}
	d := parseLokiDuration(s)
	if d <= 0 {
		return 0, fmt.Errorf("%q is not a positive duration", s)
	}
	return d, nil
}

// validateTenantLimitOverrides rejects -tenant-default-limits and
// -tenant-limits values the proxy would not enforce as published. A
// query_timeout above -backend-timeout is one of them: every VictoriaLogs call
// is still bounded by -backend-timeout.
func validateTenantLimitOverrides(defaults map[string]any, tenants map[string]map[string]any, backendTimeout time.Duration) error {
	check := func(scope string, overrides map[string]any) error {
		keys := make([]string, 0, len(overrides))
		for key := range overrides {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			value := overrides[key]
			switch key {
			case limitMaxQuerySeries:
				n, err := tenantLimitInt(value)
				if err != nil {
					return fmt.Errorf("%s %s: %v", scope, key, err)
				}
				if n <= 0 {
					// Loki accepts 0 and then fails every metric query that
					// returns a series.
					return fmt.Errorf("%s %s: must be a positive integer, got %d", scope, key, n)
				}
			case limitMaxEntriesLimitPerQuery:
				n, err := tenantLimitInt(value)
				if err != nil {
					return fmt.Errorf("%s %s: %v", scope, key, err)
				}
				if n < 0 {
					return fmt.Errorf("%s %s: must be 0 (unlimited) or a positive integer, got %d", scope, key, n)
				}
			case limitMaxQueryLength, limitMaxQueryLookback, limitMaxQueryRange:
				if _, err := tenantLimitDuration(value); err != nil {
					return fmt.Errorf("%s %s: %v", scope, key, err)
				}
			case limitQueryTimeout:
				d, err := tenantLimitDuration(value)
				if err != nil {
					return fmt.Errorf("%s %s: %v", scope, key, err)
				}
				if d <= 0 {
					return fmt.Errorf("%s %s: must be a positive duration", scope, key)
				}
				if backendTimeout > 0 && d > backendTimeout {
					return fmt.Errorf("%s %s: %s is above -backend-timeout (%s), which bounds every VictoriaLogs call; raise -backend-timeout to at least %s", scope, key, formatLokiDuration(d), formatLokiDuration(backendTimeout), formatLokiDuration(d))
				}
			default:
				want, unenforced := unenforcedTenantLimits[key]
				if unenforced && fmt.Sprint(normalizeUnenforcedLimit(value)) != fmt.Sprint(want) {
					return fmt.Errorf("%s %s: the proxy does not enforce this limit, so it publishes only %v; remove the override", scope, key, want)
				}
			}
		}
		return nil
	}
	if err := check("-tenant-default-limits", defaults); err != nil {
		return err
	}
	tenantIDs := make([]string, 0, len(tenants))
	for tenant := range tenants {
		tenantIDs = append(tenantIDs, tenant)
	}
	sort.Strings(tenantIDs)
	for _, tenant := range tenantIDs {
		if err := check(fmt.Sprintf("-tenant-limits[%q]", tenant), tenants[tenant]); err != nil {
			return err
		}
	}
	return nil
}

func normalizeUnenforcedLimit(v any) any {
	switch n := v.(type) {
	case float64:
		if n == math.Trunc(n) {
			return int(n)
		}
	case string:
		if s := strings.TrimSpace(n); s == "0" || s == "0B" {
			return "0B"
		}
	}
	return v
}

// tenantLimitsMiddleware applies Loki's query limits (limitsMiddleware,
// validateMaxEntriesLimits, checkIntervalLimit and the per-tenant
// query_timeout) before a request reaches its handler, with the combined
// limits of every tenant of a multi-tenant request. A request no limit can
// affect passes without being parsed.
func (p *Proxy) tenantLimitsMiddleware(endpoint string, next http.HandlerFunc) http.HandlerFunc {
	lookback := tenantLimitsLookbackEndpoints[endpoint]
	timeout := tenantLimitsTimeoutEndpoints[endpoint]
	query := endpoint == "query" || endpoint == "query_range"
	return func(w http.ResponseWriter, r *http.Request) {
		limits := p.queryLimitsFor(r.Header.Get("X-Scope-OrgID"))
		if timeout && limits.QueryTimeoutOverride && limits.QueryTimeout > 0 {
			ctx, cancel := context.WithTimeout(r.Context(), limits.QueryTimeout)
			defer cancel()
			r = r.WithContext(ctx)
		}
		entries := query && entryLimitAbove(r, limits.MaxEntriesLimitPerQuery)
		interval := query && limits.MaxQueryRange > 0
		window := lookback && (limits.MaxQueryLookback > 0 || (limits.MaxQueryLength > 0 && endpoint != "query"))
		if !entries && !interval && !window {
			next(w, r)
			return
		}
		reject := func(msg string) {
			p.writeError(w, http.StatusBadRequest, msg)
			p.metrics.RecordRequest(endpoint, http.StatusBadRequest, 0)
		}
		// Loki's order: decode and parse, max_entries_limit_per_query in the
		// frontend round-tripper, the limits middleware (lookback, then
		// length), and max_query_range when the engine evaluates the query.
		var expr logqlpkg.Expr
		if query {
			var ok bool
			if expr, ok = parseLimitedQuery(r, endpoint); !ok {
				// The handler answers it with the resolution or parse error.
				next(w, r)
				return
			}
			if entries {
				var msg string
				if r, msg = p.applyMaxEntriesLimit(r, expr, limits.MaxEntriesLimitPerQuery); msg != "" {
					reject(msg)
					return
				}
			}
			// Loki's frontend sends an instant log query straight to the
			// querier, past its limits middleware.
			if endpoint == "query" && !isSampleExpr(expr) {
				window = false
			}
		}
		if window {
			rewritten, empty, msg := applyLookbackAndLength(r, endpoint, limits, time.Now())
			if msg != "" {
				reject(msg)
				return
			}
			if empty != nil {
				p.writeJSON(w, empty)
				p.metrics.RecordRequest(endpoint, http.StatusOK, 0)
				return
			}
			r = rewritten
		}
		if interval {
			if msg := maxQueryRangeError(expr, limits.MaxQueryRange); msg != "" {
				reject(msg)
				return
			}
		}
		next(w, r)
	}
}

// tenantLimitsTimeoutEndpoints are the requests Loki runs under the tenant's
// query_timeout (WrapQuerySpanAndTimeout in pkg/loki/modules.go). Tail,
// patterns and the other routes are not bounded by it.
var tenantLimitsTimeoutEndpoints = map[string]bool{
	"query":        true,
	"query_range":  true,
	"series":       true,
	"labels":       true,
	"label_values": true,
	"index_stats":  true,
	"volume":       true,
	"volume_range": true,
}

// parseLimitedQuery parses a query or query_range request the way Loki
// decodes it: nil and false when it exceeds the 11,000-point resolution or
// does not parse.
func parseLimitedQuery(r *http.Request, endpoint string) (logqlpkg.Expr, bool) {
	if endpoint == "query_range" && exceedsLokiRangeResolution(r, time.Now()) {
		return nil, false
	}
	query := resolveGrafanaRangeTemplateTokens(r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), r.FormValue("step"))
	expr, err := logqlpkg.Parse(query)
	return expr, err == nil
}

// tenantLimitsLookbackEndpoints are the requests Loki's limitsMiddleware
// covers (NewLimitsMiddleware in the log, metric, instant metric, series,
// labels, detected labels and index stats tripperwares): max_query_lookback
// moves their start and max_query_length bounds their range.
var tenantLimitsLookbackEndpoints = map[string]bool{
	"query":           true,
	"query_range":     true,
	"series":          true,
	"labels":          true,
	"label_values":    true,
	"detected_labels": true,
	"index_stats":     true,
}

// lokiMaxEntriesErrorTemplate is Loki's max_entries_limit_per_query rejection
// (pkg/querier/http.go validateMaxEntriesLimits), HTTP 400.
const lokiMaxEntriesErrorTemplate = "max entries limit per query exceeded, limit > max_entries_limit_per_query (%d > %d)"

// entryLimitAbove reports whether the request's limit parameter exceeds a
// non-zero max_entries_limit_per_query.
func entryLimitAbove(r *http.Request, maxEntries int) bool {
	if maxEntries <= 0 {
		return false
	}
	limit, err := strconv.ParseUint(strings.TrimSpace(r.FormValue("limit")), 10, 32)
	return err == nil && limit > uint64(maxEntries)
}

// isSampleExpr reports whether expr evaluates to samples, which carry no entry
// limit.
func isSampleExpr(expr logqlpkg.Expr) bool {
	switch expr.(type) {
	case *logqlpkg.RangeAggregation, *logqlpkg.VectorAggregation, *logqlpkg.BinOpExpr, *logqlpkg.OpaqueMetricExpr, *logqlpkg.LiteralExpr:
		return true
	}
	return false
}

// applyMaxEntriesLimit enforces max_entries_limit_per_query on a log query:
// Loki rejects a larger client limit; -max-entries-limit-per-query-cap keeps
// the proxy's earlier behaviour of lowering it instead. Metric queries carry no
// entry limit.
func (p *Proxy) applyMaxEntriesLimit(r *http.Request, expr logqlpkg.Expr, maxEntries int) (*http.Request, string) {
	if !entryLimitAbove(r, maxEntries) || isSampleExpr(expr) {
		return r, ""
	}
	if p != nil && p.maxEntriesLimitCap {
		return withRequestParams(r, map[string]string{"limit": strconv.Itoa(maxEntries)}), ""
	}
	limit, _ := strconv.ParseUint(strings.TrimSpace(r.FormValue("limit")), 10, 32)
	return r, fmt.Sprintf(lokiMaxEntriesErrorTemplate, limit, maxEntries)
}

// maxQueryRangeError is Loki's checkIntervalLimit: a metric query whose range
// selector ([5m]) is longer than max_query_range fails with 400.
func maxQueryRangeError(expr logqlpkg.Expr, maxRange time.Duration) string {
	if maxRange <= 0 {
		return ""
	}
	// Loki walks every range selector and reports the last one over the limit.
	var msg string
	check := func(raw string) {
		if interval := parseLokiDuration(raw); interval > maxRange {
			msg = fmt.Sprintf("[interval] value exceeds limit: [%s] > [%s]", formatLokiDuration(interval), formatLokiDuration(maxRange))
		}
	}
	var visit func(logqlpkg.Expr)
	visit = func(e logqlpkg.Expr) {
		switch n := e.(type) {
		case *logqlpkg.RangeAggregation:
			check(n.Range)
		case *logqlpkg.VectorAggregation:
			visit(n.Inner)
		case *logqlpkg.BinOpExpr:
			visit(n.Left)
			visit(n.Right)
		case *logqlpkg.OpaqueMetricExpr:
			// label_replace(...) and the other functions kept as text: parse
			// their inner metric expressions from the raw form.
			for _, m := range opaqueRangeSelectorRE.FindAllStringSubmatch(n.String(), -1) {
				check(m[1])
			}
		}
	}
	visit(expr)
	return msg
}

// opaqueRangeSelectorRE finds the [range] of each range selector in the raw
// text of an opaque metric expression. A quoted filter holding exactly "[5m]"
// would match too; such a filter is rare enough to accept.
var opaqueRangeSelectorRE = regexp.MustCompile(`\[([0-9]+(?:ms|[smhdwy])(?:[0-9]+(?:ms|[smhdwy]))*)\]`)

// applyLookbackAndLength is Loki's limitsMiddleware: with max_query_lookback
// set, a start before now-lookback moves to it, and a request ending before it
// is answered empty without running; a range longer than max_query_length
// fails with Loki's error. An instant query covers only its time.
func applyLookbackAndLength(r *http.Request, endpoint string, limits queryLimits, now time.Time) (*http.Request, map[string]interface{}, string) {
	startKey, endKey := "start", "end"
	if endpoint == "query" {
		startKey, endKey = "time", "time"
	}
	startNs, startOK := parseLokiTimeToUnixNano(r.FormValue(startKey))
	endNs, endOK := parseLokiTimeToUnixNano(r.FormValue(endKey))
	if limits.MaxQueryLookback > 0 {
		minStart := now.Add(-limits.MaxQueryLookback).UnixNano()
		if endOK && endNs < minStart {
			return r, emptyLimitedResponse(endpoint, r.FormValue("query")), ""
		}
		if startOK && startNs < minStart && endpoint != "query" {
			r = withRequestParams(r, map[string]string{startKey: strconv.FormatInt(minStart, 10)})
			startNs = minStart
		}
	}
	if endpoint != "query" && startOK && endOK {
		if msg := queryLengthLimitError(limits.MaxQueryLength, startNs, endNs); msg != "" {
			return r, nil, msg
		}
	}
	return r, nil, ""
}

// emptyLimitedResponse is Loki's NewEmptyResponse for a request that lies
// wholly before max_query_lookback.
func emptyLimitedResponse(endpoint, query string) map[string]interface{} {
	resp := emptyMultiTenantResponse(endpoint)
	if (endpoint == "query" || endpoint == "query_range") && logQLProducesSamples(query) {
		resultType := "matrix"
		if endpoint == "query" {
			resultType = "vector"
		}
		resp["data"] = map[string]interface{}{"resultType": resultType, "result": []interface{}{}, "stats": map[string]interface{}{}}
	}
	return resp
}

// withRequestParams returns a copy of r with the given query parameters
// replaced, in the URL and in the parsed forms.
func withRequestParams(r *http.Request, params map[string]string) *http.Request {
	clone := r.Clone(r.Context())
	_ = clone.ParseForm()
	values := clone.URL.Query()
	for key, value := range params {
		values.Set(key, value)
		clone.Form.Set(key, value)
		if clone.PostForm != nil && clone.PostForm.Has(key) {
			clone.PostForm.Set(key, value)
		}
	}
	clone.URL.RawQuery = values.Encode()
	return clone
}
