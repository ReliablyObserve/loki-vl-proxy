package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Heavy backend admission bounds how much expensive VictoriaLogs work the proxy
// runs at once. VictoriaLogs lets every stats, sort and uniq pipe of a single
// query grow to 40% of its allowed memory and does not account for concurrent
// queries, so a handful of long-range stats or raw-row fetches running together
// can exhaust its memory. The limiter admits a bounded number of heavy calls per
// replica, queues the rest for a bounded time and then answers like Loki's
// query scheduler when its queue is full.

// Flag defaults for heavy VictoriaLogs query admission.
const (
	// DefaultBackendMaxConcurrentHeavyQueries keeps two heavy stats states,
	// each allowed 40% of VictoriaLogs memory, within its memory budget.
	DefaultBackendMaxConcurrentHeavyQueries = 2
	// DefaultBackendHeavyQueryQueueWait lets a Grafana Logs Drilldown page load
	// over 7d (about 30 parallel heavy calls) finish below Grafana's 30s data
	// source timeout.
	DefaultBackendHeavyQueryQueueWait = 20 * time.Second
	// DefaultBackendHeavyQueryMinRange marks stats over a quarter day as heavy.
	DefaultBackendHeavyQueryMinRange = 6 * time.Hour
	// DefaultBackendMaxConcurrentMetadataScans bounds long-range metadata
	// listings (stream field names and values, field names and values, stream
	// listings) per replica. Measured on VictoriaLogs v1.52.0 over a 7-day
	// retention: one stream_field_names scan holds about 0.8 GiB and takes
	// 8.8 s; three at once hold 2 GiB more and each takes 16 s, so concurrency
	// beyond two mostly buys memory, not throughput.
	DefaultBackendMaxConcurrentMetadataScans = 2
)

// Flag names and the work each admission limiter bounds, as they appear in
// the 429 message so an operator knows which flag to raise.
const (
	heavyQueryLimitFlag   = "-backend-max-concurrent-heavy-queries"
	heavyQueryLimitWork   = "heavy VictoriaLogs queries"
	metadataScanLimitFlag = "-backend-max-concurrent-metadata-scans"
	metadataScanLimitWork = "long-range VictoriaLogs metadata scans"
)

const (
	// heavyQueryRowThreshold separates log-line fetches, bounded by Loki's
	// max_entries_limit_per_query (5000 by default), from raw-row metric
	// evaluation fetches that can pull up to -manual-range-metric-row-limit rows.
	heavyQueryRowThreshold = 10_000
	// heavyQueryBucketThreshold matches Loki's 11,000-point resolution limit:
	// bucket grids finer than any Loki result are proxy-internal and expensive.
	heavyQueryBucketThreshold = 11_000
)

// lokiTooManyOutstandingRequests is the message Loki's query frontend and
// scheduler return with HTTP 429 when their queue is full
// (pkg/queue/queue.go ErrTooManyRequests).
const lokiTooManyOutstandingRequests = "too many outstanding requests"

// heavyQueryQueueFullError reports that a heavy backend call waited in the
// admission queue for the full configured time.
type heavyQueryQueueFullError struct {
	limitFlag     string // the flag that bounds this kind of work
	work          string // what the flag bounds, for the message
	maxConcurrent int
	queueWait     time.Duration
}

func (e *heavyQueryQueueFullError) Error() string {
	flag, work := e.limitFlag, e.work
	if flag == "" {
		flag, work = heavyQueryLimitFlag, heavyQueryLimitWork
	}
	return fmt.Sprintf("%s: %s are limited to %s=%d per replica and this query waited -backend-heavy-query-queue-wait=%s; retry later, narrow the time range, or raise these limits",
		lokiTooManyOutstandingRequests, work, flag, e.maxConcurrent, e.queueWait)
}

func isHeavyQueryQueueFull(err error) bool {
	var queueErr *heavyQueryQueueFullError
	return errors.As(err, &queueErr)
}

// heavyQueryLimiter is a counting semaphore with a bounded wait and per-tenant
// fairness: a released slot goes to the queued waiter whose tenant currently
// holds the fewest slots, first-come first-served among equals.
type heavyQueryLimiter struct {
	limitFlag string // named in the 429 message
	work      string
	capacity  int
	queueWait time.Duration

	mu       sync.Mutex
	inUse    int
	byTenant map[string]int
	waiters  []*heavyQueryWaiter
}

type heavyQueryWaiter struct {
	tenant  string
	granted chan struct{}
	done    bool
}

func newHeavyQueryLimiter(capacity int, queueWait time.Duration) *heavyQueryLimiter {
	if capacity <= 0 {
		return nil
	}
	if queueWait < 0 {
		queueWait = 0
	}
	return &heavyQueryLimiter{limitFlag: heavyQueryLimitFlag, work: heavyQueryLimitWork, capacity: capacity, queueWait: queueWait, byTenant: make(map[string]int)}
}

// newMetadataScanLimiter is the admission limiter for long-range metadata
// listings; it shares the heavy queue wait and the range threshold.
func newMetadataScanLimiter(capacity int, queueWait time.Duration) *heavyQueryLimiter {
	l := newHeavyQueryLimiter(capacity, queueWait)
	if l != nil {
		l.limitFlag, l.work = metadataScanLimitFlag, metadataScanLimitWork
	}
	return l
}

// acquire blocks until a slot is granted, the queue wait elapses or ctx ends.
// The returned release is idempotent.
func (l *heavyQueryLimiter) acquire(ctx context.Context, tenant string) (func(), error) {
	if l == nil {
		return func() {}, nil
	}
	return l.acquireUntil(ctx, tenant, time.Now().Add(l.queueWait))
}

// acquireUntil is acquire with an explicit deadline, so every heavy call of one
// request shares a single queue-wait budget.
func (l *heavyQueryLimiter) acquireUntil(ctx context.Context, tenant string, deadline time.Time) (func(), error) {
	if l == nil {
		return func() {}, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	l.mu.Lock()
	if l.inUse < l.capacity && len(l.waiters) == 0 {
		l.grantLocked(tenant)
		l.mu.Unlock()
		return l.releaseFunc(tenant), nil
	}
	w := &heavyQueryWaiter{tenant: tenant, granted: make(chan struct{})}
	l.waiters = append(l.waiters, w)
	l.mu.Unlock()

	var timeout <-chan time.Time
	if wait := time.Until(deadline); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		timeout = timer.C
	} else {
		closed := make(chan time.Time)
		close(closed)
		timeout = closed
	}
	select {
	case <-w.granted:
		return l.releaseFunc(tenant), nil
	case <-ctx.Done():
		if l.abandon(w) {
			return l.releaseFunc(tenant), nil
		}
		return nil, ctx.Err()
	case <-timeout:
		if l.abandon(w) {
			return l.releaseFunc(tenant), nil
		}
		return nil, &heavyQueryQueueFullError{limitFlag: l.limitFlag, work: l.work, maxConcurrent: l.capacity, queueWait: l.queueWait}
	}
}

// abandon removes w from the queue. It reports true when w was granted a slot
// concurrently, in which case the caller owns that slot.
func (l *heavyQueryLimiter) abandon(w *heavyQueryWaiter) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if w.done {
		return true
	}
	for i, candidate := range l.waiters {
		if candidate == w {
			l.waiters = append(l.waiters[:i], l.waiters[i+1:]...)
			break
		}
	}
	w.done = true
	return false
}

func (l *heavyQueryLimiter) grantLocked(tenant string) {
	l.inUse++
	l.byTenant[tenant]++
}

func (l *heavyQueryLimiter) releaseFunc(tenant string) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			l.inUse--
			if l.byTenant[tenant]--; l.byTenant[tenant] <= 0 {
				delete(l.byTenant, tenant)
			}
			l.dispatchLocked()
		})
	}
}

func (l *heavyQueryLimiter) dispatchLocked() {
	for l.inUse < l.capacity && len(l.waiters) > 0 {
		best := 0
		for i, w := range l.waiters {
			if l.byTenant[w.tenant] < l.byTenant[l.waiters[best].tenant] {
				best = i
			}
		}
		w := l.waiters[best]
		l.waiters = append(l.waiters[:best], l.waiters[best+1:]...)
		w.done = true
		l.grantLocked(w.tenant)
		close(w.granted)
	}
}

func (l *heavyQueryLimiter) stats() (inUse, queued int) {
	if l == nil {
		return 0, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inUse, len(l.waiters)
}

var trailingLimitPipeRE = regexp.MustCompile(`\|\s*(?:limit|head)\s+(\d+)\s*$`)

// isHeavyBackendRequest classifies a VictoriaLogs select call by the work it
// can make VictoriaLogs do. Raw-row fetches above log-query sizes, and stats or
// hits calls over long ranges or finer-than-Loki bucket grids, are heavy.
// Metadata listings (field names/values, streams) stay outside this limiter
// so label browsing keeps working while heavy queries queue; long-range ones
// have their own (isMetadataScanRequest).
func isHeavyBackendRequest(path string, params url.Values, minRange time.Duration) bool {
	switch path {
	case "/select/logsql/query":
		rows := rawFetchRowBound(params)
		if rows > heavyQueryRowThreshold {
			return true
		}
		if rows > 0 {
			return false
		}
		rng, ok := backendParamsRange(params)
		return !ok || rng >= minRange
	case "/select/logsql/stats_query_range", "/select/logsql/stats_query", "/select/logsql/hits":
		rng, ok := backendParamsRange(params)
		if !ok {
			// Without start or end VictoriaLogs scans every stream it ever
			// stored, so an unbounded stats call is always heavy.
			return true
		}
		if rng >= minRange {
			return true
		}
		if step, stepOK := parseBackendStep(params.Get("step")); stepOK && step > 0 {
			return rng/step > heavyQueryBucketThreshold
		}
		return false
	default:
		return false
	}
}

// isMetadataScanRequest classifies a VictoriaLogs metadata listing by its
// time range. Stream field names and values, field names and values and
// stream listings scan every partition in the range: on a 7-day retention one
// stream_field_names call holds about 0.8 GiB of VictoriaLogs memory for 9 s,
// and VictoriaLogs admits any number of them without accounting for the sum.
// Listings at or above minRange, or without a time range, are bounded by
// -backend-max-concurrent-metadata-scans; shorter ones stay unqueued so label
// pickers keep answering while heavy work waits (limits/metadata-not-queued).
func isMetadataScanRequest(path string, params url.Values, minRange time.Duration) bool {
	switch path {
	case "/select/logsql/stream_field_names", "/select/logsql/stream_field_values",
		"/select/logsql/field_names", "/select/logsql/field_values", "/select/logsql/streams":
		rng, ok := backendParamsRange(params)
		return !ok || rng >= minRange
	default:
		return false
	}
}

// backgroundInventoryKey marks a context as a background inventory refresh
// (startup warm, keep-warm, stale-entry refresh). Such a call never queues for
// an admission slot: when every slot is busy it is skipped and retried on the
// next schedule, so background work cannot pile up behind user requests.
type backgroundInventoryKey struct{}

func withBackgroundInventory(ctx context.Context) context.Context {
	return context.WithValue(ctx, backgroundInventoryKey{}, true)
}

func isBackgroundInventory(ctx context.Context) bool {
	return ctx.Value(backgroundInventoryKey{}) != nil
}

// rawFetchRowBound returns the row bound of a /select/logsql/query call: the
// smaller of the limit argument and a trailing limit pipe. Zero means unbounded.
func rawFetchRowBound(params url.Values) int {
	bound := 0
	if limit, err := strconv.Atoi(strings.TrimSpace(params.Get("limit"))); err == nil && limit > 0 {
		bound = limit
	}
	if match := trailingLimitPipeRE.FindStringSubmatch(params.Get("query")); match != nil {
		if pipeLimit, err := strconv.Atoi(match[1]); err == nil && (bound == 0 || pipeLimit < bound) {
			bound = pipeLimit
		}
	}
	return bound
}

func backendParamsRange(params url.Values) (time.Duration, bool) {
	start, startOK := parseBackendTimeParam(params.Get("start"))
	end, endOK := parseBackendTimeParam(params.Get("end"))
	if !startOK {
		return 0, false
	}
	if !endOK {
		end = time.Now().UnixNano()
	}
	if end < start {
		return 0, true
	}
	return time.Duration(end - start), true
}

func parseBackendTimeParam(raw string) (int64, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	if ns, err := strconv.ParseInt(formatVLTimestamp(raw), 10, 64); err == nil {
		return ns, true
	}
	return 0, false
}

func parseBackendStep(raw string) (time.Duration, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	if d, err := time.ParseDuration(raw); err == nil {
		return d, true
	}
	if seconds, err := strconv.ParseFloat(raw, 64); err == nil {
		return time.Duration(seconds * float64(time.Second)), true
	}
	if d := parseLokiDuration(raw); d > 0 {
		return d, true
	}
	return 0, false
}

// admitBackendRequest acquires a heavy-query slot for heavy calls. A request
// whose earlier heavy call was rejected fails fast, so fallbacks after a
// rejected fast path do not queue a second time.
func (p *Proxy) admitBackendRequest(ctx context.Context, path string, params url.Values) (func(), error) {
	limiter, operation := p.backendAdmissionLimiter(path, params)
	if limiter == nil {
		return func() {}, nil
	}
	rt := getRequestTelemetry(ctx)
	waitStart := time.Now()
	deadline := waitStart.Add(limiter.queueWait)
	if isBackgroundInventory(ctx) {
		deadline = waitStart
	}
	if rt != nil {
		rt.mu.Lock()
		rejected := rt.heavyAdmissionRejected
		if rt.heavyAdmissionDeadline.IsZero() {
			rt.heavyAdmissionDeadline = deadline
		} else {
			deadline = rt.heavyAdmissionDeadline
		}
		rt.mu.Unlock()
		if rejected != nil {
			return nil, rejected
		}
	}
	release, err := limiter.acquireUntil(ctx, getOrgID(ctx), deadline)
	outcome := "admitted"
	switch {
	case err == nil:
	case isHeavyQueryQueueFull(err) && isBackgroundInventory(ctx):
		// No client was refused: the background refresh is retried on its
		// next schedule.
		outcome = "skipped"
	case isHeavyQueryQueueFull(err):
		outcome = "rejected"
	default:
		outcome = "canceled"
	}
	p.observeInternalOperation(ctx, operation, outcome, time.Since(waitStart))
	if err != nil {
		if isHeavyQueryQueueFull(err) {
			if rt != nil {
				rt.mu.Lock()
				rt.heavyAdmissionRejected = err
				rt.mu.Unlock()
			}
		}
		return nil, err
	}
	return release, nil
}

// backendAdmissionLimiter picks the limiter a backend call must pass, if any:
// heavy stats and raw-row work goes through the heavy-query limiter, long-range
// metadata listings through the metadata-scan limiter. The two are separate so
// a 7-day label browser never waits behind 7-day charts and vice versa.
func (p *Proxy) backendAdmissionLimiter(path string, params url.Values) (*heavyQueryLimiter, string) {
	if p.heavyQueryLimiter != nil && isHeavyBackendRequest(path, params, p.backendHeavyQueryMinRange) {
		return p.heavyQueryLimiter, "backend_heavy_query_admission"
	}
	if p.metadataScanLimiter != nil && isMetadataScanRequest(path, params, p.backendHeavyQueryMinRange) {
		return p.metadataScanLimiter, "backend_metadata_scan_admission"
	}
	return nil, ""
}

// attachRelease keeps a slot until the response body is consumed or closed.
// A caller must therefore finish with one heavy response body before issuing
// the next heavy call of the same request; otherwise it waits on itself until
// the shared queue-wait budget expires.
func attachRelease(resp *http.Response, release func()) *http.Response {
	if resp == nil || resp.Body == nil {
		release()
		return resp
	}
	resp.Body = &budgetResponseBody{ReadCloser: resp.Body, release: release}
	return resp
}

// withBackendTimeoutArg passes the remaining request budget to VictoriaLogs as
// its per-query timeout argument, so VictoriaLogs stops executing a query the
// proxy has already given up on. VictoriaLogs caps the value at its own
// -search.maxQueryDuration.
func (p *Proxy) withBackendTimeoutArg(ctx context.Context, path string, params url.Values) url.Values {
	if !strings.HasPrefix(path, "/select/logsql/") || path == "/select/logsql/tail" || params.Has("timeout") {
		return params
	}
	budget := p.client.Timeout
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); budget <= 0 || remaining < budget {
			budget = remaining
		}
	}
	if budget <= 0 {
		return params
	}
	ms := budget.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	out := make(url.Values, len(params)+1)
	for k, v := range params {
		out[k] = v
	}
	out.Set("timeout", strconv.FormatInt(ms, 10)+"ms")
	return out
}

var _ io.ReadCloser = (*budgetResponseBody)(nil)

func resolveHeavyQueryMinRange(configured time.Duration) time.Duration {
	if configured <= 0 {
		return DefaultBackendHeavyQueryMinRange
	}
	return configured
}
