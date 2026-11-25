package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/capability"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

const mixedStatsGroupField = "g"

// conformance: semantics/two-parsers-keep-one
// mixedStatsClass admits only the two typed parser pairs whose categorical
// row class has an independent Loki proof. Source/profile/backend eligibility
// is checked separately by the range collector.
func (plan *orderedJSONMetricPlan) mixedStatsClass() (*mixedJSONRowClass, bool) {
	if !plan.mixedParsers || !plan.aggregated || plan.grouping == nil || plan.grouping.Without ||
		len(plan.grouping.Labels) != 1 || len(plan.required) != 1 || plan.mixedErrorsCanSurvive ||
		plan.derivesLevel || plan.preserveError || len(plan.preLevelFilters) != 0 ||
		plan.fetchQuery != plan.selector || len(plan.stages) != 4 ||
		plan.function != "count_over_time" && plan.function != "rate" {
		return nil, false
	}
	key := plan.grouping.Labels[0]
	if !plan.required[key] {
		return nil, false
	}
	if !plan.mixedStatsStages(key) {
		return nil, false
	}
	return newMixedJSONRowClass(key)
}

func (plan *orderedJSONMetricPlan) mixedStatsStages(key string) bool {
	first, second, drop, filter := plan.stages[0], plan.stages[1], plan.stages[2], plan.stages[3]
	reverse := first.parser && first.logfmt && second.parser && !second.logfmt && second.jsonField == ""
	if !plan.mixedProjectionStages(key) && !reverse || drop.keep || len(drop.match) != 0 ||
		len(drop.fields) != 2 || !drop.fields["__error__"] || !drop.fields["__error_details__"] ||
		filter.filter == nil || filter.filter.Field != key || filter.filter.Op != "!=" || filter.filter.Value != "" {
		return false
	}
	return true
}

// mixedJSONStatsQuery classifies each row once. Certified rows have a nonempty
// target; unsafe rows share the empty-target count group. A single count avoids
// emitting an extra zero-valued result for every native group and bucket.
func mixedJSONStatsQuery(base string, class *mixedJSONRowClass, key string) (string, string) {
	pipes := class.nativePipes()
	safe := pipes[len(pipes)-1].(logsql.PipeFilter).Expr
	pipes = pipes[:len(pipes)-1]
	pipes = append(pipes, logsql.PipeFormat{ResultField: mixedClassTargetPrefix + key, If: logsql.NotExpr{Expr: safe}})
	// The classifier retains the complete original envelope. Stats needs
	// only the timestamp and target: discard source collisions before using
	// a short wire name, which VL otherwise repeats for every output series.
	pipes = append(pipes,
		logsql.PipeFields{Labels: []string{"_time", mixedClassTargetPrefix + key}},
		logsql.PipeRename{Pairs: [][2]string{{mixedClassTargetPrefix + key, mixedStatsGroupField}}},
	)
	stats := mixedJSONCountStats()
	return appendMixedClassPipes(base, pipes), string(logsql.AppendStatsFuncs(nil, stats.Funcs))
}

func mixedJSONCountStats() logsql.PipeStats {
	return logsql.PipeStats{
		By:    []logsql.GroupKey{{Field: mixedStatsGroupField}},
		Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}, Alias: "c"}},
	}
}

func mixedJSONRawQuery(base string, class *mixedJSONRowClass) string {
	pipes := class.rawPipes()
	pipes = append(pipes, logsql.PipeFields{Labels: []string{class.originalField}})
	return appendMixedClassPipes(base, pipes)
}

func appendMixedClassPipes(base string, pipes []logsql.Pipe) string {
	var out strings.Builder
	out.WriteString(base)
	for _, pipe := range pipes {
		out.WriteByte(' ')
		out.WriteString(pipe.String())
	}
	return out.String()
}

// collectOrderedMixedJSONStats counts certified native rows and unsafe bucket
// edges in one pass, then merges the exact complement using the original
// evaluator. Unsupported profiles, versions and grids retain the raw route.
func (p *Proxy) collectOrderedMixedJSONStats(ctx context.Context, plan *orderedJSONMetricPlan, start, end time.Time, step time.Duration) (map[string]manualSeriesSamples, bool, error) {
	class, ok := plan.mixedStatsClass()
	if !ok || step <= 0 || end.Before(start) || plan.window < step || p.labelTranslator == nil || len(p.labelTranslator.vlToLoki) != 0 || p.jsonLineKeysActive() {
		return nil, false, nil
	}
	if !p.caps.For(ctx).On(capability.MixedJSONRowStats) {
		return nil, false, nil
	}
	bucket, ok := slidingStatsBucket(step, plan.window)
	if !ok {
		return nil, false, nil
	}
	base, err := p.translateQueryWithContext(ctx, plan.fetchQuery)
	if err != nil {
		return nil, true, err
	}
	// window<step remains on the original route until phase math can run
	// after the original snapshot without overwriting source metadata.
	fetchStart := start.Add(-plan.window)
	key := plan.grouping.Labels[0]
	// The window's stored field names settle, once, what the class would check on every row's packed
	// envelope (see certified): when they do, the native count does not pack it. The call is read again
	// after the scan, and a name that appeared meanwhile sends the request back to the class itself.
	// A class whose expanded query exceeds the backend's limit falls back before any backend work.
	if !mixedClassQueriesFit(base, class, key) {
		return nil, false, nil
	}
	// A certificate derived for this tenant, selector and window class is reused for a minute. The re-read after
	// the scan is what keeps it sound: it lists the names of the rows the scan could see, and they must all be
	// among the remembered ones, whatever the age of the entry.
	memoKey := p.mixedCertMemoKey(ctx, base, class.label, fetchStart, end)
	if entry, ok := p.mixedCertificates.get(memoKey, time.Now()); ok {
		if entry.certified == nil {
			p.observeInternalOperation(ctx, "mixed_class_certificate", "cache_hit_class", 0)
			return p.scanOrderedMixedJSONStats(ctx, plan, base, class, key, fetchStart, start, end, step, bucket)
		}
		if mixedClassQueriesFit(base, entry.certified, key) {
			if series, served, err, done := p.scanCertifiedMixedJSONStats(ctx, plan, base, entry.certified, entry.names, key, fetchStart, start, end, step, bucket); done {
				if err == nil {
					p.observeInternalOperation(ctx, "mixed_class_certificate", "cache_hit", 0)
				}
				return series, served, err
			}
		}
		p.mixedCertificates.drop(memoKey)
		return p.scanOrderedMixedJSONStats(ctx, plan, base, class, key, fetchStart, start, end, step, bucket)
	}
	if names, listed := p.mixedClassWindowFieldNames(ctx, base, fetchStart, end, bucket); listed {
		certified, ok := p.mixedClassCertificate(ctx, class, base, names, fetchStart, end, bucket)
		if !ok || !mixedClassQueriesFit(base, certified, key) {
			// The listing did settle that these names do not certify: remember it, so the next request goes
			// straight to the class instead of listing and measuring again.
			p.mixedCertificates.put(memoKey, mixedCertEntry{names: names}, time.Now())
		} else {
			if series, served, err, done := p.scanCertifiedMixedJSONStats(ctx, plan, base, certified, names, key, fetchStart, start, end, step, bucket); done {
				if err == nil {
					p.mixedCertificates.put(memoKey, mixedCertEntry{names: names, certified: certified}, time.Now())
				}
				return series, served, err
			}
		}
	}
	return p.scanOrderedMixedJSONStats(ctx, plan, base, class, key, fetchStart, start, end, step, bucket)
}

// scanCertifiedMixedJSONStats runs the native count with a certified class and then re-reads the window's field
// names: done is true when its answer stands (served, or an error to report), false when a name outside the
// certified ones appeared, so the caller must run the class itself.
func (p *Proxy) scanCertifiedMixedJSONStats(ctx context.Context, plan *orderedJSONMetricPlan, base string, certified *mixedJSONRowClass, names []string, key string, fetchStart, start, end time.Time, step, bucket time.Duration) (series map[string]manualSeriesSamples, served bool, err error, done bool) {
	series, served, err = p.scanOrderedMixedJSONStats(ctx, plan, base, certified, key, fetchStart, start, end, step, bucket)
	if err != nil {
		return series, served, err, true
	}
	if !served {
		return series, served, nil, false
	}
	if again, ok := p.mixedClassWindowFieldNames(ctx, base, fetchStart, end, bucket); ok && mixedClassNamesWithin(again, names) {
		return series, true, nil, true
	}
	p.observeInternalOperation(ctx, "mixed_class_certificate", "names_changed", 0)
	return nil, false, nil, false
}

// mixedCertMemoKey keys a remembered certificate by tenant and auth scope, the selector's backend query, the class
// label and the window class.
func (p *Proxy) mixedCertMemoKey(ctx context.Context, base, label string, from, to time.Time) string {
	params := url.Values{}
	params.Set("query", base)
	params.Set("label", label)
	params.Set("window", mixedCertWindowClass(to.Sub(from)))
	return p.metadataFieldNamesCacheKey(ctx, "mxc:", params)
}

// mixedClassQueriesFit reports whether both backend forms of the class, the native count and the exact complement,
// fit the backend's query length limit.
func mixedClassQueriesFit(base string, class *mixedJSONRowClass, key string) bool {
	query, _ := mixedJSONStatsQuery(base, class, key)
	return len(appendMixedClassPipes(query, []logsql.Pipe{mixedJSONCountStats()})) <= vlDefaultMaxQueryLen &&
		len(mixedJSONRawQuery(base, class)) <= vlDefaultMaxQueryLen
}

// scanOrderedMixedJSONStats counts the rows of the class natively and merges the exact complement for the
// buckets the native count could not take.
func (p *Proxy) scanOrderedMixedJSONStats(ctx context.Context, plan *orderedJSONMetricPlan, base string, class *mixedJSONRowClass, key string, fetchStart, start, end time.Time, step, bucket time.Duration) (map[string]manualSeriesSamples, bool, error) {
	query, agg := mixedJSONStatsQuery(base, class, key)
	// The accepted LogQL selector can fit VL while the expanded classifier
	// does not: the caller checks both complete backend forms before it chooses
	// this route; the original raw reader remains available without the class
	// expansion.
	// The empty-target group is internal, not an extra user series. Preserve
	// the existing intermediate group guard and defer the user limit to merge.
	series, err := p.collectRangeMetricHits(guardStatsGroups(ctx), query, []string{mixedStatsGroupField}, []string{key}, false, agg, fetchStart, end, bucket)
	if err != nil {
		return nil, true, err
	}
	edges := mixedClassRawCountEdges(series)
	dropSeriesWithoutPoints(series, start, end, step, plan.window)
	if len(edges) != 0 {
		rows, err := p.collectMixedClassRowSpans(ctx, plan, base, class, orderedJSONRowSpans(edges, bucket), start, end, step)
		if err != nil {
			return nil, true, err
		}
		mergeOrderedJSONRowSeries(series, rows, false)
	}
	series, err = capSeriesForRequest(ctx, series, p.resolvedMaxStatsQuerySeries(ctx))
	return series, true, err
}

// mixedClassWindowFieldNames lists the stored field names of the rows the base query selects in the window the
// native count reads (widened by a bucket at each end, as the stats call widens to its bucket edges). The call is
// not coalesced with another request's: the second read after a scan must see the rows the scan saw.
func (p *Proxy) mixedClassWindowFieldNames(ctx context.Context, base string, from, to time.Time, bucket time.Duration) ([]string, bool) {
	params := url.Values{}
	params.Set("query", base)
	params.Set("start", from.Add(-bucket).UTC().Format(time.RFC3339Nano))
	params.Set("end", to.Add(bucket).UTC().Format(time.RFC3339Nano))
	resp, err := p.vlGet(ctx, "/select/logsql/field_names", params)
	if err != nil {
		return nil, false
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, mixedClassFieldNamesMaxBody+1))
	if err != nil || len(body) > mixedClassFieldNamesMaxBody {
		return nil, false
	}
	items, err := decodeVLValueHits(body)
	if err != nil {
		return nil, false
	}
	return listingValues(items), true
}

// mixedClassCertificate derives the certified class for the window: with the fixed bounds when they fit (no further
// call), otherwise with the window's measured maximum lengths, which let a window with many stored fields, a long
// message or a long stream descriptor be certified too. It reports false when neither fits or the measurement is
// unavailable, and the request stays on the class itself.
func (p *Proxy) mixedClassCertificate(ctx context.Context, class *mixedJSONRowClass, base string, names []string, from, to time.Time, bucket time.Duration) (*mixedJSONRowClass, bool) {
	if certified, ok := class.certified(names); ok {
		return certified, true
	}
	if !mixedClassFieldNamesClean(class.label, names) {
		return nil, false
	}
	lengths, measured := p.mixedClassWindowFieldLengths(ctx, base, names, from, to, bucket)
	if !measured {
		return nil, false
	}
	return class.certifiedFor(names, lengths)
}

// mixedClassWindowFieldLengths measures the longest value of the message, the stream descriptor and every other
// stored field of the window, over the rows the native count reads (the window widened as the field names call is).
// The rows are read by VictoriaLogs' own column scan; the answer is one row of integers. A row stored after the
// measurement that is longer than its bound fails that bound's guard and goes to the exact complement, so the
// measurement can only change how much is counted natively, never the answer.
func (p *Proxy) mixedClassWindowFieldLengths(ctx context.Context, base string, names []string, from, to time.Time, bucket time.Duration) (mixedClassLengths, bool) {
	fields := []string{"_msg", "_stream"}
	for _, name := range names {
		if _, builtin := mixedCertBuiltin[name]; !builtin {
			fields = append(fields, name)
		}
	}
	if len(fields)-2 > mixedCertMeasuredFieldsMax {
		return nil, false
	}
	pipes := make([]logsql.Pipe, 0, len(fields)+1)
	stats := logsql.PipeStats{}
	for i, field := range fields {
		pipes = append(pipes, logsql.PipeLen{Field: field, Result: "mixed_len_" + strconv.Itoa(i)})
		stats.Funcs = append(stats.Funcs, logsql.StatsFuncAlias{Func: logsql.Max{Field: "mixed_len_" + strconv.Itoa(i)}, Alias: "m" + strconv.Itoa(i)})
	}
	pipes = append(pipes, stats)
	params := url.Values{}
	params.Set("query", appendMixedClassPipes(base, pipes))
	params.Set("start", from.Add(-bucket).UTC().Format(time.RFC3339Nano))
	params.Set("end", to.Add(bucket).UTC().Format(time.RFC3339Nano))
	resp, err := p.vlGet(ctx, "/select/logsql/query", params)
	if err != nil {
		return nil, false
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, mixedClassFieldNamesMaxBody+1))
	if err != nil || len(body) > mixedClassFieldNamesMaxBody {
		return nil, false
	}
	line := strings.TrimSpace(string(body))
	if line == "" || strings.Contains(line, "\n") {
		return nil, false
	}
	var row map[string]string
	if json.Unmarshal([]byte(line), &row) != nil {
		return nil, false
	}
	out := make(mixedClassLengths, len(fields))
	for i, field := range fields {
		n, err := strconv.Atoi(row["m"+strconv.Itoa(i)])
		if err != nil || n < 0 {
			return nil, false
		}
		out[field] = n
	}
	return out, true
}

// mixedClassFieldNamesMaxBody bounds the listing read: a window with more field names than fit is not certified.
const mixedClassFieldNamesMaxBody = 64 << 10

func mixedClassNamesWithin(names, allowed []string) bool {
	set := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		set[name] = struct{}{}
	}
	for _, name := range names {
		if _, ok := set[name]; !ok {
			return false
		}
	}
	return true
}

// The class guarantees native targets are nonempty; statsGroupMetric removes
// an empty target label, so exactly the empty metric denotes the unsafe group.
func mixedClassRawCountEdges(series map[string]manualSeriesSamples) []int64 {
	var edges []int64
	for key, entry := range series {
		if len(entry.Metric) != 0 {
			continue
		}
		for _, sample := range entry.Samples {
			if sample.value > 0 {
				edges = append(edges, sample.ts)
			}
		}
		delete(series, key)
	}
	return edges
}

func (p *Proxy) orderedMixedJSONStatsRange(ctx context.Context, plan *orderedJSONMetricPlan, start, end time.Time, step time.Duration) ([]byte, bool, error) {
	maxBytes := p.orderedJSONMetricMaxBytes()
	if maxBytes > math.MaxInt {
		return nil, false, nil
	}
	// The points of the answer are bounded by the configured cap (-ordered-json-metric-max-samples), carried to the
	// evaluators that have no Proxy.
	ctx = withMetricSampleLimit(ctx, p.maxMetricSamples())
	series, served, err := p.collectOrderedMixedJSONStats(ctx, plan, start, end, step)
	if errors.Is(err, errBodyTooLarge) || errors.Is(err, errStatsGroupGuard) || errors.Is(err, causeOrderedResponse) {
		// An intermediate native read is not the final series set. Preserve
		// the old complete raw fallback and all its independent bounds.
		return nil, false, nil
	}
	if !served || err != nil {
		return nil, served, err
	}
	if err := ctx.Err(); err != nil {
		return nil, true, err
	}
	body, err := buildMixedJSONStatsMatrixContext(ctx, plan, series, start, end, step, int(maxBytes))
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, true, ctxErr
	}
	return body, true, err
}

// buildMixedJSONStatsMatrix reuses the existing bucket/window reducer, then
// the mixed evaluator's Prometheus label comparator before a single encoding.
// Canonical JSON key order alone is different for escaped raw label values.
func buildMixedJSONStatsMatrix(plan *orderedJSONMetricPlan, series map[string]manualSeriesSamples, start, end time.Time, step time.Duration, maxBytes int) ([]byte, error) {
	return buildMixedJSONStatsMatrixContext(context.Background(), plan, series, start, end, step, maxBytes)
}

func buildMixedJSONStatsMatrixContext(ctx context.Context, plan *orderedJSONMetricPlan, series map[string]manualSeriesSamples, start, end time.Time, step time.Duration, maxBytes int) ([]byte, error) {
	// The shared stats estimate deliberately overcounts timestamp bytes. The
	// old mixed evaluator applies this flag to exact serialized bytes, so keep
	// its point/context bounds here and apply the unchanged byte cap below.
	maxPoints := metricSampleLimit(ctx)
	if maxPoints > math.MaxInt32 { // the reducer counts points in an int
		maxPoints = math.MaxInt32
	}
	results, err := hitsRangeMetricSeriesBounded(plan.function, series, start, end, step, plan.window, math.MaxInt, nil, ctx, int(maxPoints))
	if err != nil {
		return nil, err
	}
	sort.SliceStable(results, func(i, j int) bool {
		return compareLabelSets(results[i]["metric"].(map[string]string), results[j]["metric"].(map[string]string)) < 0
	})
	body := marshalManualMetricResponse("matrix", results)
	if len(body) > maxBytes {
		return nil, newBufferLimitError(flagOrderedJSONMetricMaxBytes, int64(len(body)), int64(maxBytes), causeOrderedOutput)
	}
	return body, nil
}

// collectMixedClassRowSpans retains the existing scanner/evaluator and ONE
// cumulative raw row/byte budget over all auxiliary spans and retries.
func (p *Proxy) collectMixedClassRowSpans(ctx context.Context, plan *orderedJSONMetricPlan, base string, class *mixedJSONRowClass, spans [][2]time.Time, start, end time.Time, step time.Duration) (map[string]manualSeriesSamples, error) {
	reader, err := p.newOrderedJSONRowReader()
	if err != nil {
		return nil, err
	}
	reader.originalField = class.originalField
	// This collector constructs the immutable class pipeline itself. Its
	// production caller admitted a selector-only base and window>=step, so
	// subdividing transport spans does not change row classification.
	reader.staticClassTransport = true
	query := mixedJSONRawQuery(base, class)
	return p.collectOrderedJSONRowSpansUsingReader(ctx, plan, reader, query, spans, start, end, step)
}

// restoreMixedClassRow restores exactly one envelope, never recursively
// interpreting an original field that happens to have the snapshot name.
func restoreMixedClassRow(row map[string]string, field string) (map[string]string, error) {
	envelope, ok := row[field]
	if !ok || envelope == "" {
		return nil, fmt.Errorf("invalid backend metric row: missing original row envelope")
	}
	clear(row)
	if err := json.Unmarshal([]byte(envelope), &row); err != nil {
		return nil, fmt.Errorf("invalid backend metric row envelope: %w", err)
	}
	if row == nil {
		return nil, fmt.Errorf("invalid backend metric row: null original row envelope")
	}
	return row, nil
}
