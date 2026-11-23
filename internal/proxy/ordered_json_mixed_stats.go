package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
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
	query, agg := mixedJSONStatsQuery(base, class, key)
	// The accepted LogQL selector can fit VL while the expanded classifier
	// does not. Check both complete backend forms before choosing this route;
	// the original raw reader remains available without the class expansion.
	if len(appendMixedClassPipes(query, []logsql.Pipe{mixedJSONCountStats()})) > vlDefaultMaxQueryLen ||
		len(mixedJSONRawQuery(base, class)) > vlDefaultMaxQueryLen {
		return nil, false, nil
	}
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
	results, err := hitsRangeMetricSeriesBounded(plan.function, series, start, end, step, plan.window, math.MaxInt, nil, ctx, maxMetricEvalSamples)
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
