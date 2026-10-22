package proxy

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// A `| json | __error__=""` metric counts the lines Loki's JSON parser accepts
// (pkg/logql/log/parser.go JSONParser.Process: jsonparser.ObjectEach over the
// line, nested objects walked only where a required label starts with their
// prefix, parser_hints.go ShouldExtractPrefix). VictoriaLogs attaches no parser
// error, so the stats pushdown splits the selected lines in three:
//
//   - lines that do not open with a brace after JSON whitespace: ObjectEach
//     fails on them (grafana/jsonparser nextToken skips only space, tab, line
//     feed and carriage return), so Loki drops them;
//   - lines VictoriaLogs parses as one JSON object and that hold none of the
//     constructs its parser accepts and Loki's rejects (jsonValidityPipes,
//     jsonLokiRejectHazard): Loki accepts every such line under any parser
//     hints, so VictoriaLogs stats count them;
//   - the rest open with a brace but VictoriaLogs cannot vouch for them; they
//     are read as raw rows, only from the buckets holding them, and evaluated
//     by the row evaluator, whose parser follows Loki's hints.
//
// The guards of the row evaluator (nesting depth, field count, line and value
// size) therefore apply to the third set only.

// jsonObjectStartFilter keeps the lines Loki's JSON parser may accept.
var jsonObjectStartFilter = "_msg:~" + strconv.Quote(`^[ \t\n\r]*\{`)

// jsonValidField is the sentinel key jsonValidityPipes splices into a line.
const jsonValidField = "__vlp"

// jsonInvalidRowsName names the stats result counting the lines of the third
// set (jsonInvalidCondition) in each bucket.
const jsonInvalidRowsName = "__json_rows"

// jsonValidityPipes sets __vlp exactly on the lines VictoriaLogs parses as one
// JSON object: the sentinel member is spliced in after the opening brace and
// unpack_json asked for it. The splice keeps a line's validity (it adds one
// leading member and cannot complete a broken line, unlike wrapping the line
// in a new object, which trailing text could close) except for the empty
// object `{}`, which it turns invalid and so only sends to the raw rows. With
// onCopy the splice works on a copy, leaving _msg (and sum_len(_msg)) intact.
func jsonValidityPipes(onCopy bool) string {
	at, from := "_msg", ""
	if onCopy {
		at, from = "__vlm", " from __vlm"
	}
	pipes := ` | replace_regexp ("^\\{", "{\"` + jsonValidField + `\":1,") at ` + at + ` | unpack_json` + from + ` fields (` + jsonValidField + `)`
	if onCopy {
		return ` | copy _msg as __vlm` + pipes + ` | delete __vlm`
	}
	return pipes
}

// jsonLokiRejectHazard matches every line VictoriaLogs' parser (fastjson)
// accepts while Loki's (grafana/jsonparser) may reject it. A key with a
// backslash escape: jsonparser unescapes keys and fails on an invalid escape or
// a lone surrogate (ObjectEach, MalformedStringEscapeError) where fastjson
// keeps the key raw. A member value starting with one of `. e E +`, or NaN or
// Inf in any case: fastjson reads them as numbers (parseRawNumber, parseValue),
// jsonparser's getType rejects them (UnknownValueTypeError). Each regexp runs
// behind a filter VictoriaLogs answers from its token index or a substring
// search where one exists, and it matches anywhere in the line, which is a
// superset: a value inside a string or an array only sends the line to the
// raw rows.
var jsonLokiRejectHazard = func() string {
	var words []string
	for _, word := range []string{"nan", "inf"} {
		for mask := 0; mask < 8; mask++ {
			b := []byte(word)
			for i := range b {
				if mask&(1<<i) != 0 {
					b[i] -= 'a' - 'A'
				}
			}
			words = append(words, "_msg:"+string(b))
		}
	}
	escapedKey := `(_msg:"\\" _msg:~` + strconv.Quote(`\\(?s:.)[^"]*"\s*:`) + `)`
	nanInf := `((` + strings.Join(words, " or ") + `) _msg:~` + strconv.Quote(`:\s*(?i:nan|inf)`) + `)`
	plus := `(_msg:"+" _msg:~` + strconv.Quote(`:\s*\+`) + `)`
	dotE := `_msg:~` + strconv.Quote(`:\s*[.eE]`)
	return "(" + escapedKey + " or " + nanInf + " or " + plus + " or " + dotE + ")"
}()

// jsonInvalidCondition selects the brace-opened lines read as raw rows, the
// ones VictoriaLogs cannot vouch for; hazard adds jsonLokiRejectHazard once a
// probe has found such a line.
func jsonInvalidCondition(hazard bool) string {
	if hazard {
		return "(-" + jsonValidField + ":* or " + jsonLokiRejectHazard + ")"
	}
	return "-" + jsonValidField + ":*"
}

// jsonRawField marks the lines read as raw rows in a stats query: jsonRawMarker
// sets it once, so the conditions after it test one field instead of repeating
// jsonInvalidCondition (a regexp scan of the line per use). It is never a
// grouped field, so it does not reach the labels.
const jsonRawField = "__vlr"

// jsonRawMarker is the pipe that sets jsonRawField on the lines of the third
// set (jsonInvalidCondition).
func jsonRawMarker(hazard bool) string {
	return " | format if (" + jsonInvalidCondition(hazard) + ") \"1\" as " + jsonRawField
}

// jsonRawRowCondition tests the marker: the lines read as raw rows when raw,
// the lines VictoriaLogs counts otherwise.
func jsonRawRowCondition(raw bool) string {
	if raw {
		return jsonRawField + ":*"
	}
	return "-" + jsonRawField + ":*"
}

// riskRestrictionKey carries pipes the parse-risk probes append before their
// `| limit 1`: a `| json | __error__=""` plan checks only the lines
// VictoriaLogs parses, because every other line is read as a raw row.
type riskRestrictionKey struct{}

func withRiskRestriction(ctx context.Context, pipes string) context.Context {
	return context.WithValue(ctx, riskRestrictionKey{}, pipes)
}

func riskRestriction(ctx context.Context) string {
	pipes, _ := ctx.Value(riskRestrictionKey{}).(string)
	return pipes
}

// jsonValidOnlyRestriction keeps the lines VictoriaLogs parses as one JSON
// object. It runs after a probe's own pipes, which may have spliced the
// sentinel into _msg already; a second splice keeps the verdict.
var jsonValidOnlyRestriction = jsonValidityPipes(true) + " | filter " + jsonValidField + ":*"

// jsonLokiRejectHazardRisk reports whether a brace-opened line in [start, end]
// matches jsonLokiRejectHazard, remembering windows found free of one like the
// parse-risk probes do.
func (p *Proxy) jsonLokiRejectHazardRisk(ctx context.Context, base string, start, end time.Time) (bool, error) {
	key := "json-reject-hazard:v1:" + getOrgID(ctx) + ":" + base
	_, risky, err := p.coveredWindowProbe(ctx, key, start, end, func(from, to time.Time, _ string) (string, bool, error) {
		// The brace filter of the split, not a first-byte anchor: unpack_json
		// trims JSON whitespace before its brace check from v1.52 on, and not
		// before.
		query := base + " | filter " + jsonObjectStartFilter + " " + jsonLokiRejectHazard + " | limit 1"
		risky, err := p.statsPushdownRiskExists(ctx, query, from, to)
		return "", risky, err
	})
	return risky, err
}

// streamServiceNameField holds the service name a split plan groups by.
const streamServiceNameField = "__vlsn"

// serviceNameFromStreamPipes set __vlsn to the service name Loki assigns at
// ingest: the first of serviceNameSourceFields that is a stream label of the
// row, as the row evaluator derives it (ensureSyntheticServiceName over the
// _stream labels); a field stored only as structured metadata does not count.
// A row with none keeps __vlsn empty, which reads as unknown_service.
func serviceNameFromStreamPipes() string {
	var b strings.Builder
	for _, field := range serviceNameSourceFields {
		name := field
		if strings.ContainsAny(field, ".-") {
			name = strconv.Quote(field)
		}
		// A stream filter in the condition: the row's stream has the label with
		// a non-empty value, as deriveServiceName reads it.
		b.WriteString(" | format if (-" + streamServiceNameField + ":* {" + name + `!=""}) "<` + field + `>" as ` + streamServiceNameField)
	}
	return b.String()
}

// orderedJSONRowSpans turns the left edges of the buckets holding raw-row
// lines into the time ranges to read: each bucket is widened to both of its
// edges (a bucket is (e, e+b] with the stats offset and [e, e+b) without),
// touching buckets are merged, and more than maxOrderedJSONRowSpans ranges
// collapse into their hull, one query instead of many.
func orderedJSONRowSpans(edges []int64, bucket time.Duration) [][2]time.Time {
	if len(edges) == 0 {
		return nil
	}
	sorted := append([]int64(nil), edges...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	size := int64(bucket)
	var spans [][2]int64
	for _, edge := range sorted {
		if n := len(spans); n > 0 && edge <= spans[n-1][1] {
			if edge+size > spans[n-1][1] {
				spans[n-1][1] = edge + size
			}
			continue
		}
		spans = append(spans, [2]int64{edge, edge + size})
	}
	if len(spans) > maxOrderedJSONRowSpans {
		spans = [][2]int64{{spans[0][0], spans[len(spans)-1][1]}}
	}
	out := make([][2]time.Time, len(spans))
	for i, span := range spans {
		out[i] = [2]time.Time{time.Unix(0, span[0]), time.Unix(0, span[1])}
	}
	return out
}

// maxOrderedJSONRowSpans bounds the raw-row queries of one evaluation.
const maxOrderedJSONRowSpans = 16

// mergeOrderedJSONRowSeries adds the samples the row evaluator found to the
// stats buckets at the evaluation points: a line at t counts in the window
// (T-window, T] exactly when t-1ns falls in the [T-window, T) range the bucket
// labels are summed over (buildHitsRangeMetricMatrix), so each window sums the
// bucket counts and the raw lines before metricWindowValue runs once.
func mergeOrderedJSONRowSeries(merged, rows map[string]manualSeriesSamples, bytesMetric bool) {
	for key, entry := range rows {
		target := merged[key]
		if target.Metric == nil {
			target.Metric = entry.Metric
		}
		for _, sample := range entry.Samples {
			target.Samples = append(target.Samples, rangeMetricSample{ts: sample.ts - 1, value: sample.value})
			if bytesMetric {
				target.PresentBuckets = append(target.PresentBuckets, sample.ts-1)
			}
		}
		sort.Slice(target.Samples, func(i, j int) bool { return target.Samples[i].ts < target.Samples[j].ts })
		if bytesMetric {
			sort.Slice(target.PresentBuckets, func(i, j int) bool { return target.PresentBuckets[i] < target.PresentBuckets[j] })
		}
		merged[key] = target
	}
}

// orderedJSONRowProjection returns the `| fields` pipe that keeps what the row
// evaluator reads from a raw row: _time, _stream, _msg, level (stream
// descriptor and detected_level) and every stored field that can expose one of
// the labels the plan requires (orderedJSONBaseLabels turns stored fields into
// labels that rename colliding parsed keys, answer filters and group). A label
// is read from the field of its own name, its query spelling (ToVL), a custom
// mapping, or any field it sanitizes from, which starts with the label's
// leading letters and digits. Empty when the plan needs every label (no hints)
// or a label has no such prefix. Also empty for a plan that derives
// detected_level: the derivation reads every stored level field (and the OTLP
// severity fields), which no label of the plan names.
func (p *Proxy) orderedJSONRowProjection(plan *orderedJSONMetricPlan) string {
	if plan.required == nil || plan.derivesLevel {
		return ""
	}
	fields := []string{"_time", "_stream", "_msg", "level"}
	add := func(name string) {
		if name != "" && !containsString(fields, name) {
			fields = append(fields, name)
		}
	}
	labels := make([]string, 0, len(plan.required))
	for label := range plan.required {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	for _, label := range labels {
		add(label)
		if p.labelTranslator == nil {
			continue
		}
		add(p.labelTranslator.ToVL(label))
		for vl, loki := range p.labelTranslator.vlToLoki {
			if loki == label {
				add(vl)
			}
		}
		if p.labelTranslator.style != LabelStyleUnderscores {
			continue
		}
		if strings.Contains(label, "__") || strings.HasSuffix(label, "_") {
			// SanitizeLabelName never yields consecutive or trailing underscores,
			// so only the field of the label's own name exposes it.
			continue
		}
		lead := label
		if i := strings.IndexByte(label, '_'); i >= 0 {
			lead = label[:i]
		}
		if lead == "" || lead == "key" || label == "key_empty" || !orderedJSONASCIIAlnum(lead) {
			return "" // sanitized from a field without a common prefix
		}
		add(lead + "*")
	}
	quoted := make([]string, len(fields))
	for i, field := range fields {
		if prefix, ok := strings.CutSuffix(field, "*"); ok {
			quoted[i] = quoteLogsQLIdent(prefix) + "*"
		} else {
			quoted[i] = quoteLogsQLIdent(field)
		}
	}
	return " | fields " + strings.Join(quoted, ", ")
}

func orderedJSONASCIIAlnum(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= utf8.RuneSelf || (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

// addOrderedJSONRowLines reads the brace-opened lines VictoriaLogs cannot
// vouch for from the buckets holding them (rowEdges), evaluates them with the
// row evaluator and adds them to the stats series.
func (p *Proxy) addOrderedJSONRowLines(ctx context.Context, plan *orderedJSONMetricPlan, merged map[string]manualSeriesSamples, base string, rowEdges []int64, hazard bool, bucket time.Duration, start, end time.Time, step time.Duration) (map[string]manualSeriesSamples, error) {
	rowQuery := base + " | filter " + jsonObjectStartFilter + jsonValidityPipes(true) + " | filter " + jsonInvalidCondition(hazard) + " | delete " + jsonValidField + p.orderedJSONRowProjection(plan)
	rows, err := p.collectOrderedJSONRowSpans(ctx, plan, rowQuery, orderedJSONRowSpans(rowEdges, bucket), start, end, step)
	if err != nil {
		return nil, err
	}
	mergeOrderedJSONRowSeries(merged, rows, plan.function == "bytes_rate" || plan.function == "bytes_over_time")
	return capSeriesForRequest(ctx, merged, p.resolvedMaxStatsQuerySeries(ctx))
}

// collectOrderedJSONRowSpans evaluates the raw-row lines of a split plan:
// rowQuery over each span, sharing the row and byte limits of one raw read.
func (p *Proxy) collectOrderedJSONRowSpans(ctx context.Context, plan *orderedJSONMetricPlan, rowQuery string, spans [][2]time.Time, start, end time.Time, step time.Duration) (map[string]manualSeriesSamples, error) {
	reader, err := p.newOrderedJSONRowReader()
	if err != nil {
		return nil, err
	}
	for _, span := range spans {
		from, to := span[0], span[1]
		if windowStart := start.Add(-plan.window); from.Before(windowStart) {
			from = windowStart
		}
		if to.After(end) {
			to = end
		}
		if to.Before(from) {
			continue
		}
		if err := p.readOrderedJSONMetricRows(ctx, plan, reader, rowQuery, from, to, start, end, step); err != nil {
			return nil, err
		}
	}
	return reader.series, nil
}

// dropSeriesWithoutPoints removes the series none of whose counted buckets falls
// in an evaluation window (T-window, T], T = start+k*step up to end: Loki's
// answer holds no series without a point, so such a series (a line in the gap
// between windows when step exceeds the window) is no series for the limit
// either. Buckets sit on the grid of gcd(step, window) anchored at
// start-window, so a bucket is inside a window or outside it entirely; a bucket
// at left edge e is the sample at e+1ns of orderedJSONSampleVisible.
func dropSeriesWithoutPoints(series map[string]manualSeriesSamples, start, end time.Time, step, window time.Duration) {
	startNS, endNS := start.UnixNano(), end.UnixNano()
	for key, entry := range series {
		visible := false
		if entry.PresentBuckets != nil {
			// A byte series counts where a line is present, even an empty one.
			for _, edge := range entry.PresentBuckets {
				if visible = orderedJSONSampleVisible(edge+1, startNS, endNS, int64(step), int64(window)); visible {
					break
				}
			}
		} else {
			for _, sample := range entry.Samples {
				if sample.value != 0 && orderedJSONSampleVisible(sample.ts+1, startNS, endNS, int64(step), int64(window)) {
					visible = true
					break
				}
			}
		}
		if !visible {
			delete(series, key)
		}
	}
}

// storedLevelForeignFieldRisk reports whether a selected line holds a stored
// level field other than detected_level and level (severity, lvl, the OTLP
// severity fields, ...). The stored level of a split plan is the copy of level,
// which cannot tell a stream label from structured metadata, and Loki reads
// the stream's level fields before the metadata's: a stream severity=error with
// metadata level=info is error for Loki and info for the copy. Such a window is
// left to the row evaluator, which derives the level like Loki's ingest.
func (p *Proxy) storedLevelForeignFieldRisk(ctx context.Context, base string, start, end time.Time) (bool, error) {
	var has []string
	for _, name := range translator.DetectedLevelFieldNames {
		if name != detectedLevelLabel && name != "level" {
			has = append(has, quoteLogsQLIdent(name)+":*")
		}
	}
	key := "json-split-level-foreign:v1:" + getOrgID(ctx) + ":" + base
	_, risky, err := p.coveredWindowProbe(ctx, key, start, end, func(from, to time.Time, _ string) (string, bool, error) {
		risky, err := p.statsPushdownRiskExists(ctx, base+" | filter ("+strings.Join(has, " OR ")+") | limit 1", from, to)
		return "", risky, err
	})
	return risky, err
}
