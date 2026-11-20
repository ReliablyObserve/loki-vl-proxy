package proxy

import (
	"io"
	"maps"
	"strings"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	fj "github.com/valyala/fastjson"
)

// numericFilterConversion distinguishes label-filter probes from unwrap
// conversions while sharing their bounded stats-hit confirmation path.
const numericFilterConversion = "label_filter"

// These trusted pipes share one typed condition and are encoded once. Request
// rewrites append them without parsing or rebuilding the condition.
var numericErrorCondition = logsql.AllOf{
	logsql.FieldFilter{Field: errorLabel, Op: logsql.FieldOpAny},
	logsql.Exclude{Expr: logsql.FieldFilter{Field: "__preserve_error__", Op: logsql.FieldOpExact, Value: "true"}},
}

var numericErrorLookupPipe = (logsql.PipeFilter{Expr: numericErrorCondition}).String()

var numericErrorCountingPipes = (logsql.PipeFormat{Template: "0", ResultField: unwrapBadField}).String() + " " +
	(logsql.PipeFormat{Template: "1", ResultField: unwrapBadField, If: numericErrorCondition}).String()

func numericErrorKey(name string) bool {
	return name == errorLabel || name == errorDetailsLabel
}

func numericErrorEntry(entry map[string]interface{}) bool {
	return entry[errorLabel] != nil || entry[errorDetailsLabel] != nil
}

func (e *lineFieldExposure) numericErrorField(name string) bool {
	return e != nil && len(e.numericPipeline) > 0 && numericErrorKey(name)
}

func (c *unwrapCheck) takeNumericErrorEntry(base string, ts int64, entry map[string]interface{}) {
	if c != nil && asString(entry[errorLabel]) != "" && asString(entry["__preserve_error__"]) != "true" && c.accepts(errorLabel, numericFilterConversion) {
		c.numericRowHit(base, ts)
		delete(entry, errorLabel)
	}
}

func (c *unwrapCheck) takeNumericErrorRow(base string, ts int64, row *fj.Value) {
	if c != nil && len(row.GetStringBytes(errorLabel)) != 0 && string(row.GetStringBytes("__preserve_error__")) != "true" && c.accepts(errorLabel, numericFilterConversion) {
		c.numericRowHit(base, ts)
		row.Del(errorLabel)
	}
}

func (c *unwrapCheck) failNumericProof(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.numericFailure == nil {
		c.numericFailure = notServed("numeric label-filter error confirmation", err)
	}
}

// A numeric-only raw read returns the same marked rows to every waiter. Each
// caller inspects those rows itself, so sharing the transport cannot lose its
// request-local hits. Stats rewrites still confirm hits inside the transport.
func (c *unwrapCheck) sharedNumericRawRead(path string) bool {
	if c == nil || c.json != nil || len(c.probes) == 0 || path != "/select/logsql/query" {
		return false
	}
	for _, probe := range c.probes {
		if !probe.numeric {
			return false
		}
	}
	return true
}

// numericRowHit records a retained error row read by an existing raw-row
// metric route, within the unwrap check's shared per-request lookup budget.
func (c *unwrapCheck) numericRowHit(base string, ts int64) {
	probes := c.numericProbesFor(base)
	c.mu.Lock()
	defer c.mu.Unlock()
	visible := false
	for _, probe := range probes {
		if !probe.numeric {
			continue
		}
		_, _, last := c.windows(probe)
		if c.visible(ts, probe, last) {
			visible = true
			break
		}
	}
	if !visible {
		return
	}
	if c.rawHits >= unwrapMaxRawHits {
		return
	}
	c.rawHits++
	from := time.Unix(0, ts)
	c.hits = append(c.hits, unwrapHit{base: base, field: errorLabel, conv: numericFilterConversion, from: from, to: from.Add(time.Nanosecond)})
}

// numericQuery applies Loki's numeric-parser hints after translation, outside
// the shared translation cache. Source error/preserve keys that the aggregation
// does not request must not become runtime errors or change generated errors.
// It also binds each physical base to its originating typed operand.
func (c *unwrapCheck) numericQuery(input, query string) string {
	if !strings.Contains(query, `"LabelFilterErr" as __error__`) {
		return query
	}
	var probes []unwrapErrorProbe
	for _, probe := range c.probes {
		if probe.numeric {
			probes = append(probes, probe)
		}
	}
	if len(probes) == 0 {
		return query
	}
	expr, ok := sharedLogQLExpr(input)
	if !ok || holdsBinaryOperand(expr) {
		// Binary markers are not a single VL pipeline. Each operand is
		// translated and bound separately by its existing handler route.
		return query
	}
	multiple := len(probes) > 1
	if multiple {
		source := ""
		var hints map[string]bool
		if lq, ok := expr.(*logqlpkg.LogQuery); ok {
			source = lq.String()
		} else {
			inputProbes := unwrapErrorProbes(expr)
			if len(inputProbes) != 1 || !inputProbes[0].numeric {
				return query
			}
			source = inputProbes[0].numericSource
			hints = inputProbes[0].hints
		}
		matched := probes[:0]
		for _, probe := range probes {
			if probe.numericSource == source && (probe.hints == nil) == (hints == nil) && maps.Equal(probe.hints, hints) {
				matched = append(matched, probe)
			}
		}
		probes = matched
		if len(probes) == 0 {
			return query
		}
	}
	var deleted []string
	for _, name := range []string{errorLabel, errorDetailsLabel, "__preserve_error__"} {
		needed := false
		for _, probe := range probes {
			if probe.hints == nil || probe.hints[name] {
				needed = true
				break
			}
		}
		if !needed {
			deleted = append(deleted, name)
		}
	}
	pipes, ok := unwrapSplitPipes(" | " + query)
	if !ok {
		c.failNumericProof(io.ErrUnexpectedEOF)
		return query
	}
	if len(deleted) > 0 {
		for i, pipe := range pipes {
			if strings.HasPrefix(pipe, "unpack_json") || strings.HasPrefix(pipe, "unpack_logfmt") {
				tail := append([]string{"delete " + strings.Join(deleted, ", ")}, pipes[i+1:]...)
				pipes = append(pipes[:i+1], tail...)
				query = strings.Join(pipes, " | ")
				break
			}
		}
	}
	if multiple {
		base := query
		for i, pipe := range pipes {
			if strings.HasPrefix(pipe, "stats ") {
				base = strings.Join(pipes[:i], " | ")
				break
			}
		}
		c.mu.Lock()
		if c.numericBases == nil {
			c.numericBases = make(map[string][]unwrapErrorProbe)
		}
		c.numericBases[base] = probes
		c.mu.Unlock()
	}
	return query
}

// numericProbesFor reads the base binding created by the translation that is
// actually executed. Physical window filters may follow that base; the longest
// registered prefix prevents a shorter, different operand from claiming it.
func (c *unwrapCheck) numericProbesFor(base string) []unwrapErrorProbe {
	var only *unwrapErrorProbe
	count := 0
	for i := range c.probes {
		if c.probes[i].numeric {
			count++
			only = &c.probes[i]
		}
	}
	if count == 1 {
		return []unwrapErrorProbe{*only}
	}
	c.mu.Lock()
	var probes []unwrapErrorProbe
	longest := -1
	for prefix, candidates := range c.numericBases {
		if base != prefix && !strings.HasPrefix(base, prefix+" | ") {
			continue
		}
		if len(prefix) > longest {
			longest = len(prefix)
			probes = candidates
		}
	}
	c.mu.Unlock()
	if len(probes) == 0 {
		c.failNumericProof(io.ErrUnexpectedEOF)
	}
	return probes
}

// Keep operand identity when a trusted window rewrite inserts pipes before
// the registered parser prefix. Unrelated/unwrap-only queries have no binding.
func (c *unwrapCheck) numericDerivedQuery(base, derived string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	longest := -1
	var probes []unwrapErrorProbe
	for prefix, candidates := range c.numericBases {
		if (base == prefix || strings.HasPrefix(base, prefix+" | ")) && len(prefix) > longest {
			longest, probes = len(prefix), candidates
		}
	}
	if len(probes) > 0 {
		c.numericBases[derived] = probes
	}
}

// numericEqualityPipeline reports the parser pipelines whose equality
// conversions are classified by the typed lowering. Other stages keep their
// existing execution path and their registered compatibility gaps.
func numericEqualityPipeline(stages []logqlpkg.Stage) bool {
	return lower.NumericEqualityErrorPipeline(stages)
}

// numericFilterCountingQuery counts retained conversion-error rows separately
// in the metric's existing stats read. Clean reads add no backend request;
// flagged buckets use the same bounded stored-row lookup as unwrap errors.
func numericFilterCountingQuery(query string) (rewritten, base string, ok bool) {
	if !strings.Contains(query, `"LabelFilterErr" as __error__`) {
		return query, "", false
	}
	pipes, valid := unwrapSplitPipes(" | " + query)
	if !valid {
		return query, "", false
	}
	for i, pipe := range pipes {
		if !strings.HasPrefix(pipe, "stats ") {
			continue
		}
		base = strings.Join(pipes[:i], " | ")
		rest, grouped := unwrapGroupStatsByFlag(" | " + strings.Join(pipes[i:], " | "))
		if !grouped {
			return query, "", false
		}
		return base + " " + numericErrorCountingPipes + rest, base, true
	}
	return query, "", false
}
