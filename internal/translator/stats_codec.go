package translator

import (
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// DecodeStats reads a translated query as the stats query the proxy sends: its text and the shape of its first stats
// pipe (StatsShape; Shaped when it has one), and whether it is a stats query at all (isStats: a stats pipe outside
// quoted strings). It is the reader the stats handlers used on the text, moved here so a handler decodes its query
// once and passes the operator down; it goes with the translator.
func DecodeStats(s string) (stats physical.VLStats, isStats bool) {
	stats, _ = StatsShape(s)
	stats.Text = s
	return stats, isStatsText(s)
}

// StatsShape reads the first `| stats` pipe of a translated query: the query before it (Base), its grouping
// (By, or ByExplicit for `by ()`) and its function and field. It reads the text as the proxy always has (the first
// "| stats " anywhere in it), so it answers for a query DecodeStats does not take for a stats query too.
func StatsShape(logsqlQuery string) (physical.VLStats, bool) {
	idx := strings.Index(logsqlQuery, "| stats ")
	if idx < 0 {
		return physical.VLStats{}, false
	}

	spec := physical.VLStats{
		Base: strings.TrimSpace(logsqlQuery[:idx]),
	}
	rest := strings.TrimSpace(logsqlQuery[idx+len("| stats "):])
	if rest == "" {
		return physical.VLStats{}, false
	}

	if strings.HasPrefix(rest, "by (") {
		closeIdx := strings.Index(rest, ")")
		if closeIdx > len("by (") {
			labels := strings.TrimSpace(rest[len("by ("):closeIdx])
			if labels != "" {
				for _, label := range strings.Split(labels, ",") {
					label = strings.TrimSpace(label)
					if label != "" {
						spec.By = append(spec.By, label)
					}
				}
			}
			rest = strings.TrimSpace(rest[closeIdx+1:])
		} else if closeIdx == len("by (") {
			// "by ()" — explicit empty grouping: one series, no label dimensions.
			spec.ByExplicit = true
			rest = strings.TrimSpace(rest[closeIdx+1:])
		}
	}

	openIdx := strings.Index(rest, "(")
	switch {
	case openIdx < 0:
		spec.Func = strings.TrimSpace(rest)
	case strings.HasSuffix(rest, ")"):
		spec.Func = strings.TrimSpace(rest[:openIdx])
		spec.Field = strings.TrimSpace(rest[openIdx+1 : len(rest)-1])
	default:
		return physical.VLStats{}, false
	}

	if spec.Base == "" || spec.Func == "" {
		return physical.VLStats{}, false
	}

	spec.Shaped = true
	return spec, true
}

// isStatsText returns true if the LogsQL query contains a stats pipe.
// It uses the typed AST parser when possible, falling back to a quote-aware
// string scanner on parse failure (e.g. malformed or unsupported syntax).
func isStatsText(logsqlQuery string) bool {
	q, err := logsql.Parse(logsqlQuery)
	if err != nil {
		// Fall back to old quote-aware heuristic only on parse failure.
		return isStatsTextHeuristic(logsqlQuery)
	}
	for _, p := range q.Pipes {
		switch p.(type) {
		case logsql.PipeStats, logsql.PipeRunningStats, logsql.PipeTotalStats:
			return true
		}
	}
	// VictoriaLogs shorthand "| rate(" / "| count(" are not yet modelled as
	// standalone pipe types in the local AST; detect them with the heuristic
	// but run it only when the query parsed successfully (so the filter region
	// is well-formed and the quote-aware walk is safe).
	return isStatsTextHeuristic(logsqlQuery)
}

// isStatsTextHeuristic is the original quote-aware string scanner for
// detecting stats pipes. It is used as a fallback when the AST parser fails
// and also to detect VL shorthand pipes (| rate(, | count() not yet in AST).
func isStatsTextHeuristic(logsqlQuery string) bool {
	inQuote := false
	for i := 0; i < len(logsqlQuery); i++ {
		if inQuote && logsqlQuery[i] == '\\' && i+1 < len(logsqlQuery) {
			i++
			continue
		}
		if logsqlQuery[i] == '"' {
			inQuote = !inQuote
			continue
		}
		if inQuote {
			continue
		}
		rest := logsqlQuery[i:]
		if strings.HasPrefix(rest, "| stats ") ||
			strings.HasPrefix(rest, "| rate(") ||
			strings.HasPrefix(rest, "| count(") {
			return true
		}
	}
	return false
}
