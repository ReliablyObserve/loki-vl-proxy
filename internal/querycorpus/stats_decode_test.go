package querycorpus_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// refSpec, refStatsSpec, refIsStats and refIsStatsHeuristic are the proxy's stats readers as they were before they
// moved into the translator's codec (internal/proxy parseStatsCompatSpec and isStatsQuery), kept as the reference
// translator.DecodeStats is held to.
type refSpec struct {
	BaseQuery   string
	GroupBy     []string
	OrigGroupBy []string
	ByExplicit  bool
	Func        string
	Field       string
}

func refStatsSpec(logsqlQuery string) (refSpec, bool) {
	idx := strings.Index(logsqlQuery, "| stats ")
	if idx < 0 {
		return refSpec{}, false
	}

	spec := refSpec{
		BaseQuery: strings.TrimSpace(logsqlQuery[:idx]),
	}
	rest := strings.TrimSpace(logsqlQuery[idx+len("| stats "):])
	if rest == "" {
		return refSpec{}, false
	}

	if strings.HasPrefix(rest, "by (") {
		closeIdx := strings.Index(rest, ")")
		if closeIdx > len("by (") {
			labels := strings.TrimSpace(rest[len("by ("):closeIdx])
			if labels != "" {
				for _, label := range strings.Split(labels, ",") {
					label = strings.TrimSpace(label)
					if label != "" {
						spec.GroupBy = append(spec.GroupBy, label)
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
		return refSpec{}, false
	}

	if spec.BaseQuery == "" || spec.Func == "" {
		return refSpec{}, false
	}

	return spec, true
}

func refIsStats(logsqlQuery string) bool {
	q, err := logsql.Parse(logsqlQuery)
	if err != nil {
		// Fall back to old quote-aware heuristic only on parse failure.
		return refIsStatsHeuristic(logsqlQuery)
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
	return refIsStatsHeuristic(logsqlQuery)
}
func refIsStatsHeuristic(logsqlQuery string) bool {
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

// translator.DecodeStats reads every corpus row's translation, under every profile and with the post-processing
// markers off as the handlers read it, exactly as the proxy's stats readers did: the same stats verdict and the
// same shape.
//
// conformance: architecture-syntax-package-layering
func TestDecodeStatsReadsLikeTheProxyReaders(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	rows := planCorpus(t)
	total := 0
	for _, prof := range planProfiles() {
		t.Run(prof.name, func(t *testing.T) {
			compared, stats, shaped := 0, 0, 0
			for _, q := range rows {
				text, ok := prof.translate(q)
				if !ok {
					continue
				}
				clean, _ := translator.SplitPost(text)
				for _, s := range []string{text, clean} {
					compared++
					got, isStats := translator.DecodeStats(s)
					if want := refIsStats(s); isStats != want {
						t.Fatalf("%q: stats %v, proxy reader %v", s, isStats, want)
					}
					spec, ok := refStatsSpec(s)
					gotSpec := refSpec{BaseQuery: got.Base, GroupBy: got.By, ByExplicit: got.ByExplicit, Func: got.Func, Field: got.Field}
					if got.Shaped != ok || (ok && !reflect.DeepEqual(gotSpec, spec)) || got.Text != s {
						t.Fatalf("%q: shape %+v (shaped %v), proxy reader %+v (%v)", s, gotSpec, got.Shaped, spec, ok)
					}
					if isStats {
						stats++
					}
					if ok {
						shaped++
					}
				}
			}
			// The keep-line profiles translate only log queries.
			t.Logf("%d texts, %d stats queries, %d shaped", compared, stats, shaped)
			total += shaped
		})
	}
	if total == 0 {
		t.Fatal("the corpus has no shaped stats query")
	}
}
