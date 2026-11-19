package proxy

import (
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// logRoute is the route the handlers take for a log query (a LogSelect at the root), predicted from its plan: an
// instant request is Loki's 400 once translated, a range request without a positive step is Loki's 400 before
// anything else, and a range request reads the plan's text newest first, the request's window and limit, in one
// VictoriaLogs call.
func logRoute(q string, p *queryplan.Plan, rp routeProfile, data bool) (routeRecord, error) {
	// The handlers read the query as the request's rewrite leaves it (lowercaseQueryKeywords): a query the rewrite
	// changes takes the route of the text it writes, with a rewrite step.
	if text := routeHandlerText(q); text != q {
		rec, err := logRoute(text, p, rp, data)
		if err != nil {
			return routeRecord{}, err
		}
		rec.Steps = uniqueSorted(append(rec.Steps, "rewrite "+text))
		return rec, nil
	}
	sel := p.Root.(*queryplan.LogSelect)
	// A json and a logfmt parser in one query: the handlers probe VictoriaLogs for the parser the lines answer
	// (preferWorkingParser), a probe the plan does not hold yet.
	if hasParser(sel, logqlpkg.ParserJSON) && hasParser(sel, logqlpkg.ParserLogfmt) {
		return routeRecord{}, routeTextDelegation(p, "working parser probe")
	}
	// The handlers send the string translator's text where the typed gate does not serve the plan's
	// (servesLogStages: an equality on a number, a reserved field, a value the translator misreads).
	// The handlers send the string translator's text for a query the lowering's admission turns away
	// (lowerLogSelect: the text screen of the flipped stage families, then the typed gate servesLogStages, which
	// leaves an equality on a number, a reserved field and a value the translator misreads to the translator).
	flipped := lower.LogStagesOf(QueryPlanFlippedLogStages)
	stages, allowed, screened := screenLogQuery(logqlpkg.CanonicalizeLogRanges(q), flipped)
	if !screened || stages != 0 && !servesLogStages(p, routeLoweringProfile(), allowed) {
		return routeRecord{}, routeTextDelegation(p, "translator text")
	}
	op, err := lower.Plan(p, routeLoweringProfile())
	if err != nil {
		return routeRecord{}, err
	}
	// Every VictoriaLogs query passes the outbound guard (guardOutboundQuery): a label named like a field VictoriaLogs
	// reserves in another case, which a parser extracted, is copied aside before it is filtered.
	vl, ok := lower.GuardOutbound(op).(physical.VLQuery)
	if !ok {
		return routeRecord{}, errRouteRulePending
	}
	if rp.instant {
		return routeRecord{Status: 400, Steps: []string{"operation prefer_working_parser bypass", "operation translate_query translated"}}, nil
	}
	// The request derives its window and step from the query's range (5m for a log query, firstRange).
	r := firstRange(p.Root)
	step, _ := parsePositiveStepDuration(strconv.FormatFloat(rp.step(r).Seconds(), 'f', -1, 64))
	if step <= 0 {
		return routeRecord{Status: 400}, nil
	}
	start := routeEnd.Add(-rp.window(r))
	form := url.Values{
		"query":   {vl.Text + " | sort by (_time desc)"},
		"start":   {strconv.FormatInt(start.UnixNano(), 10)},
		"end":     {strconv.FormatInt(routeEnd.UnixNano(), 10)},
		"limit":   {"100"},
		"timeout": {routeTimeout},
	}
	_ = data
	return routeRecord{
		Status:  200,
		Calls:   []string{"/select/logsql/query?" + form.Encode()},
		Started: 1,
		Steps:   uniqueSorted([]string{"operation prefer_working_parser bypass", "operation query_steps_eligibility disabled", "operation translate_query translated"}),
	}, nil
}

// hasParser reports a parser stage of the type in the log query's pipeline.
func hasParser(sel *queryplan.LogSelect, typ logqlpkg.ParserType) bool {
	for _, st := range sel.Stages {
		if ps, ok := st.(*logqlpkg.ParserStage); ok && ps.Type == typ {
			return true
		}
	}
	return false
}

// The log route rule is held by the harness: a prediction with the other sort direction, another limit or one more
// step mispredicts every log run it changes, and the rule itself predicts each run it does not delegate.
//
// conformance: architecture-syntax-package-layering
func TestLogRouteCatchesAWrongDecision(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	var rows []routeRow
	for _, row := range routeRows(t, filepath.Join("..", ".."), 0) {
		if _, ok := row.plan.Root.(*queryplan.LogSelect); ok && len(rows) < 40 {
			rows = append(rows, row)
		}
	}
	if len(rows) == 0 {
		t.Fatal("no log rows in the plan corpus")
	}
	logOnly := func(change func(*routeRecord)) routePrediction {
		return func(q string, p *queryplan.Plan, rp routeProfile, data bool, _ routeRecord) (routeRecord, error) {
			rec, err := logRoute(q, p, rp, data)
			if err == nil && change != nil {
				change(&rec)
			}
			return rec, err
		}
	}
	sec, problems := runRouteParity(t, rows, logOnly(nil))
	if sec.Native == 0 || sec.Mismatches != 0 {
		t.Fatalf("the log route rule: %+v\n%s", sec, joinFirstRoute(problems, 3))
	}
	for name, change := range map[string]func(*routeRecord){
		"oldest first": func(r *routeRecord) {
			for i, c := range r.Calls {
				r.Calls[i] = strings.Replace(c, "sort+by+%28_time+desc%29", "sort+by+%28_time%29", 1)
			}
		},
		"another limit": func(r *routeRecord) {
			for i, c := range r.Calls {
				r.Calls[i] = strings.Replace(c, "limit=100", "limit=1000", 1)
			}
		},
		"one more step": func(r *routeRecord) {
			if r.Status == 200 {
				r.Steps = uniqueSorted(append(r.Steps, "operation raw_rows probe"))
			}
		},
	} {
		bad, _ := runRouteParity(t, rows, logOnly(change))
		if bad.Mismatches == 0 || bad.Native >= sec.Native {
			t.Errorf("%s: the harness does not see it: %+v (rule %+v)", name, bad, sec)
		}
	}
}
