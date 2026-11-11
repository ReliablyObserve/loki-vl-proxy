package lower

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// guardedLegacy is the string translator's text the lowering is held to: for an unwrap of a label named _time,
// _stream or _stream_id in another case, the text the proxy sends for it, through the outbound guard.
func guardedLegacy(p *queryplan.Plan, legacy string, terr error) string {
	if terr != nil || p == nil || !UnwrapsReservedFieldOtherCase(p) {
		return legacy
	}
	op, err := translator.DecodePhysical(legacy)
	if err != nil {
		return legacy
	}
	return translator.EncodePhysical(GuardOutbound(op))
}

// The lowering writes the unwrap of a label named _time, _stream or _stream_id in another letter case in the outbound
// guard's form, byte for byte the translator's text guarded, under every profile and every outer aggregation: the
// guard has nothing left to rewrite in it, and no filter in it names such a field.
//
// conformance: semantics/reserved-field-name-case-outbound
func TestPlanUnwrapsReservedFieldOtherCaseSafely(t *testing.T) {
	checked := 0
	for _, name := range []string{"_TIME", "_Time", "_STREAM", "_Stream", "_STREAM_ID", "_Stream_Id"} {
		for _, shape := range []string{
			`sum_over_time({app="a"} | logfmt | unwrap N [1m])`, `sum(sum_over_time({app="a"} | logfmt | unwrap N [1m]))`,
			`sum by (app) (rate({app="a"} | json | unwrap duration(N) [5m]))`, `avg_over_time({app="a"} | logfmt | unwrap bytes(N) [1m])`,
			`max_over_time({app="a"} | logfmt | unwrap N [1m]) by (app)`, `quantile_over_time(0.5, {app="a"} | logfmt | unwrap N [1m])`,
			`stdvar_over_time({app="a"} | logfmt | unwrap N [1m])`, `sum_over_time({app="a"} | logfmt | N!="" | unwrap N [1m])`,
		} {
			q := replaceN(shape, name)
			p := rangeAggPlan(q)
			if !UnwrapsReservedFieldOtherCase(p) {
				t.Fatalf("%s: UnwrapsReservedFieldOtherCase is false", q)
			}
			for _, prof := range rangeProfiles() {
				lowered, legacy, delegated, ok := unwrapLowering(t, q, prof)
				if !ok || delegated {
					continue
				}
				checked++
				if lowered != legacy {
					t.Errorf("%s [%s]:\n lowered %s\n guarded %s", q, prof.name, lowered, legacy)
				}
				op, _ := PlanQuery(q, p, prof.lowerProfile())
				var texts []string
				collectGuardTexts(op, &texts)
				for _, text := range texts {
					if logsql.GuardReservedFieldCase(text) != text {
						t.Errorf("%s [%s]: the guard still rewrites %s", q, prof.name, text)
					}
				}
			}
		}
	}
	if checked < 200 {
		t.Fatalf("only %d lowered queries checked", checked)
	}
	if UnwrapsReservedFieldOtherCase(rangeAggPlan(`sum_over_time({app="a"} | logfmt | unwrap _time [1m])`)) ||
		UnwrapsReservedFieldOtherCase(rangeAggPlan(`sum_over_time({app="a"} | logfmt | unwrap TIME [1m])`)) {
		t.Error("a lower-case or plain name is not another case")
	}
}

func replaceN(shape, name string) string {
	out := []byte{}
	for i := 0; i < len(shape); i++ {
		if shape[i] == 'N' && (i+1 == len(shape) || shape[i+1] == ' ' || shape[i+1] == ')' || shape[i+1] == '!') {
			out = append(out, name...)
			continue
		}
		out = append(out, shape[i])
	}
	return string(out)
}

// collectGuardTexts appends the VictoriaLogs texts of op.
func collectGuardTexts(op physical.Op, texts *[]string) {
	switch o := op.(type) {
	case physical.VLQuery:
		*texts = append(*texts, o.Text)
	case physical.VLStats:
		*texts = append(*texts, o.Text)
	case physical.Binary:
		collectGuardTexts(o.L, texts)
		collectGuardTexts(o.R, texts)
	}
}
