package proxy

import (
	"reflect"
	"strings"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// applyBareFieldMutationToStreamLabels applies a bare drop and then a bare keep
// to a stream label set, as a log answer does.
func applyBareFieldMutationToStreamLabels(drop, keep []string, raw, translated map[string]string, lt *LabelTranslator) (string, map[string]string, bool) {
	_, got, changed := applyBareDropToStreamLabels(drop, raw, translated, lt)
	if !changed {
		got = translated
	}
	if len(keep) > 0 {
		if kept, ok := (keepRules{{bare: keep}}).streamLabels(got, lt); ok {
			got, changed = kept, true
		}
	}
	if !changed {
		return "", nil, false
	}
	return canonicalLabelsKey(got), got, true
}

// conditions are the stage's matchers as conditions, whichever form the rule holds them in.
func (k *keepRule) conditions() []translate.DropCondition {
	if k.eq == nil {
		return k.conds
	}
	out := make([]translate.DropCondition, 0, len(k.eq))
	for _, m := range k.eq {
		dc, _ := translate.NewDropCondition(m.Name, m.Op, m.Value) // = and != compile nothing
		out = append(out, dc)
	}
	return out
}

// flatDropKeep lists the conditions and bare names of a query's drop and keep stages.
func flatDropKeep(query string) (drop, keep []translate.DropCondition, bareDrop, bareKeep []string) {
	drop, bareDrop, rules := extractDropKeepFromAST(query)
	for _, r := range rules {
		keep = append(keep, r.conditions()...)
		bareKeep = append(bareKeep, r.bare...)
	}
	return drop, keep, bareDrop, bareKeep
}

// The keep stages of a query, applied to an entry's labels, leave Loki's labels: a name listed bare or matched by a
// matcher of the stage, in each stage in turn; a parser after a stage adds labels the stage did not see. The answers
// against Loki are TestPlanMatchesLokiOnLogQueries over the rows of the case.
//
// conformance: semantics/keep-names-and-matchers
func TestKeepRules(t *testing.T) {
	stream := map[string]string{"app": "x", "level": "info", "service_name": "x"}
	for _, tc := range []struct {
		query                    string
		sm, pf                   map[string]string
		wantStream, wantSM, want map[string]string
	}{
		{query: `{a="b"} | keep app, level="error"`, wantStream: map[string]string{"app": "x"}},
		{query: `{a="b"} | keep level="info", app`, wantStream: map[string]string{"app": "x", "level": "info"}},
		{query: `{a="b"} | keep level="info"`, wantStream: map[string]string{"level": "info"}},
		{query: `{a="b"} | keep level="info", level="error"`, wantStream: map[string]string{"level": "info"}},
		{query: `{a="b"} | keep level="error", level="info"`, wantStream: map[string]string{"level": "info"}},
		{query: `{a="b"} | keep app, app="nope"`, wantStream: map[string]string{"app": "x"}},
		{query: `{a="b"} | keep app, level | keep level`, wantStream: map[string]string{"level": "info"}},
		{query: `{a="b"} | keep level!="info"`, wantStream: map[string]string{}},
		{query: `{a="b"} | keep level!~"in.*", service_name`, wantStream: map[string]string{"service_name": "x"}},
		{
			query:      `{a="b"} | keep trace="1"`,
			sm:         map[string]string{"trace": "1", "user": "u", "__error__": "e"},
			pf:         map[string]string{"trace": "2", "msg": "m"},
			wantStream: map[string]string{}, wantSM: map[string]string{"trace": "1", "__error__": "e"}, want: map[string]string{},
		},
		{
			// The parser runs after the keep: its labels are not the keep's to remove.
			query:      `{a="b"} | keep level="error", app | json`,
			pf:         map[string]string{"level": "error", "msg": "m", "a": "x"},
			wantStream: map[string]string{"app": "x"}, want: map[string]string{"level": "error", "msg": "m", "a": "x"},
		},
		{
			query:      `{a="b"} | json | keep msg="m"`,
			pf:         map[string]string{"level": "error", "msg": "m"},
			wantStream: map[string]string{}, want: map[string]string{"msg": "m"},
		},
	} {
		lq, err := logqlpkg.Parse(tc.query)
		if err != nil {
			t.Fatalf("%s: %v", tc.query, err)
		}
		rules := keepRulesOf(lq.(*logqlpkg.LogQuery).Pipeline)
		got, _ := rules.streamLabels(stream, nil)
		if !reflect.DeepEqual(got, tc.wantStream) {
			t.Errorf("%s: stream %v, want %v", tc.query, got, tc.wantStream)
		}
		sm, pf := cloneStringMap(tc.sm), cloneStringMap(tc.pf)
		rules.entry(sm, pf, nil)
		if tc.sm != nil && !reflect.DeepEqual(sm, tc.wantSM) {
			t.Errorf("%s: structured metadata %v, want %v", tc.query, sm, tc.wantSM)
		}
		if tc.pf != nil && !reflect.DeepEqual(pf, tc.want) {
			t.Errorf("%s: parsed %v, want %v", tc.query, pf, tc.want)
		}
	}
	if len(stream) != 3 {
		t.Fatalf("the input labels were changed: %v", stream)
	}
}

// FuzzKeepRules holds the stream labels the keep stages leave to LogQL's documented meaning: stage by stage, a label
// stays when the stage lists its name bare or one of the stage's matchers on that name matches its value.
//
// conformance: semantics/keep-names-and-matchers
func FuzzKeepRules(f *testing.F) {
	for _, seed := range [][]byte{{0}, {1, 0, 2}, {3, 5, 8, 13, 21, 34}, {255, 1, 254, 2, 253, 3, 252, 4}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, seed []byte) {
		at := 0
		pick := func(n int) int {
			if len(seed) == 0 {
				return 0
			}
			v := int(seed[at%len(seed)]) + at/len(seed)
			at++
			return v % n
		}
		names := []string{"app", "level", "zone"}
		values := []string{"x", "error", "z1", ""}
		type item struct{ name, op, value string }
		var stages [][]item
		query := `{a="b"}`
		for s := 0; s <= pick(3); s++ {
			var stage []item
			var written []string
			for i := 0; i <= pick(4); i++ {
				it := item{name: names[pick(len(names))], op: []string{"", "=", "!=", "=~", "!~"}[pick(5)]}
				if it.op == "" {
					written = append(written, it.name)
				} else {
					it.value = values[pick(len(values))]
					written = append(written, it.name+it.op+`"`+it.value+`"`)
				}
				stage = append(stage, it)
			}
			stages = append(stages, stage)
			query += " | keep " + strings.Join(written, ", ")
		}
		labels := map[string]string{}
		for _, n := range names {
			if pick(4) > 0 {
				labels[n] = values[pick(len(values)-1)]
			}
		}
		want := cloneStringMap(labels)
		for _, stage := range stages {
			for name, value := range want {
				kept := false
				for _, it := range stage {
					if it.name != name {
						continue
					}
					switch it.op {
					case "":
						kept = true
					case "=":
						kept = kept || value == it.value
					case "!=":
						kept = kept || value != it.value
					case "=~":
						kept = kept || value == it.value
					case "!~":
						kept = kept || value != it.value
					}
				}
				if !kept {
					delete(want, name)
				}
			}
		}
		lq, err := logqlpkg.Parse(query)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		got, _ := keepRulesOf(lq.(*logqlpkg.LogQuery).Pipeline).streamLabels(labels, nil)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s over %v: %v, want %v", query, labels, got, want)
		}
	})
}
