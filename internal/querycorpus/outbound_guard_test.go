package querycorpus_test

import (
	"reflect"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The outbound guard (logsql.GuardReservedFieldCase, which the proxy runs on every query it sends) leaves every
// text of the plan corpus as it is, byte for byte, under every profile, the lowering's and the string translator's
// alike, unless its LogQL query names _time, _stream or _stream_id in another letter case (the rows of
// semantics/reserved-field-name-case-outbound); such a text is rewritten once and only once: a second pass changes
// nothing.
//
// conformance: semantics/reserved-field-name-case-outbound
func TestOutboundGuardOverTheCorpus(t *testing.T) {
	queries, err := plancorpus.Queries("../..")
	if err != nil {
		t.Fatal(err)
	}
	rows, rewritten := 0, 0
	check := func(q string, op physical.Op) {
		var texts []string
		collectQueryTexts(reflect.ValueOf(&op), &texts)
		for _, text := range texts {
			rows++
			sent := logsql.GuardReservedFieldCase(text)
			if sent == text {
				continue
			}
			rewritten++
			if !namesReservedFieldOtherCase(q) {
				t.Errorf("%s: the guard rewrites %s to %s, and the query names no such field", q, text, sent)
			}
			if again := logsql.GuardReservedFieldCase(sent); again != sent {
				t.Errorf("the guard's %s is rewritten again to %s", sent, again)
			}
		}
	}
	for _, q := range queries {
		expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
		var plan *queryplan.Plan
		if err == nil {
			if p, err := queryplan.Build(expr, queryplan.Inputs{}); err == nil {
				queryplan.Resolve(p)
				plan = p
			}
		}
		for _, prof := range planProfiles() {
			if plan != nil {
				if op, err := nativeLowering(q, plan, prof); err == nil {
					check(q, op)
				}
			}
			if legacy, ok := prof.translate(q); ok {
				if op, err := translator.DecodePhysical(legacy); err == nil {
					check(q, op)
				}
			}
		}
	}
	t.Logf("%d texts, %d rewritten", rows, rewritten)
	if rows < 20000 {
		t.Fatalf("only %d texts", rows)
	}
	if rewritten == 0 {
		t.Error("no corpus text is rewritten: the rows of semantics/reserved-field-name-case-outbound are missing")
	}
}

// namesReservedFieldOtherCase reports a query that names _time, _stream or _stream_id in another case as a word.
func namesReservedFieldOtherCase(q string) bool {
	for i := 0; i < len(q); i++ {
		if q[i] != '_' || i > 0 && isWordByte(q[i-1]) {
			continue
		}
		j := i + 1
		for j < len(q) && isWordByte(q[j]) {
			j++
		}
		if logsql.ReservedFieldNameOtherCase(q[i:j]) {
			return true
		}
	}
	return false
}

func isWordByte(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}

// collectQueryTexts appends the VictoriaLogs query texts an operator tree holds.
func collectQueryTexts(v reflect.Value, out *[]string) {
	switch v.Kind() {
	case reflect.Interface, reflect.Pointer:
		if !v.IsNil() {
			collectQueryTexts(v.Elem(), out)
		}
	case reflect.Struct:
		switch x := v.Interface().(type) {
		case physical.VLQuery:
			*out = append(*out, x.Text)
			return
		case physical.VLStats:
			*out = append(*out, x.Text)
			return
		}
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				collectQueryTexts(v.Field(i), out)
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			collectQueryTexts(v.Index(i), out)
		}
	}
}
