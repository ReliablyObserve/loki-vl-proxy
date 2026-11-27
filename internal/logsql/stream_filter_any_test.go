package logsql_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// TestStreamFilterAnyPrintsAndParses: a stream filter of several matcher groups prints as VictoriaLogs reads it
// ({a="1", b="2" or c="3"}), parses back to the same node and text, and a filter without `or` stays a StreamFilter.
//
// conformance: semantics/service-name-stream-filter
func TestStreamFilterAnyPrintsAndParses(t *testing.T) {
	groups := logsql.StreamFilterAny{Groups: [][]logsql.LabelMatcher{
		{{Name: "service_name", Op: "=", Value: `we"ird\x`}},
		{{Name: "service_name", Op: "=", Value: ""}, {Name: "service.name", Op: "=", Value: "a\nb"}},
		{{Name: "service_name", Op: "=", Value: ""}, {Name: "service.name", Op: "!=", Value: ""}},
	}}
	const want = `{service_name="we\"ird\\x" or service_name="", service.name="a\nb" or service_name="", service.name!=""}`
	if got := groups.String(); got != want {
		t.Fatalf("String() = %s, want %s", got, want)
	}
	if got := string(logsql.AppendFilter(nil, groups)); got != want {
		t.Fatalf("AppendFilter = %s, want %s", got, want)
	}
	q, err := logsql.Parse(want + ` level:="info"`)
	if err != nil {
		t.Fatal(err)
	}
	if got := q.String(); got != want+` level:="info"` {
		t.Fatalf("parsed and printed %s", got)
	}
	for _, tc := range []struct {
		in      string
		any     bool
		wantErr bool
	}{
		{in: `{a="1", b="2"}`},
		{in: `{a="1" or b="2"}`, any: true},
		{in: `{a="1", b="2" or c="3", d!~"x"}`, any: true},
		{in: `{a="1" or}`, wantErr: true},
		{in: `{or="1"}`, wantErr: true},
		{in: `{a="1" or or b="2"}`, wantErr: true},
	} {
		q, err := logsql.Parse(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("Parse(%s) = %v, want an error", tc.in, q)
			}
			continue
		}
		if err != nil {
			t.Fatalf("Parse(%s): %v", tc.in, err)
		}
		_, isAny := q.Filter.(logsql.StreamFilterAny)
		if isAny != tc.any {
			t.Errorf("Parse(%s) = %T, want StreamFilterAny %v", tc.in, q.Filter, tc.any)
		}
		if got := q.String(); got != tc.in {
			t.Errorf("Parse(%s) prints %s", tc.in, got)
		}
	}
}
