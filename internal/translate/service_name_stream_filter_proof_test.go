package translate

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// snProofHit evaluates ServiceNameStreamFilterProof(value) on one row: its field filters on the row's fields as a
// filter reads them, its excluded stream filter on the row's stream labels.
func snProofHit(t *testing.T, value string, r snRow) bool {
	t.Helper()
	proof, ok := ServiceNameStreamFilterProof(value).(logsql.AllOf)
	if !ok || len(proof) != 2 {
		t.Fatalf("unexpected proof %s", ServiceNameStreamFilterProof(value))
	}
	excluded, ok := proof[1].(logsql.Exclude)
	if !ok {
		t.Fatalf("unexpected proof %s", ServiceNameStreamFilterProof(value))
	}
	return evalRow(t, proof[0], r.all()) && !evalStreamFilter(t, excluded.Expr, r.tags)
}

// snDropped reports whether a read narrowed by ServiceNameStreamFilter(value) leaves out a row the derived filter
// keeps.
func snDropped(t *testing.T, value string, r snRow) bool {
	t.Helper()
	return evalRow(t, ServiceNameMatcherFilterExpr(value, false, false), r.all()) && !evalStreamFilter(t, ServiceNameStreamFilter(value), r.tags)
}

// snProofRows is every row over the first three sources (service_name, service.name, app), each absent, a stream
// label, a regular field, or both with the same or different values (VictoriaLogs reads the regular field), with a
// later source (job) or none: every layout, the operator's statement true or not.
func snProofRows() []snRow {
	sources := []string{"service_name", "service.name", "app"}
	type state struct{ tag, field string }
	states := []state{{}, {tag: "a"}, {tag: "b"}, {field: "a"}, {field: "b"}, {tag: "a", field: "a"}, {tag: "a", field: "b"}, {tag: "b", field: "a"}}
	var rows []snRow
	var walk func(i int, picked []state)
	walk = func(i int, picked []state) {
		if i == len(sources) {
			for _, job := range []string{"", "a", "c"} {
				r := snRow{tags: map[string]string{}, fields: map[string]string{}}
				for k, s := range picked {
					if s.tag != "" {
						r.tags[sources[k]] = s.tag
					}
					if s.field != "" {
						r.fields[sources[k]] = s.field
					}
				}
				if job != "" {
					r.tags["job"] = job
				}
				rows = append(rows, r)
			}
			return
		}
		for _, s := range states {
			walk(i+1, append(append([]state(nil), picked...), s))
		}
	}
	walk(0, nil)
	return rows
}

// TestServiceNameStreamFilterProofFindsEveryDroppedRow: on every row layout, the statement of
// -service-name-stream-label-only true or false (duplicate stream label and regular field included), the proof
// read of the automatic route (-service-name-stream-route) matches the row exactly when the narrowed read leaves
// out a row the derived service_name filter keeps, for every value (a source's value, one no row has,
// unknown_service, one that needs quoting). An empty proof therefore means the narrowed answer is the derived
// filter's, and a found row sends the read without the stream filter.
//
// conformance: semantics/service-name-stream-route
func TestServiceNameStreamFilterProofFindsEveryDroppedRow(t *testing.T) {
	rows := snProofRows()
	dropped := 0
	for _, value := range []string{"a", "b", "c", "x", UnknownServiceNameValue, `we"ird\`} {
		for _, r := range rows {
			want, got := snDropped(t, value, r), snProofHit(t, value, r)
			if want != got {
				t.Errorf("service_name=%q on %+v: proof finds the row %v, the narrowing leaves it out %v", value, r, got, want)
			}
			if want {
				dropped++
			}
		}
	}
	if dropped == 0 {
		t.Error("no layout has a row the narrowing leaves out: the table does not reach the fallback")
	}
	t.Logf("%d rows, %d left out by a narrowing and found by its proof", len(rows), dropped)
	mustParseLogsQL(t, logsql.Query{Filter: ServiceNameStreamFilterProof(`we"ird\`), Pipes: []logsql.Pipe{logsql.PipeLimit{N: 1}}}.String())
}

// FuzzServiceNameStreamFilterProofFindsEveryDroppedRow is the property of
// TestServiceNameStreamFilterProofFindsEveryDroppedRow over rows built from the input: each byte places one source
// field of SyntheticServiceNameFields as absent, a stream label, a regular field or both, with values from the
// input and the queried value.
func FuzzServiceNameStreamFilterProofFindsEveryDroppedRow(f *testing.F) {
	f.Add([]byte{0x01, 0x02, 0x05}, "a")
	f.Add([]byte{0x03, 0x07}, "b")
	f.Add([]byte{0x00, 0x13, 0x01, 0x02}, "a")
	f.Add([]byte{}, UnknownServiceNameValue)
	f.Add([]byte{0x05, 0x06}, `we"ird\`)
	values := []string{"a", "b", `we"ird\`}
	f.Fuzz(func(t *testing.T, spec []byte, value string) {
		if value == "" {
			return // service_name="" is never narrowed
		}
		r := snRow{tags: map[string]string{}, fields: map[string]string{}}
		for i, b := range spec {
			if i == len(SyntheticServiceNameFields) {
				break
			}
			name := SyntheticServiceNameFields[i]
			pick := func(shift uint) string {
				if b&(1<<shift) != 0 {
					return value
				}
				return values[int(b>>5)%len(values)]
			}
			if b&0x01 != 0 {
				r.tags[name] = pick(2)
			}
			if b&0x02 != 0 {
				r.fields[name] = pick(3)
			}
		}
		if want, got := snDropped(t, value, r), snProofHit(t, value, r); want != got {
			t.Fatalf("service_name=%q on %+v: proof finds the row %v, the narrowing leaves it out %v", value, r, got, want)
		}
	})
}
