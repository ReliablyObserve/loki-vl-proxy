package translate

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// snRow is a VictoriaLogs row as the service_name derivation reads it: its stream labels (tags) and its other
// fields. A stream label is also a field of the row with the same value.
type snRow struct {
	tags, fields map[string]string
}

// all is the row's fields as a filter reads them: the stream labels and the other fields.
func (r snRow) all() map[string]string {
	out := map[string]string{}
	for k, v := range r.tags {
		out[k] = v
	}
	for k, v := range r.fields {
		out[k] = v
	}
	return out
}

// evalStreamFilter evaluates a stream filter on a stream's labels the way VictoriaLogs does: a missing label reads
// as the empty value; a group keeps a stream when every matcher does, the filter when one group does.
func evalStreamFilter(t *testing.T, f logsql.FilterExpr, tags map[string]string) bool {
	t.Helper()
	sf, ok := f.(logsql.StreamFilterAny)
	if !ok {
		t.Fatalf("not a StreamFilterAny: %T", f)
	}
	for _, g := range sf.Groups {
		all := true
		for _, m := range g {
			if m.Op != "=" {
				t.Fatalf("unexpected matcher %v", m)
			}
			if tags[m.Name] != m.Value {
				all = false
			}
		}
		if all {
			return true
		}
	}
	return false
}

// snStates are the ways a row holds one source field: not at all, as a stream label, or as a regular field.
const (
	snAbsent = iota
	snTag
	snField
)

// snRows is every row over the first three sources (service_name, service.name, app) with each held in every way
// with the values a or b, and a later source (job) or none: the rows the derivation can meet. holds reports whether
// the operator's statement (-service-name-stream-label-only) is true of the row.
func snRows() (rows []snRow, holds []bool) {
	sources := []string{"service_name", "service.name", "app"}
	values := []string{"a", "b"}
	var walk func(i int, r snRow)
	walk = func(i int, r snRow) {
		if i == len(sources) {
			for _, job := range []string{"", "a", "c"} {
				c := snRow{tags: map[string]string{}, fields: map[string]string{}}
				for k, v := range r.tags {
					c.tags[k] = v
				}
				for k, v := range r.fields {
					c.fields[k] = v
				}
				if job != "" {
					c.tags["job"] = job
				}
				rows = append(rows, c)
				_, snField := c.fields["service_name"]
				_, dotField := c.fields["service.name"]
				_, dotTag := c.tags["service.name"]
				holds = append(holds, !snField && (!dotField || !dotTag))
			}
			return
		}
		for state := snAbsent; state <= snField; state++ {
			if state == snAbsent {
				walk(i+1, r)
				continue
			}
			for _, v := range values {
				c := snRow{tags: map[string]string{}, fields: map[string]string{}}
				for k, x := range r.tags {
					c.tags[k] = x
				}
				for k, x := range r.fields {
					c.fields[k] = x
				}
				if state == snTag {
					c.tags[sources[i]] = v
				} else {
					c.fields[sources[i]] = v
				}
				walk(i+1, c)
			}
		}
	}
	walk(0, snRow{tags: map[string]string{}, fields: map[string]string{}})
	return rows, holds
}

// TestServiceNameStreamFilterKeepsEveryDerivedRow: on every row the statement holds for, the stream filter keeps
// the row's stream whenever the derived service_name filter keeps the row, for every value (a source's value, one
// no row has, unknown_service), so the selector with both filters keeps exactly the rows of the derived filter
// alone. On a row the statement is false for (a regular service_name field, or a regular service.name beside a
// service.name stream label of another value, where VictoriaLogs reads the regular field) the stream filter can
// leave out a row the derivation keeps: that is the boundary the flag documents.
//
// conformance: semantics/service-name-stream-filter
func TestServiceNameStreamFilterKeepsEveryDerivedRow(t *testing.T) {
	rows, holds := snRows()
	violations := 0
	for _, value := range []string{"a", "b", "c", "x", UnknownServiceNameValue} {
		stream := ServiceNameStreamFilter(value)
		derived := ServiceNameMatcherFilterExpr(value, false, false)
		for i, r := range rows {
			kept := evalRow(t, derived, r.all())
			if !kept || evalStreamFilter(t, stream, r.tags) {
				continue
			}
			if holds[i] {
				t.Errorf("service_name=%q: the stream filter leaves out %+v, which the derivation keeps", value, r)
				continue
			}
			violations++
		}
	}
	if violations == 0 {
		t.Error("no row the statement is false for is left out: the table does not reach the boundary")
	}
	t.Logf("%d rows, %d left out where the statement is false", len(rows), violations)
}

// FuzzServiceNameStreamFilterKeepsEveryDerivedRow is the property of TestServiceNameStreamFilterKeepsEveryDerivedRow
// over rows built from the input: each byte places one source field of SyntheticServiceNameFields (absent, stream
// label, regular field) with one of a few values, the statement is enforced on the row, and the queried value
// comes from the input too.
func FuzzServiceNameStreamFilterKeepsEveryDerivedRow(f *testing.F) {
	f.Add([]byte{0x01, 0x02, 0x05}, "a")
	f.Add([]byte{0x00, 0x09, 0x01, 0x02}, "b")
	f.Add([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05}, "a")
	f.Add([]byte{}, UnknownServiceNameValue)
	f.Add([]byte{0x05, 0x06}, `we"ird\`)
	values := []string{"a", "b", `we"ird\`}
	f.Fuzz(func(t *testing.T, spec []byte, value string) {
		if value == "" {
			return // service_name="" gets no stream filter
		}
		r := snRow{tags: map[string]string{}, fields: map[string]string{}}
		for i, b := range spec {
			if i == len(SyntheticServiceNameFields) {
				break
			}
			name, v := SyntheticServiceNameFields[i], values[int(b>>2)%len(values)]
			if b&0x02 != 0 {
				v = value
			}
			switch b & 0x01 {
			case 0:
				if b&0x40 != 0 {
					r.tags[name] = v
				}
			case 1:
				// The statement: service_name only as a stream label, service.name not beside its stream label.
				if name == "service_name" || (name == "service.name" && r.tags[name] != "") {
					r.tags[name] = v
				} else {
					r.fields[name] = v
				}
			}
		}
		if evalRow(t, ServiceNameMatcherFilterExpr(value, false, false), r.all()) && !evalStreamFilter(t, ServiceNameStreamFilter(value), r.tags) {
			t.Fatalf("service_name=%q: the stream filter leaves out %+v, which the derivation keeps", value, r)
		}
	})
}

// TestServiceNameStreamFilterSelectors pins where the selector gets the stream filter: a positive service_name
// matcher with a value, with the option on and no declared stream field among the matchers; the derived filter
// stays beside it unchanged. Negated, regexp and empty matchers, the option off, and a selector whose stream filter
// another matcher already writes keep the text they have without the option.
//
// conformance: semantics/service-name-stream-filter
func TestServiceNameStreamFilterSelectors(t *testing.T) {
	m := func(n string, op logql.MatchOp, v string) logql.LabelMatcher {
		return logql.LabelMatcher{Name: n, Op: op, Value: v}
	}
	stream := func(v string) string { return ServiceNameStreamFilter(v).String() }
	chain := func(v string) string { return ServiceNameMatcherFilterExpr(v, false, false).String() }
	streamFields := map[string]bool{"app": true}
	on := SelectorOptions{ServiceNameStreamLabelOnly: true}
	for _, c := range []struct {
		name string
		ms   []logql.LabelMatcher
		opts SelectorOptions
		want string
	}{
		{"positive matcher", []logql.LabelMatcher{m("service_name", logql.MatchEq, "svc-a")}, on, stream("svc-a") + " " + chain("svc-a")},
		{"option off", []logql.LabelMatcher{m("service_name", logql.MatchEq, "svc-a")}, SelectorOptions{}, chain("svc-a")},
		{"unknown_service", []logql.LabelMatcher{m("service_name", logql.MatchEq, UnknownServiceNameValue)}, on, stream(UnknownServiceNameValue) + " " + chain(UnknownServiceNameValue)},
		{"value quoted", []logql.LabelMatcher{m("service_name", logql.MatchEq, "a\"b\\c\nd")}, on, stream("a\"b\\c\nd") + " " + chain("a\"b\\c\nd")},
		{"not equal", []logql.LabelMatcher{m("service_name", logql.MatchNeq, "svc-a")}, on, "-" + chain("svc-a")},
		{"regexp", []logql.LabelMatcher{m("service_name", logql.MatchRe, "svc-a|svc-b")}, on, ServiceNameMatcherFilterExpr("svc-a|svc-b", false, true).String()},
		{"negated regexp", []logql.LabelMatcher{m("service_name", logql.MatchNotRe, "svc-a")}, on, "-" + ServiceNameMatcherFilterExpr("svc-a", false, true).String()},
		{"empty", []logql.LabelMatcher{m("service_name", logql.MatchEq, "")}, on, "-*"},
		{"beside a field matcher, first", []logql.LabelMatcher{m("level", logql.MatchEq, "info"), m("service_name", logql.MatchEq, "svc-a")}, on, stream("svc-a") + ` level:="info" ` + chain("svc-a")},
		{"the first positive matcher", []logql.LabelMatcher{m("service_name", logql.MatchNeq, "svc-b"), m("service_name", logql.MatchEq, "svc-a"), m("service_name", logql.MatchEq, "svc-c")}, on, stream("svc-a") + " -" + chain("svc-b") + " " + chain("svc-a") + " " + chain("svc-c")},
		{"a declared stream field keeps its own stream filter", []logql.LabelMatcher{m("app", logql.MatchEq, "x"), m("service_name", logql.MatchEq, "svc-a")}, on, `{app="x"} ` + chain("svc-a")},
		{"no service_name", []logql.LabelMatcher{m("level", logql.MatchEq, "info")}, on, `level:="info"`},
	} {
		f, err := StreamSelectorFilterOpts(c.ms, nil, streamFields, nil, c.opts)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got := f.String(); got != c.want {
			t.Errorf("%s:\n got %s\nwant %s", c.name, got, c.want)
		}
		mustParseLogsQL(t, c.want)
		// The query plan's name function and a label function that renames service.name (a field mapping) change
		// nothing: service_name is the derived label either way.
		rename := func(s string) string { return strings.ReplaceAll(s, "service_name", "service.name") }
		named, err := StreamSelectorFilterOpts(c.ms, rename, streamFields, logsql.QuoteFieldName, c.opts)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got := named.String(); got != c.want {
			t.Errorf("%s with a name function:\n got %s\nwant %s", c.name, got, c.want)
		}
	}
	const want = `{service_name="svc-a" or service_name="", service.name="svc-a" or service_name="", service.name=""}`
	if got := stream("svc-a"); got != want {
		t.Errorf("ServiceNameStreamFilter = %s, want %s", got, want)
	}
	// Without options the existing entry points write what they wrote.
	ms := []logql.LabelMatcher{m("service_name", logql.MatchEq, "svc-a")}
	for name, f := range map[string]func() (logsql.FilterExpr, error){
		"StreamSelectorFilter": func() (logsql.FilterExpr, error) { return StreamSelectorFilter(ms, nil, nil) },
		"StreamSelectorFilterNamed": func() (logsql.FilterExpr, error) {
			return StreamSelectorFilterNamed(ms, nil, nil, logsql.QuoteFieldName)
		},
		"SelectorFilter": func() (logsql.FilterExpr, error) {
			return SelectorFilter([]SelectorMatcher{{Name: "service_name", Op: "=", Value: "svc-a"}}, nil, nil)
		},
	} {
		got, err := f()
		if err != nil || got.String() != chain("svc-a") {
			t.Errorf("%s = %v, %v; want the derived filter alone", name, got, err)
		}
	}
}

// BenchmarkStreamSelectorFilterServiceName is the selector builder on a service_name selector, with the option off
// (every deployment that does not set the flag: the same work as before) and on.
func BenchmarkStreamSelectorFilterServiceName(b *testing.B) {
	ms := []logql.LabelMatcher{{Name: "service_name", Op: logql.MatchEq, Value: "svc-a"}}
	for _, c := range []struct {
		name string
		opts SelectorOptions
	}{{"off", SelectorOptions{}}, {"on", SelectorOptions{ServiceNameStreamLabelOnly: true}}} {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := StreamSelectorFilterOpts(ms, nil, nil, logsql.QuoteFieldName, c.opts); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
