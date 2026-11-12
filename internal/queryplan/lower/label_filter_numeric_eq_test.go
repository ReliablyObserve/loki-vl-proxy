package lower

import (
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// The bound a numeric equality literal compares with, or none for a literal the lowering leaves to the string
// translator: a plain decimal number, a duration in Go's syntax as nanoseconds and a bytes size as bytes.
//
// conformance: semantics/label-filter-numeric-equality
func TestNumericEqualityBound(t *testing.T) {
	for _, c := range []struct {
		q, bound string // q is the label filter's text; bound "" when the lowering does not write it
	}{
		// numbers
		{"m == 200", "200"}, {"m == 0", "0"}, {"m == -0", "0"}, {"m == 200.0", "200"}, {"m == 200.", "200"}, {"m == .5", "0.5"},
		{"m == 200.50", "200.5"}, {"m == 2e2", "200"}, {"m == 2E+2", "200"}, {"m == 5e-1", "0.5"}, {"m == -3", "-3"}, {"m == +3", "3"},
		{"m == 010", "10"}, {"m == 00200.000", "200"}, {"m == 123456789.123456789", "123456789.12345679"},
		{"m == 1e15", "1000000000000000"}, {"m == 1e-7", "0.0000001"}, {"m == 0.1", "0.1"},
		{"m == 1e30", "1000000000000000000000000000000"}, {"m == 1e40", ""}, {"m == 1e400", ""}, {"m == 0x10", ""}, {"m == 0o10", ""}, {"m == 1_000", ""}, {"m == 1.5_5", ""},
		// durations
		{"d == 5s", "5000000000"}, {"d == 150ms", "150000000"}, {"d == 1m30s", "90000000000"}, {"d == 1.5h", "5400000000000"},
		{"d == 100ns", "100"}, {"d == 100us", "100000"}, {"d == 100µs", "100000"}, {"d == 0s", "0"}, {"d == 2h45m", "9900000000000"},
		{"d == 1d", ""}, {"d == 1w", ""}, {"d == 1y", ""}, {"d == 5000h", ""},
		// bytes
		{"b == 1KB", "1000"}, {"b == 1KiB", "1024"}, {"b == 1.5MiB", "1572864"}, {"b == 10B", "10"}, {"b == 2K", "2000"}, {"b == 1Ki", "1024"},
		{"b == 1.5KB", "1500"}, {"b == 1GB", "1000000000"}, {"b == 1TiB", "1099511627776"},
		{"b == 0.0005KB", ""}, {"b == 1.0000000001KB", ""},
	} {
		q := `{app="a"} | json | ` + c.q
		expr, err := logql.Parse(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		leaf := expr.(*logql.LogQuery).Pipeline[1].(*logql.LabelFilterStage).Expr
		bound, ok := numericEqualityBound(leaf)
		switch {
		case c.bound == "" && ok:
			t.Errorf("%s: bound %q, want the literal left to the string translator", c.q, bound)
		case c.bound != "" && (!ok || bound != c.bound):
			t.Errorf("%s: bound %q (%v), want %q", c.q, bound, ok, c.bound)
		}
	}
}

// Each equality lowers to the filter pipe below, in the plain profile and with the line kept, by kind, operator and
// parser: the pair of range filters, and for != the same pair negated on the entries that have the field.
//
// conformance: semantics/label-filter-numeric-equality
func TestPlanLowersNumericEquality(t *testing.T) {
	type c struct{ lit, bound string }
	kinds := []struct {
		name, field string
		lits        []c
	}{
		{"number", "m", []c{{"200", "200"}, {"200.5", "200.5"}, {"-3", "-3"}, {"2e2", "200"}, {"0", "0"}}},
		{"duration", "d", []c{{"5s", "5000000000"}, {"1m30s", "90000000000"}, {"100ms", "100000000"}}},
		{"bytes", "b", []c{{"1KB", "1000"}, {"1.5MiB", "1572864"}, {"10B", "10"}}},
	}
	unpack := map[string]string{"json": "unpack_json", "logfmt": "unpack_logfmt"}
	for _, k := range kinds {
		for _, l := range k.lits {
			for _, parser := range []string{"json", "logfmt"} {
				for _, op := range []string{"=", "==", "!="} {
					q := `{app="a"} | ` + parser + ` | ` + k.field + ` ` + op + ` ` + l.lit
					pair := k.field + ":>=" + l.bound + " " + k.field + ":<=" + l.bound
					want := `app:="a" | ` + unpack[parser] + ` | filter (` + pair + `)`
					if op == "!=" {
						want = `app:="a" | ` + unpack[parser] + ` | filter (` + k.field + `:* -(` + pair + `))`
					}
					for _, prof := range []Profile{{}, {KeepLine: true}} {
						op2, err := PlanQuery(q, planOf(t, q), prof)
						if err != nil {
							t.Fatalf("%s: %v", q, err)
						}
						got := op2.(physical.VLQuery).Text
						if prof.KeepLine {
							got = strings.Replace(got, "copy _msg as _lvp_line | ", "", 1)
						}
						if got != want {
							t.Errorf("%s (%s): got %s, want %s", q, k.name, got, want)
						}
						assertNumericEqualityTree(t, got, k.field, l.bound, op == "!=")
					}
				}
			}
		}
	}
}

// assertNumericEqualityTree reads the lowered text with the LogsQL parser and checks the filter pipe's tree: a group
// of the two comparisons of the bound on the field, or for != a group of the existence of the field and the negated
// group of those two.
func assertNumericEqualityTree(t *testing.T, text, field, bound string, negated bool) {
	t.Helper()
	q, err := logsql.Parse(text)
	if err != nil {
		t.Fatalf("%s does not parse: %v", text, err)
	}
	// The parser drops the parentheses around a whole filter; what it prints parses back to the same text.
	again, err := logsql.Parse(q.String())
	if err != nil || again.String() != q.String() {
		t.Errorf("%s reads back as %s, which reads as %v (%v)", text, q.String(), again, err)
	}
	var filter logsql.FilterExpr
	for _, p := range q.Pipes {
		if pf, ok := p.(logsql.PipeFilter); ok {
			filter = pf.Expr
		}
	}
	if filter == nil {
		t.Fatalf("%s holds no filter pipe", text)
	}
	cmp := func(f logsql.FilterExpr, op logsql.FieldOp) {
		t.Helper()
		ff, ok := f.(logsql.FieldFilter)
		if !ok || ff.Field != field || ff.Op != op || ff.Value != bound || ff.Negate {
			t.Errorf("%s: %#v is not %s %v %s", text, f, field, op, bound)
		}
	}
	pair := func(f logsql.FilterExpr) {
		t.Helper()
		if g, ok := f.(logsql.Group); ok {
			f = g.Expr
		}
		all, ok := f.(logsql.AllOf)
		if !ok || len(all) != 2 {
			t.Fatalf("%s: %#v is not two comparisons", text, f)
		}
		cmp(all[0], logsql.FieldOpGTE)
		cmp(all[1], logsql.FieldOpLTE)
	}
	if g, ok := filter.(logsql.Group); ok {
		filter = g.Expr
	}
	if !negated {
		pair(logsql.Group{Expr: filter})
		return
	}
	all, ok := filter.(logsql.AllOf)
	if !ok || len(all) != 2 {
		t.Fatalf("%s: %#v is not an existence check and a negation", text, filter)
	}
	if ff, ok := all[0].(logsql.FieldFilter); !ok || ff.Field != field || ff.Op != logsql.FieldOpAny {
		t.Errorf("%s: %#v is not %s:*", text, all[0], field)
	}
	ex, ok := all[1].(logsql.Exclude)
	if !ok {
		t.Fatalf("%s: %#v is not a negation", text, all[1])
	}
	pair(ex.Expr)
}

// The lowering names the keys a label can be stored under, quotes a name that needs it, reads a stream label from its
// scratch copy and a name_extracted label from its second unpack, and joins the operands of and/or: each operand of a
// numeric equality is one group, so none of them is split by the operator around it. A numeric equality drops an entry
// without the label under both operators, so the keys of a label are joined with OR for != too.
//
// conformance: semantics/label-filter-numeric-equality
func TestPlanLowersNumericEqualityAmongKeys(t *testing.T) {
	for _, c := range []struct{ q, prof, want string }{
		{`{app="a"} | json | http_status == 200`, "none",
			`app:="a" | unpack_json | filter ((http_status:>=200 http_status:<=200) OR ("http.status":>=200 "http.status":<=200) OR ("http-status":>=200 "http-status":<=200))`},
		{`{app="a"} | json | http_status != 200`, "none",
			`app:="a" | unpack_json | filter ((http_status:* -(http_status:>=200 http_status:<=200)) OR ("http.status":* -("http.status":>=200 "http.status":<=200)) OR ("http-status":* -("http-status":>=200 "http-status":<=200)))`},
		{`{app="a"} | logfmt | k8s.pod == 5`, "none", `app:="a" | unpack_logfmt | filter ("k8s.pod":>=5 "k8s.pod":<=5)`},
		{`{app="a"} | json | a == 1 and b != 2 or c = 3`, "none",
			`app:="a" | unpack_json | filter (((a:>=1 a:<=1) and (b:* -(b:>=2 b:<=2))) or (c:>=3 c:<=3))`},
		{`{app="a"} | json | n >= 1 and n == 2`, "none", `app:="a" | unpack_json | filter (n:>=1 and (n:>=2 n:<=2))`},
		{`{app="a"} | json | n == 2 | m != 3s`, "none", `app:="a" | unpack_json | filter (n:>=2 n:<=2) | filter (m:* -(m:>=3000000000 m:<=3000000000))`},
		{`{app="a"} | n == 2 | json`, "none", `app:="a" n:="2" | unpack_json`},
	} {
		op, err := PlanQuery(c.q, planOf(t, c.q), Profile{})
		if err != nil {
			t.Errorf("%s: %v", c.q, err)
			continue
		}
		got := op.(physical.VLQuery).Text
		// the filter of a stage after a parser is a pipe of its own; one before a parser stays as the translator writes it
		if got != c.want {
			t.Errorf("%s:\n got  %s\n want %s", c.q, got, c.want)
		}
	}
}

// The literals the lowering does not write stay with the string translator under a name of their own.
//
// conformance: semantics/label-filter-numeric-equality
func TestPlanDelegatesNumericEqualityLiterals(t *testing.T) {
	for q, want := range map[string]string{
		`{app="a"} | json | m == 0x10`:           numericEqualityConstruct,
		`{app="a"} | json | m = 0o17`:            numericEqualityConstruct,
		`{app="a"} | logfmt | m != 1_000`:        numericEqualityConstruct,
		`{app="a"} | json | m == 1e400`:          numericEqualityConstruct,
		`{app="a"} | json | d == 1d`:             numericEqualityConstruct,
		`{app="a"} | json | d != 2w`:             numericEqualityConstruct,
		`{app="a"} | json | b == 0.0005KB`:       numericEqualityConstruct,
		`{app="a"} | json | m > 0x10`:            "",
		`{app="a"} | json | m == 5 and b != 1y`:  numericEqualityConstruct,
		`{app="a"} | json | m == 5 or (b != 1w)`: numericEqualityConstruct,
		`{app="a"} | json | m == 5`:              "",
		`{app="a"} | n == 0x10`:                  "", // no parser: the filter of a stream label, the string translator's text
		`{app="a"} | json | m = "5"`:             "",
		`{app="a"} | json | m == 010`:            "",
	} {
		_, err := PlanQuery(q, planOf(t, q), Profile{})
		var nl *NotLoweredError
		switch {
		case want == "" && err != nil:
			t.Errorf("%s: %v, want lowered", q, err)
		case want != "" && (!errors.As(err, &nl) || nl.Construct != want):
			t.Errorf("%s: %v, want not lowered: %s", q, err, want)
		}
	}
}

// The plan reports the queries whose numeric equality it writes numerically, for the shadows that hold the lowering to
// the string translator: a literal it delegates, an equality before the parser and an order comparison are not.
//
// conformance: semantics/label-filter-numeric-equality
func TestWritesNumericEquality(t *testing.T) {
	for q, want := range map[string]bool{
		`{app="a"} | json | m == 5`:                                  true,
		`{app="a"} | logfmt | m != 5s`:                               true,
		`{app="a"} | json | a="x" | b = 1KB`:                         true,
		`sum(count_over_time({app="a"} | json | m == 5 [5m]))`:       true,
		`sum by (a) (rate({app="a"} | logfmt | m != 5 [5m]))`:        true,
		`{app="a"} | json | m == 0x10`:                               false,
		`{app="a"} | m == 5 | json`:                                  false,
		`{app="a"} | m == 5`:                                         false,
		`{app="a"} | json | m >= 5`:                                  false,
		`{app="a"} | json | m = "5"`:                                 false,
		`{app="a"} | json | m = ip("10.0.0.1")`:                      false,
		`{app="a"} | regexp "(?P<m>.+)" | m == 5`:                    false,
		`count_over_time({app="a"} [5m])`:                            false,
		`{app="a"} | json | m == 5 | line_format "{{.m}}"`:           true,
		`sum(rate({app="a"} | json | m == 5 [5m])) / on() vector(1)`: true,
	} {
		if got := WritesNumericEquality(planOf(t, logql.CanonicalizeLogRanges(q))); got != want {
			t.Errorf("%s: %v, want %v", q, got, want)
		}
	}
}

// numericEqualityBound reads a plain number as Go does, a duration as the nanoseconds of Go's parse and a bytes size
// as whole bytes, on any literal; a bound it writes is a plain number VictoriaLogs reads back as the same value.
//
// conformance: semantics/label-filter-numeric-equality
func FuzzNumericEqualityLowering(f *testing.F) {
	for _, s := range []string{"200", "200.5", "-3", "2e2", "0", "-0", ".5", "5.", "1e400", "0x10", "010", "1_000", "5s", "1m30s", "100us", "100µs", "1h5m", "1d", "1.5h",
		"1KB", "1.5MiB", "10B", "2K", "1kb", "0.0005KB", "1EB", "9007199254740993", "1e15", "0.0000001", "99999999999999999999"} {
		f.Add(s, uint8(0), uint8(0))
	}
	f.Fuzz(func(t *testing.T, lit string, opSel, parserSel uint8) {
		if len(lit) > 64 {
			return
		}
		op := []string{"=", "==", "!="}[int(opSel)%3]
		parser := []string{"json", "logfmt"}[int(parserSel)%2]
		q := `{app="a"} | ` + parser + ` | m ` + op + ` ` + lit
		expr, err := logql.Parse(q)
		if err != nil {
			return
		}
		lq, ok := expr.(*logql.LogQuery)
		if !ok || len(lq.Pipeline) != 2 {
			return
		}
		lf, ok := lq.Pipeline[1].(*logql.LabelFilterStage)
		if !ok || lf.Expr == nil || !lf.Expr.IsLeaf() {
			return
		}
		leaf := lf.Expr
		bound, ok := numericEqualityBound(leaf)
		if ok {
			if !isNumericEquality(leaf) {
				t.Fatalf("%s: a bound for a leaf that is no numeric equality", q)
			}
			// A bound is a plain number VictoriaLogs reads exactly: digits, one dot at most, an optional minus.
			if _, err := strconv.ParseFloat(bound, 64); err != nil || strings.ContainsAny(bound, "eE+_x") || strings.HasSuffix(bound, ".") || strings.HasPrefix(bound, ".") {
				t.Fatalf("%s: bound %q is not a plain number", q, bound)
			}
			switch leaf.Kind {
			case logql.LabelFilterValueNumber:
				want, err := strconv.ParseFloat(leaf.Value, 64)
				got, _ := strconv.ParseFloat(bound, 64)
				if err != nil || got != want {
					t.Fatalf("%s: bound %q is not the number %q", q, bound, leaf.Value)
				}
			case logql.LabelFilterValueDuration:
				d, err := time.ParseDuration(leaf.Value)
				if err != nil || bound != strconv.FormatInt(int64(d), 10) {
					t.Fatalf("%s: bound %q is not the nanoseconds of %q", q, bound, leaf.Value)
				}
			case logql.LabelFilterValueBytes:
				if strings.Contains(bound, ".") {
					t.Fatalf("%s: bound %q is not a whole number of bytes", q, bound)
				}
			}
		}
		for _, prof := range []Profile{{}, {KeepLine: true}, {StreamFields: map[string]bool{"app": true}}} {
			op2, err := PlanQuery(q, planOf(t, q), prof)
			var nl *NotLoweredError
			switch {
			case errors.As(err, &nl):
				if isNumericEquality(leaf) && !ok && nl.Construct != numericEqualityConstruct {
					t.Fatalf("%s: delegated as %q, want %q", q, nl.Construct, numericEqualityConstruct)
				}
				if ok && nl.Construct == numericEqualityConstruct {
					t.Fatalf("%s: bound %q, but delegated for its literal", q, bound)
				}
			case err != nil:
				t.Fatalf("%s: %v", q, err)
			default:
				text := op2.(physical.VLQuery).Text
				parsed, err := logsql.Parse(text)
				if err != nil {
					t.Fatalf("%s: lowered %s does not parse: %v", q, text, err)
				}
				if again, err := logsql.Parse(parsed.String()); err != nil || again.String() != parsed.String() {
					t.Fatalf("%s: lowered %s reads back as %s, which does not read again (%v)", q, text, parsed.String(), err)
				}
				if isNumericEquality(leaf) && !strings.Contains(text, ":>="+bound+" ") {
					t.Fatalf("%s: lowered %s does not compare with %s", q, text, bound)
				}
			}
		}
	})
}
