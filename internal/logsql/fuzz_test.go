// internal/logsql/fuzz_test.go
package logsql_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// FuzzFieldFilterString ensures FieldFilter.String() never panics.
// Panics would occur for unknown FieldOp values.
func FuzzFieldFilterString(f *testing.F) {
	// Seed corpus: valid FieldOp values (0-12 inclusive as of current iota)
	seeds := []struct {
		op    int
		field string
		value string
		neg   bool
	}{
		{int(logsql.FieldOpExact), "app", "nginx", false},
		{int(logsql.FieldOpRegexp), "level", "err.*", false},
		{int(logsql.FieldOpPrefix), "url", "/api", false},
		{int(logsql.FieldOpSubstring), "msg", "error", false},
		{int(logsql.FieldOpEmpty), "level", "", false},
		{int(logsql.FieldOpAny), "level", "", false},
		{int(logsql.FieldOpGT), "status", "400", false},
		{int(logsql.FieldOpGTE), "status", "500", true},
		{int(logsql.FieldOpLT), "latency", "100", false},
		{int(logsql.FieldOpLTE), "latency", "200", false},
		{int(logsql.FieldOpRange), "latency", "100,500", false},
		{int(logsql.FieldOpIn), "level", "error,warn", false},
		{int(logsql.FieldOpIPv4Range), "ip", "10.0.0.0, 10.0.0.255", false},
	}
	for _, s := range seeds {
		f.Add(s.op, s.field, s.value, s.neg)
	}
	f.Fuzz(func(t *testing.T, op int, field, value string, negate bool) {
		// Only fuzz with valid op values to test String() doesn't panic.
		// Invalid ops are expected to panic — we test only the valid range.
		if op < 0 || op > int(logsql.FieldOpIPv4Range) {
			return
		}
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("FieldFilter.String() panicked for op=%d field=%q value=%q: %v", op, field, value, r)
			}
		}()
		ff := logsql.FieldFilter{
			Field:  field,
			Op:     logsql.FieldOp(op),
			Value:  value,
			Negate: negate,
		}
		_ = ff.String()
	})
}

// FuzzQueryString ensures Query.String() never panics for valid node combinations.
func FuzzQueryString(f *testing.F) {
	// Seed corpus
	f.Add("error", "host", "nginx", "500")
	f.Add("warn", "app", "api", "400")
	f.Add("", "level", "debug", "0")
	f.Fuzz(func(t *testing.T, word, field, fieldVal, status string) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Query.String() panicked: %v", r)
			}
		}()
		// Build a query with filter + pipe stages using the fuzz inputs
		q := logsql.NewQuery(
			logsql.And(
				logsql.Word{Value: word},
				logsql.FieldExact(field, fieldVal),
			),
			logsql.PipeFilter{Expr: logsql.FieldGTE("status", status)},
			logsql.PipeStats{
				By:    []logsql.GroupKey{{Field: field}},
				Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}, Alias: "cnt"}},
			},
		)
		_ = q.String()
	})
}

// FuzzCapabilitiesFor ensures CapabilitiesFor never panics on arbitrary input.
func FuzzCapabilitiesFor(f *testing.F) {
	f.Add("v1.50.0")
	f.Add("v1.44.0")
	f.Add("")
	f.Add("not-a-version")
	f.Add("v1.50.0-beta.1")
	f.Fuzz(func(t *testing.T, semver string) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("CapabilitiesFor(%q) panicked: %v", semver, r)
			}
		}()
		_ = logsql.CapabilitiesFor(semver)
	})
}

// FuzzParseRoundTrip tests that parsing never panics on arbitrary input.
// Round-trip correctness is NOT asserted (arbitrary input won't be canonical).
func FuzzParseRoundTrip(f *testing.F) {
	// Seed with known-valid inputs from TestParseRoundTrip
	f.Add("*")
	f.Add("error")
	f.Add(`error AND app:="nginx"`)
	f.Add(`{app="nginx", env="prod"}`)
	f.Add(`_time:5m`)
	f.Add(`* | unpack_json | filter level:="error" | stats by (host) count() as cnt`)
	f.Add(`latency:range(100,500)`)
	f.Fuzz(func(t *testing.T, input string) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Parse(%q) panicked: %v", input, r)
			}
		}()
		// Must not panic — may return an error for invalid input
		q, err := logsql.Parse(input)
		if err != nil {
			return
		}
		// If parse succeeds, String() must not panic
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Query.String() panicked after parsing %q: %v", input, r)
			}
		}()
		_ = q.String()
	})
}
