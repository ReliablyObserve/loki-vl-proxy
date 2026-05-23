// internal/logsql/builder_test.go
package logsql_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

func TestNewQuery(t *testing.T) {
	q := logsql.NewQuery(
		logsql.And(
			logsql.FieldExact("app", "nginx"),
			logsql.Word{Value: "error"},
		),
		logsql.PipeUnpackJSON{},
		logsql.PipeFilter{Expr: logsql.FieldGTE("status", "500")},
		logsql.PipeStats{
			By:    []logsql.GroupKey{{Field: "host"}},
			Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}, Alias: "count"}},
		},
	)
	want := `app:="nginx" AND error | unpack_json | filter status:>=500 | stats by (host) count() as count`
	if got := q.String(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestConstructorHelpers(t *testing.T) {
	tests := []struct {
		name string
		expr logsql.FilterExpr
		want string
	}{
		{"FieldExact", logsql.FieldExact("app", "nginx"), `app:="nginx"`},
		{"FieldRegexp", logsql.FieldRegexp("level", "err.*"), `level:~"err.*"`},
		{"FieldGT", logsql.FieldGT("status", "400"), "status:>400"},
		{"FieldGTE", logsql.FieldGTE("status", "500"), "status:>=500"},
		{"FieldLT", logsql.FieldLT("latency", "100"), "latency:<100"},
		{"FieldLTE", logsql.FieldLTE("latency", "200"), "latency:<=200"},
		{"FieldAny", logsql.FieldAny("level"), "level:*"},
		{"FieldEmpty", logsql.FieldEmpty("level"), `level:""`},
		{"Not", logsql.Not(logsql.Word{Value: "debug"}), "NOT debug"},
		{
			"And",
			logsql.And(logsql.Word{Value: "a"}, logsql.Word{Value: "b"}),
			"a AND b",
		},
		{
			"Or",
			logsql.Or(logsql.Word{Value: "a"}, logsql.Word{Value: "b"}),
			"a OR b",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.expr.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBestTopN_v140(t *testing.T) {
	b := logsql.NewBuilder(logsql.CapabilitiesFor("v1.40.0"))
	pipes := b.BestTopN(5, "count")
	if len(pipes) != 2 {
		t.Fatalf("expected 2 pipes (sort+limit), got %d", len(pipes))
	}
	if got := pipes[0].String(); got != "| sort by (count desc)" {
		t.Errorf("pipe[0] = %q, want sort", got)
	}
	if got := pipes[1].String(); got != "| limit 5" {
		t.Errorf("pipe[1] = %q, want limit", got)
	}
}

func TestBestTopN_v150(t *testing.T) {
	b := logsql.NewBuilder(logsql.CapabilitiesFor("v1.50.0"))
	pipes := b.BestTopN(5, "count")
	if len(pipes) != 2 {
		t.Fatalf("expected 2 pipes, got %d", len(pipes))
	}
}

func TestBestIPv4Range_pre145(t *testing.T) {
	b := logsql.NewBuilder(logsql.CapabilitiesFor("v1.44.0"))
	f := b.BestIPv4Range("client_ip", "192.168.1.0/24")
	got := f.String()
	if got == "client_ip:ipv4_range(192.168.1.0, 192.168.1.255)" {
		t.Errorf("v1.44 should not emit native ipv4_range, got %q", got)
	}
	if got == "" {
		t.Error("BestIPv4Range returned empty filter")
	}
}

func TestBestIPv4Range_v145(t *testing.T) {
	b := logsql.NewBuilder(logsql.CapabilitiesFor("v1.45.0"))
	f := b.BestIPv4Range("client_ip", "192.168.1.0/24")
	want := "client_ip:ipv4_range(192.168.1.0, 192.168.1.255)"
	if got := f.String(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
