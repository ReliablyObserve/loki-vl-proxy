package logql_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

func TestConsumeRestOfStageSpaces(t *testing.T) {
	cases := []struct {
		in  string
		out string
	}{
		{`{app="x"} | json | status>=200 and status<300`, `{app="x"} | json | status>=200 and status<300`},
		{`{app="x"} | json | level="error" or level="warn"`, `{app="x"} | json | level="error" or level="warn"`},
		{`{app="x"} | status!=200`, `{app="x"} | status!=200`},
		{`{app="x"} | json | status>=200 and status<300 | logfmt`, `{app="x"} | json | status>=200 and status<300 | logfmt`},
	}
	for _, tc := range cases {
		lq, err := logql.ParseLogQuery(tc.in)
		if err != nil {
			t.Errorf("parse(%q) error: %v", tc.in, err)
			continue
		}
		got := lq.String()
		if got != tc.out {
			t.Errorf("String():\n  got:  %q\n  want: %q", got, tc.out)
		}
	}
}
