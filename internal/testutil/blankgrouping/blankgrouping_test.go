package blankgrouping

import "testing"

// Cut leaves quoted text, non-empty lists and clauses that are not a `by` alone.
func TestCut(t *testing.T) {
	for in, want := range map[string]string{
		`sum by( ) (rate({A="0"}[1y]))`:                 `sum by() (rate({A="0"}[1y]))`,
		"sum by (\t\n) (rate({A=\"0\"}[1y]))":           `sum by () (rate({A="0"}[1y]))`,
		`sum(rate({A="0"}[1y])) by(  )`:                 `sum(rate({A="0"}[1y])) by()`,
		`sum by( ) (sum by(  ) (rate({A="0"}[1y])))`:    `sum by() (sum by() (rate({A="0"}[1y])))`,
		`sum by () (rate({A="0"}[1y]))`:                 `sum by () (rate({A="0"}[1y]))`,
		`sum by( a ) (rate({A="0"}[1y]))`:               `sum by( a ) (rate({A="0"}[1y]))`,
		`sum by (a, ) (rate({A="0"}[1y]))`:              `sum by (a, ) (rate({A="0"}[1y]))`,
		`{A="by( )"} |= "by( )"`:                        `{A="by( )"} |= "by( )"`,
		"{A=\"0\"} |= `by( )`":                          "{A=\"0\"} |= `by( )`",
		`{A="\"by( )"} |= "x\\" by( )`:                  `{A="\"by( )"} |= "x\\" by()`,
		`sum by( ) (rate({A="0"}[1y])) / sum by( ) (x)`: `sum by() (rate({A="0"}[1y])) / sum by() (x)`,
		`sum without( ) (rate({A="0"}[1y]))`:            `sum without( ) (rate({A="0"}[1y]))`,
		`sum nearby( ) (rate({A="0"}[1y]))`:             `sum nearby( ) (rate({A="0"}[1y]))`,
		`sum by`:                                        `sum by`,
		`sum by(`:                                       `sum by(`,
		`sum by( `:                                      `sum by( `,
		`"unterminated by( )`:                           `"unterminated by( )`,
	} {
		if got := Cut(in); got != want {
			t.Errorf("%q: got %q, want %q", in, got, want)
		}
	}
}
