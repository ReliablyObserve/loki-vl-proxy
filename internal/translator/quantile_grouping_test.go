package translator

import "testing"

func TestQuantileRangeGrouping(t *testing.T) {
	for _, tc := range []struct{ grouping, want string }{
		{"by (level)", "by (level) "},
		{"by ()", "by () "},
		{"by (app, level)", "by (app, level) "},
	} {
		t.Run(tc.grouping, func(t *testing.T) {
			got, err := TranslateLogQL(`quantile_over_time(0.95, {app="api"} | json | unwrap latency [5m]) ` + tc.grouping)
			if err != nil {
				t.Fatal(err)
			}
			want := `app:="api" | unpack_json | stats ` + tc.want + `quantile(0.95, latency)`
			if got != want {
				t.Fatalf("got %q, want %q", got, want)
			}
		})
	}
}
