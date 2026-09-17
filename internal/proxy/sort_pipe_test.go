package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestSortByTimePipeCarriesTheLimit(t *testing.T) {
	cases := []struct {
		forward bool
		n       int
		want    string
	}{
		{false, 1000, " | sort by (_time desc) limit 1000"},
		{true, 200, " | sort by (_time) limit 200"},
		// A caller that folds the whole match client-side must not be truncated.
		{false, 0, " | sort by (_time desc)"},
		{true, -1, " | sort by (_time)"},
	}
	for _, tc := range cases {
		if got := sortByTimePipe(tc.forward, tc.n); got != tc.want {
			t.Errorf("sortByTimePipe(%v, %d) = %q, want %q", tc.forward, tc.n, got, tc.want)
		}
	}
}

// The row budget has to reach VictoriaLogs on the SORT, not only in the `limit`
// argument: `limit` trims the result, the sort is what buffers.
func TestLogQuerySortCarriesTheRequestedLimit(t *testing.T) {
	var got string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		got = r.FormValue("query")
		w.Header().Set("Content-Type", "application/x-ndjson")
	}))
	defer backend.Close()

	p := newTestProxy(t, backend.URL)

	for _, tc := range []struct {
		name      string
		direction string
		limit     string
		wantSort  string
	}{
		{"backward", "", "37", " | sort by (_time desc) limit 37"},
		{"forward", "forward", "5", " | sort by (_time) limit 5"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			form := url.Values{}
			form.Set("direction", tc.direction)
			form.Set("limit", tc.limit)
			r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+form.Encode(), nil)
			p.proxyLogQuery(httptest.NewRecorder(), r, `app:="x"`)

			if !strings.HasSuffix(got, tc.wantSort) {
				t.Errorf("backend query = %q, want it to end with %q", got, tc.wantSort)
			}
		})
	}
}
