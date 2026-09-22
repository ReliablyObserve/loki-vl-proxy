package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// conformance: parsed-label-series-identity
// Loki gives a bare parser metric one series per distinct set of extracted
// labels. `| regexp` and `| pattern` name theirs in the query, so the proxy
// groups by them in VictoriaLogs instead of collapsing them into the stream.
func TestParserCaptures_BareMetricIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, query, want string
	}{
		{"regexp", `count_over_time({app="x"} | regexp "job_id=(?P<jid>\\w+)" [1m])`, "by (_stream, level, jid) "},
		{"pattern", `count_over_time({app="x"} | pattern "<_> <method>" [1m])`, "by (_stream, level, method) "},
		{"json keys are not known in the query", `count_over_time({app="x"} | json [1m])`, "by (_stream, level) "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := preserveMetricStreamIdentity(tc.query, `app:="x" | unpack_json | stats count()`, nil)
			if !strings.Contains(got, tc.want) {
				t.Fatalf("got %q, want the grouping %q", got, tc.want)
			}
		})
	}
}

// The stats bucket path groups by the captures and returns them as labels.
func TestParserCaptures_StatsBucketsKeepCaptureLabels(t *testing.T) {
	var statsQuery string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch r.URL.Path {
		case "/select/logsql/stats_query_range":
			statsQuery = r.FormValue("query")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": "success",
				"data": map[string]any{"resultType": "matrix", "result": []any{
					map[string]any{
						"metric": map[string]string{"_stream": `{app="x"}`, "jid": "job_a", "__name__": "c"},
						"values": [][]any{{float64(1700000340), "3"}},
					},
					map[string]any{
						"metric": map[string]string{"_stream": `{app="x"}`, "jid": "job_b", "__name__": "c"},
						"values": [][]any{{float64(1700000340), "5"}},
					},
				}},
			})
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"result":[]}}`))
		}
	}))
	defer backend.Close()

	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(time.Minute, 100), LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	p.storeBackendVersion("v1.52.0", "v1.52.0")

	start := time.Unix(1700000400, 0).UTC()
	end := start.Add(5 * time.Minute)
	query := `count_over_time({app="x"} | regexp "job_id=(?P<jid>\\w+)" [2m])`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?query="+strings.ReplaceAll(query, " ", "%20")+
		"&start="+strconv.FormatInt(start.UnixNano(), 10)+"&end="+strconv.FormatInt(end.UnixNano(), 10)+"&step=60", nil)
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(statsQuery, "stats by (_stream, jid)") {
		t.Fatalf("expected the captures in the stats grouping, got %q", statsQuery)
	}
	var response struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode: %v", err)
	}
	seen := map[string]bool{}
	for _, series := range response.Data.Result {
		seen[series.Metric["jid"]] = true
	}
	if len(response.Data.Result) != 2 || !seen["job_a"] || !seen["job_b"] {
		t.Fatalf("expected one series per capture value, got %v", response.Data.Result)
	}
}
