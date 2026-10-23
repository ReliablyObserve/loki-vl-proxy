package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// statsRangeSynthVL answers every stats_query_range call with series of
// one point per bucket, widened to the bucket edges as VictoriaLogs does.
func statsRangeSynthVL(b testing.TB, series int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.URL.Path != "/select/logsql/stats_query_range" {
			return
		}
		step, _ := time.ParseDuration(r.Form.Get("step"))
		var off time.Duration
		if o := r.Form.Get("offset"); o != "" {
			off, _ = time.ParseDuration(o)
		}
		start := vlTruncate(parseFakeVLTime(b, r.Form.Get("start")), int64(step), int64(off))
		end := vlTruncate(parseFakeVLTime(b, r.Form.Get("end"))-1, int64(step), int64(off)) + int64(step)
		var sb strings.Builder
		sb.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
		for s := 0; s < series; s++ {
			if s > 0 {
				sb.WriteByte(',')
			}
			fmt.Fprintf(&sb, `{"metric":{"__name__":"count(*)","event":"ev%03d"},"values":[`, s)
			for k, ts := 0, start; ts < end; k, ts = k+1, ts+int64(step) {
				if k > 0 {
					sb.WriteByte(',')
				}
				fmt.Fprintf(&sb, `[%s,"%d"]`, strconv.FormatFloat(float64(ts)/1e9, 'f', -1, 64), 1+(k+s)%7)
			}
			sb.WriteString("]}")
		}
		sb.WriteString("]}}")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(sb.String()))
	}))
}

// A Logs Drilldown field breakdown over 6h at a 10s step (40 series, 2160
// points each) answered from kept buckets: the work left per request is the
// proxy's own (rebuilding the VictoriaLogs answer, the tumbling relabel, the
// Loki response).
func BenchmarkStatsExtentWarmDrilldownBreakdown(b *testing.B) {
	srv := statsRangeSynthVL(b, 40)
	defer srv.Close()
	p := newExtentTestProxy(b, srv.URL, true, 256<<20)
	q := `sum by (event) (count_over_time({env="production"} | detected_level="info" | json event="[\"event\"]" | drop __error__, __error_details__ | event!="" [10s]))`
	hdr := http.Header{"X-Query-Tags": {"Source=grafana-lokiexplore-app"}}
	end := time.Unix(1700001000, 0).UTC().Add(7 * time.Hour)
	code, body := extentQueryRange(b, p, q, end.Add(-6*time.Hour-time.Hour), end, 10*time.Second, hdr)
	if code != 200 || len(body) < 1<<20 {
		b.Fatalf("warm-up: %d, %d bytes", code, len(body))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := end.Add(-time.Duration(i%30+1) * 10 * time.Second)
		if code, body := extentQueryRange(b, p, q, e.Add(-6*time.Hour), e, 10*time.Second, hdr); code != 200 {
			b.Fatalf("%d %s", code, body)
		}
	}
}

// The metric builders' response of 30 series of 2160 points.
func BenchmarkManualMetricResponse(b *testing.B) {
	var result []map[string]interface{}
	for s := 0; s < 30; s++ {
		points := make([][]interface{}, 0, 2160)
		for k := 0; k < 2160; k++ {
			points = append(points, []interface{}{float64(1700001000 + 10*k), strconv.Itoa(k % 17)})
		}
		result = append(result, map[string]interface{}{"metric": map[string]string{"service_name": fmt.Sprintf("svc-%02d", s)}, "values": points})
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = marshalManualMetricResponse("matrix", result)
	}
}
