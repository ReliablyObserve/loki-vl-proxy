package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// volumeBenchBackend answers every VictoriaLogs shape the volume handlers have
// used (hits, stats_query, stats_query_range) with seriesCount apps and
// points buckets, so the proxy-side cost can be compared across versions.
func volumeBenchBackend(b *testing.B, seriesCount, points int, bucketStart int64) *httptest.Server {
	b.Helper()
	var hits, vector, matrix strings.Builder
	hits.WriteString(`{"hits":[`)
	vector.WriteString(`{"status":"success","data":{"resultType":"vector","result":[`)
	matrix.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
	for i := 0; i < seriesCount; i++ {
		if i > 0 {
			hits.WriteByte(',')
			vector.WriteByte(',')
			matrix.WriteByte(',')
		}
		app := fmt.Sprintf("service-%04d", i)
		fmt.Fprintf(&hits, `{"fields":{"app":%q},"timestamps":[`, app)
		fmt.Fprintf(&matrix, `{"metric":{"__name__":"_b","app":%q},"values":[`, app)
		for j := 0; j < points; j++ {
			if j > 0 {
				hits.WriteByte(',')
				matrix.WriteByte(',')
			}
			ts := bucketStart + int64(j*60)
			fmt.Fprintf(&hits, `%q`, strconv.FormatInt(ts, 10)+"000000000")
			fmt.Fprintf(&matrix, `[%d,"%d"]`, ts, 1000+i*7+j)
		}
		hits.WriteString(`],"values":[`)
		for j := 0; j < points; j++ {
			if j > 0 {
				hits.WriteByte(',')
			}
			hits.WriteString(strconv.Itoa(10 + j))
		}
		hits.WriteString(`]}`)
		matrix.WriteString(`]}`)
		fmt.Fprintf(&vector, `{"metric":{"__name__":"_b","app":%q},"value":[%d,"%d"]}`, app, bucketStart, 100000+i*13)
	}
	hits.WriteString(`]}`)
	vector.WriteString(`]}}`)
	matrix.WriteString(`]}}`)
	bodies := map[string][]byte{
		"/select/logsql/hits":              []byte(hits.String()),
		"/select/logsql/stats_query":       []byte(vector.String()),
		"/select/logsql/stats_query_range": []byte(matrix.String()),
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := bodies[r.URL.Path]
		if !ok {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[]}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	b.Cleanup(srv.Close)
	return srv
}

func BenchmarkVolumeHandlers(b *testing.B) {
	const start = int64(1789448400)
	for _, bc := range []struct {
		name       string
		rangeQuery bool
		series     int
		points     int
		params     url.Values
	}{
		{"instant_service_name_300", false, 300, 1, url.Values{"query": {"{service_name=~`.+`}"}, "targetLabels": {"service_name"}, "limit": {"5000"}}},
		{"instant_selector_label_300", false, 300, 1, url.Values{"query": {`{app=~".+"}`}, "limit": {"5000"}}},
		{"range_target_label_300x60", true, 300, 60, url.Values{"query": {`{app=~".+"}`}, "targetLabels": {"app"}, "step": {"60"}, "limit": {"5000"}}},
	} {
		b.Run(bc.name, func(b *testing.B) {
			vl := volumeBenchBackend(b, bc.series, bc.points, start)
			p, err := New(Config{BackendURL: vl.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error"})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			handler := p.handleVolume
			path := "/loki/api/v1/index/volume"
			if bc.rangeQuery {
				handler = p.handleVolumeRange
				path = "/loki/api/v1/index/volume_range"
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				params := url.Values{}
				for k, v := range bc.params {
					params[k] = v
				}
				params.Set("start", strconv.FormatInt(start, 10))
				// A distinct end per iteration keeps the response cache cold.
				params.Set("end", strconv.FormatInt((start+3600)*1_000_000_000+int64(i), 10))
				w := httptest.NewRecorder()
				handler(w, httptest.NewRequest(http.MethodGet, path+"?"+params.Encode(), nil))
				if w.Code != http.StatusOK {
					b.Fatalf("status %d: %s", w.Code, w.Body.String())
				}
			}
		})
	}
}
