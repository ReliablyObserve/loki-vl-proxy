package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// labelMapHandlerShapes are metric queries the query and query_range handlers serve: label_replace and ranking roots,
// whose label_replace calls the handlers apply to the answer, and a plain aggregation.
var labelMapHandlerShapes = []struct{ name, q string }{
	{"label_replace", `label_replace(sum by (app) (rate({app="a"}[5m])), "short", "$1", "app", "(.*)-.*")`},
	{"label_replace_2", `label_replace(label_replace(sum by (app) (count_over_time({app="a"}[5m])), "x", "$1", "app", "(.*)"), "y", "$1", "x", "(.*)")`},
	{"topk_label_replace", `topk(3, label_replace(sum by (app) (rate({app="a"}[5m])), "short", "$1", "app", "(.*)-.*"))`},
	{"topk", `topk(3, sum by (app) (rate({app="a"}[5m])))`},
	{"plain", `sum by (app) (rate({app="a"}[5m]))`},
}

// BenchmarkLabelMapHandlers serves each shape through the handlers against a stub VictoriaLogs, every request a new
// window (no response cache answers it), instant and range.
func BenchmarkLabelMapHandlers(b *testing.B) {
	p, err := New(Config{BackendURL: "http://vl.invalid", Cache: cache.New(time.Minute, 1000), LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	p.client.Transport = &routeStub{data: true}
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	for _, s := range labelMapHandlerShapes {
		for _, instant := range []bool{true, false} {
			name := s.name + "/range"
			if instant {
				name = s.name + "/instant"
			}
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					end := routeEnd.Add(time.Duration(i%100000) * time.Minute).Unix()
					form := url.Values{"query": {s.q}}
					path := "/loki/api/v1/query"
					if instant {
						form.Set("time", strconv.FormatInt(end, 10))
					} else {
						path = "/loki/api/v1/query_range"
						form.Set("start", strconv.FormatInt(end-3600, 10))
						form.Set("end", strconv.FormatInt(end, 10))
						form.Set("step", "60")
					}
					req := httptest.NewRequest(http.MethodGet, path+"?"+form.Encode(), nil)
					req.Header.Set("X-Scope-OrgID", "0")
					rec := httptest.NewRecorder()
					mux.ServeHTTP(rec, req)
					if rec.Code != http.StatusOK {
						b.Fatalf("%s: %d %s", s.q, rec.Code, rec.Body.String())
					}
				}
			})
		}
	}
}
