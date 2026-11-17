package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkDeleteCreate: one delete create request through the registered route, its token checked, against a
// VictoriaLogs stand-in that answers at once.
func BenchmarkDeleteCreate(b *testing.B) {
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"task_id":"1"}`))
	}))
	defer vl.Close()
	p, err := New(Config{BackendURL: vl.URL, Cache: cache.New(time.Minute, 1000), LogLevel: "error",
		DeleteAPIEnabled: true, DeleteAPIAuthToken: testDeleteToken})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterProxyRoutes(mux)
	target := "/loki/api/v1/delete?" + url.Values{"query": {`{app="nginx", env="prod"} |= "error"`}, "start": {"1794567075"}, "end": {"1794570675"}}.Encode()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r := httptest.NewRequest(http.MethodPost, target, nil)
		r.Header.Set("X-Delete-Token", testDeleteToken)
		r.Header.Set("X-Delete-Confirmation", "true")
		r.Header.Set("X-Scope-OrgID", "0")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		if w.Code != http.StatusNoContent {
			b.Fatalf("%d %s", w.Code, w.Body)
		}
	}
}
