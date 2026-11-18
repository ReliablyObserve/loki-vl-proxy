package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// vlTailRowRateText is what VictoriaLogs v1.54.0 writes when a live tail
// matches more than 10,000 new rows in one refresh (app/vlselect/logsql
// ProcessLiveTailRequest, maxRowsPerTailQuery, #1837): before the first rows
// it is the answer's body with status 400; after them it is a line of text in
// the NDJSON stream (the status is already sent). Older releases keep
// streaming.
const vlTailRowRateText = `cannot get tail results for query ["app:=api"]: the query matches too many logs (10000+); add more specific filters to the query; see https://docs.victoriametrics.com/victorialogs/querying/#live-tailing`

// vlTailRowRateBackend is a VictoriaLogs v1.54.0 whose live tail of the
// query hits that limit: the history read and the preflight probe answer
// with no rows, the tail answers in mode "open" with the 400, in mode
// "stream" with one row and then the text.
type vlTailRowRateBackend struct{ mode string }

func (b vlTailRowRateBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasSuffix(r.URL.Path, "/select/logsql/tail"):
		if b.mode == "open" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprint(w, vlTailRowRateText)
			return
		}
		w.Header().Set("Content-Type", "application/stream+json")
		_, _ = fmt.Fprintf(w, `{"_time":%q,"_stream":"{app=\"api\"}","_msg":"m1","app":"api"}`+"\n", time.Now().UTC().Format(time.RFC3339Nano))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		_, _ = fmt.Fprint(w, vlTailRowRateText)
	case strings.HasPrefix(r.URL.Path, "/select/logsql/"):
		w.Header().Set("Content-Type", "application/stream+json")
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// A live tail VictoriaLogs v1.54.0 refuses or ends because it matches too
// many rows per second: in the default auto mode a tail refused at its start
// is served by the polling tail (bounded pages, as for a tail VictoriaLogs
// cannot open) and stays open; a native tail refused at its start, and any
// tail whose stream ends with the text after its rows, closes with Loki's
// internal error (1011) after the rows it sent. None of VictoriaLogs' text
// reaches the client. Loki has no such limit (its tail keeps streaming), so a
// busy tail that hits it mid-stream ends earlier than Loki's on v1.54.0:
// recorded in the registry (vl-live-tail-row-rate-limit) as a follow-up.
// conformance: vl-live-tail-row-rate-limit, semantics/error-layer-internal-go-text, loki_api_v1_tail
func TestTail_VLRowRateLimitClosesWithLokiText(t *testing.T) {
	for _, tc := range []struct {
		name      string
		tail      TailMode
		mode      string
		open      bool // the tail is still open when the client stops reading
		minFrames int
	}{
		{"auto/open", TailModeAuto, "open", true, 0},
		{"auto/stream", TailModeAuto, "stream", false, 1},
		{"native/open", TailModeNative, "open", false, 0},
		{"native/stream", TailModeNative, "stream", false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(vlTailRowRateBackend{mode: tc.mode})
			t.Cleanup(srv.Close)
			c := cache.New(time.Minute, 100)
			t.Cleanup(c.Close)
			p, err := New(Config{BackendURL: srv.URL, Cache: c, LogLevel: "error", BackendTimeout: 5 * time.Second, TailMode: tc.tail})
			if err != nil {
				t.Fatalf("new proxy: %v", err)
			}
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			mux := http.NewServeMux()
			p.RegisterRoutes(mux)
			front := httptest.NewServer(mux)
			t.Cleanup(front.Close)
			wsURL := "ws" + strings.TrimPrefix(front.URL, "http") + `/loki/api/v1/tail?query=%7Bapp%3D%22api%22%7D`
			readFor := 8 * time.Second
			if tc.open {
				readFor = 3 * time.Second
			}
			got := readTail(t, wsURL, readFor)
			if strings.Contains(got.closeText+got.httpBody, "too many logs") {
				t.Fatalf("VictoriaLogs' text reached the client: %q", got.closeText+got.httpBody)
			}
			if tc.open {
				if got.httpStatus != 0 || got.closeCode != 0 {
					t.Fatalf("want an open tail, got HTTP %d %q, close %d %q", got.httpStatus, got.httpBody, got.closeCode, got.closeText)
				}
				return
			}
			if got.httpStatus != 0 || got.closeCode != websocket.CloseInternalServerErr || got.closeText != lokiErrInternal {
				t.Fatalf("want close 1011 %q, got HTTP %d %q, close %d %q", lokiErrInternal, got.httpStatus, got.httpBody, got.closeCode, got.closeText)
			}
			if got.frames < tc.minFrames {
				t.Fatalf("%d frames before the close, want at least %d", got.frames, tc.minFrames)
			}
		})
	}
}
