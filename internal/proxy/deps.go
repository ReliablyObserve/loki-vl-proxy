package proxy

import (
	"log/slog"
	"net/http"
	"net/url"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
	mw "github.com/ReliablyObserve/Loki-VL-proxy/internal/middleware"
)

// Deps holds external dependencies injected at startup.
// All fields are safe for concurrent read after New completes.
// These mirror the corresponding fields in Proxy and are extracted here
// to support future decomposition of the Proxy struct.
type Deps struct {
	backend    *url.URL
	rulerBackend *url.URL
	alertsBackend *url.URL

	client     *http.Client
	tailClient *http.Client

	cache              *cache.Cache
	compatCache        *cache.Cache
	translationCache   *cache.Cache
	streamFieldNamesCache *cache.Cache
	peerCache          *cache.PeerCache

	log          *slog.Logger
	metrics      *metrics.Metrics
	queryTracker *metrics.QueryTracker

	coalescer *mw.Coalescer
	limiter   *mw.RateLimiter
	breaker   *mw.CircuitBreaker
}
