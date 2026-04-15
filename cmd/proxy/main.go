package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
	mw "github.com/ReliablyObserve/Loki-VL-proxy/internal/middleware"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/observability"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

var version = "dev"

type envConfig struct {
	listenAddr        string
	backendURL        string
	rulerBackendURL   string
	alertsBackendURL  string
	procRoot          string
	tenantMapJSON     string
	tenantLimitsAllow string
	tenantDefaultJSON string
	tenantLimitsJSON  string
	otlpEndpoint      string
	otlpCompression   string
	otlpHeaders       string
	labelStyle        string
	fieldMappingJSON  string
	metadataFieldMode string
	extraLabelFields  string
	serviceName       string
	serviceNamespace  string
	serviceInstanceID string
	deploymentEnv     string
}

type proxyRuntimeConfig struct {
	backendURL                          string
	rulerBackendURL                     string
	alertsBackendURL                    string
	cache                               *cache.Cache
	compatCache                         *cache.Cache
	logLevel                            string
	maxConcurrent                       int
	ratePerSecond                       float64
	rateBurst                           int
	tenantMapJSON                       string
	tenantLimitsAllowPublish            string
	tenantDefaultLimitsJSON             string
	tenantLimitsJSON                    string
	maxLines                            int
	backendTimeout                      time.Duration
	backendMinVersion                   string
	backendAllowUnsupportedVersion      bool
	backendVersionCheckTimeout          time.Duration
	backendBasicAuth                    string
	backendCompression                  string
	clientResponseCompression           string
	clientResponseCompressionMinBytes   int
	backendTLSSkip                      bool
	forwardHeaders                      string
	forwardAuthorization                bool
	forwardCookies                      string
	derivedFieldsJSON                   string
	streamResponse                      bool
	emitStructuredMetadata              bool
	patternsEnabled                     bool
	patternsAutodetectFromQueries       bool
	patternsCustomRaw                   string
	patternsCustomFile                  string
	queryRangeWindowing                 bool
	queryRangeSplitInterval             time.Duration
	queryRangeMaxParallel               int
	queryRangeAdaptiveParallel          bool
	queryRangeParallelMin               int
	queryRangeParallelMax               int
	queryRangeLatencyTarget             time.Duration
	queryRangeLatencyBackoff            time.Duration
	queryRangeAdaptiveCooldown          time.Duration
	queryRangeErrorBackoffThreshold     float64
	queryRangeFreshness                 time.Duration
	queryRangeRecentCacheTTL            time.Duration
	queryRangeHistoryTTL                time.Duration
	queryRangePrefilterIndexStats       bool
	queryRangePrefilterMinWindows       int
	queryRangeStreamAwareBatching       bool
	queryRangeExpensiveHitThreshold     int64
	queryRangeExpensiveMaxParallel      int
	queryRangeAlignWindows              bool
	queryRangeWindowTimeout             time.Duration
	queryRangePartialResponses          bool
	queryRangeBackgroundWarm            bool
	queryRangeBackgroundWarmMaxWindows  int
	recentTailRefreshEnabled            bool
	recentTailRefreshWindow             time.Duration
	recentTailRefreshMaxStaleness       time.Duration
	authEnabled                         bool
	allowGlobalTenant                   bool
	registerInstrumentation             *bool
	enablePprof                         bool
	enableQueryAnalytics                bool
	adminAuthToken                      string
	tailAllowedOrigins                  string
	tailMode                            string
	metricsMaxTenants                   int
	metricsMaxClients                   int
	metricsTrustProxyHeaders            bool
	metricsExportSensitiveLabels        bool
	metricsMaxConcurrency               int
	labelStyle                          string
	metadataFieldMode                   string
	fieldMappingJSON                    string
	streamFieldsCSV                     string
	extraLabelFieldsCSV                 string
	labelValuesIndexedCache             bool
	labelValuesHotLimit                 int
	labelValuesIndexMaxEntries          int
	labelValuesIndexPersistPath         string
	labelValuesIndexPersistInterval     time.Duration
	labelValuesIndexStartupStale        time.Duration
	labelValuesIndexPeerWarmTimeout     time.Duration
	patternsPersistPath                 string
	patternsPersistInterval             time.Duration
	patternsStartupStale                time.Duration
	patternsPeerWarmTimeout             time.Duration
	peerSelf                            string
	peerDiscovery                       string
	peerDNS                             string
	peerStatic                          string
	peerAuthToken                       string
	peerWriteThrough                    bool
	peerWriteThroughMinTTL              time.Duration
	peerHotReadAheadEnabled             bool
	peerHotReadAheadInterval            time.Duration
	peerHotReadAheadJitter              time.Duration
	peerHotReadAheadTopN                int
	peerHotReadAheadMaxKeysPerInterval  int
	peerHotReadAheadMaxBytesPerInterval int64
	peerHotReadAheadMaxConcurrency      int
	peerHotReadAheadMinTTL              time.Duration
	peerHotReadAheadMaxObjectBytes      int
	peerHotReadAheadTenantFairShare     int
	peerHotReadAheadErrorBackoff        time.Duration
}

type otlpRuntimeConfig struct {
	endpoint              string
	interval              time.Duration
	headers               string
	compression           string
	timeout               time.Duration
	tlsSkipVerify         bool
	serviceName           string
	serviceNamespace      string
	serviceVersion        string
	serviceInstanceID     string
	deploymentEnvironment string
}

type serverRuntimeOptions struct {
	listenAddr           string
	handler              http.Handler
	readTimeout          time.Duration
	readHeaderTimeout    time.Duration
	writeTimeout         time.Duration
	idleTimeout          time.Duration
	maxHeaderBytes       int
	tlsClientCAFile      string
	tlsRequireClientCert bool
	connContext          func(context.Context, net.Conn) context.Context
	connRotation         httpConnRotationConfig
}

type httpServer interface {
	ListenAndServe() error
	ListenAndServeTLS(certFile, keyFile string) error
	Shutdown(ctx context.Context) error
}

type otlpMetricsPusher interface {
	Start()
	Stop()
}

type otlpPusherFactory func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher

type serverLoopOptions struct {
	listenAddr  string
	backendURL  string
	tlsCertFile string
	tlsKeyFile  string
}

type loggerConfig struct {
	level                 string
	serviceName           string
	serviceNamespace      string
	serviceVersion        string
	serviceInstanceID     string
	deploymentEnvironment string
}

type reloadableProxy interface {
	ReloadTenantMap(map[string]proxy.TenantMapping)
	ReloadFieldMappings([]proxy.FieldMapping)
}

type signalNotifier func(chan<- os.Signal, ...os.Signal)
type runtimeBuilder func(runtimeOptions, *slog.Logger, signalNotifier, otlpPusherFactory) (*runtimeState, error)
type serverRunner func(httpServer, serverLoopOptions, *slog.Logger, func(string, ...any))
type shutdownHandlerFunc func(<-chan os.Signal, httpServer, time.Duration, *slog.Logger)
type exitFunc func(int)

type runtimeOptions struct {
	cacheTTL                    time.Duration
	cacheMax                    int
	cacheMaxBytes               int
	compatCacheEnabled          bool
	compatCacheMaxPercent       int
	diskCfg                     cache.DiskCacheConfig
	proxyCfg                    proxyRuntimeConfig
	otlpCfg                     otlpRuntimeConfig
	maxBodyBytes                int64
	responseCompression         string
	responseCompressionMinBytes int
	serverOpts                  serverRuntimeOptions
}

type runtimeState struct {
	proxy        *proxy.Proxy
	server       httpServer
	cacheCleanup func()
	stopOTLP     func()
	reloadCh     chan os.Signal
	shutdownCh   chan os.Signal
}

const (
	defaultCacheMaxBytes               = 256 * 1024 * 1024
	defaultCompatCachePercent          = 10
	maxCompatCachePercent              = 50
	defaultResponseCompressionMinBytes = 1024
)

func main() {
	runMain(
		os.Args[1:],
		os.Getenv,
		os.Stdout,
		os.Stderr,
		signal.Notify,
		os.Exit,
		func(cfg metrics.OTLPConfig, m *metrics.Metrics) otlpMetricsPusher {
			return metrics.NewOTLPPusher(cfg, m)
		},
		buildRuntime,
		runServerLoop,
		handleShutdown,
	)
}

func runMain(
	args []string,
	getenv func(string) string,
	stdout io.Writer,
	stderr io.Writer,
	notify signalNotifier,
	exit exitFunc,
	newPusher otlpPusherFactory,
	buildRuntimeFn runtimeBuilder,
	runServerLoopFn serverRunner,
	handleShutdownFn shutdownHandlerFunc,
) {
	if err := run(args, getenv, stdout, notify, newPusher, buildRuntimeFn, runServerLoopFn, handleShutdownFn); err != nil {
		fmt.Fprintln(stderr, err)
		exit(1)
	}
}

func run(
	args []string,
	getenv func(string) string,
	logWriter io.Writer,
	notify signalNotifier,
	newPusher otlpPusherFactory,
	buildRuntimeFn runtimeBuilder,
	runServerLoopFn serverRunner,
	handleShutdownFn shutdownHandlerFunc,
) error {
	fs := flag.NewFlagSet("loki-vl-proxy", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	// Server flags
	listenAddr := fs.String("listen", ":3100", "Address to listen on (Loki-compatible frontend)")
	backendURL := fs.String("backend", "http://localhost:9428", "VictoriaLogs backend URL")
	rulerBackendURL := fs.String("ruler-backend", "", "Optional alert/ruler backend URL for /rules passthrough (for example vmalert)")
	alertsBackendURL := fs.String("alerts-backend", "", "Optional alert backend URL for /alerts passthrough (defaults to -ruler-backend when unset)")
	logLevel := fs.String("log-level", "info", "Log level: debug, info, warn, error")

	// Cache flags
	cacheTTL := fs.Duration("cache-ttl", 60*time.Second, "Cache TTL for label/metadata queries")
	cacheMax := fs.Int("cache-max", 10000, "Maximum cache entries")
	cacheMaxBytes := fs.Int("cache-max-bytes", defaultCacheMaxBytes, "Maximum in-memory L1 cache size in bytes")
	compatCacheEnabled := fs.Bool("compat-cache-enabled", true, "Enable the safe Tier0 compatibility-edge response cache for cacheable GET read endpoints")
	compatCacheMaxPercent := fs.Int("compat-cache-max-percent", defaultCompatCachePercent, "Percent of -cache-max-bytes reserved for the Tier0 compatibility-edge cache (0 disables, max 50)")

	// Disk cache flags
	diskCachePath := fs.String("disk-cache-path", "", "Path to L2 disk cache (bbolt). Empty disables.")
	diskCacheCompress := fs.Bool("disk-cache-compress", true, "Gzip compression for disk cache")
	diskCacheFlushSize := fs.Int("disk-cache-flush-size", 100, "Flush write buffer after N entries")
	diskCacheFlushInterval := fs.Duration("disk-cache-flush-interval", 5*time.Second, "Write buffer flush interval")
	diskCacheMinTTL := fs.Duration("disk-cache-min-ttl", 30*time.Second, "Minimum entry TTL eligible for L2 disk cache writes (shorter TTL entries stay in-memory only)")
	diskCacheMaxBytes := fs.Int64("disk-cache-max-bytes", 0, "Maximum on-disk L2 cache size in bytes (0 = unlimited)")
	// Tenant mapping
	tenantMapJSON := fs.String("tenant-map", "", `JSON tenant mapping: {"org-name":{"account_id":"1","project_id":"0"}}`)
	tenantLimitsAllowPublish := fs.String("tenant-limits-allow-publish", "", "Comma-separated limit fields published on /config/tenant/v1/limits and /loki/api/v1/drilldown-limits")
	tenantDefaultLimitsJSON := fs.String("tenant-default-limits", "", `JSON map of default published limits overrides (for example {"query_timeout":"2m","max_query_series":1000})`)
	tenantLimitsJSON := fs.String("tenant-limits", "", `JSON map of per-tenant published limits overrides keyed by X-Scope-OrgID`)

	// OTLP telemetry flags
	otlpEndpoint := fs.String("otlp-endpoint", "", "OTLP HTTP endpoint (e.g., http://otel-collector:4318/v1/metrics)")
	otlpInterval := fs.Duration("otlp-interval", 30*time.Second, "OTLP push interval")
	otlpCompression := fs.String("otlp-compression", "none", "OTLP compression: none, gzip, zstd")
	otlpTimeout := fs.Duration("otlp-timeout", 10*time.Second, "OTLP HTTP request timeout")
	otlpTLSSkipVerify := fs.Bool("otlp-tls-skip-verify", false, "Skip TLS verification for OTLP endpoint")
	otlpHeaders := fs.String("otlp-headers", "", "Comma-separated OTLP HTTP headers in key=value form")
	otelServiceName := fs.String("otel-service-name", "loki-vl-proxy", "OpenTelemetry service.name for logs and OTLP metrics")
	otelServiceNamespace := fs.String("otel-service-namespace", "", "OpenTelemetry service.namespace for logs and OTLP metrics")
	otelServiceInstanceID := fs.String("otel-service-instance-id", "", "OpenTelemetry service.instance.id for logs and OTLP metrics")
	deploymentEnvironment := fs.String("deployment-environment", "", "OpenTelemetry deployment.environment.name for logs and OTLP metrics")
	procRoot := fs.String("proc-root", "/proc", "Proc filesystem root for system metrics (/proc for container scope, /host/proc for host scope)")

	// HTTP server hardening
	readTimeout := fs.Duration("http-read-timeout", 30*time.Second, "HTTP server read timeout")
	readHeaderTimeout := fs.Duration("http-read-header-timeout", 10*time.Second, "HTTP server read header timeout")
	writeTimeout := fs.Duration("http-write-timeout", 120*time.Second, "HTTP server write timeout")
	idleTimeout := fs.Duration("http-idle-timeout", 120*time.Second, "HTTP server idle timeout")
	maxHeaderBytes := fs.Int("http-max-header-bytes", 1<<20, "HTTP max header size (default: 1MB)")
	maxBodyBytes := fs.Int64("http-max-body-bytes", 10<<20, "HTTP max request body size (default: 10MB)")
	maxConcurrent := fs.Int("max-concurrent", 100, "Maximum concurrent requests allowed through the proxy (0 disables)")
	rateLimitPerSecond := fs.Float64("rate-limit-per-second", 50, "Per-client request rate limit in requests per second (0 disables)")
	rateLimitBurst := fs.Int("rate-limit-burst", 100, "Per-client burst size for request rate limiting (0 disables burst bucket)")
	httpConnMaxAge := fs.Duration("http-conn-max-age", 10*time.Minute, "Maximum lifetime for downstream HTTP/1.x keepalive connections before responding with Connection: close (0 disables)")
	httpConnMaxAgeJitter := fs.Duration("http-conn-max-age-jitter", 2*time.Minute, "Jitter applied to downstream HTTP/1.x connection age rotation to avoid synchronized reconnects")
	httpConnMaxRequests := fs.Int("http-conn-max-requests", 256, "Maximum downstream HTTP/1.x requests served on one keepalive connection before responding with Connection: close (0 disables)")
	httpConnOverloadMaxAge := fs.Duration("http-conn-overload-max-age", 90*time.Second, "Shorter downstream HTTP/1.x connection lifetime applied while query_range backpressure is active (0 disables overload shedding)")

	// TLS server
	tlsCertFile := fs.String("tls-cert-file", "", "TLS certificate file for HTTPS server")
	tlsKeyFile := fs.String("tls-key-file", "", "TLS private key file for HTTPS server")
	tlsClientCAFile := fs.String("tls-client-ca-file", "", "CA certificate file used to verify HTTPS client certificates")
	tlsRequireClientCert := fs.Bool("tls-require-client-cert", false, "Require and verify HTTPS client certificates")

	// Response compression
	enableGzip := fs.Bool("response-gzip", true, "Deprecated: enable compressed responses for clients that accept them; prefer -response-compression")
	responseCompression := fs.String("response-compression", "", "Response compression codec: auto, gzip, none (default: auto)")
	responseCompressionMinBytes := fs.Int("response-compression-min-bytes", defaultResponseCompressionMinBytes, "Minimum response size before frontend compression starts (0 compresses any size)")

	// Grafana datasource compatibility
	maxLines := fs.Int("max-lines", 1000, "Default max lines per query")
	backendTimeout := fs.Duration("backend-timeout", 120*time.Second, "Timeout for non-streaming requests to the VictoriaLogs backend")
	backendMinVersion := fs.String("backend-min-version", "v1.30.0", "Minimum VictoriaLogs version considered fully supported at startup")
	backendAllowUnsupportedVersion := fs.Bool("backend-allow-unsupported-version", false, "Allow startup with backend versions lower than -backend-min-version (at your own risk)")
	backendVersionCheckTimeout := fs.Duration("backend-version-check-timeout", 5*time.Second, "Timeout for startup backend version compatibility check")
	backendBasicAuth := fs.String("backend-basic-auth", "", "Basic auth for VL backend (user:password)")
	backendCompression := fs.String("backend-compression", "auto", "Backend HTTP compression preference: auto, gzip, zstd, none")
	backendTLSSkip := fs.Bool("backend-tls-skip-verify", false, "Skip TLS verification for VL backend")
	forwardHeaders := fs.String("forward-headers", "", "Comma-separated list of HTTP headers to forward to VL backend")
	forwardAuthorization := fs.Bool("forward-authorization", false, "Forward Authorization header to VL backend (equivalent to including Authorization in -forward-headers)")
	forwardCookies := fs.String("forward-cookies", "", "Comma-separated list of cookie names to forward to VL backend")
	derivedFieldsJSON := fs.String("derived-fields", "", `JSON derived fields: [{"name":"traceID","matcherRegex":"trace_id=([a-f0-9]+)","url":"http://tempo/trace/${__value.raw}"}]`)
	streamResponse := fs.Bool("stream-response", false, "Stream log responses via chunked transfer encoding")
	emitStructuredMetadata := fs.Bool("emit-structured-metadata", true, "Include Loki 3-tuple stream values [timestamp, line, metadata] in query responses")
	patternsEnabled := fs.Bool("patterns-enabled", true, "Enable /loki/api/v1/patterns endpoint (Grafana Logs Drilldown patterns)")
	patternsAutodetectFromQueries := fs.Bool("patterns-autodetect-from-queries", false, "Warm /loki/api/v1/patterns cache from successful query/query_range log responses (opt-in global autodetect)")
	patternsCustomRaw := fs.String("patterns-custom", "", `JSON array (or newline-separated text) of custom Drilldown patterns always prepended to /loki/api/v1/patterns responses`)
	patternsCustomFile := fs.String("patterns-custom-file", "", "Path to custom Drilldown patterns file (JSON array or newline-separated text) loaded on startup")
	queryRangeWindowing := fs.Bool("query-range-windowing", true, "Enable query_range window splitting and window-level cache reuse for log queries")
	queryRangeSplitInterval := fs.Duration("query-range-split-interval", time.Hour, "Time window size used for query_range split/merge (for example 15m, 1h, 24h)")
	queryRangeMaxParallel := fs.Int("query-range-max-parallel", 2, "Maximum number of query_range windows fetched in parallel when adaptive parallelism is disabled")
	queryRangeAdaptiveParallel := fs.Bool("query-range-adaptive-parallel", true, "Enable adaptive query_range window parallelism based on backend latency/error feedback")
	queryRangeParallelMin := fs.Int("query-range-adaptive-min-parallel", 2, "Minimum adaptive query_range window parallelism")
	queryRangeParallelMax := fs.Int("query-range-adaptive-max-parallel", 8, "Maximum adaptive query_range window parallelism")
	queryRangeLatencyTarget := fs.Duration("query-range-latency-target", 1500*time.Millisecond, "Adaptive target backend fetch latency per window")
	queryRangeLatencyBackoff := fs.Duration("query-range-latency-backoff", 3*time.Second, "Adaptive backoff threshold: reduce parallelism when backend fetch latency exceeds this value")
	queryRangeAdaptiveCooldown := fs.Duration("query-range-adaptive-cooldown", 30*time.Second, "Minimum time between adaptive query_range parallelism adjustments")
	queryRangeErrorBackoffThreshold := fs.Float64("query-range-error-backoff-threshold", 0.02, "Adaptive backoff threshold for backend fetch errors (0-1 ratio)")
	queryRangeFreshness := fs.Duration("query-range-freshness", 10*time.Minute, "Near-now freshness boundary; windows newer than now-freshness use recent cache TTL")
	queryRangeRecentCacheTTL := fs.Duration("query-range-recent-cache-ttl", 0, "Cache TTL for near-now query_range windows (0 disables near-now result caching)")
	queryRangeHistoryTTL := fs.Duration("query-range-history-cache-ttl", 24*time.Hour, "Cache TTL for historical query_range windows older than -query-range-freshness")
	queryRangePrefilterIndexStats := fs.Bool("query-range-prefilter-index-stats", true, "Use /select/logsql/hits preflight to skip empty query_range windows before log fanout")
	queryRangePrefilterMinWindows := fs.Int("query-range-prefilter-min-windows", 8, "Minimum split windows required before enabling query_range prefilter")
	queryRangeStreamAwareBatching := fs.Bool("query-range-stream-aware-batching", true, "Reduce query_range batch parallelism for expensive windows estimated from prefilter hits")
	queryRangeExpensiveHitThreshold := fs.Int64("query-range-expensive-hit-threshold", 2000, "Prefilter hit threshold above which a query_range window is treated as expensive")
	queryRangeExpensiveMaxParallel := fs.Int("query-range-expensive-max-parallel", 1, "Maximum window parallelism for expensive query_range windows")
	queryRangeAlignWindows := fs.Bool("query-range-align-windows", true, "Align query_range split windows to fixed interval boundaries for overlap cache reuse")
	queryRangeWindowTimeout := fs.Duration("query-range-window-timeout", 20*time.Second, "Per-window backend timeout budget for query_range window fetches (0 disables)")
	queryRangePartialResponses := fs.Bool("query-range-partial-responses", false, "Allow partial query_range responses on retryable backend failures")
	queryRangeBackgroundWarm := fs.Bool("query-range-background-warm", true, "Warm failed query_range windows in background after partial response")
	queryRangeBackgroundWarmMaxWindows := fs.Int("query-range-background-warm-max-windows", 24, "Maximum query_range windows warmed in background after partial response")
	recentTailRefreshEnabled := fs.Bool("recent-tail-refresh-enabled", true, "Bypass stale near-now cache hits and fetch latest backend data while preserving historical cache")
	recentTailRefreshWindow := fs.Duration("recent-tail-refresh-window", 2*time.Minute, "How close request end must be to now to enable near-now cache freshness bypass")
	recentTailRefreshMaxStaleness := fs.Duration("recent-tail-refresh-max-staleness", 15*time.Second, "Maximum acceptable cache age for near-now requests before cache bypass")

	// Loki-style auth / instrumentation controls
	authEnabled := fs.Bool("auth.enabled", false, "Require X-Scope-OrgID on query requests. When false, requests without a tenant header use the backend default tenant.")
	registerInstrumentation := fs.Bool("server.register-instrumentation", true, "Register instrumentation handlers such as /metrics")
	enablePprof := fs.Bool("server.enable-pprof", false, "Expose /debug/pprof/* handlers")
	enableQueryAnalytics := fs.Bool("server.enable-query-analytics", false, "Expose /debug/queries query analytics")
	adminAuthToken := fs.String("server.admin-auth-token", "", "Bearer token required for admin/debug endpoints when set")
	tailAllowedOrigins := fs.String("tail.allowed-origins", "", "Comma-separated WebSocket Origin allowlist for /loki/api/v1/tail. Empty denies browser origins.")
	tailMode := fs.String("tail.mode", "auto", "Tail streaming mode: auto (native with synthetic fallback), native, or synthetic")
	metricsMaxTenants := fs.Int("metrics.max-tenants", 256, "Maximum unique tenant labels retained in exported metrics before collapsing into __overflow__")
	metricsMaxClients := fs.Int("metrics.max-clients", 256, "Maximum unique client labels retained in exported metrics before collapsing into __overflow__")
	metricsTrustProxyHeaders := fs.Bool("metrics.trust-proxy-headers", false, "Trust X-Grafana-User and X-Forwarded-For when deriving per-client metrics labels")
	metricsExportSensitiveLabels := fs.Bool("metrics.export-sensitive-labels", false, "Export per-tenant and per-client identity metrics on /metrics and OTLP")
	metricsMaxConcurrency := fs.Int("server.metrics-max-concurrency", 1, "Maximum concurrent /metrics scrapes served at once (0 disables the cap)")

	// Label translation
	labelStyle := fs.String("label-style", "passthrough", `Label name translation mode:
  passthrough  - no translation, pass VL field names as-is (use when VL stores underscores)
  underscores  - convert dots to underscores (use when VL stores OTel-style dotted names like service.name)`)
	metadataFieldMode := fs.String("metadata-field-mode", "hybrid", `Field exposure mode for detected_fields and structured metadata:
  native      - expose VictoriaLogs field names as-is
  translated  - expose only Loki-compatible translated aliases
  hybrid      - expose both native VL field names and translated aliases when they differ`)
	fieldMappingJSON := fs.String("field-mapping", "", `JSON custom field mappings: [{"vl_field":"service.name","loki_label":"service_name"}]`)
	streamFieldsCSV := fs.String("stream-fields", "", `Comma-separated VL _stream_fields labels for stream selector optimization (e.g., "app,env,namespace")`)
	extraLabelFieldsCSV := fs.String("extra-label-fields", "", `Comma-separated additional VL field names exposed on /labels and eligible for alias resolution (for example "host.id,custom.pipeline.processing")`)
	labelValuesIndexedCache := fs.Bool("label-values-indexed-cache", false, "Enable indexed browse cache for /loki/api/v1/label/{name}/values (hot subset first for empty-query requests)")
	labelValuesHotLimit := fs.Int("label-values-hot-limit", 200, "Default number of label values returned for empty-query browse requests when indexed cache is enabled")
	labelValuesIndexMaxEntries := fs.Int("label-values-index-max-entries", 200000, "Maximum indexed values retained per tenant+label when indexed label-values cache is enabled")
	labelValuesIndexPersistPath := fs.String("label-values-index-persist-path", "", "Path to persisted label-values index snapshot JSON file. Empty disables persistence.")
	labelValuesIndexPersistInterval := fs.Duration("label-values-index-persist-interval", 30*time.Second, "How often to persist the in-memory label-values index snapshot to disk")
	labelValuesIndexStartupStale := fs.Duration("label-values-index-startup-stale-threshold", 60*time.Second, "Treat on-disk label-values index snapshot older than this as stale and warm from peers before serving")
	labelValuesIndexPeerWarmTimeout := fs.Duration("label-values-index-startup-peer-warm-timeout", 5*time.Second, "Maximum time to wait for startup label-values index warm from peers when disk snapshot is stale or missing")
	patternsPersistPath := fs.String("patterns-persist-path", "", "Path to persisted patterns snapshot JSON file. Empty disables persistence.")
	patternsPersistInterval := fs.Duration("patterns-persist-interval", 30*time.Second, "How often to persist in-memory patterns snapshots to disk")
	patternsStartupStale := fs.Duration("patterns-startup-stale-threshold", 60*time.Second, "Treat on-disk patterns snapshot older than this as stale and warm from peers before serving")
	patternsPeerWarmTimeout := fs.Duration("patterns-startup-peer-warm-timeout", 5*time.Second, "Maximum time to wait for startup patterns snapshot warm from peers")
	allowGlobalTenant := fs.Bool("tenant.allow-global", false, `Allow X-Scope-OrgID "*" to bypass AccountID/ProjectID scoping and use the backend default tenant`)

	// Peer cache (fleet distribution)
	peerSelf := fs.String("peer-self", "", `This instance's address for peer cache (e.g., "10.0.0.1:3100"). Empty disables peer cache.`)
	peerDiscovery := fs.String("peer-discovery", "", `Peer discovery: "dns" (headless service) or "static" (comma-separated)`)
	peerDNS := fs.String("peer-dns", "", `Headless service DNS name for peer discovery (e.g., "proxy-headless.ns.svc.cluster.local")`)
	peerStatic := fs.String("peer-static", "", `Static peer list (e.g., "10.0.0.1:3100,10.0.0.2:3100")`)
	peerAuthToken := fs.String("peer-auth-token", "", "Shared token required on /_cache/get and /_cache/set peer-cache requests when set")
	peerWriteThrough := fs.Bool("peer-write-through", true, "Push cache writes from non-owner peers to owner peers for warmer distributed cache under skewed traffic")
	peerWriteThroughMinTTL := fs.Duration("peer-write-through-min-ttl", 30*time.Second, "Minimum TTL eligible for peer owner write-through pushes")
	peerHotReadAheadEnabled := fs.Bool("peer-hot-read-ahead-enabled", false, "Enable bounded hot read-ahead from peer hot index to prewarm local shadows")
	peerHotReadAheadInterval := fs.Duration("peer-hot-read-ahead-interval", 30*time.Second, "Base interval for periodic peer hot read-ahead pulls")
	peerHotReadAheadJitter := fs.Duration("peer-hot-read-ahead-jitter", 5*time.Second, "Random jitter added to peer hot read-ahead interval")
	peerHotReadAheadTopN := fs.Int("peer-hot-read-ahead-top-n", 256, "Number of top hot keys requested from each peer hot index")
	peerHotReadAheadMaxKeysPerInterval := fs.Int("peer-hot-read-ahead-max-keys-per-interval", 64, "Maximum number of hot keys prefetched per interval")
	peerHotReadAheadMaxBytesPerInterval := fs.Int64("peer-hot-read-ahead-max-bytes-per-interval", 8<<20, "Maximum bytes prefetched per interval")
	peerHotReadAheadMaxConcurrency := fs.Int("peer-hot-read-ahead-max-concurrency", 4, "Maximum concurrent hot-index and prefetch peer operations")
	peerHotReadAheadMinTTL := fs.Duration("peer-hot-read-ahead-min-ttl", 30*time.Second, "Minimum remaining TTL required for hot read-ahead candidates")
	peerHotReadAheadMaxObjectBytes := fs.Int("peer-hot-read-ahead-max-object-bytes", 256<<10, "Maximum object size eligible for hot read-ahead")
	peerHotReadAheadTenantFairShare := fs.Int("peer-hot-read-ahead-tenant-fair-share", 50, "Maximum per-tenant share (percent) of key budget in fairness pass")
	peerHotReadAheadErrorBackoff := fs.Duration("peer-hot-read-ahead-error-backoff", 15*time.Second, "Base read-ahead cooldown applied after peer/index errors")

	if err := fs.Parse(args); err != nil {
		return err
	}

	resolvedResponseCompression, err := resolveResponseCompression(*responseCompression, *enableGzip)
	if err != nil {
		return err
	}
	if *responseCompressionMinBytes < 0 {
		return fmt.Errorf("invalid -response-compression-min-bytes: must be >= 0")
	}
	resolvedBackendCompression, err := normalizeCompressionSetting(*backendCompression)
	if err != nil {
		return fmt.Errorf("invalid -backend-compression: %w", err)
	}

	envCfg := applyEnvOverrides(envConfig{
		listenAddr:        *listenAddr,
		backendURL:        *backendURL,
		rulerBackendURL:   *rulerBackendURL,
		alertsBackendURL:  *alertsBackendURL,
		procRoot:          *procRoot,
		tenantMapJSON:     *tenantMapJSON,
		tenantLimitsAllow: *tenantLimitsAllowPublish,
		tenantDefaultJSON: *tenantDefaultLimitsJSON,
		tenantLimitsJSON:  *tenantLimitsJSON,
		otlpEndpoint:      *otlpEndpoint,
		otlpCompression:   *otlpCompression,
		otlpHeaders:       *otlpHeaders,
		labelStyle:        *labelStyle,
		fieldMappingJSON:  *fieldMappingJSON,
		metadataFieldMode: *metadataFieldMode,
		extraLabelFields:  *extraLabelFieldsCSV,
		serviceName:       *otelServiceName,
		serviceNamespace:  *otelServiceNamespace,
		serviceInstanceID: *otelServiceInstanceID,
		deploymentEnv:     *deploymentEnvironment,
	}, getenv)

	if err := validateAdminExposure(envCfg.listenAddr, *enablePprof, *enableQueryAnalytics, *adminAuthToken); err != nil {
		return err
	}

	logger := buildLogger(logWriter, loggerConfig{
		level:                 *logLevel,
		serviceName:           envCfg.serviceName,
		serviceNamespace:      envCfg.serviceNamespace,
		serviceVersion:        version,
		serviceInstanceID:     envCfg.serviceInstanceID,
		deploymentEnvironment: envCfg.deploymentEnv,
	})
	metrics.SetProcRoot(envCfg.procRoot)
	logSystemMetricsStartup(logger)

	fatal := func(msg string, args ...any) {
		logger.Error(msg, args...)
		os.Exit(1)
	}

	runtime, err := buildRuntimeFn(runtimeOptions{
		cacheTTL:              *cacheTTL,
		cacheMax:              *cacheMax,
		cacheMaxBytes:         *cacheMaxBytes,
		compatCacheEnabled:    *compatCacheEnabled,
		compatCacheMaxPercent: *compatCacheMaxPercent,
		diskCfg: cache.DiskCacheConfig{
			Path:          *diskCachePath,
			Compression:   *diskCacheCompress,
			FlushSize:     *diskCacheFlushSize,
			FlushInterval: *diskCacheFlushInterval,
			MinTTL:        *diskCacheMinTTL,
			MaxBytes:      *diskCacheMaxBytes,
		},
		proxyCfg: proxyRuntimeConfig{
			backendURL:                          envCfg.backendURL,
			rulerBackendURL:                     envCfg.rulerBackendURL,
			alertsBackendURL:                    envCfg.alertsBackendURL,
			logLevel:                            *logLevel,
			maxConcurrent:                       *maxConcurrent,
			ratePerSecond:                       *rateLimitPerSecond,
			rateBurst:                           *rateLimitBurst,
			tenantMapJSON:                       envCfg.tenantMapJSON,
			tenantLimitsAllowPublish:            envCfg.tenantLimitsAllow,
			tenantDefaultLimitsJSON:             envCfg.tenantDefaultJSON,
			tenantLimitsJSON:                    envCfg.tenantLimitsJSON,
			maxLines:                            *maxLines,
			backendTimeout:                      *backendTimeout,
			backendMinVersion:                   *backendMinVersion,
			backendAllowUnsupportedVersion:      *backendAllowUnsupportedVersion,
			backendVersionCheckTimeout:          *backendVersionCheckTimeout,
			backendBasicAuth:                    *backendBasicAuth,
			backendCompression:                  resolvedBackendCompression,
			clientResponseCompression:           resolvedResponseCompression,
			clientResponseCompressionMinBytes:   *responseCompressionMinBytes,
			backendTLSSkip:                      *backendTLSSkip,
			forwardHeaders:                      *forwardHeaders,
			forwardAuthorization:                *forwardAuthorization,
			forwardCookies:                      *forwardCookies,
			derivedFieldsJSON:                   *derivedFieldsJSON,
			streamResponse:                      *streamResponse,
			emitStructuredMetadata:              *emitStructuredMetadata,
			patternsEnabled:                     *patternsEnabled,
			patternsAutodetectFromQueries:       *patternsAutodetectFromQueries,
			patternsCustomRaw:                   *patternsCustomRaw,
			patternsCustomFile:                  *patternsCustomFile,
			queryRangeWindowing:                 *queryRangeWindowing,
			queryRangeSplitInterval:             *queryRangeSplitInterval,
			queryRangeMaxParallel:               *queryRangeMaxParallel,
			queryRangeAdaptiveParallel:          *queryRangeAdaptiveParallel,
			queryRangeParallelMin:               *queryRangeParallelMin,
			queryRangeParallelMax:               *queryRangeParallelMax,
			queryRangeLatencyTarget:             *queryRangeLatencyTarget,
			queryRangeLatencyBackoff:            *queryRangeLatencyBackoff,
			queryRangeAdaptiveCooldown:          *queryRangeAdaptiveCooldown,
			queryRangeErrorBackoffThreshold:     *queryRangeErrorBackoffThreshold,
			queryRangeFreshness:                 *queryRangeFreshness,
			queryRangeRecentCacheTTL:            *queryRangeRecentCacheTTL,
			queryRangeHistoryTTL:                *queryRangeHistoryTTL,
			queryRangePrefilterIndexStats:       *queryRangePrefilterIndexStats,
			queryRangePrefilterMinWindows:       *queryRangePrefilterMinWindows,
			queryRangeStreamAwareBatching:       *queryRangeStreamAwareBatching,
			queryRangeExpensiveHitThreshold:     *queryRangeExpensiveHitThreshold,
			queryRangeExpensiveMaxParallel:      *queryRangeExpensiveMaxParallel,
			queryRangeAlignWindows:              *queryRangeAlignWindows,
			queryRangeWindowTimeout:             *queryRangeWindowTimeout,
			queryRangePartialResponses:          *queryRangePartialResponses,
			queryRangeBackgroundWarm:            *queryRangeBackgroundWarm,
			queryRangeBackgroundWarmMaxWindows:  *queryRangeBackgroundWarmMaxWindows,
			recentTailRefreshEnabled:            *recentTailRefreshEnabled,
			recentTailRefreshWindow:             *recentTailRefreshWindow,
			recentTailRefreshMaxStaleness:       *recentTailRefreshMaxStaleness,
			authEnabled:                         *authEnabled,
			allowGlobalTenant:                   *allowGlobalTenant,
			registerInstrumentation:             registerInstrumentation,
			enablePprof:                         *enablePprof,
			enableQueryAnalytics:                *enableQueryAnalytics,
			adminAuthToken:                      *adminAuthToken,
			tailAllowedOrigins:                  *tailAllowedOrigins,
			tailMode:                            *tailMode,
			metricsMaxTenants:                   *metricsMaxTenants,
			metricsMaxClients:                   *metricsMaxClients,
			metricsTrustProxyHeaders:            *metricsTrustProxyHeaders,
			metricsExportSensitiveLabels:        *metricsExportSensitiveLabels,
			metricsMaxConcurrency:               *metricsMaxConcurrency,
			labelStyle:                          envCfg.labelStyle,
			metadataFieldMode:                   envCfg.metadataFieldMode,
			fieldMappingJSON:                    envCfg.fieldMappingJSON,
			streamFieldsCSV:                     *streamFieldsCSV,
			extraLabelFieldsCSV:                 envCfg.extraLabelFields,
			labelValuesIndexedCache:             *labelValuesIndexedCache,
			labelValuesHotLimit:                 *labelValuesHotLimit,
			labelValuesIndexMaxEntries:          *labelValuesIndexMaxEntries,
			labelValuesIndexPersistPath:         *labelValuesIndexPersistPath,
			labelValuesIndexPersistInterval:     *labelValuesIndexPersistInterval,
			labelValuesIndexStartupStale:        *labelValuesIndexStartupStale,
			labelValuesIndexPeerWarmTimeout:     *labelValuesIndexPeerWarmTimeout,
			patternsPersistPath:                 *patternsPersistPath,
			patternsPersistInterval:             *patternsPersistInterval,
			patternsStartupStale:                *patternsStartupStale,
			patternsPeerWarmTimeout:             *patternsPeerWarmTimeout,
			peerSelf:                            *peerSelf,
			peerDiscovery:                       *peerDiscovery,
			peerDNS:                             *peerDNS,
			peerStatic:                          *peerStatic,
			peerAuthToken:                       *peerAuthToken,
			peerWriteThrough:                    *peerWriteThrough,
			peerWriteThroughMinTTL:              *peerWriteThroughMinTTL,
			peerHotReadAheadEnabled:             *peerHotReadAheadEnabled,
			peerHotReadAheadInterval:            *peerHotReadAheadInterval,
			peerHotReadAheadJitter:              *peerHotReadAheadJitter,
			peerHotReadAheadTopN:                *peerHotReadAheadTopN,
			peerHotReadAheadMaxKeysPerInterval:  *peerHotReadAheadMaxKeysPerInterval,
			peerHotReadAheadMaxBytesPerInterval: *peerHotReadAheadMaxBytesPerInterval,
			peerHotReadAheadMaxConcurrency:      *peerHotReadAheadMaxConcurrency,
			peerHotReadAheadMinTTL:              *peerHotReadAheadMinTTL,
			peerHotReadAheadMaxObjectBytes:      *peerHotReadAheadMaxObjectBytes,
			peerHotReadAheadTenantFairShare:     *peerHotReadAheadTenantFairShare,
			peerHotReadAheadErrorBackoff:        *peerHotReadAheadErrorBackoff,
		},
		otlpCfg: otlpRuntimeConfig{
			endpoint:              envCfg.otlpEndpoint,
			interval:              *otlpInterval,
			headers:               envCfg.otlpHeaders,
			compression:           envCfg.otlpCompression,
			timeout:               *otlpTimeout,
			tlsSkipVerify:         *otlpTLSSkipVerify,
			serviceName:           envCfg.serviceName,
			serviceNamespace:      envCfg.serviceNamespace,
			serviceVersion:        version,
			serviceInstanceID:     envCfg.serviceInstanceID,
			deploymentEnvironment: envCfg.deploymentEnv,
		},
		maxBodyBytes:                *maxBodyBytes,
		responseCompression:         resolvedResponseCompression,
		responseCompressionMinBytes: *responseCompressionMinBytes,
		serverOpts: serverRuntimeOptions{
			listenAddr:           envCfg.listenAddr,
			readTimeout:          *readTimeout,
			readHeaderTimeout:    *readHeaderTimeout,
			writeTimeout:         *writeTimeout,
			idleTimeout:          *idleTimeout,
			maxHeaderBytes:       *maxHeaderBytes,
			tlsClientCAFile:      *tlsClientCAFile,
			tlsRequireClientCert: *tlsRequireClientCert,
			connRotation: httpConnRotationConfig{
				maxAge:         *httpConnMaxAge,
				maxAgeJitter:   *httpConnMaxAgeJitter,
				maxRequests:    int64(*httpConnMaxRequests),
				overloadMaxAge: *httpConnOverloadMaxAge,
			},
		},
	}, logger, notify, newPusher)
	if err != nil {
		return fmt.Errorf("failed to initialize runtime: %w", err)
	}
	defer runtime.cacheCleanup()
	defer runtime.stopOTLP()

	go watchReloadSignals(runtime.reloadCh, runtime.proxy, getenv, logger)
	go runServerLoopFn(runtime.server, serverLoopOptions{
		listenAddr:  envCfg.listenAddr,
		backendURL:  envCfg.backendURL,
		tlsCertFile: *tlsCertFile,
		tlsKeyFile:  *tlsKeyFile,
	}, logger, fatal)
	handleShutdownFn(runtime.shutdownCh, runtime.server, 30*time.Second, logger)
	if err := runtime.proxy.Shutdown(context.Background()); err != nil {
		logger.Error("proxy shutdown hook failed", "error", err)
	}
	return nil
}

func buildCacheLayer(ttl time.Duration, maxEntries, maxBytes int, diskCfg cache.DiskCacheConfig, logger *slog.Logger) (*cache.Cache, func(), error) {
	if maxEntries <= 0 {
		return nil, nil, fmt.Errorf("cache-max must be greater than 0")
	}
	if maxBytes <= 0 {
		return nil, nil, fmt.Errorf("cache-max-bytes must be greater than 0")
	}

	c := cache.NewWithMaxBytes(ttl, maxEntries, maxBytes)
	if diskCfg.Path == "" {
		return c, func() { c.Close() }, nil
	}

	dc, err := cache.NewDiskCache(diskCfg)
	if err != nil {
		c.Close()
		return nil, nil, err
	}
	c.SetL2(dc)
	logger.Info("disk cache enabled",
		"path", diskCfg.Path,
		"compress", diskCfg.Compression,
		"flush_size", diskCfg.FlushSize,
		"flush_interval", diskCfg.FlushInterval.String(),
		"min_ttl", diskCfg.MinTTL.String(),
	)
	return c, func() {
		c.Close()
		_ = dc.Close()
	}, nil
}

func buildCompatCacheLayer(ttl time.Duration, maxEntries, primaryMaxBytes int, enabled bool, percent int, logger *slog.Logger) (*cache.Cache, func(), error) {
	if !enabled || percent <= 0 {
		return nil, func() {}, nil
	}
	if percent > maxCompatCachePercent {
		return nil, nil, fmt.Errorf("compat-cache-max-percent must be between 0 and %d", maxCompatCachePercent)
	}
	if maxEntries <= 0 {
		return nil, nil, fmt.Errorf("cache-max must be greater than 0 when compat cache is enabled")
	}
	if primaryMaxBytes <= 0 {
		return nil, nil, fmt.Errorf("cache-max-bytes must be greater than 0 when compat cache is enabled")
	}

	compatMaxBytes := primaryMaxBytes * percent / 100
	if compatMaxBytes <= 0 {
		return nil, nil, fmt.Errorf("compat-cache-max-percent=%d yields zero bytes; increase cache-max-bytes or percent", percent)
	}
	compatMaxEntries := maxEntries * percent / 100
	if compatMaxEntries <= 0 {
		compatMaxEntries = 1
	}

	c := cache.NewWithMaxBytes(ttl, compatMaxEntries, compatMaxBytes)
	logger.Info("compatibility edge cache enabled",
		"tier", "tier0",
		"max_entries", compatMaxEntries,
		"max_bytes", compatMaxBytes,
		"share_of_l1_percent", percent,
	)
	return c, func() { c.Close() }, nil
}

func buildRuntime(opts runtimeOptions, logger *slog.Logger, notify signalNotifier, newPusher otlpPusherFactory) (*runtimeState, error) {
	cacheLayer, cacheCleanup, err := buildCacheLayer(opts.cacheTTL, opts.cacheMax, opts.cacheMaxBytes, opts.diskCfg, logger)
	if err != nil {
		return nil, fmt.Errorf("open disk cache: %w", err)
	}
	compatCacheLayer, compatCleanup, err := buildCompatCacheLayer(opts.cacheTTL, opts.cacheMax, opts.cacheMaxBytes, opts.compatCacheEnabled, opts.compatCacheMaxPercent, logger)
	if err != nil {
		cacheCleanup()
		return nil, fmt.Errorf("build compatibility cache: %w", err)
	}

	proxyCfg := opts.proxyCfg
	proxyCfg.cache = cacheLayer
	proxyCfg.compatCache = compatCacheLayer
	builtProxyCfg, err := buildProxyConfig(proxyCfg)
	if err != nil {
		cacheCleanup()
		compatCleanup()
		return nil, fmt.Errorf("build proxy config: %w", err)
	}
	logProxyStartup(logger, builtProxyCfg, proxyCfg.peerSelf, proxyCfg.peerDiscovery, cacheLayer, compatCacheLayer)

	p, err := proxy.New(builtProxyCfg)
	if err != nil {
		cacheCleanup()
		compatCleanup()
		return nil, fmt.Errorf("create proxy: %w", err)
	}
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		cacheCleanup()
		compatCleanup()
		return nil, fmt.Errorf("backend compatibility gate: %w", err)
	}
	p.Init()

	stopOTLP := startOTLPMetricsPusher(opts.otlpCfg, p.GetMetrics(), logger, newPusher)

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	serverOpts := opts.serverOpts
	rotator := newHTTPConnRotator(serverOpts.connRotation, p.GetMetrics(), p.DownstreamConnectionPressure)
	serverOpts.connContext = nil
	if rotator != nil {
		serverOpts.connContext = rotator.ConnContextHook()
	}
	serverOpts.handler = wrapHandler(mux, opts.maxBodyBytes, opts.responseCompression, opts.responseCompressionMinBytes, rotator, p.GetMetrics())
	srv, err := buildHTTPServer(serverOpts)
	if err != nil {
		stopOTLP()
		cacheCleanup()
		compatCleanup()
		return nil, fmt.Errorf("build http server: %w", err)
	}
	srv.ConnState = p.GetMetrics().ConnStateHook()

	reloadCh, shutdownCh := buildSignalChannels(notify)
	return &runtimeState{
		proxy:  p,
		server: srv,
		cacheCleanup: func() {
			cacheCleanup()
			compatCleanup()
		},
		stopOTLP:   stopOTLP,
		reloadCh:   reloadCh,
		shutdownCh: shutdownCh,
	}, nil
}

func buildLogger(w io.Writer, cfg loggerConfig) *slog.Logger {
	logger := observability.NewLogger(w, observability.LoggerConfig{
		Level:                 cfg.level,
		ServiceName:           cfg.serviceName,
		ServiceNamespace:      cfg.serviceNamespace,
		ServiceVersion:        cfg.serviceVersion,
		ServiceInstanceID:     cfg.serviceInstanceID,
		DeploymentEnvironment: cfg.deploymentEnvironment,
	})
	slog.SetDefault(logger)
	return logger
}

func buildSignalChannels(notify signalNotifier) (chan os.Signal, chan os.Signal) {
	reloadCh := make(chan os.Signal, 1)
	notify(reloadCh, syscall.SIGHUP)
	shutdownCh := make(chan os.Signal, 1)
	notify(shutdownCh, syscall.SIGTERM, syscall.SIGINT)
	return reloadCh, shutdownCh
}

func handleShutdown(shutdownCh <-chan os.Signal, srv httpServer, timeout time.Duration, logger *slog.Logger) {
	sig := <-shutdownCh
	logger.Info("shutdown requested", "signal", sig.String())

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("http shutdown error", "error", err)
	}
	logger.Info("shutdown complete")
}

func watchReloadSignals(reloadCh <-chan os.Signal, p reloadableProxy, getenv func(string) string, logger *slog.Logger) {
	for range reloadCh {
		logger.Info("received sighup, reloading configuration")
		reloadDynamicConfig(p, getenv, logger)
	}
}

// maxBodyHandler limits the request body size to prevent resource exhaustion.
func maxBodyHandler(maxBytes int64, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
		}
		next.ServeHTTP(w, r)
	})
}

func wrapHandler(next http.Handler, maxBodyBytes int64, responseCompression string, responseCompressionMinBytes int, rotator *httpConnRotator, mm ...*metrics.Metrics) http.Handler {
	handler := maxBodyHandler(maxBodyBytes, next)
	handler = mw.CompressionHandlerWithOptions(handler, mw.CompressionOptions{
		Mode:     responseCompression,
		MinBytes: responseCompressionMinBytes,
	})
	if rotator != nil {
		handler = rotator.Wrap(handler)
	}
	var m *metrics.Metrics
	if len(mm) > 0 {
		m = mm[0]
	}
	if m != nil {
		handler = m.WrapHandler(handler)
	}
	return handler
}

func resolveResponseCompression(explicit string, legacyEnabled bool) (string, error) {
	if strings.TrimSpace(explicit) == "" {
		if legacyEnabled {
			return "auto", nil
		}
		return "none", nil
	}
	mode, err := normalizeFrontendCompressionSetting(explicit)
	if err != nil {
		return "", fmt.Errorf("invalid -response-compression: %w", err)
	}
	return mode, nil
}

func normalizeFrontendCompressionSetting(mode string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "auto":
		return "auto", nil
	case "none", "gzip":
		return strings.ToLower(strings.TrimSpace(mode)), nil
	case "zstd":
		// Keep older deployments starting cleanly, but collapse the public
		// client-facing surface to the gzip path we actually support and tune.
		return "gzip", nil
	default:
		return "", fmt.Errorf("%q (must be auto, gzip, or none)", mode)
	}
}

func validateAdminExposure(listenAddr string, enablePprof, enableQueryAnalytics bool, adminAuthToken string) error {
	if strings.TrimSpace(adminAuthToken) != "" {
		return nil
	}
	if !enablePprof && !enableQueryAnalytics {
		return nil
	}
	if isLoopbackListenAddr(listenAddr) {
		return nil
	}
	return fmt.Errorf("server.admin-auth-token is required when admin/debug endpoints are enabled on non-loopback listen addresses")
}

func isLoopbackListenAddr(listenAddr string) bool {
	listenAddr = strings.TrimSpace(listenAddr)
	if listenAddr == "" {
		return false
	}
	host := listenAddr
	if strings.HasPrefix(host, "[") && strings.Contains(host, "]:") {
		parsedHost, _, err := net.SplitHostPort(host)
		if err == nil {
			host = parsedHost
		}
	} else if strings.Contains(host, ":") {
		if parsedHost, _, err := net.SplitHostPort(host); err == nil {
			host = parsedHost
		}
	}
	host = strings.Trim(host, "[]")
	switch host {
	case "localhost", "127.0.0.1", "::1":
		return true
	case "":
		return false
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

func normalizeCompressionSetting(mode string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "auto":
		return "auto", nil
	case "none", "gzip", "zstd":
		return strings.ToLower(strings.TrimSpace(mode)), nil
	default:
		return "", fmt.Errorf("%q (must be auto, gzip, zstd, or none)", mode)
	}
}

func parseCSV(s string) []string {
	if s == "" {
		return nil
	}
	var result []string
	for _, f := range strings.Split(s, ",") {
		f = strings.TrimSpace(f)
		if f != "" {
			result = append(result, f)
		}
	}
	return result
}

func parseForwardHeaders(csv string, includeAuthorization bool) []string {
	headers := parseCSV(csv)
	if includeAuthorization {
		headers = append(headers, "Authorization")
	}
	if len(headers) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(headers))
	out := make([]string, 0, len(headers))
	for _, h := range headers {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}
		key := strings.ToLower(h)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, h)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func applyEnvOverrides(cfg envConfig, getenv func(string) string) envConfig {
	if v := getenv("LISTEN_ADDR"); v != "" {
		cfg.listenAddr = v
	}
	if v := getenv("VL_BACKEND_URL"); v != "" {
		cfg.backendURL = v
	}
	if v := getenv("RULER_BACKEND_URL"); v != "" && cfg.rulerBackendURL == "" {
		cfg.rulerBackendURL = v
	}
	if v := getenv("ALERTS_BACKEND_URL"); v != "" && cfg.alertsBackendURL == "" {
		cfg.alertsBackendURL = v
	}
	if v := getenv("PROC_ROOT"); v != "" && cfg.procRoot == "/proc" {
		cfg.procRoot = v
	}
	if v := getenv("TENANT_MAP"); v != "" && cfg.tenantMapJSON == "" {
		cfg.tenantMapJSON = v
	}
	if v := getenv("TENANT_LIMITS_ALLOW_PUBLISH"); v != "" && cfg.tenantLimitsAllow == "" {
		cfg.tenantLimitsAllow = v
	}
	if v := getenv("TENANT_DEFAULT_LIMITS"); v != "" && cfg.tenantDefaultJSON == "" {
		cfg.tenantDefaultJSON = v
	}
	if v := getenv("TENANT_LIMITS"); v != "" && cfg.tenantLimitsJSON == "" {
		cfg.tenantLimitsJSON = v
	}
	if v := getenv("OTLP_ENDPOINT"); v != "" && cfg.otlpEndpoint == "" {
		cfg.otlpEndpoint = v
	}
	if v := getenv("OTLP_COMPRESSION"); v != "" && cfg.otlpCompression == "none" {
		cfg.otlpCompression = v
	}
	if v := getenv("OTLP_HEADERS"); v != "" && cfg.otlpHeaders == "" {
		cfg.otlpHeaders = v
	}
	if v := getenv("LABEL_STYLE"); v != "" && cfg.labelStyle == "passthrough" {
		cfg.labelStyle = v
	}
	if v := getenv("FIELD_MAPPING"); v != "" && cfg.fieldMappingJSON == "" {
		cfg.fieldMappingJSON = v
	}
	if v := getenv("METADATA_FIELD_MODE"); v != "" && cfg.metadataFieldMode == "hybrid" {
		cfg.metadataFieldMode = v
	}
	if v := getenv("EXTRA_LABEL_FIELDS"); v != "" && cfg.extraLabelFields == "" {
		cfg.extraLabelFields = v
	}
	if v := getenv("OTEL_SERVICE_NAME"); v != "" && (cfg.serviceName == "" || cfg.serviceName == "loki-vl-proxy") {
		cfg.serviceName = v
	}
	if v := getenv("OTEL_SERVICE_NAMESPACE"); v != "" && cfg.serviceNamespace == "" {
		cfg.serviceNamespace = v
	}
	if v := getenv("OTEL_SERVICE_INSTANCE_ID"); v != "" && cfg.serviceInstanceID == "" {
		cfg.serviceInstanceID = v
	}
	if v := getenv("DEPLOYMENT_ENVIRONMENT"); v != "" && cfg.deploymentEnv == "" {
		cfg.deploymentEnv = v
	}
	return cfg
}

func parseTenantMapJSON(raw string) (map[string]proxy.TenantMapping, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var tenantMap map[string]proxy.TenantMapping
	if err := json.Unmarshal([]byte(raw), &tenantMap); err != nil {
		return nil, err
	}
	return tenantMap, nil
}

func parseTenantDefaultLimitsJSON(raw string) (map[string]any, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var limits map[string]any
	if err := json.Unmarshal([]byte(raw), &limits); err != nil {
		return nil, err
	}
	return limits, nil
}

func parseTenantLimitsJSON(raw string) (map[string]map[string]any, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var limits map[string]map[string]any
	if err := json.Unmarshal([]byte(raw), &limits); err != nil {
		return nil, err
	}
	return limits, nil
}

func parseFieldMappingsJSON(raw string) ([]proxy.FieldMapping, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var fieldMappings []proxy.FieldMapping
	if err := json.Unmarshal([]byte(raw), &fieldMappings); err != nil {
		return nil, err
	}
	return fieldMappings, nil
}

func parseDerivedFieldsJSON(raw string) ([]proxy.DerivedField, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var derivedFields []proxy.DerivedField
	if err := json.Unmarshal([]byte(raw), &derivedFields); err != nil {
		return nil, err
	}
	return derivedFields, nil
}

func parseCustomPatternsSource(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	normalize := func(values []string) []string {
		if len(values) == 0 {
			return nil
		}
		seen := make(map[string]struct{}, len(values))
		out := make([]string, 0, len(values))
		for _, v := range values {
			v = strings.TrimSpace(v)
			if v == "" {
				continue
			}
			if _, ok := seen[v]; ok {
				continue
			}
			seen[v] = struct{}{}
			out = append(out, v)
		}
		if len(out) == 0 {
			return nil
		}
		return out
	}

	var fromJSON []string
	if err := json.Unmarshal([]byte(raw), &fromJSON); err == nil {
		return normalize(fromJSON), nil
	}
	if strings.HasPrefix(raw, "[") {
		return nil, fmt.Errorf("expected valid JSON string array")
	}

	lines := strings.Split(raw, "\n")
	parsed := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parsed = append(parsed, line)
	}
	return normalize(parsed), nil
}

func parseCustomPatterns(inlineRaw, filePath string) ([]string, error) {
	inlinePatterns, err := parseCustomPatternsSource(inlineRaw)
	if err != nil {
		return nil, fmt.Errorf("parse -patterns-custom: %w", err)
	}

	var filePatterns []string
	filePath = strings.TrimSpace(filePath)
	if filePath != "" {
		data, readErr := os.ReadFile(filePath)
		if readErr != nil {
			return nil, fmt.Errorf("read -patterns-custom-file %q: %w", filePath, readErr)
		}
		filePatterns, err = parseCustomPatternsSource(string(data))
		if err != nil {
			return nil, fmt.Errorf("parse -patterns-custom-file %q: %w", filePath, err)
		}
	}

	combined := make([]string, 0, len(inlinePatterns)+len(filePatterns))
	combined = append(combined, inlinePatterns...)
	combined = append(combined, filePatterns...)
	return parseCustomPatternsSource(strings.Join(combined, "\n"))
}

func parseLabelModes(labelStyle, metadataFieldMode string) (proxy.LabelStyle, proxy.MetadataFieldMode, error) {
	ls := proxy.LabelStyle(labelStyle)
	switch ls {
	case proxy.LabelStylePassthrough, proxy.LabelStyleUnderscores:
	default:
		return "", "", fmt.Errorf("invalid -label-style: %q (must be 'passthrough' or 'underscores')", labelStyle)
	}
	mfm := proxy.MetadataFieldMode(metadataFieldMode)
	switch mfm {
	case proxy.MetadataFieldModeNative, proxy.MetadataFieldModeTranslated, proxy.MetadataFieldModeHybrid:
	default:
		return "", "", fmt.Errorf("invalid -metadata-field-mode: %q (must be 'native', 'translated', or 'hybrid')", metadataFieldMode)
	}
	return ls, mfm, nil
}

func parseHeaderMapCSV(s string) map[string]string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	headers := make(map[string]string)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		if k == "" || v == "" {
			continue
		}
		headers[k] = v
	}
	if len(headers) == 0 {
		return nil
	}
	return headers
}

func boolPointer(v bool) *bool {
	return &v
}

func buildOTLPConfig(cfg otlpRuntimeConfig) metrics.OTLPConfig {
	return metrics.OTLPConfig{
		Endpoint:              cfg.endpoint,
		Interval:              cfg.interval,
		Headers:               parseHeaderMapCSV(cfg.headers),
		Compression:           metrics.OTLPCompression(cfg.compression),
		Timeout:               cfg.timeout,
		TLSSkipVerify:         cfg.tlsSkipVerify,
		ServiceName:           cfg.serviceName,
		ServiceNamespace:      cfg.serviceNamespace,
		ServiceVersion:        cfg.serviceVersion,
		ServiceInstanceID:     cfg.serviceInstanceID,
		DeploymentEnvironment: cfg.deploymentEnvironment,
	}
}

func startOTLPMetricsPusher(cfg otlpRuntimeConfig, m *metrics.Metrics, logger *slog.Logger, newPusher otlpPusherFactory) func() {
	if strings.TrimSpace(cfg.endpoint) == "" {
		return func() {}
	}
	pusher := newPusher(buildOTLPConfig(cfg), m)
	pusher.Start()
	logger.Info("otlp metrics push enabled",
		"endpoint", cfg.endpoint,
		"interval", cfg.interval.String(),
		"compression", cfg.compression,
	)
	return pusher.Stop
}

func buildProxyConfig(cfg proxyRuntimeConfig) (proxy.Config, error) {
	alertsBackendURL := cfg.alertsBackendURL
	if alertsBackendURL == "" {
		alertsBackendURL = cfg.rulerBackendURL
	}
	tenantMap, err := parseTenantMapJSON(cfg.tenantMapJSON)
	if err != nil {
		return proxy.Config{}, fmt.Errorf("parse tenant map: %w", err)
	}
	tenantDefaultLimits, err := parseTenantDefaultLimitsJSON(cfg.tenantDefaultLimitsJSON)
	if err != nil {
		return proxy.Config{}, fmt.Errorf("parse default tenant limits: %w", err)
	}
	tenantLimits, err := parseTenantLimitsJSON(cfg.tenantLimitsJSON)
	if err != nil {
		return proxy.Config{}, fmt.Errorf("parse tenant limits: %w", err)
	}
	fieldMappings, err := parseFieldMappingsJSON(cfg.fieldMappingJSON)
	if err != nil {
		return proxy.Config{}, fmt.Errorf("parse field mappings: %w", err)
	}
	ls, mfm, err := parseLabelModes(cfg.labelStyle, cfg.metadataFieldMode)
	if err != nil {
		return proxy.Config{}, err
	}
	derivedFields, err := parseDerivedFieldsJSON(cfg.derivedFieldsJSON)
	if err != nil {
		return proxy.Config{}, fmt.Errorf("parse derived fields: %w", err)
	}
	customPatterns, err := parseCustomPatterns(cfg.patternsCustomRaw, cfg.patternsCustomFile)
	if err != nil {
		return proxy.Config{}, err
	}
	tailMode := proxy.TailMode(cfg.tailMode)
	switch tailMode {
	case "", proxy.TailModeAuto:
		tailMode = proxy.TailModeAuto
	case proxy.TailModeNative, proxy.TailModeSynthetic:
	default:
		return proxy.Config{}, fmt.Errorf("invalid -tail.mode: %q (must be 'auto', 'native', or 'synthetic')", cfg.tailMode)
	}

	var peerCache *cache.PeerCache
	if cfg.peerSelf != "" && cfg.peerDiscovery != "" {
		peerCache = cache.NewPeerCache(cache.PeerConfig{
			SelfAddr:                 cfg.peerSelf,
			DiscoveryType:            cfg.peerDiscovery,
			DNSName:                  cfg.peerDNS,
			StaticPeers:              cfg.peerStatic,
			Port:                     3100,
			AuthToken:                cfg.peerAuthToken,
			WriteThrough:             cfg.peerWriteThrough,
			WriteThroughMinTTL:       cfg.peerWriteThroughMinTTL,
			ReadAheadEnabled:         cfg.peerHotReadAheadEnabled,
			ReadAheadInterval:        cfg.peerHotReadAheadInterval,
			ReadAheadJitter:          cfg.peerHotReadAheadJitter,
			ReadAheadTopN:            cfg.peerHotReadAheadTopN,
			ReadAheadMaxKeys:         cfg.peerHotReadAheadMaxKeysPerInterval,
			ReadAheadMaxBytes:        cfg.peerHotReadAheadMaxBytesPerInterval,
			ReadAheadMaxConcurrency:  cfg.peerHotReadAheadMaxConcurrency,
			ReadAheadMinTTL:          cfg.peerHotReadAheadMinTTL,
			ReadAheadMaxObjectBytes:  cfg.peerHotReadAheadMaxObjectBytes,
			ReadAheadTenantFairShare: cfg.peerHotReadAheadTenantFairShare,
			ReadAheadErrorBackoff:    cfg.peerHotReadAheadErrorBackoff,
		})
	}

	return proxy.Config{
		BackendURL:                         cfg.backendURL,
		RulerBackendURL:                    cfg.rulerBackendURL,
		AlertsBackendURL:                   alertsBackendURL,
		Cache:                              cfg.cache,
		CompatCache:                        cfg.compatCache,
		LogLevel:                           cfg.logLevel,
		MaxConcurrent:                      cfg.maxConcurrent,
		RatePerSecond:                      cfg.ratePerSecond,
		RateBurst:                          cfg.rateBurst,
		TenantMap:                          tenantMap,
		TenantLimitsAllowPublish:           parseCSV(cfg.tenantLimitsAllowPublish),
		TenantDefaultLimits:                tenantDefaultLimits,
		TenantLimits:                       tenantLimits,
		MaxLines:                           cfg.maxLines,
		BackendTimeout:                     cfg.backendTimeout,
		BackendMinVersion:                  cfg.backendMinVersion,
		BackendAllowUnsupportedVersion:     cfg.backendAllowUnsupportedVersion,
		BackendVersionCheckTimeout:         cfg.backendVersionCheckTimeout,
		BackendBasicAuth:                   cfg.backendBasicAuth,
		BackendCompression:                 cfg.backendCompression,
		ClientResponseCompression:          cfg.clientResponseCompression,
		ClientResponseCompressionMinBytes:  cfg.clientResponseCompressionMinBytes,
		BackendTLSSkip:                     cfg.backendTLSSkip,
		ForwardHeaders:                     parseForwardHeaders(cfg.forwardHeaders, cfg.forwardAuthorization),
		ForwardCookies:                     parseCSV(cfg.forwardCookies),
		DerivedFields:                      derivedFields,
		StreamResponse:                     cfg.streamResponse,
		EmitStructuredMetadata:             cfg.emitStructuredMetadata,
		PatternsEnabled:                    boolPointer(cfg.patternsEnabled),
		PatternsAutodetectFromQueries:      cfg.patternsAutodetectFromQueries,
		PatternsCustom:                     customPatterns,
		QueryRangeWindowingEnabled:         cfg.queryRangeWindowing,
		QueryRangeSplitInterval:            cfg.queryRangeSplitInterval,
		QueryRangeMaxParallel:              cfg.queryRangeMaxParallel,
		QueryRangeAdaptiveParallel:         cfg.queryRangeAdaptiveParallel,
		QueryRangeParallelMin:              cfg.queryRangeParallelMin,
		QueryRangeParallelMax:              cfg.queryRangeParallelMax,
		QueryRangeLatencyTarget:            cfg.queryRangeLatencyTarget,
		QueryRangeLatencyBackoff:           cfg.queryRangeLatencyBackoff,
		QueryRangeAdaptiveCooldown:         cfg.queryRangeAdaptiveCooldown,
		QueryRangeErrorBackoffThreshold:    cfg.queryRangeErrorBackoffThreshold,
		QueryRangeFreshness:                cfg.queryRangeFreshness,
		QueryRangeRecentCacheTTL:           cfg.queryRangeRecentCacheTTL,
		QueryRangeHistoryCacheTTL:          cfg.queryRangeHistoryTTL,
		QueryRangePrefilterIndexStats:      cfg.queryRangePrefilterIndexStats,
		QueryRangePrefilterMinWindows:      cfg.queryRangePrefilterMinWindows,
		QueryRangeStreamAwareBatching:      cfg.queryRangeStreamAwareBatching,
		QueryRangeExpensiveHitThreshold:    cfg.queryRangeExpensiveHitThreshold,
		QueryRangeExpensiveMaxParallel:     cfg.queryRangeExpensiveMaxParallel,
		QueryRangeAlignWindows:             cfg.queryRangeAlignWindows,
		QueryRangeWindowTimeout:            cfg.queryRangeWindowTimeout,
		QueryRangePartialResponses:         cfg.queryRangePartialResponses,
		QueryRangeBackgroundWarm:           cfg.queryRangeBackgroundWarm,
		QueryRangeBackgroundWarmMaxWindows: cfg.queryRangeBackgroundWarmMaxWindows,
		RecentTailRefreshEnabled:           cfg.recentTailRefreshEnabled,
		RecentTailRefreshWindow:            cfg.recentTailRefreshWindow,
		RecentTailRefreshMaxStaleness:      cfg.recentTailRefreshMaxStaleness,
		AuthEnabled:                        cfg.authEnabled,
		AllowGlobalTenant:                  cfg.allowGlobalTenant,
		RegisterInstrumentation:            cfg.registerInstrumentation,
		EnablePprof:                        cfg.enablePprof,
		EnableQueryAnalytics:               cfg.enableQueryAnalytics,
		AdminAuthToken:                     cfg.adminAuthToken,
		TailAllowedOrigins:                 parseCSV(cfg.tailAllowedOrigins),
		TailMode:                           tailMode,
		MetricsMaxTenants:                  cfg.metricsMaxTenants,
		MetricsMaxClients:                  cfg.metricsMaxClients,
		MetricsTrustProxyHeaders:           cfg.metricsTrustProxyHeaders,
		MetricsExportSensitiveLabels:       cfg.metricsExportSensitiveLabels,
		MetricsMaxConcurrency:              cfg.metricsMaxConcurrency,
		LabelStyle:                         ls,
		MetadataFieldMode:                  mfm,
		FieldMappings:                      fieldMappings,
		StreamFields:                       parseCSV(cfg.streamFieldsCSV),
		ExtraLabelFields:                   parseCSV(cfg.extraLabelFieldsCSV),
		LabelValuesIndexedCache:            cfg.labelValuesIndexedCache,
		LabelValuesHotLimit:                cfg.labelValuesHotLimit,
		LabelValuesIndexMaxEntries:         cfg.labelValuesIndexMaxEntries,
		LabelValuesIndexPersistPath:        cfg.labelValuesIndexPersistPath,
		LabelValuesIndexPersistInterval:    cfg.labelValuesIndexPersistInterval,
		LabelValuesIndexStartupStale:       cfg.labelValuesIndexStartupStale,
		LabelValuesIndexPeerWarmTimeout:    cfg.labelValuesIndexPeerWarmTimeout,
		PatternsPersistPath:                cfg.patternsPersistPath,
		PatternsPersistInterval:            cfg.patternsPersistInterval,
		PatternsStartupStale:               cfg.patternsStartupStale,
		PatternsPeerWarmTimeout:            cfg.patternsPeerWarmTimeout,
		PeerCache:                          peerCache,
		PeerAuthToken:                      cfg.peerAuthToken,
	}, nil
}

func buildServerTLSConfig(clientCAFile string, requireClientCert bool) (*tls.Config, error) {
	if clientCAFile == "" {
		if requireClientCert {
			return nil, fmt.Errorf("tls-client-ca-file is required when tls-require-client-cert is enabled")
		}
		return nil, nil
	}

	caPEM, err := os.ReadFile(clientCAFile)
	if err != nil {
		return nil, fmt.Errorf("read client CA file: %w", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("failed to parse client CA PEM")
	}

	clientAuth := tls.VerifyClientCertIfGiven
	if requireClientCert {
		clientAuth = tls.RequireAndVerifyClientCert
	}

	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		ClientCAs:  pool,
		ClientAuth: clientAuth,
	}, nil
}

func buildHTTPServer(opts serverRuntimeOptions) (*http.Server, error) {
	srv := &http.Server{
		Addr:              opts.listenAddr,
		Handler:           opts.handler,
		ReadTimeout:       opts.readTimeout,
		ReadHeaderTimeout: opts.readHeaderTimeout,
		WriteTimeout:      opts.writeTimeout,
		IdleTimeout:       opts.idleTimeout,
		MaxHeaderBytes:    opts.maxHeaderBytes,
		ConnContext:       opts.connContext,
	}
	if opts.tlsClientCAFile != "" || opts.tlsRequireClientCert {
		tlsCfg, err := buildServerTLSConfig(opts.tlsClientCAFile, opts.tlsRequireClientCert)
		if err != nil {
			return nil, err
		}
		srv.TLSConfig = tlsCfg
	}
	return srv, nil
}

func runServerLoop(srv httpServer, opts serverLoopOptions, logger *slog.Logger, fatal func(string, ...any)) {
	if opts.tlsCertFile != "" && opts.tlsKeyFile != "" {
		logger.Info("proxy listening", "listen_address", opts.listenAddr, "backend_url", opts.backendURL, "tls", true)
		if err := srv.ListenAndServeTLS(opts.tlsCertFile, opts.tlsKeyFile); err != nil && err != http.ErrServerClosed {
			fatal("tls server failed", "error", err)
		}
		return
	}

	logger.Info("proxy listening", "listen_address", opts.listenAddr, "backend_url", opts.backendURL, "tls", false)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fatal("server failed", "error", err)
	}
}

func reloadDynamicConfig(p reloadableProxy, getenv func(string) string, logger *slog.Logger) {
	if v := getenv("TENANT_MAP"); v != "" {
		var newTenantMap map[string]proxy.TenantMapping
		if err := json.Unmarshal([]byte(v), &newTenantMap); err != nil {
			logger.Error("failed to reload tenant map", "error", err)
		} else {
			p.ReloadTenantMap(newTenantMap)
			logger.Info("reloaded tenant mappings", "count", len(newTenantMap))
		}
	}
	if v := getenv("FIELD_MAPPING"); v != "" {
		var newMappings []proxy.FieldMapping
		if err := json.Unmarshal([]byte(v), &newMappings); err != nil {
			logger.Error("failed to reload field mappings", "error", err)
		} else {
			p.ReloadFieldMappings(newMappings)
			logger.Info("reloaded field mappings", "count", len(newMappings))
		}
	}
}

func logProxyStartup(logger *slog.Logger, proxyCfg proxy.Config, peerSelf, peerDiscovery string, c, compat *cache.Cache) {
	if proxyCfg.TenantMap != nil {
		logger.Info("loaded tenant mappings", "count", len(proxyCfg.TenantMap))
	}
	if proxyCfg.FieldMappings != nil {
		logger.Info("loaded field mappings", "count", len(proxyCfg.FieldMappings))
	}
	if len(proxyCfg.TenantLimitsAllowPublish) > 0 || len(proxyCfg.TenantDefaultLimits) > 0 || len(proxyCfg.TenantLimits) > 0 {
		logger.Info(
			"loaded published tenant limits config",
			"allowlist_fields", len(proxyCfg.TenantLimitsAllowPublish),
			"default_overrides", len(proxyCfg.TenantDefaultLimits),
			"tenant_overrides", len(proxyCfg.TenantLimits),
		)
	}
	if proxyCfg.PatternsEnabled != nil && !*proxyCfg.PatternsEnabled {
		logger.Info("patterns endpoint disabled", "flag", "patterns-enabled")
	}
	if proxyCfg.LabelStyle == proxy.LabelStyleUnderscores {
		logger.Info("label translation enabled", "label_style", "underscores", "metadata_field_mode", string(proxyCfg.MetadataFieldMode))
	}
	if proxyCfg.LabelValuesIndexedCache {
		estimatedRAMPerLabelBytes := int64(proxyCfg.LabelValuesIndexMaxEntries) * 96
		estimatedDiskPerLabelBytes := int64(float64(estimatedRAMPerLabelBytes) * 0.6)
		logger.Info(
			"label values indexed cache enabled",
			"hot_limit", proxyCfg.LabelValuesHotLimit,
			"max_entries_per_label", proxyCfg.LabelValuesIndexMaxEntries,
			"persist_path", proxyCfg.LabelValuesIndexPersistPath,
			"persist_interval", proxyCfg.LabelValuesIndexPersistInterval,
			"startup_stale_threshold", proxyCfg.LabelValuesIndexStartupStale,
			"startup_peer_warm_timeout", proxyCfg.LabelValuesIndexPeerWarmTimeout,
			"estimated_ram_per_label_bytes", estimatedRAMPerLabelBytes,
			"estimated_disk_per_label_bytes", estimatedDiskPerLabelBytes,
		)
	}
	if strings.TrimSpace(proxyCfg.PatternsPersistPath) != "" {
		logger.Info(
			"patterns snapshot persistence enabled",
			"persist_path", proxyCfg.PatternsPersistPath,
			"persist_interval", proxyCfg.PatternsPersistInterval,
			"startup_stale_threshold", proxyCfg.PatternsStartupStale,
			"startup_peer_warm_timeout", proxyCfg.PatternsPeerWarmTimeout,
		)
	} else {
		patternsEnabled := proxyCfg.PatternsEnabled == nil || *proxyCfg.PatternsEnabled
		if patternsEnabled {
			logger.Info(
				"patterns snapshot persistence disabled",
				"hint", "set -patterns-persist-path and use StatefulSet + PVC for restart-safe patterns cache",
			)
		}
	}
	if len(proxyCfg.PatternsCustom) > 0 {
		logger.Info("custom drilldown patterns enabled", "count", len(proxyCfg.PatternsCustom))
	}
	if proxyCfg.DerivedFields != nil {
		logger.Info("loaded derived fields", "count", len(proxyCfg.DerivedFields))
	}
	if proxyCfg.QueryRangeWindowingEnabled {
		logger.Info(
			"query_range window cache enabled",
			"split_interval", proxyCfg.QueryRangeSplitInterval,
			"adaptive_parallel", proxyCfg.QueryRangeAdaptiveParallel,
			"parallel_min", proxyCfg.QueryRangeParallelMin,
			"parallel_max", proxyCfg.QueryRangeParallelMax,
			"freshness", proxyCfg.QueryRangeFreshness,
			"recent_cache_ttl", proxyCfg.QueryRangeRecentCacheTTL,
			"history_cache_ttl", proxyCfg.QueryRangeHistoryCacheTTL,
			"prefilter_index_stats", proxyCfg.QueryRangePrefilterIndexStats,
			"prefilter_min_windows", proxyCfg.QueryRangePrefilterMinWindows,
			"stream_aware_batching", proxyCfg.QueryRangeStreamAwareBatching,
			"expensive_hit_threshold", proxyCfg.QueryRangeExpensiveHitThreshold,
			"expensive_max_parallel", proxyCfg.QueryRangeExpensiveMaxParallel,
			"align_windows", proxyCfg.QueryRangeAlignWindows,
			"window_timeout", proxyCfg.QueryRangeWindowTimeout,
			"partial_responses", proxyCfg.QueryRangePartialResponses,
			"background_warm", proxyCfg.QueryRangeBackgroundWarm,
			"background_warm_max_windows", proxyCfg.QueryRangeBackgroundWarmMaxWindows,
		)
	}
	if proxyCfg.PeerCache != nil {
		c.SetL3(proxyCfg.PeerCache)
		logger.Info("peer cache enabled", "self", peerSelf, "discovery", peerDiscovery)
	}
	if compat != nil {
		logger.Info("compatibility edge cache active", "tier", "tier0")
	}
}

func logSystemMetricsStartup(logger *slog.Logger) {
	check := metrics.InspectSystemStartup()
	if check.GOOS != "linux" {
		logger.Info("system metrics startup check",
			"goos", check.GOOS,
			"proc_root", check.ProcRoot,
			"scope", check.Scope,
			"note", "linux /proc metric families are unavailable on this platform",
		)
		return
	}

	missing := check.MissingFamilies()
	if len(missing) == 0 {
		logger.Info("system metrics startup check passed",
			"proc_root", check.ProcRoot,
			"scope", check.Scope,
		)
	} else {
		logger.Warn("system metrics startup check incomplete",
			"proc_root", check.ProcRoot,
			"scope", check.Scope,
			"missing_families", strings.Join(missing, ","),
			"issues", strings.Join(check.IssueList(), "; "),
		)
	}
	for _, rec := range check.Recommendations {
		logger.Info("system metrics startup recommendation", "message", rec)
	}
}
