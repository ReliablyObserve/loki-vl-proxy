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
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/szibis/Loki-VL-proxy/internal/cache"
	"github.com/szibis/Loki-VL-proxy/internal/metrics"
	mw "github.com/szibis/Loki-VL-proxy/internal/middleware"
	"github.com/szibis/Loki-VL-proxy/internal/observability"
	"github.com/szibis/Loki-VL-proxy/internal/proxy"
)

var version = "dev"

type envConfig struct {
	listenAddr        string
	backendURL        string
	rulerBackendURL   string
	alertsBackendURL  string
	tenantMapJSON     string
	otlpEndpoint      string
	otlpCompression   string
	otlpHeaders       string
	labelStyle        string
	fieldMappingJSON  string
	metadataFieldMode string
	serviceName       string
	serviceNamespace  string
	serviceInstanceID string
	deploymentEnv     string
}

type proxyRuntimeConfig struct {
	backendURL               string
	rulerBackendURL          string
	alertsBackendURL         string
	cache                    *cache.Cache
	compatCache              *cache.Cache
	logLevel                 string
	tenantMapJSON            string
	maxLines                 int
	backendTimeout           time.Duration
	backendBasicAuth         string
	backendTLSSkip           bool
	forwardHeaders           string
	forwardCookies           string
	derivedFieldsJSON        string
	streamResponse           bool
	authEnabled              bool
	allowGlobalTenant        bool
	registerInstrumentation  *bool
	enablePprof              bool
	enableQueryAnalytics     bool
	adminAuthToken           string
	tailAllowedOrigins       string
	tailMode                 string
	metricsMaxTenants        int
	metricsMaxClients        int
	metricsTrustProxyHeaders bool
	labelStyle               string
	metadataFieldMode        string
	fieldMappingJSON         string
	streamFieldsCSV          string
	peerSelf                 string
	peerDiscovery            string
	peerDNS                  string
	peerStatic               string
	peerAuthToken            string
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
	writeTimeout         time.Duration
	idleTimeout          time.Duration
	maxHeaderBytes       int
	tlsClientCAFile      string
	tlsRequireClientCert bool
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
	cacheTTL              time.Duration
	cacheMax              int
	cacheMaxBytes         int
	compatCacheEnabled    bool
	compatCacheMaxPercent int
	diskCfg               cache.DiskCacheConfig
	proxyCfg              proxyRuntimeConfig
	otlpCfg               otlpRuntimeConfig
	maxBodyBytes          int64
	enableGzip            bool
	serverOpts            serverRuntimeOptions
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
	defaultCacheMaxBytes      = 256 * 1024 * 1024
	defaultCompatCachePercent = 10
	maxCompatCachePercent     = 50
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
	// Tenant mapping
	tenantMapJSON := fs.String("tenant-map", "", `JSON tenant mapping: {"org-name":{"account_id":"1","project_id":"0"}}`)

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

	// HTTP server hardening
	readTimeout := fs.Duration("http-read-timeout", 30*time.Second, "HTTP server read timeout")
	writeTimeout := fs.Duration("http-write-timeout", 120*time.Second, "HTTP server write timeout")
	idleTimeout := fs.Duration("http-idle-timeout", 120*time.Second, "HTTP server idle timeout")
	maxHeaderBytes := fs.Int("http-max-header-bytes", 1<<20, "HTTP max header size (default: 1MB)")
	maxBodyBytes := fs.Int64("http-max-body-bytes", 10<<20, "HTTP max request body size (default: 10MB)")

	// TLS server
	tlsCertFile := fs.String("tls-cert-file", "", "TLS certificate file for HTTPS server")
	tlsKeyFile := fs.String("tls-key-file", "", "TLS private key file for HTTPS server")
	tlsClientCAFile := fs.String("tls-client-ca-file", "", "CA certificate file used to verify HTTPS client certificates")
	tlsRequireClientCert := fs.Bool("tls-require-client-cert", false, "Require and verify HTTPS client certificates")

	// Response compression
	enableGzip := fs.Bool("response-gzip", true, "Enable gzip response compression for clients that accept it")

	// Grafana datasource compatibility
	maxLines := fs.Int("max-lines", 1000, "Default max lines per query")
	backendTimeout := fs.Duration("backend-timeout", 120*time.Second, "Timeout for non-streaming requests to the VictoriaLogs backend")
	backendBasicAuth := fs.String("backend-basic-auth", "", "Basic auth for VL backend (user:password)")
	backendTLSSkip := fs.Bool("backend-tls-skip-verify", false, "Skip TLS verification for VL backend")
	forwardHeaders := fs.String("forward-headers", "", "Comma-separated list of HTTP headers to forward to VL backend")
	forwardCookies := fs.String("forward-cookies", "", "Comma-separated list of cookie names to forward to VL backend")
	derivedFieldsJSON := fs.String("derived-fields", "", `JSON derived fields: [{"name":"traceID","matcherRegex":"trace_id=([a-f0-9]+)","url":"http://tempo/trace/${__value.raw}"}]`)
	streamResponse := fs.Bool("stream-response", false, "Stream log responses via chunked transfer encoding")

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
	allowGlobalTenant := fs.Bool("tenant.allow-global", false, `Allow X-Scope-OrgID "*" to bypass AccountID/ProjectID scoping and use the backend default tenant`)

	// Peer cache (fleet distribution)
	peerSelf := fs.String("peer-self", "", `This instance's address for peer cache (e.g., "10.0.0.1:3100"). Empty disables peer cache.`)
	peerDiscovery := fs.String("peer-discovery", "", `Peer discovery: "dns" (headless service) or "static" (comma-separated)`)
	peerDNS := fs.String("peer-dns", "", `Headless service DNS name for peer discovery (e.g., "proxy-headless.ns.svc.cluster.local")`)
	peerStatic := fs.String("peer-static", "", `Static peer list (e.g., "10.0.0.1:3100,10.0.0.2:3100")`)
	peerAuthToken := fs.String("peer-auth-token", "", "Shared token required on /_cache/get peer-cache requests when set")

	if err := fs.Parse(args); err != nil {
		return err
	}

	envCfg := applyEnvOverrides(envConfig{
		listenAddr:        *listenAddr,
		backendURL:        *backendURL,
		rulerBackendURL:   *rulerBackendURL,
		alertsBackendURL:  *alertsBackendURL,
		tenantMapJSON:     *tenantMapJSON,
		otlpEndpoint:      *otlpEndpoint,
		otlpCompression:   *otlpCompression,
		otlpHeaders:       *otlpHeaders,
		labelStyle:        *labelStyle,
		fieldMappingJSON:  *fieldMappingJSON,
		metadataFieldMode: *metadataFieldMode,
		serviceName:       *otelServiceName,
		serviceNamespace:  *otelServiceNamespace,
		serviceInstanceID: *otelServiceInstanceID,
		deploymentEnv:     *deploymentEnvironment,
	}, getenv)

	logger := buildLogger(logWriter, loggerConfig{
		level:                 *logLevel,
		serviceName:           envCfg.serviceName,
		serviceNamespace:      envCfg.serviceNamespace,
		serviceVersion:        version,
		serviceInstanceID:     envCfg.serviceInstanceID,
		deploymentEnvironment: envCfg.deploymentEnv,
	})
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
		},
		proxyCfg: proxyRuntimeConfig{
			backendURL:               envCfg.backendURL,
			rulerBackendURL:          envCfg.rulerBackendURL,
			alertsBackendURL:         envCfg.alertsBackendURL,
			logLevel:                 *logLevel,
			tenantMapJSON:            envCfg.tenantMapJSON,
			maxLines:                 *maxLines,
			backendTimeout:           *backendTimeout,
			backendBasicAuth:         *backendBasicAuth,
			backendTLSSkip:           *backendTLSSkip,
			forwardHeaders:           *forwardHeaders,
			forwardCookies:           *forwardCookies,
			derivedFieldsJSON:        *derivedFieldsJSON,
			streamResponse:           *streamResponse,
			authEnabled:              *authEnabled,
			allowGlobalTenant:        *allowGlobalTenant,
			registerInstrumentation:  registerInstrumentation,
			enablePprof:              *enablePprof,
			enableQueryAnalytics:     *enableQueryAnalytics,
			adminAuthToken:           *adminAuthToken,
			tailAllowedOrigins:       *tailAllowedOrigins,
			tailMode:                 *tailMode,
			metricsMaxTenants:        *metricsMaxTenants,
			metricsMaxClients:        *metricsMaxClients,
			metricsTrustProxyHeaders: *metricsTrustProxyHeaders,
			labelStyle:               envCfg.labelStyle,
			metadataFieldMode:        envCfg.metadataFieldMode,
			fieldMappingJSON:         envCfg.fieldMappingJSON,
			streamFieldsCSV:          *streamFieldsCSV,
			peerSelf:                 *peerSelf,
			peerDiscovery:            *peerDiscovery,
			peerDNS:                  *peerDNS,
			peerStatic:               *peerStatic,
			peerAuthToken:            *peerAuthToken,
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
		maxBodyBytes: *maxBodyBytes,
		enableGzip:   *enableGzip,
		serverOpts: serverRuntimeOptions{
			listenAddr:           envCfg.listenAddr,
			readTimeout:          *readTimeout,
			writeTimeout:         *writeTimeout,
			idleTimeout:          *idleTimeout,
			maxHeaderBytes:       *maxHeaderBytes,
			tlsClientCAFile:      *tlsClientCAFile,
			tlsRequireClientCert: *tlsRequireClientCert,
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
	p.Init()

	stopOTLP := startOTLPMetricsPusher(opts.otlpCfg, p.GetMetrics(), logger, newPusher)

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	serverOpts := opts.serverOpts
	serverOpts.handler = wrapHandler(mux, opts.maxBodyBytes, opts.enableGzip)
	srv, err := buildHTTPServer(serverOpts)
	if err != nil {
		stopOTLP()
		cacheCleanup()
		compatCleanup()
		return nil, fmt.Errorf("build http server: %w", err)
	}

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

func wrapHandler(next http.Handler, maxBodyBytes int64, enableGzip bool) http.Handler {
	handler := maxBodyHandler(maxBodyBytes, next)
	if enableGzip {
		handler = mw.GzipHandler(handler)
	}
	return handler
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
	if v := getenv("TENANT_MAP"); v != "" && cfg.tenantMapJSON == "" {
		cfg.tenantMapJSON = v
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
			SelfAddr:      cfg.peerSelf,
			DiscoveryType: cfg.peerDiscovery,
			DNSName:       cfg.peerDNS,
			StaticPeers:   cfg.peerStatic,
			Port:          3100,
		})
	}

	return proxy.Config{
		BackendURL:               cfg.backendURL,
		RulerBackendURL:          cfg.rulerBackendURL,
		AlertsBackendURL:         alertsBackendURL,
		Cache:                    cfg.cache,
		CompatCache:              cfg.compatCache,
		LogLevel:                 cfg.logLevel,
		TenantMap:                tenantMap,
		MaxLines:                 cfg.maxLines,
		BackendTimeout:           cfg.backendTimeout,
		BackendBasicAuth:         cfg.backendBasicAuth,
		BackendTLSSkip:           cfg.backendTLSSkip,
		ForwardHeaders:           parseCSV(cfg.forwardHeaders),
		ForwardCookies:           parseCSV(cfg.forwardCookies),
		DerivedFields:            derivedFields,
		StreamResponse:           cfg.streamResponse,
		AuthEnabled:              cfg.authEnabled,
		AllowGlobalTenant:        cfg.allowGlobalTenant,
		RegisterInstrumentation:  cfg.registerInstrumentation,
		EnablePprof:              cfg.enablePprof,
		EnableQueryAnalytics:     cfg.enableQueryAnalytics,
		AdminAuthToken:           cfg.adminAuthToken,
		TailAllowedOrigins:       parseCSV(cfg.tailAllowedOrigins),
		TailMode:                 tailMode,
		MetricsMaxTenants:        cfg.metricsMaxTenants,
		MetricsMaxClients:        cfg.metricsMaxClients,
		MetricsTrustProxyHeaders: cfg.metricsTrustProxyHeaders,
		LabelStyle:               ls,
		MetadataFieldMode:        mfm,
		FieldMappings:            fieldMappings,
		StreamFields:             parseCSV(cfg.streamFieldsCSV),
		PeerCache:                peerCache,
		PeerAuthToken:            cfg.peerAuthToken,
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
		Addr:           opts.listenAddr,
		Handler:        opts.handler,
		ReadTimeout:    opts.readTimeout,
		WriteTimeout:   opts.writeTimeout,
		IdleTimeout:    opts.idleTimeout,
		MaxHeaderBytes: opts.maxHeaderBytes,
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
	if proxyCfg.LabelStyle == proxy.LabelStyleUnderscores {
		logger.Info("label translation enabled", "label_style", "underscores", "metadata_field_mode", string(proxyCfg.MetadataFieldMode))
	}
	if proxyCfg.DerivedFields != nil {
		logger.Info("loaded derived fields", "count", len(proxyCfg.DerivedFields))
	}
	if proxyCfg.PeerCache != nil {
		c.SetL3(proxyCfg.PeerCache)
		logger.Info("peer cache enabled", "self", peerSelf, "discovery", peerDiscovery)
	}
	if compat != nil {
		logger.Info("compatibility edge cache active", "tier", "tier0")
	}
}
