package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
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

func main() {
	// Server flags
	listenAddr := flag.String("listen", ":3100", "Address to listen on (Loki-compatible frontend)")
	backendURL := flag.String("backend", "http://localhost:9428", "VictoriaLogs backend URL")
	logLevel := flag.String("log-level", "info", "Log level: debug, info, warn, error")

	// Cache flags
	cacheTTL := flag.Duration("cache-ttl", 60*time.Second, "Cache TTL for label/metadata queries")
	cacheMax := flag.Int("cache-max", 10000, "Maximum cache entries")

	// Disk cache flags
	diskCachePath := flag.String("disk-cache-path", "", "Path to L2 disk cache (bbolt). Empty disables.")
	diskCacheCompress := flag.Bool("disk-cache-compress", true, "Gzip compression for disk cache")
	diskCacheFlushSize := flag.Int("disk-cache-flush-size", 100, "Flush write buffer after N entries")
	diskCacheFlushInterval := flag.Duration("disk-cache-flush-interval", 5*time.Second, "Write buffer flush interval")
	// Tenant mapping
	tenantMapJSON := flag.String("tenant-map", "", `JSON tenant mapping: {"org-name":{"account_id":"1","project_id":"0"}}`)

	// OTLP telemetry flags
	otlpEndpoint := flag.String("otlp-endpoint", "", "OTLP HTTP endpoint (e.g., http://otel-collector:4318/v1/metrics)")
	otlpInterval := flag.Duration("otlp-interval", 30*time.Second, "OTLP push interval")
	otlpCompression := flag.String("otlp-compression", "none", "OTLP compression: none, gzip, zstd")
	otlpTimeout := flag.Duration("otlp-timeout", 10*time.Second, "OTLP HTTP request timeout")
	otlpTLSSkipVerify := flag.Bool("otlp-tls-skip-verify", false, "Skip TLS verification for OTLP endpoint")
	otlpHeaders := flag.String("otlp-headers", "", "Comma-separated OTLP HTTP headers in key=value form")
	otelServiceName := flag.String("otel-service-name", "loki-vl-proxy", "OpenTelemetry service.name for logs and OTLP metrics")
	otelServiceNamespace := flag.String("otel-service-namespace", "", "OpenTelemetry service.namespace for logs and OTLP metrics")
	otelServiceInstanceID := flag.String("otel-service-instance-id", "", "OpenTelemetry service.instance.id for logs and OTLP metrics")
	deploymentEnvironment := flag.String("deployment-environment", "", "OpenTelemetry deployment.environment.name for logs and OTLP metrics")

	// HTTP server hardening
	readTimeout := flag.Duration("http-read-timeout", 30*time.Second, "HTTP server read timeout")
	writeTimeout := flag.Duration("http-write-timeout", 120*time.Second, "HTTP server write timeout")
	idleTimeout := flag.Duration("http-idle-timeout", 120*time.Second, "HTTP server idle timeout")
	maxHeaderBytes := flag.Int("http-max-header-bytes", 1<<20, "HTTP max header size (default: 1MB)")
	maxBodyBytes := flag.Int64("http-max-body-bytes", 10<<20, "HTTP max request body size (default: 10MB)")

	// TLS server
	tlsCertFile := flag.String("tls-cert-file", "", "TLS certificate file for HTTPS server")
	tlsKeyFile := flag.String("tls-key-file", "", "TLS private key file for HTTPS server")
	tlsClientCAFile := flag.String("tls-client-ca-file", "", "CA certificate file used to verify HTTPS client certificates")
	tlsRequireClientCert := flag.Bool("tls-require-client-cert", false, "Require and verify HTTPS client certificates")

	// Response compression
	enableGzip := flag.Bool("response-gzip", true, "Enable gzip response compression for clients that accept it")

	// Grafana datasource compatibility
	maxLines := flag.Int("max-lines", 1000, "Default max lines per query")
	backendTimeout := flag.Duration("backend-timeout", 120*time.Second, "Timeout for non-streaming requests to the VictoriaLogs backend")
	backendBasicAuth := flag.String("backend-basic-auth", "", "Basic auth for VL backend (user:password)")
	backendTLSSkip := flag.Bool("backend-tls-skip-verify", false, "Skip TLS verification for VL backend")
	forwardHeaders := flag.String("forward-headers", "", "Comma-separated list of HTTP headers to forward to VL backend")
	forwardCookies := flag.String("forward-cookies", "", "Comma-separated list of cookie names to forward to VL backend")
	derivedFieldsJSON := flag.String("derived-fields", "", `JSON derived fields: [{"name":"traceID","matcherRegex":"trace_id=([a-f0-9]+)","url":"http://tempo/trace/${__value.raw}"}]`)
	streamResponse := flag.Bool("stream-response", false, "Stream log responses via chunked transfer encoding")

	// Loki-style auth / instrumentation controls
	authEnabled := flag.Bool("auth.enabled", false, "Require X-Scope-OrgID on query requests. When false, requests without a tenant header use the backend default tenant.")
	registerInstrumentation := flag.Bool("server.register-instrumentation", true, "Register instrumentation handlers such as /metrics")
	enablePprof := flag.Bool("server.enable-pprof", false, "Expose /debug/pprof/* handlers")
	enableQueryAnalytics := flag.Bool("server.enable-query-analytics", false, "Expose /debug/queries query analytics")
	adminAuthToken := flag.String("server.admin-auth-token", "", "Bearer token required for admin/debug endpoints when set")
	tailAllowedOrigins := flag.String("tail.allowed-origins", "", "Comma-separated WebSocket Origin allowlist for /loki/api/v1/tail. Empty denies browser origins.")
	metricsMaxTenants := flag.Int("metrics.max-tenants", 256, "Maximum unique tenant labels retained in exported metrics before collapsing into __overflow__")
	metricsMaxClients := flag.Int("metrics.max-clients", 256, "Maximum unique client labels retained in exported metrics before collapsing into __overflow__")
	metricsTrustProxyHeaders := flag.Bool("metrics.trust-proxy-headers", false, "Trust X-Grafana-User and X-Forwarded-For when deriving per-client metrics labels")

	// Label translation
	labelStyle := flag.String("label-style", "passthrough", `Label name translation mode:
  passthrough  - no translation, pass VL field names as-is (use when VL stores underscores)
  underscores  - convert dots to underscores (use when VL stores OTel-style dotted names like service.name)`)
	metadataFieldMode := flag.String("metadata-field-mode", "hybrid", `Field exposure mode for detected_fields and structured metadata:
  native      - expose VictoriaLogs field names as-is
  translated  - expose only Loki-compatible translated aliases
  hybrid      - expose both native VL field names and translated aliases when they differ`)
	fieldMappingJSON := flag.String("field-mapping", "", `JSON custom field mappings: [{"vl_field":"service.name","loki_label":"service_name"}]`)
	streamFieldsCSV := flag.String("stream-fields", "", `Comma-separated VL _stream_fields labels for stream selector optimization (e.g., "app,env,namespace")`)
	allowGlobalTenant := flag.Bool("tenant.allow-global", false, `Allow X-Scope-OrgID "*" or "0" to bypass AccountID/ProjectID scoping and use the backend default tenant`)

	// Peer cache (fleet distribution)
	peerSelf := flag.String("peer-self", "", `This instance's address for peer cache (e.g., "10.0.0.1:3100"). Empty disables peer cache.`)
	peerDiscovery := flag.String("peer-discovery", "", `Peer discovery: "dns" (headless service) or "static" (comma-separated)`)
	peerDNS := flag.String("peer-dns", "", `Headless service DNS name for peer discovery (e.g., "proxy-headless.ns.svc.cluster.local")`)
	peerStatic := flag.String("peer-static", "", `Static peer list (e.g., "10.0.0.1:3100,10.0.0.2:3100")`)
	peerAuthToken := flag.String("peer-auth-token", "", "Shared token required on /_cache/get peer-cache requests when set")

	flag.Parse()

	envCfg := applyEnvOverrides(envConfig{
		listenAddr:        *listenAddr,
		backendURL:        *backendURL,
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
	}, os.Getenv)
	*listenAddr = envCfg.listenAddr
	*backendURL = envCfg.backendURL
	*tenantMapJSON = envCfg.tenantMapJSON
	*otlpEndpoint = envCfg.otlpEndpoint
	*otlpCompression = envCfg.otlpCompression
	*otlpHeaders = envCfg.otlpHeaders
	*labelStyle = envCfg.labelStyle
	*fieldMappingJSON = envCfg.fieldMappingJSON
	*metadataFieldMode = envCfg.metadataFieldMode
	*otelServiceName = envCfg.serviceName
	*otelServiceNamespace = envCfg.serviceNamespace
	*otelServiceInstanceID = envCfg.serviceInstanceID
	*deploymentEnvironment = envCfg.deploymentEnv

	logger := observability.NewLogger(os.Stdout, observability.LoggerConfig{
		Level:                 *logLevel,
		ServiceName:           *otelServiceName,
		ServiceNamespace:      *otelServiceNamespace,
		ServiceVersion:        version,
		ServiceInstanceID:     *otelServiceInstanceID,
		DeploymentEnvironment: *deploymentEnvironment,
	})
	slog.SetDefault(logger)
	fatal := func(msg string, args ...any) {
		logger.Error(msg, args...)
		os.Exit(1)
	}

	// Parse tenant map
	tenantMap, err := parseTenantMapJSON(*tenantMapJSON)
	if err != nil {
		fatal("failed to parse tenant map json", "error", err)
	}
	if tenantMap != nil {
		logger.Info("loaded tenant mappings", "count", len(tenantMap))
	}

	// L1 in-memory cache
	c := cache.New(*cacheTTL, *cacheMax)

	// L2 disk cache (compression + write-back buffer)
	if *diskCachePath != "" {
		dc, err := cache.NewDiskCache(cache.DiskCacheConfig{
			Path:          *diskCachePath,
			Compression:   *diskCacheCompress,
			FlushSize:     *diskCacheFlushSize,
			FlushInterval: *diskCacheFlushInterval,
		})
		if err != nil {
			fatal("failed to open disk cache", "error", err)
		}
		defer func() { _ = dc.Close() }()
		c.SetL2(dc)
		logger.Info("disk cache enabled",
			"path", *diskCachePath,
			"compress", *diskCacheCompress,
			"flush_size", *diskCacheFlushSize,
			"flush_interval", diskCacheFlushInterval.String(),
		)
	}

	// Parse forward headers
	var fwdHeaders []string
	if *forwardHeaders != "" {
		for _, h := range strings.Split(*forwardHeaders, ",") {
			h = strings.TrimSpace(h)
			if h != "" {
				fwdHeaders = append(fwdHeaders, h)
			}
		}
	}

	// Parse field mappings
	fieldMappings, err := parseFieldMappingsJSON(*fieldMappingJSON)
	if err != nil {
		fatal("failed to parse field mapping json", "error", err)
	}
	if fieldMappings != nil {
		logger.Info("loaded field mappings", "count", len(fieldMappings))
	}

	// Validate label style
	ls, mfm, err := parseLabelModes(*labelStyle, *metadataFieldMode)
	if err != nil {
		fatal("invalid label mode configuration", "error", err)
	}
	if ls == proxy.LabelStyleUnderscores {
		logger.Info("label translation enabled", "label_style", "underscores", "metadata_field_mode", string(mfm))
	}

	// Parse derived fields
	derivedFields, err := parseDerivedFieldsJSON(*derivedFieldsJSON)
	if err != nil {
		fatal("failed to parse derived fields json", "error", err)
	}
	if derivedFields != nil {
		logger.Info("loaded derived fields", "count", len(derivedFields))
	}

	// Create peer cache if configured
	var peerCache *cache.PeerCache
	if *peerSelf != "" && *peerDiscovery != "" {
		peerCache = cache.NewPeerCache(cache.PeerConfig{
			SelfAddr:      *peerSelf,
			DiscoveryType: *peerDiscovery,
			DNSName:       *peerDNS,
			StaticPeers:   *peerStatic,
			Port:          3100,
		})
		c.SetL3(peerCache)
		logger.Info("peer cache enabled", "self", *peerSelf, "discovery", *peerDiscovery)
	}

	// Create proxy
	p, err := proxy.New(proxy.Config{
		BackendURL:               *backendURL,
		Cache:                    c,
		LogLevel:                 *logLevel,
		TenantMap:                tenantMap,
		MaxLines:                 *maxLines,
		BackendTimeout:           *backendTimeout,
		BackendBasicAuth:         *backendBasicAuth,
		BackendTLSSkip:           *backendTLSSkip,
		ForwardHeaders:           fwdHeaders,
		ForwardCookies:           parseCSV(*forwardCookies),
		DerivedFields:            derivedFields,
		StreamResponse:           *streamResponse,
		AuthEnabled:              *authEnabled,
		AllowGlobalTenant:        *allowGlobalTenant,
		RegisterInstrumentation:  registerInstrumentation,
		EnablePprof:              *enablePprof,
		EnableQueryAnalytics:     *enableQueryAnalytics,
		AdminAuthToken:           *adminAuthToken,
		TailAllowedOrigins:       parseCSV(*tailAllowedOrigins),
		MetricsMaxTenants:        *metricsMaxTenants,
		MetricsMaxClients:        *metricsMaxClients,
		MetricsTrustProxyHeaders: *metricsTrustProxyHeaders,
		LabelStyle:               ls,
		MetadataFieldMode:        mfm,
		FieldMappings:            fieldMappings,
		StreamFields:             parseCSV(*streamFieldsCSV),
		PeerCache:                peerCache,
		PeerAuthToken:            *peerAuthToken,
	})
	if err != nil {
		fatal("failed to create proxy", "error", err)
	}
	p.Init()

	// Start OTLP telemetry push
	if *otlpEndpoint != "" {
		pusher := metrics.NewOTLPPusher(metrics.OTLPConfig{
			Endpoint:              *otlpEndpoint,
			Interval:              *otlpInterval,
			Headers:               parseHeaderMapCSV(*otlpHeaders),
			Compression:           metrics.OTLPCompression(*otlpCompression),
			Timeout:               *otlpTimeout,
			TLSSkipVerify:         *otlpTLSSkipVerify,
			ServiceName:           *otelServiceName,
			ServiceNamespace:      *otelServiceNamespace,
			ServiceVersion:        version,
			ServiceInstanceID:     *otelServiceInstanceID,
			DeploymentEnvironment: *deploymentEnvironment,
		}, p.GetMetrics())
		pusher.Start()
		defer pusher.Stop()
		logger.Info("otlp metrics push enabled",
			"endpoint", *otlpEndpoint,
			"interval", otlpInterval.String(),
			"compression", *otlpCompression,
		)
	}

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	// Middleware chain: body limit → gzip compression
	handler := maxBodyHandler(*maxBodyBytes, mux)
	if *enableGzip {
		handler = mw.GzipHandler(handler)
	}

	// Hardened HTTP server with timeouts
	srv := &http.Server{
		Addr:           *listenAddr,
		Handler:        handler,
		ReadTimeout:    *readTimeout,
		WriteTimeout:   *writeTimeout,
		IdleTimeout:    *idleTimeout,
		MaxHeaderBytes: *maxHeaderBytes,
	}
	if *tlsClientCAFile != "" || *tlsRequireClientCert {
		tlsCfg, err := buildServerTLSConfig(*tlsClientCAFile, *tlsRequireClientCert)
		if err != nil {
			fatal("failed to configure server tls client authentication", "error", err)
		}
		srv.TLSConfig = tlsCfg
	}

	// SIGHUP config reload for tenant-map and field-mapping
	reloadCh := make(chan os.Signal, 1)
	signal.Notify(reloadCh, syscall.SIGHUP)
	go func() {
		for range reloadCh {
			logger.Info("received sighup, reloading configuration")
			if v := os.Getenv("TENANT_MAP"); v != "" {
				var newTenantMap map[string]proxy.TenantMapping
				if err := json.Unmarshal([]byte(v), &newTenantMap); err != nil {
					logger.Error("failed to reload tenant map", "error", err)
				} else {
					p.ReloadTenantMap(newTenantMap)
					logger.Info("reloaded tenant mappings", "count", len(newTenantMap))
				}
			}
			if v := os.Getenv("FIELD_MAPPING"); v != "" {
				var newMappings []proxy.FieldMapping
				if err := json.Unmarshal([]byte(v), &newMappings); err != nil {
					logger.Error("failed to reload field mappings", "error", err)
				} else {
					p.ReloadFieldMappings(newMappings)
					logger.Info("reloaded field mappings", "count", len(newMappings))
				}
			}
		}
	}()

	// Graceful shutdown on SIGTERM/SIGINT
	shutdownCh := make(chan os.Signal, 1)
	signal.Notify(shutdownCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		if *tlsCertFile != "" && *tlsKeyFile != "" {
			logger.Info("proxy listening", "listen_address", *listenAddr, "backend_url", *backendURL, "tls", true)
			if err := srv.ListenAndServeTLS(*tlsCertFile, *tlsKeyFile); err != nil && err != http.ErrServerClosed {
				fatal("tls server failed", "error", err)
			}
		} else {
			logger.Info("proxy listening", "listen_address", *listenAddr, "backend_url", *backendURL, "tls", false)
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				fatal("server failed", "error", err)
			}
		}
	}()

	sig := <-shutdownCh
	logger.Info("shutdown requested", "signal", sig.String())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("http shutdown error", "error", err)
	}
	logger.Info("shutdown complete")
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
