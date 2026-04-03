package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/szibis/Loki-VL-proxy/internal/cache"
	"github.com/szibis/Loki-VL-proxy/internal/metrics"
	"github.com/szibis/Loki-VL-proxy/internal/proxy"
)

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
	diskCacheEncryptionKey := flag.String("disk-cache-encryption-key", "", "AES-256 key (32 bytes hex) for disk cache encryption at rest")

	// Tenant mapping
	tenantMapJSON := flag.String("tenant-map", "", `JSON tenant mapping: {"org-name":{"account_id":"1","project_id":"0"}}`)

	// OTLP telemetry flags
	otlpEndpoint := flag.String("otlp-endpoint", "", "OTLP HTTP endpoint (e.g., http://otel-collector:4318/v1/metrics)")
	otlpInterval := flag.Duration("otlp-interval", 30*time.Second, "OTLP push interval")
	otlpCompression := flag.String("otlp-compression", "none", "OTLP compression: none, gzip, zstd")
	otlpTimeout := flag.Duration("otlp-timeout", 10*time.Second, "OTLP HTTP request timeout")
	otlpTLSSkipVerify := flag.Bool("otlp-tls-skip-verify", false, "Skip TLS verification for OTLP endpoint")

	// HTTP server hardening
	readTimeout := flag.Duration("http-read-timeout", 30*time.Second, "HTTP server read timeout")
	writeTimeout := flag.Duration("http-write-timeout", 120*time.Second, "HTTP server write timeout")
	idleTimeout := flag.Duration("http-idle-timeout", 120*time.Second, "HTTP server idle timeout")
	maxHeaderBytes := flag.Int("http-max-header-bytes", 1<<20, "HTTP max header size (default: 1MB)")
	maxBodyBytes := flag.Int64("http-max-body-bytes", 10<<20, "HTTP max request body size (default: 10MB)")

	flag.Parse()

	// Environment variable overrides
	if v := os.Getenv("LISTEN_ADDR"); v != "" {
		*listenAddr = v
	}
	if v := os.Getenv("VL_BACKEND_URL"); v != "" {
		*backendURL = v
	}
	if v := os.Getenv("TENANT_MAP"); v != "" && *tenantMapJSON == "" {
		*tenantMapJSON = v
	}
	if v := os.Getenv("OTLP_ENDPOINT"); v != "" && *otlpEndpoint == "" {
		*otlpEndpoint = v
	}
	if v := os.Getenv("OTLP_COMPRESSION"); v != "" && *otlpCompression == "none" {
		*otlpCompression = v
	}

	// Parse tenant map
	var tenantMap map[string]proxy.TenantMapping
	if *tenantMapJSON != "" {
		if err := json.Unmarshal([]byte(*tenantMapJSON), &tenantMap); err != nil {
			log.Fatalf("Failed to parse -tenant-map JSON: %v", err)
		}
		log.Printf("Loaded %d tenant mappings", len(tenantMap))
	}

	// L1 in-memory cache
	c := cache.New(*cacheTTL, *cacheMax)

	// L2 disk cache
	if *diskCachePath != "" {
		var encKey []byte
		if *diskCacheEncryptionKey != "" {
			encKey = []byte(*diskCacheEncryptionKey)
			if len(encKey) != 32 {
				log.Fatalf("disk-cache-encryption-key must be exactly 32 bytes, got %d", len(encKey))
			}
		}
		dc, err := cache.NewDiskCache(cache.DiskCacheConfig{
			Path:          *diskCachePath,
			Compression:   *diskCacheCompress,
			FlushSize:     *diskCacheFlushSize,
			FlushInterval: *diskCacheFlushInterval,
			EncryptionKey: encKey,
		})
		if err != nil {
			log.Fatalf("Failed to open disk cache: %v", err)
		}
		defer dc.Close()
		c.SetL2(dc)
		log.Printf("L2 disk cache enabled at %s (compress=%v, encrypted=%v, flush=%d/%s)",
			*diskCachePath, *diskCacheCompress, len(encKey) > 0, *diskCacheFlushSize, *diskCacheFlushInterval)
	}

	// Create proxy
	p, err := proxy.New(proxy.Config{
		BackendURL: *backendURL,
		Cache:      c,
		LogLevel:   *logLevel,
		TenantMap:  tenantMap,
	})
	if err != nil {
		log.Fatalf("Failed to create proxy: %v", err)
	}

	// Start OTLP telemetry push
	if *otlpEndpoint != "" {
		pusher := metrics.NewOTLPPusher(metrics.OTLPConfig{
			Endpoint:      *otlpEndpoint,
			Interval:      *otlpInterval,
			Compression:   metrics.OTLPCompression(*otlpCompression),
			Timeout:       *otlpTimeout,
			TLSSkipVerify: *otlpTLSSkipVerify,
		}, p.GetMetrics())
		pusher.Start()
		defer pusher.Stop()
		log.Printf("OTLP push → %s (every %s, compression=%s)", *otlpEndpoint, *otlpInterval, *otlpCompression)
	}

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	// Wrap with request body size limiter
	handler := maxBodyHandler(*maxBodyBytes, mux)

	// Hardened HTTP server with timeouts
	srv := &http.Server{
		Addr:           *listenAddr,
		Handler:        handler,
		ReadTimeout:    *readTimeout,
		WriteTimeout:   *writeTimeout,
		IdleTimeout:    *idleTimeout,
		MaxHeaderBytes: *maxHeaderBytes,
	}

	log.Printf("Loki-VL-proxy listening on %s, backend: %s", *listenAddr, *backendURL)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("Server failed: %v", err)
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
