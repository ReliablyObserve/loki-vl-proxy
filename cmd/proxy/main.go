package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/szibis/Loki-VL-proxy/internal/cache"
	"github.com/szibis/Loki-VL-proxy/internal/proxy"
)

func main() {
	listenAddr := flag.String("listen", ":3100", "Address to listen on (Loki-compatible frontend)")
	backendURL := flag.String("backend", "http://localhost:9428", "VictoriaLogs backend URL")
	cacheTTL := flag.Duration("cache-ttl", 60*time.Second, "Cache TTL for label/metadata queries")
	cacheMax := flag.Int("cache-max", 10000, "Maximum cache entries")
	logLevel := flag.String("log-level", "info", "Log level: debug, info, warn, error")
	flag.Parse()

	// Allow env overrides
	if v := os.Getenv("LISTEN_ADDR"); v != "" {
		*listenAddr = v
	}
	if v := os.Getenv("VL_BACKEND_URL"); v != "" {
		*backendURL = v
	}

	c := cache.New(*cacheTTL, *cacheMax)

	p, err := proxy.New(proxy.Config{
		BackendURL: *backendURL,
		Cache:      c,
		LogLevel:   *logLevel,
	})
	if err != nil {
		log.Fatalf("Failed to create proxy: %v", err)
	}

	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	log.Printf("Loki-VL-proxy listening on %s, backend: %s", *listenAddr, *backendURL)
	if err := http.ListenAndServe(*listenAddr, mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
