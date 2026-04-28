// healthcheck performs an HTTP GET against the proxy /ready endpoint.
// It is compiled into the container image so that Docker health checks work
// with the distroless base image (which has no wget/curl/shell).
//
// Usage (docker-compose):
//
//	healthcheck:
//	  test: ["CMD", "/usr/local/bin/healthcheck"]
//
// The port defaults to 3100; override with HEALTH_PORT env var.
package main

import (
	"net/http"
	"os"
)

func main() {
	port := os.Getenv("HEALTH_PORT")
	if port == "" {
		port = "3100"
	}
	resp, err := http.Get("http://localhost:" + port + "/ready") //nolint:noctx
	if err != nil {
		os.Exit(1)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		os.Exit(1)
	}
}
