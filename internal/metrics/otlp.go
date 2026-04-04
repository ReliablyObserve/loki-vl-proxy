package metrics

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/klauspost/compress/zstd"
)

// OTLPCompression defines the compression algorithm for OTLP push.
type OTLPCompression string

const (
	OTLPCompressionNone OTLPCompression = "none"
	OTLPCompressionGzip OTLPCompression = "gzip"
	OTLPCompressionZstd OTLPCompression = "zstd"
)

// OTLPPusher periodically exports proxy metrics to an OTLP HTTP endpoint.
// Uses lightweight JSON payloads — no heavy OTel SDK dependency.
type OTLPPusher struct {
	endpoint    string
	interval    time.Duration
	metrics     *Metrics
	client      *http.Client
	log         *slog.Logger
	cancel      context.CancelFunc
	headers     map[string]string
	compression OTLPCompression
}

// OTLPConfig configures the OTLP metrics pusher.
type OTLPConfig struct {
	Endpoint      string            // OTLP HTTP endpoint (e.g., http://otel-collector:4318/v1/metrics)
	Interval      time.Duration     // Push interval (default: 30s)
	Headers       map[string]string // Extra headers (e.g., auth tokens)
	Timeout       time.Duration     // HTTP request timeout (default: 10s)
	Compression   OTLPCompression   // Compression: "none", "gzip", "zstd" (default: "none")
	TLSSkipVerify bool              // Skip TLS verification (for self-signed certs)
}

// NewOTLPPusher creates a new OTLP metrics pusher.
func NewOTLPPusher(cfg OTLPConfig, m *Metrics) *OTLPPusher {
	if cfg.Interval == 0 {
		cfg.Interval = 30 * time.Second
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	if cfg.TLSSkipVerify {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	compression := cfg.Compression
	if compression == "" {
		compression = OTLPCompressionNone
	}

	return &OTLPPusher{
		endpoint:    cfg.Endpoint,
		interval:    cfg.Interval,
		metrics:     m,
		client:      &http.Client{Timeout: timeout, Transport: transport},
		log:         logger,
		headers:     cfg.Headers,
		compression: compression,
	}
}

// Start begins periodic metric export in a background goroutine.
func (p *OTLPPusher) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	go func() {
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := p.push(ctx); err != nil {
					p.log.Error("otlp push failed", "error", err)
				}
			}
		}
	}()
}

// Stop stops the periodic pusher.
func (p *OTLPPusher) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
}

// push sends current metrics as OTLP JSON.
func (p *OTLPPusher) push(ctx context.Context) error {
	payload := p.buildPayload()
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal otlp payload: %w", err)
	}

	var reqBody *bytes.Reader
	contentEncoding := ""

	switch p.compression {
	case OTLPCompressionGzip:
		var buf bytes.Buffer
		w := gzip.NewWriter(&buf)
		if _, err := w.Write(body); err != nil {
			return fmt.Errorf("gzip compress: %w", err)
		}
		if err := w.Close(); err != nil {
			return fmt.Errorf("gzip close: %w", err)
		}
		reqBody = bytes.NewReader(buf.Bytes())
		contentEncoding = "gzip"
	case OTLPCompressionZstd:
		var buf bytes.Buffer
		w, err := zstd.NewWriter(&buf)
		if err != nil {
			return fmt.Errorf("zstd writer: %w", err)
		}
		if _, err := w.Write(body); err != nil {
			return fmt.Errorf("zstd compress: %w", err)
		}
		if err := w.Close(); err != nil {
			return fmt.Errorf("zstd close: %w", err)
		}
		reqBody = bytes.NewReader(buf.Bytes())
		contentEncoding = "zstd"
	default:
		reqBody = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", p.endpoint, reqBody)
	if err != nil {
		return fmt.Errorf("create otlp request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}
	for k, v := range p.headers {
		req.Header.Set(k, v)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("otlp request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("otlp endpoint returned %d", resp.StatusCode)
	}
	return nil
}

// buildPayload constructs an OTLP ExportMetricsServiceRequest as JSON.
func (p *OTLPPusher) buildPayload() map[string]interface{} {
	now := time.Now().UnixNano()

	dataPoints := []map[string]interface{}{
		p.counterDP("loki_vl_proxy_cache_hits_total", p.metrics.cacheHits.Load(), now),
		p.counterDP("loki_vl_proxy_cache_misses_total", p.metrics.cacheMisses.Load(), now),
		p.counterDP("loki_vl_proxy_translations_total", p.metrics.translationsTotal.Load(), now),
		p.counterDP("loki_vl_proxy_translation_errors_total", p.metrics.translationErrors.Load(), now),
	}

	// Add per-endpoint request counters
	p.metrics.mu.RLock()
	for key, counter := range p.metrics.requestsTotal {
		dataPoints = append(dataPoints,
			p.counterDP("loki_vl_proxy_requests_total", counter.Load(), now,
				attr("endpoint_status", key)))
	}
	p.metrics.mu.RUnlock()

	gaugePoints := []map[string]interface{}{
		p.gaugeDP("loki_vl_proxy_uptime_seconds", time.Since(p.metrics.startTime).Seconds(), now),
		p.gaugeDP("loki_vl_proxy_active_requests", float64(p.metrics.activeRequests.Load()), now),
	}

	metrics := make([]map[string]interface{}, 0, len(dataPoints)+len(gaugePoints))
	metrics = append(metrics, dataPoints...)
	metrics = append(metrics, gaugePoints...)

	return map[string]interface{}{
		"resourceMetrics": []map[string]interface{}{
			{
				"resource": map[string]interface{}{
					"attributes": []map[string]interface{}{
						{"key": "service.name", "value": map[string]interface{}{"stringValue": "loki-vl-proxy"}},
					},
				},
				"scopeMetrics": []map[string]interface{}{
					{
						"scope": map[string]interface{}{
							"name":    "loki-vl-proxy",
							"version": "0.8.0",
						},
						"metrics": metrics,
					},
				},
			},
		},
	}
}

func (p *OTLPPusher) counterDP(name string, value int64, timeUnixNano int64, attrs ...map[string]interface{}) map[string]interface{} {
	dp := map[string]interface{}{
		"asInt":        value,
		"timeUnixNano": fmt.Sprintf("%d", timeUnixNano),
	}
	if len(attrs) > 0 {
		dp["attributes"] = attrs
	}
	return map[string]interface{}{
		"name": name,
		"sum": map[string]interface{}{
			"dataPoints":             []map[string]interface{}{dp},
			"aggregationTemporality": 2, // CUMULATIVE
			"isMonotonic":            true,
		},
	}
}

func (p *OTLPPusher) gaugeDP(name string, value float64, timeUnixNano int64) map[string]interface{} {
	return map[string]interface{}{
		"name": name,
		"gauge": map[string]interface{}{
			"dataPoints": []map[string]interface{}{
				{
					"asDouble":     value,
					"timeUnixNano": fmt.Sprintf("%d", timeUnixNano),
				},
			},
		},
	}
}

func attr(key, value string) map[string]interface{} {
	return map[string]interface{}{
		"key":   key,
		"value": map[string]interface{}{"stringValue": value},
	}
}
