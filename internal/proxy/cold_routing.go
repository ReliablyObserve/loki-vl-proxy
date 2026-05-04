package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

type ColdBackendConfig struct {
	URL             string        `yaml:"url"`
	Boundary        time.Duration `yaml:"boundary"`
	Overlap         time.Duration `yaml:"overlap"`
	Enabled         bool          `yaml:"enabled"`
	ManifestRefresh time.Duration `yaml:"manifest_refresh"`
	Timeout         time.Duration `yaml:"timeout"`
}

type ManifestRange struct {
	MinTime    int64  `json:"minTime"`
	MaxTime    int64  `json:"maxTime"`
	MinDate    string `json:"minDate"`
	MaxDate    string `json:"maxDate"`
	TotalFiles int    `json:"totalFiles"`
	TotalBytes int64  `json:"totalBytes"`
}

type ColdRouter struct {
	coldBackend     *url.URL
	client          *http.Client
	boundary        time.Duration
	overlap         time.Duration
	manifestRefresh time.Duration
	logger          *slog.Logger

	mu       sync.RWMutex
	manifest *ManifestRange
}

func NewColdRouter(cfg ColdBackendConfig, logger *slog.Logger) (*ColdRouter, error) {
	if !cfg.Enabled || cfg.URL == "" {
		return nil, nil
	}

	u, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid cold backend URL: %w", err)
	}

	boundary := cfg.Boundary
	if boundary <= 0 {
		boundary = 7 * 24 * time.Hour
	}
	overlap := cfg.Overlap
	if overlap <= 0 {
		overlap = time.Hour
	}
	refresh := cfg.ManifestRefresh
	if refresh <= 0 {
		refresh = 5 * time.Minute
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 32
	transport.ResponseHeaderTimeout = timeout

	cr := &ColdRouter{
		coldBackend:     u,
		client:          &http.Client{Transport: transport, Timeout: timeout},
		boundary:        boundary,
		overlap:         overlap,
		manifestRefresh: refresh,
		logger:          logger.With("component", "cold-router"),
	}

	return cr, nil
}

func (cr *ColdRouter) Start(ctx context.Context) {
	go cr.refreshLoop(ctx)
}

func (cr *ColdRouter) refreshLoop(ctx context.Context) {
	cr.refreshManifest(ctx)

	ticker := time.NewTicker(cr.manifestRefresh)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cr.refreshManifest(ctx)
		}
	}
}

func (cr *ColdRouter) refreshManifest(ctx context.Context) {
	u := *cr.coldBackend
	u.Path = "/manifest/range"

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		cr.logger.Error("cold manifest request build failed", "error", err)
		return
	}

	resp, err := cr.client.Do(req)
	if err != nil {
		cr.logger.Warn("cold manifest refresh failed", "error", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		cr.logger.Warn("cold manifest bad status", "status", resp.StatusCode, "body", string(body))
		return
	}

	var mr ManifestRange
	if err := json.NewDecoder(resp.Body).Decode(&mr); err != nil {
		cr.logger.Warn("cold manifest decode failed", "error", err)
		return
	}

	cr.mu.Lock()
	cr.manifest = &mr
	cr.mu.Unlock()

	cr.logger.Debug("cold manifest refreshed",
		"min_date", mr.MinDate,
		"max_date", mr.MaxDate,
		"files", mr.TotalFiles,
	)
}

type RouteDecision int

const (
	RouteHotOnly  RouteDecision = iota
	RouteColdOnly
	RouteBoth
)

func (d RouteDecision) String() string {
	switch d {
	case RouteHotOnly:
		return "hot"
	case RouteColdOnly:
		return "cold"
	case RouteBoth:
		return "both"
	default:
		return "unknown"
	}
}

func (cr *ColdRouter) Route(startNs, endNs int64) RouteDecision {
	if cr == nil {
		return RouteHotOnly
	}

	now := time.Now()
	hotBoundary := now.Add(-cr.boundary)
	coldEnd := hotBoundary.Add(cr.overlap)

	start := time.Unix(0, startNs)
	end := time.Unix(0, endNs)

	cr.mu.RLock()
	manifest := cr.manifest
	cr.mu.RUnlock()

	if manifest != nil && manifest.TotalFiles == 0 {
		return RouteHotOnly
	}
	if manifest != nil {
		manifestMax := time.Unix(0, manifest.MaxTime)
		if start.After(manifestMax) {
			return RouteHotOnly
		}
	}

	if end.Before(coldEnd) && start.Before(hotBoundary) {
		return RouteColdOnly
	}
	if start.After(hotBoundary) || start.Equal(hotBoundary) {
		return RouteHotOnly
	}

	return RouteBoth
}

func (cr *ColdRouter) ColdBoundaryNs() int64 {
	return time.Now().Add(-cr.boundary).UnixNano()
}

func (cr *ColdRouter) ColdGet(ctx context.Context, path string, params url.Values) (*http.Response, error) {
	u := *cr.coldBackend
	u.Path = path
	u.RawQuery = params.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	return cr.client.Do(req)
}

func (cr *ColdRouter) ColdPost(ctx context.Context, path string, params url.Values) (*http.Response, error) {
	u := *cr.coldBackend
	u.Path = path

	req, err := http.NewRequestWithContext(ctx, "POST", u.String(),
		io.NopCloser(stringReader(params.Encode())))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return cr.client.Do(req)
}

func (cr *ColdRouter) HasManifest() bool {
	if cr == nil {
		return false
	}
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	return cr.manifest != nil
}

func (cr *ColdRouter) ManifestRange() *ManifestRange {
	if cr == nil {
		return nil
	}
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	if cr.manifest == nil {
		return nil
	}
	cp := *cr.manifest
	return &cp
}

// MergeNDJSON streams firstBody then secondBody into a single NDJSON reader.
// Callers choose the order based on query direction:
//   - backward (newest-first): pass hot body first, then cold
//   - forward  (oldest-first): pass cold body first, then hot
func MergeNDJSON(firstBody, secondBody io.Reader) io.Reader {
	pr, pw := io.Pipe()
	go func() {
		defer func() { _ = pw.Close() }()
		if firstBody != nil {
			_, _ = io.Copy(pw, firstBody)
		}
		if secondBody != nil {
			_, _ = io.Copy(pw, secondBody)
		}
	}()
	return pr
}

func MergeValuesJSON(hotData, coldData []byte) ([]byte, error) {
	type valEntry struct {
		Value string `json:"value"`
		Hits  int64  `json:"hits"`
	}
	type valResponse struct {
		Values []valEntry `json:"values"`
	}

	var hot, cold valResponse
	if len(hotData) > 0 {
		if err := json.Unmarshal(hotData, &hot); err != nil {
			return hotData, nil
		}
	}
	if len(coldData) > 0 {
		if err := json.Unmarshal(coldData, &cold); err != nil {
			return hotData, nil
		}
	}

	merged := make(map[string]int64)
	for _, v := range hot.Values {
		merged[v.Value] += v.Hits
	}
	for _, v := range cold.Values {
		merged[v.Value] += v.Hits
	}

	result := make([]valEntry, 0, len(merged))
	for v, h := range merged {
		result = append(result, valEntry{Value: v, Hits: h})
	}

	out := valResponse{Values: result}
	return json.Marshal(out)
}

func ParseTimeRangeFromRequest(r *http.Request) (startNs, endNs int64) {
	startStr := r.FormValue("start")
	endStr := r.FormValue("end")

	if startStr != "" {
		if ns, err := parseFlexTimestamp(startStr); err == nil {
			startNs = ns
		}
	}
	if endStr != "" {
		if ns, err := parseFlexTimestamp(endStr); err == nil {
			endNs = ns
		}
	}

	if startNs == 0 {
		startNs = time.Now().Add(-time.Hour).UnixNano()
	}
	if endNs == 0 {
		endNs = time.Now().UnixNano()
	}
	return
}

func parseFlexTimestamp(s string) (int64, error) {
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		switch {
		case n > 1e18:
			return n, nil
		case n > 1e15:
			return n * 1000, nil
		case n > 1e12:
			return n * 1_000_000, nil
		default:
			return n * 1_000_000_000, nil
		}
	}

	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UnixNano(), nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UnixNano(), nil
	}

	return 0, fmt.Errorf("unparseable timestamp: %s", s)
}

type stringReader string

func (s stringReader) Read(p []byte) (int, error) {
	n := copy(p, s)
	if n < len(s) {
		return n, nil
	}
	return n, io.EOF
}
