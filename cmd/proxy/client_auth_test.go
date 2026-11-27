package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

func writeUsersFile(t *testing.T, password string) string {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "users.yaml")
	if err := os.WriteFile(path, fmt.Appendf(nil, "users:\n  - name: grafana\n    password_hash: %q\n", hash), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// A users file that is missing, empty or broken stops the start: with caller
// authentication asked for, the proxy never opens without it.
//
// conformance: client-authentication
func TestBuildClientAuth_FailsClosed(t *testing.T) {
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty.yaml")
	broken := filepath.Join(dir, "broken.yaml")
	_ = os.WriteFile(empty, nil, 0o600)
	_ = os.WriteFile(broken, []byte("users: ["), 0o600)
	good := writeUsersFile(t, "secret")
	for name, c := range map[string]struct {
		path string
		ttl  time.Duration
		max  int
	}{
		"missing":          {filepath.Join(dir, "missing.yaml"), time.Minute, 4},
		"empty":            {empty, time.Minute, 4},
		"broken":           {broken, time.Minute, 4},
		"zero slots":       {good, time.Minute, 0},
		"negative ttl":     {good, -time.Second, 4},
		"directory":        {dir, time.Minute, 4},
		"broken, no cache": {broken, 0, 4},
	} {
		if a, err := buildClientAuth(c.path, c.ttl, c.max); err == nil || a != nil {
			t.Errorf("%s: started with %v", name, err)
		}
	}
	if a, err := buildClientAuth("", time.Minute, 4); a != nil || err != nil {
		t.Fatalf("no file: %v %v", a, err)
	}
	if a, err := buildClientAuth(good, time.Minute, 4); a == nil || err != nil || a.Users().Len() != 1 {
		t.Fatalf("good file: %v", err)
	}
	// The whole start stops too.
	_, err := buildRuntime(runtimeOptions{
		cacheTTL: time.Second, cacheMax: 10, cacheMaxBytes: defaultCacheMaxBytes,
		proxyCfg: proxyRuntimeConfig{backendURL: "http://example.com", logLevel: "error", labelStyle: "passthrough", metadataFieldMode: "hybrid",
			authUsersFile: broken, authCacheTTL: time.Minute, authMaxConcurrentVerifications: 4},
		serverOpts: serverRuntimeOptions{listenAddr: "127.0.0.1:0"},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), func(chan<- os.Signal, ...os.Signal) {}, func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher { return &fakeOTLPPusher{} })
	if err == nil || !strings.Contains(err.Error(), "-server.auth-users-file") {
		t.Fatalf("buildRuntime with a broken users file: %v", err)
	}
}

// Every listener cmd/proxy builds (main, the split admin listener, the
// dedicated metrics listener) has caller authentication in front of it; the
// probe and scrape routes stay open.
//
// conformance: client-authentication
func TestBuildRuntime_ClientAuthOnEveryListener(t *testing.T) {
	path := writeUsersFile(t, "secret")
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"values":[]}`)
	}))
	t.Cleanup(vl.Close)
	rt, err := buildRuntime(runtimeOptions{
		cacheTTL: time.Second, cacheMax: 10, cacheMaxBytes: defaultCacheMaxBytes,
		proxyCfg: proxyRuntimeConfig{backendURL: vl.URL, logLevel: "error", labelStyle: "passthrough", metadataFieldMode: "hybrid",
			registerInstrumentation: boolPtr(true), metricsMaxTenants: 10, metricsMaxClients: 10,
			authUsersFile: path, authCacheTTL: time.Minute, authMaxConcurrentVerifications: 4},
		maxBodyBytes: 1 << 20, responseCompression: "none",
		serverOpts:              serverRuntimeOptions{listenAddr: "127.0.0.1:0", readTimeout: time.Second, writeTimeout: time.Second},
		adminListenAddr:         "127.0.0.1:1",
		metricsListenAddr:       "127.0.0.1:2",
		registerInstrumentation: true,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), func(chan<- os.Signal, ...os.Signal) {}, func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher { return &fakeOTLPPusher{} })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.cacheCleanup)
	handlers := map[string]http.Handler{"main": rt.server.(*http.Server).Handler}
	for _, aux := range rt.auxServers {
		handlers[aux.role] = aux.srv.Handler
	}
	if len(handlers) != 3 {
		t.Fatalf("listeners %v", handlers)
	}
	cred := "Basic " + base64.StdEncoding.EncodeToString([]byte("grafana:secret"))
	for _, c := range []struct {
		listener, method, path string
		open                   bool
	}{
		{"main", http.MethodGet, "/loki/api/v1/labels", false},
		{"main", http.MethodGet, "/loki/api/v1/status/buildinfo", false},
		{"main", http.MethodGet, "/ready", true},
		{"main", http.MethodGet, "/alive", true},
		{"admin", http.MethodPost, "/admin/cache/flush", false},
		{"admin", http.MethodGet, "/admin/capabilities", false},
		{"metrics", http.MethodGet, "/metrics", true},
	} {
		rec := httptest.NewRecorder()
		handlers[c.listener].ServeHTTP(rec, httptest.NewRequest(c.method, c.path, nil))
		refused := rec.Code == http.StatusUnauthorized && rec.Header().Get("WWW-Authenticate") != ""
		if refused == c.open {
			t.Fatalf("%s %s %s without a credential: %d (open=%v)", c.listener, c.method, c.path, rec.Code, c.open)
		}
		if c.open {
			continue
		}
		req := httptest.NewRequest(c.method, c.path, nil)
		req.Header.Set("Authorization", cred)
		rec = httptest.NewRecorder()
		handlers[c.listener].ServeHTTP(rec, req)
		if rec.Code == http.StatusUnauthorized {
			t.Fatalf("%s %s %s with a credential: %d %s", c.listener, c.method, c.path, rec.Code, rec.Body.String())
		}
	}
}

// The users file is re-read on change: a broken version keeps the last good
// users and is reported (log line, reload failure counter), a good one
// replaces them. No log line names a user, hash or password.
//
// conformance: client-authentication
func TestStartClientAuthWatcher_ReloadsAndReports(t *testing.T) {
	path := writeUsersFile(t, "secret")
	a, err := buildClientAuth(path, time.Minute, 4)
	if err != nil {
		t.Fatal(err)
	}
	p, err := proxy.New(proxy.Config{BackendURL: "http://unused", LogLevel: "error", ClientAuth: a})
	if err != nil {
		t.Fatal(err)
	}
	var logs syncBuffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	startClientAuthWatcher(ctx, p, path, 5*time.Millisecond, logger)
	waitFor := func(what string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for !strings.Contains(logs.String(), what) {
			if time.Now().After(deadline) {
				t.Fatalf("no %q in %s", what, logs.String())
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitFor("caller authentication on")
	// Replace by rename, as a mounted secret does: the watcher must not read a
	// half-written file, which it would report as a second failure.
	replace := func(data []byte) {
		t.Helper()
		next := path + ".next"
		if err := os.WriteFile(next, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(next, path); err != nil {
			t.Fatal(err)
		}
	}
	replace([]byte("users: ["))
	waitFor("auth users file reload failed")
	if a.Users().Len() != 1 {
		t.Fatal("last good users lost")
	}
	hash, _ := bcrypt.GenerateFromPassword([]byte("rotated"), bcrypt.MinCost)
	replace(fmt.Appendf(nil, "users:\n  - name: grafana\n    password_hash: %q\n  - name: second\n    password_hash: %q\n", hash, hash))
	waitFor("auth users file reloaded")
	if a.Users().Len() != 2 {
		t.Fatalf("users %d after reload", a.Users().Len())
	}
	out := logs.String()
	for _, secret := range []string{"secret", "rotated", string(hash), "grafana"} {
		if strings.Contains(out, secret) {
			t.Fatalf("log carries %q: %s", secret, out)
		}
	}
	rec := httptest.NewRecorder()
	p.GetMetrics().Handler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(rec.Body.String(), "loki_vl_proxy_auth_users_reload_failures_total 1") {
		t.Fatal("reload failure not counted")
	}
	// Mixed or low bcrypt costs are reported once at start.
	var warn syncBuffer
	logClientAuthUsers(slog.New(slog.NewTextHandler(&warn, nil)), "x", path, a.Users())
	if !strings.Contains(warn.String(), "below cost 10") {
		t.Fatalf("low cost not reported: %s", warn.String())
	}
	startClientAuthWatcher(ctx, &proxy.Proxy{}, path, time.Millisecond, logger) // caller authentication off: no-op
}
