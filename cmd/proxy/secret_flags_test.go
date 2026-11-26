package main

import (
	"bytes"
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// secretCase is one secret as the binary takes it.
type secretCase struct {
	flag, env, value string
	read             func(proxyRuntimeConfig) string
}

func secretCases() []secretCase {
	return []secretCase{
		{"peer-auth-token", "PEER_AUTH_TOKEN", "sentinel-peer-7c1e", func(c proxyRuntimeConfig) string { return c.peerAuthToken }},
		{"server.admin-auth-token", "ADMIN_AUTH_TOKEN", "sentinel-admin-2b9d", func(c proxyRuntimeConfig) string { return c.adminAuthToken }},
		{"delete-api-auth-token", "DELETE_API_AUTH_TOKEN", "sentinel-delete-5a40", func(c proxyRuntimeConfig) string { return c.deleteAPIAuthToken }},
		{"backend-basic-auth", "BACKEND_BASIC_AUTH", "vluser:sentinel-backend-e817", func(c proxyRuntimeConfig) string { return c.backendBasicAuth }},
		{"backend-version-basic-auth", "BACKEND_VERSION_BASIC_AUTH", "veruser:sentinel-version-03fc", func(c proxyRuntimeConfig) string { return c.backendVersionBasicAuth }},
	}
}

func writeSecretFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// captureRunConfig runs the startup path up to the runtime builder and returns
// the configuration it was handed.
func captureRunConfig(t *testing.T, args []string, env map[string]string) (proxyRuntimeConfig, error) {
	t.Helper()
	reloadCh := make(chan os.Signal)
	close(reloadCh)
	var captured runtimeOptions
	err := run(args, func(k string) string { return env[k] }, io.Discard, func(chan<- os.Signal, ...os.Signal) {},
		func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher { return &fakeOTLPPusher{} },
		func(opts runtimeOptions, _ *slog.Logger, _ signalNotifier, _ otlpPusherFactory) (*runtimeState, error) {
			captured = opts
			return &runtimeState{proxy: &proxy.Proxy{}, server: &fakeHTTPServer{}, cacheCleanup: func() {}, stopOTLP: func() {}, reloadCh: reloadCh, shutdownCh: make(chan os.Signal)}, nil
		},
		func(httpServer, serverLoopOptions, *slog.Logger, func(string, ...any)) {},
		func(<-chan os.Signal, httpServer, time.Duration, *slog.Logger) {})
	return captured.proxyCfg, err
}

// Each secret is read from its value flag, its file flag or its environment
// variable, in that order.
//
// conformance: secrets-outside-process-arguments
func TestRun_SecretSources(t *testing.T) {
	for _, s := range secretCases() {
		file := writeSecretFile(t, s.value+"-file\r\n")
		env := map[string]string{s.env: s.value + "-env"}
		for _, tc := range []struct {
			name string
			args []string
			env  map[string]string
			want string
		}{
			{"unset", nil, nil, ""},
			{"env", nil, env, s.value + "-env"},
			{"value flag over env", []string{"-" + s.flag + "=" + s.value}, env, s.value},
			{"empty value flag over env", []string{"-" + s.flag + "="}, env, ""},
			{"file flag", []string{"-" + s.flag + "-file=" + file}, nil, s.value + "-file"},
			{"file flag over env", []string{"-" + s.flag + "-file=" + file}, env, s.value + "-file"},
		} {
			cfg, err := captureRunConfig(t, tc.args, tc.env)
			if err != nil {
				t.Fatalf("%s %s: %v", s.flag, tc.name, err)
			}
			if got := s.read(cfg); got != tc.want {
				t.Errorf("%s %s: got %q, want %q", s.flag, tc.name, got, tc.want)
			}
		}
	}
}

// A configured secret that cannot be used stops startup; the error names the
// flag and never a value.
//
// conformance: secrets-outside-process-arguments
func TestRun_SecretSourceErrors(t *testing.T) {
	for _, s := range secretCases() {
		good := writeSecretFile(t, s.value)
		env := map[string]string{s.env: s.value}
		for _, tc := range []struct {
			name, want string
			args       []string
		}{
			{"value and file", "are both set", []string{"-" + s.flag + "=" + s.value, "-" + s.flag + "-file=" + good}},
			{"missing file", "no such file", []string{"-" + s.flag + "-file=" + good + ".absent"}},
			{"empty path", "no such file", []string{"-" + s.flag + "-file="}},
			{"empty file", "is empty", []string{"-" + s.flag + "-file=" + writeSecretFile(t, "\n")}},
			{"directory", "directory", []string{"-" + s.flag + "-file=" + t.TempDir()}},
			{"oversized file", "larger than", []string{"-" + s.flag + "-file=" + writeSecretFile(t, strings.Repeat("x", maxSecretFileBytes+1))}},
			{"bad flag syntax", "<redacted>", []string{"---" + s.flag + "=" + s.value}},
			{"unknown flag", "flag provided but not defined", []string{"-" + s.flag + "-typo=" + s.value}},
			{"later check", "invalid -response-compression-min-bytes", []string{"-" + s.flag + "=" + s.value, "-response-compression-min-bytes=-1"}},
		} {
			_, err := captureRunConfig(t, tc.args, env)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("%s %s: error %v, want %q", s.flag, tc.name, err, tc.want)
			}
			if strings.Contains(err.Error(), "sentinel") {
				t.Fatalf("%s %s: error echoes the secret: %v", s.flag, tc.name, err)
			}
		}
	}
}

// The startup checks see secrets that arrive by file or environment, and still
// refuse when none arrives.
//
// conformance: secrets-outside-process-arguments, delete-api-requires-token
func TestRun_SecretsSatisfyStartupChecks(t *testing.T) {
	for _, tc := range []struct {
		name, flag, env, want string
		args                  []string
	}{
		{"peer", "peer-auth-token", "PEER_AUTH_TOKEN", "peer-auth-token", []string{"-peer-discovery=static", "-peer-static=127.0.0.1:1", "-peer-self=127.0.0.1:2"}},
		{"delete", "delete-api-auth-token", "DELETE_API_AUTH_TOKEN", "-delete-api-auth-token", []string{"-delete-api-enabled=true"}},
		{"admin", "server.admin-auth-token", "ADMIN_AUTH_TOKEN", "server.admin-auth-token is required", []string{"-server.register-instrumentation=true", "-admin-listen=0.0.0.0:3101"}},
	} {
		if _, err := captureRunConfig(t, tc.args, nil); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s without a secret: error %v, want %q", tc.name, err, tc.want)
		}
		if _, err := captureRunConfig(t, tc.args, map[string]string{tc.env: "token"}); err != nil {
			t.Fatalf("%s from the environment: %v", tc.name, err)
		}
		file := "-" + tc.flag + "-file=" + writeSecretFile(t, "token\n")
		if _, err := captureRunConfig(t, append(append([]string{}, tc.args...), file), nil); err != nil {
			t.Fatalf("%s from a file: %v", tc.name, err)
		}
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// No secret appears in the debug-level log or in any answer of the config,
// metrics, admin and debug routes, whichever source it came from.
//
// conformance: secrets-outside-process-arguments, credential-passthrough-as-configured
func TestRun_SecretsStayOutOfLogsAndEndpoints(t *testing.T) {
	var backendMu sync.Mutex
	var backendAuth []string
	var backendSaw strings.Builder
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendMu.Lock()
		backendAuth = append(backendAuth, r.Header.Get("Authorization"))
		_ = r.Header.Write(&backendSaw)
		backendSaw.WriteString(r.URL.String())
		backendMu.Unlock()
		if r.URL.Path == "/metrics" {
			_, _ = io.WriteString(w, "vm_app_version{version=\"victoria-logs-20260101-000000-tags-v1.54.0\",short_version=\"v1.54.0\"} 1\n")
			return
		}
		_, _ = io.WriteString(w, `{"values":[]}`)
	}))
	defer backend.Close()

	cases := secretCases()
	token := map[string]string{}
	for _, s := range cases {
		token[s.flag] = s.value
	}
	var secretText []string
	for _, s := range cases {
		secretText = append(secretText, s.value, base64.StdEncoding.EncodeToString([]byte(s.value)))
	}
	secretText = append(secretText, "sentinel")

	for _, source := range []string{"flag", "file", "env"} {
		t.Run(source, func(t *testing.T) {
			args := []string{
				"-backend=" + backend.URL, "-listen=127.0.0.1:0", "-log-level=debug", "-log-buffered=false",
				"-slow-query-threshold=1ns", "-slow-upstream-threshold=1ns", "-labels-cache-warm=false",
				"-server.register-instrumentation=true", "-server.enable-pprof=true", "-server.enable-query-analytics=true",
				"-delete-api-enabled=true", "-backend-version-url=" + backend.URL + "/metrics",
				"-peer-discovery=static", "-peer-static=127.0.0.1:1", "-peer-self=127.0.0.1:2",
			}
			env := map[string]string{}
			for _, s := range cases {
				switch source {
				case "flag":
					args = append(args, "-"+s.flag+"="+s.value)
				case "file":
					args = append(args, "-"+s.flag+"-file="+writeSecretFile(t, s.value+"\n"))
				case "env":
					env[s.env] = s.value
				}
			}
			backendMu.Lock()
			backendAuth = nil
			backendSaw.Reset()
			backendMu.Unlock()

			logs := &syncBuffer{}
			var answers strings.Builder
			statuses := map[string]int{}
			err := run(args, func(k string) string { return env[k] }, logs, func(chan<- os.Signal, ...os.Signal) {},
				func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher { return &fakeOTLPPusher{} },
				buildRuntime,
				func(httpServer, serverLoopOptions, *slog.Logger, func(string, ...any)) {},
				func(_ <-chan os.Signal, srv httpServer, _ time.Duration, _ *slog.Logger) {
					handler := srv.(*http.Server).Handler
					for _, req := range []struct{ name, method, target, header, value string }{
						{"config", "GET", "/config", "", ""},
						{"ready", "GET", "/ready", "", ""},
						{"buildinfo", "GET", "/loki/api/v1/status/buildinfo", "", ""},
						{"labels", "GET", "/loki/api/v1/labels", "", ""},
						{"metrics", "GET", "/metrics", "", ""},
						{"capabilities", "GET", "/admin/capabilities", "X-Admin-Token", token["server.admin-auth-token"]},
						{"queries", "GET", "/debug/queries", "X-Admin-Token", token["server.admin-auth-token"]},
						{"cmdline", "GET", "/debug/pprof/cmdline", "X-Admin-Token", token["server.admin-auth-token"]},
						{"pprof", "GET", "/debug/pprof/", "X-Admin-Token", token["server.admin-auth-token"]},
						{"admin refused", "GET", "/admin/capabilities", "X-Admin-Token", "wrong"},
						{"delete", "POST", `/loki/api/v1/delete?query=%7Ba%3D%22b%22%7D&start=1&end=2`, "X-Delete-Token", token["delete-api-auth-token"]},
						{"delete bearer", "POST", `/loki/api/v1/delete?query=%7Ba%3D%22b%22%7D&start=1&end=2`, "Authorization", "Bearer " + token["delete-api-auth-token"]},
						{"admin bearer", "GET", "/admin/capabilities", "Authorization", "Bearer " + token["server.admin-auth-token"]},
						{"delete refused", "POST", "/loki/api/v1/delete", "X-Delete-Token", "wrong"},
						{"peer", "GET", "/_cache/get?key=k", "X-Peer-Token", token["peer-auth-token"]},
						{"peer refused", "GET", "/_cache/get?key=k", "X-Peer-Token", "wrong"},
					} {
						r := httptest.NewRequest(req.method, req.target, nil)
						if req.header != "" {
							r.Header.Set(req.header, req.value)
						}
						w := httptest.NewRecorder()
						handler.ServeHTTP(w, r)
						statuses[req.name] = w.Code
						_ = w.Header().Write(&answers)
						answers.Write(w.Body.Bytes())
					}
				})
			if err != nil {
				t.Fatal(err)
			}
			// The routes answered as configured, so the secrets are in force.
			for name, want := range map[string]int{"capabilities": 200, "admin refused": 401, "delete refused": 401, "cmdline": 404, "config": 200} {
				if statuses[name] != want {
					t.Errorf("%s: status %d, want %d", name, statuses[name], want)
				}
			}
			if statuses["admin bearer"] != 200 || statuses["delete bearer"] == 401 {
				t.Errorf("bearer token refused: admin %d, delete %d", statuses["admin bearer"], statuses["delete bearer"])
			}
			if statuses["delete"] == 401 || statuses["peer"] == 401 || statuses["peer"] == 403 {
				t.Errorf("configured token refused: delete %d, peer %d", statuses["delete"], statuses["peer"])
			}
			if statuses["peer refused"] != 401 && statuses["peer refused"] != 403 {
				t.Errorf("wrong peer token: status %d", statuses["peer refused"])
			}
			backendMu.Lock()
			defer backendMu.Unlock()
			for _, s := range cases[3:] {
				want := "Basic " + base64.StdEncoding.EncodeToString([]byte(s.value))
				if !strings.Contains(strings.Join(backendAuth, "\n"), want) {
					t.Errorf("-%s never reached the backend", s.flag)
				}
			}
			for _, s := range cases[:3] {
				if strings.Contains(backendSaw.String(), s.value) {
					t.Errorf("-%s was sent to the backend", s.flag)
				}
			}
			if !strings.Contains(logs.String(), "peer cache shared token enabled") {
				t.Fatalf("startup log was not captured:\n%s", logs.String())
			}
			for _, secret := range secretText {
				if strings.Contains(logs.String(), secret) {
					t.Errorf("log holds %q", secret)
				}
				if strings.Contains(answers.String(), secret) {
					t.Errorf("an answer holds %q", secret)
				}
			}
		})
	}
}

// Environment variables the proxy reads for secrets are set by nothing unless
// asked: an unset value keeps the admin routes on the loopback listener.
//
// conformance: secrets-outside-process-arguments
func TestRun_NoSecretKeepsAdminOnLoopback(t *testing.T) {
	reloadCh := make(chan os.Signal)
	close(reloadCh)
	for env, want := range map[string]string{"": "127.0.0.1:3101", "token": ":3100"} {
		var captured runtimeOptions
		shutdownCh := make(chan os.Signal, 1)
		shutdownCh <- syscall.SIGTERM
		err := run([]string{"-server.register-instrumentation=true"}, func(k string) string {
			if k == "ADMIN_AUTH_TOKEN" {
				return env
			}
			return ""
		}, io.Discard, func(chan<- os.Signal, ...os.Signal) {},
			func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher { return &fakeOTLPPusher{} },
			func(opts runtimeOptions, _ *slog.Logger, _ signalNotifier, _ otlpPusherFactory) (*runtimeState, error) {
				captured = opts
				return &runtimeState{proxy: &proxy.Proxy{}, server: &fakeHTTPServer{}, cacheCleanup: func() {}, stopOTLP: func() {}, reloadCh: reloadCh, shutdownCh: shutdownCh}, nil
			},
			func(httpServer, serverLoopOptions, *slog.Logger, func(string, ...any)) {},
			func(ch <-chan os.Signal, _ httpServer, _ time.Duration, _ *slog.Logger) { <-ch })
		if err != nil {
			t.Fatal(err)
		}
		if captured.adminListenAddr != want {
			t.Fatalf("ADMIN_AUTH_TOKEN=%q: admin listener %q, want %q", env, captured.adminListenAddr, want)
		}
	}
}

// Startup cost of reading the five secrets from each source.
func BenchmarkResolveSecretFlags(b *testing.B) {
	path := filepath.Join(b.TempDir(), "secret")
	if err := os.WriteFile(path, []byte("token\n"), 0o600); err != nil {
		b.Fatal(err)
	}
	for _, source := range []string{"unset", "env", "file"} {
		b.Run(source, func(b *testing.B) {
			values := make([]string, len(secretCases()))
			explicit := map[string]bool{}
			var secrets []secretFlag
			for i, s := range secretCases() {
				secrets = append(secrets, secretFlag{name: s.flag, value: &values[i], file: &path, env: s.env})
				explicit[s.flag+"-file"] = source == "file"
			}
			getenv := func(string) string {
				if source == "env" {
					return "token"
				}
				return ""
			}
			b.ReportAllocs()
			for b.Loop() {
				if err := resolveSecretFlags(explicit, getenv, secrets); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
