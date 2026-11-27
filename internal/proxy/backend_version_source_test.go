package proxy

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/capability"
)

func metricsLine(build string) string {
	return fmt.Sprintf("vm_app_version{version=%q,short_version=\"x\"} 1\n", build)
}

// behindVMAuth is a VictoriaLogs of release rel behind vmauth: the probes reach VictoriaLogs, /metrics is vmauth's.
func behindVMAuth(t *testing.T, build string) *capBackend {
	t.Helper()
	b := newCapBackend(t, build)
	b.mu.Lock()
	b.text = metricsLine(buildVMAuth)
	b.mu.Unlock()
	return b
}

// versionServer is the endpoint -backend-version-url names; it records the headers of every request.
type versionServer struct {
	srv  *httptest.Server
	mu   sync.Mutex
	seen []http.Header
}

func newVersionServer(t *testing.T, handler http.HandlerFunc) *versionServer {
	t.Helper()
	v := &versionServer{}
	v.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v.mu.Lock()
		v.seen = append(v.seen, r.Header.Clone())
		v.mu.Unlock()
		handler(w, r)
	}))
	t.Cleanup(v.srv.Close)
	return v
}

func (v *versionServer) requests() []http.Header {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]http.Header(nil), v.seen...)
}

func answerText(text string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(text)) }
}

func answerStatus(status int) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }
}

func logRecords(t *testing.T, logs *bytes.Buffer, msg string) (out []map[string]any) {
	t.Helper()
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var rec map[string]any
		if json.Unmarshal([]byte(line), &rec) == nil && rec["msg"] == msg {
			out = append(out, rec)
		}
	}
	return out
}

func versionKnownMetric(t *testing.T, p *Proxy) string {
	t.Helper()
	rec := httptest.NewRecorder()
	p.metrics.Handler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if value, ok := strings.CutPrefix(line, "loki_vl_proxy_backend_version_known "); ok {
			return value
		}
	}
	t.Fatal("loki_vl_proxy_backend_version_known is not exposed")
	return ""
}

// versionOnlyGates are the gates no probe decides: the ones the issue found off behind vmauth.
func versionOnlyGates(p *Proxy) [3]bool {
	return [3]bool{gateOn(p, capability.ExtraFiltersLen), gateOn(p, capability.MathQuotedConstantClone), gateOn(p, capability.MixedJSONRowStats)}
}

// Every way the version URL can answer: what the gates become, what the gauge says and what the WARN line names.
//
// conformance: vl-backend-version-source, backend-version-source
func TestVersionURLAnswers(t *testing.T) {
	release := make(chan struct{})
	elsewhere := newVersionServer(t, answerText(metricsLine(buildV154)))
	gzipped := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		zw := gzip.NewWriter(w)
		_, _ = zw.Write([]byte(metricsLine(buildV154)))
		_ = zw.Close()
	}
	filler := strings.Repeat("# filler\n", maxMetricsProbeBytes/9+1)
	allOn, allOff := [3]bool{false, true, true}, [3]bool{} // no probe reaches vl.extra_filters_len
	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
		known   string
		gates   [3]bool
		reason  []string // parts of the WARN reason; none when the release is read
	}{
		{"VictoriaLogs", answerText(metricsLine(buildV154)), "1", allOn, nil},
		{"VictoriaLogs, gzip", gzipped, "1", allOn, nil},
		{"VictoriaLogs ahead of an oversize tail", answerText(metricsLine(buildV154) + filler), "1", allOn, nil},
		{"vmauth's own", answerText(metricsLine(buildVMAuth)), "0", allOff, []string{"found no VictoriaLogs release", "found vmauth-"}},
		{"no version series", answerText("go_goroutines 1\n"), "0", allOff, []string{"found no VictoriaLogs release", "no vm_app_version"}},
		{"release past the bound", answerText(filler + metricsLine(buildV154)), "0", allOff, []string{"found no VictoriaLogs release", "only the first 2097152 bytes"}},
		{"401", answerStatus(http.StatusUnauthorized), "0", allOff, []string{"returned HTTP status 401"}},
		{"404", answerStatus(http.StatusNotFound), "0", allOff, []string{"returned HTTP status 404"}},
		{"500", answerStatus(http.StatusInternalServerError), "0", allOff, []string{"returned HTTP status 500"}},
		{"redirect", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, elsewhere.srv.URL+"/metrics", http.StatusFound)
		}, "0", allOff, []string{"returned HTTP status 302", "redirects are not followed"}},
		{"timeout", func(_ http.ResponseWriter, r *http.Request) {
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}, "0", allOff, []string{"failed", "deadline exceeded"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vs := newVersionServer(t, tc.handler)
			p := newCapProxy(t, behindVMAuth(t, buildV154), func(c *Config) {
				c.BackendVersionURL = vs.srv.URL + "/metrics?authKey=sekret-key"
				c.BackendVersionBasicAuth = "reader:sekret-pass"
				c.BackendBasicAuth = "backend:backend-pass"
				c.BackendHeaders = map[string]string{"X-Backend-Secret": "backend-header"}
				c.BackendVersionCheckTimeout = 300 * time.Millisecond
			})
			logs := captureLog(p)
			if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
				t.Fatal(err)
			}
			if got := versionOnlyGates(p); got != tc.gates {
				t.Errorf("gates [extra_filters math_clone mixed_json] %v, want %v", got, tc.gates)
			}
			if got := versionKnownMetric(t, p); got != tc.known {
				t.Errorf("version_known %s, want %s", got, tc.known)
			}
			partial := logRecords(t, logs, "backend version compatibility check partial")
			if tc.reason == nil {
				passed := logRecords(t, logs, "backend version compatibility check passed")
				if len(partial) != 0 || len(passed) != 1 || passed[0]["backend.version.semver"] != "v1.54.0" {
					t.Fatalf("log:\n%s", logs)
				}
			} else {
				if len(partial) != 1 || partial[0]["level"] != "WARN" {
					t.Fatalf("one WARN partial line:\n%s", logs)
				}
				reason, _ := partial[0]["reason"].(string)
				for _, part := range append(tc.reason, "-backend-version-url") {
					if !strings.Contains(reason, part) {
						t.Errorf("reason %q lacks %q", reason, part)
					}
				}
			}
			// No credential and no query string reaches a log, at any level.
			for _, secret := range []string{"sekret-key", "sekret-pass", "cmVhZGVyOnNla3JldC1wYXNz", "backend-pass", "backend-header"} {
				if strings.Contains(logs.String(), secret) {
					t.Errorf("%q is logged:\n%s", secret, logs)
				}
			}
			// The status route shows neither the version URL nor a credential.
			status := httptest.NewRecorder()
			p.handleCapabilities(status, httptest.NewRequest(http.MethodGet, "/admin/capabilities", nil))
			for _, secret := range []string{"sekret", strings.TrimPrefix(vs.srv.URL, "http://")} {
				if status.Code != http.StatusOK || strings.Contains(status.Body.String(), secret) {
					t.Errorf("/admin/capabilities reveals %q: %s", secret, status.Body)
				}
			}
			// The version URL gets its own credential and nothing the backend gets.
			seen := vs.requests()
			if len(seen) == 0 {
				t.Fatal("the version URL was not read")
			}
			for _, h := range seen {
				r := &http.Request{Header: h}
				if user, pass, ok := r.BasicAuth(); !ok || user != "reader" || pass != "sekret-pass" || h.Get("X-Backend-Secret") != "" || h.Get("X-Scope-Orgid") != "" || h.Get("AccountID") != "" {
					t.Errorf("headers sent to the version URL: %v", h)
				}
			}
		})
	}
	close(release)
	if n := len(elsewhere.requests()); n != 0 {
		t.Fatalf("a redirect was followed %d times", n)
	}
}

// Without a named source the reason of the failed read is on the WARN line too: the status, the other program.
//
// conformance: vl-backend-version-source, backend-version-source
func TestPartialCheckNamesTheReason(t *testing.T) {
	p := newCapProxy(t, behindVMAuth(t, buildV154), nil)
	logs := captureLog(p)
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	partial := logRecords(t, logs, "backend version compatibility check partial")
	if len(partial) != 1 || partial[0]["level"] != "WARN" || !strings.Contains(fmt.Sprint(partial[0]["reason"]), "metrics probe found no VictoriaLogs release (found vmauth-") {
		t.Fatalf("log:\n%s", logs)
	}
	if got := versionOnlyGates(p); got != [3]bool{} || versionKnownMetric(t, p) != "0" {
		t.Fatalf("gates %v", got)
	}
	// A periodic round with the same reason says nothing more; strict mode puts the reason in its error.
	p.runCapabilityProbes(context.Background(), true)
	p.runCapabilityProbes(context.Background(), true)
	if n := len(logRecords(t, logs, "backend version read failed")); n != 0 {
		t.Fatalf("%d repeated warnings", n)
	}
	strict := newCapProxy(t, behindVMAuth(t, buildV154), func(c *Config) { c.BackendVersionStrict = true })
	if err := strict.ValidateBackendVersionCompatibility(context.Background()); err == nil || !strings.Contains(err.Error(), "found vmauth-") {
		t.Fatalf("strict error: %v", err)
	}
}

// A periodic round warns once per change of the reason, and a recovered read says nothing.
//
// conformance: vl-backend-version-source, backend-version-source
func TestVersionReadFailureWarnsOncePerChange(t *testing.T) {
	var mu sync.Mutex
	status := http.StatusOK
	vs := newVersionServer(t, func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(metricsLine(buildV154)))
	})
	p := newCapProxy(t, behindVMAuth(t, buildV154), func(c *Config) { c.BackendVersionURL = vs.srv.URL })
	logs := captureLog(p)
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	set := func(s int) {
		mu.Lock()
		status = s
		mu.Unlock()
		p.runCapabilityProbes(context.Background(), true)
		p.runCapabilityProbes(context.Background(), true)
	}
	set(http.StatusInternalServerError)
	// An outage of the version URL keeps the release and the gates.
	if got := versionOnlyGates(p); got != [3]bool{false, true, true} || versionKnownMetric(t, p) != "1" {
		t.Fatalf("gates during an outage %v", got)
	}
	set(http.StatusOK)
	set(http.StatusUnauthorized)
	lines := logRecords(t, logs, "backend version read failed")
	if len(lines) != 2 || lines[0]["level"] != "WARN" || !strings.Contains(fmt.Sprint(lines[0]["reason"]), "500") || !strings.Contains(fmt.Sprint(lines[1]["reason"]), "401") {
		t.Fatalf("log:\n%s", logs)
	}
}

// -backend-version against the probes: confirmed it enables what they prove, contradicted it is not used, and it is
// reported once.
//
// conformance: vl-backend-version-source, backend-version-source
func TestDeclaredVersionAgainstProbes(t *testing.T) {
	for _, tc := range []struct {
		name, declared, actual string
		known                  string
		gates                  [3]bool
		warning                string
	}{
		{"confirmed", "v1.54.0", buildV154, "1", [3]bool{false, true, true}, ""},
		{"newer than the backend", "v1.54.0", buildV152, "0", [3]bool{}, "named backend version contradicted by a feature probe; it is not used"},
		{"older than the backend", "v1.52.0", buildV154, "1", [3]bool{false, false, true}, "the feature probes show a VictoriaLogs newer than the named release; the gates above it are off, update it"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newCapProxy(t, behindVMAuth(t, tc.actual), func(c *Config) { c.BackendVersion = tc.declared })
			logs := captureLog(p)
			if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
				t.Fatal(err)
			}
			p.runCapabilityProbes(context.Background(), true)
			if got := versionOnlyGates(p); got != tc.gates || versionKnownMetric(t, p) != tc.known {
				t.Fatalf("gates %v known %s: %s", got, versionKnownMetric(t, p), p.caps.Backend().HeaderValue())
			}
			partial := logRecords(t, logs, "backend version compatibility check partial")
			if len(partial) != 1 || !strings.Contains(fmt.Sprint(partial[0]["reason"]), "found vmauth-") {
				t.Fatalf("the failed read is named:\n%s", logs)
			}
			if tc.warning != "" {
				if lines := logRecords(t, logs, tc.warning); len(lines) != 1 || lines[0]["level"] != "WARN" {
					t.Fatalf("one warning:\n%s", logs)
				}
			}
			// A declared release is not a reading: traffic still retries the metrics read.
			if p.caps.Backend().Observed() {
				t.Fatal("a declared release counts as read")
			}
		})
	}
}

// A version URL that names a newer release than the one queried is not used, and the WARN line says so.
//
// conformance: vl-backend-version-source, backend-version-source
func TestVersionURLContradictedByProbes(t *testing.T) {
	vs := newVersionServer(t, answerText(metricsLine(buildV154)))
	p := newCapProxy(t, behindVMAuth(t, buildV152), func(c *Config) { c.BackendVersionURL = vs.srv.URL })
	logs := captureLog(p)
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := versionOnlyGates(p); got != [3]bool{} || versionKnownMetric(t, p) != "0" {
		t.Fatalf("gates %v", got)
	}
	partial := logRecords(t, logs, "backend version compatibility check partial")
	if len(partial) != 1 || !strings.Contains(fmt.Sprint(partial[0]["reason"]), "contradicts the named release v1.54.0 (version_url)") {
		t.Fatalf("log:\n%s", logs)
	}
	p.runCapabilityProbes(context.Background(), true)
	if n := len(logRecords(t, logs, "named backend version contradicted by a feature probe; it is not used")); n != 1 {
		t.Fatalf("%d warnings:\n%s", n, logs)
	}
}

// A declared release that differs from the one read is reported, and the lower one decides.
//
// conformance: vl-backend-version-source, backend-version-source
func TestDeclaredVersionDiffersFromTheRead(t *testing.T) {
	p := newCapProxy(t, newCapBackend(t, buildV154), func(c *Config) { c.BackendVersion = "v1.52.0" })
	logs := captureLog(p)
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.runCapabilityProbes(context.Background(), true)
	if got := p.caps.Backend().Release().String(); got != "v1.52.0" || gateOn(p, capability.MathQuotedConstantClone) {
		t.Fatalf("release %s", got)
	}
	lines := logRecords(t, logs, "declared backend version differs from the version read; the lower one decides the gates")
	if len(lines) != 1 || lines[0]["level"] != "WARN" || lines[0]["backend.version.declared"] != "v1.52.0" {
		t.Fatalf("log:\n%s", logs)
	}
}

// A declared release below the minimum blocks startup like a read one.
//
// conformance: vl-backend-version-source, backend-version-source
func TestDeclaredVersionBelowMinimumBlocksStartup(t *testing.T) {
	p := newCapProxy(t, behindVMAuth(t, buildV154), func(c *Config) { c.BackendVersion = "v1.40.0" })
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err == nil || !strings.Contains(err.Error(), "v1.40.0") {
		t.Fatalf("%v", err)
	}
}

// conformance: vl-backend-version-source, backend-version-source
func TestVersionSourceConfigValidation(t *testing.T) {
	base := func() Config {
		return Config{BackendURL: "http://127.0.0.1:1", Cache: cache.New(time.Minute, 10), LogLevel: "error"}
	}
	for name, tc := range map[string]struct {
		mutate func(*Config)
		want   string
	}{
		"scheme":          {func(c *Config) { c.BackendVersionURL = "file:///etc/passwd?k=sekret" }, "scheme must be http or https"},
		"no host":         {func(c *Config) { c.BackendVersionURL = "http:///metrics" }, "names no host"},
		"unparsable":      {func(c *Config) { c.BackendVersionURL = "http://u:sekret@[::1" }, "not a URL"},
		"userinfo":        {func(c *Config) { c.BackendVersionURL = "http://u:sekret@vl/metrics" }, "-backend-version-basic-auth"},
		"auth alone":      {func(c *Config) { c.BackendVersionBasicAuth = "u:sekret" }, "needs -backend-version-url"},
		"bad version":     {func(c *Config) { c.BackendVersion = "latest" }, "invalid -backend-version"},
		"probes off, url": {func(c *Config) { c.BackendVersionURL, c.BackendFeatureProbesDisabled = "http://vl/metrics", true }, "-backend-feature-probes=true"},
		"probes off":      {func(c *Config) { c.BackendVersion, c.BackendFeatureProbesDisabled = "v1.54.0", true }, "-backend-feature-probes=true"},
	} {
		cfg := base()
		tc.mutate(&cfg)
		if _, err := New(cfg); err == nil || !strings.Contains(err.Error(), tc.want) || strings.Contains(err.Error(), "sekret") {
			t.Errorf("%s: %v", name, err)
		}
	}
}
