//go:build integration

// Package integration spins up the compiled proxy binary against an
// in-process stub VictoriaLogs backend and asserts end-to-end behavior
// of the secure-defaults hardening surfaces: dedicated loopback admin
// listener, peer-auth-token enforcement, metadata default lookback, and
// debug-log query redaction.
//
// The suite is gated by the build tag "integration" so it is skipped by
// the default `go test ./...` run. Invoke explicitly:
//
//	GOWORK=off go test -tags=integration ./test/integration/ -count=1 -v -timeout=180s
package integration

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// proxyBinary is the path to the pre-built proxy binary, populated by
// TestMain so each test does not pay the `go build` cost separately.
var proxyBinary string

// repoRoot is the absolute path to the repo root, derived from the working
// directory of this test package (test/integration).
var repoRoot string

// TestMain builds the proxy binary once per `go test` invocation. The
// integration tests then exec this binary instead of `go run ./cmd/proxy`,
// which saves ~3-4 seconds per scenario and avoids the toolchain spawning
// a fresh build for every subprocess.
func TestMain(m *testing.M) {
	wd, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "integration TestMain: getwd: %v\n", err)
		os.Exit(2)
	}
	// test/integration/ -> repo root is two levels up.
	repoRoot = filepath.Clean(filepath.Join(wd, "..", ".."))

	tmpDir, err := os.MkdirTemp("", "lvlp-integration-bin-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "integration TestMain: mkdtemp: %v\n", err)
		os.Exit(2)
	}
	proxyBinary = filepath.Join(tmpDir, "loki-vl-proxy")

	buildCtx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	buildCmd := exec.CommandContext(buildCtx, "go", "build", "-o", proxyBinary, "./cmd/proxy")
	buildCmd.Dir = repoRoot
	buildCmd.Env = append(os.Environ(), "GOWORK=off")
	buildOut, buildErr := buildCmd.CombinedOutput()
	if buildErr != nil {
		fmt.Fprintf(os.Stderr, "integration TestMain: build failed: %v\n%s\n", buildErr, buildOut)
		os.Exit(2)
	}

	code := m.Run()
	_ = os.RemoveAll(tmpDir)
	os.Exit(code)
}

// stubBackend is a minimal in-process VictoriaLogs stand-in. It captures
// the most recent inbound request per endpoint so individual tests can
// assert that the proxy forwarded the expected query parameters (e.g. the
// injected metadata lookback window).
type stubBackend struct {
	server *httptest.Server

	mu       sync.Mutex
	lastReqs map[string]capturedRequest
	hits     map[string]int64
}

type capturedRequest struct {
	method string
	path   string
	query  map[string][]string
	header http.Header
}

func (s *stubBackend) snapshot(path string) (capturedRequest, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.lastReqs[path]
	return r, ok
}

func (s *stubBackend) hitCount(path string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits[path]
}

func (s *stubBackend) record(r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastReqs == nil {
		s.lastReqs = make(map[string]capturedRequest)
	}
	if s.hits == nil {
		s.hits = make(map[string]int64)
	}
	q := r.URL.Query()
	cloned := make(map[string][]string, len(q))
	for k, v := range q {
		cp := make([]string, len(v))
		copy(cp, v)
		cloned[k] = cp
	}
	s.lastReqs[r.URL.Path] = capturedRequest{
		method: r.Method,
		path:   r.URL.Path,
		query:  cloned,
		header: r.Header.Clone(),
	}
	s.hits[r.URL.Path]++
}

// startStubBackend launches an in-process HTTP server that emulates the
// subset of VictoriaLogs the proxy contacts at startup and on label
// metadata reads. It records every request so tests can assert query
// parameter injection without spawning a full VL.
//
// Endpoints covered:
//
//   - GET /health → 200 with VictoriaLogs server header so the startup
//     version-check passes in soft mode (default).
//   - GET /select/logsql/{stream_field_names,field_names} →
//     {"values":[]} to satisfy /loki/api/v1/labels handler.
//   - GET /select/logsql/{stream_field_values,field_values} →
//     {"values":[]} to keep label_values calls happy.
//   - GET /select/logsql/query → empty NDJSON for /loki/api/v1/query.
//   - Anything else → 200 with empty JSON object.
func startStubBackend(t *testing.T) *stubBackend {
	t.Helper()
	s := &stubBackend{}
	mux := http.NewServeMux()

	versionHeader := "VictoriaLogs/v1.50.0"

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		s.record(r)
		w.Header().Set("Server", versionHeader)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "OK")
	})

	emptyValues := func(w http.ResponseWriter, r *http.Request) {
		s.record(r)
		w.Header().Set("Server", versionHeader)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"values":[]}`)
	}
	mux.HandleFunc("/select/logsql/stream_field_names", emptyValues)
	mux.HandleFunc("/select/logsql/field_names", emptyValues)
	mux.HandleFunc("/select/logsql/stream_field_values", emptyValues)
	mux.HandleFunc("/select/logsql/field_values", emptyValues)

	// /select/logsql/query returns NDJSON. Empty body is a valid "no rows"
	// response for the proxy translation layer.
	mux.HandleFunc("/select/logsql/query", func(w http.ResponseWriter, r *http.Request) {
		s.record(r)
		w.Header().Set("Server", versionHeader)
		w.Header().Set("Content-Type", "application/stream+json")
		// Empty body — no log rows.
	})

	// Catch-all: record + 200 + empty JSON. Lets the proxy probe random
	// endpoints (e.g. capability discovery) without crashing.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		s.record(r)
		w.Header().Set("Server", versionHeader)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	})

	s.server = httptest.NewServer(mux)
	t.Cleanup(s.server.Close)
	return s
}

// URL returns the base URL of the stub backend.
func (s *stubBackend) URL() string { return s.server.URL }

// pickPorts returns three free ephemeral TCP ports on 127.0.0.1 suitable
// for --listen, --admin-listen, and --metrics-listen. Each returned port
// has been released back to the kernel before pickPorts returns, so the
// caller races against any other process for the same port; in practice
// this is good enough for short-lived test subprocesses on a developer
// machine.
func pickPorts(t *testing.T, n int) []int {
	t.Helper()
	ports := make([]int, 0, n)
	listeners := make([]net.Listener, 0, n)
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			for _, prev := range listeners {
				_ = prev.Close()
			}
			t.Fatalf("pickPorts: listen: %v", err)
		}
		listeners = append(listeners, l)
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	for _, l := range listeners {
		_ = l.Close()
	}
	return ports
}

// proxyProc is a started proxy subprocess plus captured stdout/stderr.
type proxyProc struct {
	cmd    *exec.Cmd
	stdout *syncBuf
	stderr *syncBuf

	listenAddr  string
	adminAddr   string
	metricsAddr string

	cancel context.CancelFunc
	done   chan struct{}

	exitErr atomic.Value // error from cmd.Wait
}

// syncBuf is a goroutine-safe byte buffer used to collect subprocess output.
type syncBuf struct {
	mu  sync.Mutex
	buf []byte
}

func (b *syncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *syncBuf) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]byte, len(b.buf))
	copy(out, b.buf)
	return out
}

func (b *syncBuf) String() string { return string(b.Bytes()) }

// startProxy launches the pre-built proxy binary with the supplied flags
// (in addition to --listen / --admin-listen / --metrics-listen / -backend
// derived from ephemeral ports and the stub backend URL) and waits until
// it serves /ready on the main listener.
//
// The returned proxyProc is automatically killed via t.Cleanup; callers
// only need to call stop() early if they want to assert exit behavior.
func startProxy(t *testing.T, backend *stubBackend, extraArgs ...string) *proxyProc {
	t.Helper()
	ports := pickPorts(t, 3)
	listenAddr := fmt.Sprintf("127.0.0.1:%d", ports[0])
	adminAddr := fmt.Sprintf("127.0.0.1:%d", ports[1])
	metricsAddr := fmt.Sprintf("127.0.0.1:%d", ports[2])

	args := []string{
		"-backend=" + backend.URL(),
		"-listen=" + listenAddr,
		"-admin-listen=" + adminAddr,
	}
	args = append(args, extraArgs...)

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, proxyBinary, args...)
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(), "GOWORK=off")
	stdout := &syncBuf{}
	stderr := &syncBuf{}
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("startProxy: start: %v", err)
	}

	p := &proxyProc{
		cmd:         cmd,
		stdout:      stdout,
		stderr:      stderr,
		listenAddr:  listenAddr,
		adminAddr:   adminAddr,
		metricsAddr: metricsAddr,
		cancel:      cancel,
		done:        make(chan struct{}),
	}
	go func() {
		err := cmd.Wait()
		if err != nil {
			p.exitErr.Store(err)
		}
		close(p.done)
	}()

	t.Cleanup(func() {
		p.stop()
	})

	if err := waitForReady(t, "http://"+listenAddr+"/ready", 20*time.Second); err != nil {
		// Surface the captured stdout/stderr — operators debugging a flake
		// need the proxy's own boot log to figure out what failed.
		t.Fatalf("startProxy: proxy never became ready: %v\nstdout:\n%s\nstderr:\n%s",
			err, stdout.String(), stderr.String())
	}
	return p
}

// stop best-effort terminates the proxy and waits for the wait goroutine
// to observe exit. Safe to call multiple times.
func (p *proxyProc) stop() {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Signal(os.Interrupt)
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
	}
	if p.cancel != nil {
		p.cancel()
	}
}

// waitForReady polls the given URL until it returns HTTP 200 or the
// timeout elapses. Returns the last error on failure.
func waitForReady(t *testing.T, url string, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 1 * time.Second}
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
		} else {
			lastErr = err
		}
		time.Sleep(150 * time.Millisecond)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("timeout after %s", timeout)
	}
	return lastErr
}
