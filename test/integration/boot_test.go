//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestBinaryBootsWithDefaultsOnly asserts the secure-defaults outcome of
// Task 1: a fresh proxy launched with nothing but -backend serves proxy
// traffic on the main listener and exposes admin/debug endpoints on the
// dedicated loopback admin listener (--admin-listen, default 127.0.0.1:3101).
//
// In this test we override both addresses to ephemeral ports, but the
// invariant under test is identical: the admin port must accept TCP
// connections (proves the dedicated aux listener wired up), while requests
// to admin/debug paths on the MAIN listener must miss (proves admin/debug
// did not bleed onto the public proxy port).
func TestBinaryBootsWithDefaultsOnly(t *testing.T) {
	backend := startStubBackend(t)
	p := startProxy(t, backend)

	// Main listener — /ready already returned 200 in startProxy.
	// Spot-check the proxy is serving Loki API surfaces.
	resp, err := http.Get("http://" + p.listenAddr + "/loki/api/v1/labels")
	if err != nil {
		t.Fatalf("main listener /labels: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("main listener /labels: expected 200, got %d", resp.StatusCode)
	}

	// Admin listener — TCP-connectable. Default admin mux only registers
	// routes when --server.register-instrumentation=true (and we haven't
	// passed it), so any path will return 404. A 404 over HTTP still proves
	// the dedicated aux listener is up; what we are guarding against is
	// connection-refused on this port, which would indicate the listener
	// never bound.
	adminResp, err := http.Get("http://" + p.adminAddr + "/admin/anything")
	if err != nil {
		t.Fatalf("admin listener dial: %v\nproxy stderr:\n%s", err, p.stderr.String())
	}
	_ = adminResp.Body.Close()
	if adminResp.StatusCode != http.StatusNotFound {
		t.Fatalf("admin listener: expected 404 for unregistered path (proves listener is up), got %d", adminResp.StatusCode)
	}

	// Main listener must NOT serve /admin/* — admin endpoints moved off the
	// public port when no admin-auth-token is configured. The proxy mux
	// returns 404 for /admin/* (not registered) AND 401/404 for /debug/*.
	// We assert 404 specifically, which is what http.ServeMux returns for
	// an unregistered path.
	mainAdminResp, err := http.Get("http://" + p.listenAddr + "/admin/anything")
	if err != nil {
		t.Fatalf("main listener admin probe: %v", err)
	}
	_ = mainAdminResp.Body.Close()
	if mainAdminResp.StatusCode != http.StatusNotFound {
		t.Fatalf("main listener /admin/*: expected 404 (admin moved off public port), got %d", mainAdminResp.StatusCode)
	}

	// Non-loopback bind check: dial the admin port from 0.0.0.0 (which on
	// macOS/Linux resolves to a different interface than 127.0.0.1 IF the
	// process bound to loopback only). We do this by extracting the port
	// and trying to connect via a non-loopback address only when one is
	// available; this is a soft check because some CI hosts have no
	// non-loopback IPv4. We skip silently when no candidate exists.
	if hostIP := firstNonLoopbackIPv4(); hostIP != "" {
		_, portStr, _ := net.SplitHostPort(p.adminAddr)
		dialTarget := net.JoinHostPort(hostIP, portStr)
		conn, err := net.DialTimeout("tcp", dialTarget, 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			t.Fatalf("admin listener accepted connection on non-loopback %s — expected loopback-only bind", dialTarget)
		}
		// Any error is acceptable here (connection refused, timeout, etc.)
		// — the point is that the admin port is unreachable from this host's
		// public interface.
	}
}

// firstNonLoopbackIPv4 returns a non-loopback IPv4 address belonging to a
// running interface on this machine, or "" if none is available.
func firstNonLoopbackIPv4() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, a := range addrs {
		ipn, ok := a.(*net.IPNet)
		if !ok || ipn.IP.IsLoopback() {
			continue
		}
		v4 := ipn.IP.To4()
		if v4 == nil {
			continue
		}
		return v4.String()
	}
	return ""
}

// TestBinaryFailsWhenPeerCacheLacksToken asserts the Task 2 outcome:
// peer cache configured (--peer-discovery + --peer-static) without a
// shared --peer-auth-token and without --peer-insecure-ip-allowlist must
// refuse to start. The error message must name the missing flag so
// operators can fix the misconfiguration without grepping docs.
func TestBinaryFailsWhenPeerCacheLacksToken(t *testing.T) {
	backend := startStubBackend(t)
	defer backend.server.Close()

	// Ephemeral listen address — we expect the process to exit before any
	// listener is bound, but use a free port anyway so an accidental boot
	// would not collide with another local proxy.
	ports := pickPorts(t, 2)
	listenAddr := fmt.Sprintf("127.0.0.1:%d", ports[0])
	adminAddr := fmt.Sprintf("127.0.0.1:%d", ports[1])

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, proxyBinary,
		"-backend="+backend.URL(),
		"-listen="+listenAddr,
		"-admin-listen="+adminAddr,
		"-peer-discovery=static",
		"-peer-static=10.0.0.1:3100",
	)
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, _ := cmd.CombinedOutput()

	if cmd.ProcessState == nil {
		t.Fatalf("expected process to exit; ProcessState is nil")
	}
	if cmd.ProcessState.ExitCode() == 0 {
		t.Fatalf("expected non-zero exit when peer cache is configured without --peer-auth-token, got 0\noutput:\n%s", out)
	}
	if !bytes.Contains(out, []byte("peer-auth-token")) {
		t.Fatalf("error output should name the missing flag --peer-auth-token; got:\n%s", out)
	}
}

// TestMetadataDefaultLookbackInjectsStartEnd asserts the Task 3 outcome:
// when --metadata-default-lookback is non-zero and the client omits both
// start and end, the proxy injects a bounded window before forwarding the
// metadata query to the backend.
//
// We boot with a 1h lookback, send /loki/api/v1/series with no time
// bounds, and inspect what the stub backend received on
// /select/logsql/streams. We probe /series rather than /labels because
// handleLabels additionally caps the synchronous VL fetch to
// metadataMaxFieldNamesWindow (5m) for fast initial response, which
// obscures the lookback boundary; handleSeries injects the lookback and
// forwards start/end unmodified, which is the cleanest assertion surface
// for the lookback injection itself (see applyDefaultMetadataLookback).
func TestMetadataDefaultLookbackInjectsStartEnd(t *testing.T) {
	backend := startStubBackend(t)
	p := startProxy(t, backend,
		"--metadata-default-lookback=1h",
	)

	// Capture the wall-clock window the proxy SHOULD inject relative to.
	// applyDefaultMetadataLookback uses time.Now() when called, so we
	// bracket the request with our own observation of wall time.
	windowMin := time.Now().Add(-1*time.Hour - 5*time.Second)
	resp, err := http.Get("http://" + p.listenAddr + "/loki/api/v1/series?match%5B%5D=%7Bjob%3D%22x%22%7D")
	if err != nil {
		t.Fatalf("/series: %v", err)
	}
	_ = resp.Body.Close()
	windowMax := time.Now().Add(5 * time.Second)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/series: expected 200, got %d", resp.StatusCode)
	}

	// Wait briefly for the backend handler to record the call. The
	// proxy's handler chain is synchronous on the foreground path, so the
	// call typically lands before /series returns, but we still give it a
	// short window to cover scheduling jitter.
	var captured capturedRequest
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c, ok := backend.snapshot("/select/logsql/streams")
		if ok {
			captured = c
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if captured.path == "" {
		t.Fatalf("backend never received /select/logsql/streams; backend stdout:\n%s", p.stderr.String())
	}

	startVals := captured.query["start"]
	endVals := captured.query["end"]
	if len(startVals) == 0 || len(endVals) == 0 {
		t.Fatalf("expected backend to receive start and end params (default-lookback injection); got query=%v", captured.query)
	}

	startNs, err := strconv.ParseInt(startVals[0], 10, 64)
	if err != nil {
		t.Fatalf("start param %q not a nanosecond integer: %v", startVals[0], err)
	}
	endNs, err := strconv.ParseInt(endVals[0], 10, 64)
	if err != nil {
		t.Fatalf("end param %q not a nanosecond integer: %v", endVals[0], err)
	}
	startTime := time.Unix(0, startNs)
	endTime := time.Unix(0, endNs)

	if startTime.Before(windowMin) || startTime.After(windowMax) {
		t.Fatalf("injected start=%s is outside expected window [%s, %s]", startTime, windowMin, windowMax)
	}
	if endTime.Before(windowMin) || endTime.After(windowMax) {
		t.Fatalf("injected end=%s is outside expected window [%s, %s]", endTime, windowMin, windowMax)
	}
	if !endTime.After(startTime) {
		t.Fatalf("expected end (%s) after start (%s)", endTime, startTime)
	}
	delta := endTime.Sub(startTime)
	if delta < 59*time.Minute || delta > 61*time.Minute {
		t.Fatalf("expected ~1h window between start and end, got %s", delta)
	}
}

// TestDebugLogsRedactedByDefault asserts the Task 4 outcome: when the
// proxy runs at --log-level=debug, DEBUG-severity log lines covering
// query handling (query request, translated query, VL request, etc.)
// never include the raw LogQL/LogsQL string. Instead they include a
// deterministic fingerprint (sha256:<8hex> len=<n>) emitted by
// redactQuery.
//
// We restrict the assertion to DEBUG-severity log entries on purpose:
// the INFO-level frontend access log intentionally records a truncated
// `loki.query` field for operator audit + per-client telemetry, which is
// out of scope for the secure-defaults debug-log redaction work (the
// spec language is "Debug logs no longer include raw LogQL/LogsQL or
// backend query params by default."). Conflating the two would make the
// test fail for reasons unrelated to the hardening that Task 4 shipped.
func TestDebugLogsRedactedByDefault(t *testing.T) {
	backend := startStubBackend(t)
	p := startProxy(t, backend,
		"--log-level=debug",
	)

	// The marker is distinctive enough that any leak would be unambiguous:
	// an ALL-CAPS string unlikely to collide with anything the proxy or
	// stub emits on its own.
	const marker = "CUSTOMERSECRETMARKER"
	query := fmt.Sprintf(`{job="x"}|=%q`, marker)
	url := "http://" + p.listenAddr + "/loki/api/v1/query?query=" + queryEscape(query)

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	_ = resp.Body.Close()
	// The query may succeed (200) or surface a translation error; either
	// outcome still goes through redactQuery on the debug log path. We do
	// not assert the status code; we assert on the captured logs.

	// Wait until we see a "query request" debug line (the one that goes
	// through redactQuery in handleQuery) before asserting on redaction.
	// Without this wait, scheduling jitter can make the assertion run
	// before any query-handler log is emitted.
	deadline := time.Now().Add(5 * time.Second)
	var combined string
	for time.Now().Before(deadline) {
		combined = p.stdout.String() + p.stderr.String()
		if strings.Contains(combined, `"body":"query request"`) || strings.Contains(combined, `"body":"VL request"`) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	debugLines := extractDebugLines(combined)
	if len(debugLines) == 0 {
		t.Fatalf("no DEBUG-severity log lines captured; --log-level=debug may not have taken effect\nlogs (head):\n%s",
			headN(combined, 4000))
	}

	// Assert NO DEBUG-severity line contains the raw marker.
	for _, line := range debugLines {
		if strings.Contains(line, marker) {
			t.Fatalf("DEBUG-severity log leaked raw query marker %q (redaction broken)\nline:\n%s",
				marker, line)
		}
	}
	// Assert AT LEAST ONE DEBUG-severity line contains the redacted
	// fingerprint marker. This is the evidence that redactQuery actually
	// fired on a query path (rather than being silently bypassed).
	sawFingerprint := false
	for _, line := range debugLines {
		if strings.Contains(line, "sha256:") {
			sawFingerprint = true
			break
		}
	}
	if !sawFingerprint {
		t.Fatalf("expected sha256: fingerprint in DEBUG-severity logs (redactQuery), not found\ndebug lines (head):\n%s",
			headN(strings.Join(debugLines, "\n"), 4000))
	}
}

// extractDebugLines returns the subset of newline-delimited log lines
// emitted at DEBUG severity by the proxy's structured logger. The proxy
// uses an slog handler that encodes `"severity":{"text":"DEBUG"...}` for
// debug records; we match on that fragment rather than parsing each line
// as JSON to keep the test independent of the encoder's field ordering.
func extractDebugLines(combined string) []string {
	var out []string
	for _, line := range strings.Split(combined, "\n") {
		if strings.Contains(line, `"severity":{"text":"DEBUG"`) {
			out = append(out, line)
		}
	}
	return out
}

// queryEscape is a tiny wrapper that escapes a LogQL query string for
// inclusion in a URL query string. We avoid url.QueryEscape's import
// churn at the top of the file by inlining the call here.
func queryEscape(s string) string {
	// Use the stdlib via the existing import indirection: net/url is not
	// imported in this file, so we fall back to a minimal escaper that
	// covers the characters our marker query contains (=, ", {, }, |, space).
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~':
			b.WriteByte(c)
		default:
			const hex = "0123456789ABCDEF"
			b.WriteByte('%')
			b.WriteByte(hex[c>>4])
			b.WriteByte(hex[c&0x0F])
		}
	}
	return b.String()
}

// headN returns the first n bytes of s; useful for trimming test failure
// output without losing the leading context that points at the bug.
func headN(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "...[truncated]"
}
