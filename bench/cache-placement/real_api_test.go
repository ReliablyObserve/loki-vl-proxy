// Opt-in real-HTTP cache proof. Normal go test runs only the small helper tests.
// Use -perf=setup|probe|measure|scaling-probe|scaling|cleanup; result methodology contains commands.
package cacheplacement

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"
)

// Find the repository even when go test starts in this package directory.
func defaultRoot() string {
	path, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(path, "go.mod")); err == nil {
			if _, err := os.Stat(filepath.Join(path, "cmd", "proxy", "main.go")); err == nil {
				return path
			}
		}
		parent := filepath.Dir(path)
		if parent == path {
			return "."
		}
		path = parent
	}
}

var root = defaultRoot()
var home = filepath.Join(os.TempDir(), "cache-placement-perf-proof")
var baseline string
var candidate string

var project = "cacheplacementperf1030"
var lokiPort = 54101
var vlPort = 54428
var proxyPortBase = 55200
var baselineRef string
var candidateRef string
var lokiImage = "grafana/loki:3.7.9"
var vlImage = "victoriametrics/victoria-logs:v1.52.0"

const token = "isolated-cache-perf-fixture"

func vlBase() string   { return "http://127.0.0.1:" + number(vlPort) }
func lokiBase() string { return "http://127.0.0.1:" + number(lokiPort) }
func init() {
	flag.StringVar(&project, "perf-project", project, "isolated Docker Compose project")
	flag.IntVar(&lokiPort, "perf-loki-port", lokiPort, "isolated Loki host port")
	flag.IntVar(&vlPort, "perf-vl-port", vlPort, "isolated VictoriaLogs host port")
	flag.IntVar(&proxyPortBase, "perf-proxy-port-base", proxyPortBase, "first proxy port; each node uses a ten-port block")
	flag.StringVar(&baselineRef, "perf-baseline-ref", "", "operator-supplied source identity for baseline binary")
	flag.StringVar(&candidateRef, "perf-candidate-ref", "", "operator-supplied source identity for candidate binary")
	flag.StringVar(&lokiImage, "perf-loki-image", lokiImage, "pinned Loki image")
	flag.StringVar(&vlImage, "perf-vl-image", vlImage, "pinned VictoriaLogs image")
	flag.StringVar(&root, "perf-root", root, "repository root for pinned backend configuration")
	flag.StringVar(&home, "perf-output", home, "isolated proof artifact directory")
	flag.StringVar(&baseline, "perf-baseline", "", "baseline executable")
	flag.StringVar(&candidate, "perf-candidate", "", "candidate executable")
}

var mismatchID atomic.Uint64
var runID = time.Now().UTC().Format("20060102T150405.000000000Z")
var operationFlag = flag.String("perf", "", "operation: setup, probe, measure, scaling-probe, scaling-counts, scaling, cleanup")
var controlFlag = flag.Bool("perf-control", false, "substitute baseline for candidate")
var quietFlag = flag.Bool("perf-quiet", false, "explicit quiet-host authorization for timing runs")
var profileFlag = flag.String("perf-profile", "", "optional fit or pressure profile filter")
var scalingProfileFlag = flag.String("perf-scaling-profile", "fit", "fixed-owner reader workload: fit or pool-pressure (90 metadata identities, Tier0 disabled)")
var rfFlag = flag.Int("perf-rf", 2, "peer replication factor")
var client = &http.Client{Timeout: 90 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 16, DisableCompression: true}}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func number(n int) string { return strconv.Itoa(n) }
func save(t *testing.T, path string, v any) {
	t.Helper()
	b, err := json.MarshalIndent(v, "", "  ")
	must(t, err)
	must(t, os.WriteFile(path, b, 0600))
}
func command(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.Command("rtk", args...)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %v: %.3000s", args, err, out)
	}
	return string(out)
}
func fetch(method, target string, body []byte, headers map[string]string) (int, []byte, http.Header, error) {
	req, err := http.NewRequest(method, target, bytes.NewReader(body))
	if err != nil {
		return 0, nil, nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	b, err := readBoundedBody(resp.Body, 128<<20)
	return resp.StatusCode, b, resp.Header, err
}
func readBoundedBody(r io.Reader, limit int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, fmt.Errorf("response exceeds %d bytes", limit)
	}
	return b, nil
}
func get(t *testing.T, target string) (int, []byte) {
	t.Helper()
	s, b, _, err := fetch("GET", target, nil, nil)
	must(t, err)
	return s, b
}
func ready(t *testing.T, target string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		s, _, _, err := fetch("GET", target, nil, nil)
		if err == nil && s == 200 {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("not ready: %s", target)
}
func percentile(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	v := append([]float64(nil), xs...)
	sort.Float64s(v)
	idx := float64(len(v)-1) * p
	lo := int(idx)
	hi := int(math.Ceil(idx))
	return v[lo] + (v[hi]-v[lo])*(idx-float64(lo))
}

// Correctness ignores only execution statistics and result-series ordering.
func canonical(b []byte) (string, error) {
	var value any
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if err := d.Decode(&value); err != nil {
		return "", err
	}
	var trailing any
	if err := d.Decode(&trailing); err != io.EOF {
		return "", fmt.Errorf("response must contain one JSON value: %v", err)
	}
	if top, ok := value.(map[string]any); ok {
		if data, ok := top["data"].(map[string]any); ok {
			delete(data, "stats")
			if results, ok := data["result"].([]any); ok {
				typ, _ := data["resultType"].(string)
				for _, r := range results {
					m, ok := r.(map[string]any)
					if !ok {
						continue
					}
					if typ == "matrix" || typ == "vector" {
						normalize := func(p any) {
							a, ok := p.([]any)
							if !ok || len(a) != 2 {
								return
							}
							if s, ok := a[1].(string); ok {
								if f, e := strconv.ParseFloat(s, 64); e == nil {
									a[1] = strconv.FormatFloat(f, 'g', -1, 64)
								}
							}
						}
						normalize(m["value"])
						if points, ok := m["values"].([]any); ok {
							for _, p := range points {
								normalize(p)
							}
						}
					}
				}
				sort.SliceStable(results, func(i, j int) bool {
					a, ok1 := results[i].(map[string]any)
					b, ok2 := results[j].(map[string]any)
					if !ok1 || !ok2 {
						return false
					}
					label := "metric"
					if typ == "streams" {
						label = "stream"
					}
					x, _ := json.Marshal(a[label])
					y, _ := json.Marshal(b[label])
					return string(x) < string(y)
				})
			}
		} else if data, ok := top["data"].([]any); ok {
			stringsOnly := true
			for _, v := range data {
				if _, ok := v.(string); !ok {
					stringsOnly = false
				}
			}
			if stringsOnly {
				sort.Slice(data, func(i, j int) bool { return data[i].(string) < data[j].(string) })
			}
		}
	}
	value, err := normalizeNumbers(value)
	if err != nil {
		return "", err
	}
	out, err := json.Marshal(value)
	return string(out), err
}

// Keep exact numeric values while ignoring JSON decimal/exponent spelling.
func normalizeNumbers(v any) (any, error) {
	switch n := v.(type) {
	case json.Number:
		parts := strings.SplitN(strings.ToLower(n.String()), "e", 2)
		exponent := 0
		if len(parts) == 2 {
			var err error
			exponent, err = strconv.Atoi(parts[1])
			if err != nil || exponent < -2048 || exponent > 2048 {
				return nil, fmt.Errorf("numeric exponent outside harness bound")
			}
		}
		fraction := 0
		if dot := strings.IndexByte(parts[0], '.'); dot >= 0 {
			fraction = len(parts[0]) - dot - 1
		}
		if len(parts[0]) > 4096 {
			return nil, fmt.Errorf("number outside harness bound")
		}
		rat, ok := new(big.Rat).SetString(n.String())
		if !ok {
			return nil, fmt.Errorf("invalid number")
		}
		scale := max(fraction-exponent, 0)
		raw := rat.FloatString(scale)
		if strings.Contains(raw, ".") {
			raw = strings.TrimRight(strings.TrimRight(raw, "0"), ".")
		}
		if raw == "-0" {
			raw = "0"
		}
		return json.Number(raw), nil
	case map[string]any:
		for k, x := range n {
			value, err := normalizeNumbers(x)
			if err != nil {
				return nil, err
			}
			n[k] = value
		}
		return n, nil
	case []any:
		for i, x := range n {
			value, err := normalizeNumbers(x)
			if err != nil {
				return nil, err
			}
			n[i] = value
		}
		return n, nil
	default:
		return v, nil
	}
}

type wireEvent struct {
	Kind          string     `json:"kind"`
	Node          int        `json:"node"`
	Method        string     `json:"method"`
	Path          string     `json:"path"`
	Query         url.Values `json:"query"`
	Status        int        `json:"status"`
	RequestBytes  int64      `json:"request_bytes"`
	ResponseBytes int64      `json:"response_bytes"`
	Seconds       float64    `json:"seconds"`
}
type wireTotals struct {
	VLCalls       int
	PeerCalls     int
	VLBytes       int64
	PeerBytes     int64
	PeerFetches   int
	PeerPushes    int
	PeerPushBytes int64
}
type networkMeter struct {
	mu      sync.Mutex
	pending int
	idle    chan struct{}
	active  map[net.Conn]bool
	read    atomic.Int64
	written atomic.Int64
}

func (m *networkMeter) beginLocked() {
	if m.pending == 0 {
		m.idle = make(chan struct{})
	}
	m.pending++
}
func (m *networkMeter) endLocked() {
	m.pending--
	if m.pending == 0 {
		close(m.idle)
	}
}
func (m *networkMeter) connectionState(conn net.Conn, state http.ConnState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if state == http.StateActive {
		if !m.active[conn] {
			if m.active == nil {
				m.active = map[net.Conn]bool{}
			}
			m.active[conn] = true
			m.beginLocked()
		}
	} else if state == http.StateIdle || state == http.StateClosed || state == http.StateHijacked {
		if m.active[conn] {
			delete(m.active, conn)
			m.endLocked()
		}
	}
}
func (m *networkMeter) wait(ctx context.Context) error {
	for {
		m.mu.Lock()
		pending, idle := m.pending, m.idle
		m.mu.Unlock()
		if pending == 0 {
			return nil
		}
		select {
		case <-idle:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type networkBytes struct {
	RequestHTTPBytes  int64
	ResponseHTTPBytes int64
}
type measuredConn struct {
	net.Conn
	meter *networkMeter
}

func (c *measuredConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.meter.read.Add(int64(n))
	return n, err
}
func (c *measuredConn) Write(b []byte) (int, error) {
	c.meter.mu.Lock()
	c.meter.beginLocked()
	c.meter.mu.Unlock()
	n, err := c.Conn.Write(b)
	c.meter.mu.Lock()
	c.meter.written.Add(int64(n))
	c.meter.endLocked()
	c.meter.mu.Unlock()
	return n, err
}

type measuredListener struct {
	net.Listener
	meter *networkMeter
}

func (l *measuredListener) Accept() (net.Conn, error) {
	c, e := l.Listener.Accept()
	if e != nil {
		return nil, e
	}
	return &measuredConn{Conn: c, meter: l.meter}, nil
}

type recorder struct {
	mu      sync.Mutex
	prefix  []wireTotals
	meters  map[string]*networkMeter
	path    string
	file    *os.File
	buffer  *bufio.Writer
	encoder *json.Encoder
	err     error
}

func newRecorder(t *testing.T, name string) *recorder {
	t.Helper()
	must(t, os.MkdirAll(filepath.Join(home, "wire"), 0700))
	path := filepath.Join(home, "wire", name+"-"+runID+".jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	must(t, err)
	r := &recorder{prefix: []wireTotals{{}}, path: path, file: f, buffer: bufio.NewWriterSize(f, 1<<20)}
	r.encoder = json.NewEncoder(r.buffer)
	t.Cleanup(func() { r.mu.Lock(); defer r.mu.Unlock(); _ = r.buffer.Flush(); _ = f.Close() })
	return r
}
func (r *recorder) add(e wireEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v := r.prefix[len(r.prefix)-1]
	if e.Kind == "vl" {
		v.VLCalls++
		v.VLBytes += e.ResponseBytes
	} else if e.Kind == "peer" {
		v.PeerCalls++
		v.PeerBytes += e.ResponseBytes
		if e.Method == "GET" && e.Path == "/_cache/get" {
			v.PeerFetches++
		}
		if e.Method == "POST" && e.Path == "/_cache/set" {
			v.PeerPushes++
			v.PeerPushBytes += max(e.RequestBytes, 0)
		}
	}
	r.prefix = append(r.prefix, v)
	if r.err == nil {
		r.err = r.encoder.Encode(e)
	}
}

// A completed handler is not a completed HTTP response: net/http flushes after
// ServeHTTP returns. Pair ledger and wire snapshots only after response/Write
// completion. Idle keep-alive reads do not participate in this barrier.
func (r *recorder) snapshot(ctx context.Context) (int, map[string]networkBytes, error) {
	for {
		r.mu.Lock()
		names := make([]string, 0, len(r.meters))
		meters := make(map[string]*networkMeter, len(r.meters))
		for name, m := range r.meters {
			names = append(names, name)
			meters[name] = m
		}
		r.mu.Unlock()
		sort.Strings(names)
		for _, name := range names {
			if err := meters[name].wait(ctx); err != nil {
				return 0, nil, fmt.Errorf("observer %s completion: %w", name, err)
			}
		}
		r.mu.Lock()
		busy := false
		for _, name := range names {
			meters[name].mu.Lock()
			busy = busy || meters[name].pending != 0
		}
		out := map[string]networkBytes{}
		if !busy {
			for _, name := range names {
				m := meters[name]
				out[name] = networkBytes{RequestHTTPBytes: m.read.Load(), ResponseHTTPBytes: m.written.Load()}
			}
		}
		count := len(r.prefix) - 1
		for i := len(names) - 1; i >= 0; i-- {
			meters[names[i]].mu.Unlock()
		}
		r.mu.Unlock()
		if !busy {
			return count, out, nil
		}
		if err := ctx.Err(); err != nil {
			return 0, nil, err
		}
	}
}
func (r *recorder) capture(t *testing.T) (int, map[string]networkBytes) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	count, network, err := r.snapshot(ctx)
	must(t, err)
	return count, network
}
func (r *recorder) network(t *testing.T) map[string]networkBytes {
	t.Helper()
	_, network := r.capture(t)
	return network
}
func networkDelta(before, after map[string]networkBytes) map[string]networkBytes {
	out := map[string]networkBytes{}
	for k, v := range after {
		b := before[k]
		out[k] = networkBytes{RequestHTTPBytes: v.RequestHTTPBytes - b.RequestHTTPBytes, ResponseHTTPBytes: v.ResponseHTTPBytes - b.ResponseHTTPBytes}
	}
	return out
}
func (r *recorder) count() int { r.mu.Lock(); defer r.mu.Unlock(); return len(r.prefix) - 1 }
func (r *recorder) between(start, end int) wireTotals {
	r.mu.Lock()
	defer r.mu.Unlock()
	a, b := r.prefix[start], r.prefix[end]
	return wireTotals{VLCalls: b.VLCalls - a.VLCalls, PeerCalls: b.PeerCalls - a.PeerCalls, VLBytes: b.VLBytes - a.VLBytes, PeerBytes: b.PeerBytes - a.PeerBytes, PeerFetches: b.PeerFetches - a.PeerFetches, PeerPushes: b.PeerPushes - a.PeerPushes, PeerPushBytes: b.PeerPushBytes - a.PeerPushBytes}
}
func (r *recorder) flush(t *testing.T) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	must(t, r.err)
	must(t, r.buffer.Flush())
}

type responseWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (w *responseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *responseWriter) WriteHeader(s int)           { w.status = s; w.ResponseWriter.WriteHeader(s) }
func (w *responseWriter) Write(b []byte) (int, error) {
	n, e := w.ResponseWriter.Write(b)
	w.bytes += int64(n)
	return n, e
}
func observer(t *testing.T, port int, target, kind string, node int, rec *recorder) *http.Server {
	t.Helper()
	dest, err := url.Parse(target)
	must(t, err)
	proxy := httputil.NewSingleHostReverseProxy(dest)
	proxy.Transport = &http.Transport{MaxIdleConnsPerHost: 32, DisableCompression: true}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		query := r.URL.Query()
		if r.Body != nil && r.ContentLength > 0 {
			raw, e := io.ReadAll(io.LimitReader(r.Body, 16<<20))
			if e != nil {
				http.Error(w, e.Error(), 400)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(raw))
			if strings.Contains(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded") {
				if form, e := url.ParseQuery(string(raw)); e == nil {
					for k, v := range form {
						query[k] = append(query[k], v...)
					}
				}
			}
		}
		rw := &responseWriter{ResponseWriter: w, status: 200}
		proxy.ServeHTTP(rw, r)
		rec.add(wireEvent{Kind: kind, Node: node, Method: r.Method, Path: r.URL.Path, Query: query, Status: rw.status, RequestBytes: r.ContentLength, ResponseBytes: rw.bytes, Seconds: time.Since(start).Seconds()})
	})
	ln, err := net.Listen("tcp", "127.0.0.1:"+number(port))
	must(t, err)
	meter := &networkMeter{}
	srv := &http.Server{Addr: ln.Addr().String(), Handler: h, ReadHeaderTimeout: 5 * time.Second, ConnState: meter.connectionState}
	rec.mu.Lock()
	if rec.meters == nil {
		rec.meters = map[string]*networkMeter{}
	}
	rec.meters[fmt.Sprintf("%s-node%d", kind, node)] = meter
	rec.mu.Unlock()
	go srv.Serve(&measuredListener{Listener: ln, meter: meter})
	t.Cleanup(func() { srv.Close() })
	return srv
}
func ports(i int) (int, int, int) { base := proxyPortBase + 10*i; return base, base + 2, base + 3 }

type process struct {
	Exit    map[string]any
	done    chan struct{}
	exitErr error
	cmd     *exec.Cmd
	log     *os.File
	url     string
	dir     string
}

func (p *process) stop() {
	if p.cmd == nil {
		return
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.done:
	case <-time.After(20 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
	}
	if state := p.cmd.ProcessState; state != nil {
		if usage, ok := state.SysUsage().(*syscall.Rusage); ok {
			rss := usage.Maxrss
			if runtime.GOOS == "linux" {
				rss *= 1024
			}
			p.Exit = map[string]any{"pid": p.cmd.Process.Pid, "dir": p.dir, "peak_rss_bytes": rss, "rss_source": "kernel child rusage Maxrss at exit", "cpu_user_seconds": float64(usage.Utime.Sec) + float64(usage.Utime.Usec)/1e6, "cpu_system_seconds": float64(usage.Stime.Sec) + float64(usage.Stime.Usec)/1e6}
		}
	}
	_ = p.log.Close()
	p.cmd = nil
}

type config struct {
	Pprof     bool
	Profile   string
	Entries   int
	Bytes     int
	DiskBytes int64
	Shards    int
	RF        int
	Edge      bool
	Repeats   int
}

func flags(c config, i int, dir string) []string { return flagsForRing(c, i, dir, 3, false) }
func flagsForRing(c config, i int, dir string, ringNodes int, reader bool) []string {
	port, _, _ := ports(i)
	_, back, _ := ports(0)
	peers := []string{}
	for n := 0; n < ringNodes; n++ {
		_, _, p := ports(n)
		peers = append(peers, "127.0.0.1:"+number(p))
	}
	args := []string{
		"-listen=127.0.0.1:" + number(port), "-admin-listen=127.0.0.1:" + number(port+1),
		"-backend=http://127.0.0.1:" + number(back), "-log-level=error",
		"-cache-ttl=5m", "-labels-cache-ttl=5m", "-cache-max=" + number(c.Entries), "-cache-max-bytes=" + number(c.Bytes),
		"-compat-cache-enabled=" + strconv.FormatBool(c.Edge),
		"-disk-cache-path=" + filepath.Join(dir, "cache.db"), "-disk-cache-max-bytes=" + strconv.FormatInt(c.DiskBytes, 10),
		"-disk-cache-compress=true", "-disk-cache-flush-size=1000", "-disk-cache-flush-interval=1s",
		"-peer-self=127.0.0.1:" + number(port+3), "-peer-discovery=static", "-peer-static=" + strings.Join(peers, ","),
		"-peer-auth-token=" + token, "-peer-write-through=true", "-peer-replication-factor=" + number(c.RF),
		"-peer-timeout=200ms", "-labels-cache-warm=false", "-patterns-enabled=false", "-label-values-indexed-cache=false",
		"-server.register-instrumentation=true", "-server.admin-auth-token=" + token,
		"-rate-limit-per-second=0", "-rate-limit-burst=0",
	}
	if c.Pprof {
		args = append(args, "-server.enable-pprof=true")
	}
	if reader {
		filtered := args[:0]
		for _, arg := range args {
			if !strings.HasPrefix(arg, "-disk-cache-path=") {
				filtered = append(filtered, arg)
			}
		}
		args = filtered
	}
	return args

}
func launch(t *testing.T, binary string, c config, i int, dir string) *process {
	return launchRing(t, binary, c, i, dir, 3, false)
}
func launchRing(t *testing.T, binary string, c config, i int, dir string, ringNodes int, reader bool, extraArgs ...string) *process {
	t.Helper()
	listen, _, _ := ports(i)
	for _, port := range []int{listen, listen + 1} {
		ln, err := net.Listen("tcp", "127.0.0.1:"+number(port))
		if err != nil {
			t.Fatalf("refusing occupied proxy port%d: %v", port, err)
		}
		must(t, ln.Close())
	}
	must(t, os.MkdirAll(dir, 0700))
	f, err := os.OpenFile(filepath.Join(dir, "proxy.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	must(t, err)
	args := append(flagsForRing(c, i, dir, ringNodes, reader), extraArgs...)
	cmd := exec.Command(binary, args...)
	cmd.Stdout = f
	cmd.Stderr = f
	must(t, cmd.Start())
	port, _, _ := ports(i)
	p := &process{cmd: cmd, log: f, url: "http://127.0.0.1:" + number(port), dir: dir, done: make(chan struct{})}
	go func() { p.exitErr = cmd.Wait(); close(p.done) }()
	t.Cleanup(p.stop)
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-p.done:
			out, _ := os.ReadFile(filepath.Join(dir, "proxy.log"))
			t.Fatalf("proxy exited: %v %.2000s", p.exitErr, out)
		default:
		}
		status, _, _, err := fetch("GET", p.url+"/ready", nil, nil)
		if err == nil && status == 200 {
			return p
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatal("proxy not ready")
	return p
}
func usefulMetric(name string) bool {
	if _, ok := dlvParseLayerMetric(name); ok {
		return true
	}
	base := strings.SplitN(name, "{", 2)[0]
	switch base {
	case "loki_vl_proxy_process_cpu_seconds_total", "loki_vl_proxy_process_resident_memory_bytes",
		"loki_vl_proxy_cache_tier_requests_total", "loki_vl_proxy_cache_tier_hits_total",
		"loki_vl_proxy_cache_tier_misses_total", "loki_vl_proxy_cache_tier_evictions_total",
		"loki_vl_proxy_cache_backend_fallthrough_total", "loki_vl_proxy_cache_objects", "loki_vl_proxy_cache_bytes",
		"loki_vl_proxy_peer_cache_fetches_total", "loki_vl_proxy_peer_cache_hits_total",
		"loki_vl_proxy_peer_cache_backup_hits_total", "loki_vl_proxy_peer_cache_misses_total",
		"loki_vl_proxy_peer_cache_errors_total", "loki_vl_proxy_peer_cache_write_through_pushes_total",
		"loki_vl_proxy_peer_cache_write_through_errors_total", "loki_vl_proxy_coalesced_saved_total":
		return true
	default:
		return false
	}
}
func scrape(target string) (map[string]float64, error) {
	s, b, _, err := fetch("GET", target+"/metrics", nil, nil)
	if err != nil {
		return nil, err
	}
	if s != 200 {
		return nil, fmt.Errorf("metrics HTTP %d", s)
	}
	m := map[string]float64{}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		if v, e := strconv.ParseFloat(f[1], 64); e == nil {
			if usefulMetric(f[0]) {
				m[f[0]] = v
			}
		}
	}
	for _, key := range []string{"loki_vl_proxy_process_cpu_seconds_total", "loki_vl_proxy_process_resident_memory_bytes"} {
		if _, ok := m[key]; !ok {
			return nil, fmt.Errorf("metrics missing %s", key)
		}
	}
	return m, nil
}
func diskUsage(dir string) (int64, int64) {
	var size, allocated int64
	_ = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() && strings.HasSuffix(info.Name(), ".db") {
			size += info.Size()
			if stat, ok := info.Sys().(*syscall.Stat_t); ok {
				allocated += stat.Blocks * 512
			}
		}
		return nil
	})
	return size, allocated
}

type resources struct {
	Valid            bool
	Errors           map[string]string
	ExpectedNodes    int
	SampledNodes     int
	Metrics          map[string]float64
	FileBytes        int64
	AllocatedBytes   int64
	MemoryMetricKind string
	PerNode          map[string]map[string]float64
}

func sample(ps []*process) resources {
	r := resources{Valid: true, Errors: map[string]string{}, Metrics: map[string]float64{}, MemoryMetricKind: "RSS", PerNode: map[string]map[string]float64{}}
	if runtime.GOOS != "linux" {
		r.MemoryMetricKind = "Go MemStats.Sys; actual RSS unavailable"
	}
	for _, p := range ps {
		if p == nil || p.cmd == nil {
			continue
		}
		r.ExpectedNodes++
		metrics, err := scrape(p.url)
		if err != nil {
			r.Valid = false
			r.Errors[p.url] = err.Error()
			continue
		}
		r.SampledNodes++
		r.PerNode[p.url] = metrics
		for k, v := range metrics {
			r.Metrics[k] += v
		}
		s, a := diskUsage(p.dir)
		r.FileBytes += s
		r.AllocatedBytes += a
	}
	return r
}

type fixture struct {
	Probe     string
	End       int64
	RecentEnd int64
	Streams   int
	Lines     int
	SHA256    string
}

func compose(t *testing.T, args ...string) string {
	return command(t, append([]string{"docker", "compose", "-p", project, "-f", filepath.Join(home, "compose.json")}, args...)...)
}
func prepareCompose(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(root, "test/e2e-compat/loki-local-config.yaml"))
	must(t, err)
	raw = bytes.ReplaceAll(raw, []byte("align_queries_with_step: true"), []byte("align_queries_with_step: false"))
	raw = bytes.ReplaceAll(raw, []byte("max_cache_freshness_per_query: 0s"), []byte("max_cache_freshness_per_query: 10m"))
	must(t, os.WriteFile(filepath.Join(home, "loki.yaml"), raw, 0600))
	spec := map[string]any{"services": map[string]any{
		"loki":         map[string]any{"image": lokiImage, "user": "0:0", "ports": []string{fmt.Sprintf("127.0.0.1:%d:3100", lokiPort)}, "command": []string{"-config.file=/etc/loki/local-config.yaml"}, "volumes": []string{filepath.Join(home, "loki.yaml") + ":/etc/loki/local-config.yaml:ro"}, "mem_limit": "4g"},
		"victorialogs": map[string]any{"image": vlImage, "ports": []string{fmt.Sprintf("127.0.0.1:%d:9428", vlPort)}, "command": []string{"-storageDataPath=/vlogs", "-retentionPeriod=3d"}, "volumes": []string{"vl-data:/vlogs"}, "mem_limit": "2g"},
	}, "volumes": map[string]any{"vl-data": map[string]any{}}}
	save(t, filepath.Join(home, "compose.json"), spec)
}
func seed(t *testing.T, streams int) fixture {
	ready(t, vlBase()+"/health")
	ready(t, lokiBase()+"/ready")
	fx := fixture{Probe: "cacheperf-" + strconv.FormatInt(time.Now().UnixNano(), 10), End: time.Now().UTC().Truncate(time.Hour).Add(-26 * time.Hour).Unix(), RecentEnd: time.Now().UTC().Truncate(time.Minute).Add(-time.Minute).Unix(), Streams: streams}
	historyHours := 8
	if *dlvRangeHours > 0 {
		historyHours = *dlvRangeHours + 2
		fx.End = time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour).Unix()
	}
	post := func(target, content string, b []byte) {
		s, out, _, err := fetch("POST", target, b, map[string]string{"Content-Type": content})
		must(t, err)
		if s < 200 || s >= 300 {
			t.Fatalf("seed %s: %d %.400s", target, s, out)
		}
	}
	// Emit bounded Loki requests while generating streams. The callback consumes
	// each body synchronously, so no list of encoded batches is retained.
	lokiRequests, lokiMaxBytes := 0, 0
	push := lokiPushBatcher{maxBytes: 4 << 20, emit: func(body []byte) error {
		lokiRequests++
		if len(body) > lokiMaxBytes {
			lokiMaxBytes = len(body)
		}
		post(lokiBase()+"/loki/api/v1/push", "application/json", body)
		return nil
	}}
	var vl bytes.Buffer
	for shard := 0; shard < streams; shard++ {
		for _, recent := range []bool{false, true} {
			end, hours := fx.End, historyHours
			if recent {
				end, hours = fx.RecentEnd, 2
			}
			levels := []string{"info", "warn", "error"}
			for li, level := range levels {
				labels := map[string]string{"perf_probe": fx.Probe, "shard": number(shard), "app": []string{"api-gateway", "worker"}[shard%2], "env": "production", "detected_level": level}
				values := [][]string{}
				for at, j := end-int64(hours*3600)+int64(7+li*17), 0; at < end; at, j = at+180, j+1 {
					line := fmt.Sprintf("{\"level\":%q,\"method\":%q,\"status\":%d,\"value\":%d,\"message\":\"request served\"}", level, []string{"GET", "POST", "PUT"}[j%3], []int{200, 404, 500}[li], 1+j%7)
					if j%61 == 60 && (*dlvRangeHours == 0 || shard%2 == 1) {
						line = "malformed JSON fixture line"
					} // exercise DL V2's error path too
					stamp := at*1e9 + int64(shard)
					values = append(values, []string{strconv.FormatInt(stamp, 10), line})
					row := map[string]string{"_time": time.Unix(0, stamp).UTC().Format(time.RFC3339Nano), "_msg": line}
					for k, v := range labels {
						row[k] = v
					}
					b, err := json.Marshal(row)
					must(t, err)
					vl.Write(b)
					vl.WriteByte('\n')
					fx.Lines++
				}
				must(t, push.add(map[string]any{"stream": labels, "values": values}))
			}
		}
	}
	hash := sha256.Sum256(vl.Bytes())
	fx.SHA256 = hex.EncodeToString(hash[:])
	must(t, push.flush())
	t.Logf("Loki fixture ingestion: %d bounded requests, largest encoded body %d bytes (limit %d)", lokiRequests, lokiMaxBytes, push.maxBytes)
	post(vlBase()+"/insert/jsonline?_stream_fields=perf_probe,shard,app,env,detected_level", "application/stream+json", vl.Bytes())
	post(vlBase()+"/internal/force_flush", "text/plain", nil)
	post(lokiBase()+"/flush", "text/plain", nil)
	save(t, filepath.Join(home, "fixture.json"), fx)
	// Loki can initially return an empty metric range. Use nonce in a filter
	// so a cached empty answer cannot satisfy or indefinitely block readiness.
	deadline := time.Now().Add(4 * time.Minute)
	for time.Now().Before(deadline) {
		q := fmt.Sprintf("sum(count_over_time({perf_probe=%q} != %q [%dh]))", fx.Probe, "seed-"+strconv.FormatInt(time.Now().UnixNano(), 10), historyHours)
		path := "/loki/api/v1/query?" + url.Values{"query": {q}, "time": {strconv.FormatInt(fx.End*1e9, 10)}}.Encode()
		_, body := get(t, lokiBase()+path)
		var v struct {
			Data struct{ Result []struct{ Value []any } }
		}
		if json.Unmarshal(body, &v) == nil && len(v.Data.Result) == 1 && len(v.Data.Result[0].Value) == 2 {
			if count, err := strconv.Atoi(fmt.Sprint(v.Data.Result[0].Value[1])); err == nil && count == fx.Lines*historyHours/(historyHours+2) {
				return fx
			}
		}
		time.Sleep(time.Second)
	}
	t.Fatal("Loki historical fixture count did not converge exactly")
	return fx
}

type query struct {
	Name    string
	Path    string
	Headers map[string]string
}

func paths(fx fixture, c config, mode string, run int) []query {
	// Leave 30 minutes of seeded history beyond the initial window for unseen forward refreshes.
	end := fx.End - 1800
	if mode == "shifted" {
		end -= int64(run * 60)
	}
	if mode == "refresh" {
		end += int64((run + 1) * 60)
	}
	if mode == "recent-refresh" {
		end = fx.RecentEnd - int64((c.Repeats-1-run)*60)
	}
	selector := fmt.Sprintf("{perf_probe=%q,shard=~%q}", fx.Probe, "("+strings.Join(func() []string {
		v := []string{}
		for i := 0; i < c.Shards; i++ {
			v = append(v, number(i))
		}
		return v
	}(), "|")+")")
	makeRange := func(name, q string, hours int, headers map[string]string) query {
		v := url.Values{"query": {q}, "start": {strconv.FormatInt((end-int64(hours*3600))*1e9, 10)}, "end": {strconv.FormatInt(end*1e9, 10)}, "step": {"60"}}
		return query{Name: name, Path: "/loki/api/v1/query_range?" + v.Encode(), Headers: headers}
	}
	out := []query{
		makeRange("DLV1-json-volume", "sum by (detected_level) (count_over_time("+selector+" | json [1m]))", 1, map[string]string{"X-Query-Tags": "Source=grafana-lokiexplore-app"}),
		makeRange("DLV2-json-error", "sum by (detected_level) (count_over_time("+selector+" | json | __error__!=\"\" [1m]))", 6, nil),
		makeRange("DLV3-json-volume", "sum by (detected_level) (count_over_time("+selector+" | json [1m]))", 1, nil),
		makeRange("metric-count", "sum by (app) (count_over_time("+selector+"[1m]))", 1, nil),
	}
	log := makeRange("logs", selector+" |= \"served\"", 1, nil)
	u, _ := url.Parse(log.Path)
	v := u.Query()
	v.Del("step")
	v.Set("limit", "100")
	v.Set("direction", "backward")
	u.RawQuery = v.Encode()
	log.Path = u.String()
	out = append(out, log)
	for i := 0; i < c.Shards; i++ {
		v := url.Values{"query": {fmt.Sprintf("{perf_probe=%q,shard=%q}", fx.Probe, number(i))}, "start": {strconv.FormatInt((end-3600)*1e9, 10)}, "end": {strconv.FormatInt(end*1e9, 10)}}
		out = append(out, query{Name: "metadata-app-values-" + number(i), Path: "/loki/api/v1/label/app/values?" + v.Encode()})
	}
	if c.Profile == "fixed3owners-pool-pressure" {
		filtered := []query{}
		for _, q := range out {
			if strings.HasPrefix(q.Name, "metadata-") {
				filtered = append(filtered, q)
			}
		}
		return filtered
	}
	return out
}

type result struct {
	Source                    string
	Trial                     int
	Profile                   string
	Mode                      string
	Name                      string
	Run                       int
	Node                      int
	Path                      string
	Headers                   map[string]string
	RequestEndAgeSeconds      float64
	RecentTailEligible        bool
	MetadataFreshnessEligible bool
	ProxyMS                   float64
	LokiMS                    float64
	ProxyStatus               int
	LokiStatus                int
	Equal                     bool
	ComparisonError           string
	ProxySHA256               string
	LokiSHA256                string
	VLCalls                   int
	PeerCalls                 int
	VLResponseBytes           int64
	PeerResponseBytes         int64
	WireStart                 int
	WireEnd                   int
	LokiCache                 any
}

func sumWire(events []wireEvent) (vl, peer int, vb, pb int64) {
	for _, e := range events {
		if e.Kind == "vl" {
			vl++
			vb += e.ResponseBytes
		} else if e.Kind == "peer" {
			peer++
			pb += e.ResponseBytes
		}
	}
	return
}
func request(t *testing.T, p *process, node int, q query, source string, trial int, c config, mode string, run int, rec *recorder) result {
	t.Helper()
	r := result{Source: source, Trial: trial, Profile: c.Profile, Mode: mode, Name: q.Name, Run: run, Node: node, Path: q.Path, Headers: q.Headers, WireStart: rec.count()}
	if parsed, err := url.Parse(q.Path); err == nil {
		if end, err := strconv.ParseInt(parsed.Query().Get("end"), 10, 64); err == nil {
			r.RequestEndAgeSeconds = time.Since(time.Unix(0, end)).Seconds()
			r.RecentTailEligible = r.RequestEndAgeSeconds >= 0 && r.RequestEndAgeSeconds <= 120
			r.MetadataFreshnessEligible = r.RequestEndAgeSeconds >= 0 && r.RequestEndAgeSeconds <= 24*3600
		}
	}
	var proxy, loki []byte
	reference := func() {
		start := time.Now()
		s, b, _, e := fetch("GET", lokiBase()+q.Path, nil, q.Headers)
		r.LokiMS = float64(time.Since(start).Nanoseconds()) / 1e6
		r.LokiStatus = s
		loki = b
		if e != nil {
			r.ComparisonError = "reference: " + e.Error()
		}
	}
	measured := func() {
		start := time.Now()
		s, b, _, e := fetch("GET", p.url+q.Path, nil, q.Headers)
		r.ProxyMS = float64(time.Since(start).Nanoseconds()) / 1e6
		r.ProxyStatus = s
		proxy = b
		if e != nil {
			r.ComparisonError += " proxy: " + e.Error()
		}
	}
	if (run+node)%2 == 0 {
		reference()
		measured()
	} else {
		measured()
		reference()
	}
	r.WireEnd = rec.count()
	wire := rec.between(r.WireStart, r.WireEnd)
	r.VLCalls, r.PeerCalls, r.VLResponseBytes, r.PeerResponseBytes = wire.VLCalls, wire.PeerCalls, wire.VLBytes, wire.PeerBytes
	x, xe := canonical(proxy)
	y, ye := canonical(loki)
	r.Equal = xe == nil && ye == nil && r.ComparisonError == "" && r.ProxyStatus == r.LokiStatus && x == y
	if xe != nil || ye != nil {
		r.ComparisonError += fmt.Sprintf(" canonical proxy=%v Loki=%v", xe, ye)
	}
	h := sha256.Sum256(proxy)
	r.ProxySHA256 = hex.EncodeToString(h[:])
	h = sha256.Sum256(loki)
	r.LokiSHA256 = hex.EncodeToString(h[:])
	var payload map[string]any
	if json.Unmarshal(loki, &payload) == nil {
		if data, ok := payload["data"].(map[string]any); ok {
			if stats, ok := data["stats"].(map[string]any); ok {
				r.LokiCache = stats["cache"]
			}
		}
	}
	if !r.Equal {
		dir := filepath.Join(home, "mismatches", runID)
		stem := fmt.Sprintf("%s-%d-%s-%s-%d-%d-%d", source, trial, c.Profile, mode, run, node, mismatchID.Add(1)) + "-" + q.Name
		if err := os.MkdirAll(dir, 0700); err != nil {
			r.ComparisonError += " diagnostic: " + err.Error()
		} else {
			for name, body := range map[string][]byte{"proxy": proxy, "loki": loki} {
				if err := os.WriteFile(filepath.Join(dir, stem+"-"+name+".json"), body, 0600); err != nil {
					r.ComparisonError += " diagnostic: " + err.Error()
				}
			}
		}
	}
	return r
}

type phase struct {
	Config                config
	StartupWireTotals     *wireTotals
	TrialProcessLifetimes []map[string]any
	NetworkHTTPBytes      map[string]networkBytes
	PeerFetches           int
	PeerPushes            int
	PeerPushBodyBytes     int64
	VLCalls               int
	PeerCalls             int
	VLResponseBytes       int64
	PeerResponseBytes     int64
	WireDrainSeconds      float64
	Source                string
	Trial                 int
	Profile               string
	Mode                  string
	Before                resources
	After                 resources
	PeakFileBytes         int64
	PeakAllocatedBytes    int64
	PeakMemoryMetric      float64
	ProxyCPUSeconds       float64
	ValidResourceSamples  bool
	WireStart             int
	WireEnd               int
	Calls                 []result
	Seconds               float64
}

func phaseRun(t *testing.T, ps []*process, source string, trial int, c config, fx fixture, mode string, rec *recorder, alive []int) phase {
	t.Helper()
	r := phase{Config: c, Source: source, Trial: trial, Profile: c.Profile, Mode: mode, Before: sample(ps)}
	stop := make(chan struct{})
	sampled := make(chan resources, 1)
	go func() {
		peak := resources{Valid: true, Errors: map[string]string{}}
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s := sample(ps)
				peak.Valid = peak.Valid && s.Valid
				for node, err := range s.Errors {
					peak.Errors[node] = err
				}
				peak.FileBytes = max(peak.FileBytes, s.FileBytes)
				peak.AllocatedBytes = max(peak.AllocatedBytes, s.AllocatedBytes)
				if peak.Metrics == nil {
					peak.Metrics = map[string]float64{}
				}
				peak.Metrics["memory_peak"] = max(peak.Metrics["memory_peak"], s.Metrics["loki_vl_proxy_process_resident_memory_bytes"])
			case <-stop:
				sampled <- peak
				return
			}
		}
	}()
	wireStart, networkBefore := rec.capture(t)
	r.WireStart = wireStart
	start := time.Now()
	for run := 0; run < c.Repeats; run++ {
		for i, q := range paths(fx, c, mode, run) {
			node := alive[(i+run)%len(alive)]
			r.Calls = append(r.Calls, request(t, ps[node], node, q, source, trial, c, mode, run, rec))
		}
	}
	r.Seconds = time.Since(start).Seconds()
	close(stop)
	peak := <-sampled
	// Allow asynchronous pushes to complete and keep their work in phase totals.
	drainStart := time.Now()
	deadline := time.Now().Add(3 * time.Second)
	lastChange := time.Now()
	last := rec.count()
	for time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		next := rec.count()
		if next != last {
			lastChange = time.Now()
		}
		if time.Since(lastChange) >= 500*time.Millisecond {
			break
		}
		last = next
	}
	wireEnd, networkAfter := rec.capture(t)
	r.WireEnd = wireEnd
	r.WireDrainSeconds = time.Since(drainStart).Seconds()
	r.NetworkHTTPBytes = networkDelta(networkBefore, networkAfter)
	wire := rec.between(r.WireStart, r.WireEnd)
	r.VLCalls, r.PeerCalls, r.VLResponseBytes, r.PeerResponseBytes = wire.VLCalls, wire.PeerCalls, wire.VLBytes, wire.PeerBytes
	r.PeerFetches, r.PeerPushes, r.PeerPushBodyBytes = wire.PeerFetches, wire.PeerPushes, wire.PeerPushBytes
	r.After = sample(ps)
	r.PeakFileBytes = max(peak.FileBytes, r.After.FileBytes)
	r.PeakAllocatedBytes = max(peak.AllocatedBytes, r.After.AllocatedBytes)
	r.PeakMemoryMetric = max(peak.Metrics["memory_peak"], r.After.Metrics["loki_vl_proxy_process_resident_memory_bytes"])
	r.ValidResourceSamples = r.Before.Valid && r.After.Valid && peak.Valid
	if !r.ValidResourceSamples {
		r.Before.Errors["sampling"] = "one or more peak/end samples failed; CPU/memory claims invalid"
	}
	r.ProxyCPUSeconds = r.After.Metrics["loki_vl_proxy_process_cpu_seconds_total"] - r.Before.Metrics["loki_vl_proxy_process_cpu_seconds_total"]
	return r
}
func controlSource() bool { return *controlFlag || os.Getenv("PERF_CONTROL") == "1" }
func runTrial(t *testing.T, source string, trial int, c config, fx fixture, rec *recorder) []phase {
	t.Helper()
	binary := baseline
	if source == "candidate" && !controlSource() {
		binary = candidate
	}
	trialDir := filepath.Join(home, "runs", runID, fmt.Sprintf("%s-%s-%d-rf%d", source, c.Profile, trial, c.RF))
	must(t, os.RemoveAll(trialDir))
	must(t, os.MkdirAll(trialDir, 0700))
	startupIndex, _ := rec.capture(t)
	ps := []*process{}
	lifetimes := []*process{}
	for i := 0; i < 3; i++ {
		ps = append(ps, launch(t, binary, c, i, filepath.Join(trialDir, number(i))))
		lifetimes = append(lifetimes, ps[len(ps)-1])
	}
	stopAll := func() {
		for _, p := range ps {
			p.stop()
		}
	}
	defer stopAll()
	for _, p := range ps {
		ready(t, p.url+"/ready")
	}
	// Explicit cold-fill phase records startup-independent work before warm runs.
	warm := c
	warm.Repeats = 1
	startupEnd, _ := rec.capture(t)
	startupTotals := rec.between(startupIndex, startupEnd)
	phases := []phase{phaseRun(t, ps, source, trial, warm, fx, "cold-fill", rec, []int{0})}
	phases[0].StartupWireTotals = &startupTotals
	phases = append(phases, phaseRun(t, ps, source, trial, c, fx, "exact", rec, []int{0, 1, 2}))
	prime := c
	prime.Repeats = 3
	phases = append(phases, phaseRun(t, ps, source, trial, prime, fx, "frontend-prime", rec, []int{0, 1, 2}))
	phases = append(phases, phaseRun(t, ps, source, trial, c, fx, "warm-repeat", rec, []int{0, 1, 2}))
	for _, mode := range []string{"shifted", "refresh", "recent-refresh"} {
		phases = append(phases, phaseRun(t, ps, source, trial, c, fx, mode, rec, []int{0, 1, 2}))
	}
	stopAll()
	for i := 0; i < 3; i++ {
		ps[i] = launch(t, binary, c, i, filepath.Join(trialDir, number(i)))
		lifetimes = append(lifetimes, ps[i])
	}
	phases = append(phases, phaseRun(t, ps, source, trial, c, fx, "restart", rec, []int{0, 1, 2}))
	// Clear all surviving frontend RAM before owner loss; retain replica disks.
	for i := 1; i < 3; i++ {
		ps[i].stop()
		ps[i] = launch(t, binary, c, i, filepath.Join(trialDir, number(i)))
		lifetimes = append(lifetimes, ps[i])
	}
	ps[0].stop()
	phases = append(phases, phaseRun(t, ps, source, trial, c, fx, "owner-loss", rec, []int{1, 2}))
	stopAll()
	for _, p := range lifetimes {
		if p.Exit != nil {
			phases[len(phases)-1].TrialProcessLifetimes = append(phases[len(phases)-1].TrialProcessLifetimes, p.Exit)
		}
	}
	return phases
}
func digest(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	must(t, err)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func manifest(t *testing.T) map[string]any {
	return map[string]any{
		"harness_source_sha256": digest(t, filepath.Join(root, "bench/cache-placement/real_api_test.go")),
		"run_id":                runID,
		"baseline_ref":          baselineRef, "candidate_ref": candidateRef,
		"baseline_binary": baseline, "candidate_binary": candidate, "baseline_binary_sha256": digest(t, baseline), "candidate_binary_sha256": digest(t, candidate),
		"go_version": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "same_source_control": controlSource(),
		"order": []string{"baseline", "candidate", "candidate", "baseline"}, "same_ring_addresses": true, "startup_label_warm_disabled": true,
		"disabled_background_work":    []string{"labels-cache-warm=false", "patterns-enabled=false", "label-values-indexed-cache=false", "peer-hot-read-ahead-enabled=false(default)"},
		"recent_fixture_scope":        "RecentEnd is immutable setup time, not necessarily near-now at execution; per-request end age and default 120s recent-tail/24h metadata freshness eligibility are recorded; no fresh-ingestion claim",
		"window_scope":                "historical base ends 30m before seeded history ends; forward refresh advances by 60s into previously unrequested windows; sequential phases share caches, not isolated cache-state trials",
		"network_bytes_scope":         "client-to-observer HTTP message bytes including headers/framing and encoded bodies; excludes TCP/IP, retransmissions, observer-to-target second hop",
		"startup_scope":               "request-phase zero-VL statements exclude separately recorded startup health/version traffic; default startup warmers may read VL",
		"shared_backend_observer_url": true,
		"vl_counts_scope":             "fleet aggregate; per-node backend observer URLs would change backend-auth cache keys",
		"observers":                   "stdlib keep-alive HTTP proxies before VL and peer HTTP, same addresses in every trial",
		"latency_classification":      "faster/slower only beyond both 25% and 50ms; raw values always retained; no statistical-significance claim",
		"limits":                      []string{"instrumented loopback timings; source CPU excludes observer CPU", "macOS memory metric is GoSys, not RSS", "Loki cache state retained per request, no cold-Loki claim", "one static fixture, no production workload distribution claim", "owner0 loss affects only keys on owner0; backup-hit metrics verify actual fallback"},
	}
}
func summarize(phases []phase) []map[string]any {
	groups := map[string][]result{}
	for _, p := range phases {
		for _, r := range p.Calls {
			k := r.Source + "/" + r.Profile + "/" + r.Mode + "/" + r.Name
			groups[k] = append(groups[k], r)
		}
	}
	keys := []string{}
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := []map[string]any{}
	for _, k := range keys {
		rs := groups[k]
		pv, lv := []float64{}, []float64{}
		equal := 0
		vl, peer := 0, 0
		for _, r := range rs {
			pv = append(pv, r.ProxyMS)
			lv = append(lv, r.LokiMS)
			if r.Equal {
				equal++
			}
			vl += r.VLCalls
			peer += r.PeerCalls
		}
		pm, lm := percentile(pv, .5), percentile(lv, .5)
		classification := "on-par"
		comparable := equal == len(rs)
		for _, r := range rs {
			if r.ProxyStatus != 200 || r.LokiStatus != 200 {
				comparable = false
			}
		}
		if !comparable {
			classification = "not-comparable"
		} else if pm < lm*.75 && lm-pm > 50 {
			classification = "proxy-faster"
		} else if pm > lm*1.25 && pm-lm > 50 {
			classification = "proxy-slower"
		}
		out = append(out, map[string]any{"group": k, "responses": len(rs), "equal_loki": equal, "proxy_p50_ms": pm, "proxy_p95_ms": percentile(pv, .95), "loki_p50_ms": lm, "loki_p95_ms": percentile(lv, .95), "absolute_delta_ms": pm - lm, "relative_proxy_loki": func() float64 {
			if lm == 0 {
				return 0
			}
			return pm / lm
		}(), "practical_vs_loki": classification, "request_interval_vl_calls": vl, "request_interval_peer_calls": peer})
	}
	return out
}
func TestHelpers(t *testing.T) {
	a := []byte("{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"stats\":{\"x\":2},\"result\":[{\"metric\":{\"a\":\"b\"},\"values\":[[1,\"1.0\"]]}]}}")
	b := []byte("{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"stats\":{\"x\":8},\"result\":[{\"metric\":{\"a\":\"b\"},\"values\":[[1,\"1\"]]}]}}")
	x, e := canonical(a)
	must(t, e)
	y, e := canonical(b)
	must(t, e)
	if x != y {
		t.Fatal("stats/numeric spelling canonicalization failed")
	}
	bad := bytes.ReplaceAll(b, []byte("[1,\"1\"]"), []byte("[1,\"2\"]"))
	z, e := canonical(bad)
	must(t, e)
	if z == x {
		t.Fatal("value difference hidden")
	}
	if percentile([]float64{1, 3, 5}, .5) != 3 {
		t.Fatal("percentile")
	}
	c := config{Shards: 2, Repeats: 3}
	fx := fixture{Probe: "p", End: 100000, RecentEnd: 200000}
	p0 := paths(fx, c, "exact", 0)
	p1 := paths(fx, c, "exact", 1)
	if p0[0].Path != p1[0].Path {
		t.Fatal("exact mode moved window")
	}
	p1 = paths(fx, c, "shifted", 1)
	if p0[0].Path == p1[0].Path {
		t.Fatal("shifted mode did not move")
	}
	for i := 0; i < 3; i++ {
		args := flags(c, i, "/tmp/test")
		if !strings.Contains(strings.Join(args, " "), "labels-cache-warm=false") {
			t.Fatal("startup warmer enabled")
		}
	}
}
func TestRealCacheAB(t *testing.T) {
	operation := *operationFlag
	if operation == "" {
		operation = os.Getenv("REAL_CACHE_PERF")
	}
	if operation == "" {
		t.Skip("set REAL_CACHE_PERF=setup|probe|measure|cleanup")
	}
	must(t, os.MkdirAll(home, 0700))
	prepareCompose(t)
	if operation == "cleanup" {
		compose(t, "down", "-v", "--remove-orphans")
		return
	}
	if operation == "setup" {
		compose(t, "up", "-d", "--no-build")
		fx := seed(t, 90)
		t.Logf("seeded%dlines,immutablefixture%s", fx.Lines, fx.Probe)
		return
	}
	if strings.HasPrefix(operation, "scaling") {
		t.Skip("handled by optional scaling test")
	}
	if operation == "measure" && !*quietFlag {
		t.Fatal("timing run requires -perf-quiet=true after host coordination")
	}
	if baseline == "" || candidate == "" {
		t.Fatal("supply -perf-baseline and -perf-candidate executables")
	}
	if operation != "probe" && operation != "measure" {
		t.Fatal("unknown operation")
	}
	var fx fixture
	b, err := os.ReadFile(filepath.Join(home, "fixture.json"))
	must(t, err)
	must(t, json.Unmarshal(b, &fx))
	ready(t, vlBase()+"/health")
	ready(t, lokiBase()+"/ready")
	rec := newRecorder(t, operation+fmt.Sprintf("-rf%d", *rfFlag)+func() string {
		if controlSource() {
			return "-control"
		}
		return ""
	}())
	for i := 0; i < 3; i++ {
		port, back, peer := ports(i)
		if i == 0 {
			observer(t, back, vlBase(), "vl", -1, rec)
		}
		observer(t, peer, "http://127.0.0.1:"+number(port), "peer", i, rec)
	}
	m := manifest(t)
	save(t, filepath.Join(home, "manifest.json"), m)
	rf := *rfFlag
	if rf != 1 && rf != 2 {
		t.Fatal("RF must be1or2")
	}
	if os.Getenv("PERF_RF") == "1" {
		rf = 1
	}
	cfgs := []config{{Profile: "fit", Entries: 256, Bytes: 16 << 20, DiskBytes: 16 << 20, Shards: 8, RF: rf, Edge: true, Repeats: 5}, {Profile: "pressure", Entries: 64, Bytes: 2 << 20, DiskBytes: 32 << 10, Shards: 90, RF: rf, Edge: true, Repeats: 5}, {Profile: "pooled", Entries: 256, Bytes: 16 << 20, DiskBytes: 2 << 20, Shards: 90, RF: rf, Edge: true, Repeats: 5}}
	order := []string{"baseline", "candidate", "candidate", "baseline"}
	if *profileFlag != "" {
		filtered := []config{}
		for _, c := range cfgs {
			if c.Profile == *profileFlag {
				filtered = append(filtered, c)
			}
		}
		if len(filtered) == 0 {
			t.Fatal("unknown profile")
		}
		cfgs = filtered
	}
	if operation == "probe" {
		if *profileFlag == "" {
			cfgs = cfgs[:1]
			cfgs[0].Shards = 2
		}
		for i := range cfgs {
			cfgs[i].Repeats = 2
		}
		order = order[:2]
	}
	all := []phase{}
	for _, c := range cfgs {
		for n, source := range order {
			t.Logf("starting%s %s block%d RF%d (no latency conclusions under host contention)", source, c.Profile, n, c.RF)
			phases := runTrial(t, source, n, c, fx, rec)
			all = append(all, phases...)
			suffix := fmt.Sprintf("%s-rf%d", operation, rf)
			if *profileFlag != "" {
				suffix += "-" + *profileFlag
			}
			if controlSource() {
				suffix += "-control"
			}
			rec.flush(t)
			report := map[string]any{"manifest": m, "fixture": fx, "phases": all, "wire_log": rec.path, "wire_event_count": rec.count(), "summary": summarize(all)}
			save(t, filepath.Join(home, "results-"+suffix+".json"), report)
			must(t, os.MkdirAll(filepath.Join(home, "reports"), 0700))
			save(t, filepath.Join(home, "reports", runID+"-"+suffix+".json"), report)
		}
	}
	equal, total := 0, 0
	for _, p := range all {
		for _, r := range p.Calls {
			total++
			if r.Equal {
				equal++
			}
		}
	}
	t.Logf("correctness%d/%d; mismatches retained; no timing result accepted without quiet-host control", equal, total)
	if equal != total {
		t.Errorf("%dresponses differ from Loki", total-equal)
	}
}

func TestCanonicalPreservesMeaning(t *testing.T) {
	a := []byte("{\"data\":{\"resultType\":\"matrix\",\"result\":[{\"metric\":{},\"values\":[[1.0,\"1\"]]}]}}")
	b := bytes.ReplaceAll(a, []byte("[1.0,"), []byte("[1,"))
	x, e := canonical(a)
	must(t, e)
	y, e := canonical(b)
	must(t, e)
	if x != y {
		t.Fatal("equivalent numeric timestamp differs")
	}
	a = []byte("{\"data\":{\"resultType\":\"streams\",\"result\":[{\"stream\":{},\"values\":[[\"1\",\"a\"],[\"2\",\"b\"]]}]}}")
	b = []byte("{\"data\":{\"resultType\":\"streams\",\"result\":[{\"stream\":{},\"values\":[[\"2\",\"b\"],[\"1\",\"a\"]]}]}}")
	x, e = canonical(a)
	must(t, e)
	y, e = canonical(b)
	must(t, e)
	if x == y {
		t.Fatal("log ordering difference hidden")
	}
}

func TestAllProxiesShareBackendIdentity(t *testing.T) {
	c := config{Shards: 2, Entries: 64, Bytes: 2 << 20, RF: 2}
	backend := ""
	for i := 0; i < 3; i++ {
		for _, arg := range flags(c, i, "/tmp/test") {
			if strings.HasPrefix(arg, "-backend=") {
				if backend == "" {
					backend = arg
				} else if backend != arg {
					t.Fatal("observer URLs changed backend-auth cache identity across proxies")
				}
			}
		}
	}
}

func TestNonmemberConfiguration(t *testing.T) {
	c := config{Entries: 256, Bytes: 16 << 20, RF: 2, Edge: true}
	for _, i := range []int{3, 4, 8} {
		args := flagsForRing(c, i, "/tmp/reader", 3, true)
		self, static := "", ""
		for _, arg := range args {
			if strings.HasPrefix(arg, "-peer-self=") {
				self = strings.TrimPrefix(arg, "-peer-self=")
			}
			if strings.HasPrefix(arg, "-peer-static=") {
				static = strings.TrimPrefix(arg, "-peer-static=")
			}
			if strings.HasPrefix(arg, "-disk-cache-path=") {
				t.Fatal("nonmember reader has unnecessary L2")
			}
		}
		if self == "" || static == "" {
			t.Fatal("peer configuration missing")
		}
		for _, p := range strings.Split(static, ",") {
			if p == self {
				t.Fatal("reader accidentally owns ring keys")
			}
		}
	}
}
func verifyNonmember(t *testing.T, p *process, owners int) {
	t.Helper()
	status, body, _, err := fetch("GET", p.url+"/_cache/peers", nil, map[string]string{"X-Peer-Token": token})
	must(t, err)
	if status != 200 {
		t.Fatalf("peer topology status%d: %.500s", status, body)
	}
	var topology struct {
		Peers []string
		Self  string
	}
	must(t, json.Unmarshal(body, &topology))
	if len(topology.Peers) != owners {
		t.Fatalf("ring has%d peers, want%d", len(topology.Peers), owners)
	}
	for _, addr := range topology.Peers {
		if addr == topology.Self {
			t.Fatal("reader has joined owner ring")
		}
	}
}

type coldWave struct {
	NetworkHTTPBytes  map[string]networkBytes
	Name              string
	Before            resources
	After             resources
	VLCalls           int
	PeerFetches       int
	PeerResponseBytes int64
	PeerPushBytes     int64
	Calls             []result
}

func coldReaderWave(t *testing.T, owners, readers []*process, source string, trial int, c config, q query, rec *recorder) coldWave {
	t.Helper()
	all := append(append([]*process{}, owners...), readers...)
	wave := coldWave{Name: q.Name, Before: sample(all)}
	index, networkBefore := rec.capture(t)
	barrier := make(chan struct{})
	out := make(chan result, len(readers))
	for i, p := range readers {
		go func(i int, p *process) {
			<-barrier
			out <- request(t, p, i+3, q, source, trial, c, "cold-nonmember-readers", 0, rec)
		}(i, p)
	}
	close(barrier)
	for range readers {
		wave.Calls = append(wave.Calls, <-out)
	}
	time.Sleep(500 * time.Millisecond)
	end, networkAfter := rec.capture(t)
	totals := rec.between(index, end)
	wave.VLCalls = totals.VLCalls
	wave.PeerFetches = totals.PeerFetches
	wave.PeerResponseBytes = totals.PeerBytes
	wave.PeerPushBytes = totals.PeerPushBytes
	wave.NetworkHTTPBytes = networkDelta(networkBefore, networkAfter)
	wave.After = sample(all)
	return wave
}

// A fixed request count proves owner-pool reuse without a throughput claim.
func boundedReaderPass(t *testing.T, owners, readers []*process, source string, trial int, c config, fx fixture, rec *recorder) coldWave {
	t.Helper()
	all := append(append([]*process{}, owners...), readers...)
	wave := coldWave{Before: sample(all)}
	index, networkBefore := rec.capture(t)
	barrier := make(chan struct{})
	out := make(chan []result, len(readers))
	for i, p := range readers {
		go func(i int, p *process) {
			<-barrier
			results := []result{}
			for round := 0; round < 3; round++ {
				for _, q := range paths(fx, c, "exact", 0) {
					results = append(results, request(t, p, i+3, q, source, trial, c, "bounded-owner-pool-pass", round, rec))
				}
			}
			out <- results
		}(i, p)
	}
	close(barrier)
	for range readers {
		wave.Calls = append(wave.Calls, (<-out)...)
	}
	time.Sleep(500 * time.Millisecond)
	end, networkAfter := rec.capture(t)
	totals := rec.between(index, end)
	wave.VLCalls = totals.VLCalls
	wave.PeerFetches = totals.PeerFetches
	wave.PeerResponseBytes = totals.PeerBytes
	wave.PeerPushBytes = totals.PeerPushBytes
	wave.NetworkHTTPBytes = networkDelta(networkBefore, networkAfter)
	wave.After = sample(all)
	return wave
}

type warmWorker struct {
	Node              int
	Responses         int
	Equal             int
	SuccessfulCorrect int
	HTTPErrors        int
	Seconds           float64
	ByQuery           map[string]map[string]float64
}
type warmLoad struct {
	NetworkHTTPBytes  map[string]networkBytes
	Seconds           float64
	Responses         int
	Equal             int
	SuccessfulCorrect int
	HTTPErrors        int
	ValidThroughput   bool
	QPS               float64
	CorrectQPS        float64
	Before            resources
	After             resources
	VLCalls           int
	PeerFetches       int
	PeerResponseBytes int64
	Workers           []warmWorker
}

func warmReadLoad(t *testing.T, owners, readers []*process, queries []query, rec *recorder, duration time.Duration) warmLoad {
	t.Helper()
	reference := map[string]string{}
	statuses := map[string]int{}
	for _, q := range queries {
		status, body, _, err := fetch("GET", lokiBase()+q.Path, nil, q.Headers)
		must(t, err)
		value, err := canonical(body)
		must(t, err)
		reference[q.Name] = value
		statuses[q.Name] = status
	}
	all := append(append([]*process{}, owners...), readers...)
	result := warmLoad{Before: sample(all)}
	barrier := make(chan struct{})
	out := make(chan warmWorker, len(readers))
	var ready sync.WaitGroup
	ready.Add(len(readers))
	var deadline time.Time
	for i, p := range readers {
		go func(i int, p *process) {
			worker := warmWorker{Node: i + 3, ByQuery: map[string]map[string]float64{}}
			samples := map[string][]float64{}
			ready.Done()
			<-barrier
			begin := time.Now()
			for n := 0; n < 50000 && time.Now().Before(deadline); n++ {
				q := queries[n%len(queries)]
				start := time.Now()
				status, body, _, err := fetch("GET", p.url+q.Path, nil, q.Headers)
				elapsed := float64(time.Since(start).Nanoseconds()) / 1e6
				worker.Responses++
				samples[q.Name] = append(samples[q.Name], elapsed)
				normalized, e := canonical(body)
				if err == nil && e == nil && status == statuses[q.Name] && normalized == reference[q.Name] {
					worker.Equal++
					if status == http.StatusOK && statuses[q.Name] == http.StatusOK {
						worker.SuccessfulCorrect++
					}
				}
				if err != nil || status != 200 {
					worker.HTTPErrors++
				}
			}
			worker.Seconds = time.Since(begin).Seconds()
			for name, times := range samples {
				worker.ByQuery[name] = map[string]float64{"responses": float64(len(times)), "p50_ms": percentile(times, .5), "p95_ms": percentile(times, .95)}
			}
			out <- worker
		}(i, p)
	}
	ready.Wait()
	index, networkBefore := rec.capture(t)
	start := time.Now()
	deadline = start.Add(duration)
	close(barrier)
	for range readers {
		w := <-out
		result.Workers = append(result.Workers, w)
		result.Responses += w.Responses
		result.Equal += w.Equal
		result.SuccessfulCorrect += w.SuccessfulCorrect
		result.HTTPErrors += w.HTTPErrors
	}
	result.Seconds = time.Since(start).Seconds()
	result.QPS = float64(result.Responses) / result.Seconds
	result.CorrectQPS = float64(result.SuccessfulCorrect) / result.Seconds
	result.ValidThroughput = result.Responses > 0 && result.SuccessfulCorrect == result.Responses && result.HTTPErrors == 0
	time.Sleep(500 * time.Millisecond)
	end, networkAfter := rec.capture(t)
	totals := rec.between(index, end)
	result.VLCalls = totals.VLCalls
	result.PeerFetches = totals.PeerFetches
	result.PeerResponseBytes = totals.PeerBytes
	result.NetworkHTTPBytes = networkDelta(networkBefore, networkAfter)
	result.After = sample(all)
	return result
}

type readerCell struct {
	Config                 config
	StartupWireTotals      wireTotals
	DisabledStartupWarmers []string
	Source                 string
	Trial                  int
	Readers                int
	RF                     int
	OwnerCount             int
	RingVerified           bool
	Cold                   []coldWave
	PoolPriming            *warmLoad
	SharedPoolPass         *coldWave
	Warm                   *warmLoad
	ReaderLifetimes        []map[string]any
}

func TestReaderScaling(t *testing.T) {
	operation := *operationFlag
	if operation != "scaling-probe" && operation != "scaling" && operation != "scaling-counts" {
		t.Skip("optional -perf=scaling-probe|scaling")
	}
	if operation == "scaling" && !*quietFlag {
		t.Fatal("throughput measurement requires coordinated -perf-quiet=true")
	}
	if baseline == "" || candidate == "" {
		t.Fatal("supply baseline and candidate binaries")
	}
	var fx fixture
	raw, err := os.ReadFile(filepath.Join(home, "fixture.json"))
	must(t, err)
	must(t, json.Unmarshal(raw, &fx))
	ready(t, vlBase()+"/health")
	ready(t, lokiBase()+"/ready")
	rec := newRecorder(t, "fixed3owners-readers"+operation)
	// One backend identity for ALL owners and frontends is essential.
	_, backendPort, _ := ports(0)
	observer(t, backendPort, vlBase(), "vl", -1, rec)
	for i := 0; i < 9; i++ {
		port, _, peer := ports(i)
		observer(t, peer, "http://127.0.0.1:"+number(port), "peer", i, rec)
	}
	order := []string{"baseline", "candidate", "candidate", "baseline"}
	counts := []int{1, 3, 6}
	if operation == "scaling-counts" {
		order = order[:2]
	}
	if operation == "scaling-probe" {
		order = []string{"candidate"}
	}
	c := config{Profile: "fixed3owners", Entries: 256, Bytes: 16 << 20, DiskBytes: 2 << 20, Shards: 2, RF: *rfFlag, Edge: true, Repeats: 3}
	if c.RF != 1 && c.RF != 2 {
		t.Fatal("RF must be1or2")
	}
	switch *scalingProfileFlag {
	case "fit":
	case "pool-pressure":
		c.Profile = "fixed3owners-pool-pressure"
		c.Shards = 90
		c.Edge = false
	default:
		t.Fatal("unknown scaling profile")
	}
	cells := []readerCell{}
	allOwnerLives := []map[string]any{}
	for trial, source := range order {
		binary := baseline
		if source == "candidate" && !controlSource() {
			binary = candidate
		}
		dir := filepath.Join(home, "runs", runID, "scaling-"+source+"-"+number(trial))
		must(t, os.MkdirAll(dir, 0700))
		owners := []*process{}
		for i := 0; i < 3; i++ {
			owners = append(owners, launch(t, binary, c, i, filepath.Join(dir, "owner-"+number(i))))
		}
		// Prime all owners through real Loki-compatible APIs before cold readers.
		_ = phaseRun(t, owners, source, trial, c, fx, "owner-prime", rec, []int{0, 1, 2})
		for _, count := range counts {
			t.Logf("%s RF%d: fixed3 cache owners +%d cold nonmember readers", source, c.RF, count)
			cell := readerCell{Config: c, DisabledStartupWarmers: []string{"labels-cache-warm=false", "patterns-enabled=false", "label-values-indexed-cache=false", "peer-hot-read-ahead-enabled=false (default)"}, Source: source, Trial: trial, Readers: count, RF: c.RF, OwnerCount: 3}
			startupIndex, _ := rec.capture(t)
			readers := []*process{}
			for n := 0; n < count; n++ {
				i := n + 3
				p := launchRing(t, binary, c, i, filepath.Join(dir, fmt.Sprintf("readers%d-%d", count, n)), 3, true)
				verifyNonmember(t, p, 3)
				readers = append(readers, p)
			}
			startupEnd, _ := rec.capture(t)
			cell.StartupWireTotals = rec.between(startupIndex, startupEnd)
			cell.RingVerified = true
			coldQueries := paths(fx, c, "exact", 0)
			if c.Profile == "fixed3owners-pool-pressure" {
				coldQueries = coldQueries[:2]
			}
			for _, q := range coldQueries {
				cell.Cold = append(cell.Cold, coldReaderWave(t, owners, readers, source, trial, c, q, rec))
			}
			if operation == "scaling-counts" {
				pass := boundedReaderPass(t, owners, readers, source, trial, c, fx, rec)
				cell.SharedPoolPass = &pass
			}
			if operation == "scaling" {
				if c.Profile == "fixed3owners-pool-pressure" {
					priming := warmReadLoad(t, owners, readers, paths(fx, c, "exact", 0), rec, 500*time.Millisecond)
					cell.PoolPriming = &priming
					if !priming.ValidThroughput {
						t.Error("pool priming returned failed or incorrect responses")
					}
				}
				warm := warmReadLoad(t, owners, readers, paths(fx, c, "exact", 0), rec, 5*time.Second)
				cell.Warm = &warm
				if !warm.ValidThroughput {
					t.Errorf("%s readers=%d: throughput invalid: successful correct=%d/%d HTTP errors=%d", source, count, warm.SuccessfulCorrect, warm.Responses, warm.HTTPErrors)
				}
			}
			for _, p := range readers {
				p.stop()
				cell.ReaderLifetimes = append(cell.ReaderLifetimes, p.Exit)
			}
			cells = append(cells, cell)
			rec.flush(t)
			save(t, filepath.Join(home, "reader-scaling-"+runID+".json"), map[string]any{"manifest": manifest(t), "fixture": fx, "cells": cells, "owner_lifetimes": allOwnerLives, "wire_log": rec.path, "scope": "independent concurrent requests; no distributed execution of a single LogQL query; same-host host-bound scaling"})
		}
		for _, p := range owners {
			p.stop()
			allOwnerLives = append(allOwnerLives, p.Exit)
		}
	}
	rec.flush(t)
	save(t, filepath.Join(home, "reader-scaling-"+runID+".json"), map[string]any{"manifest": manifest(t), "fixture": fx, "cells": cells, "owner_lifetimes": allOwnerLives, "wire_log": rec.path, "scope": "independent concurrent requests; no distributed execution of a single LogQL query; same-host host-bound scaling"})
	equal, total := 0, 0
	for _, cell := range cells {
		for _, wave := range cell.Cold {
			for _, r := range wave.Calls {
				total++
				if r.Equal {
					equal++
				}
			}
		}
		if cell.SharedPoolPass != nil {
			for _, r := range cell.SharedPoolPass.Calls {
				total++
				if r.Equal && r.ProxyStatus == 200 && r.LokiStatus == 200 {
					equal++
				}
			}
		}
		if cell.Warm != nil {
			total += cell.Warm.Responses
			equal += cell.Warm.Equal
		}
	}
	t.Logf("cold/warm read correctness%d/%d", equal, total)
	if equal != total {
		t.Errorf("%dresponse mismatches", total-equal)
	}
}

func TestObserverPreservesRequestsAndCountsTransfer(t *testing.T) {
	received := make(chan bool, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		received <- r.Method == "POST" && r.URL.Path == "/select/logsql/query" && r.Header.Get("X-Test") == "preserved" && string(raw) == "query=hello&start=123&end=456"
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{\"ok\":true}"))
	}))
	defer upstream.Close()
	rec := newRecorder(t, "helper-observer")
	srv := observer(t, 0, upstream.URL, "vl", -1, rec)
	code, body, _, err := fetch("POST", "http://"+srv.Addr+"/select/logsql/query?extra=kept", []byte("query=hello&start=123&end=456"), map[string]string{"Content-Type": "application/x-www-form-urlencoded", "X-Test": "preserved"})
	must(t, err)
	if code != 200 || string(body) != "{\"ok\":true}" || !<-received {
		t.Fatal("observer changed request or response")
	}
	end, network := rec.capture(t)
	totals := rec.between(0, end)
	if totals.VLCalls != 1 || totals.VLBytes != int64(len(body)) {
		t.Fatalf("body counts=%+v", totals)
	}
	net := network["vl-node-1"]
	if net.RequestHTTPBytes <= int64(len("query=hello&start=123&end=456")) || net.ResponseHTTPBytes <= int64(len(body)) {
		t.Fatalf("HTTP framing/header bytes not counted: %+v", net)
	}
}

func TestCanonicalRejectsTrailingJSON(t *testing.T) {
	for _, body := range []string{`{"status":"success"} {"extra":true}`, `{"status":"success"} garbage`} {
		if _, err := canonical([]byte(body)); err == nil {
			t.Errorf("accepted trailing input %q", body)
		}
	}
	if _, err := canonical([]byte("{\"status\":\"success\"}\n ")); err != nil {
		t.Fatal(err)
	}
}
func TestBoundedBodyRejectsOverflow(t *testing.T) {
	if _, err := readBoundedBody(strings.NewReader("12345"), 4); err == nil {
		t.Fatal("accepted oversized body")
	}
	b, err := readBoundedBody(strings.NewReader("1234"), 4)
	if err != nil || string(b) != "1234" {
		t.Fatalf("%q %v", b, err)
	}
}
func TestMatchingErrorsHaveNoSuccessfulThroughput(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
		_, _ = w.Write([]byte(`{"status":"error","error":"broken"}`))
	}))
	defer server.Close()
	_, port, err := net.SplitHostPort(server.Listener.Addr().String())
	must(t, err)
	prior := lokiPort
	lokiPort, err = strconv.Atoi(port)
	must(t, err)
	defer func() { lokiPort = prior }()
	oldHome := home
	home = t.TempDir()
	defer func() { home = oldHome }()
	rec := newRecorder(t, "error-throughput")
	load := warmReadLoad(t, nil, []*process{{url: server.URL}}, []query{{Name: "error", Path: "/broken"}}, rec, 10*time.Millisecond)
	if load.Equal == 0 || load.HTTPErrors != load.Responses {
		t.Fatalf("fixture failed: %+v", load)
	}
	if load.SuccessfulCorrect != 0 || load.CorrectQPS != 0 || load.ValidThroughput {
		t.Fatalf("errors counted as successful throughput: %+v", load)
	}
}

func TestHistoricalRefreshUsesUnseenIncreasingWindows(t *testing.T) {
	fx := fixture{End: 100000, RecentEnd: 200000}
	c := config{Repeats: 5, Shards: 2}
	seen := map[string]bool{}
	for _, mode := range []string{"exact", "shifted"} {
		for run := 0; run < c.Repeats; run++ {
			seen[paths(fx, c, mode, run)[0].Path] = true
		}
	}
	var previous int64
	for run := 0; run < c.Repeats; run++ {
		q := paths(fx, c, "refresh", run)[0]
		if seen[q.Path] {
			t.Fatal("refresh replays previously requested window")
		}
		u, err := url.Parse(q.Path)
		must(t, err)
		end, err := strconv.ParseInt(u.Query().Get("end"), 10, 64)
		must(t, err)
		if end <= previous || end > fx.End*1e9 {
			t.Fatal("refresh must increase within seeded history")
		}
		previous = end
	}
}

func TestResourceSamplesDoNotHideFailedAliveNodes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "metrics unavailable", 500) }))
	defer server.Close()
	r := sample([]*process{{url: server.URL, cmd: &exec.Cmd{}}, {url: server.URL, cmd: nil}})
	if r.Valid || r.ExpectedNodes != 1 || r.SampledNodes != 0 || len(r.Errors) != 1 {
		t.Fatalf("invalid scrape represented as valid: %+v", r)
	}
	dead := sample([]*process{{url: server.URL, cmd: nil}})
	if !dead.Valid || dead.ExpectedNodes != 0 {
		t.Fatalf("intentionally stopped node counted: %+v", dead)
	}
}

func TestPoolPressureReaderWorkloadExceedsShadowSlots(t *testing.T) {
	c := config{Profile: "fixed3owners-pool-pressure", Entries: 256, Shards: 90, Edge: false}
	qs := paths(fixture{End: 100000}, c, "exact", 0)
	if c.Edge || len(qs) <= c.Entries/10 || len(qs) != 90 {
		t.Fatalf("not a pool-pressure workload: %d", len(qs))
	}
	for _, q := range qs {
		if !strings.HasPrefix(q.Name, "metadata-") {
			t.Fatal("unexpected raw query path")
		}
	}
}

// The transport can deliver a complete response before its Write returns.
type deliveredBeforeReturnConn struct {
	net.Conn
	delivered chan struct{}
	release   chan struct{}
}

func (c *deliveredBeforeReturnConn) Write(body []byte) (int, error) {
	n, err := c.Conn.Write(body)
	close(c.delivered)
	<-c.release
	return n, err
}
func TestNetworkSnapshotWaitsForCompletedWriteAccounting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()
		release := make(chan struct{})
		var once sync.Once
		releaseWrite := func() { once.Do(func() { close(release) }) }
		defer releaseWrite()
		delivered := make(chan struct{})
		meter := &networkMeter{}
		conn := &measuredConn{Conn: &deliveredBeforeReturnConn{Conn: server, delivered: delivered, release: release}, meter: meter}
		writeDone := make(chan struct{})
		body := []byte("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
		go func() { defer close(writeDone); _, _ = conn.Write(body) }()
		received := make([]byte, len(body))
		_, err := io.ReadFull(client, received)
		must(t, err)
		<-delivered
		rec := &recorder{meters: map[string]*networkMeter{"response": meter}, prefix: []wireTotals{{}}}
		snapshot := make(chan map[string]networkBytes, 1)
		go func() { snapshot <- rec.network(t) }()
		synctest.Wait()
		select {
		case counts := <-snapshot:
			t.Fatalf("snapshot preceded completed write accounting: %+v", counts)
		default:
		}
		releaseWrite()
		<-writeDone
		counts := <-snapshot
		if counts["response"].ResponseHTTPBytes != int64(len(body)) {
			t.Fatalf("completed bytes=%+v", counts)
		}
	})
}

func TestObserverSnapshotWaitsForHTTPCompletionAndPairedLedger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()
		meter := &networkMeter{}
		meter.connectionState(server, http.StateActive)
		rec := newRecorder(t, "pending-http")
		rec.meters = map[string]*networkMeter{"http": meter}
		snapshot := make(chan struct {
			index   int
			network map[string]networkBytes
		}, 1)
		go func() {
			index, network := rec.capture(t)
			snapshot <- struct {
				index   int
				network map[string]networkBytes
			}{index, network}
		}()
		synctest.Wait()
		select {
		case <-snapshot:
			t.Fatal("snapshot completed while HTTP handler active")
		default:
		}
		rec.add(wireEvent{Kind: "vl", ResponseBytes: 2})
		synctest.Wait()
		select {
		case <-snapshot:
			t.Fatal("handler ledger entry was mistaken for response completion")
		default:
		}
		meter.written.Add(40)
		meter.connectionState(server, http.StateIdle)
		completed := <-snapshot
		if completed.index != 1 || completed.network["http"].ResponseHTTPBytes != 40 {
			t.Fatalf("inconsistent ledger/wire snapshot: %+v", completed)
		}
		// Closed after Idle must not decrement the pending count again.
		meter.connectionState(server, http.StateClosed)
		_, _, err := rec.snapshot(context.Background())
		must(t, err)
	})
}
func TestObserverSnapshotCancellationAndIdleRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()
		meter := &networkMeter{}
		conn := &measuredConn{Conn: server, meter: meter}
		meter.connectionState(server, http.StateActive)
		rec := &recorder{meters: map[string]*networkMeter{"http": meter}, prefix: []wireTotals{{}}}
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() { _, _, err := rec.snapshot(ctx); result <- err }()
		synctest.Wait()
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("pending snapshot cancellation: %v", err)
		}
		deadlineCtx, deadlineCancel := context.WithTimeout(context.Background(), time.Second)
		defer deadlineCancel()
		go func() { _, _, err := rec.snapshot(deadlineCtx); result <- err }()
		if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("pending snapshot deadline: %v", err)
		}
		meter.connectionState(server, http.StateClosed)
		readDone := make(chan struct{})
		go func() { defer close(readDone); var b [1]byte; _, _ = conn.Read(b[:]) }()
		synctest.Wait()
		_, _, err := rec.snapshot(context.Background())
		must(t, err)
		select {
		case <-readDone:
			t.Fatal("idle read unexpectedly completed")
		default:
		}
		_ = client.Close()
		<-readDone
	})
}

type lokiPushBatcher struct {
	maxBytes int
	emit     func([]byte) error
	body     []byte
	count    int
	err      error
}

const lokiPushPrefix = "{\"streams\":["
const lokiPushSuffix = "]}"

// add preserves each complete stream and its entry order. maxBytes bounds
// encoded request bodies, including wrappers and escaping, not source strings.
func (b *lokiPushBatcher) add(stream any) error {
	if b.err != nil {
		return b.err
	}
	raw, err := json.Marshal(stream)
	if err != nil {
		return err
	}
	if b.maxBytes < len(lokiPushPrefix)+len(lokiPushSuffix) || len(raw) > b.maxBytes-len(lokiPushPrefix)-len(lokiPushSuffix) {
		return fmt.Errorf("single Loki stream needs %d encoded bytes, request limit is %d", len(lokiPushPrefix)+len(raw)+len(lokiPushSuffix), b.maxBytes)
	}
	if b.count > 0 && len(b.body)+1+len(raw)+len(lokiPushSuffix) > b.maxBytes {
		if err := b.flush(); err != nil {
			return err
		}
	}
	if b.count == 0 {
		b.body = append(b.body, lokiPushPrefix...)
	} else {
		b.body = append(b.body, ',')
	}
	b.body = append(b.body, raw...)
	b.count++
	return nil
}
func (b *lokiPushBatcher) flush() error {
	if b.err != nil {
		return b.err
	}
	if b.count == 0 {
		return nil
	}
	b.body = append(b.body, lokiPushSuffix...)
	// emit must consume the body before returning: the next batch reuses it.
	if b.err = b.emit(b.body); b.err != nil {
		return b.err
	}
	b.body = b.body[:0]
	b.count = 0
	return nil
}
func TestLokiPushBatcherBoundsEncodedRequestsAndPreservesStreams(t *testing.T) {
	streams := []map[string]any{}
	for i := 0; i < 10; i++ {
		streams = append(streams, map[string]any{"stream": map[string]string{"shard": number(i), "same": "identity"}, "values": [][]string{{"1", "escaped \"line\" \\ and newline\n"}}})
	}
	one, err := json.Marshal(streams[0])
	must(t, err)
	limit := len(lokiPushPrefix) + 2*len(one) + 1 + len(lokiPushSuffix)
	var received []json.RawMessage
	requests := 0
	batch := lokiPushBatcher{maxBytes: limit, emit: func(body []byte) error {
		requests++
		if len(body) > limit {
			return fmt.Errorf("encoded request bytes%d exceed limit%d", len(body), limit)
		}
		var push struct{ Streams []json.RawMessage }
		if err := json.Unmarshal(body, &push); err != nil {
			return err
		}
		received = append(received, push.Streams...)
		return nil
	}}
	for _, stream := range streams {
		must(t, batch.add(stream))
	}
	must(t, batch.flush())
	if requests != 5 || len(received) != len(streams) {
		t.Fatalf("requests%d streams%d", requests, len(received))
	}
	for i, got := range received {
		want, err := json.Marshal(streams[i])
		must(t, err)
		if !bytes.Equal(got, want) {
			t.Fatalf("stream%d identity/content/order changed", i)
		}
	}
	must(t, batch.flush())
	if requests != 5 {
		t.Fatal("flush emitted empty payload")
	}
}
func TestLokiPushBatcherRejectsSingleOversizeStream(t *testing.T) {
	stream := map[string]any{"stream": map[string]string{"a": "b"}, "values": [][]string{{"1", "content"}}}
	raw, err := json.Marshal(stream)
	must(t, err)
	calls := 0
	batch := lokiPushBatcher{maxBytes: len(lokiPushPrefix) + len(raw) + len(lokiPushSuffix) - 1, emit: func([]byte) error { calls++; return nil }}
	if err := batch.add(stream); err == nil {
		t.Fatal("accepted indivisible oversized stream")
	}
	must(t, batch.flush())
	if calls != 0 {
		t.Fatal("oversized stream reached callback")
	}
}

func TestLokiPushBatcherStopsAfterCallbackFailure(t *testing.T) {
	failure := errors.New("ingest failed")
	calls := 0
	batch := lokiPushBatcher{maxBytes: 1024, emit: func([]byte) error { calls++; return failure }}
	must(t, batch.add(map[string]string{"stream": "identity"}))
	if err := batch.flush(); !errors.Is(err, failure) {
		t.Fatalf("callback failure lost: %v", err)
	}
	if err := batch.add(map[string]string{"another": "stream"}); !errors.Is(err, failure) {
		t.Fatal("appended after failed push", err)
	}
	if err := batch.flush(); !errors.Is(err, failure) || calls != 1 {
		t.Fatalf("retried failed payload: calls%d error%v", calls, err)
	}
}
