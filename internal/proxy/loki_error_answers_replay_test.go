package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// lokiErrorAnswersPath is the file of real Loki error answers recorded over
// HTTP, one row per Loki error class and route (internal/lokierr holds the
// constructors to it as well).
var lokiErrorAnswersPath = filepath.Join("..", "lokierr", "testdata", "loki_error_answers.jsonl")

// lokiErrorKnownDiffPath lists the rows whose proxy answer is not Loki's yet.
var lokiErrorKnownDiffPath = filepath.Join("..", "..", "conformance", "registry", "error-layer-known-differences.json")

// lokiErrorAnswer is one recorded row.
type lokiErrorAnswer struct {
	ID       string            `json:"id"`
	Route    string            `json:"route"`
	Method   string            `json:"method"`
	Target   string            `json:"target"`
	Headers  []string          `json:"headers"`
	Form     string            `json:"form"`
	Generate *lokiErrorRebuild `json:"generate"`
	Loki     lokiErrorReply    `json:"loki"`
	Msg      string            `json:"msg"`
	// NotComparable says why the row cannot be compared with the proxy yet
	// (the recorded Loki's configuration differs, or the answer moves with
	// the clock).
	NotComparable string `json:"not_comparable"`
}

// lokiErrorRebuild rebuilds a request too large to store: a query of a
// prefix and count units, or a form body of FormBytes bytes.
type lokiErrorRebuild struct {
	QueryPrefix string `json:"query_prefix"`
	QueryUnit   string `json:"query_unit"`
	QueryCount  int    `json:"query_count"`
	FormBytes   int    `json:"form_bytes"`
}

// lokiErrorReply is what a client read.
type lokiErrorReply struct {
	Status      int    `json:"status"`
	ContentType string `json:"content_type"`
	Nosniff     string `json:"nosniff"`
	Body        string `json:"body"`
}

func readLokiErrorAnswers(t *testing.T) []lokiErrorAnswer {
	t.Helper()
	f, err := os.Open(lokiErrorAnswersPath)
	if err != nil {
		t.Fatalf("open %s: %v", lokiErrorAnswersPath, err)
	}
	defer f.Close()
	var rows []lokiErrorAnswer
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<16), 1<<22)
	for sc.Scan() {
		var row lokiErrorAnswer
		if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
			t.Fatalf("%s: %v", lokiErrorAnswersPath, err)
		}
		if row.ID != "" {
			rows = append(rows, row)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return rows
}

// rawRequest is the request of a row as bytes on the wire: the target as
// recorded (an invalid escape stays invalid), its headers and form body.
func (row lokiErrorAnswer) rawRequest() string {
	target, body := row.Target, row.Form
	if g := row.Generate; g != nil {
		if g.QueryCount > 0 {
			query := g.QueryPrefix + strings.Repeat(g.QueryUnit, g.QueryCount)
			target += "?query=" + url.QueryEscape(query)
		}
		if g.FormBytes > 0 {
			body = "query=" + strings.Repeat("a", g.FormBytes-len("query="))
		}
	}
	var b strings.Builder
	b.Grow(len(target) + len(body) + 256)
	fmt.Fprintf(&b, "%s %s HTTP/1.1\r\nHost: loki\r\n", row.Method, target)
	connection := false
	for _, h := range row.Headers {
		b.WriteString(h)
		b.WriteString("\r\n")
		connection = connection || strings.HasPrefix(strings.ToLower(h), "connection:")
	}
	if body != "" || row.Method == http.MethodPost {
		fmt.Fprintf(&b, "Content-Length: %d\r\n", len(body))
	}
	if !connection {
		b.WriteString("Connection: close\r\n")
	}
	b.WriteString("\r\n")
	b.WriteString(body)
	return b.String()
}

// replay sends the row's request to the proxy at addr and reads the answer.
func (row lokiErrorAnswer) replay(t *testing.T, addr string) lokiErrorReply {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("%s: dial: %v", row.ID, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	if _, err := io.WriteString(conn, row.rawRequest()); err != nil {
		// A server may answer and close before it reads a large body.
		t.Logf("%s: write: %v", row.ID, err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: row.Method})
	if err != nil {
		t.Fatalf("%s: read answer: %v", row.ID, err)
	}
	defer resp.Body.Close()
	reply := lokiErrorReply{
		Status:      resp.StatusCode,
		ContentType: resp.Header.Get("Content-Type"),
		Nosniff:     resp.Header.Get("X-Content-Type-Options"),
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		reply.Body = string(raw)
	}
	return reply
}

// lokiErrorKnownDiffs is the committed list of rows that still differ.
type lokiErrorKnownDiffs struct {
	Description string `json:"description"`
	Ceiling     int    `json:"ceiling"`
	// Rows maps a row id to the proxy's answer today, for the reader: the
	// test compares only whether a row matches Loki.
	Rows map[string]lokiErrorKnownDiff `json:"rows"`
}

type lokiErrorKnownDiff struct {
	Route  string `json:"route"`
	Loki   string `json:"loki"`
	Proxy  string `json:"proxy"`
	Reason string `json:"reason,omitempty"`
}

func summarizeReply(r lokiErrorReply) string {
	body := r.Body
	if r.Status < 300 && len(body) > 60 {
		body = body[:60] + "..."
	} else if len(body) > 160 {
		body = body[:160] + "..."
	}
	return fmt.Sprintf("%d %s %q", r.Status, r.ContentType, body)
}

// newLokiErrorReplayProxy is a proxy as shipped (default configuration, the
// shipped middleware chain) in front of a VictoriaLogs stand-in that answers
// every query call with an empty valid answer, with Loki's max_query_length
// of the recorded stack (30d1h).
func newLokiErrorReplayProxy(t *testing.T) string {
	t.Helper()
	backend := httptest.NewServer(&errLayerBackend{failSig: "\x00none", okBody: "empty"})
	t.Cleanup(backend.Close)
	c := cache.New(time.Minute, 1000)
	t.Cleanup(c.Close)
	p, err := New(Config{
		BackendURL:            backend.URL,
		Cache:                 c,
		LogLevel:              "error",
		BackendTimeout:        5 * time.Second,
		DefaultMaxQueryLength: 721 * time.Hour,
	})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	front := httptest.NewServer(shippedChain(mux))
	t.Cleanup(front.Close)
	return strings.TrimPrefix(front.URL, "http://")
}

// TestLokiErrorAnswersReplay replays every recorded Loki error request
// against the proxy and holds the rows whose answer (status, Content-Type,
// nosniff, body, byte for byte) is not Loki's to the committed known
// differences: a row that newly differs fails, and a listed row that now
// matches fails until it is removed, so the list only shrinks. Rows recorded
// under a configuration the proxy cannot match yet (Loki with auth disabled,
// Loki's lookback and series limits) are replayed but not compared.
// LVP_LOKI_ERROR_ANSWERS_UPDATE=1 rewrites the list.
// conformance: semantics/error-request-validation-parity, semantics/error-layer-body-shape, semantics/parse-error-text-parity, status-400, status-404, status-405, status-500
func TestLokiErrorAnswersReplay(t *testing.T) {
	rows := readLokiErrorAnswers(t)
	addr := newLokiErrorReplayProxy(t)
	got := lokiErrorKnownDiffs{Rows: map[string]lokiErrorKnownDiff{}}
	compared, matched := 0, 0
	for _, row := range rows {
		reply := row.replay(t, addr)
		if row.NotComparable != "" {
			continue
		}
		compared++
		if reply == row.Loki {
			matched++
			continue
		}
		got.Rows[row.ID] = lokiErrorKnownDiff{Route: row.Route, Loki: summarizeReply(row.Loki), Proxy: summarizeReply(reply)}
	}
	t.Logf("%d rows: %d compared, %d match Loki, %d differ", len(rows), compared, matched, len(got.Rows))

	var want lokiErrorKnownDiffs
	raw, err := os.ReadFile(lokiErrorKnownDiffPath)
	if err == nil {
		err = json.Unmarshal(raw, &want)
	}
	if os.Getenv("LVP_LOKI_ERROR_ANSWERS_UPDATE") == "1" {
		got.Description = want.Description
		got.Ceiling = len(got.Rows)
		for id, d := range got.Rows {
			d.Reason = want.Rows[id].Reason
			got.Rows[id] = d
		}
		out, err := json.MarshalIndent(got, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(lokiErrorKnownDiffPath, append(out, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %s: %d rows differ", lokiErrorKnownDiffPath, len(got.Rows))
		return
	}
	if err != nil {
		t.Fatalf("read %s: %v", lokiErrorKnownDiffPath, err)
	}
	if want.Ceiling != len(want.Rows) {
		t.Errorf("%s lists %d rows but its ceiling is %d: the list may not grow, and a fix lowers the ceiling with it", lokiErrorKnownDiffPath, len(want.Rows), want.Ceiling)
	}
	var newDiffs, fixed []string
	for id, d := range got.Rows {
		if _, ok := want.Rows[id]; !ok {
			newDiffs = append(newDiffs, fmt.Sprintf("%s (%s): Loki %s, proxy %s", id, d.Route, d.Loki, d.Proxy))
		}
	}
	for id := range want.Rows {
		if _, ok := got.Rows[id]; !ok {
			fixed = append(fixed, id)
		}
	}
	sort.Strings(newDiffs)
	sort.Strings(fixed)
	for _, d := range newDiffs {
		t.Errorf("new difference from Loki: %s", d)
	}
	if len(fixed) > 0 {
		t.Errorf("rows now answer as Loki does: %s; remove them from %s and lower its ceiling (LVP_LOKI_ERROR_ANSWERS_UPDATE=1)", strings.Join(fixed, ", "), lokiErrorKnownDiffPath)
	}
}

// TestLokiErrorAnswersAreWellFormed: every row is a Loki error answer with a
// request the replay can send, and ids are unique.
// conformance: semantics/error-request-validation-parity
func TestLokiErrorAnswersAreWellFormed(t *testing.T) {
	seen := map[string]bool{}
	for _, row := range readLokiErrorAnswers(t) {
		if seen[row.ID] {
			t.Errorf("%s: duplicate id", row.ID)
		}
		seen[row.ID] = true
		if row.Loki.Status < 300 || row.Method == "" || row.Target == "" || !strings.HasPrefix(row.Target, "/") {
			t.Errorf("%s: not a replayable error row: %+v", row.ID, row)
		}
	}
	if len(seen) == 0 {
		t.Fatal("no rows")
	}
}
