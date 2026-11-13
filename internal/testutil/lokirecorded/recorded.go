// Package lokirecorded reads, replays and rewrites the recorded answers of a real
// Loki to error requests (internal/lokierr/testdata/loki_error_answers.jsonl).
// It is test support: the recorder (test/errorrecord) and the end-to-end
// comparison with a live Loki (test/e2e-compat) share its row type and its
// raw-socket replay, so a request reaches both servers as the exact bytes that
// were recorded (an invalid percent escape stays invalid, a dot segment stays a
// dot segment, HEAD and OPTIONS are sent as they are).
//
// The package imports only the standard library and is never built into the
// proxy.
package lokirecorded

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// SuccessBodyLimit is the length a recorded success body is cut to; a row
// Loki answered without an error is kept only as a probe, and its body is
// not compared.
const SuccessBodyLimit = 120

// Reply is what a client read: the status, the headers a client may rely on
// and the body.
type Reply struct {
	Status      int    `json:"status"`
	ContentType string `json:"content_type"`
	Nosniff     string `json:"nosniff"`
	Body        string `json:"body"`
	// BodyTruncated marks a success body cut to SuccessBodyLimit bytes.
	BodyTruncated bool `json:"body_truncated,omitempty"`
}

// Rebuild rebuilds a request too large to store: a query of a prefix and
// Count units, or a form body of FormBytes bytes.
type Rebuild struct {
	QueryPrefix string `json:"query_prefix"`
	QueryUnit   string `json:"query_unit"`
	QueryCount  int    `json:"query_count"`
	FormBytes   int    `json:"form_bytes"`
}

// Row is one recorded request and the answer Loki gave it.
type Row struct {
	ID      string   `json:"id"`
	Route   string   `json:"route"`
	Method  string   `json:"method"`
	Target  string   `json:"target"`
	Headers []string `json:"headers"`
	Form    string   `json:"form,omitempty"`
	Trigger string   `json:"trigger"`
	Loki    Reply    `json:"loki"`
	// Msg and Args name the internal/lokierr message and the typed inputs
	// its constructor renders the text from; empty for a Loki message no
	// constructor renders yet.
	Msg      string          `json:"msg,omitempty"`
	Args     json.RawMessage `json:"args,omitempty"`
	Generate *Rebuild        `json:"generate,omitempty"`
	// NotComparable says why the row cannot be compared with the proxy yet.
	NotComparable string `json:"not_comparable,omitempty"`
	// Issues and Wins belong to a combined-error row: the message ids of the
	// two errors the request carries, and the one Loki answers.
	Issues []string `json:"issues,omitempty"`
	Wins   string   `json:"wins,omitempty"`
}

// File is a recorded-answers file: its header line and its rows, each kept
// with the exact text of its line.
type File struct {
	// Header is the `_source` text of the first line.
	Header string
	Rows   []Row
	// Lines holds the raw text of each row's line, in row order.
	Lines []string
}

// Read parses a recorded-answers file.
func Read(path string) (*File, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(raw)
}

// Parse parses the bytes of a recorded-answers file: a header line with a
// `_source` key, then one row per line.
func Parse(raw []byte) (*File, error) {
	f := &File{}
	sc := bufio.NewScanner(bytes.NewReader(raw))
	sc.Buffer(make([]byte, 0, 1<<16), 1<<22)
	first := true
	for sc.Scan() {
		line := sc.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		if first {
			first = false
			var head struct {
				Source string `json:"_source"`
			}
			if err := json.Unmarshal([]byte(line), &head); err != nil {
				return nil, fmt.Errorf("header line: %w", err)
			}
			if head.Source == "" {
				return nil, errors.New("header line has no _source")
			}
			f.Header = head.Source
			continue
		}
		var row Row
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, fmt.Errorf("row line %d: %w", len(f.Rows)+1, err)
		}
		f.Rows = append(f.Rows, row)
		f.Lines = append(f.Lines, line)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if first {
		return nil, errors.New("empty file")
	}
	return f, nil
}

// Comparable reports whether the row's answer can be compared with the
// proxy's.
func (r Row) Comparable() bool { return r.NotComparable == "" }

// Request is the row's request as bytes on the wire: the target as recorded,
// its headers and form body, with the connection closed after the answer.
func (r Row) Request() string {
	target, body := r.Target, r.Form
	if g := r.Generate; g != nil {
		if g.QueryCount > 0 {
			target += "?query=" + url.QueryEscape(g.QueryPrefix+strings.Repeat(g.QueryUnit, g.QueryCount))
		}
		if g.FormBytes > 0 {
			body = "query=" + strings.Repeat("a", g.FormBytes-len("query="))
		}
	}
	return rawRequest(r.Method, target, r.Headers, body)
}

func rawRequest(method, target string, headers []string, body string) string {
	var b strings.Builder
	b.Grow(len(target) + len(body) + 256)
	fmt.Fprintf(&b, "%s %s HTTP/1.1\r\nHost: loki\r\n", method, target)
	connection := false
	for _, h := range headers {
		b.WriteString(h)
		b.WriteString("\r\n")
		connection = connection || strings.HasPrefix(strings.ToLower(h), "connection:")
	}
	if body != "" || method == http.MethodPost {
		fmt.Fprintf(&b, "Content-Length: %d\r\n", len(body))
	}
	if !connection {
		b.WriteString("Connection: close\r\n")
	}
	b.WriteString("\r\n")
	b.WriteString(body)
	return b.String()
}

// Shifted is a copy of the row whose absolute start, end and time parameters
// are moved by delta, so a recording made on one day replays on another: a
// stack whose Loki limits how far back a query may reach would answer the old
// timestamps with a lookback error. Values that are not plain digits (a
// number a row records as invalid, a negative one) stay as recorded, as does
// every other byte of the target. A value in nanoseconds is moved by delta in
// nanoseconds.
func (r Row) Shifted(delta time.Duration) Row {
	path, query, ok := strings.Cut(r.Target, "?")
	if !ok || delta == 0 {
		return r
	}
	parts := strings.Split(query, "&")
	for i, p := range parts {
		name, value, found := strings.Cut(p, "=")
		if !found || (name != "start" && name != "end" && name != "time") || value == "" {
			continue
		}
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil || strconv.FormatInt(n, 10) != value {
			continue
		}
		switch {
		case n >= 1_000_000_000 && n < 100_000_000_000: // seconds
			n += int64(delta / time.Second)
		case n >= 1_000_000_000_000_000_000: // nanoseconds
			n += int64(delta)
		default:
			continue
		}
		parts[i] = name + "=" + strconv.FormatInt(n, 10)
	}
	r.Target = path + "?" + strings.Join(parts, "&")
	return r
}

// Replay sends the row's request to the server at addr (host:port) over a
// plain socket and reads the answer. The connection is closed after it.
func Replay(addr string, r Row, timeout time.Duration) (Reply, error) {
	dialer := net.Dialer{Timeout: 5 * time.Second}
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return Reply{}, fmt.Errorf("%s: dial: %w", r.ID, err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(timeout))
	// A server may answer and close before it reads a large body: the write
	// error is not an error of the replay, the answer is read next.
	_, _ = io.WriteString(conn, r.Request())
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: r.Method})
	if err != nil {
		return Reply{}, fmt.Errorf("%s: read answer: %w", r.ID, err)
	}
	defer resp.Body.Close()
	reply := Reply{
		Status:      resp.StatusCode,
		ContentType: resp.Header.Get("Content-Type"),
		Nosniff:     resp.Header.Get("X-Content-Type-Options"),
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		reply.Body = string(raw)
	}
	return reply, nil
}

// ForRecording is the reply as the file stores it: the body of an answer
// that is not an error is cut to SuccessBodyLimit bytes and marked.
func (r Reply) ForRecording() Reply {
	if r.Status < 300 && len(r.Body) > SuccessBodyLimit {
		r.Body = r.Body[:SuccessBodyLimit]
		r.BodyTruncated = true
	}
	return r
}

// Summary is a short one-line form of a reply, for failure messages.
func (r Reply) Summary() string {
	body := r.Body
	limit := 160
	if r.Status < 300 {
		limit = 60
	}
	if len(body) > limit {
		body = body[:limit] + "..."
	}
	return fmt.Sprintf("%d %s %q", r.Status, r.ContentType, body)
}

// WithLoki returns the text of a row's line with its "loki" object replaced
// by reply, every other key and byte of the line kept in place, so a
// re-recording changes only what Loki answered differently.
func WithLoki(line string, reply Reply) (string, error) {
	dec := json.NewDecoder(strings.NewReader(line))
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return "", fmt.Errorf("row is not a JSON object")
	}
	var out bytes.Buffer
	out.WriteByte('{')
	first := true
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return "", err
		}
		key, _ := keyTok.(string)
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return "", err
		}
		if key == "loki" {
			value, err = marshal(reply)
			if err != nil {
				return "", err
			}
		}
		if !first {
			out.WriteByte(',')
		}
		first = false
		k, _ := marshal(key)
		out.Write(k)
		out.WriteByte(':')
		out.Write(value)
	}
	out.WriteByte('}')
	return out.String(), nil
}

// WithHeader returns the header line with its recorded Loki version and date
// replaced, the rest of the text kept.
func WithHeader(source, version string, day time.Time) string {
	const marker = " answers recorded over HTTP on "
	i := strings.Index(source, marker)
	if i < 0 {
		return source
	}
	start := strings.LastIndex(source[:i], "Loki ")
	if start < 0 {
		return source
	}
	rest := source[i+len(marker):]
	end := len("2006-01-02")
	if len(rest) < end {
		return source
	}
	return source[:start] + "Loki " + version + marker + day.UTC().Format("2006-01-02") + rest[end:]
}

// HeaderLine is the first line of a file for source.
func HeaderLine(source string) (string, error) {
	b, err := marshal(map[string]string{"_source": source})
	return string(b), err
}

func marshal(v any) (json.RawMessage, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(b.Bytes(), "\n"), nil
}
