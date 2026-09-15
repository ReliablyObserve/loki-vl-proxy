// Package degraded recognises responses that answered HTTP 200 without doing
// the full work: partial, stale or fallback answers, and answers that carry
// warnings. Such a response cannot back a benchmark number.
package degraded

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
)

// Headers are the response headers that mark a degraded answer:
//   - Warning: RFC 7234 warning, set by Loki-compatible partial responses;
//   - X-Proxy-Stale-Response: the proxy served an expired cache entry after a
//     backend failure;
//   - X-Proxy-Drilldown-Hits-Fallback: the proxy fell back from its native
//     stats path;
//   - X-Proxy-Upstream-Status / X-Proxy-Upstream-Error: the proxy converted a
//     VictoriaLogs error into an empty partial answer;
//   - X-Loki-VL-Partial-Response: query_range windows are missing;
//   - X-Multi-Tenant-Partial-Failures: some tenants failed.
var Headers = []string{
	"Warning",
	"X-Proxy-Stale-Response",
	"X-Proxy-Drilldown-Hits-Fallback",
	"X-Proxy-Upstream-Status",
	"X-Proxy-Upstream-Error",
	"X-Loki-VL-Partial-Response",
	"X-Multi-Tenant-Partial-Failures",
}

// FromHeaders returns one reason per degraded-response header present in h.
func FromHeaders(h http.Header) []string {
	var out []string
	for _, name := range Headers {
		if v := h.Values(name); len(v) > 0 {
			out = append(out, fmt.Sprintf("header %s: %s", name, strings.Join(v, "; ")))
		}
	}
	return out
}

// FromBody returns the entries of a non-empty top-level "warnings" array in a
// JSON object body (Loki's partial-result channel, mirrored by the proxy).
// Bodies that are not JSON objects carry no warnings.
func FromBody(body []byte) []string {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '{' || !bytes.Contains(trimmed, []byte(`"warnings"`)) {
		return nil
	}
	var resp struct {
		Warnings []any `json:"warnings"`
	}
	if err := json.Unmarshal(trimmed, &resp); err != nil {
		return nil
	}
	out := make([]string, 0, len(resp.Warnings))
	for _, w := range resp.Warnings {
		out = append(out, fmt.Sprintf("warnings: %v", w))
	}
	sort.Strings(out)
	return out
}

// Reasons combines FromHeaders and FromBody.
func Reasons(h http.Header, body []byte) []string {
	return append(FromHeaders(h), FromBody(body)...)
}

// warningsKey is the JSON string "warnings". Inside a JSON string every quote
// is escaped, so an unescaped occurrence is a key or a whole string value; only
// a key is followed by a colon.
var warningsKey = []byte(`"warnings"`)

// Scanner is an io.Writer that detects a non-empty "warnings" array while a
// response body streams through it, without buffering the body. Warnings is
// true once `"warnings"` is followed by `:`, `[` and an element (whitespace
// allowed between them). Only a short tail of each write is retained between writes.
type Scanner struct {
	tail     []byte
	Warnings bool
}

// maxTail bounds the bytes kept between writes and the bytes of the next write
// joined to them.
const maxTail = 256

// Write implements io.Writer.
func (s *Scanner) Write(p []byte) (int, error) {
	if s.Warnings {
		return len(p), nil
	}
	if len(s.tail) > 0 {
		head := p
		if len(head) > maxTail {
			head = head[:maxTail]
		}
		joined := append(s.tail, head...)
		found, keep := scan(joined)
		if found {
			s.Warnings, s.tail = true, nil
			return len(p), nil
		}
		if len(head) == len(p) {
			s.setTail(joined, keep)
			return len(p), nil
		}
	}
	found, keep := scan(p)
	if found {
		s.Warnings, s.tail = true, nil
		return len(p), nil
	}
	s.setTail(p, keep)
	return len(p), nil
}

func (s *Scanner) setTail(buf []byte, keep int) {
	if lowest := len(buf) - maxTail; keep < lowest {
		keep = lowest
	}
	// buf may share s.tail's backing array; append copies with memmove semantics.
	s.tail = append(s.tail[:0], buf[keep:]...)
}

// scan reports whether buf holds a non-empty warnings array and, if not, the
// offset from which bytes must be kept because a match may continue in the
// next write.
func scan(buf []byte) (bool, int) {
	keep := len(buf) - len(warningsKey) + 1
	if keep < 0 {
		keep = 0
	}
	for from := 0; ; {
		i := bytes.Index(buf[from:], warningsKey)
		if i < 0 {
			return false, keep
		}
		at := from + i
		switch nonEmptyArrayAt(buf, at+len(warningsKey)) {
		case verdictYes:
			return true, 0
		case verdictNeedMore:
			if at < keep {
				keep = at
			}
		}
		from = at + 1
	}
}

type verdict int

const (
	verdictNo verdict = iota
	verdictYes
	verdictNeedMore
)

// nonEmptyArrayAt reports whether buf[i:] starts, with optional whitespace
// between the tokens, with `:` and a JSON array holding at least one element.
func nonEmptyArrayAt(buf []byte, i int) verdict {
	i = skipSpace(buf, i)
	if i >= len(buf) {
		return verdictNeedMore
	}
	if buf[i] != ':' {
		return verdictNo
	}
	i = skipSpace(buf, i+1)
	if i >= len(buf) {
		return verdictNeedMore
	}
	if buf[i] != '[' {
		return verdictNo
	}
	i = skipSpace(buf, i+1)
	if i >= len(buf) {
		return verdictNeedMore
	}
	if buf[i] == ']' {
		return verdictNo
	}
	return verdictYes
}

func skipSpace(buf []byte, i int) int {
	for i < len(buf) && (buf[i] == ' ' || buf[i] == '\t' || buf[i] == '\n' || buf[i] == '\r') {
		i++
	}
	return i
}
