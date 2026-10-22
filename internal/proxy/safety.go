package proxy

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
)

var errBodyTooLarge = errors.New("body exceeds configured limit")

// backendBodyTooLargeError is a backend response above the bytes the proxy
// buffers for it: HTTP 502 naming the flag, never a truncated body. It matches
// errBodyTooLarge, so the paths that already degrade on that sentinel keep
// doing so.
type backendBodyTooLargeError struct {
	limit int64
}

func (e *backendBodyTooLargeError) Error() string {
	return fmt.Sprintf("backend response exceeds %d bytes; narrow the query or increase -backend-max-buffered-response-bytes", e.limit)
}

func (e *backendBodyTooLargeError) Is(target error) bool { return target == errBodyTooLarge }

// readBackendBody reads a backend response body up to limit bytes; a longer
// body fails with a backendBodyTooLargeError instead of a truncated read.
func readBackendBody(r io.Reader, limit int64) ([]byte, error) {
	body, err := readBodyLimited(r, limit)
	if errors.Is(err, errBodyTooLarge) {
		return nil, &backendBodyTooLargeError{limit: limit}
	}
	return body, err
}

// readBufferedBackendBody reads a backend response the proxy buffers whole,
// bounded by -backend-max-buffered-response-bytes.
func (p *Proxy) readBufferedBackendBody(r io.Reader) ([]byte, error) {
	return readBackendBody(r, int64(p.limits().BufferedBackendBodyBytes))
}

func safeAddCap(parts ...int) int {
	total := 0
	for _, part := range parts {
		if part <= 0 {
			continue
		}
		if total > math.MaxInt-part {
			return math.MaxInt
		}
		total += part
	}
	return total
}

func readBodyLimited(r io.Reader, limit int64) ([]byte, error) {
	if limit <= 0 {
		return io.ReadAll(r)
	}
	cap := limit + 1
	if cap > 128*1024 {
		cap = 128 * 1024
	}
	buf := bytes.NewBuffer(make([]byte, 0, cap))
	_, err := buf.ReadFrom(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	body := buf.Bytes()
	if int64(len(body)) > limit {
		return body[:limit], errBodyTooLarge
	}
	return body, nil
}
