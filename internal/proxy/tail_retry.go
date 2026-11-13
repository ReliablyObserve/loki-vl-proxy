package proxy

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"math/rand/v2"
	"net/http"
	"slices"
	"time"

	"github.com/gorilla/websocket"
)

// A live tail outlives a short VictoriaLogs outage. When a call fails for a
// reason a retry can fix (the backend is unreachable, restarting or behind a
// front proxy that answers 429 or 5xx), the client's websocket stays open and
// the proxy reopens the stream, or keeps polling, with an exponential backoff
// until -tail.backend-retry-window has passed. Loki's querier does the same
// when its ingesters restart. Failures a retry cannot fix end the tail at once
// with Loki's 1011, as does an outage that outlasts the window.
const (
	// tailRetryInitialBackoff and tailRetryMaxBackoff bound the wait between
	// attempts: it doubles from the first to the second and is jittered.
	tailRetryInitialBackoff = 250 * time.Millisecond
	tailRetryMaxBackoff     = 5 * time.Second
	// tailRetryStableAfter is how long a reopened stream must stay up, with
	// or without rows, before the outage counts as over. A backend that
	// accepts the call and drops it at once keeps spending the window.
	tailRetryStableAfter = 5 * time.Second
	// tailResumeLookback is VictoriaLogs' own tail window: its first read
	// covers the five seconds before the call.
	tailResumeLookback = 5 * time.Second
	// tailResumeMargin widens the reopened read backwards so a clock offset
	// between the proxy and VictoriaLogs cannot lose a row; the rows it
	// returns again are dropped.
	tailResumeMargin = time.Second
	// tailResumeMaxOffset caps the span a reopened read asks for.
	tailResumeMaxOffset = 24 * time.Hour
	// tailResumeDedupeFor is how long, from the first rows of a reopened
	// stream, the rows at or before the last delivered one are dropped:
	// VictoriaLogs sends the whole resumed window in its first read, however
	// long that read takes to start, and a second at most to deliver.
	tailResumeDedupeFor = 3 * time.Second
)

// Operation outcomes of loki_vl_proxy_internal_operation_total{operation="tail_backend_retry"}.
const (
	tailRetryOpName       = "tail_backend_retry"
	tailRetryOutRetried   = "retried"   // a wait before another attempt began
	tailRetryOutRecovered = "recovered" // a call succeeded again; the duration is the outage
	tailRetryOutExhausted = "exhausted" // the window passed; the tail closed with 1011
	tailRetryOutPermanent = "permanent" // a failure a retry cannot fix; the tail closed with 1011
)

// errTailMalformedRow is a stream row that is not a JSON object: a broken
// body, not a transient failure.
var errTailMalformedRow = errors.New("tail row is not JSON")

type tailRetryVerdict int

const (
	tailRetryOff       tailRetryVerdict = iota // retries are disabled
	tailRetryAgain                             // wait, then try again
	tailRetryPermanent                         // the failure cannot be fixed by retrying
	tailRetryExhausted                         // the window has passed
)

// tailRetry is one tail's retry state: the time its current outage began and
// the next backoff. It is used by a single goroutine.
type tailRetry struct {
	window, initial, max time.Duration
	backoff              time.Duration
	since                time.Time // start of the current outage; zero while healthy
}

func (p *Proxy) newTailRetry() *tailRetry {
	return &tailRetry{window: p.tailBackendRetryWindow, initial: p.tailRetryInitialBackoff, max: p.tailRetryMaxBackoff}
}

// next decides what to do after the call that failed with err: retry after the
// returned wait, or end the tail. The wait never passes the end of the window,
// so the last attempt is made when the window closes.
func (r *tailRetry) next(err error, now time.Time) (time.Duration, tailRetryVerdict) {
	if r.window <= 0 {
		return 0, tailRetryOff
	}
	if !tailRetryable(err) {
		return 0, tailRetryPermanent
	}
	if r.since.IsZero() {
		r.since = now
		r.backoff = r.initial
	}
	remaining := r.window - now.Sub(r.since)
	if remaining <= 0 {
		return 0, tailRetryExhausted
	}
	// Equal jitter: half the backoff plus a random half, so tails that lost
	// the same backend do not return together.
	wait := r.backoff/2 + rand.N(r.backoff/2+1)
	r.backoff = r.backoff * 2
	if r.backoff > r.max {
		r.backoff = r.max
	}
	if wait > remaining {
		wait = remaining
	}
	return wait, tailRetryAgain
}

// outage reports whether a failure is being retried, and for how long.
func (r *tailRetry) outage(now time.Time) (time.Duration, bool) {
	if r.since.IsZero() {
		return 0, false
	}
	return now.Sub(r.since), true
}

// reset ends the outage: the backend is healthy again.
func (r *tailRetry) reset() { r.since = time.Time{} }

// tailRetryable reports whether a failed VictoriaLogs call may succeed when
// repeated: a transport failure (refused, reset, timed out, a stream that
// ended or broke) and a 429 or 5xx answer. A malformed or oversize row, a
// client cancel and every other 4xx (VictoriaLogs rejects the query) are
// final.
func tailRetryable(err error) bool {
	if err == nil || errors.Is(err, errTailMalformedRow) || errors.Is(err, bufio.ErrTooLong) || isCanceledErr(err) {
		return false
	}
	var st *upstreamStatusError
	if errors.As(err, &st) {
		return st.status == http.StatusTooManyRequests || st.status >= http.StatusInternalServerError
	}
	return true
}

// tailSleep waits d, keeping the client connection alive with the ping ticks.
// It reports false once the client is gone.
func (p *Proxy) tailSleep(ctx context.Context, conn tailConn, ping <-chan time.Time, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return true
		case <-ping:
			if err := p.writeTailMessage(conn, websocket.PingMessage, nil); err != nil {
				p.log.Debug("websocket ping failed, client disconnected", "error", err)
				return false
			}
		}
	}
}

// tailRetryEnded closes a tail whose retries are over with Loki's 1011.
func (p *Proxy) tailRetryEnded(ctx context.Context, conn tailConn, retry *tailRetry, err error, verdict tailRetryVerdict) {
	switch verdict {
	case tailRetryPermanent:
		p.observeInternalOperation(ctx, tailRetryOpName, tailRetryOutPermanent, 0)
	case tailRetryExhausted:
		outage, _ := retry.outage(time.Now())
		p.observeInternalOperation(ctx, tailRetryOpName, tailRetryOutExhausted, outage)
	}
	p.closeTailWithError(ctx, conn, err)
}

// tailRetryRecovered counts the end of an outage that was retried.
func (p *Proxy) tailRetryRecovered(ctx context.Context, retry *tailRetry) {
	if outage, ok := retry.outage(time.Now()); ok {
		p.observeInternalOperation(ctx, tailRetryOpName, tailRetryOutRecovered, outage)
	}
}

// streamNativeTail forwards VictoriaLogs' native tail to the client and, when
// the stream fails for a reason a retry can fix, reopens it from where the
// client left off. resp is the stream already open, or nil with openErr the
// failure of the first call. It returns when the client is gone or the tail
// has been closed with an error.
func (p *Proxy) streamNativeTail(ctx context.Context, conn tailConn, tp *tailPipeline, logsqlQuery string, resp *http.Response, openErr error, ping <-chan time.Time) {
	retry := p.newTailRetry()
	tp.resume.start(retry.window > 0, time.Now())
	for {
		if resp != nil {
			opened := time.Now()
			res := p.forwardNativeTail(ctx, conn, tp, resp.Body, ping)
			_ = resp.Body.Close()
			resp = nil
			if res.clientGone || ctx.Err() != nil {
				return
			}
			openErr = res.err
			if res.rows > 0 || time.Since(opened) >= tailRetryStableAfter {
				// The stream was healthy until it failed: a new outage, and
				// VictoriaLogs had sent everything up to now.
				retry.reset()
				tp.resume.advance(time.Now())
			}
		}
		for resp == nil {
			wait, verdict := retry.next(openErr, time.Now())
			if verdict != tailRetryAgain {
				p.tailRetryEnded(ctx, conn, retry, openErr, verdict)
				return
			}
			p.observeInternalOperation(ctx, tailRetryOpName, tailRetryOutRetried, wait)
			p.log.Debug("tail backend call failed, retrying", "wait", wait, "error", p.sanitizeUpstreamError(openErr))
			if !p.tailSleep(ctx, conn, ping, wait) {
				return
			}
			resp, _, openErr = p.openNativeTailStreamFrom(ctx, logsqlQuery, tp.resume.reopen(time.Now()))
			if ctx.Err() != nil {
				if resp != nil {
					_ = resp.Body.Close()
				}
				return
			}
		}
		p.tailRetryRecovered(ctx, retry)
	}
}

// tailResume is what a reopened native stream needs to carry on without
// repeating or losing a row: where to start reading, and the entries already
// delivered at the newest timestamp.
type tailResume struct {
	track bool // retries are enabled: delivered entries are noted
	// cursor is the time VictoriaLogs is asked to read from (UnixNano): the
	// newest delivered entry, or five seconds before the last time the
	// stream was known healthy, whichever is later.
	cursor      int64
	last        int64               // newest delivered entry (UnixNano); 0 before the first
	keys        map[uint64]struct{} // hashes of the delivered entries at last
	dedupe      bool                // a stream was reopened and has not yet sent its first rows' window
	dedupeUntil time.Time           // end of the dedupe, set by the first rows after the reopen
	hash        maphash.Hash        // keys' hash, seeded per tail
	names       []string            // scratch for a sorted label list
}

// start notes the time the tail connected: VictoriaLogs' first read covers
// the lookback before it.
func (r *tailResume) start(track bool, now time.Time) {
	r.track = track
	r.cursor = now.Add(-tailResumeLookback).UnixNano()
}

// advance moves the cursor after a healthy stream failed at now: VictoriaLogs
// had sent every row it could see, a lookback before now included.
func (r *tailResume) advance(now time.Time) {
	r.cursor = max(r.cursor, r.last, now.Add(-tailResumeLookback).UnixNano())
}

// reopen is the start_offset for a stream opened at now, and arms the
// dedupe of the rows it returns again. The dedupe runs from the first rows
// the reopened stream sends, not from the call: VictoriaLogs answers the
// headers before it reads the resumed window.
func (r *tailResume) reopen(now time.Time) time.Duration {
	r.dedupe = true
	r.dedupeUntil = time.Time{}
	offset := now.Sub(time.Unix(0, r.cursor))
	if offset > tailResumeMaxOffset {
		offset = tailResumeMaxOffset // an outage the window bounds never reaches it
	}
	return max((offset+tailResumeMargin).Truncate(time.Millisecond)+time.Millisecond, time.Millisecond)
}

// filter drops, shortly after a reopen, the entries at or before the last
// delivered one: those below its timestamp, and those at it that were sent.
func (r *tailResume) filter(entries []tailEntry, now time.Time) []tailEntry {
	if !r.track || r.last == 0 || !r.dedupe {
		return entries
	}
	if r.dedupeUntil.IsZero() {
		r.dedupeUntil = now.Add(tailResumeDedupeFor)
	} else if !now.Before(r.dedupeUntil) {
		r.dedupe = false
		return entries
	}
	kept := entries[:0]
	for _, e := range entries {
		switch {
		case e.ts < r.last:
			continue
		case e.ts == r.last:
			if _, sent := r.keys[r.entryKey(e)]; sent {
				continue
			}
		}
		kept = append(kept, e)
	}
	return kept
}

// observe notes the entries about to be delivered, in timestamp order.
func (r *tailResume) observe(entries []tailEntry) {
	if !r.track || len(entries) == 0 {
		return
	}
	top := entries[len(entries)-1].ts
	if top > r.last {
		r.last = top
		if r.keys == nil {
			r.keys = make(map[uint64]struct{}, 2)
		}
		clear(r.keys)
	}
	if top < r.last {
		return
	}
	for i := len(entries) - 1; i >= 0 && entries[i].ts == top; i-- {
		r.keys[r.entryKey(entries[i])] = struct{}{}
	}
}

// entryKey identifies a delivered entry by a hash of its timestamp, labels
// and line, so noting an entry allocates nothing. A collision (2^-64) would
// drop one entry sent at the very timestamp of the last, right after a reopen.
func (r *tailResume) entryKey(e tailEntry) uint64 {
	var line string
	switch v := e.value.(type) {
	case []string:
		if len(v) > 1 {
			line = v[1]
		}
	case []interface{}:
		if len(v) > 1 {
			line, _ = v[1].(string)
		}
	}
	r.names = r.names[:0]
	for k := range e.stream {
		r.names = append(r.names, k)
	}
	slices.Sort(r.names)
	h := &r.hash
	h.Reset()
	var ts [8]byte
	for i := range ts {
		ts[i] = byte(e.ts >> (8 * i))
	}
	_, _ = h.Write(ts[:])
	for _, k := range r.names {
		_ = h.WriteByte(0)
		_, _ = h.WriteString(k)
		_ = h.WriteByte('=')
		_, _ = h.WriteString(e.stream[k])
	}
	_ = h.WriteByte(0)
	_, _ = h.WriteString(line)
	return h.Sum64()
}

// splitTailRows splits a VictoriaLogs NDJSON stream into rows and drops a last
// row the stream ended in the middle of: partial is set for it, and the
// reopened stream sends the row whole.
func splitTailRows(partial *bool) bufio.SplitFunc {
	return func(data []byte, atEOF bool) (int, []byte, error) {
		if atEOF && len(data) > 0 && bytes.IndexByte(data, '\n') < 0 {
			*partial = true
			return len(data), nil, nil
		}
		return bufio.ScanLines(data, atEOF)
	}
}

// tailRowShapeOK is the cheap per-row check of a native stream: a JSON object
// starts with { and ends with }. It is not a validation, a row it passes may
// still be skipped by the conversion.
func tailRowShapeOK(row []byte) bool {
	return len(row) >= 2 && row[0] == '{' && row[len(row)-1] == '}'
}

// errTailRowNotJSON wraps errTailMalformedRow for the log line.
func errTailRowNotJSON() error { return fmt.Errorf("native tail stream: %w", errTailMalformedRow) }
