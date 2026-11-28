package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// handleTail bridges Loki's WebSocket tail to VL's NDJSON streaming tail.
// Loki: ws:///loki/api/v1/tail?query={...}&start=...&limit=...
// VL:   GET /select/logsql/tail?query=...
func (p *Proxy) handleTail(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	// The route's query rule (lokiTailQuery) answered an empty query with
	// Loki's parse error before this handler ran.
	logqlQuery := r.FormValue("query")

	// The VictoriaLogs query of a query_range request: the stages
	// VictoriaLogs cannot run like Loki run on the proxy (tailPipeline).
	logsqlQuery, err := p.translateLogResponseQuery(r.Context(), logqlQuery)
	if err != nil {
		status := p.fail(w, r, queryRefusal(err))
		p.metrics.RecordRequest("tail", status, time.Since(start))
		return
	}
	// Loki encodes tail frames like query responses, including the
	// categorize-labels flag.
	tp := p.newTailPipeline(logqlQuery, p.shouldEmitStructuredMetadata(r))
	if tp.lineFormat != "" {
		// A template the parser rejects is answered with 400 and Loki's
		// parse error before the upgrade. Loki itself upgrades the
		// connection and its tailer fails, so the client gets nothing; the
		// status is a deliberate, documented deviation (conformance case
		// profiles/tail-line-format-parse-error-status).
		if _, err := applyLineFormatTemplateWithContext(r.Context(), nil, tp.lineFormat, lineFormatAfter{}); err != nil {
			status := p.fail(w, r, err)
			p.metrics.RecordRequest("tail", status, time.Since(start))
			return
		}
	}

	if origin := strings.TrimSpace(r.Header.Get("Origin")); origin != "" && !p.isAllowedTailOrigin(origin) {
		p.writeError(w, http.StatusForbidden, "tail origin not allowed")
		p.metrics.RecordRequest("tail", http.StatusForbidden, time.Since(start))
		return
	}

	if !tailHandshakeComplete(r) {
		// The handshake fails whatever VictoriaLogs holds: answer it before
		// the history read (the upgrader writes its own 400), as Loki's
		// router does before any query work.
		upgrader := p.tailUpgrader()
		if _, err := upgrader.Upgrade(w, r, nil); err != nil { // nosemgrep: go.gorilla.security.audit.websocket-missing-origin-check -- CheckOrigin is set in tailUpgrader()
			p.log.Error("websocket upgrade failed", "error", err)
		}
		p.metrics.RecordRequest("tail", http.StatusBadRequest, time.Since(start))
		return
	}

	r = p.withRequestScope(r)

	tailCtx, tailCancel := context.WithCancel(r.Context())
	defer tailCancel()

	history, err := p.fetchTailHistory(tailCtx, logsqlQuery, r.FormValue("start"), r.FormValue("limit"), tp)
	if err != nil {
		statusCode := p.writeErr(w, tailCtx, err)
		p.metrics.RecordRequest("tail", statusCode, time.Since(start))
		return
	}

	// Upgrade immediately after local validation so slow or blocking native tail
	// headers do not break the client handshake. Native tail remains a best-effort
	// path; if it stalls or isn't available, synthetic polling takes over.
	upgrader := p.tailUpgrader()
	conn, err := upgrader.Upgrade(w, r, nil) // nosemgrep: go.gorilla.security.audit.websocket-missing-origin-check -- CheckOrigin is set in tailUpgrader()
	if err != nil {
		p.log.Error("websocket upgrade failed", "error", err)
		p.metrics.RecordRequest("tail", http.StatusBadRequest, time.Since(start))
		return
	}
	defer func() { _ = conn.Close() }()
	// Tail is server-to-client data. Preserve control frames and tolerate small
	// legacy client messages, but never allocate an arbitrary client payload.
	conn.SetReadLimit(4096)
	p.metrics.RecordRequest("tail", http.StatusOK, time.Since(start))

	// Start a read loop to detect client disconnect (WebSocket protocol requires it).
	// When client closes, this goroutine exits and wsCtx is canceled.
	wsCtx, wsCancel := context.WithCancel(tailCtx)
	defer wsCancel()
	go func() {
		defer tailCancel()
		for {
			_, reader, err := conn.NextReader()
			if err != nil {
				return
			}
			if _, err := io.Copy(io.Discard, reader); err != nil {
				return
			}
		}
	}()

	pingTicker := time.NewTicker(time.Second)
	defer pingTicker.Stop()

	// Loki sends the entries since start, up to limit, before the live ones.
	if err := p.sendTailHistory(wsCtx, conn, tp, history); err != nil {
		return
	}

	if p.tailMode == TailModeSynthetic {
		p.log.Debug("tail connected", "logql", redactQuery(logqlQuery, p.debugLogRawQueries), "logsql", redactQuery(logsqlQuery, p.debugLogRawQueries), "native", false, "fallback", "forced synthetic tail mode")
		p.streamSyntheticTail(wsCtx, conn, logsqlQuery, tp, history)
		return
	}

	resp, nativeTail, fallbackErr := p.openNativeTailStream(wsCtx, logsqlQuery)
	p.log.Debug("tail connected", "logql", redactQuery(logqlQuery, p.debugLogRawQueries), "logsql", redactQuery(logsqlQuery, p.debugLogRawQueries), "native", nativeTail, "fallback", fallbackErr)
	if !nativeTail {
		if p.tailMode == TailModeNative {
			// A native-only tail has nothing to fall back to: a failure a
			// retry can fix is retried, the rest closes the tail.
			p.streamNativeTail(wsCtx, conn, tp, logsqlQuery, nil, fallbackErr, pingTicker.C)
			return
		}
		p.streamSyntheticTail(wsCtx, conn, logsqlQuery, tp, history)
		return
	}

	p.streamNativeTail(wsCtx, conn, tp, logsqlQuery, resp, nil, pingTicker.C)
}

// tailForwardResult is how a forwarded native stream ended.
type tailForwardResult struct {
	// err is why a stream the client still wanted ended: VictoriaLogs closed
	// it or it broke. It is nil when the client went away.
	err        error
	clientGone bool
	rows       int // rows forwarded
}

// forwardNativeTail reads VictoriaLogs' NDJSON tail stream and forwards it
// as Loki tail frames until the stream ends, the client goes away or a
// write fails. It does not close the tail: the caller decides whether the
// failure is retried or ends the tail.
func (p *Proxy) forwardNativeTail(wsCtx context.Context, conn tailConn, tp *tailPipeline, body io.Reader, ping <-chan time.Time) (res tailForwardResult) {
	// Read VL NDJSON stream and forward as Loki WebSocket frames. The
	// channel holds one frame's worth of rows, so rows that arrive together
	// are converted and sent together while a slow client still holds the
	// reader back.
	lineCh := make(chan []byte, maxTailFrameEntries)
	errCh := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(body)
		scanner.Buffer(make([]byte, 0, tailScanBufferInitial), 1024*1024) // 1MB max line
		var partial bool
		scanner.Split(splitTailRows(&partial))
		for scanner.Scan() {
			row := bytes.TrimSpace(scanner.Bytes())
			if len(row) == 0 {
				continue
			}
			if !tailRowShapeOK(row) {
				// A body that is not NDJSON of objects is broken, not a
				// moment of trouble: no retry reads it any better.
				errCh <- errTailRowNotJSON()
				return
			}
			line := append([]byte(nil), row...)
			select {
			case lineCh <- line:
			case <-wsCtx.Done():
				return
			}
		}
		err := scanner.Err()
		if err == nil && partial {
			err = io.ErrUnexpectedEOF
		}
		errCh <- err
	}()

	// rows counts the entries delivered to the client, not the rows read: the
	// rows a reopened stream sends again are dropped.
	delivered := tp.delivered
	defer func() { res.rows = tp.delivered - delivered }()
	for {
		select {
		case <-wsCtx.Done():
			res.clientGone = true
			return res
		case <-ping:
			if err := p.writeTailMessage(conn, websocket.PingMessage, nil); err != nil {
				p.log.Debug("websocket ping failed, client disconnected", "error", err)
				res.clientGone = true
				return res
			}
		case err := <-errCh:
			// Rows read before the stream ended are still sent.
			var rest []byte
		flush:
			for {
				select {
				case more := <-lineCh:
					rest = append(append(rest, more...), '\n')
				default:
					break flush
				}
			}
			if len(rest) > 0 {
				if werr := p.writeTailRows(wsCtx, conn, tp, rest); werr != nil {
					res.clientGone = true
					return res
				}
			}
			// A VictoriaLogs tail has no end of its own: a stream that stops,
			// cleanly or not, while the client is still connected is a failed
			// call; the caller tells the client or reopens the stream.
			if wsCtx.Err() != nil {
				res.clientGone = true
				return res
			}
			if err == nil {
				err = errTailStreamEnded
			}
			res.err = err
			return res
		case line := <-lineCh:
			// Rows that arrived together go out together, at most
			// maxTailFrameEntries a batch.
			batch := append(append([]byte(nil), line...), '\n')
		drain:
			for n := 1; n < maxTailFrameEntries; n++ {
				select {
				case more := <-lineCh:
					batch = append(append(batch, more...), '\n')
				default:
					break drain
				}
			}
			if err := p.writeTailRows(wsCtx, conn, tp, batch); err != nil {
				p.log.Debug("websocket write failed, client disconnected", "error", err)
				res.clientGone = true
				return res
			}
		}
	}
}

// tailSettleDelay is how long a row may take to become searchable in
// VictoriaLogs. An ingested row is searchable only once its rows-buffer shard
// flushes: shards are filled round-robin and each flushes a second after its
// first row (lib/logstorage/datadb.go rowsBuffer), and a cluster's vlinsert
// buffers rows on its own, so rows of one stream pushed in separate requests
// can become searchable out of order. VictoriaLogs' live tail skips every row
// of a stream at or below the newest one of that stream it already sent
// (app/vlselect/logsql tailProcessor.getTailRows), so a tail that reads up to
// now loses such rows for good, while Loki's tail delivers every line pushed
// after it started. The native tail therefore reads up to this long before
// now (VictoriaLogs' own default tail offset), and the polled tail reads this
// span again on every poll and drops the rows it already sent. It is the
// default of -tail.settle-delay (Proxy.tailSettle).
const tailSettleDelay = 5 * time.Second

// errTailStreamEnded is a native tail stream that ended without an error.
var errTailStreamEnded = errors.New("native tail stream ended")

// tailClientWriteError is a failed write to the tail client, as opposed to a
// failed VictoriaLogs call: nothing more can be sent to a client that is gone.
type tailClientWriteError struct{ err error }

func (e *tailClientWriteError) Error() string { return e.err.Error() }
func (e *tailClientWriteError) Unwrap() error { return e.err }

// closeTailWithError ends an upgraded tail whose VictoriaLogs call failed. Loki
// closes the connection with 1011 and its error text; the proxy's text is the
// error layer's (Loki's fixed text for the status, never the backend's or a Go
// error), cut to the 123 bytes a close frame holds. The full error goes to the
// log only.
func (p *Proxy) closeTailWithError(ctx context.Context, conn tailConn, err error) {
	_, _, reason := lokiClientError(ctx, err)
	if !p.logTailPlanDefect(ctx, err) {
		p.log.Debug("tail ended with a backend error", "error", err)
	}
	if len(reason) > maxTailCloseReasonBytes {
		reason = reason[:maxTailCloseReasonBytes]
	}
	_ = p.writeTailControl(conn, websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, reason))
}

// writeTailRows writes the tail frames of a batch of VictoriaLogs rows. Only
// a failed write is returned: a batch the pipeline cannot convert is logged
// and skipped, as a failed row was before.
func (p *Proxy) writeTailRows(ctx context.Context, conn tailConn, tp *tailPipeline, rows []byte) error {
	frames, err := p.tailFrames(ctx, tp, rows)
	if err != nil {
		p.log.Debug("tail rows skipped", "error", err)
		return nil
	}
	for _, frame := range frames {
		if err := p.writeTailMessage(conn, websocket.TextMessage, frame); err != nil {
			return &tailClientWriteError{err}
		}
	}
	return nil
}

// lokiTailDefaultLimit is how many entries from before the connection a tail
// sends when the request has no limit: Loki's default (HTTP API reference,
// /loki/api/v1/tail).
const lokiTailDefaultLimit = 100

// tailHistoryTimeout bounds the history read before the upgrade, so a slow
// backend does not hold up the handshake. Loki calls the lookback best effort,
// and a read that runs out of time sends no history.
const tailHistoryTimeout = 2 * time.Second

// tailHistory is what a tail sends before its live entries, as Loki does: the
// newest limit entries from start to the connection, oldest first.
type tailHistory struct {
	rows  []byte    // VictoriaLogs rows, newest first; nil when there are none or the read was skipped
	start time.Time // the window's start: the request's start, or an hour before end, never after end
	end   time.Time // when the window was read
}

// tailHistoryStart is a tail's start: the request's start (Loki's time
// forms), or an hour before now as in Loki, and never after now.
func tailHistoryStart(raw string, now time.Time) time.Time {
	start := now.Add(-lokiDefaultSince)
	if raw != "" {
		if t, err := lokiTimeParam(raw); err == nil {
			start = t
		}
	}
	if start.After(now) {
		start = now
	}
	return start
}

// tailHistoryLimit is a tail's limit: the request's (validated positive
// before the handler runs), or Loki's 100, held to max_entries_limit_per_query
// (0 is unlimited) so the rows read stay bounded.
func tailHistoryLimit(raw string, maxEntries int) int {
	limit := lokiTailDefaultLimit
	if n, err := strconv.Atoi(raw); err == nil && n > 0 {
		limit = n
	}
	if maxEntries > 0 && limit > maxEntries {
		limit = maxEntries
	}
	return limit
}

// fetchTailHistory reads the history of a tail before the upgrade, in the
// single VictoriaLogs call a tail makes before it streams: the newest limit
// rows from start to now. The read is best effort, as Loki's lookback is: one
// that runs out of its time, or whose body cannot be read, is skipped and the
// tail starts with its live entries. A backend that answers with an error
// status, or that exceeded -backend-timeout inside the read's cap, fails the
// request before the upgrade with Loki's status.
func (p *Proxy) fetchTailHistory(parent context.Context, logsqlQuery, startRaw, limitRaw string, tp *tailPipeline) (tailHistory, error) {
	ctx, cancel := context.WithTimeout(parent, tailHistoryTimeout)
	defer cancel()

	now := time.Now()
	h := tailHistory{start: tailHistoryStart(startRaw, now), end: now}
	limits := p.requestQueryLimits(parent)
	limit := tailHistoryLimit(limitRaw, limits.MaxEntriesLimitPerQuery)
	readStart := h.start
	// Loki upgrades the tail and sends no history when the window from start
	// is longer than max_query_length (recorded with start=1); the tail then
	// carries live entries only. The call before the upgrade stays, with the
	// shape of the access probe it replaced: the last five seconds, one row,
	// discarded, so a failing backend still fails the request.
	skipHistory := queryLengthTypedError(limits.MaxQueryLength, h.start, h.end) != nil
	if skipHistory {
		readStart, limit = h.end.Add(-5*time.Second), 1
	}

	params := url.Values{}
	params.Set("query", logsqlQuery+" | sort by (_time desc)")
	params.Set("start", formatVLTimestamp(readStart.UTC().Format(time.RFC3339Nano)))
	params.Set("end", formatVLTimestamp(h.end.UTC().Format(time.RFC3339Nano)))
	params.Set("limit", strconv.Itoa(limit))

	resp, err := p.vlGet(ctx, "/select/logsql/query", params)
	if err != nil {
		// The read is best effort: its own cap, or a client that went away,
		// skips it. A backend that exceeded -backend-timeout inside that cap
		// is a timed out call, answered before the upgrade as Loki's 504.
		if parent.Err() == nil && ctx.Err() == nil && statusFromUpstreamErr(err) == http.StatusGatewayTimeout {
			return h, err
		}
		p.log.Debug("tail history skipped", "error", err)
		return h, nil
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		return h, p.redactedBackendStatusError("", resp.StatusCode, body)
	}
	var rows []byte
	if tp.exposure.filtersLineFields() && !skipHistory {
		// The limit counts the entries a Loki label filter keeps, as in a
		// query_range response: further pages fill it.
		rows, err = p.refillLineFieldRows(ctx, resp.Body, params, limit, false, tp.exposure, !tp.levelAsMetadata,
			func(ctx context.Context, next url.Values) (*http.Response, error) {
				return p.vlGet(ctx, "/select/logsql/query", next)
			})
	} else {
		rows, err = p.readBufferedBackendBody(resp.Body)
	}
	if err != nil {
		p.log.Debug("tail history skipped", "error", err)
		return h, nil
	}
	if !skipHistory {
		h.rows = rows
	}
	return h, nil
}

// sendTailHistory writes the history frames, oldest entry first, before any
// live entry. A tail that may stream natively notes what it sent, so the
// native stream's first read, which covers the seconds before it opens, does
// not send those entries again (tailResume).
func (p *Proxy) sendTailHistory(ctx context.Context, conn tailConn, tp *tailPipeline, h tailHistory) error {
	if len(h.rows) == 0 {
		return nil
	}
	if p.tailMode != TailModeSynthetic {
		tp.resume.track = true
	}
	// Loki reads the history through its query path, whose entries carry
	// detected_level in the stream labels (categorize-labels: in the entry
	// metadata), where its live tailer sends the pushed stream as is.
	noop := tp.noop
	tp.noop = false
	defer func() { tp.noop = noop }()
	if err := p.writeTailRows(ctx, conn, tp, h.rows); err != nil {
		return err
	}
	tp.resume.afterHistory()
	return nil
}

// maxTailCloseReasonBytes is the longest reason a websocket close frame holds
// (RFC 6455 5.5: a 125-byte control payload minus the 2-byte code).
const maxTailCloseReasonBytes = 123

func (p *Proxy) openNativeTailStream(parent context.Context, logsqlQuery string) (*http.Response, bool, error) {
	return p.openNativeTailStreamFrom(parent, logsqlQuery, 0)
}

// openNativeTailStreamFrom opens the native tail. A resumed stream passes the
// time since the row it reads from as startOffset: VictoriaLogs' first read
// then covers that span (its start_offset option, 5s when absent) instead of
// the last five seconds, so the rows of a gap come back.
func (p *Proxy) openNativeTailStreamFrom(parent context.Context, logsqlQuery string, startOffset time.Duration) (*http.Response, bool, error) {
	// Use the full request context (parent) so that the response body reader — which
	// the caller uses to forward the VL streaming tail — is not cancelled by a short
	// probe deadline.  VictoriaLogs answers the headers before its first read
	// (v1.49 and v1.53 flush them first) and then streams NDJSON lines as they
	// become searchable. A cancelled probe context would kill the body reader,
	// causing the forwarded WebSocket to close with "unexpected EOF".
	// For backends that do not support the tail endpoint, VL returns a 4xx response
	// quickly, so no timeout guard is needed here.
	//
	// offset is the settle delay (-tail.settle-delay): VictoriaLogs reads only rows
	// older than that, so rows of a stream that become searchable out of order
	// are all in the read that sends them, and its per-stream skip of rows at or
	// below the newest sent one drops none of them.
	params := url.Values{"query": {logsqlQuery}, "offset": {strconv.FormatInt(p.tailSettle.Milliseconds(), 10) + "ms"}}
	if startOffset > 0 {
		params.Set("start_offset", strconv.FormatInt(startOffset.Milliseconds(), 10)+"ms")
	}
	params = guardOutboundQuery(p.scopedTenantParams(parent, params))
	tailURL := backendRequestURL(p.backend, "/select/logsql/tail")
	tailURL.RawQuery = params.Encode()
	vlURL := tailURL.String()
	querycorpus.RecordBackend("/select/logsql/tail", params.Get("query"))
	if resp, err, ok := p.predictVLRefusal(parent, http.MethodGet, "/select/logsql/tail", params); ok {
		// VictoriaLogs refuses it on this release, or its handler would panic on it: not sent (vl_refusal.go).
		if err != nil {
			return nil, false, err
		}
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		_ = resp.Body.Close()
		return nil, false, p.redactedBackendStatusError("backend tail unavailable", resp.StatusCode, body)
	}
	req, err := http.NewRequestWithContext(parent, "GET", vlURL, nil)
	if err != nil {
		return nil, false, fmt.Errorf("failed to create native tail request: %w", err)
	}
	p.applyBackendHeaders(req)
	p.forwardTenantHeaders(req)

	resp, err := p.tailClient.Do(req)
	if err != nil {
		return nil, false, err
	}
	if err := decodeCompressedHTTPResponse(resp, 0); err != nil { // a tail stream has no end, so no decoded length cap
		_ = resp.Body.Close()
		return nil, false, fmt.Errorf("backend tail decode error: %w", err)
	}
	if resp.StatusCode == http.StatusOK {
		return resp, true, nil
	}

	body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
	_ = resp.Body.Close()
	return nil, false, p.redactedBackendStatusError("backend tail unavailable", resp.StatusCode, body)
}

func (p *Proxy) streamSyntheticTail(ctx context.Context, conn tailConn, logsqlQuery string, tp *tailPipeline, history tailHistory) {
	lastSeen := newSyntheticTailSeen(maxSyntheticTailSeenEntries)
	windowStart := syntheticTailWindowStart(history, lastSeen)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	retry := p.newTailRetry()
	for {
		wait := time.Duration(-1) // the poll interval, unless a failed poll is retried sooner
		if err := p.writeSyntheticTailBatch(ctx, conn, logsqlQuery, tp, &windowStart, lastSeen); err != nil {
			var writeErr *tailClientWriteError
			if ctx.Err() != nil || errors.As(err, &writeErr) {
				return
			}
			// A poll that fails is repeated from the same cursor: the rows of
			// the outage are in the next answer, and the rows seen are dropped.
			var verdict tailRetryVerdict
			if wait, verdict = retry.next(err, time.Now()); verdict != tailRetryAgain {
				// A failed VictoriaLogs call ends the tail like a native stream
				// that fails, once the retry window is spent.
				p.tailRetryEnded(ctx, conn, retry, err, verdict)
				return
			}
			p.observeInternalOperation(ctx, tailRetryOpName, tailRetryOutRetried, wait)
			p.log.Debug("tail backend poll failed, retrying", "wait", wait, "error", p.sanitizeUpstreamError(err))
		} else {
			p.tailRetryRecovered(ctx, retry)
			retry.reset()
		}

		if wait < 0 {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			continue
		}
		if !p.tailSleep(ctx, conn, nil, wait) {
			return
		}
	}
}

// syntheticTailPollLimit is the most rows one poll of the polled tail reads.
const syntheticTailPollLimit = 200

func (p *Proxy) writeSyntheticTailBatch(ctx context.Context, conn tailConn, logsqlQuery string, tp *tailPipeline, windowStart *time.Time, lastSeen *syntheticTailSeen) error {
	end := time.Now()
	params := url.Values{}
	params.Set("query", logsqlQuery+" | sort by (_time)")
	params.Set("start", formatVLTimestamp(windowStart.UTC().Format(time.RFC3339Nano)))
	params.Set("end", formatVLTimestamp(end.UTC().Format(time.RFC3339Nano)))
	params.Set("limit", strconv.Itoa(syntheticTailPollLimit))

	resp, err := p.vlGet(ctx, "/select/logsql/query", params)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		return p.redactedBackendStatusError("synthetic tail query failed", resp.StatusCode, body)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, tailScanBufferInitial), 1024*1024)
	// A body that stops in the middle of a row is a broken call, not a row
	// that is not JSON: the row is not read, and the poll is repeated.
	var partial bool
	scanner.Split(splitTailRows(&partial))
	newest := *windowStart
	var batch []byte
	var pending []string
	rows := 0
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		rows++

		seenKey, entryTime, err := syntheticTailRowKey(line)
		if err != nil {
			// A row that is not JSON is a broken body, not a row to skip.
			return fmt.Errorf("synthetic tail row is not JSON: %w: %w", errTailMalformedRow, err)
		}
		// Rows sent before count too: a poll that reads only those still
		// moves the cursor.
		if entryTime.After(newest) {
			newest = entryTime
		}
		if lastSeen.Contains(seenKey) {
			continue
		}
		// Noted as seen once the batch is delivered: a body that breaks
		// part-way is read again from the same cursor.
		pending = append(pending, seenKey)

		batch = append(append(batch, line...), '\n')
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if partial {
		return io.ErrUnexpectedEOF
	}
	if err := p.writeTailRows(ctx, conn, tp, batch); err != nil {
		return err
	}
	for _, key := range pending {
		lastSeen.Add(key)
	}

	// The next poll reads the last -tail.settle-delay again: a row of that span
	// that was not searchable yet arrives then, and the rows already sent
	// are dropped. A full poll moves on from its newest row instead, so a
	// busy tail never reads the same page again.
	next := newest.Add(time.Nanosecond)
	if settled := end.Add(-p.tailSettle); rows < syntheticTailPollLimit && settled.Before(next) {
		next = settled
	}
	if next.After(*windowStart) {
		*windowStart = next
	}
	return nil
}

// syntheticTailRowKey is the key a polled row is noted as seen by (its time,
// stream and line) and its time, zero when it has none that parses.
func syntheticTailRowKey(line []byte) (string, time.Time, error) {
	var vlLine map[string]interface{}
	if err := json.Unmarshal(line, &vlLine); err != nil {
		return "", time.Time{}, err
	}
	timeStr, _ := stringifyEntryValue(vlLine["_time"])
	msgStr, _ := stringifyEntryValue(vlLine["_msg"])
	streamStr, _ := stringifyEntryValue(vlLine["_stream"])
	entryTime, _ := parseEntryTime(timeStr)
	return timeStr + "\x00" + streamStr + "\x00" + msgStr, entryTime, nil
}

// syntheticTailWindowStart is where the polling tail reads from after its
// history: just past the newest entry the history sent, whose rows are noted
// as seen, or, with no history, the lookback before the history was read (the
// window from start held no row then).
func syntheticTailWindowStart(history tailHistory, lastSeen *syntheticTailSeen) time.Time {
	var newest time.Time
	for _, line := range bytes.Split(history.rows, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		key, ts, err := syntheticTailRowKey(bytes.TrimSpace(line))
		if err != nil {
			continue
		}
		lastSeen.Add(key)
		if ts.After(newest) {
			newest = ts
		}
	}
	if !newest.IsZero() {
		return newest.Add(time.Nanosecond)
	}
	if history.end.IsZero() {
		return time.Now().Add(-tailResumeLookback)
	}
	if from := history.end.Add(-tailResumeLookback); from.After(history.start) {
		return from
	}
	return history.start
}

func newSyntheticTailSeen(limit int) *syntheticTailSeen {
	if limit <= 0 {
		limit = maxSyntheticTailSeenEntries
	}
	return &syntheticTailSeen{
		seen:  make(map[string]struct{}, min(128, limit)),
		order: make([]string, 0, min(128, limit)),
		limit: limit,
	}
}

func (s *syntheticTailSeen) Contains(key string) bool {
	_, ok := s.seen[key]
	return ok
}

func (s *syntheticTailSeen) Add(key string) {
	if _, ok := s.seen[key]; ok {
		return
	}
	s.seen[key] = struct{}{}
	s.order = append(s.order, key)
	if len(s.order) <= s.limit {
		return
	}
	drop := len(s.order) - s.limit
	for _, oldKey := range s.order[:drop] {
		delete(s.seen, oldKey)
	}
	n := copy(s.order, s.order[drop:])
	s.order = s.order[:n]
}
func (p *Proxy) writeTailMessage(conn tailConn, messageType int, data []byte) error {
	if err := conn.SetWriteDeadline(time.Now().Add(tailWriteTimeout)); err != nil {
		return err
	}
	return conn.WriteMessage(messageType, data)
}

func (p *Proxy) writeTailControl(conn tailConn, messageType int, data []byte) error {
	deadline := time.Now().Add(tailWriteTimeout)
	if err := conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	return conn.WriteControl(messageType, data, deadline)
}

// tailPipeline turns the VictoriaLogs rows of a tail query into Loki tail
// frames. Loki's tail runs the query's whole pipeline on every entry (the
// ingester's tailer), so the rows go
// through the conversion of a query_range response (vlReaderToLokiStreams)
// and the proxy-side stages a query_range response gets: the label filters
// VictoriaLogs cannot evaluate like Loki (lineFieldExposure), line_format,
// decolorize and derived fields.
type tailPipeline struct {
	query string
	// noop: the query has no pipeline stage. Loki's tailer then sends the
	// pushed stream as is: the frame carries the index labels only, without
	// structured metadata or detected_level, unless categorize-labels asks
	// for the metadata.
	noop bool
	// levelAsMetadata: categorize-labels with structured metadata emitted;
	// each entry carries its structured metadata and parsed labels.
	levelAsMetadata bool
	exposure        *lineFieldExposure
	lineFields      map[string]bool
	lineFormat      string
	// bothNames: the both mode adds every name of a mapped field to the stream
	// labels of a query with stages, as bothNamesLayer does for query_range,
	// except the names bothSkip holds.
	bothNames bool
	bothSkip  map[string]bool
	// resume tracks what a native tail delivered, so a stream reopened after
	// a backend outage carries on from there (tail_retry.go).
	resume tailResume
	// delivered counts the entries written to the client as frames.
	delivered int
}

// maxTailFrameEntries is the most entries one tail frame holds, as Loki's
// querier batches them.
const maxTailFrameEntries = 100

// tailScanBufferInitial is the first size of a tail row scanner's buffer;
// it grows on demand up to the scanner's maximum line. A batch is converted
// a few times a second, so a large fixed buffer would be allocated each time.
const tailScanBufferInitial = 4 * 1024

func (p *Proxy) newTailPipeline(query string, levelAsMetadata bool) *tailPipeline {
	lq, err := parseLogQLQuery(noFlowCtx, query)
	tp := &tailPipeline{
		query:           query,
		noop:            err == nil && len(lq.Pipeline) == 0,
		levelAsMetadata: levelAsMetadata,
		exposure:        p.lineFieldExposure(query),
		lineFields:      logQueryLineFields(query),
		lineFormat:      extractLineFormatTemplate(query),
	}
	if !tp.noop && p.labelTranslator.answersBothNames() {
		var all bool
		tp.bothSkip, all = p.bothNamesSkip(noFlowCtx, query)
		tp.bothNames = !all
	}
	return tp
}

// tailEntry is one entry of a tail frame.
type tailEntry struct {
	ts     int64
	stream map[string]string
	value  interface{}
}

// tailFrames returns the tail frames for a batch of VictoriaLogs NDJSON rows:
// one stream per entry, in timestamp order, at most maxTailFrameEntries
// entries a frame, as Loki's querier sends them.
func (p *Proxy) tailFrames(ctx context.Context, tp *tailPipeline, rows []byte) ([][]byte, error) {
	entries, err := p.tailEntries(ctx, tp, rows)
	if err != nil || len(entries) == 0 {
		return nil, err
	}
	sort.SliceStable(entries, func(i, j int) bool { return entries[i].ts < entries[j].ts })
	if tp.resume.track {
		if entries = tp.resume.filter(entries, time.Now()); len(entries) == 0 {
			return nil, nil
		}
		tp.resume.observe(entries)
	}
	tp.delivered += len(entries)
	frames := make([][]byte, 0, (len(entries)+maxTailFrameEntries-1)/maxTailFrameEntries)
	for start := 0; start < len(entries); start += maxTailFrameEntries {
		chunk := entries[start:min(start+maxTailFrameEntries, len(entries))]
		streams := make([]map[string]interface{}, len(chunk))
		for i, e := range chunk {
			streams[i] = map[string]interface{}{"stream": e.stream, "values": []interface{}{e.value}}
		}
		frame := map[string]interface{}{"streams": streams}
		if tp.levelAsMetadata {
			frame["encodingFlags"] = []string{"categorize-labels"}
		}
		raw, err := json.Marshal(frame)
		if err != nil {
			return nil, err
		}
		frames = append(frames, raw)
	}
	return frames, nil
}

func (p *Proxy) tailEntries(ctx context.Context, tp *tailPipeline, rows []byte) ([]tailEntry, error) {
	// Rows a Loki label filter would not keep are dropped, as in a
	// query_range response (refillLineFieldRows).
	if tp.exposure.filtersLineFields() {
		var kept bytes.Buffer
		if _, _, _, _, _, err := p.filterLineFieldPage(bytes.NewReader(rows), nil, time.Time{}, math.MaxInt, tp.exposure, !tp.levelAsMetadata, &kept); err != nil {
			return nil, err
		}
		rows = kept.Bytes()
	}
	if tp.noop && !tp.levelAsMetadata {
		return p.indexLabelTailEntries(rows, tp.lineFields), nil
	}
	streams, _, err := p.vlReaderToLokiStreams(bytes.NewReader(rows), tp.query, "", tp.levelAsMetadata, tp.levelAsMetadata, false)
	if err != nil {
		return nil, err
	}
	if len(p.derivedFields) > 0 {
		p.applyDerivedFields(streams)
	}
	if strings.Contains(tp.query, "decolorize") {
		decolorizeStreams(streams)
	}
	if streams, err = applyQueryLineFormat(ctx, streams, tp.query); err != nil {
		return nil, err
	}
	var entries []tailEntry
	for _, s := range streams {
		labels, _ := s["stream"].(map[string]string)
		values, _ := s["values"].([]interface{})
		if tp.bothNames {
			// The stream labels may be a shared descriptor map: names are
			// added to a copy.
			named := cloneStringMap(labels)
			if p.labelTranslator.addBothNames(named, tp.bothSkip) {
				labels = named
			}
		}
		_, storedLevel := labels[detectedLevelLabel]
		if tp.levelAsMetadata && storedLevel {
			// Loki's tail encoder differs from its query encoder here
			// (verified against Loki 3.7.1 on a stream pushed with a
			// detected_level label): the derived value keeps the
			// detected_level name in the entry metadata and the stream
			// label is not repeated in the frame, where a query response
			// keeps the label and renames the derived value to
			// detected_level_extracted.
			labels = cloneStringMap(labels)
			delete(labels, detectedLevelLabel)
		}
		for _, v := range values {
			tuple, _ := v.([]interface{})
			if len(tuple) < 2 {
				continue
			}
			ts, _ := tuple[0].(string)
			nanos, err := strconv.ParseInt(ts, 10, 64)
			if err != nil {
				continue
			}
			if tp.levelAsMetadata && storedLevel {
				v = tailLevelMetadata(tuple)
			}
			entries = append(entries, tailEntry{ts: nanos, stream: labels, value: v})
		}
	}
	return entries, nil
}

// tailLevelMetadata returns an entry tuple whose structured metadata names
// the derived level detected_level instead of detected_level_extracted. The
// tuple's maps may be shared read-only values, so they are copied.
func tailLevelMetadata(tuple []interface{}) []interface{} {
	if len(tuple) < 3 {
		return tuple
	}
	meta, _ := tuple[2].(map[string]interface{})
	sm, _ := meta["structuredMetadata"].(map[string]string)
	level, ok := sm[detectedLevelExtractedLabel]
	if !ok {
		return tuple
	}
	sm = cloneStringMap(sm)
	delete(sm, detectedLevelExtractedLabel)
	sm[detectedLevelLabel] = level
	newMeta := make(map[string]interface{}, len(meta))
	for k, v := range meta {
		newMeta[k] = v
	}
	newMeta["structuredMetadata"] = sm
	return []interface{}{tuple[0], tuple[1], newMeta}
}

// indexLabelTailEntries returns the entries of a tail query without
// pipeline stages in Loki's default encoding: the index labels and the line.
func (p *Proxy) indexLabelTailEntries(rows []byte, lineFields map[string]bool) []tailEntry {
	var entries []tailEntry
	scanner := bufio.NewScanner(bytes.NewReader(rows))
	scanner.Buffer(make([]byte, 0, tailScanBufferInitial), 8*1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var vlLine map[string]interface{}
		if err := json.Unmarshal(line, &vlLine); err != nil {
			continue
		}
		if e, ok := p.indexLabelTailEntry(vlLine, lineFields); ok {
			entries = append(entries, e)
		}
	}
	return entries
}

func (p *Proxy) indexLabelTailEntry(vlLine map[string]interface{}, lineFields map[string]bool) (tailEntry, bool) {
	timeStr, _ := stringifyEntryValue(vlLine["_time"])
	t, err := time.Parse(time.RFC3339Nano, timeStr)
	if err != nil {
		return tailEntry{}, false
	}
	msg, _ := stringifyEntryValue(vlLine["_msg"])
	stream := parseStreamLabels(asString(vlLine["_stream"]))
	msg = storedLogLineFromEntry(msg, vlLine, stream, lineFields, p.defaultMsgValue())
	labels := make(map[string]string, len(stream))
	for k, v := range stream {
		labels[k] = v
	}
	if !p.labelTranslator.IsPassthrough() {
		labels = p.labelTranslator.TranslateLabelsMap(labels)
	}
	if p.labelTranslator.answersBothNames() {
		p.labelTranslator.addBothNames(labels, nil)
	}
	ensureSyntheticServiceName(labels)
	ts := strconv.FormatInt(t.UnixNano(), 10)
	return tailEntry{ts: t.UnixNano(), stream: labels, value: []string{ts, msg}}, true
}

// tailHandshakeComplete reports a request the websocket upgrade can accept: a
// GET whose Connection names upgrade and whose Upgrade names websocket, with
// protocol version 13 and a key (RFC 6455, 4.2.1). Any other request fails the
// upgrade, so it is answered before the tail reads anything.
func tailHandshakeComplete(r *http.Request) bool {
	return r.Method == http.MethodGet && websocket.IsWebSocketUpgrade(r) &&
		r.Header.Get("Sec-Websocket-Version") == "13" && r.Header.Get("Sec-Websocket-Key") != ""
}
