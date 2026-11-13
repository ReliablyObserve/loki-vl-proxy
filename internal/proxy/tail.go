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

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// handleTail bridges Loki's WebSocket tail to VL's NDJSON streaming tail.
// Loki: ws:///loki/api/v1/tail?query={...}&start=...&limit=...
// VL:   GET /select/logsql/tail?query=...
func (p *Proxy) handleTail(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	logqlQuery := r.FormValue("query")
	if logqlQuery == "" {
		status := p.fail(w, r, lokierr.Parse(0, 0, "syntax error: unexpected $end"))
		p.metrics.RecordRequest("tail", status, time.Since(start))
		return
	}

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

	r = p.withRequestScope(r)

	tailCtx, tailCancel := context.WithCancel(r.Context())
	defer tailCancel()

	if err := p.preflightTailAccess(tailCtx, logsqlQuery, r.FormValue("start")); err != nil {
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

	if p.tailMode == TailModeSynthetic {
		p.log.Debug("tail connected", "logql", redactQuery(logqlQuery, p.debugLogRawQueries), "logsql", redactQuery(logsqlQuery, p.debugLogRawQueries), "native", false, "fallback", "forced synthetic tail mode")
		p.streamSyntheticTail(wsCtx, conn, logsqlQuery, tp, r.FormValue("start"))
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
		p.streamSyntheticTail(wsCtx, conn, logsqlQuery, tp, r.FormValue("start"))
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
	p.log.Debug("tail ended with a backend error", "error", err)
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

func (p *Proxy) preflightTailAccess(parent context.Context, logsqlQuery, startHint string) error {
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()

	windowStart := time.Now().Add(-5 * time.Second)
	if parsed, ok := parseEntryTime(startHint); ok {
		windowStart = parsed
	}

	params := url.Values{}
	params.Set("query", logsqlQuery+" | sort by (_time desc)")
	params.Set("start", formatVLTimestamp(windowStart.UTC().Format(time.RFC3339Nano)))
	params.Set("end", formatVLTimestamp(time.Now().UTC().Format(time.RFC3339Nano)))
	params.Set("limit", "1")

	resp, err := p.vlGet(ctx, "/select/logsql/query", params)
	if err != nil {
		// The probe is best effort: its own 2 s cap, or a client that went
		// away, skips it. A backend that exceeded -backend-timeout inside that
		// cap is a timed out call, answered before the upgrade as Loki's 504.
		if parent.Err() == nil && ctx.Err() == nil && statusFromUpstreamErr(err) == http.StatusGatewayTimeout {
			return err
		}
		p.log.Debug("tail preflight skipped", "error", err)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode < 400 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}

	body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
	return p.redactedBackendStatusError("", resp.StatusCode, body)
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
	// probe deadline.  VL v1.50+ accepts the request immediately (200 OK headers
	// arrive before any data) and then streams NDJSON lines as they are ingested.
	// A cancelled probe context would kill the body reader within 1500 ms, causing
	// the forwarded WebSocket to close with "unexpected EOF".
	// For backends that do not support the tail endpoint, VL returns a 4xx response
	// quickly, so no timeout guard is needed here.
	//
	// offset=0s overrides VL's default 5-second tail offset (tailOffsetNsecs = 5e9
	// in VL source).  Without this override, VL's streaming window end is always
	// now-5s, so data pushed at T+0 only becomes visible to the tail at T+5s —
	// colliding with the 5s ResponseHeaderTimeout on the tailClient.  With offset=0s
	// the window end is now, data is visible within one refresh_interval (~1s), and
	// VL sends headers well within the 5s budget.  VL expects a duration string
	// (e.g. "0s"), not a bare integer.

	params := url.Values{"query": {logsqlQuery}, "offset": {"0s"}}
	if startOffset > 0 {
		params.Set("start_offset", strconv.FormatInt(startOffset.Milliseconds(), 10)+"ms")
	}
	params = guardOutboundQuery(p.scopedTenantParams(parent, params))
	tailURL := backendRequestURL(p.backend, "/select/logsql/tail")
	tailURL.RawQuery = params.Encode()
	vlURL := tailURL.String()
	querycorpus.RecordBackend("/select/logsql/tail", params.Get("query"))
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

func (p *Proxy) streamSyntheticTail(ctx context.Context, conn tailConn, logsqlQuery string, tp *tailPipeline, startHint string) {
	lastSeen := newSyntheticTailSeen(maxSyntheticTailSeenEntries)
	windowStart := time.Now().Add(-5 * time.Second)
	if parsed, ok := parseEntryTime(startHint); ok {
		windowStart = parsed
	}

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

func (p *Proxy) writeSyntheticTailBatch(ctx context.Context, conn tailConn, logsqlQuery string, tp *tailPipeline, windowStart *time.Time, lastSeen *syntheticTailSeen) error {
	params := url.Values{}
	params.Set("query", logsqlQuery+" | sort by (_time)")
	params.Set("start", formatVLTimestamp(windowStart.UTC().Format(time.RFC3339Nano)))
	params.Set("end", formatVLTimestamp(time.Now().UTC().Format(time.RFC3339Nano)))
	params.Set("limit", "200")

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
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}

		var vlLine map[string]interface{}
		if err := json.Unmarshal(line, &vlLine); err != nil {
			// A row that is not JSON is a broken body, not a row to skip.
			return fmt.Errorf("synthetic tail row is not JSON: %w: %w", errTailMalformedRow, err)
		}
		timeStr, _ := stringifyEntryValue(vlLine["_time"])
		msgStr, _ := stringifyEntryValue(vlLine["_msg"])
		streamStr, _ := stringifyEntryValue(vlLine["_stream"])
		seenKey := timeStr + "\x00" + streamStr + "\x00" + msgStr
		if lastSeen.Contains(seenKey) {
			continue
		}
		// Noted as seen once the batch is delivered: a body that breaks
		// part-way is read again from the same cursor.
		pending = append(pending, seenKey)

		if entryTime, ok := parseEntryTime(timeStr); ok && entryTime.After(newest) {
			newest = entryTime
		}

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

	*windowStart = newest.Add(time.Nanosecond)
	return nil
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
// ingester's tailer, pkg/ingester/tailer.go processStream), so the rows go
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
	// resume tracks what a native tail delivered, so a stream reopened after
	// a backend outage carries on from there (tail_retry.go).
	resume tailResume
	// delivered counts the entries written to the client as frames.
	delivered int
}

// maxTailFrameEntries is the most entries one tail frame holds, as Loki's
// querier batches them (pkg/querier/tail/tail.go maxEntriesPerTailResponse).
const maxTailFrameEntries = 100

// tailScanBufferInitial is the first size of a tail row scanner's buffer;
// it grows on demand up to the scanner's maximum line. A batch is converted
// a few times a second, so a large fixed buffer would be allocated each time.
const tailScanBufferInitial = 4 * 1024

func (p *Proxy) newTailPipeline(query string, levelAsMetadata bool) *tailPipeline {
	lq, err := parseLogQLQuery(noFlowCtx, query)
	return &tailPipeline{
		query:           query,
		noop:            err == nil && len(lq.Pipeline) == 0,
		levelAsMetadata: levelAsMetadata,
		exposure:        p.lineFieldExposure(query),
		lineFields:      logQueryLineFields(query),
		lineFormat:      extractLineFormatTemplate(query),
	}
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
	ensureSyntheticServiceName(labels)
	ts := strconv.FormatInt(t.UnixNano(), 10)
	return tailEntry{ts: t.UnixNano(), stream: labels, value: []string{ts, msg}}, true
}
