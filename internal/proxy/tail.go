package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
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

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// handleTail bridges Loki's WebSocket tail to VL's NDJSON streaming tail.
// Loki: ws:///loki/api/v1/tail?query={...}&start=...&limit=...
// VL:   GET /select/logsql/tail?query=...
func (p *Proxy) handleTail(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	logqlQuery := r.FormValue("query")
	if logqlQuery == "" {
		p.writeError(w, http.StatusBadRequest, "query parameter required")
		p.metrics.RecordRequest("tail", http.StatusBadRequest, time.Since(start))
		return
	}

	// The VictoriaLogs query of a query_range request: the stages
	// VictoriaLogs cannot run like Loki run on the proxy (tailPipeline).
	logsqlQuery, err := p.translateLogResponseQuery(r.Context(), logqlQuery)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		p.metrics.RecordRequest("tail", http.StatusBadRequest, time.Since(start))
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
			p.writeError(w, http.StatusBadRequest, err.Error())
			p.metrics.RecordRequest("tail", http.StatusBadRequest, time.Since(start))
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

	if statusCode, msg, ok := p.preflightTailAccess(tailCtx, logsqlQuery, r.FormValue("start")); ok {
		p.writeError(w, statusCode, msg)
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

	resp, nativeTail, fallbackReason := p.openNativeTailStream(wsCtx, logsqlQuery)
	p.log.Debug("tail connected", "logql", redactQuery(logqlQuery, p.debugLogRawQueries), "logsql", redactQuery(logsqlQuery, p.debugLogRawQueries), "native", nativeTail, "fallback", fallbackReason)
	if !nativeTail {
		if p.tailMode == TailModeNative {
			_ = p.writeTailControl(conn, websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, fallbackReason))
			return
		}
		p.streamSyntheticTail(wsCtx, conn, logsqlQuery, tp, r.FormValue("start"))
		return
	}
	defer resp.Body.Close()

	p.forwardNativeTail(wsCtx, conn, tp, resp.Body, pingTicker.C)
}

// forwardNativeTail reads VictoriaLogs' NDJSON tail stream and forwards it
// as Loki tail frames until the stream ends, the client goes away or a
// write fails.
func (p *Proxy) forwardNativeTail(wsCtx context.Context, conn tailConn, tp *tailPipeline, body io.Reader, ping <-chan time.Time) {
	// Read VL NDJSON stream and forward as Loki WebSocket frames. The
	// channel holds one frame's worth of rows, so rows that arrive together
	// are converted and sent together while a slow client still holds the
	// reader back.
	lineCh := make(chan []byte, maxTailFrameEntries)
	errCh := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(body)
		scanner.Buffer(make([]byte, 0, tailScanBufferInitial), 1024*1024) // 1MB max line
		for scanner.Scan() {
			line := append([]byte(nil), scanner.Bytes()...)
			select {
			case lineCh <- line:
			case <-wsCtx.Done():
				return
			}
		}
		errCh <- scanner.Err()
	}()

	for {
		select {
		case <-wsCtx.Done():
			return
		case <-ping:
			if err := p.writeTailMessage(conn, websocket.PingMessage, nil); err != nil {
				p.log.Debug("websocket ping failed, client disconnected", "error", err)
				return
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
					return
				}
			}
			if err != nil && wsCtx.Err() == nil {
				p.log.Debug("tail stream ended with error", "error", err)
			}
			return
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
				return
			}
		}
	}
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
			return err
		}
	}
	return nil
}

func (p *Proxy) preflightTailAccess(parent context.Context, logsqlQuery, startHint string) (int, string, bool) {
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
		p.log.Debug("tail preflight skipped", "error", err)
		return 0, "", false
	}
	defer resp.Body.Close()

	if resp.StatusCode < 400 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return 0, "", false
	}

	body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
	msg := p.redactedBackendErrorMessage(resp.StatusCode, body)
	if msg == "" {
		msg = http.StatusText(resp.StatusCode)
	}
	return resp.StatusCode, msg, true
}

func (p *Proxy) openNativeTailStream(parent context.Context, logsqlQuery string) (*http.Response, bool, string) {
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

	params := p.scopedTenantParams(parent, url.Values{"query": {logsqlQuery}, "offset": {"0s"}})
	vlURL := p.backend.String() + "/select/logsql/tail?" + params.Encode()
	req, err := http.NewRequestWithContext(parent, "GET", vlURL, nil)
	if err != nil {
		return nil, false, "failed to create native tail request"
	}
	p.applyBackendHeaders(req)
	p.forwardTenantHeaders(req)

	resp, err := p.tailClient.Do(req)
	if err != nil {
		return nil, false, err.Error()
	}
	if err := decodeCompressedHTTPResponse(resp); err != nil {
		_ = resp.Body.Close()
		return nil, false, fmt.Sprintf("backend tail decode error: %v", err)
	}
	if resp.StatusCode == http.StatusOK {
		return resp, true, ""
	}

	body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
	_ = resp.Body.Close()
	msg := p.redactedBackendErrorMessage(resp.StatusCode, body)
	if msg == "" {
		msg = http.StatusText(resp.StatusCode)
	}
	return nil, false, fmt.Sprintf("backend tail unavailable: %s", msg)
}

func (p *Proxy) streamSyntheticTail(ctx context.Context, conn tailConn, logsqlQuery string, tp *tailPipeline, startHint string) {
	lastSeen := newSyntheticTailSeen(maxSyntheticTailSeenEntries)
	windowStart := time.Now().Add(-5 * time.Second)
	if parsed, ok := parseEntryTime(startHint); ok {
		windowStart = parsed
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		if err := p.writeSyntheticTailBatch(ctx, conn, logsqlQuery, tp, &windowStart, lastSeen); err != nil {
			p.log.Debug("synthetic tail batch failed", "error", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
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
		return fmt.Errorf("synthetic tail query failed: status=%d body=%s", resp.StatusCode, p.redactedBackendErrorMessage(resp.StatusCode, body))
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, tailScanBufferInitial), 1024*1024)
	newest := *windowStart
	var batch []byte
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}

		var vlLine map[string]interface{}
		if err := json.Unmarshal(line, &vlLine); err != nil {
			continue
		}
		timeStr, _ := stringifyEntryValue(vlLine["_time"])
		msgStr, _ := stringifyEntryValue(vlLine["_msg"])
		streamStr, _ := stringifyEntryValue(vlLine["_stream"])
		seenKey := timeStr + "\x00" + streamStr + "\x00" + msgStr
		if lastSeen.Contains(seenKey) {
			continue
		}
		lastSeen.Add(seenKey)

		if entryTime, ok := parseEntryTime(timeStr); ok && entryTime.After(newest) {
			newest = entryTime
		}

		batch = append(append(batch, line...), '\n')
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if err := p.writeTailRows(ctx, conn, tp, batch); err != nil {
		return err
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
}

// maxTailFrameEntries is the most entries one tail frame holds, as Loki's
// querier batches them (pkg/querier/tail/tail.go maxEntriesPerTailResponse).
const maxTailFrameEntries = 100

// tailScanBufferInitial is the first size of a tail row scanner's buffer;
// it grows on demand up to the scanner's maximum line. A batch is converted
// a few times a second, so a large fixed buffer would be allocated each time.
const tailScanBufferInitial = 4 * 1024

func (p *Proxy) newTailPipeline(query string, levelAsMetadata bool) *tailPipeline {
	lq, err := logqlpkg.ParseLogQuery(query)
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
