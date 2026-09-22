package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
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

	logsqlQuery, err := p.translateQueryWithContext(r.Context(), logqlQuery)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		p.metrics.RecordRequest("tail", http.StatusBadRequest, time.Since(start))
		return
	}
	lineFields := logQueryLineFields(logqlQuery)

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

	// Loki encodes tail frames like query responses, including the
	// categorize-labels flag.
	levelAsMetadata := p.shouldEmitStructuredMetadata(r)

	if p.tailMode == TailModeSynthetic {
		p.log.Debug("tail connected", "logql", redactQuery(logqlQuery, p.debugLogRawQueries), "logsql", redactQuery(logsqlQuery, p.debugLogRawQueries), "native", false, "fallback", "forced synthetic tail mode")
		p.streamSyntheticTail(wsCtx, conn, logsqlQuery, lineFields, r.FormValue("start"), levelAsMetadata)
		return
	}

	resp, nativeTail, fallbackReason := p.openNativeTailStream(wsCtx, logsqlQuery)
	p.log.Debug("tail connected", "logql", redactQuery(logqlQuery, p.debugLogRawQueries), "logsql", redactQuery(logsqlQuery, p.debugLogRawQueries), "native", nativeTail, "fallback", fallbackReason)
	if !nativeTail {
		if p.tailMode == TailModeNative {
			_ = p.writeTailControl(conn, websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, fallbackReason))
			return
		}
		p.streamSyntheticTail(wsCtx, conn, logsqlQuery, lineFields, r.FormValue("start"), levelAsMetadata)
		return
	}
	defer resp.Body.Close()

	// Read VL NDJSON stream and forward as Loki WebSocket frames
	lineCh := make(chan []byte)
	errCh := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 1024*1024), 1024*1024) // 1MB max line
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
		case <-pingTicker.C:
			if err := p.writeTailMessage(conn, websocket.PingMessage, nil); err != nil {
				p.log.Debug("websocket ping failed, client disconnected", "error", err)
				return
			}
		case err := <-errCh:
			if err != nil && wsCtx.Err() == nil {
				p.log.Debug("tail stream ended with error", "error", err)
			}
			return
		case line := <-lineCh:
			if len(line) == 0 {
				continue
			}

			// Parse VL NDJSON line
			var vlLine map[string]interface{}
			if err := json.Unmarshal(line, &vlLine); err != nil {
				continue
			}

			// Convert to Loki tail frame
			frame := p.vlLineToTailFrame(vlLine, lineFields, levelAsMetadata)
			frameJSON, err := json.Marshal(frame)
			if err != nil {
				continue
			}

			if err := p.writeTailMessage(conn, websocket.TextMessage, frameJSON); err != nil {
				p.log.Debug("websocket write failed, client disconnected", "error", err)
				return
			}
		}
	}
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

func (p *Proxy) streamSyntheticTail(ctx context.Context, conn tailConn, logsqlQuery string, lineFields map[string]bool, startHint string, levelAsMetadata bool) {
	lastSeen := newSyntheticTailSeen(maxSyntheticTailSeenEntries)
	windowStart := time.Now().Add(-5 * time.Second)
	if parsed, ok := parseEntryTime(startHint); ok {
		windowStart = parsed
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		if err := p.writeSyntheticTailBatch(ctx, conn, logsqlQuery, lineFields, &windowStart, lastSeen, levelAsMetadata); err != nil {
			p.log.Debug("synthetic tail batch failed", "error", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (p *Proxy) writeSyntheticTailBatch(ctx context.Context, conn tailConn, logsqlQuery string, lineFields map[string]bool, windowStart *time.Time, lastSeen *syntheticTailSeen, levelAsMetadata bool) error {
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
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	newest := *windowStart
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

		frameJSON, err := json.Marshal(p.vlLineToTailFrame(vlLine, lineFields, levelAsMetadata))
		if err != nil {
			continue
		}
		if err := p.writeTailMessage(conn, websocket.TextMessage, frameJSON); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
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

// vlLineToTailFrame converts a single VL NDJSON log line to a Loki tail
// WebSocket frame. lineFields are the fields the tail query's pipeline writes
// (logQueryLineFields). Loki's tail frames carry the index labels as the
// stream; with levelAsMetadata (categorize-labels) each entry also carries its
// structured metadata, including detected_level, and a stream label of the
// same name is left out of the stream. Without the flag Loki's tail sends no
// metadata, so the frame holds the stored stream fields only.
func (p *Proxy) vlLineToTailFrame(vlLine map[string]interface{}, lineFields map[string]bool, levelAsMetadata bool) map[string]interface{} {
	ts := ""
	msg := ""

	for k, v := range vlLine {
		sv := fmt.Sprintf("%v", v)
		switch k {
		case "_time":
			if t, err := time.Parse(time.RFC3339Nano, sv); err == nil {
				ts = fmt.Sprintf("%d", t.UnixNano())
			} else {
				ts = sv
			}
		case "_msg":
			msg = sv
		case "_stream":
			// Skip internal VL stream ID
		}
	}
	if ts == "" {
		ts = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	stream := parseStreamLabels(asString(vlLine["_stream"]))
	storedMsg := msg
	msg = storedLogLineFromEntry(msg, vlLine, stream, lineFields, p.defaultMsgValue())
	translate := func(labels map[string]string) map[string]string {
		if p.labelTranslator.IsPassthrough() {
			return labels
		}
		return p.labelTranslator.TranslateLabelsMap(labels)
	}
	streamLabels := make(map[string]string, len(stream))
	for k, v := range stream {
		streamLabels[k] = v
	}
	translatedStream := translate(streamLabels)
	ensureSyntheticServiceName(translatedStream)

	if !levelAsMetadata {
		return map[string]interface{}{
			"streams": []map[string]interface{}{
				{
					"stream": translatedStream,
					"values": [][]string{{ts, msg}},
				},
			},
		}
	}

	metadata := make(map[string]string, 4)
	for key, value := range vlLine {
		if isVLInternalField(key) || key == "_stream_id" {
			continue
		}
		if _, isStream := stream[key]; isStream {
			continue
		}
		if s, ok := value.(string); ok && strings.TrimSpace(s) != "" {
			metadata[key] = s
		}
	}
	metadata = translate(metadata)
	rows := logRowLevels{bodyScan: p.detectedLevelBodyScan, defaultMsg: p.defaultMsgValue()}
	// Loki's tail encoder differs from its query encoder here (verified
	// against Loki 3.7.1 on a stream pushed with a detected_level label): the
	// derived value keeps the detected_level name in the entry metadata and
	// the stream label is not repeated in the frame, where a query response
	// keeps the label and renames the derived value to detected_level_extracted.
	metadata[detectedLevelLabel] = rows.mapRow(vlLine, storedMsg, logRowStream{labels: stream, levels: levelFieldsFromLabels(stream)}).String()
	if _, ok := stream[detectedLevelLabel]; ok {
		translatedStream = cloneStringMap(translatedStream)
		delete(translatedStream, detectedLevelLabel)
	}
	return map[string]interface{}{
		"streams": []map[string]interface{}{
			{
				"stream": translatedStream,
				"values": []interface{}{
					[]interface{}{ts, msg, map[string]interface{}{"structuredMetadata": metadata}},
				},
			},
		},
		"encodingFlags": []string{"categorize-labels"},
	}
}
