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

	if origin := strings.TrimSpace(r.Header.Get("Origin")); origin != "" && !p.isAllowedTailOrigin(origin) {
		p.writeError(w, http.StatusForbidden, "tail origin not allowed")
		p.metrics.RecordRequest("tail", http.StatusForbidden, time.Since(start))
		return
	}

	r = withOrgID(r)

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
	p.metrics.RecordRequest("tail", http.StatusOK, time.Since(start))

	// Start a read loop to detect client disconnect (WebSocket protocol requires it).
	// When client closes, this goroutine exits and wsCtx is canceled.
	wsCtx, wsCancel := context.WithCancel(tailCtx)
	defer wsCancel()
	go func() {
		defer tailCancel()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	pingTicker := time.NewTicker(time.Second)
	defer pingTicker.Stop()

	if p.tailMode == TailModeSynthetic {
		p.log.Debug("tail connected", "logql", logqlQuery, "logsql", logsqlQuery, "native", false, "fallback", "forced synthetic tail mode")
		p.streamSyntheticTail(wsCtx, conn, logsqlQuery, r.FormValue("start"))
		return
	}

	resp, nativeTail, fallbackReason := p.openNativeTailStream(wsCtx, logsqlQuery)
	p.log.Debug("tail connected", "logql", logqlQuery, "logsql", logsqlQuery, "native", nativeTail, "fallback", fallbackReason)
	if !nativeTail {
		if p.tailMode == TailModeNative {
			_ = p.writeTailControl(conn, websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, fallbackReason))
			return
		}
		p.streamSyntheticTail(wsCtx, conn, logsqlQuery, r.FormValue("start"))
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
			frame := p.vlLineToTailFrame(vlLine)
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
	msg := strings.TrimSpace(string(body))
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
	vlURL := fmt.Sprintf("%s/select/logsql/tail?query=%s&offset=0s",
		p.backend.String(), url.QueryEscape(logsqlQuery))
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
	msg := strings.TrimSpace(string(body))
	if msg == "" {
		msg = http.StatusText(resp.StatusCode)
	}
	return nil, false, fmt.Sprintf("backend tail unavailable: %s", msg)
}

func (p *Proxy) streamSyntheticTail(ctx context.Context, conn tailConn, logsqlQuery, startHint string) {
	lastSeen := newSyntheticTailSeen(maxSyntheticTailSeenEntries)
	windowStart := time.Now().Add(-5 * time.Second)
	if parsed, ok := parseEntryTime(startHint); ok {
		windowStart = parsed
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		if err := p.writeSyntheticTailBatch(ctx, conn, logsqlQuery, &windowStart, lastSeen); err != nil {
			p.log.Debug("synthetic tail batch failed", "error", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (p *Proxy) writeSyntheticTailBatch(ctx context.Context, conn tailConn, logsqlQuery string, windowStart *time.Time, lastSeen *syntheticTailSeen) error {
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
		return fmt.Errorf("synthetic tail query failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
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

		frameJSON, err := json.Marshal(p.vlLineToTailFrame(vlLine))
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

// vlLineToTailFrame converts a single VL NDJSON log line to a Loki tail WebSocket frame.
func (p *Proxy) vlLineToTailFrame(vlLine map[string]interface{}) map[string]interface{} {
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

	labels := buildEntryLabels(vlLine)
	translatedLabels := labels
	if !p.labelTranslator.IsPassthrough() {
		translatedLabels = p.labelTranslator.TranslateLabelsMap(labels)
	}
	ensureDetectedLevel(translatedLabels)
	ensureSyntheticServiceName(translatedLabels)

	return map[string]interface{}{
		"streams": []map[string]interface{}{
			{
				"stream": translatedLabels,
				"values": [][]string{{ts, msg}},
			},
		},
	}
}

