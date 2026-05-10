package proxy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

func (p *Proxy) coldRouteForRequest(r *http.Request) RouteDecision {
	if p.coldRouter == nil {
		return RouteHotOnly
	}
	startNs, endNs := ParseTimeRangeFromRequest(r)
	return p.coldRouter.Route(startNs, endNs)
}

func (p *Proxy) proxyLogQueryWithCold(w http.ResponseWriter, r *http.Request, logsqlQuery string) {
	decision := p.coldRouteForRequest(r)
	p.log.Debug("cold routing decision", "decision", decision, "query", logsqlQuery)

	switch decision {
	case RouteColdOnly:
		p.proxyLogQueryCold(w, r, logsqlQuery)
	case RouteBoth:
		p.proxyLogQueryBoth(w, r, logsqlQuery)
	default:
		p.proxyLogQuery(w, r, logsqlQuery)
	}
}

// buildLogQueryParams builds request parameters for VictoriaLogs hot storage.
// It appends a LogsQL sort clause to honour the Loki direction parameter.
func (p *Proxy) buildLogQueryParams(r *http.Request, logsqlQuery string) url.Values {
	direction := r.FormValue("direction")
	if direction == "forward" {
		logsqlQuery += " | sort by (_time)"
	} else {
		logsqlQuery += " | sort by (_time desc)"
	}

	params := url.Values{}
	params.Set("query", logsqlQuery)
	if s := r.FormValue("start"); s != "" {
		params.Set("start", formatVLTimestamp(s))
	}
	if e := r.FormValue("end"); e != "" {
		params.Set("end", formatVLTimestamp(e))
	}
	limit := r.FormValue("limit")
	if limit == "" {
		limit = "1000"
	}
	params.Set("limit", sanitizeLimit(limit))
	return params
}

// buildHotQueryParamsForRange builds VictoriaLogs params with explicit nanosecond timestamps.
// Used by proxyLogQueryBoth to send a time-bounded hot sub-range.
func (p *Proxy) buildHotQueryParamsForRange(r *http.Request, logsqlQuery string, startNs, endNs int64) url.Values {
	params := p.buildLogQueryParams(r, logsqlQuery)
	params.Set("start", strconv.FormatInt(startNs, 10))
	params.Set("end", strconv.FormatInt(endNs, 10))
	return params
}

// buildColdQueryParams builds request parameters for Victoria Lakehouse cold storage.
// It does NOT append a LogsQL sort clause: the Lakehouse filter parser only understands
// simple filter terms, boolean ops, parentheses, and *.  Appending "| sort by (_time desc)"
// would be tokenised as bare _msg substring filters, returning wrong rows.
func (p *Proxy) buildColdQueryParams(r *http.Request, logsqlQuery string) url.Values {
	params := url.Values{}
	params.Set("query", logsqlQuery)
	if s := r.FormValue("start"); s != "" {
		params.Set("start", formatVLTimestamp(s))
	}
	if e := r.FormValue("end"); e != "" {
		params.Set("end", formatVLTimestamp(e))
	}
	limit := r.FormValue("limit")
	if limit == "" {
		limit = "1000"
	}
	params.Set("limit", sanitizeLimit(limit))
	return params
}

// buildColdQueryParamsForRange builds cold-storage params with explicit nanosecond timestamps.
func (p *Proxy) buildColdQueryParamsForRange(r *http.Request, logsqlQuery string, startNs, endNs int64) url.Values {
	params := p.buildColdQueryParams(r, logsqlQuery)
	params.Set("start", strconv.FormatInt(startNs, 10))
	params.Set("end", strconv.FormatInt(endNs, 10))
	return params
}

// coldBackwardChunkDuration controls the time-slice size for backward cold queries.
// Sized to match Victoria Lakehouse's Hive partition granularity (hour=HH) so each
// chunk likely maps to a single partition scan.
const coldBackwardChunkDuration = time.Hour

// countNDJSONLines counts the number of non-empty lines in an NDJSON body.
func countNDJSONLines(body []byte) int {
	count := 0
	for _, line := range bytes.Split(body, []byte("\n")) {
		if len(bytes.TrimSpace(line)) > 0 {
			count++
		}
	}
	return count
}

// coldBackwardChunkedFetch fetches rows for a backward cold query by iterating
// from newest to oldest in coldBackwardChunkDuration slices. It stops as soon
// as the accumulated row count reaches limit, ensuring only the limit newest
// rows are returned regardless of how many rows the full range contains.
//
// Each chunk is fetched in ascending order (as required by the Lakehouse) and
// prepended to the accumulation buffer so the final buffer is in ascending order.
// After the loop the caller must reverse-and-trim the returned buffer.
//
// Returns the accumulated NDJSON rows in ascending time order.
func (p *Proxy) coldBackwardChunkedFetch(ctx context.Context, baseParams url.Values, startNs, endNs int64, limit int) ([]byte, error) {
	var accumulated []byte
	accCount := 0
	chunkEnd := endNs
	chunkDurNs := coldBackwardChunkDuration.Nanoseconds()

	for chunkEnd > startNs {
		chunkStart := chunkEnd - chunkDurNs
		if chunkStart < startNs {
			chunkStart = startNs
		}

		chunkParams := cloneURLValues(baseParams)
		chunkParams.Set("start", strconv.FormatInt(chunkStart, 10))
		chunkParams.Set("end", strconv.FormatInt(chunkEnd, 10))
		chunkParams.Set("limit", strconv.Itoa(limit))

		resp, err := p.coldRouter.ColdPost(ctx, "/select/logsql/query", chunkParams)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode >= 400 {
			body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
			resp.Body.Close()
			return nil, fmt.Errorf("cold backend %d: %s", resp.StatusCode, body)
		}
		chunkBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("failed to read cold chunk response: %w", readErr)
		}

		chunkCount := countNDJSONLines(chunkBody)
		// Prepend this chunk so the accumulation buffer stays in ascending order
		// (oldest chunk first, newest chunk last).
		accumulated = append(chunkBody, accumulated...)
		accCount += chunkCount

		if accCount >= limit {
			break // have enough rows — older chunks cannot contribute to newest N
		}
		chunkEnd = chunkStart
	}
	return accumulated, nil
}

func (p *Proxy) proxyLogQueryCold(w http.ResponseWriter, r *http.Request, logsqlQuery string) {
	originalLimit, err := strconv.Atoi(r.FormValue("limit"))
	if err != nil || originalLimit <= 0 {
		originalLimit = 1000 // match buildColdQueryParams default
	}

	if r.FormValue("direction") != "forward" {
		// For backward queries the Lakehouse only scans ascending. Fetch chunks
		// newest-to-oldest so we accumulate the correct newest rows regardless of
		// how many rows the full range contains.
		startNs, endNs := ParseTimeRangeFromRequest(r)
		baseParams := p.buildColdQueryParams(r, logsqlQuery)
		// Remove start/end/limit from baseParams — coldBackwardChunkedFetch sets them per chunk.
		baseParams.Del("start")
		baseParams.Del("end")
		baseParams.Del("limit")

		ascBody, fetchErr := p.coldBackwardChunkedFetch(r.Context(), baseParams, startNs, endNs, originalLimit)
		if fetchErr != nil {
			p.writeError(w, http.StatusBadGateway, "cold backend error: "+fetchErr.Error())
			return
		}
		trimmed := trimNDJSONBodyToLimit(reverseNDJSONBody(ascBody), r.FormValue("limit"))
		syntheticResp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewReader(trimmed)),
		}
		syntheticResp.Header.Set("Content-Type", "application/x-ndjson")
		p.processLogQueryResponse(w, r, syntheticResp)
		return
	}

	// Forward direction: single fetch with original limit (Lakehouse returns oldest-first naturally).
	params := p.buildColdQueryParams(r, logsqlQuery)
	resp, err := p.coldRouter.ColdPost(r.Context(), "/select/logsql/query", params)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, "cold backend error: "+err.Error())
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		p.writeError(w, resp.StatusCode, string(body))
		return
	}
	p.processLogQueryResponse(w, r, resp)
}

func (p *Proxy) proxyLogQueryBoth(w http.ResponseWriter, r *http.Request, logsqlQuery string) {
	startNs, endNs := ParseTimeRangeFromRequest(r)
	boundaryNs := p.coldRouter.ColdBoundaryNs()

	// Time-split so each backend only covers its own range:
	//   cold → [start, boundary]   (Lakehouse parquet data)
	//   hot  → [boundary, end]     (VictoriaLogs live data)
	// This prevents boundary overlap from returning duplicate rows.
	coldEndNs := boundaryNs
	if endNs < coldEndNs {
		coldEndNs = endNs
	}
	hotStartNs := startNs
	if boundaryNs > hotStartNs {
		hotStartNs = boundaryNs
	}

	hotParams := p.buildHotQueryParamsForRange(r, logsqlQuery, hotStartNs, endNs)

	var (
		hotResp, coldResp *http.Response
		hotErr, coldErr   error
		wg                sync.WaitGroup
	)

	wg.Add(2)
	go func() {
		defer wg.Done()
		hotResp, hotErr = p.vlPost(r.Context(), "/select/logsql/query", hotParams)
	}()
	go func() {
		defer wg.Done()
		if r.FormValue("direction") != "forward" {
			// Chunked backward scan: iterate newest-to-oldest 1-hour slices.
			baseParams := p.buildColdQueryParamsForRange(r, logsqlQuery, startNs, coldEndNs)
			baseParams.Del("start")
			baseParams.Del("end")
			baseParams.Del("limit")
			clientLimit, _ := strconv.Atoi(r.FormValue("limit"))
			if clientLimit <= 0 {
				clientLimit = 1000
			}
			ascBody, fetchErr := p.coldBackwardChunkedFetch(r.Context(), baseParams, startNs, coldEndNs, clientLimit)
			if fetchErr != nil {
				coldErr = fetchErr
				return
			}
			// Return ascending body — the existing merge code below calls
			// reverseNDJSONBody(coldBody) for backward direction, so this integrates
			// with the existing merge path without additional changes.
			coldResp = &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(bytes.NewReader(ascBody)),
			}
			coldResp.Header.Set("Content-Type", "application/x-ndjson")
			return
		}
		coldResp, coldErr = p.coldRouter.ColdPost(r.Context(), "/select/logsql/query",
			p.buildColdQueryParamsForRange(r, logsqlQuery, startNs, coldEndNs))
	}()
	wg.Wait()

	if hotErr != nil && coldErr != nil {
		p.writeError(w, statusFromUpstreamErr(hotErr), hotErr.Error())
		return
	}

	// Cold failed — propagate error rather than serving a silent hot-only partial response.
	// Hot covers [boundary, end] only; returning it alone truncates the requested range.
	if coldErr != nil {
		if hotResp != nil {
			hotResp.Body.Close()
		}
		p.writeError(w, http.StatusBadGateway, "cold backend error: "+coldErr.Error())
		return
	}
	if coldResp.StatusCode >= 400 {
		body, _ := readBodyLimited(coldResp.Body, maxUpstreamErrorBodyBytes)
		coldResp.Body.Close()
		if hotResp != nil {
			hotResp.Body.Close()
		}
		p.writeError(w, coldResp.StatusCode, string(body))
		return
	}

	// Hot failed — propagate error rather than serving a silent cold-only partial response.
	// Cold covers [start, boundary] only; returning it alone silently truncates
	// the [boundary, end] range without the client knowing live data is missing.
	if hotErr != nil {
		coldResp.Body.Close()
		p.writeError(w, http.StatusBadGateway, "hot backend error: "+hotErr.Error())
		return
	}
	if hotResp.StatusCode >= 400 {
		body, _ := readBodyLimited(hotResp.Body, maxUpstreamErrorBodyBytes)
		hotResp.Body.Close()
		coldResp.Body.Close()
		p.writeError(w, hotResp.StatusCode, string(body))
		return
	}

	// Both succeeded — merge with direction-aware ordering.
	// forward:  cold (ascending [start, boundary]) then hot (ascending [boundary, end])
	// backward: hot already newest-first from [boundary, end]; cold is oldest-first from
	//           [start, boundary] so reverse it first, then append so the client receives
	//           a contiguous newest-first stream across the full range.
	defer hotResp.Body.Close()
	defer coldResp.Body.Close()

	var merged io.Reader
	if r.FormValue("direction") == "forward" {
		merged = MergeNDJSON(coldResp.Body, hotResp.Body)
	} else {
		coldBody, readErr := io.ReadAll(coldResp.Body)
		if readErr != nil {
			p.writeError(w, http.StatusBadGateway, "failed to read cold response")
			return
		}
		merged = MergeNDJSON(hotResp.Body, bytes.NewReader(reverseNDJSONBody(coldBody)))
	}
	mergedBody, err := io.ReadAll(merged)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, "failed to merge hot+cold results")
		return
	}
	// Apply the original per-request limit to the merged body; without this the
	// client can receive up to 2× the requested limit (one batch per backend).
	mergedBody = trimNDJSONBodyToLimit(mergedBody, r.FormValue("limit"))

	syntheticResp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     hotResp.Header.Clone(),
		Body:       io.NopCloser(bytes.NewReader(mergedBody)),
	}
	p.processLogQueryResponse(w, r, syntheticResp)
}

// processLogQueryResponse converts a VL NDJSON response into a Loki-format JSON response.
// Shared between hot, cold, and merged code paths.
func (p *Proxy) processLogQueryResponse(w http.ResponseWriter, r *http.Request, resp *http.Response) {
	if resp.StatusCode >= 400 {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		p.writeError(w, resp.StatusCode, string(body))
		return
	}

	categorizedLabels := requestWantsCategorizedLabels(r)
	emitStructuredMetadata := p.shouldEmitStructuredMetadata(r)
	p.metrics.RecordTupleMode(tupleModeForRequest(categorizedLabels, emitStructuredMetadata))

	if p.streamResponse {
		p.streamLogQuery(w, resp, r.FormValue("query"), categorizedLabels, emitStructuredMetadata)
		return
	}

	collectPatterns := p.patternsEnabled && p.patternsAutodetectFromQueries
	streams, patterns, err := p.vlReaderToLokiStreams(
		resp.Body,
		r.FormValue("query"),
		r.FormValue("step"),
		categorizedLabels,
		emitStructuredMetadata,
		collectPatterns,
	)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	p.storeAutodetectedPatterns(
		r.Header.Get("X-Scope-OrgID"),
		p.fingerprintFromCtx(r.Context(), r),
		r.FormValue("query"),
		r.FormValue("start"),
		r.FormValue("end"),
		r.FormValue("step"),
		patterns,
	)

	if len(p.derivedFields) > 0 {
		p.applyDerivedFields(streams)
	}

	logqlQuery := r.FormValue("query")
	if strings.Contains(logqlQuery, "decolorize") {
		decolorizeStreams(streams)
	}
	if label, cidr, ok := parseIPFilter(logqlQuery); ok {
		streams = ipFilterStreams(streams, label, cidr)
	}
	if tmpl := extractLineFormatTemplate(logqlQuery); tmpl != "" {
		applyLineFormatTemplate(streams, tmpl)
	}

	p.writeJSON(w, map[string]interface{}{
		"status": "success",
		"data": func() map[string]interface{} {
			data := map[string]interface{}{
				"resultType": "streams",
				"result":     streams,
				"stats":      map[string]interface{}{},
			}
			if categorizedLabels {
				data["encodingFlags"] = []string{"categorize-labels"}
			}
			return data
		}(),
	})
}

// reverseNDJSONBody reverses the line order of a newline-delimited JSON body.
// The Lakehouse always returns rows in ascending time order; reversing gives
// backward (newest-first) ordering without a sort clause the Lakehouse cannot parse.
func reverseNDJSONBody(body []byte) []byte {
	var lines [][]byte
	for _, line := range bytes.Split(body, []byte("\n")) {
		line = bytes.TrimRight(line, "\r")
		if len(line) > 0 {
			lines = append(lines, line)
		}
	}
	for i, j := 0, len(lines)-1; i < j; i, j = i+1, j-1 {
		lines[i], lines[j] = lines[j], lines[i]
	}
	return bytes.Join(lines, []byte("\n"))
}

// trimNDJSONBodyToLimit trims a newline-delimited JSON body to at most limit lines.
// Non-positive or unparseable limitParam is treated as 1000 (the Loki default).
func trimNDJSONBodyToLimit(body []byte, limitParam string) []byte {
	limit, err := strconv.Atoi(limitParam)
	if err != nil || limit <= 0 {
		limit = 1000
	}
	var out []byte
	count := 0
	for _, line := range bytes.Split(body, []byte("\n")) {
		line = bytes.TrimRight(line, "\r")
		if len(line) == 0 {
			continue
		}
		if count >= limit {
			break
		}
		if count > 0 {
			out = append(out, '\n')
		}
		out = append(out, line...)
		count++
	}
	return out
}
