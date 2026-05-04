package proxy

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
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

func (p *Proxy) proxyLogQueryCold(w http.ResponseWriter, r *http.Request, logsqlQuery string) {
	params := p.buildColdQueryParams(r, logsqlQuery)
	resp, err := p.coldRouter.ColdPost(r.Context(), "/select/logsql/query", params)
	if err != nil {
		// RouteColdOnly means the hot backend has no data for this range — a cold
		// failure must be surfaced rather than silently returning an empty hot response.
		p.writeError(w, http.StatusBadGateway, "cold backend error: "+err.Error())
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		p.writeError(w, resp.StatusCode, string(body))
		return
	}

	// Lakehouse stores parquet rows and scans manifest files in ascending timestamp
	// order. For backward queries the proxy must reverse the NDJSON line order so
	// vlReaderToLokiStreams assembles each stream newest-first, matching Loki semantics.
	if r.FormValue("direction") != "forward" {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			p.writeError(w, http.StatusBadGateway, "cold read error: "+readErr.Error())
			return
		}
		syntheticResp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     resp.Header.Clone(),
			Body:       io.NopCloser(reverseNDJSONLines(bytes.NewReader(body))),
		}
		p.processLogQueryResponse(w, r, syntheticResp)
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

	coldParams := p.buildColdQueryParamsForRange(r, logsqlQuery, startNs, coldEndNs)
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
		coldResp, coldErr = p.coldRouter.ColdPost(r.Context(), "/select/logsql/query", coldParams)
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

	// Hot failed — propagate rather than returning a silent cold-only partial response.
	// After time-splitting, cold covers [start, boundary] and hot covers [boundary, end].
	// Serving cold alone silently truncates the requested range without the client knowing.
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

	// Both succeeded — merge with direction-aware ordering:
	//   backward (default): hot first (newest [boundary,end]), then cold reversed (newest-first [boundary,start])
	//   forward:            cold first (oldest [start,boundary]), then hot (newest [boundary,end])
	//
	// Lakehouse stores parquet rows ascending, so cold results are always ascending.
	// For backward queries, reverse cold lines before concatenating with the already-descending hot results
	// so the stream assembler receives a consistent newest-first stream for each Loki stream key.
	defer hotResp.Body.Close()

	coldBody, err := io.ReadAll(coldResp.Body)
	coldResp.Body.Close()
	if err != nil {
		p.writeError(w, http.StatusBadGateway, "failed to read cold results")
		return
	}

	var merged io.Reader
	if r.FormValue("direction") == "forward" {
		merged = MergeNDJSON(bytes.NewReader(coldBody), hotResp.Body)
	} else {
		merged = MergeNDJSON(hotResp.Body, reverseNDJSONLines(bytes.NewReader(coldBody)))
	}
	mergedBody, err := io.ReadAll(merged)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, "failed to merge hot+cold results")
		return
	}

	// Enforce the original limit: each split sub-request was sent the full limit,
	// so the merged result can contain up to 2× limit entries without truncation.
	if limitStr := r.FormValue("limit"); limitStr != "" {
		if n, convErr := strconv.Atoi(limitStr); convErr == nil && n > 0 {
			mergedBody = limitNDJSONLines(mergedBody, n)
		}
	}

	syntheticResp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     hotResp.Header.Clone(),
		Body:       io.NopCloser(bytes.NewReader(mergedBody)),
	}
	p.processLogQueryResponse(w, r, syntheticResp)
}

// reverseNDJSONLines reads all NDJSON lines from r, reverses their order, and
// returns an io.Reader over the reversed content. Used to correct ascending
// Lakehouse output to descending order for backward Loki queries.
func reverseNDJSONLines(r io.Reader) io.Reader {
	data, err := io.ReadAll(r)
	if err != nil || len(data) == 0 {
		return bytes.NewReader(data)
	}
	lines := bytes.Split(bytes.TrimRight(data, "\n"), []byte("\n"))
	nonEmpty := lines[:0]
	for _, l := range lines {
		if len(bytes.TrimSpace(l)) > 0 {
			nonEmpty = append(nonEmpty, l)
		}
	}
	for i, j := 0, len(nonEmpty)-1; i < j; i, j = i+1, j-1 {
		nonEmpty[i], nonEmpty[j] = nonEmpty[j], nonEmpty[i]
	}
	return bytes.NewReader(bytes.Join(nonEmpty, []byte("\n")))
}

// limitNDJSONLines returns data truncated to at most n non-empty NDJSON lines.
func limitNDJSONLines(data []byte, n int) []byte {
	count := 0
	pos := 0
	for pos < len(data) && count < n {
		end := bytes.IndexByte(data[pos:], '\n')
		var line []byte
		if end < 0 {
			line = data[pos:]
			if len(bytes.TrimSpace(line)) > 0 {
				count++
			}
			pos = len(data)
		} else {
			line = data[pos : pos+end]
			if len(bytes.TrimSpace(line)) > 0 {
				count++
			}
			pos += end + 1
		}
	}
	return data[:pos]
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
