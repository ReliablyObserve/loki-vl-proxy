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

	// Hot failed — serve cold only (cold fully covers its own time split).
	if hotErr != nil || (hotResp != nil && hotResp.StatusCode >= 400) {
		if hotResp != nil {
			hotResp.Body.Close()
		}
		defer coldResp.Body.Close()
		if coldResp.StatusCode >= 400 {
			body, _ := readBodyLimited(coldResp.Body, maxUpstreamErrorBodyBytes)
			p.writeError(w, coldResp.StatusCode, string(body))
			return
		}
		p.processLogQueryResponse(w, r, coldResp)
		return
	}

	// Both succeeded — merge with direction-aware ordering so the stream assembler
	// receives entries in the order expected by vlReaderToLokiStreams:
	//   backward (default): hot first (newest), then cold (oldest)
	//   forward:            cold first (oldest), then hot (newest)
	defer hotResp.Body.Close()
	defer coldResp.Body.Close()

	var merged io.Reader
	if r.FormValue("direction") == "forward" {
		merged = MergeNDJSON(coldResp.Body, hotResp.Body)
	} else {
		merged = MergeNDJSON(hotResp.Body, coldResp.Body)
	}
	mergedBody, err := io.ReadAll(merged)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, "failed to merge hot+cold results")
		return
	}

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
