package proxy

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
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

func (p *Proxy) proxyLogQueryCold(w http.ResponseWriter, r *http.Request, logsqlQuery string) {
	params := p.buildLogQueryParams(r, logsqlQuery)
	resp, err := p.coldRouter.ColdPost(r.Context(), "/select/logsql/query", params)
	if err != nil {
		p.log.Warn("cold backend error, falling back to hot", "error", err)
		p.proxyLogQuery(w, r, logsqlQuery)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		p.log.Warn("cold backend returned error, falling back to hot", "status", resp.StatusCode)
		p.proxyLogQuery(w, r, logsqlQuery)
		return
	}

	p.processLogQueryResponse(w, r, resp)
}

func (p *Proxy) proxyLogQueryBoth(w http.ResponseWriter, r *http.Request, logsqlQuery string) {
	params := p.buildLogQueryParams(r, logsqlQuery)

	var (
		hotResp, coldResp *http.Response
		hotErr, coldErr   error
		wg                sync.WaitGroup
	)

	wg.Add(2)
	go func() {
		defer wg.Done()
		hotResp, hotErr = p.vlPost(r.Context(), "/select/logsql/query", params)
	}()
	go func() {
		defer wg.Done()
		coldResp, coldErr = p.coldRouter.ColdPost(r.Context(), "/select/logsql/query", params)
	}()
	wg.Wait()

	if hotErr != nil && coldErr != nil {
		p.writeError(w, statusFromUpstreamErr(hotErr), hotErr.Error())
		return
	}

	// Cold failed — serve hot only
	if coldErr != nil || (coldResp != nil && coldResp.StatusCode >= 400) {
		if coldResp != nil {
			coldResp.Body.Close()
		}
		if hotErr != nil {
			p.writeError(w, statusFromUpstreamErr(hotErr), hotErr.Error())
			return
		}
		defer hotResp.Body.Close()
		if hotResp.StatusCode >= 400 {
			body, _ := readBodyLimited(hotResp.Body, maxUpstreamErrorBodyBytes)
			p.writeError(w, hotResp.StatusCode, string(body))
			return
		}
		p.processLogQueryResponse(w, r, hotResp)
		return
	}

	// Hot failed — serve cold only
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

	// Both succeeded — merge
	defer hotResp.Body.Close()
	defer coldResp.Body.Close()

	merged := MergeNDJSON(hotResp.Body, coldResp.Body)
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
