package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// A wide binary range can exceed the manual metric response limit before its
// outer aggregation reduces the series. Retry only that bounded failure on
// disjoint evaluation steps. Each step is independent, so folding each part
// before merging gives the same series and values as folding the whole range.
func (p *Proxy) binaryAggregationResponse(r *http.Request, binary *logqlpkg.BinOpExpr, chain []*logqlpkg.VectorAggregation, resultType string, depth int) *bufferedResponseWriter {
	buf := &bufferedResponseWriter{}
	p.proxyBinaryLogQL(buf, r, binary, resultType)
	if buf.code == 0 || buf.code == http.StatusOK {
		for _, agg := range chain {
			buf.body = foldBinaryAggregation(buf.body, agg)
		}
		return buf
	}
	if resultType != "matrix" || depth >= 6 || buf.code != http.StatusInternalServerError || !bytes.Contains(buf.body, []byte("manual metric response exceeds")) {
		return buf
	}
	leftReq, rightReq, ok := splitBinaryAggregationRange(r)
	if !ok {
		return buf
	}
	left := p.binaryAggregationResponse(leftReq, binary, chain, resultType, depth+1)
	if left.code != 0 && left.code != http.StatusOK {
		return left
	}
	right := p.binaryAggregationResponse(rightReq, binary, chain, resultType, depth+1)
	if right.code != 0 && right.code != http.StatusOK {
		return right
	}
	merged, err := mergeBinaryAggregationRanges(left.body, right.body, p.limits().BufferedBackendBodyBytes)
	if err != nil {
		failed := &bufferedResponseWriter{}
		p.writeError(failed, http.StatusInternalServerError, err.Error())
		return failed
	}
	merged, err = capSeriesToLimit(r.Context(), merged, p.resolvedMaxStatsQuerySeries(r.Context()))
	if err != nil {
		failed := &bufferedResponseWriter{}
		p.writeError(failed, http.StatusBadRequest, err.Error())
		return failed
	}
	result := &bufferedResponseWriter{body: merged}
	copyHeaders(result.Header(), left.Header())
	copyHeaders(result.Header(), right.Header())
	return result
}

func splitBinaryAggregationRange(r *http.Request) (*http.Request, *http.Request, bool) {
	start, startOK := parseLokiTimeToUnixNano(r.FormValue("start"))
	end, endOK := parseLokiTimeToUnixNano(r.FormValue("end"))
	step, stepOK := parsePositiveStepDuration(r.FormValue("step"))
	if !startOK || !endOK || !stepOK || end <= start || int64(step) <= 0 {
		return nil, nil, false
	}
	points := (end-start)/int64(step) + 1
	if points < 2 {
		return nil, nil, false
	}
	leftEnd := start + (points/2-1)*int64(step)
	rightStart := leftEnd + int64(step)
	return binaryAggregationRangeAt(r, start, leftEnd), binaryAggregationRangeAt(r, rightStart, end), true
}

func binaryAggregationRangeAt(r *http.Request, start, end int64) *http.Request {
	child := cloneMetricQueryRequest(r, r.FormValue("query"))
	params := child.URL.Query()
	for key, value := range map[string]int64{"start": start, "end": end} {
		encoded := strconv.FormatInt(value, 10)
		child.Form.Set(key, encoded)
		if child.Method == http.MethodPost {
			child.PostForm.Set(key, encoded)
		}
		params.Set(key, encoded)
	}
	child.URL.RawQuery = params.Encode()
	return child
}

func mergeBinaryAggregationRanges(left, right []byte, limit int) ([]byte, error) {
	type series struct {
		Metric map[string]string `json:"metric"`
		Values [][]any           `json:"values"`
	}
	type answer struct {
		Data struct {
			ResultType string   `json:"resultType"`
			Result     []series `json:"result"`
		} `json:"data"`
	}
	combined := map[string]*series{}
	for _, body := range [][]byte{left, right} {
		var response answer
		if err := json.Unmarshal(body, &response); err != nil || response.Data.ResultType != "matrix" {
			return nil, fmt.Errorf("invalid binary aggregation range response")
		}
		for _, item := range response.Data.Result {
			key := binaryLabelKey(item.Metric)
			if prior := combined[key]; prior != nil {
				prior.Values = append(prior.Values, item.Values...)
			} else {
				copy := item
				combined[key] = &copy
			}
		}
	}
	result := make([]*series, 0, len(combined))
	for _, item := range combined {
		result = append(result, item)
	}
	sort.Slice(result, func(i, j int) bool { return compareLabelSets(result[i].Metric, result[j].Metric) < 0 })
	body, err := json.Marshal(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": result}})
	if err != nil {
		return nil, err
	}
	if len(body) > limit {
		return nil, fmt.Errorf("manual metric response exceeds %d bytes; narrow the query or increase -backend-max-buffered-response-bytes", limit)
	}
	return body, nil
}
