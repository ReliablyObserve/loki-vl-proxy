package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"

	"github.com/grafana/jsonparser"
	fj "github.com/valyala/fastjson"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// Streams without a level label.
//
// A metric read whose series is the stream (stats by (_stream), next to the
// labels a parser captures) has no level column: the proxy gives each series
// the level its stream's labels decide (ensureStreamDetectedLevel). A stream
// without a level label decides none, while Loki gives every line the level its
// structured metadata or its body carries at ingest, else unknown, so one such
// stream can be several Loki series. VictoriaLogs' detected_level chain
// (translator.BuildDetectedLevelChain) computes that value from the whole row.
//
// The read is sent as it always was. Only when its answer names a stream that
// decides no level is a second read sent: the same query with the chain ahead of
// its pipes and detected_level next to the stream in its group, restricted with
// extra_filters to the streams without a level label (levelLessStreamFilter).
// Its answer replaces those streams' series. A window whose streams all carry a
// level label therefore sends today's LogsQL and nothing more, and the chain
// (narrowed to what the window holds, pruneLevelChain) reads only the rows
// that need it.

// streamDecidesLevel reports whether a stream's labels (VictoriaLogs names, as
// _stream holds them) decide detected_level for all its lines: a level field of
// Loki's list (streamLevel) or a detected_level label. It is the exact
// complement of levelLessStreamFilter.
func streamDecidesLevel(stream map[string]string) bool {
	if stream[detectedLevelLabel] != "" {
		return true
	}
	_, ok := streamLevel(stream)
	return ok
}

// levelLessStreamFilter is the stream filter of the streams that decide no
// level: every name streamDecidesLevel reads is missing or empty. VictoriaLogs
// matches a missing label with ="" in a stream filter.
var levelLessStreamFilter = sync.OnceValue(func() string {
	matchers := make([]logsql.LabelMatcher, 0, len(levelFieldNames)+1)
	matchers = append(matchers, logsql.LabelMatcher{Name: detectedLevelLabel, Op: "="})
	for _, name := range levelFieldNames {
		matchers = append(matchers, logsql.LabelMatcher{Name: name, Op: "="})
	}
	return logsql.StreamFilter{Matchers: matchers}.String()
})

// levelLessParams returns params restricted to the streams that decide no
// level, through extra_filters: an argument VictoriaLogs ANDs with the query,
// so the query text keeps the chain the presence probe and the witness find,
// and the cache keys, which hold every argument, keep the two reads apart.
func levelLessParams(params url.Values) url.Values {
	out := cloneURLValues(params)
	out.Add("extra_filters", levelLessStreamFilter())
	return out
}

// withLevelChainAhead returns query with the detected_level chain between its
// filters and its pipes, where it reads each row as VictoriaLogs stored it, as
// Loki's ingest does before any parser runs.
func withLevelChainAhead(query string) string {
	chain := fullLevelChain()
	at := translator.FirstTopLevelPipe(query)
	if at < 0 {
		return query + " " + chain
	}
	return query[:at] + chain + " " + query[at:]
}

// statsStreamKey is the key of a stats answer's _stream label: a read that does
// not group by the stream has none, and is passed through unread.
var statsStreamKey = []byte(`"_stream":`)

// withLevelLessStreams completes the answer of a stats_query or
// stats_query_range read (path, params, body) whose series are streams: the series of the streams that
// decide no level, which carry no detected_level, are read again with
// VictoriaLogs' derivation, grouped by stream and detected_level, and replace
// them. A series that holds detected_level (VictoriaLogs derived it), or a level
// the read grouped by (ensureStreamDetectedLevel reads it), is decided. The
// merged answer holds at most maxSeries series, as the read does
// (seriesCountingReader).
func (p *Proxy) withLevelLessStreams(ctx context.Context, path string, params url.Values, body []byte, maxSeries int) ([]byte, error) {
	if !bytes.Contains(body, statsStreamKey) {
		return body, nil
	}
	// The labels of each series are read in place: only an answer that names a
	// stream without a level label is parsed and rebuilt.
	var levelLess map[string]bool
	_, _ = jsonparser.ArrayEach(body, func(res []byte, _ jsonparser.ValueType, _ int, _ error) {
		metric, kind, _, err := jsonparser.Get(res, "metric")
		if err != nil || kind != jsonparser.Object {
			return
		}
		if _, _, _, err := jsonparser.Get(metric, detectedLevelLabel); err == nil {
			return
		}
		if lv, err := jsonparser.GetString(metric, "level"); err == nil && lv != "" {
			return
		}
		streamStr, err := jsonparser.GetString(metric, "_stream")
		if err != nil || levelLess[streamStr] || streamDecidesLevel(parseStreamLabels(streamStr)) {
			return
		}
		if levelLess == nil {
			levelLess = map[string]bool{}
		}
		levelLess[streamStr] = true
	}, "data", "result")
	if levelLess == nil {
		return body, nil
	}
	grouped, ok := logsql.WithGroupKeyAfterStream(params.Get("query"), detectedLevelLabel)
	if !ok {
		return body, nil
	}
	follow := levelLessParams(params)
	follow.Set("query", withLevelChainAhead(grouped))
	resp, err := p.vlPost(ctx, path, follow)
	if err != nil {
		return nil, &statsUpstreamError{err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		errBody, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		return nil, &statsUpstreamError{status: resp.StatusCode, body: errBody}
	}
	derivedBody, err := readBodyLimited(&seriesCountingReader{r: resp.Body, limit: maxSeries}, int64(p.limits().BufferedBackendBodyBytes))
	if err != nil {
		return nil, err
	}
	derived, err := fj.ParseBytes(derivedBody)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	v, err := fj.ParseBytes(body)
	if err != nil {
		return body, nil // the caller reports what it cannot read
	}
	results := v.GetArray("data", "result")
	// The second read's series replace the first read's series of the streams
	// it answers; a stream it does not answer (no row matched any more) keeps
	// the first read's series.
	derivedResults := derived.GetArray("data", "result")
	answered := make(map[string]bool, len(levelLess))
	for _, res := range derivedResults {
		answered[string(res.GetObject("metric").Get("_stream").GetStringBytes())] = true
	}
	var arena fj.Arena
	merged := arena.NewArray()
	n := 0
	for _, res := range results {
		if !answered[string(res.GetObject("metric").Get("_stream").GetStringBytes())] {
			merged.SetArrayItem(n, res)
			n++
		}
	}
	for _, res := range derivedResults {
		merged.SetArrayItem(n, res)
		n++
	}
	if maxSeries > 0 && n > maxSeries {
		return nil, errSeriesCountExceeded
	}
	v.Get("data").Set("result", merged)
	return v.MarshalTo(nil), nil
}

// levelLessErr is the error of withLevelLessStreams as a route that is not the
// stats_query_range route reports it: a status VictoriaLogs answered, with its
// body for the logs only, or the transport error.
func (p *Proxy) levelLessErr(path string, err error) error {
	var upstream *statsUpstreamError
	if !errors.As(err, &upstream) {
		return err
	}
	if upstream.status > 0 {
		return p.redactedBackendStatusError(path, upstream.status, upstream.body)
	}
	return upstream.err
}
