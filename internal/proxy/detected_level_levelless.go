package proxy

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"time"
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
	return logsql.StreamFilter{Matchers: levelLessMatchers()}.String()
})

// levelLessMatchers are the matchers of levelLessStreamFilter.
func levelLessMatchers() []logsql.LabelMatcher {
	matchers := make([]logsql.LabelMatcher, 0, len(levelFieldNames)+1)
	matchers = append(matchers, logsql.LabelMatcher{Name: detectedLevelLabel, Op: "="})
	for _, name := range levelFieldNames {
		matchers = append(matchers, logsql.LabelMatcher{Name: name, Op: "="})
	}
	return matchers
}

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
// levelLessStreamsOf returns the streams of a per-stream stats answer that
// decide no level and carry no detected_level, or nil.
func levelLessStreamsOf(body []byte) map[string]bool {
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
	return levelLess
}

func (p *Proxy) withLevelLessStreams(ctx context.Context, path string, params url.Values, body []byte, maxSeries int) ([]byte, error) {
	// A first read restricted to the labelled streams (levelLessFirstRead) is
	// followed by the read of the others whatever it answered. Otherwise the
	// labels of each series are read in place, and only an answer that names a
	// stream without a level label is parsed and rebuilt.
	params, known := withoutLabelledRestriction(params)
	var levelLess map[string]bool
	if !known {
		if !bytes.Contains(body, statsStreamKey) {
			return body, nil
		}
		if levelLess = levelLessStreamsOf(body); levelLess == nil {
			return body, nil
		}
		p.rememberLevelLessBase(ctx, path, params)
	}
	grouped, ok := logsql.WithGroupKeyAfterStream(params.Get("query"), detectedLevelLabel)
	if !ok {
		return body, nil
	}
	follow := levelLessParams(params)
	follow.Set("query", withLevelChainAhead(grouped))
	resp, err := p.vlPost(context.WithValue(ctx, levelLessReadKey{}, true), path, follow)
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
	if known && len(derivedResults) == 0 {
		// The base holds no stream without a level label any more.
		p.forgetLevelLessBase(ctx, path, params)
	}
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

// takeStatsQueryRangeSlot waits for a stats_query_range slot
// (-stats-query-range-concurrency) and returns its release, which may be called
// more than once; without the limit it returns at once.
func (p *Proxy) takeStatsQueryRangeSlot(ctx context.Context) (func(), error) {
	sem := p.statsQueryRangeSem
	if sem == nil {
		return func() {}, nil
	}
	select {
	case <-sem:
		var once sync.Once
		return func() { once.Do(func() { sem <- struct{}{} }) }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// levelLessReadKey marks the read of the streams without a level label: its
// chain is narrowed on the instant stats route too, and its presence witness
// rides every stats pipe (pruneLevelChain).
type levelLessReadKey struct{}

func levelLessRead(ctx context.Context) bool {
	on, _ := ctx.Value(levelLessReadKey{}).(bool)
	return on
}

// labelledStreamFilter is the complement of levelLessStreamFilter: the streams
// whose labels decide a level.
var labelledStreamFilter = sync.OnceValue(func() string {
	return logsql.Exclude{Expr: logsql.StreamFilter{Matchers: levelLessMatchers()}}.String()
})

// levelLessBaseTTL is how long the proxy remembers that a per-stream read held
// a stream without a level label.
const levelLessBaseTTL = 10 * time.Minute

func (p *Proxy) levelLessBaseKey(ctx context.Context, path string, params url.Values) string {
	return p.windowProbeKey(ctx, "level-less-base:v1:"+path+"\x00"+params.Get("query"))
}

// levelLessFirstRead returns the params of a per-stream stats read. When the
// same read lately named a stream without a level label, it is restricted to
// the labelled streams: the others are read once, with the chain
// (withLevelLessStreams), instead of twice. The two reads cover disjoint
// streams whose union is every stream the read matches.
func (p *Proxy) levelLessFirstRead(ctx context.Context, path string, params url.Values) url.Values {
	if p.cache == nil {
		return params
	}
	if _, ok := p.cacheGet(ctx, p.cache, p.levelLessBaseKey(ctx, path, params)); !ok {
		return params
	}
	out := cloneURLValues(params)
	out.Add("extra_filters", labelledStreamFilter())
	return out
}

func (p *Proxy) rememberLevelLessBase(ctx context.Context, path string, params url.Values) {
	if p.cache != nil {
		p.cache.SetWithTTL(p.levelLessBaseKey(ctx, path, params), []byte("1"), levelLessBaseTTL)
	}
}

func (p *Proxy) forgetLevelLessBase(ctx context.Context, path string, params url.Values) {
	if p.cache != nil {
		p.cache.Invalidate(p.levelLessBaseKey(ctx, path, params))
	}
}

// withoutLabelledRestriction returns params without the restriction
// levelLessFirstRead added, and whether it was there.
func withoutLabelledRestriction(params url.Values) (url.Values, bool) {
	filters := params["extra_filters"]
	at := slices.Index(filters, labelledStreamFilter())
	if at < 0 {
		return params, false
	}
	out := cloneURLValues(params)
	out["extra_filters"] = slices.Delete(slices.Clone(filters), at, at+1)
	if len(out["extra_filters"]) == 0 {
		delete(out, "extra_filters")
	}
	return out, true
}
