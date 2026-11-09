package proxy

import (
	"bytes"
	"context"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	mw "github.com/ReliablyObserve/Loki-VL-proxy/internal/middleware"
)

// Query statistics: Loki fills data.stats of every query and query_range
// answer, Grafana turns data.stats.summary into the frame's meta.stats (the
// query inspector's "Query details" and the Logs Drilldown footer's "Total
// bytes processed"). The proxy fills the same object from what it measured
// for THIS request (the request flow, query_flow.go): its own time, its wait
// in the heavy-query queue, the VictoriaLogs calls it made and the bytes and
// log rows it read from them, the entries it returned and its own cache
// lookups. Nothing is estimated: a field nothing measures stays 0.
//
// One layer fills it for every path (log, metric, cold, multi-tenant, limit
// and cache answers): it sits inside the request logger, which owns the
// request's flow, and outside the response caches, so a cached body never
// carries stats and a cache hit reports its own work (no VictoriaLogs bytes,
// the cache entry it found), as Loki's results cache does. A response-cache
// hit is finished by the cache itself (query_stats_cache.go): it knows the
// body's entry count and, for a gzip client, holds the body's compressed
// start, so a hit neither scans nor recompresses the answer.

// queryStatsActive reports whether the stats layer fills this request's
// answer: the response caches then must not replay a stored encoded body,
// which the layer could not fill.
func queryStatsActive(ctx context.Context) bool {
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return false
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.statsWriter != nil
}

// The Loki cache sections of stats.cache, in Loki's order.
const (
	lokiCacheChunk = iota
	lokiCacheIndex
	lokiCacheResult
	lokiCacheStatsResult
	lokiCacheVolumeResult
	lokiCacheSeriesResult
	lokiCacheLabelResult
	lokiCacheInstantMetricResult
	lokiCacheClassCount
)

var lokiCacheClassNames = [lokiCacheClassCount]string{
	"chunk", "index", "result", "statsResult", "volumeResult", "seriesResult", "labelResult", "instantMetricResult",
}

// lokiCacheClassOf maps a proxy cache key to the Loki cache section that
// plays the same part, or -1 when no Loki section does (routing hints,
// estimates, metadata lookups). The proxy's whole-answer caches (compat
// response cache, the inner query_range cache, the log window cache, the
// settled steps cache) are Loki's results cache; the stats extent cache holds
// VictoriaLogs stats answers (Loki's statsResult); volume segments hold
// volume rows (volumeResult).
func lokiCacheClassOf(key string) int {
	switch {
	case isStatsSidecarKey(key):
		return -1 // the stats layer's own bookkeeping (query_stats_cache.go)
	case strings.HasPrefix(key, statsExtentKeyPrefix):
		return lokiCacheStatsResult
	case strings.HasPrefix(key, volumeSegmentKeyVersion+":"):
		return lokiCacheVolumeResult
	case strings.HasPrefix(key, "compat:v2:query_range:"), strings.HasPrefix(key, "query_range:"),
		strings.HasPrefix(key, queryStepsKeyPrefix):
		return lokiCacheResult
	case strings.HasPrefix(key, "query_range_window_has_hits"):
		return -1 // a row-count estimate, not an answer
	case strings.HasPrefix(key, "query_range_window"):
		return lokiCacheResult
	case strings.HasPrefix(key, "compat:v2:query:"):
		return lokiCacheInstantMetricResult
	}
	return -1
}

// flowLokiCache is one Loki cache section's share of a request's lookups and
// stores.
type flowLokiCache struct {
	requested, found, stored int32
	received, sent           int64
	dur                      time.Duration
}

// lokiCacheSlots maps a Loki cache section to its slot in the request flow:
// only the sections a proxy cache plays (lokiCacheClassOf) are counted.
var lokiCacheSlots = [lokiCacheClassCount]int8{-1, -1, 0, 1, 2, -1, -1, 3}

const lokiCacheSlotCount = 4

func recordFlowCacheStore(ctx context.Context, key string, n int) {
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return
	}
	class := lokiCacheClassOf(key)
	if class < 0 || lokiCacheSlots[class] < 0 {
		return
	}
	rt.mu.Lock()
	lc := &rt.flow.lokiCache[lokiCacheSlots[class]]
	lc.stored++
	lc.sent += int64(max(n, 0))
	rt.mu.Unlock()
}

func recordFlowQueue(ctx context.Context, d time.Duration) {
	rt := getRequestTelemetry(ctx)
	if rt == nil || d <= 0 {
		return
	}
	rt.mu.Lock()
	rt.flow.queueDur += d
	rt.mu.Unlock()
}

func recordFlowVLRows(ctx context.Context, rows int64) {
	rt := getRequestTelemetry(ctx)
	if rt == nil || rows <= 0 {
		return
	}
	rt.mu.Lock()
	rt.flow.vlRows += rows
	rt.mu.Unlock()
}

// queryStatsLayer fills data.stats of query and query_range answers.
func (p *Proxy) queryStatsLayer(endpoint string, next http.HandlerFunc) http.HandlerFunc {
	if endpoint != "query" && endpoint != "query_range" {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		rt := getRequestTelemetry(r.Context())
		if rt == nil {
			next(w, r)
			return
		}
		sw := queryStatsWriterPool.Get().(*queryStatsWriter)
		sw.reset(w, rt)
		rt.mu.Lock()
		rt.statsWriter = sw
		rt.mu.Unlock()
		next(sw, r)
		sw.finish()
		rt.mu.Lock()
		rt.statsWriter = nil
		rt.mu.Unlock()
		sw.reset(nil, nil)
		queryStatsWriterPool.Put(sw)
	}
}

var queryStatsWriterPool = sync.Pool{New: func() any { return new(queryStatsWriter) }}

// queryStatsTailHold is how many trailing bytes the writer holds back: enough
// for the longest answer ending it rewrites (a sorted-key matrix envelope
// end plus trailing whitespace).
const queryStatsTailHold = 64

// queryStatsWriter passes the answer through, counting the entries it
// returns, and holds back its last bytes so it can put the stats object into
// the envelope's end once the handler has finished. Its buffers are fixed
// arrays and the writer is pooled: filling the stats allocates nothing.
type queryStatsWriter struct {
	http.ResponseWriter
	rt          *requestTelemetry
	wroteHeader bool
	patch       bool
	// done: the answer was finished by the response cache (serveCachedAnswer).
	done         bool
	entriesKnown bool
	headN, tailN int
	total        int                      // bytes the handler wrote
	head         [queryStatsTailHold]byte // the first bytes, to check the envelope's start
	tail         [queryStatsTailHold]byte // the held-back end of the body
	entries      entryCounter
	buf          [2560]byte // the stats object, and the gzip block header and trailer
}

func (w *queryStatsWriter) reset(rw http.ResponseWriter, rt *requestTelemetry) {
	w.ResponseWriter, w.rt = rw, rt
	w.wroteHeader, w.patch, w.done, w.entriesKnown = false, false, false, false
	w.headN, w.tailN, w.total = 0, 0, 0
	w.entries = entryCounter{}
}

func (w *queryStatsWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *queryStatsWriter) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}
	w.decide(code)
	w.ResponseWriter.WriteHeader(code)
}

// decide fixes, before the headers go out, whether the answer is filled: a
// JSON success answer the handler did not encode.
func (w *queryStatsWriter) decide(code int) {
	w.wroteHeader = true
	h := w.Header()
	w.patch = code == http.StatusOK && h.Get("Content-Encoding") == "" &&
		strings.Contains(h.Get("Content-Type"), "json")
	if w.patch {
		h.Del("Content-Length")
	}
}

func (w *queryStatsWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.decide(http.StatusOK) // the implicit 200 stays implicit below
	}
	if !w.patch {
		return w.ResponseWriter.Write(b)
	}
	w.total += len(b)
	if w.headN < len(w.head) {
		w.headN += copy(w.head[w.headN:], b)
		if w.entries.mode == entryModeUnknown && w.headN >= len(statsHeadStreams) {
			w.entries.mode = entryModeOf(w.head[:w.headN])
		}
	}
	if !w.entriesKnown {
		w.entries.add(b)
	}
	if w.tailN+len(b) <= queryStatsTailHold {
		w.tailN += copy(w.tail[w.tailN:], b)
		return len(b), nil
	}
	excess := w.tailN + len(b) - queryStatsTailHold
	if excess <= w.tailN {
		if _, err := w.ResponseWriter.Write(w.tail[:excess]); err != nil {
			return 0, err
		}
		w.tailN = copy(w.tail[:], w.tail[excess:w.tailN])
		w.tailN += copy(w.tail[w.tailN:], b)
		return len(b), nil
	}
	if w.tailN > 0 {
		if _, err := w.ResponseWriter.Write(w.tail[:w.tailN]); err != nil {
			return 0, err
		}
	}
	if _, err := w.ResponseWriter.Write(b[:excess-w.tailN]); err != nil {
		return 0, err
	}
	w.tailN = copy(w.tail[:], b[len(b)-queryStatsTailHold:])
	return len(b), nil
}

// Flush sends what is not held back (a streamed answer keeps streaming).
func (w *queryStatsWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *queryStatsWriter) finish() {
	if w.done || !w.patch || w.tailN == 0 {
		return
	}
	s, ok := statsSplice(w.head[:w.headN], w.tail[:w.tailN])
	if !ok {
		_, _ = w.ResponseWriter.Write(w.tail[:w.tailN])
		return
	}
	// An answer the client compression threshold left uncompressed stays
	// so with its stats, as before them.
	mw.SkipCompressionUnder(w.ResponseWriter, w.total)
	// The answer's end with its stats, written at once.
	end := w.appendStatsEnd(w.buf[:0], s)
	if w.rt.statsPrefixCapture && mw.MarkEncodedResponsePrefix(w.ResponseWriter) {
		// The response cache keeps the encoding up to here for its hits
		// (query_stats_cache.go); it needs the bytes written after the mark.
		suffix := append([]byte(nil), end...)
		w.rt.mu.Lock()
		w.rt.statsSuffix = suffix
		w.rt.mu.Unlock()
	}
	_, _ = w.ResponseWriter.Write(end)
}

// appendStatsEnd appends the answer's end cut by s with the stats object in
// its place.
func (w *queryStatsWriter) appendStatsEnd(dst []byte, s statsSplit) []byte {
	dst = append(append(dst, s.pre...), s.insert...)
	dst = w.appendStats(dst)
	return append(append(dst, s.post...), s.ws...)
}

var (
	statsHeadStatusFirst = []byte(`{"status":"success","data":{`)
	statsHeadDataFirst   = []byte(`{"data":{`)
	statsHeadStreams     = []byte(`{"status":"success","data":{"resultType":"streams"`)
	statsHeadMatrix      = []byte(`{"status":"success","data":{"resultType":"matrix"`)
	statsHeadVector      = []byte(`{"status":"success","data":{"resultType":"vector"`)
	statsTailEmpty       = []byte(`"stats":{}}}`)
	statsTailSorted      = []byte(`"stats":{}},"status":"success"}`)
	statsTailSortedNone  = []byte(`"},"status":"success"}`)
	statsTailResultEnd   = []byte(`]}}`)
	statsResultTypeKey   = []byte(`"resultType":"`)
	statsInsertKey       = []byte(`,"stats":`)
	statsKey             = []byte(`"stats":`)
	statsCloseData       = []byte(`}}`)
)

// statsSplit is an answer's end cut where the stats object goes: the answer
// with stats is pre + insert + stats + post + ws.
type statsSplit struct {
	pre, insert, post, ws []byte
}

// statsSplice cuts the end of a Loki success envelope where data.stats goes:
// `"stats":{}` is replaced, and an envelope without a stats key gets one as
// data's last key (Loki's order: resultType, result, stats). Any other
// ending is left alone (false), and so is an ending without stats whose
// held-back bytes show a stats key. Known limit: an answer whose data holds a
// stats key earlier than its last 64 bytes and ends with an array would get a
// second stats key; no proxy writer orders data that way (every writer that
// writes stats writes it last, as Loki does). head is the answer's first bytes, tail its
// last; they may overlap for a short answer.
func statsSplice(head, tail []byte) (statsSplit, bool) {
	body := bytes.TrimRight(tail, " \t\r\n")
	s := statsSplit{ws: tail[len(body):]}
	switch {
	case bytes.HasPrefix(head, statsHeadStatusFirst) && bytes.HasSuffix(body, statsTailEmpty):
		s.pre, s.post = body[:len(body)-4], statsCloseData
	case bytes.HasPrefix(head, statsHeadStatusFirst) && bytes.HasSuffix(body, statsTailResultEnd) &&
		!bytes.Contains(body, statsKey):
		s.pre, s.insert, s.post = body[:len(body)-2], statsInsertKey, statsCloseData
	case bytes.HasPrefix(head, statsHeadDataFirst) && bytes.HasSuffix(body, statsTailSorted):
		cut := len(body) - len(statsTailSorted) + len(`"stats":`)
		s.pre, s.post = body[:cut], body[cut+2:]
	case bytes.HasPrefix(head, statsHeadDataFirst) && bytes.HasSuffix(body, statsTailSortedNone) &&
		sortedEnvelopeEndsWithResultType(body):
		cut := len(body) - len(statsTailSortedNone) + 1
		s.pre, s.insert, s.post = body[:cut], statsInsertKey, body[cut:]
	default:
		return statsSplit{}, false
	}
	return s, true
}

// sortedEnvelopeEndsWithResultType: data's last key is resultType with a
// plain value (`"resultType":"matrix"},"status":"success"}`).
func sortedEnvelopeEndsWithResultType(body []byte) bool {
	end := len(body) - len(statsTailSortedNone)
	i := bytes.LastIndex(body[:end], statsResultTypeKey)
	if i < 0 {
		return false
	}
	switch string(body[i+len(statsResultTypeKey) : end]) {
	case "matrix", "vector", "streams", "scalar":
		return true
	}
	return false
}

// entryCounter counts the entries an answer returns as Loki's
// totalEntriesReturned does: one per log entry of a streams answer and one
// per series of a matrix or vector answer. In JSON a quote inside a string is
// always escaped, so these sequences only occur as structure: a `[` after `[`
// or `,` and before `"` opens a log entry (`["<ts>","<line>"...]`), a `{`
// after `"metric":` opens a series (label values are strings, so a label
// named "metric" is followed by a quote, never a brace). One pass over each
// write finds the bracket bytes; the last bytes of a write are carried so a
// sequence split across writes is counted once. The answer's first bytes
// name its result type for the status-first writers, and then only that
// type's sequence is looked for.
type entryCounter struct {
	n       int64
	carry   [9]byte
	cn      int
	pending bool // the last byte was an entry's `[`, its quote not seen yet
	mode    uint8
}

const (
	entryModeUnknown = iota // both sequences
	entryModeAny            // both, for good (the start names no type)
	entryModeStreams
	entryModeSeries
)

var seriesOpenPrefix = []byte(`"metric":`)

func entryModeOf(head []byte) uint8 {
	switch {
	case bytes.HasPrefix(head, statsHeadStreams):
		return entryModeStreams
	case bytes.HasPrefix(head, statsHeadMatrix), bytes.HasPrefix(head, statsHeadVector):
		return entryModeSeries
	}
	return entryModeAny
}

// before is the byte k positions before b[i] in the whole answer (0 before
// its start).
func (c *entryCounter) before(b []byte, i, k int) byte {
	if j := i - k; j >= 0 {
		return b[j]
	} else if j += c.cn; j >= 0 {
		return c.carry[j]
	}
	return 0
}

func (c *entryCounter) add(b []byte) {
	if len(b) == 0 {
		return
	}
	if c.pending {
		if b[0] == '"' {
			c.n++
		}
		c.pending = false
	}
	if c.mode != entryModeSeries {
		for off := 0; off < len(b); {
			i := bytes.IndexByte(b[off:], '[')
			if i < 0 {
				break
			}
			i += off
			if prev := c.before(b, i, 1); prev == '[' || prev == ',' {
				if i+1 < len(b) {
					if b[i+1] == '"' {
						c.n++
					}
				} else {
					c.pending = true
				}
			}
			off = i + 1
		}
	}
	if c.mode != entryModeStreams {
		for off := 0; off < len(b); {
			i := bytes.IndexByte(b[off:], '{')
			if i < 0 {
				break
			}
			i += off
			match := true
			for k := 1; k <= len(seriesOpenPrefix); k++ {
				if c.before(b, i, k) != seriesOpenPrefix[len(seriesOpenPrefix)-k] {
					match = false
					break
				}
			}
			if match {
				c.n++
			}
			off = i + 1
		}
	}
	if len(b) >= len(c.carry) {
		c.cn = copy(c.carry[:], b[len(b)-len(c.carry):])
		return
	}
	// A short write: keep the last bytes of carry+b.
	keep := min(c.cn, len(c.carry)-len(b))
	copy(c.carry[:], c.carry[c.cn-keep:c.cn])
	c.cn = keep + copy(c.carry[keep:], b)
}

// appendStats renders the request's stats in Loki's shape and key order:
// summary, then cache. The sections Loki fills from its own components
// (querier, ingester, index) have no proxy counterpart and are left out:
// zero-filled they would show as measurements in Grafana's inspector.
func (w *queryStatsWriter) appendStats(dst []byte) []byte {
	rt := w.rt
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return appendQueryStats(dst, &rt.flow, time.Since(rt.start), w.entries.n)
}

// lokiCacheZeroSections are the cache sections of a request that did not use
// them, rendered once.
var lokiCacheZeroSections = func() (out [lokiCacheClassCount]string) {
	for i := range out {
		out[i] = `"` + lokiCacheClassNames[i] + `":{"entriesFound":0,"entriesRequested":0,"entriesStored":0,` +
			`"bytesReceived":0,"bytesSent":0,"requests":0,"downloadTime":0,"queryLengthServed":0}`
	}
	return out
}()

func appendQueryStats(b []byte, flow *flowCounters, exec time.Duration, entries int64) []byte {
	execSec := exec.Seconds()
	bytesProcessed, lines := flow.vlBytes, flow.vlRows
	var bytesPerSec, linesPerSec int64
	if execSec > 0 {
		bytesPerSec = int64(float64(bytesProcessed) / execSec)
		linesPerSec = int64(float64(lines) / execSec)
	}
	b = append(b, `{"summary":{"bytesProcessedPerSecond":`...)
	b = strconv.AppendInt(b, bytesPerSec, 10)
	b = append(b, `,"linesProcessedPerSecond":`...)
	b = strconv.AppendInt(b, linesPerSec, 10)
	b = append(b, `,"totalBytesProcessed":`...)
	b = strconv.AppendInt(b, bytesProcessed, 10)
	b = append(b, `,"totalLinesProcessed":`...)
	b = strconv.AppendInt(b, lines, 10)
	b = append(b, `,"execTime":`...)
	b = strconv.AppendFloat(b, execSec, 'f', -1, 64)
	b = append(b, `,"queueTime":`...)
	b = strconv.AppendFloat(b, flow.queueDur.Seconds(), 'f', -1, 64)
	b = append(b, `,"subqueries":0,"totalEntriesReturned":`...)
	b = strconv.AppendInt(b, entries, 10)
	b = append(b, `,"splits":`...)
	b = strconv.AppendInt(b, int64(flow.vlCalls), 10)
	b = append(b, `,"shards":0,"totalPostFilterLines":0,"totalStructuredMetadataBytesProcessed":0},"cache":{`...)
	for i, slot := range lokiCacheSlots {
		if i > 0 {
			b = append(b, ',')
		}
		if slot < 0 || flow.lokiCache[slot] == (flowLokiCache{}) {
			b = append(b, lokiCacheZeroSections[i]...)
			continue
		}
		c := &flow.lokiCache[slot]
		b = append(b, '"')
		b = append(b, lokiCacheClassNames[i]...)
		b = append(b, `":{"entriesFound":`...)
		b = strconv.AppendInt(b, int64(c.found), 10)
		b = append(b, `,"entriesRequested":`...)
		b = strconv.AppendInt(b, int64(c.requested), 10)
		b = append(b, `,"entriesStored":`...)
		b = strconv.AppendInt(b, int64(c.stored), 10)
		b = append(b, `,"bytesReceived":`...)
		b = strconv.AppendInt(b, c.received, 10)
		b = append(b, `,"bytesSent":`...)
		b = strconv.AppendInt(b, c.sent, 10)
		b = append(b, `,"requests":`...)
		b = strconv.AppendInt(b, int64(c.requested+c.stored), 10)
		b = append(b, `,"downloadTime":`...)
		b = strconv.AppendInt(b, int64(c.dur), 10)
		b = append(b, `,"queryLengthServed":0}`...)
	}
	return append(b, `}}`...)
}
