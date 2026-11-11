package proxy

import (
	"bytes"
	"context"
	"encoding/binary"
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

// queryStatsLayer fills data.stats of query, query_range, volume and
// volume_range answers: every answer Loki writes a stats object into.
func (p *Proxy) queryStatsLayer(endpoint string, next http.HandlerFunc) http.HandlerFunc {
	var kind uint8
	switch endpoint {
	case "query":
		kind = statsRouteInstant
	case "query_range":
		kind = statsRouteRange
	case "volume", "volume_range":
		kind = statsRouteVolume
	default:
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
		sw.route = kind
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

// The routes the stats layer fills, each with Loki's accounting for it.
const (
	statsRouteRange   = iota // query_range: log or metric answers
	statsRouteInstant        // query
	statsRouteVolume         // volume, volume_range: Loki reports no processed lines or bytes
)

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
	route       uint8
	wroteHeader bool
	patch       bool
	// done: the answer was finished by the response cache (serveCachedAnswer).
	done         bool
	entriesKnown bool
	// hit: a response-cache hit (query_stats_cache.go): replayed holds the
	// stored computation's totals, queryLength the window it served (ns).
	hit          bool
	replayed     statsReplay
	queryLength  int64
	headN, tailN int
	total        int                      // bytes the handler wrote
	head         [queryStatsTailHold]byte // the first bytes, to check the envelope's start
	tail         [queryStatsTailHold]byte // the held-back end of the body
	entries      entryCounter
	buf          [4608]byte // the stats object, and the gzip block header and trailer
}

func (w *queryStatsWriter) reset(rw http.ResponseWriter, rt *requestTelemetry) {
	w.ResponseWriter, w.rt = rw, rt
	w.route = statsRouteRange
	w.wroteHeader, w.patch, w.done, w.entriesKnown = false, false, false, false
	w.hit, w.replayed, w.queryLength = false, statsReplay{}, 0
	w.headN, w.tailN, w.total = 0, 0, 0
	w.entries = entryCounter{}
}

func (w *queryStatsWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *queryStatsWriter) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}
	if w.decide(code) {
		h := w.Header()
		h.Set("Content-Type", "application/json")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Del("Content-Length")
	}
	w.ResponseWriter.WriteHeader(code)
}

// decide fixes, before the headers go out, whether the answer is filled: a
// JSON success answer (application/json, the type every query answer is
// written with) the handler did not encode. Its callers set the type again
// as the constant it is, with nosniff, so a filled answer is never read as
// anything but JSON (set where the body is written, which is where code
// scanning looks for it).
func (w *queryStatsWriter) decide(code int) bool {
	w.wroteHeader = true
	h := w.Header()
	w.patch = code == http.StatusOK && h.Get("Content-Encoding") == "" &&
		h.Get("Content-Type") == "application/json"
	return w.patch
}

func (w *queryStatsWriter) Write(b []byte) (int, error) {
	// The implicit 200 stays implicit below.
	if !w.wroteHeader && w.decide(http.StatusOK) {
		h := w.Header()
		h.Set("Content-Type", "application/json")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Del("Content-Length")
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
	case bytes.HasPrefix(head, statsHeadStatusFirst) && bytes.HasSuffix(body, statsTailEmpty) &&
		keyStartsAt(body, len(body)-len(statsTailEmpty)):
		s.pre, s.post = body[:len(body)-4], statsCloseData
	case bytes.HasPrefix(head, statsHeadStatusFirst) && bytes.HasSuffix(body, statsTailResultEnd) &&
		!bytes.Contains(body, statsKey):
		s.pre, s.insert, s.post = body[:len(body)-2], statsInsertKey, statsCloseData
	case bytes.HasPrefix(head, statsHeadDataFirst) && bytes.HasSuffix(body, statsTailSorted) &&
		keyStartsAt(body, len(body)-len(statsTailSorted)):
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

// keyStartsAt reports whether the quote at body[i] opens an object key: in
// compact JSON a key's quote follows `{` or `,`, while the same bytes inside a
// string (a key or value holding `\"stats":{}`) follow a backslash or other
// text.
func keyStartsAt(body []byte, i int) bool {
	return i > 0 && (body[i-1] == ',' || body[i-1] == '{')
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
//
// For a log entry it also sums the bytes of the entry's line (its second
// element, unescaped: an escape sequence counts as one byte), the measure
// Loki's totalBytesProcessed counts: the timestamp string is skipped to its
// closing quote and the line string read to its unescaped closing quote,
// carried across writes.
type entryCounter struct {
	n         int64
	lineBytes int64
	carry     [9]byte
	cn        int
	pending   bool // the last byte was an entry's `[`, its quote not seen yet
	mode      uint8
	ls        uint8 // where a log entry's scan is (lineScan*)
	esc       bool  // inside the line: the last byte was an escaping backslash
	hexLeft   uint8 // inside the line: hex digits of a \u escape still to skip
}

const (
	entryModeUnknown = iota // both sequences
	entryModeAny            // both, for good (the start names no type)
	entryModeStreams
	entryModeSeries
)

// The scan of one log entry: its timestamp string, the comma, the line's
// opening quote, the line string.
const (
	lineScanIdle = iota
	lineScanTimestamp
	lineScanComma
	lineScanQuote
	lineScanLine
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
	if c.mode != entryModeSeries {
		c.addLogEntries(b)
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

// addLogEntries counts the log entries of b and the bytes of their lines.
func (c *entryCounter) addLogEntries(b []byte) {
	off, bs := 0, -1 // bs: the write's next backslash at or after the last line read (entryInWrite)
	if c.pending {
		c.pending = false
		if b[0] == '"' {
			c.n++
			c.ls, off = lineScanTimestamp, 1
		}
	}
	for off < len(b) {
		switch c.ls {
		case lineScanTimestamp:
			i := bytes.IndexByte(b[off:], '"')
			if i < 0 {
				return
			}
			c.ls, off = lineScanComma, off+i+1
		case lineScanComma, lineScanQuote:
			want := byte(',')
			if c.ls == lineScanQuote {
				want = '"'
			}
			if b[off] != want {
				c.ls = lineScanIdle
				continue
			}
			c.ls++
			off++
		case lineScanLine:
			off = c.scanLine(b, off)
		default:
			i := bytes.IndexByte(b[off:], '[')
			if i < 0 {
				return
			}
			i += off
			off = i + 1
			if prev := c.before(b, i, 1); prev != '[' && prev != ',' {
				continue
			}
			if off == len(b) {
				c.pending = true
				return
			}
			if b[off] == '"' {
				c.n++
				off = c.entryInWrite(b, off+1, &bs)
			}
		}
	}
}

// entryInWrite reads a log entry whose timestamp starts at b[off] when the
// timestamp and the whole line lie in this write and the line holds no
// escape (the common case), and then follows it to the next entry of the
// same stream when that one opens right after it. Any other case is left to
// the state machine (c.ls set where it is). The next backslash of the write
// is looked up once and reused (*bs), so a write without escapes costs one
// IndexByte per entry, as counting entries alone did.
func (c *entryCounter) entryInWrite(b []byte, off int, bs *int) int {
	for {
		ts := off
		for ts+8 <= len(b) && allDigits8(binary.LittleEndian.Uint64(b[ts:])) {
			ts += 8
		}
		for ts < len(b) && b[ts] >= '0' && b[ts] <= '9' {
			ts++
		}
		if ts == len(b) || b[ts] != '"' {
			// The timestamp goes on past this write, or is not a digit run:
			// the state machine reads it.
			c.ls = lineScanTimestamp
			return ts
		}
		line := ts + 3 // past `","`
		if line > len(b) || b[ts+1] != ',' || b[ts+2] != '"' {
			c.ls = lineScanComma
			return ts + 1
		}
		end := bytes.IndexByte(b[line:], '"')
		if end < 0 {
			c.ls = lineScanLine
			return line
		}
		end += line
		if *bs < line {
			if i := bytes.IndexByte(b[line:], '\\'); i < 0 {
				*bs = len(b)
			} else {
				*bs = line + i
			}
		}
		if *bs < end { // an escape in the line
			c.ls = lineScanLine
			return line
		}
		c.lineBytes += int64(end - line)
		off = end + 1
		// `"],["` opens the next entry of the same stream.
		if off+3 >= len(b) || b[off] != ']' || b[off+1] != ',' || b[off+2] != '[' || b[off+3] != '"' {
			c.ls = lineScanIdle
			return off
		}
		c.n++
		off += 4
	}
}

// allDigits8 reports whether the 8 bytes of v are all ASCII digits: each
// byte's high nibble is 3, and stays 3 when 6 is added (0x3A-0x3F become
// 0x40-0x45); no addition carries into the next byte.
func allDigits8(v uint64) bool {
	const high, six = 0xF0F0F0F0F0F0F0F0, 0x0606060606060606
	return v&high == 0x3030303030303030 && (v+six)&high == 0x3030303030303030
}

// scanLine reads the line string from b[off:] to its closing quote or b's
// end, adding its unescaped length, and returns where the scan stopped.
func (c *entryCounter) scanLine(b []byte, off int) int {
	for off < len(b) {
		i := bytes.IndexByte(b[off:], '"')
		seg := b[off:]
		if i >= 0 {
			seg = b[off : off+i]
		}
		c.addLineSegment(seg)
		if i < 0 {
			return len(b)
		}
		off += i + 1
		if c.esc { // an escaped quote: one byte of the line
			c.esc = false
			c.lineBytes++
			continue
		}
		c.ls = lineScanIdle
		return off
	}
	return off
}

// addLineSegment adds the unescaped length of a line segment that holds no
// quote: a byte, or an escape sequence (`\x`, `\uXXXX`) as one byte.
func (c *entryCounter) addLineSegment(seg []byte) {
	if !c.esc && c.hexLeft == 0 && bytes.IndexByte(seg, '\\') < 0 {
		c.lineBytes += int64(len(seg))
		return
	}
	for _, ch := range seg {
		switch {
		case c.hexLeft > 0:
			c.hexLeft--
		case c.esc:
			c.esc = false
			c.lineBytes++
			if ch == 'u' {
				c.hexLeft = 4
			}
		case ch == '\\':
			c.esc = true
		default:
			c.lineBytes++
		}
	}
}

// appendStats renders the request's stats in Loki's shape and key order:
// summary, querier, ingester, cache, index. The querier, ingester and index
// sections measure Loki's own components; the proxy has none of them, so it
// writes them as Loki does for a component that did no work (all zero, as
// Loki's ingester section reads for a window outside its ingesters and its
// querier section for a results-cache hit). Grafana then lists the same
// "Ingester:" stats for the proxy as for Loki.
func (w *queryStatsWriter) appendStats(dst []byte) []byte {
	rt := w.rt
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return appendQueryStats(dst, &rt.flow, w.report(&rt.statsTotals), time.Since(rt.start))
}

// statsReplay is what a response-cache hit replays of the computation that
// stored the answer (query_stats_cache.go): its processed totals and time.
type statsReplay struct {
	bytes, lines, postFilter int64
	exec                     time.Duration
}

// statsReport is everything appendQueryStats renders besides the flow.
type statsReport struct {
	summary     statsSummary
	entries     int64
	queryLength int64 // ns of the window a result-cache hit served
}

// statsSummary is the summary's processed totals and the time the rates are
// taken over.
type statsSummary struct {
	bytes, lines, postFilter int64
	rateOver                 time.Duration // 0: this request's execTime
}

// report gives the summary its Loki meaning (the query-stats-loki-accounting
// decision of the query-stats-envelope registry item):
//
//   - volume answers: no processed lines or bytes, as Loki's.
//   - a response-cache hit of an answer Loki computes on every request (a
//     non-empty log answer, an instant answer) replays the stored
//     computation's totals and rates; any other hit reports no processing,
//     as Loki's results cache does.
//   - a log answer computed now: the Loki-meaning totals when the proxy
//     derived them (query_stats_totals.go), else what it read: the rows
//     VictoriaLogs returned and the bytes of the lines it returned. When the
//     window's lines are known and its bytes are not, the bytes are the
//     window's lines times the returned lines' mean size.
//   - a metric answer: only derived totals. VictoriaLogs returns aggregated
//     rows, whose count and size mean nothing in Loki's terms.
//
// A line count is never below the post-filter count it contains.
func (w *queryStatsWriter) report(totals *statsTotals) statsReport {
	rep := statsReport{entries: w.entries.n, queryLength: w.queryLength}
	logs := w.entries.mode == entryModeStreams
	switch {
	case w.route == statsRouteVolume:
	case w.hit && w.replayed.exec > 0:
		r := w.replayed
		rep.summary = statsSummary{bytes: r.bytes, lines: r.lines, postFilter: r.postFilter, rateOver: r.exec}
	case w.hit:
	default:
		rep.summary = processedSummary(&w.rt.flow, totals, logs, w.entries.n, w.entries.lineBytes)
	}
	return rep
}

func processedSummary(flow *flowCounters, totals *statsTotals, logs bool, entries, lineBytes int64) statsSummary {
	var s statsSummary
	if logs {
		// The rows VictoriaLogs returned, or the entries returned when the
		// answer came from the proxy's own window caches.
		read := max(flow.vlRows, entries)
		s.lines, s.postFilter, s.bytes = read, read, lineBytes
	}
	if totals.hasLines {
		s.lines = totals.lines
		if logs && !totals.hasBytes && entries > 0 {
			s.bytes = int64(float64(lineBytes) / float64(entries) * float64(totals.lines))
		}
	}
	if totals.hasPostFilter {
		s.postFilter = totals.postFilter
	}
	if totals.hasBytes {
		s.bytes = totals.bytes
	}
	s.lines = max(s.lines, s.postFilter)
	return s
}

// The zero sections of Loki's components, in Loki's key order and zero form
// (recorded in testdata/loki_query_stats_object.json: Loki omits the
// ingester's recvWaitTime when it is zero).
const (
	lokiStoreZero = `"store":{"totalChunksRef":0,"totalChunksDownloaded":0,"chunksDownloadTime":0,` +
		`"queryReferencedStructuredMetadata":false,"queryUsedV2Engine":false,"chunk":{"headChunkBytes":0,` +
		`"headChunkLines":0,"decompressedBytes":0,"decompressedLines":0,"compressedBytes":0,"totalDuplicates":0,` +
		`"postFilterLines":0,"headChunkStructuredMetadataBytes":0,"decompressedStructuredMetadataBytes":0},` +
		`"chunkRefsFetchTime":0,"congestionControlLatency":0,"pipelineWrapperFilteredLines":0,"dataobj":{` +
		`"prePredicateDecompressedRows":0,"prePredicateDecompressedBytes":0,` +
		`"prePredicateDecompressedStructuredMetadataBytes":0,"postPredicateRows":0,"postPredicateDecompressedBytes":0,` +
		`"postPredicateStructuredMetadataBytes":0,"postFilterRows":0,"pagesScanned":0,"pagesDownloaded":0,` +
		`"pagesDownloadedBytes":0,"pageBatches":0,"totalRowsAvailable":0,"totalPageDownloadTime":0}}`
	lokiQuerierIngesterZero = `"querier":{` + lokiStoreZero + `,"querierExecTime":0},"ingester":{"totalReached":0,` +
		`"totalChunksMatched":0,"totalBatches":0,"totalLinesSent":0,` + lokiStoreZero + `}`
	lokiIndexZero = `"index":{"totalChunks":0,"postFilterChunks":0,"shardsDuration":0,"usedBloomFilters":false,` +
		`"totalStreams":0,"chunkRefsLookupTime":0,"bloomFilterTime":0}`
)

// lokiCacheZeroSections are the cache sections of a request that did not use
// them, rendered once.
var lokiCacheZeroSections = func() (out [lokiCacheClassCount]string) {
	for i := range out {
		out[i] = `"` + lokiCacheClassNames[i] + `":{"entriesFound":0,"entriesRequested":0,"entriesStored":0,` +
			`"bytesReceived":0,"bytesSent":0,"requests":0,"downloadTime":0,"queryLengthServed":0}`
	}
	return out
}()

func appendQueryStats(b []byte, flow *flowCounters, rep statsReport, exec time.Duration) []byte {
	execSec := exec.Seconds()
	s := rep.summary
	rateSec := execSec
	if s.rateOver > 0 {
		rateSec = s.rateOver.Seconds()
	}
	var bytesPerSec, linesPerSec int64
	if rateSec > 0 {
		bytesPerSec = int64(float64(s.bytes) / rateSec)
		linesPerSec = int64(float64(s.lines) / rateSec)
	}
	b = append(b, `{"summary":{"bytesProcessedPerSecond":`...)
	b = strconv.AppendInt(b, bytesPerSec, 10)
	b = append(b, `,"linesProcessedPerSecond":`...)
	b = strconv.AppendInt(b, linesPerSec, 10)
	b = append(b, `,"totalBytesProcessed":`...)
	b = strconv.AppendInt(b, s.bytes, 10)
	b = append(b, `,"totalLinesProcessed":`...)
	b = strconv.AppendInt(b, s.lines, 10)
	b = append(b, `,"execTime":`...)
	b = strconv.AppendFloat(b, execSec, 'f', -1, 64)
	b = append(b, `,"queueTime":`...)
	b = strconv.AppendFloat(b, flow.queueDur.Seconds(), 'f', -1, 64)
	b = append(b, `,"subqueries":0,"totalEntriesReturned":`...)
	b = strconv.AppendInt(b, rep.entries, 10)
	b = append(b, `,"splits":`...)
	b = strconv.AppendInt(b, int64(flow.vlCalls), 10)
	b = append(b, `,"shards":0,"totalPostFilterLines":`...)
	b = strconv.AppendInt(b, s.postFilter, 10)
	b = append(b, `,"totalStructuredMetadataBytesProcessed":0},`...)
	b = append(b, lokiQuerierIngesterZero...)
	b = append(b, `,"cache":{`...)
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
		b = append(b, `,"queryLengthServed":`...)
		var served int64
		if c.found > 0 && i == lokiCacheResult {
			served = rep.queryLength
		}
		b = strconv.AppendInt(b, served, 10)
		b = append(b, '}')
	}
	b = append(b, `},`...)
	b = append(b, lokiIndexZero...)
	return append(b, '}')
}
