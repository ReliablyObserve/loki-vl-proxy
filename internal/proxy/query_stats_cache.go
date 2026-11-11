package proxy

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"net/http"
	"strings"
	"time"

	mw "github.com/ReliablyObserve/Loki-VL-proxy/internal/middleware"
)

// A response-cache hit of a query or query_range answer is finished here with
// this request's stats, without reading or recompressing the cached body:
//
//   - The cached value carries the answer's entry count after the body, and
//     for an answer Loki computes on every request (a non-empty log answer, an
//     instant answer: Loki's results cache keeps neither by default) the
//     computation's processed totals and time, which the hit replays (a short
//     trailer the answer never shows; query and query_range compat keys carry
//     queryStatsCompatFence, so no code that does not know the trailer reads
//     such a value, on this replica or a peer).
//   - For a gzip client, a sidecar entry next to the body (local only, with
//     the body's remaining TTL) holds the gzip encoding of the body up to its
//     last bytes (sync-flushed, unfinished), its CRC-32 and those last bytes.
//     A hit writes that encoding, one final stored deflate block with the last
//     bytes and the stats object, and the gzip trailer: one valid gzip member.
//     The sidecar is captured from the miss's own compressed answer (the
//     compressor is flushed where the stats begin, and the CRC of the part
//     before them is unwound from the member's trailer), so the encoding is
//     made once, as the stored encoded copies were; a body from a peer gets its
//     sidecar on its first local gzip hit.
//
// A sidecar is used only for the body it was built from (same length and the
// same last bytes) and is dropped when the body is stored again.

const (
	// queryStatsCompatFence versions query and query_range compat keys whose
	// values end with the stats trailer.
	queryStatsCompatFence  = "qs2:"
	statsSidecarGzipSuffix = "#qs1gz"
	statsSidecarMagic      = "qs1"
	statsTrailerMagic      = "\x00qs2"
)

func statsSidecarKey(cacheKey string) string { return cacheKey + statsSidecarGzipSuffix }

func isStatsSidecarKey(key string) bool { return strings.HasSuffix(key, statsSidecarGzipSuffix) }

// statsTrailer is what a cached query answer carries for its hits: the
// answer's entry count and, when hits replay it, the stored computation.
type statsTrailer struct {
	entries  int64
	logs     bool // a log (streams) answer
	replay   bool
	replayed statsReplay
}

// The trailer's kind byte: bit 0 set when hits replay the computation that
// follows, bit 1 set for a log answer.
const (
	statsTrailerReplay = 1 << 0
	statsTrailerLogs   = 1 << 1
)

// appendStatsTrailer appends the stats trailer to a cached body: its fields
// as uvarints, their length, and a magic a JSON body cannot end with.
func appendStatsTrailer(dst []byte, t statsTrailer) []byte {
	n := len(dst)
	dst = binary.AppendUvarint(dst, uint64(max(t.entries, 0)))
	var kind byte
	if t.logs {
		kind |= statsTrailerLogs
	}
	if t.replay {
		kind |= statsTrailerReplay
	}
	dst = append(dst, kind)
	if t.replay {
		r := t.replayed
		for _, v := range [...]int64{r.bytes, r.lines, r.postFilter, int64(r.exec), r.splits, r.shards, int64(r.querierExec)} {
			dst = binary.AppendUvarint(dst, uint64(max(v, 0)))
		}
	}
	dst = append(dst, byte(len(dst)-n))
	return append(dst, statsTrailerMagic...)
}

// splitStatsTrailer returns a cached value's body and, when it has the
// trailer, the trailer.
func splitStatsTrailer(value []byte) (body []byte, t statsTrailer, ok bool) {
	if !bytes.HasSuffix(value, []byte(statsTrailerMagic)) || len(value) < len(statsTrailerMagic)+3 {
		return value, statsTrailer{}, false
	}
	end := len(value) - len(statsTrailerMagic) - 1
	n := int(value[end])
	if n < 2 || n > end {
		return value, statsTrailer{}, false
	}
	raw := value[end-n : end]
	entries, m := binary.Uvarint(raw)
	if m <= 0 || len(raw) == m {
		return value, statsTrailer{}, false
	}
	kind := value[end-n+m]
	t.entries, raw = int64(entries), raw[m+1:]
	if kind&^(statsTrailerReplay|statsTrailerLogs) != 0 {
		return value, statsTrailer{}, false
	}
	t.logs = kind&statsTrailerLogs != 0
	if kind&statsTrailerReplay != 0 {
		var fields [7]int64
		for i := range fields {
			v, m := binary.Uvarint(raw)
			if m <= 0 {
				return value, statsTrailer{}, false
			}
			fields[i], raw = int64(v), raw[m:]
		}
		t.replay = true
		t.replayed = statsReplay{bytes: fields[0], lines: fields[1], postFilter: fields[2], exec: time.Duration(fields[3]),
			splits: fields[4], shards: fields[5], querierExec: time.Duration(fields[6])}
	}
	if len(raw) != 0 {
		return value, statsTrailer{}, false
	}
	return value[:end-n], t, true
}

// countEntries counts a body's entries as the stats layer does when it
// writes it.
func countEntries(body []byte) int64 {
	c := entryCounter{mode: entryModeOf(body[:min(len(body), queryStatsTailHold)])}
	c.add(body)
	return c.n
}

type statsSidecar struct {
	bodyLen  int
	crc      uint32 // CRC-32 of the body up to its tail
	tail     []byte
	gzPrefix []byte // the gzip encoding of the body up to its tail, sync-flushed
}

func buildStatsSidecar(body []byte) (statsSidecar, error) {
	n := min(len(body), queryStatsTailHold)
	prefix := body[:len(body)-n]
	encoded, err := mw.EncodeGzipPrefix(prefix)
	if err != nil {
		return statsSidecar{}, err
	}
	return statsSidecar{bodyLen: len(body), crc: crc32.ChecksumIEEE(prefix), tail: body[len(body)-n:], gzPrefix: encoded}, nil
}

func (sc statsSidecar) encode() []byte {
	out := make([]byte, 0, len(statsSidecarMagic)+binary.MaxVarintLen64+4+1+len(sc.tail)+len(sc.gzPrefix))
	out = append(out, statsSidecarMagic...)
	out = binary.AppendUvarint(out, uint64(sc.bodyLen))
	out = binary.LittleEndian.AppendUint32(out, sc.crc)
	out = append(out, byte(len(sc.tail)))
	out = append(out, sc.tail...)
	return append(out, sc.gzPrefix...)
}

// decodeStatsSidecar reads an encoded sidecar; its slices alias raw.
func decodeStatsSidecar(raw []byte) (statsSidecar, bool) {
	if !bytes.HasPrefix(raw, []byte(statsSidecarMagic)) {
		return statsSidecar{}, false
	}
	rest := raw[len(statsSidecarMagic):]
	bodyLen, n := binary.Uvarint(rest)
	if n <= 0 || len(rest[n:]) < 5 {
		return statsSidecar{}, false
	}
	rest = rest[n:]
	sc := statsSidecar{bodyLen: int(bodyLen), crc: binary.LittleEndian.Uint32(rest)}
	rest = rest[4:]
	tailN := int(rest[0])
	if len(rest[1:]) < tailN {
		return statsSidecar{}, false
	}
	sc.tail, sc.gzPrefix = rest[1:1+tailN], rest[1+tailN:]
	return sc, len(sc.gzPrefix) > 0
}

// matches reports whether sc was built from body.
func (sc statsSidecar) matches(body []byte) bool {
	return sc.bodyLen == len(body) && len(sc.tail) == min(len(body), queryStatsTailHold) &&
		bytes.Equal(sc.tail, body[len(body)-len(sc.tail):])
}

// gzipStatsSidecarFor returns the gzip sidecar of a cached body, building and
// storing it when there is none for this body.
func (p *Proxy) gzipStatsSidecarFor(r *http.Request, cacheKey string, body []byte, ttl time.Duration) (statsSidecar, bool) {
	key := statsSidecarKey(cacheKey)
	if raw, ok := p.cachePeek(r.Context(), p.compatCache, key); ok {
		if sc, ok := decodeStatsSidecar(raw); ok && sc.matches(body) {
			return sc, true
		}
	}
	sc, err := buildStatsSidecar(body)
	if err != nil {
		return statsSidecar{}, false
	}
	if ttl > 0 {
		p.compatCache.SetLocalOnlyWithTTL(key, sc.encode(), ttl)
	}
	return sc, true
}

// statsWriterFor returns the stats layer's writer when w is it.
func statsWriterFor(w http.ResponseWriter, r *http.Request) *queryStatsWriter {
	rt := getRequestTelemetry(r.Context())
	if rt == nil {
		return nil
	}
	rt.mu.Lock()
	sw := rt.statsWriter
	rt.mu.Unlock()
	if sw == nil || w != http.ResponseWriter(sw) {
		return nil
	}
	return sw
}

// serveCompatHitWithStats writes a cached query or query_range answer (value
// as stored, trailer included) with this request's stats. It reports false
// when the request has no stats layer; the caller then serves the body.
func (p *Proxy) serveCompatHitWithStats(w http.ResponseWriter, r *http.Request, cacheKey string, value []byte, ttl time.Duration) bool {
	sw := statsWriterFor(w, r)
	if sw == nil || sw.wroteHeader {
		return false
	}
	body, trailer, ok := splitStatsTrailer(value)
	if !ok {
		trailer.entries = countEntries(body)
	}
	sw.entries.n, sw.entriesKnown = trailer.entries, true
	sw.entries.mode = entryModeOf(body[:min(len(body), queryStatsTailHold)])
	sw.hit = true
	logs := trailer.logs || sw.entries.mode == entryModeStreams
	if trailer.replay {
		sw.replayed = trailer.replayed
	} else if sw.route == statsRouteRange && !logs {
		// A metric results-cache hit serves its window (Loki's
		// queryLengthServed); Loki's empty log answers report none.
		sw.queryLength = requestWindowLength(r)
	}
	w.Header().Set("Content-Type", "application/json")
	// A body the client compression threshold leaves uncompressed is
	// served as before, with its stats added uncompressed.
	if encoding, minBytes := p.compatCacheResponseEncoding(r); encoding == "gzip" && len(body) >= minBytes {
		if sc, ok := p.gzipStatsSidecarFor(r, cacheKey, body, ttl); ok && sw.writeGzipWithStats(sc, body) {
			return true
		}
	}
	_, _ = sw.Write(body)
	return true
}

// writeGzipWithStats finishes the gzip member sc holds: the stored encoding
// of the body up to its tail, then one final stored block with the tail cut
// for the stats object (statsSplice), the stats and the rest of the tail,
// then the trailer. False when the tail has no place for stats or the block
// would be too long; nothing is written then.
func (w *queryStatsWriter) writeGzipWithStats(sc statsSidecar, body []byte) bool {
	n := min(len(body), queryStatsTailHold)
	s, ok := statsSplice(body[:n], sc.tail)
	if !ok {
		return false
	}
	// [stored block header][the answer's end with its stats][trailer], so the
	// hit writes the stored encoding and then this, in two writes.
	const blockHeader = 5
	out := w.appendStatsEnd(w.buf[:blockHeader], s)
	size := len(out) - blockHeader
	if size > 0xffff {
		return false
	}
	mw.AppendGzipStoredBlock(out[:0], size)
	crc := crc32.Update(sc.crc, crc32.IEEETable, out[blockHeader:])
	out = mw.AppendGzipTrailer(out, crc, uint32(sc.bodyLen-len(sc.tail)+size))
	h := w.Header()
	h.Set("Content-Encoding", "gzip")
	h.Set("Vary", "Accept-Encoding")
	w.wroteHeader, w.patch, w.done = true, false, true
	if _, err := w.ResponseWriter.Write(sc.gzPrefix); err == nil {
		_, _ = w.ResponseWriter.Write(out)
	}
	return true
}

// captureGzipStatsSidecar asks the compressor of a gzip miss to keep its
// encoding up to the stats (the stats layer marks it), so the miss's own
// compression becomes the hits' sidecar.
func (p *Proxy) captureGzipStatsSidecar(w http.ResponseWriter, r *http.Request, cacheKey string, body func() ([]byte, time.Duration)) {
	sw := statsWriterFor(w, r)
	if sw == nil {
		return
	}
	if encoding, _ := p.compatCacheResponseEncoding(r); encoding != "gzip" {
		return
	}
	rt := sw.rt
	if !mw.RegisterEncodedResponsePrefixCapture(w, "gzip", func(_ string, encoded []byte, mark int) {
		rt.mu.Lock()
		suffix := rt.statsSuffix
		rt.mu.Unlock()
		cached, ttl := body()
		if mark <= 0 || len(cached) == 0 || ttl <= 0 || len(encoded) < mark+8 {
			return
		}
		crc := binary.LittleEndian.Uint32(encoded[len(encoded)-8:])
		size := binary.LittleEndian.Uint32(encoded[len(encoded)-4:])
		prefixLen := int(size) - len(suffix)
		tailN := len(cached) - prefixLen
		if prefixLen < 0 || tailN < 0 || tailN > queryStatsTailHold {
			return
		}
		sc := statsSidecar{bodyLen: len(cached), crc: crc32Unappend(crc, suffix), tail: cached[prefixLen:], gzPrefix: encoded[:mark]}
		p.compatCache.SetLocalOnlyWithTTL(statsSidecarKey(cacheKey), sc.encode(), ttl)
	}) {
		return
	}
	rt.mu.Lock()
	rt.statsPrefixCapture = true
	rt.mu.Unlock()
}

// crc32InverseIndex maps the top byte of an IEEE table entry to its index:
// the 256 top bytes are distinct, which makes one CRC step reversible.
var crc32InverseIndex = func() (inv [256]byte) {
	for i, v := range crc32.IEEETable {
		inv[v>>24] = byte(i)
	}
	return inv
}()

// crc32Unappend returns the IEEE CRC-32 of a when crc is the CRC-32 of a
// followed by suffix: the CRC steps of suffix undone from the last byte.
func crc32Unappend(crc uint32, suffix []byte) uint32 {
	x := ^crc
	for i := len(suffix) - 1; i >= 0; i-- {
		idx := crc32InverseIndex[x>>24]
		x = ((x ^ crc32.IEEETable[idx]) << 8) | uint32(idx^suffix[i])
	}
	return ^x
}

// compatCacheValue is what the response cache stores for an answer: a copy
// of the body, and for a query or query_range answer the stats layer
// counted, the stats trailer. Storing a body again drops the gzip sidecar of
// the one before.
func (p *Proxy) compatCacheValue(w http.ResponseWriter, r *http.Request, cacheKey string, body []byte) []byte {
	sw := statsWriterFor(w, r)
	if sw == nil || sw.route == statsRouteVolume {
		if sw != nil {
			p.compatCache.Invalidate(statsSidecarKey(cacheKey))
		}
		return append([]byte(nil), body...)
	}
	p.compatCache.Invalidate(statsSidecarKey(cacheKey))
	value := make([]byte, len(body), len(body)+statsTrailerMaxLen)
	copy(value, body)
	return appendStatsTrailer(value, sw.trailer())
}

// statsTrailerMaxLen bounds a trailer: eight uvarints, the kind, the length
// and the magic.
const statsTrailerMaxLen = 8*binary.MaxVarintLen64 + 2 + len(statsTrailerMagic)

// trailer is the stats trailer of the answer this writer counted: hits of
// an answer Loki computes on every request (a non-empty log answer; an
// instant answer) replay its totals and time.
func (w *queryStatsWriter) trailer() statsTrailer {
	t := statsTrailer{entries: w.entries.n}
	logs := w.isLogAnswer()
	t.logs = logs
	if w.route != statsRouteInstant && (!logs || w.entries.n == 0) {
		return t
	}
	rt := w.rt
	rt.mu.Lock()
	rep := w.report(&rt.statsTotals)
	exec := time.Since(rt.start)
	rt.mu.Unlock()
	s := rep.summary
	t.replay = true
	t.replayed = statsReplay{bytes: s.bytes, lines: s.lines, postFilter: s.postFilter, exec: max(exec, time.Nanosecond),
		splits: rep.splits, shards: rep.shards, querierExec: rep.querierExec}
	return t
}

// requestWindowLength is a query_range request's window (ns), 0 when it
// cannot be read.
func requestWindowLength(r *http.Request) int64 {
	start, okS := parseLokiTimeToUnixNano(r.FormValue("start"))
	end, okE := parseLokiTimeToUnixNano(r.FormValue("end"))
	if !okS || !okE || end < start {
		return 0
	}
	return end - start
}

// compatHitBody is the body of a cached value served without the stats
// layer: a query or query_range value without its entry-count trailer.
func compatHitBody(endpoint string, value []byte) []byte {
	if endpoint == "query" || endpoint == "query_range" {
		body, _, _ := splitStatsTrailer(value)
		return body
	}
	return value
}
