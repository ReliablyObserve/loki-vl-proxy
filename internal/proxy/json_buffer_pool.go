package proxy

import (
	"bytes"

	fj "github.com/valyala/fastjson"
)

// Parser arenas are expensive to reconstruct even for common ~120 KiB metric
// responses. Keep those reusable while discarding state from multi-MiB inputs.
// This is an input-size cutoff, not a bound on arena bytes or process memory.
const maxPooledStatsParserInputSize = 1024 * 1024

// resetJSONBufferForReuse keeps small response buffers reusable, but drops
// oversized backing storage. Reset alone retains the allocation, which can
// otherwise keep large query responses alive.
func resetJSONBufferForReuse(buf *bytes.Buffer) {
	if buf.Cap() > maxPooledBufSize {
		*buf = bytes.Buffer{}
	} else {
		buf.Reset()
	}
}

func releaseJSONBuffer(buf *bytes.Buffer) {
	resetJSONBufferForReuse(buf)
	jsonBufPool.Put(buf)
}

// resetFJMarshalScratchForReuse applies the retention bound to fastjson scratch.
// This does not limit response sizes or in-flight serialization allocations.
func resetFJMarshalScratchForReuse(scratch *[]byte) {
	if cap(*scratch) > maxPooledBufSize {
		*scratch = nil
	} else {
		*scratch = (*scratch)[:0]
	}
}

func releaseFJMarshalScratch(scratch *[]byte) {
	resetFJMarshalScratchForReuse(scratch)
	fjMarshalPool.Put(scratch)
}

// resetFJParserForReuse drops copied input and cached JSON values after large
// parses, including failures and no-op transformations. Parser's zero state is
// ready for use, as in fastjson.ParserPool.Get. The bound is on input size, not
// on the total bytes of the parser's internal value arena.
func resetFJParserForReuse(parser *fj.Parser, inputSize int) {
	if inputSize > maxPooledStatsParserInputSize {
		*parser = fj.Parser{}
	}
}

func releaseFJParser(pool *fj.ParserPool, parser *fj.Parser, inputSize int) {
	resetFJParserForReuse(parser, inputSize)
	pool.Put(parser)
}
