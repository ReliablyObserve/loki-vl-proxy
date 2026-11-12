package proxy

import (
	"context"
	"net/http"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// unsupportedBinaryMarkerMessage is the answer to a translated query that is a binary expression the handler cannot
// evaluate: its text is never sent to VictoriaLogs, which would read the marker as part of a query.
const unsupportedBinaryMarkerMessage = "unsupported metric query: range aggregations require compatible unwrap or translator support"

// binaryOfStats reads a translated query as a binary expression when it is no stats query a handler can take: it
// has a stats pipe (isStats) but no stats spec could be read from it (stats.Shaped is false, as the stats readers
// find no function in `stddev(x)|||2`). Only then is the text decoded (translator.DecodePhysical), so a normal
// metric request, whose stats spec reads, does not pay for it. The binary expression is what the translator writes
// for a metric whose client query is no binary expression (stdvar_over_time); it is never a stats query.
func binaryOfStats(stats physical.VLStats, isStats bool) (physical.Binary, bool) {
	if !isStats || stats.Shaped {
		return physical.Binary{}, false
	}
	decoded, err := translator.DecodePhysical(stats.Text)
	if err != nil {
		return physical.Binary{}, false
	}
	bin, ok := decoded.(physical.Binary)
	return bin, ok
}

// rejectBinaryMarker answers a binary expression that reached a stats handler and reports whether it did. The route
// never lets one through (scalarStatsBinaryOf, the binary routes); this stops one from being sent to VictoriaLogs as a
// query, where the marker is read as part of the text and the answer is the backend's parse error.
func (p *Proxy) rejectBinaryMarker(w http.ResponseWriter, stats physical.VLStats) bool {
	if _, isBinary := binaryOfStats(stats, true); !isBinary {
		return false
	}
	p.writeError(w, http.StatusBadRequest, unsupportedBinaryMarkerMessage)
	return true
}

// scalarStatsBinary is a translated query that is a stats query and a number joined by an operator: the form the
// translator writes for a metric whose client query is not a binary expression, stdvar_over_time (the standard
// deviation raised to the power 2, as VictoriaLogs has no variance function).
type scalarStatsBinary struct {
	op         string
	stats      physical.VLStats
	scalar     float64
	scalarLeft bool
}

// scalarStatsBinaryOf reads a binary expression as a stats query and a number joined by an operator, ok false for
// any other: a join of two queries, a modifier, a nested expression, or an operand that is no stats query.
func scalarStatsBinaryOf(bin physical.Binary) (b scalarStatsBinary, ok bool) {
	if bin.Tail != "" || bin.Bool || !scalarBinaryOps[bin.Op] {
		return b, false
	}
	operand, number := bin.L, bin.R
	if _, leftIsNumber := bin.L.(physical.ScalarOp); leftIsNumber {
		operand, number, b.scalarLeft = bin.R, bin.L, true
	}
	n, isNumber := number.(physical.ScalarOp)
	query, isQuery := operand.(physical.VLQuery)
	if !isNumber || !isQuery {
		return b, false
	}
	stats, isStats := translator.DecodeStats(query.Text)
	if !isStats || !stats.Shaped {
		return b, false
	}
	b.op, b.stats, b.scalar = bin.Op, stats, n.V
	return b, true
}

// scalarBinaryOps are the operators a number can be applied to a vector with (the set operators take two vectors).
var scalarBinaryOps = map[string]bool{
	"+": true, "-": true, "*": true, "/": true, "%": true, "^": true,
	"==": true, "!=": true, ">": true, "<": true, ">=": true, "<=": true,
}

// statsNativeKey marks a request whose stats query is read from VictoriaLogs as translated, not evaluated again
// from the client's query.
type statsNativeKey struct{}

// statsNative reports whether the request's stats query is to be answered by VictoriaLogs as translated.
func statsNative(ctx context.Context) bool {
	native, _ := ctx.Value(statsNativeKey{}).(bool)
	return native
}

// proxyScalarStatsBinary answers a stats query joined to a number: the stats query goes through the stats handler
// as translated (statsNativeKey: the client's query names another function than the one the stats compute), and the
// binary route's scalar evaluation applies the number to its answer. resultType is "matrix" or "vector".
func (p *Proxy) proxyScalarStatsBinary(w http.ResponseWriter, r *http.Request, b scalarStatsBinary, resultType string) {
	r = r.WithContext(context.WithValue(r.Context(), statsNativeKey{}, true))
	buf := &bufferedResponseWriter{}
	if resultType == "matrix" {
		p.proxyStatsQueryRange(buf, r, b.stats)
	} else {
		p.proxyStatsQuery(buf, r, b.stats)
	}
	copyHeaders(w.Header(), buf.Header())
	if buf.code != 0 && buf.code != http.StatusOK {
		// A failed operand is the answer, as the binary route sends it.
		setReplayContentType(w, buf.code)
		w.WriteHeader(buf.code)
		_, _ = w.Write(buf.body) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter -- the stats handler's own error body
		return
	}
	result, err := matchBinaryScalarContext(r.Context(), buf.body, b.scalar, b.scalarLeft, b.op, resultType, false)
	if err != nil {
		p.writeErr(w, r.Context(), err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(result) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter -- the binary evaluator's own JSON
}
