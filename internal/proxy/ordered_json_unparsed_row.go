package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"
)

// A `| json` metric whose evaluated window holds a line that does not open
// with a brace is answered by the row evaluator: Loki's parser rejects that
// line, and the evaluator answers Loki's pipeline error for it or evaluates
// the rows. The strict split looks such a line up before it trusts its stats
// buckets (jsonUnparsedLineRisk), and the row evaluator looks it up again to
// build the error (unparsedLinePipelineError). A row VictoriaLogs returned for
// that lookup stays in the window it was found in (rows are append-only), so
// it is remembered per tenant and lookup text, local to the replica, for the
// parse-risk probe TTL: a later lookup of a window that holds its timestamp
// reads it with no VictoriaLogs call. That keeps the row evaluator from
// asking the question the risk check just answered, and lets a repeat or a
// refresh skip the speculative stats query and the other checks the strict
// split starts beside the lookup, so it sends the same requests every time.

// unparsedLineRowMaxBytes bounds a remembered row; a longer one is read again
// when needed.
const unparsedLineRowMaxBytes = 64 << 10

// unparsedLineRow is a remembered row: its _time in Unix nanoseconds and the
// row as VictoriaLogs returned it.
type unparsedLineRow struct {
	T   int64           `json:"t"`
	Row json.RawMessage `json:"r"`
}

// unparsedLineLookup is the lookup text for a base query: the first line of
// the base that does not open with a brace.
func unparsedLineLookup(base string) string {
	return base + " | filter -" + jsonObjectStartFilter + " | limit 1"
}

// unparsedLineWindow is the part of [start-window, end] a range query
// evaluates: (start-window, last], last the final evaluation time on the
// step grid, as the bounds [from, to].
func unparsedLineWindow(start, end time.Time, step, window time.Duration) (from, to time.Time) {
	last := start.Add(end.Sub(start) / step * step)
	return start.Add(-window).Add(time.Nanosecond), last
}

func (p *Proxy) unparsedLineRowKey(ctx context.Context, lookup string) string {
	return p.windowProbeKey(ctx, "json-unparsed-row:v1:"+getOrgID(ctx)+":"+lookup)
}

// cachedUnparsedLineRow returns a remembered row of lookup whose time lies in
// [from, to].
func (p *Proxy) cachedUnparsedLineRow(ctx context.Context, lookup string, from, to time.Time) ([]byte, bool) {
	if p.cache == nil {
		return nil, false
	}
	raw, ok := p.cacheGet(ctx, p.cache, p.unparsedLineRowKey(ctx, lookup))
	if !ok {
		return nil, false
	}
	var r unparsedLineRow
	if json.Unmarshal(raw, &r) != nil || len(r.Row) == 0 || r.T < from.UnixNano() || r.T > to.UnixNano() {
		return nil, false
	}
	return r.Row, true
}

// readUnparsedLineRow asks VictoriaLogs for one row of lookup in [from, to]
// and remembers it; nil when the window holds none.
func (p *Proxy) readUnparsedLineRow(ctx context.Context, lookup string, from, to time.Time) ([]byte, error) {
	// Loki's window is left-open, right-closed; VictoriaLogs' end is exclusive.
	params := url.Values{"query": {lookup}, "start": {from.UTC().Format(time.RFC3339Nano)}, "end": {to.Add(time.Nanosecond).UTC().Format(time.RFC3339Nano)}}
	resp, err := p.vlPost(ctx, "/select/logsql/query", params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		return nil, p.redactedBackendStatusError("backend returned", resp.StatusCode, body)
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64<<10), 8<<20)
	if !scanner.Scan() {
		return nil, scanner.Err()
	}
	row := append([]byte(nil), scanner.Bytes()...)
	if len(row) == 0 {
		return nil, nil
	}
	var fields struct {
		Time string `json:"_time"`
	}
	if err := json.Unmarshal(row, &fields); err != nil {
		return nil, fmt.Errorf("invalid backend metric row: %w", err)
	}
	if ts, err := time.Parse(time.RFC3339Nano, fields.Time); err == nil && p.cache != nil && p.parseRiskProbeCacheTTL > 0 && len(row) <= unparsedLineRowMaxBytes {
		if raw, err := json.Marshal(unparsedLineRow{T: ts.UnixNano(), Row: row}); err == nil {
			p.cache.SetLocalOnlyWithTTL(p.unparsedLineRowKey(ctx, lookup), raw, p.parseRiskProbeCacheTTL)
		}
	}
	return row, nil
}

// unparsedLineRowFor returns a row of lookup in [from, to]: a remembered one,
// or one VictoriaLogs returns (nil when the window holds none).
func (p *Proxy) unparsedLineRowFor(ctx context.Context, lookup string, from, to time.Time) ([]byte, error) {
	if row, ok := p.cachedUnparsedLineRow(ctx, lookup, from, to); ok {
		return row, nil
	}
	return p.readUnparsedLineRow(ctx, lookup, from, to)
}
