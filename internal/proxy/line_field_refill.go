package proxy

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// DefaultLabelFilterRefillMaxPages is the default of
// -label-filter-refill-max-pages: how many more pages of rows a log query
// whose label filters drop rows the backend matched (lineFieldExposure.dropsRow)
// reads to fill its limit. Each page is one more request of at most limit
// rows, so a response reads at most (1 + pages) x limit rows. A window that
// still has rows after that answers with fewer than limit lines; 0 reads no
// further page.
const (
	DefaultLabelFilterRefillMaxPages = 8
)

// vlRowFetch posts one VictoriaLogs log query with the given parameters.
type vlRowFetch func(ctx context.Context, params url.Values) (*http.Response, error)

// refillLineFieldRows reads a VictoriaLogs NDJSON log response (first) for
// params, drops the rows a Loki label filter would not match
// (lineFieldExposure.dropsRow) and, while the limit is not filled and the
// page was full, reads the next page: rows older than the oldest row read
// (backward) or newer than the newest (forward), up to
// -label-filter-refill-max-pages more requests. It returns the kept rows as an NDJSON body in response
// order. levelAsLabel mirrors the classification: the stored level field is
// a stream label of the response unless levels go to structured metadata.
func (p *Proxy) refillLineFieldRows(ctx context.Context, first io.Reader, params url.Values, limit int, forward bool, exposure *lineFieldExposure, levelAsLabel bool, fetch vlRowFetch) ([]byte, error) {
	var (
		out      bytes.Buffer
		kept     int
		boundary time.Time
		seen     map[string]struct{} // rows read at the boundary timestamps
	)
	page := first
	pageLimit := limit
	for round := 0; ; round++ {
		read, fresh, oldest, newest, newSeen, err := p.filterLineFieldPage(page, seen, boundary, limit-kept, exposure, levelAsLabel, &out)
		if err != nil {
			return nil, err
		}
		kept += fresh
		edge := oldest
		if forward {
			edge = newest
		}
		// Stop when the limit is filled, the window holds no more rows, the
		// budget is spent, or a page brought nothing past the last boundary
		// (more rows than the limit share one timestamp).
		if kept >= limit || read < pageLimit || round >= p.labelFilterRefillMaxPages || edge.IsZero() || (round > 0 && edge.Equal(boundary)) {
			return out.Bytes(), nil
		}
		next := url.Values{}
		for k, v := range params {
			next[k] = v
		}
		if forward {
			boundary = newest
			next.Set("start", boundary.UTC().Format(vlNanoTimeLayout))
		} else {
			boundary = oldest
			// One nanosecond past the boundary: VictoriaLogs versions differ
			// on whether end is inclusive; rows already read are skipped.
			next.Set("end", boundary.Add(time.Nanosecond).UTC().Format(vlNanoTimeLayout))
		}
		seen = newSeen
		// The rows already read at the boundary come back first; they do not
		// take the place of new rows.
		next.Set("limit", strconv.Itoa(limit+len(seen)))
		pageLimit = limit + len(seen)
		resp, err := fetch(ctx, next)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode >= 400 {
			body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
			_ = resp.Body.Close()
			return nil, fmt.Errorf("backend status %d: %s", resp.StatusCode, body)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return nil, err
		}
		page = bytes.NewReader(body)
	}
}

// vlNanoTimeLayout writes a VictoriaLogs time bound with all nine
// fractional digits, so the bound is not rounded to a coarser precision.
const vlNanoTimeLayout = "2006-01-02T15:04:05.000000000Z"

// filterLineFieldPage copies the rows of one page that the filters keep to
// out, at most want of them, skipping rows read on the previous page at its
// boundary. It returns the rows the page held, the rows kept, the page's
// oldest and newest timestamps and the rows at the timestamps the next page
// starts from (boundary and one nanosecond past it).
func (p *Proxy) filterLineFieldPage(page io.Reader, seen map[string]struct{}, boundary time.Time, want int, exposure *lineFieldExposure, levelAsLabel bool, out *bytes.Buffer) (read, kept int, oldest, newest time.Time, nextSeen map[string]struct{}, err error) {
	scanner := bufio.NewScanner(page)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	type rowTime struct {
		line string
		ts   time.Time
	}
	var rows []rowTime
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		read++
		parser := vlFJParserPool.Get()
		value, perr := parser.ParseBytes(line)
		if perr != nil {
			vlFJParserPool.Put(parser)
			continue
		}
		ts, terr := time.Parse(time.RFC3339Nano, string(value.GetStringBytes("_time")))
		if terr == nil {
			rows = append(rows, rowTime{string(line), ts})
			if oldest.IsZero() || ts.Before(oldest) {
				oldest = ts
			}
			if newest.IsZero() || ts.After(newest) {
				newest = ts
			}
		}
		if _, dup := seen[string(line)]; dup {
			vlFJParserPool.Put(parser)
			continue
		}
		stored := value.GetStringBytes(translator.StoredLineField)
		if stored == nil {
			stored = value.GetStringBytes("_msg")
		}
		drop := false
		if !isVLMissingMsgBytes(stored, p.defaultMsgValue()) {
			stream := parseStreamLabels(string(value.GetStringBytes("_stream")))
			if level := value.GetStringBytes("level"); levelAsLabel && len(bytes.TrimSpace(level)) > 0 {
				// parseStreamLabels shares its map; copy before adding level.
				withLevel := maps.Clone(stream)
				if withLevel == nil {
					withLevel = map[string]string{}
				}
				withLevel["level"] = string(level)
				stream = withLevel
			}
			row := lineRow{line: stored, keys: jsonLineKeys(stored), stream: stream}
			drop = exposure.dropsRow(&row)
		}
		vlFJParserPool.Put(parser)
		if drop || kept >= want {
			continue
		}
		out.Write(line)
		out.WriteByte('\n')
		kept++
	}
	if err = scanner.Err(); err != nil {
		return 0, 0, time.Time{}, time.Time{}, nil, err
	}
	// The next page starts at the boundary row's timestamp; remember the rows
	// read there (and one nanosecond past it) so they are not counted twice.
	edge := oldest
	if !boundary.IsZero() && edge.IsZero() {
		edge = boundary
	}
	nextSeen = make(map[string]struct{})
	for _, r := range rows {
		if d := r.ts.Sub(edge); d >= 0 && d <= time.Nanosecond {
			nextSeen[r.line] = struct{}{}
		}
		if r.ts.Equal(newest) {
			nextSeen[r.line] = struct{}{}
		}
	}
	return read, kept, oldest, newest, nextSeen, nil
}
