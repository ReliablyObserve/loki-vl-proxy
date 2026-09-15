// Package dataspan pins benchmark windows to the seeded data and checks that
// Loki and VictoriaLogs hold the same lines before a comparison starts.
package dataspan

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Auto is the flag value that asks for detection from VictoriaLogs.
const Auto = "auto"

// Span is the time range covered by the seeded data.
type Span struct {
	Start time.Time
	End   time.Time
}

// ParseTime accepts RFC3339 / RFC3339Nano, or an integer Unix timestamp in
// seconds, milliseconds or nanoseconds (chosen by magnitude).
func ParseTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("%q is neither RFC3339 nor a Unix timestamp", s)
	}
	switch {
	case n >= 1e17: // nanoseconds
		return time.Unix(0, n), nil
	case n >= 1e11: // milliseconds
		return time.UnixMilli(n), nil
	default:
		return time.Unix(n, 0), nil
	}
}

var httpClient = &http.Client{Timeout: 5 * time.Minute}

// seedSelectorVL matches the seeded streams in VictoriaLogs (every seeded
// stream has a non-empty app label); seedSelectorLoki is the Loki equivalent.
const (
	seedSelectorVL   = `app:*`
	seedSelectorLoki = `{app=~".+"}`
)

// CheckWindow is the window size of the per-service ingest comparison and of
// the gap check.
const CheckWindow = time.Hour

// DetectVL returns the min and max `_time` of the seeded streams in
// VictoriaLogs. It fails when a CheckWindow inside that span holds no seeded
// line: a gap larger than one window means the seed did not complete and
// queries over the gap would time empty ranges.
func DetectVL(ctx context.Context, vlURL string) (Span, error) {
	row, err := vlStatsRow(ctx, vlURL, seedSelectorVL+` | stats min(_time) min_time, max(_time) max_time`)
	if err != nil {
		return Span{}, err
	}
	span, err := parseSpanRow(row)
	if err != nil {
		return Span{}, err
	}
	counts, err := VLWindowCounts(ctx, vlURL, span, CheckWindow)
	if err != nil {
		return Span{}, fmt.Errorf("gap check: %w", err)
	}
	if gaps := Gaps(span, CheckWindow, counts); len(gaps) > 0 {
		return Span{}, fmt.Errorf("seeded data in VictoriaLogs has %d empty %s windows between %s and %s (first ends %s); re-seed on a fresh stack",
			len(gaps), CheckWindow, span.Start.UTC().Format(time.RFC3339), span.End.UTC().Format(time.RFC3339), gaps[0].UTC().Format(time.RFC3339))
	}
	return span, nil
}

func parseSpanRow(row map[string]string) (Span, error) {
	minRaw, maxRaw := row["min_time"], row["max_time"]
	if minRaw == "" || maxRaw == "" {
		return Span{}, fmt.Errorf("VictoriaLogs holds no seeded data (min_time=%q max_time=%q)", minRaw, maxRaw)
	}
	start, err := time.Parse(time.RFC3339Nano, minRaw)
	if err != nil {
		return Span{}, fmt.Errorf("parse min_time %q: %w", minRaw, err)
	}
	end, err := time.Parse(time.RFC3339Nano, maxRaw)
	if err != nil {
		return Span{}, fmt.Errorf("parse max_time %q: %w", maxRaw, err)
	}
	if end.Before(start) {
		return Span{}, fmt.Errorf("max_time %s is before min_time %s", maxRaw, minRaw)
	}
	return Span{Start: start, End: end}, nil
}

// Cell identifies the lines of one seeded service (app, cluster, region) in
// the window (End-window, End].
type Cell struct {
	End     int64 // Unix seconds, aligned to the window
	App     string
	Cluster string
	Region  string
}

func (c Cell) String() string {
	return fmt.Sprintf("%s app=%q cluster=%q region=%q", time.Unix(c.End, 0).UTC().Format(time.RFC3339), c.App, c.Cluster, c.Region)
}

// Counts holds line counts per Cell.
type Counts map[Cell]int64

// Total sums all cells.
func (c Counts) Total() int64 {
	var n int64
	for _, v := range c {
		n += v
	}
	return n
}

// WindowEnds returns the first and last window end whose windows
// (end-window, end], aligned to the Unix epoch, cover the span.
func WindowEnds(s Span, window time.Duration) (first, last time.Time) {
	first = s.Start.Add(-time.Nanosecond).Truncate(window).Add(window)
	last = s.End.Truncate(window)
	if last.Before(s.End) {
		last = last.Add(window)
	}
	return first, last
}

// Gaps returns the ends of the windows covering the span that hold no line.
func Gaps(s Span, window time.Duration, counts Counts) []time.Time {
	perWindow := map[int64]int64{}
	for c, n := range counts {
		perWindow[c.End] += n
	}
	first, last := WindowEnds(s, window)
	var gaps []time.Time
	for t := first; !t.After(last); t = t.Add(window) {
		if perWindow[t.Unix()] == 0 {
			gaps = append(gaps, t)
		}
	}
	return gaps
}

// VLWindowCounts counts seeded lines per service and window in VictoriaLogs.
// VictoriaLogs labels a `_time` bucket by its start and includes it ([T, T+w));
// an offset of window-1ns turns the buckets into (end-window, end], the same
// windows Loki's count_over_time evaluates at each step.
func VLWindowCounts(ctx context.Context, vlURL string, s Span, window time.Duration) (Counts, error) {
	first, last := WindowEnds(s, window)
	q := fmt.Sprintf(`_time:(%s, %s] %s | stats by (_time:%dns offset %dns, app, cluster, region) count() lines`,
		first.Add(-window).UTC().Format(time.RFC3339Nano), last.UTC().Format(time.RFC3339Nano), seedSelectorVL,
		window.Nanoseconds(), (window - time.Nanosecond).Nanoseconds())
	body, err := get(ctx, vlURL+"/select/logsql/query?"+url.Values{"query": {q}}.Encode(), nil)
	if err != nil {
		return nil, err
	}
	counts := Counts{}
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		row := map[string]string{}
		if err := json.Unmarshal(line, &row); err != nil {
			return nil, fmt.Errorf("decode VictoriaLogs row %q: %w", line, err)
		}
		bucket, err := time.Parse(time.RFC3339Nano, row["_time"])
		if err != nil {
			return nil, fmt.Errorf("parse bucket %q: %w", row["_time"], err)
		}
		end := bucket.Add(window - time.Nanosecond)
		if end.UnixNano()%window.Nanoseconds() != 0 {
			return nil, fmt.Errorf("bucket %s does not end on a %s boundary", row["_time"], window)
		}
		n, err := strconv.ParseInt(row["lines"], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse VictoriaLogs count %q: %w", row["lines"], err)
		}
		counts[Cell{End: end.Unix(), App: row["app"], Cluster: row["cluster"], Region: row["region"]}] += n
	}
	return counts, sc.Err()
}

// LokiWindowCountsQuery returns the range query that counts seeded lines per
// service and window in Loki.
func LokiWindowCountsQuery(window time.Duration) string {
	return fmt.Sprintf(`sum by (app, cluster, region) (count_over_time(%s[%ds]))`, seedSelectorLoki, int64(window/time.Second))
}

// LokiWindowCounts counts seeded lines per service and window in Loki. The
// request carries Cache-Control: no-cache so Loki's results cache cannot
// return counts from before its ingesters flushed.
func LokiWindowCounts(ctx context.Context, lokiURL string, s Span, window time.Duration) (Counts, error) {
	first, last := WindowEnds(s, window)
	v := url.Values{
		"query": {LokiWindowCountsQuery(window)},
		"start": {strconv.FormatInt(first.Unix(), 10)},
		"end":   {strconv.FormatInt(last.Unix(), 10)},
		"step":  {strconv.FormatInt(int64(window/time.Second), 10)},
	}
	body, err := get(ctx, lokiURL+"/loki/api/v1/query_range?"+v.Encode(), http.Header{"Cache-Control": {"no-cache"}})
	if err != nil {
		return nil, err
	}
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
		Warnings []string `json:"warnings"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode Loki response: %w", err)
	}
	if resp.Status != "success" || len(resp.Warnings) > 0 {
		return nil, fmt.Errorf("unexpected Loki answer (status %q, warnings %v): %s", resp.Status, resp.Warnings, truncate(body))
	}
	counts := Counts{}
	for _, series := range resp.Data.Result {
		for _, sample := range series.Values {
			if len(sample) != 2 {
				return nil, fmt.Errorf("unexpected Loki sample %v", sample)
			}
			ts, ok := sample[0].(float64)
			raw, ok2 := sample[1].(string)
			if !ok || !ok2 {
				return nil, fmt.Errorf("unexpected Loki sample %v", sample)
			}
			f, err := strconv.ParseFloat(raw, 64)
			if err != nil {
				return nil, fmt.Errorf("parse Loki count %q: %w", raw, err)
			}
			cell := Cell{End: int64(math.Round(ts)), App: series.Metric["app"], Cluster: series.Metric["cluster"], Region: series.Metric["region"]}
			counts[cell] += int64(math.Round(f))
		}
	}
	return counts, nil
}

// DiffCounts lists the cells whose counts differ, sorted by window and service.
func DiffCounts(loki, vl Counts) []string {
	cells := map[Cell]struct{}{}
	for c := range loki {
		cells[c] = struct{}{}
	}
	for c := range vl {
		cells[c] = struct{}{}
	}
	keys := make([]Cell, 0, len(cells))
	for c := range cells {
		if loki[c] != vl[c] {
			keys = append(keys, c)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].End != keys[j].End {
			return keys[i].End < keys[j].End
		}
		return keys[i].String() < keys[j].String()
	})
	out := make([]string, 0, len(keys))
	for _, c := range keys {
		out = append(out, fmt.Sprintf("%s: loki=%d victorialogs=%d", c, loki[c], vl[c]))
	}
	return out
}

// retentionLookback is how far back the existing-data checks look for streams
// with the seeded app labels: the compose stack keeps 7 days on both backends.
const retentionLookback = 7 * 24 * time.Hour

// LokiExistingData describes data Loki already holds that a new seed would mix
// with: any stream in [start, end], or a stream within the retention lookback
// whose app label is one of apps. It returns nil when there is none.
func LokiExistingData(ctx context.Context, lokiURL string, start, end time.Time, apps []string) ([]string, error) {
	var reasons []string
	names, err := lokiStrings(ctx, lokiURL+"/loki/api/v1/labels", start, end)
	if err != nil {
		return nil, err
	}
	if len(names) > 0 {
		reasons = append(reasons, fmt.Sprintf("Loki holds streams in %s → %s (labels %s)", start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339), strings.Join(head(names, 8), ", ")))
	}
	values, err := lokiStrings(ctx, lokiURL+"/loki/api/v1/label/app/values", end.Add(-retentionLookback), end)
	if err != nil {
		return nil, err
	}
	if both := intersect(values, apps); len(both) > 0 {
		reasons = append(reasons, fmt.Sprintf("Loki holds streams with the seeded app labels in the last %s: %s", retentionLookback, strings.Join(head(both, 8), ", ")))
	}
	return reasons, nil
}

// VLExistingData is LokiExistingData for VictoriaLogs.
func VLExistingData(ctx context.Context, vlURL string, start, end time.Time, apps []string) ([]string, error) {
	var reasons []string
	row, err := vlStatsRow(ctx, vlURL, fmt.Sprintf(`_time:[%s, %s] | stats count() lines`, start.UTC().Format(time.RFC3339Nano), end.UTC().Format(time.RFC3339Nano)))
	if err != nil {
		return nil, err
	}
	if n, _ := strconv.ParseInt(row["lines"], 10, 64); n > 0 {
		reasons = append(reasons, fmt.Sprintf("VictoriaLogs holds %d lines in %s → %s", n, start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339)))
	}
	v := url.Values{
		"query": {"*"},
		"field": {"app"},
		"start": {strconv.FormatInt(end.Add(-retentionLookback).UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
	}
	body, err := get(ctx, vlURL+"/select/logsql/field_values?"+v.Encode(), nil)
	if err != nil {
		return nil, err
	}
	var resp struct {
		Values []struct {
			Value string `json:"value"`
		} `json:"values"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode VictoriaLogs field values: %w", err)
	}
	values := make([]string, 0, len(resp.Values))
	for _, fv := range resp.Values {
		values = append(values, fv.Value)
	}
	if both := intersect(values, apps); len(both) > 0 {
		reasons = append(reasons, fmt.Sprintf("VictoriaLogs holds streams with the seeded app labels in the last %s: %s", retentionLookback, strings.Join(head(both, 8), ", ")))
	}
	return reasons, nil
}

// lokiStrings reads a Loki labels or label-values answer over [start, end].
func lokiStrings(ctx context.Context, endpoint string, start, end time.Time) ([]string, error) {
	v := url.Values{"start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
	body, err := get(ctx, endpoint+"?"+v.Encode(), http.Header{"Cache-Control": {"no-cache"}})
	if err != nil {
		return nil, err
	}
	var resp struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode Loki answer: %w", err)
	}
	if resp.Status != "success" {
		return nil, fmt.Errorf("unexpected Loki status %q: %s", resp.Status, truncate(body))
	}
	return resp.Data, nil
}

func intersect(have, want []string) []string {
	set := make(map[string]struct{}, len(want))
	for _, w := range want {
		set[w] = struct{}{}
	}
	var out []string
	seen := map[string]struct{}{}
	for _, h := range have {
		if _, ok := set[h]; !ok {
			continue
		}
		if _, dup := seen[h]; dup {
			continue
		}
		seen[h] = struct{}{}
		out = append(out, h)
	}
	sort.Strings(out)
	return out
}

func vlStatsRow(ctx context.Context, vlURL, query string) (map[string]string, error) {
	body, err := get(ctx, vlURL+"/select/logsql/query?"+url.Values{"query": {query}}.Encode(), nil)
	if err != nil {
		return nil, err
	}
	sc := bufio.NewScanner(strings.NewReader(string(body)))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		row := map[string]string{}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, fmt.Errorf("decode VictoriaLogs row %q: %w", line, err)
		}
		return row, nil
	}
	return map[string]string{}, nil
}

func get(ctx context.Context, u string, header http.Header) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range header {
		req.Header[k] = v
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: HTTP %d: %s", u, resp.StatusCode, truncate(body))
	}
	return body, nil
}

func truncate(b []byte) string {
	const max = 300
	if len(b) > max {
		return string(b[:max]) + "…"
	}
	return string(b)
}

// WaitEqual polls both backends until Loki and VictoriaLogs hold the same
// number of seeded lines for every service in every CheckWindow of the span,
// or the timeout elapses. Loki keeps recently pushed chunks in ingesters that
// the read path may not consult for historical timestamps until they are
// flushed, so counts converge some time after seeding. Comparing per service
// and window, not one total, catches lines that are missing in one place and
// duplicated in another.
func WaitEqual(ctx context.Context, lokiURL, vlURL string, s Span, timeout, poll time.Duration, logf func(string, ...any)) error {
	deadline := time.Now().Add(timeout)
	var lastDiff []string
	var lastLoki, lastVL int64
	for attempt := 1; ; attempt++ {
		vlCounts, vlErr := VLWindowCounts(ctx, vlURL, s, CheckWindow)
		lokiCounts, lokiErr := LokiWindowCounts(ctx, lokiURL, s, CheckWindow)
		switch {
		case vlErr != nil:
			logf("  entry check %d: VictoriaLogs count failed: %v", attempt, vlErr)
		case lokiErr != nil:
			logf("  entry check %d: Loki count failed: %v", attempt, lokiErr)
		case vlCounts.Total() == 0:
			return fmt.Errorf("entry check: VictoriaLogs holds no seeded lines in %s → %s", s.Start.Format(time.RFC3339), s.End.Format(time.RFC3339))
		default:
			diff := DiffCounts(lokiCounts, vlCounts)
			if len(diff) == 0 {
				logf("  ✓ entry check: Loki and VictoriaLogs both hold %d lines, equal in all %d service × %s windows", vlCounts.Total(), len(vlCounts), CheckWindow)
				return nil
			}
			lastDiff, lastLoki, lastVL = diff, lokiCounts.Total(), vlCounts.Total()
			logf("  entry check %d: loki=%d victorialogs=%d, %d service windows differ (first: %s), waiting...", attempt, lastLoki, lastVL, len(diff), diff[0])
		}
		if time.Now().Add(poll).After(deadline) {
			return fmt.Errorf("entry check timed out after %s: Loki holds %d lines, VictoriaLogs %d, over %s → %s, and %d service windows differ:\n    %s\nthe comparison would not measure the same data",
				timeout, lastLoki, lastVL, s.Start.Format(time.RFC3339), s.End.Format(time.RFC3339), len(lastDiff), strings.Join(head(lastDiff, 10), "\n    "))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(poll):
		}
	}
}

func head(list []string, n int) []string {
	if len(list) > n {
		return append(list[:n:n], fmt.Sprintf("… and %d more", len(list)-n))
	}
	return list
}
