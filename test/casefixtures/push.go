package casefixtures

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Ticks are the instants a stream's entries are written at: every tick holds every entry once. They run from
// span before at (the last one at most one tick before it, so nothing is written ahead of the clock) at a fixed
// interval, half a second past the interval's boundary so no line sits on a window edge, plus the sub-millisecond
// part of at so a second seed never repeats a (timestamp, line) pair Loki would drop and VictoriaLogs would keep.
func Ticks(at time.Time, span, every time.Duration) []time.Time {
	if every <= 0 || span < every {
		return nil
	}
	jitter := time.Duration(at.UnixNano() % int64(time.Millisecond))
	first := at.Truncate(every).Add(-span)
	var out []time.Time
	for t := first; t.Add(every).Before(at.Add(time.Nanosecond)); t = t.Add(every) {
		out = append(out, t.Add(500*time.Millisecond+jitter))
	}
	return out
}

// stamp is the timestamp of the i-th entry of a tick: a millisecond apart, so the entries of one stream never
// share one.
func stamp(tick time.Time, i int) time.Time { return tick.Add(time.Duration(i) * time.Millisecond) }

// metadataOf is the structured metadata Loki stores for an entry.
func (e Entry) lokiMetadata() map[string]string {
	if len(e.Metadata) == 0 && len(e.LokiMetadata) == 0 {
		return nil
	}
	out := make(map[string]string, len(e.Metadata)+len(e.LokiMetadata))
	for k, v := range e.Metadata {
		out[k] = v
	}
	for k, v := range e.LokiMetadata {
		out[k] = v
	}
	return out
}

// LokiBody is Loki's push body for the stream over the ticks. VictoriaLogs' Loki push routes receive the same bytes.
func LokiBody(s Stream, ticks []time.Time) ([]byte, error) {
	values := make([]any, 0, len(ticks)*len(s.Entries))
	for _, tick := range ticks {
		for i, e := range s.Entries {
			ts := strconv.FormatInt(stamp(tick, i).UnixNano(), 10)
			if md := e.lokiMetadata(); md != nil {
				values = append(values, []any{ts, e.Line, md})
			} else {
				values = append(values, []any{ts, e.Line})
			}
		}
	}
	return json.Marshal(map[string]any{"streams": []any{map[string]any{"stream": s.Labels, "values": values}}})
}

// streamFields are the label names of a stream, sorted: the _stream_fields of the JSON-line route.
func streamFields(s Stream) []string {
	names := make([]string, 0, len(s.Labels))
	for k := range s.Labels {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

// JSONLineBody is the VictoriaLogs body of the JSON-line route: one row per entry and tick, the labels, the
// structured metadata and the projected fields as plain fields.
func JSONLineBody(s Stream, ticks []time.Time) ([]byte, error) {
	var buf bytes.Buffer
	for _, tick := range ticks {
		for i, e := range s.Entries {
			row := map[string]string{"_time": stamp(tick, i).UTC().Format(time.RFC3339Nano), "_msg": e.Line}
			for k, v := range s.Labels {
				row[k] = v
			}
			for k, v := range e.Metadata {
				row[k] = v
			}
			for k, v := range e.VLFields {
				row[k] = v
			}
			b, err := json.Marshal(row)
			if err != nil {
				return nil, err
			}
			buf.Write(b)
			buf.WriteByte('\n')
		}
	}
	return buf.Bytes(), nil
}

// Target is the stack a seed writes to.
type Target struct {
	Loki   string // Loki base URL
	VL     string // VictoriaLogs base URL
	Tenant string // X-Scope-OrgID of Loki ("0" is VictoriaLogs' default account)
	Client *http.Client
}

func (t Target) client() *http.Client {
	if t.Client != nil {
		return t.Client
	}
	return &http.Client{Timeout: 2 * time.Minute}
}

func (t Target) post(ctx context.Context, target, contentType string, body []byte, headers map[string]string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", contentType)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := t.client().Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%s answered %d: %s", target, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return nil
}

// Push writes the stream's entries at the ticks to Loki and to VictoriaLogs.
func (t Target) Push(ctx context.Context, s Stream, ticks []time.Time) error {
	if len(ticks) == 0 {
		return nil
	}
	lokiBody, err := LokiBody(s, ticks)
	if err != nil {
		return err
	}
	if err := t.post(ctx, strings.TrimRight(t.Loki, "/")+"/loki/api/v1/push", "application/json", lokiBody,
		map[string]string{"X-Scope-OrgID": t.Tenant}); err != nil {
		return fmt.Errorf("stream %s to Loki: %w", s.Name, err)
	}
	vl := strings.TrimRight(t.VL, "/")
	switch s.Route {
	case RouteLokiPushRaw:
		err = t.post(ctx, vl+"/insert/loki/api/v1/push?disable_message_parsing=1", "application/json", lokiBody, nil)
	case RouteLokiPush:
		err = t.post(ctx, vl+"/insert/loki/api/v1/push", "application/json", lokiBody, nil)
	default:
		var body []byte
		if body, err = JSONLineBody(s, ticks); err == nil {
			err = t.post(ctx, vl+"/insert/jsonline?_stream_fields="+url.QueryEscape(strings.Join(streamFields(s), ",")),
				"application/stream+json", body, nil)
		}
	}
	if err != nil {
		return fmt.Errorf("stream %s to VictoriaLogs: %w", s.Name, err)
	}
	return nil
}

// Flush asks VictoriaLogs to make what it received searchable (best effort: an old version has no such route).
func (t Target) Flush(ctx context.Context) {
	_ = t.post(ctx, strings.TrimRight(t.VL, "/")+"/internal/force_flush", "text/plain", nil, nil)
}

func (t Target) get(ctx context.Context, target string, headers map[string]string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, 0, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := t.client().Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return raw, resp.StatusCode, err
}

// selector is the stream's labels as a LogQL stream selector.
func selector(s Stream) string {
	names := streamFields(s)
	parts := make([]string, len(names))
	for i, k := range names {
		parts[i] = k + "=" + strconv.Quote(s.Labels[k])
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// vlFilter is the stream's labels as VictoriaLogs field filters.
func vlFilter(s Stream) string {
	names := streamFields(s)
	parts := make([]string, len(names))
	for i, k := range names {
		parts[i] = k + ":=" + strconv.Quote(s.Labels[k])
	}
	return strings.Join(parts, " ")
}

// Counts reads how many lines of the stream each side holds over the window ending at end: Loki through its metric
// path (the one the verifier's precheck counts with), VictoriaLogs with stats count().
func (t Target) Counts(ctx context.Context, s Stream, end time.Time, window time.Duration) (loki, vl float64, err error) {
	q := url.Values{}
	q.Set("query", fmt.Sprintf("sum(count_over_time(%s[%ds]))", selector(s), int(window/time.Second)))
	q.Set("time", strconv.FormatInt(end.UnixNano(), 10))
	raw, status, err := t.get(ctx, strings.TrimRight(t.Loki, "/")+"/loki/api/v1/query?"+q.Encode(), map[string]string{"X-Scope-OrgID": t.Tenant})
	if err != nil {
		return 0, 0, err
	}
	if status != http.StatusOK {
		return 0, 0, fmt.Errorf("the Loki query answered %d: %s", status, strings.TrimSpace(string(raw)))
	}
	var lk struct {
		Data struct {
			Result []struct {
				Value []any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &lk); err != nil {
		return 0, 0, fmt.Errorf("the Loki count is not JSON: %w", err)
	}
	for _, r := range lk.Data.Result {
		if len(r.Value) == 2 {
			if str, ok := r.Value[1].(string); ok {
				f, _ := strconv.ParseFloat(str, 64)
				loki += f
			}
		}
	}
	vq := url.Values{}
	vq.Set("query", vlFilter(s)+" | stats count() as c")
	vq.Set("start", end.Add(-window).UTC().Format(time.RFC3339Nano))
	vq.Set("end", end.UTC().Format(time.RFC3339Nano))
	raw, status, err = t.get(ctx, strings.TrimRight(t.VL, "/")+"/select/logsql/query?"+vq.Encode(), nil)
	if err != nil {
		return 0, 0, err
	}
	if status != http.StatusOK {
		return 0, 0, fmt.Errorf("VictoriaLogs answered %d: %s", status, strings.TrimSpace(string(raw)))
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var row map[string]string
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return 0, 0, fmt.Errorf("VictoriaLogs' count is not JSON: %w", err)
		}
		f, _ := strconv.ParseFloat(row["c"], 64)
		vl += f
	}
	return loki, vl, nil
}

// WaitReady waits until both sides count at least want lines of the stream over the window and count the same
// number: a Loki that is still cutting its head block answers its metric path with nothing (loki_not_ready), and a
// VictoriaLogs that has not flushed lacks rows (data_differs). It returns the last counts when ctx ends.
func (t Target) WaitReady(ctx context.Context, s Stream, end time.Time, window time.Duration, want float64) (loki, vl float64, err error) {
	for {
		loki, vl, err = t.Counts(ctx, s, end, window)
		if err == nil && loki >= want && loki == vl {
			return loki, vl, nil
		}
		select {
		case <-ctx.Done():
			if err == nil {
				err = fmt.Errorf("the Loki side holds %.0f lines, VictoriaLogs %.0f, %.0f wanted", loki, vl, want)
			}
			return loki, vl, err
		case <-time.After(3 * time.Second):
		}
	}
}
