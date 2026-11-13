package querycorpus_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// The proxy side of the Loki differential: the real query_range handler of a proxy built with proxy.New, in front
// of a VictoriaLogs stand-in that answers /select/logsql/query with the rows VictoriaLogs' own storage returned for
// a text (the store oracle's answer, recorded) and every metadata endpoint with nothing (a proxy that has learned no
// field inventory yet). The handler turns those rows into Loki streams exactly as it does in production: stream
// labels, structured metadata, parsed labels, the line, and every proxy-side step after the rows (line_format,
// decolorize, drop and keep). The stand-in also records the query text the handler sends.

// diffStart is the time of entry 0 of every data set; entry i is at diffStart plus i nanoseconds (the store oracle
// stores its row i there).
var diffStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// diffVL is the VictoriaLogs stand-in.
type diffVL struct {
	version string
	mu      sync.Mutex
	rows    []map[string]string
	sent    []string
}

func (s *diffVL) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	switch r.URL.Path {
	case "/health":
		_, _ = io.WriteString(w, "OK")
	case "/metrics":
		_, _ = io.WriteString(w, `vm_app_version{version="victoria-logs-20260101-000000-tags-`+s.version+`-0-g0000000", short_version="`+s.version+`"} 1`+"\n")
	case "/select/logsql/query":
		s.mu.Lock()
		s.sent = append(s.sent, r.Form.Get("query"))
		rows := s.rows
		s.mu.Unlock()
		_, _ = w.Write(serveDiffRows(rows, r.Form))
	case "/select/logsql/hits":
		_, _ = io.WriteString(w, `{"hits":[]}`)
	case "/select/logsql/stats_query_range":
		_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
	case "/select/logsql/stats_query":
		_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"vector","result":[]}}`)
	default:
		_, _ = io.WriteString(w, `{"values":[]}`)
	}
}

// serveDiffRows is VictoriaLogs' answer over rows: those inside the request's start and end (inclusive), in the
// order the query's last sort asks for, at most limit, one JSON object per line.
func serveDiffRows(rows []map[string]string, form url.Values) []byte {
	start, end := parseDiffTime(form.Get("start"), 0), parseDiffTime(form.Get("end"), 1<<62)
	type row struct {
		ts int64
		m  map[string]string
	}
	var in []row
	for _, m := range rows {
		ts, err := time.Parse(time.RFC3339Nano, m["_time"])
		if err != nil {
			continue
		}
		if n := ts.UnixNano(); n >= start && n <= end {
			in = append(in, row{n, m})
		}
	}
	asc := strings.HasSuffix(strings.TrimSpace(form.Get("query")), "sort by (_time)")
	sort.SliceStable(in, func(i, j int) bool {
		if asc {
			return in[i].ts < in[j].ts
		}
		return in[i].ts > in[j].ts
	})
	if limit, err := strconv.Atoi(form.Get("limit")); err == nil && limit > 0 && len(in) > limit {
		in = in[:limit]
	}
	var b bytes.Buffer
	for _, r := range in {
		// VictoriaLogs' HTTP answer leaves the fields without a value out of each row.
		m := make(map[string]string, len(r.m))
		for k, v := range r.m {
			if v != "" {
				m[k] = v
			}
		}
		line, _ := json.Marshal(m)
		b.Write(line)
		b.WriteByte('\n')
	}
	return b.Bytes()
}

// parseDiffTime reads a VictoriaLogs time parameter as the handler writes it (nanoseconds, seconds or RFC 3339).
func parseDiffTime(v string, def int64) int64 {
	if v == "" {
		return def
	}
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		if n < 1e12 {
			return n * 1e9
		}
		return n
	}
	if ts, err := time.Parse(time.RFC3339Nano, v); err == nil {
		return ts.UnixNano()
	}
	return def
}

// diffResponseEntry is one entry of the handler's answer: its labels (the stream's, and for a categorized answer
// the entry's structured metadata and parsed labels apart) and its line.
type diffResponseEntry struct {
	line     string
	stream   map[string]string
	metadata map[string]string
	parsed   map[string]string
}

// diffHandlerRun is one request through a fresh proxy.
type diffHandlerRun struct {
	status  int
	body    string
	sent    []string
	entries map[int]diffResponseEntry
	// unmapped counts the entries whose timestamp is no row of the data set; repeated those that came twice.
	unmapped, repeated int
}

// runDiffHandler serves q from a fresh proxy of the profile over rows; categorized asks for Loki's categorized
// labels (structured metadata and parsed labels apart from the stream's).
func runDiffHandler(t *testing.T, prof diffProfile, q string, rows []map[string]string, categorized bool) diffHandlerRun {
	t.Helper()
	vl := &diffVL{version: prof.version, rows: rows}
	srv := httptest.NewServer(vl)
	defer srv.Close()
	p, err := proxy.New(proxy.Config{
		BackendURL:             srv.URL,
		Cache:                  cache.New(time.Minute, 1000),
		LogLevel:               "error",
		LabelStyle:             prof.style,
		MetadataFieldMode:      prof.mode,
		EmitStructuredMetadata: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	_ = p.ValidateBackendVersionCompatibility(context.Background())
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	form := url.Values{
		"query":     {q},
		"start":     {strconv.FormatInt(diffStart.Add(-time.Minute).Unix(), 10)},
		"end":       {strconv.FormatInt(diffStart.Add(time.Minute).Unix(), 10)},
		"limit":     {"5000"},
		"direction": {"backward"},
	}
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+form.Encode(), nil)
	req.Header.Set("X-Scope-OrgID", "0")
	if categorized {
		req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
	}
	rec := httptest.NewRecorder()
	vl.mu.Lock()
	vl.sent = nil
	vl.mu.Unlock()
	mux.ServeHTTP(rec, req)
	vl.mu.Lock()
	sent := append([]string(nil), vl.sent...)
	vl.mu.Unlock()
	out := diffHandlerRun{status: rec.Code, body: rec.Body.String(), sent: sent, entries: map[int]diffResponseEntry{}}
	if rec.Code == http.StatusOK {
		if err := out.decode(rec.Body.Bytes()); err != nil {
			out.status = -1
			out.body = err.Error()
		}
	}
	return out
}

func (run *diffHandlerRun) decode(body []byte) error {
	var resp struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Stream map[string]string   `json:"stream"`
				Values [][]json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return err
	}
	if resp.Data.ResultType != "streams" {
		return fmt.Errorf("result type %q", resp.Data.ResultType)
	}
	for _, s := range resp.Data.Result {
		for _, v := range s.Values {
			if len(v) < 2 {
				return fmt.Errorf("value of %d elements", len(v))
			}
			var ts, line string
			if err := json.Unmarshal(v[0], &ts); err != nil {
				return err
			}
			if err := json.Unmarshal(v[1], &line); err != nil {
				return err
			}
			e := diffResponseEntry{line: line, stream: s.Stream}
			if len(v) > 2 {
				var meta struct {
					StructuredMetadata map[string]string `json:"structuredMetadata"`
					Parsed             map[string]string `json:"parsed"`
				}
				if err := json.Unmarshal(v[2], &meta); err != nil {
					return err
				}
				e.metadata, e.parsed = meta.StructuredMetadata, meta.Parsed
			}
			n, err := strconv.ParseInt(ts, 10, 64)
			idx := n - diffStart.UnixNano()
			if err != nil || idx < 0 || idx >= 1<<20 {
				run.unmapped++
				continue
			}
			if _, dup := run.entries[int(idx)]; dup {
				run.repeated++
				continue
			}
			run.entries[int(idx)] = e
		}
	}
	return nil
}

// diffSentText is the LogsQL text of the handler's first log query call, without the sort the handler appends for
// the direction; "" when it sent none.
func diffSentText(sent []string) string {
	if len(sent) == 0 {
		return ""
	}
	s := strings.TrimSpace(sent[0])
	for _, suffix := range []string{" | sort by (_time desc)", " | sort by (_time)"} {
		s = strings.TrimSuffix(s, suffix)
	}
	return s
}
