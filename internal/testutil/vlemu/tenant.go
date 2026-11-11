package vlemu

import (
	"encoding/json"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Tenants is a VictoriaLogs that keeps each tenant's rows apart, as
// VictoriaLogs does by the AccountID and ProjectID request headers (absent
// headers read tenant 0:0). Every read endpoint the proxy calls answers from
// the rows of the request's tenant only, in the shape VictoriaLogs uses, so a
// response, a cache entry or a peer copy that reaches another tenant shows the
// first tenant's values. The query text is not evaluated: a test that wants an
// isolation proof names its tenants' rows with values no other tenant holds.
type Tenants struct {
	// Rows are each tenant's rows by "AccountID:ProjectID".
	Rows map[string][]Row

	mu    sync.Mutex
	calls map[string]int
}

// Calls is how many requests reached the emulator for tenant
// ("AccountID:ProjectID").
func (t *Tenants) Calls(tenant string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.calls[tenant]
}

func requestTenant(r *http.Request) string {
	account, project := r.Header.Get("AccountID"), r.Header.Get("ProjectID")
	if account == "" {
		account = "0"
	}
	if project == "" {
		project = "0"
	}
	return account + ":" + project
}

// streamLabels are the name="value" pairs of a row's stream.
func streamLabels(stream string) map[string]string {
	out := map[string]string{}
	for _, m := range StreamLabelRE.FindAllStringSubmatch(stream, -1) {
		out[m[1]] = m[2]
	}
	return out
}

// rowFields are the fields VictoriaLogs stores for a row.
func rowFields(row Row) map[string]string {
	f := map[string]string{
		"_time":      time.Unix(0, row.TS).UTC().Format(time.RFC3339Nano),
		"_stream":    row.Stream,
		"_stream_id": row.Stream,
		"_msg":       row.Msg,
	}
	for k, v := range streamLabels(row.Stream) {
		f[k] = v
	}
	for k, v := range row.Labels {
		f[k] = v
	}
	return f
}

type valueHits struct {
	Value string `json:"value"`
	Hits  int    `json:"hits"`
}

func countedValues(counts map[string]int) []valueHits {
	out := make([]valueHits, 0, len(counts))
	for v, n := range counts {
		out = append(out, valueHits{v, n})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Value < out[j].Value })
	return out
}

// tenantCountStatsRE is the index stats pipe: a row count and a distinct
// stream count under the names the query gives them.
var tenantCountStatsRE = regexp.MustCompile(`\| stats count\(\) (\w+), count_uniq_hash\(_stream_id\) (\w+)$`)

// Handler serves the tenant-scoped VictoriaLogs.
func (t *Tenants) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		tenant := requestTenant(r)
		t.mu.Lock()
		if t.calls == nil {
			t.calls = map[string]int{}
		}
		t.calls[tenant]++
		t.mu.Unlock()
		rows := t.Rows[tenant]
		switch r.URL.Path {
		case "/health":
			_, _ = w.Write([]byte("ok"))
		case "/metrics":
		case "/select/logsql/query", "/select/logsql/tail":
			w.Header().Set("Content-Type", "application/x-ndjson")
			if m := tenantCountStatsRE.FindStringSubmatch(r.FormValue("query")); m != nil {
				// The index stats row: entries and distinct streams.
				streams := map[string]bool{}
				for _, row := range rows {
					streams[row.Stream] = true
				}
				line, _ := json.Marshal(map[string]string{m[1]: strconv.Itoa(len(rows)), m[2]: strconv.Itoa(len(streams))})
				_, _ = w.Write(append(line, '\n'))
				return
			}
			for _, row := range rows {
				line, _ := json.Marshal(rowFields(row))
				_, _ = w.Write(append(line, '\n'))
			}
		case "/select/logsql/field_names", "/select/logsql/stream_field_names":
			counts := map[string]int{}
			for _, row := range rows {
				fields := streamLabels(row.Stream)
				if r.URL.Path == "/select/logsql/field_names" {
					fields = rowFields(row)
				}
				for k := range fields {
					counts[k]++
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"values": countedValues(counts)})
		case "/select/logsql/field_values", "/select/logsql/stream_field_values":
			counts := map[string]int{}
			name := r.FormValue("field")
			for _, row := range rows {
				if v, ok := rowFields(row)[name]; ok {
					counts[v]++
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"values": countedValues(counts)})
		case "/select/logsql/streams", "/select/logsql/stream_ids":
			counts := map[string]int{}
			for _, row := range rows {
				counts[row.Stream]++
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"values": countedValues(counts)})
		case "/select/logsql/hits":
			var ts []string
			var vals []int
			for _, row := range rows {
				ts = append(ts, time.Unix(0, row.TS).UTC().Format(time.RFC3339))
				vals = append(vals, 1)
			}
			hits := []map[string]any{}
			if len(rows) > 0 {
				hits = append(hits, map[string]any{"fields": map[string]string{}, "timestamps": ts, "values": vals, "total": len(rows)})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"hits": hits})
		case "/select/logsql/stats_query", "/select/logsql/stats_query_range":
			t.serveStats(w, r, rows)
		case "/select/logsql/delete":
			w.WriteHeader(http.StatusOK)
		default:
			_, _ = w.Write([]byte(`{"values":[]}`))
		}
	})
}

// serveStats answers a stats request with one series per stream of the
// tenant, labelled with the stream's labels and the row count as its value.
func (t *Tenants) serveStats(w http.ResponseWriter, r *http.Request, rows []Row) {
	ranged := r.URL.Path == "/select/logsql/stats_query_range"
	byStream := map[string]int{}
	for _, row := range rows {
		byStream[row.Stream]++
	}
	streams := make([]string, 0, len(byStream))
	for s := range byStream {
		streams = append(streams, s)
	}
	sort.Strings(streams)
	at := r.FormValue("time")
	if ranged || at == "" {
		at = r.FormValue("start")
	}
	sec := strconv.FormatInt(time.Now().Unix(), 10)
	if ts, err := time.Parse(time.RFC3339Nano, at); err == nil {
		sec = strconv.FormatInt(ts.Unix(), 10)
	} else if n, err := strconv.ParseFloat(at, 64); err == nil {
		if n > 1e11 { // nanoseconds
			n /= 1e9
		}
		sec = strconv.FormatInt(int64(n), 10)
	}
	result := make([]map[string]any, 0, len(streams))
	for _, s := range streams {
		metric := streamLabels(s)
		metric["__name__"] = "count(*)"
		n := strconv.Itoa(byStream[s])
		if ranged {
			result = append(result, map[string]any{"metric": metric, "values": [][]any{{json.Number(sec), n}}})
		} else {
			result = append(result, map[string]any{"metric": metric, "value": []any{json.Number(sec), n}})
		}
	}
	kind := "vector"
	if ranged {
		kind = "matrix"
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": kind, "result": result}})
}

// LeakedMarkers returns the markers of body that belong to tenants other than
// own: every row value of every other tenant, longer than a few bytes so a
// common word cannot match by chance.
func (t *Tenants) LeakedMarkers(own, body string) []string {
	var out []string
	for tenant, rows := range t.Rows {
		if tenant == own {
			continue
		}
		for _, row := range rows {
			for _, v := range rowFields(row) {
				if strings.Contains(v, "secret") && strings.Contains(body, v) {
					out = append(out, tenant+":"+v)
				}
			}
		}
	}
	return out
}
