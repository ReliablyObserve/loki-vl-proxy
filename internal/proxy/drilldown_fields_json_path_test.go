package proxy

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// drilldownJSONPathRows are JSON lines of one stream: a level key (error,
// info, warn), a nested service.name (two values) or none, and plain text.
func drilldownJSONPathRows(rng *rand.Rand, s0 time.Time, n int) []pushdownRow {
	rows := make([]pushdownRow, 0, n)
	for i := 0; i < n; i++ {
		row := pushdownRow{ts: s0.Add(time.Duration(i) * 3 * time.Second), stream: map[string]string{"service_name": "api-gateway"}}
		level := []string{"error", "info", "warn"}[rng.Intn(3)]
		switch rng.Intn(6) {
		case 0:
			row.msg = fmt.Sprintf(`{"level":%q,"n":%d}`, level, i) // no service key
		case 1:
			row.msg = fmt.Sprintf("plain line %d", i)
		case 2:
			row.msg = fmt.Sprintf(`{"level":%q,"service":{"name":"billing"},"n":%d}`, level, i)
		default:
			row.msg = fmt.Sprintf(`{"level":%q,"service":{"name":"api-gateway"},"n":%d}`, level, i)
		}
		rows = append(rows, row)
	}
	return rows
}

// lokiDrilldownJSONPathReference is Loki's answer to the Drilldown Fields
// breakdown `sum by (service_name_extracted) (count_over_time({...}
// [| detected_level="error"] | json service_name_extracted="[\"service\"][\"name\"]"
// | drop __error__, __error_details__ | service_name_extracted!="" [1m]))`:
// the json expression parser reads the nested key into the label
// (pkg/logql/log/parser.go:671 JSONExpressionParser.Process), detected_level
// is the line's level key (Loki's ingest), a line without the key has an
// empty label and the filter drops it.
func lokiDrilldownJSONPathReference(rows []pushdownRow, pill bool, start, end time.Time, step time.Duration) map[string]map[int64]string {
	out := map[string]map[int64]string{}
	for at := start; !at.After(end); at = at.Add(step) {
		counts := map[string]float64{}
		for _, row := range rows {
			if !row.ts.After(at.Add(-step)) || row.ts.After(at) {
				continue
			}
			var obj struct {
				Level   string `json:"level"`
				Service struct {
					Name string `json:"name"`
				} `json:"service"`
			}
			if json.Unmarshal([]byte(row.msg), &obj) != nil || obj.Service.Name == "" {
				continue
			}
			if pill && obj.Level != "error" {
				continue
			}
			counts[obj.Service.Name]++
		}
		for name, v := range counts {
			key := canonicalLabelsKey(map[string]string{"service_name_extracted": name})
			if out[key] == nil {
				out[key] = map[int64]string{}
			}
			out[key][at.Unix()] = strconv.FormatFloat(v, 'f', -1, 64)
		}
	}
	return out
}

// The Logs Drilldown Fields breakdown over a nested JSON field, with and
// without a level pill, answers Loki's series (Loki 1 series, the proxy none
// with the pill on 2026-11-04): random rows, both forms, equal to the
// Loki-semantics reference, from VictoriaLogs stats.
// conformance: drilldown/fields-pill-json-path-extracted-empty, loki_api_v1_query_range
func TestDrilldownFieldsJSONPathBreakdown_LikeLoki(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(time.Minute), s0.Add(8*time.Minute)
	for seed := int64(1); seed <= 6; seed++ {
		rows := drilldownJSONPathRows(rand.New(rand.NewSource(seed)), s0, 180)
		srv, fake := newPushdownFakeVL(t, rows, nil)
		p := newFilterPushdownProxy(t, srv.URL)
		fake.stored = p.labelTranslator.ToVL
		for _, pill := range []bool{true, false} {
			level := ""
			if pill {
				level = ` | detected_level="error"`
			}
			query := `sum by (service_name_extracted) (count_over_time({service_name="api-gateway"}` + level +
				` | json service_name_extracted="[\"service\"][\"name\"]" | drop __error__, __error_details__ | service_name_extracted!="" [1m]))`
			params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {"60"}}
			req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
			req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
			rec := httptest.NewRecorder()
			p.handleQueryRange(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("seed %d pill=%v: %d %s", seed, pill, rec.Code, rec.Body)
			}
			want := lokiDrilldownJSONPathReference(rows, pill, start, end, time.Minute)
			if len(want) == 0 {
				t.Fatalf("seed %d pill=%v: the reference has no series", seed, pill)
			}
			if got := decodeExtractedMatrix(t, rec.Body.Bytes()); !reflect.DeepEqual(got, want) {
				t.Errorf("seed %d pill=%v\n got: %v\nwant: %v", seed, pill, got, want)
			}
		}
		srv.Close()
	}
}
