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

// The Logs Drilldown Fields breakdown over a nested JSON field, with and
// without a level pill, answers Loki's series (Loki 1 series, the proxy none
// with the pill on 2026-11-04): random rows, both forms, equal to Loki's
// answer recorded with Loki's engine over the same lines (test/oracle/loki
// engine; LVP_RUN_ORACLES=1 records again), from VictoriaLogs stats.
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
			want := lokiPushdownMatrix(t, "drilldown_fields_json_path", drilldownJSONPathLokiRows(rows), query, start, end, time.Minute)
			if len(want) == 0 {
				t.Fatalf("seed %d pill=%v: Loki answers no series", seed, pill)
			}
			if got := decodeExtractedMatrix(t, rec.Body.Bytes()); !reflect.DeepEqual(got, want) {
				t.Errorf("seed %d pill=%v\n got: %v\nwant: %v", seed, pill, got, want)
			}
		}
		srv.Close()
	}
}

// drilldownJSONPathLokiRows are the rows as Loki stores them: a line's detected_level is structured metadata Loki
// derives at ingest, the level key of a JSON line, and "unknown" for a line that names no level.
func drilldownJSONPathLokiRows(rows []pushdownRow) []pushdownRow {
	out := make([]pushdownRow, len(rows))
	for i, r := range rows {
		level := "unknown"
		var obj struct {
			Level string `json:"level"`
		}
		if json.Unmarshal([]byte(r.msg), &obj) == nil && obj.Level != "" {
			level = obj.Level
		}
		r.loki = map[string]string{"detected_level": level}
		out[i] = r
	}
	return out
}
