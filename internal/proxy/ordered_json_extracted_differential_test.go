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
	"strings"
	"testing"
	"time"
)

// extractedDifferentialRows draws rows whose stream carries a level label or
// not, structured metadata under another spelling or not, and whose bodies are
// the shapes that tell Loki's parser from unpack_json: valid with and without
// level, the literal level_extracted key, both keys in either order, a nested
// object under level, an array, a repeated key, a key before a syntax error,
// plain text and logfmt. Seeds cycle through three row sets (see
// extractedDifferentialRows) so the stats pushdown is exercised on clean data
// and each kind of risky line meets its probe.
func extractedDifferentialRows(rng *rand.Rand, s0 time.Time, n int, mode int) []pushdownRow {
	levels := []string{"error", "warn", "info", "debug"}
	pick := func() string { return levels[rng.Intn(len(levels))] }
	rows := make([]pushdownRow, 0, n)
	for i := 0; i < n; i++ {
		row := pushdownRow{ts: s0.Add(time.Duration(i) * 2 * time.Second), stream: map[string]string{"app": "api"}}
		if rng.Intn(2) == 0 {
			row.stream["level"] = pick()
		}
		if rng.Intn(10) < 3 {
			v := fmt.Sprintf("0.%d.0", rng.Intn(3))
			row.vl, row.loki = map[string]string{"service.version": v}, map[string]string{"service_version": v}
		}
		kind := rng.Intn(14)
		// mode 0 holds no line the two parsers could read differently, mode 1
		// adds the lines holding both keys, mode 2 every shape.
		for (mode == 0 && kind >= 6 && kind <= 11) || (mode == 1 && kind >= 8 && kind <= 11) {
			kind = rng.Intn(14)
		}
		switch kind {
		case 0, 1, 2:
			row.msg = fmt.Sprintf(`{"level":%q,"pipeline":"logs/loki","n":%d}`, pick(), i)
		case 3, 4:
			row.msg = fmt.Sprintf(`{"pipeline":"logs/loki","n":%d}`, i)
		case 5:
			row.msg = fmt.Sprintf(`{"level_extracted":"lit%d","pipeline":"logs/loki"}`, rng.Intn(2))
		case 6:
			row.msg = fmt.Sprintf(`{"level":%q,"level_extracted":"lit%d"}`, pick(), rng.Intn(2))
		case 7:
			row.msg = fmt.Sprintf(`{"level_extracted":"lit%d","level":%q}`, rng.Intn(2), pick())
		case 8:
			row.msg = `{"level":{"x":"y"},"pipeline":"logs/loki"}`
		case 9:
			row.msg = `{"level":["a","b"],"pipeline":"logs/loki"}`
		case 10:
			row.msg = fmt.Sprintf(`{"level":%q,"pipeline":"logs/loki","level":%q}`, pick(), pick())
		case 11:
			row.msg = fmt.Sprintf(`{"level":%q,"msg": truncated %d`, pick(), i)
		case 12:
			row.msg = fmt.Sprintf("plain text line %d", i)
		default:
			row.msg = fmt.Sprintf("level=%s op=update n=%d", pick(), i)
		}
		rows = append(rows, row)
	}
	return rows
}

func extractedDifferentialQuery(rng *rand.Rand) string {
	fn := []string{"count_over_time", "rate", "bytes_over_time", "bytes_rate"}[rng.Intn(4)]
	window := []string{"1m", "2m"}[rng.Intn(2)]
	op := []string{"=", "!=", "=~", "!~"}[rng.Intn(4)]
	value := []string{"error", "lit0", "warn", ""}[rng.Intn(4)]
	if strings.Contains(op, "~") {
		value = []string{"err.*|lit.*", ".*", "", "warn|info"}[rng.Intn(4)]
	}
	filter := fmt.Sprintf(` | level_extracted%s%s`, op, strconv.Quote(value))
	if rng.Intn(2) == 0 {
		filter = ` | drop __error__` + filter
	}
	inner := fmt.Sprintf(`%s({app="api"} | json%s [%s])`, fn, filter, window)
	switch rng.Intn(4) {
	case 0:
		return "sum(" + inner + ")"
	case 1:
		return "sum by (level) (" + inner + ")"
	case 2:
		return "sum by (level_extracted) (" + inner + ")"
	}
	return "sum by (level, level_extracted) (" + inner + ")"
}

// A differential run of the name_extracted metrics: random rows and queries,
// every answer equal to the Loki-semantics reference evaluator (the raw
// evaluator whose parser mirrors Loki's). The stats pushdown answers with one
// stats call and no raw rows, or a parse-risk probe keeps the raw evaluator;
// both must equal the reference, and a query Loki fails (a parser error
// reaches the aggregation) must fail here too.
//
// Two differences are declared, not tested: a body holding both name and
// name_extracted on a stream with name (Loki keeps the first in line order),
// and a stream label with metadata of the same name (Loki renames the
// metadata); see semantics/extracted-metric-known-differences.
// conformance: parser-error-and-label-collision, semantics/json-extracted-metric-pushdown, semantics/extracted-metric-known-differences
func TestOrderedJSONExtractedMetricDifferential(t *testing.T) {
	seeds, perSeed := 50, 12
	if testing.Short() {
		seeds = 12
	}
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(10*time.Minute)
	pushed, flagged, failed := 0, 0, 0
	for seed := int64(1); seed <= int64(seeds); seed++ {
		rng := rand.New(rand.NewSource(seed))
		rows := extractedDifferentialRows(rng, s0, 300, int(seed%3))
		srv, fake := newPushdownFakeVL(t, rows, nil)
		p := newFilterPushdownProxy(t, srv.URL)
		fake.stored = p.labelTranslator.ToVL
		seen := map[string]bool{}
		for q := 0; q < perSeed; q++ {
			query := extractedDifferentialQuery(rng)
			if seen[query] {
				continue // the response cache would answer a repeated query
			}
			seen[query] = true
			plan, ok := compileOrderedJSONMetric(query)
			if !ok {
				t.Fatalf("seed %d: %s does not compile to an ordered JSON plan", seed, query)
			}
			want, refErr := lokiPushdownReferenceErr(plan, rows, start, end, time.Minute)
			fake.mu.Lock()
			fake.raw, fake.stats, fake.guards = 0, nil, nil
			fake.mu.Unlock()
			params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {"60"}}
			rec := httptest.NewRecorder()
			p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
			fake.mu.Lock()
			raw, stats := fake.raw, len(fake.stats)
			fake.mu.Unlock()
			label := fmt.Sprintf("seed %d: %s", seed, query)
			if refErr != nil {
				failed++
				if rec.Code != http.StatusBadRequest {
					t.Errorf("%s: Loki fails this query (%v), the proxy answered %d", label, refErr, rec.Code)
				}
				continue
			}
			if rec.Code != http.StatusOK {
				t.Errorf("%s: status %d: %s", label, rec.Code, rec.Body)
				continue
			}
			got := decodeExtractedMatrix(t, rec.Body.Bytes())
			if !reflect.DeepEqual(got, want) {
				t.Errorf("%s (stats=%d raw=%d)\n got: %v\nwant: %v", label, stats, raw, got, want)
			}
			switch {
			case plan.pushdown && raw == 0 && stats == 1:
				pushed++
			case raw >= 1:
				flagged++
			default:
				t.Errorf("%s: neither one stats call without raw rows nor the raw evaluator: stats=%d raw=%d", label, stats, raw)
			}
		}
	}
	t.Logf("pushed down: %d, raw evaluator (probe or ineligible): %d, queries Loki fails: %d", pushed, flagged, failed)
	if pushed < seeds*perSeed/8 {
		t.Fatalf("only %d queries took the stats pushdown; the differential run no longer exercises it", pushed)
	}
}

func decodeExtractedMatrix(t *testing.T, body []byte) map[string]map[int64]string {
	t.Helper()
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("invalid matrix: %v: %s", err, body)
	}
	got := map[string]map[int64]string{}
	for _, series := range resp.Data.Result {
		key := canonicalLabelsKey(series.Metric)
		if got[key] == nil {
			got[key] = map[int64]string{}
		}
		for _, pair := range series.Values {
			ts, _ := pair[0].(float64)
			got[key][int64(ts)], _ = pair[1].(string)
		}
	}
	return got
}
