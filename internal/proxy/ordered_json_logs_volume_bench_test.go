package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkOrderedJSONLogsVolume measures Grafana's `{...} | json` logs volume
// query over one hour of lines (one every 200ms) against the fake backend.
func BenchmarkOrderedJSONLogsVolume(b *testing.B) {
	benchmarkOrderedJSONQuery(b, `sum by (level, detected_level) (count_over_time({app="api"} | json | drop __error__[1m]))`)
}

// BenchmarkOrderedJSONRowEvaluator measures the row evaluator (a `without`
// grouping has no stats pushdown) over the same hour of lines.
func BenchmarkOrderedJSONRowEvaluator(b *testing.B) {
	benchmarkOrderedJSONQuery(b, `sum without (n, counter, msg) (count_over_time({app="api"} | json | drop __error__[1m]))`)
}

// BenchmarkOrderedMixedParserMetric measures the bounded row evaluator on the
// same one-hour fixture as the JSON-only benchmark, with the Drilldown stages.
func BenchmarkOrderedMixedParserMetric(b *testing.B) {
	benchmarkOrderedJSONQuery(b, `sum by (level) (count_over_time({app="api"} | json level="[\"level\"]" | logfmt | drop __error__, __error_details__ | level!="" [1m]))`)
}

func benchmarkOrderedJSONQuery(b *testing.B, query string) {
	s0 := time.Unix(1700000400, 0).UTC()
	lines := make([]jsonVolumeLine, 0, 18000)
	for i := 0; i < 18000; i++ {
		lines = append(lines, jsonVolumeFixtureLine(s0.Add(time.Duration(i)*200*time.Millisecond), i))
	}
	srv, _ := newJSONVolumeFakeVL(b, lines)
	p, err := New(Config{BackendURL: srv.URL, Cache: cache.NewDisabled(), LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	p.storeBackendVersion("v1.50.0", "v1.50.0")
	params := url.Values{
		"query": {query},
		"start": {strconv.FormatInt(s0.Add(time.Minute).Unix(), 10)},
		"end":   {strconv.FormatInt(s0.Add(time.Hour).Unix(), 10)},
		"step":  {"60"},
	}
	target := "/loki/api/v1/query_range?" + params.Encode()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, target, nil))
		if rec.Code != http.StatusOK {
			b.Fatalf("status %d: %s", rec.Code, rec.Body)
		}
	}
}

func BenchmarkOrderedMixedParserAdmission(b *testing.B) {
	q := `max(sum by (x)(count_over_time({app="api"} |~ "red|green|blue" | json | logfmt | x=~"alpha|bravo|charlie" [1m])))`
	r := fenceShapeRequest(q)
	b.Run("warm-response-fence", func(b *testing.B) {
		responseKeyFences(r, q, responseKeyCompatQueryRange)
		b.ReportAllocs()
		for b.Loop() {
			responseKeyFences(r, q, responseKeyCompatQueryRange)
		}
	})
	b.Run("cold-flag-normalization", func(b *testing.B) {
		flagged := strings.Replace(q, "| logfmt", "| logfmt --strict", 1)
		b.ReportAllocs()
		for b.Loop() {
			handlerText(flagged, defaultExecutionLimits().QueryLengthBytes)
		}
	})
}

func BenchmarkOrderedMixedParserSeriesOrder(b *testing.B) {
	const count = 4096
	series := make(map[string]manualSeriesSamples, count)
	for i := range count {
		labels := map[string]string{"tenant_id": strconv.Itoa(i), "region": strconv.Itoa(i % 8)}
		if i == 0 {
			labels = nil
		}
		series[canonicalLabelsKey(labels)] = manualSeriesSamples{Metric: labels}
	}
	b.Run("names-per-comparison", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			keys := make([]string, 0, len(series))
			for key := range series {
				keys = append(keys, key)
			}
			slices.SortFunc(keys, func(a, z string) int { return compareLabelSets(series[a].Metric, series[z].Metric) })
		}
	})
	b.Run("names-per-series", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			seriesKeysInLabelOrder(series, func(s manualSeriesSamples) map[string]string { return s.Metric })
		}
	})
}
