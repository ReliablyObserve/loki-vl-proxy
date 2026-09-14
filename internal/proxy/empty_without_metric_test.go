package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestEmptyWithoutInstantPreservesSeries(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if query := r.FormValue("query"); !strings.Contains(query, "by (_stream, level)") {
			t.Errorf("missing stream grouping: %s", query)
		}
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"kind":"0"},"value":[1609459200,"1"]},{"metric":{"kind":"1"},"value":[1609459200,"1"]}]}}`))
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	for _, query := range []string{
		`sum without()(count_over_time({app="api"}[5m]))`,
		`sum(count_over_time({app="api"}[5m])) without()`,
		`sum without()(sum without()(count_over_time({app="api"}[5m])))`,
	} {
		t.Run(query, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/loki/api/v1/query?time=1609459200&query="+url.QueryEscape(query), nil)
			w := httptest.NewRecorder()
			p.handleQuery(w, r)
			if w.Code != 200 {
				t.Fatalf("status %d: %s", w.Code, w.Body)
			}
			samples, err := binarySamplesByTime(context.Background(), w.Body.Bytes())
			if err != nil || len(samples[1609459200]) != 2 {
				t.Fatalf("lost series: %s (%v)", w.Body, err)
			}
			for _, sample := range samples[1609459200] {
				if sample.value != 1 || (sample.labels["kind"] != "0" && sample.labels["kind"] != "1") {
					t.Fatalf("wrong sample: %+v", sample)
				}
			}
		})
	}
}

func TestEmptyWithoutDropsMetricNameAndSumsCollisions(t *testing.T) {
	body := []byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"a","kind":"0"},"value":[123,"2"]},{"metric":{"__name__":"b","kind":"0"},"value":[123,"3"]},{"metric":{"kind":"1"},"value":[123,"7"]}]}}`)
	got, err := sumWithoutMetricName(context.Background(), body)
	if err != nil || !json.Valid(got) {
		t.Fatalf("invalid output %s: %v", got, err)
	}
	samples, err := binarySamplesByTime(context.Background(), got)
	if err != nil || len(samples[123]) != 2 {
		t.Fatalf("wrong groups %s: %v", got, err)
	}
	for _, sample := range samples[123] {
		want := map[string]float64{"0": 5, "1": 7}[sample.labels["kind"]]
		if sample.value != want || len(sample.labels) != 1 {
			t.Fatalf("wrong collision sum: %+v", sample)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got, err := sumWithoutMetricName(ctx, body); err == nil || got != nil {
		t.Fatalf("canceled work returned %s, %v", got, err)
	}
}

func TestEmptyWithoutLeavesOtherAggregationsUnchanged(t *testing.T) {
	for _, query := range []string{`sum(count_over_time({a="b"}[5m]))`, `sum by()(count_over_time({a="b"}[5m]))`, `sum without(a)(count_over_time({a="b"}[5m]))`, `count without()(count_over_time({a="b"}[5m]))`} {
		if _, count := unwrapEmptySumWithout(query); count > 0 {
			t.Fatalf("unexpected rewrite: %s", query)
		}
	}
}

func TestEmptyWithoutEmptyLabelsPreserveInputGroups(t *testing.T) {
	body := []byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"kind":"0","optional":""},"value":[123,"2"]},{"metric":{"kind":"0"},"value":[123,"3"]}]}}`)
	ctx := binaryEvaluationContext(context.Background())
	first, err := sumWithoutMetricName(ctx, body)
	if err != nil {
		t.Fatal(err)
	}
	samples, err := binarySamplesByTime(ctx, first)
	if err != nil || len(samples[123]) != 2 {
		t.Fatalf("first grouping must retain distinct inputs: %s (%v)", first, err)
	}
	for _, sample := range samples[123] {
		if len(sample.labels) != 1 || sample.labels["kind"] != "0" {
			t.Fatalf("empty output label retained: %+v", sample)
		}
	}
	second, err := sumWithoutMetricName(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	samples, err = binarySamplesByTime(ctx, second)
	if err != nil || len(samples[123]) != 1 || samples[123][0].value != 5 {
		t.Fatalf("second aggregation must sum newly identical groups: %s (%v)", second, err)
	}
	_, count := unwrapEmptySumWithout(`sum without()(sum without()(count_over_time({a="b"}[5m])))`)
	if count != 2 {
		t.Fatalf("lost aggregation passes: %d", count)
	}
}
