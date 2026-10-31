package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	fj "github.com/valyala/fastjson"
)

func TestStatsJSONSeriesTransformDifferential(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	p.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, nil)
	query := `sum by (service_name, detected_level) (count_over_time({app="test"}[60s]))`
	series := `[{"metric":{"service.name":"api","level":"warn","empty":""},"values":[[1700000000,"1"],[1700000000.125,"NaN"],[1700000060,"3"]],"unknown":"kept-if-unchanged"},{"metric":{"_stream":"{app=\"worker\",level=\"error\"}"},"values":[[1700000000,"2"]]}]`
	for _, body := range []string{
		`{"status":"success","data":{"resultType":"matrix","result":` + series + `,"stats":{"rows":5},"extra":true},"warnings":["warning"],"extra":true}`,
		`{"status":"success","result":` + series + `,"error":"detail","extra":true}`,
		`{"status":"success","results":` + series + `,"extra":true}`,
		`{"status":"success","data":{"resultType":"matrix","result":` + series + `},"result":` + series + `,"results":` + series + `}`,
		`{"status":"success","data":{"resultType":"matrix","result":[]},"results":[]}`,
	} {
		for _, cutoff := range []int64{0, 1700000000_000000000, 1700000000_125000000, 1700000060_000000000} {
			t.Run(fmt.Sprintf("%d/%d", len(body), cutoff), func(t *testing.T) {
				keep := func(ts int64) bool { return ts > cutoff }
				got, _, handled := p.trimAndTranslateStatsQRBySeries([]byte(body), keep, query)
				if !handled {
					t.Fatal("valid fixture not handled by bounded path")
				}
				want := p.trimAndTranslateStatsQRFJFull(context.Background(), []byte(body), keep, query)
				if !statsJSONEquivalent(got, want) {
					t.Fatalf("fused mismatch\ngot %s\nwant %s", got, want)
				}
				got, handled = trimStatsQRBySeries([]byte(body), keep, nil)
				if !handled {
					t.Fatal("valid fixture not handled by trim path")
				}
				want = trimStatsQRByTimeFJShiftedFull([]byte(body), keep, nil)
				if !statsJSONEquivalent(got, want) {
					t.Fatalf("trim mismatch\ngot %s\nwant %s", got, want)
				}
			})
		}
	}
}

func statsJSONEquivalent(a, b []byte) bool {
	if !json.Valid(a) || !json.Valid(b) {
		return bytes.Equal(a, b)
	}
	// Label maps deliberately have no wire order. UseNumber keeps timestamp
	// and value number spelling precise rather than rounding via float64.
	decode := func(body []byte) interface{} {
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.UseNumber()
		var value interface{}
		if err := decoder.Decode(&value); err != nil {
			return string(body)
		}
		return value
	}
	return reflect.DeepEqual(decode(a), decode(b))
}

func TestStatsJSONSeriesTransformEdgeCases(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	p.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, nil)
	query := `sum by (detected_level) (count_over_time({app="test"}[60s]))`
	bodies := []string{
		`{broken`,
		`{"results":[]} trailing`,
		`{"status":"success","results":[null,5,"str",[]]}`,
		`{"status":"error","error":"backend detail","results":[{"metric":{"service.name":"api"},"value":[1700000000,"9"]}]}`,
		`{"status":null,"results":[{"metric":null,"values":[[],[null,"v"],[true,"v"],["2026-10-31T00:00:00.123456789Z","3"]]}]}`,
		`{"data":{"resultType":"matrix","result":[{"metric":{"app":"api"},"values":[[1700000000.1234567,"1"],[1700000000123456789,"2"],[1.7e9,"3"],[-1.5,"4"]]}]}}`,
		`{"data":{"result":[{"metric":{},"values":"not-array","extra":"present"}]}}`,
		`{"results":[{"metric":{"level":"","detected_level":"","__name__":"count"},"values":[]}]}`,
		`{"results":[{"metric":{"app":"api"},"values":[[1700000000,"1",{"nested":["]","\\\""]}],[1700000060,"2"]],"unknown":{"nested":[1,2,3]}}]}`,
		`{"results":[{"metric":{"service.name":"api"},"values":[[NaN,"1"]]}]}`,
		`{"\u0072esults":[{"metric":{"service.name":"api"},"values":[[1700000000,"1"]]}]}`,
		`{"results":[],"results":[{"metric":{"service.name":"api"},"values":[[1700000000,"1"]]}]}`,
		`{"results":[{"metric":{"app":"api"},"values":[[1700000000,"1"]]}],"results":[]}`,
	}
	for i, body := range bodies {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			assertStatsJSONSeriesMatchesReference(t, p, []byte(body), query)
		})
	}
}

func assertStatsJSONSeriesMatchesReference(t *testing.T, p *Proxy, body []byte, query string) {
	t.Helper()
	for _, keep := range []func(int64) bool{nil, func(int64) bool { return true }, func(ts int64) bool { return ts > 1700000000_000000000 }} {
		want := p.trimAndTranslateStatsQRFJFull(context.Background(), body, keep, query)
		got, _, handled := p.trimAndTranslateStatsQRBySeries(body, keep, query)
		if handled && !statsJSONEquivalent(got, want) {
			t.Fatalf("fused mismatch\ninput %s\ngot %s\nwant %s", body, got, want)
		}
		if keep == nil {
			continue // trim-only's contract requires keep.
		}
		for _, relabel := range []func(int64) int64{nil, func(ns int64) int64 { return ns + 125000000 }} {
			want = trimStatsQRByTimeFJShiftedFull(body, keep, relabel)
			got, handled = trimStatsQRBySeries(body, keep, relabel)
			if handled && !statsJSONEquivalent(got, want) {
				t.Fatalf("trim mismatch\ninput %s\ngot %s\nwant %s", body, got, want)
			}
		}
	}
}

func TestStatsJSONSeriesPreservesReferenceDepthBoundary(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	p.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, nil)
	for _, depth := range []int{fj.MaxDepth - 8, fj.MaxDepth - 3, fj.MaxDepth, fj.MaxDepth + 1} {
		body := `{"results":[{"metric":{"service.name":"api"},"values":[[1700000000,"1"]],"extra":` + strings.Repeat("[", depth) + `0` + strings.Repeat("]", depth) + `}]}`
		assertStatsJSONSeriesMatchesReference(t, p, []byte(body), `{app="test"}`)
	}
}

func TestStatsJSONSeriesNoopPreservesOriginalBody(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	body := []byte(`{ "status":"success", "data":{"resultType":"matrix","result":[{"metric":{"app":"api"},"values":[[1700000000,"1"]],"extra":42}]},"warnings":["preserved"] }`)
	got, operation, handled := p.trimAndTranslateStatsQRBySeries(body, nil, `{app="api"}`)
	if !handled || operation != "noop" || &got[0] != &body[0] {
		t.Fatal("no-op must preserve original bytes and report one noop operation")
	}
	got, handled = trimStatsQRBySeries(body, func(int64) bool { return true }, nil)
	if !handled || &got[0] != &body[0] {
		t.Fatal("trim no-op must preserve original bytes")
	}
}

func TestLargeStatsJSONRecordsOneOperation(t *testing.T) {
	body := buildStatsMatrixBody(12000, 2)
	for _, test := range []struct {
		style   LabelStyle
		outcome string
	}{{LabelStyleUnderscores, "ok"}, {LabelStylePassthrough, "noop"}} {
		p := newTestProxy(t, "http://unused")
		p.labelTranslator = NewLabelTranslator(test.style, nil)
		p.trimAndTranslateStatsQRFJ(context.Background(), body, nil, `{app="test"}`)
		recorder := httptest.NewRecorder()
		p.metrics.Handler(recorder, httptest.NewRequest("GET", "/metrics", nil))
		want := fmt.Sprintf("loki_vl_proxy_internal_operation_total{operation=%q,outcome=%q} 1\n", "trim_translate_stats_qr", test.outcome)
		if !strings.Contains(recorder.Body.String(), want) {
			t.Fatalf("expected exactly one %s operation per response", test.outcome)
		}
	}
}

func TestStatsJSONSeriesOutputSurvivesParserAndBufferReuse(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	p.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, nil)
	query := `{app="test"}`
	keep := func(ts int64) bool { return ts > 1700000000_000000000 }
	for _, transform := range []func([]byte) []byte{
		func(body []byte) []byte {
			out, _, _ := p.trimAndTranslateStatsQRBySeries(body, keep, query)
			return out
		},
		func(body []byte) []byte {
			out, _ := trimStatsQRBySeries(body, keep, nil)
			return out
		},
	} {
		body := buildStatsMatrixBody(2, 3)
		out := transform(body)
		want := bytes.Clone(out)
		if len(out) == 0 || bytes.Equal(out, body) {
			t.Fatal("fixture must rebuild a response")
		}
		for range 8 {
			transform(buildStatsMatrixBody(3, 6))
		}
		clear(body)
		if !bytes.Equal(out, want) {
			t.Fatal("returned output aliased input, parser or pooled buffer")
		}
	}
}

func FuzzStatsJSONSeriesTransformDifferential(f *testing.F) {
	for _, body := range []string{
		`{"data":{"resultType":"matrix","result":[{"metric":{"service.name":"api"},"values":[[1700000000,"1"],[1700000060,"2"]]}]}}`,
		`{"results":[{"metric":{"level":"error"},"values":[]}]}`,
		`{"results":[],"data":{"result":[]}}`,
		`{broken`,
	} {
		f.Add([]byte(body))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		if len(body) > 256<<10 {
			t.Skip()
		}
		p := newTestProxy(t, "http://unused")
		p.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, nil)
		assertStatsJSONSeriesMatchesReference(t, p, body, `sum by (detected_level) (count_over_time({app="test"}[60s]))`)
	})
}

func TestLargeStatsJSONAllocationBudget(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	p.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, nil)
	query := `sum by (service_name) (count_over_time({app="test"}[60s]))`
	for _, fixture := range []struct {
		name           string
		series, points int
		fusedBudget    float64
	}{{"many_points", 1000, 60, 20000}, {"many_short_series", 12000, 2, 50000}} {
		t.Run(fixture.name, func(t *testing.T) {
			body := buildStatsMatrixBody(fixture.series, fixture.points)
			if len(body) <= maxPooledStatsParserInputSize {
				t.Fatal("fixture must exercise large response dispatch")
			}
			for _, transform := range []struct {
				name   string
				budget float64
				run    func([]byte) []byte
			}{
				{"fused", fixture.fusedBudget, func(body []byte) []byte {
					return p.trimAndTranslateStatsQRFJ(context.Background(), body, nil, query)
				}},
				{"trim", 5000, func(body []byte) []byte {
					return trimStatsQRByTimeFJShifted(body, func(ts int64) bool { return ts > 1700000000_000000000 }, nil)
				}},
			} {
				t.Run(transform.name, func(t *testing.T) {
					var result []byte
					allocs := testing.AllocsPerRun(3, func() { result = transform.run(body) })
					if len(result) == 0 || bytes.Equal(result, body) {
						t.Fatal("expected rebuilt large response")
					}
					// The full-tree reference reconstructs over 100,000
					// allocations on these >1MiB inputs after bounded release.
					// Leave room for labels while rejecting that expansion.
					if allocs > transform.budget {
						t.Fatalf("large response allocated %.0f objects; budget %.0f", allocs, transform.budget)
					}
					t.Logf("large response: %.0f allocations (budget %.0f)", allocs, transform.budget)
				})
			}
		})
	}
}

func BenchmarkStatsJSONSeriesCompare(b *testing.B) {
	for _, fixture := range []struct {
		name           string
		series, points int
	}{{"small", 10, 60}, {"many_points", 1000, 60}, {"many_short_series", 12000, 2}} {
		body := buildStatsMatrixBody(fixture.series, fixture.points)
		for _, mode := range []string{"full_tree", "per_series"} {
			b.Run(fixture.name+"/"+mode, func(b *testing.B) {
				p := newUnderscokeProxy(b)
				b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
				query := `sum by (service_name) (count_over_time({app="test"}[60s]))`
				b.SetBytes(int64(len(body)))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if mode == "full_tree" {
						_ = p.trimAndTranslateStatsQRFJFull(b.Context(), body, nil, query)
					} else {
						_ = p.trimAndTranslateStatsQRFJ(b.Context(), body, nil, query)
					}
				}
			})
		}
	}
}
