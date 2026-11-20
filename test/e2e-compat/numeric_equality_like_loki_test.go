//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestCompat_NumericEqualityLikeLoki: a numeric, duration or size equality after a json/logfmt parser keeps the lines Loki
// keeps, for each class of value the query plan reads like Loki: the spellings of the value both read, a size or a
// duration where a number is asked (and the other way round), hexadecimal, binary and octal integers, the unit
// spellings VictoriaLogs does not read (`1kb`, `1 KB`, `5000000us`), and a value Loki marks with __error__.
// Each parser has a unique dual-written fixture. Queries compare full log entries, samples and pipeline errors
// under metric wrappers, binary operands, offsets and evaluated-window gaps. Six cases assert the exact accepted
// default series-identity projection rather than claim exact Loki parity. Present-empty, overflow and key-collision
// axes remain counted by the full storage oracle (semantics/label-filter-numeric-equality).
//
// conformance: semantics/label-filter-numeric-equality, semantics/label-filter-numeric-error-label, loki_api_v1_query_range
func TestCompat_NumericEqualityLikeLoki(t *testing.T) {
	for _, parser := range []string{"json", "logfmt"} {
		t.Run(parser, func(t *testing.T) { testCompatNumericEqualityLikeLoki(t, parser) })
	}
}

func testCompatNumericEqualityLikeLoki(t *testing.T, parser string) {
	probe := fmt.Sprintf("numeric-eq-%d", time.Now().UnixNano())
	stamp := time.Now().Add(-3 * time.Minute).Truncate(time.Minute).Add(time.Second)
	values := map[string][]string{
		"n": {"200", "200.0", "2e2", "0200", "2_00", "1KB", "1000ns", "0xC8", "0b11001000", "0o310", "abc", "201", "NaN", "+Inf", "-Infinity", "probe-preserve-true", "probe-preserve-custom"},
		"d": {"5s", "5000ms", "5000000us", "5000000μs", "+5s", "05s", "5000000000", "1GB", "1m30s", "4s1000ms", "abc"},
		"b": {"1KB", "1kb", "1 KB", "1Kb", "1000", "1,000", "001KB", "1KB ", "1µs", "1_000", "0x3e8", "1e3", "2KB"},
	}
	var vlRows []string
	var lokiValues [][]any
	evaluation := stamp.Truncate(time.Minute).Add(time.Minute)
	var gapValues [][]any
	errorRow := map[string]int{}
	sourceRows := map[string]int{}
	i := 0
	for _, key := range []string{"n", "d", "b"} {
		for _, v := range values[key] {
			if v == "abc" || key == "b" && v == "1_000" {
				errorRow[key] = i
			}
			ts := stamp.Add(time.Duration(i) * 100 * time.Millisecond)
			b, _ := json.Marshal(map[string]string{key: v, "row": strconv.Itoa(i)})
			line := string(b)
			if key == "n" && v == "200.0" {
				line = fmt.Sprintf(`{"n":"200.0","row":%q,"__lvp_numeric_error":"user-value"}`, strconv.Itoa(i))
			}
			if key == "n" && v == "200" {
				sourceRows["custom-error"] = i
				// In this numeric-filter pipeline Loki retains these source
				// labels too (recorded process oracle and live HTTP response).
				line = fmt.Sprintf(`{"n":"200","row":%q,"__error__":"custom_source_error","__error_details__":"source"}`, strconv.Itoa(i))
			}
			if parser == "logfmt" {
				line = key + "=" + strconv.Quote(v) + " row=" + strconv.Quote(strconv.Itoa(i))
				if key == "n" && v == "200.0" {
					line += ` __lvp_numeric_error="user-value"`
				}
				if key == "n" && v == "200" {
					line += ` __error__="custom_source_error" __error_details__="source"`
				}
			}
			if strings.HasPrefix(v, "probe-preserve-") {
				preserve := strings.TrimPrefix(v, "probe-preserve-")
				sourceRows["preserve-"+preserve] = i
				if parser == "json" {
					line = fmt.Sprintf(`{"n":%q,"row":%q,"__preserve_error__":%q}`, v, strconv.Itoa(i), preserve)
				} else {
					line += " __preserve_error__=" + strconv.Quote(preserve)
				}
			}
			row, _ := json.Marshal(map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": line, "app": "numeric-eq", "probe": probe})
			vlRows = append(vlRows, string(row))
			lokiValues = append(lokiValues, []any{strconv.FormatInt(ts.UnixNano(), 10), line})
			i++
		}
	}
	for j, value := range []string{"200", "abc", "200"} {
		ts := evaluation.Add(-time.Second)
		if j > 0 {
			ts = evaluation.Add(time.Duration(19+j) * time.Second)
		}
		line := fmt.Sprintf(`{"n":%q}`, value)
		if parser == "logfmt" {
			line = "n=" + strconv.Quote(value)
		}
		if j == 2 {
			if parser == "json" {
				line = `{"n":"200","__error__":"custom_source_error"}`
			} else {
				line += ` __error__="custom_source_error"`
			}
		}
		row, _ := json.Marshal(map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": line, "app": "numeric-eq", "probe": probe + "-gap"})
		vlRows = append(vlRows, string(row))
		gapValues = append(gapValues, []any{strconv.FormatInt(ts.UnixNano(), 10), line})
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app,probe",
		strings.Join(vlRows, "\n")+"\n", map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VictoriaLogs ingest: %d %s", status, body)
	}
	push, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{
		"stream": map[string]string{"app": "numeric-eq", "probe": probe}, "values": lokiValues,
	}, map[string]any{
		"stream": map[string]string{"app": "numeric-eq", "probe": probe + "-gap"}, "values": gapValues,
	}}})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(push), map[string]string{"Content-Type": "application/json"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)
	headers := map[string]string{"X-Scope-OrgID": "0"}
	window := func(query string) url.Values {
		return url.Values{"query": {query}, "start": {strconv.FormatInt(stamp.Add(-time.Minute).UnixNano(), 10)},
			"end": {strconv.FormatInt(stamp.Add(time.Minute).UnixNano(), 10)}, "limit": {"1000"}}
	}
	lines := func(resp map[string]interface{}) []string {
		var out []string
		for _, s := range extractArray(extractMap(resp, "data"), "result") {
			stream := s.(map[string]interface{})
			v, _ := stream["values"].([]interface{})
			for _, pair := range v {
				if p, _ := pair.([]interface{}); len(p) >= 2 {
					// JSON map keys are sorted by encoding/json; stream order
					// is not part of the query contract. Preserve every entry field.
					row, err := json.Marshal([]any{stream["stream"], p})
					if err != nil {
						t.Fatal(err)
					}
					out = append(out, string(row))
				}
			}
		}
		sort.Strings(out)
		return out
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		all := window(fmt.Sprintf(`{probe=%q}`, probe))
		_, _, lp := queryRangeGET(t, lokiURL, all, headers)
		_, _, pp := queryRangeGET(t, proxyURL, all, headers)
		if len(lines(lp)) == i && len(lines(pp)) == i {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fixture not visible on both sides: Loki %d, proxy %d lines", len(lines(lp)), len(lines(pp)))
		}
		time.Sleep(time.Second)
	}
	for _, filter := range []string{
		`n = 200`, `n == 2e2`, `n != 200`, `d = 5s`, `d != 5s`, `d == 5000ms`, `b = 1KB`, `b != 1KB`, `b == 1000B`,
	} {
		t.Run(filter, func(t *testing.T) {
			query := fmt.Sprintf(`{probe=%q} | %s | %s`, probe, parser, filter)
			ls, lb, lp := queryRangeGET(t, lokiURL, window(query), headers)
			ps, pb, pp := queryRangeGET(t, proxyURL, window(query), headers)
			if ls != http.StatusOK || ps != http.StatusOK {
				t.Fatalf("%q: Loki=%d %s proxy=%d %s", query, ls, lb, ps, pb)
			}
			if l, p := strings.Join(lines(lp), "\n"), strings.Join(lines(pp), "\n"); l != p {
				t.Errorf("%q: proxy %d lines, Loki %d\nproxy %s\n Loki %s", query, len(lines(pp)), len(lines(lp)), p, l)
			}
		})
	}
	// Select exactly one invalid-conversion row before the numeric filter.
	// Loki's first failing series is otherwise nondeterministic when many
	// rows fail. Reuse this test's dual-written fixture for metric nestings.
	for _, filter := range []string{`n = 200`, `n != 200`, `d = 5s`, `d != 5s`, `b = 1KB`, `b != 1KB`} {
		key := filter[:1]
		pipeline := fmt.Sprintf(`{probe=%q} | %s | row=%q | %s`, probe, parser, strconv.Itoa(errorRow[key]), filter)
		cleanCount := fmt.Sprintf(`count_over_time({probe=%q} | %s | row="1" | n=200 [1m])`, probe, parser)
		for _, metric := range numericEqualityMetricShapes(pipeline, cleanCount) {
			t.Run(metric, func(t *testing.T) {
				params := url.Values{"query": {metric},
					"start": {strconv.FormatInt(stamp.Add(10*time.Second).UnixNano(), 10)},
					"end":   {strconv.FormatInt(stamp.Add(20*time.Second).UnixNano(), 10)}, "step": {"5"}}
				ls, lb, _ := queryRangeGET(t, lokiURL, params, headers)
				ps, pb, _ := queryRangeGET(t, proxyURL, params, headers)
				if ls != http.StatusBadRequest || !strings.Contains(lb, "LabelFilterErr") {
					t.Fatalf("Loki oracle did not expose the fixture's conversion error: %d %s", ls, lb)
				}
				if ps != ls || strings.TrimSpace(pb) != strings.TrimSpace(lb) {
					t.Errorf("metric conversion error mismatch: Loki=%d %s proxy=%d %s", ls, lb, ps, pb)
				}
			})
		}
	}
	for _, source := range []string{"custom-error", "preserve-true", "preserve-custom"} {
		pipeline := fmt.Sprintf(`{probe=%q} | %s | row=%q | n=200`, probe, parser, strconv.Itoa(sourceRows[source]))
		clean := fmt.Sprintf(`count_over_time({probe=%q} | %s | row="1" | n=200 [1m])`, probe, parser)
		sourceErrors := map[int]string{}
		for index, query := range numericEqualityMetricShapes(pipeline, clean) {
			t.Run(fmt.Sprintf("source-%s-shape-%d", source, index), func(t *testing.T) {
				params := url.Values{"query": {query}, "start": {strconv.FormatInt(stamp.Add(10*time.Second).UnixNano(), 10)},
					"end": {strconv.FormatInt(stamp.Add(20*time.Second).UnixNano(), 10)}, "step": {"5"}}
				ls, lb, lp := queryRangeGET(t, lokiURL, params, headers)
				ps, pb, pp := queryRangeGET(t, proxyURL, params, headers)
				if ls != http.StatusOK && ls != http.StatusBadRequest || ps != ls {
					t.Fatalf("source label status: Loki=%d %s proxy=%d %s", ls, lb, ps, pb)
				}
				if ls == http.StatusOK {
					want := flattenMatrixResult(t, extractMap(lp, "data")["result"])
					got := flattenMatrixResult(t, extractMap(pp, "data")["result"])
					if source == "preserve-true" && (index == 4 || index == 5 || index == 8) {
						// conformance: parsed-identity/dynamic-key-parsers
						// These existing default stats routes keep stream identity.
						// Assert the exact accepted projection, and fail a stale gap.
						if reflect.DeepEqual(got, want) {
							t.Fatal("stale parsed-identity/dynamic-key-parsers deviation")
						}
						for _, raw := range extractArray(extractMap(lp, "data"), "result") {
							metric := extractMap(raw.(map[string]interface{}), "metric")
							for _, key := range []string{"n", "row", "__error__", "__error_details__", "__preserve_error__"} {
								delete(metric, key)
							}
						}
						want = flattenMatrixResult(t, extractMap(lp, "data")["result"])
						t.Log("known default identity projection: parsed-identity/dynamic-key-parsers")
					}
					if index == 0 && len(want) == 0 || !reflect.DeepEqual(got, want) {
						t.Fatalf("source label samples: Loki=%d %s proxy=%d %s", ls, lb, ps, pb)
					}
					return
				}
				sourceErrors[index] = strings.TrimSpace(lb)
				if source == "preserve-custom" && index >= 12 {
					// Both operands fail on the same row with different parser
					// hints. Loki may report either operand's complete error.
					if strings.TrimSpace(pb) != sourceErrors[0] && strings.TrimSpace(pb) != sourceErrors[2] {
						t.Fatalf("binary error is neither Loki operand error: %s", pb)
					}
					return
				}
				if ls != http.StatusBadRequest || ps != ls || strings.TrimSpace(pb) != strings.TrimSpace(lb) {
					t.Fatalf("source preserve marker: Loki=%d %s proxy=%d %s", ls, lb, ps, pb)
				}
			})
		}
	}
	// Source error keys are retained in Loki's answer but are not the generated
	// errors its __error__ filters inspect. Compare both successful samples and
	// retained-source failures on the raw-row and stats paths.
	for _, tc := range []struct {
		name, tail  string
		nonempty    bool
		sourceError bool
	}{
		{"retained-source-error", `row="0" | n=200`, false, true},
		{"source-error-filtered", `row="0" | n=200 | __error__=""`, false, true},
		{"clean-sample", `row="1" | n=200`, true, false},
		{"clean-error-filter", `row="1" | n=200 | __error__=""`, true, false},
		{"drop-conversion-error", `row!="0" | n=200 | __error__=""`, true, false},
		{"only-failed-row-filtered", fmt.Sprintf(`row=%q | n=200 | __error__=""`, strconv.Itoa(errorRow["n"])), false, false},
	} {
		for _, step := range []string{"5", "60"} {
			t.Run(tc.name+"-step-"+step, func(t *testing.T) {
				query := fmt.Sprintf(`count_over_time({probe=%q} | %s | %s [1m])`, probe, parser, tc.tail)
				params := url.Values{"query": {query},
					"start": {strconv.FormatInt(stamp.Add(10*time.Second).UnixNano(), 10)},
					"end":   {strconv.FormatInt(stamp.Add(20*time.Second).UnixNano(), 10)}, "step": {step}}
				if step == "60" {
					// A minute-aligned evaluated sample includes the fixture;
					// an unaligned ten-second request can contain no Loki step.
					start := stamp.Truncate(time.Minute).Add(time.Minute)
					params.Set("start", strconv.FormatInt(start.UnixNano(), 10))
					params.Set("end", strconv.FormatInt(start.Add(time.Minute).UnixNano(), 10))
				}
				ls, lb, lp := queryRangeGET(t, lokiURL, params, headers)
				ps, pb, pp := queryRangeGET(t, proxyURL, params, headers)
				if tc.sourceError {
					if ls != http.StatusBadRequest || ps != ls || strings.TrimSpace(pb) != strings.TrimSpace(lb) {
						t.Fatalf("retained source error: Loki=%d %s proxy=%d %s", ls, lb, ps, pb)
					}
					return
				}
				if ls != http.StatusOK || ps != ls {
					t.Fatalf("Loki=%d %s proxy=%d %s", ls, lb, ps, pb)
				}
				want := flattenMatrixResult(t, extractMap(lp, "data")["result"])
				got := flattenMatrixResult(t, extractMap(pp, "data")["result"])
				if (len(want) > 0) != tc.nonempty {
					t.Fatalf("Loki fixture nonempty=%v, expected %v", len(want) > 0, tc.nonempty)
				}
				if !reflect.DeepEqual(got, want) {
					t.Errorf("complete metric samples: proxy=%v Loki=%v", got, want)
				}
			})
		}
	}
	// Errors between evaluated windows are not samples and must not turn a
	// successful response into a pipeline error or an inconclusive-proof 500.
	for _, wrap := range []string{"%s", "sum(%s)"} {
		t.Run("window-gap-"+wrap, func(t *testing.T) {
			query := fmt.Sprintf(wrap, fmt.Sprintf(`count_over_time({probe=%q} | %s | n=200 [5s])`, probe+"-gap", parser))
			params := url.Values{"query": {query}, "start": {strconv.FormatInt(evaluation.UnixNano(), 10)},
				"end": {strconv.FormatInt(evaluation.Add(time.Minute).UnixNano(), 10)}, "step": {"60"}}
			ls, lb, lp := queryRangeGET(t, lokiURL, params, headers)
			ps, pb, pp := queryRangeGET(t, proxyURL, params, headers)
			if ls != http.StatusOK || ps != ls {
				t.Fatalf("window-gap error: Loki=%d %s proxy=%d %s", ls, lb, ps, pb)
			}
			want := flattenMatrixResult(t, extractMap(lp, "data")["result"])
			got := flattenMatrixResult(t, extractMap(pp, "data")["result"])
			if len(want) == 0 || !reflect.DeepEqual(got, want) {
				t.Fatalf("window-gap samples: proxy=%v Loki=%v", got, want)
			}
		})
	}
}

// Reuse the same wrapper set for ordinary conversion failures and source
// error/preserve labels; each depth is checked against the real Loki answer.
func numericEqualityMetricShapes(pipeline, cleanCount string) []string {
	count := "count_over_time(" + pipeline + " [1m])"
	return []string{count, "rate(" + pipeline + " [1m])", "sum(" + count + ")", "sum by (app) (" + count + ")",
		"topk(1," + count + ")", "sort(" + count + ")", "(" + count + ") * 2", "(" + count + ") or vector(1)",
		`label_replace(` + count + `,"copied","$1","app","(.*)")`, cleanCount + " + (" + count + ")",
		"(" + count + ") + " + cleanCount, "count_over_time(" + pipeline + " [1m] offset 5s)",
		"sum(" + count + ") + (" + count + ")", "(" + count + ") + sum(" + count + ")"}
}
