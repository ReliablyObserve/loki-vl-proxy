package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// The rows that tell the two parser orders apart. A guard of the row class protects a row from a spelling Loki reads
// as the extracted key or from a message the backend reads differently; whether the explicit `json k="[\"k\"]"` needs
// it is not a matter of opinion but of what Loki answers for the row, which is recorded for every spelling below
// and replayed against real VictoriaLogs (1.49 and 1.54) through the route, for both orders.
func mixedShapeKeyRows(at time.Time) []pushdownRow {
	long := strings.Repeat("a", 129)
	lines := []string{
		// label k
		`{"k":"body"}`, `k=body`, `{"k":"other"}`,
		`{" k ":"alias","k":"body"}`, `{"k":"body"," k ":"alias"}`, `{"":"empty","k":"body"}`,
		`{"` + long + `":"x","k":"body"}`, `{"k":"body","other":{"` + long + `":"x"}}`,
		`{"k":"body","k.sub":"x"}`, `{"k-sub":"x","k":"body"}`, `{"k":"body","note":["` + long + `","commas,{braces}"]}`,
		`{"K":"alias","k":"body"}`, `{"k":"body","k":"body"}`, `{"k":"body","k":"later"}`, `{"k":"later","k":"body"}`,
		`{"k":{"n":"x"}}`, `{"k":"body","o":{"k":"inner"}}`,
		// label user_id: nested and aliased spellings of the same key
		`{"user_id":"body"}`, `user_id=body`,
		`{"user":{"id":"alias"},"user_id":"body"}`, `{"user_id":"body","user":{"id":"alias"}}`,
		`{"user-id":"alias","user_id":"body"}`, `{"user_id":"body","user.id":"alias"}`, `{"user_id":"body","user_id":"later"}`,
		`{" user_id ":"alias","user_id":"body"}`, `{"user":"x","user_id":"body"}`, `{"user_id":"body","user_id_x":"y"}`,
		`{"user":{"id":"alias","n":{"m":"deep"}},"user_id":"body"}`, `{"u":{"s":{"e":{"r":"x"}}},"user_id":"body"}`,
		`{"user_id":{"n":"x"}}`,
	}
	rows := make([]pushdownRow, len(lines))
	for i, msg := range lines {
		rows[i] = pushdownRow{ts: at.Add(time.Duration(i+1) * time.Second), stream: map[string]string{"app": "mixed-shape-keys"}, msg: msg}
	}
	return rows
}

func mixedShapeKeysSet(at time.Time) oraclerun.EngineDataSet {
	set := oraclerun.EngineDataSet{Streams: pushdownEngineStreams(mixedShapeKeyRows(at))}
	for _, label := range []string{"k", "user_id"} {
		for _, shape := range []struct{ name, stages string }{
			{"forward", fmt.Sprintf(`| json %s="[\"%s\"]" | logfmt`, label, label)}, {"reverse", `| logfmt | json`},
		} {
			for _, function := range []string{"count_over_time", "rate"} {
				query := fmt.Sprintf(`sum by(%s)(%s({app="mixed-shape-keys"} %s | drop __error__, __error_details__ | %s!="" [2m]))`,
					label, function, shape.stages, label)
				id := fmt.Sprintf("shapekeys/%s/%s/%s", label, shape.name, function)
				set.Requests = append(set.Requests, lokiMetricRequest(id, query, at.UnixNano(), at.Add(2*time.Minute).UnixNano(), 30*time.Second))
			}
		}
	}
	return set
}

// conformance: semantics/two-parsers-keep-one
func TestOrderedMixedStatsShapeKeysMatchRecordedLoki(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("requires existing VictoriaLogs storage/HTTP and recorded Loki oracles")
	}
	at := time.Unix(1700004800, 0).UTC()
	set := mixedShapeKeysSet(at)
	sets := []oraclerun.EngineDataSet{set}
	if os.Getenv("LVP_MIXED_SHAPE_APPEND_GOLDEN") == "1" {
		fresh, err := oraclerun.LokiEngine(filepath.Join("..", "..", "test", "oracle", "loki"), sets)
		if err != nil {
			t.Fatal(err)
		}
		appendMixedReal48Golden(t, sets, fresh)
	}
	answers := mixedStatsSavedAnswers(t, sets)
	var store oraclerun.ServeDataSet
	for _, row := range mixedShapeKeyRows(at) {
		store.Rows = append(store.Rows, oraclerun.ServeRow{TS: row.ts.UnixNano(), Stream: [][2]string{{"app", "mixed-shape-keys"}}, Fields: [][2]string{{"_msg", row.msg}}})
	}
	for _, version := range []struct{ name, modfile string }{{"v1.49.0", "go.v1.49.0.mod"}, {"v1.54.0", ""}} {
		t.Run(version.name, func(t *testing.T) {
			backend, err := oraclerun.StartVLServe(filepath.Join("..", "..", "test", "oracle", "vl"), version.modfile, []oraclerun.ServeDataSet{store})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(backend.Stop)
			p, err := New(Config{BackendURL: backend.URL, Cache: cache.NewDisabled(), LogLevel: "error", MetadataInventoryParallelism: -1})
			if err != nil {
				t.Fatal(err)
			}
			p.storeBackendVersion(version.name, version.name)
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			for _, request := range set.Requests {
				form, err := url.ParseQuery(request.Form)
				if err != nil {
					t.Fatal(err)
				}
				plan, ok := compileOrderedJSONMetric(form.Get("query"))
				if !ok {
					t.Fatalf("%s: typed mixed plan missing", request.ID)
				}
				bounds := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+request.Form, nil)
				start, end, step, err := orderedJSONMetricTimes(bounds, true)
				if err != nil {
					t.Fatal(err)
				}
				series, served, err := p.collectOrderedMixedJSONStats(context.Background(), plan, start, end, step)
				if err != nil || !served {
					t.Fatalf("%s: served=%v error=%v", request.ID, served, err)
				}
				body, err := buildMixedJSONStatsMatrix(plan, series, start, end, step, p.limits().BufferedBackendBodyBytes)
				if err != nil {
					t.Fatal(err)
				}
				want := answers[request.ID]
				if err := recordedParserDifference(recordedParserCase{Status: want.Status, Response: want.Body}, http.StatusOK, body); err != nil {
					t.Fatalf("%s: %v", request.ID, err)
				}
				httpAnswer := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+request.Form, nil)
				if err := recordedParserDifference(recordedParserCase{Status: want.Status, Response: want.Body}, httpAnswer.Code, httpAnswer.Body.Bytes()); err != nil {
					t.Fatalf("%s full HTTP route: %v", request.ID, err)
				}
			}
		})
	}
}
