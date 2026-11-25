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

type mixedKeysScenario struct {
	name  string
	lines []string
	// dirty names the labels whose window must NOT be certified: the key list shows a spelling of that label.
	dirty map[string]bool
}

// labels are the labels some row of the scenario carries as a key: a request for another would be empty against
// empty, which proves nothing.
func (sc mixedKeysScenario) labels() []string {
	var out []string
	for _, label := range []string{"k", "user_id"} {
		for _, line := range sc.lines {
			if strings.Contains(line, `"`+label+`":`) {
				out = append(out, label)
				break
			}
		}
	}
	return out
}

func mixedKeysScenarios() []mixedKeysScenario {
	long129, long128 := strings.Repeat("a", 129), strings.Repeat("b", 128)
	both := map[string]bool{"k": true, "user_id": true}
	return []mixedKeysScenario{
		// Hazards the key list cannot show: they stay per-row guards or are no hazard at all. The window is certified
		// and the native count must still equal Loki's.
		{name: "invisible", lines: []string{
			`{"k":"body"}`, `{"k":"other"}`, `{"k":"body","o":{}}`, `{"k":"body","n":null}`, `{" k ":null,"k":"body"}`,
			`{"k":"body","arr":[]}`, `{"k":"body","arr":["x","y"]}`, `{"k":"body","b":true,"num":12}`,
			`{"k":"body","note":"` + long129 + `"}`, `{"k":"body","k":"body"}`, `{"k":"body","k":"later"}`, `{"k":"later","k":"body"}`,
			`{"user_id":"body"}`, `{"user":{},"user_id":"body"}`, `{"user":null,"user_id":"body"}`, `{"user":[],"user_id":"body"}`,
			`{"user":"x","user_id":"body"}`, `{"user_id":"body","user_id":"later"}`, `{"user_id":"body","u":{"s":{}}}`,
			`{"user":{"id":null},"user_id":"body"}`,
		}},
		// Arrays of objects: the list shows the key `user`, not `user.id`; Loki decides whether that is a hazard.
		{name: "array-of-objects", lines: []string{`{"user":[{"id":"alias"}],"user_id":"body"}`, `{"user_id":"body"}`, `{"k":"body","k2":[{"k":"alias"}]}`, `{"k":"body"}`}},
		// Spellings the list shows; the window must be dirty for the label they alias (and for both when the name is
		// outside the class's admission).
		{name: "space-alias", lines: []string{`{" k ":"alias","k":"body"}`, `{"k":"body"}`}, dirty: map[string]bool{"k": true}},
		{name: "space-alias-user", lines: []string{`{" user_id ":"alias","user_id":"body"}`, `{"user_id":"body"}`}, dirty: map[string]bool{"user_id": true}},
		{name: "empty-key", lines: []string{`{"":"empty","k":"body"}`, `{"k":"body"}`, `{"user_id":"body"}`}, dirty: both},
		{name: "long-key-129", lines: []string{`{"` + long129 + `":"x","k":"body"}`, `{"k":"body"}`, `{"user_id":"body"}`}, dirty: both},
		{name: "long-key-128", lines: []string{`{"` + long128 + `":"x","k":"body"}`, `{"k":"body"}`, `{"user_id":"body"}`}},
		{name: "nested-prefix", lines: []string{`{"user":{"id":"alias"},"user_id":"body"}`, `{"user_id":"body"}`}, dirty: map[string]bool{"user_id": true}},
		{name: "dotted-literal", lines: []string{`{"user.id":"alias","user_id":"body"}`, `{"user_id":"body"}`}, dirty: map[string]bool{"user_id": true}},
		{name: "hyphen", lines: []string{`{"user-id":"alias","user_id":"body"}`, `{"user_id":"body"}`}, dirty: map[string]bool{"user_id": true}},
		{name: "double-underscore", lines: []string{`{"user__id":"alias","user_id":"body"}`, `{"user_id":"body"}`}, dirty: map[string]bool{"user_id": true}},
		{name: "trailing-underscore", lines: []string{`{"user_id_":"alias","user_id":"body"}`, `{"user_id":"body"}`}, dirty: map[string]bool{"user_id": true}},
		{name: "deep-nested", lines: []string{`{"a":{"user":{"id":"x"}},"user_id":"body"}`, `{"user_id":"body"}`}},
		{name: "blank-nested", lines: []string{`{"user":{"":{"id":"alias"}},"user_id":"body"}`, `{"user_id":"body"}`}, dirty: map[string]bool{"user_id": true}},
		{name: "benign-lookalikes", lines: []string{`{"k.sub":"x","k":"body"}`, `{"k-sub":"x","k":"body"}`, `{"K":"alias","k":"body"}`, `{"ключ":"x","k":"body"}`, `{"user":{"name":"x"},"user_id":"body"}`, `{"user_id_x":"y","user_id":"body"}`, `{"User_Id":"x","user_id":"body"}`}},
	}
}

func mixedKeysSet(at time.Time) (oraclerun.EngineDataSet, []mixedKeysScenario) {
	scenarios := mixedKeysScenarios()
	var rows []pushdownRow
	var requests []oraclerun.EngineRequest
	for _, sc := range scenarios {
		app := "mixed-keys-" + sc.name
		for i, msg := range sc.lines {
			rows = append(rows, pushdownRow{ts: at.Add(time.Duration(i+1) * time.Second), stream: map[string]string{"app": app}, msg: msg})
		}
		for _, label := range sc.labels() {
			query := fmt.Sprintf(`sum by(%s)(count_over_time({app=%q} | logfmt | json | drop __error__, __error_details__ | %s!="" [2m]))`, label, app, label)
			requests = append(requests, lokiMetricRequest("keyset/"+sc.name+"/"+label, query, at.UnixNano(), at.Add(2*time.Minute).UnixNano(), 30*time.Second))
		}
	}
	return oraclerun.EngineDataSet{Streams: pushdownEngineStreams(rows), Requests: requests}, scenarios
}

// conformance: semantics/two-parsers-keep-one
func TestOrderedMixedStatsKeySetMatchesRecordedLoki(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("requires existing VictoriaLogs storage/HTTP and recorded Loki oracles")
	}
	at := time.Unix(1700008400, 0).UTC()
	set, scenarios := mixedKeysSet(at)
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
	for _, sc := range scenarios {
		for i, msg := range sc.lines {
			store.Rows = append(store.Rows, oraclerun.ServeRow{TS: at.Add(time.Duration(i+1) * time.Second).UnixNano(), Stream: [][2]string{{"app", "mixed-keys-" + sc.name}}, Fields: [][2]string{{"_msg", msg}}})
		}
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
			for _, sc := range scenarios {
				for _, label := range sc.labels() {
					id := "keyset/" + sc.name + "/" + label
					var request oraclerun.EngineRequest
					for _, r := range set.Requests {
						if r.ID == id {
							request = r
						}
					}
					form, err := url.ParseQuery(request.Form)
					if err != nil {
						t.Fatal(err)
					}
					plan, ok := compileOrderedJSONMetric(form.Get("query"))
					if !ok {
						t.Fatalf("%s: typed mixed plan missing", id)
					}
					bounds := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+request.Form, nil)
					start, end, step, err := orderedJSONMetricTimes(bounds, true)
					if err != nil {
						t.Fatal(err)
					}
					rt := newRequestTelemetry()
					ctx := context.WithValue(context.Background(), requestTelemetryKey, rt)
					series, served, err := p.collectOrderedMixedJSONStats(ctx, plan, start, end, step)
					if err != nil || !served {
						t.Fatalf("%s: served=%v error=%v", id, served, err)
					}
					body, err := buildMixedJSONStatsMatrix(plan, series, start, end, step, p.limits().BufferedBackendBodyBytes)
					if err != nil {
						t.Fatal(err)
					}
					want := answers[id]
					if err := recordedParserDifference(recordedParserCase{Status: want.Status, Response: want.Body}, http.StatusOK, body); err != nil {
						t.Fatalf("%s: %v", id, err)
					}
					rt.mu.Lock()
					certified := rt.internalOpsByType[internalOperationBreakdownKey("mixed_class_keys", "certified")]
					dirty := rt.internalOpsByType[internalOperationBreakdownKey("mixed_class_keys", "dirty")]
					rt.mu.Unlock()
					if sc.dirty[label] && (dirty != 1 || certified != 0) || !sc.dirty[label] && (certified != 1 || dirty != 0) {
						t.Fatalf("%s: key set outcome certified=%d dirty=%d, want dirty=%v", id, certified, dirty, sc.dirty[label])
					}
					httpAnswer := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+request.Form, nil)
					if err := recordedParserDifference(recordedParserCase{Status: want.Status, Response: want.Body}, httpAnswer.Code, httpAnswer.Body.Bytes()); err != nil {
						t.Fatalf("%s full HTTP route: %v", id, err)
					}
				}
			}
		})
	}
}
