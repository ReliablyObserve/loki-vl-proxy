package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/casefixtures"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// conformance: semantics/two-parsers-keep-one
func TestMixedJSONRowClassActualVictoriaLogsPartition(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("requires the existing VictoriaLogs storage oracle")
	}
	var rows []oraclerun.StoreRow
	var unsafeApps []string
	for _, witness := range casefixtures.OrderedMixedParserRawComplement() {
		var stream [][2]string
		for key, value := range witness.Stream.Labels {
			stream = append(stream, [2]string{key, value})
		}
		sort.Slice(stream, func(i, j int) bool { return stream[i][0] < stream[j][0] })
		rows = append(rows, oraclerun.StoreRow{Stream: stream, Fields: [][2]string{{"_msg", witness.Stream.Entries[0].Line}}})
	}
	// Positive and unsafe controls supplement the shared raw-complement inputs.
	// Every row must retain its entire original envelope, even a scratch collision.
	for _, control := range []struct {
		app, line string
		fields    [][2]string
		stream    [][2]string
	}{
		{"class-safe-flat", `{"k":"blue"}`, nil, nil},
		{"class-safe-nested", `{"k":"blue","service":{"name":"api"}}`, nil, nil},
		{"class-unsafe-logfmt", `k=blue`, nil, nil},
		{"class-unsafe-scratch", `{"k":"blue"}`, [][2]string{{mixedClassOriginalField, "original"}}, nil},
		{"class-unsafe-safe-flag", `{"k":"blue"}`, [][2]string{{mixedStatsSafeField, "1"}}, nil},
		{"class-unsafe-valid-flag", `{"k":"blue"}`, [][2]string{{mixedClassValidField, "1"}}, nil},
		{"class-unsafe-target", `{"k":"blue"}`, [][2]string{{mixedClassTargetPrefix + "k", "red"}}, nil},
		{"class-unsafe-duplicate", `{"k":"blue","k":"red"}`, nil, nil},
		{"class-unsafe-long-key", `{"` + strings.Repeat("x", 129) + `":0,"k":"blue"}`, nil, nil},
		{"class-unsafe-long-line", `{"k":"blue","padding":"` + strings.Repeat("x", 4097) + `"}`, nil, nil},
		{"class-unsafe-metadata-alias", `{"k":"blue"}`, [][2]string{{"k_", "stored"}}, nil},
		{"class-unsafe-stream-alias", `{"k":"blue"}`, nil, [][2]string{{"k_", "stored"}}},
		{"class-unsafe-double-underscore", `{"k":"blue"}`, [][2]string{{"user__id", "stored"}}, nil},
		{"class-unsafe-empty-key-alias", `{"k":"blue"}`, [][2]string{{"___", "stored"}}, nil},
	} {
		fields := append([][2]string{{"_msg", control.line}}, control.fields...)
		stream := append([][2]string{{"app", control.app}, {"env", "compat_registry"}}, control.stream...)
		rows = append(rows, oraclerun.StoreRow{Stream: stream, Fields: fields})
		if strings.HasPrefix(control.app, "class-unsafe-") {
			unsafeApps = append(unsafeApps, control.app)
		}
	}
	class, ok := newMixedJSONRowClass("k")
	if !ok {
		t.Fatal("ordinary class rejected")
	}

	// Output fields must discard source g/c without losing their originals.
	for _, field := range []string{"g", "c"} {
		rows = append(rows, oraclerun.StoreRow{Stream: [][2]string{{"app", "class-safe-short-" + field}, {"env", "compat_registry"}}, Fields: [][2]string{{"_msg", `{"k":"blue"}`}, {field, "original-source"}}})
	}
	envelope := "__class_oracle_original"
	base := `env:="compat_registry"`
	queries := []string{base + (logsql.PipePackJSON{ResultField: envelope}).String() + " | fields " + envelope}
	for _, pipes := range [][]logsql.Pipe{class.nativePipes(), class.rawPipes()} {
		queries = append(queries, base+strings.TrimPrefix((logsql.Query{Pipes: pipes}).String(), "*")+" | fields "+class.originalField)
	}
	classified, aggregate := mixedJSONStatsQuery(base, class, "k")
	queries = append(queries, classified+" | stats by ("+mixedStatsGroupField+") "+aggregate)
	for _, version := range []struct{ name, modfile string }{{"v1.49.0", "go.v1.49.0.mod"}, {"v1.54.0", ""}} {
		t.Run(version.name, func(t *testing.T) {
			answers, err := oraclerun.VLStore(filepath.Join("..", "..", "test", "oracle", "vl"), version.modfile, rows, queries)
			if err != nil {
				t.Fatal(err)
			}
			wantAnswers := 4
			if len(answers) != wantAnswers {
				t.Fatalf("missing query answers: %d", len(answers))
			}
			original := mixedClassOracleEnvelopes(t, answers[0], envelope)
			native := mixedClassOracleEnvelopes(t, answers[1], class.originalField)
			raw := mixedClassOracleEnvelopes(t, answers[2], class.originalField)
			if len(original) != len(rows) || len(native) == 0 || len(raw) == 0 {
				t.Fatalf("source/native/raw coverage: %d/%d/%d; source expected %d", len(original), len(native), len(raw), len(rows))
			}
			for _, app := range []string{"class-safe-flat", "class-safe-nested"} {
				if _, found := native[app]; !found {
					t.Errorf("positive control %s was not native", app)
				}
			}
			for _, field := range []string{"g", "c"} {
				app := "class-safe-short-" + field
				if native[app][field] != "original-source" {
					t.Errorf("compact stats output lost original %s", field)
				}
			}
			mixedClassRelevantNativeLokiProof(t, "k", "compat_registry", native)
			for _, witness := range casefixtures.OrderedMixedParserRawComplement() {
				if _, found := raw[witness.Stream.Labels["app"]]; !found {
					t.Errorf("uncertified source %s left the exact raw complement", witness.Name)
				}
			}
			for _, app := range unsafeApps {
				if _, found := raw[app]; !found {
					t.Errorf("unsafe control %s left the exact raw complement", app)
				}
			}
			for app, row := range native {
				if _, found := raw[app]; found {
					t.Errorf("native/raw overlap for %s", app)
				}
				if !reflect.DeepEqual(row, original[app]) {
					t.Errorf("native envelope changed %s", app)
				}
			}
			for app, row := range raw {
				if !reflect.DeepEqual(row, original[app]) {
					t.Errorf("raw envelope changed %s", app)
				}
			}
			if len(native)+len(raw) != len(original) {
				t.Error("partition lost original rows")
			}
			if answers[3].Error != "" {
				t.Fatal(answers[3].Error)
			}
			if len(answers[3].Rows) != 2 {
				t.Fatalf("expected native blue group and one unsafe auxiliary group: %v", answers[3].Rows)
			}
			var nativeCount, rawCount int
			for _, fields := range answers[3].Rows {
				values := make(map[string]string)
				for _, field := range fields {
					key, value, ok := strings.Cut(field, "=")
					if !ok {
						t.Fatalf("invalid stats field %q", field)
					}
					values[key] = value
				}
				count, err := strconv.Atoi(values["c"])
				if err != nil {
					t.Fatal(err)
				}
				switch values[mixedStatsGroupField] {
				case "blue":
					nativeCount += count
				case "":
					rawCount += count
				default:
					t.Fatalf("native and unsafe groups contaminated one another: %v", values)
				}
			}
			if nativeCount != len(native) || rawCount != len(raw) {
				t.Fatalf("combined stats native/raw=%d/%d, exact partition=%d/%d", nativeCount, rawCount, len(native), len(raw))
			}
		})
	}
}

// conformance: semantics/two-parsers-keep-one
func TestMixedJSONRowClassRelevantAliasesActualVictoriaLogsAndLoki(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("requires real VictoriaLogs and Loki oracles")
	}
	for _, key := range []string{"k", "user_id"} {
		t.Run(key, func(t *testing.T) {
			literal := `"` + key + `":"blue"`
			deep := `"leaf"`
			for i := 0; i < 70; i++ {
				deep = `{"":` + deep + `}`
			}
			controls := []struct {
				name, line string
				native     bool
				fields     [][2]string
			}{
				{"literal", `{` + literal + `}`, true, nil},
				{"irrelevant-dot-before", `{"other.key":"x",` + literal + `}`, true, nil},
				{"irrelevant-dot-after", `{` + literal + `,"other.key":"x"}`, true, nil},
				{"irrelevant-digit-before", `{"1other":"x",` + literal + `}`, true, nil},
				{"irrelevant-digit-after", `{` + literal + `,"1other":"x"}`, true, nil},
				{"irrelevant-space-before", `{"other key":"x",` + literal + `}`, true, nil},
				{"irrelevant-space-after", `{` + literal + `,"other key":"x"}`, true, nil},
				{"trim-before", `{" ` + key + ` ":"alias",` + literal + `}`, false, nil},
				{"trim-after", `{` + literal + `," ` + key + ` ":"alias"}`, false, nil},
				{"nested-trim", `{" ` + key + ` ":{"leaf":"alias"},` + literal + `}`, false, nil},
				{"empty-key", `{"":"alias",` + literal + `}`, false, nil},
				{"empty-object-depth", `{"` + key[:1] + `":` + deep + `,` + literal + `}`, false, nil},
				{"irrelevant-object-depth", `{"other":` + strings.Repeat(`{"n":`, 70) + `"leaf"` + strings.Repeat(`}`, 70) + `,` + literal + `}`, true, nil},
				{"duplicate", `{` + literal + `,` + literal + `}`, false, nil},
				{"long-unrelated", `{"` + strings.Repeat("a", 129) + `":"x",` + literal + `}`, false, nil},
				{"long-unrelated-after", `{` + literal + `,"` + strings.Repeat("a", 129) + `":"x"}`, false, nil},
				{"long-nested-key", `{"other":{"` + strings.Repeat("a", 129) + `":"x"},` + literal + `}`, false, nil},
				{"bounded-key", `{"` + strings.Repeat("a", 128) + `":"x",` + literal + `}`, true, nil},
				// A quoted run of 129 name characters is rejected wherever it is, a long value included: the row takes
				// the exact complement instead of the native count (the 128-character "bounded-key" stays native).
				{"long-array-value", `{` + literal + `,"other":["` + strings.Repeat("a", 129) + `","commas,{braces}"]}`, false, nil},
				{"bounded-array-value", `{` + literal + `,"other":["` + strings.Repeat("a", 128) + `","commas,{braces}"]}`, true, nil},
				{"long-malformed-value-colon", `{` + literal + `,"other":"` + strings.Repeat("a", 129) + `":0}`, false, nil},
				{"source-target", `{` + literal + `}`, false, [][2]string{{key, "stored"}}},
				{"source-scratch", `{` + literal + `}`, false, [][2]string{{mixedClassTargetPrefix + key, "poison"}}},
				{"body-scratch", `{"__vlmc_valid":1,` + literal + `}`, false, nil},
			}
			if key == "user_id" {
				controls = append(controls, struct {
					name, line string
					native     bool
					fields     [][2]string
				}{"canonical-nested", `{"user":{"id":"alias"},` + literal + `}`, false, nil})
				for _, alias := range []string{"user-id", " user ", " id "} {
					controls = append(controls, struct {
						name, line string
						native     bool
						fields     [][2]string
					}{"alias-before-" + alias, `{"` + alias + `":"alias",` + literal + `}`, false, nil}, struct {
						name, line string
						native     bool
						fields     [][2]string
					}{"alias-after-" + alias, `{` + literal + `,"` + alias + `":"alias"}`, false, nil})
				}
			}
			controls = append(controls, []struct {
				name, line string
				native     bool
				fields     [][2]string
			}{
				{"single-colon-spaces", `{"` + key + `"  : "blue"}`, true, nil},
				{"duplicate-spaced", `{"` + key + `" : "blue", "` + key + `"  : "blue"}`, false, nil},
				{"duplicate-nested", `{` + literal + `,"nested":{` + literal + `}}`, false, nil},
				{"label-in-value", `{` + literal + `,"note":"` + key + ` ` + key + `"}`, true, nil},
				// The quoted name twice (as a key and as a value) is the duplicate-key check's literal count: the row
				// takes the exact complement instead of the native count, and its answer is Loki's either way.
				{"label-only-value", `{` + literal + `,"note":"` + key + `"}`, false, nil},
				{"escaped-value", `{` + literal + `,"note":"\"` + key + `\":x"}`, false, nil},
				{"escaped-key", `{"\u006b":"alias",` + literal + `}`, false, nil},
			}...)
			env := "relevant_aliases_" + key
			var rows []oraclerun.StoreRow
			wantNative := make(map[string]bool)
			for i, control := range controls {
				app := fmt.Sprintf("relevant-%s-%02d", key, i)
				fields := append([][2]string{{"_msg", control.line}}, control.fields...)
				rows = append(rows, oraclerun.StoreRow{Stream: [][2]string{{"app", app}, {"env", env}}, Fields: fields})
				wantNative[app] = control.native
			}
			class, _ := newMixedJSONRowClass(key)
			base := `env:="` + env + `"`
			originalField := "__relevant_original"
			queries := []string{base + (logsql.PipePackJSON{ResultField: originalField}).String() + " | fields " + originalField}
			for _, pipes := range [][]logsql.Pipe{class.nativePipes(), class.rawPipes()} {
				queries = append(queries, base+strings.TrimPrefix((logsql.Query{Pipes: pipes}).String(), "*")+" | fields "+class.originalField)
			}
			query, agg := mixedJSONStatsQuery(base, class, key)
			queries = append(queries, query+" | stats by ("+mixedStatsGroupField+") "+agg)
			for _, version := range []struct{ name, modfile string }{{"v1.49.0", "go.v1.49.0.mod"}, {"v1.54.0", ""}} {
				t.Run(version.name, func(t *testing.T) {
					answers, err := oraclerun.VLStore(filepath.Join("..", "..", "test", "oracle", "vl"), version.modfile, rows, queries)
					if err != nil {
						t.Fatal(err)
					}
					wantAnswers := 4
					if len(answers) != wantAnswers {
						t.Fatal("missing real VL replies")
					}
					original := mixedClassOracleEnvelopes(t, answers[0], originalField)
					native := mixedClassOracleEnvelopes(t, answers[1], class.originalField)
					raw := mixedClassOracleEnvelopes(t, answers[2], class.originalField)
					if len(original) != len(rows) || len(native)+len(raw) != len(rows) {
						t.Fatal("source/partition loss")
					}
					for app, row := range original {
						_, isNative := native[app]
						if isNative != wantNative[app] {
							t.Errorf("%s native=%v want=%v", app, isNative, wantNative[app])
						}
						if isNative {
							if _, both := raw[app]; both {
								t.Fatal("partition overlap")
							}
							if !reflect.DeepEqual(native[app], row) {
								t.Fatal("native source envelope changed")
							}
						} else if !reflect.DeepEqual(raw[app], row) {
							t.Fatal("raw complement source envelope changed")
						}
					}
					groups := mixedClassOracleStatsGroups(t, answers[3], mixedStatsGroupField)
					if !reflect.DeepEqual(groups, map[string]int{"blue": len(native), "": len(raw)}) {
						t.Fatalf("native/unsafe counts changed:%v", groups)
					}
					mixedClassRelevantNativeLokiProof(t, key, env, native)
				})
			}
		})
	}
}

func mixedClassRelevantNativeLokiProof(t *testing.T, key, env string, native map[string]map[string]string) {
	t.Helper()
	at := time.Unix(1700002800, 0).UTC()
	var engine oraclerun.EngineDataSet
	for app, row := range native {
		metadata := make(map[string]string)
		for name, value := range row {
			if !isVLInternalField(name) && name != "app" && name != "env" && name != "level" && value != "" {
				metadata[name] = value
			}
		}
		engine.Streams = append(engine.Streams, oraclerun.EngineStream{Labels: map[string]string{"app": app, "env": env}, Entries: []oraclerun.EngineEntry{{TS: at.UnixNano(), Line: row["_msg"], Metadata: metadata}}})
	}
	for _, function := range []string{"count_over_time", "rate"} {
		for _, stages := range []string{fmt.Sprintf(`| json %s="[\"%s\"]" | logfmt`, key, key), `| logfmt | json`} {
			query := fmt.Sprintf(`sum by(%s)(%s({env=%q} %s | drop __error__, __error_details__ | %s!="" [1m]))`, key, function, env, stages, key)
			plan, ok := compileOrderedJSONMetric(query)
			if !ok {
				t.Fatal("missing original exact plan")
			}
			for app, row := range native {
				stream := map[string]string{"app": app, "env": env}
				base := cloneStringMap(stream)
				for name, value := range row {
					if !isVLInternalField(name) && name != "level" && value != "" {
						base[name] = value
					}
				}
				labels, keep, err := plan.processWithStreamLabels(row["_msg"], base, stream)
				if err != nil || !keep || labels[key] != "blue" {
					t.Fatalf("native disagrees with exact evaluator %s: %v %v %v", app, labels, keep, err)
				}
			}
			id := function + stages
			engine.Requests = append(engine.Requests, lokiMetricRequest(id, query, at.UnixNano(), at.Add(30*time.Second).UnixNano(), 30*time.Second))
			engine.Requests = append(engine.Requests, oraclerun.EngineRequest{ID: id + "instant", Instant: true, Form: url.Values{"query": {query}, "time": {strconv.FormatInt(at.Unix(), 10)}}.Encode()})
		}
	}
	answers, err := oraclerun.LokiEngine(filepath.Join("..", "..", "test", "oracle", "loki"), []oraclerun.EngineDataSet{engine})
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range engine.Requests {
		form, _ := url.ParseQuery(request.Form)
		count := float64(len(native))
		if strings.Contains(form.Get("query"), "rate(") {
			count /= 60
		}
		point := []any{at.Unix(), formatMetricSampleValue(count)}
		result := map[string]any{"metric": map[string]string{key: "blue"}}
		kind := "matrix"
		if request.Instant {
			kind = "vector"
			result["value"] = point
		} else {
			result["values"] = []any{point, []any{at.Add(30 * time.Second).Unix(), formatMetricSampleValue(count)}}
		}
		body, _ := json.Marshal(map[string]any{"status": "success", "data": map[string]any{"resultType": kind, "result": []any{result}}})
		want := answers[request.ID]
		if err := recordedParserDifference(recordedParserCase{Status: want.Status, Response: want.Body}, http.StatusOK, body); err != nil {
			t.Fatalf("native cohort Loki metric %s:%v", request.ID, err)
		}
	}
}

func mixedClassOracleStatsGroups(t *testing.T, answer oraclerun.StoreAnswer, field string) map[string]int {
	t.Helper()
	if answer.Error != "" {
		t.Fatal(answer.Error)
	}
	out := make(map[string]int)
	for _, row := range answer.Rows {
		values := make(map[string]string)
		for _, cell := range row {
			key, value, ok := strings.Cut(cell, "=")
			if !ok {
				t.Fatal("invalid stats row")
			}
			values[key] = value
		}
		count, err := strconv.Atoi(values["c"])
		if err != nil {
			t.Fatal(err)
		}
		out[values[field]] += count
	}
	return out
}

func mixedClassOracleEnvelopes(t *testing.T, answer oraclerun.StoreAnswer, field string) map[string]map[string]string {
	t.Helper()
	if answer.Error != "" {
		t.Fatal(answer.Error)
	}
	out := make(map[string]map[string]string)
	for _, fields := range answer.Rows {
		var original map[string]string
		for _, encoded := range fields {
			if value, ok := strings.CutPrefix(encoded, field+"="); ok {
				if err := json.Unmarshal([]byte(value), &original); err != nil {
					t.Fatal(err)
				}
			}
		}
		if original["app"] == "" || original["_msg"] == "" || original["_time"] == "" {
			t.Fatalf("incomplete original envelope: %v", original)
		}
		if _, duplicate := out[original["app"]]; duplicate {
			t.Fatalf("duplicate original app %q", original["app"])
		}
		out[original["app"]] = original
	}
	return out
}
