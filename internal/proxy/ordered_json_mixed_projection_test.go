package proxy

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestOrderedMixedMetricKeepsOnlyObservableMutableLabel(t *testing.T) {
	plan, ok := compileOrderedJSONMetric(`sum by (level) (count_over_time({app="api"} | json level="[\"level\"]" | logfmt | drop __error__, __error_details__ | level!="" [1m]))`)
	if !ok {
		t.Fatal("mixed plan not owned")
	}
	base := map[string]string{"app": "api", "pod": "one", "level": "stored", "extra": "metadata"}
	before := cloneStringMap(base)
	labels, keep, err := plan.processWithContext(context.Background(), `{"level":"body"}`, base, map[string]string{"app": "api", "level": "stored"}, base)
	if err != nil || !keep {
		t.Fatalf("keep=%v error=%v", keep, err)
	}
	if len(labels) != 1 || labels["level"] != "stored" {
		t.Fatalf("mutable labels retain unobservable values: %v", labels)
	}
	if !reflect.DeepEqual(base, before) {
		t.Fatalf("changed source label identity: %v", base)
	}
}

func TestOrderedMixedMetricProjectionMatchesFullLabels(t *testing.T) {
	for _, function := range []string{"count_over_time", "rate", "bytes_over_time", "bytes_rate"} {
		plan, ok := compileOrderedJSONMetric(`sum by (level) (` + function + `({app="api"} | json level="[\"level\"]" | logfmt | drop __error__, __error_details__ | level!="" [1m]))`)
		if !ok || plan.projectedLabel != "level" {
			t.Fatalf("%s: explicit single-key plan was not projected", function)
		}
		full := *plan
		full.projectedLabel = ""
		bases := []map[string]string{
			{"app": "api", "extra": "one"},
			{"app": "api", "level": "", "extra": "two"},
			{"app": "api", "level": "stored", "extra": "three"},
			{"app": "api", "level": "body", "n": "1"},
			{"app": "api", "__error__": "source", "__error_details__": "detail", "__preserve_error__": "true"},
		}
		wide := map[string]string{"app": "api", "level": "stored"}
		for i := range 1024 {
			wide[fmt.Sprintf("unused_%d", i)] = "metadata"
		}
		bases = append(bases, wide)
		for i, base := range bases {
			before := cloneStringMap(base)
			for _, stream := range []map[string]string{{"app": "api"}, base} {
				for _, line := range []string{
					`{"level":"body","n":1}`, `{"level":"body"`, `{"level":"first","level":"second"}`,
					`{"le\u0076el":"escaped"}`, `{"level":null}`, `{"level":{"a":1}}`, `{"level":[1,2]}`,
					`{}`, `[]`, `"quoted"`, `{} level=shadow`, ` {"level":"body"}`, "",
					`level=first level=second`, `level="quoted value"`, `level=`, `other=value`,
					`{"level":"` + strings.Repeat("x", 64<<10+1) + `"}`, strings.Repeat("x", 1<<20+1),
				} {
					got, keep, err := plan.processWithContext(context.Background(), line, base, stream, base)
					want, wantKeep, wantErr := full.processWithContext(context.Background(), line, base, stream, base)
					if keep != wantKeep || fmt.Sprint(err) != fmt.Sprint(wantErr) {
						t.Fatalf("%s/base%d: keep/error differs: %v/%v vs %v/%v", function, i, keep, err, wantKeep, wantErr)
					}
					if keep && err == nil && !reflect.DeepEqual(plan.groupLabels(got), full.groupLabels(want)) {
						t.Fatalf("%s/base%d: grouped labels differ: %v vs %v", function, i, got, want)
					}
					if !reflect.DeepEqual(base, before) {
						t.Fatalf("%s/base%d: mutated original collision provenance", function, i)
					}
				}
			}
		}
	}
}

func TestOrderedMixedMetricProjectionEligibility(t *testing.T) {
	for _, tc := range []struct {
		key, grouping, stages string
		projected             bool
	}{
		{"level", "level", "", true},
		{"tenant_id", "tenant_id", "", true},
		{"value", "other", "", false},
		{"value", "value,other", "", false},
		{"value_extracted", "value_extracted", "", false},
		{"detected_level", "detected_level", "", false},
		{"service_name", "service_name", "", false},
		{"__error__", "__error__", "", false},
		{"value", "value", `| detected_level="error" `, false},
		{"value", "value", `| env="prod" `, true},
	} {
		query := `sum by (` + tc.grouping + `) (count_over_time({app="api"} ` + tc.stages + `| json ` + tc.key + `="[\"` + tc.key + `\"]" | logfmt | drop __error__, __error_details__ | ` + tc.key + `!="" [1m]))`
		plan, ok := compileOrderedJSONMetric(query)
		if !ok {
			t.Fatalf("not compiled: %s", query)
		}
		if (plan.projectedLabel != "") != tc.projected {
			t.Errorf("%s: projected=%q, want enabled=%v", query, plan.projectedLabel, tc.projected)
		}
	}
	for _, query := range []string{
		`sum by (value)(count_over_time({app="api"} | json | logfmt | drop __error__, __error_details__ | value!="" [1m]))`,
		`count_over_time({app="api"} | json | logfmt | keep value [1m])`,
		`sum by (value)(count_over_time({app="api"} | json | logfmt [1m]))`,
		`sum without (value)(count_over_time({app="api"} | json | logfmt [1m]))`,
	} {
		plan, ok := compileOrderedJSONMetric(query)
		if ok && plan.projectedLabel != "" {
			t.Errorf("general mixed plan unexpectedly projected: %s", query)
		}
	}
}
