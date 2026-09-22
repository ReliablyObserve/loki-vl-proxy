package translator

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

func underscoreLabelsForTest(label string) string {
	if label == "service_name" {
		return "service.name"
	}
	return label
}

const coalesceServiceNamePipe = `| coalesce(service_name, "service.name", service, app, application, app_name, name, app_kubernetes_io_name, container, container_name, "k8s.container.name", k8s_container_name, component, workload, job, "k8s.job.name", k8s_job_name) default "unknown_service" as service_name`

func TestServiceNameGroupingComputesDerivedValue(t *testing.T) {
	caps := logsql.CapabilitiesFor("v1.52.0")
	for _, tc := range []struct{ logql, want string }{
		{
			`sum by (service_name) (count_over_time({env="prod"}[5m]))`,
			`env:="prod" ` + coalesceServiceNamePipe + ` | stats by (service_name) count()`,
		},
		{
			`sum by (namespace, service_name) (bytes_over_time({env="prod"} | json [5m]))`,
			`env:="prod" ` + coalesceServiceNamePipe + ` | unpack_json | stats by (namespace, service_name) sum_len(_msg)`,
		},
		{
			`sum by (service_name) (count_over_time({env="prod"}[5m])) / sum by (service_name) (count_over_time({env="dev"}[5m]))`,
			BinaryMetricPrefix + `/:env:="prod" ` + coalesceServiceNamePipe + ` | stats by (service_name) count()|||env:="dev" ` + coalesceServiceNamePipe + ` | stats by (service_name) count()`,
		},
		// Grouping by another label leaves the query unchanged.
		{
			`sum by (pod) (count_over_time({service_name="x"}[5m]))`,
			serviceNameMatcherFilter(`"x"`, false, false) + ` | stats by (pod) count()`,
		},
	} {
		got, err := TranslateLogQLWithCapabilities(tc.logql, underscoreLabelsForTest, nil, caps)
		if err != nil || got != tc.want {
			t.Errorf("%s:\n got %s (%v)\nwant %s", tc.logql, got, err, tc.want)
		}
	}
}

func TestServiceNameGroupingRateComputesDerivedValueOnce(t *testing.T) {
	got, err := TranslateLogQLWithCapabilities(`sum by (service_name) (rate({env="prod"} | logfmt [1m]))`, underscoreLabelsForTest, nil, logsql.CapabilitiesFor("v1.52.0"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(got, "| coalesce(") != 1 || !strings.HasPrefix(got, `env:="prod" `+coalesceServiceNamePipe+` | unpack_logfmt | stats by (service_name) `) || strings.Contains(got, "service.name)") {
		t.Fatalf("got %s", got)
	}
}

// Before VictoriaLogs v1.51 there is no coalesce pipe; format pipes that keep
// a non-empty destination compute the same value.
func TestServiceNameGroupingFallsBackToFormatPipes(t *testing.T) {
	got, err := TranslateLogQLWithCapabilities(`sum by (service_name) (count_over_time({env="prod"}[5m]))`, underscoreLabelsForTest, nil, logsql.CapabilitiesFor("v1.50.0"))
	if err != nil {
		t.Fatal(err)
	}
	var want strings.Builder
	want.WriteString(`env:="prod" `)
	for _, field := range syntheticServiceNameFields[1:] {
		want.WriteString(`| format "<` + field + `>" as service_name keep_original_fields `)
	}
	want.WriteString(`| format "unknown_service" as service_name keep_original_fields | stats by (service_name) count()`)
	if got != want.String() {
		t.Fatalf("got  %s\nwant %s", got, want.String())
	}
}

// A matcher that reduces to match-all or match-none stays a valid filter in a
// pipe position.
func TestServiceNameEmptyMatchersAfterParserStage(t *testing.T) {
	for _, tc := range []struct{ logql, want string }{
		{`{app="x"} | json | service_name!=""`, `app:="x" | unpack_json | filter *`},
		{`{app="x"} | json | service_name=""`, `app:="x" | unpack_json | filter ` + matchNoStreamsFilter},
		{`{app="x"} | service_name!=""`, `app:="x" *`},
	} {
		got, err := TranslateLogQLWithLabels(tc.logql, nil)
		if err != nil || got != tc.want {
			t.Errorf("%s:\n got %s (%v)\nwant %s", tc.logql, got, err, tc.want)
		}
	}
}
