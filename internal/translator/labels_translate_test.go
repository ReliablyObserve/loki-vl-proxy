package translator

import (
	"testing"
)

func TestTranslateLogQLWithLabels(t *testing.T) {
	// Simulate a label translator that converts underscore labels to dotted VL fields
	labelFn := func(label string) string {
		mapping := map[string]string{
			"service_name":           "service.name",
			"k8s_pod_name":           "k8s.pod.name",
			"k8s_namespace_name":     "k8s.namespace.name",
			"k8s_cluster_name":       "k8s.cluster.name",
			"deployment_environment": "deployment.environment",
			"host_name":              "host.name",
		}
		if mapped, ok := mapping[label]; ok {
			return mapped
		}
		return label
	}

	tests := []struct {
		name  string
		logql string
		want  string
	}{
		{
			name:  "service_name expands to synthetic matcher set",
			logql: `{service_name="auth"}`,
			want:  `(service_name:=auth OR "service.name":=auth OR service:=auth OR app:=auth OR application:=auth OR app_name:=auth OR name:=auth OR app_kubernetes_io_name:=auth OR container:=auth OR container_name:=auth OR "k8s.container.name":=auth OR k8s_container_name:=auth OR component:=auth OR workload:=auth OR job:=auth OR "k8s.job.name":=auth OR k8s_job_name:=auth)`,
		},
		{
			name:  "multiple labels with synthetic service_name",
			logql: `{service_name="auth",level="error"}`,
			want:  `(service_name:=auth OR "service.name":=auth OR service:=auth OR app:=auth OR application:=auth OR app_name:=auth OR name:=auth OR app_kubernetes_io_name:=auth OR container:=auth OR container_name:=auth OR "k8s.container.name":=auth OR k8s_container_name:=auth OR component:=auth OR workload:=auth OR job:=auth OR "k8s.job.name":=auth OR k8s_job_name:=auth) level:=error`,
		},
		{
			name:  "k8s label translated",
			logql: `{k8s_pod_name="my-pod"}`,
			want:  `"k8s.pod.name":=my-pod`,
		},
		{
			name:  "non-mapped label passes through",
			logql: `{app="nginx"}`,
			want:  `app:=nginx`,
		},
		{
			name:  "regex matcher with synthetic service_name",
			logql: `{service_name=~"auth.*"}`,
			want:  `(service_name:~"auth.*" OR "service.name":~"auth.*" OR service:~"auth.*" OR app:~"auth.*" OR application:~"auth.*" OR app_name:~"auth.*" OR name:~"auth.*" OR app_kubernetes_io_name:~"auth.*" OR container:~"auth.*" OR container_name:~"auth.*" OR "k8s.container.name":~"auth.*" OR k8s_container_name:~"auth.*" OR component:~"auth.*" OR workload:~"auth.*" OR job:~"auth.*" OR "k8s.job.name":~"auth.*" OR k8s_job_name:~"auth.*")`,
		},
		{
			name:  "negated matcher with synthetic service_name",
			logql: `{service_name!="auth"}`,
			want:  `-service_name:=auth -"service.name":=auth -service:=auth -app:=auth -application:=auth -app_name:=auth -name:=auth -app_kubernetes_io_name:=auth -container:=auth -container_name:=auth -"k8s.container.name":=auth -k8s_container_name:=auth -component:=auth -workload:=auth -job:=auth -"k8s.job.name":=auth -k8s_job_name:=auth`,
		},
		{
			name:  "negated regex with synthetic service_name",
			logql: `{service_name!~"auth.*"}`,
			want:  `-service_name:~"auth.*" -"service.name":~"auth.*" -service:~"auth.*" -app:~"auth.*" -application:~"auth.*" -app_name:~"auth.*" -name:~"auth.*" -app_kubernetes_io_name:~"auth.*" -container:~"auth.*" -container_name:~"auth.*" -"k8s.container.name":~"auth.*" -k8s_container_name:~"auth.*" -component:~"auth.*" -workload:~"auth.*" -job:~"auth.*" -"k8s.job.name":~"auth.*" -k8s_job_name:~"auth.*"`,
		},
		{
			name:  "service_name with line filter",
			logql: `{service_name="auth"} |= "error"`,
			want:  `(service_name:=auth OR "service.name":=auth OR service:=auth OR app:=auth OR application:=auth OR app_name:=auth OR name:=auth OR app_kubernetes_io_name:=auth OR container:=auth OR container_name:=auth OR "k8s.container.name":=auth OR k8s_container_name:=auth OR component:=auth OR workload:=auth OR job:=auth OR "k8s.job.name":=auth OR k8s_job_name:=auth) ~"error"`,
		},
		{
			name:  "backtick regex matcher",
			logql: "{service_name=~`auth.*`}",
			want:  `(service_name:~"auth.*" OR "service.name":~"auth.*" OR service:~"auth.*" OR app:~"auth.*" OR application:~"auth.*" OR app_name:~"auth.*" OR name:~"auth.*" OR app_kubernetes_io_name:~"auth.*" OR container:~"auth.*" OR container_name:~"auth.*" OR "k8s.container.name":~"auth.*" OR k8s_container_name:~"auth.*" OR component:~"auth.*" OR workload:~"auth.*" OR job:~"auth.*" OR "k8s.job.name":~"auth.*" OR k8s_job_name:~"auth.*")`,
		},
		{
			name:  "non empty app matcher",
			logql: `{app!="",service_name!=""}`,
			want:  `app:!"" service_name:!"" "service.name":!"" service:!"" app:!"" application:!"" app_name:!"" name:!"" app_kubernetes_io_name:!"" container:!"" container_name:!"" "k8s.container.name":!"" k8s_container_name:!"" component:!"" workload:!"" job:!"" "k8s.job.name":!"" k8s_job_name:!""`,
		},
		{
			name:  "parsed field non empty filter",
			logql: `{app="api"} | json | path_extracted!=""`,
			want:  `app:=api | unpack_json | filter path_extracted:!""`,
		},
		{
			name:  "translated field alias after parser maps back to dotted VL field",
			logql: `{app="api"} | json | service_name="auth"`,
			want:  `app:=api | unpack_json | filter "service.name":=auth`,
		},
		{
			name:  "dotted structured metadata filter after parser is quoted",
			logql: `{app="api"} | json | service.name="auth"`,
			want:  `app:=api | unpack_json | filter "service.name":=auth`,
		},
		{
			name:  "dotted structured metadata non empty filter is quoted",
			logql: `{app="api"} | json | service.name!=""`,
			want:  `app:=api | unpack_json | filter "service.name":!""`,
		},
		{
			name:  "native dotted field equality filter in pipeline",
			logql: `{deployment_environment="dev",k8s_namespace_name="sample_ns"} | k8s.cluster.name = ` + "`cluster-alpha`",
			want:  `"deployment.environment":=dev "k8s.namespace.name":=sample_ns "k8s.cluster.name":=cluster-alpha`,
		},
		{
			name:  "underscored alias equality filter maps to same dotted field",
			logql: `{deployment_environment="dev",k8s_namespace_name="sample_ns"} | k8s_cluster_name = ` + "`cluster-alpha`",
			want:  `"deployment.environment":=dev "k8s.namespace.name":=sample_ns "k8s.cluster.name":=cluster-alpha`,
		},
		{
			name:  "malformed spaced dotted triplet with trailing dot normalizes to valid dotted filter",
			logql: `{deployment_environment="dev",k8s_namespace_name="sample_ns"} | custom . ` + "`pipeline.processing.`" + ` = ` + "`vector-processing`",
			want:  `"deployment.environment":=dev "k8s.namespace.name":=sample_ns "custom.pipeline.processing":=vector-processing`,
		},
		{
			name:  "malformed dotted stage from drilldown degrades to dotted-prefix regex filter",
			logql: `{deployment_environment="dev",k8s_namespace_name="sample_ns"} | k8s . ` + "`cluster.`",
			want:  `"deployment.environment":=dev "k8s.namespace.name":=sample_ns ~"k8s\.cluster\."`,
		},
		{
			name:  "malformed nested dotted stage keeps full prefix for regex fallback",
			logql: `{app="api"} | custom . ` + "`pipeline.`",
			want:  `app:=api ~"custom\.pipeline\."`,
		},
		{
			name:  "repeated include filter clicks are deduplicated after parser",
			logql: `{app="api"} | json | source_message_bytes="89" | source_message_bytes = "89" | source_message_bytes = ` + "`89`",
			want:  `app:=api | unpack_json | filter source_message_bytes:=89`,
		},
		{
			name:  "repeated exclude filter clicks are deduplicated after parser",
			logql: `{app="api"} | json | source_message_bytes!="89" | source_message_bytes != "89"`,
			want:  `app:=api | unpack_json | filter -source_message_bytes:=89`,
		},
		{
			name:  "include then exclude same value keeps latest filter stage",
			logql: `{app="api"} | json | source_message_bytes="89" | source_message_bytes!="89"`,
			want:  `app:=api | unpack_json | filter -source_message_bytes:=89`,
		},
		{
			name:  "exclude then include same value keeps latest filter stage",
			logql: `{app="api"} | json | source_message_bytes!="89" | source_message_bytes="89"`,
			want:  `app:=api | unpack_json | filter source_message_bytes:=89`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TranslateLogQLWithLabels(tt.logql, labelFn)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("TranslateLogQLWithLabels(%q) =\n  got:  %q\n  want: %q", tt.logql, got, tt.want)
			}
		})
	}
}

func TestTranslateLogQLWithLabels_NilFn(t *testing.T) {
	// With nil labelFn, should behave like TranslateLogQL
	got, err := TranslateLogQLWithLabels(`{app="nginx"}`, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != `app:=nginx` {
		t.Errorf("nil labelFn: got %q, want %q", got, `app:=nginx`)
	}
}

func TestTranslateSingleLabelFilter_DottedTripletKeyOperatorValue(t *testing.T) {
	tests := []struct {
		name    string
		stage   string
		labelFn LabelTranslateFunc
		want    string
	}{
		{
			name:  "native dotted key with equals operator and literal value",
			stage: "k8s.cluster.name = `my-cluster`",
			want:  `"k8s.cluster.name":=my-cluster`,
		},
		{
			name:  "translated underscore alias still resolves to dotted key",
			stage: "k8s_cluster_name = `my-cluster`",
			labelFn: func(label string) string {
				if label == "k8s_cluster_name" {
					return "k8s.cluster.name"
				}
				return label
			},
			want: `"k8s.cluster.name":=my-cluster`,
		},
		{
			name:  "malformed spaced dotted key with trailing dot is sanitized",
			stage: "custom . `pipeline.processing.` = `vector-processing`",
			want:  `"custom.pipeline.processing":=vector-processing`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := translateSingleLabelFilter(tt.stage, tt.labelFn)
			if !ok {
				t.Fatalf("expected stage to be parsed as label/operator/value triplet: %q", tt.stage)
			}
			if got != tt.want {
				t.Fatalf("translateSingleLabelFilter(%q) = %q, want %q", tt.stage, got, tt.want)
			}
		})
	}
}
