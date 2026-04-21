package translator

import "testing"

// =============================================================================
// Real-world LogQL patterns from production deployments.
// Source: Grafana Loki v3.7 docs, Stack Overflow, community reports.
// TDD: tests first, then fix translator to pass.
// =============================================================================

func TestRealWorld_ErrorHandling(t *testing.T) {
	tests := []struct {
		name  string
		logql string
		want  string
	}{
		{
			name:  "error filter after json parse",
			logql: `{app="nginx"} | json | __error__ = ""`,
			want:  `app:=nginx | unpack_json | filter __error__:=""`,
		},
		{
			name:  "error filter not JSONParserErr",
			logql: `{app="nginx"} | json | __error__ != "JSONParserErr"`,
			want:  `app:=nginx | unpack_json | filter -__error__:=JSONParserErr`,
		},
		{
			name:  "drop error label",
			logql: `{app="nginx"} | json | drop __error__, __error_details__`,
			want:  `app:=nginx | unpack_json | delete __error__, __error_details__`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TranslateLogQL(tt.logql)
			if err != nil {
				t.Fatalf("error: %v", err)
			}
			if got != tt.want {
				t.Errorf("TranslateLogQL()\n  got  = %q\n  want = %q", got, tt.want)
			}
		})
	}
}

func TestRealWorld_NegativeLineFilters(t *testing.T) {
	tests := []struct {
		name  string
		logql string
		want  string
	}{
		{
			name:  "exclude health ready metrics — three chained",
			logql: `{app="nginx"} != "/health" != "/ready" != "/metrics"`,
			want:  `app:=nginx NOT ~"/health" NOT ~"/ready" NOT ~"/metrics"`,
		},
		{
			name:  "kafka exclude replica manager noise",
			logql: `{instance=~"kafka-[23]",name="kafka"} != "kafka.server:type=ReplicaManager"`,
			want:  `instance:~"kafka-[23]" name:=kafka NOT ~"kafka.server:type=ReplicaManager"`,
		},
		{
			name:  "case insensitive regex match",
			logql: `{job="nginx"} |~ "(?i)error|exception|fatal"`,
			want:  `job:=nginx ~"(?i)error|exception|fatal"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TranslateLogQL(tt.logql)
			if err != nil {
				t.Fatalf("error: %v", err)
			}
			if got != tt.want {
				t.Errorf("TranslateLogQL()\n  got  = %q\n  want = %q", got, tt.want)
			}
		})
	}
}

func TestRealWorld_ComplexKubernetes(t *testing.T) {
	tests := []struct {
		name  string
		logql string
		want  string
	}{
		{
			name:  "k8s OOMKilled events",
			logql: `{job="integrations/kubernetes/eventhandler"} |= "OOMKilled" | json`,
			want:  `job:=integrations/kubernetes/eventhandler ~"OOMKilled" | unpack_json`,
		},
		{
			name:  "cross cluster pod search with json filter",
			logql: `{cluster=~"eks-.*",namespace="payments"} | json | status >= 500`,
			want:  `cluster:~"eks-.*" namespace:=payments | unpack_json | filter status:>=500`,
		},
		{
			name:  "container restart correlation",
			logql: `{namespace=~"prod.*",container!=""} |= "Back-off restarting failed container"`,
			want:  `namespace:~"prod.*" container:!"" ~"Back-off restarting failed container"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TranslateLogQL(tt.logql)
			if err != nil {
				t.Fatalf("error: %v", err)
			}
			if got != tt.want {
				t.Errorf("TranslateLogQL()\n  got  = %q\n  want = %q", got, tt.want)
			}
		})
	}
}

func TestRealWorld_MultiStage(t *testing.T) {
	tests := []struct {
		name  string
		logql string
		want  string
	}{
		{
			name:  "logfmt filter compound or",
			logql: `{job="loki-dev/query-frontend"} |= "metrics.go" != "out of order" | logfmt | duration > "30s"`,
			want:  `job:=loki-dev/query-frontend ~"metrics.go" NOT ~"out of order" | unpack_logfmt | filter duration:>30s`,
		},
		{
			name:  "json then label_format then line_format",
			logql: `{app="api"} | json | label_format info="{{.method}} {{.path}}" | line_format "{{.info}} {{.status}}"`,
			want:  `app:=api | unpack_json | format "<method> <path>" as info | format "<info> <status>"`,
		},
		{
			name:  "pattern then filter then drop",
			logql: `{job="nginx"} | pattern "<ip> - - <_> <method> <uri> <status>" | status >= 400 | drop ip`,
			want:  `job:=nginx | extract "<ip> - - <_> <method> <uri> <status>" | filter status:>=400 | delete ip`,
		},
		{
			name:  "drilldown multi-level filter after parser chain",
			logql: `{cluster="us-east-1"} | detected_level="error" or detected_level="info" or detected_level="warn" | json | logfmt | drop __error__, __error_details__`,
			want:  `cluster:=us-east-1 (level:=error or level:=info or level:=warn) | unpack_json | unpack_logfmt | delete __error__, __error_details__`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TranslateLogQL(tt.logql)
			if err != nil {
				t.Fatalf("error: %v", err)
			}
			if got != tt.want {
				t.Errorf("TranslateLogQL()\n  got  = %q\n  want = %q", got, tt.want)
			}
		})
	}
}

func TestRealWorld_MetricAlerts(t *testing.T) {
	tests := []struct {
		name  string
		logql string
		want  string
	}{
		{
			name:  "error rate sum by job",
			logql: `sum by (job) (rate({app="foo",env="production"} |= "error" [5m]))`,
			want:  `app:=foo env:=production ~"error" | stats by (job) rate()`,
		},
		{
			name:  "count by level",
			logql: `sum(count_over_time({job="mysql"}[5m])) by (level)`,
			want:  `job:=mysql | stats by (level) count()`,
		},
		{
			name:  "bytes rate by namespace",
			logql: `sum by (namespace) (bytes_rate({cluster="prod-k8s"}[5m]))`,
			want:  `__binary__:/:cluster:=prod-k8s | stats by (namespace) sum_len(_msg)|||300`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TranslateLogQL(tt.logql)
			if err != nil {
				t.Fatalf("error: %v", err)
			}
			if got != tt.want {
				t.Errorf("TranslateLogQL()\n  got  = %q\n  want = %q", got, tt.want)
			}
		})
	}
}
