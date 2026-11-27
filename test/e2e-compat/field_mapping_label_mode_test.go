//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// -field-mapping-label-mode against live VictoriaLogs (issue #1762): every mode
// lists its names on /labels and detected_labels, every name of a mapping reads
// values, lines and volume, and answers keep one name per field.
//
// conformance: field-mapping-label-names, semantics/field-mapping-label-modes
func TestCompat_FieldMappingLabelModes(t *testing.T) {
	n := time.Now().UnixNano()
	namespace := fmt.Sprintf("fmlm-%d", n)
	account := strconv.Itoa(int(n%1_000_000) + 17_620_000)
	now := time.Now()
	tenantLabelIngest(t, map[string]string{
		"_time": now.Add(-time.Minute).UTC().Format(time.RFC3339Nano), "_msg": "hello",
		"k8s.namespace.name": namespace, "service.name": "fmlm",
	}, "k8s.namespace.name,service.name", account)
	if status, body := hardeningRequest(t, "POST", vlURL+"/internal/force_flush", "", nil); status != 200 {
		t.Fatalf("flush %d: %s", status, body)
	}
	window := url.Values{
		"start": {strconv.FormatInt(now.Add(-10*time.Minute).UnixNano(), 10)},
		"end":   {strconv.FormatInt(now.Add(time.Second).UnixNano(), 10)},
	}.Encode()

	for mode, want := range map[proxy.FieldMappingLabelMode]struct {
		labels []string
		answer string
	}{
		proxy.FieldMappingLabelModeUnset: {[]string{"k8s_namespace_name", "service_name"}, "k8s_namespace_name"},
		proxy.FieldMappingLabelModeAlias: {[]string{"namespace", "service_name"}, "namespace"},
		proxy.FieldMappingLabelModeBoth:  {[]string{"k8s_namespace_name", "namespace", "service_name"}, "namespace"},
	} {
		t.Run(string(mode), func(t *testing.T) {
			server := labelModeServer(t, func(cfg *proxy.Config) {
				cfg.TenantLabel = ""
				cfg.AuthEnabled = true
				cfg.LabelStyle = proxy.LabelStyleUnderscores
				cfg.MetadataFieldMode = proxy.MetadataFieldModeTranslated
				cfg.FieldMappingLabelMode = mode
				cfg.ExtraLabelFields = []string{"k8s.namespace.name", "namespace"}
				cfg.FieldMappings = []proxy.FieldMapping{
					{VLField: "k8s.namespace.name", LokiLabel: "namespace"},
					{VLField: "k8s.namespace.name", LokiLabel: "k8s_namespace_name"},
				}
				tenant := proxy.TenantMapping{AccountID: account, ProjectID: "0"}
				cfg.TenantMap = map[string]proxy.TenantMapping{"fmlm": tenant}
			})
			get := func(path string) string {
				status, body := labelModeGet(t, server, "fmlm", path)
				if status != 200 {
					t.Fatalf("%s: %d %s", path, status, body)
				}
				return body
			}
			strs := func(path string) []string {
				var out struct{ Data []string }
				if err := json.Unmarshal([]byte(get(path)), &out); err != nil {
					t.Fatal(err)
				}
				return out.Data
			}
			// VictoriaLogs makes a flushed row searchable shortly after.
			deadline := time.Now().Add(30 * time.Second)
			for len(strs("/loki/api/v1/label/namespace/values?"+window)) == 0 {
				if time.Now().After(deadline) {
					t.Fatal("ingested row did not become searchable")
				}
				time.Sleep(500 * time.Millisecond)
				window = url.Values{
					"start": {strconv.FormatInt(now.Add(-10*time.Minute).UnixNano(), 10)},
					"end":   {strconv.FormatInt(time.Now().UnixNano(), 10)},
				}.Encode()
			}
			if got := strs("/loki/api/v1/labels?" + window); !slices.Equal(got, want.labels) {
				t.Errorf("labels = %v, want %v", got, want.labels)
			}
			var detected struct {
				DetectedLabels []struct{ Label string }
			}
			if err := json.Unmarshal([]byte(get("/loki/api/v1/detected_labels?"+window)), &detected); err != nil {
				t.Fatal(err)
			}
			var names []string
			for _, label := range detected.DetectedLabels {
				names = append(names, label.Label)
			}
			for _, name := range want.labels {
				if !slices.Contains(names, name) {
					t.Errorf("detected_labels %v misses %s", names, name)
				}
			}
			for _, name := range []string{"namespace", "k8s_namespace_name"} {
				if got := strs("/loki/api/v1/label/" + name + "/values?" + window); !slices.Equal(got, []string{namespace}) {
					t.Errorf("values of %s = %v", name, got)
				}
				selector := url.QueryEscape(fmt.Sprintf(`{%s=%q}`, name, namespace))
				var streams struct {
					Data struct {
						Result []struct{ Stream map[string]string }
					}
				}
				if err := json.Unmarshal([]byte(get("/loki/api/v1/query_range?query="+selector+"&"+window)), &streams); err != nil {
					t.Fatal(err)
				}
				if len(streams.Data.Result) != 1 {
					t.Fatalf("query by %s: %d streams", name, len(streams.Data.Result))
				}
				stream := streams.Data.Result[0].Stream
				if stream[want.answer] != namespace {
					t.Errorf("stream %v lacks %s", stream, want.answer)
				}
				for _, other := range []string{"namespace", "k8s_namespace_name"} {
					if _, ok := stream[other]; ok && other != want.answer {
						t.Errorf("stream %v carries a second name of the field", stream)
					}
				}
				if mode == proxy.FieldMappingLabelModeUnset && name != want.answer {
					continue // docs/KNOWN_ISSUES.md: volume by another mapping name
				}
				volume := get("/loki/api/v1/index/volume?query=" + selector + "&targetLabels=" + name + "&" + window)
				if !strings.Contains(volume, fmt.Sprintf(`%q:%q`, name, namespace)) {
					t.Errorf("volume by %s: %s", name, volume)
				}
			}
		})
	}
}

// A parsed key named like an alias in -field-mapping-label-mode alias and both
// answers as in a Loki that stored the alias (owner decision 2026-11-26 on
// #1762): the same lines go to Loki under the pre-migration names (pod,
// namespace) and to VictoriaLogs under the OpenTelemetry names, and the proxy's
// detected fields, field values, filtered lines and groupings equal Loki's.
// The lines carry pod and namespace keys whose values differ from the stream
// labels; one service has no mapped field on half of its streams.
//
// conformance: field-mapping-label-names, semantics/field-mapping-label-modes
func TestCompat_FieldMappingAliasCollisionsMatchLoki(t *testing.T) {
	n := time.Now().UnixNano()
	account := strconv.Itoa(int(n%1_000_000) + 17_630_000)
	end := time.Now().Truncate(time.Second)
	start := end.Add(-10 * time.Minute)
	type stream struct {
		svc, parser string
		mapped      bool
		i           int
	}
	var streams []stream
	for _, parser := range []string{"logfmt", "json"} {
		svc := fmt.Sprintf("fmlc-%s-%d", parser, n)
		for i := 0; i < 4; i++ {
			streams = append(streams, stream{svc, parser, i < 2, i})
		}
	}
	var lokiStreams []map[string]any
	var vlRows strings.Builder
	for _, s := range streams {
		var values [][]string
		for k := 0; k < 30; k++ {
			ts := end.Add(-5*time.Minute + time.Duration(k)*10*time.Second)
			line := fmt.Sprintf("level=info pod=line-%d-%d namespace=lns-%d ts=%d", s.i, k%2, k%3, k)
			if s.parser == "json" {
				b, _ := json.Marshal(map[string]any{"level": "warn", "pod": fmt.Sprintf("jpod-%d-%d", s.i, k%2), "namespace": fmt.Sprintf("jns-%d", k%3), "n": k})
				line = string(b)
			}
			values = append(values, []string{strconv.FormatInt(ts.UnixNano(), 10), line})
			row := map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": line, "service.name": s.svc, "shard": strconv.Itoa(s.i)}
			if s.mapped {
				row["k8s.pod.name"], row["k8s.namespace.name"] = fmt.Sprintf("pod-%d", s.i), "ns-a"
			}
			b, _ := json.Marshal(row)
			vlRows.Write(b)
			vlRows.WriteByte('\n')
		}
		labels := map[string]string{"service_name": s.svc, "shard": strconv.Itoa(s.i)}
		if s.mapped {
			labels["pod"], labels["namespace"] = fmt.Sprintf("pod-%d", s.i), "ns-a"
		}
		lokiStreams = append(lokiStreams, map[string]any{"stream": labels, "values": values})
	}
	body, _ := json.Marshal(map[string]any{"streams": lokiStreams})
	if status, out := hardeningRequest(t, "POST", lokiURL+"/loki/api/v1/push", string(body), map[string]string{"Content-Type": "application/json"}); status >= 300 {
		t.Fatalf("Loki push %d: %s", status, out)
	}
	target := vlURL + "/insert/jsonline?_stream_fields=service.name,shard,k8s.pod.name,k8s.namespace.name&_msg_field=_msg&_time_field=_time"
	if status, out := hardeningRequest(t, "POST", target, vlRows.String(), map[string]string{"AccountID": account, "ProjectID": "0", "Content-Type": "application/stream+json"}); status >= 300 {
		t.Fatalf("VictoriaLogs ingest %d: %s", status, out)
	}
	if status, out := hardeningRequest(t, "POST", vlURL+"/internal/force_flush", "", nil); status != 200 {
		t.Fatalf("flush %d: %s", status, out)
	}
	window := url.Values{"start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
	at := strconv.FormatInt(end.Unix(), 10)

	// Each request, normalized: field lists by name, parsers and jsonPath;
	// values sorted; log answers as stream label sets with their line counts;
	// metric answers as series with their value.
	type request struct {
		name, path string
		params     url.Values
		norm       func(t *testing.T, body string) string
	}
	with := func(base url.Values, kv ...string) url.Values {
		v := url.Values{}
		for k, vs := range base {
			v[k] = vs
		}
		for i := 0; i+1 < len(kv); i += 2 {
			v.Set(kv[i], kv[i+1])
		}
		return v
	}
	var reqs []request
	for _, parser := range []string{"logfmt", "json"} {
		sel := fmt.Sprintf(`{service_name="fmlc-%s-%d"}`, parser, n)
		reqs = append(reqs, request{"detected_fields " + parser, "/loki/api/v1/detected_fields", with(window, "query", sel, "limit", "1000"), collisionFieldsNorm})
		for _, f := range []string{"pod_extracted", "namespace_extracted", "pod"} {
			reqs = append(reqs, request{"values " + f + " " + parser, "/loki/api/v1/detected_field/" + f + "/values", with(window, "query", sel, "limit", "1000"), collisionValuesNorm})
		}
		q := sel + " | " + parser
		for _, filter := range []string{`pod_extracted=~".+-1"`, `pod="pod-0"`, `pod=~"(pod-1|.*-2-0)"`, `namespace_extracted!=""`} {
			reqs = append(reqs, request{"logs " + filter + " " + parser, "/loki/api/v1/query_range", with(window, "query", q+" | "+filter, "limit", "1000"), collisionStreamsNorm})
		}
		for _, by := range []string{"pod_extracted", "pod", "namespace, namespace_extracted"} {
			m := fmt.Sprintf("sum by (%s) (count_over_time(%s [10m]))", by, q)
			reqs = append(reqs, request{"instant " + m, "/loki/api/v1/query", url.Values{"query": {m}, "time": {at}}, collisionVectorNorm})
		}
		// The Logs Drilldown field breakdown of a renamed key, in count and rate
		// form, and with a stream label in the grouping too: the plan writes these
		// in alias and both mode.
		for _, m := range []string{
			fmt.Sprintf(`sum by (pod_extracted) (count_over_time(%s | pod_extracted!="" [10m]))`, q),
			fmt.Sprintf(`sum by (pod_extracted) (rate(%s | pod_extracted!="" [10m]))`, q),
			fmt.Sprintf(`sum by (pod, namespace_extracted) (count_over_time(%s | pod_extracted!="" [10m]))`, q),
		} {
			reqs = append(reqs, request{"instant " + m, "/loki/api/v1/query", url.Values{"query": {m}, "time": {at}}, collisionVectorNorm})
		}
	}

	// Loki answers the pushed lines (its ingester holds them right away).
	deadline := time.Now().Add(60 * time.Second)
	for {
		status, out := hardeningRequest(t, "GET", lokiURL+reqs[0].path+"?"+reqs[0].params.Encode(), "", nil)
		if status == 200 && strings.Contains(string(out), "pod_extracted") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Loki does not answer the pushed lines: %d %s", status, out)
		}
		time.Sleep(time.Second)
	}
	want := map[string]string{}
	for _, r := range reqs {
		status, out := hardeningRequest(t, "GET", lokiURL+r.path+"?"+r.params.Encode(), "", nil)
		if status != 200 {
			t.Fatalf("Loki %s: %d %s", r.name, status, out)
		}
		want[r.name] = r.norm(t, string(out))
		if want[r.name] == "" {
			t.Fatalf("Loki answers %s with nothing: no comparison", r.name)
		}
	}
	for _, mode := range []proxy.FieldMappingLabelMode{proxy.FieldMappingLabelModeAlias, proxy.FieldMappingLabelModeBoth} {
		t.Run(string(mode), func(t *testing.T) {
			server := labelModeServer(t, func(cfg *proxy.Config) {
				cfg.TenantLabel = ""
				cfg.AuthEnabled = true
				cfg.LabelStyle = proxy.LabelStyleUnderscores
				cfg.MetadataFieldMode = proxy.MetadataFieldModeTranslated
				cfg.FieldMappingLabelMode = mode
				cfg.FieldMappings = []proxy.FieldMapping{
					{VLField: "k8s.pod.name", LokiLabel: "pod"}, {VLField: "k8s.pod.name", LokiLabel: "k8s_pod_name"},
					{VLField: "k8s.namespace.name", LokiLabel: "namespace"}, {VLField: "k8s.namespace.name", LokiLabel: "k8s_namespace_name"},
				}
				cfg.TenantMap = map[string]proxy.TenantMapping{"fmlc": {AccountID: account, ProjectID: "0"}}
			})
			deadline := time.Now().Add(30 * time.Second)
			for {
				status, out := labelModeGet(t, server, "fmlc", reqs[0].path+"?"+reqs[0].params.Encode())
				if status == 200 && strings.Contains(out, "pod_extracted") {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("VictoriaLogs does not answer the ingested rows: %d %s", status, out)
				}
				time.Sleep(500 * time.Millisecond)
			}
			for _, r := range reqs {
				status, out := labelModeGet(t, server, "fmlc", r.path+"?"+r.params.Encode())
				if status != 200 {
					t.Errorf("%s: %d %s", r.name, status, out)
					continue
				}
				if got := r.norm(t, out); got != want[r.name] {
					t.Errorf("%s:\n proxy %s\n  Loki %s", r.name, got, want[r.name])
				}
			}
		})
	}
}

// collisionFieldsNorm is a detected_fields answer by name, parsers and
// jsonPath, without the service_name entry of no values the proxy adds for
// OpenTelemetry streams in every mode (a difference main has too).
func collisionFieldsNorm(t *testing.T, body string) string {
	var out struct {
		Fields []struct {
			Label       string   `json:"label"`
			Parsers     []string `json:"parsers"`
			JSONPath    []string `json:"jsonPath"`
			Cardinality int      `json:"cardinality"`
		} `json:"fields"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatal(err)
	}
	var rows []string
	for _, f := range out.Fields {
		if f.Label == "service_name" && len(f.Parsers) == 0 && f.Cardinality == 0 {
			continue
		}
		slices.Sort(f.Parsers)
		rows = append(rows, fmt.Sprintf("%s%v%v=%d", f.Label, f.Parsers, f.JSONPath, f.Cardinality))
	}
	slices.Sort(rows)
	return strings.Join(rows, " ")
}

func collisionValuesNorm(t *testing.T, body string) string {
	var out struct {
		Values []string `json:"values"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatal(err)
	}
	slices.Sort(out.Values)
	return strings.Join(out.Values, ",")
}

func collisionStreamsNorm(t *testing.T, body string) string {
	var out struct {
		Data struct {
			Result []struct {
				Stream map[string]string `json:"stream"`
				Values [][]string        `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatal(err)
	}
	var rows []string
	for _, s := range out.Data.Result {
		b, _ := json.Marshal(s.Stream)
		rows = append(rows, fmt.Sprintf("%s=%d", b, len(s.Values)))
	}
	slices.Sort(rows)
	return strings.Join(rows, " ")
}

func collisionVectorNorm(t *testing.T, body string) string {
	var out struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  []any             `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatal(err)
	}
	var rows []string
	for _, s := range out.Data.Result {
		b, _ := json.Marshal(s.Metric)
		rows = append(rows, fmt.Sprintf("%s=%v", b, s.Value[1]))
	}
	slices.Sort(rows)
	return strings.Join(rows, " ")
}
