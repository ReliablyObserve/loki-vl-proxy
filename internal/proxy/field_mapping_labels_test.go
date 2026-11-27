package proxy

import (
	"context"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// mappingVL is a VictoriaLogs stand-in holding one stream whose fields are
// stream fields, with a call count per endpoint. meta holds the row's fields
// that are no stream fields (structured metadata); queries records the LogsQL
// of every query call.
type mappingVL struct {
	fields map[string]string
	meta   map[string]string

	mu      sync.Mutex
	calls   map[string]int
	queries []string
}

func (v *mappingVL) server(t testing.TB) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v.mu.Lock()
		if v.calls == nil {
			v.calls = map[string]int{}
		}
		if strings.HasPrefix(r.URL.Path, "/select/") {
			v.calls[r.URL.Path]++
		}
		v.mu.Unlock()
		_ = r.ParseForm()
		if r.URL.Path == "/select/logsql/query" {
			v.mu.Lock()
			v.queries = append(v.queries, r.Form.Get("query"))
			v.mu.Unlock()
		}
		field := r.Form.Get("field")
		names := make([]string, 0, len(v.fields))
		for name := range v.fields {
			names = append(names, name)
		}
		sort.Strings(names)
		var pairs []string
		for _, name := range names {
			pairs = append(pairs, name+"="+strconv.Quote(v.fields[name]))
		}
		stream := "{" + strings.Join(pairs, ",") + "}"
		listing := func(values ...string) {
			items := make([]map[string]any, 0, len(values))
			for _, value := range values {
				items = append(items, map[string]any{"value": value, "hits": 1})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"values": items})
		}
		switch r.URL.Path {
		case "/select/logsql/stream_field_names":
			listing(names...)
		case "/select/logsql/field_names":
			all := slices.Concat(names, slices.Sorted(maps.Keys(v.meta)))
			listing(all...)
		case "/select/logsql/stream_field_values":
			if value, ok := v.fields[field]; ok {
				listing(value)
				return
			}
			listing()
		case "/select/logsql/field_values":
			if value, ok := v.fields[field]; ok {
				listing(value)
				return
			}
			if value, ok := v.meta[field]; ok {
				listing(value)
				return
			}
			listing()
		case "/select/logsql/streams":
			listing(stream)
		case "/select/logsql/query":
			row := map[string]string{"_time": "2026-01-01T00:00:00Z", "_msg": "hello", "_stream": stream, "_stream_id": "0"}
			for name, value := range v.fields {
				row[name] = value
			}
			for name, value := range v.meta {
				row[name] = value
			}
			_ = json.NewEncoder(w).Encode(row)
		case "/select/logsql/stats_query", "/select/logsql/stats_query_range":
			// One row grouped by the requested fields, or by _stream.
			metric := map[string]string{}
			query := r.Form.Get("query")
			by := query[strings.LastIndex(query, "stats by (")+len("stats by ("):]
			by = by[:strings.Index(by, ")")]
			for _, name := range strings.Split(by, ",") {
				name = strings.Trim(strings.TrimSpace(name), "`\"")
				if name == "_stream" {
					metric[name] = stream
				} else if value, ok := v.fields[name]; ok {
					metric[name] = value
				} else if value, ok := v.meta[name]; ok {
					metric[name] = value
				}
			}
			if r.URL.Path == "/select/logsql/stats_query_range" {
				_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{
					"resultType": "matrix",
					"result":     []any{map[string]any{"metric": metric, "values": []any{[]any{1767225300.0, "5"}}}},
				}})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{
				"resultType": "vector",
				"result":     []any{map[string]any{"metric": metric, "value": []any{1767225600.0, "5"}}},
			}})
		default:
			_, _ = io.WriteString(w, "{}")
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// snapshot returns the calls made since the previous snapshot.
func (v *mappingVL) snapshot() map[string]int {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := v.calls
	v.calls = map[string]int{}
	return out
}

func mappingVLData() *mappingVL {
	return &mappingVL{fields: map[string]string{
		"k8s.namespace.name": "prod",
		"k8s.pod.name":       "api-1",
		"host.name":          "node-a",
		"custom.team":        "core",
		"service.name":       "api",
	}}
}

// One alias with the base name unmapped (pod), several aliases including the
// base name as the last mapping (namespace, ns), a custom field with one alias
// (team), an unmapped field (host.name) and declared fields with and without data.
func mappingTestMappings() []FieldMapping {
	return []FieldMapping{
		{VLField: "k8s.namespace.name", LokiLabel: "namespace"},
		{VLField: "k8s.namespace.name", LokiLabel: "ns"},
		{VLField: "k8s.namespace.name", LokiLabel: "k8s_namespace_name"},
		{VLField: "k8s.pod.name", LokiLabel: "pod"},
		{VLField: "custom.team", LokiLabel: "team"},
		{VLField: "k8s.cluster.name", LokiLabel: "cluster"},
		{VLField: "deployment.environment", LokiLabel: "env"},
	}
}

func newMappingProxy(t testing.TB, backend string, c *cache.Cache, cfg Config) *Proxy {
	t.Helper()
	if c == nil {
		c = cache.New(time.Minute, 1000)
		t.Cleanup(c.Close)
	}
	cfg.BackendURL, cfg.Cache, cfg.LogLevel = backend, c, "error"
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	p.storeBackendVersion("v1.54.0", "v1.54.0")
	return p
}

func mappingGet(t testing.TB, p *Proxy, path string, params url.Values) []byte {
	t.Helper()
	if params == nil {
		params = url.Values{}
	}
	if params.Get("start") == "" {
		params.Set("start", "1767222000000000000")
		params.Set("end", "1767225601000000000")
	}
	mux := http.NewServeMux()
	p.RegisterProxyRoutes(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path+"?"+params.Encode(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: %d %s", path, rec.Code, rec.Body.String())
	}
	return rec.Body.Bytes()
}

func mappingStrings(t testing.TB, p *Proxy, path string) []string {
	t.Helper()
	var out struct {
		Data []string `json:"data"`
	}
	if err := json.Unmarshal(mappingGet(t, p, path, nil), &out); err != nil {
		t.Fatal(err)
	}
	return out.Data
}

func mappingDetectedLabels(t testing.TB, p *Proxy) []string {
	t.Helper()
	var out struct {
		DetectedLabels []struct {
			Label string `json:"label"`
		} `json:"detectedLabels"`
	}
	if err := json.Unmarshal(mappingGet(t, p, "/loki/api/v1/detected_labels", url.Values{"query": {`{pod="api-1"}`}}), &out); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, l := range out.DetectedLabels {
		names = append(names, l.Label)
	}
	sort.Strings(names)
	return names
}

// mappingAnswerLabels returns the sorted label names of the first series of a
// /series or log query answer.
func mappingAnswerLabels(t testing.TB, p *Proxy, path string) []string {
	t.Helper()
	body := mappingGet(t, p, path, url.Values{"query": {`{pod="api-1"}`}, "match[]": {`{pod="api-1"}`}})
	var labels map[string]string
	if strings.HasSuffix(path, "series") {
		var out struct {
			Data []map[string]string `json:"data"`
		}
		if err := json.Unmarshal(body, &out); err != nil || len(out.Data) != 1 {
			t.Fatalf("series: %v %s", err, body)
		}
		labels = out.Data[0]
	} else {
		var out struct {
			Data struct {
				Result []struct {
					Stream map[string]string `json:"stream"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &out); err != nil || len(out.Data.Result) != 1 {
			t.Fatalf("query: %v %s", err, body)
		}
		labels = out.Data.Result[0].Stream
	}
	var names []string
	for name := range labels {
		if name != "detected_level" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

// mappingVolume returns the metric of every series of a volume answer.
func mappingVolume(t testing.TB, p *Proxy, params url.Values) []map[string]string {
	t.Helper()
	body := mappingGet(t, p, "/loki/api/v1/index/volume", params)
	var out struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	var metrics []map[string]string
	for _, r := range out.Data.Result {
		metrics = append(metrics, r.Metric)
	}
	return metrics
}

// The reproduction of issue #1762: /labels with two mappings of one field and
// declared names, in each mode.
//
// conformance: field-mapping-label-names, semantics/field-mapping-label-modes
func TestFieldMappingLabelMode_IssueReproduction(t *testing.T) {
	for mode, want := range map[FieldMappingLabelMode][]string{
		"":                            {"csa_public_key", "k8s_namespace_name", "service_name"},
		FieldMappingLabelModeOriginal: {"csa_public_key", "k8s_namespace_name", "service_name"},
		FieldMappingLabelModeAlias:    {"csa_public_key", "namespace", "service_name"},
		FieldMappingLabelModeBoth:     {"csa_public_key", "k8s_namespace_name", "namespace", "service_name"},
	} {
		t.Run(string(mode), func(t *testing.T) {
			vl := &mappingVL{fields: map[string]string{"k8s.namespace.name": "prod", "service.name": "api"}}
			p := newMappingProxy(t, vl.server(t).URL, nil, Config{
				LabelStyle: LabelStyleUnderscores, MetadataFieldMode: MetadataFieldModeTranslated,
				FieldMappingLabelMode: mode,
				ExtraLabelFields:      []string{"k8s.namespace.name", "csa_public_key", "namespace"},
				FieldMappings: []FieldMapping{
					{VLField: "k8s.namespace.name", LokiLabel: "namespace"},
					{VLField: "k8s.namespace.name", LokiLabel: "k8s_namespace_name"},
				},
			})
			if got := mappingStrings(t, p, "/loki/api/v1/labels"); !slices.Equal(got, want) {
				t.Fatalf("labels = %v, want %v", got, want)
			}
			for _, name := range []string{"namespace", "k8s_namespace_name"} {
				if got := mappingStrings(t, p, "/loki/api/v1/label/"+name+"/values"); !slices.Equal(got, []string{"prod"}) {
					t.Fatalf("values of %s = %v", name, got)
				}
			}
		})
	}
}

// A declared name that is a mapping's loki_label is listed as declared in the
// listing modes, with or without data, and a declared base name of a custom
// field reads that field.
//
// conformance: field-mapping-label-names, semantics/field-mapping-label-modes
func TestFieldMappingLabelMode_DeclaredMappingNames(t *testing.T) {
	for mode, want := range map[FieldMappingLabelMode][]string{
		FieldMappingLabelModeUnset: {"k8s_namespace_name", "pod", "service_name"},
		// No alias in the original mode: a declared alias lists its field's base name.
		FieldMappingLabelModeOriginal: {"k8s_namespace_name", "k8s_pod_name", "service_name"},
		FieldMappingLabelModeAlias:    {"k8s_namespace_name", "namespace", "pod", "service_name"},
		FieldMappingLabelModeBoth:     {"k8s_namespace_name", "namespace", "pod", "service_name"},
	} {
		vl := &mappingVL{fields: map[string]string{"k8s.namespace.name": "prod", "service.name": "api"}}
		p := newMappingProxy(t, vl.server(t).URL, nil, Config{LabelStyle: LabelStyleUnderscores, FieldMappingLabelMode: mode,
			ExtraLabelFields: []string{"k8s_namespace_name", "pod"},
			FieldMappings: []FieldMapping{
				{VLField: "k8s.namespace.name", LokiLabel: "namespace"},
				{VLField: "k8s.namespace.name", LokiLabel: "k8s_namespace_name"},
				{VLField: "k8s.pod.name", LokiLabel: "pod"},
			}})
		if got := mappingStrings(t, p, "/loki/api/v1/labels"); !slices.Equal(got, want) {
			t.Errorf("%s: labels = %v, want %v", mode, got, want)
		}
	}
}

// Every mode on every label endpoint, for both label styles and every metadata
// field mode: one alias, several aliases, an unmapped field and declared fields.
//
// conformance: field-mapping-label-names, semantics/field-mapping-label-modes, loki_api_v1_labels, loki_api_v1_label_name_values
func TestFieldMappingLabelMode_Endpoints(t *testing.T) {
	type want struct {
		labels, detected, answers []string
	}
	underscores := map[FieldMappingLabelMode]want{
		// The default lists the last mapping of each field; the declared fields
		// without data keep their last mapping too.
		FieldMappingLabelModeUnset: {
			labels:   []string{"cluster", "env", "host_name", "k8s_namespace_name", "pod", "service_name", "team"},
			detected: []string{"host_name", "k8s_namespace_name", "pod", "service_name", "team"},
			answers:  []string{"host_name", "k8s_namespace_name", "pod", "service_name", "team"},
		},
		// cluster was declared by its alias; deployment.environment by its field,
		// so without data it is not listed under the alias.
		FieldMappingLabelModeAlias: {
			labels:   []string{"cluster", "deployment_environment", "host_name", "namespace", "ns", "pod", "service_name", "team"},
			detected: []string{"host_name", "namespace", "ns", "pod", "service_name", "team"},
			answers:  []string{"host_name", "ns", "pod", "service_name", "team"},
		},
		FieldMappingLabelModeBoth: {
			labels:   []string{"cluster", "custom_team", "deployment_environment", "host_name", "k8s_namespace_name", "k8s_pod_name", "namespace", "ns", "pod", "service_name", "team"},
			detected: []string{"custom_team", "host_name", "k8s_namespace_name", "k8s_pod_name", "namespace", "ns", "pod", "service_name", "team"},
			answers:  []string{"host_name", "ns", "pod", "service_name", "team"},
		},
	}
	passthrough := map[FieldMappingLabelMode]want{
		FieldMappingLabelModeUnset: {
			labels:   []string{"cluster", "env", "host.name", "k8s_namespace_name", "pod", "service.name", "service_name", "team"},
			detected: []string{"host.name", "k8s_namespace_name", "pod", "service.name", "service_name", "team"},
			answers:  []string{"host.name", "k8s_namespace_name", "pod", "service.name", "service_name", "team"},
		},
		// Without translation the base name is the stored name, so
		// k8s_namespace_name is one more alias, and the last one.
		FieldMappingLabelModeAlias: {
			labels:   []string{"cluster", "deployment.environment", "host.name", "k8s_namespace_name", "namespace", "ns", "pod", "service.name", "service_name", "team"},
			detected: []string{"host.name", "k8s_namespace_name", "namespace", "ns", "pod", "service.name", "service_name", "team"},
			answers:  []string{"host.name", "k8s_namespace_name", "pod", "service.name", "service_name", "team"},
		},
		FieldMappingLabelModeBoth: {
			labels:   []string{"cluster", "custom.team", "deployment.environment", "host.name", "k8s.namespace.name", "k8s.pod.name", "k8s_namespace_name", "namespace", "ns", "pod", "service.name", "service_name", "team"},
			detected: []string{"custom.team", "host.name", "k8s.namespace.name", "k8s.pod.name", "k8s_namespace_name", "namespace", "ns", "pod", "service.name", "service_name", "team"},
			answers:  []string{"host.name", "k8s_namespace_name", "pod", "service.name", "service_name", "team"},
		},
	}
	for style, wants := range map[LabelStyle]map[FieldMappingLabelMode]want{LabelStyleUnderscores: underscores, LabelStylePassthrough: passthrough} {
		for _, metadata := range []MetadataFieldMode{MetadataFieldModeTranslated, MetadataFieldModeHybrid, MetadataFieldModeNative} {
			originalCalls := map[string]map[string]int{}
			for _, mode := range []FieldMappingLabelMode{FieldMappingLabelModeUnset, FieldMappingLabelModeAlias, FieldMappingLabelModeBoth} {
				t.Run(string(style)+"/"+string(metadata)+"/"+string(mode), func(t *testing.T) {
					vl := mappingVLData()
					backend := vl.server(t).URL
					newProxy := func() *Proxy {
						return newMappingProxy(t, backend, nil, Config{
							LabelStyle: style, MetadataFieldMode: metadata, FieldMappingLabelMode: mode,
							FieldMappings:    mappingTestMappings(),
							ExtraLabelFields: []string{"cluster", "deployment.environment", "host.name"},
						})
					}
					p := newProxy()
					w := wants[mode]
					// calls runs one request twice and holds the VictoriaLogs calls
					// of both to the default mode's.
					calls := func(name string, request func()) {
						t.Helper()
						for _, run := range []string{" cold", " repeat"} {
							vl.snapshot()
							request()
							got := vl.snapshot()
							if mode == FieldMappingLabelModeUnset {
								originalCalls[name+run] = got
							} else if base, ok := originalCalls[name+run]; ok && !maps.Equal(got, base) {
								t.Errorf("%s%s: VictoriaLogs calls %v, default mode %v", name, run, got, base)
							}
							if run == " repeat" && name != "series" && len(got) != 0 {
								t.Errorf("%s repeat made VictoriaLogs calls: %v", name, got)
							}
						}
					}
					calls("labels", func() {
						if got := mappingStrings(t, p, "/loki/api/v1/labels"); !slices.Equal(got, w.labels) {
							t.Errorf("labels = %v\nwant %v", got, w.labels)
						}
					})
					calls("detected_labels", func() {
						if got := mappingDetectedLabels(t, p); !slices.Equal(got, w.detected) {
							t.Errorf("detected_labels = %v\nwant %v", got, w.detected)
						}
					})
					calls("series", func() {
						if got := mappingAnswerLabels(t, p, "/loki/api/v1/series"); !slices.Equal(got, w.answers) {
							t.Errorf("series labels = %v\nwant %v", got, w.answers)
						}
					})
					if metadata == MetadataFieldModeTranslated {
						if got := mappingAnswerLabels(t, p, "/loki/api/v1/query_range"); !slices.Equal(got, w.answers) {
							t.Errorf("stream labels = %v\nwant %v", got, w.answers)
						}
					}
					// Every name of a mapping reads values in every mode, and so does
					// every name a listing shows. Each name asks a proxy with cold
					// caches, so the calls are the name's own.
					values := map[string]string{
						"namespace": "prod", "ns": "prod", "k8s_namespace_name": "prod",
						"pod": "api-1", "team": "core",
					}
					if style == LabelStyleUnderscores {
						values["host_name"], values["k8s_pod_name"] = "node-a", "api-1"
						values["custom_team"] = "core"
					} else {
						values["host.name"] = "node-a"
					}
					names := slices.Sorted(maps.Keys(values))
					for _, name := range names {
						// A custom field's -label-style name is a query name in the
						// listing modes, which list it; the default has no such name.
						if mode == FieldMappingLabelModeUnset && name == "custom_team" {
							continue
						}
						p = newProxy()
						calls("values "+name, func() {
							if got := mappingStrings(t, p, "/loki/api/v1/label/"+name+"/values"); !slices.Equal(got, []string{values[name]}) {
								t.Errorf("values of %s = %v, want %s", name, got, values[name])
							}
						})
					}
					if mode == FieldMappingLabelModeUnset {
						return
					}
					// Volume answers under whichever name targetLabels asks for.
					p = newProxy()
					for _, name := range names {
						value := values[name]
						got := mappingVolume(t, p, url.Values{"query": {`{pod="api-1"}`}, "targetLabels": {name}})
						if len(got) != 1 || got[0][name] != value {
							t.Errorf("volume by %s = %v, want %s", name, got, value)
						}
					}
				})
			}
		}
	}
}

// aggregateBy=labels without targetLabels names every label a listing shows.
//
// conformance: field-mapping-label-names, semantics/field-mapping-label-modes
func TestFieldMappingLabelMode_VolumeLabelNames(t *testing.T) {
	for mode, want := range map[FieldMappingLabelMode][]string{
		FieldMappingLabelModeUnset: {"host_name", "k8s_namespace_name", "pod", "service_name", "team"},
		FieldMappingLabelModeAlias: {"host_name", "namespace", "ns", "pod", "service_name", "team"},
		FieldMappingLabelModeBoth:  {"custom_team", "host_name", "k8s_namespace_name", "k8s_pod_name", "namespace", "ns", "pod", "service_name", "team"},
	} {
		vl := mappingVLData()
		p := newMappingProxy(t, vl.server(t).URL, nil, Config{LabelStyle: LabelStyleUnderscores, FieldMappingLabelMode: mode, FieldMappings: mappingTestMappings()})
		var got []string
		for _, metric := range mappingVolume(t, p, url.Values{"query": {`{}`}, "aggregateBy": {"labels"}}) {
			for name := range metric {
				got = append(got, name)
			}
		}
		sort.Strings(got)
		if !slices.Equal(got, want) {
			t.Errorf("%s: volume label names = %v, want %v", mode, got, want)
		}
	}
}

// An alias that is also a stored field of its own: listings hold the name once
// and queries for it read the mapped field, in every mode that uses the alias.
//
// conformance: field-mapping-label-names, semantics/field-mapping-label-modes
func TestFieldMappingLabelMode_AliasCollidesWithStoredField(t *testing.T) {
	for _, mode := range []FieldMappingLabelMode{FieldMappingLabelModeUnset, FieldMappingLabelModeAlias, FieldMappingLabelModeBoth} {
		vl := &mappingVL{fields: map[string]string{"k8s.namespace.name": "prod", "namespace": "legacy"}}
		p := newMappingProxy(t, vl.server(t).URL, nil, Config{LabelStyle: LabelStyleUnderscores, FieldMappingLabelMode: mode,
			FieldMappings: []FieldMapping{{VLField: "k8s.namespace.name", LokiLabel: "namespace"}}})
		want := []string{"namespace", "service_name"}
		if mode == FieldMappingLabelModeBoth {
			want = []string{"k8s_namespace_name", "namespace", "service_name"}
		}
		if got := mappingStrings(t, p, "/loki/api/v1/labels"); !slices.Equal(got, want) {
			t.Errorf("%s: labels = %v, want %v", mode, got, want)
		}
		if got := mappingStrings(t, p, "/loki/api/v1/label/namespace/values"); !slices.Equal(got, []string{"prod"}) {
			t.Errorf("%s: values = %v, want the mapped field's", mode, got)
		}
		if got := p.labelTranslator.ToVL("namespace"); got != "k8s.namespace.name" {
			t.Errorf("%s: query field = %q", mode, got)
		}
	}
}

// A name two mappings claim belongs to the later one, and a base name another
// mapped field owns is not taken from it.
//
// conformance: field-mapping-label-names, semantics/field-mapping-label-modes
func TestFieldMappingLabelMode_ClaimedNames(t *testing.T) {
	lt := NewLabelTranslator(LabelStyleUnderscores, []FieldMapping{
		{VLField: "a.first", LokiLabel: "app"},
		{VLField: "b.second", LokiLabel: "app"},
		{VLField: "c.third", LokiLabel: "d_fourth"},
		{VLField: "d.fourth", LokiLabel: "four"},
		{VLField: "e_x", LokiLabel: "ex"},
		{VLField: "e.x", LokiLabel: "dotted_ex"},
	})
	lt.SetFieldMappingLabelMode(FieldMappingLabelModeBoth)
	for field, want := range map[string][]string{
		"a.first":  {"a_first"},
		"b.second": {"app", "b_second"},
		"c.third":  {"d_fourth", "c_third"},
		"d.fourth": {"four"},
		"e_x":      {"ex", "e_x"},
		"e.x":      {"dotted_ex"},
	} {
		var one [1]string
		if got := lt.listedNames(field, &one); !slices.Equal(got, want) {
			t.Errorf("%s listed as %v, want %v", field, got, want)
		}
	}
	for name, field := range map[string]string{"app": "b.second", "d_fourth": "c.third", "e_x": "e_x", "a_first": "a.first"} {
		if got := lt.ToVL(name); got != field {
			t.Errorf("%s reads %q, want %q", name, got, field)
		}
	}
}

// The default mode keeps the cache identities of a proxy without the setting,
// and each listing mode has its own: replicas sharing a cache tier never serve
// another mode's answer.
//
// conformance: field-mapping-label-names, semantics/field-mapping-label-modes
func TestFieldMappingLabelMode_SharedCacheTier(t *testing.T) {
	vl := mappingVLData()
	srv := vl.server(t)
	shared := cache.New(time.Minute, 1000)
	t.Cleanup(shared.Close)
	cfg := func(mode FieldMappingLabelMode) Config {
		return Config{LabelStyle: LabelStyleUnderscores, FieldMappingLabelMode: mode, FieldMappings: mappingTestMappings(),
			ExtraLabelFields: []string{"cluster"}}
	}
	modes := []FieldMappingLabelMode{FieldMappingLabelModeUnset, FieldMappingLabelModeOriginal, FieldMappingLabelModeAlias, FieldMappingLabelModeBoth}
	namespaces := map[string]FieldMappingLabelMode{}
	for _, mode := range modes {
		p := newMappingProxy(t, srv.URL, shared, cfg(mode))
		namespace := p.scopeFingerprint(context.Background(), "")
		if prior, seen := namespaces[namespace]; seen {
			t.Errorf("modes %q and %q share a cache namespace", prior, mode)
		} else {
			namespaces[namespace] = mode
		}
	}
	// The keys of the label-values index (and its persisted and peer
	// snapshot) and of the label cache warm-up follow the namespace.
	warm := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels?query=%2A", nil)
	indexKeys, warmKeys := map[string]bool{}, map[string]bool{}
	for _, mode := range modes {
		p := newMappingProxy(t, srv.URL, shared, cfg(mode))
		indexKeys[p.scopedIndexOrg(nil, "")] = true
		warmKeys[p.canonicalReadCacheKey("labels", "", warm)] = true
	}
	if len(indexKeys) != 4 || len(warmKeys) != 4 {
		t.Errorf("%d index keys and %d warm-up keys, want 4 of each", len(indexKeys), len(warmKeys))
	}
	// Each proxy's answers through the shared cache equal the answers of a
	// proxy of its mode with a cache of its own, in either order.
	answers := func(p *Proxy) []string {
		out := []string{
			strings.Join(mappingStrings(t, p, "/loki/api/v1/labels"), ","),
			strings.Join(mappingDetectedLabels(t, p), ","),
			strings.Join(mappingAnswerLabels(t, p, "/loki/api/v1/series"), ","),
			strings.Join(mappingAnswerLabels(t, p, "/loki/api/v1/query_range"), ","),
			strings.Join(mappingStrings(t, p, "/loki/api/v1/label/custom_team/values"), ","),
		}
		metrics, _ := json.Marshal(mappingVolume(t, p, url.Values{"query": {`{}`}, "aggregateBy": {"labels"}}))
		return append(out, string(metrics))
	}
	want := map[FieldMappingLabelMode][]string{}
	for _, mode := range modes {
		want[mode] = answers(newMappingProxy(t, srv.URL, nil, cfg(mode)))
	}
	for _, order := range [][]FieldMappingLabelMode{modes, {FieldMappingLabelModeBoth, FieldMappingLabelModeAlias, FieldMappingLabelModeOriginal, FieldMappingLabelModeUnset}} {
		shared.InvalidatePrefix("")
		for _, mode := range order {
			if got := answers(newMappingProxy(t, srv.URL, shared, cfg(mode))); !slices.Equal(got, want[mode]) {
				t.Errorf("mode %q behind the shared cache:\n%q\nwant\n%q", mode, got, want[mode])
			}
		}
	}
}

// A reload of the mappings keeps the mode.
//
// conformance: field-mapping-label-names, semantics/field-mapping-label-modes
func TestFieldMappingLabelMode_Reload(t *testing.T) {
	vl := mappingVLData()
	p := newMappingProxy(t, vl.server(t).URL, nil, Config{LabelStyle: LabelStyleUnderscores, FieldMappingLabelMode: FieldMappingLabelModeBoth,
		FieldMappings: []FieldMapping{{VLField: "k8s.pod.name", LokiLabel: "pod"}}, ExtraLabelFields: []string{"cluster"}})
	before := p.scopeFingerprint(context.Background(), "")
	p.ReloadFieldMappings([]FieldMapping{{VLField: "k8s.pod.name", LokiLabel: "pod_alias"}})
	want := []string{"cluster", "custom_team", "host_name", "k8s_namespace_name", "k8s_pod_name", "pod_alias", "service_name"}
	if got := mappingStrings(t, p, "/loki/api/v1/labels"); !slices.Equal(got, want) {
		t.Errorf("labels after reload = %v, want %v", got, want)
	}
	if p.scopeFingerprint(context.Background(), "") == before {
		t.Error("a reload must change the cache namespace")
	}
}

// conformance: field-mapping-label-names, semantics/field-mapping-label-modes
func TestFieldMappingLabelMode_Invalid(t *testing.T) {
	if _, err := New(Config{BackendURL: "http://127.0.0.1:1", LogLevel: "error", FieldMappingLabelMode: "aliases"}); err == nil || !strings.Contains(err.Error(), "-field-mapping-label-mode") {
		t.Fatalf("error = %v", err)
	}
}

// Random mappings: the default mode changes nothing; the listing modes keep
// every mapping name's query field, answer under one listed name, and every
// listed alias reads its field.
func FuzzFieldMappingLabelMode(f *testing.F) {
	f.Add("k8s.namespace.name", "namespace", "k8s.namespace.name", "k8s_namespace_name", true)
	f.Add("a.b", "a_b", "a_b", "a_b", true)
	f.Add("a", "b", "b", "a", false)
	f.Add("x.y", "app", "z.w", "app", true)
	f.Add("service.name", "service_name", "k8s_pod_name", "pod", false)
	f.Fuzz(func(t *testing.T, field1, label1, field2, label2 string, underscores bool) {
		style := LabelStylePassthrough
		if underscores {
			style = LabelStyleUnderscores
		}
		mappings := []FieldMapping{{VLField: field1, LokiLabel: label1}, {VLField: field2, LokiLabel: label2}}
		plain := NewLabelTranslator(style, mappings)
		original := NewLabelTranslator(style, mappings)
		original.SetFieldMappingLabelMode(FieldMappingLabelModeUnset)
		for _, mode := range []FieldMappingLabelMode{FieldMappingLabelModeAlias, FieldMappingLabelModeBoth} {
			lt := NewLabelTranslator(style, mappings)
			lt.SetFieldMappingLabelMode(mode)
			for _, m := range mappings {
				if m.VLField == "" || m.LokiLabel == "" {
					continue
				}
				if lt.ToVL(m.LokiLabel) != plain.ToVL(m.LokiLabel) {
					t.Fatalf("%s: mapping name %q reads %q, default %q", mode, m.LokiLabel, lt.ToVL(m.LokiLabel), plain.ToVL(m.LokiLabel))
				}
				if original.ToLoki(m.VLField) != plain.ToLoki(m.VLField) || original.ToVL(m.LokiLabel) != plain.ToVL(m.LokiLabel) {
					t.Fatalf("the default mode changed %+v", m)
				}
				var one [1]string
				listed := lt.listedNames(m.VLField, &one)
				if len(listed) == 0 {
					t.Fatalf("%s: %q lists nothing", mode, m.VLField)
				}
				answer := lt.ToLoki(m.VLField)
				if !slices.Contains(listed, answer) {
					t.Fatalf("%s: %q answers %q, lists %v", mode, m.VLField, answer, listed)
				}
				for _, name := range listed {
					if name != lt.baseName(m.VLField) && lt.ToVL(name) != m.VLField {
						t.Fatalf("%s: listed alias %q of %q reads %q", mode, name, m.VLField, lt.ToVL(name))
					}
				}
				// The alias functions of the lowering: an alias names its field both ways, any other label none.
				for _, label := range []string{m.LokiLabel, lt.baseName(m.VLField), answer} {
					field := lt.aliasField(label)
					if lt.isAliasLabel(label) != (field != "") {
						t.Fatalf("%s: alias %q names field %q", mode, label, field)
					}
					if field != "" && lt.fieldAlias(field) != label {
						t.Fatalf("%s: field %q of alias %q names %q", mode, field, label, lt.fieldAlias(field))
					}
				}
			}
		}
		// The default mode gives the lowering no alias functions: its text is unchanged.
		for _, lt := range []*LabelTranslator{plain, original} {
			if prof := lt.withAliases(lower.Profile{}); prof.AliasField != nil || prof.FieldAlias != nil {
				t.Fatal("the default mode gave the lowering alias functions")
			}
		}
	})
}
