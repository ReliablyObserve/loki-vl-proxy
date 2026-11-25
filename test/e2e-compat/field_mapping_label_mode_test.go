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
		proxy.FieldMappingLabelModeOriginal: {[]string{"k8s_namespace_name", "service_name"}, "k8s_namespace_name"},
		proxy.FieldMappingLabelModeAlias:    {[]string{"namespace", "service_name"}, "namespace"},
		proxy.FieldMappingLabelModeBoth:     {[]string{"k8s_namespace_name", "namespace", "service_name"}, "namespace"},
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
				if mode == proxy.FieldMappingLabelModeOriginal && name != want.answer {
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
