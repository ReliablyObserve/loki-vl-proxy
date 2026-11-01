//go:build e2e

package e2e_compat

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// conformance: profiles/keep-structured-metadata-key, loki_api_v1_query_range
// The same OTel-style attribute is stored under its Loki label name on one
// side and its raw dotted VictoriaLogs field name on the other.
func TestCompat_KeepStructuredMetadataKey(t *testing.T) {
	now := time.Now().UTC()
	f := dlFixture{
		app:   fmt.Sprintf("keepmetadata%d", now.UnixNano()),
		start: now.Add(-2 * time.Minute),
	}
	f.end = f.start.Add(3 * time.Second)
	stamp := strconv.FormatInt(f.start.Add(time.Second).UnixNano(), 10)
	secondStamp := strconv.FormatInt(f.start.Add(2*time.Second).UnixNano(), 10)
	labels := map[string]string{"app": f.app}
	line := "metadata keep fixture"
	dlPost(t, lokiURL+"/loki/api/v1/push", "application/json", dlJSON(map[string]interface{}{
		"streams": []interface{}{map[string]interface{}{
			"stream": labels,
			"values": []interface{}{
				[]interface{}{stamp, line, map[string]string{"deployment_environment": "production"}},
				[]interface{}{secondStamp, line, map[string]string{"deployment_environment": "staging"}},
			},
		}},
	}))
	vlRows := []string{string(dlJSON(map[string]string{
		"_time":                  f.start.Add(time.Second).Format(time.RFC3339Nano),
		"_msg":                   line,
		"app":                    f.app,
		"deployment.environment": "production",
	})), string(dlJSON(map[string]string{
		"_time":                  f.start.Add(2 * time.Second).Format(time.RFC3339Nano),
		"_msg":                   line,
		"app":                    f.app,
		"deployment_environment": "staging",
	}))}
	dlPost(t, vlURL+"/insert/jsonline?_stream_fields=app", "application/stream+json", []byte(strings.Join(vlRows, "\n")))
	dlWaitBothBackends(t, f, f.appSelector(), f.appSelector(), 2)

	route, _ := newJSONVolumeRouteProxyWithConfig(t, func(cfg *proxy.Config) {
		cfg.MetadataFieldMode = proxy.MetadataFieldModeTranslated
	})
	for _, categorized := range []bool{false, true} {
		var headers map[string]string
		if categorized {
			headers = map[string]string{"X-Loki-Response-Encoding-Flags": "categorize-labels"}
		}
		for _, stage := range []string{" | keep deployment_environment", " | keep deployment_environment, app"} {
			query := f.appSelector() + stage
			t.Run(fmt.Sprintf("categorized=%t/%s", categorized, stage), func(t *testing.T) {
				loki := dlEntries(t, dlQueryRange(t, lokiURL, query, f, headers))
				proxy := dlEntries(t, dlQueryRange(t, route, query, f, headers))
				if !reflect.DeepEqual(proxy, loki) {
					t.Fatalf("%s: Loki=%+v proxy=%+v", query, loki, proxy)
				}
			})
		}
	}
}
