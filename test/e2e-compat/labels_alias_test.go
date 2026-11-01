//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// conformance: loki_api_v1_label, loki_api_v1_labels
func TestCompat_LabelsSingularAlias(t *testing.T) {
	now := time.Now()
	params := url.Values{"start": {strconv.FormatInt(now.Add(-time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(now.UnixNano(), 10)}, "query": {`{app="e2e-test"}`}}
	client := &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	var lokiReference map[string]any
	for _, backend := range []struct{ name, base string }{{"Loki", lokiURL}, {"proxy", proxyURL}} {
		name, base := backend.name, backend.base
		t.Run(name, func(t *testing.T) {
			var reference map[string]any
			for _, path := range []string{"/loki/api/v1/labels", "/loki/api/v1/label"} {
				for _, method := range []string{http.MethodGet, http.MethodPost} {
					req, err := http.NewRequest(method, base+path+"?"+params.Encode(), nil)
					if method == http.MethodPost {
						req, err = http.NewRequest(method, base+path, strings.NewReader(params.Encode()))
						if err == nil {
							req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
						}
					}
					if err != nil {
						t.Fatal(err)
					}
					resp, err := client.Do(req)
					if err != nil {
						t.Fatal(err)
					}
					body, err := io.ReadAll(resp.Body)
					resp.Body.Close()
					if err != nil {
						t.Fatal(err)
					}
					if resp.StatusCode != http.StatusOK {
						t.Fatalf("%s %s: %d %s", method, path, resp.StatusCode, body)
					}
					var result map[string]any
					if err := json.Unmarshal(body, &result); err != nil {
						t.Fatalf("%s %s: %v", method, path, err)
					}
					if result["status"] != "success" {
						t.Fatalf("%s %s: %s", method, path, body)
					}
					if labels := extractStrings(result, "data"); !contains(labels, "app") {
						t.Fatalf("%s %s did not include the ingested app label: %s", method, path, body)
					}
					if reference == nil {
						reference = result
					} else if !reflect.DeepEqual(reference, result) {
						t.Fatalf("%s %s differs from canonical labels: got=%s reference=%v", method, path, body, reference)
					}
					if lokiReference == nil {
						lokiReference = result
					} else if !reflect.DeepEqual(lokiReference, result) {
						t.Fatalf("%s %s differs from Loki: got=%s Loki=%v", method, path, body, lokiReference)
					}
				}
			}
			// A second form selector must not reuse the nonempty response above.
			empty := url.Values{"start": {params.Get("start")}, "end": {params.Get("end")}, "query": {`{app="read-api-proof-missing-fixture"}`}}
			for _, path := range []string{"/loki/api/v1/labels", "/loki/api/v1/label"} {
				req, err := http.NewRequest(http.MethodPost, base+path, strings.NewReader(empty.Encode()))
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				resp, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				var result map[string]any
				if err := json.Unmarshal(body, &result); err != nil {
					t.Fatal(err)
				}
				// Loki omits data for an empty labels response; the proxy uses [].
				if resp.StatusCode != http.StatusOK || result["status"] != "success" || len(extractStrings(result, "data")) != 0 {
					t.Fatalf("POST %s reused a different selector's labels: status=%d body=%s", path, resp.StatusCode, body)
				}
			}
		})
	}
}
