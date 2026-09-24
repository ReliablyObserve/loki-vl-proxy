//go:build e2e

package e2e_compat

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

type matrixCombo struct {
	style   proxy.LabelStyle
	mode    proxy.MetadataFieldMode
	emit    bool
	dotted  string
	browse  string
	indexed bool
}

func (c matrixCombo) String() string {
	return fmt.Sprintf("%s/%s/emit=%v/dotted=%s/browse=%s/indexed=%v", c.style, c.mode, c.emit, c.dotted, c.browse, c.indexed)
}

func (c matrixCombo) lokiProfile() bool {
	return c.style == proxy.LabelStyleUnderscores && c.mode == proxy.MetadataFieldModeTranslated
}

func (c matrixCombo) rejectDotted() bool {
	switch c.dotted {
	case "reject":
		return true
	case "accept":
		return false
	}
	return c.lokiProfile()
}

func (c matrixCombo) browseOn() bool {
	switch c.browse {
	case "on":
		return true
	case "off":
		return false
	}
	return !c.lokiProfile() || c.indexed
}

func (c matrixCombo) metadataKeys(fields ...string) []string {
	if !c.emit {
		return nil
	}
	translate := func(f string) string {
		if c.style == proxy.LabelStyleUnderscores {
			return proxy.SanitizeLabelName(f)
		}
		return f
	}
	set := map[string]bool{}
	for _, f := range fields {
		switch c.mode {
		case proxy.MetadataFieldModeNative:
			set[f] = true
		case proxy.MetadataFieldModeTranslated:
			set[translate(f)] = true
		default:
			set[f], set[translate(f)] = true, true
		}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func inProcessMatrixProxy(t *testing.T, c matrixCombo) string {
	t.Helper()
	p, err := proxy.New(proxy.Config{
		BackendURL:                 vlURL,
		Cache:                      cache.NewDisabled(),
		LogLevel:                   "error",
		LabelStyle:                 c.style,
		MetadataFieldMode:          c.mode,
		EmitStructuredMetadata:     c.emit,
		LogQLDottedNames:           c.dotted,
		LabelBrowseExtensions:      c.browse,
		LabelValuesIndexedCache:    c.indexed,
		LabelValuesHotLimit:        200,
		LabelValuesIndexMaxEntries: 1000,
		DisableLabelsCacheWarm:     true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterProxyRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server.URL
}

var (
	matrixFixtureOnce sync.Once
	matrixFixtureApp  string
)

// ensureMatrixFixture pushes one JSON line with dotted keys to Loki and
// VictoriaLogs, for the detected_fields naming check.
func ensureMatrixFixture(t *testing.T) string {
	t.Helper()
	matrixFixtureOnce.Do(func() {
		app := fmt.Sprintf("compat-matrix-%d", time.Now().UnixNano())
		now := time.Now().Add(-30 * time.Second)
		pushStream(t, now, streamDef{
			Labels: map[string]string{"app": app, "env": "matrix", "level": "info"},
			Lines: []string{
				`{"msg":"login","http.method":"GET","http.status_code":200}`,
				`{"msg":"logout","http.method":"POST","http.status_code":201}`,
			},
		})
		forceVLFlush(t)
		matrixFixtureApp = app
	})
	if matrixFixtureApp == "" {
		t.Fatal("matrix fixture not ingested")
	}
	return matrixFixtureApp
}

type detectedFieldEntry struct {
	Label    string   `json:"label"`
	JSONPath []string `json:"jsonPath"`
	Parsers  []string `json:"parsers"`
}

func matrixDetectedFields(t *testing.T, base, query string) map[string]detectedFieldEntry {
	t.Helper()
	params := url.Values{
		"query": {query},
		"start": {strconv.FormatInt(time.Now().Add(-time.Hour).UnixNano(), 10)},
		"end":   {strconv.FormatInt(time.Now().UnixNano(), 10)},
	}
	status, body := rejectedQueryGet(t, base, "/loki/api/v1/detected_fields", params, "0", nil)
	var resp struct {
		Fields []detectedFieldEntry `json:"fields"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &resp) != nil {
		t.Fatalf("detected_fields %s: %d %s", base, status, body)
	}
	out := map[string]detectedFieldEntry{}
	for _, f := range resp.Fields {
		out[f.Label] = f
	}
	return out
}

func assertMatrixCombo(t *testing.T, base string, c matrixCombo, lokiDottedErr string, app string) {
	t.Helper()
	end := time.Now()
	window := func(v url.Values) url.Values {
		v.Set("start", strconv.FormatInt(end.Add(-time.Hour).UnixNano(), 10))
		v.Set("end", strconv.FormatInt(end.UnixNano(), 10))
		return v
	}

	// Dotted names.
	status, body := rejectedQueryGet(t, base, "/loki/api/v1/query_range",
		window(url.Values{"query": {`{service_name="structured-metadata-e2e"} | k8s.pod.name!=""`}, "limit": {"10"}}), "0", nil)
	if c.rejectDotted() {
		var env struct{ Error string }
		if status != http.StatusBadRequest || json.Unmarshal(body, &env) != nil || env.Error != lokiDottedErr {
			t.Errorf("dotted name: %d %s, want Loki's 400 %q", status, body, lokiDottedErr)
		}
	} else if status != http.StatusOK {
		t.Errorf("dotted name with dotted names accepted: %d %s", status, body)
	}

	// Browse parameters.
	status, body = rejectedQueryGet(t, base, "/loki/api/v1/label/level/values", window(url.Values{"limit": {"1"}}), "0", nil)
	var values struct{ Data []string }
	if status != http.StatusOK || json.Unmarshal(body, &values) != nil {
		t.Fatalf("label values: %d %s", status, body)
	}
	if c.browseOn() && len(values.Data) != 1 {
		t.Errorf("browse on: limit=1 returned %v", values.Data)
	}
	if !c.browseOn() && len(values.Data) < 2 {
		t.Errorf("browse off: limit=1 must be ignored, got %v", values.Data)
	}

	// Structured-metadata keys.
	resp := queryRangeCategorized(t, base, `{service_name="structured-metadata-e2e",level="info"}`)
	var got []string
	if tuple := firstStreamTuple(t, resp); len(tuple) > 2 {
		meta, _ := tuple[2].(map[string]interface{})
		sm, _ := meta["structuredMetadata"].(map[string]interface{})
		for k := range sm {
			if k != "detected_level" {
				got = append(got, k)
			}
		}
	}
	sort.Strings(got)
	if want := c.metadataKeys("cloud.region", "http.target"); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("structured metadata keys %v, want %v", got, want)
	}

	// detected_fields name of a dotted JSON key.
	fields := matrixDetectedFields(t, base, fmt.Sprintf(`{app=%q}`, app))
	want := map[bool]string{true: "http_method", false: "http.method"}[c.rejectDotted()]
	if f, ok := fields[want]; !ok || len(f.JSONPath) != 1 || f.JSONPath[0] != "http.method" {
		t.Errorf("detected_fields: no %q with jsonPath [http.method]: %+v", want, fields)
	}
}

// TestCompat_OptionMatrixAgainstStack runs every combination of the
// compatibility options (label style, metadata mode, structured metadata,
// dotted names, browse extensions, indexed cache) as an in-process proxy
// against the stack's VictoriaLogs, and the stack's own variants, checking
// dotted-name handling (Loki's exact error when rejected), browse
// parameters, structured-metadata keys and detected_fields names. The
// Loki-profile expectations are taken from Loki itself.
//
// conformance: loki-compatible-profile, profiles/dotted-name-parse-error, profiles/dotted-names-accepted-outside-loki-profile, profiles/label-browse-params-ignored, profiles/structured-metadata-keys-per-profile, profiles/detected-fields-dotted-json-keys
func TestCompat_OptionMatrixAgainstStack(t *testing.T) {
	ensureDataIngested(t)
	ensureStructuredMetadataData(t)
	app := ensureMatrixFixture(t)

	// Loki's answers the Loki-profile combinations must reproduce.
	end := time.Now()
	status, body := rejectedQueryGet(t, lokiURL, "/loki/api/v1/query_range", url.Values{
		"query": {`{service_name="structured-metadata-e2e"} | k8s.pod.name!=""`}, "limit": {"10"},
		"start": {strconv.FormatInt(end.Add(-time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)},
	}, "0", nil)
	lokiDottedErr := strings.TrimSpace(string(body))
	if status != http.StatusBadRequest {
		t.Fatalf("Loki fixture drifted: dotted name answered %d %s", status, body)
	}
	lokiFields := matrixDetectedFields(t, lokiURL, fmt.Sprintf(`{app=%q}`, app))
	if f, ok := lokiFields["http_method"]; !ok || len(f.JSONPath) != 1 || f.JSONPath[0] != "http.method" {
		t.Fatalf("Loki detected_fields for a dotted JSON key: %+v (want http_method with jsonPath [http.method])", lokiFields)
	}

	for _, style := range []proxy.LabelStyle{proxy.LabelStyleUnderscores, proxy.LabelStylePassthrough} {
		for _, mode := range []proxy.MetadataFieldMode{proxy.MetadataFieldModeTranslated, proxy.MetadataFieldModeHybrid, proxy.MetadataFieldModeNative} {
			for _, emit := range []bool{true, false} {
				for _, dotted := range []string{"auto", "reject", "accept"} {
					for _, browse := range []string{"auto", "on", "off"} {
						for _, indexed := range []bool{false, true} {
							c := matrixCombo{style, mode, emit, dotted, browse, indexed}
							t.Run("in-process/"+c.String(), func(t *testing.T) {
								assertMatrixCombo(t, inProcessMatrixProxy(t, c), c, lokiDottedErr, app)
							})
						}
					}
				}
			}
		}
	}

	u, tr, hy, na := proxy.LabelStyleUnderscores, proxy.MetadataFieldModeTranslated, proxy.MetadataFieldModeHybrid, proxy.MetadataFieldModeNative
	for _, v := range []struct {
		name string
		url  string
		c    matrixCombo
	}{
		{"parity (13100, indexed cache)", proxyURL, matrixCombo{u, tr, true, "auto", "auto", true}},
		{"explore (13102)", proxyUnderscoreURL, matrixCombo{u, tr, true, "auto", "auto", false}},
		{"drilldown default (13110)", patternsAutodetectProxyURL, matrixCombo{u, tr, true, "auto", "auto", false}},
		{"translated-metadata (13107)", proxyTranslatedMetadataURL, matrixCombo{u, tr, true, "auto", "auto", false}},
		{"otel-hybrid (13111)", proxyOTelHybridURL, matrixCombo{u, hy, true, "auto", "auto", false}},
		{"native-metadata (13106)", proxyNativeMetadataURL, matrixCombo{u, na, true, "auto", "auto", false}},
		{"no-metadata (13108)", proxyNoStructuredMetadataURL, matrixCombo{u, tr, false, "auto", "auto", false}},
	} {
		t.Run("stack/"+v.name, func(t *testing.T) {
			assertMatrixCombo(t, v.url, v.c, lokiDottedErr, app)
		})
	}
}
