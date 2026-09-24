package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func newProfileProxy(t *testing.T, backendURL string, style LabelStyle, mode MetadataFieldMode, indexed bool) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL:                 backendURL,
		Cache:                      cache.New(60*time.Second, 1000),
		LogLevel:                   "error",
		LabelStyle:                 style,
		MetadataFieldMode:          mode,
		LabelValuesIndexedCache:    indexed,
		LabelValuesHotLimit:        2,
		LabelValuesIndexMaxEntries: 1000,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

// The Loki-compatible profile answers a dotted LogQL name with Loki's parse
// error on every endpoint that parses LogQL, before any backend call. The
// messages are Loki v3.7.7's (see internal/logql lokiDottedNameErrors).
//
// conformance: profiles/dotted-name-parse-error, profiles/grafana-shows-loki-error-text
func TestLokiProfile_DottedNamesReturnLokiParseError(t *testing.T) {
	var calls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "unexpected backend call", http.StatusTeapot)
	}))
	defer backend.Close()
	p := newProfileProxy(t, backend.URL, LabelStyleUnderscores, MetadataFieldModeTranslated, false)
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	owner := "{env=\"production\"} | json | pipeline=`metrics/prometheus` | k8s.namespace.name=`monitoring`"
	cases := []struct{ path, param, value, want string }{
		{"/loki/api/v1/query_range", "query", owner, "parse error at line 1, col 64: syntax error: unexpected ."},
		{"/loki/api/v1/query_range", "query", `sum by (k8s.namespace.name) (count_over_time({env="production"}[5m]))`, "parse error at line 1, col 12: syntax error: unexpected ., expecting , or )"},
		{"/loki/api/v1/query", "query", `sum without (k8s.namespace.name) (count_over_time({env="production"}[5m]))`, "parse error at line 1, col 17: syntax error: unexpected ., expecting , or )"},
		{"/loki/api/v1/query_range", "query", `{env="production"} | keep k8s.namespace.name`, "parse error at line 1, col 30: syntax error: unexpected ."},
		{"/loki/api/v1/query_range", "query", `{k8s.namespace.name="x"}`, "parse error at line 1, col 5: syntax error: unexpected ., expecting = or =~ or !~ or !="},
		{"/loki/api/v1/labels", "query", `{k8s.namespace.name="monitoring"}`, "parse error at line 1, col 5: syntax error: unexpected ., expecting = or =~ or !~ or !="},
		{"/loki/api/v1/label/app/values", "query", `{k8s.namespace.name="monitoring"}`, "parse error at line 1, col 5: syntax error: unexpected ., expecting = or =~ or !~ or !="},
		{"/loki/api/v1/series", "match[]", `{k8s.namespace.name="monitoring"}`, "parse error at line 1, col 5: syntax error: unexpected ., expecting = or =~ or !~ or !="},
		{"/loki/api/v1/index/volume", "query", `{k8s.namespace.name="monitoring"}`, "parse error at line 1, col 5: syntax error: unexpected ., expecting = or =~ or !~ or !="},
		{"/loki/api/v1/detected_fields", "query", `{env="production"} | k8s.namespace.name="monitoring"`, "parse error at line 1, col 25: syntax error: unexpected ."},
	}
	for _, tc := range cases {
		t.Run(tc.path+"/"+tc.value, func(t *testing.T) {
			body := url.Values{tc.param: {tc.value}, "start": {"1700000000"}, "end": {"1700003600"}}.Encode()
			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set("X-Scope-OrgID", "0")
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			// message is what Grafana's Loki datasource displays; it must be
			// Loki's text, as error is for API clients.
			var got struct{ ErrorType, Error, Message string }
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || w.Code != http.StatusBadRequest || got.ErrorType != "bad_data" || got.Error != tc.want || got.Message != tc.want {
				t.Fatalf("status=%d body=%.300s, want 400 %q", w.Code, w.Body, tc.want)
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("rejected queries reached the backend %d times", calls.Load())
	}
}

// Hybrid and native metadata modes expose dotted VictoriaLogs field names,
// so they keep accepting them in queries (a documented, profile-scoped
// extension), as does passthrough label style.
//
// conformance: profiles/dotted-names-accepted-outside-loki-profile
func TestLokiProfile_OtherProfilesAcceptDottedNames(t *testing.T) {
	for _, profile := range []struct {
		style LabelStyle
		mode  MetadataFieldMode
	}{
		{LabelStyleUnderscores, MetadataFieldModeHybrid},
		{LabelStyleUnderscores, MetadataFieldModeNative},
		{LabelStylePassthrough, MetadataFieldModeTranslated},
	} {
		p := newProfileProxy(t, "http://127.0.0.1:1", profile.style, profile.mode, false)
		if p.lokiNames {
			t.Fatalf("%s/%s: treated as the Loki-compatible profile", profile.style, profile.mode)
		}
		if msg := p.lokiNameError(`{env="production"} | k8s.namespace.name="monitoring"`); msg != "" {
			t.Fatalf("%s/%s: dotted name rejected: %s", profile.style, profile.mode, msg)
		}
		if p.lokiNameCheck() != nil {
			t.Fatalf("%s/%s: selector name check enabled", profile.style, profile.mode)
		}
	}
	if !newProfileProxy(t, "http://127.0.0.1:1", LabelStyleUnderscores, MetadataFieldModeTranslated, false).lokiNames {
		t.Fatal("underscores + translated is not the Loki-compatible profile")
	}
}

func labelValuesBackend(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/select/logsql/field_names":
			http.Error(w, "unsupported", http.StatusNotFound)
		case "/select/logsql/stream_field_names":
			_, _ = w.Write([]byte(`{"values":[{"value":"app","hits":1},{"value":"pod","hits":1}]}`))
		case "/select/logsql/stream_field_values", "/select/logsql/field_values":
			// Honour VictoriaLogs' own limit, so a client limit forwarded
			// upstream shows in the answer.
			values := []string{"alpha", "beta", "delta", "gamma"}
			if n, err := strconv.Atoi(r.FormValue("limit")); err == nil && n > 0 && n < len(values) {
				values = values[:n]
			}
			parts := make([]string, 0, len(values))
			for _, v := range values {
				parts = append(parts, `{"value":"`+v+`","hits":1}`)
			}
			_, _ = w.Write([]byte(`{"values":[` + strings.Join(parts, ",") + `]}`))
		default:
			http.Error(w, "unexpected path "+r.URL.Path, http.StatusTeapot)
		}
	}))
}

func getStrings(t *testing.T, p *Proxy, target string) []string {
	t.Helper()
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.Header.Set("X-Scope-OrgID", "0")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	var resp struct {
		Data []string `json:"data"`
	}
	if w.Code != http.StatusOK {
		t.Fatalf("GET %s: status %d body %s", target, w.Code, w.Body)
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("GET %s: decode %v body %s", target, err, w.Body)
	}
	return resp.Data
}

// Loki reads only start, end and query on /labels and /label/{name}/values
// (loghttp.ParseLabelQuery), so limit, offset and search change nothing. The
// Loki-compatible profile ignores them too; the proxy's browse window stays
// available in the other profiles and wherever the operator enabled the
// indexed browse cache.
//
// conformance: profiles/label-browse-params-ignored
func TestLokiProfile_LabelBrowseParamsIgnored(t *testing.T) {
	backend := labelValuesBackend(t)
	defer backend.Close()

	loki := newProfileProxy(t, backend.URL, LabelStyleUnderscores, MetadataFieldModeTranslated, false)
	if got := getStrings(t, loki, "/loki/api/v1/label/app/values?limit=2&offset=1&search=ta"); len(got) != 4 {
		t.Fatalf("Loki profile: label values with browse params = %v, want all 4 values", got)
	}
	if got := getStrings(t, loki, "/loki/api/v1/labels?search=zz"); len(got) < 2 {
		t.Fatalf("Loki profile: labels with search = %v, want every label", got)
	}

	hybrid := newProfileProxy(t, backend.URL, LabelStyleUnderscores, MetadataFieldModeHybrid, false)
	if got := getStrings(t, hybrid, "/loki/api/v1/label/app/values?limit=2"); len(got) != 2 {
		t.Fatalf("hybrid profile: label values limit=2 = %v, want the 2-value browse window", got)
	}

	indexed := newProfileProxy(t, backend.URL, LabelStyleUnderscores, MetadataFieldModeTranslated, true)
	if got := getStrings(t, indexed, "/loki/api/v1/label/app/values?limit=2"); len(got) != 2 {
		t.Fatalf("Loki profile with -label-values-indexed-cache: limit=2 = %v, want the 2-value browse window", got)
	}
}
