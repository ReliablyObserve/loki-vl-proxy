package proxy

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
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// compatOptions is one combination of the compatibility settings.
type compatOptions struct {
	style        LabelStyle
	mode         MetadataFieldMode
	emit         bool
	dotted       string // -logql-dotted-names
	browse       string // -label-browse-extensions
	indexed      bool   // -label-values-indexed-cache
	noMessageFld bool   // -error-response-message-field=false
}

func (o compatOptions) String() string {
	return fmt.Sprintf("style=%s/mode=%s/emit=%v/dotted=%s/browse=%s/indexed=%v", o.style, o.mode, o.emit, o.dotted, o.browse, o.indexed)
}

func (o compatOptions) config(backendURL string) Config {
	return Config{
		BackendURL:                 backendURL,
		Cache:                      cache.New(60*time.Second, 1000),
		LogLevel:                   "error",
		LabelStyle:                 o.style,
		MetadataFieldMode:          o.mode,
		EmitStructuredMetadata:     o.emit,
		LogQLDottedNames:           o.dotted,
		LabelBrowseExtensions:      o.browse,
		LabelValuesIndexedCache:    o.indexed,
		LabelValuesHotLimit:        2,
		LabelValuesIndexMaxEntries: 1000,
		DisableErrorMessageField:   o.noMessageFld,
	}
}

func newCompatProxy(t *testing.T, backendURL string, o compatOptions) *Proxy {
	t.Helper()
	p, err := New(o.config(backendURL))
	if err != nil {
		t.Fatalf("New(%s): %v", o, err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func newProfileProxy(t *testing.T, backendURL string, style LabelStyle, mode MetadataFieldMode, indexed bool) *Proxy {
	t.Helper()
	return newCompatProxy(t, backendURL, compatOptions{style: style, mode: mode, emit: true, indexed: indexed})
}

// Expected behaviour of a combination, derived independently of the proxy.
func (o compatOptions) wantRejectDotted() bool {
	switch o.dotted {
	case CompatReject:
		return true
	case CompatAccept:
		return false
	}
	return o.style == LabelStyleUnderscores && o.mode == MetadataFieldModeTranslated
}

func (o compatOptions) wantBrowse() bool {
	switch o.browse {
	case CompatOn:
		return true
	case CompatOff:
		return false
	}
	return !(o.style == LabelStyleUnderscores && o.mode == MetadataFieldModeTranslated) || o.indexed
}

// wantMetadataKeys is the structured-metadata key set for stored fields.
func (o compatOptions) wantMetadataKeys(fields ...string) []string {
	if !o.emit {
		return nil
	}
	translate := func(f string) string {
		if o.style == LabelStyleUnderscores {
			return SanitizeLabelName(f)
		}
		return f
	}
	set := map[string]bool{}
	for _, f := range fields {
		switch o.mode {
		case MetadataFieldModeNative:
			set[f] = true
		case MetadataFieldModeTranslated:
			set[translate(f)] = true
		default:
			set[f] = true
			set[translate(f)] = true
		}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func allCompatOptions() []compatOptions {
	var out []compatOptions
	for _, style := range []LabelStyle{LabelStyleUnderscores, LabelStylePassthrough} {
		for _, mode := range []MetadataFieldMode{MetadataFieldModeTranslated, MetadataFieldModeHybrid, MetadataFieldModeNative} {
			for _, emit := range []bool{true, false} {
				for _, dotted := range []string{CompatAuto, CompatReject, CompatAccept} {
					for _, browse := range []string{CompatAuto, CompatOn, CompatOff} {
						for _, indexed := range []bool{false, true} {
							out = append(out, compatOptions{style: style, mode: mode, emit: emit, dotted: dotted, browse: browse, indexed: indexed})
						}
					}
				}
			}
		}
	}
	return out
}

// matrixBackend is a fake VictoriaLogs serving one OTel row with dotted
// stream labels, dotted stored metadata fields (http.target, cloud.region)
// and a JSON line with a dotted key (http.method), plus label values.
func matrixBackend(t *testing.T, calls *atomic.Int64) *httptest.Server {
	t.Helper()
	row := `{"_time":"2026-04-04T17:18:49.971082Z","_msg":"{\"msg\":\"login\",\"http.method\":\"GET\"}",` +
		`"_stream":"{service.name=\"svc\",k8s.pod.name=\"pod-1\",level=\"info\"}","service.name":"svc","k8s.pod.name":"pod-1","level":"info",` +
		`"http.method":"GET","http.target":"/api/login","cloud.region":"eu-west-1"}`
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = w.Write([]byte(row + "\n"))
		case "/select/logsql/field_names":
			http.Error(w, "unsupported", http.StatusNotFound)
		case "/select/logsql/stream_field_names":
			_, _ = w.Write([]byte(`{"values":[{"value":"app","hits":1},{"value":"pod","hits":1}]}`))
		case "/select/logsql/stream_field_values", "/select/logsql/field_values":
			values := []string{"alpha", "beta", "delta", "gamma"}
			if n, err := strconv.Atoi(r.FormValue("limit")); err == nil && n > 0 && n < len(values) {
				values = values[:n]
			}
			parts := make([]string, 0, len(values))
			for _, v := range values {
				parts = append(parts, `{"value":"`+v+`","hits":1}`)
			}
			_, _ = w.Write([]byte(`{"values":[` + strings.Join(parts, ",") + `]}`))
		case "/select/logsql/hits":
			_, _ = w.Write([]byte(`{"hits":[]}`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
}

func serve(p *Proxy, method, target string, headers map[string]string) *httptest.ResponseRecorder {
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	req := httptest.NewRequest(method, target, nil)
	req.Header.Set("X-Scope-OrgID", "0")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

// TestCompatOptionMatrix walks every combination of -label-style,
// -metadata-field-mode, -emit-structured-metadata, -logql-dotted-names,
// -label-browse-extensions and -label-values-indexed-cache and checks, per
// combination: dotted-name handling (Loki's parse error or translation),
// label browse parameters, structured-metadata keys and detected_fields names
// of dotted JSON keys. No combination may silently change behaviour.
//
// conformance: loki-compatible-profile, profiles/dotted-name-parse-error, profiles/dotted-names-accepted-outside-loki-profile, profiles/label-browse-params-ignored, profiles/structured-metadata-keys-per-profile, profiles/detected-fields-dotted-json-keys
func TestCompatOptionMatrix(t *testing.T) {
	var calls atomic.Int64
	backend := matrixBackend(t, &calls)
	defer backend.Close()
	dotted := `{service_name="svc"} | k8s.pod.name="pod-1"`
	dottedErr := "parse error at line 1, col 27: syntax error: unexpected ."
	window := "start=1775322000000000000&end=1775325600000000000"

	for _, o := range allCompatOptions() {
		t.Run(o.String(), func(t *testing.T) {
			p := newCompatProxy(t, backend.URL, o)

			// Dotted names.
			before := calls.Load()
			w := serve(p, http.MethodGet, "/loki/api/v1/query_range?"+window+"&limit=10&query="+url.QueryEscape(dotted), nil)
			if o.wantRejectDotted() {
				var got struct{ Error, Message string }
				if w.Code != http.StatusBadRequest || json.Unmarshal(w.Body.Bytes(), &got) != nil || got.Error != dottedErr {
					t.Fatalf("dotted name: status %d body %.200s, want 400 %q", w.Code, w.Body, dottedErr)
				}
				if calls.Load() != before {
					t.Fatal("a rejected dotted name reached the backend")
				}
			} else if w.Code == http.StatusBadRequest {
				t.Fatalf("dotted name rejected with dotted names accepted: %.200s", w.Body)
			}

			// Label browse parameters.
			var values struct{ Data []string }
			w = serve(p, http.MethodGet, "/loki/api/v1/label/app/values?"+window+"&limit=2", nil)
			if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &values) != nil {
				t.Fatalf("label values: %d %.200s", w.Code, w.Body)
			}
			if want := map[bool]int{true: 2, false: 4}[o.wantBrowse()]; len(values.Data) != want {
				t.Fatalf("label values with limit=2 returned %v, want %d values (browse=%v)", values.Data, want, o.wantBrowse())
			}

			// Structured metadata keys.
			w = serve(p, http.MethodGet, "/loki/api/v1/query_range?"+window+"&limit=10&query="+url.QueryEscape(`{service_name="svc"}`),
				map[string]string{"X-Loki-Response-Encoding-Flags": "categorize-labels"})
			if w.Code != http.StatusOK {
				t.Fatalf("query_range: %d %.200s", w.Code, w.Body)
			}
			var qr struct {
				Data struct {
					Result []struct {
						Values [][]json.RawMessage `json:"values"`
					} `json:"result"`
				} `json:"data"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &qr); err != nil || len(qr.Data.Result) == 0 || len(qr.Data.Result[0].Values) == 0 {
				t.Fatalf("query_range body: %v %.300s", err, w.Body)
			}
			var gotKeys []string
			if v := qr.Data.Result[0].Values[0]; len(v) > 2 {
				var meta struct {
					StructuredMetadata map[string]string `json:"structuredMetadata"`
				}
				_ = json.Unmarshal(v[2], &meta)
				for k := range meta.StructuredMetadata {
					if k != detectedLevelLabel {
						gotKeys = append(gotKeys, k)
					}
				}
			}
			sort.Strings(gotKeys)
			if want := o.wantMetadataKeys("cloud.region", "http.target"); fmt.Sprint(gotKeys) != fmt.Sprint(want) {
				t.Fatalf("structured metadata keys %v, want %v", gotKeys, want)
			}

			// detected_fields name of a dotted JSON key.
			w = serve(p, http.MethodGet, "/loki/api/v1/detected_fields?"+window+"&query="+url.QueryEscape(`{service_name="svc"}`), nil)
			var df struct {
				Fields []struct {
					Label    string   `json:"label"`
					JSONPath []string `json:"jsonPath"`
					Parsers  []string `json:"parsers"`
				} `json:"fields"`
			}
			if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &df) != nil {
				t.Fatalf("detected_fields: %d %.200s", w.Code, w.Body)
			}
			wantLabel := map[bool]string{true: "http_method", false: "http.method"}[o.wantRejectDotted()]
			found := false
			for _, f := range df.Fields {
				if f.Label == wantLabel && len(f.JSONPath) == 1 && f.JSONPath[0] == "http.method" {
					found = true
				}
				if o.wantRejectDotted() && strings.Contains(f.Label, ".") && len(f.Parsers) > 0 {
					t.Fatalf("detected_fields offers parsed field %q that dotted-name rejection refuses", f.Label)
				}
			}
			if !found {
				t.Fatalf("detected_fields has no %q with jsonPath [http.method]: %.500s", wantLabel, w.Body)
			}
		})
	}
}

func TestCompatOptions_InvalidValuesRejected(t *testing.T) {
	for _, o := range []compatOptions{
		{style: LabelStyleUnderscores, mode: MetadataFieldModeTranslated, dotted: "maybe"},
		{style: LabelStyleUnderscores, mode: MetadataFieldModeTranslated, browse: "yes"},
	} {
		if _, err := New(o.config("http://127.0.0.1:1")); err == nil {
			t.Fatalf("%s: invalid setting accepted", o)
		}
	}
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

// -error-response-message-field=false restores the previous error body.
//
// conformance: profiles/grafana-shows-loki-error-text
func TestErrorResponseMessageField_Optional(t *testing.T) {
	for _, off := range []bool{false, true} {
		p := newCompatProxy(t, "http://127.0.0.1:1", compatOptions{style: LabelStyleUnderscores, mode: MetadataFieldModeTranslated, emit: true, noMessageFld: off})
		w := serve(p, http.MethodGet, "/loki/api/v1/query_range?query="+url.QueryEscape(`{a.b="x"}`), nil)
		var body map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || w.Code != http.StatusBadRequest {
			t.Fatalf("off=%v: %d %s", off, w.Code, w.Body)
		}
		if _, has := body["message"]; has == off {
			t.Fatalf("off=%v: message field present=%v: %s", off, has, w.Body)
		}
	}
}
