package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// The reads a Drilldown detected route makes after its first one answer a failure like every other route: a read
// that decides the answer fails the request with Loki's error, and an extra read next to a required one keeps
// the required one's answer. Every cell fails the named calls in each backend failure mode
// and answers every other call with a valid body.

func followupRoute(path, query string) errLayerRoute {
	now := time.Now().Truncate(time.Minute)
	return errLayerRoute{
		name: path,
		path: path,
		params: url.Values{
			"query": {query},
			"start": {fmt.Sprint(now.Add(-6 * time.Hour).Unix())},
			"end":   {fmt.Sprint(now.Unix())},
		},
	}
}

// followupModes are the failure modes a mixed cell can have: a refused connection fails every call.
func followupModes() []string {
	var modes []string
	for _, m := range errLayerModes {
		if m != "conn_refused" {
			modes = append(modes, m)
		}
	}
	return modes
}

// followupCell fails the calls that hold any of failing, in mode, and returns what the client saw and the
// error-layer classes it fails.
func followupCell(t *testing.T, rt errLayerRoute, mode, okBody string, failing ...string) (errLayerOutcome, []string) {
	t.Helper()
	backend := &errLayerBackend{mode: mode, failContains: failing, okBody: okBody, seen: map[string]struct{}{}}
	outcome, host := runErrLayerBackendCell(t, rt, backend)
	if backend.failHits.Load() == 0 {
		t.Fatalf("no call held %q; the cell proves nothing (calls %q)", failing, backend.order)
	}
	return outcome, errLayerClasses(rt, mode, outcome, host)
}

// requireFailed fails the test unless the client saw Loki's error for mode.
func requireFailed(t *testing.T, o errLayerOutcome, classes []string, mode string) {
	t.Helper()
	if len(classes) > 0 || (o.status == http.StatusOK && mode != "oversize") {
		t.Fatalf("a failed call is not Loki's error %v: status %d, body %.300q", classes, o.status, o.body)
	}
}

// A field inside the line is read by the unpack query, the last read of the values: its failure is the answer. The
// one 400 that is not a failure is VictoriaLogs' parser refusing the extended query, which leaves no values.
//
// conformance: semantics/error-layer-silent-success
func TestDetectedFieldValues_UnpackReadFailureFailsTheRequest(t *testing.T) {
	rt := followupRoute("/loki/api/v1/detected_field/status/values", `{app="api"}`)
	for _, mode := range followupModes() {
		t.Run(mode, func(t *testing.T) {
			o, classes := followupCell(t, rt, mode, "empty", "| unpack_json from _msg")
			if mode == "vl400_parse" {
				if o.status != http.StatusOK || !strings.Contains(o.body, `"values":[]`) {
					t.Fatalf("a refused unpack query is no values: %d %.200q", o.status, o.body)
				}
				return
			}
			requireFailed(t, o, classes, mode)
		})
	}
	t.Run("no values is an answer", func(t *testing.T) {
		backend := &errLayerBackend{failContains: []string{"never"}, okBody: "empty"}
		o, _ := runErrLayerBackendCell(t, rt, backend)
		if o.status != http.StatusOK || !strings.Contains(o.body, `"values":[]`) {
			t.Fatalf("got %d %.200q", o.status, o.body)
		}
	})
}

// The unpack retry of the stored-column lookup is the same read: the lookup found the column and no values, so a
// failed retry cannot be answered with the empty list.
//
// conformance: semantics/error-layer-silent-success
func TestDetectedFieldValues_NativeUnpackRetryFailureFailsTheRequest(t *testing.T) {
	for _, tc := range []struct {
		name, mode string
		status     int
	}{
		{"vl500", "vl500", http.StatusInternalServerError},
		{"malformed", "malformed", http.StatusInternalServerError},
		{"truncated", "truncated", http.StatusInternalServerError},
		{"refused_query", "vl400_parse", http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := &errLayerBackend{mode: tc.mode}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = r.ParseForm()
				switch {
				case r.URL.Path == "/select/logsql/field_names":
					_, _ = w.Write([]byte(`{"values":[{"value":"status","hits":4}]}`))
				case r.URL.Path == "/select/logsql/field_values" && strings.Contains(r.Form.Get("query"), "unpack_json"):
					backend.fail(w, r)
				default:
					_, _ = w.Write([]byte(`{"values":[]}`))
				}
			}))
			defer srv.Close()
			c := cache.New(time.Minute, 100)
			defer c.Close()
			p, err := New(Config{BackendURL: srv.URL, Cache: c, LogLevel: "error", BackendTimeout: 5 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = p.Shutdown(context.Background()) }()
			mux := http.NewServeMux()
			p.RegisterRoutes(mux)
			rt := followupRoute("/loki/api/v1/detected_field/status/values", `{app="api"}`)
			req := httptest.NewRequest(http.MethodGet, rt.path+"?"+rt.params.Encode(), nil)
			req.Header.Set("X-Scope-OrgID", "0")
			w := httptest.NewRecorder()
			shippedChain(mux).ServeHTTP(w, req)
			if w.Code != tc.status {
				t.Fatalf("status %d, want %d; body %.200q", w.Code, tc.status, w.Body.String())
			}
			if tc.status != http.StatusOK && w.Body.String() != lokiErrInternal {
				t.Fatalf("body %q, want Loki's %q", w.Body.String(), lokiErrInternal)
			}
		})
	}
}

// A value held by a stream label is read from the labels once the fields hold none: when both label reads fail the
// request fails; one working read answers.
//
// conformance: semantics/error-layer-silent-success
func TestDetectedFieldValues_LabelReadFailureFailsTheRequest(t *testing.T) {
	rt := followupRoute("/loki/api/v1/detected_field/status/values", `{app="api"}`)
	for _, mode := range followupModes() {
		t.Run("both_fail/"+mode, func(t *testing.T) {
			o, classes := followupCell(t, rt, mode, "empty", "streams ", "| sort by (_time desc)")
			requireFailed(t, o, classes, mode)
		})
	}
	for _, only := range []string{"streams ", "| sort by (_time desc)"} {
		t.Run("one_fails/"+strings.TrimSpace(only), func(t *testing.T) {
			// The stored lookup falls back to the sampled scan, and the scan supplement only adds level and
			// service_name: neither alone decides the values of status.
			o, _ := runErrLayerBackendCell(t, rt, &errLayerBackend{mode: "vl500", failContains: []string{only}, okBody: "empty"})
			if o.status != http.StatusOK {
				t.Fatalf("one working label read must answer, got %d %.200q", o.status, o.body)
			}
		})
	}
	t.Run("level_needs_the_supplement", func(t *testing.T) {
		lrt := followupRoute("/loki/api/v1/detected_field/level/values", `{app="api"}`)
		for _, mode := range []string{"vl500", "malformed", "truncated"} {
			o, classes := followupCell(t, lrt, mode, "empty", "| sort by (_time desc)")
			requireFailed(t, o, classes, mode)
		}
	})
}

// The scan is the read that decides detected_fields: with it failed the stored fields alone are not the fields of
// the query's lines. The stored-field lookup is an extra: its failure keeps the scan's fields.
//
// conformance: semantics/error-layer-silent-success
func TestDetectedFields_ScanFailureFailsTheRequestAndLookupFailureDoesNot(t *testing.T) {
	rt := followupRoute("/loki/api/v1/detected_fields", `{app="api"}`)
	for _, mode := range followupModes() {
		t.Run("scan_fails/"+mode, func(t *testing.T) {
			o, classes := followupCell(t, rt, mode, "rows", "query ")
			requireFailed(t, o, classes, mode)
		})
	}
	for _, mode := range []string{"vl500", "malformed", "truncated", "conn_reset"} {
		t.Run("lookup_fails/"+mode, func(t *testing.T) {
			backend := &errLayerBackend{mode: mode, failContains: []string{"field_names "}, okBody: "rows"}
			o, _ := runErrLayerBackendCell(t, rt, backend)
			if o.status != http.StatusOK || !strings.Contains(o.body, `"label":"amount"`) {
				t.Fatalf("the scan's fields must answer: %d %.300q", o.status, o.body)
			}
		})
	}
}

// The one-sample proof of rate_counter is an optimisation: every failed read of it is no verdict, the per-row
// evaluator answers, and a key sample that could not be read whole is used once and not remembered.
//
// conformance: semantics/error-layer-silent-success
func TestRateCounterProof_FailedReadsAreNoVerdictAndNoCachedKeys(t *testing.T) {
	const (
		goodKeys = `{"name":"__lvpk.a","hits":"3"}` + "\n" + `{"name":"__lvpk.b","hits":"2"}` + "\n"
		statsOK  = `{"n":"3","u":"3"}`
	)
	newProxy := func(t *testing.T, handler http.HandlerFunc) *Proxy {
		srv := httptest.NewServer(handler)
		t.Cleanup(srv.Close)
		c := cache.New(time.Minute, 100)
		t.Cleanup(c.Close)
		p, err := New(Config{BackendURL: srv.URL, Cache: c, LogLevel: "error", BackendTimeout: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
		return p
	}
	to := time.Now().Truncate(time.Minute)
	from := to.Add(-5 * time.Minute)

	t.Run("key sample", func(t *testing.T) {
		for _, tc := range []struct {
			name, body string
			wantKeys   int
			cached     bool
		}{
			{"clean", goodKeys, 2, true},
			{"empty", "", 0, true},
			{"a row that is not JSON", goodKeys + "VLSENTINEL\n", 2, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var calls int
				p := newProxy(t, func(w http.ResponseWriter, r *http.Request) {
					calls++
					_, _ = w.Write([]byte(tc.body))
				})
				for i := 0; i < 2; i++ {
					if keys := p.rateCounterIdentityKeys(context.Background(), "*", "amount", from, to); len(keys) != tc.wantKeys {
						t.Fatalf("keys %v, want %d", keys, tc.wantKeys)
					}
				}
				if want := map[bool]int{true: 1, false: 2}[tc.cached]; calls != want {
					t.Fatalf("%d backend call(s) for two proofs, want %d", calls, want)
				}
			})
		}
		t.Run("a failed read is not remembered", func(t *testing.T) {
			var calls int
			p := newProxy(t, func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.WriteHeader(http.StatusInternalServerError)
			})
			for i := 0; i < 2; i++ {
				if keys := p.rateCounterIdentityKeys(context.Background(), "*", "amount", from, to); len(keys) != 0 {
					t.Fatalf("keys %v from a failed read", keys)
				}
			}
			if calls != 2 {
				t.Fatalf("%d backend call(s), want 2", calls)
			}
		})
	})

	t.Run("count", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			status int
			body   string
			want   bool
		}{
			{"one sample per series", 200, statsOK, true},
			{"two samples in a series", 200, `{"n":"3","u":"2"}`, false},
			{"vl500", 500, "VLSENTINEL", false},
			{"vl429", 429, "VLSENTINEL", false},
			{"malformed", 200, `{"n":"3",VLSENTINEL`, false},
			{"not an object", 200, `[1,2]`, false},
			{"empty", 200, ``, false},
			{"no count", 200, `{"x":"1"}`, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				p := newProxy(t, func(w http.ResponseWriter, r *http.Request) {
					_ = r.ParseForm()
					if strings.Contains(r.Form.Get("query"), "field_names") {
						_, _ = w.Write([]byte(goodKeys))
						return
					}
					w.WriteHeader(tc.status)
					_, _ = w.Write([]byte(tc.body))
				})
				if got := p.rateCounterOneSample(context.Background(), "*", "amount", "amount", from, to); got != tc.want {
					t.Fatalf("proof %v, want %v", got, tc.want)
				}
			})
		}
	})
}

// BenchmarkFetchUnpackedFieldValues is the happy path of the unpack read: a 200 answer with 200 values.
func BenchmarkFetchUnpackedFieldValues(b *testing.B) {
	var body strings.Builder
	body.WriteString(`{"values":[`)
	for i := 0; i < 200; i++ {
		if i > 0 {
			body.WriteByte(',')
		}
		fmt.Fprintf(&body, `{"value":"value-%04d","hits":0}`, i)
	}
	body.WriteString(`]}`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body.String()))
	}))
	defer srv.Close()
	p, err := New(Config{BackendURL: srv.URL, Cache: cache.NewDisabled(), LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		values, err := p.fetchUnpackedFieldValues(ctx, `{app="api"}`, "1", "2", "status", 100)
		if err != nil || len(values) != 200 {
			b.Fatalf("got %d values, err %v", len(values), err)
		}
	}
}

// The service_name list read from the detected labels comes from the scan supplement when no stored label holds
// it, so a failed supplement is the error and not a list without service_name.
//
// conformance: semantics/error-layer-silent-success
func TestServiceNameValuesFromDetectedLabels_SupplementFailureIsTheError(t *testing.T) {
	for _, mode := range []string{"vl500", "malformed", "truncated"} {
		t.Run(mode, func(t *testing.T) {
			backend := &errLayerBackend{mode: mode, failContains: []string{"| sort by (_time desc)"}, okBody: "empty"}
			srv := httptest.NewServer(backend)
			defer srv.Close()
			c := cache.New(time.Minute, 100)
			defer c.Close()
			p, err := New(Config{BackendURL: srv.URL, Cache: c, LogLevel: "error", BackendTimeout: 5 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = p.Shutdown(context.Background()) }()
			values, err := p.serviceNameValuesFromDetectedLabels(context.Background(), `{app="api"}`, "1", "2")
			if err == nil {
				t.Fatalf("got values %v and no error from a failed supplement", values)
			}
		})
	}
}

// BenchmarkRateCounterIdentityKeys is the happy path of the key discovery: one read of 40 keys, never cached.
func BenchmarkRateCounterIdentityKeys(b *testing.B) {
	var body strings.Builder
	for i := 0; i < 40; i++ {
		fmt.Fprintf(&body, `{"name":"__lvpk.key%02d","hits":"%d"}`+"\n", i, 1000-i)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(body.String()))
	}))
	defer srv.Close()
	p, err := New(Config{BackendURL: srv.URL, Cache: cache.NewDisabled(), LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	to := time.Now().Truncate(time.Minute)
	from := to.Add(-5 * time.Minute)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// A different query each time, so the key cache never answers.
		if keys := p.rateCounterIdentityKeys(ctx, fmt.Sprintf("app:=%d", i), "amount", from, to); len(keys) != 40 {
			b.Fatalf("got %d keys", len(keys))
		}
	}
}

// BenchmarkDetectLabelsFakeVL is the happy path of detected_labels: the stored read answers and the scan supplies
// the level label, with the response cache off.
func BenchmarkDetectLabelsFakeVL(b *testing.B) {
	var rows strings.Builder
	for i := 0; i < 200; i++ {
		fmt.Fprintf(&rows, `{"_time":"2026-04-01T00:00:%02dZ","_stream":"{app=\"api\",pod=\"p%d\"}","app":"api","level":"info","_msg":"x"}`+"\n", i%60, i%7)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch r.URL.Path {
		case "/select/logsql/query":
			_, _ = w.Write([]byte(rows.String()))
		case "/select/logsql/streams":
			_, _ = w.Write([]byte(`{"values":[{"value":"{app=\"api\",pod=\"p1\"}","hits":3}]}`))
		default:
			_, _ = w.Write([]byte(`{"values":[]}`))
		}
	}))
	defer srv.Close()
	p, err := New(Config{BackendURL: srv.URL, Cache: cache.NewDisabled(), LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := p.detectLabels(ctx, `{app="api"}`, "1", "2", 100); err != nil {
			b.Fatal(err)
		}
	}
}
