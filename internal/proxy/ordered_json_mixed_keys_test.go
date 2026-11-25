package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// conformance: semantics/two-parsers-keep-one
func TestMixedJSONKeyCoreAndNamesClean(t *testing.T) {
	cores := map[string]string{
		"k": "k", " k ": "k", "user_id": "user_id", "user.id": "user_id", "user-id": "user_id", "user__id": "user_id",
		"user_id_": "user_id", "_user_id": "user_id", "User_Id": "User_Id", "a b  c": "a_b_c", "ключ": "", "": "", "ü_k": "k",
	}
	for in, want := range cores {
		if got := mixedJSONKeyCore(in); got != want {
			t.Errorf("core(%q) = %q, want %q", in, got, want)
		}
	}
	long := strings.Repeat("a", 129)
	for _, tc := range []struct {
		label string
		names []string
		clean bool
	}{
		{"k", []string{"k"}, true},
		{"k", []string{"k", "other", "k_sub", "k.sub", "K", "ключ", "kk"}, true}, // benign lookalikes
		{"k", []string{"k", " k "}, false},
		{"k", []string{"k", "-k"}, false},
		{"k", []string{"k", ""}, false},
		{"k", []string{"k", "_msg"}, false},
		{"k", []string{"k", long}, false},
		{"k", []string{"k", strings.Repeat("b", 128)}, true},
		{"user_id", []string{"user_id", "user", "id", "user_name", "User_Id", "user_id_x"}, true},
		{"user_id", []string{"user_id", "user.id"}, false},
		{"user_id", []string{"user_id", "user-id"}, false},
		{"user_id", []string{"user_id", "user__id"}, false},
		{"user_id", []string{"user_id", "user_id_"}, false},
		{"user_id", []string{"user_id", "user. id"}, false},
		{"user_id", []string{"user_id", "user..id"}, false},
		{"a_b_c", []string{"a_b_c", "a.b.c"}, false},
		{"a_b_c", []string{"a_b_c", "a.b", "b.c", "a_b"}, true},
		{"", []string{"x"}, false},
		{"k", nil, true},
	} {
		if got := mixedJSONKeyNamesClean(tc.label, tc.names); got != tc.clean {
			t.Errorf("clean(%q, %q) = %v, want %v", tc.label, tc.names, got, tc.clean)
		}
	}
}

type mixedKeysBackend struct {
	mu          sync.Mutex
	listings    [][]string // each listing request takes the next; the last repeats; nil entry = unavailable
	listCalls   int
	statsQuerys []string
}

func (b *mixedKeysBackend) next() ([]string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	i := b.listCalls
	b.listCalls++
	if i >= len(b.listings) {
		i = len(b.listings) - 1
	}
	return b.listings[i], b.listings[i] != nil
}

func newMixedKeysProxy(t *testing.T, at time.Time, b *mixedKeysBackend) *Proxy {
	t.Helper()
	original, err := json.Marshal(map[string]string{"_time": at.Add(time.Nanosecond).Format(time.RFC3339Nano), "_stream": `{app="api"}`, "_msg": `k=body`})
	if err != nil {
		t.Fatal(err)
	}
	outer, err := json.Marshal(map[string]string{mixedClassOriginalField: string(original)})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case statsQueryRangePath:
			b.mu.Lock()
			b.statsQuerys = append(b.statsQuerys, r.FormValue("query"))
			b.mu.Unlock()
			fmt.Fprintf(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"c","%s":"body"},"values":[[%d,"1"]]},{"metric":{"__name__":"%s","%s":""},"values":[[%d,"1"]]}]}}`, mixedStatsGroupField, at.Add(-30*time.Second).Unix(), "c", mixedStatsGroupField, at.Unix())
		case "/select/logsql/query":
			if strings.Contains(r.FormValue("query"), mixedJSONKeyPrefix) {
				names, ok := b.next()
				if !ok {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				for _, name := range names {
					fmt.Fprintf(w, "{\"name\":%q,\"hits\":\"1\"}\n", mixedJSONKeyPrefix+name)
				}
				fmt.Fprintf(w, "{\"name\":\"app\",\"hits\":\"1\"}\n") // a stored field is not a key
				return
			}
			_, _ = w.Write(append(outer, '\n'))
		case "/select/logsql/field_names":
			w.WriteHeader(http.StatusServiceUnavailable) // the stored-field certificate is not under test here
		default:
			t.Errorf("unexpected backend read %s", r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	t.Cleanup(srv.Close)
	p, err := New(Config{BackendURL: srv.URL, Cache: cache.NewDisabled(), LogLevel: "error", MetadataInventoryParallelism: -1})
	if err != nil {
		t.Fatal(err)
	}
	p.storeBackendVersion("v1.54.0", "v1.54.0")
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

// conformance: semantics/two-parsers-keep-one
func TestOrderedMixedStatsKeyCheckedFlow(t *testing.T) {
	at := time.Unix(1700002800, 0).UTC()
	const query = `sum by(k)(count_over_time({app="api"} | logfmt | json | drop __error__, __error_details__ | k!="" [1m]))`
	run := func(p *Proxy) map[string]int {
		t.Helper()
		plan, ok := compileOrderedJSONMetric(query)
		if !ok {
			t.Fatal("typed mixed plan missing")
		}
		rt := newRequestTelemetry()
		ctx := context.WithValue(context.Background(), requestTelemetryKey, rt)
		series, served, err := p.collectOrderedMixedJSONStats(ctx, plan, at, at.Add(time.Minute), 30*time.Second)
		if err != nil || !served || len(series) == 0 {
			t.Fatalf("served=%v series=%d error=%v", served, len(series), err)
		}
		out := map[string]int{}
		rt.mu.Lock()
		for _, outcome := range []string{"certified", "dirty", "cache_dirty", "names_changed", "unlisted"} {
			out[outcome] = rt.internalOpsByType[internalOperationBreakdownKey("mixed_class_keys", outcome)]
		}
		rt.mu.Unlock()
		return out
	}
	hasKeyText := func(q string) bool { return strings.Contains(q, `{129}`) }
	want := func(t *testing.T, got map[string]int, outcome string) {
		t.Helper()
		for k, v := range got {
			if (k == outcome) != (v == 1) || v > 1 {
				t.Fatalf("outcomes %v, want only %s", got, outcome)
			}
		}
	}

	t.Run("clean names count natively without the key-text guards, listed before and after", func(t *testing.T) {
		b := &mixedKeysBackend{listings: [][]string{{"k", "other"}}}
		p := newMixedKeysProxy(t, at, b)
		want(t, run(p), "certified")
		if b.listCalls != 2 || len(b.statsQuerys) != 1 || hasKeyText(b.statsQuerys[0]) {
			t.Fatalf("listings=%d stats=%d keyText=%v", b.listCalls, len(b.statsQuerys), len(b.statsQuerys) > 0 && hasKeyText(b.statsQuerys[0]))
		}
		// The remembered verdict skips the first listing; the one after the scan is always read.
		want(t, run(p), "certified")
		if b.listCalls != 3 || len(b.statsQuerys) != 2 {
			t.Fatalf("repeat: listings=%d stats=%d", b.listCalls, len(b.statsQuerys))
		}
	})
	t.Run("an aliasing name sends the window to the full class and is remembered", func(t *testing.T) {
		b := &mixedKeysBackend{listings: [][]string{{"k", " k "}}}
		p := newMixedKeysProxy(t, at, b)
		want(t, run(p), "dirty")
		if b.listCalls != 1 || len(b.statsQuerys) != 1 || !hasKeyText(b.statsQuerys[0]) {
			t.Fatalf("listings=%d stats=%d", b.listCalls, len(b.statsQuerys))
		}
		want(t, run(p), "cache_dirty")
		if b.listCalls != 1 || len(b.statsQuerys) != 2 || !hasKeyText(b.statsQuerys[1]) {
			t.Fatalf("repeat must not list again: listings=%d stats=%d", b.listCalls, len(b.statsQuerys))
		}
	})
	t.Run("a name that appears during the scan discards the native answer", func(t *testing.T) {
		b := &mixedKeysBackend{listings: [][]string{{"k"}, {"k", " k "}}}
		p := newMixedKeysProxy(t, at, b)
		want(t, run(p), "names_changed")
		if len(b.statsQuerys) != 2 || hasKeyText(b.statsQuerys[0]) || !hasKeyText(b.statsQuerys[1]) {
			t.Fatalf("native then full class expected: %d stats calls", len(b.statsQuerys))
		}
		// The verdict was dropped, so the next request lists again.
		before := b.listCalls
		want(t, run(p), "dirty")
		if b.listCalls != before+1 {
			t.Fatalf("listings %d -> %d", before, b.listCalls)
		}
	})
	t.Run("an unavailable listing never runs the native count without the guards", func(t *testing.T) {
		b := &mixedKeysBackend{listings: [][]string{nil}}
		p := newMixedKeysProxy(t, at, b)
		want(t, run(p), "unlisted")
		if len(b.statsQuerys) != 1 || !hasKeyText(b.statsQuerys[0]) {
			t.Fatalf("full class expected: %d stats calls", len(b.statsQuerys))
		}
	})
	t.Run("a listing that reaches its bound is not clean", func(t *testing.T) {
		names := make([]string, mixedJSONKeyNamesMax+1)
		for i := range names {
			names[i] = fmt.Sprintf("f%d", i)
		}
		b := &mixedKeysBackend{listings: [][]string{names}}
		p := newMixedKeysProxy(t, at, b)
		want(t, run(p), "unlisted")
		if !hasKeyText(b.statsQuerys[0]) {
			t.Fatal("full class expected")
		}
	})
}
