package proxy

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// A request over clean field names counts natively without packing the envelope, and its answer is the answer of
// the full class on the same data, which also takes a window whose field names are not clean. The data holds the
// rows the certified guards send to the exact complement (an apostrophe, <, = and \ in the message, a long message,
// non-ASCII, control and quote characters in another field, non-JSON bodies, an extra field).
// conformance: semantics/two-parsers-keep-one
func TestOrderedMixedStatsCertifiedClassMatchesFullClass(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("requires the VictoriaLogs oracle")
	}
	at := time.Unix(1700002800, 0).UTC()
	long := `{"k":"long","pad":"` + strings.Repeat("A", 1300) + `"}`
	type row struct {
		ts     time.Duration
		msg    string
		fields [][2]string
	}
	clean := []row{
		{-100 * time.Second, `{"k":"body"}`, nil}, {-95 * time.Second, `{"k":"body"}`, [][2]string{{"meta", "plain"}}},
		{-90 * time.Second, `{"k":"blue","n":1}`, nil}, {-80 * time.Second, `k=body`, nil}, {-70 * time.Second, `{"k":"it's"}`, nil},
		{-60 * time.Second, `{"k":"a<b"}`, nil}, {-55 * time.Second, `{"k":"a=b"}`, nil}, {-50 * time.Second, `{"k":"a\\b"}`, nil},
		{-45 * time.Second, long, nil}, {-40 * time.Second, `{"k":"body"}`, [][2]string{{"meta", "é€ unicode"}}},
		{-35 * time.Second, `{"k":"body"}`, [][2]string{{"meta", "quote\" back\\ ctl\x01 apos' lt<"}}},
		{-30 * time.Second, `{"k":"body"}`, [][2]string{{"meta", strings.Repeat("m", 49)}}},
		{-25 * time.Second, `{"k":"-0"}`, nil}, {-20 * time.Second, `{"k":"body","k":"later"}`, nil},
		{-15 * time.Second, `{"k":"body"}`, [][2]string{{"meta", "x"}, {"extra", "y"}}}, {-10 * time.Second, `{ "k":"spaced" }`, nil},
		{0, `{"k":"body"}`, nil}, {5 * time.Second, `{"k":"blue"}`, nil}, {30 * time.Second, `{"k":"body"}`, nil}, {70 * time.Second, `{"k":"late"}`, nil},
	}
	unclean := append([]row{{-20 * time.Second, `{"k":"body"}`, [][2]string{{"a__b", "x"}}}}, clean...)
	query := `sum by(k)(count_over_time({app="mixed-cert"} | json k="[\"k\"]" | logfmt | drop __error__, __error_details__ | k!="" [1m]))`
	start, end, step := at.Add(-2*time.Minute), at.Add(2*time.Minute), 15*time.Second
	for _, version := range []struct{ name, modfile string }{{"v1.49.0", "go.v1.49.0.mod"}, {"v1.54.0", ""}} {
		for _, dataset := range []struct {
			name         string
			rows         []row
			wantCertSkip bool
		}{{"clean", clean, false}, {"unclean-name", unclean, true}} {
			t.Run(version.name+"/"+dataset.name, func(t *testing.T) {
				var store oraclerun.ServeDataSet
				for _, r := range dataset.rows {
					store.Rows = append(store.Rows, oraclerun.ServeRow{TS: at.Add(r.ts).UnixNano(), Stream: [][2]string{{"app", "mixed-cert"}},
						Fields: append([][2]string{{"_msg", r.msg}}, r.fields...)})
				}
				backend, err := oraclerun.StartVLServe(filepath.Join("..", "..", "test", "oracle", "vl"), version.modfile, []oraclerun.ServeDataSet{store})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(backend.Stop)
				target, _ := url.Parse(backend.URL)
				run := func(failFieldNames bool) ([]byte, []string) {
					var mu sync.Mutex
					var stats []string
					reverse := httputil.NewSingleHostReverseProxy(target)
					front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if failFieldNames && strings.HasSuffix(r.URL.Path, "/field_names") {
							http.Error(w, "forced", http.StatusInternalServerError)
							return
						}
						if strings.HasSuffix(r.URL.Path, "/stats_query_range") {
							raw, _ := io.ReadAll(r.Body)
							r.Body = io.NopCloser(bytes.NewReader(raw))
							form, _ := url.ParseQuery(string(raw))
							mu.Lock()
							stats = append(stats, form.Get("query"))
							mu.Unlock()
						}
						reverse.ServeHTTP(w, r)
					}))
					defer front.Close()
					p, err := New(Config{BackendURL: front.URL, Cache: cache.NewDisabled(), LogLevel: "error", MetadataInventoryParallelism: -1})
					if err != nil {
						t.Fatal(err)
					}
					p.storeBackendVersion(version.name, version.name)
					defer func() { _ = p.Shutdown(context.Background()) }()
					plan, ok := compileOrderedJSONMetric(query)
					if !ok {
						t.Fatal("typed mixed plan missing")
					}
					series, served, err := p.collectOrderedMixedJSONStats(context.Background(), plan, start, end, step)
					if err != nil || !served {
						t.Fatalf("served=%v error=%v", served, err)
					}
					body, err := buildMixedJSONStatsMatrix(plan, series, start, end, step, p.limits().BufferedBackendBodyBytes)
					if err != nil {
						t.Fatal(err)
					}
					mu.Lock()
					defer mu.Unlock()
					return body, append([]string(nil), stats...)
				}
				got, gotQueries := run(false)
				want, wantQueries := run(true)
				if string(got) != string(want) {
					t.Fatalf("the certified path answers differently from the full class:\n got  %s\n want %s", got, want)
				}
				packs := func(qs []string) bool {
					for _, q := range qs {
						if strings.Contains(q, "| pack_json as "+mixedClassOriginalField) {
							return true
						}
					}
					return false
				}
				if len(gotQueries) == 0 || len(wantQueries) == 0 {
					t.Fatalf("no stats call recorded (%d, %d)", len(gotQueries), len(wantQueries))
				}
				if !packs(wantQueries) {
					t.Fatal("the full class did not pack the envelope")
				}
				if dataset.wantCertSkip && !packs(gotQueries) {
					t.Fatalf("an unclean field name was certified: %v", gotQueries)
				}
				if !dataset.wantCertSkip && strings.Contains(gotQueries[0], "| pack_json as "+mixedClassOriginalField) {
					t.Fatalf("clean names were not certified: %s", gotQueries[0])
				}
				t.Logf("%s %s: %d bytes identical; native count packs the envelope: %v", version.name, dataset.name, len(got), packs(gotQueries))
			})
		}
	}
}
