package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
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
	// A window with more stored fields than the fixed bounds allow (nine others besides meta and extra), a message
	// near the long bound and a long other field: certified only from the measured maximum lengths.
	var wide []row
	for i, r := range clean {
		extra := append([][2]string(nil), r.fields...)
		for f := 1; f <= 9; f++ {
			extra = append(extra, [2]string{"wide" + strconv.Itoa(f), strings.Repeat("v", 3*f) + strconv.Itoa(i)})
		}
		wide = append(wide, row{r.ts, r.msg, extra})
	}
	wide = append(wide,
		row{-5 * time.Second, `{"k":"body","pad":"` + strings.Repeat("B", 900) + `"}`, nil},
		row{-4 * time.Second, `{"k":"blue","q":"` + strings.Repeat(`\"`, 3) + `"}`, nil},
		row{-3 * time.Second, `{"k":"body"}`, [][2]string{{"wide1", strings.Repeat("w", 47)}}})
	query := `sum by(k)(count_over_time({app="mixed-cert"} | json k="[\"k\"]" | logfmt | drop __error__, __error_details__ | k!="" [1m]))`
	start, end, step := at.Add(-2*time.Minute), at.Add(2*time.Minute), 15*time.Second
	for _, version := range []struct{ name, modfile string }{{"v1.49.0", "go.v1.49.0.mod"}, {"v1.54.0", ""}} {
		for _, dataset := range []struct {
			name         string
			rows         []row
			wantCertSkip bool
			wantMeasured bool
		}{{"clean", clean, false, false}, {"unclean-name", unclean, true, false}, {"wide-measured", wide, false, true}} {
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
				// mode: "certified" (the request as it runs), "full" (no field names: the full class) or "shrunk" (the measured
				// lengths halved, so rows longer than their bounds go to the exact complement).
				run := func(mode string) ([]byte, []string, int) {
					failFieldNames := mode == "full"
					var mu sync.Mutex
					var stats []string
					measured, listings := 0, 0
					reverse := httputil.NewSingleHostReverseProxy(target)
					front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if strings.HasSuffix(r.URL.Path, "/field_names") {
							mu.Lock()
							listings++
							mu.Unlock()
						}
						if failFieldNames && strings.HasSuffix(r.URL.Path, "/field_names") {
							http.Error(w, "forced", http.StatusInternalServerError)
							return
						}
						if strings.HasSuffix(r.URL.Path, "/select/logsql/query") && strings.Contains(r.URL.Query().Get("query"), "mixed_len_") {
							mu.Lock()
							measured++
							mu.Unlock()
							if mode == "shrunk" {
								rec := httptest.NewRecorder()
								reverse.ServeHTTP(rec, r)
								var row map[string]string
								if err := json.Unmarshal(bytes.TrimSpace(rec.Body.Bytes()), &row); err == nil {
									for k, v := range row {
										n, _ := strconv.Atoi(v)
										row[k] = strconv.Itoa(n / 2)
									}
									out, _ := json.Marshal(row)
									_, _ = w.Write(out)
									return
								}
								_, _ = w.Write(rec.Body.Bytes())
								return
							}
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
					if mode == "certified" {
						// The same request again: the certificate is remembered, so only the listing after the scan
						// runs (a remembered unclean listing runs none), and the answer is the same bytes.
						mu.Lock()
						l0, m0 := listings, measured
						mu.Unlock()
						series, served, err := p.collectOrderedMixedJSONStats(context.Background(), plan, start, end, step)
						if err != nil || !served {
							t.Fatalf("repeat: served=%v error=%v", served, err)
						}
						again, err := buildMixedJSONStatsMatrix(plan, series, start, end, step, p.limits().BufferedBackendBodyBytes)
						if err != nil {
							t.Fatal(err)
						}
						if string(again) != string(body) {
							t.Fatalf("the remembered certificate answers differently:\n got  %s\n want %s", again, body)
						}
						mu.Lock()
						wantListings := 1
						if dataset.wantCertSkip {
							wantListings = 0
						}
						if listings-l0 != wantListings || measured != m0 {
							t.Fatalf("repeat made %d listings (want %d) and %d measurements (want 0)", listings-l0, wantListings, measured-m0)
						}
						mu.Unlock()
					}
					mu.Lock()
					defer mu.Unlock()
					return body, append([]string(nil), stats...), measured
				}
				got, gotQueries, gotMeasured := run("certified")
				want, wantQueries, _ := run("full")
				if string(got) != string(want) {
					t.Fatalf("the certified path answers differently from the full class:\n got  %s\n want %s", got, want)
				}
				if dataset.wantMeasured {
					if gotMeasured == 0 {
						t.Fatal("the wide window was not measured")
					}
					shrunk, shrunkQueries, _ := run("shrunk")
					if string(shrunk) != string(want) {
						t.Fatalf("halved measured bounds change the answer:\n got  %s\n want %s", shrunk, want)
					}
					if len(shrunkQueries) == 0 || strings.Contains(shrunkQueries[0], "| pack_json as "+mixedClassOriginalField) {
						t.Fatalf("the shrunk run was not certified: %v", shrunkQueries)
					}
				} else if gotMeasured != 0 {
					t.Fatalf("a window the fixed bounds certify (or an unclean one) was measured %d times", gotMeasured)
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
