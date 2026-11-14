package proxy

import (
	"context"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// Every metric query of the plan corpus is translated to the same text with the flag on and off, under every
// translator profile of the route parity test, with the metric constructs of metricFlipCandidates served from the
// lowering: they write the string translator's text, byte for byte, through the proxy's own translation path, so a
// flip of them moves neither the translation cache, nor the requests VictoriaLogs sees, nor the keys of its answers. The one
// metric shape served with another text is the keep identity (keepIdentityConstruct, semantics/
// keep-before-unwrap-series-identity), which the handlers read too (keepUnwrapIdentity). Each of them is served
// on the corpus under every profile. (The log queries the lowering serves are held to the
// translator by the corpus shadow and the label-filter grids, with their recorded divergences.)
//
// conformance: architecture-syntax-package-layering
func TestQueryPlanLowering_CorpusTextParity(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	corpus, err := plancorpus.Queries(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	total := map[string]int{}
	for _, prof := range loweringParityProfiles {
		t.Run(prof.name, func(t *testing.T) {
			start := func(disable bool) *Proxy {
				cfg := Config{BackendURL: "http://unused", Cache: cache.New(time.Minute, 100), LogLevel: "error", DisableQueryPlanLowering: disable}
				prof.config(&cfg)
				p, err := New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
				if prof.version != "" {
					p.backendVersionMu.Lock()
					p.backendVersionSemver = prof.version
					p.backendVersionMu.Unlock()
				}
				return p
			}
			on, off := start(false), start(true)
			withFlipped(on, metricFlipCandidates...)
			// No translation cache: every row is translated, not read back.
			on.translationCache, off.translationCache = nil, nil
			ctx := context.Background()
			differs, keep := 0, 0
			for _, q := range corpus {
				if strings.HasPrefix(strings.TrimSpace(q), "{") {
					continue
				}
				got, gotErr := on.translateQueryOpts(ctx, q, false)
				want, wantErr := off.translateQueryOpts(ctx, q, false)
				if _, kept := on.keepUnwrapIdentity(q); kept {
					keep++
					continue
				}
				if got != want || (gotErr == nil) != (wantErr == nil) {
					if differs++; differs <= 10 {
						t.Errorf("%q:\n flag on  %q %v\n flag off %q %v", q, got, gotErr, want, wantErr)
					}
				}
			}
			if keep == 0 {
				t.Error("no keep identity row: the case tests nothing")
			}
			counts := shadowCounts(t, on)
			total["keep identity rows"] += keep
			for _, c := range metricFlipCandidates {
				n := counts["flipped "+c]
				total[c] += n
				if n == 0 {
					t.Errorf("%s is flipped, but no corpus row is served from the lowering", c)
				}
			}
			if n := shadowTotal(shadowCounts(t, off), "flipped"); n != 0 {
				t.Fatalf("flag off: %d flipped translations", n)
			}
		})
	}
	constructs := make([]string, 0, len(total))
	for c := range total {
		constructs = append(constructs, c)
	}
	sort.Strings(constructs)
	for _, c := range constructs {
		t.Logf("served from the lowering: %-18s %d translations", c, total[c])
	}
}

// FuzzMetricFlipGate holds the metric flip to the string translator on any query: with the candidate metric
// constructs flipped, a query the lowering serves (writesTranslatorText passed) is the translator's text, or the keep
// identity; the gate and the lowering never panic out of lowerMetric. The translator's text is also taken with its
// keyword label names quoted (lower.QuoteKeywordFieldText), as the corpus shadow compares it: VictoriaLogs rejects
// them bare (semantics/translator-writes-keyword-field-names-bare).
//
// conformance: architecture-syntax-package-layering
func FuzzMetricFlipGate(f *testing.F) {
	for _, q := range []string{
		`sum by (app) (rate({app="a"} |= "x" [5m]))`, `count_over_time({app="a"} | json | status="500" [5m])`,
		`sum(rate({app="a"}[5m])) / sum(rate({app="b"}[5m]))`, `label_replace(rate({app="a"}[5m]), "x", "$1", "app", "(.*)")`,
		`topk(2, sum by (app) (rate({app="a"}[5m])))`, `stddev(count_over_time({app="a"}[5m]))`,
		`sum without (pod) (rate({app="a"}[5m])) / 2`, `sum(count_over_time({app="a"} | json | n == 5 [5m]))`,
		`sum(count_over_time({app="a"} | _time="x" [5m]))`, `max_over_time({app="a"} | logfmt | unwrap d [5m]) by (app)`,
		`first_over_time({app="a"} | json | keep app, n | unwrap n [5m])`, `sum by (app) (bytes_over_time(({app="a"} |~ "x")[5m]))`,
		// The recorded divergences of log queries, inside a metric operand.
		"count_over_time({app=\"x\"} | s=`a detected_level b` [5m])",
		`sum(count_over_time({app="x"} | o="\x02\x00\x02" | json | K_extracted!~"p|q" [5m]))`,
		`sum(count_over_time({service_name="api-gateway"} | status > - .5 [5m]))`,
		`count_over_time({app="nginx"} | pattern "<_> <a><b" [5m])`,
		`count_over_time({app="a"} | json | not="x" [5m])`, `sum(count_over_time({app="a"} | n > +2 [5m]))`,
	} {
		f.Add(q)
	}
	newProxy := func(disable bool) *Proxy {
		p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(time.Minute, 100), LogLevel: "error",
			LabelStyle: LabelStyleUnderscores, DisableQueryPlanLowering: disable})
		if err != nil {
			f.Fatal(err)
		}
		f.Cleanup(func() { _ = p.Shutdown(context.Background()) })
		p.translationCache = nil
		return p
	}
	on, off := newProxy(false), newProxy(true)
	withFlipped(on, metricFlipCandidates...)
	f.Fuzz(func(t *testing.T, q string) {
		if len(q) > 512 || strings.HasPrefix(strings.TrimSpace(q), "{") {
			return
		}
		got, gotErr := on.translateQueryOpts(context.Background(), q, false)
		want, wantErr := off.translateQueryOpts(context.Background(), q, false)
		if _, kept := on.keepUnwrapIdentity(q); kept {
			return
		}
		if (got != want && got != lower.QuoteKeywordFieldText(want)) || (gotErr == nil) != (wantErr == nil) {
			t.Fatalf("%q:\n flag on  %q %v\n flag off %q %v", q, got, gotErr, want, wantErr)
		}
	})
}
