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
