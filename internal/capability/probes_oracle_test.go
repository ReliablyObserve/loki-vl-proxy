package capability_test

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/capability"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// Every feature probe is held to VictoriaLogs itself: it is sent to the VictoriaLogs query handlers
// (test/oracle/vl serve) of the oldest supported release and of the release the oracle module pins, VictoriaLogs
// must answer it (the process does not exit, the connection is not dropped), and the answer must classify as the
// verdict the gate's release table says. The probes are the only shapes the proxy sends for no client, so none may
// be one that crashes a release (the math quoted-constant shape, which crashes v1.49.0 and v1.52.0, can never be
// one). Runs with LVP_RUN_ORACLES=1; the module cache must be private (GOMODCACHE, GOPRIVATE=github.com/VictoriaMetrics/VictoriaLogs).
//
// conformance: vl-capability-layer, pipe-coalesce, vl-stats-pipe-after-stats, vl-stats-range-pipe-before-stats, stream-metadata-endpoints
func TestProbesCrashSafe(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("LVP_RUN_ORACLES=1 runs VictoriaLogs")
	}
	dir := filepath.Join("..", "..", "test", "oracle", "vl")
	rows := oraclerun.ServeDataSet{Tenant: 0, Rows: []oraclerun.ServeRow{
		{TS: 1760000000000000000, Stream: [][2]string{{"app", "a"}}, Fields: [][2]string{{"level", "info"}, {"_msg", "hello x=1"}}},
	}}
	for _, modfile := range []string{"go.v1.49.0.mod", ""} {
		release := oracleRelease(t, dir, modfile)
		rel := capability.ParseRelease(release)
		t.Run(release, func(t *testing.T) {
			srv, err := oraclerun.StartVLServe(dir, modfile, []oraclerun.ServeDataSet{rows})
			if err != nil {
				t.Fatal(err)
			}
			defer srv.Stop()
			tested := 0
			for _, g := range capability.Gates() {
				p := g.Probe
				if p == nil {
					continue
				}
				tested++
				if p.CrashSafe[0] != "v1.49.0" {
					t.Errorf("%s: the probe must name the floor among the releases it was proven on: %v", g.Name, p.CrashSafe)
				}
				req, _ := http.NewRequest(http.MethodGet, srv.URL+p.Path+"?"+p.Values().Encode(), nil)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Errorf("%s on %s: VictoriaLogs dropped the connection (a crash): %v", g.Name, release, err)
					continue
				}
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				want := capability.VerdictOff
				if g.In(rel) {
					want = capability.VerdictOn
				}
				// A Behaviour probe's accepted answer means the gate is off: the verdict is the gate's, not the HTTP outcome.
				got := p.Classify(resp.StatusCode, body)
				t.Logf("%s on %s: %d %q", g.Name, release, resp.StatusCode, body)
				if got != want {
					t.Errorf("%s on %s: answered %d %q, probe says %v, the release table says %v", g.Name, release, resp.StatusCode, body, got, want)
				}
			}
			if tested < 4 {
				t.Errorf("only %d probes tested", tested)
			}
			// VictoriaLogs is still serving after every probe.
			resp, err := http.Get(srv.URL + "/select/logsql/query?query=*&limit=1")
			if err != nil {
				t.Fatalf("VictoriaLogs stopped answering after the probes: %v", err)
			}
			resp.Body.Close()
		})
	}
}

func oracleRelease(t *testing.T, dir, modfile string) string {
	t.Helper()
	name := "go.mod"
	if modfile != "" {
		name = modfile
	}
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`github\.com/VictoriaMetrics/VictoriaLogs => github\.com/VictoriaMetrics/VictoriaLogs (v[0-9.]+)`).FindSubmatch(data)
	if m == nil {
		t.Fatalf("no VictoriaLogs release in %s", name)
	}
	return string(m[1])
}
