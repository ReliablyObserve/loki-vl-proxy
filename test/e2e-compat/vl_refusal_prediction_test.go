//go:build e2e

package e2e_compat

import (
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql/refusalcases"
)

// The refusals the proxy predicts (logsql.PredictRefusal) held to the stack's VictoriaLogs, the release the version
// matrix runs: every row of the refusal table predicted for the running release is refused by VictoriaLogs with the
// predicted status, and every row predicted to run is answered. A false prediction would refuse a query VictoriaLogs
// runs, so a release that answers otherwise fails here. The rows VictoriaLogs crashes on (a math pipe with a quoted
// constant) are never sent to the shared stack: the oracle test holds them (internal/proxy
// TestVLRefusalPredictionMatchesVictoriaLogs).
//
// conformance: vl-query-too-long, vl-pipe-before-release, vl-stats-missing-stats-pipe, vl-stats-range-pipe-before-stats, vl-stats-pipe-after-stats
func TestVLCapability_RefusalPredictionsMatchVictoriaLogs(t *testing.T) {
	release := stackVLRelease(t)
	v := logsql.ParseVersion(release)
	if !v.Known {
		t.Fatalf("no VictoriaLogs release in %s/metrics", vlURL)
	}
	predicted, sent := 0, 0
	for _, c := range refusalcases.Cases() {
		in := c.In
		if in.Path == logsql.PathTail || strings.Contains(in.Query, "| math") && strings.ContainsAny(in.Query, "\"'`") {
			continue // never a crash shape on the shared stack
		}
		in.MaxQueryLen = 16384 // the stack's VictoriaLogs runs with the default -search.maxQueryLen
		r, ok := logsql.PredictRefusal(in, v)
		if ok && r.Crash {
			continue
		}
		status, body := stackVLCall(t, in)
		sent++
		switch {
		case ok:
			predicted++
			if status != r.Status {
				t.Errorf("%s: predicted %d %q, VictoriaLogs %s answered %d %q", c.Name, r.Status, r.Message, release, status, body)
			}
		case status != http.StatusOK:
			if c.Release == release || c.Rule == "" {
				t.Errorf("%s: predicted to run, VictoriaLogs %s answered %d %q", c.Name, release, status, body)
			}
		}
	}
	t.Logf("VictoriaLogs %s: %d rows sent, %d predicted refusals", release, sent, predicted)
}

var stackVLVersionRe = regexp.MustCompile(`short_version="(v[0-9]+\.[0-9]+\.[0-9]+)`)

// stackVLRelease is the release of the stack's VictoriaLogs, from its /metrics.
func stackVLRelease(t *testing.T) string {
	t.Helper()
	resp, err := http.Get(vlURL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "vm_app_version{") {
			if m := stackVLVersionRe.FindStringSubmatch(line); m != nil {
				return m[1]
			}
		}
	}
	return ""
}

// stackVLCall sends a refusal table row to the stack's VictoriaLogs over a recent hour of tenant 0.
func stackVLCall(t *testing.T, in logsql.RefusalInput) (int, string) {
	t.Helper()
	end := time.Now().UTC()
	form := url.Values{"query": {in.Query}, "start": {end.Add(-time.Hour).Format(time.RFC3339)}, "end": {end.Format(time.RFC3339)}}
	switch in.Path {
	case logsql.PathStatsQueryRange:
		form.Set("step", "1m")
	case logsql.PathStatsQuery:
		form.Set("time", end.Format(time.RFC3339))
	}
	if in.Limit {
		form.Set("limit", "5")
	}
	for _, f := range in.ExtraFilters {
		form.Add("extra_filters", f)
	}
	for _, f := range in.ExtraStreamFilters {
		form.Add("extra_stream_filters", f)
	}
	req, err := http.NewRequest(http.MethodPost, vlURL+in.Path, strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("AccountID", "0")
	req.Header.Set("ProjectID", "0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("VictoriaLogs call: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return resp.StatusCode, string(body)
}
