package proxy

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/capability"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql/refusalcases"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// The refusal predictions held to VictoriaLogs itself: every row of the refusal table (logsql.RefusalCases) is sent
// to VictoriaLogs' own query handlers (test/oracle/vl serve) of the oldest supported release and of the newest one,
// and the prediction for that release must be VictoriaLogs' answer: a refusal with the same status, read the same
// way by the proxy's error classification; a dropped connection for a predicted crash; an answer for every row
// predicted to run. Each row then goes through the proxy's own call path twice, with prediction off (VictoriaLogs
// answers) and on (no call): the client gets the same answer byte for byte. Runs with LVP_RUN_ORACLES=1.

// oracleVLRelease is the VictoriaLogs release the oracle module builds with modfile ("" for its go.mod).
func oracleVLRelease(t *testing.T, dir, modfile string) string {
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

// cloneVerdict is the oracle's roundtrip verdict of a query: whether VictoriaLogs' parser accepts it and every clone
// of it reads back.
type cloneVerdict struct {
	Accept bool   `json:"accept"`
	OK     bool   `json:"ok"`
	Panic  string `json:"panic"`
}

// cloneVerdicts runs the VictoriaLogs oracle's roundtrip command (test/oracle/vl roundtrip.go: the handler's parse,
// its time filter and limit pipes, and every clone) over the queries of the cases that hold a math pipe.
func cloneVerdicts(t *testing.T, dir, modfile string, cases []refusalcases.Case) map[string]cloneVerdict {
	t.Helper()
	work := t.TempDir()
	in := filepath.Join(work, "in.jsonl")
	var b strings.Builder
	var queries []string
	for _, c := range cases {
		if strings.Contains(c.In.Query, "| math") {
			line, _ := json.Marshal(map[string]string{"id": strconv.Itoa(len(queries)), "q": c.In.Query})
			b.Write(line)
			b.WriteByte('\n')
			queries = append(queries, c.In.Query)
		}
	}
	if err := os.WriteFile(in, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	bin, err := oraclerun.Build(dir, modfile, work, "vl-oracle")
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(work, "out.jsonl")
	if err := oraclerun.Run(bin, "roundtrip", "-in", in, "-out", out); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	verdicts := map[string]cloneVerdict{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var v struct {
			ID string `json:"id"`
			cloneVerdict
		}
		if err := json.Unmarshal([]byte(line), &v); err != nil {
			t.Fatal(err)
		}
		i, _ := strconv.Atoi(v.ID)
		verdicts[queries[i]] = v.cloneVerdict
	}
	return verdicts
}

// vlOracleAnswer is VictoriaLogs' answer to a call: its status and raw message, or a dropped connection.
type vlOracleAnswer struct {
	status  int
	raw     string
	dropped bool
}

func sendToVL(t *testing.T, base string, in logsql.RefusalInput) vlOracleAnswer {
	t.Helper()
	form := url.Values{"query": {in.Query}, "start": {"2025-10-09T00:00:00Z"}, "end": {"2025-10-10T00:00:00Z"}}
	switch in.Path {
	case logsql.PathStatsQueryRange:
		form.Set("step", "1m")
	case logsql.PathStatsQuery:
		form.Set("time", "2025-10-10T00:00:00Z")
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
	resp, err := http.PostForm(base+in.Path, form)
	if err != nil {
		return vlOracleAnswer{dropped: true}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return vlOracleAnswer{dropped: true}
	}
	return vlOracleAnswer{status: resp.StatusCode, raw: extractVLErrorMsg(body)}
}

// conformance: vl-query-too-long, vl-extra-filters-too-long, vl-pipe-before-release, vl-stats-missing-stats-pipe, vl-stats-range-pipe-before-stats, vl-stats-pipe-after-stats, vl-math-quoted-constant-clone-crash
func TestVLRefusalPredictionMatchesVictoriaLogs(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("LVP_RUN_ORACLES=1 runs VictoriaLogs")
	}
	dir := filepath.Join("..", "..", "test", "oracle", "vl")
	rows := oraclerun.ServeDataSet{Tenant: 0, Rows: []oraclerun.ServeRow{
		{TS: 1760000000000000000, Stream: [][2]string{{"app", "a"}}, Fields: [][2]string{{"level", "info"}, {"_msg", "hello x=1"}}},
		{TS: 1760000010000000000, Stream: [][2]string{{"app", "a"}}, Fields: [][2]string{{"level", "error"}, {"_msg", "bye x=2"}}},
	}}
	cases := refusalcases.Cases()
	extra := `{app="` + strings.Repeat("a", 16400) + `"}`
	cases = append(cases,
		refusalcases.Case{Name: "extra_stream_filters over the length limit", In: logsql.RefusalInput{Path: logsql.PathStatsQueryRange, Query: `* | stats count() as c`, ExtraStreamFilters: []string{extra}}},
		refusalcases.Case{Name: "extra_filters over the length limit", In: logsql.RefusalInput{Path: logsql.PathQuery, Query: `*`, ExtraFilters: []string{`level:` + strings.Repeat("a", 16400)}}},
	)
	for _, modfile := range []string{"go.v1.49.0.mod", ""} {
		release := oracleVLRelease(t, dir, modfile)
		set := capability.ForRelease(capability.ParseRelease(release))
		t.Run(release, func(t *testing.T) {
			srv, err := oraclerun.StartVLServe(dir, modfile, []oraclerun.ServeDataSet{rows})
			if err != nil {
				t.Fatal(err)
			}
			defer srv.Stop()
			clones := cloneVerdicts(t, dir, modfile, cases)
			predicted, refused := 0, 0
			for _, c := range cases {
				in := c.In
				in.MaxQueryLen = 16384 // VictoriaLogs' default -search.maxQueryLen, which serve keeps
				got := sendToVL(t, srv.URL, in)
				r, ok := logsql.PredictRefusal(in, set)
				refusedByVL := got.dropped || isVLRefusalMessage(got.status, got.raw)
				if refusedByVL {
					refused++
				}
				switch {
				case ok && r.Crash:
					// VictoriaLogs' own clone (the oracle's roundtrip: the limit arg's last-N query, the time filter
					// clone, a cluster's remote query) must fail to read the query back. A call this server does not
					// clone may still be answered.
					if v := clones[c.In.Query]; !v.Accept || v.OK {
						t.Errorf("%s: predicted a crash, VictoriaLogs' clone %+v", c.Name, v)
					}
				case ok:
					if got.status != r.Status || !isVLRefusalMessage(got.status, got.raw) ||
						classifyVLError(got.status, got.raw) != classifyVLError(r.Status, r.Message) {
						t.Errorf("%s: predicted %d %q, VictoriaLogs answered %d %q", c.Name, r.Status, r.Message, got.status, got.raw)
					}
				case got.dropped || got.status != http.StatusOK:
					// Not predicted, and VictoriaLogs refused: a row the table expects a rule for on this release
					// is a missed prediction, a row it expects to run is a wrong table.
					if c.Release == release || c.Release == "" {
						t.Errorf("%s: not predicted (want %q), VictoriaLogs answered %d %q", c.Name, c.Rule, got.status, got.raw)
					}
				default:
					if c.Rule != "" && c.Release == release {
						t.Errorf("%s: want %q, VictoriaLogs answered %d", c.Name, c.Rule, got.status)
					}
				}
				if !ok || !r.Crash {
					if v, mathRow := clones[c.In.Query]; mathRow && v.Accept && !v.OK {
						t.Errorf("%s: VictoriaLogs' clone fails on a query not guarded: %+v", c.Name, v)
					}
				}
				if ok {
					predicted++
					if !r.Crash || got.dropped {
						checkProxyAnswerEqual(t, srv.URL, release, c.Name, in)
					}
				}
			}
			t.Logf("%s: %d rows, %d refused by VictoriaLogs, %d predicted", release, len(cases), refused, predicted)
		})
	}
}

// checkProxyAnswerEqual sends in through the proxy's call path with prediction off and on, over VictoriaLogs at
// base: off makes at least one call, on makes none, and the client answer is the same.
func checkProxyAnswerEqual(t *testing.T, base, release, name string, in logsql.RefusalInput) {
	t.Helper()
	var calls atomic.Int64
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		target, _ := url.Parse(base)
		target.Path, target.RawQuery = r.URL.Path, r.URL.RawQuery
		req, _ := http.NewRequestWithContext(r.Context(), r.Method, target.String(), r.Body)
		req.Header = r.Header.Clone()
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			hj, _ := w.(http.Hijacker)
			conn, _, _ := hj.Hijack()
			_ = conn.Close() // VictoriaLogs dropped it: so does the front
			return
		}
		defer resp.Body.Close()
		for k, vs := range resp.Header {
			w.Header()[k] = vs
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	defer front.Close()
	answer := func(predict bool) (string, int64) {
		before := calls.Load()
		p, _ := newFlightProxy(t, front.URL, func(cfg *Config) {
			cfg.VLRefusalPredictionDisabled = !predict
			cfg.CBFailThreshold = 1000
		})
		p.storeBackendVersion("oracle", release)
		if in.MaxQueryLen > 0 {
			p.vlMaxQueryLen.Store(&vlQueryLen{bytes: in.MaxQueryLen, release: p.caps.Backend().Digest(), until: time.Now().Add(time.Hour).UnixNano()})
		}
		params := url.Values{"query": {in.Query}, "start": {"2025-10-09T00:00:00Z"}, "end": {"2025-10-10T00:00:00Z"}}
		switch in.Path {
		case logsql.PathStatsQueryRange:
			params.Set("step", "1m")
		case logsql.PathStatsQuery:
			params.Set("time", "2025-10-10T00:00:00Z")
		}
		if in.Limit {
			params.Set("limit", "5")
		}
		if len(in.ExtraFilters) > 0 {
			params["extra_filters"] = in.ExtraFilters
		}
		if len(in.ExtraStreamFilters) > 0 {
			params["extra_stream_filters"] = in.ExtraStreamFilters
		}
		resp, err := p.vlPost(context.Background(), in.Path, params)
		if err == nil {
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode < http.StatusBadRequest {
				t.Fatalf("%s: answered %d", name, resp.StatusCode)
			}
			err = p.redactedBackendStatusError("", resp.StatusCode, body)
		}
		rec := httptest.NewRecorder()
		p.fail(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range", nil), err)
		return rec.Result().Status + "\n" + rec.Header().Get("Content-Type") + "\n" + rec.Body.String(), calls.Load() - before
	}
	off, offCalls := answer(false)
	on, onCalls := answer(true)
	if offCalls == 0 || onCalls != 0 || off != on {
		t.Errorf("%s: prediction off %d calls %q, on %d calls %q", name, offCalls, off, onCalls, on)
	}
}
