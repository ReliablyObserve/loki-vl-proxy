//go:build e2e

package e2e_compat

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// lokiPatternAnswersPath holds Loki's patterns for the fixture lines, read by the miner's unit test
// (internal/proxy TestPatternMinerMatchesLokisRecordedPatterns). LVP_RECORD_LOKI_PATTERNS=1 records it again.
const lokiPatternAnswersPath = "../../internal/proxy/testdata/loki_pattern_answers.json.gz"

// patternOracleStream is one stream of fixture lines: a kind of line, a pod and a level, each line at an offset
// before the end of the range.
type patternOracleStream struct {
	Kind      string   `json:"kind"`
	Pod       string   `json:"pod"`
	Level     string   `json:"level"`
	Lines     []string `json:"lines"`
	OffsetsMs []int64  `json:"offsets_ms"`
}

// patternOracleQuery is a /patterns request over some of the streams, from FromMs before the end of the range to
// its end, and Loki's answer.
type patternOracleQuery struct {
	Name    string                `json:"name"`
	Query   string                `json:"query"`
	Streams []int                 `json:"streams"`
	FromMs  int64                 `json:"from_ms"`
	Loki    []lokiRecordedPattern `json:"loki"`
}

type patternOracleAnswers struct {
	// EndMs is the end of every request's range, in milliseconds since the epoch.
	EndMs   int64                 `json:"end_ms"`
	Streams []patternOracleStream `json:"streams"`
	Queries []patternOracleQuery  `json:"queries"`
}

type lokiRecordedPattern struct {
	Pattern string `json:"pattern"`
	Total   int64  `json:"total"`
}

// patternOracleLinesPerStream is how many lines each stream holds, one every 500 ms.
const patternOracleLinesPerStream = 800

// patternOracleStreams is the fixture, the same on every run: the kinds of lines services write, with values that
// vary, and lines at the edges of how Loki mines patterns, few enough clusters per stream for Loki to keep every
// one (it caps a stream at 300 and stops mining a stream that evicts too many).
func patternOracleStreams() []patternOracleStream {
	r := rand.New(rand.NewSource(20261106))
	pick := func(values ...string) string { return values[r.Intn(len(values))] }
	ip := func() string { return fmt.Sprintf("10.%d.%d.%d", r.Intn(4), r.Intn(250), r.Intn(250)) }
	hexOf := func(n int, digits string) string {
		b := make([]byte, n)
		for i := range b {
			b[i] = digits[r.Intn(len(digits))]
		}
		return string(b)
	}
	hex := func(n int) string { return hexOf(n, "0123456789abcdef") }
	clock := func() string { return fmt.Sprintf("10:%02d:%02d.%03d", r.Intn(60), r.Intn(60), r.Intn(1000)) }
	agents := []string{"kube-probe/1.28", "curl/8.4.0", "Mozilla/5.0 (X11; Linux x86_64)", "Go-http-client/1.1", "sqlmap/1.7.8#stable (https://sqlmap.org)"}
	path := func() string {
		return pick("/api/v1/cart", "/api/v1/users/"+strconv.Itoa(r.Intn(900)), "/healthz", "/static/app."+hex(6)+".js", "/login")
	}
	words := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliet", "kilo", "lima", "mike", "november", "oscar", "papa", "quebec", "romeo", "sierra", "tango", "uniform", "victor", "whiskey", "xray", "yankee", "zulu"}
	kinds := []struct {
		kind, level string
		pods        []string
		line        func(pod string) string
	}{
		{"nginx", "info", nil, func(string) string {
			return fmt.Sprintf(`%s - %s [06/Nov/2026:10:%02d:%02d +0000] "%s %s HTTP/1.1" %s %d "-" "%s" %d`, ip(), pick("-", "-", "admin"), r.Intn(60), r.Intn(60),
				pick("GET", "GET", "POST", "PUT", "DELETE"), path(), pick("200", "200", "201", "304", "404", "500"), r.Intn(90000), pick(agents...), r.Intn(900))
		}},
		{"apache", "info", nil, func(string) string {
			return fmt.Sprintf(`%s - - [06/Nov/2026:10:%02d:%02d +0000] "%s %s HTTP/1.0" %s %d`, ip(), r.Intn(60), r.Intn(60), pick("GET", "HEAD"), path(), pick("200", "301", "404"), r.Intn(9000))
		}},
		{"envoy", "info", nil, func(string) string {
			return fmt.Sprintf(`[2026-11-06T%sZ] "%s %s HTTP/2" %s - %d %d %d %d "%s" "%s" "%s-%s-%s" "shop.local" "%s:8080"`, clock(), pick("GET", "POST"), path(),
				pick("200", "503"), r.Intn(900), r.Intn(90000), r.Intn(900), r.Intn(900), ip(), pick(agents[:2]...), hex(8), hex(4), hex(12), ip())
		}},
		{"logfmt", "info", nil, func(string) string {
			switch r.Intn(3) {
			case 0:
				return fmt.Sprintf(`level=info msg="cache hit" key=user:%s ttl=%ds size_bytes=%d op=get`, hex(6), r.Intn(90000), r.Intn(60000))
			case 1:
				return fmt.Sprintf(`level=info msg="cache write" key=session:%s op=set ttl=%ds size_bytes=%d`, hex(6), r.Intn(90000), r.Intn(60000))
			}
			return fmt.Sprintf(`level=info msg="cache miss" key=%s:%s op=get`, pick("user", "rate_limit", "cache:query"), hex(6))
		}},
		{"logfmt_mixed", "info", nil, func(string) string {
			switch r.Intn(4) {
			case 0:
				return fmt.Sprintf(`ts=2026-11-06T%sZ level=info caller=%s.go:%d msg="request served" path=%s status=%s duration=%dms`, clock(), pick("server", "handler"), r.Intn(400), path(), pick("200", "404"), r.Intn(900))
			case 1:
				return fmt.Sprintf(`time=2026-11-06T%sZ traceID=%s level=info msg="%s"`, clock(), hex(16), pick("queue drained", "batch flushed to disk", "leader elected"))
			case 2:
				return fmt.Sprintf(`level=info component=%s msg="sync done" items=%d errors=%d`, pick("indexer", "compactor", "ruler"), r.Intn(5000), r.Intn(3))
			}
			return fmt.Sprintf(`level=info msg="user %s logged in from %s" user=%s`, pick("alice", "bob", "carol"), ip(), hex(6))
		}},
		{"postgres", "info", nil, func(string) string {
			switch r.Intn(3) {
			case 0:
				return fmt.Sprintf(`2026-11-06 %s UTC [%d] LOG:  duration: %d.%03d ms  statement: SELECT * FROM %s WHERE id = $1`, clock(), 1000+r.Intn(9000), r.Intn(900), r.Intn(1000), pick("orders", "users", "payments", "sessions"))
			case 1:
				return fmt.Sprintf(`2026-11-06 %s UTC [%d] LOG:  connection authorized: user=%s database=%s`, clock(), 1000+r.Intn(9000), pick("app", "replicator", "reporting"), pick("shop", "billing"))
			}
			return fmt.Sprintf(`2026-11-06 %s UTC [%d] LOG:  checkpoint complete: wrote %d buffers (%d.%d%%); distance=%d kB`, clock(), 1000+r.Intn(9000), r.Intn(5000), r.Intn(10), r.Intn(10), r.Intn(90000))
		}},
		{"plain", "info", nil, func(string) string {
			switch r.Intn(3) {
			case 0:
				return fmt.Sprintf("worker %d processed %d items in %dms", r.Intn(8), r.Intn(5000), r.Intn(900))
			case 1:
				return fmt.Sprintf("connection from %s:%d accepted for user %s", ip(), 1024+r.Intn(60000), pick("api", "auth", "cache", "db"))
			}
			return fmt.Sprintf("job %s-%s failed: timeout after %ds, retrying in %ds", pick("export", "import", "reindex"), hex(8), r.Intn(120), r.Intn(30))
		}},
		{"klog", "info", nil, func(string) string {
			return fmt.Sprintf(`I1106 %s%03d       1 %s.go:%d] "%s" pod="%s/%s-%s" duration="%dms"`, clock(), r.Intn(1000), pick("controller", "reconciler"), r.Intn(500),
				pick("Reconciling pod", "Syncing endpoints", "Updated status"), pick("default", "shop"), pick("web", "api"), hex(5), r.Intn(900))
		}},
		{"syslog", "info", nil, func(string) string {
			return fmt.Sprintf(`Nov  6 10:%02d:%02d host-%d sshd[%d]: %s %s from %s port %d ssh2`, r.Intn(60), r.Intn(60), r.Intn(3), 1000+r.Intn(9000),
				pick("Accepted publickey for", "Failed password for", "Accepted password for"), pick("root", "deploy", "git"), ip(), 1024+r.Intn(60000))
		}},
		{"log4j", "info", nil, func(string) string {
			return fmt.Sprintf(`2026-11-06 10:%02d:%02d,%03d INFO  [%s] com.example.%s - %s %d %s customer %d`, r.Intn(60), r.Intn(60), r.Intn(1000), pick("main", "pool-1-thread-3", "http-nio-8080-exec-7"),
				pick("OrderService", "BillingService"), pick("Order", "Invoice"), r.Intn(90000), pick("created for", "cancelled by"), r.Intn(90000))
		}},
		{"zap_tabs", "info", nil, func(string) string {
			return fmt.Sprintf("2026-11-06T%sZ\tINFO\t%s/handler.go:%d\t%s\t{\"path\": \"%s\", \"status\": %s}", clock(), pick("server", "api"), r.Intn(200), pick("request served", "request failed"), path(), pick("200", "500"))
		}},
		{"python", "info", nil, func(string) string {
			return fmt.Sprintf("INFO:%s:Processed batch %d of %d (%d.%d%%) in %d.%ds", pick("root", "worker.tasks"), r.Intn(40), 40, r.Intn(100), r.Intn(10), r.Intn(60), r.Intn(10))
		}},
		{"json", "info", nil, func(string) string {
			if r.Intn(2) == 0 {
				return fmt.Sprintf(`{"message":"order %d shipped to %s via %s","level":"info","order_id":%d}`, r.Intn(90000), pick("Berlin", "Lisbon", "Oslo", "Austin"), pick("dhl", "ups", "fedex"), r.Intn(90000))
			}
			return fmt.Sprintf(`{"msg":"payment %s for user %s took %dms","level":"info"}`, pick("settled", "declined", "refunded"), hex(6), r.Intn(900))
		}},
		{"json_fields", "info", nil, func(string) string {
			switch r.Intn(5) {
			case 0:
				return fmt.Sprintf(`{"log":"GET %s returned %s in %dms\n","stream":"stdout"}`, path(), pick("200", "404"), r.Intn(900))
			case 1:
				return fmt.Sprintf(`{"content":"export %s finished with %d rows","level":"info"}`, pick("daily", "weekly"), r.Intn(90000))
			case 2:
				return fmt.Sprintf(`{"data":{"message":"nested message %d is not a top level field"},"level":"info"}`, r.Intn(900))
			case 3:
				return fmt.Sprintf(`{"service":{"name":"batch-etl"},"job":"export-orders","total":%d,"level":"info"}`, r.Intn(90000))
			}
			return fmt.Sprintf(`{"msg":%d,"message":"numeric msg field skipped for message %s now","level":"info"}`, r.Intn(900), pick("one", "two"))
		}},
		{"many_prefixes", "info", nil, func(string) string {
			return fmt.Sprintf("%s service started successfully in %dms on node %s", pick(words...), r.Intn(900), pick("east", "west"))
		}},
		{"hex_ids", "info", nil, func(string) string {
			return fmt.Sprintf("request %s handled by %s for tenant %s", hexOf(8, "abcdef"), pick("frontend", "backend"), hexOf(6, "abcdef"))
		}},
		{"last_token", "info", nil, func(string) string {
			return fmt.Sprintf("cache warmed for region %s with status %s", pick("eu", "us", "ap"), pick("ok", "partial", "stale", "cold", "hot", "warm", "fresh", "expired"))
		}},
		{"short_long", "info", nil, func(string) string {
			switch r.Intn(4) {
			case 0:
				return pick("ok", "done", "ready")
			case 1:
				return fmt.Sprintf("tick %d", r.Intn(900))
			case 2:
				var b strings.Builder
				for i := 0; i < 90; i++ {
					fmt.Fprintf(&b, "f%d ", r.Intn(9))
				}
				return strings.TrimSpace(b.String())
			}
			return fmt.Sprintf("snapshot %d written to bucket %s", r.Intn(900), pick("cold", "warm"))
		}},
		{"unicode", "info", nil, func(string) string {
			return fmt.Sprintf("użytkownik %d zalogował się z %s — sesja %s ✓", r.Intn(900), pick("Kraków", "Gdańsk", "Łódź"), hex(6))
		}},
		{"double_spaces", "info", nil, func(string) string {
			return fmt.Sprintf("WARN  [%s]  queue depth %d  above  limit %d", pick("ingest", "export"), r.Intn(9000), 5000)
		}},
		{"errors", "error", nil, func(string) string {
			return fmt.Sprintf("panic recovered in %s: runtime error: index out of range [%d] with length %d", pick("handler.ServeHTTP", "worker.Run"), r.Intn(9), r.Intn(9))
		}},
		{"pods_differ", "info", []string{"pod-a", "pod-b"}, func(pod string) string {
			// The pods differ only in their last token, which Loki mines apart per stream and would merge as one.
			currency := map[string]string{"pod-a": "eur", "pod-b": "usd"}[pod]
			return fmt.Sprintf("payment captured for order %d amount %d currency %s", r.Intn(90000), r.Intn(900), currency)
		}},
		{"multi", "info", []string{"pod-a", "pod-b", "pod-c"}, func(pod string) string {
			switch pod {
			case "pod-a":
				return fmt.Sprintf("checkout %s completed for cart %d", pick("card", "paypal"), r.Intn(9000))
			case "pod-b":
				return fmt.Sprintf("checkout %s completed for cart %d", pick("card", "voucher", "invoice"), r.Intn(9000))
			}
			return fmt.Sprintf("checkout %s failed for cart %d", pick("card", "paypal"), r.Intn(9000))
		}},
	}
	var streams []patternOracleStream
	for _, k := range kinds {
		pods := k.pods
		if pods == nil {
			pods = []string{"pod-a"}
		}
		for _, pod := range pods {
			s := patternOracleStream{Kind: k.kind, Pod: pod, Level: k.level}
			for i := 0; i < patternOracleLinesPerStream; i++ {
				s.Lines = append(s.Lines, k.line(pod))
				s.OffsetsMs = append(s.OffsetsMs, -int64(patternOracleLinesPerStream-i)*500)
			}
			streams = append(streams, s)
		}
	}
	return streams
}

// patternOracleQueries is the /patterns requests asked of both backends: each stream alone; every stream of the
// service; a regexp selector over two kinds; the three pods of one service, mined apart by Loki; an error-level
// stream; and the last part of the range alone.
func patternOracleQueries(service string, streams []patternOracleStream) []patternOracleQuery {
	var queries []patternOracleQuery
	byKind := map[string][]int{}
	all := make([]int, len(streams))
	for i, s := range streams {
		all[i] = i
		byKind[s.Kind] = append(byKind[s.Kind], i)
	}
	seen := map[string]bool{}
	for _, s := range streams {
		if seen[s.Kind] {
			continue
		}
		seen[s.Kind] = true
		queries = append(queries, patternOracleQuery{Name: s.Kind, Query: fmt.Sprintf(`{service_name=%q, kind=%q}`, service, s.Kind), Streams: byKind[s.Kind], FromMs: -patternOracleLinesPerStream * 500})
	}
	queries = append(queries,
		patternOracleQuery{Name: "service", Query: fmt.Sprintf(`{service_name=%q}`, service), Streams: all, FromMs: -patternOracleLinesPerStream * 500},
		patternOracleQuery{Name: "regexp_kinds", Query: fmt.Sprintf(`{service_name=%q, kind=~"nginx|plain"}`, service), Streams: append(append([]int{}, byKind["nginx"]...), byKind["plain"]...), FromMs: -patternOracleLinesPerStream * 500},
		patternOracleQuery{Name: "pod_b_only", Query: fmt.Sprintf(`{service_name=%q, kind="multi", pod="pod-b"}`, service), Streams: byKind["multi"][1:2], FromMs: -patternOracleLinesPerStream * 500},
		patternOracleQuery{Name: "nginx_last_quarter", Query: fmt.Sprintf(`{service_name=%q, kind="nginx"}`, service), Streams: byKind["nginx"], FromMs: -patternOracleLinesPerStream * 500 / 4},
	)
	return queries
}

// seedPatternOracle writes each stream to Loki and VictoriaLogs, in time order as Loki's pattern ingester reads
// them, ending at end.
func seedPatternOracle(t *testing.T, service string, streams []patternOracleStream, end time.Time) {
	t.Helper()
	for _, s := range streams {
		labels := map[string]string{"service_name": service, "app": service, "kind": s.Kind, "pod": s.Pod, "level": s.Level}
		for from := 0; from < len(s.Lines); from += 400 {
			to := min(from+400, len(s.Lines))
			var values [][]string
			var vl strings.Builder
			for i := from; i < to; i++ {
				ts := end.Add(time.Duration(s.OffsetsMs[i]) * time.Millisecond)
				values = append(values, []string{strconv.FormatInt(ts.UnixNano(), 10), s.Lines[i]})
				row := map[string]string{"_time": ts.Format(time.RFC3339Nano), "_msg": s.Lines[i]}
				for k, v := range labels {
					row[k] = v
				}
				encoded, _ := json.Marshal(row)
				vl.Write(encoded)
				vl.WriteByte('\n')
			}
			body, _ := json.Marshal(map[string]any{"streams": []map[string]any{{"stream": labels, "values": values}}})
			for _, push := range []struct{ url, contentType, body string }{
				{lokiURL + "/loki/api/v1/push", "application/json", string(body)},
				{vlURL + "/insert/jsonline?_stream_fields=" + url.QueryEscape("service_name,app,kind,pod,level"), "application/stream+json", vl.String()},
			} {
				resp, err := http.Post(push.url, push.contentType, strings.NewReader(push.body))
				if err != nil {
					t.Fatal(err)
				}
				msg, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode/100 != 2 {
					t.Fatalf("push to %s: status %d: %s", push.url, resp.StatusCode, msg)
				}
			}
		}
	}
}

// fetchRecordedPatterns is a backend's /patterns answer for a selector as pattern texts and their line totals.
func fetchRecordedPatterns(t *testing.T, base, query string, start, end time.Time) []lokiRecordedPattern {
	t.Helper()
	params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.Unix(), 10)}, "end": {strconv.FormatInt(end.Unix(), 10)}}
	req, _ := http.NewRequest(http.MethodGet, base+"/loki/api/v1/patterns?"+params.Encode(), nil)
	req.Header.Set("X-Scope-OrgID", "0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("patterns from %s: status %d: %s", base, resp.StatusCode, body)
	}
	var payload struct {
		Data []struct {
			Pattern string     `json:"pattern"`
			Samples [][2]int64 `json:"samples"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	out := make([]lokiRecordedPattern, 0, len(payload.Data))
	for _, e := range payload.Data {
		p := lokiRecordedPattern{Pattern: e.Pattern}
		for _, s := range e.Samples {
			p.Total += s[1]
		}
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Total != out[j].Total {
			return out[i].Total > out[j].Total
		}
		return out[i].Pattern < out[j].Pattern
	})
	return out
}

// patternAnswerMatch is patternTextShares and whether the answers are the same: the same texts with the same totals.
func patternAnswerMatch(loki, proxy []lokiRecordedPattern) (lines, precision float64, same bool) {
	same = len(loki) == len(proxy)
	totals := map[string]int64{}
	for _, p := range proxy {
		totals[p.Pattern] = p.Total
	}
	for _, l := range loki {
		if t, ok := totals[l.Pattern]; !ok || t != l.Total {
			same = false
		}
	}
	lines, precision = patternTextShares(loki, proxy)
	return lines, precision, same
}

// patternTextShares is how much of Loki's answer the proxy's has with the same text: the share of the lines in
// Loki's patterns that are in a pattern the proxy answers too, and the share of the proxy's patterns that are
// Loki's. When Loki has no pattern, the proxy matches by having none.
func patternTextShares(loki, proxy []lokiRecordedPattern) (lines, precision float64) {
	if len(loki) == 0 {
		if len(proxy) == 0 {
			return 1, 1
		}
		return 0, 0
	}
	have := map[string]bool{}
	for _, p := range proxy {
		have[p.Pattern] = true
	}
	lokiHas := map[string]bool{}
	var matchedLines, allLines int64
	for _, l := range loki {
		lokiHas[l.Pattern] = true
		allLines += l.Total
		if have[l.Pattern] {
			matchedLines += l.Total
		}
	}
	var ours int
	for _, p := range proxy {
		if lokiHas[p.Pattern] {
			ours++
		}
	}
	if len(proxy) > 0 {
		precision = float64(ours) / float64(len(proxy))
	}
	return float64(matchedLines) / float64(allLines), precision
}

// Loki's pattern ingester and the proxy mine the same lines into patterns: for kinds of lines services write and
// lines at the edges of Loki's mining, and for the request shapes Logs Drilldown and API clients send (one stream,
// a whole service, a regexp selector, one pod of several, the last part of the range), this reports how much of
// Loki's answer the proxy gives with the same pattern texts, and records Loki's answers for the miner's unit test,
// which holds the proxy to them without a stack.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns
func TestDrilldown_Patterns_TextsMatchLokisOnTheSameLines(t *testing.T) {
	waitForPatternABReady(t)
	streams := patternOracleStreams()
	service := fmt.Sprintf("pattern-oracle-%d", time.Now().UnixNano())
	end := time.Now().UTC().Truncate(time.Second)
	seedPatternOracle(t, service, streams, end)
	queries := patternOracleQueries(service, streams)

	var lokiLines, proxyLines float64
	exact := 0
	for i := range queries {
		q := &queries[i]
		start := end.Add(time.Duration(q.FromMs) * time.Millisecond)
		// Loki's pattern ingester answers a push after a few seconds; its answer settles once two reads agree.
		var loki []lokiRecordedPattern
		for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); time.Sleep(3 * time.Second) {
			next := fetchRecordedPatterns(t, lokiURL, q.Query, start, end)
			settled := fmt.Sprint(next) == fmt.Sprint(loki) && (len(next) > 0 || time.Now().After(deadline.Add(-60*time.Second)))
			loki = next
			if settled {
				break
			}
		}
		q.Loki = loki
		proxy := fetchRecordedPatterns(t, proxyURL, q.Query, start, end)
		lines, precision, same := patternAnswerMatch(loki, proxy)
		if same {
			exact++
		}
		lokiLines++
		proxyLines += lines
		var total int64
		for _, p := range loki {
			total += p.Total
		}
		t.Logf("%-18s Loki %3d patterns (%5d lines), proxy %3d: %5.1f%% of Loki's lines, %5.1f%% of the proxy's patterns are Loki's, same answer %v",
			q.Name, len(loki), total, len(proxy), 100*lines, 100*precision, same)
		if testing.Verbose() && lines < 1 {
			for _, p := range loki {
				t.Logf("    loki  %5d %s", p.Total, p.Pattern)
			}
			for _, p := range proxy {
				t.Logf("    proxy %5d %s", p.Total, p.Pattern)
			}
		}
	}
	t.Logf("mean over %d requests: %.1f%% of Loki's lines in patterns with Loki's text; %d answers the same as Loki's", len(queries), 100*proxyLines/lokiLines, exact)
	if os.Getenv("LVP_RECORD_LOKI_PATTERNS") != "1" {
		return
	}
	f, err := os.Create(lokiPatternAnswersPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zw := gzip.NewWriter(f)
	enc := json.NewEncoder(zw)
	enc.SetIndent("", " ")
	if err := enc.Encode(patternOracleAnswers{EndMs: end.UnixMilli(), Streams: streams, Queries: queries}); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("recorded %s", lokiPatternAnswersPath)
}
