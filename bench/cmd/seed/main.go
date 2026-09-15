// seed: ingest dense historical log data into both Loki and VictoriaLogs for benchmarking.
//
// Generates N days of realistic multi-service logs with production-like metadata,
// back-filling from (now - N days) to now. Both backends receive the same
// entries so loki-bench comparison runs do equivalent work on both sides:
//
//   - the log line bytes are identical: Loki stores the line pushed to
//     /loki/api/v1/push, VictoriaLogs stores the same bytes as `_msg` pushed to
//     /insert/jsonline (no ingest-time JSON parsing, no injected fields);
//   - the stream labels are identical and become the VictoriaLogs stream fields;
//   - the log level is attached identically: as structured metadata `level` in
//     Loki and as a regular (non-stream) `level` field in VictoriaLogs.
//
// Usage:
//
//	go run ./cmd/seed/ \
//	  --loki=http://localhost:13101 \
//	  --vl=http://localhost:19428 \
//	  --days=4 \
//	  --lines-per-batch=200 \
//	  --batch-interval=30s
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/dataspan"
)

// service describes one workload stream.
type service struct {
	app       string
	namespace string
	env       string
	region    string
	cluster   string
	version   string
	format    string // "json" | "logfmt" | "nginx" | "postgres"
}

var services = []service{
	{"api-gateway", "prod", "production", "us-east-1", "eks-prod-a", "v2.14.3", "json"},
	{"api-gateway", "prod", "production", "us-west-2", "eks-prod-b", "v2.14.3", "json"},
	{"payment-service", "prod", "production", "us-east-1", "eks-prod-a", "v1.8.0", "logfmt"},
	{"auth-service", "prod", "production", "us-east-1", "eks-prod-a", "v3.2.1", "json"},
	{"nginx-ingress", "ingress-nginx", "production", "us-east-1", "eks-prod-a", "1.9.4", "nginx"},
	{"worker-service", "prod", "production", "us-east-1", "eks-prod-a", "v0.9.2", "logfmt"},
	{"db-postgres", "data", "production", "us-east-1", "eks-data-a", "15.3", "postgres"},
	{"cache-redis", "data", "production", "us-east-1", "eks-data-a", "7.2.0", "logfmt"},
	{"frontend-ssr", "prod", "production", "us-east-1", "eks-prod-a", "v4.1.0", "json"},
	{"frontend-ssr", "prod", "production", "us-west-2", "eks-prod-b", "v4.1.0", "json"},
	{"batch-etl", "batch", "production", "us-east-1", "eks-batch-a", "v2.0.5", "json"},
	{"ml-serving", "ml", "production", "us-east-1", "eks-ml-a", "v1.3.0", "json"},
}

var levels = []string{"debug", "info", "info", "info", "info", "warn", "warn", "error"}

var httpMethods = []string{"GET", "GET", "GET", "GET", "POST", "POST", "PUT", "DELETE", "PATCH"}
var apiPaths = []string{
	"/api/v1/users", "/api/v1/users/{id}", "/api/v1/orders", "/api/v1/orders/{id}",
	"/api/v1/products", "/api/v2/events", "/api/v2/analytics", "/health", "/metrics",
	"/api/v1/payments", "/api/v1/sessions", "/api/v1/search",
}
var httpStatuses = []int{200, 200, 200, 200, 200, 201, 204, 400, 401, 403, 404, 429, 500, 502, 503}
var statusWeights = []int{40, 10, 5, 5, 5, 5, 2, 2, 2, 1, 5, 1, 1, 1, 1}

var workers = []string{"worker-0", "worker-1", "worker-2", "worker-3", "worker-4"}
var jobNames = []string{"process-orders", "sync-inventory", "send-emails", "cleanup-sessions", "update-metrics", "export-reports"}
var dbTables = []string{"users", "orders", "payments", "sessions", "products", "events", "logs"}
var cacheOps = []string{"GET", "SET", "DEL", "EXPIRE", "INCR", "HGET", "HSET", "ZADD", "ZRANGE"}
var mlModels = []string{"bert-large", "gpt-small", "clip-v2", "classification-v3", "embedding-v1"}
var etlJobs = []string{"raw-to-parquet", "daily-aggregation", "feature-extraction", "model-training", "data-export"}

var client = &http.Client{Timeout: 30 * time.Second}

// logLine is one generated log line plus the level it carries. The level is
// pushed alongside the line (Loki structured metadata / VictoriaLogs field) so
// both backends expose the same level information; it is empty for formats
// that have no level (nginx access logs).
type logLine struct {
	text  string
	level string
}

// entry is one timestamped log line of a stream.
type entry struct {
	ts   time.Time
	line logLine
}

// streamBatch is one stream (label set) with the entries pushed for it in one batch.
type streamBatch struct {
	labels  map[string]string
	entries []entry
}

func randID(n int) string {
	const charset = "abcdef0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func randPod(app string) string {
	return fmt.Sprintf("%s-%s-%s", app, randID(5), randID(4))
}

func randIP() string {
	return fmt.Sprintf("10.%d.%d.%d", rand.Intn(10), rand.Intn(254), rand.Intn(254)+1)
}

func weightedStatus() int {
	total := 0
	for _, w := range statusWeights {
		total += w
	}
	r := rand.Intn(total)
	for i, w := range statusWeights {
		r -= w
		if r < 0 {
			return httpStatuses[i]
		}
	}
	return 200
}

// jsonLine encodes a JSON log line. The encoded object is the complete line:
// no outer wrapper and no injected `_msg`, so both backends store the same bytes.
func jsonLine(fields map[string]interface{}, level string) logLine {
	b, _ := json.Marshal(fields)
	return logLine{text: string(b), level: level}
}

func genJSONLine(svc service, ts time.Time) logLine {
	level := levels[rand.Intn(len(levels))]
	method := httpMethods[rand.Intn(len(httpMethods))]
	path := apiPaths[rand.Intn(len(apiPaths))]
	status := weightedStatus()
	latency := rand.Intn(800) + 1
	if status >= 500 {
		latency += rand.Intn(3000)
	}
	traceID := randID(32)
	spanID := randID(16)
	userID := fmt.Sprintf("usr_%d", rand.Intn(100000))
	pod := randPod(svc.app)

	fields := map[string]interface{}{
		"level":      level,
		"msg":        fmt.Sprintf("%s %s %d %dms", method, path, status, latency),
		"ts":         ts.Format(time.RFC3339Nano),
		"method":     method,
		"path":       path,
		"status":     status,
		"latency_ms": latency,
		"trace_id":   traceID,
		"span_id":    spanID,
		"user_id":    userID,
		"pod":        pod,
		"service":    svc.app,
		"version":    svc.version,
		"region":     svc.region,
		"cluster":    svc.cluster,
	}
	if status >= 400 {
		errs := []string{"connection refused", "timeout", "invalid token", "rate limited", "upstream unavailable"}
		fields["error"] = errs[rand.Intn(len(errs))]
	}
	return jsonLine(fields, level)
}

func genLogfmtLine(svc service, ts time.Time) logLine {
	level := levels[rand.Intn(len(levels))]
	switch svc.app {
	case "worker-service":
		job := jobNames[rand.Intn(len(jobNames))]
		worker := workers[rand.Intn(len(workers))]
		dur := rand.Intn(30000) + 100
		queued := rand.Intn(500)
		return logLine{fmt.Sprintf(`level=%s ts=%s msg="job completed" job=%s worker=%s duration_ms=%d queued=%d trace_id=%s service=%s version=%s`,
			level, ts.Format(time.RFC3339), job, worker, dur, queued, randID(32), svc.app, svc.version), level}
	case "payment-service":
		methods := []string{"card", "bank_transfer", "crypto", "paypal"}
		payMethod := methods[rand.Intn(len(methods))]
		amount := rand.Intn(100000) + 100
		status := []string{"authorized", "authorized", "authorized", "declined", "pending"}
		s := status[rand.Intn(len(status))]
		return logLine{fmt.Sprintf(`level=%s ts=%s msg="payment processed" method=%s amount_cents=%d status=%s user_id=usr_%d trace_id=%s service=%s version=%s`,
			level, ts.Format(time.RFC3339), payMethod, amount, s, rand.Intn(100000), randID(32), svc.app, svc.version), level}
	case "cache-redis":
		op := cacheOps[rand.Intn(len(cacheOps))]
		key := fmt.Sprintf("cache:%s:%s", dbTables[rand.Intn(len(dbTables))], randID(8))
		latency := rand.Intn(50) + 1
		hit := rand.Intn(10) > 2
		return logLine{fmt.Sprintf(`level=%s ts=%s msg="cache op" op=%s key=%s hit=%v latency_us=%d service=%s version=%s`,
			level, ts.Format(time.RFC3339), op, key, hit, latency, svc.app, svc.version), level}
	case "db-postgres":
		return genPostgresLine(svc, ts)
	default:
		return logLine{fmt.Sprintf(`level=%s ts=%s msg="event" service=%s version=%s trace_id=%s`,
			level, ts.Format(time.RFC3339), svc.app, svc.version, randID(32)), level}
	}
}

func genPostgresLine(svc service, ts time.Time) logLine {
	ops := []string{"SELECT", "INSERT", "UPDATE", "DELETE", "VACUUM", "ANALYZE"}
	op := ops[rand.Intn(len(ops))]
	table := dbTables[rand.Intn(len(dbTables))]
	rows := rand.Intn(10000)
	dur := rand.Intn(5000) + 1
	level := "info"
	if dur > 3000 {
		level = "warn"
	}
	pid := rand.Intn(32768) + 1000
	return logLine{fmt.Sprintf(`level=%s ts=%s pid=%d op=%s table=%s rows=%d duration_ms=%d db=app_production service=%s`,
		level, ts.Format(time.RFC3339), pid, op, table, rows, dur, svc.app), level}
}

func genNginxLine(svc service, ts time.Time) logLine {
	method := httpMethods[rand.Intn(len(httpMethods))]
	path := apiPaths[rand.Intn(len(apiPaths))]
	status := weightedStatus()
	size := rand.Intn(50000) + 100
	reqTime := rand.Float64() * 2.0
	upstream := fmt.Sprintf("10.0.%d.%d:8080", rand.Intn(10), rand.Intn(254))
	// Access logs carry no level; nothing is attached as metadata.
	return logLine{text: fmt.Sprintf(`%s - - [%s] "%s %s HTTP/1.1" %d %d "https://app.example.com" "Mozilla/5.0 (compatible)" %.3f %s`,
		randIP(), ts.Format("02/Jan/2006:15:04:05 -0700"),
		method, path, status, size, reqTime, upstream)}
}

func genMLLine(svc service, ts time.Time) logLine {
	model := mlModels[rand.Intn(len(mlModels))]
	batchSize := []int{1, 8, 16, 32, 64}[rand.Intn(5)]
	latency := rand.Intn(2000) + 10
	tokens := rand.Intn(4096) + 1
	level := "info"
	if latency > 1500 {
		level = "warn"
	}
	fields := map[string]interface{}{
		"level":    level,
		"msg":      fmt.Sprintf("inference completed model=%s batch=%d latency=%dms tokens=%d", model, batchSize, latency, tokens),
		"ts":       ts.Format(time.RFC3339Nano),
		"model":    model,
		"batch":    batchSize,
		"latency":  latency,
		"tokens":   tokens,
		"trace_id": randID(32),
		"service":  svc.app,
		"version":  svc.version,
		"pod":      randPod(svc.app),
	}
	return jsonLine(fields, level)
}

func genETLLine(svc service, ts time.Time) logLine {
	job := etlJobs[rand.Intn(len(etlJobs))]
	records := rand.Intn(1000000) + 1000
	dur := rand.Intn(600) + 10
	level := "info"
	if dur > 300 {
		level = "warn"
	}
	fields := map[string]interface{}{
		"level":      level,
		"msg":        fmt.Sprintf("etl job %s completed: %d records in %ds", job, records, dur),
		"ts":         ts.Format(time.RFC3339Nano),
		"job":        job,
		"records":    records,
		"duration_s": dur,
		"source":     fmt.Sprintf("s3://data-lake/raw/%s", ts.Format("2006/01/02")),
		"dest":       fmt.Sprintf("s3://data-warehouse/%s/v1", job),
		"trace_id":   randID(32),
		"service":    svc.app,
		"version":    svc.version,
	}
	return jsonLine(fields, level)
}

func genLine(svc service, ts time.Time) logLine {
	switch svc.format {
	case "json":
		switch svc.app {
		case "ml-serving":
			return genMLLine(svc, ts)
		case "batch-etl":
			return genETLLine(svc, ts)
		default:
			return genJSONLine(svc, ts)
		}
	case "logfmt", "postgres":
		return genLogfmtLine(svc, ts)
	case "nginx":
		return genNginxLine(svc, ts)
	default:
		return logLine{fmt.Sprintf(`level=info ts=%s msg=event service=%s`, ts.Format(time.RFC3339), svc.app), "info"}
	}
}

// lokiPushBody encodes batches as a Loki push request. Entries with a level
// carry it as structured metadata (the third element of a value).
func lokiPushBody(batches []streamBatch) ([]byte, error) {
	streams := make([]map[string]interface{}, 0, len(batches))
	for _, b := range batches {
		values := make([]interface{}, 0, len(b.entries))
		for _, e := range b.entries {
			ts := fmt.Sprintf("%d", e.ts.UnixNano())
			if e.line.level != "" {
				values = append(values, []interface{}{ts, e.line.text, map[string]string{"level": e.line.level}})
			} else {
				values = append(values, []interface{}{ts, e.line.text})
			}
		}
		streams = append(streams, map[string]interface{}{"stream": b.labels, "values": values})
	}
	return json.Marshal(map[string]interface{}{"streams": streams})
}

// vlJSONLineBody encodes batches as VictoriaLogs /insert/jsonline rows: `_msg`
// is exactly the Loki line, `_time` the same timestamp, every stream label is a
// field (and a stream field), and the level is a regular field. It returns the
// sorted stream field names for the `_stream_fields` query arg.
func vlJSONLineBody(batches []streamBatch) ([]byte, []string, error) {
	fieldSet := map[string]struct{}{}
	var buf bytes.Buffer
	for _, b := range batches {
		for k := range b.labels {
			fieldSet[k] = struct{}{}
		}
		for _, e := range b.entries {
			row := make(map[string]string, len(b.labels)+3)
			for k, v := range b.labels {
				row[k] = v
			}
			if e.line.level != "" {
				row["level"] = e.line.level
			}
			row["_time"] = e.ts.UTC().Format(time.RFC3339Nano)
			row["_msg"] = e.line.text
			j, err := json.Marshal(row)
			if err != nil {
				return nil, nil, err
			}
			buf.Write(j)
			buf.WriteByte('\n')
		}
	}
	fields := make([]string, 0, len(fieldSet))
	for k := range fieldSet {
		fields = append(fields, k)
	}
	sort.Strings(fields)
	return buf.Bytes(), fields, nil
}

func pushLoki(lokiURL string, batches []streamBatch) error {
	body, err := lokiPushBody(batches)
	if err != nil {
		return err
	}
	return post(lokiURL+"/loki/api/v1/push", "application/json", body)
}

func pushVL(vlURL string, batches []streamBatch) error {
	body, streamFields, err := vlJSONLineBody(batches)
	if err != nil {
		return err
	}
	u := vlURL + "/insert/jsonline?_stream_fields=" + url.QueryEscape(strings.Join(streamFields, ","))
	return post(u, "application/stream+json", body)
}

func post(u, contentType string, body []byte) error {
	resp, err := client.Post(u, contentType, bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return nil
}

// buildActiveServices returns n services cycling through the built-in pool.
// Repeats beyond the pool get a distinct region and a cluster derived from the
// pool entry's own cluster, so every index has a unique label set (the pool's
// (app, cluster) pairs are unique, and the region/cluster suffix separates cycles).
func buildActiveServices(n int) []service {
	out := make([]service, n)
	for i := range out {
		base := services[i%len(services)]
		out[i] = base
		if cycle := i / len(services); cycle > 0 {
			out[i].region = fmt.Sprintf("eu-west-%d", cycle+1)
			out[i].cluster = fmt.Sprintf("%s-eu-%d", base.cluster, cycle+1)
		}
	}
	return out
}

// streamLabels returns the Loki stream labels (VictoriaLogs stream fields) of a service.
func streamLabels(svc service) map[string]string {
	return map[string]string{
		"app":        svc.app,
		"namespace":  svc.namespace,
		"job":        svc.namespace + "/" + svc.app,
		"env":        svc.env,
		"region":     svc.region,
		"cluster":    svc.cluster,
		"version":    svc.version,
		"log_format": svc.format,
	}
}

// seedConfig holds the seed flags.
type seedConfig struct {
	lokiURL, vlURL  string
	days            int
	serviceCount    int
	ratePerSvc      float64
	linesPerBatch   int
	batchInterval   time.Duration
	skipLoki        bool
	skipVL          bool
	highCardinality bool
	podsPerService  int
	force           bool
	// readyTimeout keeps retrying the existing-data check while a backend is
	// still starting (connection refused or HTTP 5xx).
	readyTimeout time.Duration
	// now is the seed's wall clock; tests set it.
	now time.Time
	// span overrides days×24h when set (tests seed short spans).
	span time.Duration
}

func main() {
	var cfg seedConfig
	flag.StringVar(&cfg.lokiURL, "loki", "http://localhost:13101", "Loki push URL")
	flag.StringVar(&cfg.vlURL, "vl", "http://localhost:19428", "VictoriaLogs push URL")
	flag.IntVar(&cfg.days, "days", 4, "Days of historical data to seed (4 covers the longest workload window; keep at 6 or below)")
	flag.IntVar(&cfg.serviceCount, "services", 12, "Number of service streams (cycles through built-in pool; repeats get distinct region/cluster labels)")
	flag.Float64Var(&cfg.ratePerSvc, "rate", 0, "Target log lines/sec per service (overrides --lines-per-batch when set)")
	flag.IntVar(&cfg.linesPerBatch, "lines-per-batch", 21, "Lines per service per time step (ignored when --rate is set)")
	flag.DurationVar(&cfg.batchInterval, "batch-interval", 30*time.Second, "Simulated time step between push batches")
	flag.BoolVar(&cfg.skipLoki, "skip-loki", false, "Skip Loki ingestion")
	flag.BoolVar(&cfg.skipVL, "skip-vl", false, "Skip VictoriaLogs ingestion")
	flag.BoolVar(&cfg.highCardinality, "high-cardinality", false, "Add pod as a stream label with --pods-per-service unique pod IDs per service. Creates N×services unique Loki streams, exposing Loki O(streams×retention) memory pressure vs VL columnar model.")
	flag.IntVar(&cfg.podsPerService, "pods-per-service", 50, "Number of unique pod IDs per service when --high-cardinality is set")
	flag.BoolVar(&cfg.force, "force", false, "Seed even when a backend already holds data in the target span or streams with the seeded app labels (the result will not be comparable unless both backends hold the same prior data)")
	flag.DurationVar(&cfg.readyTimeout, "ready-timeout", 3*time.Minute, "How long to retry the existing-data check while Loki or VictoriaLogs is still starting")
	flag.Parse()
	cfg.now = time.Now()
	if err := run(context.Background(), cfg, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "seed: %v\n", err)
		os.Exit(1)
	}
}

// run seeds both backends. It refuses to start when a backend already holds
// data (unless cfg.force) and stops at the first failed push: a partial seed
// leaves the backends with different data, and retrying a push can duplicate
// rows in VictoriaLogs, so the only safe recovery is a re-seed on a fresh stack.
func run(ctx context.Context, cfg seedConfig, out io.Writer) error {
	// When --rate is given, derive lines-per-batch from the target rate.
	if cfg.ratePerSvc > 0 {
		computed := int(math.Round(cfg.ratePerSvc * cfg.batchInterval.Seconds()))
		if computed < 1 {
			computed = 1
		}
		cfg.linesPerBatch = computed
	}

	activeServices := buildActiveServices(cfg.serviceCount)

	// Build pod pools for high-cardinality mode: N unique pod IDs per service.
	// Each batch rotates through the pod pool, creating N×services unique Loki streams.
	var podPool map[string][]string
	if cfg.highCardinality {
		podPool = make(map[string][]string, len(activeServices))
		for _, svc := range activeServices {
			key := podPoolKey(svc)
			if _, ok := podPool[key]; ok {
				continue
			}
			pods := make([]string, cfg.podsPerService)
			for i := range pods {
				pods[i] = fmt.Sprintf("%s-%s-%s", svc.app, randID(5), randID(4))
			}
			podPool[key] = pods
		}
	}

	span := cfg.span
	if span == 0 {
		span = time.Duration(cfg.days) * 24 * time.Hour
	}
	end := cfg.now.Add(-time.Minute)
	start := end.Add(-span)

	if err := checkBackendsEmpty(ctx, cfg, start, end, activeServices, out); err != nil {
		return err
	}

	totalBatches := int(end.Sub(start) / cfg.batchInterval)
	// In high-cardinality mode each service fans out into podsPerService streams per batch.
	streamsPerBatch := len(activeServices)
	uniqueStreams := len(activeServices)
	if cfg.highCardinality {
		uniqueStreams = len(activeServices) * cfg.podsPerService
	}
	totalLines := totalBatches * cfg.linesPerBatch * streamsPerBatch
	linesPerHour := float64(cfg.linesPerBatch) * float64(streamsPerBatch) * (3600.0 / cfg.batchInterval.Seconds())
	linesPerMin := linesPerHour / 60.0
	effectiveRate := float64(cfg.linesPerBatch) / cfg.batchInterval.Seconds()

	fmt.Fprintf(out, "═══════════════════════════════════════════════════════════\n")
	fmt.Fprintf(out, " Seed Configuration\n")
	fmt.Fprintf(out, "═══════════════════════════════════════════════════════════\n")
	fmt.Fprintf(out, "  Period:        %s  (%s → %s)\n", span, start.Format("2006-01-02 15:04"), end.Format("2006-01-02 15:04"))
	fmt.Fprintf(out, "  Services:      %d base streams\n", len(activeServices))
	if cfg.highCardinality {
		fmt.Fprintf(out, "  High-cardinality: %d pods/service → %d unique Loki streams\n", cfg.podsPerService, uniqueStreams)
	}
	fmt.Fprintf(out, "  Rate/service:  %.1f lines/sec  (~%.0f lines/min per service)\n",
		effectiveRate, effectiveRate*60)
	fmt.Fprintf(out, "  Total rate:    %.0f lines/sec  (~%.0f/min  ~%.0f/hr)\n",
		effectiveRate*float64(streamsPerBatch), linesPerMin, linesPerHour)
	fmt.Fprintf(out, "  Total volume:  ~%s lines  (%d batches × %d lines × %d streams)\n",
		fmtCount(totalLines), totalBatches, cfg.linesPerBatch, streamsPerBatch)
	fmt.Fprintf(out, "  Targets:       loki=%v  vl=%v\n", !cfg.skipLoki, !cfg.skipVL)
	fmt.Fprintf(out, "═══════════════════════════════════════════════════════════\n\n")

	startWall := time.Now()
	var pushed, batchesOK int
	var lastTS time.Time
	reportEvery := totalBatches / 20
	if reportEvery < 1 {
		reportEvery = 1
	}

	batchNum := 0
	for ts := start; ts.Before(end); ts = ts.Add(cfg.batchInterval) {
		var batches []streamBatch
		if cfg.highCardinality {
			batches = buildStreamsHighCardinality(ts, cfg.linesPerBatch, activeServices, podPool, batchNum)
		} else {
			batches = buildStreamsFor(ts, cfg.linesPerBatch, activeServices)
		}
		batchNum++
		for _, b := range batches {
			if n := len(b.entries); n > 0 && b.entries[n-1].ts.After(lastTS) {
				lastTS = b.entries[n-1].ts
			}
		}

		if !cfg.skipLoki {
			if err := pushLoki(cfg.lokiURL, batches); err != nil {
				return pushError("Loki", ts, pushed, err)
			}
		}
		if !cfg.skipVL {
			if err := pushVL(cfg.vlURL, batches); err != nil {
				return pushError("VictoriaLogs", ts, pushed, err)
			}
		}

		pushed += cfg.linesPerBatch * len(activeServices)
		batchesOK++

		if batchesOK%reportEvery == 0 {
			elapsed := time.Since(startWall)
			wallRate := float64(pushed) / elapsed.Seconds()
			eta := time.Duration(float64(totalLines-pushed)/wallRate) * time.Second
			pct := float64(ts.Sub(start)) / float64(end.Sub(start)) * 100
			fmt.Fprintf(out, "  %.0f%% — %s — %s lines pushed  %.0f lines/s wall  ETA %s\n",
				pct, ts.Format("2006-01-02 15:04"),
				fmtCount(pushed), wallRate, eta.Truncate(time.Second))
		}
	}

	elapsed := time.Since(startWall)
	fmt.Fprintf(out, "\n═══════════════════════════════════════════════════════════\n")
	fmt.Fprintf(out, " Seed Complete\n")
	fmt.Fprintf(out, "═══════════════════════════════════════════════════════════\n")
	fmt.Fprintf(out, "  Lines pushed:  %s across %d batches\n", fmtCount(pushed), batchesOK)
	fmt.Fprintf(out, "  Wall time:     %s  (%.0f lines/s effective)\n",
		elapsed.Truncate(time.Second), float64(pushed)/elapsed.Seconds())
	fmt.Fprintf(out, "  Data shape:    %d services  %.1f lines/sec/svc  %.0f lines/hr total\n",
		len(activeServices), effectiveRate, linesPerHour)
	fmt.Fprintf(out, "  Data span:     %s → %s\n", start.UTC().Format(time.RFC3339Nano), lastTS.UTC().Format(time.RFC3339Nano))
	if cfg.highCardinality {
		fmt.Fprintf(out, "  Unique streams: %d (%d services × %d pods)\n",
			uniqueStreams, len(activeServices), cfg.podsPerService)
		fmt.Fprintf(out, "\nNow run: ./bench/run-comparison.sh --workloads=high_cardinality\n")
	} else {
		fmt.Fprintf(out, "\nNow run: ./bench/run-comparison.sh --workloads=small,heavy,long_range\n")
	}
	fmt.Fprintf(out, "(run-comparison.sh pins the workload reference time to the data end: DATA_END=%s)\n",
		lastTS.UTC().Format(time.RFC3339Nano))
	return nil
}

// retryUntilReady calls check until it succeeds or timeout elapses, so a seed
// started right after `docker compose up` waits for the backend.
func retryUntilReady(ctx context.Context, timeout time.Duration, out io.Writer, backend string, check func() ([]string, error)) ([]string, error) {
	deadline := time.Now().Add(timeout)
	for {
		r, err := check()
		if err == nil || !time.Now().Add(5*time.Second).Before(deadline) {
			return r, err
		}
		fmt.Fprintf(out, "  waiting for %s: %v\n", backend, err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

func pushError(backend string, ts time.Time, pushed int, err error) error {
	return fmt.Errorf("%s push of the batch at %s failed after %s lines: %w; the backends now hold different data, re-seed on a fresh stack (docker compose down -v)",
		backend, ts.UTC().Format(time.RFC3339), fmtCount(pushed), err)
}

// checkBackendsEmpty refuses to seed over existing data: a backend that
// already holds lines in the target span, or streams with the seeded app
// labels, would answer every query over data the other backend may not hold
// (VictoriaLogs keeps duplicate rows, Loki drops duplicate entries).
func checkBackendsEmpty(ctx context.Context, cfg seedConfig, start, end time.Time, svcs []service, out io.Writer) error {
	apps := make([]string, 0, len(svcs))
	for _, svc := range svcs {
		apps = append(apps, svc.app)
	}
	var reasons []string
	if !cfg.skipLoki {
		r, err := retryUntilReady(ctx, cfg.readyTimeout, out, "Loki", func() ([]string, error) {
			return dataspan.LokiExistingData(ctx, cfg.lokiURL, start, end, apps)
		})
		if err != nil {
			return fmt.Errorf("check Loki for existing data: %w", err)
		}
		reasons = append(reasons, r...)
	}
	if !cfg.skipVL {
		r, err := retryUntilReady(ctx, cfg.readyTimeout, out, "VictoriaLogs", func() ([]string, error) {
			return dataspan.VLExistingData(ctx, cfg.vlURL, start, end, apps)
		})
		if err != nil {
			return fmt.Errorf("check VictoriaLogs for existing data: %w", err)
		}
		reasons = append(reasons, r...)
	}
	if len(reasons) == 0 {
		return nil
	}
	if cfg.force {
		fmt.Fprintf(out, "⚠ --force: seeding over existing data:\n  - %s\n\n", strings.Join(reasons, "\n  - "))
		return nil
	}
	return fmt.Errorf("refusing to seed over existing data (use a fresh stack: docker compose down -v, or pass --force):\n  - %s", strings.Join(reasons, "\n  - "))
}

func podPoolKey(svc service) string {
	return svc.app + "/" + svc.region + "/" + svc.cluster
}

// buildStreamsHighCardinality builds streams with pod as a stream label, cycling through
// the pod pool so each batch rotates to a different pod, creating podPool[svc] unique
// Loki streams per service over time.
func buildStreamsHighCardinality(ts time.Time, linesPerSvc int, svcs []service, podPool map[string][]string, batchNum int) []streamBatch {
	batches := buildStreamsFor(ts, linesPerSvc, svcs)
	for i, svc := range svcs {
		pods := podPool[podPoolKey(svc)]
		batches[i].labels["pod"] = pods[batchNum%len(pods)] // high-cardinality label — unique stream per pod
	}
	return batches
}

func fmtCount(n int) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%.2fM", float64(n)/1_000_000)
	case n >= 1_000:
		return fmt.Sprintf("%.1fk", float64(n)/1_000)
	default:
		return fmt.Sprintf("%d", n)
	}
}

// buildStreamsFor generates one batch of entries per service. Lines of all
// services are interleaved across the batch's first second so no two streams
// share a timestamp: limit-capped log queries then cut at the same line on
// every backend instead of breaking ties between streams differently.
func buildStreamsFor(ts time.Time, linesPerSvc int, svcs []service) []streamBatch {
	batches := make([]streamBatch, 0, len(svcs))
	slots := time.Duration(linesPerSvc * len(svcs))
	for s, svc := range svcs {
		entries := make([]entry, 0, linesPerSvc)
		for i := 0; i < linesPerSvc; i++ {
			lineTS := ts.Add(time.Duration(i*len(svcs)+s) * time.Second / slots)
			entries = append(entries, entry{ts: lineTS, line: genLine(svc, lineTS)})
		}
		batches = append(batches, streamBatch{labels: streamLabels(svc), entries: entries})
	}
	return batches
}
