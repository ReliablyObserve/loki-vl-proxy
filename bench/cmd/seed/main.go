// seed: ingest dense historical log data into both Loki and VictoriaLogs for benchmarking.
//
// Generates N days of realistic multi-service logs with production-like metadata,
// back-filling from (now - N days) to now. Both Loki and VictoriaLogs receive
// identical streams so loki-bench comparison runs have the same data on both sides.
//
// Usage:
//
//	go run ./cmd/seed/ \
//	  --loki=http://localhost:3101 \
//	  --vl=http://localhost:9428 \
//	  --days=3 \
//	  --lines-per-batch=200 \
//	  --batch-interval=30s
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"time"
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

func genJSONLine(svc service, ts time.Time) string {
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

	entry := map[string]interface{}{
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
		entry["error"] = errs[rand.Intn(len(errs))]
	}

	b, _ := json.Marshal(entry)
	// Inject _msg for VictoriaLogs (stores original JSON string as message).
	full := map[string]interface{}{}
	_ = json.Unmarshal(b, &full)
	full["_msg"] = string(b)
	out, _ := json.Marshal(full)
	return string(out)
}

func genLogfmtLine(svc service, ts time.Time) string {
	level := levels[rand.Intn(len(levels))]
	switch svc.app {
	case "worker-service":
		job := jobNames[rand.Intn(len(jobNames))]
		worker := workers[rand.Intn(len(workers))]
		dur := rand.Intn(30000) + 100
		queued := rand.Intn(500)
		return fmt.Sprintf(`level=%s ts=%s msg="job completed" job=%s worker=%s duration_ms=%d queued=%d trace_id=%s service=%s version=%s`,
			level, ts.Format(time.RFC3339), job, worker, dur, queued, randID(32), svc.app, svc.version)
	case "payment-service":
		methods := []string{"card", "bank_transfer", "crypto", "paypal"}
		payMethod := methods[rand.Intn(len(methods))]
		amount := rand.Intn(100000) + 100
		status := []string{"authorized", "authorized", "authorized", "declined", "pending"}
		s := status[rand.Intn(len(status))]
		return fmt.Sprintf(`level=%s ts=%s msg="payment processed" method=%s amount_cents=%d status=%s user_id=usr_%d trace_id=%s service=%s version=%s`,
			level, ts.Format(time.RFC3339), payMethod, amount, s, rand.Intn(100000), randID(32), svc.app, svc.version)
	case "cache-redis":
		op := cacheOps[rand.Intn(len(cacheOps))]
		key := fmt.Sprintf("cache:%s:%s", dbTables[rand.Intn(len(dbTables))], randID(8))
		latency := rand.Intn(50) + 1
		hit := rand.Intn(10) > 2
		return fmt.Sprintf(`level=%s ts=%s msg="cache op" op=%s key=%s hit=%v latency_us=%d service=%s version=%s`,
			level, ts.Format(time.RFC3339), op, key, hit, latency, svc.app, svc.version)
	case "db-postgres":
		return genPostgresLine(svc, ts)
	default:
		return fmt.Sprintf(`level=%s ts=%s msg="event" service=%s version=%s trace_id=%s`,
			level, ts.Format(time.RFC3339), svc.app, svc.version, randID(32))
	}
}

func genPostgresLine(svc service, ts time.Time) string {
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
	return fmt.Sprintf(`level=%s ts=%s pid=%d op=%s table=%s rows=%d duration_ms=%d db=app_production service=%s`,
		level, ts.Format(time.RFC3339), pid, op, table, rows, dur, svc.app)
}

func genNginxLine(svc service, ts time.Time) string {
	method := httpMethods[rand.Intn(len(httpMethods))]
	path := apiPaths[rand.Intn(len(apiPaths))]
	status := weightedStatus()
	size := rand.Intn(50000) + 100
	reqTime := rand.Float64() * 2.0
	upstream := fmt.Sprintf("10.0.%d.%d:8080", rand.Intn(10), rand.Intn(254))
	return fmt.Sprintf(`%s - - [%s] "%s %s HTTP/1.1" %d %d "https://app.example.com" "Mozilla/5.0 (compatible)" %.3f %s`,
		randIP(), ts.Format("02/Jan/2006:15:04:05 -0700"),
		method, path, status, size, reqTime, upstream)
}

func genMLLine(svc service, ts time.Time) string {
	model := mlModels[rand.Intn(len(mlModels))]
	batchSize := []int{1, 8, 16, 32, 64}[rand.Intn(5)]
	latency := rand.Intn(2000) + 10
	tokens := rand.Intn(4096) + 1
	level := "info"
	if latency > 1500 {
		level = "warn"
	}
	entry := map[string]interface{}{
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
	b, _ := json.Marshal(entry)
	full := map[string]interface{}{}
	_ = json.Unmarshal(b, &full)
	full["_msg"] = string(b)
	out, _ := json.Marshal(full)
	return string(out)
}

func genETLLine(svc service, ts time.Time) string {
	job := etlJobs[rand.Intn(len(etlJobs))]
	records := rand.Intn(1000000) + 1000
	dur := rand.Intn(600) + 10
	level := "info"
	if dur > 300 {
		level = "warn"
	}
	entry := map[string]interface{}{
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
	b, _ := json.Marshal(entry)
	full := map[string]interface{}{}
	_ = json.Unmarshal(b, &full)
	full["_msg"] = string(b)
	out, _ := json.Marshal(full)
	return string(out)
}

func genLine(svc service, ts time.Time) string {
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
		return fmt.Sprintf(`level=info ts=%s msg=event service=%s`, ts.Format(time.RFC3339), svc.app)
	}
}

func buildStreams(ts time.Time, linesPerService int) []map[string]interface{} {
	return buildStreamsFor(ts, linesPerService, services)
}

func pushLoki(lokiURL string, streams []map[string]interface{}) error {
	payload := map[string]interface{}{"streams": streams}
	body, _ := json.Marshal(payload)
	resp, err := client.Post(lokiURL+"/loki/api/v1/push", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}

func pushVL(vlURL string, streams []map[string]interface{}) error {
	payload := map[string]interface{}{"streams": streams}
	body, _ := json.Marshal(payload)
	resp, err := client.Post(vlURL+"/insert/loki/api/v1/push", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}

func main() {
	lokiURL        := flag.String("loki", "http://localhost:3101", "Loki push URL")
	vlURL          := flag.String("vl", "http://localhost:9428", "VictoriaLogs push URL")
	days           := flag.Int("days", 7, "Days of historical data to seed")
	serviceCount   := flag.Int("services", 12, "Number of service streams (cycles through built-in pool; add more regions for >12)")
	ratePerSvc     := flag.Float64("rate", 0, "Target log lines/sec per service (overrides --lines-per-batch when set)")
	linesPerBatch  := flag.Int("lines-per-batch", 21, "Lines per service per time step (ignored when --rate is set)")
	batchInterval  := flag.Duration("batch-interval", 30*time.Second, "Simulated time step between push batches")
	skipLoki       := flag.Bool("skip-loki", false, "Skip Loki ingestion")
	skipVL         := flag.Bool("skip-vl", false, "Skip VictoriaLogs ingestion")
	highCardinality := flag.Bool("high-cardinality", false, "Add pod as a stream label with --pods-per-service unique pod IDs per service. Creates N×services unique Loki streams, exposing Loki O(streams×retention) memory pressure vs VL columnar model.")
	podsPerService  := flag.Int("pods-per-service", 50, "Number of unique pod IDs per service when --high-cardinality is set")
	flag.Parse()

	// When --rate is given, derive lines-per-batch from the target rate.
	if *ratePerSvc > 0 {
		computed := int(math.Round(*ratePerSvc * batchInterval.Seconds()))
		if computed < 1 {
			computed = 1
		}
		*linesPerBatch = computed
	}

	// Build the active service list by cycling through the built-in pool.
	activeServices := make([]service, *serviceCount)
	for i := range activeServices {
		activeServices[i] = services[i%len(services)]
		// Give duplicate services a unique region/cluster suffix so they form distinct streams.
		if i >= len(services) {
			activeServices[i].region = fmt.Sprintf("eu-west-%d", (i/len(services))+1)
			activeServices[i].cluster = fmt.Sprintf("eks-eu-%d", (i/len(services))+1)
		}
	}

	// Build pod pools for high-cardinality mode: N unique pod IDs per service.
	// Each batch rotates through the pod pool, creating N×services unique Loki streams.
	var podPool map[string][]string
	if *highCardinality {
		podPool = make(map[string][]string, len(activeServices))
		for _, svc := range activeServices {
			key := svc.app + "/" + svc.region
			if _, ok := podPool[key]; ok {
				continue
			}
			pods := make([]string, *podsPerService)
			for i := range pods {
				pods[i] = fmt.Sprintf("%s-%s-%s", svc.app, randID(5), randID(4))
			}
			podPool[key] = pods
		}
	}

	end := time.Now().Add(-time.Minute)
	start := end.Add(-time.Duration(*days) * 24 * time.Hour)

	totalBatches := int(end.Sub(start) / *batchInterval)
	// In high-cardinality mode each service fans out into podsPerService streams per batch.
	streamsPerBatch := len(activeServices)
	uniqueStreams := len(activeServices)
	if *highCardinality {
		uniqueStreams = len(activeServices) * *podsPerService
	}
	totalLines := totalBatches * *linesPerBatch * streamsPerBatch
	linesPerHour := float64(*linesPerBatch) * float64(streamsPerBatch) * (3600.0 / batchInterval.Seconds())
	linesPerMin := linesPerHour / 60.0
	effectiveRate := float64(*linesPerBatch) / batchInterval.Seconds()

	fmt.Printf("═══════════════════════════════════════════════════════════\n")
	fmt.Printf(" Seed Configuration\n")
	fmt.Printf("═══════════════════════════════════════════════════════════\n")
	fmt.Printf("  Period:        %d days  (%s → %s)\n", *days, start.Format("2006-01-02 15:04"), end.Format("2006-01-02 15:04"))
	fmt.Printf("  Services:      %d base streams\n", len(activeServices))
	if *highCardinality {
		fmt.Printf("  High-cardinality: %d pods/service → %d unique Loki streams\n", *podsPerService, uniqueStreams)
	}
	fmt.Printf("  Rate/service:  %.1f lines/sec  (~%.0f lines/min per service)\n",
		effectiveRate, effectiveRate*60)
	fmt.Printf("  Total rate:    %.0f lines/sec  (~%.0f/min  ~%.0f/hr)\n",
		effectiveRate*float64(streamsPerBatch), linesPerMin, linesPerHour)
	fmt.Printf("  Total volume:  ~%s lines  (%d batches × %d lines × %d streams)\n",
		fmtCount(totalLines), totalBatches, *linesPerBatch, streamsPerBatch)
	fmt.Printf("  Targets:       loki=%v  vl=%v\n", !*skipLoki, !*skipVL)
	fmt.Printf("═══════════════════════════════════════════════════════════\n\n")

	startWall := time.Now()
	var pushed, errCount, batchesOK int
	reportEvery := totalBatches / 20
	if reportEvery < 1 {
		reportEvery = 1
	}

	batchNum := 0
	for ts := start; ts.Before(end); ts = ts.Add(*batchInterval) {
		var streams []map[string]interface{}
		if *highCardinality {
			streams = buildStreamsHighCardinality(ts, *linesPerBatch, activeServices, podPool, batchNum)
		} else {
			streams = buildStreamsFor(ts, *linesPerBatch, activeServices)
		}
		batchNum++

		if !*skipLoki {
			if err := pushLoki(*lokiURL, streams); err != nil {
				fmt.Fprintf(os.Stderr, "warn: loki push at %s: %v\n", ts.Format(time.RFC3339), err)
				errCount++
			}
		}
		if !*skipVL {
			if err := pushVL(*vlURL, streams); err != nil {
				fmt.Fprintf(os.Stderr, "warn: vl push at %s: %v\n", ts.Format(time.RFC3339), err)
				errCount++
			}
		}

		pushed += *linesPerBatch * len(activeServices)
		batchesOK++

		if batchesOK%reportEvery == 0 {
			elapsed := time.Since(startWall)
			wallRate := float64(pushed) / elapsed.Seconds()
			eta := time.Duration(float64(totalLines-pushed)/wallRate) * time.Second
			pct := float64(ts.Sub(start)) / float64(end.Sub(start)) * 100
			fmt.Printf("  %.0f%% — %s — %s lines pushed  %.0f lines/s wall  ETA %s  errors=%d\n",
				pct, ts.Format("2006-01-02 15:04"),
				fmtCount(pushed), wallRate, eta.Truncate(time.Second), errCount)
		}
	}

	elapsed := time.Since(startWall)
	fmt.Printf("\n═══════════════════════════════════════════════════════════\n")
	fmt.Printf(" Seed Complete\n")
	fmt.Printf("═══════════════════════════════════════════════════════════\n")
	fmt.Printf("  Lines pushed:  %s across %d batches\n", fmtCount(pushed), batchesOK)
	fmt.Printf("  Wall time:     %s  (%.0f lines/s effective)\n",
		elapsed.Truncate(time.Second), float64(pushed)/elapsed.Seconds())
	fmt.Printf("  Errors:        %d\n", errCount)
	fmt.Printf("  Data shape:    %d services  %.1f lines/sec/svc  %.0f lines/hr total\n",
		len(activeServices), effectiveRate, linesPerHour)
	if *highCardinality {
		fmt.Printf("  Unique streams: %d (%d services × %d pods)\n",
			uniqueStreams, len(activeServices), *podsPerService)
		fmt.Printf("\nNow run: ./bench/run-comparison.sh --workloads=high_cardinality\n")
	} else {
		fmt.Printf("\nNow run: ./bench/run-comparison.sh --workloads=small,heavy,long_range\n")
	}
}

// buildStreamsHighCardinality builds streams with pod as a stream label, cycling through
// the pod pool so each batch rotates to a different pod, creating podPool[svc] unique
// Loki streams per service over time.
func buildStreamsHighCardinality(ts time.Time, linesPerSvc int, svcs []service, podPool map[string][]string, batchNum int) []map[string]interface{} {
	streams := make([]map[string]interface{}, 0, len(svcs))
	for _, svc := range svcs {
		key := svc.app + "/" + svc.region
		pods := podPool[key]
		pod := pods[batchNum%len(pods)]

		values := make([][]string, 0, linesPerSvc)
		for i := 0; i < linesPerSvc; i++ {
			lineTS := ts.Add(time.Duration(i) * time.Second / time.Duration(linesPerSvc))
			line := genLine(svc, lineTS)
			values = append(values, []string{fmt.Sprintf("%d", lineTS.UnixNano()), line})
		}
		streams = append(streams, map[string]interface{}{
			"stream": map[string]string{
				"app":        svc.app,
				"namespace":  svc.namespace,
				"job":        svc.namespace + "/" + svc.app,
				"env":        svc.env,
				"region":     svc.region,
				"cluster":    svc.cluster,
				"version":    svc.version,
				"log_format": svc.format,
				"pod":        pod, // high-cardinality label — creates unique stream per pod
			},
			"values": values,
		})
	}
	return streams
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

// buildStreamsFor is like buildStreams but uses a configurable service list.
func buildStreamsFor(ts time.Time, linesPerSvc int, svcs []service) []map[string]interface{} {
	streams := make([]map[string]interface{}, 0, len(svcs))
	for _, svc := range svcs {
		values := make([][]string, 0, linesPerSvc)
		for i := 0; i < linesPerSvc; i++ {
			lineTS := ts.Add(time.Duration(i) * time.Second / time.Duration(linesPerSvc))
			line := genLine(svc, lineTS)
			values = append(values, []string{fmt.Sprintf("%d", lineTS.UnixNano()), line})
		}
		streams = append(streams, map[string]interface{}{
			"stream": map[string]string{
				"app":        svc.app,
				"namespace":  svc.namespace,
				"job":        svc.namespace + "/" + svc.app,
				"env":        svc.env,
				"region":     svc.region,
				"cluster":    svc.cluster,
				"version":    svc.version,
				"log_format": svc.format,
			},
			"values": values,
		})
	}
	return streams
}
