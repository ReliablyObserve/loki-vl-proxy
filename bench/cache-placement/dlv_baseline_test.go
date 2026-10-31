package cacheplacement

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

type dlvCell struct {
	ID            string
	Index, Repeat int
	Shape, State  string
	RF            int
	Tier0         bool
	Shards        []int
	Samples       int
}
type dlvWave struct {
	FreshRAM bool
	Nodes    []int
}

var dlvShapes = []string{"DLV1", "DLV2", "DLV3", "native"}
var dlvStates = []string{"exact", "shifted", "refresh", "restart", "owner-loss"}

func dlvPlan(shards, samples, repeats int) ([]dlvCell, error) {
	if shards < 2 || shards > 16 || shards%2 != 0 || samples < 1 || samples > 20 || repeats < 1 || repeats > 4 {
		return nil, fmt.Errorf("require even shards2..16, samples1..20 and repeats1..4")
	}
	var cells []dlvCell
	for repeat := 0; repeat < repeats; repeat++ {
		for _, shape := range dlvShapes {
			for _, state := range dlvStates {
				for _, rf := range []int{1, 2} {
					for _, tier0 := range []bool{true, false} {
						index := len(cells)
						c := dlvCell{ID: fmt.Sprintf("%s-%s-rf%d-tier0%t-repeat%d", shape, state, rf, tier0, repeat), Index: index, Repeat: repeat, Shape: shape, State: state, RF: rf, Tier0: tier0, Samples: samples}
						for n := 0; n < shards; n++ {
							c.Shards = append(c.Shards, index*shards+n)
						}
						cells = append(cells, c)
					}
				}
			}
		}
	}
	return cells, nil
}
func dlvWindow(fx fixture, c dlvCell, sample int) (int64, error) {
	if sample < 0 {
		return 0, fmt.Errorf("negative sample")
	}
	end := fx.End - 1800
	switch c.State {
	case "exact", "restart", "owner-loss":
	case "shifted":
		end -= int64(sample+1) * 60
	case "refresh":
		end += int64(sample+1) * 60
	default:
		return 0, fmt.Errorf("unknown state%q", c.State)
	}
	width := int64(3600)
	if c.Shape == "DLV2" {
		width = 6 * 3600
	}
	if end > fx.End || end-width-60 < fx.End-8*3600 {
		return 0, fmt.Errorf("window outside seeded history")
	}
	return end, nil
}
func dlvWaves(c dlvCell) []dlvWave {
	if c.State != "restart" && c.State != "owner-loss" {
		w := dlvWave{}
		for i := 0; i < c.Samples; i++ {
			w.Nodes = append(w.Nodes, i%3)
		}
		return []dlvWave{w}
	}
	alive := []int{0, 1, 2}
	if c.State == "owner-loss" {
		alive = []int{1, 2}
	}
	var waves []dlvWave
	for remaining := c.Samples; remaining > 0; {
		w := dlvWave{FreshRAM: true}
		for i := 0; i < len(alive) && remaining > 0; i++ {
			w.Nodes = append(w.Nodes, alive[(i+len(waves))%len(alive)])
			remaining--
		}
		waves = append(waves, w)
	}
	return waves
}
func dlvClassify(status int, body []byte) (string, error) {
	if status != 200 {
		if status >= 400 && status < 500 {
			return "reference_error", nil
		}
		return "reference_failure", nil
	}
	if _, err := canonical(body); err != nil {
		return "", err
	}
	var v struct {
		Status string
		Data   struct {
			ResultType string
			Result     json.RawMessage
		}
	}
	if err := json.Unmarshal(body, &v); err != nil {
		return "", err
	}
	if v.Status != "success" || v.Data.ResultType != "matrix" {
		return "", fmt.Errorf("expected successful matrix")
	}
	var result []json.RawMessage
	if err := json.Unmarshal(v.Data.Result, &result); err != nil || result == nil {
		return "", fmt.Errorf("missing or malformed matrix result")
	}
	if len(result) == 0 {
		return "success_empty", nil
	}
	points := 0
	for _, raw := range result {
		var series struct {
			Metric map[string]string
			Values [][]json.RawMessage
		}
		if err := json.Unmarshal(raw, &series); err != nil || series.Metric == nil || series.Values == nil {
			return "", fmt.Errorf("malformed matrix series")
		}
		previousTimestamp := math.Inf(-1)
		for _, point := range series.Values {
			points++
			if len(point) != 2 {
				return "", fmt.Errorf("malformed matrix point")
			}
			timestamp, err := strconv.ParseFloat(string(point[0]), 64)
			if err != nil || math.IsNaN(timestamp) || math.IsInf(timestamp, 0) || timestamp <= previousTimestamp {
				return "", fmt.Errorf("invalid, duplicate or unordered matrix timestamp")
			}
			previousTimestamp = timestamp
			var value string
			if err := json.Unmarshal(point[1], &value); err != nil {
				return "", fmt.Errorf("invalid matrix value")
			}
			if _, err := strconv.ParseFloat(value, 64); err != nil {
				return "", err
			}
		}
	}
	if points == 0 {
		return "success_empty", nil
	}
	return "success_nonempty", nil
}
func dlvDelta(before, after map[string]float64, key string) *float64 {
	b, bok := before[key]
	a, aok := after[key]
	if !bok || !aok || a < b || math.IsNaN(a) || math.IsNaN(b) || math.IsInf(a, 0) || math.IsInf(b, 0) {
		return nil
	}
	value := a - b
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return nil
	}
	return &value
}
func TestDLVPlanHasIndependentBalancedCells(t *testing.T) {
	cells, err := dlvPlan(2, 20, 1)
	must(t, err)
	if len(cells) != 80 {
		t.Fatalf("cells=%d want80", len(cells))
	}
	ids := map[string]bool{}
	shards := map[int]bool{}
	for _, c := range cells {
		if ids[c.ID] {
			t.Fatalf("duplicate cell%s", c.ID)
		}
		ids[c.ID] = true
		if len(c.Shards) != 2 || c.Shards[0]%2 != 0 || c.Shards[1] != c.Shards[0]+1 {
			t.Fatalf("unbalanced group%+v", c)
		}
		for _, s := range c.Shards {
			if shards[s] {
				t.Fatalf("shared shard%d", s)
			}
			shards[s] = true
		}
	}
	repeated, err := dlvPlan(2, 20, 2)
	must(t, err)
	if len(repeated) != 160 || repeated[80].Shards[0] <= cells[79].Shards[1] {
		t.Fatal("control repeats share Loki query namespace")
	}
	for _, v := range []struct{ shards, samples, repeats int }{{0, 20, 1}, {3, 20, 1}, {2, 0, 1}, {2, 20, 0}} {
		if _, err := dlvPlan(v.shards, v.samples, v.repeats); err == nil {
			t.Fatalf("accepted invalid%+v", v)
		}
	}
}
func TestDLVWindowPlannerPreservesPhaseAndBounds(t *testing.T) {
	fx := fixture{End: 100000}
	for _, state := range []string{"exact", "shifted", "refresh", "restart", "owner-loss"} {
		c := dlvCell{Shape: "DLV2", State: state, Samples: 20}
		first, err := dlvWindow(fx, c, 0)
		must(t, err)
		for i := 0; i < 20; i++ {
			end, err := dlvWindow(fx, c, i)
			must(t, err)
			if (end-first)%60 != 0 || end > fx.End || end-6*3600-60 < fx.End-8*3600 {
				t.Fatalf("invalid window%s sample%d end%d", state, i, end)
			}
			if state == "shifted" && end != fx.End-1800-int64((i+1)*60) {
				t.Fatal("shift replayed initial window")
			}
			if state == "refresh" && end != fx.End-1800+int64((i+1)*60) {
				t.Fatal("refresh must advance unseen windows")
			}
			if state != "shifted" && state != "refresh" && end != fx.End-1800 {
				t.Fatal("fixed mode moved")
			}
		}
	}
	if _, err := dlvWindow(fx, dlvCell{Shape: "DLV2", State: "refresh"}, 40); err == nil {
		t.Fatal("accepted window outside seeded data")
	}
}
func TestDLVRestartSamplesAlwaysHaveFreshRAM(t *testing.T) {
	for _, state := range []string{"exact", "restart", "owner-loss"} {
		c := dlvCell{State: state, Samples: 20}
		waves := dlvWaves(c)
		n := 0
		if len(waves) == 0 {
			t.Fatal("missing sample schedule")
		}
		for _, w := range waves {
			if w.FreshRAM != (state == "restart" || state == "owner-loss") {
				t.Fatal("wrong RAM lifecycle")
			}
			seen := map[int]bool{}
			for _, node := range w.Nodes {
				if state == "owner-loss" && node == 0 {
					t.Fatal("request routed to lost node")
				}
				if w.FreshRAM && seen[node] {
					t.Fatal("later request would be a warm RAM sample")
				}
				seen[node] = true
				n++
			}
		}
		if n != 20 {
			t.Fatalf("%s samples%d", state, n)
		}
	}
}
func TestDLVResponseClassesFollowActualReference(t *testing.T) {
	tests := []struct {
		status     int
		body, want string
	}{
		{200, "{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[{\"metric\":{},\"values\":[[1,\"2\"]]}]}}", "success_nonempty"},
		{200, "{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[]}}", "success_empty"},
		{400, "parse error: invalid JSON", "reference_error"},
	}
	for _, c := range tests {
		got, err := dlvClassify(c.status, []byte(c.body))
		must(t, err)
		if got != c.want {
			t.Fatalf("class%q want%q", got, c.want)
		}
	}
	for _, body := range []string{"{\"status\":\"success\"}", "{\"data\":{\"result\":[]}} {}", "{\"data\":"} {
		if _, err := dlvClassify(200, []byte(body)); err == nil {
			t.Fatalf("accepted malformed success%s", body)
		}
	}
}
func TestDLVAbsentCountersStayUnsupported(t *testing.T) {
	key := "loki_vl_proxy_cache_tier_hits_total{tier=\"l0\"}"
	if got := dlvDelta(nil, map[string]float64{key: 3}, key); got != nil {
		t.Fatal("missing baseline fabricated as zero")
	}
	if got := dlvDelta(map[string]float64{key: 2}, map[string]float64{key: 5}, key); got == nil || *got != 3 {
		t.Fatal("observed delta missing")
	}
	if got := dlvDelta(map[string]float64{key: 5}, map[string]float64{key: 2}, key); got != nil {
		t.Fatal("counter reset represented as negative hit")
	}
}

var dlvRepeatIndex = flag.Int("dlv-repeat-index", -1, "reserved namespace repeat index; -1 selects all")
var dlvOperation = flag.String("dlv-baseline", "", "opt-in: setup, probe, measure, cleanup")
var dlvBinary = flag.String("dlv-binary", "", "one current proxy executable")
var dlvSource = flag.String("dlv-source-ref", "9956043537323d3bc30742fff509f7cb6edce5be", "expected binary source revision")
var dlvOutput = flag.String("dlv-output", filepath.Join(os.TempDir(), "dlv-cache-baseline"), "private artifact directory")
var dlvProject = flag.String("dlv-project", "dlvbaseline1031", "isolated Compose project")
var dlvRoot = flag.String("dlv-root", defaultRoot(), "repository root")
var dlvLokiPort = flag.Int("dlv-loki-port", 60101, "isolated Loki port")
var dlvVLPort = flag.Int("dlv-vl-port", 60428, "isolated VictoriaLogs port")
var dlvProxyPort = flag.Int("dlv-proxy-port-base", 61200, "first proxy ten-port block")
var dlvShards = flag.Int("dlv-shards-per-cell", 0, "even2..16; default2 setup/probe,8 measure")
var dlvSamples = flag.Int("dlv-samples", 0, "samples per cell1..20; default3 probe,20 measure")
var dlvRepeats = flag.Int("dlv-control-repeats", 1, "independent namespace repetitions1..4; setup must reserve them")
var dlvShapeFilter = flag.String("dlv-shapes", "", "optional CSV DLV1,DLV2,DLV3,native")
var dlvStateFilter = flag.String("dlv-states", "", "optional CSV exact,shifted,refresh,restart,owner-loss")
var dlvRFFilter = flag.String("dlv-rfs", "", "CSV replication factors")
var dlvTierFilter = flag.String("dlv-tier0", "on,off", "CSV Tier0 states")
var dlvKeepWork = flag.Bool("dlv-keep-work", false, "retain closed cell process/cache directories after report")
var dlvEviction = flag.Bool("dlv-eviction-diagnostic", true, "untimed compatibility-cache eviction attempt after exact cells; no observed-miss claim")

type dlvResponse struct {
	Source, Profile, Mode, Name                   string
	Trial, Run, Node                              int
	Path                                          string
	Headers                                       map[string]string
	RequestEndAgeSeconds                          float64
	RecentTailEligible, MetadataFreshnessEligible bool
	ProxyMS, LokiMS                               float64
	ProxyStatus, LokiStatus                       int
	Equal                                         bool
	ComparisonError                               string
	ProxySHA256, LokiSHA256                       string
	LokiCache                                     any
	ReferenceClass, ProxyClass                    string
	Successful                                    bool
}
type dlvTier0 struct {
	UnsupportedReason string
	State             string
	Hits              *float64
	Misses            *float64
	Requests          *float64
	Objects           *float64
	Bytes             *float64
}
type dlvRange struct {
	Method        string
	Path          string
	Query         url.Values
	Status        int
	RequestBytes  int64
	ResponseBytes int64
	Node          int
}
type dlvObservation struct {
	Before, After              resources
	ObserverStart, ObserverEnd int
	NetworkHTTPBytes           map[string]networkBytes
	Backend                    wireTotals
	BackendRequests            []dlvRange
	PeerRequests               []dlvRange
	ProxyLoki                  struct {
		Tier0                  dlvTier0
		OtherCacheLayerMetrics any
	}
	ProxyVL struct {
		HTTPReads             int
		HTTPResponseBodyBytes int64
		CacheLayerMetrics     any
		UnsupportedReason     string
	}
	ReferenceLoki struct{ PerRequestStatistics string }
	SharedStorage struct {
		Attribution               string
		TierCounterDeltas         map[string]*float64
		LogicalDiskOccupancyBytes *float64
		UnknownCacheLayerMetrics  any
	}
	ProxyCPUSeconds *float64
	Valid           bool
}
type dlvBatch struct {
	Name        string
	Wave        int
	FreshRAM    bool
	Rows        []dlvResponse
	Observation dlvObservation
}
type dlvDiagnostic struct {
	State         string
	ChurnRequests int
	ChurnCorrect  int
	ProvenMiss    bool
	Result        *dlvBatch
	Reason        string
}
type dlvStartup struct {
	Name             string
	Wire             wireTotals
	NetworkHTTPBytes map[string]networkBytes
}
type dlvCellReport struct {
	Startup          []dlvStartup
	Cell             dlvCell
	Config           config
	LaunchFlags      [][]string
	Priming          []dlvBatch
	Measurements     []dlvBatch
	Eviction         *dlvDiagnostic
	SnapshotSHA256   map[string]string
	ProcessLifetimes []map[string]any
	Summary          map[string]any
	Errors           []string
}
type dlvFixturePlan struct {
	Version        int
	ShardsPerCell  int
	Repeats        int
	NamespaceCells int
	FixtureSHA256  string
}

func dlvSelected(value string, allowed []string) (map[string]bool, error) {
	out := map[string]bool{}
	if value == "" {
		for _, v := range allowed {
			out[v] = true
		}
		return out, nil
	}
	for _, v := range strings.Split(value, ",") {
		found := false
		for _, a := range allowed {
			if a == v {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("unknown selection%q", v)
		}
		out[v] = true
	}
	return out, nil
}
func dlvQuery(fx fixture, c dlvCell, end int64) query {
	shardNames := make([]string, len(c.Shards))
	for i, s := range c.Shards {
		shardNames[i] = number(s)
	}
	selector := fmt.Sprintf("{perf_probe=%q,env=\"production\",app=\"api-gateway\",shard=~%q}", fx.Probe, "("+strings.Join(shardNames, "|")+")")
	if c.Shape == "DLV2" {
		selector = fmt.Sprintf("{perf_probe=%q,env=\"production\",shard=~%q}", fx.Probe, "("+strings.Join(shardNames, "|")+")")
	}
	expression := "sum by (detected_level) (count_over_time(" + selector + " | json [1m]))"
	headers := map[string]string(nil)
	width := int64(3600)
	switch c.Shape {
	case "DLV1":
		headers = map[string]string{"X-Query-Tags": "Source=grafana-lokiexplore-app"}
	case "DLV2":
		expression = "sum by (detected_level) (count_over_time(" + selector + " | json | __error__!=\"\" [1m]))"
		width = 6 * 3600
	case "native":
		expression = "sum by (detected_level) (count_over_time(" + selector + "[1m]))"
	}
	params := url.Values{"query": {expression}, "start": {strconv.FormatInt((end-width)*1e9, 10)}, "end": {strconv.FormatInt(end*1e9, 10)}, "step": {"60"}}
	return query{Name: c.Shape, Path: "/loki/api/v1/query_range?" + params.Encode(), Headers: headers}
}
func dlvGauge(r resources, key string) *float64 {
	if !r.Valid {
		return nil
	}
	value, ok := r.Metrics[key]
	if !ok || math.IsNaN(value) || math.IsInf(value, 0) {
		return nil
	}
	return &value
}
func dlvObserve(t *testing.T, ps []*process, c dlvCell, rec *recorder, run func() []dlvResponse) dlvBatch {
	t.Helper()
	ledgerOffset, ledgerIndex := dlvLedgerCursor(t, rec)
	before := sample(ps)
	start, networkBefore := rec.capture(t)
	rows := run()
	dlvDrain(t, rec)
	end, networkAfter := rec.capture(t)
	after := sample(ps)
	o := dlvObservation{Before: before, After: after, ObserverStart: start, ObserverEnd: end, NetworkHTTPBytes: networkDelta(networkBefore, networkAfter), Backend: rec.between(start, end), Valid: before.Valid && after.Valid}
	dlvAttributeObservation(&o, c)
	ledgerEnd, _ := dlvLedgerCursor(t, rec)
	// The recorder intentionally owns a write-only descriptor. Use an independent
	// read handle after flushing the bounded section; do not reopen/truncate it.
	ledger, err := os.Open(rec.path)
	must(t, err)
	defer ledger.Close()
	scanner := bufio.NewScanner(io.NewSectionReader(ledger, ledgerOffset, ledgerEnd-ledgerOffset))
	scanner.Buffer(make([]byte, 4096), 16<<20)
	index := ledgerIndex
	for scanner.Scan() {
		if index >= start && index < end {
			var event wireEvent
			must(t, json.Unmarshal(scanner.Bytes(), &event))
			row := dlvRange{Method: event.Method, Path: event.Path, Query: event.Query, Status: event.Status, RequestBytes: event.RequestBytes, ResponseBytes: event.ResponseBytes, Node: event.Node}
			if event.Kind == "vl" {
				o.BackendRequests = append(o.BackendRequests, row)
			} else {
				o.PeerRequests = append(o.PeerRequests, row)
			}
		}
		index++
	}
	must(t, scanner.Err())
	for i := range rows {
		rows[i].Successful = o.Valid && rows[i].Equal && rows[i].ProxyStatus == 200 && rows[i].LokiStatus == 200 && strings.HasPrefix(rows[i].ReferenceClass, "success_")
	}
	return dlvBatch{Rows: rows, Observation: o}
}
func dlvAttributeObservation(o *dlvObservation, c dlvCell) {
	before, after := o.Before, o.After
	o.ProxyLoki.Tier0.State = "disabled"
	if c.Tier0 {
		o.ProxyLoki.Tier0.State = "enabled"
	}
	o.ProxyLoki.Tier0.UnsupportedReason = "Compatibility cache has no dedicated exported hit/miss/occupancy metric; physical tier=l0 is a hot-key sidecar, not compatibility Tier0."
	o.ProxyVL.HTTPReads = o.Backend.VLCalls
	o.ProxyVL.HTTPResponseBodyBytes = o.Backend.VLBytes
	o.ProxyVL.UnsupportedReason = "Existing aggregate L1/L2/peer counters do not establish a VL semantic-layer hit or occupancy."
	o.ReferenceLoki.PerRequestStatistics = "Each response retains reference Loki data.stats.cache independently from proxy caches."
	o.SharedStorage.Attribution = "unattributed aggregate physical tiers; no API/VL semantic-layer attribution"
	o.SharedStorage.TierCounterDeltas = map[string]*float64{}
	for key := range after.Metrics {
		if _, layerMetric := dlvParseLayerMetric(key); layerMetric {
			continue
		}
		if strings.Contains(key, "{tier=") && strings.HasSuffix(strings.SplitN(key, "{", 2)[0], "_total") {
			if o.Valid {
				o.SharedStorage.TierCounterDeltas[key] = dlvDelta(before.Metrics, after.Metrics, key)
			} else {
				o.SharedStorage.TierCounterDeltas[key] = nil
			}
		}
	}
	if o.Valid {
		o.ProxyCPUSeconds = dlvDelta(before.Metrics, after.Metrics, "loki_vl_proxy_process_cpu_seconds_total")
	}
	dlvAttributeLayerMetrics(o, c)
}
func dlvRequest(t *testing.T, p *process, node int, q query, c dlvCell, ordinal int) dlvResponse {
	t.Helper()
	row := dlvResponse{Source: "current", Trial: c.Repeat, Profile: c.ID, Mode: c.State, Name: c.Shape, Run: ordinal, Node: node, Path: q.Path, Headers: q.Headers}
	if parsed, err := url.Parse(q.Path); err == nil {
		if end, err := strconv.ParseInt(parsed.Query().Get("end"), 10, 64); err == nil {
			row.RequestEndAgeSeconds = time.Since(time.Unix(0, end)).Seconds()
			row.RecentTailEligible = row.RequestEndAgeSeconds >= 0 && row.RequestEndAgeSeconds <= 120
			row.MetadataFreshnessEligible = row.RequestEndAgeSeconds >= 0 && row.RequestEndAgeSeconds <= 24*3600
		}
	}
	var proxyBody, referenceBody []byte
	reference := func() {
		begin := time.Now()
		status, body, _, err := fetch("GET", lokiBase()+q.Path, nil, q.Headers)
		row.LokiMS = float64(time.Since(begin).Nanoseconds()) / 1e6
		row.LokiStatus = status
		referenceBody = body
		if err != nil {
			row.ComparisonError = "reference transport: " + err.Error()
		}
	}
	measured := func() {
		begin := time.Now()
		status, body, _, err := fetch("GET", p.url+q.Path, nil, q.Headers)
		row.ProxyMS = float64(time.Since(begin).Nanoseconds()) / 1e6
		row.ProxyStatus = status
		proxyBody = body
		if err != nil {
			row.ComparisonError += " proxy transport: " + err.Error()
		}
	}
	if ordinal%2 == 0 {
		reference()
		measured()
	} else {
		measured()
		reference()
	}
	var err error
	row.ReferenceClass, err = dlvClassify(row.LokiStatus, referenceBody)
	if err != nil {
		row.ComparisonError += " reference body: " + err.Error()
	}
	row.ProxyClass, err = dlvClassify(row.ProxyStatus, proxyBody)
	if err != nil {
		row.ComparisonError += " proxy body: " + err.Error()
	}
	row.Equal = row.ComparisonError == "" && row.ProxyStatus == row.LokiStatus
	if row.Equal && row.ProxyStatus == 200 {
		a, ae := canonical(proxyBody)
		b, be := canonical(referenceBody)
		row.Equal = ae == nil && be == nil && a == b
	} else if row.Equal {
		a, ae := canonical(proxyBody)
		b, be := canonical(referenceBody)
		row.Equal = ae == nil && be == nil && a == b || bytes.Equal(proxyBody, referenceBody)
	}
	proxyHash := sha256.Sum256(proxyBody)
	referenceHash := sha256.Sum256(referenceBody)
	row.ProxySHA256 = hex.EncodeToString(proxyHash[:])
	row.LokiSHA256 = hex.EncodeToString(referenceHash[:])
	var parsed map[string]any
	if json.Unmarshal(referenceBody, &parsed) == nil {
		if data, ok := parsed["data"].(map[string]any); ok {
			if stats, ok := data["stats"].(map[string]any); ok {
				row.LokiCache = stats["cache"]
			}
		}
	}
	if !row.Equal {
		dir := filepath.Join(home, "mismatches", runID, c.ID)
		must(t, os.MkdirAll(dir, 0700))
		stem := fmt.Sprintf("%s-node%d-row%d-%d", c.State, node, ordinal, mismatchID.Add(1))
		must(t, os.WriteFile(filepath.Join(dir, stem+"-proxy.body"), proxyBody, 0600))
		must(t, os.WriteFile(filepath.Join(dir, stem+"-loki.body"), referenceBody, 0600))
	}
	return row
}
func dlvCopyClosedCaches(ps []*process, destinations []string) error {
	for _, p := range ps {
		if p != nil && p.cmd != nil {
			return fmt.Errorf("refuse snapshot of running proxy")
		}
	}
	if len(ps) != len(destinations) {
		return fmt.Errorf("snapshot destination mismatch")
	}
	for i, p := range ps {
		if p == nil {
			continue
		}
		if err := os.MkdirAll(destinations[i], 0700); err != nil {
			return err
		}
		entries, err := os.ReadDir(p.dir)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if !strings.HasSuffix(entry.Name(), ".db") {
				continue
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return fmt.Errorf("cache snapshot is not regular file")
			}
			in, err := os.Open(filepath.Join(p.dir, entry.Name()))
			if err != nil {
				return err
			}
			out, err := os.OpenFile(filepath.Join(destinations[i], entry.Name()), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err != nil {
				in.Close()
				return err
			}
			_, copyErr := io.Copy(out, in)
			closeErr := out.Close()
			inErr := in.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
			if inErr != nil {
				return inErr
			}
		}
	}
	return nil
}
func dlvSummary(batches []dlvBatch) map[string]any {
	classes := map[string]int{}
	groups := map[string][]dlvResponse{}
	correct, total, successful := 0, 0, 0
	for _, b := range batches {
		for _, row := range b.Rows {
			total++
			if row.Equal {
				correct++
			}
			classes[row.ReferenceClass]++
			if row.Successful && row.Equal && row.ProxyStatus == 200 && row.LokiStatus == 200 && strings.HasPrefix(row.ReferenceClass, "success_") {
				successful++
				groups[row.ReferenceClass] = append(groups[row.ReferenceClass], row)
			}
		}
	}
	summary := map[string]any{"responses": total, "equal_reference": correct, "successful_valid_200": successful, "reference_classes": classes, "latency_by_success_class": map[string]any{}}
	latencies := summary["latency_by_success_class"].(map[string]any)
	for class, rows := range groups {
		var proxy, loki []float64
		for _, r := range rows {
			proxy = append(proxy, r.ProxyMS)
			loki = append(loki, r.LokiMS)
		}
		latencies[class] = map[string]any{"samples": len(rows), "proxy_p50_ms": percentile(proxy, .5), "proxy_p95_ms": percentile(proxy, .95), "loki_p50_ms": percentile(loki, .5), "loki_p95_ms": percentile(loki, .95)}
	}
	return summary
}

func dlvLedgerCursor(t *testing.T, rec *recorder) (int64, int) {
	t.Helper()
	rec.mu.Lock()
	defer rec.mu.Unlock()
	must(t, rec.err)
	must(t, rec.buffer.Flush())
	offset, err := rec.file.Seek(0, io.SeekCurrent)
	must(t, err)
	return offset, len(rec.prefix) - 1
}

// This is the existing bounded quiet-period policy for asynchronous replica
// work, followed by the strict active-response/Write completion barrier.
func dlvDrain(t *testing.T, rec *recorder) {
	t.Helper()
	until := time.Now().Add(3 * time.Second)
	changed := time.Now()
	previous := rec.count()
	for time.Now().Before(until) {
		time.Sleep(100 * time.Millisecond)
		count := rec.count()
		if count != previous {
			changed = time.Now()
		}
		if time.Since(changed) >= 500*time.Millisecond {
			break
		}
		previous = count
	}
	_, _ = rec.capture(t)
}
func dlvExtraFlags() []string {
	return []string{"-compat-cache-max-percent=10", "-disk-cache-codec=s2", "-disk-cache-flush-bytes=4194304", "-disk-cache-max-buffer-bytes=16777216", "-disk-cache-max-entries=100000", "-disk-cache-max-index-bytes=16777216"}
}
func dlvConfig(c dlvCell) config {
	return config{Profile: "focused-dlv", Entries: 1024, Bytes: 64 << 20, DiskBytes: 64 << 20, Shards: len(c.Shards), RF: c.RF, Edge: c.Tier0, Repeats: c.Samples}
}
func dlvValidation(b dlvBatch, native bool) []string {
	var errors []string
	if !b.Observation.Valid {
		errors = append(errors, "resource samples incomplete")
	}
	for _, r := range b.Rows {
		if !r.Equal {
			errors = append(errors, fmt.Sprintf("parity failure node%d row%d: proxy%d/reference%d %s", r.Node, r.Run, r.ProxyStatus, r.LokiStatus, r.ComparisonError))
		}
		if r.ReferenceClass == "reference_failure" {
			errors = append(errors, "reference HTTP/transport failure")
		}
		if native && r.ReferenceClass != "success_nonempty" {
			errors = append(errors, "native control must return a nonempty matrix")
		}
	}
	return errors
}
func dlvRunCell(t *testing.T, binary string, c dlvCell, fx fixture, rec *recorder) dlvCellReport {
	t.Helper()
	cfg := dlvConfig(c)
	report := dlvCellReport{Cell: c, Config: cfg, SnapshotSHA256: map[string]string{}}
	dir := filepath.Join(home, "runs", runID, c.ID)
	must(t, os.MkdirAll(dir, 0700))
	var processes []*process
	stop := func(ps []*process) {
		for _, p := range ps {
			if p != nil && p.cmd != nil {
				p.stop()
				if p.Exit != nil {
					report.ProcessLifetimes = append(report.ProcessLifetimes, p.Exit)
				}
				if p.exitErr != nil {
					report.Errors = append(report.Errors, "proxy shutdown: "+p.exitErr.Error())
				}
			}
		}
	}
	defer func() { stop(processes) }()
	if !*dlvKeepWork {
		t.Cleanup(func() { _ = os.RemoveAll(dir) })
	}
	launchWave := func(name string, source []*process, lost bool) []*process {
		startupStart, networkBefore := rec.capture(t)
		ps := make([]*process, 3)
		dest := make([]string, 3)
		for i := range ps {
			dest[i] = filepath.Join(dir, name, "node"+number(i))
		}
		if source != nil {
			copySource := append([]*process(nil), source...)
			if lost {
				copySource[0] = nil
			}
			must(t, dlvCopyClosedCaches(copySource, dest))
		}
		for i := range ps {
			if lost && i == 0 {
				continue
			}
			ps[i] = launchRing(t, binary, cfg, i, dest[i], 3, false, dlvExtraFlags()...)
			if name == "prime" {
				report.LaunchFlags = append(report.LaunchFlags, append(flags(cfg, i, dest[i]), dlvExtraFlags()...))
			}
		}
		dlvDrain(t, rec)
		startupEnd, networkAfter := rec.capture(t)
		report.Startup = append(report.Startup, dlvStartup{Name: name, Wire: rec.between(startupStart, startupEnd), NetworkHTTPBytes: networkDelta(networkBefore, networkAfter)})
		return ps
	}
	processes = launchWave("prime", nil, false)
	initialEnd := fx.End - 1800
	primeQuery := dlvQuery(fx, c, initialEnd)
	prime := dlvObserve(t, processes, c, rec, func() []dlvResponse {
		var rows []dlvResponse
		for node, p := range processes {
			rows = append(rows, dlvRequest(t, p, node, primeQuery, c, node))
		}
		return rows
	})
	prime.Name = "cell-prime"
	report.Priming = append(report.Priming, prime)
	report.Errors = append(report.Errors, dlvValidation(prime, c.Shape == "native")...)
	if c.State == "restart" || c.State == "owner-loss" {
		stop(processes)
		for node, p := range processes {
			files, err := os.ReadDir(p.dir)
			must(t, err)
			for _, f := range files {
				if strings.HasSuffix(f.Name(), ".db") {
					report.SnapshotSHA256[fmt.Sprintf("node%d/%s", node, f.Name())] = digest(t, filepath.Join(p.dir, f.Name()))
				}
			}
		}
	}
	primed := append([]*process(nil), processes...)
	ordinal := 0
	for waveIndex, wave := range dlvWaves(c) {
		if wave.FreshRAM {
			processes = launchWave("wave"+number(waveIndex), primed, c.State == "owner-loss")
		}
		batch := dlvObserve(t, processes, c, rec, func() []dlvResponse {
			var rows []dlvResponse
			for _, node := range wave.Nodes {
				end, err := dlvWindow(fx, c, ordinal)
				must(t, err)
				rows = append(rows, dlvRequest(t, processes[node], node, dlvQuery(fx, c, end), c, ordinal))
				ordinal++
			}
			return rows
		})
		batch.Name = "measured"
		batch.Wave = waveIndex
		batch.FreshRAM = wave.FreshRAM
		report.Measurements = append(report.Measurements, batch)
		report.Errors = append(report.Errors, dlvValidation(batch, c.Shape == "native")...)
		if wave.FreshRAM {
			stop(processes)
		}
	}
	if c.State == "exact" && c.Tier0 && *dlvEviction {
		diag := dlvDiagnostic{State: "eviction_attempt", Reason: "No dedicated compatibility-cache miss metric; an eviction attempt cannot be labelled an observed Tier0 lookup miss."}
		native := c
		native.Shape = "native"
		native.State = "shifted"
		limit := cfg.Entries*10/100 + 1
		for i := 0; i < limit; i++ {
			q := dlvQuery(fx, native, initialEnd-int64(i+1)*60)
			row := dlvRequest(t, processes[0], 0, q, native, 1000+i)
			diag.ChurnRequests++
			if row.Equal && row.ProxyStatus == 200 {
				diag.ChurnCorrect++
			} else {
				report.Errors = append(report.Errors, "eviction churn parity failure")
			}
		}
		dlvDrain(t, rec)
		replay := dlvObserve(t, processes, c, rec, func() []dlvResponse { return []dlvResponse{dlvRequest(t, processes[0], 0, primeQuery, c, 2000)} })
		replay.Name = "untimed-eviction-replay"
		diag.Result = &replay
		report.Errors = append(report.Errors, dlvValidation(replay, c.Shape == "native")...)
		report.Eviction = &diag
	}
	stop(processes)
	report.Summary = dlvSummary(report.Measurements)
	return report
}
func dlvBinaryManifest(t *testing.T) map[string]any {
	t.Helper()
	info, err := buildinfo.ReadFile(*dlvBinary)
	must(t, err)
	settings := map[string]string{}
	for _, s := range info.Settings {
		settings[s.Key] = s.Value
	}
	if revision := settings["vcs.revision"]; revision != "" && !strings.HasPrefix(revision, *dlvSource) {
		t.Fatalf("binary source%s differs from requested%s", revision, *dlvSource)
	}
	return map[string]any{
		"requested_backend_images":     map[string]string{"loki": lokiImage, "victorialogs": vlImage},
		"backend_image_identity_scope": "requested image references only; actual container image IDs must be captured separately",
		"compose_spec_sha256":          digest(t, filepath.Join(home, "compose.json")),
		"loki_config_sha256":           digest(t, filepath.Join(home, "loki.yaml")),
		"source_ref_claim":             *dlvSource, "binary": *dlvBinary, "binary_sha256": digest(t, *dlvBinary), "binary_go_version": info.GoVersion, "binary_build_settings": settings, "source_revision_available": settings["vcs.revision"] != "", "source_tree_modified": settings["vcs.modified"],
		"focused_harness_sha256":           digest(t, filepath.Join(root, "bench/cache-placement/dlv_baseline_test.go")),
		"shared_harness_sha256":            digest(t, filepath.Join(root, "bench/cache-placement/real_api_test.go")),
		"layer_attribution_harness_sha256": digest(t, filepath.Join(root, "bench/cache-placement/dlv_layer_attribution_test.go")),
		"measurement_host":                 map[string]string{"os": runtime.GOOS, "arch": runtime.GOARCH, "go": runtime.Version()},
		"semantic_cache_dimensions":        []string{"proxy_loki_facing", "proxy_vl_backend", "reference_loki_server"},
		"aggregate_tier_attribution":       "unattributed; tier=l0 is a hot-key sidecar, not compatibility Tier0",
		"unsupported_layer_metrics":        "null is unsupported/missing, never a measured zero",
		"timing_scope":                     "descriptive instrumented same-host latency; other user services may contend; no universal speedup or scaling claim",
		"fixture_scope":                    "independent disjoint per-cell shard groups; reference cache keys separate; backend chunk/OS caches may already be warm",
		"observer_scope":                   "paired ledger/wire active-response completion plus bounded500ms quiet period(up to3s) for asynchronous replica work",
		"storage_limits":                   map[string]any{"primary_ram_bytes": 64 << 20, "primary_entries": 1024, "additional_compat_percent": 10, "logical_disk_bytes": 64 << 20, "disk_entries": 100000, "estimated_index_bytes": 16 << 20, "write_queue_bytes": 16 << 20, "flush_trigger_bytes": 4 << 20, "disk_codec": "s2", "profiling_on_timed_runs": false},
		"resource_scope":                   "logical admission limits are not physical SSD caps; bbolt lengths/allocated blocks and GoSys/kernel Maxrss are separate; no per-semantic-layer allocator or occupancy inferred",
	}
}
func TestDLVCacheBaseline(t *testing.T) {
	op := *dlvOperation
	if op == "" {
		t.Skip("opt-in -dlv-baseline=setup|probe|measure|cleanup")
	}
	if op != "setup" && op != "probe" && op != "measure" && op != "cleanup" {
		t.Fatal("unknown DLV operation")
	}
	oldRoot, oldHome, oldProject := root, home, project
	oldLoki, oldVL, oldProxy := lokiPort, vlPort, proxyPortBase
	root, home, project = *dlvRoot, *dlvOutput, *dlvProject
	lokiPort, vlPort, proxyPortBase = *dlvLokiPort, *dlvVLPort, *dlvProxyPort
	defer func() {
		root, home, project = oldRoot, oldHome, oldProject
		lokiPort, vlPort, proxyPortBase = oldLoki, oldVL, oldProxy
	}()
	must(t, os.MkdirAll(home, 0700))
	if op == "setup" {
		must(t, dlvSetupUnused(home))
		setupShards := *dlvShards
		if setupShards == 0 {
			setupShards = 2
		}
		must(t, dlvClaimSetupAttempt(home, map[string]any{"run_id": runID, "operation": op, "started_at": time.Now().UTC(), "project": project, "shards_per_cell": setupShards, "control_repeats": *dlvRepeats, "loki_port": lokiPort, "vl_port": vlPort}))
	}
	prepareCompose(t)
	if op == "cleanup" {
		compose(t, "down", "-v", "--remove-orphans")
		return
	}
	shards := *dlvShards
	if shards == 0 {
		shards = 2
		if op == "measure" {
			shards = 8
		}
	}
	samples := *dlvSamples
	if samples == 0 {
		samples = 20
		if op == "probe" {
			samples = 3
		}
	}
	plans, err := dlvPlan(shards, samples, *dlvRepeats)
	must(t, err)
	if *dlvRepeatIndex < -1 || *dlvRepeatIndex >= *dlvRepeats {
		t.Fatal("repeat index outside reserved namespaces")
	}
	if op == "setup" {
		compose(t, "up", "-d", "--no-build")
		fx := seed(t, len(plans)*shards)
		save(t, filepath.Join(home, "dlv-fixture-plan.json"), dlvFixturePlan{Version: 1, ShardsPerCell: shards, Repeats: *dlvRepeats, NamespaceCells: len(plans), FixtureSHA256: fx.SHA256})
		t.Logf("seeded%drows for%d independent DLV namespaces", fx.Lines, len(plans))
		return
	}
	if *dlvBinary == "" {
		t.Fatal("-dlv-binary is required")
	}
	manifest := dlvBinaryManifest(t)
	var fx fixture
	var fixturePlan dlvFixturePlan
	raw, err := os.ReadFile(filepath.Join(home, "fixture.json"))
	must(t, err)
	must(t, json.Unmarshal(raw, &fx))
	raw, err = os.ReadFile(filepath.Join(home, "dlv-fixture-plan.json"))
	must(t, err)
	must(t, json.Unmarshal(raw, &fixturePlan))
	if fixturePlan.Version != 1 || fixturePlan.ShardsPerCell != shards || fixturePlan.Repeats != *dlvRepeats || fixturePlan.NamespaceCells != len(plans) || fixturePlan.FixtureSHA256 != fx.SHA256 || fx.Streams != len(plans)*shards {
		t.Fatal("fixture namespaces differ; use a fresh setup with matching shards and control repeats")
	}
	stateFilter := *dlvStateFilter
	if stateFilter == "" && op == "probe" {
		stateFilter = "exact"
	}
	rfFilter := *dlvRFFilter
	if rfFilter == "" {
		rfFilter = "1,2"
		if op == "probe" {
			rfFilter = "2"
		}
	}
	shapes, err := dlvSelected(*dlvShapeFilter, dlvShapes)
	must(t, err)
	states, err := dlvSelected(stateFilter, dlvStates)
	must(t, err)
	rfs, err := dlvSelected(rfFilter, []string{"1", "2"})
	must(t, err)
	tiers, err := dlvSelected(*dlvTierFilter, []string{"on", "off"})
	must(t, err)
	ready(t, vlBase()+"/health")
	ready(t, lokiBase()+"/ready")
	rec := newRecorder(t, "dlv-"+op)
	for i := 0; i < 3; i++ {
		port, backend, peer := ports(i)
		if i == 0 {
			observer(t, backend, vlBase(), "vl", -1, rec)
		}
		observer(t, peer, "http://127.0.0.1:"+number(port), "peer", i, rec)
	}
	selected := []dlvCell{}
	for _, c := range plans {
		tier := "off"
		if c.Tier0 {
			tier = "on"
		}
		if shapes[c.Shape] && states[c.State] && rfs[number(c.RF)] && tiers[tier] && (*dlvRepeatIndex < 0 || c.Repeat == *dlvRepeatIndex) {
			selected = append(selected, c)
		}
	}
	if len(selected) == 0 {
		t.Fatal("no selected DLV cells")
	}
	manifest["run_id"] = runID
	manifest["operation"] = op
	manifest["samples_per_cell"] = samples
	manifest["shards_per_cell"] = shards
	manifest["selected_cells"] = selected
	report := map[string]any{"manifest": manifest, "fixture": fx, "fixture_plan": fixturePlan, "cell_files": []map[string]any{}, "wire_log": rec.path}
	must(t, os.MkdirAll(filepath.Join(home, "cells", runID), 0700))
	reportPath := filepath.Join(home, "dlv-results-"+runID+".json")
	manifest["namespace_claim_policy"] = "persistent exclusive fixture+cell claim before any priming; reuse is rejected even after interrupted runs"
	for _, c := range selected {
		claim, err := dlvClaimNamespace(home, fx.SHA256, dlvClaimProvenance{RunID: runID, SourceRef: *dlvSource, BinarySHA256: manifest["binary_sha256"].(string), ClaimedAt: time.Now().UTC(), Cell: c, Config: dlvConfig(c)})
		must(t, err)
		var cellReport dlvCellReport
		t.Run(c.ID, func(t *testing.T) { cellReport = dlvRunCell(t, *dlvBinary, c, fx, rec) })
		if cellReport.Cell.ID == "" {
			cellReport.Cell = c
			cellReport.Errors = append(cellReport.Errors, "cell aborted; see test output")
		}
		cellPath := filepath.Join(home, "cells", runID, c.ID+".json")
		save(t, cellPath, cellReport)
		report["cell_files"] = append(report["cell_files"].([]map[string]any), map[string]any{"cell": c, "path": cellPath, "sha256": digest(t, cellPath), "namespace_claim": claim, "summary": cellReport.Summary, "errors": cellReport.Errors})
		rec.flush(t)
		save(t, reportPath, report)
		if len(cellReport.Errors) > 0 {
			t.Errorf("%s: %v", c.ID, cellReport.Errors)
		}
	}
	rec.flush(t)
	report["wire_ledger_sha256"] = digest(t, rec.path)
	save(t, reportPath, report)
	save(t, filepath.Join(home, "dlv-latest.json"), report)
	t.Logf("DLV report%s cells%d; no universal latency/throughput claim", reportPath, len(selected))
}

func TestDLVSnapshotRefusesLiveAndCopiesIndependently(t *testing.T) {
	source, destination := t.TempDir(), t.TempDir()
	must(t, os.WriteFile(filepath.Join(source, "cache.db"), []byte("closed snapshot"), 0600))
	must(t, os.WriteFile(filepath.Join(source, "proxy.log"), []byte("not cache"), 0600))
	p := &process{dir: source, cmd: &exec.Cmd{}}
	if err := dlvCopyClosedCaches([]*process{p}, []string{destination}); err == nil {
		t.Fatal("copied live cache")
	}
	p.cmd = nil
	must(t, dlvCopyClosedCaches([]*process{p}, []string{destination}))
	got, err := os.ReadFile(filepath.Join(destination, "cache.db"))
	must(t, err)
	if string(got) != "closed snapshot" {
		t.Fatal("snapshot bytes changed")
	}
	if _, err := os.Stat(filepath.Join(destination, "proxy.log")); !os.IsNotExist(err) {
		t.Fatal("copied non-cache file")
	}
	must(t, os.WriteFile(filepath.Join(destination, "cache.db"), []byte("different wave"), 0600))
	got, err = os.ReadFile(filepath.Join(source, "cache.db"))
	must(t, err)
	if string(got) != "closed snapshot" {
		t.Fatal("wave modified shared primed snapshot")
	}
	if err := dlvCopyClosedCaches([]*process{p}, []string{destination}); err == nil {
		t.Fatal("overwrote existing wave")
	}
}
func TestDLVHotnessCountersNeverBecomeCompatMetrics(t *testing.T) {
	key := "loki_vl_proxy_cache_tier_hits_total{tier=\"l0\"}"
	for _, enabled := range []bool{true, false} {
		o := dlvObservation{Before: resources{Valid: true, Metrics: map[string]float64{key: 2}}, After: resources{Valid: true, Metrics: map[string]float64{key: 5}}, Valid: true}
		dlvAttributeObservation(&o, dlvCell{Tier0: enabled})
		if o.ProxyLoki.Tier0.Hits != nil || o.ProxyLoki.Tier0.Misses != nil || o.ProxyLoki.Tier0.Objects != nil || o.ProxyVL.CacheLayerMetrics != nil {
			t.Fatal("aggregate physical counter attributed to semantic layer")
		}
		if got := o.SharedStorage.TierCounterDeltas[key]; got == nil || *got != 3 {
			t.Fatal("unattributed counter missing")
		}
		want := "disabled"
		if enabled {
			want = "enabled"
		}
		if o.ProxyLoki.Tier0.State != want {
			t.Fatal("disabled cache described as lookup miss")
		}
	}
}
func TestDLVSummaryExcludesMatchingHTTPErrors(t *testing.T) {
	rows := []dlvResponse{
		{Successful: true, Equal: true, ProxyStatus: 500, LokiStatus: 500, ReferenceClass: "reference_failure", ProxyMS: 1, LokiMS: 1},
		{Successful: true, Equal: true, ProxyStatus: 200, LokiStatus: 200, ReferenceClass: "success_nonempty", ProxyMS: 2, LokiMS: 3},
	}
	got := dlvSummary([]dlvBatch{{Rows: rows}})
	if got["successful_valid_200"] != 1 {
		t.Fatalf("matching HTTP errors counted as successful latency: %v", got)
	}
}
func TestDLVClassifyRejectsMalformedTimestamp(t *testing.T) {
	body := []byte("{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[{\"metric\":{},\"values\":[[{},\"2\"]]}]}}")
	if _, err := dlvClassify(200, body); err == nil {
		t.Fatal("accepted malformed timestamp")
	}
}

func TestDLVRejectsInvalidPointOrderAndNonfiniteDelta(t *testing.T) {
	for _, points := range []string{"[[null,\"2\"]]", "[[2,\"2\"],[1,\"3\"]]", "[[1,\"2\"],[1,\"3\"]]"} {
		body := []byte("{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[{\"metric\":{},\"values\":" + points + "}]}}")
		if _, err := dlvClassify(200, body); err == nil {
			t.Fatalf("accepted invalid points %s", points)
		}
	}
	if got := dlvDelta(map[string]float64{"x": -math.MaxFloat64}, map[string]float64{"x": math.MaxFloat64}, "x"); got != nil {
		t.Fatal("nonfinite computed delta")
	}
	body := []byte("{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[{\"metric\":{},\"values\":[]}]}}")
	if got, err := dlvClassify(200, body); err != nil || got != "success_empty" {
		t.Fatalf("empty points %s %v", got, err)
	}
}

func TestDLVObserveReadsWriteOnlyLedger(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wire.jsonl")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)
	must(t, err)
	defer file.Close()
	rec := &recorder{prefix: []wireTotals{{}}, path: path, file: file, buffer: bufio.NewWriter(file)}
	rec.encoder = json.NewEncoder(rec.buffer)
	batch := dlvObserve(t, nil, dlvCell{}, rec, func() []dlvResponse {
		rec.add(wireEvent{Kind: "vl", Method: "GET", Path: "/select/logsql/query", Status: 200, ResponseBytes: 42})
		return nil
	})
	if len(batch.Observation.BackendRequests) != 1 || batch.Observation.BackendRequests[0].ResponseBytes != 42 {
		t.Fatalf("missing observed ledger event: %+v", batch.Observation)
	}
}

func TestDLVNamespaceClaimCannotBeReused(t *testing.T) {
	dir := t.TempDir()
	cell := dlvCell{ID: "DLV1-exact-rf2-tier0true-repeat0"}
	proof := dlvClaimProvenance{RunID: "first", SourceRef: "source", BinarySHA256: "binary", Cell: cell, Config: dlvConfig(cell), ClaimedAt: time.Unix(123, 0).UTC()}
	claim, err := dlvClaimNamespace(dir, strings.Repeat("a", 64), proof)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	first, err := os.ReadFile(claim)
	must(t, err)
	proof.RunID = "second"
	if _, err := dlvClaimNamespace(dir, strings.Repeat("a", 64), proof); err == nil {
		t.Fatal("accepted previously primed reference namespace")
	}
	after, err := os.ReadFile(claim)
	must(t, err)
	if !bytes.Equal(first, after) {
		t.Fatal("claim provenance overwritten")
	}
	proof.Cell.ID = "DLV1-exact-rf2-tier0true-repeat1"
	if _, err := dlvClaimNamespace(dir, strings.Repeat("a", 64), proof); err != nil {
		t.Fatal("independent repeat rejected", err)
	}
	if _, err := dlvClaimNamespace(dir, strings.Repeat("b", 64), proof); err != nil {
		t.Fatal("independent fixture rejected", err)
	}
}
func TestDLVSetupRefusesExistingFixture(t *testing.T) {
	dir := t.TempDir()
	if err := dlvSetupUnused(dir); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"fixture.json", "dlv-fixture-plan.json"} {
		must(t, os.WriteFile(filepath.Join(dir, name), []byte("{}"), 0600))
		if err := dlvSetupUnused(dir); err == nil {
			t.Fatalf("setup would reseed existing %s", name)
		}
		must(t, os.Remove(filepath.Join(dir, name)))
	}
}

type dlvClaimProvenance struct {
	RunID, SourceRef, BinarySHA256 string
	ClaimedAt                      time.Time
	Cell                           dlvCell
	Config                         config
}

func dlvClaimNamespace(dir, fixtureSHA string, proof dlvClaimProvenance) (string, error) {
	if decoded, err := hex.DecodeString(fixtureSHA); err != nil || len(decoded) != sha256.Size || proof.Cell.ID == "" || filepath.Base(proof.Cell.ID) != proof.Cell.ID {
		return "", fmt.Errorf("invalid fixture or cell claim identity")
	}
	claimDir := filepath.Join(dir, "namespace-claims", fixtureSHA)
	if err := os.MkdirAll(claimDir, 0700); err != nil {
		return "", err
	}
	path := filepath.Join(claimDir, proof.Cell.ID+".json")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return "", fmt.Errorf("reference namespace already claimed or unavailable (%s); use an unused repeat or fresh output/project: %w", path, err)
	}
	// Retain a partial claim if writing fails: no later invocation may silently
	// treat an interrupted run as a pristine reference-cache namespace.
	defer file.Close()
	if err := json.NewEncoder(file).Encode(proof); err != nil {
		return "", err
	}
	if err := file.Sync(); err != nil {
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	return path, nil
}
func dlvSetupUnused(dir string) error {
	for _, name := range []string{"fixture.json", "dlv-fixture-plan.json", "dlv-setup-attempt.json"} {
		_, err := os.Lstat(filepath.Join(dir, name))
		if err == nil {
			return fmt.Errorf("refuse reseeding existing fixture manifest %s; cleanup and use a fresh output directory/project", name)
		}
		if !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func TestDLVFailedSetupAttemptCannotBeReseeded(t *testing.T) {
	dir := t.TempDir()
	proof := map[string]any{"run_id": "failed-before-manifest", "project": "isolated-project"}
	if err := dlvClaimSetupAttempt(dir, proof); err != nil {
		t.Fatal(err)
	}
	if err := dlvSetupUnused(dir); err == nil {
		t.Fatal("partial setup without fixture manifest is reusable")
	}
	if err := dlvClaimSetupAttempt(dir, map[string]any{"run_id": "retry"}); err == nil {
		t.Fatal("setup attempt marker overwritten")
	}
	body, err := os.ReadFile(filepath.Join(dir, "dlv-setup-attempt.json"))
	must(t, err)
	if !bytes.Contains(body, []byte("failed-before-manifest")) {
		t.Fatal("lost failed-attempt provenance")
	}
}
func dlvClaimSetupAttempt(dir string, proof any) error {
	file, err := os.OpenFile(filepath.Join(dir, "dlv-setup-attempt.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("setup already attempted or marker unavailable; cleanup and use a fresh output/project: %w", err)
	}
	// A failed setup may already have ingested some batches. Retain the marker
	// on every failure and through cleanup, even before fixture.json exists.
	defer file.Close()
	if err := json.NewEncoder(file).Encode(proof); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	return file.Close()
}
