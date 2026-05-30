package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	fj "github.com/valyala/fastjson"
)

const (
	maxBatchCombos  = 2000
	maxBatchBuckets = 60 // skip batching when n_buckets > this; must be ≥ maxDrilldownStatsBucketsShort
)

type fieldBatchEntry struct {
	lokiField      string   // Loki-side field name (used in output metric key)
	primaryVLField string   // raw VL field name for raw-body metric lookup
	vlFields       []string // all VL by() fields contributed by this entry
	resultCh       chan []byte
}

type fieldBatch struct {
	batcher   *drilldownFieldBatcher
	key       string
	orgID     string
	cleanBase string
	startRaw  string
	endRaw    string
	stepRaw   string

	mu       sync.Mutex
	entries  []fieldBatchEntry
	launched bool
	timer    *time.Timer
}

type drilldownFieldBatcher struct {
	proxy     *Proxy
	window    time.Duration
	maxFields int

	mu      sync.Mutex
	pending map[string]*fieldBatch

	sem chan struct{}
}

func newDrilldownFieldBatcher(p *Proxy, windowDur time.Duration, maxFields int) *drilldownFieldBatcher {
	if windowDur <= 0 || maxFields <= 0 {
		return nil
	}
	sem := make(chan struct{}, 2)
	sem <- struct{}{}
	sem <- struct{}{}
	return &drilldownFieldBatcher{
		proxy:     p,
		window:    windowDur,
		maxFields: maxFields,
		pending:   make(map[string]*fieldBatch),
		sem:       sem,
	}
}

func fieldBatchKey(orgID, cleanBase, startRaw, endRaw, stepRaw string) string {
	startBucketed := bucketTimestampString(startRaw, 30*time.Second)
	endBucketed := bucketTimestampString(endRaw, 30*time.Second)
	return orgID + "\x00" + cleanBase + "\x00" + startBucketed + "\x00" + endBucketed + "\x00" + stepRaw
}

func bucketCount(startRaw, endRaw, stepRaw string) int {
	startNs, startOk := parseLokiTimeToUnixNano(startRaw)
	endNs, endOk := parseLokiTimeToUnixNano(endRaw)
	stepDur, ok := parsePositiveStepDuration(stepRaw)
	if !ok || !startOk || !endOk || stepDur <= 0 || startNs <= 0 || endNs <= startNs {
		return 0
	}
	rangeNs := endNs - startNs
	stepNs := stepDur.Nanoseconds()
	n := int(rangeNs / stepNs)
	if n < 1 {
		n = 1
	}
	return n
}

func (b *drilldownFieldBatcher) submit(ctx context.Context, orgID, cleanBase, lokiField, primaryVLField string, vlFields []string, startRaw, endRaw, stepRaw string) []byte {
	if n := bucketCount(startRaw, endRaw, stepRaw); n > maxBatchBuckets {
		return nil
	}
	key := fieldBatchKey(orgID, cleanBase, startRaw, endRaw, stepRaw)

	b.mu.Lock()
	batch, ok := b.pending[key]
	if !ok {
		batch = &fieldBatch{
			batcher:   b,
			key:       key,
			orgID:     orgID,
			cleanBase: cleanBase,
			startRaw:  startRaw,
			endRaw:    endRaw,
			stepRaw:   stepRaw,
		}
		batch.timer = time.AfterFunc(b.window, batch.fire)
		b.pending[key] = batch
	}
	b.mu.Unlock()

	batch.mu.Lock()
	if batch.launched {
		batch.mu.Unlock()
		return nil
	}
	resultCh := make(chan []byte, 1)
	batch.entries = append(batch.entries, fieldBatchEntry{
		lokiField:      lokiField,
		primaryVLField: primaryVLField,
		vlFields:       vlFields,
		resultCh:       resultCh,
	})
	shouldFire := len(batch.entries) >= b.maxFields
	batch.mu.Unlock()

	if shouldFire {
		batch.timer.Stop()
		go batch.fire()
	}

	select {
	case body := <-resultCh:
		return body
	case <-ctx.Done():
		return nil
	}
}

func (batch *fieldBatch) fire() {
	b := batch.batcher

	b.mu.Lock()
	delete(b.pending, batch.key)
	b.mu.Unlock()

	batch.mu.Lock()
	if batch.launched {
		batch.mu.Unlock()
		return
	}
	batch.launched = true
	entries := make([]fieldBatchEntry, len(batch.entries))
	copy(entries, batch.entries)
	batch.mu.Unlock()

	distribute := func(results map[string][]byte) {
		for _, e := range entries {
			e.resultCh <- results[e.lokiField]
		}
	}

	if len(entries) == 0 {
		return
	}

	// Collect unique VL field names preserving first-occurrence order.
	seen := make(map[string]bool)
	var allVLFields []string
	for _, e := range entries {
		for _, f := range e.vlFields {
			if !seen[f] {
				seen[f] = true
				allVLFields = append(allVLFields, f)
			}
		}
	}

	batchQuery := batch.cleanBase +
		" | stats by (" + strings.Join(allVLFields, ", ") + ") count() as _c" +
		" | sort by (_c desc)" +
		" | limit " + strconv.Itoa(maxBatchCombos)

	ctx30s, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Acquire semaphore — limits concurrent batch VL calls to 2.
	select {
	case <-b.sem:
	case <-ctx30s.Done():
		distribute(nil)
		return
	}
	defer func() { b.sem <- struct{}{} }()

	params := buildStatsQueryRangeParams(batchQuery, batch.startRaw, batch.endRaw, batch.stepRaw)

	resp, err := b.proxy.vlPost(ctx30s, "/select/logsql/stats_query_range", params)
	if err != nil {
		distribute(nil)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		distribute(nil)
		return
	}

	body, err := readBodyLimited(resp.Body, maxDrilldownResponseBytes)
	if err != nil {
		distribute(nil)
		return
	}

	// Use raw VL body (no label translation) so marginalizeBatchResult can look up
	// fields by their VL names (primaryVLField). Translation renames/removes keys
	// (e.g., level→detected_level) which breaks the per-field metric lookup.
	results := marginalizeBatchResult(body, entries)

	for lokiField, data := range results {
		results[lokiField] = limitLokiMatrixSeries(data, maxDrilldownSeries)
	}

	distribute(results)
}

func marginalizeBatchResult(translated []byte, entries []fieldBatchEntry) map[string][]byte {
	var p fj.Parser
	v, err := p.ParseBytes(translated)
	if err != nil {
		return nil
	}

	resultArr := v.GetArray("data", "result")
	if len(resultArr) == 0 {
		return nil
	}

	// lokiField → fieldValue → unixSec → accumulated count
	type marginals = map[string]map[int64]int64
	fieldMarginals := make(map[string]marginals)

	// vlField → lokiField: raw VL body uses VL field names (e.g. "level"), output uses Loki names.
	vlToLoki := make(map[string]string, len(entries))
	for _, e := range entries {
		vlToLoki[e.primaryVLField] = e.lokiField
		if fieldMarginals[e.lokiField] == nil {
			fieldMarginals[e.lokiField] = make(marginals)
		}
	}

	// Collect all timestamps for alignment.
	tsSet := make(map[int64]bool)

	for _, item := range resultArr {
		metric := item.GetObject("metric")
		values := item.GetArray("values")
		if metric == nil || len(values) == 0 {
			continue
		}

		// Build a label map from this metric object once.
		labelMap := make(map[string]string)
		metric.Visit(func(k []byte, val *fj.Value) {
			labelMap[string(k)] = string(val.GetStringBytes())
		})

		for vlField, lokiField := range vlToLoki {
			fieldValue, ok := labelMap[vlField]
			if !ok {
				continue
			}
			if fieldMarginals[lokiField][fieldValue] == nil {
				fieldMarginals[lokiField][fieldValue] = make(map[int64]int64)
			}
			for _, pair := range values {
				arr := pair.GetArray()
				if len(arr) < 2 {
					continue
				}
				ts := arr[0].GetInt64()
				countStr := string(arr[1].GetStringBytes())
				cnt, _ := strconv.ParseInt(countStr, 10, 64)
				fieldMarginals[lokiField][fieldValue][ts] += cnt
				tsSet[ts] = true
			}
		}
	}

	// Sorted global timestamp list.
	allTS := make([]int64, 0, len(tsSet))
	for ts := range tsSet {
		allTS = append(allTS, ts)
	}
	sort.Slice(allTS, func(i, j int) bool { return allTS[i] < allTS[j] })

	results := make(map[string][]byte, len(entries))
	for _, e := range entries {
		perValue := fieldMarginals[e.lokiField]
		if perValue == nil {
			continue
		}

		// Rank field values by total count descending.
		type valCount struct {
			val   string
			total int64
		}
		ranked := make([]valCount, 0, len(perValue))
		for val, tsCounts := range perValue {
			var total int64
			for _, c := range tsCounts {
				total += c
			}
			ranked = append(ranked, valCount{val, total})
		}
		sort.Slice(ranked, func(i, j int) bool {
			if ranked[i].total != ranked[j].total {
				return ranked[i].total > ranked[j].total
			}
			return ranked[i].val < ranked[j].val
		})

		var buf []byte
		buf = append(buf, `{"status":"success","data":{"resultType":"matrix","result":[`...)

		for si, vc := range ranked {
			if si > 0 {
				buf = append(buf, ',')
			}
			buf = append(buf, `{"metric":{`...)
			buf = appendJSONQuoted(buf, e.lokiField)
			buf = append(buf, ':')
			buf = appendJSONQuoted(buf, vc.val)
			buf = append(buf, `},"values":[`...)

			tsCounts := perValue[vc.val]
			for ti, ts := range allTS {
				if ti > 0 {
					buf = append(buf, ',')
				}
				cnt := tsCounts[ts]
				buf = append(buf, '[')
				buf = strconv.AppendInt(buf, ts, 10)
				buf = append(buf, ',')
				buf = appendJSONQuoted(buf, strconv.FormatInt(cnt, 10))
				buf = append(buf, ']')
			}
			buf = append(buf, `]}`...)
		}

		buf = append(buf, `]}}`...)
		results[e.lokiField] = buf
	}

	return results
}

func appendJSONQuoted(buf []byte, s string) []byte {
	b, _ := json.Marshal(s)
	return append(buf, b...)
}
