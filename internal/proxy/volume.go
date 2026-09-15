package proxy

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	fj "github.com/valyala/fastjson"
)

// Loki volume semantics (pkg/storage/stores/index/seriesvolume, v3.7):
//   - volume is the number of bytes of the matching log lines, not a line count;
//   - limit defaults to 100 and keeps the largest volumes of each bucket,
//     ties broken by name;
//   - aggregateBy=series (default) names a result by the labels in the
//     targetLabels list, or by the selector's label names when targetLabels
//     is empty; aggregateBy=labels names it by a single label name;
//   - every targetLabels entry is required: streams without it are skipped;
//   - an instant volume is stamped at the request end; a volume_range bucket
//     is stamped at its end minus 1ms, and the last bucket at the request end.
const (
	defaultVolumeSeriesLimit  = 100
	volumeAggregateBySeries   = "series"
	volumeAggregateByLabels   = "labels"
	volumeBytesField          = "_b"
	volumeStreamField         = "_stream"
	lokiVolumeSplitGap        = time.Millisecond
	lokiDefaultVolumeLookback = time.Hour

	volumeLevelUnpackPipes = " | unpack_json from _msg fields (level, detected_level) keep_original_fields" +
		" | unpack_logfmt from _msg fields (level, detected_level) keep_original_fields"
)

var (
	errVolumeInvalidAggregateBy = errors.New("invalid aggregation option")
	errVolumeNonPositiveLimit   = errors.New("limit must be a positive value")
	errVolumeEndBeforeStart     = errors.New("end timestamp must not be before or equal to start time")
	errVolumeNonPositiveStep    = errors.New("zero or negative query resolution step widths are not accepted. Try a positive integer")
)

func sumHitsValues(body []byte) int {
	hits := parseHits(body)
	total := 0
	for _, h := range hits.Hits {
		for _, v := range h.Values {
			total += v
		}
	}
	return total
}

// volumeRequest is a parsed Loki volume or volume_range request.
type volumeRequest struct {
	query    string
	startNs  int64
	endNs    int64
	stepNs   int64 // zero for the instant volume endpoint
	targets  []string
	byLabels bool
	limit    int
}

func (p *Proxy) translateVolumeMetric(fields map[string]string) map[string]string {
	if fields == nil {
		return nil
	}
	translated := fields
	if p != nil && p.labelTranslator != nil && !p.labelTranslator.IsPassthrough() {
		translated = p.labelTranslator.TranslateLabelsMap(fields)
	}
	if translated == nil {
		return nil
	}
	serviceSignal := hasServiceSignal(translated)
	ensureSyntheticServiceName(translated)
	if !serviceSignal && strings.TrimSpace(translated["service_name"]) == unknownServiceName {
		delete(translated, "service_name")
	}
	return translated
}

func normalizeDrilldownGroupingLabel(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	switch strings.ToLower(raw) {
	case "$__all", "__all":
		return ""
	default:
		return raw
	}
}

func requestedVolumeTargetLabels(r *http.Request) string {
	if r == nil {
		return ""
	}
	if direct := strings.TrimSpace(r.FormValue("targetLabels")); direct != "" {
		return direct
	}
	for _, key := range []string{"drillDownLabel", "fieldBy", "labelBy", "var-fieldBy", "var-labelBy"} {
		if candidate := normalizeDrilldownGroupingLabel(r.FormValue(key)); candidate != "" {
			return candidate
		}
	}
	return ""
}

// parseVolumeRequest applies Loki's volume parameter rules
// (pkg/loghttp.ParseVolumeInstantQuery / ParseVolumeRangeQuery).
func parseVolumeRequest(r *http.Request, rangeQuery bool, now time.Time) (volumeRequest, error) {
	req := volumeRequest{query: r.FormValue("query"), limit: defaultVolumeSeriesLimit}

	switch r.FormValue("aggregateBy") {
	case "", volumeAggregateBySeries:
	case volumeAggregateByLabels:
		req.byLabels = true
	default:
		return req, errVolumeInvalidAggregateBy
	}

	if raw := strings.TrimSpace(r.FormValue("limit")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return req, err
		}
		switch {
		case n < 0:
			return req, errVolumeNonPositiveLimit
		case n > 0:
			req.limit = n
		}
	}

	req.endNs = now.UnixNano()
	if raw := strings.TrimSpace(firstNonEmpty(r.FormValue("end"), r.FormValue("to"))); raw != "" {
		ns, ok := parseLokiTimeToUnixNano(raw)
		if !ok {
			return req, errors.New("cannot parse \"" + raw + "\" to a valid timestamp")
		}
		req.endNs = ns
	}
	// Loki's default start: min(end, now) - 1h.
	req.startNs = req.endNs
	if nowNs := now.UnixNano(); nowNs < req.startNs {
		req.startNs = nowNs
	}
	req.startNs -= int64(lokiDefaultVolumeLookback)
	if raw := strings.TrimSpace(firstNonEmpty(r.FormValue("start"), r.FormValue("from"))); raw != "" {
		ns, ok := parseLokiTimeToUnixNano(raw)
		if !ok {
			return req, errors.New("cannot parse \"" + raw + "\" to a valid timestamp")
		}
		req.startNs = ns
	}
	if req.endNs < req.startNs {
		return req, errVolumeEndBeforeStart
	}

	if rangeQuery {
		if raw := strings.TrimSpace(r.FormValue("step")); raw != "" {
			step, ok := parsePositiveStepDuration(raw)
			if !ok {
				return req, errVolumeNonPositiveStep
			}
			req.stepNs = int64(step)
		} else {
			// Loki's default range step: range/250, at least one second.
			req.stepNs = int64(math.Max(math.Floor(float64(req.endNs-req.startNs)/float64(time.Second)/250), 1)) * int64(time.Second)
		}
		if (req.endNs-req.startNs)/req.stepNs > lokiMaxPointsPerSeries {
			return req, errors.New(errLokiStepTooSmall)
		}
	}

	req.targets = splitTargetLabels(requestedVolumeTargetLabels(r))
	return req, nil
}

// handleVolume answers Loki's instant volume from VictoriaLogs sum_len(_msg).
// Loki: GET /loki/api/v1/index/volume?query={...}&start=...&end=...
// Response: {"status":"success","data":{"resultType":"vector","result":[{"metric":{...},"value":[end,"bytes"]}]}}
func (p *Proxy) handleVolume(w http.ResponseWriter, r *http.Request) {
	p.serveVolume(w, r, "volume", false)
}

// handleVolumeRange answers Loki's volume_range from VictoriaLogs sum_len(_msg)
// bucketed by step.
// Loki: GET /loki/api/v1/index/volume_range?query={...}&start=...&end=...&step=60
// Response: {"status":"success","data":{"resultType":"matrix","result":[{"metric":{...},"values":[[ts,"bytes"],...]}]}}
func (p *Proxy) handleVolumeRange(w http.ResponseWriter, r *http.Request) {
	p.serveVolume(w, r, "volume_range", true)
}

func (p *Proxy) serveVolume(w http.ResponseWriter, r *http.Request, endpoint string, rangeQuery bool) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, endpoint) {
		return
	}
	r = p.withRequestScope(r)
	orgID := r.Header.Get("X-Scope-OrgID")
	req, err := parseVolumeRequest(r, rangeQuery, start)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		p.metrics.RecordRequest(endpoint, http.StatusBadRequest, time.Since(start))
		return
	}
	cacheKey := p.canonicalReadCacheKey(endpoint, orgID, r)
	if cached, remaining, _, ok := p.endpointReadCacheEntry(endpoint, cacheKey); ok {
		if !p.shouldBypassRecentTailCache(endpoint, remaining, r) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(cached)
			p.metrics.RecordRequest(endpoint, http.StatusOK, time.Since(start))
			p.metrics.RecordCacheHit()
			if p.shouldRefreshLabelsInBackground(remaining, CacheTTLs[endpoint]) {
				p.refreshVolumeCacheAsync(endpoint, orgID, cacheKey, req, p.snapshotForwardedAuth(r))
			}
			return
		}
	}
	p.metrics.RecordCacheMiss()

	result, err := p.computeVolume(r.Context(), req)
	if err != nil {
		if p.serveStaleReadCacheOnError(w, endpoint, cacheKey, start, err) {
			return
		}
		status := statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
		p.metrics.RecordRequest(endpoint, status, time.Since(start))
		return
	}
	p.setEndpointJSONCacheWithTTL(endpoint, cacheKey, CacheTTLs[endpoint], result)
	p.writeJSON(w, result)
	p.metrics.RecordRequest(endpoint, http.StatusOK, time.Since(start))
}

func (p *Proxy) refreshVolumeCacheAsync(endpoint, orgID, cacheKey string, req volumeRequest, savedReq *http.Request) {
	refreshKey := "refresh:" + endpoint + ":" + cacheKey
	go func() {
		_, err, _ := p.labelRefreshGroup.Do(refreshKey, func() (interface{}, error) {
			ctx, cancel := context.WithTimeout(context.Background(), p.labelBackgroundTimeout())
			defer cancel()
			if orgID != "" {
				ctx = context.WithValue(ctx, orgIDKey, orgID)
			}
			if savedReq != nil {
				ctx = context.WithValue(ctx, origRequestKey, savedReq)
			}
			result, err := p.computeVolume(ctx, req)
			if err == nil {
				p.setEndpointJSONCacheWithTTL(endpoint, cacheKey, CacheTTLs[endpoint], result)
			}
			return nil, err
		})
		if err != nil {
			p.log.Debug("background volume refresh failed", "endpoint", endpoint, "cache_key", cacheKey, "error", err)
		}
	}()
}

// volumeSelectorLabelNames returns the label names of the query's stream
// selector, which name aggregateBy=series results when targetLabels is empty.
func volumeSelectorLabelNames(query string) []string {
	selector, _, ok := splitLeadingSelector(query)
	if !ok {
		return nil
	}
	var names []string
	for _, matcher := range splitSelectorMatchers(selector[1 : len(selector)-1]) {
		idx := strings.IndexAny(matcher, "=!")
		if idx <= 0 {
			continue
		}
		names = appendUniqueStrings(names, strings.TrimSpace(matcher[:idx]))
	}
	return names
}

// volumePlan is the VictoriaLogs stats query that computes one volume request.
type volumePlan struct {
	// keys are the Loki label names a result is named by. Empty means every
	// stream label (grouping by _stream).
	keys []string
	// required keys must be non-empty on a stream for it to count.
	required map[string]bool
	// groupFields are the VictoriaLogs fields of the stats by (...) clause.
	groupFields []string
	// filterFields get a non-empty filter pushed down to VictoriaLogs.
	filterFields []string
	// pushLimit sorts and limits each bucket in VictoriaLogs; only valid when
	// each VictoriaLogs row maps to exactly one Loki series.
	pushLimit    bool
	unpackLevels bool
}

// syntheticVolumeLabel reports the label multi-tenant fan-out fills in after
// the backend answers: it is neither a backend field nor required.
func syntheticVolumeLabel(name string) bool {
	return name == "__tenant_id__"
}

func (p *Proxy) planVolume(ctx context.Context, req volumeRequest, logsqlQuery string) volumePlan {
	plan := volumePlan{required: map[string]bool{}}
	keys := req.targets
	if len(keys) == 0 && !req.byLabels {
		keys = volumeSelectorLabelNames(req.query)
	}
	if len(keys) == 0 {
		plan.groupFields = []string{volumeStreamField}
		return plan
	}
	plan.keys = keys

	lookup := url.Values{}
	lookup.Set("query", logsqlQuery)
	lookup.Set("start", strconv.FormatInt(req.startNs, 10))
	lookup.Set("end", strconv.FormatInt(req.endNs, 10))

	// direct: every key is one VictoriaLogs field, so each stats row is one
	// Loki series and VictoriaLogs can apply the limit itself.
	direct := true
	for _, key := range keys {
		if syntheticVolumeLabel(key) {
			direct = false
			continue
		}
		if len(req.targets) > 0 && key != "detected_level" {
			plan.required[key] = true
		}
		switch key {
		case "service_name", "detected_level":
			direct = false
			if key == "detected_level" {
				plan.unpackLevels = true
			}
			plan.groupFields = appendUniqueStrings(plan.groupFields, p.derivedVolumeSourceFields([]string{key})...)
		default:
			fields := p.resolveTargetLabelFields(ctx, key, lookup)
			if len(fields) != 1 {
				direct = false
			} else if plan.required[key] {
				plan.filterFields = appendUniqueStrings(plan.filterFields, fields[0])
			}
			plan.groupFields = appendUniqueStrings(plan.groupFields, fields...)
		}
	}
	if len(plan.groupFields) == 0 {
		plan.groupFields = []string{volumeStreamField}
	}
	plan.pushLimit = direct && !req.byLabels
	return plan
}

// volumeStatsQuery renders the LogsQL stats query for a plan. The plain form
// (optimised=false) drops every optional pipe and groups by _stream only; it is
// the fallback when VictoriaLogs rejects the optimised rewrite.
func volumeStatsQuery(req volumeRequest, logsqlQuery string, plan volumePlan, optimised bool) string {
	var b strings.Builder
	b.Grow(len(logsqlQuery) + 128)
	b.WriteString(logsqlQuery)
	if req.stepNs > 0 {
		// stats_query_range aligns start/end to the step; Loki's first and last
		// buckets cover only the requested range.
		b.WriteString(" | filter _time:[")
		b.WriteString(time.Unix(0, req.startNs).UTC().Format(time.RFC3339Nano))
		b.WriteString(", ")
		b.WriteString(time.Unix(0, req.endNs).UTC().Format(time.RFC3339Nano))
		b.WriteString(")")
	}
	groupFields := []string{volumeStreamField}
	if optimised {
		if plan.unpackLevels && !strings.Contains(logsqlQuery, "unpack_json") && !strings.Contains(logsqlQuery, "unpack_logfmt") {
			// Unpack only the level fields and keep existing values: a full
			// unpack could replace _msg (changing its length) or _time.
			b.WriteString(volumeLevelUnpackPipes)
		}
		if len(plan.filterFields) > 0 {
			b.WriteString(" | filter")
			for _, field := range plan.filterFields {
				b.WriteString(" ")
				b.WriteString(quoteLogsQLIdent(field))
				b.WriteString(":*")
			}
		}
		groupFields = plan.groupFields
	}
	b.WriteString(" | stats by (")
	for i, field := range groupFields {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(quoteLogsQLIdent(field))
	}
	b.WriteString(") sum_len(_msg) as ")
	b.WriteString(volumeBytesField)
	if optimised && plan.pushLimit {
		// stats_query_range partitions sort by _time, so this keeps each bucket's
		// top N; the label fields break ties deterministically, like Loki's names.
		b.WriteString(" | sort by (")
		b.WriteString(volumeBytesField)
		b.WriteString(" desc")
		for _, field := range groupFields {
			b.WriteString(", ")
			b.WriteString(quoteLogsQLIdent(field))
		}
		b.WriteString(") limit ")
		b.WriteString(strconv.Itoa(req.limit))
	}
	return b.String()
}

func (p *Proxy) computeVolume(ctx context.Context, req volumeRequest) (map[string]interface{}, error) {
	query := req.query
	if strings.TrimSpace(query) == "" {
		query = "*"
	}
	logsqlQuery, err := p.translateQueryWithContext(ctx, query)
	if err != nil {
		return nil, err
	}
	plan := p.planVolume(ctx, req, logsqlQuery)
	optimisedQuery := volumeStatsQuery(req, logsqlQuery, plan, true)
	body, err := p.fetchVolumeStats(ctx, req, optimisedQuery)
	if err != nil {
		plainQuery := volumeStatsQuery(req, logsqlQuery, plan, false)
		if plainQuery == optimisedQuery || !isUpstreamQueryRejected(err) {
			return nil, err
		}
		// The optimised query is a proxy rewrite: retry the plain _stream form
		// so only the user's own query decides whether the request is invalid.
		plan.groupFields = []string{volumeStreamField}
		body, err = p.fetchVolumeStats(ctx, req, plainQuery)
		if err != nil {
			return nil, err
		}
	}
	return p.volumeResponse(req, plan, body), nil
}

func (p *Proxy) fetchVolumeStats(ctx context.Context, req volumeRequest, statsQuery string) ([]byte, error) {
	path := "/select/logsql/stats_query"
	var params url.Values
	if req.stepNs > 0 {
		path = "/select/logsql/stats_query_range"
		params = buildStatsQueryRangeParams(statsQuery,
			strconv.FormatInt(req.startNs, 10), strconv.FormatInt(req.endNs, 10),
			formatVLStep(strconv.FormatFloat(float64(req.stepNs)/float64(time.Second), 'f', -1, 64)))
	} else {
		params = url.Values{}
		params.Set("query", statsQuery)
		params.Set("start", strconv.FormatInt(req.startNs, 10))
		params.Set("end", strconv.FormatInt(req.endNs, 10))
	}
	resp, err := p.vlPost(ctx, path, params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return nil, p.redactedBackendStatusError("", resp.StatusCode, body)
	}
	return body, nil
}

// volumeBucketMillis returns the Loki timestamp of the bucket VictoriaLogs
// labels with its start: the bucket end minus 1ms, or the request end for the
// last bucket (pkg/util.ForInterval with endTimeInclusive).
func volumeBucketMillis(req volumeRequest, bucketStartSeconds float64) int64 {
	endMs := req.endNs / int64(time.Millisecond)
	if req.stepNs <= 0 {
		return endMs
	}
	bucketStartNs := int64(math.Round(bucketStartSeconds*1e3)) * int64(time.Millisecond)
	bucketEndNs := bucketStartNs + req.stepNs
	if bucketEndNs >= req.endNs {
		return endMs
	}
	return (bucketEndNs - int64(lokiVolumeSplitGap)) / int64(time.Millisecond)
}

// lokiVolumeSeriesName renders labels the way Loki names a series volume
// (Prometheus labels.String: sorted, `name="value"`, comma-space separated).
func lokiVolumeSeriesName(metric map[string]string) string {
	names := make([]string, 0, len(metric))
	for name := range metric {
		names = append(names, name)
	}
	sort.Strings(names)
	var b strings.Builder
	b.WriteByte('{')
	for i, name := range names {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(name)
		b.WriteByte('=')
		b.WriteString(strconv.Quote(metric[name]))
	}
	b.WriteByte('}')
	return b.String()
}

type volumeSample struct {
	ms    int64
	bytes uint64
}

type volumeSeries struct {
	name    string
	metric  map[string]string
	samples []volumeSample
}

// volumeNamesForRow maps one VictoriaLogs stats row to the Loki volume names it
// contributes to, following seriesvolume's aggregateBy rules.
func (p *Proxy) volumeNamesForRow(req volumeRequest, plan volumePlan, row map[string]string, metrics map[string]map[string]string) []string {
	var labels map[string]string
	if raw, ok := row[volumeStreamField]; ok {
		labels = make(map[string]string, len(row)+4)
		for k, v := range parseStreamLabels(raw) {
			labels[k] = v
		}
		for k, v := range row {
			if k != volumeStreamField {
				labels[k] = v
			}
		}
	} else {
		labels = row
	}
	translated := p.translateVolumeMetric(labels)
	// Loki's ingester names streams without a service label
	// service_name="unknown_service"; translateVolumeMetric drops that
	// synthetic value for other label surfaces.
	ensureSyntheticServiceName(translated)
	if plan.unpackLevels {
		ensureDetectedLevel(translated)
	}
	for key := range plan.required {
		if translated[key] == "" {
			return nil
		}
	}

	if req.byLabels {
		var names []string
		if len(plan.keys) == 0 {
			for name, value := range translated {
				if value != "" {
					names = append(names, name)
				}
			}
			return names
		}
		for _, key := range plan.keys {
			if translated[key] != "" {
				names = append(names, key)
			}
		}
		return names
	}

	metric := make(map[string]string, len(plan.keys))
	if len(plan.keys) == 0 {
		for name, value := range translated {
			if value != "" {
				metric[name] = value
			}
		}
	} else {
		for _, key := range plan.keys {
			value := translated[key]
			// Synthetic labels stay (empty) so fan-out can fill them in;
			// detected_level keeps the proxy's historical empty-level bucket.
			if value != "" || syntheticVolumeLabel(key) || key == "detected_level" {
				metric[key] = value
			}
		}
	}
	name := lokiVolumeSeriesName(metric)
	if _, ok := metrics[name]; !ok {
		metrics[name] = metric
	}
	return []string{name}
}

func (p *Proxy) volumeResponse(req volumeRequest, plan volumePlan, body []byte) map[string]interface{} {
	buckets := map[int64]map[string]uint64{}
	metrics := map[string]map[string]string{}

	var parser fj.Parser
	if v, err := parser.ParseBytes(body); err == nil {
		row := map[string]string{}
		var names []string
		add := func(point *fj.Value) {
			arr := point.GetArray()
			if len(arr) < 2 {
				return
			}
			ts, err := arr[0].Float64()
			if err != nil {
				return
			}
			bytes, err := strconv.ParseUint(string(arr[1].GetStringBytes()), 10, 64)
			if err != nil {
				return
			}
			ms := volumeBucketMillis(req, ts)
			bucket := buckets[ms]
			if bucket == nil {
				bucket = map[string]uint64{}
				buckets[ms] = bucket
			}
			for _, name := range names {
				bucket[name] += bytes
			}
		}
		for _, item := range v.GetArray("data", "result") {
			clear(row)
			if obj := item.GetObject("metric"); obj != nil {
				obj.Visit(func(key []byte, val *fj.Value) {
					if string(key) == "__name__" {
						return
					}
					row[string(key)] = string(val.GetStringBytes())
				})
			}
			names = p.volumeNamesForRow(req, plan, row, metrics)
			if len(names) == 0 {
				continue
			}
			if value := item.Get("value"); value != nil {
				add(value)
			}
			for _, point := range item.GetArray("values") {
				add(point)
			}
		}
	}

	// Each bucket keeps its top `limit` volumes (seriesvolume.MapToVolumeResponse).
	seriesByName := map[string]*volumeSeries{}
	for ms, bucket := range buckets {
		type entry struct {
			name  string
			bytes uint64
		}
		entries := make([]entry, 0, len(bucket))
		for name, bytes := range bucket {
			entries = append(entries, entry{name, bytes})
		}
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].bytes != entries[j].bytes {
				return entries[i].bytes > entries[j].bytes
			}
			return entries[i].name < entries[j].name
		})
		if len(entries) > req.limit {
			entries = entries[:req.limit]
		}
		for _, e := range entries {
			s := seriesByName[e.name]
			if s == nil {
				s = &volumeSeries{name: e.name}
				if req.byLabels {
					s.metric = map[string]string{e.name: ""}
				} else {
					s.metric = metrics[e.name]
				}
				seriesByName[e.name] = s
			}
			s.samples = append(s.samples, volumeSample{ms: ms, bytes: e.bytes})
		}
	}

	// queryrange.toPrometheusData: vector unless a series has several samples;
	// series ordered by their first sample's volume, then by name.
	resultType := "vector"
	series := make([]*volumeSeries, 0, len(seriesByName))
	for _, s := range seriesByName {
		sort.Slice(s.samples, func(i, j int) bool { return s.samples[i].ms < s.samples[j].ms })
		if len(s.samples) > 1 {
			resultType = "matrix"
		}
		series = append(series, s)
	}
	sort.Slice(series, func(i, j int) bool {
		if series[i].samples[0].bytes != series[j].samples[0].bytes {
			return series[i].samples[0].bytes > series[j].samples[0].bytes
		}
		return series[i].name < series[j].name
	})

	result := make([]map[string]interface{}, 0, len(series))
	for _, s := range series {
		item := map[string]interface{}{"metric": s.metric}
		if resultType == "vector" {
			item["value"] = volumeSamplePair(s.samples[0])
		} else {
			values := make([][]interface{}, 0, len(s.samples))
			for _, sample := range s.samples {
				values = append(values, volumeSamplePair(sample))
			}
			item["values"] = values
		}
		result = append(result, item)
	}
	return map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": resultType,
			"result":     result,
		},
	}
}

func volumeSamplePair(sample volumeSample) []interface{} {
	return []interface{}{float64(sample.ms) / 1e3, strconv.FormatUint(sample.bytes, 10)}
}
