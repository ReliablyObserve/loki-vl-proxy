package proxy

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"

	"github.com/grafana/jsonparser"
	fj "github.com/valyala/fastjson"
)

type statsJSONSlot struct {
	key        string
	inData     bool
	raw        []byte
	start, end int
}

type statsJSONEnvelope struct {
	parser fj.Parser
	value  *fj.Value
	slots  []statsJSONSlot
}

// parseStatsJSONEnvelope removes only the result arrays before parsing the
// remaining metadata. Series stay as views into body, rather than a tree of all
// samples. Using fastjson for the small skeleton preserves its key/metadata
// interpretation; unexpected or non-standard JSON uses the original path.
func parseStatsJSONEnvelope(body []byte) (*statsJSONEnvelope, bool) {
	if !json.Valid(body) || !statsJSONWithinParserDepth(body) {
		return nil, false
	}
	var arrays []statsJSONSlot
	for _, slot := range []statsJSONSlot{{key: "result", inData: true}, {key: "result"}, {key: "results"}} {
		path := []string{slot.key}
		if slot.inData {
			path = []string{"data", "result"}
		}
		raw, kind, end, err := jsonparser.Get(body, path...)
		if err != nil || kind != jsonparser.Array {
			continue
		}
		slot.raw, slot.start, slot.end = raw, end-len(raw), end
		arrays = append(arrays, slot)
	}
	metadataSize := len(body)
	for _, slot := range arrays {
		metadataSize -= len(slot.raw) - 2 // substitute []
	}
	if metadataSize < 0 || metadataSize > maxPooledStatsParserInputSize {
		return nil, false
	}
	positions := append([]statsJSONSlot(nil), arrays...)
	sort.Slice(positions, func(i, j int) bool { return positions[i].start < positions[j].start })
	metadata := make([]byte, 0, metadataSize)
	previous := 0
	for _, slot := range positions {
		if slot.start < previous || slot.end > len(body) {
			return nil, false
		}
		metadata = append(metadata, body[previous:slot.start]...)
		metadata = append(metadata, '[', ']')
		previous = slot.end
	}
	metadata = append(metadata, body[previous:]...)
	envelope := &statsJSONEnvelope{}
	value, err := envelope.parser.ParseBytes(metadata)
	if err != nil {
		return nil, false
	}
	envelope.value = value
	for _, slot := range arrays {
		array := value.Get(slot.key)
		if slot.inData {
			array = value.Get("data", "result")
		}
		if array == nil || array.Type() != fj.TypeArray || len(bytes.TrimSpace(slot.raw[1:len(slot.raw)-1])) == 0 {
			continue
		}
		envelope.slots = append(envelope.slots, slot)
	}
	return envelope, true
}

// A per-series parse starts at a shallower depth than a whole-envelope parse.
// Conservatively fall back near fastjson's depth boundary so splitting cannot
// accept a response the reference parser rejects. JSON syntax is validated first.
func statsJSONWithinParserDepth(body []byte) bool {
	depth := 0
	quoted := false
	for i := 0; i < len(body); i++ {
		c := body[i]
		if quoted {
			switch c {
			case '\\':
				i++
			case '"':
				quoted = false
			}
			continue
		}
		switch c {
		case '"':
			quoted = true
		case '{', '[':
			depth++
			if depth >= fj.MaxDepth {
				return false
			}
		case '}', ']':
			depth--
		}
	}
	return true
}

// eachStatsJSONSeries reuses one parser across all series. Its arena therefore
// scales with the largest series, not the sum of every series' samples. One very
// large series can still require a large arena; this is not a process budget.
func eachStatsJSONSeries(raw []byte, parser *fj.Parser, largest *int, visit func(*fj.Value)) bool {
	supported := true
	_, err := jsonparser.ArrayEach(raw, func(series []byte, kind jsonparser.ValueType, _ int, parseErr error) {
		if !supported {
			return
		}
		if kind != jsonparser.Object || parseErr != nil {
			supported = false
			return
		}
		if len(series) > *largest {
			*largest = len(series)
		}
		value, err := parser.ParseBytes(series)
		if err != nil {
			supported = false
			return
		}
		visit(value)
	})
	return err == nil && supported
}

// eachStatsJSONSeriesUntil is eachStatsJSONSeries for a pass that may stop
// early: once visit returns false no further series is parsed or checked, and
// it reports true. A caller that stops reads every series again in a pass that
// checks them all.
func eachStatsJSONSeriesUntil(raw []byte, parser *fj.Parser, largest *int, visit func(*fj.Value) bool) bool {
	supported, done := true, false
	_, err := jsonparser.ArrayEach(raw, func(series []byte, kind jsonparser.ValueType, _ int, parseErr error) {
		if !supported || done {
			return
		}
		if kind != jsonparser.Object || parseErr != nil {
			supported = false
			return
		}
		if len(series) > *largest {
			*largest = len(series)
		}
		value, err := parser.ParseBytes(series)
		if err != nil {
			supported = false
			return
		}
		done = !visit(value)
	})
	return done || err == nil && supported
}

func statsJSONSeriesNeedsTrim(item *fj.Value, keep func(int64) bool) bool {
	if keep == nil {
		return false
	}
	points := item.GetArray("values")
	for _, point := range points {
		parts, _ := point.Array()
		if len(parts) > 0 && !keep(statsQRFJPointNano(parts[0])) {
			return true
		}
	}
	return false
}

func trimStatsQRBySeries(body []byte, keep func(int64) bool, relabel func(int64) int64) ([]byte, bool) {
	envelope, supported := parseStatsJSONEnvelope(body)
	if !supported {
		return nil, false
	}
	// Keep the legacy precedence: nonempty results, otherwise data.result.
	var selected *statsJSONSlot
	for i := range envelope.slots {
		slot := &envelope.slots[i]
		if slot.key == "results" || slot.inData && selected == nil {
			selected = slot
		}
	}
	if selected == nil {
		return body, true
	}
	parser := statsQRFJPool.Get()
	largest := 0
	defer func() { releaseFJParser(&statsQRFJPool, parser, largest) }()
	// The detection pass parses every series only to learn whether one needs a
	// rewrite: a relabel rewrites them all, and the rewrite below parses and
	// checks every series the same way, so it is skipped then.
	needsTrim := relabel != nil
	if !needsTrim {
		if !eachStatsJSONSeries(selected.raw, parser, &largest, func(item *fj.Value) {
			if !needsTrim && statsJSONSeriesNeedsTrim(item, keep) {
				needsTrim = true
			}
		}) {
			return nil, false
		}
	}
	if !needsTrim {
		return body, true
	}
	buf := jsonBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer releaseJSONBuffer(buf)
	buf.Grow(len(body))
	scratch := fjMarshalPool.Get().(*[]byte)
	defer releaseFJMarshalScratch(scratch)
	buf.WriteByte('{')
	if status := envelope.value.Get("status"); status != nil {
		buf.WriteString(`"status":`)
		marshalFJ(buf, status, scratch)
		buf.WriteByte(',')
	}
	if selected.inData {
		buf.WriteString(`"data":{`)
		if resultType := envelope.value.Get("data", "resultType"); resultType != nil {
			buf.WriteString(`"resultType":`)
			marshalFJ(buf, resultType, scratch)
			buf.WriteByte(',')
		}
		buf.WriteString(`"result":`)
	} else {
		buf.WriteString(`"results":`)
	}
	if !writeStatsJSONSeries(buf, selected.raw, parser, &largest, func(seriesBuf *bytes.Buffer, item *fj.Value) {
		one := [1]*fj.Value{item}
		writeFilteredStatsQRSeriesFJ(seriesBuf, one[:], keep, relabel, scratch)
	}) {
		return nil, false
	}
	if selected.inData {
		if stats := envelope.value.Get("data", "stats"); stats != nil {
			buf.WriteString(`,"stats":`)
			marshalFJ(buf, stats, scratch)
		}
		buf.WriteByte('}')
	}
	buf.WriteByte('}')
	return bytes.Clone(buf.Bytes()), true
}

// writeStatsJSONSeries reuses the existing one-or-many series writers without
// changing their handling of malformed points, empty series or timestamp types.
func writeStatsJSONSeries(buf *bytes.Buffer, raw []byte, parser *fj.Parser, largest *int, write func(*bytes.Buffer, *fj.Value)) bool {
	seriesBuf := jsonBufPool.Get().(*bytes.Buffer)
	seriesBuf.Reset()
	defer releaseJSONBuffer(seriesBuf)
	buf.WriteByte('[')
	written := false
	supported := eachStatsJSONSeries(raw, parser, largest, func(item *fj.Value) {
		seriesBuf.Reset()
		write(seriesBuf, item) // emits [] or [one series]
		if seriesBuf.Len() <= 2 {
			return
		}
		if written {
			buf.WriteByte(',')
		}
		written = true
		buf.Write(seriesBuf.Bytes()[1 : seriesBuf.Len()-1])
	})
	buf.WriteByte(']')
	return supported
}

func (p *Proxy) trimAndTranslateStatsQRBySeries(body []byte, keep func(int64) bool, originalQuery string) ([]byte, string, bool) {
	envelope, supported := parseStatsJSONEnvelope(body)
	if !supported {
		return nil, "", false
	}
	if len(envelope.slots) == 0 {
		return body, "", true
	}
	parser := statsTranslateFJPool.Get()
	largest := 0
	defer func() { releaseFJParser(&statsTranslateFJPool, parser, largest) }()
	grouping := requestedLevelGrouping(originalQuery)
	translated, synthetic := make(map[string]string, 8), make(map[string]string, 8)
	// The detection pass stops at the first series that needs a rebuild: the
	// rebuild below parses and checks every series the same way, so a series
	// it would reject is still rejected.
	needsRebuild := false
	for _, slot := range envelope.slots {
		if needsRebuild {
			break
		}
		if !eachStatsJSONSeriesUntil(slot.raw, parser, &largest, func(item *fj.Value) bool {
			_, changed := p.translateStatsQRMetricFJ(item.Get("metric"), grouping, translated, synthetic)
			needsRebuild = changed || statsJSONSeriesNeedsTrim(item, keep)
			return !needsRebuild
		}) {
			return nil, "", false
		}
	}
	if !needsRebuild {
		return body, "noop", true
	}
	buf := jsonBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer releaseJSONBuffer(buf)
	buf.Grow(len(body) + len(`{"status":"success",`))
	scratch := fjMarshalPool.Get().(*[]byte)
	defer releaseFJMarshalScratch(scratch)
	buf.WriteString(`{"status":"success"`)
	for _, slot := range envelope.slots {
		buf.WriteByte(',')
		if slot.inData {
			buf.WriteString(`"data":{`)
			if resultType := envelope.value.Get("data", "resultType"); resultType != nil {
				buf.WriteString(`"resultType":`)
				marshalFJ(buf, resultType, scratch)
				buf.WriteByte(',')
			}
		}
		buf.WriteByte('"')
		buf.WriteString(slot.key)
		buf.WriteString(`":`)
		if !writeStatsJSONSeries(buf, slot.raw, parser, &largest, func(seriesBuf *bytes.Buffer, item *fj.Value) {
			metric, changed := p.translateStatsQRMetricFJ(item.Get("metric"), grouping, translated, synthetic)
			result := trimTranslateResult{valsTrim: statsJSONSeriesNeedsTrim(item, keep)}
			if changed {
				result.metric = metric
			}
			one := [1]*fj.Value{item}
			results := [1]trimTranslateResult{result}
			writeTrimmedTranslatedStatsFJ(seriesBuf, one[:], results[:], keep, scratch)
		}) {
			return nil, "", false
		}
		if slot.inData {
			if stats := envelope.value.Get("data", "stats"); stats != nil {
				buf.WriteString(`,"stats":`)
				marshalFJ(buf, stats, scratch)
			}
			buf.WriteByte('}')
		}
	}
	if errorValue := envelope.value.Get("error"); errorValue != nil {
		buf.WriteString(`,"error":`)
		marshalFJ(buf, errorValue, scratch)
	}
	buf.WriteByte('}')
	return bytes.Clone(buf.Bytes()), "ok", true
}

func (p *Proxy) translateStatsQRMetricFJ(metricVal *fj.Value, levelGrouping levelGroupingRequest, translated, syntheticLabels map[string]string) (map[string]string, bool) {
	if metricVal == nil || metricVal.Type() != fj.TypeObject {
		return nil, false
	}

	for k := range translated {
		delete(translated, k)
	}
	changed := false
	hadStream := false

	metricVal.GetObject().Visit(func(k []byte, vv *fj.Value) {
		key := string(k)
		val := string(vv.GetStringBytes())
		switch key {
		case "__name__":
			changed = true
		case "_stream":
			hadStream = true
			for streamKey, streamValue := range parseStreamLabels(val) {
				lokiKey := streamKey
				if !p.labelTranslator.IsPassthrough() {
					lokiKey = p.labelTranslator.ToLoki(streamKey)
				}
				if streamValue != "" || translated[lokiKey] == "" {
					translated[lokiKey] = streamValue
				}
			}
			changed = true
		default:
			lokiKey := key
			if !p.labelTranslator.IsPassthrough() {
				lokiKey = p.labelTranslator.ToLoki(key)
			}
			if lokiKey != key {
				changed = true
			}
			if val != "" || translated[lokiKey] == "" {
				translated[lokiKey] = val
			}
		}
	})

	for k := range syntheticLabels {
		delete(syntheticLabels, k)
	}
	for k, val := range translated {
		syntheticLabels[k] = val
	}

	serviceSignal := hasServiceSignal(syntheticLabels)
	beforeSyntheticCount := len(syntheticLabels)
	levelGrouping.apply(syntheticLabels, translated, hadStream)
	if hadStream {
		ensureSyntheticServiceName(syntheticLabels)
		if !serviceSignal && strings.TrimSpace(syntheticLabels["service_name"]) == unknownServiceName {
			delete(syntheticLabels, "service_name")
		}
	}
	if dropEmptyLabelValues(syntheticLabels) {
		changed = true
	}
	if len(syntheticLabels) != beforeSyntheticCount {
		changed = true
	}
	for key, value := range syntheticLabels {
		if existing, ok := translated[key]; ok && existing == value {
			continue
		}
		translated[key] = value
		changed = true
	}
	return syntheticLabels, changed
}

// statsTumblingRelabelKey carries a range == step answer's relabel
// (*statsTumblingRelabel) to the stats call that answers it.
type statsTumblingRelabelKey struct{}

// statsTumblingRelabel is the relabel the caller applies to the written
// answer; done reports that the answer was written relabelled already.
type statsTumblingRelabel struct {
	keep    func(int64) bool
	relabel func(int64) int64
	done    bool
}

// trimTranslateRelabelStatsQRBySeries is trimAndTranslateStatsQRBySeries
// followed by trimStatsQRBySeries(translated, relabelKeep, relabel) in one
// pass: each series is translated, and the translated series is relabelled
// as the second pass reads it from the translated answer, so the translated
// answer is never assembled, validated or parsed again. It also reports the
// size and the series count the translated answer would have had, which
// decide whether the two passes would have taken these paths (the caller's
// series limit and the relabel's per-series path). It reports false for an
// answer it does not rewrite exactly as the two passes do: not a single
// non-empty data.result, an error key, nothing to translate, a translated
// answer with no series, or a translated series the second pass cannot read.
func (p *Proxy) trimTranslateRelabelStatsQRBySeries(body []byte, keep func(int64) bool, originalQuery string, relabelKeep func(int64) bool, relabel func(int64) int64) (out []byte, translatedSize, translatedSeries int, ok bool) {
	envelope, supported := parseStatsJSONEnvelope(body)
	// Without a resultType the translated answer is re-encoded when it is
	// wrapped (wrapAsLokiResponse), which the relabel then reads.
	if !supported || len(envelope.slots) != 1 || !envelope.slots[0].inData || envelope.value.Get("error") != nil || envelope.value.Get("data", "resultType") == nil {
		return nil, 0, 0, false
	}
	slot := envelope.slots[0]
	parser := statsTranslateFJPool.Get()
	largest := 0
	defer func() { releaseFJParser(&statsTranslateFJPool, parser, largest) }()
	grouping := requestedLevelGrouping(originalQuery)
	translated, synthetic := make(map[string]string, 8), make(map[string]string, 8)
	needsRebuild := false
	if !eachStatsJSONSeriesUntil(slot.raw, parser, &largest, func(item *fj.Value) bool {
		_, changed := p.translateStatsQRMetricFJ(item.Get("metric"), grouping, translated, synthetic)
		needsRebuild = changed || statsJSONSeriesNeedsTrim(item, keep)
		return !needsRebuild
	}) || !needsRebuild {
		return nil, 0, 0, false
	}
	scratch := fjMarshalPool.Get().(*[]byte)
	defer releaseFJMarshalScratch(scratch)

	// The translated answer's envelope, as trimAndTranslateStatsQRBySeries
	// writes it around its series, and the relabel's, as trimStatsQRBySeries
	// writes it from the translated envelope.
	var head, tail bytes.Buffer
	head.WriteString(`{"status":"success","data":{`)
	if resultType := envelope.value.Get("data", "resultType"); resultType != nil {
		head.WriteString(`"resultType":`)
		marshalFJ(&head, resultType, scratch)
		head.WriteByte(',')
	}
	head.WriteString(`"result":`)
	if stats := envelope.value.Get("data", "stats"); stats != nil {
		tail.WriteString(`,"stats":`)
		marshalFJ(&tail, stats, scratch)
	}
	tail.WriteString(`}}`)
	translatedMeta := append(append(append([]byte(nil), head.Bytes()...), `[]`...), tail.Bytes()...)
	var meta fj.Parser
	metaValue, err := meta.ParseBytes(translatedMeta)
	if err != nil {
		return nil, 0, 0, false
	}

	relabelParser := statsQRFJPool.Get()
	relabelLargest := 0
	defer func() { releaseFJParser(&statsQRFJPool, relabelParser, relabelLargest) }()
	buf := jsonBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer releaseJSONBuffer(buf)
	buf.Grow(len(body))
	buf.WriteByte('{')
	if status := metaValue.Get("status"); status != nil {
		buf.WriteString(`"status":`)
		marshalFJ(buf, status, scratch)
		buf.WriteByte(',')
	}
	buf.WriteString(`"data":{`)
	if resultType := metaValue.Get("data", "resultType"); resultType != nil {
		buf.WriteString(`"resultType":`)
		marshalFJ(buf, resultType, scratch)
		buf.WriteByte(',')
	}
	buf.WriteString(`"result":[`)

	seriesBuf := jsonBufPool.Get().(*bytes.Buffer)
	defer releaseJSONBuffer(seriesBuf)
	relabelBuf := jsonBufPool.Get().(*bytes.Buffer)
	defer releaseJSONBuffer(relabelBuf)
	readable, written := true, false
	if !eachStatsJSONSeries(slot.raw, parser, &largest, func(item *fj.Value) {
		if !readable {
			return
		}
		metric, changed := p.translateStatsQRMetricFJ(item.Get("metric"), grouping, translated, synthetic)
		result := trimTranslateResult{valsTrim: statsJSONSeriesNeedsTrim(item, keep)}
		if changed {
			result.metric = metric
		}
		seriesBuf.Reset()
		one := [1]*fj.Value{item}
		results := [1]trimTranslateResult{result}
		writeTrimmedTranslatedStatsFJ(seriesBuf, one[:], results[:], keep, scratch)
		if seriesBuf.Len() <= 2 {
			return // not part of the translated answer
		}
		series := seriesBuf.Bytes()[1 : seriesBuf.Len()-1]
		if translatedSeries > 0 {
			translatedSize++ // the comma between series
		}
		translatedSeries++
		translatedSize += len(series)
		// The second pass reads this series from the translated answer.
		if len(series) > relabelLargest {
			relabelLargest = len(series)
		}
		value, err := relabelParser.ParseBytes(series)
		if err != nil || value.Type() != fj.TypeObject {
			readable = false
			return
		}
		relabelBuf.Reset()
		relabelOne := [1]*fj.Value{value}
		writeFilteredStatsQRSeriesFJ(relabelBuf, relabelOne[:], relabelKeep, relabel, scratch)
		if relabelBuf.Len() <= 2 {
			return
		}
		if written {
			buf.WriteByte(',')
		}
		written = true
		buf.Write(relabelBuf.Bytes()[1 : relabelBuf.Len()-1])
	}) || !readable || translatedSeries == 0 {
		return nil, 0, 0, false
	}
	buf.WriteByte(']')
	if stats := metaValue.Get("data", "stats"); stats != nil {
		buf.WriteString(`,"stats":`)
		marshalFJ(buf, stats, scratch)
	}
	buf.WriteString(`}}`)
	translatedSize += head.Len() + 2 + tail.Len()
	return bytes.Clone(buf.Bytes()), translatedSize, translatedSeries, true
}

// trimTranslateRelabelStatsQRFJ is trimAndTranslateStatsQRFJFull, the wrap
// and trimStatsQRByTimeFJShifted(translated, relabelKeep, relabel) in one
// parse of an answer within the pooled parser's input bound: each series is
// translated and relabelled as it is written, so the translated answer is
// never assembled or parsed again. It also reports the series count the
// translated answer would have had (the caller's series limit). It reports
// false for an answer it does not rewrite exactly as the two passes do: over
// the input bound, not a single non-empty data.result with a resultType, an
// error key or another result slot, nothing to translate, a point the first
// pass trims, a series that is not a metric and a values array of
// [number, value] points, or a value the second pass's parse would read
// back differently (the parts the two passes write and read again are
// written and read here the same way).
func (p *Proxy) trimTranslateRelabelStatsQRFJ(body []byte, keep func(int64) bool, originalQuery string, relabelKeep func(int64) bool, relabel func(int64) int64) (out []byte, translatedSeries int, ok bool) {
	if len(body) > maxPooledStatsParserInputSize || relabel == nil || relabelKeep == nil {
		return nil, 0, false
	}
	parser := statsTranslateFJPool.Get()
	defer releaseFJParser(&statsTranslateFJPool, parser, len(body))
	v, err := parser.ParseBytes(body)
	if err != nil {
		return nil, 0, false
	}
	data, items, ok := statsOneParseResult(v)
	if !ok {
		return nil, 0, false
	}
	grouping := requestedLevelGrouping(originalQuery)
	translated, synthetic := make(map[string]string, 8), make(map[string]string, 8)
	needsRebuild := false
	for _, item := range items {
		if !statsOneParseSeries(item, keep) {
			return nil, 0, false
		}
		if _, changed := p.translateStatsQRMetricFJ(item.Get("metric"), grouping, translated, synthetic); changed {
			needsRebuild = true
		}
	}
	if !needsRebuild {
		return nil, 0, false
	}

	w := &statsOneParseWriter{scratch: fjMarshalPool.Get().(*[]byte), part: jsonBufPool.Get().(*bytes.Buffer)}
	defer releaseFJMarshalScratch(w.scratch)
	defer releaseJSONBuffer(w.part)
	buf := jsonBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer releaseJSONBuffer(buf)
	buf.Grow(len(body))
	buf.WriteString(`{"status":"success","data":{"resultType":`)
	if !w.roundTrip(buf, func(b *bytes.Buffer) { marshalFJ(b, data.Get("resultType"), w.scratch) }) {
		return nil, 0, false
	}
	buf.WriteString(`,"result":[`)
	written := false
	for _, item := range items {
		if !statsQRSeriesKeepsPoint(item.Get("values"), relabelKeep) {
			continue // Loki never returns a matrix series without samples
		}
		if written {
			buf.WriteByte(',')
		}
		written = true
		metric, changed := p.translateStatsQRMetricFJ(item.Get("metric"), grouping, translated, synthetic)
		if !w.series(buf, item, metric, changed, relabelKeep, relabel) {
			return nil, 0, false
		}
	}
	buf.WriteByte(']')
	if stats := data.Get("stats"); stats != nil {
		buf.WriteString(`,"stats":`)
		if !w.roundTrip(buf, func(b *bytes.Buffer) { marshalFJ(b, stats, w.scratch) }) {
			return nil, 0, false
		}
	}
	buf.WriteString(`}}`)
	return bytes.Clone(buf.Bytes()), len(items), true
}

// statsOneParseResult is the data object and the series of an answer the one
// parse takes: a single non-empty data.result with a resultType, no error
// key and no other result slot the translation would also rewrite.
func statsOneParseResult(v *fj.Value) (*fj.Value, []*fj.Value, bool) {
	if v.Type() != fj.TypeObject || v.Get("error") != nil {
		return nil, nil, false
	}
	for _, key := range []string{"result", "results"} {
		if r := v.Get(key); r != nil && r.Type() == fj.TypeArray && len(r.GetArray()) > 0 {
			return nil, nil, false
		}
	}
	data := v.Get("data")
	if data == nil || data.Type() != fj.TypeObject || data.Get("resultType") == nil {
		return nil, nil, false
	}
	items := data.GetArray("result")
	return data, items, len(items) > 0
}

// statsOneParseSeries reports whether the one parse takes a series: a metric
// and a values array of [number, value] points whose value reads back
// unchanged, none of which the first pass's trim drops (its raw point filter
// decides those).
func statsOneParseSeries(item *fj.Value, keep func(int64) bool) bool {
	if item.Type() != fj.TypeObject || item.Get("metric") == nil {
		return false
	}
	values := item.Get("values")
	if values == nil || values.Type() != fj.TypeArray {
		return false
	}
	for _, point := range values.GetArray() {
		parts, _ := point.Array()
		if len(parts) != 2 || parts[0].Type() != fj.TypeNumber || !statsValueReadsBack(parts[1]) {
			return false
		}
		if keep != nil && !keep(statsQRFJPointNano(parts[0])) {
			return false
		}
	}
	return true
}

// statsOneParseWriter writes the one parse's series. The small parts the
// first pass writes and the second reads back (a series' labels, the
// resultType, the stats block) are written, read and written again as the two
// passes do, and must come out as JSON.
type statsOneParseWriter struct {
	scratch *[]byte
	part    *bytes.Buffer
	parser  fj.Parser
}

func (w *statsOneParseWriter) roundTrip(dst *bytes.Buffer, write func(*bytes.Buffer)) bool {
	w.part.Reset()
	write(w.part)
	value, err := w.parser.ParseBytes(w.part.Bytes())
	if err != nil {
		return false
	}
	// fastjson writes a string it must escape with Go's quoting, which is not
	// always JSON: such an answer is left to the two passes.
	start := dst.Len()
	marshalFJ(dst, value, w.scratch)
	return json.Valid(dst.Bytes()[start:])
}

// series writes one translated series with the points relabelKeep keeps,
// relabelled, as writeFilteredStatsQRSeriesFJ writes it from the translated
// answer.
func (w *statsOneParseWriter) series(buf *bytes.Buffer, item *fj.Value, metric map[string]string, changed bool, relabelKeep func(int64) bool, relabel func(int64) int64) bool {
	buf.WriteString(`{"metric":`)
	if !w.roundTrip(buf, func(b *bytes.Buffer) {
		if changed {
			marshalStringMapJSONTo(b, metric)
		} else {
			marshalFJ(b, item.Get("metric"), w.scratch)
		}
	}) {
		return false
	}
	buf.WriteString(`,"values":[`)
	first := true
	for _, point := range item.GetArray("values") {
		parts := point.GetArray()
		tsNs := statsQRFJPointNano(parts[0])
		if !relabelKeep(tsNs) {
			continue
		}
		if !first {
			buf.WriteByte(',')
		}
		first = false
		buf.WriteByte('[')
		*w.scratch = appendLokiSampleTime((*w.scratch)[:0], relabel(tsNs))
		buf.Write(*w.scratch)
		buf.WriteByte(',')
		marshalFJ(buf, parts[1], w.scratch)
		buf.WriteByte(']')
	}
	buf.WriteString(`]}`)
	return true
}

// statsValueReadsBack reports whether a sample value is written, read back
// and written again unchanged: a JSON number (fastjson keeps a number's text,
// and also takes text that is no JSON number), or a string with nothing to
// escape (fastjson writes any other string with Go's quoting).
func statsValueReadsBack(v *fj.Value) bool {
	switch v.Type() {
	case fj.TypeNumber:
		var text [32]byte
		return json.Valid(v.MarshalTo(text[:0]))
	case fj.TypeString:
		for _, c := range v.GetStringBytes() {
			if c < 0x20 || c == '"' || c == '\\' {
				return false
			}
		}
		return true
	}
	return false
}
