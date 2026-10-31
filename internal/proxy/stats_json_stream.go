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
	needsTrim := relabel != nil
	if !eachStatsJSONSeries(selected.raw, parser, &largest, func(item *fj.Value) {
		if !needsTrim && statsJSONSeriesNeedsTrim(item, keep) {
			needsTrim = true
		}
	}) {
		return nil, false
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
	needsRebuild := false
	for _, slot := range envelope.slots {
		if !eachStatsJSONSeries(slot.raw, parser, &largest, func(item *fj.Value) {
			if needsRebuild {
				return
			}
			_, changed := p.translateStatsQRMetricFJ(item.Get("metric"), grouping, translated, synthetic)
			needsRebuild = changed || statsJSONSeriesNeedsTrim(item, keep)
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
