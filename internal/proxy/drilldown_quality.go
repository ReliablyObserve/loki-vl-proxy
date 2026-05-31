package proxy

import (
	"bytes"
	"strconv"

	fj "github.com/valyala/fastjson"
)

// zerofillStatsMatrix inserts [ts,"0"] entries for every step in
// [startSec, endSec] (inclusive, at stepSec intervals) that is absent
// from each series in body. body must be a Loki matrix JSON produced by
// a VL stats_query_range call.
//
// VictoriaLogs stats_query_range omits time buckets with zero count.
// Loki's count_over_time emits every step in the query window, including
// zeros. Filling the gaps makes the proxy response match Loki's
// continuous-line behaviour — Grafana draws a solid line rather than
// disconnected spikes.
func zerofillStatsMatrix(body []byte, startSec, endSec, stepSec int64) []byte {
	if stepSec <= 0 || startSec >= endSec {
		return body
	}

	var p fj.Parser
	v, err := p.ParseBytes(body)
	if err != nil || v == nil {
		return body
	}
	result := v.GetArray("data", "result")
	if len(result) == 0 {
		return body
	}

	// Build the complete expected time axis once.
	n := (endSec-startSec)/stepSec + 1
	axis := make([]int64, 0, n)
	for ts := startSec; ts <= endSec; ts += stepSec {
		axis = append(axis, ts)
	}
	if len(axis) == 0 {
		return body
	}

	var buf bytes.Buffer
	buf.Grow(len(body) + len(result)*len(axis)*20)
	buf.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)

	for i, item := range result {
		if i > 0 {
			buf.WriteByte(',')
		}

		// Index existing timestamp → value from VL response.
		existing := make(map[int64]string, len(axis))
		for _, pair := range item.GetArray("values") {
			arr := pair.GetArray()
			if len(arr) < 2 {
				continue
			}
			ts := arr[0].GetInt64()
			val := string(arr[1].GetStringBytes())
			if ts > 0 {
				existing[ts] = val
			}
		}

		buf.WriteString(`{"metric":`)
		if m := item.Get("metric"); m != nil {
			buf.Write(m.MarshalTo(nil))
		} else {
			buf.WriteString(`{}`)
		}
		buf.WriteString(`,"values":[`)

		first := true
		for _, ts := range axis {
			val, ok := existing[ts]
			if !ok {
				val = "0"
			}
			if !first {
				buf.WriteByte(',')
			}
			first = false
			buf.WriteByte('[')
			buf.WriteString(strconv.FormatInt(ts, 10))
			buf.WriteString(`,"`)
			buf.WriteString(val)
			buf.WriteString(`"]`)
		}
		buf.WriteString(`]}`)
	}
	buf.WriteString(`]}}`)
	return buf.Bytes()
}
