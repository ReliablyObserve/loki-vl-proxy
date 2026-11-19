package proxy

import (
	"bytes"
	"cmp"
	"slices"
	"strconv"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	fj "github.com/valyala/fastjson"
)

// refFoldPoint and refFoldSeries are matrixReference's point and result.
type refFoldPoint struct {
	ts    int64
	value string
}

type refFoldSeries struct {
	column int
	name   string
	labels []string
	points []refFoldPoint
}

// matrixReference is the folded answer reader as first written (each row decoded with fastjson, every value
// written again with appendJSONString), kept as the reference of seriesFoldRead.matrix: it reads's answer (rows of /select/logsql/query, one per bucket and group of the query's last
// stats pipe) back into the answer stats_query_range gives for a stats query of these groups: a result per column and
// group, its labels the grouping less the bucket and its points the buckets, ordered as VictoriaLogs orders them. A
// folded group (an empty series hash) answers its merged value and series count in the value and count columns; a
// group that keeps its hash answers the bucket stats' results (and a count of one when the fold adds the count
// column). A row that is not an object with a bucket time fails the read.
func (s *seriesFoldRead) matrixReference(raw []byte) ([]byte, error) {
	if s == nil {
		return nil, &statsBucketsUnreadableError{"folded read without its grouping"}
	}
	labels := make([]string, 0, len(s.by)+1)
	for _, b := range s.by {
		if name, err := strconv.Unquote(b); err == nil {
			b = name
		}
		labels = append(labels, b)
	}
	sid := slices.Index(labels, dynamicSeriesHashLabel)
	if sid < 0 {
		return nil, &statsBucketsUnreadableError{"folded read without its series hash"}
	}
	// The unwrap check's counting form groups by its flag too: every row then names it.
	if bytes.Contains(raw, seriesFoldFlagKey) {
		labels = append(labels, unwrapBadField)
	}
	var parser fj.Parser
	byKey := map[string]*refFoldSeries{}
	var order []*refFoldSeries
	values := make([]string, len(labels))
	var key []byte
	add := func(column int, name string, ts int64, value string) {
		key = append(key[:0], byte(column>>24), byte(column>>16), byte(column>>8), byte(column))
		key = append(key, name...)
		for _, v := range values {
			key = append(append(key, 0), v...)
		}
		ss := byKey[string(key)]
		if ss == nil {
			ss = &refFoldSeries{column: column, name: name, labels: slices.Clone(values)}
			byKey[string(key)] = ss
			order = append(order, ss)
		}
		ss.points = append(ss.points, refFoldPoint{ts: ts, value: value})
	}
	for len(raw) > 0 {
		line := raw
		if i := bytes.IndexByte(raw, '\n'); i >= 0 {
			line, raw = raw[:i], raw[i+1:]
		} else {
			raw = nil
		}
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		row, err := parser.ParseBytes(line)
		if err != nil || row.Type() != fj.TypeObject {
			return nil, &statsBucketsUnreadableError{"folded read: a row that is not a JSON object"}
		}
		ts, ok := parseVLTimeNanos(string(row.GetStringBytes("_time")))
		if !ok {
			return nil, &statsBucketsUnreadableError{"folded read: a row without its bucket"}
		}
		for i, l := range labels {
			values[i] = string(row.GetStringBytes(l))
		}
		if values[sid] == "" {
			add(slices.Index(s.columns, s.valueColumn), s.valueColumn, ts, string(row.GetStringBytes(lower.SeriesFoldField)))
			if s.countColumn != "" {
				add(s.countIndex(), s.countColumn, ts, string(row.GetStringBytes(lower.SeriesFoldSeriesCount)))
			}
			continue
		}
		for i, c := range s.columns {
			add(i, c, ts, string(row.GetStringBytes(c)))
		}
		if s.added {
			add(len(s.columns), lower.SeriesFoldCount, ts, "1")
		}
	}
	return s.writeMatrixReference(order, labels)
}

// writeMatrixReference writes the results of a folded answer as stats_query_range writes them: ordered by column, name and
// labels, each one's points by time, a time in seconds as float64(ns)/1e9 writes.
func (s *seriesFoldRead) writeMatrixReference(results []*refFoldSeries, labels []string) ([]byte, error) {
	sortKey := func(ss *refFoldSeries) string {
		b := []byte{byte(ss.column >> 24), byte(ss.column >> 16), byte(ss.column >> 8), byte(ss.column)}
		b = append(b, ss.name...)
		b = append(b, '{')
		first := true
		for i, v := range ss.labels {
			if v == "" {
				continue
			}
			if !first {
				b = append(b, ',')
			}
			first = false
			b = appendJSONString(b, labels[i])
			b = append(b, ':')
			b = appendJSONString(b, v)
		}
		return string(append(b, '}'))
	}
	keys := make(map[*refFoldSeries]string, len(results))
	for _, ss := range results {
		keys[ss] = sortKey(ss)
		slices.SortStableFunc(ss.points, func(a, b refFoldPoint) int { return cmp.Compare(a.ts, b.ts) })
	}
	slices.SortStableFunc(results, func(a, b *refFoldSeries) int { return strings.Compare(keys[a], keys[b]) })
	out := make([]byte, 0, 4096)
	out = append(out, `{"status":"success","data":{"resultType":"matrix","result":[`...)
	for i, ss := range results {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, `{"metric":{"__name__":`...)
		out = appendJSONString(out, ss.name)
		for j, l := range labels {
			out = append(out, ',')
			out = appendJSONString(out, l)
			out = append(out, ':')
			out = appendJSONString(out, ss.labels[j])
		}
		out = append(out, `},"values":[`...)
		for j, pt := range ss.points {
			if j > 0 {
				out = append(out, ',')
			}
			out = append(out, '[')
			out = strconv.AppendFloat(out, float64(pt.ts)/1e9, 'f', -1, 64)
			out = append(out, ',')
			out = appendJSONString(out, pt.value)
			out = append(out, ']')
		}
		out = append(out, "]}"...)
		if int64(len(out)) > maxStatsBucketResponseBytes {
			return nil, errBodyTooLarge
		}
	}
	return append(out, "]}}"...), nil
}
