package proxy

import (
	"encoding/json"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"

	fj "github.com/valyala/fastjson"
)

// foldLevel is one vector aggregation of a fold: the series that share the
// labels it keeps become one, valued by its operator (pkg/logql/evaluator.go
// VectorAggEvaluator.Next).
type foldLevel struct {
	merge  withoutMerge
	labels map[string]bool
	// keep: `by`, the series keep only the listed labels they have. Otherwise
	// `without`: they lose the listed ones.
	keep bool
	// dropAll: the series keep no label (an aggregation with no grouping, `by ()`).
	dropAll bool
}

// withoutFoldLevel is applyWithoutGrouping's level: drop excludeLabels
// (withoutAllLabels drops every label) and combine with op.
func withoutFoldLevel(excludeLabels []string, op string) foldLevel {
	level := foldLevel{merge: withoutMergeForOp(op), labels: make(map[string]bool, len(excludeLabels))}
	for _, name := range excludeLabels {
		name = strings.TrimSpace(name)
		level.labels[name] = true
		if name == withoutAllLabels {
			level.dropAll = true
		}
	}
	return level
}

// byFoldLevel keeps only the listed labels a series has; none keeps none.
func byFoldLevel(keepLabels []string, op string) foldLevel {
	level := foldLevel{merge: withoutMergeForOp(op), keep: true, dropAll: len(keepLabels) == 0, labels: make(map[string]bool, len(keepLabels))}
	for _, name := range keepLabels {
		level.labels[strings.TrimSpace(name)] = true
	}
	return level
}

func (l foldLevel) keeps(name string) bool {
	if l.dropAll {
		return false
	}
	return l.labels[name] == l.keep
}

// typedPoint is a sample of a series.
type typedPoint struct{ ts, v float64 }

type typedSeries struct {
	metric map[string]string
	points []typedPoint
}

// typedBody is a Prometheus-shaped matrix or vector, decoded once so that every
// level of a fold works on the series themselves and the answer is encoded once.
type typedBody struct {
	vector bool
	series []typedSeries
}

var typedFoldParsers fj.ParserPool

// decodeTypedBody reads a successful matrix or vector answer whose labels are
// strings and whose samples are numeric timestamps with a number or a string
// for the value; ok is false for anything else, which the generic fold reads.
func decodeTypedBody(body []byte) (typedBody, bool) {
	parser := typedFoldParsers.Get()
	defer typedFoldParsers.Put(parser)
	root, err := parser.ParseBytes(body)
	if err != nil || string(root.GetStringBytes("status")) != "success" {
		return typedBody{}, false
	}
	data := root.Get("data")
	if data == nil {
		return typedBody{}, false
	}
	var out typedBody
	switch string(data.GetStringBytes("resultType")) {
	case "vector":
		out.vector = true
	case "matrix":
	default:
		return typedBody{}, false
	}
	items, err := data.Get("result").Array()
	if err != nil {
		return typedBody{}, false
	}
	out.series = make([]typedSeries, 0, len(items))
	for _, item := range items {
		metric, err := item.Get("metric").Object()
		if err != nil {
			return typedBody{}, false
		}
		series := typedSeries{metric: make(map[string]string, metric.Len())}
		bad := false
		metric.Visit(func(k []byte, v *fj.Value) {
			if v.Type() != fj.TypeString {
				bad = true
				return
			}
			series.metric[string(k)] = string(v.GetStringBytes())
		})
		if bad {
			return typedBody{}, false
		}
		var samples []*fj.Value
		if out.vector {
			value := item.Get("value")
			if value == nil {
				return typedBody{}, false
			}
			samples = []*fj.Value{value}
		} else if values := item.Get("values"); values != nil {
			if samples, err = values.Array(); err != nil {
				return typedBody{}, false
			}
		}
		series.points = make([]typedPoint, 0, len(samples))
		for _, sample := range samples {
			pair, err := sample.Array()
			if err != nil || len(pair) < 2 {
				continue
			}
			if pair[0].Type() != fj.TypeNumber {
				return typedBody{}, false
			}
			ts, _ := pair[0].Float64()
			var v float64
			switch pair[1].Type() {
			case fj.TypeString:
				v, _ = strconv.ParseFloat(string(pair[1].GetStringBytes()), 64)
			case fj.TypeNumber:
				v, _ = pair[1].Float64()
			default:
				if out.vector {
					v = 0 // an instant value that is not a string reads as 0
				} else {
					continue
				}
			}
			series.points = append(series.points, typedPoint{ts, v})
		}
		out.series = append(out.series, series)
	}
	return out, true
}

type typedCell struct {
	ts, v float64
	n     int
}

type typedGroup struct {
	key    string
	metric map[string]string
	cells  []typedCell
	at     map[float64]int32 // sample time -> index in cells
}

// fold applies one level: the series fall into the group their kept labels
// name, and each group takes, at every sample time, the operator's value of
// the samples there. A vector keeps the time of the first series of a group.
func (b typedBody) fold(level foldLevel) typedBody {
	groups := map[string]*typedGroup{}
	var order []*typedGroup
	var pairs [][2]string
	var key strings.Builder
	for _, series := range b.series {
		pairs = pairs[:0]
		for name, value := range series.metric {
			if level.keeps(name) {
				pairs = append(pairs, [2]string{name, value})
			}
		}
		sort.Slice(pairs, func(i, j int) bool { return pairs[i][0] < pairs[j][0] })
		key.Reset()
		for _, pair := range pairs {
			key.WriteString(strconv.Itoa(len(pair[0])))
			key.WriteByte(':')
			key.WriteString(pair[0])
			key.WriteByte('=')
			key.WriteString(strconv.Itoa(len(pair[1])))
			key.WriteByte(':')
			key.WriteString(pair[1])
			key.WriteByte(';')
		}
		group := groups[key.String()]
		if group == nil {
			group = &typedGroup{key: key.String(), metric: make(map[string]string, len(pairs)), at: make(map[float64]int32, len(series.points))}
			for _, pair := range pairs {
				group.metric[pair[0]] = pair[1]
			}
			groups[group.key] = group
			order = append(order, group)
		}
		for _, point := range series.points {
			ts := point.ts
			if b.vector {
				ts = 0 // one cell: the group's time is its first series'
			}
			i, ok := group.at[ts]
			if !ok {
				i = int32(len(group.cells))
				group.at[ts] = i
				group.cells = append(group.cells, typedCell{ts: point.ts})
			}
			cell := &group.cells[i]
			cell.n++
			cell.v = level.merge.combine(cell.v, point.v, cell.n)
		}
	}
	out := typedBody{vector: b.vector, series: make([]typedSeries, 0, len(order))}
	sort.Slice(order, func(i, j int) bool { return order[i].key < order[j].key })
	for _, group := range order {
		slices.SortFunc(group.cells, func(a, c typedCell) int {
			switch {
			case a.ts < c.ts:
				return -1
			case a.ts > c.ts:
				return 1
			}
			return 0
		})
		points := make([]typedPoint, len(group.cells))
		for i, cell := range group.cells {
			points[i] = typedPoint{cell.ts, cell.v}
		}
		out.series = append(out.series, typedSeries{metric: group.metric, points: points})
	}
	return out
}

// encode writes the answer the way encoding/json writes a map of it: the keys
// sorted, so the bytes are the ones the generic fold wrote.
func (b typedBody) encode() []byte {
	points := 0
	for _, series := range b.series {
		points += len(series.points)
	}
	out := make([]byte, 0, 64+len(b.series)*96+points*34)
	out = append(out, `{"data":{"result":[`...)
	for i, series := range b.series {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, `{"metric":{`...)
		names := make([]string, 0, len(series.metric))
		for name := range series.metric {
			names = append(names, name)
		}
		sort.Strings(names)
		for j, name := range names {
			if j > 0 {
				out = append(out, ',')
			}
			out = appendJSONText(out, name)
			out = append(out, ':')
			out = appendJSONText(out, series.metric[name])
		}
		if b.vector {
			out = append(out, `},"value":`...)
			out = appendTypedPoint(out, series.points[0])
			out = append(out, '}')
			continue
		}
		out = append(out, `},"values":[`...)
		for j, point := range series.points {
			if j > 0 {
				out = append(out, ',')
			}
			out = appendTypedPoint(out, point)
		}
		out = append(out, `]}`...)
	}
	kind := "matrix"
	if b.vector {
		kind = "vector"
	}
	out = append(out, `],"resultType":"`...)
	out = append(out, kind...)
	out = append(out, `"},"status":"success"}`...)
	return out
}

func appendTypedPoint(out []byte, p typedPoint) []byte {
	out = append(out, '[')
	out = appendJSONNumber(out, p.ts)
	out = append(out, `,"`...)
	out = strconv.AppendFloat(out, p.v, 'f', -1, 64)
	return append(out, `"]`...)
}

// appendJSONNumber writes f as encoding/json does.
func appendJSONNumber(out []byte, f float64) []byte {
	format := byte('f')
	if abs := math.Abs(f); abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		format = 'e'
	}
	out = strconv.AppendFloat(out, f, format, -1, 64)
	if format == 'e' {
		if n := len(out); n >= 4 && out[n-4] == 'e' && out[n-3] == '-' && out[n-2] == '0' {
			out[n-2] = out[n-1]
			out = out[:n-1]
		}
	}
	return out
}

// appendJSONText writes s as a JSON string as encoding/json does.
func appendJSONText(out []byte, s string) []byte {
	plain := true
	for i := 0; i < len(s); i++ {
		if c := s[i]; c < 0x20 || c > 0x7e || c == '"' || c == '\\' || c == '<' || c == '>' || c == '&' {
			plain = false
			break
		}
	}
	if plain {
		out = append(out, '"')
		out = append(out, s...)
		return append(out, '"')
	}
	enc, _ := json.Marshal(s)
	return append(out, enc...)
}

// foldLevels decodes body once, applies every level in turn, and encodes the
// answer once. ok is false when body is not a shape decodeTypedBody reads.
func foldLevels(body []byte, levels ...foldLevel) ([]byte, bool) {
	folded, ok := decodeTypedBody(body)
	if !ok {
		return nil, false
	}
	for _, level := range levels {
		folded = folded.fold(level)
	}
	return folded.encode(), true
}
