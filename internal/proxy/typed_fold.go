package proxy

import (
	"encoding/json"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
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

// decodeTypedBody reads a successful matrix or vector answer whose labels are
// strings and whose samples are numeric timestamps with a number or a string
// for the value; ok is false for anything else, which the generic fold reads.
// It scans the bytes into the series directly: a JSON document tree of a body
// of millions of samples is a few gigabytes under load, the series are 16 bytes
// a sample.
func decodeTypedBody(body []byte) (typedBody, bool) {
	s := typedScanner{b: body}
	var out typedBody
	var kind string
	resultAt := -1
	status := ""
	if !s.expect('{') {
		return typedBody{}, false
	}
	for first := true; ; first = false {
		if s.peek() == '}' {
			s.i++
			break
		}
		if !first && !s.expect(',') {
			return typedBody{}, false
		}
		key, ok := s.str()
		if !ok || !s.expect(':') {
			return typedBody{}, false
		}
		switch key {
		case "status":
			if status, ok = s.str(); !ok {
				return typedBody{}, false
			}
		case "data":
			if !s.expect('{') {
				return typedBody{}, false
			}
			for firstData := true; ; firstData = false {
				if s.peek() == '}' {
					s.i++
					break
				}
				if !firstData && !s.expect(',') {
					return typedBody{}, false
				}
				dkey, ok := s.str()
				if !ok || !s.expect(':') {
					return typedBody{}, false
				}
				switch dkey {
				case "resultType":
					if kind, ok = s.str(); !ok {
						return typedBody{}, false
					}
				case "result":
					resultAt = s.i
					if !s.skip() {
						return typedBody{}, false
					}
				default:
					if !s.skip() {
						return typedBody{}, false
					}
				}
			}
		default:
			if !s.skip() {
				return typedBody{}, false
			}
		}
	}
	if status != "success" || resultAt < 0 || (kind != "matrix" && kind != "vector") {
		return typedBody{}, false
	}
	out.vector = kind == "vector"
	s.i = resultAt
	if !s.series(&out) {
		return typedBody{}, false
	}
	return out, true
}

// typedScanner is a cursor over a JSON document.
type typedScanner struct {
	b []byte
	i int
}

func (s *typedScanner) ws() {
	for s.i < len(s.b) && (s.b[s.i] == ' ' || s.b[s.i] == '\n' || s.b[s.i] == '\t' || s.b[s.i] == '\r') {
		s.i++
	}
}

func (s *typedScanner) peek() byte {
	s.ws()
	if s.i >= len(s.b) {
		return 0
	}
	return s.b[s.i]
}

func (s *typedScanner) expect(c byte) bool {
	if s.peek() != c {
		return false
	}
	s.i++
	return true
}

// str reads a JSON string.
func (s *typedScanner) str() (string, bool) {
	if s.peek() != '"' {
		return "", false
	}
	start := s.i
	s.i++
	escaped := false
	for s.i < len(s.b) {
		switch s.b[s.i] {
		case '\\':
			escaped = true
			s.i += 2
			continue
		case '"':
			s.i++
			if !escaped {
				return string(s.b[start+1 : s.i-1]), true
			}
			var out string
			if json.Unmarshal(s.b[start:s.i], &out) != nil {
				return "", false
			}
			return out, true
		}
		s.i++
	}
	return "", false
}

// number reads a JSON number.
func (s *typedScanner) number() (float64, bool) {
	s.ws()
	start := s.i
	for s.i < len(s.b) && (s.b[s.i] >= '0' && s.b[s.i] <= '9' || s.b[s.i] == '-' || s.b[s.i] == '+' || s.b[s.i] == '.' || s.b[s.i] == 'e' || s.b[s.i] == 'E') {
		s.i++
	}
	if s.i == start {
		return 0, false
	}
	if v, ok := smallUint(s.b[start:s.i]); ok {
		return v, true
	}
	v, err := strconv.ParseFloat(string(s.b[start:s.i]), 64)
	return v, err == nil
}

// smallUint reads up to 15 decimal digits (a sample's whole-second timestamp).
// A float64 holds every such integer exactly, so the result is the one
// strconv.ParseFloat returns, without converting the bytes to a string.
func smallUint(b []byte) (float64, bool) {
	if len(b) == 0 || len(b) > 15 {
		return 0, false
	}
	var u uint64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		u = u*10 + uint64(c-'0')
	}
	return float64(u), true
}

// skip passes over one JSON value.
func (s *typedScanner) skip() bool {
	switch c := s.peek(); c {
	case 0:
		return false
	case '"':
		_, ok := s.str()
		return ok
	case '{', '[':
		depth := 0
		for s.i < len(s.b) {
			switch s.b[s.i] {
			case '"':
				if _, ok := s.str(); !ok {
					return false
				}
				continue
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					s.i++
					return true
				}
			}
			s.i++
		}
		return false
	default:
		for s.i < len(s.b) && s.b[s.i] != ',' && s.b[s.i] != '}' && s.b[s.i] != ']' && s.b[s.i] != ' ' && s.b[s.i] != '\n' && s.b[s.i] != '\t' && s.b[s.i] != '\r' {
			s.i++
		}
		return true
	}
}

// series reads the "result" array into out.
func (s *typedScanner) series(out *typedBody) bool {
	if !s.expect('[') {
		return false
	}
	for first := true; ; first = false {
		if s.peek() == ']' {
			s.i++
			return true
		}
		if !first && !s.expect(',') {
			return false
		}
		if !s.expect('{') {
			return false
		}
		series := typedSeries{}
		seenMetric, seenValue := false, false
		for firstKey := true; ; firstKey = false {
			if s.peek() == '}' {
				s.i++
				break
			}
			if !firstKey && !s.expect(',') {
				return false
			}
			key, ok := s.str()
			if !ok || !s.expect(':') {
				return false
			}
			switch {
			case key == "metric":
				if series.metric, ok = s.metric(); !ok {
					return false
				}
				seenMetric = true
			case key == "values" && !out.vector:
				if !s.points(&series, false) {
					return false
				}
				seenValue = true
			case key == "value" && out.vector:
				if !s.points(&series, true) {
					return false
				}
				seenValue = true
			default:
				return false
			}
		}
		if !seenMetric || (out.vector && !seenValue) {
			return false
		}
		out.series = append(out.series, series)
	}
}

// metric reads an object of string values.
func (s *typedScanner) metric() (map[string]string, bool) {
	if !s.expect('{') {
		return nil, false
	}
	m := map[string]string{}
	for first := true; ; first = false {
		if s.peek() == '}' {
			s.i++
			return m, true
		}
		if !first && !s.expect(',') {
			return nil, false
		}
		k, ok := s.str()
		if !ok || !s.expect(':') {
			return nil, false
		}
		v, ok := s.str()
		if !ok {
			return nil, false
		}
		m[k] = v
	}
}

// points reads the samples of a matrix series (an array of pairs), or the one
// pair of a vector series.
func (s *typedScanner) points(series *typedSeries, one bool) bool {
	if one {
		return s.pair(series, true)
	}
	if !s.expect('[') {
		return false
	}
	for first := true; ; first = false {
		if s.peek() == ']' {
			s.i++
			return true
		}
		if !first && !s.expect(',') {
			return false
		}
		if !s.pair(series, false) {
			return false
		}
	}
}

// pair reads [timestamp, value]: a pair of fewer than two elements is passed
// over, a value that is neither a string nor a number is passed over in a
// matrix and reads 0 in a vector.
func (s *typedScanner) pair(series *typedSeries, vector bool) bool {
	if !s.expect('[') {
		return false
	}
	if s.peek() == ']' {
		s.i++
		return !vector
	}
	ts, ok := s.number()
	if !ok {
		return false
	}
	if s.peek() == ']' {
		s.i++
		return !vector
	}
	if !s.expect(',') {
		return false
	}
	var v float64
	keep := true
	switch c := s.peek(); {
	case c == '"':
		raw, ok := s.str()
		if !ok {
			return false
		}
		v, _ = strconv.ParseFloat(raw, 64)
	case c == '-' || c >= '0' && c <= '9':
		if v, ok = s.number(); !ok {
			return false
		}
	default:
		if !s.skip() {
			return false
		}
		keep = vector
	}
	for s.peek() == ',' { // elements past the value
		s.i++
		if !s.skip() {
			return false
		}
	}
	if !s.expect(']') {
		return false
	}
	if keep {
		series.points = append(series.points, typedPoint{ts, v})
	}
	return true
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
