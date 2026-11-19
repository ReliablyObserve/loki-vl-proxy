package proxy

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/grafana/jsonparser"
)

// A folded read's answer, decoded once (seriesFoldRead.decode) from the rows of /select/logsql/query as they arrive,
// into the results stats_query_range would answer for a stats query of the read's groups: a result per column and
// group, ordered as VictoriaLogs orders them, each one's points by time. The decoded answer is what identical reads
// in flight share (seriesFoldPost, the coalescer's SharedValuer); a read files it straight into its buckets
// (fileSeriesFoldAnswer), or writes it as the stats_query_range answer it stands for (writeSeriesFoldMatrix) when a
// check of the answer reads the body (the unwrap check's flagged groups, the JSON parser check, a bound).

// seriesFoldPoint is one bucket's value of one result of a folded answer: its time and its value as the answer
// writes it (a JSON string's content, escaped).
type seriesFoldPoint struct {
	ts    int64
	value []byte
}

// seriesFoldSeries is one result of a folded answer read back into stats_query_range's shape: its column, name and
// label values (the last stats pipe's grouping less the bucket, then the unwrap check's flag, as the answer writes
// them), and its points.
type seriesFoldSeries struct {
	column int
	name   string
	labels [][]byte
	points []seriesFoldPoint
	sort   string
}

// seriesFoldAnswer is a decoded folded answer. It is shared by the reads of one call and never changed once decoded.
type seriesFoldAnswer struct {
	// labels are the label names of every result: the grouping less the bucket, then the flag (unwrapBadField),
	// which the answer holds when hasFlag; results carry a value per name (the flag's empty without it).
	labels  [][]byte
	hasFlag bool
	results []*seriesFoldSeries
	// matrixLen is the exact length of the stats_query_range answer the results stand for (writeSeriesFoldMatrix),
	// cleanFlags the count of results whose flag is "0" (the unwrap check cuts it out of a clean answer).
	matrixLen, cleanFlags int
	// flagged: a result's flag names a group of flagged rows (1) or of NaN rows (2), which the unwrap check reads;
	// plain: no label value holds an escape and every value is a plain number's text, so the answer files with no
	// JSON decoding left (fileSeriesFoldAnswer).
	flagged, plain bool
}

// SharedValue makes the decoded answer the value identical reads share (middleware.SharedValuer).
func (a *seriesFoldAnswer) SharedValue() any { return a }

// Read and Close let the answer stand as the shared call's body; the coalescer reads its value, never its bytes.
func (a *seriesFoldAnswer) Read([]byte) (int, error) { return 0, io.EOF }
func (a *seriesFoldAnswer) Close() error             { return nil }

// seriesFoldField is one field of a folded answer's row: its name and its value as the row writes them.
type seriesFoldField struct{ key, value []byte }

// seriesFoldFlatPoint is a point of a folded answer filed under its result (an index of the decoder's results).
type seriesFoldFlatPoint struct {
	series int
	point  seriesFoldPoint
}

// The keys of a folded answer's rows the reader takes, and a count of one.
var (
	seriesFoldTimeKey  = []byte("_time")
	seriesFoldValueKey = []byte(lower.SeriesFoldField)
	seriesFoldCountKey = []byte(lower.SeriesFoldSeriesCount)
	seriesFoldOne      = []byte("1")
	seriesFoldFlagName = []byte(unwrapBadField)
)

// matrix reads a folded read's answer (rows of /select/logsql/query, one per bucket and group of the query's last
// stats pipe) back into the answer stats_query_range gives for a stats query of these groups (decode, then
// writeSeriesFoldMatrix).
func (s *seriesFoldRead) matrix(raw []byte) ([]byte, error) {
	a, err := s.decode(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	return writeSeriesFoldMatrix(a, false), nil
}

// decode reads a folded read's answer from r, row by row: a result per column and group, its labels the grouping
// less the bucket and its points the buckets, ordered as VictoriaLogs orders them. A folded group (an empty series
// hash) answers its merged value and series count in the value and count columns; a group that keeps its hash
// answers the bucket stats' results (and a count of one when the fold adds the count column). A row that is not an
// object of strings with a bucket time fails the read; an error of r (the body bound) fails it first. The values are
// copied as the answer writes them, never decoded. The stats_query_range answer they stand for is bounded as its
// writer bounded it: past twice the stats body bound by its estimate, or past the bound, the read fails with
// errBodyTooLarge.
func (s *seriesFoldRead) decode(r io.Reader) (*seriesFoldAnswer, error) {
	if s == nil {
		return nil, &statsBucketsUnreadableError{"folded read without its grouping"}
	}
	labels := make([][]byte, 0, len(s.by)+1)
	for _, b := range s.by {
		if name, err := strconv.Unquote(b); err == nil {
			b = name
		}
		labels = append(labels, []byte(b))
	}
	sid := slices.IndexFunc(labels, func(l []byte) bool { return string(l) == dynamicSeriesHashLabel })
	if sid < 0 {
		return nil, &statsBucketsUnreadableError{"folded read without its series hash"}
	}
	// The unwrap check's counting form groups by its flag too: every row then names it. The flag is the last label;
	// the answer holds it when a row does.
	labels = append(labels, seriesFoldFlagName)
	columns := make([][]byte, len(s.columns))
	for i, c := range s.columns {
		columns[i] = []byte(c)
	}
	valueColumn, countColumn, countIndex := slices.Index(s.columns, s.valueColumn), s.countColumn, s.countIndex()
	a := &seriesFoldAnswer{labels: labels, plain: true}
	var (
		byKey   = map[string]int{}
		times   = map[string]int64{}
		fields  []seriesFoldField
		values  = make([][]byte, len(labels))
		flat    []seriesFoldFlatPoint
		key     []byte
		rowKey  []byte
		scratch = make([]byte, 0, 64)
		arena   seriesFoldArena
		failed  error
	)
	field := func(name []byte) []byte {
		for _, f := range fields {
			if bytes.Equal(f.key, name) {
				return f.value
			}
		}
		return nil
	}
	// add files a point under its column, name and the row's label values (rowKey, built once per row).
	add := func(column int, name string, ts int64, value []byte) {
		key = append(key[:0], byte(column>>24), byte(column>>16), byte(column>>8), byte(column))
		key = append(append(key, name...), rowKey...)
		i, ok := byKey[string(key)]
		if !ok {
			i = len(a.results)
			byKey[string(key)] = i
			kept := make([][]byte, len(values))
			for j, v := range values {
				kept[j] = arena.copy(v)
			}
			a.results = append(a.results, &seriesFoldSeries{column: column, name: name, labels: kept})
		}
		a.plain = a.plain && plainNumberText(value)
		flat = append(flat, seriesFoldFlatPoint{series: i, point: seriesFoldPoint{ts: ts, value: arena.copy(value)}})
	}
	unreadable := &statsBucketsUnreadableError{"folded read: a row that is not an object of strings with its bucket"}
	row := func(line []byte) error {
		if line = bytes.TrimSpace(line); len(line) == 0 {
			return nil
		}
		// The row's structure is read by ObjectEach (a value that is not a string fails the read below); the string
		// contents are copied as written and checked once, in the stats_query_range answer they are written into.
		if line[0] != '{' || line[len(line)-1] != '}' {
			return unreadable
		}
		fields = fields[:0]
		allStrings := true
		if err := jsonparser.ObjectEach(line, func(k, v []byte, t jsonparser.ValueType, _ int) error {
			allStrings = allStrings && t == jsonparser.String
			fields = append(fields, seriesFoldField{k, v})
			return nil
		}); err != nil || !allStrings {
			return unreadable
		}
		at := field(seriesFoldTimeKey)
		ts, ok := times[string(at)]
		if !ok {
			if ts, ok = parseVLTimeNanos(string(at)); !ok {
				return unreadable
			}
			times[string(at)] = ts
		}
		rowKey = rowKey[:0]
		for i, l := range labels {
			values[i] = field(l)
			if bytes.IndexByte(values[i], '\\') >= 0 {
				a.plain = false
			}
			rowKey = appendSeriesFoldKeyValue(append(rowKey, 0), values[i], &scratch)
		}
		if values[len(values)-1] != nil {
			a.hasFlag = true
		}
		if len(values[sid]) == 0 {
			add(valueColumn, s.valueColumn, ts, field(seriesFoldValueKey))
			if countColumn != "" {
				add(countIndex, countColumn, ts, field(seriesFoldCountKey))
			}
			return nil
		}
		for i, c := range columns {
			add(i, s.columns[i], ts, field(c))
		}
		if s.added {
			add(len(s.columns), lower.SeriesFoldCount, ts, seriesFoldOne)
		}
		return nil
	}
	// The rows are read as they arrive: a malformed row fails the read once the whole answer is read, so an answer
	// past the body bound fails with that bound, as when the answer was read whole first.
	br := bufio.NewReaderSize(r, 64<<10)
	var long []byte
	for {
		line, err := br.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			long = append(long[:0], line...)
			for errors.Is(err, bufio.ErrBufferFull) {
				line, err = br.ReadSlice('\n')
				long = append(long, line...)
			}
			line = long
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		if failed == nil && len(line) > 0 {
			failed = row(line)
		}
		if err != nil {
			break
		}
	}
	if failed != nil {
		return nil, failed
	}
	a.order(flat, times)
	if a.estimate() > 2*maxStatsBucketResponseBytes {
		return nil, errBodyTooLarge
	}
	if a.matrixLen = a.length(times); len(a.results) > 0 && int64(a.matrixLen-len(seriesFoldMatrixTail)) > maxStatsBucketResponseBytes {
		return nil, errBodyTooLarge
	}
	for _, ss := range a.results {
		flag := ss.labels[len(ss.labels)-1]
		switch {
		case !a.hasFlag:
		case string(flag) == "0":
			a.cleanFlags++
		case len(flag) > 0 && (flag[0] == '1' || flag[0] == unwrapNaNFlag[0]):
			a.flagged = true
		}
	}
	return a, nil
}

// plainNumberText reports a value whose text is a number's as VictoriaLogs writes one (digits, a sign, a point, an
// exponent, NaN and Inf), or empty: no byte the stats_query_range answer would escape or split a point at.
func plainNumberText(v []byte) bool {
	for _, c := range v {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '.' || c == '-' || c == '+') {
			return false
		}
	}
	return true
}

// order sorts the results as stats_query_range orders them (by column, name and the non-empty labels, as
// VictoriaLogs keys a result) and gives each its points by time (stable: points of one time keep the rows' order),
// ranking the distinct times once and filing the points by rank, then by result, in linear time.
func (a *seriesFoldAnswer) order(flat []seriesFoldFlatPoint, times map[string]int64) {
	quoted := a.quotedLabels()
	for _, ss := range a.results {
		b := []byte{byte(ss.column >> 24), byte(ss.column >> 16), byte(ss.column >> 8), byte(ss.column)}
		b = append(b, ss.name...)
		b = append(b, '{')
		first := true
		for i, v := range ss.labels {
			if len(v) == 0 {
				continue
			}
			if !first {
				b = append(b, ',')
			}
			first = false
			b = appendSeriesFoldSortValue(append(append(b, quoted[i]...), ':'), v)
		}
		ss.sort = string(append(b, '}'))
	}
	// The times' ranks.
	distinct := make([]int64, 0, len(times))
	for _, ts := range times {
		distinct = append(distinct, ts)
	}
	slices.Sort(distinct)
	distinct = slices.Compact(distinct)
	rank := func(ts int64) int { return sort.Search(len(distinct), func(i int) bool { return distinct[i] >= ts }) }
	ranks := make([]int32, len(flat))
	byRank := make([]int, len(distinct)+1)
	for i, p := range flat {
		r := rank(p.point.ts)
		ranks[i] = int32(r)
		byRank[r+1]++
	}
	for i := 1; i < len(byRank); i++ {
		byRank[i] += byRank[i-1]
	}
	sorted := make([]int32, len(flat))
	for i := range flat {
		r := ranks[i]
		sorted[byRank[r]] = int32(i)
		byRank[r]++
	}
	// Then by result, each result's points in time order.
	offsets := make([]int, len(a.results)+1)
	for _, p := range flat {
		offsets[p.series+1]++
	}
	for i := 1; i < len(offsets); i++ {
		offsets[i] += offsets[i-1]
	}
	points := make([]seriesFoldPoint, len(flat))
	next := slices.Clone(offsets[:len(a.results)])
	for _, i := range sorted {
		p := flat[i]
		points[next[p.series]] = p.point
		next[p.series]++
	}
	for i, ss := range a.results {
		ss.points = points[offsets[i]:offsets[i+1]:offsets[i+1]]
	}
	slices.SortStableFunc(a.results, func(x, y *seriesFoldSeries) int { return strings.Compare(x.sort, y.sort) })
}

// quotedLabels are the label names as the answer writes them; the flag only when the answer holds it.
func (a *seriesFoldAnswer) quotedLabels() [][]byte {
	quoted := make([][]byte, len(a.labels))
	for i, l := range a.labels {
		quoted[i] = appendJSONString(nil, string(l))
	}
	return quoted
}

// written is the number of labels the answer writes per result: the flag only when the answer holds it.
func (a *seriesFoldAnswer) written() int {
	if a.hasFlag {
		return len(a.labels)
	}
	return len(a.labels) - 1
}

// estimate is the size writeSeriesFoldMatrix estimated for the answer (the bound it refused past twice the stats body
// bound).
func (a *seriesFoldAnswer) estimate() int64 {
	size := 64
	for _, ss := range a.results {
		size += 64 + len(ss.name) + len(ss.sort) + 32*len(ss.points)
		for _, pt := range ss.points {
			size += len(pt.value)
		}
	}
	return int64(size)
}

// The fixed parts of the stats_query_range answer writeSeriesFoldMatrix writes.
const (
	seriesFoldMatrixHead   = `{"status":"success","data":{"resultType":"matrix","result":[`
	seriesFoldMatrixTail   = "]}}"
	seriesFoldResultHead   = `{"metric":{"__name__":`
	seriesFoldResultValues = `},"values":[`
)

// length is the exact length of writeSeriesFoldMatrix's answer, from the answer's times (each written once).
func (a *seriesFoldAnswer) length(times map[string]int64) int {
	tsLen := make(map[int64]int, len(times))
	var buf [32]byte
	for _, ts := range times {
		if _, ok := tsLen[ts]; !ok {
			tsLen[ts] = len(strconv.AppendFloat(buf[:0], float64(ts)/1e9, 'f', -1, 64))
		}
	}
	quoted := a.quotedLabels()
	n := len(seriesFoldMatrixHead) + len(seriesFoldMatrixTail)
	for i, ss := range a.results {
		if i > 0 {
			n++
		}
		n += len(seriesFoldResultHead) + len(appendJSONString(buf[:0], ss.name)) + len(seriesFoldResultValues) + len("]}")
		for j := range a.written() {
			n += 1 + len(quoted[j]) + 3 + len(ss.labels[j])
		}
		for j, pt := range ss.points {
			if j > 0 {
				n++
			}
			n += 1 + tsLen[pt.ts] + 2 + len(pt.value) + 2
		}
	}
	return n
}

// writeSeriesFoldMatrix writes the results of a folded answer as stats_query_range writes them: ordered by column,
// name and labels, each one's points by time, a time in seconds as float64(ns)/1e9 writes, the values as the answer
// wrote them. dropFlag leaves a clean result's flag out (`,"__lvp_bad":"0"`), as the unwrap check cuts it.
func writeSeriesFoldMatrix(a *seriesFoldAnswer, dropFlag bool) []byte {
	quoted := a.quotedLabels()
	written := a.written()
	size := a.matrixLen
	if size == 0 {
		size = int(a.estimate())
	}
	out := make([]byte, 0, size)
	out = append(out, seriesFoldMatrixHead...)
	for i, ss := range a.results {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, seriesFoldResultHead...)
		out = appendJSONString(out, ss.name)
		for j := range written {
			if dropFlag && j == len(a.labels)-1 && string(ss.labels[j]) == "0" {
				continue
			}
			out = append(append(append(append(append(out, ','), quoted[j]...), ':', '"'), ss.labels[j]...), '"')
		}
		out = append(out, seriesFoldResultValues...)
		for j, pt := range ss.points {
			if j > 0 {
				out = append(out, ',')
			}
			out = append(out, '[')
			out = strconv.AppendFloat(out, float64(pt.ts)/1e9, 'f', -1, 64)
			out = append(append(append(out, ',', '"'), pt.value...), '"', ']')
		}
		out = append(out, "]}"...)
	}
	return append(out, seriesFoldMatrixTail...)
}

// strippedLen is the length of the answer's matrix once the unwrap check cut its clean flags out (dropFlag).
func (a *seriesFoldAnswer) strippedLen() int {
	return a.matrixLen - a.cleanFlags*(1+len(unwrapKeptFlag))
}

// countIndex is the column of the count a folded group answers.
func (s *seriesFoldRead) countIndex() int {
	if i := slices.Index(s.columns, s.countColumn); i >= 0 {
		return i
	}
	return len(s.columns)
}

// appendSeriesFoldKeyValue appends a label value's text: the value as the answer wrote it, decoded when it holds an
// escape, so two spellings of one value are one group. scratch is a buffer the decoding may use.
func appendSeriesFoldKeyValue(dst, v []byte, scratch *[]byte) []byte {
	if bytes.IndexByte(v, '\\') >= 0 {
		if text, err := jsonparser.Unescape(v, (*scratch)[:cap(*scratch)]); err == nil {
			if cap(text) > cap(*scratch) {
				*scratch = text
			}
			return append(dst, text...)
		}
	}
	return append(dst, v...)
}

// appendSeriesFoldSortValue appends a label value as the answer wrote it (a JSON string's content) in the spelling
// appendJSONString gives its decoded text, so results sort by their values whatever escapes VictoriaLogs chose: a
// value holding an escape or a byte appendJSONString escapes is decoded and written again, any other is that spelling
// already.
func appendSeriesFoldSortValue(dst, v []byte) []byte {
	for _, c := range v {
		if c < 0x20 || c == '\\' || c == '"' || c == '<' || c == '>' || c == '&' {
			if text, err := jsonparser.ParseString(v); err == nil {
				return appendJSONString(dst, text)
			}
			break
		}
	}
	return append(append(append(dst, '"'), v...), '"')
}

// seriesFoldArena holds the bytes a decoded answer keeps of its rows (label and point values), in blocks, so the rows
// themselves are not kept.
type seriesFoldArena struct{ cur []byte }

const seriesFoldArenaBlock = 64 << 10

// copy keeps b in the arena; an empty value is kept as nil.
func (ar *seriesFoldArena) copy(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	if cap(ar.cur)-len(ar.cur) < len(b) {
		ar.cur = make([]byte, 0, max(seriesFoldArenaBlock, len(b)))
	}
	n := len(ar.cur)
	ar.cur = append(ar.cur, b...)
	return ar.cur[n:len(ar.cur):len(ar.cur)]
}
