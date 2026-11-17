package proxy

import (
	"bufio"
	"io"
	"sync"
	"sync/atomic"
	"time"

	fj "github.com/valyala/fastjson"
)

// patternLineRow is a row of the exact tier's lines query, decoded.
type patternLineRow struct {
	stream, level, msg string
	timeNs             int64
	// timeOK is false when the row's _time is no time; missing is true when the row has no message
	// (isVLMissingMsgBytes: an empty one, or VictoriaLogs' marker of none).
	timeOK, missing bool
	// err is set when the row is not JSON: the read fails there.
	err error
}

// patternLineDecoder decodes rows of the lines query. It keeps the level fields' cache of the stream labels and
// the last stream's name (the rows of a stream come one after another, so its name is made once): each goroutine
// that decodes rows holds its own.
type patternLineDecoder struct {
	levels logRowLevels
	stream string
}

func newPatternLineRowDecoder(levels logRowLevels) *patternLineDecoder {
	levels.streams = make(map[string]logRowStream, 16)
	return &patternLineDecoder{levels: levels}
}

func (d *patternLineDecoder) decode(v *fj.Value, row *patternLineRow) {
	msg := v.GetStringBytes("_msg")
	*row = patternLineRow{msg: string(msg), missing: isVLMissingMsgBytes(msg, d.levels.defaultMsg)}
	row.timeNs, row.timeOK = patternRowTimeNs(v.GetStringBytes("_time"))
	if id := v.GetStringBytes(patternExactStreamField); string(id) != d.stream {
		d.stream = string(id)
	}
	row.stream = d.stream
	row.level = patternLevelFromFJ(v, &d.levels)
}

// scanPatternLineRows hands each row of the lines query's answer r to consume, decoded, in the order of the
// answer: consume returns false for a row it cannot use. More than maxRows rows, a row that is not JSON or one
// consume rejects is an error, as with scanPatternRows. With decoders above 1, a goroutine reads the lines and
// decoders goroutines decode batches of them while consume runs on the caller's goroutine, row after row in
// order; the read stops at the first error as one goroutine stops, and every goroutine has ended on return.
func scanPatternLineRows(r io.Reader, maxRows int, levels logRowLevels, decoders int, consume func(*patternLineRow) bool) error {
	if decoders < 2 {
		d := newPatternLineRowDecoder(levels)
		var row patternLineRow
		return scanPatternRows(r, maxRows, func(v *fj.Value) bool {
			d.decode(v, &row)
			return consume(&row)
		})
	}
	s := &patternRowScan{
		parse: make(chan *patternRowBatch, decoders),
		order: make(chan *patternRowBatch, decoders*patternMineQueueBatches),
	}
	s.wg.Add(1 + decoders)
	go s.read(r, maxRows)
	for range decoders {
		go s.decode(levels)
	}
	err := s.consume(consume)
	s.wg.Wait()
	return err
}

// patternRowBatch is a run of the answer's lines: the bytes of each, then each one decoded, then the error the
// read ended with after them, if it ended there.
type patternRowBatch struct {
	raw  []byte
	ends []int
	rows []patternLineRow
	tail error
	done chan struct{}
}

type patternRowScan struct {
	parse, order chan *patternRowBatch
	// stop is set once consume met an error: the reader stops reading and the decoders decoding.
	stop atomic.Bool
	wg   sync.WaitGroup
}

// patternRowBatches keeps the batches of finished reads for the next ones.
var patternRowBatches = sync.Pool{New: func() any {
	return &patternRowBatch{raw: make([]byte, 0, patternMineBatchBytes+64<<10), ends: make([]int, 0, patternMineBatchRows)}
}}

func (s *patternRowScan) batch() *patternRowBatch {
	b := patternRowBatches.Get().(*patternRowBatch)
	b.raw, b.ends, b.rows, b.tail = b.raw[:0], b.ends[:0], b.rows[:0], nil
	b.done = make(chan struct{})
	return b
}

func (s *patternRowScan) send(b *patternRowBatch) {
	s.order <- b
	s.parse <- b
}

// read splits the answer into batches of lines, as scanPatternRows reads it, and hands each to a decoder and, in
// the answer's order, to consume.
func (s *patternRowScan) read(r io.Reader, maxRows int) {
	defer s.wg.Done()
	defer close(s.parse)
	defer close(s.order)
	b := s.batch()
	rows := 0
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), maxPatternCountsRowBytes)
	for sc.Scan() {
		if s.stop.Load() {
			return
		}
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		if rows++; rows > maxRows {
			b.tail = errPatternCounts{"more rows than -patterns-max-counts-rows"}
			s.send(b)
			return
		}
		b.raw = append(b.raw, line...)
		b.ends = append(b.ends, len(b.raw))
		if len(b.ends) == patternMineBatchRows || len(b.raw) >= patternMineBatchBytes {
			s.send(b)
			b = s.batch()
		}
	}
	b.tail = sc.Err()
	s.send(b)
}

func (s *patternRowScan) decode(levels logRowLevels) {
	defer s.wg.Done()
	d := newPatternLineRowDecoder(levels)
	var parser fj.Parser
	for b := range s.parse {
		if !s.stop.Load() {
			b.rows = slicesGrowTo(b.rows, len(b.ends))
			from := 0
			for i, end := range b.ends {
				row := &b.rows[i]
				v, err := parser.ParseBytes(b.raw[from:end])
				from = end
				if err != nil {
					*row = patternLineRow{err: err}
					b.rows = b.rows[:i+1]
					break
				}
				d.decode(v, row)
			}
		}
		close(b.done)
	}
}

// slicesGrowTo is rows with length n, reusing its array when it holds n.
func slicesGrowTo(rows []patternLineRow, n int) []patternLineRow {
	if cap(rows) >= n {
		return rows[:n]
	}
	return make([]patternLineRow, n)
}

// consume hands the decoded rows to consume in the answer's order and returns the read's error: the first row
// that is not JSON or that consume rejects, or the error the read ended with.
func (s *patternRowScan) consume(consume func(*patternLineRow) bool) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.halt()
			s.wg.Wait()
			panic(r)
		}
		if err != nil {
			s.halt()
		}
	}()
	for b := range s.order {
		<-b.done
		for i := range b.rows {
			row := &b.rows[i]
			if row.err != nil {
				return row.err
			}
			if !consume(row) {
				return errPatternCounts{"unexpected row"}
			}
		}
		if b.tail != nil {
			return b.tail
		}
		clear(b.rows) // the lines' text is not held past the batch
		patternRowBatches.Put(b)
	}
	return nil
}

// halt stops the reader and the decoders, and takes the batches the reader still hands over so that it can end.
func (s *patternRowScan) halt() {
	s.stop.Store(true)
	for b := range s.order {
		_ = b
	}
}

// patternRowTimeNs is parseLokiTimeToUnixNano of a row's _time. The time as VictoriaLogs writes it (RFC 3339 in
// UTC, whole seconds or up to 9 digits of fraction) is read from the bytes; any other text, and one out of range,
// is read by parseLokiTimeToUnixNano.
func patternRowTimeNs(b []byte) (int64, bool) {
	if ns, ok, read := parseVLRowTime(b); read {
		return ns, ok
	}
	return parseLokiTimeToUnixNano(string(b))
}

// parseVLRowTime reads YYYY-MM-DDTHH:MM:SS[.F{1,9}]Z with every field in range; read is false for anything else.
func parseVLRowTime(b []byte) (ns int64, ok, read bool) {
	if len(b) < 20 || len(b) > 30 || b[4] != '-' || b[7] != '-' || b[10] != 'T' || b[13] != ':' || b[16] != ':' || b[len(b)-1] != 'Z' {
		return 0, false, false
	}
	digits := func(from, to int) (int, bool) {
		n := 0
		for _, c := range b[from:to] {
			if c < '0' || c > '9' {
				return 0, false
			}
			n = n*10 + int(c-'0')
		}
		return n, true
	}
	year, okY := digits(0, 4)
	month, okM := digits(5, 7)
	day, okD := digits(8, 10)
	hour, okH := digits(11, 13)
	minute, okMin := digits(14, 16)
	sec, okS := digits(17, 19)
	if !okY || !okM || !okD || !okH || !okMin || !okS {
		return 0, false, false
	}
	nsec := 0
	if frac := b[19 : len(b)-1]; len(frac) > 0 {
		if frac[0] != '.' || len(frac) < 2 {
			return 0, false, false
		}
		n, okF := digits(20, len(b)-1)
		if !okF {
			return 0, false, false
		}
		for i := len(frac) - 1; i < 9; i++ {
			n *= 10
		}
		nsec = n
	}
	if month < 1 || month > 12 || day < 1 || day > patternDaysIn(month, year) || hour > 23 || minute > 59 || sec > 59 {
		return 0, false, false
	}
	ns, ok = unixNanoIfHeld(time.Date(year, time.Month(month), day, hour, minute, sec, nsec, time.UTC))
	return ns, ok, true
}

func patternDaysIn(month, year int) int {
	switch month {
	case 2:
		if year%4 == 0 && (year%100 != 0 || year%400 == 0) {
			return 29
		}
		return 28
	case 4, 6, 9, 11:
		return 30
	}
	return 31
}
