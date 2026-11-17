package vlemu

import (
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// The fold of a parser's series that lie in one bucket (the proxy's lower.SeriesFold): after the bucket stats pipe,
// a row's hash is blanked unless its series is in the subquery's list (the series with lines in more than one bucket
// of the read's grid, or a line whose value holds "nan"), and a second stats pipe adds the blanked rows up. The
// emulator runs the subquery's meaning over the rows the request selects (VictoriaLogs scopes a subquery to the
// read's time range), without its pipes.

// seriesFoldBlank is the pipe that blanks the hash, its subquery cut out (cutSeriesFold).
const seriesFoldBlank = `format if (-__lvp_sid:in(__lvp_span) __lvp_fold:<inf __lvp_fold:>-inf) "" as __lvp_sid`

var (
	seriesFoldGridRE  = regexp.MustCompile(`stats by \(__lvp_sid, _time:(\S+) offset (\S+)\) `)
	seriesFoldWriteRE = regexp.MustCompile(`^format if \(__lvp_sid:=""\) "(.*)" as (\w+)$`)
	// math <a> / <b> as <c> and math <a> * <b> as <c>: the value of a series in a bucket.
	emuMathOpRE = regexp.MustCompile(`^math (\w+) ([/*]) (\w+) as (\w+)$`)
)

// emuSeriesFold is the subquery's grid of a folded read.
type emuSeriesFold struct{ bucket, offset int64 }

// cutSeriesFold cuts the subquery out of a folded read's query: the query with the blanking pipe written as
// seriesFoldBlank, and the subquery's bucket grid; nil when the query does not fold.
func cutSeriesFold(e *Emulator, query string) (string, *emuSeriesFold) {
	const head = "-__lvp_sid:in("
	at := strings.Index(query, head)
	if at < 0 {
		return query, nil
	}
	open := at + len(head)
	depth, end := 1, -1
	for i := open; i < len(query) && end < 0; i++ {
		switch query[i] {
		case '"':
			for i++; i < len(query) && query[i] != '"'; i++ {
				if query[i] == '\\' {
					i++
				}
			}
		case '(':
			depth++
		case ')':
			if depth--; depth == 0 {
				end = i
			}
		}
	}
	if end < 0 {
		e.T.Fatalf("emulator: unclosed subquery in %s", query)
	}
	m := seriesFoldGridRE.FindStringSubmatch(query[open:end])
	if m == nil {
		e.T.Fatalf("emulator: no bucket grid in the subquery %s", query[open:end])
	}
	bucket, err1 := time.ParseDuration(m[1])
	offset, err2 := time.ParseDuration(m[2])
	if err1 != nil || err2 != nil || bucket <= 0 {
		e.T.Fatalf("emulator: subquery grid %q offset %q", m[1], m[2])
	}
	return query[:open] + "__lvp_span" + query[end:], &emuSeriesFold{bucket: int64(bucket), offset: int64(offset)}
}

// spanning are the series hashes the subquery lists over rows (their fields and times): lines in more than one
// bucket of the grid, or a line whose unwrapped value holds "nan" in any case.
func (s *emuSeriesFold) spanning(rows []emuRec, raw []string) map[string]bool {
	first := map[string]int64{}
	out := map[string]bool{}
	for i, rec := range rows {
		sid := rec.f[seriesHashField]
		if strings.Contains(strings.ToLower(raw[i]), "nan") {
			out[sid] = true
		}
		b := FloorDiv(rec.t+s.offset, s.bucket)
		if prev, ok := first[sid]; !ok {
			first[sid] = b
		} else if prev != b {
			out[sid] = true
		}
	}
	return out
}

// foldPipe runs one pipe of the fold over recs and reports whether it was one.
func foldPipe(pipe string, recs []emuRec, span map[string]bool) bool {
	if pipe == seriesFoldBlank {
		for _, rec := range recs {
			v, err := strconv.ParseFloat(rec.f["__lvp_fold"], 64)
			if !span[rec.f[seriesHashField]] && err == nil && !math.IsInf(v, 0) && !math.IsNaN(v) {
				rec.f[seriesHashField] = ""
			}
		}
		return true
	}
	if m := seriesFoldWriteRE.FindStringSubmatch(pipe); m != nil {
		for _, rec := range recs {
			if rec.f[seriesHashField] == "" {
				rec.f[m[2]] = strings.ReplaceAll(m[1], "<__lvp_fold>", rec.f["__lvp_fold"])
			}
		}
		return true
	}
	if m := emuMathOpRE.FindStringSubmatch(pipe); m != nil {
		for _, rec := range recs {
			a, _ := strconv.ParseFloat(rec.f[m[1]], 64)
			b, _ := strconv.ParseFloat(rec.f[m[3]], 64)
			v := a * b
			if m[2] == "/" {
				v = a / b
			}
			rec.f[m[4]] = strconv.FormatFloat(v, 'f', -1, 64)
		}
		return true
	}
	return false
}
