package vlemu

import (
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// The fold of a parser's series that lie in one bucket (the proxy's lower.SeriesFold). A subquery classes each
// series hash over the rows the read selects: "s" spanning (lines in more than one bucket of the read's grid, or a
// line whose value holds "nan" or "inf"), "m" several lines in one bucket, none for one line. Before the bucket stats
// pipe each row gets its series' class (__lvp_c) and, where the read folds them there, the hash of a series of one
// line is blanked; after it the hash of a series of several lines in one bucket is blanked and a second stats pipe
// adds the blanked rows up. The emulator runs the subquery's meaning, without its pipes, over the rows the request
// selects (VictoriaLogs scopes a subquery to the read's time range).

// seriesFoldSpanField is the field every subquery of a folded read is cut to (cutSeriesFold).
const seriesFoldSpanField = "__lvp_span"

var (
	seriesFoldGridRE = regexp.MustCompile(`stats by \(__lvp_sid, _time:(\S+) offset (\S+)\) `)
	// format [if (<condition>)] "<template>" as <field>, the fold's own pipes.
	seriesFoldFormatRE = regexp.MustCompile(`^format (?:if \((.*)\) )?"(.*)" as (\w+)$`)
	// math <a> / <b> as <c> and math <a> * <b> as <c>: the value of a series in a bucket.
	emuMathOpRE = regexp.MustCompile(`^math (\w+) ([/*]) (\w+) as (\w+)$`)
	// <field>:<op><value> terms of a fold condition: :="v", :in(__lvp_span), :<inf, :>-inf, :* (each may be negated).
	seriesFoldTermRE = regexp.MustCompile(`^(-?)(\w+):(=".*"|in\(` + seriesFoldSpanField + `\)|<inf|>-inf|\*)$`)
)

// emuSeriesFold is a folded read: the subquery's grid and the series classes over the read's rows.
type emuSeriesFold struct {
	bucket, offset int64
	classes        map[string]string
}

// cutSeriesFold cuts every subquery of a folded read out of its query, each written as in(__lvp_span), and returns
// the subquery's bucket grid; nil when the query does not fold.
func cutSeriesFold(e *Emulator, query string) (string, *emuSeriesFold) {
	const head = ":in("
	var fold *emuSeriesFold
	for from := 0; ; {
		at := strings.Index(query[from:], head)
		if at < 0 {
			return query, fold
		}
		open := from + at + len(head)
		from = open
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
			e.T.Fatalf("emulator: unclosed in( in %s", query)
		}
		m := seriesFoldGridRE.FindStringSubmatch(query[open:end])
		if m == nil {
			continue // a list of values, or a subquery cut already
		}
		bucket, err1 := time.ParseDuration(m[1])
		offset, err2 := time.ParseDuration(m[2])
		if err1 != nil || err2 != nil || bucket <= 0 {
			e.T.Fatalf("emulator: subquery grid %q offset %q", m[1], m[2])
		}
		fold = &emuSeriesFold{bucket: int64(bucket), offset: int64(offset)}
		query = query[:open] + seriesFoldSpanField + query[end:]
	}
}

// classify reads the series classes of rows (their fields and times; raw the unwrapped value of each).
func (s *emuSeriesFold) classify(rows []emuRec, raw []string) {
	type seen struct {
		first     int64
		lines     int
		span, odd bool
	}
	by := map[string]*seen{}
	for i, rec := range rows {
		sid := rec.f[seriesHashField]
		b := FloorDiv(rec.t+s.offset, s.bucket)
		st := by[sid]
		if st == nil {
			st = &seen{first: b}
			by[sid] = st
		}
		st.lines++
		st.span = st.span || b != st.first
		v := strings.ToLower(raw[i])
		st.odd = st.odd || strings.Contains(v, "nan") || strings.Contains(v, "inf")
	}
	s.classes = map[string]string{}
	for sid, st := range by {
		switch {
		case st.span || st.odd:
			s.classes[sid] = "s"
		case st.lines > 1:
			s.classes[sid] = "m"
		}
	}
}

// keys is the subquery's answer: "s<hash>" and "m<hash>" by class.
func (s *emuSeriesFold) keys() map[string]bool {
	out := map[string]bool{}
	for sid, c := range s.classes {
		out[c+sid] = true
	}
	return out
}

// foldPipe runs one pipe of the fold over recs and reports whether it was one.
func (s *emuSeriesFold) foldPipe(pipe string, recs []emuRec) bool {
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
	m := seriesFoldFormatRE.FindStringSubmatch(pipe)
	if m == nil || !seriesFoldFormat(m[1], m[3]) {
		return false
	}
	keys := s.keys()
	for _, rec := range recs {
		if m[1] != "" && !s.holds(m[1], rec.f, keys) {
			continue
		}
		v := m[2]
		for _, ref := range regexp.MustCompile(`<(\w+)>`).FindAllStringSubmatch(v, -1) {
			v = strings.ReplaceAll(v, ref[0], rec.f[ref[1]])
		}
		rec.f[m[3]] = v
	}
	return true
}

// holds reports whether the fields f match the condition cond, a list of the terms seriesFoldTermRE reads.
func (s *emuSeriesFold) holds(cond string, f map[string]string, keys map[string]bool) bool {
	for _, term := range strings.Fields(cond) {
		m := seriesFoldTermRE.FindStringSubmatch(term)
		if m == nil {
			panic("emulator: fold condition " + cond)
		}
		v := f[m[2]]
		var ok bool
		switch op := m[3]; {
		case op == "*":
			ok = v != ""
		case op == "<inf" || op == ">-inf":
			x, err := strconv.ParseFloat(v, 64)
			ok = err == nil && !math.IsNaN(x) && (op == "<inf" && !math.IsInf(x, 1) || op == ">-inf" && !math.IsInf(x, -1))
		case strings.HasPrefix(op, "in("):
			ok = keys[v]
		default:
			want, _ := strconv.Unquote(op[1:])
			ok = v == want
		}
		if ok == (m[1] == "-") {
			return false
		}
	}
	return true
}

// seriesFoldFormat reports a format pipe of the fold: one that writes its keys, the class, the mark or the hash, or
// writes a column of a blanked row.
func seriesFoldFormat(cond, field string) bool {
	switch field {
	case "__lvp_ks", "__lvp_km", "__lvp_c", "__lvp_fb", seriesHashField:
		return true
	}
	return cond == `__lvp_fb:="1"` || cond == `__lvp_sid:=""`
}
