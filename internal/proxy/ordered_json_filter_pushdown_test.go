package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// pushdownRow is one stored line: stream labels (spelled alike on both
// sides), structured metadata in its VictoriaLogs spelling (vl) and its Loki
// spelling (loki), and the raw log line.
type pushdownRow struct {
	ts     time.Time
	stream map[string]string
	vl     map[string]string
	loki   map[string]string
	msg    string
}

// pushdownFakeVL evaluates the LogsQL the stats pushdown and its probes emit
// with VictoriaLogs semantics over fixture rows: unpack_json flattens nested
// keys with a dot and keeps keys raw, unpack_logfmt splits on spaces, format
// copies a stored field, filter pipes compare stored or unpacked values and
// stats buckets group by the named fields.
type pushdownFakeVL struct {
	mu     sync.Mutex
	rows   []pushdownRow
	stored func(string) string // Loki label -> stored spelling
	stats  []string
	guards []string
	raw    int
	// presence answers the level presence probe from the rows; otherwise the
	// probe gets no counts (the proxy then keeps the full chain). presenceFail
	// makes it a backend error, presenceLine replaces the honest answer.
	presence      bool
	presenceFail  bool
	presenceLine  func(honest string) string
	presenceCalls []string
	// presenceGate, when set, holds every presence probe until it is closed.
	presenceGate chan struct{}
	// rawQueries holds the query and time range of every raw-row request.
	rawQueries []fakeRawQuery
	// jsonUnpack replaces vlUnpackJSONFields (vlFastjsonUnpackFields mirrors
	// VictoriaLogs' parser exactly).
	jsonUnpack func(string) map[string]string
	// onStats, when set, runs at the start of every stats_query_range request.
	onStats func(r *http.Request, query string)
}

type fakeRawQuery struct {
	query      string
	start, end int64
}

var pushdownLiteralFormatRE = regexp.MustCompile("^format if \\((.+)\\) \"([^<\"]+)\" as (\\S+)$")

var pushdownFormatRE = regexp.MustCompile("^format if \\((.+)\\) \"<([^>]+)>\" as (\\S+)$")

// pushdownFormatBlankRE matches format if (<filter>) "" as dst.
var pushdownFormatBlankRE = regexp.MustCompile("^format if \\((.+)\\) \"\" as (\\S+)$")

// pushdownFormatMarkRE matches format if (<filter>) "1" as dst.
var pushdownFormatMarkRE = regexp.MustCompile("^format if \\((.+)\\) \"1\" as (\\S+)$")

// vlUnpackJSONFields mirrors unpack_json: a line that is one JSON object after
// trimming whitespace yields its keys raw, nested objects flattened with a
// dot, arrays kept as their JSON text; anything else yields nothing.
func vlUnpackJSONFields(msg string) map[string]string {
	msg = strings.TrimSpace(msg)
	if msg == "" || msg[0] != '{' {
		return nil
	}
	dec := json.NewDecoder(strings.NewReader(msg))
	dec.UseNumber()
	var object map[string]any
	if dec.Decode(&object) != nil || dec.More() {
		return nil
	}
	out := map[string]string{}
	var walk func(prefix string, object map[string]any)
	walk = func(prefix string, object map[string]any) {
		for key, value := range object {
			switch v := value.(type) {
			case map[string]any:
				walk(prefix+key+".", v)
			case string:
				out[prefix+key] = v
			case json.Number:
				out[prefix+key] = v.String()
			case bool:
				out[prefix+key] = strconv.FormatBool(v)
			case nil:
				out[prefix+key] = ""
			default:
				raw, _ := json.Marshal(v)
				out[prefix+key] = string(raw)
			}
		}
	}
	walk("", object)
	return out
}

func (f *pushdownFakeVL) values(row pushdownRow) map[string]string {
	values := map[string]string{"_msg": row.msg}
	for k, v := range row.stream {
		values[k] = v
	}
	for k, v := range row.vl {
		values[k] = v
	}
	var streamParts []string
	for k, v := range row.stream {
		streamParts = append(streamParts, k+"="+strconv.Quote(v))
	}
	sort.Strings(streamParts)
	values["_stream"] = "{" + strings.Join(streamParts, ",") + "}"
	return values
}

func fieldList(pipe string) []string {
	open, closing := strings.Index(pipe, "("), strings.Index(pipe, ")")
	if open < 0 || closing < open {
		return nil
	}
	return strings.Split(pipe[open+1:closing], ", ")
}

// splitLogsQLPipes splits a query at " | " outside quoted strings.
func splitLogsQLPipes(query string) []string {
	var parts []string
	var quote byte
	last := 0
	for i := 0; i < len(query); i++ {
		c := query[i]
		switch {
		case quote != 0:
			if c == '\\' && quote == '"' {
				i++
			} else if c == quote {
				quote = 0
			}
		case c == '"' || c == '`':
			quote = c
		case c == ' ' && strings.HasPrefix(query[i:], " | "):
			parts = append(parts, query[last:i])
			last = i + 3
			i += 2
		}
	}
	return append(parts, query[last:])
}

// fakeLogsQLFilter parses and evaluates the filter expressions the pushdown
// and its probes emit with VictoriaLogs semantics over a row's values:
// `field:="v"`, `field:~"re"`, `field:*`, `field:*"text"*`, `field:"phrase"` and `field:word`
// (a word delimited by non-word characters; letters, digits and underscores
// are word characters), `-`/`!` negation, parentheses, implicit and, `or`.
type fakeLogsQLFilter struct {
	t    testing.TB
	src  string
	pos  int
	vals map[string]string
}

func (f *fakeLogsQLFilter) skipSpace() {
	for f.pos < len(f.src) && f.src[f.pos] == ' ' {
		f.pos++
	}
}

func (f *fakeLogsQLFilter) peek(s string) bool {
	f.skipSpace()
	return strings.HasPrefix(f.src[f.pos:], s)
}

func (f *fakeLogsQLFilter) expr() bool {
	result := f.term()
	for f.peek("or ") || f.peek("OR ") {
		f.pos += 3
		result = f.term() || result
	}
	return result
}

func (f *fakeLogsQLFilter) term() bool {
	result := f.factor()
	for {
		f.skipSpace()
		if f.pos >= len(f.src) || f.peek(")") || f.peek("or ") || f.peek("OR ") {
			return result
		}
		if f.peek("and ") || f.peek("AND ") {
			f.pos += 4
		}
		result = f.factor() && result
	}
}

func (f *fakeLogsQLFilter) factor() bool {
	f.skipSpace()
	if f.peek("-") || f.peek("!") {
		f.pos++
		return !f.factor()
	}
	if f.peek("(") {
		f.pos++
		result := f.expr()
		if !f.peek(")") {
			f.t.Errorf("fake VL: expected ) at %d in %q", f.pos, f.src)
			return false
		}
		f.pos++
		return result
	}
	return f.fieldFilter()
}

func (f *fakeLogsQLFilter) quoted() string {
	f.skipSpace()
	if f.pos < len(f.src) && f.src[f.pos] == '`' {
		end := strings.IndexByte(f.src[f.pos+1:], '`')
		s := f.src[f.pos+1 : f.pos+1+end]
		f.pos += end + 2
		return s
	}
	if f.pos < len(f.src) && f.src[f.pos] == '"' {
		end := f.pos + 1
		for end < len(f.src) && f.src[end] != '"' {
			if f.src[end] == '\\' {
				end++
			}
			end++
		}
		s, err := strconv.Unquote(f.src[f.pos : end+1])
		if err != nil {
			f.t.Errorf("fake VL: bad string at %d in %q: %v", f.pos, f.src, err)
		}
		f.pos = end + 1
		return s
	}
	start := f.pos
	for f.pos < len(f.src) && f.src[f.pos] != ' ' && f.src[f.pos] != ')' && f.src[f.pos] != ':' {
		f.pos++
	}
	return f.src[start:f.pos]
}

func (f *fakeLogsQLFilter) fieldFilter() bool {
	f.skipSpace()
	if strings.HasPrefix(f.src[f.pos:], "{") {
		// A stream filter {label=~"re"}: the row's _stream holds the label
		// with a value the regexp matches in full.
		f.pos++
		var name string
		if strings.HasPrefix(f.src[f.pos:], `"`) {
			name = f.quoted()
		} else {
			end := f.pos + strings.IndexAny(f.src[f.pos:], "=!")
			name, f.pos = f.src[f.pos:end], end
		}
		op := ""
		for _, candidate := range []string{"=~", "!=", "="} {
			if strings.HasPrefix(f.src[f.pos:], candidate) {
				op = candidate
				break
			}
		}
		if op == "" {
			f.t.Errorf("fake VL: unsupported stream filter at %d in %q", f.pos, f.src)
			return false
		}
		f.pos += len(op)
		pattern := f.quoted()
		if !strings.HasPrefix(f.src[f.pos:], "}") {
			f.t.Errorf("fake VL: expected } at %d in %q", f.pos, f.src)
			return false
		}
		f.pos++
		// A label the stream lacks reads as the empty string.
		value := ""
		if m := fakeRegexp(`[{,]` + regexp.QuoteMeta(name) + `="((?:[^"\\]|\\.)*)"`).FindStringSubmatch(f.vals["_stream"]); m != nil {
			value, _ = strconv.Unquote(`"` + m[1] + `"`)
		}
		switch op {
		case "=":
			return value == pattern
		case "!=":
			return value != pattern
		}
		return fakeRegexp("^(?:" + pattern + ")$").MatchString(value)
	}
	name := f.quoted()
	if !strings.HasPrefix(f.src[f.pos:], ":") {
		f.t.Errorf("fake VL: expected : after %q at %d in %q", name, f.pos, f.src)
		return false
	}
	f.pos++
	if name == "_stream" && strings.HasPrefix(f.src[f.pos:], "{") {
		// A stream filter with one regexp matcher: _stream:{label=~"re"}.
		end := strings.IndexByte(f.src[f.pos:], '}')
		label, pattern, _ := strings.Cut(f.src[f.pos+1:f.pos+end], "=~")
		f.pos += end + 1
		pattern, _ = strconv.Unquote(pattern)
		for _, m := range regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_]*)="((?:[^"\\]|\\.)*)"`).FindAllStringSubmatch(f.vals["_stream"], -1) {
			if m[1] == label && regexp.MustCompile("^(?:"+pattern+")$").MatchString(m[2]) {
				return true
			}
		}
		return false
	}
	value := f.vals[name]
	switch {
	case strings.HasPrefix(f.src[f.pos:], `*"`):
		// A substring filter, field:*"text"*.
		f.pos++
		sub := f.quoted()
		if !strings.HasPrefix(f.src[f.pos:], "*") {
			f.t.Errorf("fake VL: expected * after the substring at %d in %q", f.pos, f.src)
			return false
		}
		f.pos++
		return strings.Contains(value, sub)
	case strings.HasPrefix(f.src[f.pos:], "*"):
		f.pos++
		return value != ""
	case strings.HasPrefix(f.src[f.pos:], "="):
		f.pos++
		return value == f.quoted()
	case strings.HasPrefix(f.src[f.pos:], "~"):
		f.pos++
		return fakeRegexp(f.quoted()).MatchString(value)
	case strings.HasPrefix(f.src[f.pos:], "!"):
		f.pos++
		return value != f.quoted()
	case strings.HasPrefix(f.src[f.pos:], "seq("):
		// seq("a","b"): the phrases in this order, each after the one before.
		f.pos += len("seq(")
		var phrases []string
		for {
			phrases = append(phrases, f.quoted())
			f.skipSpace()
			if f.pos < len(f.src) && f.src[f.pos] == ',' {
				f.pos++
				continue
			}
			break
		}
		if f.pos >= len(f.src) || f.src[f.pos] != ')' {
			f.t.Errorf("fake VL: expected ) closing seq at %d in %q", f.pos, f.src)
			return false
		}
		f.pos++
		return vlSequence(value, phrases)
	default:
		// A word or phrase, or with a trailing * a word prefix. VictoriaLogs'
		// tokenizer takes every Unicode letter and digit and the underscore
		// as word runes (isTokenRune).
		// The boundary is checked only on a side where the phrase starts or
		// ends with a word rune (getPhrasePos): a phrase of punctuation is a
		// substring.
		word := f.quoted()
		isWordRune := func(r rune) bool { return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r) }
		before := ""
		if first, _ := utf8.DecodeRuneInString(word); isWordRune(first) {
			before = `(^|[^\p{L}\p{Nd}_])`
		}
		if strings.HasPrefix(f.src[f.pos:], "*") {
			f.pos++
			return fakeRegexp(before + regexp.QuoteMeta(word)).MatchString(value)
		}
		return vlPhrasePos(value, word) >= 0
	}
}

var fakeRegexps sync.Map

// fakeRegexp compiles a pattern once for every evaluation of the fake.
func fakeRegexp(pattern string) *regexp.Regexp {
	if re, ok := fakeRegexps.Load(pattern); ok {
		return re.(*regexp.Regexp)
	}
	re := regexp.MustCompile(pattern)
	fakeRegexps.Store(pattern, re)
	return re
}

// fakeStatsFunc is one `fn() [if (cond)] as name` of a stats pipe.
type fakeStatsFunc struct {
	name, cond string
	sumLen     bool
}

// fakeStatsFuncs parses the functions of a stats pipe: count() and
// sum_len(_msg), each with an optional if (...) filter.
func fakeStatsFuncs(t testing.TB, statsPipe string) []fakeStatsFunc {
	rest := strings.TrimPrefix(statsPipe, "| stats ")
	if strings.HasPrefix(rest, "by (") {
		rest = rest[strings.IndexByte(rest, ')')+2:]
	}
	var funcs []fakeStatsFunc
	for rest != "" {
		var fn fakeStatsFunc
		switch {
		case strings.HasPrefix(rest, "count() "):
			rest = strings.TrimPrefix(rest, "count() ")
		case strings.HasPrefix(rest, "sum_len(_msg) "):
			fn.sumLen = true
			rest = strings.TrimPrefix(rest, "sum_len(_msg) ")
		default:
			t.Errorf("fake VL: unsupported stats function in %q", statsPipe)
			return nil
		}
		if strings.HasPrefix(rest, "if (") {
			depth, end := 0, 3
			var quote byte
		scan:
			for ; end < len(rest); end++ {
				c := rest[end]
				switch {
				case quote != 0:
					switch c {
					case '\\':
						end++
					case quote:
						quote = 0
					}
				case c == '"':
					quote = c
				case c == '(':
					depth++
				case c == ')':
					if depth--; depth == 0 {
						break scan
					}
				}
			}
			fn.cond = rest[4:end]
			rest = rest[end+2:]
		}
		rest = strings.TrimPrefix(rest, "as ")
		fn.name, rest, _ = strings.Cut(rest, ", ")
		funcs = append(funcs, fn)
	}
	return funcs
}

func fakeLogsQLMatch(t testing.TB, expr string, values map[string]string) bool {
	f := &fakeLogsQLFilter{t: t, src: expr, vals: values}
	result := f.expr()
	f.skipSpace()
	if f.pos != len(f.src) {
		t.Errorf("fake VL: trailing text at %d in %q", f.pos, expr)
	}
	return result
}

// unpackOptions parses `[from f] [fields (a, b)] [keep_original_fields] [result_prefix "p"]`.
func unpackOptions(pipe string) (fields []string, keep bool, prefix string) {
	if i := strings.Index(pipe, "fields ("); i >= 0 {
		fields = strings.Split(pipe[i+8:i+8+strings.Index(pipe[i+8:], ")")], ", ")
		for j, field := range fields {
			fields[j] = strings.Trim(field, "`")
		}
	}
	if i := strings.Index(pipe, `result_prefix "`); i >= 0 {
		prefix = pipe[i+15 : i+15+strings.Index(pipe[i+15:], `"`)]
	}
	return fields, strings.Contains(pipe, "keep_original_fields"), prefix
}

// applyPipes evaluates a query (its base filter and every pipe before the
// stats pipe) over one row with VictoriaLogs semantics and reports the row's
// values and whether it survives the filters.
func (f *pushdownFakeVL) applyPipes(t testing.TB, query string, row pushdownRow) (map[string]string, bool) {
	values := f.values(row)
	pipes := splitLogsQLPipes(query)
	if !fakeLogsQLMatch(t, pipes[0], values) {
		return values, false
	}
	inChain := false
	// A chain built with translator.SharedUnpack holds the unpack of its plan:
	// the unpack and the copies after the level are run as VictoriaLogs runs
	// them, the rest of the chain is emulated whole.
	sharedChain := chainSharedUnpack(query) != ""
	for _, pipe := range pipes[1:] {
		runPipe := true
		switch {
		case inChain:
			// The rest of translator.BuildDetectedLevelChain: its result is
			// already in detected_level.
			inChain = pipe != "delete __dl, __dl_*, __j_*, __l_*"
			runPipe = sharedChain && (!inChain || strings.HasPrefix(pipe, "unpack_json") || chainSharedCopy(pipe))
		case chainStart(pipe):
			// The start of translator.BuildDetectedLevelChain; the chain is
			// emulated whole by emulateDetectedLevelChainFor, for the fields and
			// keys it was built from.
			values["detected_level"] = emulateDetectedLevelChainFor(values, chainPresence(query))
			if chainStoredLevelDecides(values, chainPresence(query)) {
				values["__dl_h"] = "1" // the rows a stored level decides, as the chain's marker pipes flag them
			}
			inChain = true
			runPipe = sharedChain && (strings.HasPrefix(pipe, "unpack_json") || chainSharedCopy(pipe))
		}
		if !runPipe {
			continue
		}
		switch {
		case strings.HasPrefix(pipe, "unpack_json") || strings.HasPrefix(pipe, "unpack_logfmt"):
			source := "_msg"
			if _, args, found := strings.Cut(pipe, " "); found {
				if rest, ok := strings.CutPrefix(args, "from "); ok {
					source, _, _ = strings.Cut(rest, " ")
				}
			}
			var unpacked map[string]string
			switch {
			case strings.HasPrefix(pipe, "unpack_json") && f.jsonUnpack != nil:
				unpacked = f.jsonUnpack(values[source])
			case strings.HasPrefix(pipe, "unpack_json"):
				unpacked = vlUnpackJSONFields(values[source])
			default:
				unpacked = slidingLogfmtFields(values[source])
			}
			if i := strings.Index(pipe, " if ("); i >= 0 && i < strings.Index(pipe, "fields (") {
				// unpack_json if (cond) ...: rows failing the condition are not parsed.
				depth, end := 0, i+4
				for ; end < len(pipe); end++ {
					if pipe[end] == '(' {
						depth++
					} else if pipe[end] == ')' {
						if depth--; depth == 0 {
							break
						}
					}
				}
				if !fakeLogsQLMatch(t, pipe[i+5:end], values) {
					continue
				}
			}
			fields, keep, prefix := unpackOptions(pipe)
			if fields == nil {
				for field := range unpacked {
					fields = append(fields, field)
				}
			}
			for _, field := range fields {
				if keep && values[prefix+field] != "" {
					continue
				}
				if v, ok := unpacked[field]; ok {
					values[prefix+field] = v
				} else if !keep {
					values[prefix+field] = ""
				}
			}
		case strings.HasPrefix(pipe, "format \"<") && strings.HasSuffix(pipe, " keep_original_fields"):
			// format "<src>" as dst keep_original_fields: dst takes src where dst is empty.
			src, dst, _ := strings.Cut(strings.TrimSuffix(strings.TrimPrefix(pipe, "format \"<"), " keep_original_fields"), ">\" as ")
			if values[dst] == "" {
				values[dst] = values[src]
			}
		case strings.HasPrefix(pipe, "format \"") && strings.HasSuffix(pipe, " keep_original_fields"):
			value, dst, _ := strings.Cut(strings.TrimSuffix(strings.TrimPrefix(pipe, "format \""), " keep_original_fields"), "\" as ")
			if values[dst] == "" {
				values[dst] = value
			}
		case strings.HasPrefix(pipe, "coalesce("):
			// coalesce(a, b) default "v" as dst: the first non-empty field, else v.
			list, rest, _ := strings.Cut(strings.TrimPrefix(pipe, "coalesce("), ") default ")
			fallback, dst, _ := strings.Cut(rest, " as ")
			value, _ := strconv.Unquote(fallback)
			for _, field := range strings.Split(list, ", ") {
				if v := values[strings.Trim(field, "\"`")]; v != "" {
					value = v
					break
				}
			}
			values[dst] = value
		case strings.HasPrefix(pipe, "format if (") && pushdownFormatBlankRE.MatchString(pipe):
			// format if (<filter>) "" as dst: dst is empty where the filter holds.
			m := pushdownFormatBlankRE.FindStringSubmatch(pipe)
			if fakeLogsQLMatch(t, m[1], values) {
				values[m[2]] = ""
			}
		case strings.HasPrefix(pipe, "format if (") && pushdownFormatMarkRE.MatchString(pipe):
			// format if (<filter>) "1" as dst: dst is 1 where the filter holds.
			m := pushdownFormatMarkRE.FindStringSubmatch(pipe)
			if fakeLogsQLMatch(t, m[1], values) {
				values[m[2]] = "1"
			}
		case strings.HasPrefix(pipe, "format if ("):
			if lit := pushdownLiteralFormatRE.FindStringSubmatch(pipe); lit != nil {
				// format if (<filter>) "<text>" as dst: dst is the text where the filter holds.
				if fakeLogsQLMatch(t, lit[1], values) {
					values[lit[3]] = lit[2]
				}
				continue
			}
			m := pushdownFormatRE.FindStringSubmatch(pipe)
			if m == nil {
				t.Errorf("fake VL: unsupported format pipe %q", pipe)
				return nil, false
			}
			// format if (<filter>) "<src>" as dst: dst is src's value where the filter holds.
			if fakeLogsQLMatch(t, m[1], values) && values[m[2]] != "" {
				values[m[3]] = values[m[2]]
			}
		case strings.HasPrefix(pipe, "copy "):
			for _, pair := range strings.Split(strings.TrimPrefix(pipe, "copy "), ", ") {
				src, dst, _ := strings.Cut(pair, " as ")
				values[dst] = values[strings.Trim(src, "`")]
			}
		case strings.HasPrefix(pipe, "delete "):
			for _, field := range strings.Split(strings.TrimPrefix(pipe, "delete "), ", ") {
				if prefix, ok := strings.CutSuffix(field, "*"); ok {
					for name := range values {
						if strings.HasPrefix(name, prefix) {
							delete(values, name)
						}
					}
				} else {
					delete(values, strings.Trim(field, "`"))
				}
			}
		case strings.HasPrefix(pipe, "replace_regexp ("):
			m := regexp.MustCompile(`^replace_regexp \(("(?:[^"\\]|\\.)*"), ("(?:[^"\\]|\\.)*")\) at (\S+)$`).FindStringSubmatch(pipe)
			if m == nil {
				t.Errorf("fake VL: unsupported replace_regexp pipe %q", pipe)
				return nil, false
			}
			re, _ := strconv.Unquote(m[1])
			repl, _ := strconv.Unquote(m[2])
			values[m[3]] = regexp.MustCompile(re).ReplaceAllString(values[m[3]], repl)
		case strings.HasPrefix(pipe, "filter "):
			if !fakeLogsQLMatch(t, strings.TrimPrefix(pipe, "filter "), values) {
				return values, false
			}
		case strings.HasPrefix(pipe, "fields "):
			kept := map[string]string{}
			for _, field := range strings.Split(strings.TrimPrefix(pipe, "fields "), ", ") {
				field = strings.Trim(field, "`")
				for name, value := range values {
					if prefix, ok := strings.CutSuffix(field, "*"); (ok && strings.HasPrefix(name, prefix)) || name == field {
						kept[name] = value
					}
				}
			}
			values = kept
		case strings.HasPrefix(pipe, "stats "), strings.HasPrefix(pipe, "limit "):
		default:
			t.Errorf("fake VL: unsupported pipe %q", pipe)
			return nil, false
		}
	}
	return values, true
}

// guardMatches evaluates a `| limit 1` probe for one row.
func (f *pushdownFakeVL) guardMatches(t testing.TB, query string, row pushdownRow) bool {
	_, kept := f.applyPipes(t, query, row)
	return kept
}

func newPushdownFakeVL(t testing.TB, rows []pushdownRow, stored func(string) string) (*httptest.Server, *pushdownFakeVL) {
	t.Helper()
	fake := &pushdownFakeVL{rows: rows, stored: stored}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			if r.Context().Err() == nil { // a cancelled probe is not a failure
				t.Errorf("parse form: %v", err)
			}
			return
		}
		query := r.Form.Get("query")
		switch r.URL.Path {
		case "/select/logsql/stats_query":
			fake.mu.Lock()
			fake.stats = append(fake.stats, query)
			fake.mu.Unlock()
			// Reuse the existing instant stats emulator after applying this
			// fake's base selector and pre-stats pipes to the original rows.
			emulator := &vlEmulator{t: t}
			beforeStats, _, found := strings.Cut(query, " | stats ")
			if !found {
				t.Errorf("fake VL: missing instant stats pipe: %s", query)
				return
			}
			for _, row := range fake.rows {
				if values, kept := fake.applyPipes(t, beforeStats, row); kept {
					emulator.rows = append(emulator.rows, emuRow{ts: row.ts.UnixNano(), labels: values, msg: row.msg})
				}
			}
			emulator.serveStats(w, r)
		case "/select/logsql/stats_query_range":
			fake.mu.Lock()
			fake.stats = append(fake.stats, query)
			fake.mu.Unlock()
			if fake.onStats != nil {
				fake.onStats(r, query)
			}
			start, end, step, offset, ok := slidingBucketParams(t, r)
			if !ok {
				return
			}
			statsPipe := query[strings.LastIndex(query, "| stats "):]
			groupBy := fieldList(statsPipe)
			if !strings.Contains(statsPipe, "stats by (") {
				groupBy = nil
			}
			funcs := fakeStatsFuncs(t, statsPipe)
			type point struct {
				name   string
				labels string
			}
			points := map[point]map[int64]float64{}
			labelSets := map[string]map[string]string{}
			add := func(k point, ts int64, v float64) {
				if points[k] == nil {
					points[k] = map[int64]float64{}
				}
				points[k][ts] += v
			}
			for _, row := range fake.rows {
				ts := row.ts.UnixNano()
				if ts < start || ts >= end {
					continue
				}
				values, kept := fake.applyPipes(t, query, row)
				if !kept {
					continue
				}
				labels := map[string]string{}
				for _, field := range groupBy {
					labels[field] = values[field] // VictoriaLogs reports every grouped field, empty or not
				}
				key := canonicalLabelsKey(labels)
				labelSets[key] = labels
				bucket := vlTruncate(ts, step, offset)
				for _, fn := range funcs {
					if fn.cond != "" && !fakeLogsQLMatch(t, fn.cond, values) {
						add(point{fn.name, key}, bucket, 0) // every stats column is emitted for every group row, as logsql.go writes them
						continue
					}
					if fn.sumLen {
						add(point{fn.name, key}, bucket, float64(len(values["_msg"])))
					} else {
						add(point{fn.name, key}, bucket, 1)
					}
				}
			}
			keys := make([]point, 0, len(points))
			for k := range points {
				keys = append(keys, k)
			}
			sort.Slice(keys, func(i, j int) bool { return keys[i].name+keys[i].labels < keys[j].name+keys[j].labels })
			var sb strings.Builder
			sb.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
			for i, k := range keys {
				if i > 0 {
					sb.WriteByte(',')
				}
				fmt.Fprintf(&sb, `{"metric":{"__name__":%q`, k.name)
				for _, field := range groupBy {
					fmt.Fprintf(&sb, `,%q:%q`, field, labelSets[k.labels][field])
				}
				sb.WriteString(`},"values":[`)
				tss := make([]int64, 0, len(points[k]))
				for ts := range points[k] {
					tss = append(tss, ts)
				}
				sort.Slice(tss, func(i, j int) bool { return tss[i] < tss[j] })
				for j, ts := range tss {
					if j > 0 {
						sb.WriteByte(',')
					}
					fmt.Fprintf(&sb, `[%s,%q]`, strconv.FormatFloat(float64(ts)/1e9, 'f', -1, 64), strconv.FormatFloat(points[k][ts], 'f', -1, 64))
				}
				sb.WriteString(`]}`)
			}
			sb.WriteString(`]}}`)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(sb.String()))
		case "/select/logsql/query":
			start := parseFakeVLTime(t, r.Form.Get("start"))
			end := parseFakeVLTime(t, r.Form.Get("end"))
			if isLevelPresenceQuery(query) {
				fake.mu.Lock()
				fake.presenceCalls = append(fake.presenceCalls, query)
				fake.mu.Unlock()
				if fake.presenceGate != nil {
					<-fake.presenceGate
				}
				if fake.presenceFail {
					http.Error(w, "backend failure", http.StatusInternalServerError)
					return
				}
				line := "{}\n"
				if fake.presence {
					var rows []map[string]string
					for _, row := range fake.rows {
						if ts := row.ts.UnixNano(); ts < start || ts >= end {
							continue
						}
						if values, kept := fake.applyPipes(t, presenceBase(query), row); kept {
							rows = append(rows, values)
						}
					}
					if line = levelPresenceResponse(rows); fake.presenceLine != nil {
						line = fake.presenceLine(line)
					}
				}
				_, _ = w.Write([]byte(line))
				return
			}
			guard := strings.HasSuffix(query, " | limit 1")
			fake.mu.Lock()
			if guard {
				fake.guards = append(fake.guards, query)
			} else {
				fake.raw++
				fake.rawQueries = append(fake.rawQueries, fakeRawQuery{query, start, end})
			}
			fake.mu.Unlock()
			w.Header().Set("Content-Type", "application/x-ndjson")
			for _, row := range fake.rows {
				ts := row.ts.UnixNano()
				if ts < start || ts >= end {
					continue
				}
				if !fakeLogsQLMatch(t, splitLogsQLPipes(query)[0], fake.values(row)) {
					continue
				}
				if guard && !fake.guardMatches(t, query, row) {
					continue
				}
				encoded := map[string]string{"_time": row.ts.UTC().Format(time.RFC3339Nano), "_msg": row.msg}
				var streamParts []string
				for k, v := range row.stream {
					encoded[k] = v
					streamParts = append(streamParts, k+"="+strconv.Quote(v))
				}
				sort.Strings(streamParts)
				encoded["_stream"] = "{" + strings.Join(streamParts, ",") + "}"
				for k, v := range row.vl {
					encoded[k] = v
				}
				if !guard && strings.Contains(query, " | filter ") {
					// The raw rows of a split plan: what the pipes leave of
					// each selected row.
					values, kept := fake.applyPipes(t, query, row)
					if !kept {
						continue
					}
					encoded = map[string]string{"_time": encoded["_time"]}
					for k, v := range values {
						if v != "" {
							encoded[k] = v
						}
					}
				}
				line, _ := json.Marshal(encoded)
				_, _ = w.Write(append(line, '\n'))
				if guard {
					return
				}
			}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, fake
}

// lokiPushdownReference evaluates the plan with Loki semantics over the rows
// through the proxy's raw evaluator, whose parser mirrors Loki's (proven
// against Loki by the e2e parity suite): the sample at t covers (t-window, t]
// and a step without lines is absent. A line reaching the aggregation with a
// parser error fails the fixture, because Loki fails such a query.
func lokiPushdownReference(t testing.TB, plan *orderedJSONMetricPlan, rows []pushdownRow, start, end time.Time, step time.Duration) map[string]map[int64]string {
	t.Helper()
	out, err := lokiPushdownReferenceErr(plan, rows, start, end, step)
	if err != nil {
		t.Fatalf("reference: %v", err)
	}
	return out
}

// lokiPushdownReferenceErr is lokiPushdownReference returning the error Loki
// answers a line with a parser error or an unparsable pipeline with.
func lokiPushdownReferenceErr(plan *orderedJSONMetricPlan, rows []pushdownRow, start, end time.Time, step time.Duration) (map[string]map[int64]string, error) {
	out := map[string]map[int64]string{}
	for at := start; !at.After(end); at = at.Add(step) {
		values := map[string]float64{}
		for _, row := range rows {
			if !row.ts.After(at.Add(-plan.window)) || row.ts.After(at) {
				continue
			}
			base := cloneStringMap(row.stream)
			for k, v := range row.loki {
				base[k] = v
			}
			if plan.derivesLevel {
				referenceDerivedLevel(base, row)
			}
			labels, ok, err := plan.processWithStreamLabels(row.msg, base, row.stream)
			if err != nil {
				return nil, fmt.Errorf("%q: %w", row.msg, err)
			}
			if !ok {
				continue
			}
			if labels["__error__"] != "" {
				return nil, fmt.Errorf("%q reaches the aggregation with %s; Loki fails such a query", row.msg, labels["__error__"])
			}
			key := canonicalLabelsKey(plan.groupLabels(labels))
			switch plan.function {
			case "bytes_over_time", "bytes_rate":
				values[key] += float64(len(row.msg))
			default:
				values[key]++
			}
		}
		for key, v := range values {
			if plan.function == "rate" || plan.function == "bytes_rate" {
				v /= plan.window.Seconds()
			}
			if out[key] == nil {
				out[key] = map[int64]string{}
			}
			out[key][at.Unix()] = strconv.FormatFloat(v, 'f', -1, 64)
		}
	}
	return out, nil
}

// filterPushdownFixture is a JSON stream in the shape of an OTel collector
// log: service_version is structured metadata (stored as service.version),
// pipeline and level are keys of the body, one line in six is unparseable and
// one is plain text.
func filterPushdownFixture(s0 time.Time) []pushdownRow {
	var rows []pushdownRow
	for i := 0; i < 120; i++ {
		ts := s0.Add(time.Duration(i) * 5 * time.Second)
		row := pushdownRow{ts: ts, stream: map[string]string{"app": "api"}}
		switch i % 8 {
		case 0:
			row.stream["level"] = "info"
			row.msg = `{"level":"info","service_version":"0.96.0","pipeline":"logs/loki","n":` + strconv.Itoa(i) + `}`
		case 1: // the body's level collides with the stream label: level_extracted
			row.stream["level"] = "warn"
			row.msg = `{"level":"error","service_version":"0.96.0","pipeline":"logs/loki"}`
		case 2:
			row.msg = `{"level":"error","service_version":"0.95.0","pipeline":"logs/loki"}`
		case 3: // no service_version key
			row.msg = `{"level":"debug","pipeline":"traces/otlp"}`
		case 4: // unparseable, no keys before the error
			row.msg = `{"msg": "truncated ` + strconv.Itoa(i)
		case 5:
			row.msg = "plain text line " + strconv.Itoa(i)
		case 6: // structured metadata wins over the body's key (service_version_extracted)
			row.stream["level"] = "info"
			row.vl, row.loki = map[string]string{"service.version": "0.96.0"}, map[string]string{"service_version": "0.96.0"}
			row.msg = `{"pipeline":"logs/loki","service_version":"9.9.9"}`
		default:
			row.vl, row.loki = map[string]string{"service.version": "0.95.0"}, map[string]string{"service_version": "0.95.0"}
			row.msg = `{"level":"warn","pipeline":"logs/loki"}`
		}
		rows = append(rows, row)
	}
	return rows
}

// levelProbes is the number of guard probes a plan deriving detected_level
// adds to the pushdown's own, and levelChain the pipes that compute it.
const levelProbes = 3

var levelChain = translator.BuildDetectedLevelChain(translator.DetectedLevelFieldNames, translator.DetectedLevelChainGrouping, nil)

// sharedLevelChain is the chain of a plan that hands its unpack to it: the
// labels, and the condition of the plan's own unpack ("" when it has none).
func sharedLevelChain(cond string, labels ...string) string {
	return translator.BuildDetectedLevelChainShared(translator.DetectedLevelFieldNames, translator.DetectedLevelChainGrouping, nil, &translator.SharedUnpack{Keys: labels, Cond: cond})
}

func newFilterPushdownProxy(t *testing.T, backendURL string) *Proxy {
	t.Helper()
	p := newSlidingTestProxy(t, backendURL)
	p.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, nil)
	if got := p.labelTranslator.ToVL("service_version"); got != "service.version" {
		t.Fatalf("service_version is stored as %q, want service.version", got)
	}
	return p
}

// The metric shapes Grafana Explore and Logs Drilldown send for a `| json`
// stream filtered on parsed keys (Explore's logs volume with query-builder
// filters, a user's grouped and ungrouped sums without `drop __error__`, a
// Drilldown field breakdown) are answered from VictoriaLogs stats buckets
// without a raw-row scan, with the label filters pushed down as filter pipes,
// a structured-metadata label read from its stored spelling, and the values
// equal to Loki's.
// conformance: parser-error-and-label-collision, semantics/json-filter-pushdown-underscore-label, semantics/json-filter-pushdown-without-error-drop, semantics/json-filter-pushdown-ungrouped-sum, semantics/json-filter-pushdown-translated-label
func TestOrderedJSONFilterPushdownUsesStatsBuckets(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	rows := filterPushdownFixture(s0)
	start, end := s0.Add(time.Minute), s0.Add(10*time.Minute)
	const stored = " | format if (`service.version`:*) \"<service.version>\" as service_version"
	for _, tc := range []struct {
		query  string
		guards int
		stats  string
	}{
		// Explore's logs volume for `{...} | json | service_version=`0.96.0` | pipeline=`logs/loki``.
		{`sum by (level, detected_level) (count_over_time({app="api"} | json | service_version="0.96.0" | pipeline="logs/loki" | drop __error__ [1m]))`, 2 + levelProbes,
			` | filter (service_version:* or ` + "`service.version`" + `:* or _msg:"service_version" or _msg:~"\\\\u") | filter (pipeline:* or _msg:"pipeline" or _msg:~"\\\\u") ` + sharedLevelChain("(-level:*) or (-service_version:* -`service.version`:*) or (-pipeline:*)", "level", "service_version", "pipeline") + stored + ` | filter service_version:="0.96.0" | filter pipeline:="logs/loki" | stats by (level, detected_level) count() as c`},
		// A user's grouped sum: the filter excludes unparsed lines, so errors need not be dropped.
		{`sum by (level) (count_over_time({app="api"} | json | pipeline="logs/loki" [1m]))`, 2,
			` | filter (pipeline:* or _msg:"pipeline" or _msg:~"\\\\u") | unpack_json if ((-level:*) or (-pipeline:*)) fields (level, pipeline) keep_original_fields | filter pipeline:="logs/loki" | stats by (level) count() as c`},
		// The same without grouping.
		{`sum(count_over_time({app="api"} | json | pipeline="logs/loki" [1m]))`, 2,
			` | filter (pipeline:* or _msg:"pipeline" or _msg:~"\\\\u") | unpack_json if ((-pipeline:*)) fields (pipeline) keep_original_fields | filter pipeline:="logs/loki" | stats count() as c`},
		{`sum(rate({app="api"} | json | pipeline=~"logs/.*" [2m]))`, 2,
			` | filter (pipeline:* or _msg:"pipeline" or _msg:~"\\\\u") | unpack_json if ((-pipeline:*)) fields (pipeline) keep_original_fields | filter pipeline:~"logs/" | stats count() as c`},
		// A Drilldown field breakdown on a structured-metadata label.
		{`sum by (service_version) (count_over_time({app="api"} | json | drop __error__ | service_version!="" [1m]))`, 2,
			` | filter (service_version:* or ` + "`service.version`" + `:* or _msg:"service_version" or _msg:~"\\\\u") | unpack_json if ((-service_version:* -` + "`service.version`" + `:*)) fields (service_version) keep_original_fields` + stored + ` | filter -service_version:="" | stats by (service_version) count() as c`},
		// A field breakdown on a body key without dropping errors: `!=""` rejects unparsed lines.
		{`sum by (pipeline) (count_over_time({app="api"} | json | pipeline!="" [1m]))`, 2,
			` | filter (pipeline:* or _msg:"pipeline" or _msg:~"\\\\u") | unpack_json if ((-pipeline:*)) fields (pipeline) keep_original_fields | filter -pipeline:="" | stats by (pipeline) count() as c`},
		{`sum by (level, detected_level) (bytes_over_time({app="api"} | json | service_version=~"0\\.9[0-9]\\.0" | drop __error__ [1m]))`, 2 + levelProbes,
			` | filter (service_version:* or ` + "`service.version`" + `:* or _msg:"service_version" or _msg:~"\\\\u") ` + sharedLevelChain("(-level:*) or (-service_version:* -`service.version`:*)", "level", "service_version") + stored + ` | filter service_version:~"^(?-s:0\\.9[0-9]\\.0)$" | stats by (level, detected_level) sum_len(_msg) as c, count() as __sample_count`},
	} {
		t.Run(tc.query, func(t *testing.T) {
			srv, fake := newPushdownFakeVL(t, rows, nil)
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			plan, ok := compileOrderedJSONMetric(tc.query)
			if !ok || !plan.pushdown {
				t.Fatalf("expected a pushdown plan, got ok=%v plan=%+v", ok, plan)
			}
			want := lokiPushdownReference(t, plan, rows, start, end, time.Minute)
			if len(want) == 0 {
				t.Fatal("fixture yields no series")
			}
			got := runJSONVolumeQueryRange(t, p, tc.query, start, end, time.Minute)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("result differs from the Loki reference\n got: %v\nwant: %v", got, want)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			guards, stats := tc.guards, fake.stats
			if strings.Contains(tc.query, "detected_level") {
				if chain := chainStatsCalls(fake.stats); len(chain) == 0 {
					// Every row the filters keep has a stream level: the stored
					// level fields decide, in one stats query grouped by their
					// copies and with none of the chain's guard probes.
					guards -= levelProbes
					if len(fake.stats) != 1 || !strings.Contains(fake.stats[0], " | copy detected_level as __dl_d, level as __dl_l | unpack_json") || !strings.Contains(fake.stats[0], " | stats by (level, __dl_d, __dl_l) ") {
						t.Fatalf("expected one stored-level stats query: %q", fake.stats)
					}
					stats = nil
				} else {
					// Some do not: the stored-level attempt, with its label probes,
					// then the chain with the level probes; the chain takes the
					// label probes' verdict from the attempt.
					stats = chain
				}
			}
			if fake.raw != 0 || len(fake.guards) != guards || stats != nil && len(stats) != 1 {
				t.Fatalf("expected %d probes and one stats call without raw rows: raw=%d stats=%d have %d guards", guards, fake.raw, len(stats), len(fake.guards))
			}
			if stats != nil && !strings.HasSuffix(stats[0], tc.stats) {
				t.Fatalf("stats query %q does not end with %q", stats[0], tc.stats)
			}
		})
	}
}

// Each probe keeps the exact raw evaluator for a line the two parsers read
// differently, and the result still equals Loki's: a nested object Loki
// flattens to the filtered label, a key spelled with a character Loki
// sanitizes to an underscore, a partial line holding the filtered key before
// its syntax error when errors are not dropped, and a stored label on a line
// when errors are not dropped.
// conformance: parser-error-and-label-collision, semantics/json-label-spelling-probe, semantics/partial-parse-before-error, semantics/json-filter-pushdown-without-error-drop
func TestOrderedJSONFilterPushdownProbesKeepRawEvaluator(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	base := func(i int) pushdownRow {
		return pushdownRow{ts: s0.Add(time.Duration(i) * 10 * time.Second), stream: map[string]string{"app": "api", "level": "info"}, msg: `{"service_version":"0.96.0","pipeline":"logs/loki"}`}
	}
	volume := `sum by (level, detected_level) (count_over_time({app="api"} | json | service_version="0.96.0" | pipeline="logs/loki" | drop __error__ [1m]))`
	for _, tc := range []struct {
		name, query, msg string
		vl, loki         map[string]string
		guards           int
		pipelineError    bool // Loki fails the query: the line passes the filter with a parser error
	}{
		{"nested key", volume, `{"service":{"version":"0.96.0"},"pipeline":"logs/loki"}`, nil, nil, 2, false},
		{"dotted key", volume, `{"service.version":"0.96.0","pipeline":"logs/loki"}`, nil, nil, 2, false},
		{"hyphenated key", volume, `{"service-version":"0.96.0","pipeline":"logs/loki"}`, nil, nil, 2, false},
		// A non-ASCII rune Loki sanitizes is a word rune for VictoriaLogs' tokenizer: one word, serviceéversion.
		{"key joined by a non-ASCII letter", volume, `{"serviceéversion":"0.96.0","pipeline":"logs/loki"}`, nil, nil, 2, false},
		{"key joined by an invalid byte", volume, "{\"service\xffversion\":\"0.96.0\",\"pipeline\":\"logs/loki\"}", nil, nil, 2, false},
		{"nested parent joined by a non-ASCII letter", `sum by (level, detected_level) (count_over_time({app="api"} | json | k8s_pod_name="p" | pipeline="logs/loki" | drop __error__ [1m]))`, `{"k8sépod":{"name":"p"},"pipeline":"logs/loki"}`, nil, nil, 2, false},
		{"spaced nested parent", volume, `{" service ":{"version":"0.96.0"},"pipeline":"logs/loki"}`, nil, nil, 2, false},
		{"blank key in the nesting", volume, `{"service":{"":{"version":"0.96.0"}},"pipeline":"logs/loki"}`, nil, nil, 2, false},
		{"sibling nested five deep", volume, `{"service":{"a":{"b":{"c":{"d":{"e":1}}}}},"version":"0.96.0"},"pipeline":"logs/loki"}`, nil, nil, 2, false},
		{"brace inside a sibling string", volume, `{"service":{"msg":"}","version":"0.96.0"},"pipeline":"logs/loki"}`, nil, nil, 2, false},
		// Loki reads the nested keys before the syntax error and, with the
		// error dropped, counts the line; unpack_json reads nothing.
		{"nested key before syntax error", volume, `{"service":{"version":"0.96.0"},"pipeline":"logs/loki","msg": truncated`, nil, nil, 2, false},
		{"key before syntax error", `sum by (level) (count_over_time({app="api"} | json | pipeline="logs/loki" [1m]))`, `{"pipeline":"logs/loki","msg": truncated`, nil, nil, 2, true},
		{"stored filter label", `sum by (level) (count_over_time({app="api"} | json | pipeline="logs/loki" [1m]))`, `{"n":1}`, map[string]string{"pipeline": "logs/loki"}, map[string]string{"pipeline": "logs/loki"}, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := []pushdownRow{base(0), base(1), base(2)}
			risky := base(3)
			risky.stream = map[string]string{"app": "api"}
			risky.msg, risky.vl, risky.loki = tc.msg, tc.vl, tc.loki
			rows = append(rows, risky)
			srv, fake := newPushdownFakeVL(t, rows, nil)
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			plan, ok := compileOrderedJSONMetric(tc.query)
			if !ok || !plan.pushdown {
				t.Fatalf("expected a pushdown plan, got ok=%v", ok)
			}
			at := s0.Add(time.Minute)
			if tc.pipelineError {
				params := url.Values{"query": {tc.query}, "start": {strconv.FormatInt(at.UnixNano(), 10)}, "end": {strconv.FormatInt(at.UnixNano(), 10)}, "step": {"60"}}
				rec := httptest.NewRecorder()
				p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
				if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "pipeline error") {
					t.Fatalf("expected Loki's pipeline error, got %d %s", rec.Code, rec.Body)
				}
			} else {
				want := lokiPushdownReference(t, plan, rows, at, at, time.Minute)
				got := runJSONVolumeQueryRange(t, p, tc.query, at, at, time.Minute)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("result differs from the Loki reference\n got: %v\nwant: %v", got, want)
				}
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			// The probes run concurrently and the first one that finds a risky
			// line cancels the others, so a sibling probe may not reach
			// VictoriaLogs at all.
			if fake.raw != 1 || len(fake.guards) == 0 || len(fake.guards) > tc.guards+levelProbes {
				t.Fatalf("expected the probe to keep the raw evaluator: raw=%d guards=%q stats=%q", fake.raw, fake.guards, fake.stats)
			}
		})
	}
}

// An ingestion route that stores the body's keys as fields (the e2e
// generator's VictoriaLogs JSON route stores pipeline beside _msg) keeps the
// pushdown without `drop __error__`: the stored-field probe fires only for a
// stored value whose body yields none.
// conformance: parser-error-and-label-collision, semantics/json-filter-pushdown-without-error-drop
func TestOrderedJSONFilterPushdownStoredBodyKeyKeepsStats(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	var rows []pushdownRow
	for i := 0; i < 12; i++ {
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(i) * 5 * time.Second), stream: map[string]string{"app": "api", "level": "info"},
			vl: map[string]string{"pipeline": "logs/loki"}, loki: map[string]string{"pipeline": "logs/loki"},
			msg: `{"pipeline":"logs/loki","n":` + strconv.Itoa(i) + `}`})
	}
	srv, fake := newPushdownFakeVL(t, rows, nil)
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	query := `sum by (level) (count_over_time({app="api"} | json | pipeline="logs/loki" [1m]))`
	plan, _ := compileOrderedJSONMetric(query)
	at := s0.Add(time.Minute)
	want := lokiPushdownReference(t, plan, rows, at, at, time.Minute)
	got := runJSONVolumeQueryRange(t, p, query, at, at, time.Minute)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("result differs from the Loki reference\n got: %v\nwant: %v", got, want)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.raw != 0 || len(fake.stats) != 1 || len(fake.guards) != 2 {
		t.Fatalf("expected two probes and one stats call without raw rows: raw=%d stats=%q guards=%q", fake.raw, fake.stats, fake.guards)
	}
	// The two probes run concurrently, so find the stored-field one by shape.
	probe := ` | filter pipeline:* | unpack_json fields (pipeline) | filter pipeline:="" | limit 1`
	if !strings.HasSuffix(fake.guards[0], probe) && !strings.HasSuffix(fake.guards[1], probe) {
		t.Fatalf("stored-field probe %q does not end with %q", fake.guards, probe)
	}
}

// The key-spelling probe's parts: the word prefilter holds every piece of
// the label between underscores, the flat key regexp matches a key spelled
// with a sanitized rune, spaces or a blank name (and only such keys; nesting
// is left to unpack_json), and the nested splits name every dotted field a
// nesting can produce. None of the patterns needs balanced braces, so
// VictoriaLogs prepares them in microseconds instead of a third of a second.
// conformance: parser-error-and-label-collision, semantics/json-label-spelling-probe
func TestOrderedJSONKeySpellingProbeParts(t *testing.T) {
	for _, tc := range []struct {
		label, text string
		match       bool
	}{
		{"service_version", `{"service_version":"1"}`, false},
		{"service_version", `{"service-version":"1"}`, true},
		{"service_version", `{"service.version":"1"}`, true},
		{"service_version", `{"service version":"1"}`, true},
		{"service_version", `{"service/version":"1"}`, true},
		{"service_version", `{"servicé_version":"1"}`, false},
		{"service_version", `{"service":{"version":"1"}}`, false}, // nesting: verified by unpack_json
		{"service_version", `{"version":"1","service":"2"}`, false},
		{"service_version", `{"a":{"service_version":"1"}}`, false},
		{"service_version", `{"service": {"name": "api-gateway"}, "method": "GET", "level": "info", "version": "v1"}`, false},
		{"service_version", `{" service ":{"version":"1"}}`, true},    // Loki trims unicode spaces
		{"service_version", `{"service":{"":{"version":"1"}}}`, true}, // blank key skipped by Loki
		{"k8s_pod_name", `{"k8s.pod.name":"p"}`, true},
		{"k8s_pod_name", `{"k8s-pod":{"name":"p"}}`, true}, // a piece spelled with a sanitized rune
		{"k8s_pod_name", `{"k8s_pod":{"name":"p"}}`, false},
		{"k8s_pod_name", `{"k8s_pod_name":"p"}`, false},
		{"_1st", `{"1st":"a"}`, true},
		{"_1st", `{"_1st":"a"}`, false},
		{"_1st", `{"-1st":"a"}`, true},
		{"a__b", `{"a\"b":"1"}`, true},
		{"a__b", `{"a":{"_b":"1"}}`, false},
		{"a__b", `{"a_":{"b":"1"}}`, false},
	} {
		pattern := orderedJSONKeyAliasPattern([]string{tc.label})
		if pattern == "" {
			t.Fatalf("%s: no pattern", tc.label)
		}
		if strings.Contains(pattern, `\{(?:`) {
			t.Fatalf("%s: pattern nests braces: %s", tc.label, pattern)
		}
		if got := regexp.MustCompile(pattern).MatchString(tc.text); got != tc.match {
			t.Errorf("%s on %s: match=%v, want %v (pattern %s)", tc.label, tc.text, got, tc.match, pattern)
		}
	}
	if got := orderedJSONKeyAliasPattern([]string{"level", "pipeline"}); got != "" {
		t.Errorf("labels without an underscore have no other spelling, got %s", got)
	}
	for _, tc := range []struct {
		label string
		spans []string
	}{
		{"service_version", []string{"service", "service_version", "version"}},
		{"k8s_pod_name", []string{"k8s", "k8s_pod", "k8s_pod_name", "pod", "pod_name", "name"}},
		{"_1st", []string{"_1st", "1st"}},
		{"a__b", []string{"a", "a_", "a__b", "_b", "b"}},
		{"level", []string{"level"}},
	} {
		if got := orderedJSONLabelSpans(tc.label); !reflect.DeepEqual(got, tc.spans) {
			t.Errorf("spans of %s: %q, want %q", tc.label, got, tc.spans)
		}
	}
	if got := logsqlWordPrefilter([]string{"service", "or", "service"}); got != `(_msg:"service" or _msg:"or")` {
		t.Errorf("word prefilter: %s", got)
	}
	for _, tc := range []struct {
		labels []string
		want   string
	}{
		{[]string{"level", "service_version"}, `_msg:"service"*`},
		{[]string{"_1st"}, `(_msg:"1st"* or _msg:~` + strconv.Quote(`"[\s\v\x{85}\p{Z}]*[^\x00-\x7F]`) + `)`},
		{[]string{"a__b", "k8s_pod_name"}, `(_msg:"a"* or _msg:"k8s"*)`},
		{[]string{"level"}, ""},
	} {
		if got := orderedJSONSpellingPrefilter(tc.labels); got != tc.want {
			t.Errorf("spelling prefilter of %q: %s, want %s", tc.labels, got, tc.want)
		}
	}
	for _, tc := range []struct {
		label  string
		dotted []string
		loose  []string
	}{
		{"service_version", []string{"service.version"}, []string{`{"service":{"version":1}}`, `{"service": {"name": "a"}, "version": "v1"}`}},
		{"k8s_pod_name", []string{"k8s.pod_name", "k8s_pod.name", "k8s.pod.name"}, []string{`{"k8s":{"pod":{"name":"p"}}}`, `{"k8s_pod":{"name":"p"}}`}},
		{"_1st_x", []string{"_1st.x", "1st.x"}, []string{`{"1st":{"x":1}}`}},
		{"a__b", []string{"a._b", "a_.b"}, nil},
		{"level", nil, nil},
	} {
		dotted, loose := orderedJSONNestedSplits([]string{tc.label})
		if !reflect.DeepEqual(dotted, tc.dotted) {
			t.Errorf("nested names of %s: %q, want %q", tc.label, dotted, tc.dotted)
		}
		for _, text := range tc.loose {
			matched := false
			for _, pattern := range loose {
				matched = matched || regexp.MustCompile(pattern).MatchString(text)
			}
			if !matched {
				t.Errorf("no loose pattern of %s matches %s: %q", tc.label, text, loose)
			}
		}
	}
	for _, tc := range []struct {
		text  string
		match bool
	}{
		{`service_version=1 x=2`, false},
		{`service-version=1`, true},
		{`x=1 service.version=1`, true},
		{`service version=1`, false},
		{`msg="service-version=1"`, false},
	} {
		if got := regexp.MustCompile(logfmtKeyAliasPattern([]string{"service_version"})).MatchString(tc.text); got != tc.match {
			t.Errorf("logfmt %q: match=%v, want %v", tc.text, got, tc.match)
		}
	}
}

// The pushdown admits a label filter without `drop __error__` only when the
// filter rejects the empty value, and reads labels through their parsed key
// unless Loki never yields them from one.
// conformance: parser-error-and-label-collision, semantics/json-filter-pushdown-without-error-drop, semantics/json-filter-pushdown-ungrouped-sum
func TestOrderedJSONFilterPushdownEligibility(t *testing.T) {
	for _, tc := range []struct {
		query        string
		pushdown     bool
		fields       []string
		errorFilters []string
	}{
		{`sum by (level) (count_over_time({env="production"} | json | pipeline="logs/loki" [1m]))`, true, []string{"level"}, []string{"pipeline"}},
		{`sum(count_over_time({env="production"} | json | pipeline="logs/loki" [1m]))`, true, nil, []string{"pipeline"}},
		{`sum by (level) (count_over_time({env="production"} | json | pipeline!="logs/loki" [1m]))`, false, nil, nil},
		{`sum by (level) (count_over_time({env="production"} | json | pipeline!="" [1m]))`, true, []string{"level"}, []string{"pipeline"}},
		{`sum by (level) (count_over_time({env="production"} | json | pipeline=~".*" [1m]))`, false, nil, nil},
		{`sum by (level) (count_over_time({env="production"} | json | pipeline=~"logs/.*" [1m]))`, true, []string{"level"}, []string{"pipeline"}},
		{`sum by (level) (count_over_time({env="production"} | json | pipeline!~"logs/.*" [1m]))`, false, nil, nil},
		{`sum by (level) (count_over_time({env="production"} | json | pipeline!~".*" [1m]))`, true, []string{"level"}, []string{"pipeline"}},
		{`sum by (level) (count_over_time({env="production"} | json | pipeline="" [1m]))`, false, nil, nil},
		{`sum by (level) (count_over_time({env="production"} | json | pipeline="x" | drop __error__ [1m]))`, true, []string{"level"}, nil},
		{`sum by (level) (count_over_time({env="production"} | json | pipeline="x" | trace_id!="" | drop __error_details__ [1m]))`, true, []string{"level"}, []string{"pipeline", "trace_id"}},
		{`sum by (service_version, detected_level) (count_over_time({env="production"} | json | drop __error__ [1m]))`, true, []string{"service_version"}, nil},
		{`sum by (k8s_pod_name) (count_over_time({env="production"} | json | drop __error__ | k8s_pod_name!="" [1m]))`, true, []string{"k8s_pod_name"}, nil},
		{`sum(count_over_time({env="production"} | json | drop __error__ [1m]))`, false, nil, nil},
		{`sum by (__error__) (count_over_time({env="production"} | json | pipeline="x" [1m]))`, false, nil, nil},
		// `__error__=""` keeps the lines the parser accepts: the split pushdown (ordered_json_valid_split.go).
		{`sum by (level) (count_over_time({env="production"} | json | __error__="" [1m]))`, true, []string{"level"}, nil},
		// name_extracted: the key name where the stream has the label name, the key name_extracted otherwise.
		{`sum by (level_extracted) (count_over_time({env="production"} | json | drop __error__ [1m]))`, true, []string{"level_extracted"}, nil},
		{`sum by (level) (count_over_time({env="production"} | json | level_extracted!="" [1m]))`, true, []string{"level"}, []string{"level_extracted"}},
		{`sum by (k8s_pod_name_extracted) (count_over_time({env="production"} | json | drop __error__ [1m]))`, true, []string{"k8s_pod_name_extracted"}, nil},
		{`sum by (level_extracted_extracted) (count_over_time({env="production"} | json | drop __error__ [1m]))`, false, nil, nil},
		{`sum by (service_name_extracted) (count_over_time({env="production"} | json | drop __error__ [1m]))`, false, nil, nil},
		{`sum by (detected_level_extracted) (count_over_time({env="production"} | json | drop __error__ [1m]))`, false, nil, nil},
		{`sum by (level, detected_level) (count_over_time({env="production"} | logfmt | level_extracted!="" [1m]))`, true, []string{"level"}, []string{"level_extracted"}},
		{`sum by (level_extracted, detected_level) (count_over_time({env="production"} | drop __error__ [1m]))`, false, nil, nil},
		{`sum by (service_name) (count_over_time({env="production"} | json | drop __error__ [1m]))`, false, nil, nil},
		// A filter on detected_level compares the derived value after the pipes that compute it.
		{`sum by (level) (count_over_time({env="production"} | json | detected_level="error" | drop __error__ [1m]))`, true, []string{"level"}, nil},
		// The same filter before the parser holds the same value (the parser cannot set detected_level).
		{`sum by (level) (count_over_time({env="production"} | detected_level="error" | json | drop __error__ [1m]))`, true, []string{"level"}, nil},
		{`sum by (level, detected_level) (count_over_time({env="production"} | detected_level!~"info|warn" | json | drop __error__ [1m]))`, true, []string{"level"}, nil},
		{`sum by (level) (count_over_time({env="production"} | detected_level!="" | json | drop __error__ [1m]))`, true, []string{"level"}, nil},
		{`sum by (level) (count_over_time({env="production"} | detected_level="error" | json | pipeline="x" [1m]))`, true, []string{"level"}, []string{"pipeline"}},
		{`sum by (level) (count_over_time({env="production"} | detected_level="error" | json [1m]))`, false, nil, nil},
		{`sum by (a_b_c_d_e_f_g_h) (count_over_time({env="production"} | json | drop __error__ [1m]))`, false, nil, nil},
		{`sum by (level, detected_level) (count_over_time({env="production"} | logfmt | service_version="1" | drop __error__ [1m]))`, true, []string{"level"}, nil},
		{`sum by (level, detected_level) (count_over_time({env="production"} | logfmt | service_version="1" [1m]))`, true, []string{"level"}, []string{"service_version"}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			plan, ok := compileOrderedJSONMetric(tc.query)
			if !ok {
				if tc.pushdown {
					t.Fatal("expected an ordered JSON plan")
				}
				return
			}
			if plan.pushdown != tc.pushdown || !reflect.DeepEqual(plan.unpackFields, tc.fields) || !reflect.DeepEqual(plan.pushdownErrorFilters, tc.errorFilters) {
				t.Fatalf("pushdown=%v fields=%q errorFilters=%q, want %v %q %q", plan.pushdown, plan.unpackFields, plan.pushdownErrorFilters, tc.pushdown, tc.fields, tc.errorFilters)
			}
		})
	}
}

// A `| logfmt` volume filtered on an underscore label keeps the raw
// evaluator when a line spells the key with a character Loki sanitizes.
// conformance: parser-error-and-label-collision, semantics/json-label-spelling-probe
func TestLogfmtFilterPushdownAliasKeepsRawEvaluator(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	rows := []pushdownRow{
		{ts: s0.Add(10 * time.Second), stream: map[string]string{"app": "api"}, msg: "level=info service_version=1 pipeline=logs"},
		{ts: s0.Add(20 * time.Second), stream: map[string]string{"app": "api"}, msg: "level=warn service_version=2 pipeline=logs"},
	}
	query := `sum by (level, detected_level) (count_over_time({app="api"} | logfmt | pipeline="logs" | drop __error__ [1m]))`
	for _, tc := range []struct {
		name, msg string
		raw       int
	}{
		{"same spelling", "level=error pipeline=logs service_version=3", 0},
		{"sanitized key", "level=error pipe-line=logs", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := append(rows, pushdownRow{ts: s0.Add(30 * time.Second), stream: map[string]string{"app": "api"}, msg: tc.msg})
			srv, fake := newPushdownFakeVL(t, rows, nil)
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			query := strings.ReplaceAll(query, "pipeline", "pipe_line")
			plan, ok := compileOrderedJSONMetric(query)
			if !ok || !plan.pushdown {
				t.Fatal("expected a pushdown plan")
			}
			at := s0.Add(time.Minute)
			want := lokiPushdownReference(t, plan, rows, at, at, time.Minute)
			got := runJSONVolumeQueryRange(t, p, query, at, at, time.Minute)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("result differs from the Loki reference\n got: %v\nwant: %v", got, want)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			// A risky line cancels the probes still running; the stored level
			// fields alone decide the level of a window of stream levels, without
			// the level probes.
			if fake.raw != tc.raw || len(fake.guards) == 0 || len(fake.guards) > 1+levelProbes || tc.raw == 0 && len(fake.guards) != 1 && len(fake.guards) != 1+levelProbes {
				t.Fatalf("raw=%d guards=%d, want raw=%d", fake.raw, len(fake.guards), tc.raw)
			}
		})
	}
}

// The evaluator counter distinguishes stats-bucket answers from raw-row
// evaluations on /metrics, and says why the raw evaluator answered: a probe
// found a line the parsers read differently, or the pipeline is not one the
// pushdown covers.
// conformance: parser-error-and-label-collision, semantics/json-filter-pushdown-underscore-label
func TestRangeMetricEvaluatorCounter(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	rows := filterPushdownFixture(s0)
	rows = append(rows, pushdownRow{ts: s0.Add(2 * time.Minute), stream: map[string]string{"app": "api"}, msg: `{"service-version":"0.96.0","pipeline":"logs/loki"}`})
	srv, fake := newPushdownFakeVL(t, rows, nil)
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	start, end := s0.Add(time.Minute), s0.Add(10*time.Minute)
	runJSONVolumeQueryRange(t, p, `sum by (level) (count_over_time({app="api"} | json | pipeline="logs/loki" [1m]))`, start, end, time.Minute)
	runJSONVolumeQueryRange(t, p, `sum by (level) (count_over_time({app="api"} | json | pipeline="logs/loki" | drop __error__ [1m]))`, start, end, time.Minute)
	runJSONVolumeQueryRange(t, p, `sum by (service_name_extracted) (count_over_time({app="api"} | json | drop __error__ [1m]))`, start, end, time.Minute)
	runJSONVolumeQueryRange(t, p, `sum by (level) (count_over_time({app="api"} | json | service_version="0.96.0" | drop __error__ [1m]))`, start, end, time.Minute)
	rec := httptest.NewRecorder()
	p.metrics.Handler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.Bytes()
	for _, want := range []string{
		`loki_vl_proxy_parser_metric_evaluations_total{evaluator="raw_rows",reason="ineligible"} 1`,
		`loki_vl_proxy_parser_metric_evaluations_total{evaluator="raw_rows",reason="probe"} 1`,
		`loki_vl_proxy_parser_metric_evaluations_total{evaluator="vl_stats_buckets",reason="pushdown"} 2`,
	} {
		if !bytes.Contains(body, []byte(want)) {
			t.Fatalf("/metrics lacks %q in:\n%s", want, body)
		}
	}
}

// A label filter before the parser reads stream labels and structured
// metadata, which VictoriaLogs filters in the base query, so a Drilldown
// field breakdown filtered on a stream label (`{env="production",
// namespace="monitoring"} | detected_level="info" | json | drop __error__,
// __error_details__ | export_ms!="" | pipeline="logs/loki"`) is answered
// from stats buckets instead of the raw-row evaluator that took 1.6 s over
// the namespace's 24 h while VictoriaLogs answered in 4 ms.
// conformance: parser-error-and-label-collision, semantics/json-filter-pushdown-without-error-drop, semantics/label-filter-before-parser-pushdown
func TestOrderedJSONFilterPushdownLabelFilterBeforeParser(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	var rows []pushdownRow
	for i := 0; i < 24; i++ {
		row := pushdownRow{ts: s0.Add(time.Duration(i) * 5 * time.Second), stream: map[string]string{"app": "api", "namespace": "prod", "level": "info"},
			msg: `{"level":"info","export_ms":"` + strconv.Itoa(10+i%3) + `","pipeline":"logs/loki"}`}
		if i%4 == 1 {
			row.stream["namespace"] = "dev"
		}
		if i%4 == 2 {
			row.msg = "plain text " + strconv.Itoa(i)
		}
		rows = append(rows, row)
	}
	query := `sum by (export_ms) (count_over_time({app="api"} | namespace="prod" | json | drop __error__, __error_details__ | export_ms!="" | pipeline="logs/loki" [1m]))`
	srv, fake := newPushdownFakeVL(t, rows, nil)
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	plan, ok := compileOrderedJSONMetric(query)
	if !ok || !plan.pushdown {
		t.Fatalf("expected a pushdown plan, got ok=%v plan=%+v", ok, plan)
	}
	if !strings.Contains(plan.fetchQuery, `namespace="prod"`) || len(plan.stages) != 4 {
		t.Fatalf("the filter before the parser belongs to the fetch query: %q stages=%d", plan.fetchQuery, len(plan.stages))
	}
	var selected []pushdownRow
	for _, row := range rows {
		if row.stream["namespace"] == "prod" {
			selected = append(selected, row)
		}
	}
	start, end := s0.Add(time.Minute), s0.Add(2*time.Minute)
	want := lokiPushdownReference(t, plan, selected, start, end, time.Minute)
	if len(want) != 3 {
		t.Fatalf("fixture yields %d series, want 3", len(want))
	}
	got := runJSONVolumeQueryRange(t, p, query, start, end, time.Minute)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("result differs from the Loki reference\n got: %v\nwant: %v", got, want)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.raw != 0 || len(fake.stats) != 1 || !strings.Contains(fake.stats[0], `namespace:="prod"`) {
		t.Fatalf("expected one stats call filtered on the stream label without raw rows: raw=%d stats=%q", fake.raw, fake.stats)
	}
}

// detectedLevelFilterFixture is the lines of the detected_level filter tests:
// every stream has a level, and lines 1, 2 and 7 hold a `level=debug` token (or
// JSON level key) of their own in a stream whose level is info, which Loki
// counts as info, while line 5 holds a `level=info` token in a stream whose
// level is debug, which Loki counts as debug. With withChain, every fourth line has no stored level and
// the body decides (the chain).
func detectedLevelFilterFixture(s0 time.Time, withChain bool) []pushdownRow {
	var rows []pushdownRow
	for i := 0; i < 120; i++ {
		ts := s0.Add(time.Duration(i) * 5 * time.Second)
		row := pushdownRow{ts: ts, stream: map[string]string{"app": "api", "level": "info"}}
		switch i % 8 {
		case 0:
			row.msg = `{"msg":"ok","n":` + strconv.Itoa(i) + `}`
		case 1:
			row.msg = "level=debug user=bob request served"
		case 2:
			row.msg = `{"msg":"x level=debug","level":"debug"}`
		case 3:
			row.stream["level"] = "warn"
			row.msg = `{"level":"error","msg":"collision"}`
		case 4:
			row.stream["level"] = "WARNING"
			row.msg = `{"msg":"upper case spelling"}`
		case 5:
			row.stream["level"] = "debug"
			row.msg = `level=info msg="cache miss"`
		case 6:
			row.stream["level"] = "error"
			row.msg = `{"msg":"boom"}`
		default:
			row.msg = `msg="a level=debug b"`
		}
		if withChain && i%4 == 0 {
			// No stored level: the body decides (the chain).
			delete(row.stream, "level")
			switch i % 16 {
			case 0:
				row.msg = `{"level":"debug","n":` + strconv.Itoa(i) + `}`
			case 4:
				row.msg = "level=warn op=select"
			case 8:
				row.msg = `{"msg":"request failed with error"}`
			default:
				row.msg = `{"counter":` + strconv.Itoa(i) + `}`
			}
		}
		rows = append(rows, row)
	}
	return rows
}

// The Explore logs volume filtered on the level before the parser
// (`{...} | detected_level="info" | json | drop __error__`) reads the stored
// level fields, as the same filter after the parser does, instead of
// unpacking every line as logfmt: that unpack lets a `level=` token of the
// message override the stored level, which Loki's ingest-assigned
// detected_level never does. The VictoriaLogs queries (the stats and every
// probe) carry no logfmt unpack, and the answer equals the raw evaluator's
// (the Loki reference) for the same filter after the parser and a count of
// the lines whose level the filter accepts, whatever the operator, with a
// stored level and without (the chain).
// conformance: parser-error-and-label-collision, semantics/detected-level-filter-before-parser-pushdown, semantics/label-filter-before-parser-pushdown
func TestOrderedJSONDetectedLevelFilterBeforeParserReadsStoredLevel(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(time.Minute), s0.Add(10*time.Minute)
	for _, fixture := range []struct {
		name  string
		chain bool
	}{{"stored level", false}, {"no stored level", true}} {
		rows := detectedLevelFilterFixture(s0, fixture.chain)
		for _, grouping := range []string{"level, detected_level", "detected_level", "level"} {
			for _, f := range []struct{ op, value string }{{"=", "info"}, {"!=", "info"}, {"=~", "info|warn"}, {"!~", "info|warn"}, {"=~", "debu.*"}} {
				filter := "detected_level" + f.op + strconv.Quote(f.value)
				query := `sum by (` + grouping + `) (count_over_time({app="api"} | ` + filter + ` | json | drop __error__ [1m]))`
				t.Run(fixture.name+"/"+grouping+"/"+filter, func(t *testing.T) {
					plan, ok := compileOrderedJSONMetric(query)
					if !ok || !plan.pushdown || len(plan.preLevelFilters) != 1 || strings.Contains(plan.statsQuery(), "detected_level") {
						t.Fatalf("expected a pushdown plan reading the level filter itself: ok=%v plan=%+v", ok, plan)
					}
					// The Loki reference: the filter after the parser, on the
					// detected_level the raw evaluator derives for every row.
					afterParser := strings.Replace(query, `| `+filter+` | json | drop __error__`, `| json | drop __error__ | `+filter, 1)
					reference, ok := compileOrderedJSONMetric(afterParser)
					if !ok {
						t.Fatalf("reference plan for %s", afterParser)
					}
					want := lokiPushdownReference(t, reference, rows, start, end, time.Minute)
					// An independent count of the rows each window holds whose
					// level (the stream's, else the body's) the filter accepts.
					condition, err := translator.NewDropCondition("detected_level", f.op, f.value)
					if err != nil {
						t.Fatal(err)
					}
					total := 0
					for _, row := range rows {
						base := map[string]string{}
						referenceDerivedLevel(base, row)
						if row.ts.After(start.Add(-time.Minute)) && !row.ts.After(end) && condition.Matches(base["detected_level"]) {
							total++
						}
					}
					if total == 0 {
						t.Fatal("fixture: the filter keeps nothing")
					}
					srv, fake := newPushdownFakeVL(t, rows, nil)
					p := newFilterPushdownProxy(t, srv.URL)
					fake.stored = p.labelTranslator.ToVL
					got := runJSONVolumeQueryRange(t, p, query, start, end, time.Minute)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("result differs from the Loki reference\n got: %v\nwant: %v", got, want)
					}
					sum := 0
					for _, series := range got {
						for _, v := range series {
							n, _ := strconv.Atoi(v)
							sum += n
						}
					}
					if sum != total {
						t.Fatalf("the answer counts %d lines, the filter accepts %d", sum, total)
					}
					fake.mu.Lock()
					defer fake.mu.Unlock()
					if fake.raw != 0 || len(fake.stats) == 0 {
						t.Fatalf("expected stats buckets without raw rows: raw=%d stats=%d guards=%q", fake.raw, len(fake.stats), fake.guards)
					}
					for _, call := range append(append([]string(nil), fake.stats...), fake.guards...) {
						// The chain unpacks logfmt itself to read a body level; the
						// legacy rendering of the filter is the unpack followed by
						// a filter on the level it overwrote.
						if strings.Contains(call, "| unpack_logfmt | filter") || !fixture.chain && strings.Contains(call, "unpack_logfmt") {
							t.Fatalf("the level is read from the stored fields; the query unpacks logfmt: %s", call)
						}
						if base, _, _ := strings.Cut(call, " | "); base != `app:="api"` {
							t.Fatalf("the query starts from the bare selector, got %q in %s", base, call)
						}
					}
					if !fixture.chain {
						// A stored level decides: one stats query over the stored
						// fields, filtered by the stored-level pipes.
						if len(fake.stats) != 1 || !strings.Contains(fake.stats[0], "copy detected_level as __dl_d, level as __dl_l | filter (__dl_d:* OR -__dl_l:* OR ") {
							t.Fatalf("expected one stored-level stats query: %q", fake.stats)
						}
						// The parse-risk probe of the grouped level label reads the same base.
						if grouping != "detected_level" && len(fake.guards) == 0 {
							t.Fatal("expected the parse-risk probe of the level label")
						}
						return
					}
					// Lines without a stored level ask again with the chain, whose
					// filter (a row the witness flagged passes it) is on the result.
					chain := chainStatsCalls(fake.stats)
					if len(chain) != 1 || !strings.Contains(chain[0], `__lw:="1")`) {
						t.Fatalf("expected one chain stats query with the level filter: %q", fake.stats)
					}
				})
			}
		}
	}
}

// When a probe finds a line the two parsers read differently (a level-less
// stream holding truncated JSON), the raw-row evaluator answers the volume
// filtered on the level before the parser. It must give Loki's answer too: the
// rows come from the selector narrowed to those the filter can let through
// (never by a logfmt unpack that lets a `level=` token override the stored
// level), and the filter is applied to the level each row is given.
// conformance: parser-error-and-label-collision, semantics/detected-level-filter-before-parser-pushdown
func TestOrderedJSONDetectedLevelFilterBeforeParserRawEvaluator(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	rows := detectedLevelFilterFixture(s0, false)
	for i, msg := range []string{`{"level":"error","msg": truncated`, `{"level":"debug","msg": cut`} {
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(30+i*60) * time.Second), stream: map[string]string{"app": "api"}, msg: msg})
	}
	start, end := s0.Add(time.Minute), s0.Add(10*time.Minute)
	for _, filter := range []string{`detected_level="info"`, `detected_level="error"`, `detected_level!="info"`, `detected_level=~"info|debug"`} {
		query := `sum by (level, detected_level) (count_over_time({app="api"} | ` + filter + ` | json | drop __error__ [1m]))`
		t.Run(filter, func(t *testing.T) {
			reference, ok := compileOrderedJSONMetric(strings.Replace(query, `| `+filter+` | json | drop __error__`, `| json | drop __error__ | `+filter, 1))
			if !ok {
				t.Fatal("reference plan")
			}
			want := lokiPushdownReference(t, reference, rows, start, end, time.Minute)
			srv, fake := newPushdownFakeVL(t, rows, nil)
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			if got := runJSONVolumeQueryRange(t, p, query, start, end, time.Minute); !reflect.DeepEqual(got, want) {
				t.Fatalf("result differs from the Loki reference\n got: %v\nwant: %v", got, want)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if fake.raw != 1 || len(fake.rawQueries) != 1 {
				t.Fatalf("expected the raw-row evaluator after the probe: raw=%d", fake.raw)
			}
			raw := fake.rawQueries[0].query
			if strings.Contains(raw, "unpack_logfmt") || !strings.HasPrefix(raw, `app:="api" | copy detected_level as __lr_d, level as __lr_l | filter (__lr_d:* OR -__lr_l:*`) || !strings.Contains(raw, "| delete __lr_d, __lr_l") {
				t.Fatalf("the raw fetch is the selector narrowed by the stored-level filter: %s", raw)
			}
		})
	}
}

// Without `drop __error__` Loki fails a metric on a line that is not JSON, but
// only on a line the filter before the parser lets through: a level-less line
// whose body says "error" is filtered out of `detected_level="info"` before
// Loki parses it, a `level=debug` message in an info stream is not. The lookup
// of an unparsed line reads the rows the filter can let through and must skip
// one the filter rejects.
// conformance: parser-error-and-label-collision, semantics/detected-level-filter-before-parser-pushdown, semantics/pipeline-error-text-parity
func TestOrderedJSONDetectedLevelFilterBeforeParserPipelineError(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	valid := []pushdownRow{
		{ts: s0.Add(10 * time.Second), stream: map[string]string{"app": "api", "level": "info"}, msg: `{"status":"200"}`},
		{ts: s0.Add(20 * time.Second), stream: map[string]string{"app": "api", "level": "info"}, msg: `{"status":"500"}`},
	}
	query := `sum by (level) (count_over_time({app="api"} | detected_level="info" | json [1m]))`
	at := s0.Add(time.Minute)
	for _, tc := range []struct {
		name string
		line pushdownRow
		fail bool
	}{
		{"level-less error line is filtered out", pushdownRow{ts: s0.Add(30 * time.Second), stream: map[string]string{"app": "api"}, msg: "plain text error occurred"}, false},
		{"level token in an info stream is counted", pushdownRow{ts: s0.Add(30 * time.Second), stream: map[string]string{"app": "api", "level": "info"}, msg: "level=debug user=bob request served"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := append(append([]pushdownRow(nil), valid...), tc.line)
			srv, fake := newPushdownFakeVL(t, rows, nil)
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			params := url.Values{"query": {query}, "start": {strconv.FormatInt(at.UnixNano(), 10)}, "end": {strconv.FormatInt(at.UnixNano(), 10)}, "step": {"60"}}
			rec := httptest.NewRecorder()
			p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
			if tc.fail {
				if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "pipeline error: 'JSONParserErr'") || !strings.Contains(rec.Body.String(), `detected_level=\"info\"`) {
					t.Fatalf("expected Loki's pipeline error for the info line: %d %s", rec.Code, rec.Body)
				}
				return
			}
			reference, ok := compileOrderedJSONMetric(`sum by (level) (count_over_time({app="api"} | json | detected_level="info" [1m]))`)
			if !ok {
				t.Fatal("reference plan")
			}
			want := lokiPushdownReference(t, reference, rows, at, at, time.Minute)
			if got := runJSONVolumeQueryRange(t, p, query, at, at, time.Minute); !reflect.DeepEqual(got, want) {
				t.Fatalf("result differs from the Loki reference\n got: %v\nwant: %v", got, want)
			}
		})
	}
}

// A metric that keeps the stream's labels (no vector aggregation, or `sum
// without`) carries the detected_level Loki assigned at ingest on every
// series, and answers alike whether the filter on it comes before or after the
// parser.
// conformance: parser-error-and-label-collision, semantics/detected-level-filter-before-parser-pushdown
func TestOrderedJSONDetectedLevelFilterBeforeParserKeepsStreamLabels(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	rows := detectedLevelFilterFixture(s0, true)
	start, end := s0.Add(time.Minute), s0.Add(3*time.Minute)
	for _, shape := range []struct{ query, afterParser string }{
		{`count_over_time({app="api"} | detected_level="info" | json | drop __error__ [1m])`, `count_over_time({app="api"} | json | drop __error__ | detected_level="info" [1m])`},
		{`sum without (level) (count_over_time({app="api"} | detected_level!="info" | json | drop __error__ [1m]))`, `sum without (level) (count_over_time({app="api"} | json | drop __error__ | detected_level!="info" [1m]))`},
	} {
		t.Run(shape.query, func(t *testing.T) {
			srv, fake := newPushdownFakeVL(t, rows, nil)
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			// The same filter after the parser is a stage of the raw evaluator.
			want := runJSONVolumeQueryRange(t, p, shape.afterParser, start, end, time.Minute)
			if len(want) == 0 {
				t.Fatal("fixture: the filter keeps nothing")
			}
			if got := runJSONVolumeQueryRange(t, p, shape.query, start, end, time.Minute); !reflect.DeepEqual(got, want) {
				t.Fatalf("result differs from the filter after the parser\n got: %v\nwant: %v", got, want)
			}
		})
	}
}

// The two directions of the old logfmt rendering: a `level=info` token in the
// message of a stream whose level is debug was counted as info (the unpack
// overwrote the stored level), and a `level=error` token in an info stream
// dropped its line from the info count. Loki's detected_level is the stream's
// level whatever the message says, in its canonical spelling (`Information`
// is info). The counts are Loki's, from the stored level fields alone.
// conformance: parser-error-and-label-collision, semantics/detected-level-filter-before-parser-pushdown
func TestOrderedJSONDetectedLevelFilterBeforeParserIgnoresMessageLevelToken(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	stream := func(level string) map[string]string { return map[string]string{"app": "api", "level": level} }
	rows := []pushdownRow{
		{ts: s0.Add(10 * time.Second), stream: stream("info"), msg: `{"msg":"ok"}`},
		{ts: s0.Add(20 * time.Second), stream: stream("info"), msg: "level=error user=bob request served"},
		{ts: s0.Add(30 * time.Second), stream: stream("debug"), msg: `level=info msg="cache miss"`},
		{ts: s0.Add(40 * time.Second), stream: stream("debug"), msg: `{"msg":"plain debug"}`},
		{ts: s0.Add(50 * time.Second), stream: stream("Information"), msg: `{"msg":"stored spelling"}`},
	}
	at := s0.Add(time.Minute)
	for _, tc := range []struct {
		filter string
		want   map[string]string // lines the filter lets through, by detected_level
	}{
		{`detected_level="info"`, map[string]string{`{detected_level="info"}`: "3"}},
		{`detected_level="debug"`, map[string]string{`{detected_level="debug"}`: "2"}},
		{`detected_level="error"`, map[string]string{}},
	} {
		t.Run(tc.filter, func(t *testing.T) {
			srv, fake := newPushdownFakeVL(t, rows, nil)
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			query := `sum by (detected_level) (count_over_time({app="api"} | ` + tc.filter + ` | json | drop __error__ [1m]))`
			got := map[string]string{}
			for key, series := range runJSONVolumeQueryRange(t, p, query, at, at, time.Minute) {
				got[key] = series[at.Unix()]
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("answer %v, want %v", got, tc.want)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if fake.raw != 0 {
				t.Fatalf("expected stats buckets without raw rows: raw=%d", fake.raw)
			}
		})
	}
}

// Two filters on detected_level in a row before the parser both apply, from
// the stored level fields (or the level chain), as the same two filters after
// the parser do; and an instant query on the shape is answered from the stats
// buckets (one vector), not from raw rows, with Loki's answer.
// conformance: parser-error-and-label-collision, semantics/detected-level-filter-before-parser-pushdown
func TestOrderedJSONDetectedLevelFilterBeforeParserTwoFiltersAndInstant(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(time.Minute), s0.Add(10*time.Minute)
	for _, chain := range []bool{false, true} {
		rows := detectedLevelFilterFixture(s0, chain)
		for _, filters := range []string{
			`detected_level!="debug" | detected_level=~"info|warn"`,
			`detected_level=~"info|warn|error" | detected_level!~"warn"`,
		} {
			name := filters
			if chain {
				name += "/chain"
			}
			query := `sum by (level, detected_level) (count_over_time({app="api"} | ` + filters + ` | json | drop __error__ [1m]))`
			reference := `sum by (level, detected_level) (count_over_time({app="api"} | json | drop __error__ | ` + filters + ` [1m]))`
			t.Run("range/"+name, func(t *testing.T) {
				plan, ok := compileOrderedJSONMetric(query)
				if !ok || !plan.pushdown || len(plan.preLevelFilters) != 2 {
					t.Fatalf("expected a pushdown plan holding both level filters: ok=%v plan=%+v", ok, plan)
				}
				referencePlan, ok := compileOrderedJSONMetric(reference)
				if !ok {
					t.Fatalf("reference plan for %s", reference)
				}
				want := lokiPushdownReference(t, referencePlan, rows, start, end, time.Minute)
				if len(want) == 0 {
					t.Fatal("fixture: the filters keep nothing")
				}
				srv, fake := newPushdownFakeVL(t, rows, nil)
				p := newFilterPushdownProxy(t, srv.URL)
				fake.stored = p.labelTranslator.ToVL
				if got := runJSONVolumeQueryRange(t, p, query, start, end, time.Minute); !reflect.DeepEqual(got, want) {
					t.Fatalf("result differs from the Loki reference\n got: %v\nwant: %v", got, want)
				}
				fake.mu.Lock()
				defer fake.mu.Unlock()
				if fake.raw != 0 || len(fake.stats) == 0 {
					t.Fatalf("expected stats buckets without raw rows: raw=%d stats=%d", fake.raw, len(fake.stats))
				}
				for _, call := range fake.stats {
					if strings.Contains(call, "| unpack_logfmt | filter") {
						t.Fatalf("the level is read from the stored fields; the query unpacks logfmt: %s", call)
					}
				}
			})
			t.Run("instant/"+name, func(t *testing.T) {
				srv, fake := newPushdownFakeVL(t, rows, nil)
				p := newFilterPushdownProxy(t, srv.URL)
				fake.stored = p.labelTranslator.ToVL
				at := s0.Add(5 * time.Minute)
				want := rawInstantVolume(t, p, reference, at)
				if len(want.values) == 0 {
					t.Fatal("fixture: the filters keep nothing")
				}
				fake.mu.Lock()
				raw := fake.raw
				fake.mu.Unlock()
				got, rec := runJSONVolumeQueryInstant(t, p, query, at)
				if rec.Code != http.StatusOK {
					t.Fatalf("status %d: %s", rec.Code, rec.Body)
				}
				if !reflect.DeepEqual(got.values, want.values) {
					t.Fatalf("instant result differs from the Loki reference\n got: %v\nwant: %v", got.values, want.values)
				}
				fake.mu.Lock()
				defer fake.mu.Unlock()
				if fake.raw != raw || len(fake.stats) == 0 {
					t.Fatalf("expected the instant query on stats buckets without raw rows: raw=%d (before %d) stats=%d", fake.raw, raw, len(fake.stats))
				}
			})
		}
	}
}

// A server that does not scan the body for the level (-detected-level-body-scan=false) keeps answering the Drilldown field breakdown filtered on the
// level before the parser from stats buckets, with the filter rendered by the
// translator, and answers a volume grouped by the level from raw rows.
// conformance: parser-error-and-label-collision, semantics/label-filter-before-parser-pushdown
func TestOrderedJSONDetectedLevelFilterBeforeParserWithoutBodyScan(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	var rows []pushdownRow
	for i := 0; i < 24; i++ {
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(i) * 5 * time.Second), stream: map[string]string{"app": "api", "level": "info"}, msg: `{"export_ms":"` + strconv.Itoa(i%3) + `"}`})
	}
	start, end := s0.Add(time.Minute), s0.Add(2*time.Minute)
	for _, tc := range []struct {
		query    string
		rawRows  bool
		unpacked string
	}{
		{`sum by (export_ms) (count_over_time({app="api"} | detected_level="info" | json | drop __error__, __error_details__ | export_ms!="" [1m]))`, false, "| unpack_logfmt | filter level:=\"info\""},
		{`sum by (export_ms, detected_level) (count_over_time({app="api"} | detected_level="info" | json | drop __error__, __error_details__ | export_ms!="" [1m]))`, true, ""},
	} {
		t.Run(tc.query, func(t *testing.T) {
			srv, fake := newPushdownFakeVL(t, rows, nil)
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			p.detectedLevelBodyScan = false
			runJSONVolumeQueryRange(t, p, tc.query, start, end, time.Minute)
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if (fake.raw > 0) != tc.rawRows || !tc.rawRows && (len(fake.stats) != 1 || !strings.Contains(fake.stats[0], tc.unpacked)) {
				t.Fatalf("raw=%d stats=%q, want raw rows %v", fake.raw, fake.stats, tc.rawRows)
			}
		})
	}
}

// A `| json` metric that Loki fails on a line that is not a JSON object
// (no `drop __error__`, no filter an unparsed line fails) is failed the way
// Loki fails it: the exact text of logqlmodel.PipelineError with the line's
// series, and before the rows are scanned, from one `| limit 1` lookup of
// such a line. A line at the window's left edge is excluded, as Loki
// excludes it.
// conformance: parser-error-and-label-collision, status-400, semantics/pipeline-error-text-parity
func TestOrderedJSONPipelineErrorMatchesLokiText(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	rows := []pushdownRow{
		{ts: s0.Add(10 * time.Second), stream: map[string]string{"app": "api", "level": "info"}, msg: `{"status":"200"}`},
		{ts: s0.Add(20 * time.Second), stream: map[string]string{"app": "api", "level": "info"}, msg: `{"status":"500"}`},
		{ts: s0.Add(30 * time.Second), stream: map[string]string{"app": "api", "level": "warn"}, msg: `plain text`},
	}
	srv, fake := newPushdownFakeVL(t, rows, nil)
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	run := func(query string, at time.Time) (int, string) {
		params := url.Values{"query": {query}, "start": {strconv.FormatInt(at.UnixNano(), 10)}, "end": {strconv.FormatInt(at.UnixNano(), 10)}, "step": {"60"}}
		rec := httptest.NewRecorder()
		p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
		var body struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		return rec.Code, body.Error
	}
	query := `sum by (status) (count_over_time({app="api"} | json [1m]))`
	code, msg := run(query, s0.Add(time.Minute))
	// The series carries the labels Loki derives at ingest (detected_level,
	// service_name), which the proxy derives on the read path.
	want := "pipeline error: 'JSONParserErr' for series: '{__error__=\"JSONParserErr\", __error_details__=\"Value looks like object, but can't find closing '}' symbol\", app=\"api\", detected_level=\"warn\", level=\"warn\", service_name=\"api\"}'.\n" +
		"Use a label filter to intentionally skip this error. (e.g | __error__!=\"JSONParserErr\").\n" +
		"To skip all potential errors you can match empty errors.(e.g __error__=\"\")\n" +
		"The label filter can also be specified after unwrap. (e.g | unwrap latency | __error__=\"\" )\n"
	if code != http.StatusBadRequest || msg != want {
		t.Fatalf("got %d %q\nwant 400 %q", code, msg, want)
	}
	fake.mu.Lock()
	if fake.raw != 0 || len(fake.guards) != 1 || !strings.HasSuffix(fake.guards[0], ` | filter -_msg:~"^[ \\t\\n\\r]*\\{" | limit 1`) {
		t.Fatalf("expected the error from one lookup without a raw scan: raw=%d guards=%q", fake.raw, fake.guards)
	}
	fake.mu.Unlock()
	// The plain line sits exactly one window before the evaluation: outside
	// Loki's left-open window, so the query succeeds.
	if code, msg := run(query, s0.Add(90*time.Second)); code != http.StatusOK {
		t.Fatalf("a line at the window's left edge must not fail the query: %d %s", code, msg)
	}
	// An end off the step grid: Loki evaluates at start and start+step only
	// (s0-60s and s0), so the plain line at s0+30s is in no window.
	{
		params := url.Values{"query": {query}, "start": {strconv.FormatInt(s0.Add(-time.Minute).UnixNano(), 10)}, "end": {strconv.FormatInt(s0.Add(40*time.Second).UnixNano(), 10)}, "step": {"60"}}
		rec := httptest.NewRecorder()
		p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("a line after the last evaluation must not fail the query: %d %s", rec.Code, rec.Body)
		}
	}
	// A filter an unparsed line fails excludes it, so the pushdown answers
	// without the lookup.
	fake.mu.Lock()
	raw, guards := fake.raw, len(fake.guards)
	fake.mu.Unlock()
	if code, _ := run(`sum by (status) (count_over_time({app="api"} | json | status!="" [1m]))`, s0.Add(time.Minute)); code != http.StatusOK {
		t.Fatalf("a filter rejecting the empty value excludes unparsed lines: %d", code)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.raw != raw || len(fake.guards) != guards+2 {
		t.Fatalf("expected the stats pushdown and its two probes to answer the filtered query: raw=%d guards=%q", fake.raw-raw, fake.guards[guards:])
	}
}

// referenceDerivedLevel gives a fixture row the detected_level the raw
// evaluator derives from what VictoriaLogs stores for it.
func referenceDerivedLevel(base map[string]string, row pushdownRow) {
	entry := map[string]string{"_msg": row.msg}
	for k, v := range row.stream {
		entry[k] = v
	}
	for k, v := range row.vl {
		entry[k] = v
	}
	levels := logRowLevels{bodyScan: true}
	withDerivedLevel(base, levels.stringRow(entry, logRowStream{labels: row.stream, levels: levelFieldsFromLabels(row.stream)}), row.stream)
}
