package translate

import (
	"hash/maphash"
	"regexp/syntax"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// The builders of the regexp and pattern parsers and of decolorize in a log query: the pipes the string
// translator writes for them, and the label names it reads from their text. The string translator keeps the
// text of the stage's argument (a double-quoted one as written, a backtick one quoted); the typed stage holds
// the argument unquoted, so the builders quote it the way the translator quotes a backtick argument, which is
// also its text for a double-quoted argument written with the escapes strconv.Quote writes.

// ExtractRegexp is the pipe of a `| regexp` stage: `| extract_regexp "<regexp>"`, the regexp Go-quoted.
func ExtractRegexp(pattern string) logsql.Pipe {
	return logsql.PipeExtractRegexp{Pattern: pattern, Quoted: true}
}

// ExtractPattern is the pipe of a `| pattern` stage: `| extract "<pattern>"`. ok is false for a wildcard-only
// pattern (NoopPattern), which writes no pipe.
func ExtractPattern(pattern string) (logsql.Pipe, bool) {
	if NoopPattern(pattern) {
		return nil, false
	}
	return logsql.PipeExtract{Pattern: pattern}, true
}

// Decolorize is the pipe of a `| decolorize` stage.
func Decolorize() logsql.Pipe { return logsql.PipeDecolorize{} }

// NoopPattern reports whether a pattern is a wildcard-only expression. VictoriaLogs' extract needs named
// placeholders and rejects these, which Drilldown flows emit, so the stage is a no-op.
func NoopPattern(pattern string) bool {
	switch strings.TrimSpace(pattern) {
	case "(.*)", ".*", "^.*$", "(?s).*", "(?s:.*)":
		return true
	}
	return false
}

// RegexpCaptureNames are the names of the named groups of a regexp, the labels a `| regexp` stage defines. The
// string translator reads them with regexp.Compile, so a regexp Go does not compile defines none. Compile parses
// the regexp with syntax.Perl and cannot fail after that, so parsing alone gives the same names and the same
// verdict, without building the program (about half of the allocations).
func RegexpCaptureNames(pattern string) []string {
	re, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return nil
	}
	var names []string
	for _, n := range re.CapNames() {
		if n != "" {
			names = append(names, n)
		}
	}
	return names
}

// SharedRegexpCaptureNames is RegexpCaptureNames for a reader that only reads the names: the names of a regexp met
// before are the ones read then (regexpCaptures), so a regexp that queries repeat is parsed once. A regexp is kept
// from the second time it is met (regexpSeen), so one that is never sent again costs what RegexpCaptureNames costs.
// The list is shared: it is never changed, and it is clipped, so an append copies it.
func SharedRegexpCaptureNames(pattern string) []string {
	if len(pattern) > regexpCaptureMaxBytes {
		return RegexpCaptureNames(pattern)
	}
	if v, ok := regexpCaptures.Load(pattern); ok {
		return v.([]string)
	}
	if regexpCaptureCount.Load() >= regexpCapturesMax || !seenBefore(pattern) {
		return RegexpCaptureNames(pattern)
	}
	// The pattern is part of the query it came from: the cache keeps a copy, not the whole query, and the names are
	// read from the copy (they are parts of the text parsed).
	key := strings.Clone(pattern)
	names := slices.Clip(RegexpCaptureNames(key))
	if _, loaded := regexpCaptures.LoadOrStore(key, names); !loaded {
		regexpCaptureCount.Add(1)
	}
	return names
}

// regexpCaptures holds the capture names of the regexps SharedRegexpCaptureNames read, while it holds fewer than
// regexpCapturesMax of at most regexpCaptureMaxBytes each. Like the proxy's validation cache, it stops growing when
// it is full.
var (
	regexpCaptures     sync.Map // pattern -> []string
	regexpCaptureCount atomic.Int32
)

const (
	regexpCapturesMax     = 1024
	regexpCaptureMaxBytes = 1024
)

// regexpSeen holds a hash of each regexp met lately, one per slot: a regexp whose hash is in its slot was met before.
// A collision only keeps a regexp one sighting early or late.
var (
	regexpSeen     [4096]atomic.Uint64
	regexpSeenSeed = maphash.MakeSeed()
)

// seenBefore reports whether the regexp was met before, and notes it.
func seenBefore(pattern string) bool {
	h := maphash.String(regexpSeenSeed, pattern)
	slot := &regexpSeen[h%uint64(len(regexpSeen))]
	if slot.Load() == h {
		return true
	}
	slot.Store(h)
	return false
}

// PatternCaptureNames are the names the placeholders of a `| pattern` text define, in the order written and
// with repeats: `<name>` with a name that starts with a letter (the string translator's reading; `<_>` and a
// name that starts with an underscore define none). It reads the text as the regexp <([A-Za-z][A-Za-z0-9_]*)>
// finds its matches, leftmost first and apart, and the names are parts of the text.
func PatternCaptureNames(text string) []string {
	var out []string
	for i := 0; i < len(text); i++ {
		if text[i] != '<' {
			continue
		}
		end, ok := patternPlaceholderEnd(text, i+1)
		if !ok {
			continue
		}
		if out == nil {
			out = make([]string, 0, placeholdersAhead(text[i:]))
		}
		out = append(out, text[i+1:end])
		i = end
	}
	return out
}

// patternPlaceholderEnd is the index of the > that closes a placeholder name starting at text[j] (a letter, then
// letters, digits and underscores), and false where none does.
func patternPlaceholderEnd(text string, j int) (int, bool) {
	if j >= len(text) || !isASCIILetter(text[j]) {
		return 0, false
	}
	for j++; j < len(text); j++ {
		switch c := text[j]; {
		case c == '>':
			return j, true
		case isASCIILetter(c) || c >= '0' && c <= '9' || c == '_':
		default:
			return 0, false
		}
	}
	return 0, false
}

// placeholdersAhead is the number of < in text: room for the names that can follow.
func placeholdersAhead(text string) int {
	n := 0
	for i := 0; i < len(text); i++ {
		if text[i] == '<' {
			n++
		}
	}
	return n
}

func isASCIILetter(c byte) bool { return c|0x20 >= 'a' && c|0x20 <= 'z' }
