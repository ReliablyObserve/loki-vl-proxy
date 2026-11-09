package lower

import (
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// A label function that names the tenant's stream labels (translator.WithStreamLabels) makes a metric query read
// them as Loki does. VictoriaLogs' Loki push unpacks the keys of a JSON line into fields after the stream labels,
// and a block keeps the last value written for a name, so a line whose JSON has a key named like a stream label
// stores the line's value under the label's name, while _stream keeps the label. In Loki the stream label wins.
// Before its first pipe the metric query therefore writes each name's value from _stream back into the field of
// that name, on the entries whose stream has the tag, and the others keep their field.

// restoreMemo holds the restore pipes, printed, of each distinct set of stream label names, which a tenant's
// requests all share. The memo is bounded: past maxRestoreMemo sets they are built each time.
var (
	restoreMemo      sync.Map // names (as the probe answers them) -> restoreEntry
	restoreMemoCount atomic.Int64
)

const maxRestoreMemo = 1024

type restoreEntry struct {
	text string
	ok   bool
}

// streamTagRestore is the restore pipes, printed, for the stream labels the label function carries ("" when it
// carries none), and false when a name is not a plain label name.
func streamTagRestore(label func(string) string) (text string, ok bool) {
	names, found := translate.StreamLabelList(label)
	if !found {
		return "", true
	}
	if e, hit := restoreMemo.Load(names); hit {
		return e.(restoreEntry).text, e.(restoreEntry).ok
	}
	pipes, ok := streamTagRestorePipes(strings.Split(names, ","))
	var b strings.Builder
	for i, p := range pipes {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(p.String())
	}
	text = b.String()
	if restoreMemoCount.Add(1) <= maxRestoreMemo {
		restoreMemo.Store(names, restoreEntry{text, ok})
	}
	return text, ok
}

// levelFieldNames are the stored fields that decide detected_level (translator.DetectedLevelFieldNames): the
// derivation reads them as stored, so they keep the stored field and no restore is written for them.
var levelFieldNames = []string{
	"detected_level",
	"level", "LEVEL", "Level", "log.level",
	"severity", "SEVERITY", "Severity", "SeverityText",
	"lvl", "LVL", "Lvl",
	"severity_text", "Severity_Text", "SEVERITY_TEXT",
	"severity_number",
}

// streamTagScratch names the scratch field holding a name's quoted stream value.
const streamTagScratch = "__lxst_"

// quotedBody matches the inside of a Go-quoted string: runs of plain bytes and backslash escapes. The regexp
// spells the quote \x22 and has no alternation, so the pipe holds no '"' or '|' that a quote-aware pipe scanner
// could take for a string or a pipe boundary.
const quotedBody = `[^\x22\\]*(?:\\.[^\x22\\]*)*`

// streamTagRestorePipes writes the value each name has in the entry's _stream into the field of that name.
// _stream is `{name="value",...}`, names sorted, values Go-quoted (lib/logstorage/rows.go marshalToStreamTag): the
// anchored regexp walks whole tags, so a name inside another tag's value or a longer name ending in it never
// matches; extract unquotes the captured value, and only on the entries whose stream has the tag (if): the others
// keep their field. skip_empty_results would not do: VictoriaLogs reuses an extract's results across consecutive
// entries with the same source value (lib/logstorage/pipe_extract.go writeBlock), so every entry of a run with an
// empty capture would get the first entry's original field.
//
// ok is false for a name that is not a plain label name: the string translator writes it into the pipes as it
// is, and the lowering leaves such a query to it.
func streamTagRestorePipes(names []string) (pipes []logsql.Pipe, ok bool) {
	i := 0
	for _, n := range names {
		if !plainLabelName(n) {
			return nil, false
		}
		if slices.Contains(levelFieldNames, n) {
			continue
		}
		scratch := streamTagScratch + strconv.Itoa(i)
		re := `^\{(?:[^=\x22]*=\x22` + quotedBody + `\x22,)*?` + regexp.QuoteMeta(n) + `=(?P<` + scratch + `>\x22` + quotedBody + `\x22)`
		pipes = append(pipes,
			logsql.PipeExtractRegexp{Pattern: re, From: "_stream"},
			logsql.PipeExtract{If: logsql.FieldFilter{Field: scratch, Op: logsql.FieldOpAny}, Pattern: "<" + n + ">", From: scratch},
			logsql.PipeDelete{Labels: []string{scratch}})
		i++
	}
	return pipes, true
}

// plainLabelName reports whether s is a label name made of letters, digits, underscores, dots and hyphens.
func plainLabelName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '_' || c == '.' || c == '-' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9':
		default:
			return false
		}
	}
	return true
}
