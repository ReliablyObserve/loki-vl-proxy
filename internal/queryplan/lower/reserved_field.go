package lower

import (
	"regexp"
	"sync"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// A Loki label may be called _time, _stream, _stream_id or _msg, the fields VictoriaLogs keeps for itself
// (logsql.ReservedFieldName). VictoriaLogs keeps such a label only as a tag of the stream: a stream label called
// _time is in _stream (`{_time="a",app="x"}`) and nowhere else, and an entry's field called so is dropped at ingest
// (_time, _stream, _stream_id) or is the line (_msg). A filter that names the field is VictoriaLogs' own time,
// stream, stream id or line filter, which rejects the value or reads it as something else (`_time:>=5s` is a time
// range). So the lowering filters the tag, as VictoriaLogs models it:
//
//   - a stream selector matcher, and a label filter =, ==, !=, =~ or !~ on a string, is the stream filter
//     {name op "value"} (translate.ReservedFieldStreamFilter); a label filter's regexp is Loki's whole-value
//     reading of it (translate.LokiLabelFilterPattern) with . not matching a line break, as Loki reads a label
//     filter's regexp;
//   - a comparison (a number, a duration, a bytes size, ip()) reads the tag's value out of _stream into a scratch
//     field ahead of the stage (reservedTagPipes), compares that field, and deletes it at the end; a stage that is
//     that one comparison also keeps only the streams that have the tag ({name=~".+"}), which is what Loki keeps
//     (a missing label fails every comparison) and spares VictoriaLogs the other streams;
//   - a label filter after a parser, a drop, a keep or a label_format is left to the string translator (those
//     stages can define, rename or remove the label, and VictoriaLogs' unpack_json and unpack_logfmt write a key
//     called _time over the entry's own _time).
//
// _time, _stream and _stream_id in another case (_TIME, _Stream, _STREAM_ID: logsql.ReservedFieldNameOtherCase)
// are fields to VictoriaLogs, but no field filter can name them: bare they are its own filters, and quoted they do
// not survive VictoriaLogs' clone of the query (it prints them bare and the second parse panics). A stream selector
// matcher on one is the stream filter on its tag, as above (a selector names stream labels only); a label filter
// copies the field into a scratch field ahead of the stage (`| copy _TIME as __lxrf_TIME`), filters the scratch
// field, and deletes it at the end. The internal/queryplan/lower roundtrip grid (TestPlanTextsSurviveVictoriaLogsClone)
// holds every text the lowering writes for these names to VictoriaLogs' clone.

// reservedAfterStageConstruct names a label filter on a reserved field name after a stage that can change labels.
const reservedAfterStageConstruct = "LogSelect label filter reserved field after a stage"

// reservedScratch prefixes the scratch field a comparison on a reserved field reads its tag's value into.
const reservedScratch = "__lxrf"

// reservedLeafField is the field a label filter leaf names after the label function, and whether it is reserved in
// lower case or in another case.
func reservedLeafField(e *logql.LabelFilterExpr, labelFn func(string) string) (string, bool) {
	field := translate.SanitizeFieldIdentifier(e.Name)
	if labelFn != nil {
		field = translate.SanitizeFieldIdentifier(labelFn(field))
	}
	return field, logsql.ReservedFieldName(field) || logsql.ReservedFieldNameOtherCase(field)
}

// reservedLeaf is the filter of a label filter leaf on the reserved field: the stream filter on its tag, or, for
// a comparison, the comparison of the scratch field the tag is read into (noted in lb.reserved for its pipes).
func (lb *leafBuilder) reservedLeaf(field string, e *logql.LabelFilterExpr) logsql.FilterExpr {
	if e.Kind == logql.LabelFilterValueString && logsql.ReservedFieldName(field) {
		switch e.Cmp {
		case "=", "==":
			return translate.ReservedFieldStreamFilter(field, "=", e.Value)
		case "!=":
			return translate.ReservedFieldStreamFilter(field, "!=", e.Value)
		case "=~", "!~":
			return translate.ReservedFieldStreamFilter(field, e.Cmp, "(?-s:"+translate.LokiLabelFilterPattern(e.Value)+")")
		}
	}
	tag := reservedTagOf(field)
	lb.noteReserved(field)
	f, _ := translate.LabelFilterLeaf(tag.scratch, e.Cmp, e.Value, e.Kind == logql.LabelFilterValueIP, lb.caps) // the operator is known
	return f
}

// noteReserved records that a comparison reads field's tag: once per query, and its pipes go ahead of the stage.
func (lb *leafBuilder) noteReserved(field string) {
	for _, f := range lb.reserved {
		if f == field {
			return
		}
	}
	lb.reserved = append(lb.reserved, field)
	lb.stageReserved = append(lb.stageReserved, field)
}

// reservedTag is what a comparison on a reserved field reads: the scratch field the tag's value goes into, the pipes
// that put it there (reservedTagPipes), the delete of that field alone, and the stream filter of the streams that
// have the tag. They are built once per name.
type reservedTag struct {
	scratch string
	pipes   []logsql.Pipe
	delete  logsql.Pipe
	guard   logsql.FilterExpr
}

var reservedTags = func() map[string]reservedTag {
	tags := map[string]reservedTag{}
	for _, f := range []string{"_time", "_stream", "_stream_id", "_msg"} {
		scratch := reservedScratch + f
		tags[f] = reservedTag{
			scratch: scratch,
			pipes:   reservedTagPipes(f, scratch),
			delete:  logsql.PipeDelete{Labels: []string{scratch}},
			guard:   translate.ReservedFieldStreamFilter(f, "=~", ".+"),
		}
	}
	return tags
}()

// reservedTagPipes reads the value field has as a tag of the entry's stream into scratch, as streamTagRestorePipes
// restores a stream label: _stream is `{name="value",...}`, names sorted, values Go-quoted, and the anchored regexp
// walks whole tags, so a name inside another tag's value or a longer name ending in it never matches; the quoted
// value goes into a second scratch field, which extract unquotes into scratch on the entries whose stream has the
// tag (an entry without one gets no field), and is then deleted.
func reservedTagPipes(field, scratch string) []logsql.Pipe {
	quoted := reservedScratch + "q" + field
	re := `^\{(?:[^=\x22]*=\x22` + quotedBody + `\x22,)*?` + regexp.QuoteMeta(field) + `=(?P<` + quoted + `>\x22` + quotedBody + `\x22)`
	return []logsql.Pipe{
		logsql.PipeExtractRegexp{Pattern: re, From: "_stream"},
		logsql.PipeExtract{If: logsql.FieldFilter{Field: quoted, Op: logsql.FieldOpAny}, Pattern: "<" + scratch + ">", From: quoted},
		logsql.PipeDelete{Labels: []string{quoted}},
	}
}

// reservedTagOf is the reservedTag of a reserved field: built once for the four lower-case names; for a name in
// another case, the copy of the field into its scratch field, its delete and no guard (the field can be a stream
// label or structured metadata, so no stream filter selects the entries that have it).
func reservedTagOf(field string) reservedTag {
	if tag, ok := reservedTags[field]; ok {
		return tag
	}
	if tag, ok := otherCaseTags.Load(field); ok {
		return tag.(reservedTag)
	}
	scratch := reservedScratch + field
	tag := reservedTag{
		scratch: scratch,
		pipes:   []logsql.Pipe{logsql.PipeCopy{Pairs: [][2]string{{field, scratch}}}},
		delete:  logsql.PipeDelete{Labels: []string{scratch}},
	}
	otherCaseTags.Store(field, tag)
	return tag
}

// otherCaseTags are the reservedTags of the names in another case, built on first use. They are bounded by
// construction, whatever the client sends: a name is stored only when logsql.ReservedFieldNameOtherCase accepts
// it, and _time, _stream and _stream_id have 15, 63 and 255 spellings in another case.
var otherCaseTags sync.Map

// reservedStagePipes are the pipes ahead of a stage that first reads the tags of fields.
func reservedStagePipes(pre []logsql.Pipe, fields []string) []logsql.Pipe {
	for _, f := range fields {
		pre = append(pre, reservedTagOf(f).pipes...)
	}
	return pre
}

// reservedDelete deletes the scratch fields of the reserved fields a query compared.
func reservedDelete(fields []string) logsql.Pipe {
	if len(fields) == 1 {
		return reservedTagOf(fields[0]).delete
	}
	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = reservedTagOf(f).scratch
	}
	return logsql.PipeDelete{Labels: names}
}

// reservedGuard is the stream filter that keeps the streams with a tag field, for a stage that is one comparison
// on it: nil for any other stage.
func reservedGuard(e *logql.LabelFilterExpr, labelFn func(string) string) logsql.FilterExpr {
	if !e.IsLeaf() || e.Kind == logql.LabelFilterValueString {
		return nil
	}
	field, ok := reservedLeafField(e, labelFn)
	if !ok {
		return nil
	}
	return reservedTagOf(field).guard
}

// changesLabels reports a stage after which a label can be defined, renamed or gone: a parser, a drop, a keep or a
// label_format.
func changesLabels(st queryplan.Stage) bool {
	switch st.(type) {
	case *queryplan.Parse, *queryplan.Drop, *queryplan.Keep, *queryplan.LabelFormat:
		return true
	}
	return false
}

// ReadsReservedField reports a plan whose lowering filters a label called after a field VictoriaLogs keeps for
// itself (logsql.ReservedFieldName) as the tag of the stream it is: a stream selector matcher, or a label filter
// with only filters before it, that names one under prof's label function. The string translator writes these as
// field filters, which VictoriaLogs rejects (`_time:="a"`, `_stream:="a"`) or reads as its own filter (`_time:>=5s`,
// `_msg:="a"`), so the shadows do not hold the lowering to its text there
// (semantics/translator-writes-reserved-field-names).
func ReadsReservedField(p *queryplan.Plan, prof Profile) bool {
	return readsReservedField(p.Root, prof)
}

func readsReservedField(n queryplan.Node, prof Profile) bool {
	switch n := n.(type) {
	case *queryplan.LogSelect:
		return selectReadsReservedField(n, prof)
	case *queryplan.RangeAgg:
		return n.In != nil && selectReadsReservedField(n.In, prof)
	case *queryplan.VectorAgg:
		return readsReservedField(n.In, prof)
	case *queryplan.Rank:
		return readsReservedField(n.In, prof)
	case *queryplan.LabelReplace:
		return readsReservedField(n.In, prof)
	case *queryplan.BinOp:
		return readsReservedField(n.L, prof) || readsReservedField(n.R, prof)
	}
	return false
}

func selectReadsReservedField(sel *queryplan.LogSelect, prof Profile) bool {
	for _, m := range sel.Matchers {
		label := translate.SanitizeFieldIdentifier(m.Name)
		if prof.Label != nil {
			label = translate.SanitizeFieldIdentifier(prof.Label(label))
		}
		if logsql.ReservedFieldName(label) || logsql.ReservedFieldNameOtherCase(label) {
			return true
		}
	}
	for _, st := range sel.Stages {
		if changesLabels(st) {
			return false
		}
		if lf, ok := st.(*queryplan.LabelFilter); ok && lf.Expr != nil && exprReadsReservedField(lf.Expr, prof.Label) {
			return true
		}
	}
	return false
}

func exprReadsReservedField(e *logql.LabelFilterExpr, labelFn func(string) string) bool {
	if !e.IsLeaf() {
		return exprReadsReservedField(e.Left, labelFn) || exprReadsReservedField(e.Right, labelFn)
	}
	_, ok := reservedLeafField(e, labelFn)
	return ok
}
