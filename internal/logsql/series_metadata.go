package logsql

import (
	"slices"
	"strings"
)

// SeriesMetadata is the Loki series identity of a stored row as VictoriaLogs can write it into the row's _stream: the
// stream labels, and the row's other stored fields named as Loki names structured metadata in a series. It is built
// for a window from the names VictoriaLogs lists there (NewSeriesMetadata) and written into a query by
// WithSeriesMetadata. A field a row does not hold adds nothing to its _stream, so a SeriesMetadata built from a
// superset of the fields of the rows a query reads gives the same answer.
type SeriesMetadata struct {
	// pipes write each metadata field under its Loki name ahead of the set_stream_fields pipe.
	pipes []Pipe
	// fields are the fields set_stream_fields reads: the stream fields, then the metadata fields under their Loki
	// names, each written as a pipe names a field (QuoteFieldName).
	fields []string
	// key is the identity in one spelling, for cache keys and tests.
	key string
	// stored maps each metadata field's Loki name (as fields writes it) to its stored name, for a later drop.
	stored map[string]string
}

// Empty reports a SeriesMetadata with no metadata field: writing it changes no row's _stream.
func (s SeriesMetadata) Empty() bool { return len(s.pipes) == 0 && s.key == "" }

// Key is the identity in one spelling: the stream fields and the metadata fields with their Loki names.
func (s SeriesMetadata) Key() string { return s.key }

// seriesMetadataSkipped are the stored fields that are never written as metadata: VictoriaLogs' own fields, and
// detected_level, which the proxy derives for every row and groups by on its own.
var seriesMetadataSkipped = map[string]bool{"_msg": true, "_time": true, "_stream": true, "_stream_id": true, "detected_level": true}

// NewSeriesMetadata is the series identity of the rows of a window whose stream fields are streamFields and whose
// fields (the stream fields among them, as VictoriaLogs' field_names lists them) are fields. Every field that is not
// a stream field, not one of VictoriaLogs' own fields and not detected_level is structured metadata: Loki names it by
// sanitize (Loki sanitizes a structured metadata name when it is pushed: service.name is service_name), and a
// sanitized name equal to a stream label of the row's stream gets the suffix _extracted, as in Loki's series labels.
// The row decides the suffix: a stream that does not hold that label keeps the plain name. A field whose sanitized
// name is the name of another stored field that is not a stream field is left out (Loki holds one value under the
// name). ok is false when there is no metadata field: the identity is the stream's.
func NewSeriesMetadata(streamFields, fields []string, sanitize func(string) string) (s SeriesMetadata, ok bool) {
	stream := make(map[string]bool, len(streamFields))
	for _, f := range streamFields {
		stream[f] = true
	}
	stored := make(map[string]bool, len(fields))
	for _, f := range fields {
		stored[f] = true
	}
	var meta []string
	for _, f := range fields {
		if !stream[f] && !seriesMetadataSkipped[f] && f != "" {
			meta = append(meta, f)
		}
	}
	if len(meta) == 0 {
		return SeriesMetadata{}, false
	}
	slices.Sort(meta)
	meta = slices.Compact(meta)
	names := slices.Clone(streamFields)
	slices.Sort(names)
	names = slices.Compact(names)
	var b strings.Builder
	b.WriteString(strings.Join(names, ","))
	b.WriteString("|")
	var written []string
	for _, f := range meta {
		name := sanitize(f)
		switch {
		case stream[name]:
			// The stream label keeps the name; the metadata takes it with _extracted, row by row.
			extracted := name + "_extracted"
			if stored[extracted] && !stream[extracted] {
				continue
			}
			s.pipes = append(s.pipes,
				PipeFormat{Template: "<" + f + ">", ResultField: QuoteFieldName(extracted), If: FieldFilter{Field: QuoteFieldName(name), Op: FieldOpAny}},
				PipeFormat{Template: "<" + f + ">", ResultField: QuoteFieldName(name), If: FieldFilter{Field: QuoteFieldName(name), Op: FieldOpEmpty}})
			written = append(written, extracted)
			s.remember(extracted, f)
			b.WriteString(f + ">" + name + "?" + extracted + ",")
		case name == f:
			written = append(written, f)
			s.remember(f, f)
			b.WriteString(f + ",")
		case stored[name]:
			// Another stored field already has the name.
			continue
		default:
			s.pipes = append(s.pipes, PipeFormat{Template: "<" + f + ">", ResultField: QuoteFieldName(name)})
			written = append(written, name)
			s.remember(name, f)
			b.WriteString(f + ">" + name + ",")
		}
	}
	if len(written) == 0 {
		return SeriesMetadata{}, false
	}
	for _, n := range names {
		s.fields = append(s.fields, QuoteFieldName(n))
	}
	for _, n := range written {
		if !stream[n] {
			s.fields = append(s.fields, QuoteFieldName(n))
		}
	}
	s.pipes = append(s.pipes, PipeSetStreamFields{Fields: s.fields})
	s.key = b.String()
	return s, true
}

// remember notes that the metadata field stored as f is named name in a series.
func (s *SeriesMetadata) remember(name, f string) {
	if s.stored == nil {
		s.stored = map[string]string{}
	}
	s.stored[QuoteFieldName(name)] = f
}

// ReadsStreamIdentity reports a query whose stats read a series per stream: a stats pipe groups by _stream, or a pipe
// before the last stats pipe reads the _stream field into a series identity (set_stream_fields over _stream, a rename
// or copy of it). A log query, a query that groups by labels alone and a query without stats do not.
func ReadsStreamIdentity(q Query) bool {
	last := -1
	for i, p := range q.Pipes {
		if _, ok := p.(PipeStats); ok {
			last = i
		}
	}
	for i := 0; i <= last; i++ {
		switch p := q.Pipes[i].(type) {
		case PipeStats:
			if slices.ContainsFunc(p.By, func(k GroupKey) bool { return k.Field == "_stream" }) {
				return true
			}
		case PipeSetStreamFields:
			if slices.Contains(p.Fields, "_stream") {
				return true
			}
		case PipeRename:
			if slices.ContainsFunc(p.Pairs, func(pair [2]string) bool { return pair[0] == "_stream" }) {
				return true
			}
		case PipeCopy:
			if slices.ContainsFunc(p.Pairs, func(pair [2]string) bool { return pair[0] == "_stream" }) {
				return true
			}
		}
	}
	return false
}

// WithSeriesMetadata is q with the series identity s written into each row's _stream, ahead of the query's own
// stages: after its filters and the pipes that derive the labels Loki derives at ingest (detected_level, service_name:
// format pipes, filter pipes and their scratch fields, whose names begin with "__"), before the first pipe that can
// write a stored field or reads the stream (a parser, an extraction, a copy, a rename, a stats pipe). Every later pipe
// reads the rebuilt _stream, so each series per stream becomes a series per stream and metadata set. A query that
// already holds it is returned as it is, with false.
func WithSeriesMetadata(q Query, s SeriesMetadata) (Query, bool) {
	if s.Empty() || holdsSeriesMetadata(q) {
		return q, false
	}
	at := 0
	for at < len(q.Pipes) && derivesIngestLabels(q.Pipes[at]) {
		at++
	}
	pipes := make([]Pipe, 0, len(q.Pipes)+len(s.pipes))
	pipes = append(pipes, q.Pipes[:at]...)
	pipes = append(pipes, s.withoutDropped(q.Pipes[at:])...)
	pipes = append(pipes, q.Pipes[at:]...)
	q.Pipes = pipes
	return q, true
}

// withoutDropped is s's pipes without the metadata fields a delete pipe of rest (a drop stage) names, before its
// last stats pipe: Loki's series do not hold a dropped label.
func (s SeriesMetadata) withoutDropped(rest []Pipe) []Pipe {
	last := -1
	for i, p := range rest {
		if _, ok := p.(PipeStats); ok {
			last = i
		}
	}
	dropped := map[string]bool{}
	for _, p := range rest[:last+1] {
		if d, ok := p.(PipeDelete); ok {
			for _, l := range d.Labels {
				dropped[l] = true
			}
		}
	}
	if len(dropped) == 0 {
		return s.pipes
	}
	set := s.pipes[len(s.pipes)-1].(PipeSetStreamFields)
	kept := make([]string, 0, len(set.Fields))
	for _, f := range set.Fields {
		if stored, meta := s.stored[f]; meta && (dropped[f] || dropped[stored] || dropped[QuoteFieldName(stored)]) {
			continue
		}
		kept = append(kept, f)
	}
	if len(kept) == len(set.Fields) {
		return s.pipes
	}
	pipes := append(slices.Clone(s.pipes[:len(s.pipes)-1]), PipeSetStreamFields{Fields: kept})
	return pipes
}

// holdsSeriesMetadata reports a query an identity was written into already: a set_stream_fields pipe that does not
// read _stream (the proxy's other set_stream_fields pipes, the parser identities, keep the stream in it).
func holdsSeriesMetadata(q Query) bool {
	for _, p := range q.Pipes {
		if set, ok := p.(PipeSetStreamFields); ok && !slices.Contains(set.Fields, "_stream") {
			return true
		}
	}
	return false
}

// derivesIngestLabels reports a pipe of the leading run WithSeriesMetadata writes after: a filter, a format (the
// detected_level and service_name derivations write with it), a delete of scratch fields, and a parser or an
// extraction into scratch fields (the detected_level derivation reads the line's level with them).
func derivesIngestLabels(p Pipe) bool {
	switch p := p.(type) {
	case PipeFilter, PipeFormat:
		return true
	case PipeDelete:
		return !slices.ContainsFunc(p.Labels, func(l string) bool { return !strings.HasPrefix(l, "__") })
	case PipeUnpackJSON:
		return strings.HasPrefix(p.ResultPrefix, "__")
	case PipeUnpackLogfmt:
		return strings.HasPrefix(p.ResultPrefix, "__")
	case PipeExtractRegexp:
		return scratchCaptures(p.Pattern)
	}
	return false
}

// scratchCaptures reports a regexp whose named captures all begin with "__" (it has at least one).
func scratchCaptures(pattern string) bool {
	n := 0
	for rest := pattern; ; {
		i := strings.Index(rest, "(?P<")
		if i < 0 {
			return n > 0
		}
		rest = rest[i+len("(?P<"):]
		if !strings.HasPrefix(rest, "__") {
			return false
		}
		n++
	}
}
