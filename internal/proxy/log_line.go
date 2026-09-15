package proxy

import (
	"bytes"
	"regexp"
	"slices"
	"strings"
	"sync"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	fj "github.com/valyala/fastjson"
)

// vlDefaultMsgValue is what VictoriaLogs stores in _msg when an ingested row
// has no message: the default of its -defaultMsgValue flag, unchanged across
// releases (app/vlinsert/insertutil/flags.go).
const vlDefaultMsgValue = "missing _msg field; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field"

// The Loki log line for a VictoriaLogs row is the stored _msg, which is the
// line Loki returns for the same push.
//
// VictoriaLogs has no line to return when it stored no message: a Loki push of
// a JSON line is unpacked into fields and _msg holds the -defaultMsgValue
// placeholder (the same happens for /insert/jsonline rows and OTLP map bodies
// without a message). For those rows only, the line is rebuilt as a flat JSON
// object of the row's fields, which is the closest to Loki's line the stored
// data allows: VictoriaLogs keeps values as strings, flattens nested objects
// into dotted keys, drops nulls and empty values, and cannot tell body keys
// from structured metadata. Keys are sorted for a stable line. A row with no
// such fields (a pushed empty line) returns "".
//
// Fields left out of a rebuilt line: VictoriaLogs internals, stream labels,
// and fields the LogQL pipeline itself writes (pipelineLineFields), because
// Loki's line never changes with the pipeline.

func (p *Proxy) defaultMsgValue() string {
	if p == nil {
		return ""
	}
	return p.backendDefaultMsgValue
}

func isVLMissingMsg(msg, defaultMsgValue string) bool {
	return msg == "" || msg == vlDefaultMsgValue || (defaultMsgValue != "" && msg == defaultMsgValue)
}

func isVLMissingMsgBytes(msg []byte, defaultMsgValue string) bool {
	return len(msg) == 0 || string(msg) == vlDefaultMsgValue || (defaultMsgValue != "" && string(msg) == defaultMsgValue)
}

// skipLogLineField reports whether a row field stays out of a rebuilt line.
// Conversions inside comparisons and map lookups do not allocate.
func skipLogLineField[T string | []byte](key T, streamLabels map[string]string, pipelineFields map[string]bool) bool {
	switch string(key) {
	case "_time", "_msg", "_stream", "_stream_id":
		return true
	}
	if _, ok := streamLabels[string(key)]; ok {
		return true
	}
	return pipelineFields[string(key)]
}

type logLineField[T string | []byte] struct {
	key   T
	value T
}

var (
	logLineStringFieldsPool = sync.Pool{New: func() interface{} { return new([]logLineField[string]) }}
	logLineBytesFieldsPool  = sync.Pool{New: func() interface{} { return new([]logLineField[[]byte]) }}
	logLineBufPool          = sync.Pool{New: func() interface{} { return new([]byte) }}
)

// storedLogLineFromEntry returns the Loki line for a decoded VictoriaLogs row.
func storedLogLineFromEntry(msg string, entry map[string]interface{}, streamLabels map[string]string, pipelineFields map[string]bool, defaultMsgValue string) string {
	if !isVLMissingMsg(msg, defaultMsgValue) {
		return msg
	}
	fieldsPtr := logLineStringFieldsPool.Get().(*[]logLineField[string])
	fields := (*fieldsPtr)[:0]
	for key, value := range entry {
		if skipLogLineField(key, streamLabels, pipelineFields) {
			continue
		}
		if s, ok := stringifyEntryValue(value); ok && s != "" {
			fields = append(fields, logLineField[string]{key: key, value: s})
		}
	}
	slices.SortFunc(fields, func(a, b logLineField[string]) int { return strings.Compare(a.key, b.key) })
	line := encodeLogLine(fields)
	clear(fields)
	*fieldsPtr = fields[:0]
	logLineStringFieldsPool.Put(fieldsPtr)
	return line
}

// storedLogLineFromFJ returns the Loki line for a fastjson VictoriaLogs row.
// Keys and values are filtered and encoded from the parser's bytes.
func storedLogLineFromFJ(row *fj.Value, streamLabels map[string]string, pipelineFields map[string]bool, defaultMsgValue string) string {
	msg := row.GetStringBytes("_msg")
	if !isVLMissingMsgBytes(msg, defaultMsgValue) {
		return string(msg)
	}
	obj, err := row.Object()
	if err != nil {
		return ""
	}
	fieldsPtr := logLineBytesFieldsPool.Get().(*[]logLineField[[]byte])
	fields := (*fieldsPtr)[:0]
	obj.Visit(func(key []byte, v *fj.Value) {
		if skipLogLineField(key, streamLabels, pipelineFields) {
			return
		}
		var value []byte
		switch v.Type() {
		case fj.TypeString:
			value = v.GetStringBytes()
		case fj.TypeNull:
			return
		default:
			// VictoriaLogs returns every field as a string; keep other JSON
			// values as their JSON text.
			value = v.MarshalTo(nil)
		}
		if len(value) > 0 {
			fields = append(fields, logLineField[[]byte]{key: key, value: value})
		}
	})
	slices.SortFunc(fields, func(a, b logLineField[[]byte]) int { return bytes.Compare(a.key, b.key) })
	line := encodeLogLine(fields)
	clear(fields)
	*fieldsPtr = fields[:0]
	logLineBytesFieldsPool.Put(fieldsPtr)
	return line
}

func encodeLogLine[T string | []byte](fields []logLineField[T]) string {
	if len(fields) == 0 {
		return ""
	}
	bufPtr := logLineBufPool.Get().(*[]byte)
	buf := append((*bufPtr)[:0], '{')
	for i, field := range fields {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = appendLogLineJSONString(buf, field.key)
		buf = append(buf, ':')
		buf = appendLogLineJSONString(buf, field.value)
	}
	buf = append(buf, '}')
	line := string(buf)
	*bufPtr = buf[:0]
	logLineBufPool.Put(bufPtr)
	return line
}

// appendLogLineJSONString appends s as a JSON string without HTML escaping, as
// log shippers write JSON lines ("<" stays "<").
func appendLogLineJSONString[T string | []byte](buf []byte, s T) []byte {
	const hex = "0123456789abcdef"
	buf = append(buf, '"')
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 0x20 && c != '"' && c != '\\' {
			continue
		}
		buf = append(buf, s[start:i]...)
		switch c {
		case '"', '\\':
			buf = append(buf, '\\', c)
		case '\n':
			buf = append(buf, '\\', 'n')
		case '\r':
			buf = append(buf, '\\', 'r')
		case '\t':
			buf = append(buf, '\\', 't')
		default:
			buf = append(buf, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xf])
		}
		start = i + 1
	}
	buf = append(buf, s[start:]...)
	return append(buf, '"')
}

var patternCaptureRE = regexp.MustCompile(`<([A-Za-z_][A-Za-z0-9_]*)>`)

// logQueryLineFields returns the fields a LogQL query's pipeline writes; see
// pipelineLineFields.
func logQueryLineFields(query string) map[string]bool {
	lq, err := logqlpkg.ParseLogQuery(query)
	if err != nil {
		return nil
	}
	return pipelineLineFields(lq.Pipeline)
}

// pipelineLineFields returns the fields VictoriaLogs writes for pipeline stages
// that run before the line is rebuilt: regexp and pattern captures, explicit
// json/logfmt extractions, and label_format targets. Rebuilt lines leave them
// out so the pipeline does not change the line.
func pipelineLineFields(pipeline []logqlpkg.Stage) map[string]bool {
	names := pipelineRegexpCaptureFields(pipeline)
	add := func(name string) {
		if name == "" || name == "_" {
			return
		}
		if names == nil {
			names = make(map[string]bool)
		}
		names[name] = true
	}
	for _, stage := range pipeline {
		switch s := stage.(type) {
		case *logqlpkg.ParserStage:
			if s.Type == logqlpkg.ParserPattern {
				for _, match := range patternCaptureRE.FindAllStringSubmatch(s.Param, -1) {
					add(match[1])
				}
			}
			for _, field := range s.Fields {
				add(field.Name)
			}
		case *logqlpkg.LabelFormatStage:
			for _, format := range s.Formats {
				add(format.Name)
			}
		}
	}
	return names
}
