package translator

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// TestTranslateLabelFormatRename: `| label_format dst=src` copies src's value
// into dst only when src exists (dst is kept otherwise) and removes src
// (Loki LabelsFormatter.Process). A quoted value stays a template.
//
// conformance: profiles/stage-field-exposure
func TestTranslateLabelFormatRename(t *testing.T) {
	for logql, want := range map[string]string{
		`{app="a"} | label_format u=user`:               `app:="a" | format if (user:*) "<user>" as u skip_empty_results | delete user`,
		`{app="a"} | label_format x="{{.app}}", u=user`: `app:="a" | format "<app>" as x | format if (user:*) "<user>" as u skip_empty_results | delete user`,
		"{app=\"a\"} | label_format x=`{{.app}}`":       `app:="a" | format "<app>" as x`,
		`{app="a"} | logfmt | label_format code=status`: `app:="a" | unpack_logfmt | format if (status:*) "<status>" as code skip_empty_results | delete status`,
		`{app="a"} | label_format u=user | u="u1"`:      `app:="a" | format if (user:*) "<user>" as u skip_empty_results | delete user | filter u:="u1"`,
	} {
		got, err := TranslateLogQL(logql)
		if err != nil {
			t.Fatalf("%s: %v", logql, err)
		}
		if got != want {
			t.Errorf("%s:\n got %s\nwant %s", logql, got, want)
		}
	}
}

// TestTranslateLogQueryKeepingLine: the stored line is copied aside once,
// before the first line_format.
//
// conformance: profiles/stage-field-exposure
func TestTranslateLogQueryKeepingLine(t *testing.T) {
	for logql, want := range map[string]string{
		`{app="a"} | json | line_format "{{.user}}" |= "u"`:           `app:="a" | unpack_json | copy _msg as _lvp_line | format "<user>" | filter ~"u"`,
		`{app="a"} | line_format "a" | line_format "{{.x}}" | logfmt`: `app:="a" | copy _msg as _lvp_line | format "a" | format "<x>" | unpack_logfmt`,
		`{app="a"} | json`: `app:="a" | unpack_json`,
	} {
		got, err := TranslateLogQueryKeepingLine(logql, nil, nil, logsql.Capabilities{})
		if err != nil || got != want {
			t.Errorf("%s:\n got %s %v\nwant %s", logql, got, err, want)
		}
	}
}
