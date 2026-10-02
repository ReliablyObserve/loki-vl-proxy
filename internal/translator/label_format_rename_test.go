package translator

import "testing"

// TestTranslateLabelFormatRename: `| label_format dst=src` copies src's value
// into dst when src exists and removes src (Loki LabelsFormatter.Process);
// LogsQL rename does both. A quoted value stays a template.
//
// conformance: profiles/stage-field-exposure
func TestTranslateLabelFormatRename(t *testing.T) {
	for logql, want := range map[string]string{
		`{app="a"} | label_format u=user`:               `app:="a" | rename user as u`,
		`{app="a"} | label_format x="{{.app}}", u=user`: `app:="a" | format "<app>" as x | rename user as u`,
		"{app=\"a\"} | label_format x=`{{.app}}`":       `app:="a" | format "<app>" as x`,
		`{app="a"} | logfmt | label_format code=status`: `app:="a" | unpack_logfmt | rename status as code`,
		`{app="a"} | label_format u=user | u="u1"`:      `app:="a" | rename user as u | filter u:="u1"`,
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
