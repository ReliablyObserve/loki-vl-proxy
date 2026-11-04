package translator

import (
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// marshalStreamForTest renders _stream as VictoriaLogs does
// (lib/logstorage/stream_tags.go marshalString, rows.go marshalToStreamTag):
// tags sorted by name, `name="value"` with Go quoting, comma separated.
func marshalStreamForTest(tags map[string]string) string {
	names := make([]string, 0, len(tags))
	for n := range tags {
		names = append(names, n)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, n := range names {
		parts = append(parts, n+"="+strconv.Quote(tags[n]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

var restorePipeRE = regexp.MustCompile("^extract_regexp `(.*)` from _stream$")

// applyRestoreForTest runs the pipes streamTagRestorePipes writes for name on
// one entry: Go's regexp is VictoriaLogs' (RE2), the extract pattern `<name>`
// unquotes a quoted value (lib/logstorage/pattern.go tryUnquoteString), and
// the extract runs only when something was captured (if), so the entry keeps
// its field otherwise.
func applyRestoreForTest(t testing.TB, name, stream, field string) string {
	t.Helper()
	pipes := strings.Split(strings.TrimPrefix(streamTagRestorePipes([]string{name}), "| "), " | ")
	if len(pipes) != 3 {
		t.Fatalf("restore of %q: %d pipes: %q", name, len(pipes), pipes)
	}
	m := restorePipeRE.FindStringSubmatch(pipes[0])
	if m == nil {
		t.Fatalf("restore of %q: unexpected first pipe %q", name, pipes[0])
	}
	if want := `extract if (__lxst_0:*) "<` + name + `>" from __lxst_0`; pipes[1] != want || pipes[2] != "delete __lxst_0" {
		t.Fatalf("restore of %q: pipes %q", name, pipes)
	}
	sub := regexp.MustCompile(m[1]).FindStringSubmatch(stream)
	if sub == nil {
		return field
	}
	v, err := strconv.Unquote(sub[1])
	if err != nil {
		t.Fatalf("captured %q from %s is not a quoted string: %v", sub[1], stream, err)
	}
	if v == "" {
		return field
	}
	return v
}

// Edge cases of the stream value restore: the label shadowed by the line's key,
// first and last tag, a name inside another tag's value, a longer name ending
// in it, quotes, backslashes and commas in values, no such tag (the stored
// field stays: structured metadata or a non-stream field), and every name the
// detected_level derivation reads keeps the stored field.
//
// conformance: semantics/stream-label-shadowed-by-ingest-json-key
func TestStreamTagRestoreEdgeCases(t *testing.T) {
	cases := []struct {
		name   string
		tags   map[string]string
		field  string
		expect string
	}{
		{"shadowed", map[string]string{"app": "api", "version": "v2"}, "v1", "v2"},
		{"first tag", map[string]string{"version": "v2", "zz": "1"}, "v1", "v2"},
		{"last tag", map[string]string{"a": "1", "version": "v2"}, "v1", "v2"},
		{"no such tag keeps the field", map[string]string{"app": "api"}, "v7", "v7"},
		{"no tag, no field", map[string]string{"app": "api"}, "", ""},
		{"name inside another value", map[string]string{"a": `x,version="v9"`, "version": "v2"}, "v1", "v2"},
		{"name inside another value, no tag", map[string]string{"a": `{version="v9"}`, "b": "c"}, "v1", "v1"},
		{"longer name ending in it", map[string]string{"app_version": "v9", "version": "v2"}, "v1", "v2"},
		{"only the longer name", map[string]string{"app_version": "v9"}, "v1", "v1"},
		{"quotes, backslash, comma", map[string]string{"version": `a"b\c,d`}, "x", `a"b\c,d`},
		{"value ending in a backslash before it", map[string]string{"a": `\`, "version": "v3"}, "v1", "v3"},
		{"unicode", map[string]string{"version": "wersja-ż"}, "v1", "wersja-ż"},
		{"control character", map[string]string{"version": "a\tb"}, "v1", "a\tb"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := applyRestoreForTest(t, "version", marshalStreamForTest(c.tags), c.field); got != c.expect {
				t.Fatalf("%s field %q: got %q, want %q", marshalStreamForTest(c.tags), c.field, got, c.expect)
			}
		})
	}
	for _, name := range DetectedLevelFieldNames {
		if got := streamTagRestorePipes([]string{name}); got != "" {
			t.Errorf("%s is read by the detected_level derivation and must keep its stored field: %s", name, got)
		}
	}
	if got := streamTagRestorePipes(nil); got != "" {
		t.Errorf("no names: %q", got)
	}
}

// The restore sits after the query's filters and before its first pipe, holds
// no '"' or '|' a quote-aware pipe scanner could misread, and
// WithoutStreamTagRestore removes it whole.
//
// conformance: semantics/stream-label-shadowed-by-ingest-json-key
func TestStreamTagRestorePlacementAndStrip(t *testing.T) {
	restore := streamTagRestorePipes([]string{"env", "version"})
	for _, pipe := range strings.Split(restore, " | ") {
		if body, ok := strings.CutPrefix(pipe, "| "); ok {
			pipe = body
		}
		if strings.HasPrefix(pipe, "extract_regexp ") && strings.ContainsAny(pipe, `"|`) {
			t.Errorf("the regexp pipe holds a quote or a pipe character: %s", pipe)
		}
	}
	parts := withStreamTagRestore([]string{`app:="a"`, `~"x"`, `| unpack_logfmt`, `| stats by (version) count()`}, []string{"env", "version"})
	if len(parts) != 5 || parts[2] != restore {
		t.Fatalf("placement: %q", parts)
	}
	query := strings.Join(parts, " ")
	if got, want := WithoutStreamTagRestore(query), `app:="a" ~"x" | unpack_logfmt | stats by (version) count()`; got != want {
		t.Fatalf("strip:\n got %s\nwant %s", got, want)
	}
	if got := withStreamTagRestore([]string{`app:="a"`}, []string{"version"}); len(got) != 2 || got[1] != streamTagRestorePipes([]string{"version"}) {
		t.Fatalf("no pipe: %q", got)
	}
	// VictoriaLogs reuses an extract's results across consecutive entries with
	// the same source value (lib/logstorage/pipe_extract.go writeBlock), the
	// original field included under skip_empty_results or keep_original_fields:
	// a run of entries the stream gives no tag would all take the first
	// entry's field (one span_id series for a block). Only `if` keeps each
	// entry's own field.
	if strings.Contains(restore, "skip_empty_results") || strings.Contains(restore, "keep_original_fields") || strings.Count(restore, "extract if (__lxst_") != 2 {
		t.Fatalf("the restore must extract only where the stream has the tag: %s", restore)
	}
	if q := `app:="a" | extract_regexp "x(?P<y>.+)" | stats count()`; WithoutStreamTagRestore(q) != q {
		t.Fatalf("a query's own extract must stay: %s", WithoutStreamTagRestore(q))
	}
}

// FuzzStreamTagRestoreLikeLoki: for any stream tags and stored field, the
// restored value is the stream label's (Loki's), or the stored field when the
// stream has no such label.
func FuzzStreamTagRestoreLikeLoki(f *testing.F) {
	f.Add("version", "v2", "app", "api", "v1", true)
	f.Add("version", `a"b\c`, "a", `x,version="v9"`, "v1", true)
	f.Add("env", "", "app_env", "e", "stored", false)
	f.Add("version", "v2", "zzz", "{version=\"q\"}", "", true)
	f.Fuzz(func(t *testing.T, name, value, other, otherValue, field string, has bool) {
		if !regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`).MatchString(name) || other == name || other == "" ||
			strings.ContainsAny(other, `=",{}`) || containsString(DetectedLevelFieldNames, name) {
			return
		}
		tags := map[string]string{other: otherValue}
		if has && value != "" {
			tags[name] = value // VictoriaLogs keeps no empty stream tag
		}
		for k, v := range tags {
			if v == "" {
				delete(tags, k)
			}
		}
		want := field
		if v, ok := tags[name]; ok {
			want = v
		}
		if got := applyRestoreForTest(t, name, marshalStreamForTest(tags), field); got != want {
			t.Fatalf("%s field %q: got %q, want %q", marshalStreamForTest(tags), field, got, want)
		}
	})
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
