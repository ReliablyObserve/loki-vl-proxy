package logsql

import (
	"strings"
	"testing"
)

// sanitizeForTest is the label name sanitizing the proxy passes (every character that is not a letter, a digit or
// an underscore becomes one underscore).
func sanitizeForTest(name string) string {
	var b strings.Builder
	for _, r := range name {
		if r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('_')
	}
	return b.String()
}

// The series identity of a window's rows: the stream fields, and the other stored fields under Loki's names, a name
// a stream label holds taking _extracted on the rows of such a stream.
//
// conformance: metric-identity/structured-metadata-splits-series
func TestNewSeriesMetadata(t *testing.T) {
	stream := []string{"service_name", "app", "env", "level"}
	fields := []string{"_msg", "_stream", "_stream_id", "_time", "app", "env", "level", "service_name", "detected_level",
		"trace_id", "service.name", "k8s.pod.name"}
	s, ok := NewSeriesMetadata(stream, fields, sanitizeForTest)
	if !ok {
		t.Fatal("no metadata")
	}
	var b strings.Builder
	for _, p := range s.pipes {
		b.WriteString(p.(interface{ String() string }).String())
		b.WriteByte(' ')
	}
	want := `| format "<k8s.pod.name>" as k8s_pod_name ` +
		`| format if (service_name:*) "<service.name>" as service_name_extracted ` +
		`| format if (service_name:"") "<service.name>" as service_name ` +
		`| set_stream_fields app, env, level, service_name, k8s_pod_name, service_name_extracted, trace_id `
	if got := b.String(); got != want {
		t.Fatalf("pipes\n got %s\nwant %s", got, want)
	}
	if s.Key() == "" || s.Empty() {
		t.Fatal("empty key")
	}
	for _, none := range [][]string{
		{"_msg", "_time", "_stream", "_stream_id", "app", "level"},
		{"app", "detected_level"},
		nil,
	} {
		if _, ok := NewSeriesMetadata([]string{"app", "level"}, none, sanitizeForTest); ok {
			t.Errorf("%v: metadata found", none)
		}
	}
	// A sanitized name another stored field already has is left out.
	if s, ok := NewSeriesMetadata([]string{"app"}, []string{"app", "a.b", "a_b"}, sanitizeForTest); !ok || strings.Contains(s.Key(), "a.b") {
		t.Errorf("collision: %q %v", s.Key(), ok)
	}
}

// Where the identity is written: after the filters and the pipes that derive the labels Loki derives at ingest,
// before the query's own stages, once.
//
// conformance: metric-identity/structured-metadata-splits-series
func TestWithSeriesMetadata(t *testing.T) {
	s, _ := NewSeriesMetadata([]string{"app"}, []string{"app", "trace_id"}, sanitizeForTest)
	cases := []struct{ in, want string }{
		{`app:="a" | stats by (_stream, level) count()`,
			`app:="a" | set_stream_fields app, trace_id | stats by (_stream, level) count()`},
		{`app:="a" | format "<service.name>" as service_name keep_original_fields | filter x:* | stats by (_stream) count()`,
			`app:="a" | format "<service.name>" as service_name keep_original_fields | filter x:* | set_stream_fields app, trace_id | stats by (_stream) count()`},
		{`app:="a" | unpack_json from _msg result_prefix "__j_" | delete __j_* | unpack_json | stats by (_stream) count()`,
			`app:="a" | unpack_json from _msg result_prefix "__j_" | delete __j_* | set_stream_fields app, trace_id | unpack_json | stats by (_stream) count()`},
		{`app:="a" | delete trace_id | stats by (_stream) count()`,
			`app:="a" | set_stream_fields app | delete trace_id | stats by (_stream) count()`},
		{`app:="a" | stats by (_stream) count() | delete trace_id`,
			`app:="a" | set_stream_fields app, trace_id | stats by (_stream) count() | delete trace_id`},
	}
	for _, c := range cases {
		q, err := Parse(c.in)
		if err != nil {
			t.Fatalf("%s: %v", c.in, err)
		}
		if !ReadsStreamIdentity(*q) {
			t.Errorf("%s: no stream identity", c.in)
		}
		out, changed := WithSeriesMetadata(*q, s)
		if !changed || out.String() != c.want {
			t.Errorf("%s\n got %s (%v)\nwant %s", c.in, out.String(), changed, c.want)
			continue
		}
		if again, changed := WithSeriesMetadata(out, s); changed || again.String() != c.want {
			t.Errorf("%s: written twice: %s", c.in, again.String())
		}
	}
	if _, changed := WithSeriesMetadata(Query{}, SeriesMetadata{}); changed {
		t.Error("empty identity written")
	}
}

// Which reads take the identity: those whose stats read a series per stream.
//
// conformance: metric-identity/structured-metadata-splits-series
func TestReadsStreamIdentity(t *testing.T) {
	for q, want := range map[string]bool{
		`app:="a" | stats by (_stream, level, detected_level) count() as __lvp_inner | stats by (app) count()`:                true,
		`app:="a" | set_stream_fields __lvp_p_*, level, _stream | rename _stream as __lvp_sid | stats by (__lvp_sid) count()`: true,
		`app:="a" | copy _stream as __s | stats by (__s) count()`:                                                             true,
		`app:="a" | stats by (app) count()`:      false,
		`app:="a" | stats count()`:               false,
		`app:="a"`:                               false,
		`app:="a" | fields _time, _stream, _msg`: false,
		`app:="a" | stats by (app) count() | stats by (_stream) count()`:        true,
		`app:="a" | stats by (app) count() | rename _stream as x | sort by (x)`: false,
	} {
		parsed, err := Parse(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if got := ReadsStreamIdentity(*parsed); got != want {
			t.Errorf("%s: %v, want %v", q, got, want)
		}
	}
}
