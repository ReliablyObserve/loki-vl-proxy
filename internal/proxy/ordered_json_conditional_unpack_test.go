package proxy

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

// The pipes of the ordered JSON stats pushdown parse only the lines that can
// hold a label a filter requires: a prefilter per required label, then an
// unpack restricted to lines lacking a label as a stored field.
// conformance: parser-json, parser-logfmt, semantics/json-filter-pushdown-underscore-label, semantics/json-filter-pushdown-translated-label
func TestOrderedJSONUnpackPipesPrefilter(t *testing.T) {
	const jsonEsc = ` or _msg:~"\\\\u")`
	stored := map[string]string{"service_version": "service.version"}
	for _, tc := range []struct {
		name     string
		parser   string
		unpack   []string
		stored   map[string]string
		required []string
		want     string
	}{
		{"no required label keeps the whole-line unpack", "json", []string{"level"}, nil, nil,
			" | unpack_json fields (level) keep_original_fields"},
		{"no parser has nothing to restrict", "", []string{"level"}, nil, []string{"level"}, ""},
		{"single field", "json", []string{"pipeline"}, nil, []string{"pipeline"},
			` | filter (pipeline:* or _msg:"pipeline"` + jsonEsc + ` | unpack_json if ((-pipeline:*)) fields (pipeline) keep_original_fields`},
		{"grouped label beside the required one", "json", []string{"level", "pipeline"}, nil, []string{"pipeline"},
			` | filter (pipeline:* or _msg:"pipeline"` + jsonEsc + ` | unpack_json if ((-level:*) or (-pipeline:*)) fields (level, pipeline) keep_original_fields`},
		{"stored spelling is read and kept", "json", []string{"service_version"}, stored, []string{"service_version"},
			` | filter (service_version:* or ` + "`service.version`" + `:* or _msg:"service_version"` + jsonEsc +
				` | unpack_json if ((-service_version:* -` + "`service.version`" + `:*)) fields (service_version) keep_original_fields` +
				` | format if (` + "`service.version`" + `:*) "<service.version>" as service_version`},
		{"two required labels", "json", []string{"pipeline", "level"}, nil, []string{"pipeline", "level"},
			` | filter (pipeline:* or _msg:"pipeline"` + jsonEsc + ` | filter (level:* or _msg:"level"` + jsonEsc +
				` | unpack_json if ((-pipeline:*) or (-level:*)) fields (pipeline, level) keep_original_fields`},
		{"logfmt has no escape spelling", "logfmt", []string{"job_id"}, nil, []string{"job_id"},
			` | filter (job_id:* or _msg:"job_id") | unpack_logfmt if ((-job_id:*)) fields (job_id) keep_original_fields`},
		{"leading underscore is quoted as an identifier", "json", []string{"_x"}, nil, []string{"_x"},
			` | filter (_x:* or _msg:"_x"` + jsonEsc + ` | unpack_json if ((-_x:*)) fields (_x) keep_original_fields`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := orderedJSONUnpackPipes(tc.parser, tc.unpack, tc.stored, tc.required); got != tc.want {
				t.Fatalf("got  %q\nwant %q", got, tc.want)
			}
		})
	}
}

// Only a filter that rejects the empty value makes its label required: a line
// without the label cannot pass it, whatever the parser does. A label that is
// grouped on, or compared with != or !~ against a value, is held by lines that
// lack it too; a pipe other than a filter before the parser could set it.
// conformance: parser-json, semantics/json-filter-pushdown-without-error-drop, semantics/label-filter-before-parser-pushdown
func TestOrderedJSONRequiredFields(t *testing.T) {
	const base = `app:="api"`
	for _, tc := range []struct {
		query string
		base  string
		want  []string
	}{
		{`sum by (pipeline) (count_over_time({app="api"} | json | drop __error__ | pipeline!="" [1m]))`, base, []string{"pipeline"}},
		{`sum by (level) (count_over_time({app="api"} | json | pipeline="logs/loki" [1m]))`, base, []string{"pipeline"}},
		{`sum(rate({app="api"} | json | pipeline=~"logs/.*" | drop __error__ [1m]))`, base, []string{"pipeline"}},
		{`sum(rate({app="api"} | json | pipeline=~".*" | drop __error__ [1m]))`, base, nil},
		{`sum by (level) (count_over_time({app="api"} | json | pipeline="a" | level!="" [1m]))`, base, []string{"pipeline", "level"}},
		{`sum by (level) (count_over_time({app="api"} | json | pipeline!="a" | drop __error__ [1m]))`, base, nil},
		{`sum by (level) (count_over_time({app="api"} | json | pipeline!~"a.*" | drop __error__ [1m]))`, base, nil},
		{`sum by (level, detected_level) (count_over_time({app="api"} | json | drop __error__ [1m]))`, base, nil},
		{`sum by (pipeline) (count_over_time({app="api"} | json | pipeline!="" [1m]))`, base + ` | unpack_json`, nil},
		{`sum by (pipeline) (count_over_time({app="api"} | json | pipeline!="" [1m]))`, base + ` | format "<x>" as pipeline`, nil},
		{`sum by (pipeline) (count_over_time({app="api"} | json | pipeline!="" [1m]))`, base + ` | filter level:="info"`, []string{"pipeline"}},
	} {
		t.Run(tc.query+tc.base, func(t *testing.T) {
			plan, ok := compileOrderedJSONMetric(tc.query)
			if !ok || !plan.pushdown {
				t.Fatalf("expected a pushdown plan, got ok=%v", ok)
			}
			if got := plan.requiredFields(tc.base); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("requiredFields = %v, want %v", got, tc.want)
			}
		})
	}
}

// The prefiltered and conditionally unpacked query keeps exactly the lines the
// whole-line unpack keeps, with the same values, on lines shaped to break a
// careless prefilter: keys spelled with a \u escape, nested objects, keys
// Loki sanitizes (user.id, pipe-line), the field's name inside a value or a
// string, arrays, unparseable and plain lines, and stored fields with and
// without the body's key.
// conformance: parser-json, semantics/json-filter-pushdown-underscore-label, semantics/json-label-spelling-probe, semantics/json-filter-pushdown-translated-label, parsed-label-series-identity
func TestOrderedJSONConditionalUnpackEqualsWholeLineUnpack(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	msgs := []string{
		`{"pipeline":"logs/loki","level":"info"}`,
		`{"level":"info"}`,
		`{"\u0070ipeline":"escaped","level":"warn"}`,
		`{"pipeline":""}`,
		`{"pipeline":null}`,
		`{"pipeline":["a"]}`,
		`{"a":{"pipeline":"nested"}}`,
		`{"pipe-line":"x","user.id":"7","user_id":"8"}`,
		`{"msg":"the pipeline failed","level":"error"}`,
		`{"msg":"pipeline"}`,
		`  {"pipeline":"leading space"}`,
		`{"pipeline":"truncated`,
		`plain text pipeline line`,
		`pipeline=logfmt`,
		`{"service_version":"9.9.9","pipeline":"logs/loki"}`,
		`{"service":{"version":"1"}}`,
		`{"PIPELINE":"upper"}`,
		`{"pipeline_x":"longer key","level":"debug"}`,
	}
	var rows []pushdownRow
	for i, msg := range msgs {
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(i) * time.Second), stream: map[string]string{"app": "api"}, msg: msg})
		// The same body with a stored pipeline and with a stored service.version.
		rows = append(rows,
			pushdownRow{ts: s0.Add(time.Duration(i) * time.Second), stream: map[string]string{"app": "api", "pipeline": "stream"}, msg: msg},
			pushdownRow{ts: s0.Add(time.Duration(i) * time.Second), stream: map[string]string{"app": "api"}, vl: map[string]string{"service.version": "0.1"}, msg: msg})
	}
	stored := map[string]string{"service_version": "service.version"}
	fake := &pushdownFakeVL{}
	for _, tc := range []struct {
		name     string
		parser   string
		unpack   []string
		stored   map[string]string
		required []string
		filters  string
	}{
		{"not empty", "json", []string{"pipeline"}, nil, []string{"pipeline"}, ` | filter -pipeline:=""`},
		{"equals", "json", []string{"level", "pipeline"}, nil, []string{"pipeline"}, ` | filter pipeline:="logs/loki"`},
		{"regexp", "json", []string{"pipeline"}, nil, []string{"pipeline"}, ` | filter pipeline:~"^(?:l.*)$"`},
		{"stored spelling", "json", []string{"service_version", "pipeline"}, stored, []string{"service_version"}, ` | filter -service_version:=""`},
		{"two required", "json", []string{"pipeline", "level"}, nil, []string{"pipeline", "level"}, ` | filter -pipeline:="" | filter -level:=""`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			whole := "app:=\"api\"" + orderedJSONUnpackPipes(tc.parser, tc.unpack, tc.stored, nil) + tc.filters
			cond := "app:=\"api\"" + orderedJSONUnpackPipes(tc.parser, tc.unpack, tc.stored, tc.required) + tc.filters
			kept := 0
			for _, row := range rows {
				wantValues, wantKept := fake.applyPipes(t, whole, row)
				gotValues, gotKept := fake.applyPipes(t, cond, row)
				if wantKept != gotKept {
					t.Fatalf("row %+v %q: kept=%v, whole-line unpack kept=%v", row.stream, row.msg, gotKept, wantKept)
				}
				if !gotKept {
					continue
				}
				kept++
				for _, field := range tc.unpack {
					if gotValues[field] != wantValues[field] {
						t.Fatalf("row %q: %s=%q, whole-line unpack %q", row.msg, field, gotValues[field], wantValues[field])
					}
				}
			}
			if kept == 0 || strings.Count(cond, "unpack_json if") != 1 {
				t.Fatalf("fixture kept %d rows by %q", kept, cond)
			}
		})
	}
}

// BenchmarkOrderedJSONUnpackPipes measures rendering the pipes of a filtered
// breakdown (the prefilter and conditional unpack) and of an unfiltered volume.
func BenchmarkOrderedJSONUnpackPipes(b *testing.B) {
	stored := map[string]string{"service_version": "service.version"}
	b.Run("filtered", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = orderedJSONUnpackPipes("json", []string{"level", "service_version"}, stored, []string{"service_version"})
		}
	})
	b.Run("unfiltered", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = orderedJSONUnpackPipes("json", []string{"level"}, nil, nil)
		}
	})
}
