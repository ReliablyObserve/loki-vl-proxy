package translator

import (
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

var (
	narrowUnpackRE  = regexp.MustCompile(`\| unpack_(?:logfmt|json)(?: fields \(([^)]*)\) skip_empty_results)?(?: |$)`)
	narrowVariantRE = regexp.MustCompile(`\| format if \("([^"]+)":\*\) "<[^>]+>" as (\S+) keep_original_fields`)
)

func translateNarrowed(t testing.TB, logql string) string {
	t.Helper()
	out, err := TranslateLogQLWithCapabilities(logql, nil, nil, logsql.CapabilitiesFor("v1.53.0"))
	if err != nil {
		t.Fatalf("%s: %v", logql, err)
	}
	return out
}

// The unwrap metrics whose `| logfmt` is the last stage before the unwrap and
// whose grouping names plain labels unpack only the unwrapped label (in every
// key spelling) and the grouped labels; every other shape keeps the bare
// unpack.
// conformance: quality/unwrap-logfmt-narrowed-unpack, parser-logfmt, metric-series-identity
func TestNarrowUnwrapLogfmtUnpackEdgeTable(t *testing.T) {
	for _, tc := range []struct {
		logql string
		want  string // the unpack pipe, "" for none
	}{
		{`sum by (service_name) (sum_over_time({service_name="cache-redis"} | logfmt | unwrap bytes(size_bytes) [5m]))`,
			`| unpack_logfmt fields (size_bytes, "size.bytes", "size-bytes", service_name) skip_empty_results `},
		{`sum by (service_name) (sum_over_time({service_name="cache-redis"} | logfmt | unwrap duration(ttl) [5m]))`,
			`| unpack_logfmt fields (ttl, service_name) skip_empty_results `},
		{`sum by (service_name) (sum_over_time({service_name="cache-redis"} | logfmt | unwrap duration_seconds(ttl) [5m]))`,
			`| unpack_logfmt fields (ttl, service_name) skip_empty_results `},
		{`sum by (service_name) (rate({service_name="payment-service"} | logfmt | unwrap amount [5m]))`,
			`| unpack_logfmt fields (amount, service_name) skip_empty_results `},
		{`sum by (op, service_name) (sum_over_time({env="production"} | logfmt | unwrap latency_ms [5m]))`,
			`| unpack_logfmt fields (latency_ms, "latency.ms", "latency-ms", op, service_name) skip_empty_results `},
		{`sum by (http_status) (sum_over_time({env="production"} | logfmt | unwrap size_bytes [5m]))`,
			`| unpack_logfmt fields (size_bytes, "size.bytes", "size-bytes", http_status, "http.status", "http-status") skip_empty_results `},
		{`quantile_over_time(0.95, {env="production"} | logfmt | unwrap latency_ms [5m]) by (service_name)`,
			`| unpack_logfmt fields (latency_ms, "latency.ms", "latency-ms", service_name) skip_empty_results `},
		// The series of an ungrouped or `without` metric are told apart by the line
		// and stream (internal names): the bare unpack stays.
		{`sum(sum_over_time({service_name="payment-service"} | logfmt | unwrap amount [5m]))`, `| unpack_logfmt `},
		{`sum without (pod) (sum_over_time({env="production"} | logfmt | unwrap latency_ms [5m]))`, `| unpack_logfmt `},
		{`sum_over_time({service_name="payment-service"} | logfmt | unwrap amount [5m])`, `| unpack_logfmt `},
		// A stage after the parser reads or writes other fields.
		{`sum by (service_name) (sum_over_time({service_name="payment-service"} | logfmt | unwrap amount | __error__="" [5m]))`, `| unpack_logfmt `},
		{`sum by (service_name) (sum_over_time({service_name="cache-redis"} | logfmt | level="info" | unwrap size_bytes [5m]))`, `| unpack_logfmt `},
		{`sum by (service_name) (sum_over_time({service_name="cache-redis"} | logfmt | label_format x=op | unwrap size_bytes [5m]))`, `| unpack_logfmt `},
		// Another parser, or none.
		{`sum by (service_name) (sum_over_time({service_name="api-gateway"} | json | unwrap duration_ms [5m]))`, `| unpack_json `},
		{`sum by (service_name) (count_over_time({service_name="cache-redis"} | logfmt [5m]))`, `| unpack_logfmt `},
		// Other evaluation paths keep their query.
		{`max by (service_name) (max_over_time({service_name="cache-redis"} | logfmt | unwrap size_bytes [5m]))`, `| unpack_logfmt `},
	} {
		got := narrowUnpackRE.FindAllString(translateNarrowed(t, tc.logql), -1)
		if len(got) != 1 || got[0] != tc.want {
			t.Errorf("%s:\n got %q\nwant %q", tc.logql, got, tc.want)
		}
	}
}

func TestNarrowUnwrapLogfmtUnpackKeepsOtherQueries(t *testing.T) {
	const base = `app:="x" | unpack_logfmt`
	for _, tc := range []struct{ query, by, gate, want string }{
		{base, "service_name", "size_bytes", base + ` fields (size_bytes, "size.bytes", "size-bytes", service_name) skip_empty_results`},
		{base, emptyByGrouping, "amount", base + " fields (amount) skip_empty_results"},
		{base, "", "amount", base},
		{base, "_stream, _msg", "amount", base},
		{base, "service.version", "amount", base},
		{base, `"k8s.pod.name"`, "amount", base},
		{base, "op", "x-y", base},
		{base + " | filter op:=a", "op", "amount", base + " | filter op:=a"},
		{`app:="x" | unpack_json | unpack_logfmt`, "op", "amount", `app:="x" | unpack_json | unpack_logfmt`},
		{`app:="x" | unpack_logfmt fields (a)`, "op", "amount", `app:="x" | unpack_logfmt fields (a)`},
		{`app:="x" | unpack_logfmt from msg`, "op", "amount", `app:="x" | unpack_logfmt from msg`},
		{base, "amount, amount", "amount", base + " fields (amount) skip_empty_results"},
	} {
		if got := narrowUnwrapLogfmtUnpack(tc.query, tc.by, tc.gate); got != tc.want {
			t.Errorf("%q by %q gate %q:\n got %q\nwant %q", tc.query, tc.by, tc.gate, got, tc.want)
		}
	}
}

// narrowRow is a line before the unpack: the fields it already has (stream
// labels, structured metadata, derived service_name) and the logfmt pairs
// VictoriaLogs' parser yields from its body, in order.
type narrowRow struct {
	stored map[string]string
	pairs  [][2]string
}

// emulateNarrowPipes runs the unpack and the key pipes of a translated query
// over a row the way VictoriaLogs does, and returns the value of each read
// field (missing as empty) and whether the unwrap filter keeps the row.
func emulateNarrowPipes(t testing.TB, query string, row narrowRow, read []string, unwrap string) (map[string]string, bool) {
	t.Helper()
	m := narrowUnpackRE.FindStringSubmatchIndex(query)
	if m == nil {
		t.Fatalf("no unpack in %q", query)
	}
	f := map[string]string{}
	for k, v := range row.stored {
		f[k] = v
	}
	if m[2] < 0 { // bare: every key, overwriting
		for _, p := range row.pairs {
			f[p[0]] = p[1]
		}
	} else { // named fields, skip_empty_results
		names := map[string]bool{}
		for _, n := range strings.Split(query[m[2]:m[3]], ", ") {
			if u, err := strconv.Unquote(n); err == nil {
				n = u
			}
			names[n] = true
		}
		for _, p := range row.pairs {
			if names[p[0]] && (p[1] != "" || f[p[0]] == "") {
				f[p[0]] = p[1]
			}
		}
	}
	for _, v := range narrowVariantRE.FindAllStringSubmatch(query[m[1]:], -1) {
		if f[v[1]] != "" && f[v[2]] == "" {
			f[v[2]] = f[v[1]]
		}
	}
	out := map[string]string{}
	for _, name := range read {
		out[name] = f[name]
	}
	return out, f[unwrap] != ""
}

// hasEmptyPair reports whether the line has a key of the label, in any
// spelling, with an empty value.
func hasEmptyPair(row narrowRow, name string) bool {
	for _, key := range parsedKeyVariants(name) {
		for _, p := range row.pairs {
			if p[0] == key && p[1] == "" {
				return true
			}
		}
	}
	return false
}

// Over random unwrap metrics, groupings and lines (stored labels and
// metadata, keys in every spelling, repeated keys, empty values), the narrowed
// unpack gives every field the rest of the query reads the value the bare
// unpack gives it, and keeps the same rows. The one difference is Loki's own
// rule: a key with an empty value adds no label (pkg/logql/log/parser.go:425),
// so the field keeps its earlier value where the bare unpack empties it.
// conformance: quality/unwrap-logfmt-narrowed-unpack, parser-logfmt, metric-series-identity
func TestNarrowUnwrapLogfmtUnpackDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(20261104))
	groups := [][]string{{"service_name"}, {"op"}, {"op", "service_name"}, {"http_status"}, {"app", "op"}}
	unwraps := []string{"size_bytes", "amount", "ttl", "latency_ms"}
	convs := []string{"", "bytes", "duration"}
	keys := []string{"size_bytes", "size.bytes", "size-bytes", "amount", "ttl", "latency_ms", "latency.ms", "op", "service_name", "http_status", "http.status", "app", "msg", "level"}
	values := []string{"", "1", "250", "1KB", "5s", "x", "abc"}
	for i := 0; i < 400; i++ {
		by := groups[rng.Intn(len(groups))]
		unwrap := unwraps[rng.Intn(len(unwraps))]
		conv := convs[rng.Intn(len(convs))]
		field := unwrap
		if conv != "" {
			field = conv + "(" + unwrap + ")"
		}
		logql := `sum by (` + strings.Join(by, ", ") + `) (sum_over_time({app="x"} | logfmt | unwrap ` + field + ` [5m]))`
		narrowed := translateNarrowed(t, logql)
		m := narrowUnpackRE.FindStringSubmatchIndex(narrowed)
		if m == nil || m[2] < 0 {
			t.Fatalf("%s: not narrowed: %s", logql, narrowed)
		}
		bare := narrowed[:m[0]] + "| unpack_logfmt " + narrowed[m[1]:]
		read := append([]string{unwrap}, by...)
		for j := 0; j < 20; j++ {
			row := narrowRow{stored: map[string]string{"app": "x", "service_name": "svc"}}
			for _, k := range []string{"op", "size_bytes", "amount", "http_status"} {
				if rng.Intn(5) == 0 {
					row.stored[k] = values[1+rng.Intn(len(values)-1)]
				}
			}
			for n := rng.Intn(6); n > 0; n-- {
				row.pairs = append(row.pairs, [2]string{keys[rng.Intn(len(keys))], values[rng.Intn(len(values))]})
			}
			gotN, keepN := emulateNarrowPipes(t, narrowed, row, read, unwrap)
			gotB, keepB := emulateNarrowPipes(t, bare, row, read, unwrap)
			for _, name := range read {
				if gotN[name] != gotB[name] && (!hasEmptyPair(row, name) || gotN[name] == "") {
					t.Fatalf("%s row %+v: %s narrowed %q, bare %q", logql, row, name, gotN[name], gotB[name])
				}
			}
			if keepN != keepB && (!hasEmptyPair(row, unwrap) || !keepN) {
				t.Fatalf("%s row %+v: kept narrowed %v, bare %v", logql, row, keepN, keepB)
			}
		}
	}
}

// The narrowed unpack names the unwrapped label and every grouped label, ends
// the query it was given, and leaves every other query alone.
func FuzzNarrowUnwrapLogfmtUnpack(f *testing.F) {
	f.Add(`app:="x" | unpack_logfmt`, "service_name", "size_bytes")
	f.Add(`app:="x" | unpack_logfmt`, "op, service_name", "latency_ms")
	f.Add(`app:="x" | unpack_logfmt | filter a:=b`, "op", "amount")
	f.Add(`app:="x" | unpack_logfmt`, emptyByGrouping, "amount")
	f.Add(`app:="x" | unpack_logfmt`, "_stream", "amount")
	f.Fuzz(func(t *testing.T, query, by, gate string) {
		got := narrowUnwrapLogfmtUnpack(query, by, gate)
		if got == query {
			return
		}
		rest, ok := strings.CutPrefix(got, query+" fields (")
		if !ok || !strings.HasSuffix(rest, ") skip_empty_results") || !strings.HasSuffix(query, " | unpack_logfmt") {
			t.Fatalf("%q by %q gate %q: rewritten as %q", query, by, gate, got)
		}
		names := map[string]bool{}
		for _, n := range strings.Split(strings.TrimSuffix(rest, ") skip_empty_results"), ", ") {
			if u, err := strconv.Unquote(n); err == nil {
				n = u
			}
			names[n] = true
		}
		labels := []string{gate}
		if by != emptyByGrouping {
			labels = append(labels, strings.Split(by, ",")...)
		}
		for _, l := range labels {
			l = strings.TrimSpace(l)
			if !labelIdentRE.MatchString(l) || strings.HasPrefix(l, "_") || !names[l] {
				t.Fatalf("%q by %q gate %q: label %q not named in %q", query, by, gate, l, got)
			}
		}
	})
}
