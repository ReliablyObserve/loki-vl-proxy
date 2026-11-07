package logsql_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// readCase is a query the parser reads, with the canonical form it prints. VictoriaLogs' parser reads the input
// and the canonical form to the same query (checked with the parser of VictoriaLogs v1.51.1, lib/logstorage, when
// the cases were written).
type readCase struct{ in, want string }

// readCases are the pipes and filters the translator writes, by construct. Each parses, prints as want, and the
// printed form parses and prints the same.
var readCases = map[string][]readCase{
	"format": {
		{`* | format "<a> <b>" as c`, `* | format "<a> <b>" as c`},
		{`* | format "<a>"`, `* | format "<a>"`},
		{`* | format foo as c`, `* | format "foo" as c`},
		{`* | format if (a:b) "<a>" as c`, `* | format if (a:b) "<a>" as c`},
		{`* | format if (a:b) "<a>" as c keep_original_fields`, `* | format if (a:b) "<a>" as c keep_original_fields`},
		{`* | format "<a>" as c skip_empty_results`, `* | format "<a>" as c skip_empty_results`},
		{`* | format if (a:* -b:*) "<a>" keep_original_fields`, `* | format if (a:* -b:*) "<a>" keep_original_fields`},
		{`* | format "a\\b\"c\n<d>" as e`, `* | format "a\\b\"c\n<d>" as e`},
		{"* | format `a\\b<c>` as d", `* | format "a\\b<c>" as d`},
		{`* | format 'x<y>' as z`, `* | format "x<y>" as z`},
		{`* | format if () "<a>" as b`, `* | format if () "<a>" as b`},
	},
	"extract": {
		{`* | extract "<a> <_>"`, `* | extract "<a> <_>"`},
		{`* | extract "<a> <_>" from _msg`, `* | extract "<a> <_>" from _msg`},
		{`* | extract if (-a:*) "<a>" from b`, `* | extract if (-a:*) "<a>" from b`},
		{`* | extract "<a>" from "b.c" keep_original_fields`, `* | extract "<a>" from "b.c" keep_original_fields`},
		{`* | extract "<a>" skip_empty_results`, `* | extract "<a>" skip_empty_results`},
		{`* | extract_regexp "(?P<a>[0-9]+)"`, "* | extract_regexp `(?P<a>[0-9]+)`"},
		{"* | extract_regexp `(?P<a>\\d+)` from b", "* | extract_regexp `(?P<a>\\d+)` from b"},
		{`* | extract_regexp if (c:*) "(?P<a>x)" from b skip_empty_results`, "* | extract_regexp if (c:*) `(?P<a>x)` from b skip_empty_results"},
		{"* | extract_regexp \"(?P<a>`)\"", "* | extract_regexp \"(?P<a>`)\""},
	},
	"replace": {
		{`* | replace ("a", "b")`, `* | replace ("a", "b")`},
		{`* | replace ("a", "b") at c`, `* | replace ("a", "b") at c`},
		{`* | replace ("a", "b") at c limit 2`, `* | replace ("a", "b") at c limit 2`},
		{`* | replace if (c:*) ("a", "b") at c`, `* | replace if (c:*) ("a", "b") at c`},
		{`* | replace ("a\"b", "c\\d")`, `* | replace ("a\"b", "c\\d")`},
		{`* | replace_regexp ("a+", "b")`, "* | replace_regexp (`a+`, \"b\")"},
		{"* | replace_regexp (`^\\d`, \"0\") at c", "* | replace_regexp (`^\\d`, \"0\") at c"},
		{`* | replace_regexp if (c:i(inf)) ("(?i)x$", "") at c limit 1`, "* | replace_regexp if (c:i(\"inf\")) (`(?i)x$`, \"\") at c limit 1"},
	},
	"rename, copy": {
		{`* | rename a as b`, `* | rename a as b`},
		{`* | rename a b`, `* | rename a as b`},
		{`* | mv a as b, c as d`, `* | rename a as b, c as d`},
		{`* | rename "a.b" as c`, `* | rename "a.b" as c`},
		{`* | copy a as b`, `* | copy a as b`},
		{`* | copy a b`, `* | copy a as b`},
		{`* | copy a as b, "c.d" as e`, `* | copy a as b, "c.d" as e`},
		{`* | copy prefix* as other_*`, `* | copy prefix* as other_*`},
	},
	"pack": {
		{`* | pack_json`, `* | pack_json`},
		{`* | pack_json as p`, `* | pack_json as p`},
		{`* | pack_json fields (a, b)`, `* | pack_json fields (a, b)`},
		{`* | pack_json fields (a, "b.c") as p`, `* | pack_json fields (a, "b.c") as p`},
		{`* | pack_logfmt fields (a) as p`, `* | pack_logfmt fields (a) as p`},
		{`* | pack_logfmt p`, `* | pack_logfmt as p`},
	},
	"unpack": {
		{`* | unpack_json`, `* | unpack_json`},
		{`* | unpack_json from a`, `* | unpack_json from a`},
		{`* | unpack_json a`, `* | unpack_json from a`},
		{`* | unpack_json fields (a, b)`, `* | unpack_json fields (a, b)`},
		{`* | unpack_json from _msg fields (a, "b.c", d*) result_prefix "p_" keep_original_fields`, `* | unpack_json from _msg fields (a, "b.c", d*) result_prefix "p_" keep_original_fields`},
		{`* | unpack_json if (a:*) from b skip_empty_results`, `* | unpack_json if (a:*) from b skip_empty_results`},
		{`* | unpack_json preserve_keys (a)`, `* | unpack_json preserve_keys (a)`},
		{`* | unpack_json result_prefix p`, `* | unpack_json result_prefix "p"`},
		{`* | unpack_logfmt`, `* | unpack_logfmt`},
		{`* | unpack_logfmt from a fields (b) result_prefix "p." skip_empty_results`, `* | unpack_logfmt from a fields (b) result_prefix "p." skip_empty_results`},
		{`* | unpack_logfmt if (-a:*) fields (a)`, `* | unpack_logfmt if (-a:*) fields (a)`},
	},
	"stats": {
		{`* | stats count()`, `* | stats count()`},
		{`* | stats count() n`, `* | stats count() as n`},
		{`* | stats count() as n, sum(a)`, `* | stats count() as n, sum(a)`},
		{`* | stats by (a) count() n, max(b) m`, `* | stats by (a) count() as n, max(b) as m`},
		{`* | stats by (_time:5m) count() n`, `* | stats by (_time:5m) count() as n`},
		{`* | stats by (_time:300000000000ns offset 104999999999ns, a) count() n`, `* | stats by (_time:300000000000ns offset 104999999999ns, a) count() as n`},
		{`* | stats by (_time:1h offset -2h) count() n`, `* | stats by (_time:1h offset -2h) count() as n`},
		{"* | stats by (\"a.b\", `c`) count() n", "* | stats by (\"a.b\", `c`) count() as n"},
		{`* | stats count() if (a:b) n`, `* | stats count() if (a:b) as n`},
		{`* | stats count(a) n`, `* | stats count(a) as n`},
		{`* | stats count(a, b) n`, `* | stats count(a, b) as n`},
		{`* | stats count(*) n`, `* | stats count() as n`},
		{`* | stats count_uniq(a, b) n`, `* | stats count_uniq(a, b) as n`},
		{`* | count() n`, `* | stats count() as n`},
		{`* | rate() r`, `* | stats rate() as r`},
		{`* | stats count() x:y`, `* | stats count() as x:y`},
		{`* | stats by (a) sum(b) c | stats max(c)`, `* | stats by (a) sum(b) as c | stats max(c)`},
	},
	"coalesce": {
		{`* | coalesce(a, b) as c`, `* | coalesce(a, b) as c`},
		{`* | coalesce(a, "b.c") default "d" as e`, `* | coalesce(a, "b.c") default "d" as e`},
		{`* | coalesce(a) default d`, `* | coalesce(a) default "d"`},
	},
	"math/update": {
		{`* | math a + 1 as b | stats count()`, `* | math a + 1 as b | stats count()`},
	},
	"filters": {
		{`"a.b":="c"`, `"a.b":="c"`},
		{`"a.b":~"c"`, `"a.b":~"c"`},
		{"`a.b`:*", "`a.b`:*"},
		{`"a b":x`, `"a b":x`},
		{`-a:b`, `-a:b`},
		{`-a:*`, `-a:*`},
		{`-(a:b OR c:d)`, `-(a:b OR c:d)`},
		{`- a:b`, `-a:b`},
		{`!a:b`, `NOT a:b`},
		{`!(a OR b)`, `NOT (a OR b)`},
		{`NOT a:b`, `NOT a:b`},
		{`not a`, `NOT a`},
		{`a:!b`, `NOT a:b`},
		{`a:!"b"`, `a:!"b"`},
		{`a:!~"b"`, `NOT a:~"b"`},
		{`a:!"b"*`, `NOT a:b*`},
		{`a:-b`, `NOT a:b`},
		{`a:>5`, `a:>5`},
		{`a:>=5`, `a:>=5`},
		{`a:<5`, `a:<5`},
		{`a:<=1.5`, `a:<=1.5`},
		{`a:>.5`, `a:>.5`},
		{`a:>+5`, `a:>+5`},
		{`a:>-5`, `a:>-5`},
		{`a:<=1h5m`, `a:<=1h5m`},
		{`a:>10KB`, `a:>10KB`},
		{`a:>"5"`, `a:>"5"`},
		{`a b c`, `a b c`},
		{`a b or c d`, `a b OR c d`},
		{`a and b or c`, `a AND b OR c`},
		{`(a or b) c`, `(a OR b) c`},
		{`(a or b) and c`, `(a OR b) AND c`},
		{`a (b or c)`, `a (b OR c)`},
		{`a:(b or c)`, `a:(b OR c)`},
		{`_msg:(i(a) or i(b))`, `_msg:(i("a") OR i("b"))`},
		{`a:"b"`, `a:b`},
		{`a:b*`, `a:b*`},
		{`a:"b c"*`, `a:"b c"*`},
		{`a:*b*`, `a:*b*`},
		{`a:*"b c"*`, `a:*"b c"*`},
		{`a:=b`, `a:="b"`},
		{`a:=b*`, `a:="b"*`},
		{`="b"`, `="b"`},
		{`="b"*`, `="b"*`},
		{`~"a"`, `~"a"`},
		{`!~"a"`, `NOT ~"a"`},
		{`"a b"*`, `"a b"*`},
		{`*a*`, `*a*`},
		{`a:i(b)`, `a:i("b")`},
		{`a:i(b*)`, `a:i(b*)`},
		{`i(a*)`, `i(a*)`},
		{`i("a b")`, `i("a b")`},
		{`a:seq("b","c")`, `a:seq("b","c")`},
		{`seq(a, b)`, `seq("a","b")`},
		{`a:contains_all(b, c)`, `a:contains_all("b","c")`},
		{`contains_any("a")`, `contains_any("a")`},
		{`a:in(b, "c")`, `a:in(b, "c")`},
		{`a:in()`, `a:in()`},
		{`a:in(* | fields a)`, `a:in(* | fields a)`},
		{`in(a:b | fields c)`, `in(a:b | fields c)`},
		{`a:range(1, 5)`, `a:range(1, 5)`},
		{`a:ipv4_range(1.2.3.4, 5.6.7.8)`, `a:ipv4_range(1.2.3.4, 5.6.7.8)`},
		{`a:ipv4_range("1.2.3.0/24")`, `a:ipv4_range("1.2.3.0/24")`},
		{`a:ipv6_range("::", "ffff::")`, `a:ipv6_range("::", "ffff::")`},
		{`a:eq_field(b)`, `a:eq_field(b)`},
		{`a:le_field(b)`, `a:le_field(b)`},
		{`a:lt_field(b)`, `a:lt_field(b)`},
		{`a:len_range(1, 5)`, `a:len_range(1, 5)`},
		{`len_range(1, inf)`, `len_range(1, inf)`},
		{`a:string_range("a", "b")`, `a:string_range("a", "b")`},
		{`a:value_type(uint64)`, `a:value_type(uint64)`},
		{`a:exact("b")`, `a:exact("b")`},
		{`a:re("b")`, `a:re("b")`},
		{`pattern_match("a <_>")`, `pattern_match("a <_>")`},
		{`a:contains_common_case("b")`, `a:contains_common_case("b")`},
		{`_stream:{a="b", c=~"d"}`, `{a="b", c=~"d"}`},
		{`_stream_id:in(0123456789abcdef0123456789abcdef0123456789abcdef)`, `_stream_id:in(0123456789abcdef0123456789abcdef0123456789abcdef)`},
		{`{a="b"} c`, `{a="b"} c`},
		{`_time:5m`, `_time:5m`},
		{`_time:5m offset 1h`, `_time:5m offset 1h`},
		{`_time:1h5m`, `_time:1h5m`},
		{`_time:2026-11-07`, `_time:2026-11-07`},
		{`_time:2026-11-07T00:10:00Z`, `_time:2026-11-07T00:10:00Z`},
		{`_time:[2026-11-07T00:10:00Z, 2026-11-07T01:00:00Z)`, `_time:[2026-11-07T00:10:00Z, 2026-11-07T01:00:00Z)`},
		{`_time:(now-1h, now]`, `_time:(now-1h, now]`},
		{`_time:day_range[08:00, 18:00] offset 2h`, `_time:day_range[08:00, 18:00] offset 2h`},
		{`_time:week_range[Mon, Fri]`, `_time:week_range[Mon, Fri]`},
		{`a:/api`, `a:/api`},
		{`a # comment`, `a`},
		{`b`, `b`},
		{`a | filter b c | filter -d:e`, `a | filter b c | filter -d:e`},
	},
}

// conformance: architecture-syntax-package-layering
func TestParseReadsTranslatorQueries(t *testing.T) {
	for construct, cases := range readCases {
		t.Run(construct, func(t *testing.T) {
			for _, c := range cases {
				q, err := logsql.Parse(c.in)
				if err != nil {
					t.Errorf("Parse(%q): %v", c.in, err)
					continue
				}
				if got := q.String(); got != c.want {
					t.Errorf("Parse(%q) prints %q, want %q", c.in, got, c.want)
				}
				q2, err := logsql.Parse(c.want)
				if err != nil {
					t.Errorf("Parse(%q): %v", c.want, err)
					continue
				}
				if got := q2.String(); got != c.want {
					t.Errorf("the canonical form %q prints again as %q", c.want, got)
				}
			}
		})
	}
}

// rejectedQueries are what VictoriaLogs v1.51.1 rejects (checked with its parser) and the parser must too.
var rejectedQueries = []string{
	`* | format`,
	`* | format if "<a>"`,
	`* | format "<a>" as`,
	`* | format if (a:b "<a>"`,
	`* | extract`,
	`* | extract "x"`,
	`* | extract "<a>" from`,
	`* | extract_regexp`,
	`* | replace ("a")`,
	`* | replace ("a", "b") at`,
	`* | replace ("a", "b") limit -1`,
	`* | replace ("a", "b") limit x`,
	`* | replace_regexp ("a")`,
	`* | rename`,
	`* | rename a as`,
	`* | copy a as b,`,
	`* | copy a as`,
	`* | pack_json fields a`,
	`* | unpack_json fields`,
	`* | unpack_json result_prefix`,
	`* | unpack_logfmt fields (a b)`,
	`* | stats`,
	`* | stats by (a)`,
	`* | stats by (_time:foo) count() n`,
	`* | stats by (_time:5m offset x) count() n`,
	`* | stats count() if a:b n`,
	`* | stats quantile(2, a) q`,
	`* | stats by (a) count() a`,
	`* | coalesce() as a`,
	`* | fields`,
	`* | fields a,`,
	`* | delete`,
	`* | limit -1`,
	`* | sample 0`,
	`* | generate_sequence limit 5`,
	`a:in`,
	`in`,
	`contains_all`,
	`a:>`,
	`-`,
	`a b)`,
	`(a`,
	`"abc`,
	`a:"b`,
	`a:(b`,
	`a:range(1)`,
	`a:range(foo, bar)`,
	`a:eq_field(b, c)`,
	`a:ipv4_range(1.2.3)`,
	`a:in(b c)`,
	`_time:foo`,
	`_time:[a, b)`,
	`_time:5m offset x`,
	`_stream:foo`,
	`a"b"`,
	`(a)b`,
	`a!b`,
	`a*b`,
	`1 + 1`,
	`a:in-x`,
}

// conformance: architecture-syntax-package-layering
func TestParseRejectsWhatVictoriaLogsRejects(t *testing.T) {
	for _, in := range rejectedQueries {
		if q, err := logsql.Parse(in); err == nil {
			t.Errorf("Parse(%q) = %q, want an error", in, q.String())
		}
	}
}
