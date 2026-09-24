package logql

import "testing"

// lokiDottedNameErrors are Loki v3.7.7's answers (HTTP 400 bodies) for
// queries whose first invalid token is a "." — recorded against grafana/loki
// 3.7.7 on the e2e stack with GET /loki/api/v1/query_range.
var lokiDottedNameErrors = []struct {
	query string
	want  string
}{
	{`{app=~".+"} | http.2xx="1"`, "parse error at line 1, col 19: syntax error: unexpected NUMBER"},
	{`{app=~".+"} | label_format k8s.x=app`, "parse error at line 1, col 31: syntax error: unexpected ., expecting ="},
	{`sum(sum_over_time({app=~".+"} | json | unwrap duration(http.latency) [5m]))`, "parse error at line 1, col 60: syntax error: unexpected ., expecting )"},
	{".", "parse error at line 1, col 1: syntax error: unexpected ."},
	{"{.a=\"x\"}", "parse error at line 1, col 2: syntax error: unexpected ., expecting IDENTIFIER or }"},
	{"{a.b=\"x\"}", "parse error at line 1, col 3: syntax error: unexpected ., expecting = or =~ or !~ or !="},
	{"{a=.b}", "parse error at line 1, col 4: syntax error: unexpected ., expecting STRING"},
	{"{a=\"x\" .}", "parse error at line 1, col 8: syntax error: unexpected ., expecting } or ,"},
	{"{a=\"x\", b.c!=\"y\"}", "parse error at line 1, col 10: syntax error: unexpected ., expecting = or =~ or !~ or !="},
	{"{a=\"x\", b.c=~\"y\"}", "parse error at line 1, col 10: syntax error: unexpected ., expecting = or =~ or !~ or !="},
	{"{a=\"x\".}", "parse error at line 1, col 7: syntax error: unexpected ., expecting } or ,"},
	{"{a=\"x\"} != .", "parse error at line 1, col 12: syntax error: unexpected ., expecting STRING or ip"},
	{"{a=\"x\"} !> .", "parse error at line 1, col 12: syntax error: unexpected ., expecting STRING or ip"},
	{"{a=\"x\"} !~ .", "parse error at line 1, col 12: syntax error: unexpected ., expecting STRING or ip"},
	{"{a=\"x\"} !~ \"y\" | b.c=\"d\"", "parse error at line 1, col 19: syntax error: unexpected ."},
	{"{a=\"x\"} | (.b=\"c\")", "parse error at line 1, col 12: syntax error: unexpected ., expecting IDENTIFIER or ("},
	{"{a=\"x\"} | (b=\"c\") .", "parse error at line 1, col 19: syntax error: unexpected ."},
	{"{a=\"x\"} | b != ip(\"1.1.1.1\") | c.d=\"e\"", "parse error at line 1, col 33: syntax error: unexpected ."},
	{"{a=\"x\"} | b .", "parse error at line 1, col 13: syntax error: unexpected ."},
	{"{a=\"x\"} | b > .c", "parse error at line 1, col 15: syntax error: unexpected ."},
	{"{a=\"x\"} | b > 5 .", "parse error at line 1, col 17: syntax error: unexpected ."},
	{"{a=\"x\"} | b > 5s .", "parse error at line 1, col 18: syntax error: unexpected ."},
	{"{a=\"x\"} | b.c", "parse error at line 1, col 12: syntax error: unexpected ."},
	{"{a=\"x\"} | b=.c", "parse error at line 1, col 13: syntax error: unexpected ."},
	{"{a=\"x\"} | b=\"y\" .", "parse error at line 1, col 17: syntax error: unexpected ."},
	{"{a=\"x\"} | b=\"y\" | .", "parse error at line 1, col 19: syntax error: unexpected ."},
	{"{a=\"x\"} | b=\"y\" | label_format c.d=e", "parse error at line 1, col 33: syntax error: unexpected ., expecting ="},
	{"{a=\"x\"} | b=\"y\" and .c=\"d\"", "parse error at line 1, col 21: syntax error: unexpected ., expecting IDENTIFIER or ("},
	{"{a=\"x\"} | b=\"y\" and c.d=\"e\"", "parse error at line 1, col 22: syntax error: unexpected ."},
	{"{a=\"x\"} | b=\"y\" c.d=\"e\"", "parse error at line 1, col 18: syntax error: unexpected ."},
	{"{a=\"x\"} | b=\"y\" or .c=\"d\"", "parse error at line 1, col 20: syntax error: unexpected ., expecting IDENTIFIER or ("},
	{"{a=\"x\"} | b=\"y\", .c=\"d\"", "parse error at line 1, col 18: syntax error: unexpected ., expecting IDENTIFIER or ("},
	{"{a=\"x\"} | b=\"y\", c.d=\"e\"", "parse error at line 1, col 19: syntax error: unexpected ."},
	{"{a=\"x\"} | b=ip(.)", "parse error at line 1, col 16: syntax error: unexpected ., expecting STRING"},
	{"{a=\"x\"} | decolorize .", "parse error at line 1, col 22: syntax error: unexpected ."},
	{"{a=\"x\"} | drop .a", "parse error at line 1, col 16: syntax error: unexpected ., expecting IDENTIFIER"},
	{"{a=\"x\"} | drop a=\"y\", .b", "parse error at line 1, col 23: syntax error: unexpected ., expecting IDENTIFIER"},
	{"{a=\"x\"} | drop b.c=~\"d\"", "parse error at line 1, col 17: syntax error: unexpected ."},
	{"{a=\"x\"} | json | .", "parse error at line 1, col 18: syntax error: unexpected ."},
	{"{a=\"x\"} | json a, .b", "parse error at line 1, col 19: syntax error: unexpected ., expecting IDENTIFIER"},
	{"{a=\"x\"} | json a.b | c", "parse error at line 1, col 17: syntax error: unexpected ."},
	{"{a=\"x\"} | json a=.b", "parse error at line 1, col 18: syntax error: unexpected ., expecting STRING"},
	{"{a=\"x\"} | json a=\"b\" .", "parse error at line 1, col 22: syntax error: unexpected ."},
	{"{a=\"x\"} | keep a .", "parse error at line 1, col 18: syntax error: unexpected ."},
	{"{a=\"x\"} | keep a, .b", "parse error at line 1, col 19: syntax error: unexpected ., expecting IDENTIFIER"},
	{"{a=\"x\"} | keep b.c=\"d\"", "parse error at line 1, col 17: syntax error: unexpected ."},
	{"{a=\"x\"} | label_format a=\"x\".", "parse error at line 1, col 29: syntax error: unexpected ."},
	{"{a=\"x\"} | label_format a=b .", "parse error at line 1, col 28: syntax error: unexpected ."},
	{"{a=\"x\"} | label_format a=b, .c=d", "parse error at line 1, col 29: syntax error: unexpected ., expecting IDENTIFIER"},
	{"{a=\"x\"} | line_format .", "parse error at line 1, col 23: syntax error: unexpected ., expecting STRING"},
	{"{a=\"x\"} | line_format \"x\" .", "parse error at line 1, col 27: syntax error: unexpected ."},
	{"{a=\"x\"} | logfmt --strict .a", "parse error at line 1, col 27: syntax error: unexpected ."},
	{"{a=\"x\"} | logfmt .a", "parse error at line 1, col 18: syntax error: unexpected ."},
	{"{a=\"x\"} | logfmt a, .b", "parse error at line 1, col 21: syntax error: unexpected ., expecting IDENTIFIER"},
	{"{a=\"x\"} | pattern .", "parse error at line 1, col 19: syntax error: unexpected ., expecting STRING"},
	{"{a=\"x\"} | regexp .", "parse error at line 1, col 18: syntax error: unexpected ., expecting STRING"},
	{"{a=\"x\"} |= .", "parse error at line 1, col 12: syntax error: unexpected ., expecting STRING or ip"},
	{"{a=\"x\"} |= \"y\" .", "parse error at line 1, col 16: syntax error: unexpected ."},
	{"{a=\"x\"} |= \"y\" or .", "parse error at line 1, col 19: syntax error: unexpected ., expecting STRING or ip"},
	{"{a=\"x\"} |= ip(.)", "parse error at line 1, col 15: syntax error: unexpected ., expecting STRING"},
	{"{a=\"x\"} |> .", "parse error at line 1, col 12: syntax error: unexpected ., expecting STRING or ip"},
	{"{a=\"x\"} |~ .", "parse error at line 1, col 12: syntax error: unexpected ., expecting STRING or ip"},
	{"{env=\"production\", .a=\"x\"}", "parse error at line 1, col 20: syntax error: unexpected ., expecting IDENTIFIER"},
	{"{env=\"production\", a.b=\"x\"}", "parse error at line 1, col 21: syntax error: unexpected ., expecting = or =~ or !~ or !="},
	{"{env=\"production\"} | .foo=\"x\"", "parse error at line 1, col 22: syntax error: unexpected ."},
	{"{env=\"production\"} | (a.b=\"y\")", "parse error at line 1, col 24: syntax error: unexpected ."},
	{"{env=\"production\"} | (a=\"x\" or b.c=\"y\")", "parse error at line 1, col 33: syntax error: unexpected ."},
	{"{env=\"production\"} | a . b=\"x\"", "parse error at line 1, col 24: syntax error: unexpected ."},
	{"{env=\"production\"} | a = \"x\" | b.c", "parse error at line 1, col 33: syntax error: unexpected ."},
	{"{env=\"production\"} | a > 5 | b.c > 5", "parse error at line 1, col 31: syntax error: unexpected ."},
	{"{env=\"production\"} | a_b=\"x\" | b.c=\"y\" | d.e=\"z\"", "parse error at line 1, col 33: syntax error: unexpected ."},
	{"{env=\"production\"} | a..b=\"x\"", "parse error at line 1, col 23: syntax error: unexpected ."},
	{"{env=\"production\"} | a.5=\"x\"", "parse error at line 1, col 23: syntax error: unexpected NUMBER"},
	{"{env=\"production\"} | a.b", "parse error at line 1, col 23: syntax error: unexpected ."},
	{"{env=\"production\"} | a.b != \"x\"", "parse error at line 1, col 23: syntax error: unexpected ."},
	{"{env=\"production\"} | a.b !~ \"x\"", "parse error at line 1, col 23: syntax error: unexpected ."},
	{"{env=\"production\"} | a.b <= 5", "parse error at line 1, col 23: syntax error: unexpected ."},
	{"{env=\"production\"} | a.b == 5", "parse error at line 1, col 23: syntax error: unexpected ."},
	{"{env=\"production\"} | a.b > 5", "parse error at line 1, col 23: syntax error: unexpected ."},
	{"{env=\"production\"} | a.b=\"x\"", "parse error at line 1, col 23: syntax error: unexpected ."},
	{"{env=\"production\"} | \u00e4.b=\"x\"", "parse error at line 1, col 23: syntax error: unexpected ."},
	{"{env=\"production\"} | a.b=\"x\" |", "parse error at line 1, col 23: syntax error: unexpected ."},
	{"{env=\"production\"} | a.b=\"x\" | unknown(", "parse error at line 1, col 23: syntax error: unexpected ."},
	{"{env=\"production\"} | a.b=~\"x\"", "parse error at line 1, col 23: syntax error: unexpected ."},
	{"{env=\"production\"} | a=\"x\" | a.b=\"x\"", "parse error at line 1, col 31: syntax error: unexpected ."},
	{"{env=\"production\"} | a=\"x\" | label_format a.b=\"{{.x}}\"", "parse error at line 1, col 44: syntax error: unexpected ., expecting ="},
	{"{env=\"production\"} | a=\"x\" and b.c=\"y\"", "parse error at line 1, col 33: syntax error: unexpected ."},
	{"{env=\"production\"} | decolorize | a.b=\"x\"", "parse error at line 1, col 36: syntax error: unexpected ."},
	{"{env=\"production\"} | drop a, b.c", "parse error at line 1, col 31: syntax error: unexpected ."},
	{"{env=\"production\"} | drop a.b", "parse error at line 1, col 28: syntax error: unexpected ."},
	{"{env=\"production\"} | drop a.b=\"x\"", "parse error at line 1, col 28: syntax error: unexpected ."},
	{"{env=\"production\"} | env=\"x\" and a.b=\"y\"", "parse error at line 1, col 35: syntax error: unexpected ."},
	{"{env=\"production\"} | env=\"x\" or a.b=\"y\"", "parse error at line 1, col 34: syntax error: unexpected ."},
	{"{env=\"production\"} | env=\"x\", a.b=\"y\"", "parse error at line 1, col 32: syntax error: unexpected ."},
	{"{env=\"production\"} | json .a", "parse error at line 1, col 27: syntax error: unexpected ."},
	{"{env=\"production\"} | json | a > 1 and b.c < 2", "parse error at line 1, col 40: syntax error: unexpected ."},
	{"{env=\"production\"} | json | a_b=\"x\" | c.d=\"y\"", "parse error at line 1, col 40: syntax error: unexpected ."},
	{"{env=\"production\"} | json | a.b = ip(\"1.2.3.4\")", "parse error at line 1, col 30: syntax error: unexpected ."},
	{"{env=\"production\"} | json | a.b > 10KB", "parse error at line 1, col 30: syntax error: unexpected ."},
	{"{env=\"production\"} | json | a.b > 1s", "parse error at line 1, col 30: syntax error: unexpected ."},
	{"{env=\"production\"} | json | a.b=\"x\"", "parse error at line 1, col 30: syntax error: unexpected ."},
	{"{env=\"production\"} | json | a.b=\"x\" | unknownstage", "parse error at line 1, col 30: syntax error: unexpected ."},
	{"{env=\"production\"} | json a.b", "parse error at line 1, col 28: syntax error: unexpected ."},
	{"{env=\"production\"} | json a.b, c", "parse error at line 1, col 28: syntax error: unexpected ."},
	{"{env=\"production\"} | json a.b=\"x\"", "parse error at line 1, col 28: syntax error: unexpected ."},
	{"{env=\"production\"} | json a.b=\"x\" | c=\"d\"", "parse error at line 1, col 28: syntax error: unexpected ."},
	{"{env=\"production\"} | json x=\"a\", a.b", "parse error at line 1, col 35: syntax error: unexpected ."},
	{"{env=\"production\"} | keep .a", "parse error at line 1, col 27: syntax error: unexpected ., expecting IDENTIFIER"},
	{"{env=\"production\"} | keep a.b", "parse error at line 1, col 28: syntax error: unexpected ."},
	{"{env=\"production\"} | keep a=\"x\", b.c", "parse error at line 1, col 35: syntax error: unexpected ."},
	{"{env=\"production\"} | keep x, a.b", "parse error at line 1, col 31: syntax error: unexpected ."},
	{"{env=\"production\"} | label_format .a=b", "parse error at line 1, col 35: syntax error: unexpected ., expecting IDENTIFIER"},
	{"{env=\"production\"} | label_format a.b=x", "parse error at line 1, col 36: syntax error: unexpected ., expecting ="},
	{"{env=\"production\"} | label_format a=.b", "parse error at line 1, col 37: syntax error: unexpected ., expecting IDENTIFIER or STRING"},
	{"{env=\"production\"} | label_format a=\"x\", b.c=\"y\"", "parse error at line 1, col 43: syntax error: unexpected ., expecting ="},
	{"{env=\"production\"} | label_format a=b, c.d=e", "parse error at line 1, col 41: syntax error: unexpected ., expecting ="},
	{"{env=\"production\"} | label_format a=b.c", "parse error at line 1, col 38: syntax error: unexpected ."},
	{"{env=\"production\"} | line_format \"{{.a}}\" | a.b=\"x\"", "parse error at line 1, col 46: syntax error: unexpected ."},
	{"{env=\"production\"} | logfmt --strict a.b", "parse error at line 1, col 39: syntax error: unexpected ."},
	{"{env=\"production\"} | logfmt a.b", "parse error at line 1, col 30: syntax error: unexpected ."},
	{"{env=\"production\"} | logfmt a=`x`, b.c", "parse error at line 1, col 37: syntax error: unexpected ."},
	{"{env=\"production\"} | pattern `<a> <b>` | a.b=\"x\"", "parse error at line 1, col 43: syntax error: unexpected ."},
	{"{env=\"production\"} | regexp \"(?P<a>.)\" | a.b=\"x\"", "parse error at line 1, col 43: syntax error: unexpected ."},
	{"{env=\"production\"} | unpack | a.b=\"x\"", "parse error at line 1, col 32: syntax error: unexpected ."},
	{"{env=\"production\"} |= \"a\" |~ \"b\" != \"c\" | a.b=\"x\"", "parse error at line 1, col 44: syntax error: unexpected ."},
	{"{env=\"production\"} |= \"x\" | a.b=\"x\"", "parse error at line 1, col 30: syntax error: unexpected ."},
	{"{env=\"production\"} |= \"x\" or \"y\" | a.b=\"c\"", "parse error at line 1, col 37: syntax error: unexpected ."},
	{"{env=\"production\"} |> \"<_>\" | a.b=\"x\"", "parse error at line 1, col 32: syntax error: unexpected ."},
	{"{env=\"production\"}.", "parse error at line 1, col 19: syntax error: unexpected ."},
	{"{env=\"production\"}\n|   a.b=\"x\"", "parse error at line 2, col 6: syntax error: unexpected ."},
	{"avg_over_time({env=\"production\"} | json | unwrap x [5m]) without (a.b)", "parse error at line 1, col 68: syntax error: unexpected ., expecting , or )"},
	{"count_over_time({a=\"x\"} | json a.b [5m])", "parse error at line 1, col 33: syntax error: unexpected ."},
	{"count_over_time({a=\"x\"}[5m] offset .)", "parse error at line 1, col 36: syntax error: unexpected ., expecting DURATION"},
	{"count_over_time({a=\"x\"}[5m]).", "parse error at line 1, col 29: syntax error: unexpected ."},
	{"count_over_time({env=\"production\"}[5m]) / on (a.b) count_over_time({env=\"production\"}[5m])", "parse error at line 1, col 48: syntax error: unexpected ., expecting , or )"},
	{"count_over_time({env=\"production\"}[5m]) by (a.b)", "parse error at line 1, col 46: syntax error: unexpected ., expecting , or )"},
	{"max by (a.b) (count_over_time({a=\"x\"}[5m]))", "parse error at line 1, col 10: syntax error: unexpected ., expecting , or )"},
	{"quantile_over_time(0.5, {env=\"production\"} | json | unwrap x [5m]) by (a.b)", "parse error at line 1, col 73: syntax error: unexpected ., expecting , or )"},
	{"rate(.)", "parse error at line 1, col 6: syntax error: unexpected ., expecting NUMBER or { or ("},
	{"rate({a=\"x\"}[5m] .)", "parse error at line 1, col 18: syntax error: unexpected ., expecting )"},
	{"rate({a=\"x\"}[5m]) .", "parse error at line 1, col 19: syntax error: unexpected ."},
	{"sort(sum by (a.b) (count_over_time({env=\"production\"}[5m])))", "parse error at line 1, col 15: syntax error: unexpected ., expecting , or )"},
	{"sum by (.a) (count_over_time({env=\"production\"}[5m]))", "parse error at line 1, col 9: syntax error: unexpected ., expecting IDENTIFIER or )"},
	{"sum by (a, .b) (count_over_time({env=\"production\"}[5m]))", "parse error at line 1, col 12: syntax error: unexpected ., expecting IDENTIFIER"},
	{"sum by (a.5) (count_over_time({env=\"production\"}[5m]))", "parse error at line 1, col 10: syntax error: unexpected NUMBER, expecting , or )"},
	{"sum by (a.b)", "parse error at line 1, col 10: syntax error: unexpected ., expecting , or )"},
	{"sum by (a.b) (count_over_time({env=\"production\"}[5m]))", "parse error at line 1, col 10: syntax error: unexpected ., expecting , or )"},
	{"sum by (a.b) (sum by (c) (count_over_time({a=\"x\"}[5m])))", "parse error at line 1, col 10: syntax error: unexpected ., expecting , or )"},
	{"sum by (a.b) (sum_over_time({env=\"production\"} | json | unwrap x [5m]))", "parse error at line 1, col 10: syntax error: unexpected ., expecting , or )"},
	{"sum by (a) .", "parse error at line 1, col 12: syntax error: unexpected ., expecting ("},
	{"sum by (a) (count_over_time({a=\"x\"}[5m])) / .", "parse error at line 1, col 45: syntax error: unexpected ."},
	{"sum by (a) (count_over_time({a=\"x\"}[5m])) / on .", "parse error at line 1, col 48: syntax error: unexpected ., expecting ("},
	{"sum by (a) (count_over_time({a=\"x\"}[5m])) / on(.a) sum by (a) (count_over_time({a=\"x\"}[5m]))", "parse error at line 1, col 48: syntax error: unexpected ., expecting IDENTIFIER or )"},
	{"sum by (a) (count_over_time({a=\"x\"}[5m])) / on(a, .b) sum by (a) (count_over_time({a=\"x\"}[5m]))", "parse error at line 1, col 51: syntax error: unexpected ., expecting IDENTIFIER"},
	{"sum by (a) (count_over_time({a=\"x\"}[5m])) / on(a) .", "parse error at line 1, col 51: syntax error: unexpected ."},
	{"sum by (a) (count_over_time({a=\"x\"}[5m])) / on(a) group_left .", "parse error at line 1, col 62: syntax error: unexpected ."},
	{"sum by (a) (count_over_time({a=\"x\"}[5m])) / on(a) group_left(.b) sum by (a) (count_over_time({a=\"x\"}[5m]))", "parse error at line 1, col 62: syntax error: unexpected ., expecting IDENTIFIER or )"},
	{"sum by (a) (count_over_time({a=\"x\"}[5m])) > bool .", "parse error at line 1, col 50: syntax error: unexpected ."},
	{"sum by (a) (count_over_time({a=\"x\"}[5m])) and on(a.b) sum by (a) (count_over_time({a=\"x\"}[5m]))", "parse error at line 1, col 51: syntax error: unexpected ., expecting , or )"},
	{"sum by (a) (count_over_time({env=\"production\"}[5m])) / ignoring (a.b) sum by (a) (count_over_time({env=\"production\"}[5m]))", "parse error at line 1, col 67: syntax error: unexpected ., expecting , or )"},
	{"sum by (a) (count_over_time({env=\"production\"}[5m])) / on (a.b) sum by (a) (count_over_time({env=\"production\"}[5m]))", "parse error at line 1, col 61: syntax error: unexpected ., expecting , or )"},
	{"sum by (a) (count_over_time({env=\"production\"}[5m])) / on (a) group_left (a.b) sum by (a) (count_over_time({env=\"production\"}[5m]))", "parse error at line 1, col 76: syntax error: unexpected ., expecting , or )"},
	{"sum by (a) (count_over_time({env=\"production\"}[5m])) / on (a) group_right (a.b) sum by (a) (count_over_time({env=\"production\"}[5m]))", "parse error at line 1, col 77: syntax error: unexpected ., expecting , or )"},
	{"sum by (a) (count_over_time({env=\"production\"}[5m])) > bool on (a.b) sum by (a) (count_over_time({env=\"production\"}[5m]))", "parse error at line 1, col 66: syntax error: unexpected ., expecting , or )"},
	{"sum by (x, a.b) (count_over_time({env=\"production\"}[5m]))", "parse error at line 1, col 13: syntax error: unexpected ., expecting , or )"},
	{"sum without (.a) (count_over_time({a=\"x\"}[5m]))", "parse error at line 1, col 14: syntax error: unexpected ., expecting IDENTIFIER or )"},
	{"sum without (a.b) (count_over_time({env=\"production\"}[5m]))", "parse error at line 1, col 15: syntax error: unexpected ., expecting , or )"},
	{"sum_over_time({env=\"production\"} | json | unwrap x [5m]) by (a.b)", "parse error at line 1, col 63: syntax error: unexpected ., expecting , or )"},
	{"sum(.)", "parse error at line 1, col 5: syntax error: unexpected ."},
	{"sum(count_over_time({env=\"production\"} | a.b=\"x\" [5m]))", "parse error at line 1, col 43: syntax error: unexpected ."},
	{"sum(count_over_time({env=\"production\"} | drop a.b [5m]))", "parse error at line 1, col 48: syntax error: unexpected ."},
	{"sum(count_over_time({env=\"production\"}[5m])) by (a.b)", "parse error at line 1, col 51: syntax error: unexpected ., expecting , or )"},
	{"sum(count_over_time({env=\"production\"}[5m])) by (a) / on() group_left(b.c) sum(count_over_time({env=\"production\"}[5m])) by (a)", "parse error at line 1, col 72: syntax error: unexpected ., expecting , or )"},
	{"sum(count_over_time({env=\"production\"}[5m])) without (a.b)", "parse error at line 1, col 56: syntax error: unexpected ., expecting , or )"},
	{"sum(rate({a=\"x\"} | unwrap a . [5m]))", "parse error at line 1, col 29: syntax error: unexpected ., expecting RANGE or |"},
	{"sum(rate({a=\"x\"} | unwrap duration_seconds(a.b) [5m]))", "parse error at line 1, col 45: syntax error: unexpected ., expecting )"},
	{"sum(rate({a=\"x\"} | unwrap duration(.a) [5m]))", "parse error at line 1, col 36: syntax error: unexpected ., expecting IDENTIFIER"},
	{"sum(rate({a=\"x\"}[5m])) by (a) .", "parse error at line 1, col 31: syntax error: unexpected ."},
	{"sum(rate({env=\"production\"} | json | unwrap a | b.c=\"y\" [5m]))", "parse error at line 1, col 50: syntax error: unexpected ."},
	{"sum(rate({env=\"production\"} | json | unwrap a.b | x=\"y\" [5m]))", "parse error at line 1, col 46: syntax error: unexpected ., expecting RANGE or |"},
	{"sum(rate({env=\"production\"} | unwrap .a [5m]))", "parse error at line 1, col 38: syntax error: unexpected ., expecting IDENTIFIER or BYTES_CONV or DURATION_CONV or DURATION_SECONDS_CONV"},
	{"sum(rate({env=\"production\"} | unwrap a.b [5m]))", "parse error at line 1, col 39: syntax error: unexpected ., expecting RANGE or |"},
	{"sum(rate({env=\"production\"} | unwrap bytes(.a) [5m]))", "parse error at line 1, col 44: syntax error: unexpected ., expecting IDENTIFIER"},
	{"sum(rate({env=\"production\"} | unwrap bytes(a.b) [5m]))", "parse error at line 1, col 45: syntax error: unexpected ., expecting )"},
	{"sum(rate({env=\"production\"} | unwrap duration(a.b) [5m]))", "parse error at line 1, col 48: syntax error: unexpected ., expecting )"},
	{"sum(sum_over_time({env=\"production\"} | json | unwrap a.b [5m]))", "parse error at line 1, col 55: syntax error: unexpected ., expecting RANGE or |"},
	{"topk by (a.b) (5, count_over_time({env=\"production\"}[5m]))", "parse error at line 1, col 11: syntax error: unexpected ., expecting , or )"},
	{"topk(.)", "parse error at line 1, col 6: syntax error: unexpected ."},
	{"topk(5, .)", "parse error at line 1, col 9: syntax error: unexpected ."},
	{"topk(5, sum by (a.b) (count_over_time({env=\"production\"}[5m])))", "parse error at line 1, col 18: syntax error: unexpected ., expecting , or )"},
	{"vector(.)", "parse error at line 1, col 8: syntax error: unexpected ., expecting NUMBER"},
	{"vector(1) + on(a.b) vector(1)", "parse error at line 1, col 17: syntax error: unexpected ., expecting , or )"},
}

// conformance: profiles/dotted-name-parse-error
func TestDottedNameError_MatchesLoki(t *testing.T) {
	for _, tc := range lokiDottedNameErrors {
		if got := DottedNameError(tc.query); got != tc.want {
			t.Errorf("DottedNameError(%q)\n got  %q\n want %q", tc.query, got, tc.want)
		}
	}
}

// Loki accepts these: every dot is inside a string, a number, a duration, a
// byte size, a [range] or a comment.
//
// conformance: profiles/dotted-name-parse-error
func TestDottedNameError_AcceptsDotsLokiAccepts(t *testing.T) {
	for _, q := range []string{
		`{env="production"} | json a="b.c"`,
		`{env="production"} | a="b.c"`,
		`{env="production"} | pattern "<a>.<b>"`,
		"{env=\"production\"} | label_format a=`{{.b.c}}`",
		`{env="production"} | line_format "{{.a}}"`,
		`{env="production"} | json | line_format "{{.a.b}}"`,
		`{env="production"} |= ip("1.2.3.4")`,
		`{env="production"} | a > 1.5`,
		`{env="production"} | a > 1.5s`,
		`{env="production"} | a > 1.5KB`,
		`{env="production"} | a > .5`,
		`{env="production"} # a.b`,
		"{env=\"production\"} // a.b",
		"{env=\"production\"} /* a.b */ | x=\"y\"",
		`sum(count_over_time({env="production"}[5m] offset 1.5h))`,
		`label_replace(count_over_time({app="x"}[5m]), "a.b", "$1", "x.y", "(.*)")`,
		`{env="production"} | k8s_namespace_name=` + "`monitoring`",
		`sum by (k8s_namespace_name) (count_over_time({env="production"} | json [5m]))`,
		`quantile_over_time(0.99, {app="x"} | unwrap latency [5m])`,
		`vector(0.5)`,
		`{env="production"}`,
	} {
		if got := DottedNameError(q); got != "" {
			t.Errorf("DottedNameError(%q) = %q, want no error", q, got)
		}
	}
}

// A query that is already invalid before its dotted name gets Loki's error
// for that earlier token from Loki, and the dotted-name error from the proxy
// (both are 400 parse errors). Pinned so a change in either direction is
// deliberate.
//
// conformance: profiles/dotted-name-parse-error
func TestDottedNameError_EarlierSyntaxErrorReportsTheDot(t *testing.T) {
	for _, tc := range []struct{ query, lokiWant, got string }{
		{`{a="x"} | unpack a.b`, "parse error at line 1, col 18: syntax error: unexpected IDENTIFIER", "parse error at line 1, col 19: syntax error: unexpected ."},
		{`{env="production"} | unwrap a.b`, "parse error at line 1, col 22: syntax error: unexpected unwrap", "parse error at line 1, col 30: syntax error: unexpected ., expecting RANGE or |"},
	} {
		if got := DottedNameError(tc.query); got != tc.got {
			t.Errorf("DottedNameError(%q) = %q, want %q (Loki: %q)", tc.query, got, tc.got, tc.lokiWant)
		}
	}
}

// DottedNameError runs on untrusted query text before any other parsing, so
// it must never panic, whatever the bytes.
func FuzzDottedNameError(f *testing.F) {
	for _, tc := range lokiDottedNameErrors {
		f.Add(tc.query)
	}
	for _, seed := range []string{"", "\"", "`", "'", "[", "/*", "#", ".\n.", "{a=\"\\", "a.\xff", "sum by (", "| unwrap bytes(", "1.2.3.4.", "\x00.", "((((((((((.", "))))."} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, query string) {
		_ = DottedNameError(query)
	})
}

func BenchmarkDottedNameError(b *testing.B) {
	q := `sum by (level) (count_over_time({env="production"} |= "GET /api/v1.2" | json | pipeline=` + "`metrics/prometheus`" + ` | k8s_namespace_name=` + "`monitoring`" + ` [5m]))`
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if DottedNameError(q) != "" {
			b.Fatal("unexpected error")
		}
	}
}
