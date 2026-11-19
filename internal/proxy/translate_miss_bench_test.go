package proxy

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkTranslateQueryMiss is the translate-on-miss path of every request: each iteration translates a query the
// translation cache has not seen, so it runs the string translator and stores the answer.
func BenchmarkTranslateQueryMiss(b *testing.B) {
	p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(time.Minute, 1000), LogLevel: "error", LabelStyle: LabelStyleUnderscores})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.translateQueryOpts(context.Background(), `{app="api", namespace="prod"} |= "error `+strconv.Itoa(i)+`" != "debug"`, false); err != nil {
			b.Fatal(err)
		}
	}
}

// translateMissShapes are the query shapes of BenchmarkTranslateQueryMissShapes: the constructs the typed lowering
// serves (log queries with line, pattern and label filters and no parser) and the ones it does not, each with a
// text that differs per iteration so the translation cache misses.
var translateMissShapes = []struct {
	name  string
	query func(i string) string
}{
	{"native line filters", func(i string) string { return `{app="api", namespace="prod"} |= "error ` + i + `" != "debug"` }},
	{"native pattern filter", func(i string) string { return `{app="api"} |> "<_> GET <path> ` + i + `" |= "200"` }},
	{"native label filters", func(i string) string { return `{app="api"} |= "x" | status >= 500 | path =~ "/api/` + i + `.*"` }},
	{"derived level filter", func(i string) string { return `{service_name="api"} |= "error ` + i + `" | detected_level="error"` }},
	// The log stage families the lowering serves (QueryPlanFlippedLogStages), and the log shapes their screen turns away.
	{"level filter after parser (turned away)", func(i string) string {
		return `{app="api"} |= "error ` + i + `" | logfmt | detected_level="error"`
	}},
	{"service_name filter (turned away)", func(i string) string { return `{app="api"} |= "error ` + i + `" | service_name="api"` }},
	// The translator-only known divergences the plan serves: a marker byte in a label filter value behind a parser, a
	// reserved field name under a metric query; and a backslash escape the screens read through.
	{"marker bytes after parser", func(i string) string { return `{app="api"} |= "e` + i + `" | o="\x02\x00\x02" | json | k="v"` }},
	{"reserved field metric", func(i string) string { return `sum(count_over_time({app="api"} |= "e` + i + `" | _time="10" [5m]))` }},
	{"escaped regexp metric", func(i string) string { return `sum(rate({app="api"} |~ "\\d+ ` + i + `" [5m]))` }},
	{"pattern parser", func(i string) string {
		return `{app="api"} |= "` + i + `" | pattern "<_> <m> <_>" | m="GET"`
	}},
	{"pattern parser, a new pattern each time", func(i string) string {
		return `{app="api"} |= "x" | pattern "<_> <m> ` + i + `<_>" | m="GET"`
	}},
	{"regexp parser", func(i string) string {
		return "{app=\"api\"} |= \"" + i + "\" | regexp `(?P<m>[A-Z]+) ` | m=\"GET\""
	}},
	{"regexp parser, a new regexp each time", func(i string) string {
		return "{app=\"api\"} |= \"x\" | regexp `(?P<m>[A-Z]+) " + i + "` | m=\"GET\""
	}},
	{"unspaced pattern parser (served)", func(i string) string {
		return `{app="api"} |= "` + i + `" |pattern"<_> <m> <_>" | m="GET"`
	}},
	{"label_format", func(i string) string { return `{app="api"} |= "` + i + `" | json | label_format x="{{.a}}"` }},
	{"line_format", func(i string) string { return `{app="api"} |= "` + i + `" | json | line_format "{{.a}} {{.b}}"` }},
	{"line_format, a new template each time", func(i string) string {
		return `{app="api"} |= "x" | json | line_format "{{.a}} ` + i + `"`
	}},
	{"drop keep", func(i string) string { return `{app="api"} |= "` + i + `" | json | drop a, b | keep c, d` }},
	{"parser logfmt", func(i string) string { return `{app="api"} | logfmt | status >= 500 | path = "/api/` + i + `"` }},
	{"parser json", func(i string) string { return `{app="api"} |= "error ` + i + `" | json | status="500"` }},
	{"parser json params", func(i string) string { return `{app="api"} |= "` + i + `" | json m="a.b", n="c" | m="x"` }},
	{"parser json numeric equality (turned away)", func(i string) string {
		return `{app="api"} |= "` + i + `" | json | status == 500`
	}},
	{"unpack (turned away)", func(i string) string { return `{app="api"} |= "` + i + `" | unpack | x="y"` }},
	{"decolorize (turned away)", func(i string) string { return `{app="api"} |= "` + i + `" | decolorize` }},
	{"metric rate", func(i string) string { return `sum by (level) (rate({app="api"} |= "error ` + i + `" [5m]))` }},
	{"metric unwrap", func(i string) string { return `sum(rate({app="api"} | json | unwrap bytes [5m])) + ` + i }},
	{"metric unwrap keep", func(i string) string {
		return `sum(quantile_over_time(0.9, {app="api"} |= "x ` + i + `" | json | keep app, d | unwrap d [5m]))`
	}},
	{"metric unwrap parser identity", func(i string) string {
		return `sum by (app) (stddev_over_time({app="api"} |= "x ` + i + `" | json | unwrap d [5m]))`
	}},
	{"metric unwrap parser identity nested", func(i string) string {
		return `sum by (app) (stddev_over_time({app="api"} |= "x ` + i + `" | json | unwrap d [5m])) + sum by (app) (stddev_over_time({app="api"} | json | unwrap d [5m]))`
	}},
	{"metric unwrap parser sum", func(i string) string {
		return `sum by (app) (sum_over_time({app="api"} |= "x ` + i + `" | logfmt | unwrap d [5m]))`
	}},
	// One shape per metric construct the lowering serves, and the metric shapes it plans and leaves to the string
	// translator (delegated by the lowering, or turned away by writesTranslatorText): what a flip costs the queries it
	// does not serve.
	{"vector sum by count", func(i string) string {
		return `sum by (app) (count_over_time({app="api"} | json | status="` + i + `" [5m]))`
	}},
	{"vector unwrap quantile", func(i string) string {
		return `sum by (app) (quantile_over_time(0.9, {app="api"} |= "` + i + `" | logfmt | unwrap d [5m]))`
	}},
	{"vector without empty", func(i string) string { return `sum without () (count_over_time({app="api"} |= "` + i + `" [5m]))` }},
	{"vector stddev", func(i string) string { return `stddev by (app) (count_over_time({app="api"} |= "` + i + `" [5m]))` }},
	{"vector numeric equality", func(i string) string {
		return `sum(count_over_time({app="api"} |= "` + i + `" | json | status == 500 [5m]))`
	}},
	{"range count", func(i string) string { return `count_over_time({app="api"} |= "error ` + i + `" [5m])` }},
	{"range unwrap", func(i string) string {
		return `max_over_time({app="api"} |= "` + i + `" | json | unwrap d [5m]) by (app)`
	}},
	{"range unpack", func(i string) string { return `count_over_time({app="api"} |= "` + i + `" | unpack | x="y" [5m])` }},
	// A label filter or a drop after a parser under a label the lowering resolves under its other spellings.
	{"vector parser label filter", func(i string) string {
		return `sum by (service_name) (sum_over_time({app="api"} |= "` + i + `" | json | unwrap duration_ms | __error__="" [5m]))`
	}},
	{"vector parser label drop", func(i string) string {
		return `sum by (user_id) (count_over_time({app="api"} |= "` + i + `" | json | drop __error__ [5m]))`
	}},
	{"vector parser label filter identity", func(i string) string {
		return `sum(avg_over_time({app="api"} |= "` + i + `" | json | unwrap duration_ms | __error__="" [5m]))`
	}},
	{"vector parser label existence in rate (delegated)", func(i string) string {
		return `sum by (a_b) (rate({app="api"} |= "` + i + `" | json | a_b != "" [5m]))`
	}},
	{"vector parser label filter max of max", func(i string) string {
		return `max by (service_name) (max_over_time({app="api"} |= "` + i + `" | json | unwrap duration_ms | __error__="" [1m]))`
	}},
	{"vector parser label filter stddev identity", func(i string) string {
		return `sum(stddev_over_time({app="api"} |= "` + i + `" | json | __error__="" | unwrap duration_ms [5m]))`
	}},
	{"range parser label filter", func(i string) string {
		return `avg_over_time({app="api"} |= "` + i + `" | json | unwrap duration_ms | __error__="" [5m]) by (service_name)`
	}},
	{"binary operands", func(i string) string {
		return `sum(rate({app="api"} |= "` + i + `" [5m])) / sum(rate({app="api"}[5m]))`
	}},
	{"binary vector operand", func(i string) string { return `vector(1) + sum(rate({app="api"} |= "` + i + `" [5m]))` }},
	// Held to the binary route by the plan, translated by the string translator.
	{"binary bool", func(i string) string { return `sum(rate({app="api"} |= "` + i + `" [5m])) > bool 1` }},
	{"binary set operator", func(i string) string {
		return `sum by (app) (rate({app="api"} |= "` + i + `" [5m])) and sum by (app) (rate({app="api"}[5m]))`
	}},
	{"binary chain", func(i string) string {
		return `sum(rate({app="api"} |= "` + i + `" [5m])) / sum(rate({app="api"}[5m])) * 100`
	}},
	{"label_replace", func(i string) string {
		return `label_replace(sum by (app) (rate({app="api"} |= "` + i + `" [5m])), "x", "$1", "app", "(.*)")`
	}},
	{"topk", func(i string) string { return `topk(3, sum by (app) (rate({app="api"} |= "` + i + `" [5m])))` }},
	{"vector literal", func(i string) string { return `vector(` + i + `)` }},
	// Spellings the string translator reads otherwise than the plan, which the plan serves (no text screen turns them
	// away to the translator any more): a quantile literal, quoted text with a parenthesis under a binary operation in a
	// label_replace, an operator, a number and a vector matching modifier spelled another way, an unwrap conversion
	// with a blank.
	{"spelling quantile literal", func(i string) string {
		return `quantile_over_time(0.50, {app="api"} |= "` + i + `" | unwrap n [5m])`
	}},
	{"spelling quoted parenthesis", func(i string) string {
		return `label_replace(rate({app="api"} |= "(` + i + `" [5m]) + rate({app="b"}[5m]), "d", "$1", "app", "(.*)")`
	}},
	{"spelling binary operator", func(i string) string { return `sum(rate({app="api"} |= "` + i + `" [5m]))/2` }},
	{"spelling binary number", func(i string) string { return `sum(rate({app="api"} |= "` + i + `" [5m])) * 1e3` }},
	{"spelling vector matching", func(i string) string {
		return `sum by (app, env) (rate({app="api"} |= "` + i + `" [5m])) / on(app,env) sum by (app, env) (rate({app="b"}[5m]))`
	}},
	{"spelling unwrap conversion", func(i string) string {
		return `sum(sum_over_time({app="api"} |= "` + i + `" | unwrap bytes (n) [5m]))`
	}},
}

// benchTranslateMissShapes runs every shape of translateMissShapes on a proxy built from cfg.
func benchTranslateMissShapes(b *testing.B, cfg Config) { benchTranslateMissShapesWith(b, cfg, false) }

// benchTranslateMissShapesWith is benchTranslateMissShapes; validated runs the request's validation of each query
// (cachedQueryValidation) before its translation, as the handlers do, with the validation cache full, as on a proxy
// that has run a while: each query is parsed by the validation.
func benchTranslateMissShapesWith(b *testing.B, cfg Config, validated bool) {
	if validated {
		size := validationCacheSize.Load()
		validationCacheSize.Store(validationCacheMaxSize)
		b.Cleanup(func() { validationCacheSize.Store(size) })
	}
	// The plan memo full, as on a proxy that has run a while (the handlers fill it): a query's parse kept there
	// replaces an older one, and the memo's memory does not grow during the run.
	fillPlanMemo(b)
	cfg.BackendURL, cfg.Cache, cfg.LogLevel, cfg.LabelStyle = "http://unused", cache.New(time.Minute, 1000), "error", LabelStyleUnderscores
	for _, shape := range translateMissShapes {
		b.Run(shape.name, func(b *testing.B) {
			p, err := New(cfg)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			ctx := context.Background()
			if validated && cachedQueryValidation(shape.query("0")).msg != "" {
				b.Skip("the validation rejects the shape, so no request translates it")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				q := shape.query(strconv.Itoa(i))
				if validated && cachedQueryValidation(q).msg != "" {
					b.Fatalf("%q: rejected by the validation", q)
				}
				if _, err := p.translateQueryOpts(ctx, q, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkTranslateQueryMissShapes is the translate-on-miss path of every request, per query shape: what a
// translation costs the request that finds the translation cache empty. Run on a base commit and a branch with
// benchstat; BenchmarkTranslateQueryMissShapesTranslator is the same code with the typed lowering off.
func BenchmarkTranslateQueryMissShapes(b *testing.B) { benchTranslateMissShapes(b, Config{}) }

// BenchmarkTranslateQueryMissShapesValidated is BenchmarkTranslateQueryMissShapes with the request's validation in
// front of each translation: what a request whose query no cache holds costs, its parses included.
func BenchmarkTranslateQueryMissShapesValidated(b *testing.B) {
	benchTranslateMissShapesWith(b, Config{}, true)
}

// fillPlanMemo fills both generations of the plan memo with parses of queries the benchmarks never send.
func fillPlanMemo(b *testing.B) {
	b.Helper()
	for i := 0; i < 2*planMemoLimit; i++ {
		q := `{app="memo-fill-` + strconv.Itoa(i) + `"} |= "x" | json | status="500"`
		expr, err := parseLogQLExpr(context.Background(), q)
		if err != nil {
			b.Fatal(err)
		}
		planMemoStore(q, &planMemoEntry{expr: expr})
	}
}
