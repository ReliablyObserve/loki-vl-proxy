package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// internalWording are words no client answer may carry: the translator's and
// the proxy's own vocabulary, LogsQL and the backend's name.
var internalWording = []string{
	"loki-vl-proxy", "translator", "translatable", "LogsQL", "VictoriaLogs", "not a valid LogQL parser",
	"requires a range metric", "requires `| unwrap", "unsupported metric query", "unsupported aggregation",
	"unsupported instant", "invalid range metric query", "uniform offset", "distinct offsets", "translation",
	"invalid range selector", "invalid start timestamp", "invalid end timestamp", "invalid step",
}

// errRefusalAnswer is what a client reads.
type errRefusalAnswer struct {
	status      int
	contentType string
	nosniff     string
	body        string
}

func getRefusalAnswer(t *testing.T, addr, route string, params url.Values) errRefusalAnswer {
	t.Helper()
	u := url.URL{Scheme: "http", Host: addr, Path: "/loki/api/v1/" + route, RawQuery: params.Encode()}
	resp, err := http.Get(u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return errRefusalAnswer{resp.StatusCode, resp.Header.Get("Content-Type"), resp.Header.Get("X-Content-Type-Options"), string(raw)}
}

func refusalParams(route, query string) url.Values {
	now := time.Now().Unix()
	v := url.Values{"query": {query}}
	if route == "query_range" {
		v.Set("start", fmt.Sprint(now-3600))
		v.Set("end", fmt.Sprint(now))
		v.Set("step", "60")
	} else {
		v.Set("time", fmt.Sprint(now))
	}
	return v
}

const (
	lokiInternalText      = "Internal Server Error"
	lokiUnplacedSyntax    = "parse error : syntax error: unexpected $end"
	lokiQuantileParameter = "parse error : parameter required for operation quantile_over_time"
)

// TestQueryRefusalAnswersAreLokisNotTheTranslators sends queries that pass the
// proxy's validation and that the translation, the plan or the evaluation then
// refuses, and holds the answer to what Loki answers for the same request: the
// text and status of Loki's rejection where Loki rejects the query and the
// proxy can build the message, Loki's position-free syntax error where it
// rejects it with a position only the parser has, and Loki's fixed 500 where
// Loki accepts a query the proxy cannot serve yet. No answer carries the
// translator's or the proxy's own wording.
// conformance: semantics/error-layer-internal-go-text, semantics/unknown-pipeline-stage-error-text, semantics/vector-aggregation-over-scalar-rejected, semantics/translator-reads-only-space-between-tokens, semantics/quantile-over-time-parameter-required, semantics/offset-different-per-range-vector, semantics/zero-range-metric, semantics/parse-error-text-parity
func TestQueryRefusalAnswersAreLokisNotTheTranslators(t *testing.T) {
	addr := newLokiErrorReplayProxy(t)
	const textPlain = "text/plain; charset=utf-8"
	cases := []struct {
		name   string
		route  string
		query  string
		status int
		body   string
	}{
		// Loki accepts these: its fixed 500 until the plan serves them.
		{"vector aggregation over a literal", "query_range", `sum(1)`, 500, lokiInternalText},
		{"vector aggregation over a literal, instant", "query", `sum(1)`, 500, lokiInternalText},
		{"vector aggregation over vector()", "query_range", `sum(vector(1))`, 500, lokiInternalText},
		{"count over a float literal", "query_range", `count(1.5) by ()`, 500, lokiInternalText},
		{"unwrap after a tab", "query_range", "quantile_over_time(0.5, {a=\"b\"} |\tunwrap c [5m]) by (d)", 500, lokiInternalText},
		{"zero range", "query_range", `rate({a="b"}[0s])`, 500, lokiInternalText},
		{"distinct offsets", "query_range", `rate({a="b"}[5m] offset 1h) / rate({a="b"}[5m] offset 2h)`, 500, lokiInternalText},
		{"distinct offsets, instant", "query", `rate({a="b"}[5m] offset 1h) / rate({a="b"}[5m] offset 2h)`, 500, lokiInternalText},
		{"mixed offsets under topk", "query_range", `topk(1, rate({a="b"}[5m] offset 1h) + rate({a="b"}[5m]))`, 500, lokiInternalText},
		{"mixed offsets under topk, instant", "query", `topk(1, rate({a="b"}[5m] offset 1h) + rate({a="b"}[5m]))`, 500, lokiInternalText},
		// Loki rejects these with a message the proxy builds from typed facts.
		{"quantile without its parameter", "query_range", `quantile_over_time({a="b"} | unwrap c [5m])`, 400, lokiQuantileParameter},
		{"quantile without its parameter, instant", "query", `quantile_over_time({a="b"} | unwrap c [5m])`, 400, lokiQuantileParameter},
		{"label_replace regex", "query_range", `label_replace(rate({a="b"}[5m]), "x", "$1", "a", "(")`, 400,
			"parse error : invalid regex in label_replace: error parsing regexp: missing closing ): `^(?:()$`"},
		{"range too large for a duration", "query_range", `rate({a="b"}[9999999999h])`, 400, "parse error : duration out of range"},
		// Loki rejects these with a position only the parser has.
		{"unknown pipeline stage", "query_range", `{a="b"} | foo`, 400, lokiUnplacedSyntax},
		{"unknown pipeline stage, instant", "query", `{a="b"} | foo`, 400, lokiUnplacedSyntax},
		{"unknown pipeline stage then another", "query_range", `{a="b"} | foo | a="b"`, 400, lokiUnplacedSyntax},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := getRefusalAnswer(t, addr, tc.route, refusalParams(tc.route, tc.query))
			if got.status != tc.status || strings.TrimSuffix(got.body, "\n") != tc.body {
				t.Fatalf("%s %q: got %d %q, want %d %q", tc.route, tc.query, got.status, got.body, tc.status, tc.body)
			}
			if got.contentType != textPlain || got.nosniff != "nosniff" {
				t.Errorf("headers: Content-Type %q, X-Content-Type-Options %q, want %q and nosniff", got.contentType, got.nosniff, textPlain)
			}
			assertNoInternalWording(t, got.body)
		})
	}
}

func assertNoInternalWording(t *testing.T, text string) {
	t.Helper()
	for _, word := range internalWording {
		if strings.Contains(text, word) {
			t.Errorf("answer %q carries internal wording %q", text, word)
		}
	}
}

// TestQueryRefusalClassification: queryRefusal reads the class of a failure
// from its type, never from its text.
// conformance: semantics/error-layer-internal-go-text
func TestQueryRefusalClassification(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		status    int
		text      string
		construct string
	}{
		{"unsupported outer aggregation", &translator.UnsupportedError{Msg: "sum() requires a range metric inside (e.g. rate({...}[5m]))", Func: "sum"}, 500, lokiInternalText, "sum"},
		{"unsupported unwrap", &translator.UnsupportedError{Msg: "quantile_over_time requires `| unwrap <field>` for range aggregation", Func: "quantile_over_time"}, 500, lokiInternalText, "quantile_over_time"},
		{"wrapped unsupported", fmt.Errorf("translation: %w", &translator.UnsupportedError{Msg: "x", Func: "avg"}), 500, lokiInternalText, "avg"},
		{"count_values is no LogQL function", &translator.UnsupportedError{Msg: "count_values is not translatable to LogsQL", Func: "count_values"}, 400, lokiUnplacedSyntax, ""},
		{"parse error", &translator.ParseError{Msg: "unmatched '{' in stream selector", Pos: -1}, 400, lokiUnplacedSyntax, ""},
		{"untyped translator text", errors.New(`unknown pipeline stage "foo" — not a valid LogQL parser or label filter`), 400, lokiUnplacedSyntax, ""},
		{"already typed", lokierr.StepNotPositive(), 400, "zero or negative query resolution step widths are not accepted. Try a positive integer", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := queryRefusal(tc.err)
			if got.Status() != tc.status || got.Text() != tc.text {
				t.Fatalf("got %d %q, want %d %q", got.Status(), got.Text(), tc.status, tc.text)
			}
			if construct := constructOf(got.Cause()); construct != tc.construct {
				t.Errorf("construct %q, want %q", construct, tc.construct)
			}
			assertNoInternalWording(t, got.Text())
			if tc.status == 500 && got.Cause() == nil {
				t.Error("a 500 keeps the internal reason as its cause, for the log line")
			}
		})
	}
}

// TestBareMetricRefusal: only a quantile_over_time without its quantile is a
// message Loki gives; any other bare function is a construct not served yet.
// conformance: semantics/quantile-over-time-parameter-required
func TestBareMetricRefusal(t *testing.T) {
	cases := []struct {
		query  string
		status int
		text   string
	}{
		{`quantile_over_time({a="b"} | unwrap c [5m])`, 400, lokiQuantileParameter},
		{`quantile_over_time(0.5, {a="b"} | unwrap c [5m])`, 500, lokiInternalText},
		{`rate({a="b"}[5m])`, 500, lokiInternalText},
	}
	for _, tc := range cases {
		expr, err := logqlpkg.Parse(tc.query)
		if err != nil {
			t.Fatalf("%q: %v", tc.query, err)
		}
		got := bareMetricRefusal(expr)
		if got.Status() != tc.status || got.Text() != tc.text {
			t.Errorf("%q: got %d %q, want %d %q", tc.query, got.Status(), got.Text(), tc.status, tc.text)
		}
	}
	if got := bareMetricRefusal(nil); got.Status() != 500 || got.Text() != lokiInternalText {
		t.Errorf("no expression: got %d %q", got.Status(), got.Text())
	}
}

// TestRangeWindowRefusal: a zero range is a query Loki accepts; a range too
// large for a duration is one it rejects.
// conformance: semantics/zero-range-metric, semantics/range-duration-grammar-wider-than-loki
func TestRangeWindowRefusal(t *testing.T) {
	cases := []struct {
		window string
		zero   bool
	}{
		{"0s", true}, {"0", true}, {"0m0s", true}, {"00h", true}, {"0ms", true},
		{"", false}, {"9999999999h", false}, {"1h", false}, {"0.5s", false}, {"10s", false}, {"abc", false},
	}
	for _, tc := range cases {
		got := rangeWindowRefusal(tc.window)
		if tc.zero {
			if got.Status() != 500 || got.Text() != lokiInternalText || constructOf(got.Cause()) != "zero-range" {
				t.Errorf("%q: got %d %q (%q), want the 500 of a zero range", tc.window, got.Status(), got.Text(), constructOf(got.Cause()))
			}
			continue
		}
		if got.Status() != 400 || got.Text() != "parse error : duration out of range" {
			t.Errorf("%q: got %d %q, want Loki's duration out of range", tc.window, got.Status(), got.Text())
		}
	}
}

// TestTimestampParamError renders Loki's text from the same strconv call.
// conformance: semantics/error-request-validation-parity
func TestTimestampParamError(t *testing.T) {
	cases := []struct{ name, raw, text string }{
		{"start", "abc", `could not parse 'start' parameter: strconv.ParseInt: parsing "abc": invalid syntax`},
		{"end", "now", `could not parse 'end' parameter: strconv.ParseInt: parsing "now": invalid syntax`},
		{"start", "99999999999999999999", `could not parse 'start' parameter: strconv.ParseInt: parsing "99999999999999999999": value out of range`},
		{"end", "2026-13-45T00:00:00Z", `could not parse 'end' parameter: strconv.ParseInt: parsing "2026-13-45T00:00:00Z": invalid syntax`},
	}
	for _, tc := range cases {
		got := timestampParamError(tc.name, tc.raw)
		if got.Status() != 400 || got.Text() != tc.text {
			t.Errorf("%s=%q: got %d %q, want 400 %q", tc.name, tc.raw, got.Status(), got.Text(), tc.text)
		}
	}
	// A value strconv reads fine is no failure of the parameter: not Loki's text.
	if got := timestampParamError("start", "123"); got.Status() != 500 || got.Text() != lokiInternalText {
		t.Errorf("a readable number: got %d %q", got.Status(), got.Text())
	}
}

// TestStepParamError: Loki's two texts for a step the proxy cannot read.
// conformance: semantics/error-request-validation-parity
func TestStepParamError(t *testing.T) {
	const notPositive = "zero or negative query resolution step widths are not accepted. Try a positive integer"
	cases := []struct{ raw, text string }{
		{"0", notPositive}, {"-5", notPositive}, {"0s", notPositive}, {"-1m", notPositive},
		{"abc", `cannot parse "abc" to a valid duration`}, {"5xyz", `cannot parse "5xyz" to a valid duration`},
	}
	for _, tc := range cases {
		got := stepParamError(tc.raw)
		if got.Status() != 400 || got.Text() != tc.text {
			t.Errorf("step=%q: got %d %q, want 400 %q", tc.raw, got.Status(), got.Text(), tc.text)
		}
	}
}

// TestOffsetErrorsAreTyped: extractLogQLOffset reports mixed and distinct
// offsets as typed failures, so no caller reads their text to decide.
// conformance: semantics/offset-different-per-range-vector
func TestOffsetErrorsAreTyped(t *testing.T) {
	_, _, err := extractLogQLOffset(`rate({a="b"}[5m] offset 1h) + rate({a="b"}[5m])`)
	if err == nil || !isMixedOffsetError(err) {
		t.Fatalf("mixed offsets: %v, mixed %v", err, isMixedOffsetError(err))
	}
	if got := offsetRefusal(err); got.Status() != 500 || constructOf(got.Cause()) != "mixed-offsets" {
		t.Errorf("mixed: got %d, construct %q", got.Status(), constructOf(got.Cause()))
	}
	_, _, err = extractLogQLOffset(`rate({a="b"}[5m] offset 1h) + rate({a="b"}[5m] offset 2h)`)
	if err == nil || isMixedOffsetError(err) {
		t.Fatalf("distinct offsets: %v, mixed %v", err, isMixedOffsetError(err))
	}
	if got := offsetRefusal(err); got.Status() != 500 || constructOf(got.Cause()) != "distinct-offsets" {
		t.Errorf("distinct: got %d, construct %q", got.Status(), constructOf(got.Cause()))
	}
	assertNoInternalWording(t, offsetRefusal(err).Text())
}

// TestWithoutUnwrapAndLabelReplaceErrors: the texts the proxy builds from
// typed inputs are Loki's byte for byte.
// conformance: semantics/error-layer-internal-go-text
func TestWithoutUnwrapAndLabelReplaceErrors(t *testing.T) {
	if got := withoutUnwrapError("sum_over_time"); got.Status() != 400 || got.Text() != "parse error : invalid aggregation sum_over_time without unwrap" {
		t.Errorf("without unwrap: %d %q", got.Status(), got.Text())
	}
	err := errors.New("error parsing regexp: missing closing ): `^(?:()$`")
	if got := labelReplaceRegexError(err); got.Status() != 400 || got.Text() != "parse error : invalid regex in label_replace: error parsing regexp: missing closing ): `^(?:()$`" || got.Cause() != err {
		t.Errorf("label_replace regex: %d %q", got.Status(), got.Text())
	}
}

// TestNotServedKeepsTheReasonOutOfTheAnswer: the construct and the failure
// are in the error's cause (the log line), and the text a client reads is
// Loki's fixed 500.
// conformance: semantics/error-layer-internal-go-text
func TestNotServedKeepsTheReasonOutOfTheAnswer(t *testing.T) {
	cause := errors.New("sum() requires a range metric inside")
	got := notServed("vector-aggregation-over-scalar", cause)
	if got.Status() != 500 || got.Text() != lokiInternalText || got.Kind() != lokierr.KindInternal {
		t.Fatalf("got %d %q kind %v", got.Status(), got.Text(), got.Kind())
	}
	if constructOf(got) != "vector-aggregation-over-scalar" || !errors.Is(got, cause) {
		t.Errorf("construct %q, cause kept %v", constructOf(got), errors.Is(got, cause))
	}
	if !strings.Contains(got.Cause().Error(), "sum() requires") {
		t.Errorf("cause %q lost the reason", got.Cause())
	}
}

// FuzzRefusalErrors: the constructors that read a raw request value or a
// range literal answer Loki's 400 or the fixed 500 for any input, terminate,
// and never carry the proxy's own wording.
// conformance: semantics/error-layer-internal-go-text
func FuzzRefusalErrors(f *testing.F) {
	for _, seed := range []string{"", "0", "0s", "-1", "abc", "1e3", "9999999999h", "0m0s", "5xyz", "2026-13-45T00:00:00Z", "99999999999999999999", " 5", "5 ", "\x00", "0.5s", "-0"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		for _, got := range []*lokierr.Error{
			rangeWindowRefusal(raw), timestampParamError("start", raw), timestampParamError("end", raw),
			stepParamError(raw), instantTimeError(raw),
		} {
			if s := got.Status(); s != 400 && s != 500 {
				t.Fatalf("%q: status %d", raw, s)
			}
			if s := got.Status(); s == 500 && got.Text() != lokiInternalText {
				t.Fatalf("%q: a 500 answers %q", raw, got.Text())
			}
			for _, word := range internalWording {
				if strings.Contains(got.Text(), word) && !strings.Contains(raw, word) {
					t.Fatalf("%q: answer %q carries internal wording %q", raw, got.Text(), word)
				}
			}
		}
		_ = isZeroDuration(raw)
	})
}

// TestNotServedNamesTheConstructInTheLogLineOnly: the request error log line
// names the construct and the reason; the answer a client reads holds neither.
// conformance: semantics/error-layer-internal-go-text
func TestNotServedNamesTheConstructInTheLogLineOnly(t *testing.T) {
	var logged strings.Builder
	p := &Proxy{log: slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelInfo}))}
	rec := httptest.NewRecorder()
	status := p.writeErr(rec, context.Background(), notServed("zero-range", errors.New("range 0s is not a positive duration")))
	if status != 500 || rec.Body.String() != lokiInternalText {
		t.Fatalf("answer %d %q", status, rec.Body.String())
	}
	line := logged.String()
	for _, want := range []string{"construct=zero-range", "range 0s is not a positive duration", "msg_id=internal", "code=500"} {
		if !strings.Contains(line, want) {
			t.Errorf("log line %q lacks %q", line, want)
		}
	}
	logged.Reset()
	p.writeErr(httptest.NewRecorder(), context.Background(), proxyFailure(errors.New("decode failed")))
	if strings.Contains(logged.String(), "construct=") {
		t.Errorf("a failure that is no construct names one: %q", logged.String())
	}
}
