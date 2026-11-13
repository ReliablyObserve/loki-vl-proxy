package lokierr

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// AnswersPath is the file of real Loki error answers (one row per Loki error
// class and route) that the constructors and the proxy replay are held to.
const answersPath = "testdata/loki_error_answers.jsonl"

// answer is one recorded Loki error answer.
type answer struct {
	ID     string          `json:"id"`
	Route  string          `json:"route"`
	Method string          `json:"method"`
	Msg    string          `json:"msg"`
	Args   json.RawMessage `json:"args"`
	Loki   struct {
		Status      int    `json:"status"`
		ContentType string `json:"content_type"`
		Nosniff     string `json:"nosniff"`
		Body        string `json:"body"`
	} `json:"loki"`
}

func readAnswers(t *testing.T) []answer {
	t.Helper()
	f, err := os.Open(answersPath)
	if err != nil {
		t.Fatalf("open %s: %v", answersPath, err)
	}
	defer f.Close()
	var out []answer
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<16), 1<<22)
	for sc.Scan() {
		var a answer
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			t.Fatalf("%s: %v", answersPath, err)
		}
		if a.ID != "" {
			out = append(out, a)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// lokiText is the message of a recorded answer: the body of a text answer
// without http.Error's newline, or the error field of format_query's JSON.
func lokiText(t *testing.T, a answer) string {
	t.Helper()
	body := a.Loki.Body
	if strings.HasPrefix(a.Loki.ContentType, "application/json") {
		var doc struct {
			Status string `json:"status"`
			Error  string `json:"error"`
		}
		if err := json.Unmarshal([]byte(body), &doc); err != nil {
			t.Fatalf("%s: %v", a.ID, err)
		}
		return doc.Error
	}
	return strings.TrimSuffix(body, "\n")
}

// numError runs the standard library parse Loki's message names on raw.
func numError(t *testing.T, fn, raw string) *strconv.NumError {
	t.Helper()
	var err error
	switch fn {
	case "ParseInt":
		_, err = strconv.ParseInt(raw, 10, 64)
	case "Atoi":
		_, err = strconv.Atoi(raw)
	default:
		t.Fatalf("unknown strconv function %q", fn)
	}
	var ne *strconv.NumError
	if !errors.As(err, &ne) {
		t.Fatalf("%s(%q) gave %v, not a *strconv.NumError", fn, raw, err)
	}
	return ne
}

// build renders the constructor of a recorded row from its typed arguments.
func build(t *testing.T, a answer) *Error {
	t.Helper()
	var args struct {
		Line     int    `json:"line"`
		Col      int    `json:"col"`
		Detail   string `json:"detail"`
		Size     int    `json:"size"`
		Limit    int    `json:"limit"`
		Name     string `json:"name"`
		Func     string `json:"func"`
		Raw      string `json:"raw"`
		LengthNS int64  `json:"length_ns"`
		LimitNS  int64  `json:"limit_ns"`
		Escape   string `json:"escape"`
	}
	if len(a.Args) > 0 {
		if err := json.Unmarshal(a.Args, &args); err != nil {
			t.Fatalf("%s args: %v", a.ID, err)
		}
	}
	switch a.Msg {
	case "parse":
		return Parse(args.Line, args.Col, args.Detail)
	case "parse-input-size":
		return InputTooLong(args.Size, args.Limit)
	case "param-number":
		return ParamNumber(args.Name, numError(t, args.Func, args.Raw))
	case "param-duration":
		return ParamDuration(args.Name, args.Raw)
	case "number":
		return Number(numError(t, args.Func, args.Raw))
	case "step-duration":
		return StepDuration(args.Raw)
	case "step-overflow":
		return StepOverflow(args.Raw)
	case "step-not-positive":
		return StepNotPositive()
	case "step-too-many-points":
		return TooManyPoints()
	case "end-before-start":
		return EndBeforeStart()
	case "limit-not-positive":
		return LimitNotPositive()
	case "direction":
		return Direction(args.Raw)
	case "interval-negative":
		return IntervalNegative()
	case "aggregate-by":
		return AggregateBy()
	case "delay-for":
		return DelayFor(args.Limit)
	case "instant-log-query":
		return InstantLogQuery()
	case "only-label-matchers":
		return OnlyLabelMatchers()
	case "only-log-selector":
		return OnlyLogSelector()
	case "url-escape":
		return URLEscape(url.EscapeError(args.Escape))
	case "url-semicolon":
		return Semicolon()
	case "post-too-large":
		return PostTooLarge()
	case "limit-query-length":
		return QueryLength(time.Duration(args.LengthNS), time.Duration(args.LimitNS))
	case "limit-lookback":
		return Lookback(time.Duration(args.LimitNS))
	case "limit-index-series":
		return IndexSeriesLimit(args.Limit)
	case "not-found":
		return NotFound()
	case "method-not-allowed":
		return MethodNotAllowed()
	case "tail-not-websocket":
		return TailNotWebSocket()
	}
	t.Fatalf("%s: no constructor for message %q", a.ID, a.Msg)
	return nil
}

// TestConstructorsRenderLokiRecordedAnswers renders every recorded Loki error
// whose message has a constructor from the row's typed inputs and requires
// Loki's text byte for byte, Loki's status and, for router messages, Loki's
// body shape.
// conformance: semantics/error-request-validation-parity, semantics/error-layer-body-shape, status-400, status-404, status-405, status-500
func TestConstructorsRenderLokiRecordedAnswers(t *testing.T) {
	seen := map[string]int{}
	for _, a := range readAnswers(t) {
		if a.Msg == "" {
			continue
		}
		e := build(t, a)
		seen[a.Msg]++
		if e.Msg().ID() != a.Msg {
			t.Errorf("%s: built message %q, row says %q", a.ID, e.Msg().ID(), a.Msg)
		}
		if got, want := e.Text(), lokiText(t, a); got != want {
			t.Errorf("%s (%s): text\n got  %q\n want %q", a.ID, a.Msg, got, want)
		}
		if e.Status() != a.Loki.Status {
			t.Errorf("%s (%s): status %d, Loki %d", a.ID, a.Msg, e.Status(), a.Loki.Status)
		}
		switch e.Shape() {
		case ShapeHTTPError:
			if !strings.HasSuffix(a.Loki.Body, "\n") || a.Loki.Nosniff != "nosniff" || !strings.HasPrefix(a.Loki.ContentType, "text/plain") {
				t.Errorf("%s (%s): Loki's answer is not http.Error's shape: %q %q", a.ID, a.Msg, a.Loki.ContentType, a.Loki.Body)
			}
		case ShapeBodyless:
			if a.Loki.Body != "" || a.Loki.ContentType != "" {
				t.Errorf("%s (%s): Loki's answer has a body: %q %q", a.ID, a.Msg, a.Loki.ContentType, a.Loki.Body)
			}
		}
	}
	// Every message seen in a Loki recording has a row. The rest are held by
	// TestConstructorTexts.
	for _, id := range []string{"parse", "param-number", "param-duration", "number", "step-duration", "step-overflow",
		"step-not-positive", "step-too-many-points", "end-before-start", "limit-not-positive", "direction",
		"interval-negative", "aggregate-by", "delay-for", "instant-log-query", "only-label-matchers",
		"only-log-selector", "url-escape", "url-semicolon", "post-too-large", "parse-input-size",
		"limit-query-length", "limit-lookback", "limit-index-series", "not-found", "method-not-allowed",
		"tail-not-websocket"} {
		if seen[id] == 0 {
			t.Errorf("no recorded Loki answer renders message %q", id)
		}
	}
}

// TestConstructorTexts holds the messages no HTTP recording can reach (the
// live stack cannot fail its storage, cancel a request it is answering or
// fill its queue) to Loki's texts as the proxy's error-layer gate records
// them, and the classes, statuses and shapes of every message.
// conformance: semantics/error-layer-cancel-deadline, semantics/error-layer-backend-failure-status, status-429, status-500
func TestConstructorTexts(t *testing.T) {
	cause := errors.New("dial tcp 10.0.0.9:9428: connect: connection refused")
	cases := []struct {
		e      *Error
		kind   Kind
		status int
		text   string
	}{
		{Canceled(cause), KindCanceled, 499, "the request was cancelled by the client"},
		{Deadline(cause), KindDeadline, http.StatusGatewayTimeout, "request timed out, decrease the duration of the request or add more label matchers (prefer exact match over regex match) to reduce the amount of data processed"},
		{QueueFull(cause), KindQueueFull, http.StatusTooManyRequests, "too many outstanding requests"},
		{BackendFailed(cause), KindBackendFailed, http.StatusInternalServerError, "Internal Server Error"},
		{Internal(cause), KindInternal, http.StatusInternalServerError, "Internal Server Error"},
		{SeriesLimit(500), KindLimit, http.StatusBadRequest, "maximum number of series (500) reached for a single query; consider reducing query cardinality by adding more specific stream selectors, reducing the time range, or aggregating results with functions like sum(), count() or topk()"},
		{Parse(0, 0, "syntax error: unexpected $end"), KindParse, http.StatusBadRequest, "parse error : syntax error: unexpected $end"},
		{Parse(1, 4, "syntax error: unexpected $end"), KindParse, http.StatusBadRequest, "parse error at line 1, col 4: syntax error: unexpected $end"},
		{Parse(0, 25, `not a valid duration string: "1h1h"`), KindParse, http.StatusBadRequest, `parse error at line 0, col 25: not a valid duration string: "1h1h"`},
		{ParserMessage("parse error : queries require at least one regexp or equality matcher"), KindParse, http.StatusBadRequest, "parse error : queries require at least one regexp or equality matcher"},
		{MethodNotAllowed(), KindRoute, http.StatusMethodNotAllowed, ""},
		{WriteNotSupported(), KindRoute, http.StatusMethodNotAllowed, "write operations are not supported, this is a read-only proxy"},
		{LimitReached("some-flag", 7, cause), KindLimit, http.StatusBadRequest, "limit reached while evaluating the query"},
		{ResponseTooLarge("some-flag", 65, 64, cause), KindLimit, http.StatusInternalServerError, "rpc error: code = ResourceExhausted desc = grpc: trying to send message larger than max (65 vs. 64)"},
	}
	for _, tc := range cases {
		if tc.e.Kind() != tc.kind || tc.e.Status() != tc.status || tc.e.Text() != tc.text || tc.e.Error() != tc.text {
			t.Errorf("%s: kind %s status %d text %q; want %s %d %q", tc.e.Msg().ID(), tc.e.Kind(), tc.e.Status(), tc.e.Text(), tc.kind, tc.status, tc.text)
		}
		if c := tc.e.Cause(); c != nil && !errors.Is(tc.e, cause) {
			t.Errorf("%s: cause %v not reachable with errors.Is", tc.e.Msg().ID(), c)
		}
		if strings.Contains(tc.e.Text(), "10.0.0.9") {
			t.Errorf("%s: the cause reached the client text", tc.e.Msg().ID())
		}
	}
}

// TestEveryMessageIsDescribed: every message has an id, a kind, a Loki status
// and a distinct id.
// conformance: architecture-syntax-package-layering
func TestEveryMessageIsDescribed(t *testing.T) {
	ids := map[string]Msg{}
	for m := Msg(1); m < msgCount; m++ {
		d := messages[m]
		if d.id == "" || d.kind == 0 || d.status < 400 {
			t.Errorf("message %d is not described: %+v", m, d)
		}
		if other, dup := ids[d.id]; dup {
			t.Errorf("messages %d and %d share id %q", other, m, d.id)
		}
		ids[d.id] = m
		if Kind(d.kind).String() == "unknown" {
			t.Errorf("message %q: kind %d has no name", d.id, d.kind)
		}
	}
	if Msg(200).ID() != "none" || Kind(200).String() != "unknown" {
		t.Errorf("out of range ids must not panic")
	}
}

// TestWithCauseKeepsTheText: a cause never changes the client text, and As
// finds a wrapped error.
// conformance: semantics/error-layer-backend-text, semantics/error-layer-transport-text
func TestWithCauseKeepsTheText(t *testing.T) {
	cause := errors.New(`VictoriaLogs: cannot parse query [{"x"}]`)
	e := StepNotPositive().WithCause(cause)
	if e.Text() != textStepNotPos || !errors.Is(e, cause) {
		t.Fatalf("WithCause: %q, cause reachable %v", e.Text(), errors.Is(e, cause))
	}
	wrapped := fmt.Errorf("handler: %w", e)
	got, ok := As(wrapped)
	if !ok || got != e {
		t.Fatalf("As(%v) = %v %v", wrapped, got, ok)
	}
	joined := errors.Join(errors.New("first"), fmt.Errorf("second: %w", e))
	if got, ok := As(joined); !ok || got != e {
		t.Fatalf("As(errors.Join(...)) = %v %v", got, ok)
	}
	if _, ok := As(errors.Join(cause, errors.New("x"))); ok {
		t.Fatal("As found a lokierr.Error in a joined plain error")
	}
	if allocs := testing.AllocsPerRun(100, func() { _, _ = As(wrapped) }); allocs != 0 {
		t.Fatalf("As allocates %.0f times", allocs)
	}
	if allocs := testing.AllocsPerRun(100, func() { _, _ = As(cause) }); allocs != 0 {
		t.Fatalf("As of a plain error allocates %.0f times", allocs)
	}
	if _, ok := As(cause); ok {
		t.Fatal("As found a lokierr.Error in a plain error")
	}
	if _, ok := As(nil); ok {
		t.Fatal("As(nil)")
	}
}

// TestFormatDuration holds the Prometheus duration form Loki prints limits in.
// conformance: status-400
func TestFormatDuration(t *testing.T) {
	for d, want := range map[time.Duration]string{
		0:                                  "0s",
		time.Millisecond:                   "1ms",
		1500 * time.Millisecond:            "1s500ms",
		time.Hour:                          "1h",
		721 * time.Hour:                    "30d1h",
		90 * 24 * time.Hour:                "90d",
		14 * 24 * time.Hour:                "2w",
		365 * 24 * time.Hour:               "1y",
		(365*24 + 1) * time.Hour:           "365d1h",
		-time.Hour:                         "-1h",
		time.Nanosecond:                    "0s",
		6*time.Hour + 30*time.Second:       "6h30s",
		2*24*time.Hour + 61*time.Minute:    "2d1h1m",
		7*24*time.Hour + 7*24*time.Hour*52: "53w",
	} {
		if got := FormatDuration(d); got != want {
			t.Errorf("FormatDuration(%v) = %q, want %q", d, got, want)
		}
	}
}

// TestPackageIsALeaf: lokierr imports the standard library only, so every
// layer (the syntax packages, the plan, the proxy) can build its errors.
// conformance: architecture-syntax-package-layering
func TestPackageIsALeaf(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range f.Imports {
			path, _ := strconv.Unquote(imp.Path.Value)
			if first, _, _ := strings.Cut(path, "/"); strings.Contains(first, ".") {
				t.Errorf("%s imports %s: lokierr imports the standard library only", name, path)
			}
		}
	}
}

// TestLimitCarriesItsFlagForTheLogOnly: a limit or queue error keeps the flag
// that sets the limit and its value apart from the client text, whichever
// constructor built it, and WithLimit never changes the text.
// conformance: semantics/error-layer-proxy-limit-status
func TestLimitCarriesItsFlagForTheLogOnly(t *testing.T) {
	cause := errors.New("flag text: narrow the query or increase -some-flag")
	for _, tc := range []struct {
		name string
		e    *Error
	}{
		{"limit reached", LimitReached("some-flag", 7, cause)},
		{"response too large", ResponseTooLarge("some-flag", 8, 7, cause)},
		{"series", SeriesLimit(7).WithLimit("some-flag", 7)},
		{"input too long", InputTooLong(9, 7).WithLimit("some-flag", 7)},
		{"queue full", QueueFull(cause).WithLimit("some-flag", 7)},
		{"series of", SeriesLimitOf("some-flag", 7, cause)},
		{"queue full at", QueueFullAt("some-flag", 7, cause)},
	} {
		flag, value, ok := tc.e.Limit()
		if !ok || flag != "some-flag" || value != 7 {
			t.Errorf("%s: Limit() = %q %d %v", tc.name, flag, value, ok)
		}
		if strings.Contains(tc.e.Text(), "some-flag") || strings.Contains(tc.e.Error(), "some-flag") {
			t.Errorf("%s: the flag reached the client text %q", tc.name, tc.e.Text())
		}
	}
	plain := SeriesLimit(7)
	withLimit := plain.WithLimit("some-flag", 7)
	if _, _, ok := plain.Limit(); ok {
		t.Error("WithLimit changed the error it was called on")
	}
	if withLimit.Text() != plain.Text() || withLimit.Msg() != plain.Msg() {
		t.Errorf("WithLimit changed the answer: %q vs %q", withLimit.Text(), plain.Text())
	}
	if _, _, ok := StepNotPositive().Limit(); ok {
		t.Error("an error with no limit reports one")
	}
}
