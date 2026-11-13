package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// errorWriterGoldenPath holds what the error writers answer for every failure
// kind the error layer knows, recorded byte for byte. A change of any answer
// fails TestErrorWriterAnswersAreUnchanged; LVP_ERROR_WRITER_GOLDEN_UPDATE=1
// rewrites the file when a change is intended.
var errorWriterGoldenPath = filepath.Join("testdata", "error_writer_answers.json")

// timeoutNetError is a transport error that reports a timeout (net.Error).
type timeoutNetError struct{}

func (timeoutNetError) Error() string   { return "i/o timeout on 10.0.0.9:9428" }
func (timeoutNetError) Timeout() bool   { return true }
func (timeoutNetError) Temporary() bool { return true }

// errorWriterCase is one failure handed to the error writers.
type errorWriterCase struct {
	name string
	ctx  func() context.Context
	err  error
}

func liveCtx() context.Context { return context.Background() }

func canceledCtx() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func expiredCtx() context.Context {
	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(1, 0))
	cancel()
	return ctx
}

// errorWriterCases covers every branch of lokiClientError: each typed proxy
// error, each VictoriaLogs error class, the transport errors, the context
// first rule, errors whose text merely says "timeout" or "canceled" (a failed
// backend call: classification is by type), each typed proxy limit, a limit
// message with no type behind it, an untyped error and nil.
func errorWriterCases() []errorWriterCase {
	refused := &url.Error{Op: "Post", URL: "http://10.0.0.9:9428/select/logsql/query", Err: errors.New("dial tcp 10.0.0.9:9428: connect: connection refused")}
	return []errorWriterCase{
		{"nil", liveCtx, nil},
		{"untyped", liveCtx, errors.New("boom: unexpected EOF reading {\"x\"}")},
		{"wrapped canceled", liveCtx, fmt.Errorf("vl call: %w", context.Canceled)},
		{"wrapped deadline", liveCtx, fmt.Errorf("vl call: %w", context.DeadlineExceeded)},
		{"text timeout", liveCtx, errors.New("vl said: read timeout while scanning")},
		{"text deadline exceeded", liveCtx, errors.New("rpc: deadline exceeded")},
		{"text context canceled", liveCtx, errors.New("post: context canceled")},
		{"transport refused", liveCtx, refused},
		{"transport timeout", liveCtx, &url.Error{Op: "Get", URL: "http://10.0.0.9:9428/select/logsql/hits", Err: timeoutNetError{}}},
		{"ctx canceled", canceledCtx, refused},
		{"ctx expired", expiredCtx, refused},
		{"ctx canceled nil", canceledCtx, nil},
		{"vl 400 rejected", liveCtx, &upstreamStatusError{status: 400, msg: "cannot parse query arg: unexpected token", class: vlErrorQueryRejected}},
		{"vl 422 rejected", liveCtx, &upstreamStatusError{status: 422, msg: "cannot parse query arg: x", class: vlErrorQueryRejected}},
		{"vl 400 resource limit", liveCtx, &upstreamStatusError{status: 400, msg: "since it requires more than 1GB", class: vlErrorResourceLimit}},
		{"vl 400 unclassified", liveCtx, &upstreamStatusError{status: 400, msg: "unknown stats func foo", class: vlErrorUnclassified}},
		{"vl 400 unsupported path", liveCtx, &upstreamStatusError{status: 400, msg: "unsupported path requested: /select/logsql/x", class: vlErrorUnsupportedPath}},
		{"vl 503 queue full", liveCtx, &upstreamStatusError{status: 503, msg: "couldn't start executing the request in 10s", class: vlErrorQueueFull}},
		{"vl 429", liveCtx, &upstreamStatusError{status: 429, msg: "too many requests", class: vlErrorUnclassified}},
		{"vl 500", liveCtx, &upstreamStatusError{status: 500, msg: "VL backend returned 500", class: vlErrorUnclassified}},
		{"vl 502 front", liveCtx, &upstreamStatusError{status: 502, msg: "bad gateway", class: vlErrorUnclassified}},
		{"aborted timed out", liveCtx, &vlResponseAbortedError{elapsed: time.Second, timedOut: true, cause: errors.New("unexpected EOF")}},
		{"aborted", liveCtx, &vlResponseAbortedError{elapsed: time.Second, cause: errors.New("connection reset by peer")}},
		{"series limit", liveCtx, newSeriesLimitError(context.Background(), 500)},
		{"series limit row budget", liveCtx, fmt.Errorf("fold: %w", newSeriesLimitError(foldingBudgetContext(1000), 1000))},
		{"translator parse", liveCtx, &translator.ParseError{Msg: "parse error : syntax error: unexpected $end", Pos: -1}},
		{"ordered json pipeline", liveCtx, &orderedJSONPipelineError{message: "pipeline error: invalid \xff byte"}},
		{"vector match", liveCtx, vectorMatchError("found duplicate series for the match group {a=\"b\"} on the right hand-side of the operation")},
		{"label values too large", liveCtx, newLabelValuesTooLarge(2048, 1024)},
		{"body too large", liveCtx, newBackendBodyTooLarge(4096)},
		{"manual metric response too large", liveCtx, newManualMetricResponseTooLarge(64)},
		{"row limit", liveCtx, newRowLimitError(100, "series", 2000)},
		{"binary budget", liveCtx, newBinaryBudgetError("binary-evaluation-budget")},
		{"heavy queue full", liveCtx, newHeavyQueueFull(&heavyQueryQueueFullError{maxConcurrent: 4, queueWait: time.Second})},
		{"query length limit", liveCtx, queryLengthLimit(strings.Repeat("x", 600), 512)},
		{"proxy limit text is no limit", liveCtx, errors.New("manual metric response exceeds 67108864 bytes; narrow the query or increase -backend-max-buffered-response-bytes")},
		{"proxy limit text with quote", liveCtx, errors.New("x \"y\"; narrow the query or increase -flag")},
		{"proxy limit wrapped in url error", liveCtx, &url.Error{Op: "Get", URL: "http://h", Err: errors.New("a; narrow the query or increase -flag")}},
	}
}

// errorWriterAnswer is what a client reads of one written error.
type errorWriterAnswer struct {
	Status      int    `json:"status"`
	ContentType string `json:"content_type"`
	Nosniff     string `json:"nosniff"`
	Body        string `json:"body,omitempty"`
	// BodyQuoted is the body as a Go string literal when it is not valid
	// UTF-8 (JSON would replace its bytes).
	BodyQuoted string `json:"body_quoted,omitempty"`
	Returned   int    `json:"returned"`
}

func recordErrorAnswer(rec *httptest.ResponseRecorder, returned int) errorWriterAnswer {
	a := errorWriterAnswer{
		Status:      rec.Code,
		ContentType: rec.Header().Get("Content-Type"),
		Nosniff:     rec.Header().Get("X-Content-Type-Options"),
		Returned:    returned,
	}
	if body := rec.Body.String(); utf8.ValidString(body) {
		a.Body = body
	} else {
		a.BodyQuoted = strconv.Quote(body)
	}
	return a
}

// errorWriterAnswers runs every case through every error-layer writer and
// shape: writeErr, writeErrShaped with each of Loki's body shapes,
// writeGrafanaStatsFailure, and writeBackendError over VictoriaLogs bodies.
func errorWriterAnswers(t *testing.T) map[string]errorWriterAnswer {
	t.Helper()
	p := &Proxy{}
	shapes := []struct {
		name  string
		shape errorShape
	}{
		{"frontend", writeLokiErrorBody},
		{"http_error", writeLokiHTTPError},
		{"ruler_json", writeLokiRulerError},
		{"alerts_json", writeLokiAlertsError},
	}
	out := map[string]errorWriterAnswer{}
	for _, tc := range errorWriterCases() {
		rec := httptest.NewRecorder()
		out["writeErr/"+tc.name] = recordErrorAnswer(rec, p.writeErr(rec, tc.ctx(), tc.err))
		for _, s := range shapes {
			rec := httptest.NewRecorder()
			out["writeErrShaped/"+s.name+"/"+tc.name] = recordErrorAnswer(rec, p.writeErrShaped(rec, tc.ctx(), tc.err, s.shape))
		}
		rec = httptest.NewRecorder()
		p.writeGrafanaStatsFailure(rec, tc.err)
		out["writeGrafanaStatsFailure/"+tc.name] = recordErrorAnswer(rec, 0)
	}
	bodies := []struct {
		name   string
		status int
		body   string
	}{
		{"parse 400", 400, `{"error":"cannot parse query arg: unexpected token after \"foo\""}`},
		{"stats 422", 422, `{"status":"error","errorType":"422","error":"cannot parse query arg: x"}`},
		{"memory 400", 400, `{"error":"cannot execute query [*]: since it requires more than 1GB of memory"}`},
		{"queue 503", 503, "couldn't start executing the request in 10s"},
		{"plain 500", 500, "internal error at 10.0.0.9:9428"},
		{"empty 502", 502, ""},
		{"429", 429, "too many requests"},
		{"unsupported path", 400, "unsupported path requested: /select/logsql/stream_field_values"},
	}
	for _, b := range bodies {
		rec := httptest.NewRecorder()
		out["writeBackendError/"+b.name] = recordErrorAnswer(rec, p.writeBackendError(rec, b.status, []byte(b.body)))
	}
	return out
}

// TestErrorWriterAnswersAreUnchanged holds every answer of the error layer's
// writers to the recorded bytes: the status, Content-Type, nosniff and body
// for each failure kind, shape and writer.
// conformance: semantics/error-layer-backend-text, semantics/error-layer-transport-text, semantics/error-layer-body-shape, semantics/error-layer-cancel-deadline, semantics/error-layer-backend-failure-status, semantics/error-layer-proxy-limit-status
func TestErrorWriterAnswersAreUnchanged(t *testing.T) {
	got := errorWriterAnswers(t)
	if os.Getenv("LVP_ERROR_WRITER_GOLDEN_UPDATE") == "1" {
		raw, err := json.MarshalIndent(got, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(errorWriterGoldenPath, append(raw, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %s: %d answers", errorWriterGoldenPath, len(got))
		return
	}
	raw, err := os.ReadFile(errorWriterGoldenPath)
	if err != nil {
		t.Fatalf("read %s: %v", errorWriterGoldenPath, err)
	}
	var want map[string]errorWriterAnswer
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatalf("parse %s: %v", errorWriterGoldenPath, err)
	}
	if len(got) != len(want) {
		t.Errorf("%d answers, %s holds %d", len(got), errorWriterGoldenPath, len(want))
	}
	for key, w := range want {
		g, ok := got[key]
		switch {
		case !ok:
			t.Errorf("%s: no longer recorded", key)
		case g != w:
			t.Errorf("%s changed:\n got  %+v\n want %+v", key, g, w)
		}
	}
	for key := range got {
		if _, ok := want[key]; !ok {
			t.Errorf("%s: not in %s", key, errorWriterGoldenPath)
		}
	}
	if t.Failed() {
		t.Logf("an intended change is recorded with LVP_ERROR_WRITER_GOLDEN_UPDATE=1")
	}
}
