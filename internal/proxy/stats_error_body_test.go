package proxy

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
)

// statsErrorBodyCases are bodies a stats call can answer with a success
// status: VictoriaLogs' own errors, success answers whose series, labels or
// values spell "error" or "status", and bodies the decoding rejects.
var statsErrorBodyCases = []struct {
	name, body string
	isErr      bool
	msg        string
}{
	{"success matrix", `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"a"},"values":[[1,"1"]]}]}}`, false, ""},
	{"success without status", `{"data":{"resultType":"matrix","result":[]}}`, false, ""},
	{"error key", `{"error":"cannot parse query"}`, true, "cannot parse query"},
	{"status error with msg", `{"status":"error","msg":"too many series"}`, true, "too many series"},
	{"status error with message", `{"status":"error","message":"m"}`, true, "m"},
	{"status error with error", `{"status":"error","error":"e"}`, true, "e"},
	{"status error, the error key wins", `{"status":"error","error":"e","msg":"m"}`, true, "e"},
	{"status error without text", `{"status":"error"}`, true, ""},
	{"status error, non-string msg", `{"status":"error","msg":42}`, true, ""},
	{"error key not a string", `{"error":{"code":1}}`, false, ""},
	{"error key null", `{"error":null,"data":{}}`, false, ""},
	{"error key not a string, status error", `{"error":7,"status":"error","msg":"x"}`, true, "x"},
	{"status not a string", `{"status":1}`, false, ""},
	{"status other string", `{"status":"warning","msg":"x"}`, false, ""},
	{"escaped error key", `{"error":"escaped"}`, true, "escaped"},
	{"escaped status value", `{"status":"error","msg":"x"}`, true, "x"},
	{"duplicate status, error last", `{"status":"success","status":"error","msg":"x"}`, true, "x"},
	{"duplicate status, success last", `{"status":"error","status":"success","msg":"x"}`, false, ""},
	{"duplicate error, string last", `{"error":1,"error":"late"}`, true, "late"},
	{"duplicate error, non-string last", `{"error":"early","error":1}`, false, ""},
	{"label named error", `{"status":"success","data":{"result":[{"metric":{"error":"x","status":"error"},"values":[]}]}}`, false, ""},
	{"value spelling error", `{"status":"success","data":{"result":[{"metric":{"msg":"\"error\":\"x\""},"values":[]}]}}`, false, ""},
	{"error key after a large data", `{"data":{"result":[` + strings.Repeat(`{"metric":{},"values":[[1,"1"]]},`, 50) + `{"metric":{},"values":[]}]},"error":"late"}`, true, "late"},
	{"whitespace around", " \n{ \"error\" : \"spaced\" }\n ", true, "spaced"},
	{"invalid JSON with an error key", `{"error":"x"`, false, ""},
	{"invalid JSON with status error", `{"status":"error","msg":"x",}`, false, ""},
	{"top-level array", `[{"error":"x"}]`, false, ""},
	{"top-level string", `"error"`, false, ""},
	{"null", `null`, false, ""},
	{"empty", ``, false, ""},
	{"empty object", `{}`, false, ""},
	{"trailing data", `{"error":"x"} {}`, false, ""},
	{"invalid UTF-8 key", "{\"err\xffor\":\"x\"}", false, ""},
}

// The answer of vlStatsErrorBody is the decoding's for every body: VictoriaLogs'
// error forms, a success answer that holds the words, duplicate and escaped
// keys, and bodies the decoding rejects.
// conformance: quality/stats-answer-transform-cost, semantics/error-layer-silent-success
func TestVLStatsErrorBodyMatchesDecoding(t *testing.T) {
	for _, tc := range statsErrorBodyCases {
		t.Run(tc.name, func(t *testing.T) {
			msg, isErr := vlStatsErrorBody([]byte(tc.body))
			refMsg, refErr := vlStatsErrorBodyDecoded([]byte(tc.body))
			if isErr != refErr || msg != refMsg {
				t.Fatalf("got (%q, %v), the decoding (%q, %v)", msg, isErr, refMsg, refErr)
			}
			if isErr != tc.isErr || msg != tc.msg {
				t.Fatalf("got (%q, %v), want (%q, %v)", msg, isErr, tc.msg, tc.isErr)
			}
		})
	}
}

// A success answer of many series is not decoded whole to learn it is no
// error: the check reads its top-level keys only.
// conformance: quality/stats-answer-transform-cost
func TestVLStatsErrorBodyDoesNotDecodeSuccessAnswers(t *testing.T) {
	var b bytes.Buffer
	b.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
	for i := 0; i < 20000; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`{"metric":{"__name__":"count(*)","pod":"pod-` + strconv.Itoa(i) + `"},"values":[[1700000000,"1"]]}`)
	}
	b.WriteString(`]}}`)
	body := b.Bytes()
	allocs := testing.AllocsPerRun(5, func() {
		if _, isErr := vlStatsErrorBody(body); isErr {
			t.Fatal("a success answer is no error")
		}
	})
	if allocs > 2 {
		t.Fatalf("the check allocated %.0f objects for a success answer: it decoded the body", allocs)
	}
}

// FuzzVLStatsErrorBodyDifferential: whatever the body, vlStatsErrorBody
// answers what decoding the whole body answers.
func FuzzVLStatsErrorBodyDifferential(f *testing.F) {
	for _, tc := range statsErrorBodyCases {
		f.Add([]byte(tc.body))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		if len(body) > 64<<10 {
			t.Skip()
		}
		msg, isErr := vlStatsErrorBody(body)
		refMsg, refErr := vlStatsErrorBodyDecoded(body)
		if isErr != refErr || msg != refMsg {
			t.Fatalf("%q: got (%q, %v), the decoding (%q, %v)", body, msg, isErr, refMsg, refErr)
		}
	})
}

// A high-cardinality breakdown (many single-point series, over the size that
// is transformed one series at a time) answers the whole-tree reference's
// bytes through the trim, the translation and the tumbling relabel, with and
// without a series that needs no rewrite first, and with a series the
// per-series path cannot read after the first rewrite.
// conformance: quality/stats-answer-transform-cost
func TestStatsJSONSeriesLargeAnswerMatchesReference(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	p.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, nil)
	churn := func(series int, tail string) []byte {
		var b bytes.Buffer
		b.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
		for i := 0; i < series; i++ {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(`{"metric":{"__name__":"count(*)","pod":"pod-` + strconv.Itoa(i) + `"},"values":[[` + strconv.Itoa(1699999940+60*(i%50)) + `,"1"]]}`)
		}
		b.WriteString(tail + `]}}`)
		return b.Bytes()
	}
	for name, body := range map[string][]byte{
		"churn":                        churn(20000, ""),
		"many points":                  buildStatsMatrixBody(1000, 60),
		"unreadable series at the end": churn(20000, `,{"metric":{"pod":"x"},"values":[[1700000000,"1"]],"bad":[}`),
	} {
		t.Run(name, func(t *testing.T) {
			if len(body) <= maxPooledStatsParserInputSize {
				t.Fatal("the fixture must take the per-series path")
			}
			assertStatsJSONSeriesMatchesReference(t, p, body, `sum by (pod) (count_over_time({app="a"}[60s]))`)
		})
	}
}

// BenchmarkVLStatsErrorBody: the check against decoding the whole body, on a
// small answer, a breakdown of 1,000 series and one of 100,000.
func BenchmarkVLStatsErrorBody(b *testing.B) {
	build := func(series int) []byte {
		var buf bytes.Buffer
		buf.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
		for i := 0; i < series; i++ {
			if i > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString(`{"metric":{"__name__":"count(*)","pod":"pod-` + strconv.Itoa(i) + `"},"values":[[1700000000,"1"]]}`)
		}
		buf.WriteString(`]}}`)
		return buf.Bytes()
	}
	for _, size := range []int{1, 1000, 100000} {
		body := build(size)
		for name, check := range map[string]func([]byte) (string, bool){"check": vlStatsErrorBody, "decoded": vlStatsErrorBodyDecoded} {
			b.Run(strconv.Itoa(size)+"/"+name, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(body)))
				for b.Loop() {
					_, _ = check(body)
				}
			})
		}
	}
}
