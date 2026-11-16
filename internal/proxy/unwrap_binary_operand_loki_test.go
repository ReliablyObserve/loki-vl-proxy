package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// TestUnwrapBinaryOperandMatchesRecordedLoki pins that a rejected unwrap value fails a binary operation whichever
// operand reads it: the operands are evaluated one after the other, and the check decides on the hits of every
// operand before the answer goes out. Loki's status and text are recorded from its engine over the same line, at the
// instant and over a range. Every request runs twice on one proxy: an answer the check replaced is never kept by a
// response cache.
//
// conformance: semantics/unwrap-conversion-error, semantics/unwrap-conversion-error-binary-operand
func TestUnwrapBinaryOperandMatchesRecordedLoki(t *testing.T) {
	at := time.Unix(1700000000, 0)
	badTS := at.Add(3*time.Minute + 7*time.Second)
	const line = "n=1 v=x7"
	stream := map[string]string{"service_name": "pay"}
	count := `sum by (service_name) (count_over_time({service_name="pay"} | logfmt [5m]))`
	clean := `sum by (service_name) (sum_over_time({service_name="pay"} | logfmt | unwrap n [5m]))`
	rejected := `sum by (service_name) (sum_over_time({service_name="pay"} | logfmt | unwrap v [5m]))`
	queries := []string{
		count + " + " + rejected,
		clean + " + " + rejected,
		rejected + " / " + count,
		"(" + count + " + " + clean + ") * " + rejected,
		count + " + " + clean,
	}
	instant := func(q string) url.Values {
		return url.Values{"query": {q}, "time": {strconv.FormatInt(badTS.Add(time.Minute).UnixNano(), 10)}}
	}
	ranged := func(q string) url.Values {
		return url.Values{"query": {q}, "start": {strconv.FormatInt(at.UnixNano(), 10)}, "end": {strconv.FormatInt(at.Add(10*time.Minute).UnixNano(), 10)}, "step": {"60"}}
	}
	set := oraclerun.EngineDataSet{Streams: []oraclerun.EngineStream{{Labels: stream, Entries: []oraclerun.EngineEntry{{TS: badTS.UnixNano(), Line: line}}}}}
	for i, q := range queries {
		set.Requests = append(set.Requests,
			oraclerun.EngineRequest{ID: fmt.Sprintf("instant/%d", i), Instant: true, Form: instant(q).Encode()},
			oraclerun.EngineRequest{ID: fmt.Sprintf("range/%d", i), Form: ranged(q).Encode()})
	}
	answers := lokiEngineAnswers(t, "unwrap_binary_operand", []oraclerun.EngineDataSet{set})
	row := func(fields map[string]string) map[string]string {
		out := map[string]string{"_time": badTS.UTC().Format(time.RFC3339Nano), "_stream": `{service_name="pay"}`, "_stream_id": "s1", "_msg": line, "service_name": "pay"}
		for k, v := range fields {
			out[k] = v
		}
		return out
	}
	for i, q := range queries {
		for _, mode := range []string{"instant", "range"} {
			id := fmt.Sprintf("%s/%d", mode, i)
			loki, ok := answers[id]
			if !ok {
				t.Fatalf("%s: no recorded answer", id)
			}
			fake := &conversionFakeVL{rows: []map[string]string{row(map[string]string{"n": "1", "v": "x7"})}, stored: []map[string]string{row(nil)}, badBucket: badTS.Truncate(time.Minute).Unix()}
			p := newTestProxy(t, newConversionFakeVL(t, fake).URL)
			for run := 1; run <= 2; run++ {
				rec := httptest.NewRecorder()
				if mode == "instant" {
					p.handleQuery(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+instant(q).Encode(), nil))
				} else {
					p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+ranged(q).Encode(), nil))
				}
				if rec.Code != loki.Status {
					t.Errorf("%s run %d %s: proxy %d %.200s, Loki %d %.200s", mode, run, q, rec.Code, rec.Body.String(), loki.Status, loki.Error)
					break
				}
				if loki.Status == http.StatusBadRequest && strings.TrimSpace(rec.Body.String()) != strings.TrimSpace(loki.Error) {
					t.Errorf("%s run %d %s:\n proxy %q\n Loki  %q", mode, run, q, rec.Body.String(), loki.Error)
				}
			}
		}
	}
}
