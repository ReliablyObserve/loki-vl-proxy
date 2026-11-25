package querycorpus_test

import (
	"encoding/json"
	"strings"
	"testing"
)

func lokiMatrix(series ...string) json.RawMessage {
	body := `{"status":"success","data":{"resultType":"matrix","result":[`
	for i, s := range series {
		if i > 0 {
			body += ","
		}
		body += s
	}
	return json.RawMessage(body + `]}}`)
}

func lokiSeries(app, values string) string {
	return `{"metric":{"app":"` + app + `"},"values":` + values + `}`
}

func tieRecord(body json.RawMessage, vlBody string, status int) *metricRecord {
	return &metricRecord{
		Q: "topk(1, sum by (app) (count_over_time({a=\"b\"}[5m])))", Inputs: "in",
		Loki: map[string]metricLokiAnswer{"range": {Status: status, Body: body}},
		VL:   map[string]map[string]metricVLAnswer{"v1.54.0": {"k": {Status: 200, Body: vlBody}}},
	}
}

// conformance: semantics/rank-over-binary-operation
func TestLokiTieOnlyDifferenceAcceptsOnlyAnotherPickAmongTiedSeries(t *testing.T) {
	committed := tieRecord(lokiMatrix(lokiSeries("a", `[[1,"9"],[2,"7"]]`)), `{"values":[]}`, 200)
	for _, tc := range []struct {
		name string
		live *metricRecord
		want bool
	}{
		{"the same answer", tieRecord(lokiMatrix(lokiSeries("a", `[[1,"9"],[2,"7"]]`)), `{"values":[]}`, 200), false}, // equal: not a difference to excuse
		{"another series kept at a tie", tieRecord(lokiMatrix(lokiSeries("b", `[[1,"9"],[2,"7"]]`)), `{"values":[]}`, 200), true},
		{"the tie split across series", tieRecord(lokiMatrix(lokiSeries("a", `[[1,"9"]]`), lokiSeries("b", `[[2,"7"]]`)), `{"values":[]}`, 200), true},
		{"a different value", tieRecord(lokiMatrix(lokiSeries("a", `[[1,"9"],[2,"8"]]`)), `{"values":[]}`, 200), false},
		{"a missing step", tieRecord(lokiMatrix(lokiSeries("a", `[[1,"9"]]`)), `{"values":[]}`, 200), false},
		{"another status", tieRecord(lokiMatrix(lokiSeries("a", `[[1,"9"],[2,"7"]]`)), `{"values":[]}`, 500), false},
		{"another VictoriaLogs answer", tieRecord(lokiMatrix(lokiSeries("b", `[[1,"9"],[2,"7"]]`)), `{"values":[1]}`, 200), false},
		{"another label name", tieRecord(json.RawMessage(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"env":"a"},"values":[[1,"9"],[2,"7"]]}]}}`), `{"values":[]}`, 200), false},
	} {
		if got := lokiTieOnlyDifference(tc.live, committed); got != tc.want {
			t.Errorf("%s: lokiTieOnlyDifference = %v, want %v", tc.name, got, tc.want)
		}
	}
	// Tied series with different label names: another pick is accepted only with names some committed answer holds.
	namespaced := `{"metric":{"app":"b","namespace":"n"},"values":[[1,"9"],[2,"7"]]}`
	both := tieRecord(lokiMatrix(lokiSeries("a", `[[1,"9"],[2,"7"]]`)), `{"values":[]}`, 200)
	both.Loki["step"] = metricLokiAnswer{Status: 200, Body: lokiMatrix(namespaced)}
	pick := tieRecord(lokiMatrix(namespaced), `{"values":[]}`, 200)
	pick.Loki["step"] = both.Loki["step"]
	if !lokiTieOnlyDifference(pick, both) {
		t.Error("a tied series with label names another committed answer holds is a tie difference")
	}
	if lokiTieOnlyDifference(tieRecord(lokiMatrix(namespaced), `{"values":[]}`, 200), committed) {
		t.Error("label names no committed answer holds are a difference")
	}
	unranked := tieRecord(lokiMatrix(lokiSeries("b", `[[1,"9"],[2,"7"]]`)), `{"values":[]}`, 200)
	base := tieRecord(committed.Loki["range"].Body, `{"values":[]}`, 200)
	unranked.Q, base.Q = "sum by (app) (rate({a=\"b\"}[5m]))", "sum by (app) (rate({a=\"b\"}[5m]))"
	if lokiTieOnlyDifference(unranked, base) {
		t.Error("only a ranking breaks ties at random")
	}
	// sort and sort_desc return series of one value in any order, never other samples or another value order.
	vector := func(samples ...string) *metricRecord {
		body := `{"status":"success","data":{"resultType":"vector","result":[`
		for i, s := range samples {
			if i > 0 {
				body += ","
			}
			app, value, _ := strings.Cut(s, "=")
			body += `{"metric":{"app":"` + app + `"},"value":[1,"` + value + `"]}`
		}
		r := tieRecord(json.RawMessage(body+`]}}`), `{"values":[]}`, 200)
		r.Q = `sort_desc(max by (app) (count_over_time({a="b"}[5m])))`
		return r
	}
	order := vector("a=9", "b=1", "c=1")
	if !lokiTieOnlyDifference(vector("a=9", "c=1", "b=1"), order) {
		t.Error("another order of equal values is a tie difference")
	}
	for name, live := range map[string]*metricRecord{
		"another value order": vector("b=1", "a=9", "c=1"),
		"another series":      vector("a=9", "d=1", "b=1"),
		"a missing series":    vector("a=9", "b=1"),
	} {
		if lokiTieOnlyDifference(live, order) {
			t.Errorf("%s: not a tie difference", name)
		}
	}
	if lokiTieOnlyDifference(nil, committed) || lokiTieOnlyDifference(committed, nil) {
		t.Error("a missing record is never a tie difference")
	}
	other := tieRecord(lokiMatrix(lokiSeries("b", `[[1,"9"],[2,"7"]]`)), `{"values":[]}`, 200)
	other.Inputs = "changed"
	if lokiTieOnlyDifference(other, committed) {
		t.Error("answers recorded for other inputs are not the committed ones")
	}
}
