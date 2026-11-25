package querycorpus_test

import (
	"encoding/json"
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
	if lokiTieOnlyDifference(nil, committed) || lokiTieOnlyDifference(committed, nil) {
		t.Error("a missing record is never a tie difference")
	}
	other := tieRecord(lokiMatrix(lokiSeries("b", `[[1,"9"],[2,"7"]]`)), `{"values":[]}`, 200)
	other.Inputs = "changed"
	if lokiTieOnlyDifference(other, committed) {
		t.Error("answers recorded for other inputs are not the committed ones")
	}
}
