package logsql_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/capability"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql/refusalcases"
)

// refusalBaseline holds, per release, a digest of every answer the refusal prediction and the capability view gave
// before the gates moved into the capability table (internal/capability): the Capabilities of the release and the
// prediction of every row of the refusal table, with and without a learned length limit. The table-driven gate set
// must give the same answers, so moving the gates changed no answer the proxy gives.
//
// conformance: vl-pipe-before-release, vl-stats-range-pipe-before-stats, vl-stats-pipe-after-stats, vl-math-quoted-constant-clone-crash, vl-extra-filters-too-long, vl-query-too-long
var refusalBaseline = map[string]string{
	"":        "8270db51a32b4072",
	"v1.29.0": "231d0f8cf4677977",
	"v1.45.0": "c08a8d21a5934b38",
	"v1.49.0": "6520be106e9546df",
	"v1.50.0": "355ad2a8022cb298",
	"v1.51.0": "dcc895729e1614be",
	"v1.51.1": "0d4816b8bde41197",
	"v1.52.0": "fc0ab716d08b5426",
	"v1.52.1": "9484aa301bd300a9",
	"v1.52.2": "63ce78088f7d7a02",
	"v1.53.0": "9c7b3131e60592e0",
	"v1.54.0": "cd76241d9e74ff7b",
	"v1.55.0": "eedf890574b7d5df",
	"v2.0.0":  "f3cab9d4e5f3e9f7",
}

func TestGateTableKeepsEveryRefusalAnswer(t *testing.T) {
	extra := `{app="` + strings.Repeat("a", 16400) + `"}`
	cases := refusalcases.Cases()
	cases = append(cases,
		refusalcases.Case{Name: "extra_stream_filters over the length limit", In: logsql.RefusalInput{Path: logsql.PathStatsQueryRange, Query: `* | stats count() as c`, MaxQueryLen: 16384, ExtraStreamFilters: []string{extra}}},
		refusalcases.Case{Name: "extra_filters over the length limit", In: logsql.RefusalInput{Path: logsql.PathQuery, Query: `*`, MaxQueryLen: 16384, ExtraFilters: []string{`level:` + strings.Repeat("a", 16400)}}},
		refusalcases.Case{Name: "extra_filters over the length limit tail", In: logsql.RefusalInput{Path: logsql.PathTail, Query: `*`, MaxQueryLen: 16384, ExtraFilters: []string{`level:` + strings.Repeat("a", 16400)}}},
	)
	for rel, want := range refusalBaseline {
		out := map[string]string{}
		set := capability.ForRelease(capability.ParseRelease(rel))
		b, _ := json.Marshal(logsql.CapabilitiesFor(rel))
		out["caps|"+rel] = string(b)
		for _, cs := range cases {
			for _, mq := range []int{0, 16384} {
				in := cs.In
				if in.MaxQueryLen == 0 {
					in.MaxQueryLen = mq
				}
				r, ok := logsql.PredictRefusal(in, set)
				b, _ := json.Marshal(struct {
					OK bool
					R  logsql.Refusal
				}{ok, r})
				out["predict|"+rel+"|"+cs.Name+"|"+string(rune('0'+mq/16384))] = string(b)
			}
		}
		keys := make([]string, 0, len(out))
		for k := range out {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		lines := make([]string, len(keys))
		for i, k := range keys {
			lines[i] = k + "=" + out[k]
		}
		sum := sha256.Sum256([]byte(strings.Join(lines, "\n")))
		if got := hex.EncodeToString(sum[:])[:16]; got != want {
			t.Errorf("release %q: answers digest %s, want %s (the gate table changed an answer)", rel, got, want)
		}
	}
}
