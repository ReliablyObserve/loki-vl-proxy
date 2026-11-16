package proxy

import (
	"bufio"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// buildPhaseRow is one row of internal/lokierr/testdata/loki_build_phase.jsonl: a request and Loki's answer to it.
type buildPhaseRow struct {
	ID      string `json:"id"`
	Route   string `json:"route"`
	Target  string `json:"target"`
	Trigger string `json:"trigger"`
	State   string `json:"state"`
	Loki    struct {
		Status int    `json:"status"`
		Body   string `json:"body"`
	} `json:"loki"`
}

// buildPhaseOldWindows are the windows of the recorded rows that end before Loki's last three hours, where it reads
// the store only and the fresh Loki's store holds no line: the window past the retention, the windows at the
// retention's edge, and the windows ending five hours to 26 hours before the recording.
var buildPhaseOldWindows = []string{
	":past-retention", ":inside-retention-edge", ":past-retention-edge", ":starts-past-retention-ends-inside",
	":ends-300m-before", ":ends-600m-before", ":ends-780m-before", ":ends-870m-before", ":ends-900m-before",
	":ends-1560m-before",
}

// A pipeline stage Loki refuses to build answers 400 only when Loki builds it: over a window that reaches its last
// three hours (where it asks its ingesters, which build the pipeline whatever they hold: a selector no stream
// matches and a line filter no line passes fail too), and 200 with an empty result over an older window whose store
// holds nothing. A request error (an instant log query, a limit parameter that is not a number, the resolution, the
// query length), a parser action and a tree check answer 400 over any window, the oldest included. A selector or
// label filter regex error is Loki's parse error on /labels and /series too, and a pipeline build error there is
// `only label matchers are supported`. Recorded over HTTP from a Loki that serves data (error-answers-record).
//
// conformance: semantics/loki-build-phase-window
func TestLokiBuildPhaseWindowRecorded(t *testing.T) {
	f, err := os.Open(filepath.Join("..", "lokierr", "testdata", "loki_build_phase.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<22)
	rows := 0
	for first := true; sc.Scan(); first = false {
		if first {
			continue
		}
		var r buildPhaseRow
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		rows++
		if r.State != "serving" {
			t.Errorf("%s: recorded from a %q Loki, not one that serves data", r.ID, r.State)
		}
		want := 400
		if r.Trigger == "phase:valid:past-retention" {
			want = 200 // the control: a valid query over the window
		} else if strings.HasPrefix(r.Trigger, "build:") && !strings.Contains(r.Trigger, "instant-past-retention") &&
			!strings.Contains(r.Trigger, "limit-param") && !strings.Contains(r.Trigger, "step-too-small") &&
			!strings.Contains(r.Trigger, "over-length") {
			for _, w := range buildPhaseOldWindows {
				if strings.HasSuffix(r.Trigger, w) {
					want = 200
				}
			}
		}
		if r.Loki.Status != want {
			t.Errorf("%s %s: Loki answers %d %.80q, the rule says %d", r.ID, r.Trigger, r.Loki.Status, r.Loki.Body, want)
		}
		if want == 200 && !strings.Contains(r.Loki.Body, `"result":[]`) {
			t.Errorf("%s %s: Loki's 200 is not empty: %.120s", r.ID, r.Trigger, r.Loki.Body)
		}
		if r.Route == "labels" || r.Route == "series" {
			u, err := url.Parse(r.Target)
			if err != nil {
				t.Fatal(err)
			}
			q := u.Query().Get("query") + u.Query().Get("match[]")
			wantBody := "parse error : error parsing regexp: missing closing ): `(`"
			if strings.Contains(q, "line_format") {
				wantBody = "only label matchers are supported"
			}
			if r.Loki.Body != wantBody {
				t.Errorf("%s: Loki answers %q, not %q", r.ID, r.Loki.Body, wantBody)
			}
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if rows < 60 {
		t.Fatalf("%d rows", rows)
	}
}
