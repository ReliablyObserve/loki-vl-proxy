package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/lokirecorded"
)

// Every row the recorder asks gets the state of the Loki it was recorded from,
// reported like a changed answer; a second run against the same Loki changes
// nothing.
func TestStateIsWrittenIntoEveryReRecordedRow(t *testing.T) {
	answers, order := copyFiles(t)
	before, err := lokirecorded.Read(answers)
	if err != nil {
		t.Fatal(err)
	}
	kept := map[string]string{}
	for _, r := range before.Rows {
		if !r.Comparable() {
			kept[r.ID] = r.State
		}
	}
	base := fakeLoki(t, recordedAnswers(t, answers, order), "3.7.9")
	var out bytes.Buffer
	// The committed rows were recorded from a serving Loki; this one is fresh.
	cfg := config{loki: base, files: []string{answers}, state: lokirecorded.StateFresh}
	changed, err := run(cfg, &out)
	if err != nil {
		t.Fatal(err)
	}
	f, err := lokirecorded.Read(answers)
	if err != nil {
		t.Fatal(err)
	}
	comparable := 0
	for _, r := range f.Rows {
		if !r.Comparable() {
			if r.State != kept[r.ID] {
				t.Errorf("%s: a row that was not re-recorded changed its state to %q", r.ID, r.State)
			}
			continue
		}
		comparable++
		if r.State != lokirecorded.StateFresh {
			t.Errorf("%s: state %q", r.ID, r.State)
		}
	}
	if changed != comparable {
		t.Fatalf("%d rows changed, want the %d re-recorded ones:\n%.2000s", changed, comparable, out.String())
	}
	if changed, err = run(cfg, io.Discard); err != nil || changed != 0 {
		t.Fatalf("second run changed %d rows (%v)", changed, err)
	}
}

// A row added by hand with no answer yet is recorded even though it is marked
// not comparable; the mark stays for a reviewer to lift.
func TestRowsWithoutAnAnswerAreRecorded(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "answers.jsonl")
	row := `{"id":"r9","route":"patterns","method":"GET","target":"/loki/api/v1/patterns?query=vector%281%29","headers":["X-Scope-OrgID: 0"],"trigger":"t","loki":{"status":0,"content_type":"","nosniff":"","body":""},"not_comparable":"no answer yet"}`
	if err := os.WriteFile(path, []byte(`{"_source":"Real Grafana Loki 3.7.9 answers recorded over HTTP on 2026-11-13 (x)."}`+"\n"+row+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := lokirecorded.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	served := map[string]lokirecorded.Reply{f.Rows[0].Request(): {Status: 200, ContentType: "application/json", Body: `{"status":"success","data":[]}`}}
	base := fakeLoki(t, served, "3.7.9")
	changed, err := run(config{loki: base, files: []string{path}, state: lokirecorded.StateServing}, io.Discard)
	if err != nil || changed != 1 {
		t.Fatalf("changed %d (%v)", changed, err)
	}
	if f, err = lokirecorded.Read(path); err != nil {
		t.Fatal(err)
	}
	got := f.Rows[0]
	if got.Loki.Status != 200 || got.Loki.Body != `{"status":"success","data":[]}` || got.State != lokirecorded.StateServing || got.NotComparable != "no answer yet" {
		t.Fatalf("row %+v", got)
	}
}

// fixtureLoki is a Loki stand-in for the ingest: it takes one push and answers
// the serving probe with an empty matrix until emptyFor probes have passed.
type fixtureLoki struct {
	pushStatus int
	emptyFor   int32
	probes     atomic.Int32
	services   atomic.Int32
	pushed     atomic.Int32
	body       atomic.Value
}

func (f *fixtureLoki) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/loki/api/v1/push":
		raw, _ := io.ReadAll(r.Body)
		f.body.Store(raw)
		f.pushed.Add(1)
		w.WriteHeader(f.pushStatus)
	case "/services":
		if f.services.Add(1) == 1 {
			_, _ = io.WriteString(w, "ingester => Running\nruler => Starting\n")
			return
		}
		_, _ = io.WriteString(w, "ingester => Running\nruler => Running\n")
	case "/loki/api/v1/query_range":
		if r.URL.Query().Get("query") != servingProbe {
			http.Error(w, "unexpected query", http.StatusBadRequest)
			return
		}
		result := `[]`
		if f.probes.Add(1) > f.emptyFor {
			result = `[{"metric":{},"values":[[1,"3"]]}]`
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"matrix","result":`+result+`}}`)
	default:
		http.NotFound(w, r)
	}
}

func TestIngestPushesTheFixtureAndWaitsUntilLokiServesIt(t *testing.T) {
	old := ingestPoll
	ingestPoll = time.Millisecond
	t.Cleanup(func() { ingestPoll = old })
	loki := &fixtureLoki{pushStatus: http.StatusNoContent, emptyFor: 1}
	srv := httptest.NewServer(loki)
	defer srv.Close()
	at := time.Unix(1794570675, 0)
	var out bytes.Buffer
	if err := ingestFixture(srv.URL, time.Minute, at, &out); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if loki.pushed.Load() != 1 || loki.probes.Load() != 2 || loki.services.Load() < 2 {
		t.Fatalf("pushes %d, probes %d", loki.pushed.Load(), loki.probes.Load())
	}
	var push struct {
		Streams []struct {
			Stream map[string]string `json:"stream"`
			Values [][]string        `json:"values"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(loki.body.Load().([]byte), &push); err != nil {
		t.Fatal(err)
	}
	if len(push.Streams) != len(fixtureStreams) || push.Streams[0].Stream["app"] != "api-gateway" {
		t.Fatalf("pushed streams %+v", push.Streams)
	}
	for _, s := range push.Streams {
		seen := map[string]bool{}
		for _, v := range s.Values {
			if seen[v[0]] {
				t.Fatalf("stream %v repeats timestamp %s", s.Stream, v[0])
			}
			seen[v[0]] = true
		}
	}
	if !strings.Contains(out.String(), "serves the fixture") {
		t.Fatalf("output %s", out.String())
	}
}

func TestIngestReportsARefusedPush(t *testing.T) {
	srv := httptest.NewServer(&fixtureLoki{pushStatus: http.StatusBadRequest})
	defer srv.Close()
	if err := ingestFixture(srv.URL, time.Minute, time.Now(), io.Discard); err == nil || !strings.Contains(err.Error(), "400") {
		t.Fatalf("err %v", err)
	}
}

// A Loki whose range metric path answers no series, an error or no JSON is not serving.
func TestServingNeedsASeries(t *testing.T) {
	for name, h := range map[string]http.HandlerFunc{
		"empty matrix": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
		},
		"error":    func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "x", http.StatusInternalServerError) },
		"not json": func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "nope") },
	} {
		srv := httptest.NewServer(h)
		if serving(srv.URL, time.Now()) {
			t.Errorf("%s: reported serving", name)
		}
		srv.Close()
	}
	if serving("http://127.0.0.1:1", time.Now()) {
		t.Error("an unreachable Loki reported serving")
	}
}

// A /services answer that is not a list of running modules is not running.
func TestModuleStatesNeedEveryModuleRunning(t *testing.T) {
	for body, want := range map[string]bool{
		"ingester => Running\nruler => Running\n":  true,
		"ingester => Running\nruler => Starting\n": false,
		"": false, "not a list": false,
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, body) }))
		if _, got := moduleStates(srv.URL); got != want {
			t.Errorf("%q: running %v, want %v", body, got, want)
		}
		srv.Close()
	}
	if _, got := moduleStates("http://127.0.0.1:1"); got {
		t.Error("an unreachable Loki reported running modules")
	}
}

// With the shift on, a row is asked with its times moved to now, and its
// stored target keeps the times it was written with.
func TestShiftAsksTheRowsWindowAsOfNow(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "answers.jsonl")
	target := "/loki/api/v1/patterns?query=%7Ba%3D%22b%22%7D&start=1794567075&end=1794570675&step=60"
	row := `{"id":"r9","route":"patterns","method":"GET","target":"` + target + `","headers":["X-Scope-OrgID: 0"],"trigger":"t","loki":{"status":400,"content_type":"text/plain; charset=utf-8","nosniff":"nosniff","body":"old"}}`
	if err := os.WriteFile(path, []byte(`{"_source":"Real Grafana Loki 3.7.9 answers recorded over HTTP on 2026-11-13 (x)."}`+"\n"+row+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	old := now
	now = func() time.Time { return time.Unix(1794570675, 0).Add(48 * time.Hour) }
	t.Cleanup(func() { now = old })
	f, err := lokirecorded.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	shifted := f.Rows[0].Shifted(48 * time.Hour)
	if !strings.Contains(shifted.Target, "start=1794739875&end=1794743475") {
		t.Fatalf("shifted target %s", shifted.Target)
	}
	reply := lokirecorded.Reply{Status: 400, ContentType: "text/plain; charset=utf-8", Nosniff: "nosniff", Body: "only label matchers are supported"}
	base := fakeLoki(t, map[string]lokirecorded.Reply{shifted.Request(): reply}, "3.7.9")
	changed, err := run(config{loki: base, files: []string{path}, shift: true}, io.Discard)
	if err != nil || changed != 1 {
		t.Fatalf("changed %d (%v)", changed, err)
	}
	if f, err = lokirecorded.Read(path); err != nil {
		t.Fatal(err)
	}
	if f.Rows[0].Loki != reply || f.Rows[0].Target != target {
		t.Fatalf("row %+v", f.Rows[0])
	}
}

// A row with a bound Shifted keeps (a negative start) is sent as recorded, so
// its answer, which can name the window's length, stays the stored target's.
func TestRowWithABoundThatDoesNotMoveIsSentAsRecorded(t *testing.T) {
	for target, want := range map[string]bool{
		"/x?query=a&start=1794567075&end=1794570675":                   true,
		"/x?query=a&time=1794570675":                                   true,
		"/x?query=a&start=1794567075000000000&end=1794570675000000000": true,
		"/x?query=a":                                   true,
		"/x?query=a&start=-1&end=1794570675":           false,
		"/x?query=a&start=abc&end=1794570675":          false,
		"/x?query=a&start=01794567075&end=1794570675":  false,
		"/x?query=a&start=1794567075&end=1794570675.5": false,
		"/x?query=a&start=5&end=1794570675":            false,
	} {
		if got := wholeWindowMoves(lokirecorded.Row{Target: target}); got != want {
			t.Errorf("%s: %v, want %v", target, got, want)
		}
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "answers.jsonl")
	target := "/loki/api/v1/labels?start=-1&end=1794570675"
	row := `{"id":"r9","route":"labels","method":"GET","target":"` + target + `","headers":["X-Scope-OrgID: 0"],"trigger":"t","loki":{"status":400,"content_type":"text/plain; charset=utf-8","nosniff":"nosniff","body":"old"}}`
	if err := os.WriteFile(path, []byte(`{"_source":"Real Grafana Loki 3.7.9 answers recorded over HTTP on 2026-11-13 (x)."}`+"\n"+row+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	old := now
	now = func() time.Time { return time.Unix(1794570675, 0).Add(48 * time.Hour) }
	t.Cleanup(func() { now = old })
	f, err := lokirecorded.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	reply := lokirecorded.Reply{Status: 400, ContentType: "text/plain; charset=utf-8", Nosniff: "nosniff", Body: "length of the recorded window"}
	base := fakeLoki(t, map[string]lokirecorded.Reply{f.Rows[0].Request(): reply}, "3.7.9")
	if changed, err := run(config{loki: base, files: []string{path}, shift: true}, io.Discard); err != nil || changed != 1 {
		t.Fatalf("changed %d (%v)", changed, err)
	}
	if f, err = lokirecorded.Read(path); err != nil || f.Rows[0].Loki != reply {
		t.Fatalf("row %+v (%v)", f.Rows, err)
	}
}
