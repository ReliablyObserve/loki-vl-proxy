package casefixtures

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// stack is a Loki and a VictoriaLogs in one server: it counts the lines pushed per label set and answers both
// sides' counts from them, so a seed can be run against it without a real stack.
type stack struct {
	mu     sync.Mutex
	loki   map[string]int // selector text -> lines Loki holds
	vl     map[string]int // vlFilter text -> lines VictoriaLogs holds
	refuse string         // a stream name whose VictoriaLogs write is refused
	posts  int
}

func (st *stack) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	st.mu.Lock()
	defer st.mu.Unlock()
	body, _ := io.ReadAll(r.Body)
	switch {
	case r.URL.Path == "/loki/api/v1/push":
		var b struct {
			Streams []struct {
				Stream map[string]string `json:"stream"`
				Values [][]any           `json:"values"`
			} `json:"streams"`
		}
		_ = json.Unmarshal(body, &b)
		for _, s := range b.Streams {
			st.loki[selector(Stream{Labels: s.Stream})] += len(s.Values)
			st.posts++
		}
		w.WriteHeader(http.StatusNoContent)
	case strings.HasPrefix(r.URL.Path, "/insert/"):
		var labels map[string]string
		if strings.HasPrefix(r.URL.Path, "/insert/jsonline") {
			rows := bytes.Split(bytes.TrimSpace(body), []byte("\n"))
			var first map[string]string
			_ = json.Unmarshal(rows[0], &first)
			labels = map[string]string{}
			for _, f := range strings.Split(r.URL.Query().Get("_stream_fields"), ",") {
				labels[f] = first[f]
			}
			if st.refuse != "" && labels["app"] == st.refuse {
				http.Error(w, "refused", http.StatusBadRequest)
				return
			}
			st.vl[vlFilter(Stream{Labels: labels})] += len(rows)
		} else {
			var b struct {
				Streams []struct {
					Stream map[string]string `json:"stream"`
					Values [][]any           `json:"values"`
				} `json:"streams"`
			}
			_ = json.Unmarshal(body, &b)
			for _, s := range b.Streams {
				if st.refuse != "" && s.Stream["app"] == st.refuse {
					http.Error(w, "refused", http.StatusBadRequest)
					return
				}
				st.vl[vlFilter(Stream{Labels: s.Stream})] += len(s.Values)
			}
		}
		w.WriteHeader(http.StatusOK)
	case r.URL.Path == "/loki/api/v1/query":
		q := r.URL.Query().Get("query")
		n := 0
		for sel, c := range st.loki {
			if strings.Contains(q, sel) {
				n = c
			}
		}
		fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"%d"]}]}}`, n)
	case r.URL.Path == "/select/logsql/query":
		q := r.URL.Query().Get("query")
		n := 0
		for f, c := range st.vl {
			if strings.HasPrefix(q, f+" |") {
				n = c
			}
		}
		fmt.Fprintf(w, "{\"c\":\"%d\"}\n", n)
	}
}

func newStack(t *testing.T) (*stack, Target) {
	t.Helper()
	st := &stack{loki: map[string]int{}, vl: map[string]int{}}
	srv := httptest.NewServer(st)
	t.Cleanup(srv.Close)
	return st, Target{Loki: srv.URL, VL: srv.URL, Tenant: "0"}
}

func TestChoose(t *testing.T) {
	all := Streams()
	if got := Choose(all, ""); len(got) != len(all) {
		t.Errorf("no list keeps every stream: %d of %d", len(got), len(all))
	}
	got := Choose(all, "nginx, ip-line,nothing")
	if len(got) != 2 || got[0].Name != "ip-line" && got[1].Name != "ip-line" {
		t.Errorf("Choose = %v", got)
	}
}

func TestSeedWritesEveryStreamAndWaitsForBothSides(t *testing.T) {
	st, target := newStack(t)
	streams := Choose(Streams(), "ip-line,nginx,visual-detected-level-stage-label,line-fields")
	cfg := Config{Target: target, Span: 2 * time.Minute, Every: 30 * time.Second, Wait: 10 * time.Second}
	var out bytes.Buffer
	if failed := Seed(context.Background(), cfg, streams, time.Now(), &out); len(failed) != 0 {
		t.Fatalf("failed: %v\n%s", failed, out.String())
	}
	for _, s := range streams {
		want := 4 * len(s.Entries)
		if st.loki[selector(s)] != want || st.vl[vlFilter(s)] != want {
			t.Errorf("%s: Loki %d, VictoriaLogs %d lines, want %d", s.Name, st.loki[selector(s)], st.vl[vlFilter(s)], want)
		}
		if !strings.Contains(out.String(), "ready "+s.Name+": ") {
			t.Errorf("no ready line for %s:\n%s", s.Name, out.String())
		}
	}
}

func TestSeedKeepsGoingPastAStreamOneSideRefuses(t *testing.T) {
	st, target := newStack(t)
	st.refuse = "nginx"
	streams := Choose(Streams(), "nginx,ip-line")
	cfg := Config{Target: target, Span: time.Minute, Every: 30 * time.Second, Wait: time.Second}
	var out, errOut bytes.Buffer
	if code := Run(context.Background(), cfg, streams, &out, &errOut); code != 1 {
		t.Errorf("exit code %d, want 1", code)
	}
	if !strings.Contains(errOut.String(), "stream nginx to VictoriaLogs") {
		t.Errorf("the refused stream is not named:\n%s", errOut.String())
	}
	if !strings.Contains(out.String(), "ready ip-line: ") {
		t.Errorf("the stream after the refused one was not written and checked:\n%s", out.String())
	}
}

func TestSeedReportsCountsThatNeverEqual(t *testing.T) {
	_, target := newStack(t)
	// A Loki with no such stream: nothing was pushed, so the counts stay at zero while a count of 99 is wanted.
	s := Streams()[0]
	if _, _, err := target.WaitReady(func() context.Context {
		c, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		t.Cleanup(cancel)
		return c
	}(), s, time.Now(), time.Minute, 99); err == nil {
		t.Error("counts that never reach the wanted number are an error")
	}
}

func TestFollowWritesATickPerIntervalUntilTheContextEnds(t *testing.T) {
	st, target := newStack(t)
	s := Choose(Streams(), "ipprobe-t")
	cfg := Config{Target: target, Span: time.Minute, Every: 20 * time.Millisecond, Follow: true}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var errOut bytes.Buffer
	Follow(ctx, cfg, s, time.Now().Add(-time.Second), &errOut)
	if n := st.loki[selector(s[0])]; n < 3*len(s[0].Entries) {
		t.Errorf("Loki holds %d lines after the follow ran, want at least three ticks", n)
	}
	// A refused write is reported and does not stop the loop.
	st.refuse = "ipprobe-t"
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	Follow(ctx2, cfg, s, time.Now().Add(-time.Second), &errOut)
	if !strings.Contains(errOut.String(), "to VictoriaLogs") {
		t.Errorf("a refused write is not reported: %q", errOut.String())
	}
}

func TestRunFollowsAfterTheBacklog(t *testing.T) {
	st, target := newStack(t)
	s := Choose(Streams(), "ipprobe-t")
	cfg := Config{Target: target, Span: 2 * time.Minute, Every: 30 * time.Second, Wait: 10 * time.Second, Follow: true}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var out, errOut bytes.Buffer
	if code := Run(ctx, cfg, s, &out, &errOut); code != 0 {
		t.Fatalf("exit code %d: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "following: one tick per 30s") {
		t.Errorf("no follow line:\n%s", out.String())
	}
	// Four ticks of backlog; the follow writes the next one at once when the clock is already past it, and a second
	// one when the run starts within the last seconds of a 30 second boundary and the 3 second context spans it.
	if n := st.loki[selector(s[0])]; n < 4*len(s[0].Entries) || n > 6*len(s[0].Entries) {
		t.Errorf("the backlog holds %d lines", n)
	}
}
