package casefixtures

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/registry"
)

const registryRoot = "../../conformance/registry"

// selectorsOf are the stream selectors of a LogQL query, as matcher lists.
func selectorsOf(t *testing.T, query string) [][]logql.LabelMatcher {
	t.Helper()
	expr, err := logql.Parse(query)
	if err != nil {
		t.Fatalf("query %q does not parse: %v", query, err)
	}
	var out [][]logql.LabelMatcher
	var walk func(v reflect.Value, depth int)
	walk = func(v reflect.Value, depth int) {
		if depth > 64 || !v.IsValid() {
			return
		}
		if v.Type() == reflect.TypeOf(&logql.StreamSelector{}) {
			if !v.IsNil() {
				out = append(out, v.Interface().(*logql.StreamSelector).Matchers)
			}
			return
		}
		switch v.Kind() {
		case reflect.Pointer, reflect.Interface:
			if !v.IsNil() {
				walk(v.Elem(), depth+1)
			}
		case reflect.Struct:
			for i := 0; i < v.NumField(); i++ {
				if v.Type().Field(i).IsExported() {
					walk(v.Field(i), depth+1)
				}
			}
		case reflect.Slice, reflect.Array:
			for i := 0; i < v.Len(); i++ {
				walk(v.Index(i), depth+1)
			}
		}
	}
	walk(reflect.ValueOf(expr), 0)
	return out
}

// matches reports whether the labels satisfy every matcher as Loki does: a missing label is the empty string and a
// regexp is anchored.
func matches(labels map[string]string, ms []logql.LabelMatcher) bool {
	for _, m := range ms {
		v := labels[m.Name]
		switch m.Op {
		case logql.MatchEq:
			if v != m.Value {
				return false
			}
		case logql.MatchNeq:
			if v == m.Value {
				return false
			}
		case logql.MatchRe, logql.MatchNotRe:
			re, err := regexp.Compile("^(?:" + m.Value + ")$")
			if err != nil {
				return false
			}
			if re.MatchString(v) != (m.Op == logql.MatchRe) {
				return false
			}
		}
	}
	return true
}

// loadCases reads the registry's cases by id.
func loadCases(t *testing.T) map[string]registry.Case {
	t.Helper()
	cases, problems, err := registry.LoadAll(registryRoot)
	if err != nil || len(problems) > 0 {
		t.Fatalf("registry: %v %v", err, problems)
	}
	out := make(map[string]registry.Case, len(cases))
	for _, c := range cases {
		out[c.ID] = c
	}
	return out
}

func params(c registry.Case) map[string]string {
	req, _ := c.Doc["request"].(map[string]any)
	p, _ := req["params"].(map[string]any)
	out := map[string]string{}
	for k, v := range p {
		out[k] = fmt.Sprint(v)
	}
	return out
}

func TestEveryEmptyCaseIsServedOrExplained(t *testing.T) {
	served := map[string]bool{}
	for _, s := range Streams() {
		for _, c := range s.Cases {
			served[c] = true
		}
	}
	listed := map[string]bool{}
	for _, id := range EmptyCases {
		listed[id] = true
		switch {
		case served[id] && Unservable[id] != "":
			t.Errorf("%s is both served by a stream and listed as unservable", id)
		case !served[id] && Unservable[id] == "":
			t.Errorf("%s is empty in the live run and has neither a stream nor a reason", id)
		}
	}
	if len(listed) != len(EmptyCases) || len(EmptyCases) != 49 {
		t.Errorf("EmptyCases holds %d ids (%d distinct), the live run had 49", len(EmptyCases), len(listed))
	}
	for id := range Unservable {
		if !listed[id] {
			t.Errorf("%s is explained but is not one of the empty cases", id)
		}
	}
	for id := range served {
		if !listed[id] {
			t.Errorf("a stream serves %s, which is not one of the empty cases", id)
		}
	}
}

func TestStreamsAreWellFormed(t *testing.T) {
	names := map[string]bool{}
	labelSets := map[string]string{}
	for _, s := range Streams() {
		if s.Name == "" || names[s.Name] {
			t.Errorf("stream name %q is empty or repeated", s.Name)
		}
		names[s.Name] = true
		if len(s.Labels) == 0 || len(s.Entries) == 0 || len(s.Cases)+len(s.Decoys) == 0 {
			t.Errorf("%s: labels, entries and cases are all required", s.Name)
		}
		key := selector(s)
		if other, ok := labelSets[key]; ok {
			t.Errorf("%s and %s share the label set %s", s.Name, other, key)
		}
		labelSets[key] = s.Name
		switch s.Route {
		case RouteJSONLine:
		case RouteLokiPush, RouteLokiPushRaw:
			// The route forwards Loki's own body: nothing in it can differ between the sides.
			for _, e := range s.Entries {
				if len(e.LokiMetadata) > 0 || len(e.VLFields) > 0 {
					t.Errorf("%s: route %s sends one body, so an entry cannot carry a side-only field", s.Name, s.Route)
				}
			}
		default:
			t.Errorf("%s: unknown route %q", s.Name, s.Route)
		}
		for _, e := range s.Entries {
			for k := range e.Metadata {
				if _, ok := s.Labels[k]; ok {
					t.Errorf("%s: metadata key %q is also a stream label", s.Name, k)
				}
			}
		}
	}
}

// Every case a stream lists selects it, and every selector of a served case is selected by a stream listing it:
// nothing here can be a stream no case reads or a case whose selector names a stream that is not written.
func TestStreamsAnswerTheirCasesSelectors(t *testing.T) {
	cases := loadCases(t)
	byCase := map[string][]Stream{}
	for _, s := range Streams() {
		for _, id := range s.Cases {
			byCase[id] = append(byCase[id], s)
		}
	}
	for id, streams := range byCase {
		c, ok := cases[id]
		if !ok {
			t.Errorf("%s is not a registry case", id)
			continue
		}
		query := params(c)["query"]
		if query == "" {
			t.Errorf("%s has no query for a selector to be read from", id)
			continue
		}
		selectors := selectorsOf(t, query)
		if len(selectors) == 0 {
			t.Errorf("%s: no stream selector in %q", id, query)
		}
		for _, ms := range selectors {
			hit := false
			for _, s := range streams {
				hit = hit || matches(s.Labels, ms)
			}
			if !hit {
				t.Errorf("%s: no stream listed for it matches the selector of %q", id, query)
			}
		}
		for _, s := range streams {
			hit := false
			for _, ms := range selectors {
				hit = hit || matches(s.Labels, ms)
			}
			if !hit {
				t.Errorf("%s: stream %s is listed for the case and no selector of %q matches it", id, s.Name, query)
			}
		}
	}
	// A decoy is written for a case and must stay outside its selectors.
	for _, s := range Streams() {
		for _, id := range s.Decoys {
			c, ok := cases[id]
			if !ok {
				t.Errorf("%s: decoy for %s, which is not a registry case", s.Name, id)
				continue
			}
			for _, ms := range selectorsOf(t, params(c)["query"]) {
				if matches(s.Labels, ms) {
					t.Errorf("%s: stream %s is a decoy and the case's selector matches it", id, s.Name)
				}
			}
			if len(byCase[id]) == 0 {
				t.Errorf("%s: a decoy for a case no stream serves", id)
			}
		}
	}
}

// relativeOffset reads now, now-<n><unit> and now+<n><unit> (units s, m, h, d, w) as the verifier does.
func relativeOffset(v string) (time.Duration, bool) {
	if v == "now" {
		return 0, true
	}
	rest, ok := strings.CutPrefix(v, "now")
	if !ok || len(rest) < 3 || (rest[0] != '-' && rest[0] != '+') {
		return 0, false
	}
	unit := map[byte]time.Duration{'s': time.Second, 'm': time.Minute, 'h': time.Hour, 'd': 24 * time.Hour, 'w': 7 * 24 * time.Hour}[rest[len(rest)-1]]
	n, err := strconv.Atoi(rest[1 : len(rest)-1])
	if unit == 0 || err != nil {
		return 0, false
	}
	d := time.Duration(n) * unit
	if rest[0] == '-' {
		d = -d
	}
	return d, true
}

// verifierSettle is the verifier's default distance between its start and the end of every window.
const verifierSettle = 5 * time.Minute

// A served case reads a window that holds a tick of the backlog when the verifier starts as the backlog ends (the
// worst case without -follow): the window ends five minutes before the start and reaches back as far as the case says.
func TestServedCasesWindowsHoldTicks(t *testing.T) {
	cases := loadCases(t)
	at := time.Date(2026, 11, 14, 18, 0, 0, 0, time.UTC)
	ticks := Ticks(at, DefaultSpan, DefaultEvery)
	if len(ticks) != int(DefaultSpan/DefaultEvery) {
		t.Fatalf("%d ticks for a span of %v every %v", len(ticks), DefaultSpan, DefaultEvery)
	}
	start := at // the verifier starts when the backlog ends
	for _, s := range Streams() {
		for _, id := range s.Cases {
			p := params(cases[id])
			from, to := time.Duration(-5*time.Minute), time.Duration(0)
			switch {
			case p["start"] != "":
				f, ok1 := relativeOffset(p["start"])
				e, ok2 := relativeOffset(p["end"])
				if !ok1 || !ok2 {
					t.Errorf("%s: window %q..%q is not relative to now, so no seed made before the run can hold it", id, p["start"], p["end"])
					continue
				}
				from, to = f, e
			case p["time"] != "":
				e, ok := relativeOffset(p["time"])
				if !ok {
					t.Errorf("%s: time %q is not relative to now", id, p["time"])
					continue
				}
				from, to = e-DefaultSpan, e // a range query reads back from its time; the widest range here is the seed's span
			}
			lo, hi := start.Add(-verifierSettle+from), start.Add(-verifierSettle+to)
			n := 0
			for _, tk := range ticks {
				if !tk.Before(lo) && !tk.After(hi) {
					n++
				}
			}
			if n == 0 {
				t.Errorf("%s: its window [%s, %s] holds no tick of stream %s", id, lo.Format(time.RFC3339), hi.Format(time.RFC3339), s.Name)
			}
		}
	}
}

func TestTicks(t *testing.T) {
	at := time.Date(2026, 11, 14, 18, 0, 17, 123456789, time.UTC)
	ticks := Ticks(at, 10*time.Minute, 30*time.Second)
	if len(ticks) != 20 {
		t.Fatalf("%d ticks, want 20", len(ticks))
	}
	jitter := time.Duration(at.UnixNano() % int64(time.Millisecond))
	for i, tk := range ticks {
		if tk.After(at) {
			t.Errorf("tick %d (%s) is ahead of the clock", i, tk)
		}
		if got := tk.Sub(tk.Truncate(30 * time.Second)); got != 500*time.Millisecond+jitter {
			t.Errorf("tick %d sits %v past its boundary, want half a second plus the jitter %v", i, got, jitter)
		}
		if i > 0 && tk.Sub(ticks[i-1]) != 30*time.Second {
			t.Errorf("tick %d is %v after the previous one", i, tk.Sub(ticks[i-1]))
		}
	}
	other := Ticks(at.Add(time.Second+time.Nanosecond), 10*time.Minute, 30*time.Second)
	seen := map[time.Time]bool{}
	for _, tk := range ticks {
		seen[tk] = true
	}
	for _, tk := range other {
		if seen[tk] {
			t.Errorf("a second seed repeats the timestamp %s", tk)
		}
	}
	if Ticks(at, time.Second, time.Minute) != nil || Ticks(at, time.Minute, 0) != nil {
		t.Error("a span shorter than a tick, or no tick, gives no ticks")
	}
}

func TestBodiesCarryTheSameLinesAndMetadata(t *testing.T) {
	at := time.Date(2026, 11, 14, 18, 0, 0, 0, time.UTC)
	ticks := Ticks(at, 2*time.Minute, 30*time.Second)
	for _, s := range Streams() {
		lokiBody, err := LokiBody(s, ticks)
		if err != nil {
			t.Fatal(err)
		}
		var loki struct {
			Streams []struct {
				Stream map[string]string `json:"stream"`
				Values [][]any           `json:"values"`
			} `json:"streams"`
		}
		if err := json.Unmarshal(lokiBody, &loki); err != nil || len(loki.Streams) != 1 {
			t.Fatalf("%s: Loki body: %v", s.Name, err)
		}
		if !reflect.DeepEqual(loki.Streams[0].Stream, s.Labels) {
			t.Errorf("%s: Loki stream labels %v, want %v", s.Name, loki.Streams[0].Stream, s.Labels)
		}
		vlBody, err := JSONLineBody(s, ticks)
		if err != nil {
			t.Fatal(err)
		}
		var rows []map[string]string
		for _, line := range bytes.Split(bytes.TrimSpace(vlBody), []byte("\n")) {
			var row map[string]string
			if err := json.Unmarshal(line, &row); err != nil {
				t.Fatalf("%s: VictoriaLogs row %q: %v", s.Name, line, err)
			}
			rows = append(rows, row)
		}
		values := loki.Streams[0].Values
		if len(values) != len(rows) || len(values) != len(ticks)*len(s.Entries) {
			t.Fatalf("%s: %d Loki values, %d VictoriaLogs rows, %d expected", s.Name, len(values), len(rows), len(ticks)*len(s.Entries))
		}
		seenTS := map[string]bool{}
		for i, v := range values {
			ts := v[0].(string)
			if seenTS[ts] {
				t.Errorf("%s: timestamp %s is used twice", s.Name, ts)
			}
			seenTS[ts] = true
			ns, _ := strconv.ParseInt(ts, 10, 64)
			row := rows[i]
			rowTime, err := time.Parse(time.RFC3339Nano, row["_time"])
			if err != nil || rowTime.UnixNano() != ns {
				t.Errorf("%s: row %d is at %s, Loki at %s", s.Name, i, row["_time"], ts)
			}
			if row["_msg"] != v[1].(string) {
				t.Errorf("%s: row %d line %q, Loki %q", s.Name, i, row["_msg"], v[1])
			}
			for k, want := range s.Labels {
				if row[k] != want {
					t.Errorf("%s: row %d field %s=%q, want label %q", s.Name, i, k, row[k], want)
				}
			}
			entry := s.Entries[i%len(s.Entries)]
			for k, want := range entry.Metadata {
				if row[k] != want {
					t.Errorf("%s: row %d lacks the metadata %s=%q", s.Name, i, k, want)
				}
				if md, ok := lokiMetadataOf(v); !ok || md[k] != want {
					t.Errorf("%s: Loki value %d lacks the metadata %s=%q", s.Name, i, k, want)
				}
			}
			if len(entry.lokiMetadata()) == 0 && len(v) != 2 {
				t.Errorf("%s: Loki value %d carries metadata the entry does not hold", s.Name, i)
			}
		}
	}
}

func lokiMetadataOf(v []any) (map[string]string, bool) {
	if len(v) != 3 {
		return nil, false
	}
	raw, ok := v[2].(map[string]any)
	if !ok {
		return nil, false
	}
	out := map[string]string{}
	for k, e := range raw {
		out[k] = fmt.Sprint(e)
	}
	return out, true
}

// stackDouble records what a seed writes and answers counts like a stack that holds every line it was given.
type stackDouble struct {
	mu     sync.Mutex
	loki   map[string][]byte // body by stream label set
	vl     []string          // route and query of each VictoriaLogs write
	vlBody [][]byte
	lines  map[string]int
	tenant string
	flush  int
	fail   bool
}

func (d *stackDouble) handler(ls Stream) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		d.mu.Lock()
		defer d.mu.Unlock()
		body, _ := io.ReadAll(r.Body)
		switch {
		case d.fail:
			http.Error(w, "boom", http.StatusInternalServerError)
		case r.URL.Path == "/loki/api/v1/push":
			d.tenant = r.Header.Get("X-Scope-OrgID")
			d.loki[ls.Name] = body
			var b struct {
				Streams []struct {
					Values [][]any `json:"values"`
				} `json:"streams"`
			}
			_ = json.Unmarshal(body, &b)
			d.lines[ls.Name] = len(b.Streams[0].Values)
			w.WriteHeader(http.StatusNoContent)
		case strings.HasPrefix(r.URL.Path, "/insert/"):
			d.vl = append(d.vl, r.URL.Path+"?"+r.URL.RawQuery+" "+r.Header.Get("Content-Type"))
			d.vlBody = append(d.vlBody, body)
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/internal/force_flush":
			d.flush++
		case r.URL.Path == "/loki/api/v1/query":
			fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"%d"]}]}}`, d.lines[ls.Name])
		case r.URL.Path == "/select/logsql/query":
			fmt.Fprintf(w, `{"c":"%d"}`+"\n", d.lines[ls.Name])
		default:
			http.NotFound(w, r)
		}
	})
}

func TestPushWritesTheSameBodiesToBothSides(t *testing.T) {
	at := time.Date(2026, 11, 14, 18, 0, 0, 0, time.UTC)
	ticks := Ticks(at, time.Minute, 30*time.Second)
	for _, route := range []Route{RouteJSONLine, RouteLokiPushRaw, RouteLokiPush} {
		s := Stream{Name: "t", Cases: []string{"c"}, Labels: map[string]string{"app": "t", "b": "2"}, Route: route,
			Entries: []Entry{{Line: "one", Metadata: map[string]string{"m": "1"}}, {Line: "two"}}}
		d := &stackDouble{loki: map[string][]byte{}, lines: map[string]int{}}
		srv := httptest.NewServer(d.handler(s))
		target := Target{Loki: srv.URL, VL: srv.URL, Tenant: "0"}
		if err := target.Push(context.Background(), s, ticks); err != nil {
			t.Fatalf("%s: %v", route, err)
		}
		target.Flush(context.Background())
		srv.Close()
		if d.tenant != "0" {
			t.Errorf("%s: Loki got tenant %q", route, d.tenant)
		}
		want, _ := LokiBody(s, ticks)
		if !bytes.Equal(d.loki["t"], want) {
			t.Errorf("%s: Loki body differs from LokiBody", route)
		}
		if len(d.vl) != 1 || d.flush != 1 {
			t.Fatalf("%s: %d VictoriaLogs writes and %d flushes", route, len(d.vl), d.flush)
		}
		switch route {
		case RouteJSONLine:
			jl, _ := JSONLineBody(s, ticks)
			if !bytes.Equal(d.vlBody[0], jl) || !strings.HasPrefix(d.vl[0], "/insert/jsonline?_stream_fields=app%2Cb ") {
				t.Errorf("jsonline write: %s", d.vl[0])
			}
		case RouteLokiPushRaw:
			if !bytes.Equal(d.vlBody[0], want) || !strings.HasPrefix(d.vl[0], "/insert/loki/api/v1/push?disable_message_parsing=1 ") {
				t.Errorf("raw Loki push write: %s", d.vl[0])
			}
		case RouteLokiPush:
			if !bytes.Equal(d.vlBody[0], want) || !strings.HasPrefix(d.vl[0], "/insert/loki/api/v1/push? ") {
				t.Errorf("parsing Loki push write: %s", d.vl[0])
			}
		}
	}
	if err := (Target{}).Push(context.Background(), Stream{}, nil); err != nil {
		t.Errorf("no ticks write nothing: %v", err)
	}
}

func TestPushReportsARefusal(t *testing.T) {
	s := Streams()[0]
	d := &stackDouble{loki: map[string][]byte{}, lines: map[string]int{}, fail: true}
	srv := httptest.NewServer(d.handler(s))
	defer srv.Close()
	ticks := Ticks(time.Now(), time.Minute, 30*time.Second)
	err := Target{Loki: srv.URL, VL: srv.URL}.Push(context.Background(), s, ticks)
	if err == nil || !strings.Contains(err.Error(), "to Loki") {
		t.Errorf("a refused Loki push is an error naming Loki: %v", err)
	}
	// Loki accepts, VictoriaLogs refuses.
	split := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/insert/") {
			http.Error(w, "no", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer split.Close()
	err = Target{Loki: split.URL, VL: split.URL}.Push(context.Background(), s, ticks)
	if err == nil || !strings.Contains(err.Error(), "to VictoriaLogs") {
		t.Errorf("a refused VictoriaLogs push is an error naming VictoriaLogs: %v", err)
	}
}

func TestWaitReadyWaitsForEqualCounts(t *testing.T) {
	s := Stream{Name: "t", Labels: map[string]string{"app": "t"}, Entries: []Entry{{Line: "a"}}}
	var mu sync.Mutex
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/loki/api/v1/query":
			calls++
			n := 0
			if calls > 1 { // the first answer is Loki's blank window
				n = 4
			}
			if n == 0 {
				fmt.Fprint(w, `{"status":"success","data":{"resultType":"vector","result":[]}}`)
				return
			}
			fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"%d"]}]}}`, n)
		case "/select/logsql/query":
			fmt.Fprint(w, "{\"c\":\"4\"}\n")
		}
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	l, v, err := Target{Loki: srv.URL, VL: srv.URL, Tenant: "0"}.WaitReady(ctx, s, time.Now(), 10*time.Minute, 4)
	if err != nil || l != 4 || v != 4 || calls < 2 {
		t.Errorf("WaitReady = %v %v %v after %d Loki counts", l, v, err, calls)
	}
	short, stop := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer stop()
	if _, _, err := (Target{Loki: srv.URL, VL: srv.URL}).WaitReady(short, s, time.Now(), time.Minute, 99); err == nil {
		t.Error("counts below the wanted ones end in an error when the context ends")
	}
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "x", http.StatusBadGateway) }))
	defer bad.Close()
	if _, _, err := (Target{Loki: bad.URL, VL: bad.URL}).Counts(context.Background(), s, time.Now(), time.Minute); err == nil {
		t.Error("a Loki that answers 502 is an error")
	}
	vlBad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/loki/api/v1/query" {
			fmt.Fprint(w, `{"data":{"result":[]}}`)
			return
		}
		http.Error(w, "x", http.StatusBadGateway)
	}))
	defer vlBad.Close()
	if _, _, err := (Target{Loki: vlBad.URL, VL: vlBad.URL}).Counts(context.Background(), s, time.Now(), time.Minute); err == nil {
		t.Error("a VictoriaLogs that answers 502 is an error")
	}
}

func TestCountQueriesNameTheStreamsLabels(t *testing.T) {
	s := Stream{Labels: map[string]string{"b": "2", "a": `x"y`}}
	if got := selector(s); got != `{a="x\"y",b="2"}` {
		t.Errorf("selector = %s", got)
	}
	if got := vlFilter(s); got != `a:="x\"y" b:="2"` {
		t.Errorf("vlFilter = %s", got)
	}
	u, _ := url.Parse("http://x/?" + url.Values{"query": {vlFilter(s)}}.Encode())
	if u.Query().Get("query") != vlFilter(s) {
		t.Error("the filter survives encoding")
	}
}

// lokiLabels are a stream's labels as Loki holds them (service_name derived from app when absent). Streams()
// already carries it as a label, so this is the identity for every seeded stream; the leak test applies it to the
// raw labels too, to catch a stream written without it.
func lokiLabels(labels map[string]string) map[string]string { return withServiceName(labels) }

// generatorServices are the services the visual stack's generator writes (the selectors of the cases the live run
// proved on that data): a selector that can select one of them is shared with data the cases already read.
var generatorServices = []string{"api-gateway", "payment-service", "cache-redis", "frontend-ssr", "otel-collector", "batch-etl", "nginx-ingress", "checkout-svc"}

// readsGeneratorData reports whether the selector can select a stream of the generator.
func readsGeneratorData(ms []logql.LabelMatcher) bool {
	for _, g := range generatorServices {
		for _, ns := range []string{"prod", "monitoring", "ingress-nginx"} {
			labels := map[string]string{"app": g, "service_name": g, "env": "production", "namespace": ns, "level": "info", "cluster": "us-east-1"}
			if matches(labels, ms) {
				return true
			}
		}
	}
	return false
}

// catchAll reports whether the selector matches every stream: `{service_name=~".+"}` reads whatever is stored.
func catchAll(ms []logql.LabelMatcher) bool {
	return matches(map[string]string{"service_name": "any"}, ms) && matches(map[string]string{"service_name": "other", "app": "z"}, ms)
}

// holdsTicks reports whether a case reads a window the seed's backlog reaches, as TestServedCasesWindowsHoldTicks
// computes it: an absolute window reads no seeded data.
func holdsTicks(p map[string]string) bool {
	for _, k := range []string{"start", "end", "time"} {
		if v, ok := p[k]; ok {
			if _, rel := relativeOffset(v); !rel {
				return false
			}
		}
	}
	if s, ok := p["start"]; ok {
		if d, _ := relativeOffset(s); d < -48*time.Hour {
			return false // further back than the seed reaches
		}
		if e, _ := relativeOffset(p["end"]); e < -DefaultSpan-verifierSettle {
			return false
		}
	}
	return true
}

// A stream must not change what a case that does not list it reads: with Loki's labels (service_name derived from
// app) and with VictoriaLogs' raw fields, no stream may match the selectors of a case that reads relative time and
// is not one of the stream's cases, and no stream may match on one side only (the verifier's counts would differ for
// the case, and every case selecting the same streams).
func TestStreamsLeakIntoNoOtherCase(t *testing.T) {
	cases := loadCases(t)
	for _, s := range Streams() {
		listed := map[string]bool{}
		for _, id := range append(append([]string(nil), s.Cases...), s.Decoys...) {
			listed[id] = true
		}
		for id, c := range cases {
			p := params(c)
			if p["query"] == "" || !holdsTicks(p) {
				continue
			}
			expr, err := logql.Parse(p["query"])
			if err != nil || expr == nil {
				continue
			}
			for _, ms := range selectorsOf(t, p["query"]) {
				onLoki, onVL := matches(lokiLabels(s.Labels), ms), matches(s.Labels, ms)
				if catchAll(ms) {
					continue // reads every stream by design, on both sides alike
				}
				if onLoki != onVL {
					t.Errorf("stream %s is selected by %s on %s only: the two sides would count different lines", s.Name, id, map[bool]string{true: "Loki", false: "VictoriaLogs"}[onLoki])
				}
				// A selector of fixture names alone read nothing before the seed, so the data is new to it; one that
				// can select the generator's streams would see its proven answer change.
				if (onLoki || onVL) && !listed[id] && readsGeneratorData(ms) {
					t.Errorf("stream %s is selected by %s, which does not list it and reads the generator's data too: it would change that case's data", s.Name, id)
				}
			}
		}
	}
}

// Every metric query on {app="x"} reads every stream of the selector. Loki splits a metric query's series by
// structured metadata and the proxy does not, so a stream of that selector with metadata makes each fixed case on it
// differ for a reason of its own.
func TestSharedSelectorStreamsCarryNoStructuredMetadata(t *testing.T) {
	for _, s := range Streams() {
		if s.Labels["app"] != "x" {
			continue
		}
		for _, e := range s.Entries {
			if len(e.Metadata) > 0 || len(e.LokiMetadata) > 0 {
				t.Errorf("stream %s of the shared selector {app=\"x\"} carries structured metadata %v%v", s.Name, e.Metadata, e.LokiMetadata)
			}
		}
	}
}
