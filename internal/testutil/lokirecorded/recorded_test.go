package lokirecorded

import (
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

const answersPath = "../../lokierr/testdata/loki_error_answers.jsonl"

func TestReadCommittedFile(t *testing.T) {
	f, err := Read(answersPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(f.Header, "answers recorded over HTTP on ") {
		t.Fatalf("header %q has no recording date", f.Header)
	}
	if len(f.Rows) == 0 || len(f.Rows) != len(f.Lines) {
		t.Fatalf("rows %d, lines %d", len(f.Rows), len(f.Lines))
	}
	seen := map[string]bool{}
	for _, r := range f.Rows {
		if r.ID == "" || seen[r.ID] {
			t.Fatalf("row id %q is empty or repeated", r.ID)
		}
		seen[r.ID] = true
	}
}

// Rewriting a row's line with its own answer must give the line back: the
// recorder then changes a file only where Loki's answer changed.
func TestWithLokiKeepsEveryLine(t *testing.T) {
	f, err := Read(answersPath)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range f.Rows {
		got, err := WithLoki(f.Lines[i], r.Loki)
		if err != nil {
			t.Fatalf("%s: %v", r.ID, err)
		}
		if got != f.Lines[i] {
			t.Fatalf("%s: line changed\n got %.300s\nwant %.300s", r.ID, got, f.Lines[i])
		}
	}
}

func TestWithLokiReplacesOnlyTheAnswer(t *testing.T) {
	line := `{"id":"r1","loki":{"status":400,"content_type":"text/plain","nosniff":"nosniff","body":"a"},"msg":"parse","args":{"x":1}}`
	got, err := WithLoki(line, Reply{Status: 404, ContentType: "text/plain", Body: "<b> & c"})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"id":"r1","loki":{"status":404,"content_type":"text/plain","nosniff":"","body":"<b> & c"},"msg":"parse","args":{"x":1}}`
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestHeaderVersionAndDate(t *testing.T) {
	src := "Real Grafana Loki 3.7.9 answers recorded over HTTP on 2026-11-13 (single binary)."
	got := WithHeader(src, "3.8.0", time.Date(2027, 1, 2, 0, 0, 0, 0, time.UTC))
	want := "Real Grafana Loki 3.8.0 answers recorded over HTTP on 2027-01-02 (single binary)."
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if WithHeader("no marker", "1", time.Now()) != "no marker" {
		t.Fatal("a header without the marker must stay as it is")
	}
}

func TestForRecordingCutsSuccessBodiesOnly(t *testing.T) {
	long := strings.Repeat("x", 300)
	ok := Reply{Status: 200, Body: long}.ForRecording()
	if len(ok.Body) != SuccessBodyLimit || !ok.BodyTruncated {
		t.Fatalf("success body %d bytes truncated=%v", len(ok.Body), ok.BodyTruncated)
	}
	bad := Reply{Status: 400, Body: long}.ForRecording()
	if len(bad.Body) != 300 || bad.BodyTruncated {
		t.Fatalf("error body %d bytes truncated=%v", len(bad.Body), bad.BodyTruncated)
	}
}

func TestShiftedMovesOnlyPlainTimes(t *testing.T) {
	r := Row{Target: "/loki/api/v1/query_range?query=%7B%7D&start=1794567075&end=1794570675&step=60&limit=5"}
	got := r.Shifted(2 * time.Hour).Target
	want := "/loki/api/v1/query_range?query=%7B%7D&start=1794574275&end=1794577875&step=60&limit=5"
	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
	for _, target := range []string{
		"/loki/api/v1/labels?start=abc&end=-5",
		"/loki/api/v1/labels?start=01794567075",
		"/loki/api/v1/query?time=1.5",
		"/loki/api/v1/labels",
		"/loki/api/v1/labels?start=1794567075;end=1",
	} {
		r := Row{Target: target}
		shifted := r.Shifted(time.Hour).Target
		if target == "/loki/api/v1/labels?start=1794567075;end=1" {
			// A semicolon keeps the whole value non-numeric.
			if shifted != target {
				t.Fatalf("%s changed to %s", target, shifted)
			}
			continue
		}
		if shifted != target {
			t.Fatalf("%s changed to %s", target, shifted)
		}
	}
	ns := Row{Target: "/x?start=1794567075000000000"}.Shifted(time.Hour).Target
	if ns != "/x?start=1794570675000000000" {
		t.Fatalf("nanoseconds: %s", ns)
	}
}

func TestRequestKeepsTargetBytesAndHeaders(t *testing.T) {
	r := Row{Method: "GET", Target: "/loki/api/v1/labels?start=%zz", Headers: []string{"X-Scope-OrgID: 0"}}
	got := r.Request()
	want := "GET /loki/api/v1/labels?start=%zz HTTP/1.1\r\nHost: loki\r\nX-Scope-OrgID: 0\r\nConnection: close\r\n\r\n"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	post := Row{Method: "POST", Target: "/x", Form: "query=a"}.Request()
	if !strings.Contains(post, "Content-Length: 7\r\n") || !strings.HasSuffix(post, "\r\n\r\nquery=a") {
		t.Fatalf("post request %q", post)
	}
	big := Row{Method: "POST", Target: "/x", Generate: &Rebuild{FormBytes: 1000}}.Request()
	if !strings.Contains(big, "Content-Length: 1000\r\n") {
		t.Fatalf("generated form body not sized: %.120q", big)
	}
}

func TestReplayReadsAnswerOverARawSocket(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	got := make(chan string, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		buf := make([]byte, 4096)
		n, _ := c.Read(buf)
		got <- string(buf[:n])
		_, _ = c.Write([]byte("HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain; charset=utf-8\r\nX-Content-Type-Options: nosniff\r\nContent-Length: 4\r\nConnection: close\r\n\r\nnope"))
	}()
	reply, err := Replay(ln.Addr().String(), Row{ID: "r1", Method: http.MethodGet, Target: "/a/./b?x=%zz"}, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if reply != (Reply{Status: 400, ContentType: "text/plain; charset=utf-8", Nosniff: "nosniff", Body: "nope"}) {
		t.Fatalf("reply %+v", reply)
	}
	if line := strings.SplitN(<-got, "\r\n", 2)[0]; line != "GET /a/./b?x=%zz HTTP/1.1" {
		t.Fatalf("request line %q", line)
	}
}

func TestParseRejectsMalformedFiles(t *testing.T) {
	for name, text := range map[string]string{
		"empty":     "",
		"no source": "{\"x\":1}\n",
		"bad row":   "{\"_source\":\"s\"}\nnot json\n",
	} {
		if _, err := Parse([]byte(text)); err == nil {
			t.Fatalf("%s: no error", name)
		}
	}
	if _, err := Read("does-not-exist.jsonl"); !os.IsNotExist(err) {
		t.Fatalf("missing file: %v", err)
	}
}
