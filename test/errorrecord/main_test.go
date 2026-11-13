package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/lokirecorded"
)

const (
	committedAnswers = "../../internal/lokierr/testdata/loki_error_answers.jsonl"
	committedOrder   = "../../internal/lokierr/testdata/loki_check_order.jsonl"
)

// fakeLoki answers on a plain socket with the recorded answer of the row
// whose request bytes it receives, and the buildinfo of a Loki version.
func fakeLoki(t *testing.T, answers map[string]lokirecorded.Reply, version string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serve(conn, answers, version)
		}
	}()
	return "http://" + ln.Addr().String()
}

func serve(conn net.Conn, answers map[string]lokirecorded.Reply, version string) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	r := bufio.NewReader(conn)
	var raw strings.Builder
	length := 0
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		raw.WriteString(line)
		if strings.HasPrefix(strings.ToLower(line), "content-length:") {
			length, _ = strconv.Atoi(strings.TrimSpace(line[len("content-length:"):]))
		}
		if line == "\r\n" {
			break
		}
	}
	body := make([]byte, length)
	_, _ = io.ReadFull(r, body)
	raw.Write(body)
	var reply lokirecorded.Reply
	switch {
	case strings.HasPrefix(raw.String(), "GET /loki/api/v1/status/buildinfo "):
		reply = lokirecorded.Reply{Status: 200, ContentType: "application/json", Body: `{"version":"` + version + `"}`}
	default:
		var ok bool
		if reply, ok = answers[raw.String()]; !ok {
			reply = lokirecorded.Reply{Status: 599, Body: "unknown request"}
		}
	}
	var out bytes.Buffer
	fmt.Fprintf(&out, "HTTP/1.1 %d X\r\n", reply.Status)
	if reply.ContentType != "" {
		fmt.Fprintf(&out, "Content-Type: %s\r\n", reply.ContentType)
	}
	if reply.Nosniff != "" {
		fmt.Fprintf(&out, "X-Content-Type-Options: %s\r\n", reply.Nosniff)
	}
	fmt.Fprintf(&out, "Content-Length: %d\r\nConnection: close\r\n\r\n%s", len(reply.Body), reply.Body)
	_, _ = conn.Write(out.Bytes())
}

// recordedAnswers is the request -> answer map of every comparable row of
// the files.
func recordedAnswers(t *testing.T, paths ...string) map[string]lokirecorded.Reply {
	t.Helper()
	answers := map[string]lokirecorded.Reply{}
	for _, path := range paths {
		f, err := lokirecorded.Read(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range f.Rows {
			answers[row.Request()] = row.Loki
		}
	}
	return answers
}

func copyFiles(t *testing.T) (answers, order string) {
	t.Helper()
	dir := t.TempDir()
	answers, order = filepath.Join(dir, "answers.jsonl"), filepath.Join(dir, "order.jsonl")
	for src, dst := range map[string]string{committedAnswers: answers, committedOrder: order} {
		raw, err := os.ReadFile(src)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dst, raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return answers, order
}

func mustRead(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// A Loki that answers exactly what is recorded leaves both files byte for
// byte as they were: the recorder rewrites a file only where Loki changed.
func TestRecordingTheRecordedAnswersChangesNothing(t *testing.T) {
	answers, order := copyFiles(t)
	before := mustRead(t, answers) + mustRead(t, order)
	base := fakeLoki(t, recordedAnswers(t, answers, order), "3.7.9")
	var out bytes.Buffer
	changed, err := run(config{loki: base, files: []string{answers, order}}, &out)
	if err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if changed != 0 {
		t.Fatalf("%d rows changed against a Loki that answers as recorded:\n%s", changed, out.String())
	}
	if after := mustRead(t, answers) + mustRead(t, order); after != before {
		t.Fatal("the files changed although no answer did")
	}
}

// A changed answer rewrites that row's loki object and the header, nothing else.
func TestRecordingRewritesOnlyTheChangedAnswerAndTheHeader(t *testing.T) {
	answers, order := copyFiles(t)
	f, err := lokirecorded.Read(answers)
	if err != nil {
		t.Fatal(err)
	}
	target := f.Rows[0]
	if !target.Comparable() {
		t.Fatal("the first row is expected to be comparable")
	}
	served := recordedAnswers(t, answers, order)
	changedReply := target.Loki
	changedReply.Body = "a new message"
	served[target.Request()] = changedReply
	base := fakeLoki(t, served, "3.8.0")
	day := time.Date(2027, 2, 3, 12, 0, 0, 0, time.UTC)
	old := now
	now = func() time.Time { return day }
	t.Cleanup(func() { now = old })

	before := strings.Split(mustRead(t, answers), "\n")
	var out bytes.Buffer
	changed, err := run(config{loki: base, files: []string{answers, order}}, &out)
	if err != nil || changed != 1 {
		t.Fatalf("changed %d, err %v\n%s", changed, err, out.String())
	}
	after := strings.Split(mustRead(t, answers), "\n")
	if len(after) != len(before) {
		t.Fatalf("line count %d -> %d", len(before), len(after))
	}
	for i := range before {
		switch i {
		case 0:
			if !strings.Contains(after[0], "Loki 3.8.0 answers recorded over HTTP on 2027-02-03") {
				t.Fatalf("header not updated: %.200s", after[0])
			}
		case 1:
			want := strings.Replace(before[1], `"body":"`+strings.ReplaceAll(target.Loki.Body, `"`, `\"`)+`"`, `"body":"a new message"`, 1)
			if after[1] != want {
				t.Fatalf("row line\n got %s\nwant %s", after[1], want)
			}
		default:
			if after[i] != before[i] {
				t.Fatalf("line %d changed", i+1)
			}
		}
	}
}

func TestCheckWritesNothingAndReportsTheChange(t *testing.T) {
	answers, order := copyFiles(t)
	f, err := lokirecorded.Read(answers)
	if err != nil {
		t.Fatal(err)
	}
	served := recordedAnswers(t, answers, order)
	reply := f.Rows[0].Loki
	reply.Status = 500
	served[f.Rows[0].Request()] = reply
	base := fakeLoki(t, served, "3.7.9")
	before := mustRead(t, answers)
	var out bytes.Buffer
	changed, err := run(config{loki: base, files: []string{answers}, check: true}, &out)
	if err != nil || changed != 1 {
		t.Fatalf("changed %d, err %v", changed, err)
	}
	if !strings.Contains(out.String(), f.Rows[0].ID) || !strings.Contains(out.String(), "now      500") {
		t.Fatalf("report does not name the row and its new answer:\n%s", out.String())
	}
	if mustRead(t, answers) != before {
		t.Fatal("-check wrote the file")
	}
}

func TestRowsNotComparableKeepTheirRecordedAnswer(t *testing.T) {
	answers, _ := copyFiles(t)
	f, err := lokirecorded.Read(answers)
	if err != nil {
		t.Fatal(err)
	}
	// A Loki that answers nothing recorded: every row it is asked changes, none of the others is asked.
	base := fakeLoki(t, map[string]lokirecorded.Reply{}, "3.7.9")
	comparable := 0
	for _, r := range f.Rows {
		if r.Comparable() {
			comparable++
		}
	}
	var out bytes.Buffer
	changed, err := run(config{loki: base, files: []string{answers}, check: true}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if changed != comparable {
		t.Fatalf("%d rows changed, want the %d comparable ones", changed, comparable)
	}
}

func TestOnlyAnHTTPLokiIsAccepted(t *testing.T) {
	for _, base := range []string{"https://loki", "loki:3100", "", "ftp://x"} {
		if _, err := run(config{loki: base, version: "1"}, io.Discard); err == nil {
			t.Fatalf("%q was accepted", base)
		}
	}
}
