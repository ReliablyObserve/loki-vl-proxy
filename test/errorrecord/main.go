// Command errorrecord re-records the answers of a real Loki to the error
// requests of internal/lokierr/testdata (loki_error_answers.jsonl and
// loki_check_order.jsonl) from a running Loki, over HTTP only.
//
// It sends every comparable row's request, byte for byte, to the Loki at -loki
// and rewrites only the row's "loki" object and the date and version of the
// file's header line; every other byte of the files stays. A row that is not
// comparable (it depends on the recorded Loki's configuration, or on an
// authentication the stack does not run) keeps its recorded answer unless -all
// is given. It adds no row and removes none: a new probe is a row written by
// hand, and its answer is recorded by this command.
//
// With -check nothing is written: the command lists the rows whose answer
// changed and exits 1 when there are any. A scheduled run uses it to notice a
// Loki bump, and uploads the rewritten files for review.
//
// Some of Loki's answers move with its state: a Loki that holds data answers a
// few wrong-method and non-selector requests differently from one that has just
// started with none. The answers are recorded from a Loki that serves data:
// -ingest writes a small fixture to the Loki and waits until its range metric
// path answers with it, then exits (run it once, before recording). Before it
// records, the command asks the Loki whether it serves that data, and writes
// the state it found ("serving" or "fresh") into every row it re-records; a
// row whose state changed is reported like a changed answer.
//
// The command is test support. It reads and writes files under the repository
// and talks to the Loki it is given; it starts and stops nothing.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/lokirecorded"
)

const (
	defaultAnswers = "internal/lokierr/testdata/loki_error_answers.jsonl"
	defaultOrder   = "internal/lokierr/testdata/loki_check_order.jsonl"
	rowTimeout     = 60 * time.Second
)

var now = time.Now

type config struct {
	loki    string
	files   []string
	check   bool
	all     bool
	version string
	// state is written into every re-recorded row; empty leaves each row's state as it is.
	state string
}

func main() {
	var cfg config
	var answers, order string
	flag.StringVar(&cfg.loki, "loki", "http://localhost:13101", "base URL of the running Loki to record from (http only)")
	flag.StringVar(&answers, "answers", defaultAnswers, "the recorded answers file")
	flag.StringVar(&order, "order", defaultOrder, "the recorded check-order file; empty to skip it")
	flag.BoolVar(&cfg.check, "check", false, "write nothing; list the rows whose answer changed and exit 1 when any did")
	flag.BoolVar(&cfg.all, "all", false, "also re-record rows marked not comparable")
	flag.StringVar(&cfg.version, "version", "", "Loki version for the header; read from the Loki's buildinfo when empty")
	ingest := flag.Bool("ingest", false, "write the fixture to the Loki, wait until its range metric path serves it, and exit")
	ingestWait := flag.Duration("ingest-wait", 8*time.Minute, "with -ingest: how long to wait for the Loki to serve the fixture")
	flag.Parse()
	if *ingest {
		if err := ingestFixture(cfg.loki, *ingestWait, now(), os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "errorrecord:", err)
			os.Exit(2)
		}
		return
	}
	cfg.state = lokirecorded.StateFresh
	if serving(cfg.loki, now()) {
		cfg.state = lokirecorded.StateServing
	}
	cfg.files = []string{answers}
	if order != "" {
		cfg.files = append(cfg.files, order)
	}
	changed, err := run(cfg, os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, "errorrecord:", err)
		os.Exit(2)
	}
	if cfg.check && changed > 0 {
		os.Exit(1)
	}
}

// run records every file and returns the number of rows whose answer changed.
func run(cfg config, out io.Writer) (int, error) {
	u, err := url.Parse(cfg.loki)
	if err != nil || u.Scheme != "http" || u.Host == "" {
		return 0, fmt.Errorf("-loki %q must be an http base URL (the requests are sent as raw bytes over a plain socket)", cfg.loki)
	}
	version := cfg.version
	if version == "" {
		if version, err = buildVersion(cfg.loki); err != nil {
			return 0, fmt.Errorf("reading the Loki version: %w (pass -version)", err)
		}
	}
	total := 0
	for _, path := range cfg.files {
		n, err := recordFile(path, u.Host, version, cfg, out)
		if err != nil {
			return total, fmt.Errorf("%s: %w", path, err)
		}
		total += n
	}
	return total, nil
}

func recordFile(path, host, version string, cfg config, out io.Writer) (int, error) {
	file, err := lokirecorded.Read(path)
	if err != nil {
		return 0, err
	}
	lines := make([]string, len(file.Lines))
	changed, kept := 0, 0
	for i, row := range file.Rows {
		lines[i] = file.Lines[i]
		// A row added by hand with no answer yet (status 0) is recorded whatever it is marked.
		if !row.Comparable() && !cfg.all && row.Loki.Status != 0 {
			kept++
			continue
		}
		reply, err := lokirecorded.Replay(host, row, rowTimeout)
		if err != nil {
			return changed, err
		}
		reply = reply.ForRecording()
		stateMoved := cfg.state != "" && cfg.state != row.State
		if reply == row.Loki && !stateMoved {
			continue
		}
		changed++
		if reply != row.Loki {
			fmt.Fprintf(out, "%s %s %s\n  recorded %s\n  now      %s\n", row.ID, row.Route, row.Trigger, row.Loki.Summary(), reply.Summary())
			if lines[i], err = lokirecorded.WithLoki(lines[i], reply); err != nil {
				return changed, fmt.Errorf("%s: %w", row.ID, err)
			}
		}
		if stateMoved {
			fmt.Fprintf(out, "%s %s recorded from a %s Loki (was %q)\n", row.ID, row.Route, cfg.state, row.State)
			if lines[i], err = lokirecorded.WithState(lines[i], cfg.state); err != nil {
				return changed, fmt.Errorf("%s: %w", row.ID, err)
			}
		}
	}
	fmt.Fprintf(out, "%s: %d rows, %d answers or states changed, %d not re-recorded (not comparable), recorded from a %q Loki\n", path, len(file.Rows), changed, kept, cfg.state)
	header := file.Header
	if changed > 0 || !strings.Contains(header, "Loki "+version+" answers recorded") {
		header = lokirecorded.WithHeader(header, version, now())
	}
	if cfg.check {
		return changed, nil
	}
	if changed == 0 && header == file.Header {
		return 0, nil
	}
	first, err := lokirecorded.HeaderLine(header)
	if err != nil {
		return changed, err
	}
	text := first + "\n" + strings.Join(lines, "\n") + "\n"
	return changed, os.WriteFile(path, []byte(text), 0o644)
}

// buildVersion is the version a Loki reports on /loki/api/v1/status/buildinfo.
func buildVersion(base string) (string, error) {
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Get(strings.TrimRight(base, "/") + "/loki/api/v1/status/buildinfo")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return "", err
	}
	text := string(raw)
	const key = `"version":"`
	i := strings.Index(text, key)
	if resp.StatusCode != http.StatusOK || i < 0 {
		return "", errors.New("buildinfo did not answer with a version")
	}
	rest := text[i+len(key):]
	j := strings.IndexByte(rest, '"')
	if j <= 0 {
		return "", errors.New("buildinfo version is empty")
	}
	return rest[:j], nil
}
