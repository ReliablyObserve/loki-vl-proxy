package oraclerun

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// ServeRow is one log entry for the VictoriaLogs query server (test/oracle/vl serve): its time in nanoseconds, its
// stream labels (passed first, as the Loki push API passes them) and its other fields.
type ServeRow struct {
	TS     int64       `json:"ts"`
	Stream [][2]string `json:"stream"`
	Fields [][2]string `json:"fields"`
}

// ServeDataSet is the rows of one tenant (an account id).
type ServeDataSet struct {
	Tenant uint32     `json:"tenant"`
	Rows   []ServeRow `json:"rows"`
}

// VLServer is a running VictoriaLogs query server.
type VLServer struct {
	// URL is where it serves /select/*.
	URL  string
	cmd  *exec.Cmd
	in   io.WriteCloser
	work string
}

// StartVLServe builds the serve command of the VictoriaLogs oracle in oracleDir (test/oracle/vl) with modfile (""
// for its go.mod), stores the data sets in it and starts it; it answers once every row is searchable. Stop ends it.
func StartVLServe(oracleDir, modfile string, sets []ServeDataSet) (*VLServer, error) {
	work, err := os.MkdirTemp("", "vl-serve")
	if err != nil {
		return nil, err
	}
	s := &VLServer{work: work}
	fail := func(err error) (*VLServer, error) {
		s.Stop()
		return nil, err
	}
	if modfile != "" {
		modfile = filepath.Join("..", modfile)
	}
	bin, err := Build(filepath.Join(oracleDir, "serve"), modfile, work, "serve")
	if err != nil {
		return fail(err)
	}
	in := filepath.Join(work, "in.jsonl")
	f, err := os.Create(in)
	if err != nil {
		return fail(err)
	}
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	for _, set := range sets {
		if err := enc.Encode(set); err != nil {
			_ = f.Close()
			return fail(err)
		}
	}
	if err := f.Close(); err != nil {
		return fail(err)
	}
	s.cmd = exec.Command(bin, "-in", in) // #nosec G204 -- test-only: bin is an oracle this package built
	s.cmd.Env = Env()
	logPath := filepath.Join(work, "stderr.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		return fail(err)
	}
	defer func() { _ = logFile.Close() }()
	s.cmd.Stderr = logFile
	if s.in, err = s.cmd.StdinPipe(); err != nil {
		return fail(err)
	}
	stdout, err := s.cmd.StdoutPipe()
	if err != nil {
		return fail(err)
	}
	if err := s.cmd.Start(); err != nil {
		return fail(err)
	}
	ready := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if url, ok := strings.CutPrefix(sc.Text(), "listening "); ok {
				ready <- url
			}
		}
		close(ready)
	}()
	select {
	case url, ok := <-ready:
		if !ok {
			_ = s.cmd.Wait()
			log, _ := os.ReadFile(logPath)
			if len(log) > 4000 {
				log = log[len(log)-4000:]
			}
			s.cmd = nil
			return fail(fmt.Errorf("the VictoriaLogs query server stopped before it listened:\n%s", log))
		}
		s.URL = url
	case <-time.After(5 * time.Minute):
		return fail(fmt.Errorf("the VictoriaLogs query server did not listen within 5 minutes"))
	}
	return s, nil
}

// Stop ends the server (closing its stdin asks it to stop; it is killed when it does not within a minute) and
// removes its files.
func (s *VLServer) Stop() {
	if s.cmd != nil && s.cmd.Process != nil {
		_ = s.in.Close()
		done := make(chan struct{})
		go func() { _ = s.cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Minute):
			_ = s.cmd.Process.Kill() // the process this server started, by its own handle
			<-done
		}
	}
	_ = os.RemoveAll(s.work)
}
