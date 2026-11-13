// Command serve runs VictoriaLogs' own query HTTP handlers (app/vlselect over app/vlstorage, the code of the
// victoria-logs binary) in this process over a fixed data set, as the VictoriaLogs side of the proxy's metric
// differential (internal/querycorpus plan_loki_vl_metric_diff_test.go).
//
//	go run -modfile=go.v1.49.0.mod ./serve -in datasets.jsonl
//
// Each input line is a data set: a tenant (its account id) and its rows, each with its time in nanoseconds, the
// stream labels (passed first, as the Loki push API passes them) and the other fields (structured metadata, then
// _msg). The rows are stored through vlstorage, the same storage the binary's ingestion routes write to, and the
// command waits until every data set answers its row count; it then prints "listening <url>" on stdout and serves
// /select/* with vlselect.RequestHandler until it is stopped. The proxy's tests send their requests through it and
// record VictoriaLogs' answers, so the stats_query_range and stats_query handlers (step buckets, offsets, the
// [start, end) bounds, the Prometheus-shaped answer) are VictoriaLogs' own, not a copy of them.
//
// The storage lives in a temporary directory removed on exit; its retention is long enough for any fixed time.
// It is a command of its own so the parse oracle (the module's root command) does not compile VictoriaLogs' HTTP
// application packages. Like the rest of this module it is used only in tests.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlselect"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// Row is one log entry: its time in nanoseconds, its stream labels and its other fields.
type Row struct {
	TS     int64       `json:"ts"`
	Stream [][2]string `json:"stream"`
	Fields [][2]string `json:"fields"`
}

// DataSet is the rows of one tenant.
type DataSet struct {
	Tenant uint32 `json:"tenant"`
	Rows   []Row  `json:"rows"`
}

func main() {
	in := flag.String("in", "", "data sets, one per line")
	flag.Parse()
	if err := run(*in); err != nil {
		fmt.Fprintln(os.Stderr, "serve:", err)
		os.Exit(1)
	}
}

func run(in string) error {
	sets, err := readDataSets(in)
	if err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "vl-serve-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	for name, value := range map[string]string{"storageDataPath": dir + "/data", "retentionPeriod": "100y", "search.maxQueryDuration": "10m"} {
		if err := flag.Set(name, value); err != nil {
			return err
		}
	}
	vlstorage.Init()
	defer vlstorage.Stop()
	vlselect.Init()
	defer vlselect.Stop()

	var st vlstorage.Storage
	for _, set := range sets {
		tenant := logstorage.TenantID{AccountID: set.Tenant}
		for _, r := range set.Rows {
			lr := logstorage.GetLogRows(nil, nil, nil, nil, "")
			fields := make([]logstorage.Field, 0, len(r.Stream)+len(r.Fields))
			for _, f := range r.Stream {
				fields = append(fields, logstorage.Field{Name: f[0], Value: f[1]})
			}
			for _, f := range r.Fields {
				fields = append(fields, logstorage.Field{Name: f[0], Value: f[1]})
			}
			lr.MustAdd(tenant, r.TS, fields, len(r.Stream))
			st.MustAddRows(lr)
			logstorage.PutLogRows(lr)
		}
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !vlselect.RequestHandler(w, r) {
			http.NotFound(w, r)
		}
	})
	// The storage's own forced flush (/internal/force_flush), then a wait until every row is found through its
	// stream: rows added are searchable a little before their streams' index entries are.
	flush := httptest.NewRecorder()
	if !vlstorage.RequestHandler(flush, httptest.NewRequest(http.MethodPost, "/internal/force_flush", nil)) || flush.Code != http.StatusOK {
		return fmt.Errorf("force flush: status %d: %s", flush.Code, flush.Body.String())
	}
	if err := waitSearchable(handler, sets); err != nil {
		return err
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: handler, ReadHeaderTimeout: time.Minute}
	go func() { _ = srv.Serve(ln) }()
	fmt.Printf("listening http://%s\n", ln.Addr())
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	// The caller stops the command with a signal or by closing its stdin.
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
		stop <- syscall.SIGTERM
	}()
	<-stop
	return srv.Close()
}

// waitSearchable waits until every data set's rows are all found through their streams (VictoriaLogs makes added
// rows and their stream index entries visible to queries after a short delay).
func waitSearchable(h http.Handler, sets []DataSet) error {
	deadline := time.Now().Add(time.Minute)
	for _, set := range sets {
		if len(set.Rows) == 0 {
			continue
		}
		for {
			n, err := countRows(h, set.Tenant)
			if err != nil {
				return err
			}
			if n == len(set.Rows) {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("tenant %d: %d of %d rows searchable", set.Tenant, n, len(set.Rows))
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	return nil
}

// countRows counts a tenant's rows by stream: a row whose stream is not indexed yet is not counted.
func countRows(h http.Handler, tenant uint32) (int, error) {
	form := url.Values{"query": {"* | stats by (_stream) count() as n"}}
	req := httptest.NewRequest(http.MethodPost, "/select/logsql/query", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("AccountID", strconv.FormatUint(uint64(tenant), 10))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		return 0, fmt.Errorf("count rows: status %d: %s", rec.Code, rec.Body.String())
	}
	total := 0
	for _, line := range strings.Split(strings.TrimSpace(rec.Body.String()), "\n") {
		if line == "" {
			continue
		}
		var row struct {
			Stream string `json:"_stream"`
			N      string `json:"n"`
		}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return 0, fmt.Errorf("count rows: %v: %q", err, line)
		}
		if row.Stream == "" || row.Stream == "{}" {
			continue
		}
		n, err := strconv.Atoi(row.N)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

func readDataSets(in string) ([]DataSet, error) {
	f, err := os.Open(in)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }() // read only: a close error loses nothing
	var sets []DataSet
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<28)
	for sc.Scan() {
		var s DataSet
		if err := json.Unmarshal(sc.Bytes(), &s); err != nil {
			return nil, err
		}
		sets = append(sets, s)
	}
	return sets, sc.Err()
}
