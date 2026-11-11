package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// The store command answers what VictoriaLogs stores for some rows and what its queries return over them, through
// lib/logstorage itself: a storage in a temporary directory, LogRows.MustAdd for every row and Storage.RunQuery for
// every query. The ingestion routes hand their rows to MustAdd in one of two ways, which a row picks:
//
//   - Stream set: the Loki push API (stream labels, then structured metadata, then _msg) and OpenTelemetry (resource
//     attributes, then the log record's attributes and _msg) pass the stream labels first and their count, so the
//     stream is those fields (MustAdd with streamFieldsLen = len(Stream)).
//   - Stream unset: jsonline (and the Loki push API with _stream_fields) pass every field and name the stream fields
//     with the request's _stream_fields (StreamFields; MustAdd with -1). jsonline takes the timestamp out of the
//     _time field first (insertutil.ExtractTimestampFromFields empties it), which a row with TimeField does.

// StoreRow is one log entry as an ingestion route hands it to VictoriaLogs.
type StoreRow struct {
	Stream       [][2]string `json:"stream,omitempty"`
	Fields       [][2]string `json:"fields"`
	StreamFields []string    `json:"stream_fields,omitempty"`
	TimeField    bool        `json:"time_field,omitempty"`
}

// StoreInput is the data (Rows, read from the first line that has them) and one query over it.
type StoreInput struct {
	ID    string     `json:"id"`
	Rows  []StoreRow `json:"rows,omitempty"`
	Query string     `json:"q,omitempty"`
}

// StoreVerdict is VictoriaLogs' answer for one query: an error, or the rows it returns (each a sorted list of
// name=value pairs, _time left out, the rows sorted), and for the first line the rows as stored (query *).
type StoreVerdict struct {
	ID    string     `json:"id"`
	Error string     `json:"error,omitempty"`
	Rows  [][]string `json:"rows,omitempty"`
}

// storeTimestamp is the timestamp of every row: a fixed time inside the storage's retention, so the answers do not
// depend on the day they are recorded.
var storeTimestamp = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()

// storeDay is how VictoriaLogs writes the day of storeTimestamp in _time.
const storeDay = "2026-01-01T00:00:00"

func store(in, out string) error {
	// VictoriaLogs' storage reads its memory limits from the command line flags, which must be parsed first.
	if !flag.Parsed() {
		_ = flag.CommandLine.Parse(nil)
	}
	inputs, err := readStoreInputs(in)
	if err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "vl-store-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	s := logstorage.MustOpenStorage(dir+"/data", &logstorage.StorageConfig{Retention: 100 * 365 * 24 * time.Hour})
	defer s.MustClose()
	tenant := logstorage.TenantID{}
	for _, x := range inputs {
		for i, r := range x.Rows {
			lr := logstorage.GetLogRows(r.StreamFields, nil, nil, nil, "")
			fields := make([]logstorage.Field, 0, len(r.Stream)+len(r.Fields))
			for _, f := range r.Stream {
				fields = append(fields, logstorage.Field{Name: f[0], Value: f[1]})
			}
			for _, f := range r.Fields {
				fields = append(fields, logstorage.Field{Name: f[0], Value: f[1]})
			}
			if r.TimeField {
				for j := range fields {
					if fields[j].Name == "_time" {
						fields[j].Value = ""
						break
					}
				}
			}
			streamLen := -1
			if r.Stream != nil {
				streamLen = len(r.Stream)
			}
			lr.MustAdd(tenant, storeTimestamp+int64(i), fields, streamLen)
			s.MustAddRows(lr)
			logstorage.PutLogRows(lr)
		}
	}
	s.DebugFlush()
	var verdicts []StoreVerdict
	for _, x := range inputs {
		q := x.Query
		if x.Rows != nil && q == "" {
			q = "*"
		}
		if q == "" {
			continue
		}
		verdicts = append(verdicts, runStoreQuery(s, tenant, x.ID, q))
	}
	return writeStoreVerdicts(out, verdicts)
}

func readStoreInputs(in string) ([]StoreInput, error) {
	f, err := os.Open(in)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var inputs []StoreInput
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var x StoreInput
		if err := json.Unmarshal(sc.Bytes(), &x); err != nil {
			return nil, err
		}
		inputs = append(inputs, x)
	}
	return inputs, sc.Err()
}

// runStoreQuery is VictoriaLogs' answer for q over the stored rows. A panic of VictoriaLogs' code is an error.
func runStoreQuery(s *logstorage.Storage, tenant logstorage.TenantID, id, q string) (v StoreVerdict) {
	v.ID = id
	defer func() {
		if p := recover(); p != nil {
			v = StoreVerdict{ID: id, Error: fmt.Sprintf("panic: %v", p)}
		}
	}()
	pq, err := logstorage.ParseQuery(q)
	if err != nil {
		v.Error = err.Error()
		return v
	}
	var mu sync.Mutex
	write := func(_ uint, db *logstorage.DataBlock) {
		cols := db.GetColumns(false)
		mu.Lock()
		defer mu.Unlock()
		for i := 0; i < db.RowsCount(); i++ {
			var row []string
			for _, c := range cols {
				if c.Name == "_time" && strings.HasPrefix(c.Values[i], storeDay) {
					continue // the row's own timestamp
				}
				row = append(row, c.Name+"="+c.Values[i])
			}
			sort.Strings(row)
			v.Rows = append(v.Rows, row)
		}
	}
	qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, []logstorage.TenantID{tenant}, pq, false, nil)
	if err := s.RunQuery(qctx, write); err != nil {
		v.Error = err.Error()
		return v
	}
	sort.Slice(v.Rows, func(i, j int) bool { return fmt.Sprint(v.Rows[i]) < fmt.Sprint(v.Rows[j]) })
	return v
}

// writeStoreVerdicts writes one verdict per line.
func writeStoreVerdicts(out string, verdicts []StoreVerdict) error {
	dst, err := os.Create(out)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(dst)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for _, v := range verdicts {
		if err := enc.Encode(v); err != nil {
			_ = dst.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		_ = dst.Close()
		return err
	}
	return dst.Close()
}
