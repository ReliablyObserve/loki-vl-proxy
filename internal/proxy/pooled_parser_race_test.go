package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// The tests below guard against reading bytes or values that a pooled
// fastjson parser still owns after the parser went back to vlFJParserPool.
// fastjson.Parser.Parse reuses its internal buffer, so another goroutine that
// takes the same parser overwrites anything borrowed from the previous parse.
// Run them with -race: the detector reports the shared write, and the content
// assertions catch values that leaked in from another request's rows.

const (
	pooledParserRaceWorkers = 12
	pooledParserRaceRounds  = 8
)

var pooledParserRaceMarker = regexp.MustCompile(`s[0-9]+x`)

// pooledParserRaceErrors collects failures from worker goroutines.
type pooledParserRaceErrors struct {
	mu   sync.Mutex
	msgs []string
}

func (e *pooledParserRaceErrors) add(format string, args ...interface{}) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.msgs) < 20 {
		e.msgs = append(e.msgs, fmt.Sprintf(format, args...))
	}
}

func (e *pooledParserRaceErrors) report(t *testing.T) {
	t.Helper()
	for _, msg := range e.msgs {
		t.Error(msg)
	}
}

// pooledParserRaceRows renders NDJSON rows whose logfmt _msg values all carry
// the scanner's own marker, so a value from another scan is detectable. The
// padding differs per scanner so an overwritten buffer changes the bytes read.
func pooledParserRaceRows(scanner, rows int) string {
	var b strings.Builder
	pad := strings.Repeat("abcdefgh", 1+scanner%7)
	for i := 0; i < rows; i++ {
		fmt.Fprintf(&b,
			`{"_time":"2026-01-01T00:00:%02d.%09dZ","_stream":"{app=\"race\"}","app":"race","_msg":"marker=s%dx_row%d owner=s%dx pad=%s"}`+"\n",
			i%60, i, scanner, i, scanner, pad)
	}
	return b.String()
}

func TestDetectFieldSummariesStream_ConcurrentScansKeepOwnMessageBytes(t *testing.T) {
	const rows = 150
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scanner, err := strconv.Atoi(r.URL.Query().Get("scanner"))
		if err != nil {
			http.Error(w, "missing scanner", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(pooledParserRaceRows(scanner, rows)))
	}))
	defer vl.Close()

	p := newTestProxy(t, vl.URL)
	client := vl.Client()
	fetch := func(scanner int) (*http.Response, error) {
		return client.Get(vl.URL + "/select/logsql/query?scanner=" + strconv.Itoa(scanner))
	}

	// detected_fields scan: every logfmt value must come from this scan's rows.
	scanFields := func(errs *pooledParserRaceErrors, scanner int) {
		resp, err := fetch(scanner)
		if err != nil {
			errs.add("scanner %d: fetch: %v", scanner, err)
			return
		}
		defer resp.Body.Close()
		_, fieldValues, _ := p.detectFieldSummariesStream(resp.Body)
		want := fmt.Sprintf("s%dx", scanner)
		if got := fieldValues["owner"]; len(got) != 1 || got[0] != want {
			errs.add("scanner %d: owner values = %v, want [%s]", scanner, got, want)
		}
		markers := fieldValues["marker"]
		if len(markers) != rows {
			errs.add("scanner %d: %d marker values, want %d", scanner, len(markers), rows)
		}
		for _, v := range markers {
			if m := pooledParserRaceMarker.FindString(v); m != want {
				errs.add("scanner %d: marker value %q belongs to another scan", scanner, v)
				return
			}
		}
	}
	// detected_labels scan on the same parser pool, as Drilldown issues both at once.
	scanLabels := func(errs *pooledParserRaceErrors, scanner int) {
		resp, err := fetch(scanner)
		if err != nil {
			errs.add("labels scanner %d: fetch: %v", scanner, err)
			return
		}
		defer resp.Body.Close()
		if got := scanDetectedLabelSummariesStream(resp.Body, p.labelTranslator); got["app"] == nil {
			errs.add("labels scanner %d: app label missing", scanner)
		}
	}

	var errs pooledParserRaceErrors
	var wg sync.WaitGroup
	for w := 0; w < pooledParserRaceWorkers; w++ {
		wg.Add(2)
		go func(worker int) {
			defer wg.Done()
			for round := 0; round < pooledParserRaceRounds; round++ {
				scanFields(&errs, worker*100+round)
			}
		}(w)
		go func(worker int) {
			defer wg.Done()
			for round := 0; round < pooledParserRaceRounds; round++ {
				scanLabels(&errs, (pooledParserRaceWorkers+worker)*100+round)
			}
		}(w)
	}
	wg.Wait()
	errs.report(t)
}

func TestHandleSeries_ConcurrentRequestsKeepOwnStreams(t *testing.T) {
	const streamsPerResponse = 64
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		marker := pooledParserRaceMarker.FindString(r.Form.Encode())
		if marker == "" {
			http.Error(w, "missing marker", http.StatusBadRequest)
			return
		}
		var b strings.Builder
		b.WriteString(`{"values":[`)
		for i := 0; i < streamsPerResponse; i++ {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `{"value":"{app=\"%s\",pod=\"%s-pod-%d\"}","hits":1}`, marker, marker, i)
		}
		b.WriteString(`]}`)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(b.String()))
	}))
	defer vl.Close()

	p := newTestProxy(t, vl.URL)

	var errs pooledParserRaceErrors
	var wg sync.WaitGroup
	for w := 0; w < pooledParserRaceWorkers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for round := 0; round < pooledParserRaceRounds; round++ {
				marker := fmt.Sprintf("s%dx", worker*100+round)
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/series?match[]="+
					url.QueryEscape(fmt.Sprintf(`{app="%s"}`, marker)), nil)
				_ = req.ParseForm()
				p.handleSeries(rec, req)
				body := rec.Body.String()
				if rec.Code != http.StatusOK {
					errs.add("marker %s: status %d: %s", marker, rec.Code, body)
					continue
				}
				// Each stream carries the marker in app, pod and the synthetic service_name.
				found := pooledParserRaceMarker.FindAllString(body, -1)
				if len(found) != 3*streamsPerResponse {
					errs.add("marker %s: %d marker labels in response, want %d", marker, len(found), 3*streamsPerResponse)
					continue
				}
				for _, m := range found {
					if m != marker {
						errs.add("marker %s: response carries labels of %s", marker, m)
						break
					}
				}
			}
		}(w)
	}
	wg.Wait()
	errs.report(t)
}
