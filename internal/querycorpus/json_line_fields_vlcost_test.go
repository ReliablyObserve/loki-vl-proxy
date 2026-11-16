package querycorpus_test

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// TestJSONLineFieldsVictoriaLogsCost measures what -json-line-fields-as-line-keys costs VictoriaLogs (v1.53.0 and
// v1.49.0, its own query handlers through the serve oracle): the same reads the proxy sends with the flag off and with
// it on, over a data set of placeholder rows (JSON lines stored as fields) and logfmt rows, median of repeated runs.
// It records nothing and asserts nothing but that both answer; LVP_JSONLINE_VLCOST=1 runs it.
func TestJSONLineFieldsVictoriaLogsCost(t *testing.T) {
	if os.Getenv("LVP_JSONLINE_VLCOST") != "1" {
		t.Skip("a measurement: LVP_JSONLINE_VLCOST=1 runs it")
	}
	rows := jlCostRows(600_000)
	const tenant = 7
	const placeholder = `"missing _msg field; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field"`
	exclusion := func(guard string) string { return `NOT _msg:=` + placeholder + ` OR {` + guard + `!=""}` }
	start, end := metricEnd.Add(-2*time.Hour).Format(time.RFC3339), metricEnd.Format(time.RFC3339)
	split := func(sel, brace, valid string) string {
		return sel + ` | filter ` + brace + ` | replace_regexp ("^\\{", "{\"__vlp\":1,") at _msg | unpack_json fields (__vlp)` + valid +
			` | unpack_json fields (status) keep_original_fields | format if (-__vlp:*) "1" as __vlr | format if (__vlr:*) "" as status | stats by (status) count() if (-__vlr:*) as c, count() if (__vlr:*) as __json_rows`
	}
	braceOff := `_msg:~"^[ \\t\\n\\r]*\\{"`
	braceOn := `(` + braceOff + ` or _msg:=` + placeholder + `)`
	type read struct {
		name, path, query, extra string
	}
	reads := []struct{ off, on read }{
		{
			read{"logfmt unwrap, by stream (off)", "/select/logsql/stats_query_range", `app:="jl" | unpack_logfmt | stats by (_stream) max(amount) as c`, ""},
			read{"logfmt unwrap, by stream (on)", "/select/logsql/stats_query_range", `app:="jl" | unpack_logfmt | stats by (_stream) max(amount) as c`, exclusion("amount")},
		},
		{
			read{"label filter count (off)", "/select/logsql/stats_query_range", `app:="jl" status:="200" | stats by (_stream) count() as c`, ""},
			read{"label filter count (on)", "/select/logsql/stats_query_range", `app:="jl" status:="200" | stats by (_stream) count() as c`, exclusion("status")},
		},
		{
			read{"log rows, label filter (off)", "/select/logsql/query", `app:="jl" status:="200" | sort by (_time desc) | limit 1000`, ""},
			read{"log rows, label filter (on)", "/select/logsql/query", `app:="jl" status:="200" | sort by (_time desc) | limit 1000`, exclusion("status")},
		},
		{
			read{"json split (off)", "/select/logsql/stats_query_range", split(`app:="jl" fmt:="json"`, braceOff, ""), ""},
			read{"json split (on)", "/select/logsql/stats_query_range", split(`app:="jl" fmt:="json"`, braceOn, ` | format if (_msg:=`+placeholder+`) "1" as __vlp`), ""},
		},
		// The same answer either way: what the flag's filters cost on rows they do not change.
		{
			read{"same answer: json split, kept lines (off)", "/select/logsql/stats_query_range", split(`app:="jl" fmt:="jsonline"`, braceOff, ""), ""},
			read{"same answer: json split, kept lines (on)", "/select/logsql/stats_query_range", split(`app:="jl" fmt:="jsonline"`, braceOn, ` | format if (_msg:=`+placeholder+`) "1" as __vlp`), ""},
		},
		{
			read{"same answer: logfmt unwrap, logfmt rows (off)", "/select/logsql/stats_query_range", `app:="jl" fmt:="logfmt" | unpack_logfmt | stats by (_stream) max(amount) as c`, ""},
			read{"same answer: logfmt unwrap, logfmt rows (on)", "/select/logsql/stats_query_range", `app:="jl" fmt:="logfmt" | unpack_logfmt | stats by (_stream) max(amount) as c`, exclusion("amount")},
		},
		{
			read{"same answer: count, kept lines (off)", "/select/logsql/stats_query_range", `app:="jl" fmt:="jsonline" | stats by (_stream) count() as c`, ""},
			read{"same answer: count, kept lines (on)", "/select/logsql/stats_query_range", `app:="jl" fmt:="jsonline" | stats by (_stream) count() as c`, exclusion("status")},
		},
		{
			read{"json risk probe (off)", "/select/logsql/query", `app:="jl" fmt:="json" | filter -` + braceOff + ` | limit 1`, ""},
			read{"json risk probe (on)", "/select/logsql/query", `app:="jl" fmt:="json" | filter -` + braceOff + ` | limit 1`, `NOT _msg:=` + placeholder},
		},
	}
	runs := 15
	for _, prof := range diffProfiles() {
		srv, err := oraclerun.StartVLServe(filepath.Join("..", "..", "test", "oracle", "vl"), prof.modfile, []oraclerun.ServeDataSet{{Tenant: tenant, Rows: rows}})
		if err != nil {
			t.Fatal(err)
		}
		time1 := func(r read) (time.Duration, int) {
			form := url.Values{"query": {r.query}, "start": {start}, "end": {end}}
			if r.path != "/select/logsql/query" {
				form.Set("step", "300s")
			}
			if r.extra != "" {
				form.Set("extra_filters", r.extra)
			}
			req, _ := http.NewRequest(http.MethodPost, srv.URL+r.path, strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set("AccountID", strconv.Itoa(tenant))
			req.Header.Set("ProjectID", "0")
			t0 := time.Now()
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("%s %s: %d %s", prof.version, r.name, resp.StatusCode, body)
			}
			return time.Since(t0), len(body)
		}
		median := func(r read) (time.Duration, int) {
			var ds []time.Duration
			size := 0
			time1(r)
			for i := 0; i < runs; i++ {
				d, n := time1(r)
				ds = append(ds, d)
				size = n
			}
			sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
			return ds[len(ds)/2], size
		}
		var b strings.Builder
		for _, pair := range reads {
			// Interleaved: off, on, off, on, so drift on the host lands on both.
			offD, offN := median(pair.off)
			onD, onN := median(pair.on)
			offD2, _ := median(pair.off)
			onD2, _ := median(pair.on)
			off, on := (offD+offD2)/2, (onD+onD2)/2
			fmt.Fprintf(&b, "\n  %-34s off %8s (%6d B)  on %8s (%6d B)  %+6.1f%%", strings.TrimSuffix(pair.off.name, " (off)"), off.Round(time.Microsecond), offN, on.Round(time.Microsecond), onN, 100*(float64(on)-float64(off))/float64(off))
		}
		t.Logf("VictoriaLogs %s, %d rows, median of %d runs, twice interleaved:%s", prof.version, len(rows), runs, b.String())
		srv.Stop()
	}
}

// jlCostRows are n rows over two hours of six streams: two of placeholder rows (a JSON line stored as fields), two of
// logfmt rows and two of JSON lines VictoriaLogs kept (pushed with disable_message_parsing=1), the line in _msg.
func jlCostRows(n int) []oraclerun.ServeRow {
	const placeholder = "missing _msg field; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field"
	first := metricEnd.Add(-2 * time.Hour)
	every := 2 * time.Hour / time.Duration(n)
	out := make([]oraclerun.ServeRow, 0, n)
	for i := 0; i < n; i++ {
		s := i % 6
		stream := [][2]string{{"app", "jl"}, {"pod", string(rune('a' + s))}, {"service_name", "jl"}}
		amount := strconv.Itoa((i * 7919) % 500)
		status := []string{"200", "500"}[i%2]
		row := oraclerun.ServeRow{TS: first.Add(time.Duration(i) * every).UnixNano()}
		switch {
		case s < 2:
			row.Stream = append(stream, [2]string{"fmt", "json"})
			row.Fields = [][2]string{{"amount", amount}, {"route", "/pay"}, {"status", status}, {"_msg", placeholder}}
		case s >= 4:
			row.Stream = append(stream, [2]string{"fmt", "jsonline"})
			row.Fields = [][2]string{{"_msg", `{"amount":"` + amount + `","route":"/pay","status":"` + status + `"}`}}
		default:
			row.Stream = append(stream, [2]string{"fmt", "logfmt"})
			row.Fields = [][2]string{{"_msg", "amount=" + amount + " route=/pay status=" + status}}
		}
		out = append(out, row)
	}
	return out
}
