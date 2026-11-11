package lower

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// VictoriaLogs clones a query by printing it and parsing the text again (lib/logstorage Query.Clone), and panics
// when the second parse fails: its /select/logsql/query handler does it for every query with a limit (the last-N
// optimization of app/vlstorage RunQuery, GetLastNResultsQuery), and so do the live tail and a cluster's remote
// query. A text VictoriaLogs accepts but cannot read back from its own print restarts the VictoriaLogs process. The
// VictoriaLogs oracle's roundtrip command runs each text through the handler's steps (parse at a timestamp, the
// request's _time filter, the sort and limit pipes, GetLastNResultsQuery) and every clone on the way, with the panic
// recovered: the verdicts of v1.49.0 and v1.53.0 are recorded in testdata/vl-clone-roundtrip.jsonl.gz
// (LVP_RUN_ORACLES=1 records them again; the oracle downloads VictoriaLogs, so set GOMODCACHE to a fresh directory
// when the shared cache holds other copies).
const vlCloneRoundtrip = "testdata/vl-clone-roundtrip.jsonl.gz"

type vlCloneVerdict struct {
	Accept bool   `json:"accept"`
	OK     bool   `json:"ok"`
	Panic  string `json:"panic,omitempty"`
}

type vlCloneRow struct {
	Query string                    `json:"q"`
	V     map[string]vlCloneVerdict `json:"v"`
}

// vlCloneNames are the names the grids put where a label or a field stands: _time, _stream, _stream_id and _msg in
// lower case and other cases, and the words LogsQL reads as keywords, which the lowering quotes.
func vlCloneNames() []string {
	var names []string
	for _, n := range []string{"_time", "_stream", "_stream_id", "_msg"} {
		names = append(names, n, strings.ToUpper(n), strings.ToUpper(n[:2])+n[2:])
	}
	names = append(names, "_TiMe", "_sTrEaM", "_Stream_Id", "_Msg")
	for _, w := range []string{"not", "in", "options", "and", "or", "contains_all", "contains_any", "contains_common_case", "equals_common_case"} {
		names = append(names, w, strings.ToUpper(w), strings.ToUpper(w[:1])+w[1:])
	}
	return names
}

// vlClonePositions are the places of a LogQL query where the lowering writes a label name (W).
var vlClonePositions = []string{
	`{W="a"}`, `{app="a", W="a"}`, `{app="a", W!="a"}`, `{app="a", W=~"a.*"}`, `{app="a", W!~"a"}`, `{app="a", W=""}`,
	`{app="a"} | W="x"`, `{app="a"} | W!="x"`, `{app="a"} | W=~"x.*"`, `{app="a"} | W!~"x"`, `{app="a"} | W>5`,
	`{app="a"} | W>=5s`, `{app="a"} | W<5KB`, `{app="a"} | W=ip("10.0.0.1")`, `{app="a"} | W="x" or b="y"`,
	`{app="a"} |= "z" | W="x"`, `{app="a"} | W>5 | W<10`, `{app="a"} | json | W="x"`, `{app="a"} | logfmt | W>5`,
	`{app="a"} | json x="W" | x="1"`, `{app="a"} | drop W`, `{app="a"} | keep W`, `{app="a"} | label_format x=W`,
	`{app="a"} | label_format W=app`, `sum by (W) (count_over_time({app="a"}[1m]))`,
	`count_over_time({app="a", W="a"}[1m])`, `count_over_time({app="a"} | W="x" [1m])`, `rate({app="a"} | W>5 [1m])`,
	`sum(sum_over_time({app="a"} | logfmt | unwrap W [1m]))`, `sum_over_time({app="a"} | json | unwrap W [1m])`,
	`sum by (app) (rate({app="a"} | logfmt | unwrap duration(W) [5m]))`, `avg_over_time({app="a"} | logfmt | unwrap bytes(W) [1m])`,
	`max_over_time({app="a"} | logfmt | unwrap W [1m]) by (app)`, `quantile_over_time(0.5, {app="a"} | logfmt | unwrap W [1m])`,
	`stdvar_over_time({app="a"} | logfmt | unwrap W [1m])`,
}

// vlCloneSpelling is a query of the spelling grid, and whether VictoriaLogs is expected to accept it and fail on
// its clone.
type vlCloneSpelling struct {
	q     string
	crash bool
}

// vlCloneSpellings is the LogsQL grid of the names alone: each spelling, bare and quoted, in every kind of filter, a
// stream filter and the pipes that name a field. It records which spellings VictoriaLogs reads back from its print:
// a filter on a quoted name in another case (`"_TIME":="x"`) is the one it accepts and then fails on.
func vlCloneSpellings() []vlCloneSpelling {
	filters := []string{
		`app:="a" N:="x"`, `app:="a" N:x`, `app:="a" -N:="x"`, `app:="a" N:~"x"`, `app:="a" N:>5`, `app:="a" N:>=5s`,
		`app:="a" N:range(1, 5)`, `app:="a" N:in("a", "b")`, `app:="a" N:*`, `app:="a" N:i("x")`, `app:="a" (N:="x" or b:="y")`,
		`app:="a" | filter N:="x"`, `app:="a" | format if (N:*) "<a>" as b`,
	}
	pipes := []string{
		`app:="a" | copy N as y`, `app:="a" | copy a as N`, `app:="a" | rename N as y`, `app:="a" | delete N`,
		`app:="a" | fields N`, `app:="a" | stats by (N) count()`,
	}
	var out []vlCloneSpelling
	for _, n := range []string{"_time", "_TIME", "_Time", "_TiMe", "_stream", "_STREAM", "_Stream", "_sTrEaM", "_stream_id", "_STREAM_ID", "_Stream_Id", "_tİme", "_stream_İd", "_msg", "_MSG", "_Msg"} {
		for _, quoted := range []bool{false, true} {
			spelling := n
			if quoted {
				spelling = `"` + n + `"`
			}
			for _, k := range filters {
				out = append(out, vlCloneSpelling{strings.ReplaceAll(k, "N", spelling), quoted && logsql.ReservedFieldNameOtherCase(n)})
			}
			for _, k := range pipes {
				out = append(out, vlCloneSpelling{strings.ReplaceAll(k, "N", spelling), false})
			}
		}
		out = append(out, vlCloneSpelling{fmt.Sprintf(`app:="a" {%s="x"}`, n), false}, vlCloneSpelling{fmt.Sprintf(`app:="a" _stream:{%s=~"x.*"}`, n), false})
	}
	return out
}

// vlCloneProfiles are the profiles the plan grid is lowered under: the plain one, a label function and the stream
// fields, on both VictoriaLogs versions.
func vlCloneProfiles() []parserProfile {
	all := parserProfiles()
	return []parserProfile{all[0], all[2], all[7]}
}

// vlCloneE2EQueries are the queries of test/e2e-compat TestCompat_ReservedFieldNameCaseLikeLoki (svc stands for the
// run's service name): the texts the stack run sends are held to the clone before it runs.
var vlCloneE2EQueries = []string{
	`{service_name="svc"}`, `sum(sum_over_time({service_name="svc"} | logfmt | unwrap _TIME [1m]))`,
	`sum by (service_name) (sum_over_time({service_name="svc"} | logfmt | unwrap _TIME [1m]))`,
	`{service_name="svc"} | logfmt | _Stream_ID="si"`, `{service_name="svc"} | logfmt | _Stream_ID!="si"`,
	`count_over_time({service_name="svc"} | logfmt | _Stream_ID="si" [1m])`,
}

// vlClonePlanQueries are the LogQL queries of the plan grid: every name in every position, the queries of the
// Loki agreement grid (reservedFieldCaseQueries) and of the e2e test.
func vlClonePlanQueries() []string {
	var qs []string
	for _, n := range vlCloneNames() {
		for _, pos := range vlClonePositions {
			qs = append(qs, strings.ReplaceAll(pos, "W", n))
		}
	}
	qs = append(qs, reservedFieldCaseQueries...)
	return append(qs, vlCloneE2EQueries...)
}

// vlClonePlanTexts are the texts the lowering sends to VictoriaLogs for every query of the plan grid.
func vlClonePlanTexts() []string {
	seen := map[string]bool{}
	for _, q := range vlClonePlanQueries() {
		p := rangeAggPlan(q)
		if p == nil {
			continue
		}
		for _, prof := range vlCloneProfiles() {
			var texts []string
			if op, err := PlanQuery(q, p, Profile{Caps: prof.caps, Label: prof.label, StreamFields: prof.stream}); err == nil {
				collectVLTexts(reflect.ValueOf(&op), &texts)
			}
			if legacy, err := translator.TranslateLogQLWithCapabilities(logql.CanonicalizeLogRanges(q), prof.label, prof.stream, prof.caps); err == nil {
				if op, err := translator.DecodePhysical(legacy); err == nil {
					collectVLTexts(reflect.ValueOf(&op), &texts)
				}
			}
			for _, t := range texts {
				// What the proxy sends: every text passes the outbound guard (internal/proxy guardOutboundQuery).
				seen[logsql.GuardReservedFieldCase(t)] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// collectVLTexts appends the VictoriaLogs query texts an operator tree holds.
func collectVLTexts(v reflect.Value, out *[]string) {
	switch v.Kind() {
	case reflect.Interface, reflect.Pointer:
		if !v.IsNil() {
			collectVLTexts(v.Elem(), out)
		}
	case reflect.Struct:
		switch x := v.Interface().(type) {
		case physical.VLQuery:
			*out = append(*out, x.Text)
			return
		case physical.VLStats:
			*out = append(*out, x.Text)
			return
		}
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				collectVLTexts(v.Field(i), out)
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			collectVLTexts(v.Index(i), out)
		}
	}
}

// Every text the proxy sends for a label called _time, _stream, _stream_id or _msg in any case, or after a word
// LogsQL reads as a keyword, in every position of the grid (selector, label filters, comparisons, parsers, drop, keep,
// label_format, groupings, range aggregations and unwrap), survives VictoriaLogs' clone on v1.49.0 and v1.53.0:
// the lowering's and the string translator's texts, each through the outbound guard (logsql.GuardReservedFieldCase,
// which the proxy runs on every query it sends), are none of them a query VictoriaLogs accepts and then panics on. A
// text missing from the recorded verdicts fails the test until they are recorded again. The spelling grid records
// why the guard rewrites a filter on _TIME and the like: VictoriaLogs reads `"_TIME":="x"`, prints it `_TIME:="x"`
// and fails on its own print, where a pipe and a stream filter read the name back as written.
//
// conformance: semantics/reserved-field-name-case-outbound
func TestPlanTextsSurviveVictoriaLogsClone(t *testing.T) {
	plan, spellings := vlClonePlanTexts(), vlCloneSpellings()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		texts := make([]string, 0, len(spellings)+len(plan))
		for _, sp := range spellings {
			texts = append(texts, sp.q)
		}
		recordVLCloneRoundtrip(t, append(texts, plan...))
	}
	rows := readVLCloneRoundtrip(t)
	for _, q := range plan {
		r, ok := rows[q]
		if !ok {
			t.Errorf("%s: no recorded verdict: record them again with LVP_RUN_ORACLES=1", q)
			continue
		}
		for _, v := range []string{"v1.49.0", "v1.53.0"} {
			if x := r.V[v]; x.Accept && !x.OK {
				t.Errorf("%s: VictoriaLogs %s accepts it and fails on its clone: %s", q, v, x.Panic)
			}
		}
	}
	if len(plan) < 1000 {
		t.Fatalf("only %d plan texts", len(plan))
	}
	for _, sp := range spellings {
		r, ok := rows[sp.q]
		if !ok {
			t.Fatalf("%s: no recorded verdict: record them again with LVP_RUN_ORACLES=1", sp.q)
		}
		for _, v := range []string{"v1.49.0", "v1.53.0"} {
			if x := r.V[v]; (x.Accept && !x.OK) != sp.crash {
				t.Errorf("%s: VictoriaLogs %s accepts %v, clone ok %v, want a failing clone %v (%s)", sp.q, v, x.Accept, x.OK, sp.crash, x.Panic)
			}
		}
	}
}

func readVLCloneRoundtrip(t *testing.T) map[string]vlCloneRow {
	t.Helper()
	f, err := os.Open(vlCloneRoundtrip)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	rows := map[string]vlCloneRow{}
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var r vlCloneRow
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		rows[r.Query] = r
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return rows
}

func recordVLCloneRoundtrip(t *testing.T, texts []string) {
	t.Helper()
	dir := filepath.Join("..", "..", "..", "test", "oracle", "vl")
	work := t.TempDir()
	in := filepath.Join(work, "in.jsonl")
	var b strings.Builder
	for i, q := range texts {
		line, _ := json.Marshal(map[string]string{"id": fmt.Sprint(i), "q": q})
		b.Write(line)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(in, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	rows := make([]vlCloneRow, len(texts))
	for i, q := range texts {
		rows[i] = vlCloneRow{Query: q, V: map[string]vlCloneVerdict{}}
	}
	for _, v := range []string{"v1.49.0", "v1.53.0"} {
		modfile := ""
		if v == "v1.49.0" {
			modfile = "go.v1.49.0.mod"
		}
		bin, err := oraclerun.Build(dir, modfile, work, "vl-"+v)
		if err != nil {
			t.Fatal(err)
		}
		out := filepath.Join(work, v+".jsonl")
		if err := oraclerun.Run(bin, "roundtrip", "-in", in, "-out", out); err != nil {
			t.Fatal(err)
		}
		f, err := os.Open(out)
		if err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 1<<24)
		for sc.Scan() {
			var x struct {
				ID string `json:"id"`
				vlCloneVerdict
			}
			if err := json.Unmarshal(sc.Bytes(), &x); err != nil {
				t.Fatal(err)
			}
			var i int
			_, _ = fmt.Sscan(x.ID, &i)
			rows[i].V[v] = x.vlCloneVerdict
		}
		_ = f.Close()
	}
	var w strings.Builder
	for _, r := range rows {
		line, _ := json.Marshal(r)
		w.Write(line)
		w.WriteByte('\n')
	}
	plain := filepath.Join(work, "verdicts.jsonl")
	if err := os.WriteFile(plain, []byte(w.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := oraclerun.Gzip(plain, vlCloneRoundtrip); err != nil {
		t.Fatal(err)
	}
}
