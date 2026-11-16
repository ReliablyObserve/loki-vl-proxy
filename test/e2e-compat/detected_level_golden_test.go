//go:build e2e

package e2e_compat

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The proxy's detected_level table (internal/proxy/testdata/detected_level_golden.json) holds inputs and the
// level Loki gives each one. Rows with source loki-live or loki-oracle are answered by a real Loki: each is pushed
// to Loki as its own stream (the row's stream labels plus cr_golden and cr_row, the row's fields as structured
// metadata, the row's msg as the line), or, for a row with an otlp record, through Loki's OTLP endpoint; Loki's
// detected_level is read back with both response encodings.
//
// TestCompat_DetectedLevelGoldenMatchesLoki checks the table against the stack's Loki. With
// LVP_RECORD_DETECTED_LEVEL_GOLDEN=1 it writes Loki's answers into the table instead (want, and for OTLP rows the
// fields VictoriaLogs stores for the record) and the evidence of the run into
// internal/proxy/testdata/detected_level_golden_loki.json (Loki's build, its level limits, every row's push status
// and labels). Rows of other sources describe VictoriaLogs storage shapes Loki never receives and are not pushed.

const dlGoldenPath = "../../internal/proxy/testdata/detected_level_golden.json"
const dlGoldenEvidencePath = "../../internal/proxy/testdata/detected_level_golden_loki.json"

type dlGoldenOTLP struct {
	SeverityNumber int    `json:"severityNumber"`
	SeverityText   string `json:"severityText"`
}

type dlGoldenRow struct {
	Name         string            `json:"name"`
	Stream       map[string]string `json:"stream"`
	Fields       map[string]string `json:"fields"`
	Msg          string            `json:"msg"`
	JSONUnpacked bool              `json:"jsonUnpacked,omitempty"`
	OTLP         *dlGoldenOTLP     `json:"otlp,omitempty"`
	Source       string            `json:"source"`
	Want         string            `json:"want"`
	Note         string            `json:"note,omitempty"`
}

// dlGoldenAnswer is what Loki answered for one row.
type dlGoldenAnswer struct {
	Name       string `json:"name"`
	PushStatus int    `json:"push_status"`
	PushError  string `json:"push_error,omitempty"`
	// Labels of the entry in the default encoding, Metadata in the categorize-labels encoding (stream, structured
	// metadata, parsed), without the cr_golden/cr_row bookkeeping labels.
	Labels     map[string]string            `json:"labels,omitempty"`
	Categories map[string]map[string]string `json:"categories,omitempty"`
	Level      string                       `json:"level"`
	// VictoriaLogs' stored fields of an OTLP row (severity fields and the record's attributes).
	VLFields map[string]string `json:"vl_fields,omitempty"`
}

type dlGoldenEvidence struct {
	Recorded   string            `json:"recorded"`
	Loki       map[string]string `json:"loki_buildinfo"`
	LokiLimits map[string]string `json:"loki_level_limits"`
	VLVersion  string            `json:"victorialogs_version"`
	Rows       []dlGoldenAnswer  `json:"rows"`
}

func dlGoldenPushed(r dlGoldenRow) bool {
	return (r.Source == "loki-live" || r.Source == "loki-oracle") && !r.JSONUnpacked
}

// dlGoldenSampleEvery: the check pushes every dlGoldenSampleEvery-th row of each category (the name up to its
// second slash, e.g. sm/key-level or plain/boundary), the first of each included, about 200 rows; recording and
// LVP_DETECTED_LEVEL_GOLDEN_FULL=1 push every row.
const dlGoldenSampleEvery = 12

// dlGoldenSample marks the rows the check pushes.
func dlGoldenSample(rows []dlGoldenRow, all bool) []bool {
	out := make([]bool, len(rows))
	seen := map[string]int{}
	for i, r := range rows {
		if !dlGoldenPushed(r) {
			continue
		}
		category := r.Name
		if a := strings.IndexByte(category, '/'); a >= 0 {
			if b := strings.IndexByte(category[a+1:], '/'); b >= 0 {
				category = category[:a+1+b]
			}
		}
		out[i] = all || seen[category]%dlGoldenSampleEvery == 0
		seen[category]++
	}
	return out
}

// conformance: severity-detected-level-derivation, severity/recorded-level-table
func TestCompat_DetectedLevelGoldenMatchesLoki(t *testing.T) {
	record := os.Getenv("LVP_RECORD_DETECTED_LEVEL_GOLDEN") == "1"
	raw, err := os.ReadFile(dlGoldenPath)
	if err != nil {
		t.Fatal(err)
	}
	var rows []dlGoldenRow
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatal(err)
	}
	run := fmt.Sprintf("dlgolden%d", time.Now().UnixNano())
	service := run + "-otlp"
	base := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	at := func(i int) time.Time { return base.Add(time.Duration(i) * time.Millisecond) }

	sampled := dlGoldenSample(rows, record || os.Getenv("LVP_DETECTED_LEVEL_GOLDEN_FULL") == "1")
	answers := make([]dlGoldenAnswer, len(rows))
	byStamp := map[string]int{}
	var otlpRows, batchRows []int
	var batch []any
	pushed := 0
	for i, r := range rows {
		answers[i].Name = r.Name
		if !sampled[i] {
			continue
		}
		pushed++
		stamp := strconv.FormatInt(at(i).UnixNano(), 10)
		byStamp[stamp] = i
		if r.OTLP != nil {
			otlpRows = append(otlpRows, i)
			continue
		}
		labels := map[string]string{"cr_golden": run, "cr_row": strconv.Itoa(i)}
		for k, v := range r.Stream {
			labels[k] = v
		}
		value := []any{stamp, r.Msg}
		if len(r.Fields) > 0 {
			value = append(value, r.Fields)
		}
		stream := map[string]any{"stream": labels, "values": []any{value}}
		if !record {
			batch = append(batch, stream)
			batchRows = append(batchRows, i)
			continue
		}
		// Recording pushes each row on its own, so a row Loki rejects is recorded as rejected and cannot take
		// other rows with it.
		body := dlJSON(map[string]any{"streams": []any{stream}})
		answers[i].PushStatus, answers[i].PushError = dlGoldenPost(t, lokiURL+"/loki/api/v1/push", "application/json", body)
	}
	if len(batch) > 0 {
		// The table holds only rows Loki accepted when they were recorded: one push carries them all.
		status, msg := dlGoldenPost(t, lokiURL+"/loki/api/v1/push", "application/json", dlJSON(map[string]any{"streams": batch}))
		if status/100 != 2 {
			t.Fatalf("Loki rejected the table's rows: %d %s", status, msg)
		}
		for _, i := range batchRows {
			answers[i].PushStatus = status
		}
	}
	if len(otlpRows) > 0 {
		var records [][]byte
		for _, i := range otlpRows {
			o := rows[i].OTLP
			records = append(records, otlpLogRecord(at(i), o.SeverityNumber, o.SeverityText, rows[i].Msg, map[string]string{"cr_row": strconv.Itoa(i)}))
		}
		otlp := otlpLogsData(map[string]string{"service.name": service}, records)
		status, msg := dlGoldenPost(t, lokiURL+"/otlp/v1/logs", "application/x-protobuf", otlp)
		for _, i := range otlpRows {
			answers[i].PushStatus, answers[i].PushError = status, msg
		}
		if record {
			if status, msg := dlGoldenPost(t, vlURL+"/insert/opentelemetry/v1/logs", "application/x-protobuf", otlp); status/100 != 2 {
				t.Fatalf("VictoriaLogs OTLP push: %d %s", status, msg)
			}
		}
	}
	accepted := 0
	for _, a := range answers {
		if a.PushStatus/100 == 2 {
			accepted++
		}
	}
	window := dlFixture{start: base, end: at(len(rows)).Add(time.Second)}
	selectors := []string{fmt.Sprintf(`{cr_golden=%q}`, run)}
	if len(otlpRows) > 0 {
		selectors = append(selectors, fmt.Sprintf(`{service_name=%q}`, service))
	}

	// Poll until every accepted row is visible in both encodings.
	deadline := time.Now().Add(90 * time.Second)
	for {
		seen := 0
		for _, categorized := range []bool{false, true} {
			var headers map[string]string
			if categorized {
				headers = map[string]string{"X-Loki-Response-Encoding-Flags": "categorize-labels"}
			}
			for _, sel := range selectors {
				for stamp, e := range dlGoldenEntries(t, lokiURL, sel, window, headers) {
					i, ok := byStamp[stamp]
					if !ok {
						t.Fatalf("entry at %s is no row of this run", stamp)
					}
					seen++
					if categorized {
						answers[i].Categories = map[string]map[string]string{"stream": dlGoldenStrip(e.stream), "structured_metadata": dlGoldenStrip(e.metadata), "parsed": dlGoldenStrip(e.parsed)}
						continue
					}
					answers[i].Labels = dlGoldenStrip(e.stream)
				}
			}
		}
		if seen == 2*accepted {
			break
		}
		if time.Now().After(deadline) {
			// An accepted row Loki never shows (it stores no entry for it) is recorded as absent; in the
			// comparison it fails like any other difference.
			t.Logf("Loki shows %d of %d entries (both encodings) of the accepted rows", seen, 2*accepted)
			break
		}
		time.Sleep(2 * time.Second)
	}
	for i := range answers {
		a := &answers[i]
		if a.PushStatus/100 != 2 {
			a.Level = "rejected"
			continue
		}
		if a.Labels == nil || a.Categories == nil {
			a.Level = "absent"
			if !record && sampled[i] {
				t.Errorf("%s: Loki accepted the row but shows no entry for it", a.Name)
			}
			continue
		}
		plain := dlLevel(dlEntry{stream: a.Labels}, false)
		categorized := dlLevel(dlEntry{metadata: a.Categories["structured_metadata"]}, true)
		if plain != categorized {
			t.Errorf("%s: Loki's two encodings disagree: default %q, categorize-labels %q", a.Name, plain, categorized)
		}
		a.Level = plain
	}

	if record {
		if len(otlpRows) > 0 {
			fields := dlGoldenVLFields(t, service, window, byStamp)
			for _, i := range otlpRows {
				answers[i].VLFields = fields[i]
				if fields[i] == nil {
					t.Fatalf("%s: VictoriaLogs holds no row for the OTLP record", rows[i].Name)
				}
			}
		}
		// Rows Loki rejects leave the table (the evidence file keeps them with Loki's status and text): the
		// table holds what Loki stores.
		var kept []dlGoldenRow
		for i := range rows {
			if !dlGoldenPushed(rows[i]) {
				kept = append(kept, rows[i])
				continue
			}
			if answers[i].PushStatus/100 != 2 || answers[i].Level == "absent" {
				t.Logf("%s: Loki stores no entry for the row (push %d %s); left out of the table", rows[i].Name, answers[i].PushStatus, answers[i].PushError)
				continue
			}
			rows[i].Want = answers[i].Level
			if rows[i].OTLP != nil {
				rows[i].Fields = answers[i].VLFields
			}
			kept = append(kept, rows[i])
		}
		ev := dlGoldenEvidence{
			Recorded:   time.Now().UTC().Format("2006-01-02"),
			Loki:       dlGoldenBuildInfo(t),
			LokiLimits: dlGoldenLokiLimits(t),
			VLVersion:  dlGoldenVLVersion(t),
		}
		for i, r := range rows {
			if dlGoldenPushed(r) {
				ev.Rows = append(ev.Rows, answers[i])
			}
		}
		dlGoldenWrite(t, dlGoldenPath, kept)
		dlGoldenWrite(t, dlGoldenEvidencePath, ev)
		t.Logf("recorded %d rows (%d accepted by Loki) from Loki %s", pushed, accepted, ev.Loki["version"])
		return
	}
	differ := 0
	for i, r := range rows {
		if !sampled[i] {
			continue
		}
		if answers[i].Level != r.Want {
			differ++
			t.Errorf("%s: Loki %q, table %q (line %q, stream %v, fields %v, otlp %+v)", r.Name, answers[i].Level, r.Want, dlGoldenShort(r.Msg), r.Stream, r.Fields, r.OTLP)
		}
	}
	t.Logf("%d rows pushed, %d accepted, %d differ from the table", pushed, accepted, differ)
}

// dlGoldenPost pushes one body and returns the status and the error text of a rejected push.
func dlGoldenPost(t *testing.T, target, contentType string, body []byte) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-Scope-OrgID", "0")
	resp, err := dlHTTP.Do(req)
	if err != nil {
		t.Fatalf("push %s: %v", target, err)
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 == 2 {
		return resp.StatusCode, ""
	}
	return resp.StatusCode, strings.TrimSpace(string(msg))
}

func dlGoldenEntries(t *testing.T, base, selector string, f dlFixture, headers map[string]string) map[string]dlEntry {
	t.Helper()
	params := url.Values{}
	params.Set("query", selector)
	params.Set("start", strconv.FormatInt(f.start.Add(-time.Second).UnixNano(), 10))
	params.Set("end", strconv.FormatInt(f.end.UnixNano(), 10))
	params.Set("limit", "5000")
	params.Set("direction", "forward")
	req, _ := http.NewRequest(http.MethodGet, base+"/loki/api/v1/query_range?"+params.Encode(), nil)
	req.Header.Set("X-Scope-OrgID", "0")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("query_range %s: %v", selector, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("query_range %s: status %d: %s", selector, resp.StatusCode, body)
	}
	var out dlStreamsResponse
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode query_range %s: %v", selector, err)
	}
	if out.Status != "success" || len(out.Warnings) > 0 {
		t.Fatalf("query_range %s: status=%s warnings=%v", selector, out.Status, out.Warnings)
	}
	return dlEntries(t, out)
}

// dlGoldenStrip drops the run's bookkeeping labels.
func dlGoldenStrip(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		switch k {
		case "cr_golden", "cr_row":
			continue
		}
		out[k] = v
	}
	return out
}

// dlGoldenVLFields reads the fields VictoriaLogs stores for each OTLP record of the run, by row: every field but
// VictoriaLogs' own (_time, _msg, _stream, _stream_id), the resource's service.name and the row marker.
func dlGoldenVLFields(t *testing.T, service string, f dlFixture, byStamp map[string]int) map[int]map[string]string {
	t.Helper()
	forceVLFlush(t)
	params := url.Values{}
	params.Set("query", fmt.Sprintf(`{service.name=%q}`, service))
	params.Set("start", f.start.Add(-time.Second).Format(time.RFC3339Nano))
	params.Set("end", f.end.Format(time.RFC3339Nano))
	resp, err := http.PostForm(vlURL+"/select/logsql/query", params)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out := map[int]map[string]string{}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var row map[string]string
		if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
			t.Fatalf("decode VictoriaLogs row: %v", err)
		}
		ts, err := time.Parse(time.RFC3339Nano, row["_time"])
		if err != nil {
			t.Fatalf("VictoriaLogs _time %q: %v", row["_time"], err)
		}
		i, ok := byStamp[strconv.FormatInt(ts.UnixNano(), 10)]
		if !ok {
			continue
		}
		fields := map[string]string{}
		for k, v := range row {
			switch k {
			case "_time", "_msg", "_stream", "_stream_id", "service.name", "cr_row":
				continue
			}
			fields[k] = v
		}
		out[i] = fields
	}
	return out
}

func dlGoldenGet(t *testing.T, target string) []byte {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, target, nil)
	req.Header.Set("X-Scope-OrgID", "0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", target, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: %d %s", target, resp.StatusCode, body)
	}
	return body
}

func dlGoldenBuildInfo(t *testing.T) map[string]string {
	var info map[string]any
	if err := json.Unmarshal(dlGoldenGet(t, lokiURL+"/loki/api/v1/status/buildinfo"), &info); err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, k := range []string{"version", "revision", "branch", "goVersion"} {
		if v, ok := info[k].(string); ok {
			out[k] = v
		}
	}
	return out
}

// dlGoldenLokiLimits reads the level settings from Loki's /config (YAML, one key per line under limits_config).
func dlGoldenLokiLimits(t *testing.T) map[string]string {
	out := map[string]string{}
	lines := strings.Split(string(dlGoldenGet(t, lokiURL+"/config")), "\n")
	for i, line := range lines {
		key, value, ok := strings.Cut(strings.TrimSpace(line), ":")
		if !ok {
			continue
		}
		switch key {
		case "discover_log_levels", "log_level_from_json_max_depth":
			out[key] = strings.TrimSpace(value)
		case "log_level_fields":
			value = strings.TrimSpace(value)
			for j := i + 1; value == "" && j < len(lines) && strings.HasPrefix(strings.TrimSpace(lines[j]), "- "); j++ {
				out[key] += strings.TrimPrefix(strings.TrimSpace(lines[j]), "- ") + ","
			}
			if value != "" {
				out[key] = value
			}
		}
	}
	return out
}

func dlGoldenVLVersion(t *testing.T) string {
	for _, line := range strings.Split(string(dlGoldenGet(t, vlURL+"/metrics")), "\n") {
		if strings.HasPrefix(line, "vm_app_version{") {
			if _, rest, ok := strings.Cut(line, `short_version="`); ok {
				v, _, _ := strings.Cut(rest, `"`)
				return v
			}
		}
	}
	return ""
}

// dlGoldenWrite writes JSON with sorted map keys, two-space indent and a final newline.
func dlGoldenWrite(t *testing.T, path string, v any) {
	t.Helper()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Clean(path), buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

func dlGoldenShort(s string) string {
	if len(s) > 80 {
		return s[:80] + "..."
	}
	return s
}
