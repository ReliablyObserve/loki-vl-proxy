//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/lokirecorded"
)

// Paths, from this package's directory, of the recorded answers of a real
// Loki, the generated error compatibility matrix that picks the rows asked of
// the live stack, and the rows the proxy is known to answer differently.
const (
	errorAnswersPath    = "../../internal/lokierr/testdata/loki_error_answers.jsonl"
	errorCoveragePath   = "../../conformance/registry/generated/error-coverage.json"
	errorKnownDiffsPath = "../../conformance/registry/error-layer-known-differences.json"
)

// errorAnswersDelta is how far the recorded absolute start, end and time
// parameters move so the rows are asked of the stack as of now: the recorded
// Loki limited how far back a query may reach, and a stack's Loki may too. It
// is measured from the latest end or time the rows carry.
func errorAnswersDelta(rows []lokirecorded.Row, now time.Time) time.Duration {
	var latest int64
	for _, row := range rows {
		_, query, _ := strings.Cut(row.Target, "?")
		for _, p := range strings.Split(query, "&") {
			name, value, _ := strings.Cut(p, "=")
			if name != "end" && name != "time" {
				continue
			}
			if n, err := strconv.ParseInt(value, 10, 64); err == nil && n >= 1_000_000_000 && n < 100_000_000_000 && n > latest {
				latest = n
			}
		}
	}
	delta := time.Duration(now.Unix()-latest) * time.Second
	if latest == 0 || delta < 0 {
		return 0
	}
	return delta
}

// TestCompat_ErrorAnswersLikeLoki asks the stack's Loki and the proxy the
// requests of one representative recorded row for every route x error kind x
// message cell of the generated error compatibility matrix
// (conformance/registry/generated/error-coverage.json, e2e_rows), as bytes on
// a plain socket, and compares status, Content-Type, X-Content-Type-Options
// and body. It covers every route and every error kind the recorded rows
// hold: request parameters, the form and URL decoding, LogQL syntax and
// validation, limits, methods, unknown paths and the tail upgrade.
//
// A difference the proxy is known to have (error-layer-known-differences.json)
// is reported and skipped; a new one fails. The deterministic unit replay
// (TestLokiErrorAnswersReplay) owns the strict ratchet. A row the stack's Loki
// answers differently from its recording is a stale recording, not a proxy
// difference: it is reported and skipped, and the recorder (test/errorrecord)
// writes it again.
// conformance: semantics/error-request-validation-parity, semantics/error-layer-body-shape, semantics/parse-error-text-parity, status-400, status-404, status-405, status-500
func TestCompat_ErrorAnswersLikeLoki(t *testing.T) {
	file, err := lokirecorded.Read(errorAnswersPath)
	if err != nil {
		t.Fatal(err)
	}
	byID := make(map[string]lokirecorded.Row, len(file.Rows))
	for _, row := range file.Rows {
		byID[row.ID] = row
	}
	var coverage struct {
		E2ERows []string `json:"e2e_rows"`
	}
	var known struct {
		Rows map[string]json.RawMessage `json:"rows"`
	}
	for path, into := range map[string]any{errorCoveragePath: &coverage, errorKnownDiffsPath: &known} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, into); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
	}
	if len(coverage.E2ERows) == 0 {
		t.Fatalf("%s lists no e2e rows: regenerate it with conformance/scripts/error_registry.py", errorCoveragePath)
	}
	lokiHost, proxyHost := hostOf(t, lokiURL), hostOf(t, proxyURL)
	delta := errorAnswersDelta(file.Rows, time.Now())
	stale, differ, compared := 0, 0, 0
	for _, id := range coverage.E2ERows {
		row, ok := byID[id]
		if !ok {
			t.Fatalf("%s names row %s, which %s does not hold: regenerate it", errorCoveragePath, id, errorAnswersPath)
		}
		t.Run(id+"_"+row.Route+"_"+strings.NewReplacer(":", "_", " ", "_", "|", "_").Replace(row.Trigger), func(t *testing.T) {
			ask := row.Shifted(delta)
			fromLoki, err := lokirecorded.Replay(lokiHost, ask, 30*time.Second)
			if err != nil {
				t.Fatalf("Loki: %v", err)
			}
			fromProxy, err := lokirecorded.Replay(proxyHost, ask, 30*time.Second)
			if err != nil {
				t.Fatalf("proxy: %v", err)
			}
			if fromLoki != row.Loki {
				stale++
				t.Skipf("the stack's Loki answers differently from the recording (stale recording; run test/errorrecord): recorded %s, live %s", row.Loki.Summary(), fromLoki.Summary())
			}
			compared++
			_, isKnown := known.Rows[id]
			switch {
			case fromProxy == fromLoki && isKnown:
				t.Logf("row %s now matches Loki; the unit replay asks for it to leave the known differences", id)
			case fromProxy == fromLoki:
			case isKnown:
				differ++
				t.Skipf("known difference: Loki %s, proxy %s", fromLoki.Summary(), fromProxy.Summary())
			default:
				t.Errorf("proxy answers differently from Loki\nrequest %s %s\nLoki  %s\nproxy %s", row.Method, row.Target, fromLoki.Summary(), fromProxy.Summary())
			}
		})
	}
	t.Logf("%d representative rows: %d compared with the live Loki, %d of them known differences, %d stale recordings", len(coverage.E2ERows), compared, differ, stale)
	if compared == 0 {
		// Every row skipped as stale proves nothing: the stack's Loki is not the one that was recorded.
		t.Fatalf("no row was compared: the stack's Loki answers differently from the recording on all %d rows (run test/errorrecord against it)", len(coverage.E2ERows))
	}
}

func hostOf(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		t.Fatalf("%q is not a base URL: %v", raw, err)
	}
	return u.Host
}
