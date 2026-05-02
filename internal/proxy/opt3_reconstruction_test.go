package proxy

import (
	"strings"
	"testing"
)

func TestOPT3_SkipFlagConsistency(t *testing.T) {
	cases := []struct {
		logql    string
		wantSkip bool
	}{
		{`{app="foo"} | logfmt`, true},
		{`{app="foo"} | regexp "(?P<msg>.*)"`, true},
		{`{app="foo"} | pattern "<msg>"`, true},
		{`{app="foo"} | json`, false},
		{`{app="foo"}`, false},
		{`{app="foo"} | json | status >= 400`, false},
	}
	for _, tc := range cases {
		got := hasTextExtractionParser(tc.logql)
		if got != tc.wantSkip {
			t.Errorf("hasTextExtractionParser(%q) = %v, want %v", tc.logql, got, tc.wantSkip)
		}
	}
}

func TestOPT3_ReconstructFlagMatchesPerEntryCall(t *testing.T) {
	// Verify that precomputing the flag once gives identical results to per-entry call.
	queries := []string{
		`{app="foo"} | logfmt`,
		`{app="foo"} | json`,
		`{app="foo"}`,
		`{app="foo"} | regexp "(?P<msg>.*)"`,
	}
	msg := `original message`
	entry := map[string]interface{}{
		"_time":   "1746100000000000000",
		"_msg":    msg,
		"_stream": `{app="foo"}`,
		"status":  "200",
		"method":  "GET",
	}
	streamLabels := map[string]string{"app": "foo"}

	for _, q := range queries {
		// Per-entry approach (old):
		perEntry := reconstructLogLine(msg, entry, streamLabels, q)
		// Precomputed flag approach (new):
		flag := hasTextExtractionParser(q)
		withFlag := reconstructLogLineWithFlag(msg, entry, streamLabels, flag)

		if perEntry != withFlag {
			t.Errorf("query %q: reconstructLogLine=%q, reconstructLogLineWithFlag=%q — must be identical",
				q, perEntry, withFlag)
		}
	}
}

func TestOPT3_LogfmtQuerySkipsReconstruction(t *testing.T) {
	// For logfmt queries, skipReconstruction=true, so the msg must be returned unchanged.
	msg := "level=info msg=started"
	entry := map[string]interface{}{
		"_msg":   msg,
		"status": "200", // extra field that would normally trigger reconstruction
	}
	streamLabels := map[string]string{"app": "svc"}

	result := reconstructLogLineWithFlag(msg, entry, streamLabels, true /* skip */)
	if result != msg {
		t.Errorf("logfmt skip: expected original msg %q, got %q", msg, result)
	}
	if strings.HasPrefix(result, `{"_msg":`) {
		t.Errorf("logfmt skip: result must not be JSON-reconstructed, got %q", result)
	}
}

func TestOPT3_JsonQueryDoesReconstruct(t *testing.T) {
	// For json queries, skipReconstruction=false, so extra fields merge into the line.
	msg := "{}"
	entry := map[string]interface{}{
		"_msg":    msg,
		"_stream": `{app="svc"}`,
		"status":  "200",
		"method":  "GET",
	}
	streamLabels := map[string]string{"app": "svc"}

	result := reconstructLogLineWithFlag(msg, entry, streamLabels, false /* don't skip */)
	if !strings.Contains(result, "status") {
		t.Errorf("json query: expected 'status' in reconstructed line, got %q", result)
	}
	if !strings.HasPrefix(result, `{"_msg":`) {
		t.Errorf("json query: expected JSON reconstruction, got %q", result)
	}
}
