package proxy

import (
	"encoding/json"
	"reflect"
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
		{`{app="foo"} | json`, true},
		{`{app="foo"}`, false},
		{`{app="foo"} | json | status >= 400`, true},
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

		// Both functions may produce different JSON key orderings (Go map iteration
		// is non-deterministic), so compare semantically via unmarshal.
		if perEntry != withFlag {
			var a, b interface{}
			errA := json.Unmarshal([]byte(perEntry), &a)
			errB := json.Unmarshal([]byte(withFlag), &b)
			if errA != nil || errB != nil || !reflect.DeepEqual(a, b) {
				t.Errorf("query %q: reconstructLogLine=%q, reconstructLogLineWithFlag=%q — must be semantically identical",
					q, perEntry, withFlag)
			}
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

func TestOPT3_JsonQuerySkipsReconstruction(t *testing.T) {
	// For | json queries, hasTextExtractionParser returns true → skipReconstruction=true
	// → original msg returned unchanged.  Loki returns the original log line for | json.
	msg := `{"method":"POST","status":"200"}`
	entry := map[string]interface{}{
		"_msg":    msg,
		"_stream": `{app="svc"}`,
		"status":  "200",
		"method":  "POST",
	}
	streamLabels := map[string]string{"app": "svc"}

	result := reconstructLogLineWithFlag(msg, entry, streamLabels, true /* skip — json query */)
	if result != msg {
		t.Errorf("json query: expected original msg %q, got %q", msg, result)
	}
}
