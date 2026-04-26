package proxy

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// =============================================================================
// Fix 1: Stable log stream ordering
//
// vlLogsToLokiStreams and vlReaderToLokiStreams now track insertion order via a
// streamOrder []string slice and apply sort.SliceStable for same-nanosecond
// timestamps (secondary sort by message content).
// =============================================================================

// buildNDJSON builds a newline-delimited JSON body from a slice of log records.
// Each record is a map with at least "_time" and "_msg" keys.
func buildNDJSON(records []map[string]interface{}) []byte {
	var sb strings.Builder
	for _, rec := range records {
		b, _ := json.Marshal(rec)
		sb.Write(b)
		sb.WriteByte('\n')
	}
	return []byte(sb.String())
}

// TestStreamOrdering_DeterministicStreamOrder verifies that multiple calls to
// vlLogsToLokiStreams with multi-stream input always return streams in the same
// (insertion) order.
func TestStreamOrdering_DeterministicStreamOrder(t *testing.T) {
	// Three streams with distinct label sets; order: stream-A, stream-B, stream-C.
	records := []map[string]interface{}{
		{"_time": "2024-01-01T00:00:00.000000001Z", "_msg": "a1", "_stream": `{app="stream-A"}`},
		{"_time": "2024-01-01T00:00:00.000000002Z", "_msg": "b1", "_stream": `{app="stream-B"}`},
		{"_time": "2024-01-01T00:00:00.000000003Z", "_msg": "c1", "_stream": `{app="stream-C"}`},
		{"_time": "2024-01-01T00:00:00.000000004Z", "_msg": "a2", "_stream": `{app="stream-A"}`},
		{"_time": "2024-01-01T00:00:00.000000005Z", "_msg": "b2", "_stream": `{app="stream-B"}`},
	}
	body := buildNDJSON(records)

	const iterations = 15
	var firstOrder []string

	for i := 0; i < iterations; i++ {
		streams := vlLogsToLokiStreams(body)
		if len(streams) != 3 {
			t.Fatalf("iter %d: expected 3 streams, got %d", i, len(streams))
		}

		order := make([]string, len(streams))
		for j, s := range streams {
			labels, ok := s["stream"].(map[string]string)
			if !ok {
				t.Fatalf("iter %d stream %d: stream labels have unexpected type", i, j)
			}
			order[j] = labels["app"]
		}

		if i == 0 {
			firstOrder = order
			continue
		}
		for j, app := range order {
			if app != firstOrder[j] {
				t.Errorf("iter %d: stream order changed: got %v, want %v", i, order, firstOrder)
				break
			}
		}
	}
}

// TestStreamOrdering_DeterministicValueOrder verifies that within a single
// stream the log values are returned in the same order on every call.
func TestStreamOrdering_DeterministicValueOrder(t *testing.T) {
	records := []map[string]interface{}{
		{"_time": "2024-01-01T00:00:01Z", "_msg": "first", "_stream": `{app="myapp"}`},
		{"_time": "2024-01-01T00:00:02Z", "_msg": "second", "_stream": `{app="myapp"}`},
		{"_time": "2024-01-01T00:00:03Z", "_msg": "third", "_stream": `{app="myapp"}`},
	}
	body := buildNDJSON(records)

	const iterations = 15
	var firstMsgs []string

	for i := 0; i < iterations; i++ {
		streams := vlLogsToLokiStreams(body)
		if len(streams) != 1 {
			t.Fatalf("iter %d: expected 1 stream, got %d", i, len(streams))
		}
		values, ok := streams[0]["values"].([][]string)
		if !ok {
			t.Fatalf("iter %d: values have unexpected type %T", i, streams[0]["values"])
		}

		msgs := make([]string, len(values))
		for j, v := range values {
			msgs[j] = v[1]
		}

		if i == 0 {
			firstMsgs = msgs
			continue
		}
		for j, m := range msgs {
			if m != firstMsgs[j] {
				t.Errorf("iter %d: value order changed at index %d: got %v, want %v", i, j, msgs, firstMsgs)
				break
			}
		}
	}
}

// TestStreamOrdering_SameNanosecondSortedByMessage verifies that entries with
// identical nanosecond timestamps within a stream are sorted alphabetically by
// message content so that repeated calls always produce the same sequence.
func TestStreamOrdering_SameNanosecondSortedByMessage(t *testing.T) {
	// Three entries all sharing the exact same nanosecond timestamp.
	sameTS := "2024-06-15T12:00:00.123456789Z"
	records := []map[string]interface{}{
		{"_time": sameTS, "_msg": "zzz-last", "_stream": `{app="tiebreak"}`},
		{"_time": sameTS, "_msg": "aaa-first", "_stream": `{app="tiebreak"}`},
		{"_time": sameTS, "_msg": "mmm-middle", "_stream": `{app="tiebreak"}`},
	}
	body := buildNDJSON(records)

	// Expected lexicographic order: aaa-first, mmm-middle, zzz-last.
	want := []string{"aaa-first", "mmm-middle", "zzz-last"}

	const iterations = 15
	for i := 0; i < iterations; i++ {
		streams := vlLogsToLokiStreams(body)
		if len(streams) != 1 {
			t.Fatalf("iter %d: expected 1 stream, got %d", i, len(streams))
		}
		values, ok := streams[0]["values"].([][]string)
		if !ok {
			t.Fatalf("iter %d: values have unexpected type %T", i, streams[0]["values"])
		}
		if len(values) != 3 {
			t.Fatalf("iter %d: expected 3 values, got %d", i, len(values))
		}
		for j, v := range values {
			if v[1] != want[j] {
				t.Errorf("iter %d: position %d: got msg %q, want %q (full order: %v)",
					i, j, v[1], want[j], func() []string {
						msgs := make([]string, len(values))
						for k, vv := range values {
							msgs[k] = vv[1]
						}
						return msgs
					}())
			}
		}
	}
}

// TestStreamOrdering_InsertionOrderPreserved checks that when multiple streams
// appear interleaved in the input, the output stream order matches the order
// each distinct stream was *first* seen.
func TestStreamOrdering_InsertionOrderPreserved(t *testing.T) {
	// Streams appear in order: X, Y, Z (interleaved but first-seen order is X, Y, Z).
	records := []map[string]interface{}{
		{"_time": "2024-01-01T00:00:01Z", "_msg": "x1", "_stream": `{svc="X"}`},
		{"_time": "2024-01-01T00:00:02Z", "_msg": "y1", "_stream": `{svc="Y"}`},
		{"_time": "2024-01-01T00:00:03Z", "_msg": "z1", "_stream": `{svc="Z"}`},
		{"_time": "2024-01-01T00:00:04Z", "_msg": "x2", "_stream": `{svc="X"}`},
		{"_time": "2024-01-01T00:00:05Z", "_msg": "y2", "_stream": `{svc="Y"}`},
	}
	body := buildNDJSON(records)

	streams := vlLogsToLokiStreams(body)
	if len(streams) != 3 {
		t.Fatalf("expected 3 streams, got %d", len(streams))
	}

	wantOrder := []string{"X", "Y", "Z"}
	for i, s := range streams {
		labels := s["stream"].(map[string]string)
		if labels["svc"] != wantOrder[i] {
			t.Errorf("stream[%d]: got svc=%q, want %q", i, labels["svc"], wantOrder[i])
		}
	}

	// Also verify stream X has 2 values and Y has 2, Z has 1.
	counts := map[string]int{"X": 2, "Y": 2, "Z": 1}
	for i, s := range streams {
		labels := s["stream"].(map[string]string)
		svc := labels["svc"]
		values, ok := s["values"].([][]string)
		if !ok {
			t.Fatalf("stream[%d] values have unexpected type %T", i, s["values"])
		}
		if len(values) != counts[svc] {
			t.Errorf("stream %q: expected %d values, got %d", svc, counts[svc], len(values))
		}
	}
}

// =============================================================================
// Fix 2: stringifyEntryValue handles JSON objects
//
// map[string]interface{} and []interface{} values are now JSON-marshalled
// instead of being rendered as Go's default %v format.
// =============================================================================

func TestStringifyEntryValue_Nil(t *testing.T) {
	got, ok := stringifyEntryValue(nil)
	if ok {
		t.Errorf("nil: expected ok=false, got ok=true")
	}
	if got != "" {
		t.Errorf("nil: expected empty string, got %q", got)
	}
}

func TestStringifyEntryValue_String(t *testing.T) {
	got, ok := stringifyEntryValue("hello world")
	if !ok {
		t.Errorf("string: expected ok=true, got ok=false")
	}
	if got != "hello world" {
		t.Errorf("string: expected %q, got %q", "hello world", got)
	}
}

func TestStringifyEntryValue_EmptyString(t *testing.T) {
	got, ok := stringifyEntryValue("")
	if !ok {
		t.Errorf("empty string: expected ok=true, got ok=false")
	}
	if got != "" {
		t.Errorf("empty string: expected empty string, got %q", got)
	}
}

func TestStringifyEntryValue_MapObject(t *testing.T) {
	input := map[string]interface{}{
		"key": "val",
		"num": 42.0,
	}
	got, ok := stringifyEntryValue(input)
	if !ok {
		t.Errorf("map: expected ok=true, got ok=false")
	}
	// Must be valid JSON and start with '{'.
	if len(got) == 0 || got[0] != '{' {
		t.Errorf("map: expected JSON object string starting with '{', got %q", got)
	}
	if !json.Valid([]byte(got)) {
		t.Errorf("map: result is not valid JSON: %q", got)
	}
	// Must NOT look like Go's default map formatting.
	if strings.HasPrefix(got, "map[") {
		t.Errorf("map: result looks like Go %%v output (not JSON): %q", got)
	}
	// Round-trip: the JSON must contain the expected keys/values.
	var decoded map[string]interface{}
	if err := json.Unmarshal([]byte(got), &decoded); err != nil {
		t.Fatalf("map: cannot unmarshal result: %v", err)
	}
	if decoded["key"] != "val" {
		t.Errorf("map: key=%q, want %q", decoded["key"], "val")
	}
	if decoded["num"] != 42.0 {
		t.Errorf("map: num=%v, want 42.0", decoded["num"])
	}
}

func TestStringifyEntryValue_SliceArray(t *testing.T) {
	input := []interface{}{"a", "b", "c"}
	got, ok := stringifyEntryValue(input)
	if !ok {
		t.Errorf("slice: expected ok=true, got ok=false")
	}
	// Must be valid JSON array.
	if len(got) == 0 || got[0] != '[' {
		t.Errorf("slice: expected JSON array string starting with '[', got %q", got)
	}
	if !json.Valid([]byte(got)) {
		t.Errorf("slice: result is not valid JSON: %q", got)
	}
	// Must NOT look like Go's default slice formatting.
	if strings.HasPrefix(got, "[a ") || strings.HasPrefix(got, "[a b") {
		t.Errorf("slice: result looks like Go %%v output (not JSON): %q", got)
	}
	// Round-trip.
	var decoded []interface{}
	if err := json.Unmarshal([]byte(got), &decoded); err != nil {
		t.Fatalf("slice: cannot unmarshal result: %v", err)
	}
	if len(decoded) != 3 {
		t.Errorf("slice: expected 3 elements, got %d", len(decoded))
	}
}

func TestStringifyEntryValue_Float(t *testing.T) {
	got, ok := stringifyEntryValue(float64(3.14))
	if !ok {
		t.Errorf("float: expected ok=true, got ok=false")
	}
	if got == "" {
		t.Errorf("float: expected non-empty string")
	}
	if !strings.Contains(got, "3") {
		t.Errorf("float: expected string representation to contain '3', got %q", got)
	}
}

func TestStringifyEntryValue_Bool(t *testing.T) {
	for _, tc := range []struct {
		input interface{}
		want  string
	}{
		{true, "true"},
		{false, "false"},
	} {
		got, ok := stringifyEntryValue(tc.input)
		if !ok {
			t.Errorf("bool(%v): expected ok=true", tc.input)
		}
		if got != tc.want {
			t.Errorf("bool(%v): expected %q, got %q", tc.input, tc.want, got)
		}
	}
}

func TestStringifyEntryValue_NestedMapObject(t *testing.T) {
	// Nested objects are common in VL-parsed JSON log lines.
	input := map[string]interface{}{
		"level": "error",
		"error": map[string]interface{}{
			"code": "E_TIMEOUT",
			"retry": true,
		},
	}
	got, ok := stringifyEntryValue(input)
	if !ok {
		t.Fatalf("nested map: expected ok=true")
	}
	if !json.Valid([]byte(got)) {
		t.Errorf("nested map: result is not valid JSON: %q", got)
	}
	if strings.HasPrefix(got, "map[") {
		t.Errorf("nested map: result looks like Go %%v output: %q", got)
	}
}

func TestStringifyEntryValue_MapWithNullValue(t *testing.T) {
	input := map[string]interface{}{
		"key": nil,
	}
	got, ok := stringifyEntryValue(input)
	if !ok {
		t.Fatalf("map with null: expected ok=true")
	}
	if !json.Valid([]byte(got)) {
		t.Errorf("map with null: result is not valid JSON: %q", got)
	}
}

func TestStringifyEntryValue_EmptySlice(t *testing.T) {
	input := []interface{}{}
	got, ok := stringifyEntryValue(input)
	if !ok {
		t.Fatalf("empty slice: expected ok=true")
	}
	if got != "[]" {
		t.Errorf("empty slice: expected %q, got %q", "[]", got)
	}
}

func TestStringifyEntryValue_EmptyMap(t *testing.T) {
	input := map[string]interface{}{}
	got, ok := stringifyEntryValue(input)
	if !ok {
		t.Fatalf("empty map: expected ok=true")
	}
	if got != "{}" {
		t.Errorf("empty map: expected %q, got %q", "{}", got)
	}
}

// TestStringifyEntryValue_UsedInVlLogsToLokiStreams confirms that when
// vlLogsToLokiStreams processes an NDJSON line whose _msg field is a JSON
// object (as VL may produce), the resulting log line is valid JSON — not the
// Go map[...] representation.
func TestStringifyEntryValue_UsedInVlLogsToLokiStreams(t *testing.T) {
	// Construct an NDJSON line where _msg is a raw JSON object.
	// json.Marshal will embed the nested object as a JSON value.
	record := map[string]interface{}{
		"_time":   "2024-01-01T00:00:00Z",
		"_stream": `{app="json-msg-test"}`,
		"_msg":    map[string]interface{}{"event": "login", "user": "alice"},
	}
	body := buildNDJSON([]map[string]interface{}{record})

	streams := vlLogsToLokiStreams(body)
	if len(streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(streams))
	}
	values, ok := streams[0]["values"].([][]string)
	if !ok {
		t.Fatalf("values have unexpected type %T", streams[0]["values"])
	}
	if len(values) != 1 {
		t.Fatalf("expected 1 value, got %d", len(values))
	}
	msg := values[0][1]
	if !json.Valid([]byte(msg)) {
		t.Errorf("_msg was a JSON object but got rendered as non-JSON: %q", msg)
	}
	if strings.HasPrefix(msg, "map[") {
		t.Errorf("_msg was rendered using Go %%v format (not JSON): %q", msg)
	}
	// Verify content round-trips correctly.
	var decoded map[string]interface{}
	if err := json.Unmarshal([]byte(msg), &decoded); err != nil {
		t.Fatalf("cannot unmarshal log message: %v (raw: %q)", err, msg)
	}
	if decoded["event"] != "login" || decoded["user"] != "alice" {
		t.Errorf("unexpected decoded content: %v", decoded)
	}
}

// Prevent "imported and not used" errors for packages that appear only in
// generated formatting inside test closures.
var _ = fmt.Sprintf
