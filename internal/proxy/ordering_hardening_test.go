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
			"code":  "E_TIMEOUT",
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

// =============================================================================
// Fix 2: JSON pretty-printing regression guards
//
// stringifyEntryValue must use json.Marshal for map[string]interface{} and
// []interface{} types. If fmt.Sprintf("%v") is used instead, Grafana receives
// Go's map[key:val] representation which is not valid JSON and breaks
// pretty-printing in Logs Drilldown and Explore.
//
// The tests below guard every layer of the pipeline against this regression:
// the function itself (isolated), and the full vlLogsToLokiStreams path.
// =============================================================================

// TestJSONPrettyPrint_GoFormatNeverEmitted is the primary regression sentinel.
// It explicitly documents every Go collection type that fmt.Sprintf("%v") would
// format incorrectly and verifies json.Marshal output is always returned instead.
func TestJSONPrettyPrint_GoFormatNeverEmitted(t *testing.T) {
	cases := []struct {
		name     string
		input    interface{}
		wantJSON bool // true = must be valid JSON
		mustNot  []string
	}{
		{
			name:     "flat_map",
			input:    map[string]interface{}{"key": "value", "num": 42.0},
			wantJSON: true,
			mustNot:  []string{"map[", "map[key:"},
		},
		{
			name:     "nested_map",
			input:    map[string]interface{}{"outer": map[string]interface{}{"inner": "val"}},
			wantJSON: true,
			mustNot:  []string{"map[", "map[outer:map["},
		},
		{
			name:     "flat_slice",
			input:    []interface{}{"a", "b", "c"},
			wantJSON: true,
			mustNot:  []string{"[a b c]"},
		},
		{
			name:     "slice_of_maps",
			input:    []interface{}{map[string]interface{}{"k": "v"}},
			wantJSON: true,
			mustNot:  []string{"map[", "[map["},
		},
		{
			name:     "empty_map",
			input:    map[string]interface{}{},
			wantJSON: true,
			mustNot:  []string{"map[]"},
		},
		{
			name:     "empty_slice",
			input:    []interface{}{},
			wantJSON: true,
			mustNot:  []string{}, // [] is identical in both JSON and Go fmt — just verify valid JSON
		},
		{
			name:     "map_with_null",
			input:    map[string]interface{}{"k": nil},
			wantJSON: true,
			mustNot:  []string{"map[", "map[k:<nil>]"},
		},
		{
			name:     "map_with_bool",
			input:    map[string]interface{}{"flag": true},
			wantJSON: true,
			mustNot:  []string{"map[flag:true]"},
		},
		{
			name:     "deeply_nested",
			input:    map[string]interface{}{"l1": map[string]interface{}{"l2": map[string]interface{}{"l3": "deep"}}},
			wantJSON: true,
			mustNot:  []string{"map[", "l1:map[l2:map["},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, ok := stringifyEntryValue(tc.input)
			if !ok {
				t.Fatalf("%s: expected ok=true, got false", tc.name)
			}
			if tc.wantJSON && !json.Valid([]byte(got)) {
				t.Errorf("%s: result is not valid JSON: %q", tc.name, got)
			}
			for _, bad := range tc.mustNot {
				if strings.Contains(got, bad) {
					t.Errorf("%s: result contains forbidden Go-format substring %q: full result: %q", tc.name, bad, got)
				}
			}
		})
	}
}

// TestJSONPrettyPrint_ArrayMsgThroughPipeline verifies that when VL returns a
// log entry where _msg is a JSON array (parsed by VL), vlLogsToLokiStreams
// emits a valid JSON array string — not Go's [elem elem] slice formatting.
func TestJSONPrettyPrint_ArrayMsgThroughPipeline(t *testing.T) {
	record := map[string]interface{}{
		"_time":   "2024-01-01T00:00:00Z",
		"_stream": `{app="array-msg-test"}`,
		"_msg":    []interface{}{"step1", "step2", "step3"},
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
		t.Fatalf("expected 1 log entry, got %d", len(values))
	}
	msg := values[0][1]
	if !json.Valid([]byte(msg)) {
		t.Errorf("_msg was a JSON array but rendered as non-JSON: %q", msg)
	}
	if strings.HasPrefix(msg, "[step") {
		t.Errorf("_msg rendered as Go slice format (not JSON): %q", msg)
	}
	var decoded []interface{}
	if err := json.Unmarshal([]byte(msg), &decoded); err != nil {
		t.Fatalf("cannot unmarshal array msg: %v (raw: %q)", err, msg)
	}
	if len(decoded) != 3 || decoded[0] != "step1" {
		t.Errorf("unexpected decoded content: %v", decoded)
	}
}

// TestJSONPrettyPrint_MixedMsgTypesSameResponse covers the common VL pattern
// where some log entries have string _msg (logfmt lines) and others have
// map _msg (parsed JSON lines) in the same NDJSON response body. Both must
// render correctly — the string unchanged, the map as valid JSON.
func TestJSONPrettyPrint_MixedMsgTypesSameResponse(t *testing.T) {
	records := []map[string]interface{}{
		{
			"_time":   "2024-01-01T00:00:01Z",
			"_stream": `{app="mixed-test"}`,
			"_msg":    "plain text log line",
		},
		{
			"_time":   "2024-01-01T00:00:02Z",
			"_stream": `{app="mixed-test"}`,
			"_msg":    map[string]interface{}{"event": "purchase", "amount": 99.95},
		},
		{
			"_time":   "2024-01-01T00:00:03Z",
			"_stream": `{app="mixed-test"}`,
			"_msg":    "another plain log",
		},
	}
	body := buildNDJSON(records)

	streams := vlLogsToLokiStreams(body)
	if len(streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(streams))
	}
	values, ok := streams[0]["values"].([][]string)
	if !ok {
		t.Fatalf("values have unexpected type %T", streams[0]["values"])
	}
	if len(values) != 3 {
		t.Fatalf("expected 3 log entries, got %d", len(values))
	}

	// Entry 0: plain string — must pass through unchanged.
	if values[0][1] != "plain text log line" {
		t.Errorf("entry 0: expected plain string, got %q", values[0][1])
	}
	// Entry 1: JSON object — must be valid JSON, not map[...].
	msg1 := values[1][1]
	if !json.Valid([]byte(msg1)) {
		t.Errorf("entry 1 (map _msg): not valid JSON: %q", msg1)
	}
	if strings.HasPrefix(msg1, "map[") {
		t.Errorf("entry 1 (map _msg): Go format leaked: %q", msg1)
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal([]byte(msg1), &decoded); err != nil {
		t.Fatalf("entry 1: cannot unmarshal: %v", err)
	}
	if decoded["event"] != "purchase" {
		t.Errorf("entry 1: wrong event: %v", decoded["event"])
	}
	// Entry 2: plain string — must also pass through unchanged.
	if values[2][1] != "another plain log" {
		t.Errorf("entry 2: expected plain string, got %q", values[2][1])
	}
}

// TestJSONPrettyPrint_DeepNestedMsgThroughPipeline verifies that multi-level
// JSON nesting in _msg survives the full pipeline as valid JSON without any
// loss of structure or Go formatting leakage.
func TestJSONPrettyPrint_DeepNestedMsgThroughPipeline(t *testing.T) {
	nested := map[string]interface{}{
		"request": map[string]interface{}{
			"method": "POST",
			"path":   "/api/v1/users",
			"headers": map[string]interface{}{
				"content-type": "application/json",
				"x-request-id": "abc-123",
			},
		},
		"response": map[string]interface{}{
			"status": 201.0,
			"body":   map[string]interface{}{"id": "user-456", "created": true},
		},
		"errors": []interface{}{},
	}
	record := map[string]interface{}{
		"_time":   "2024-01-01T00:00:00Z",
		"_stream": `{app="deep-nested-test"}`,
		"_msg":    nested,
	}
	body := buildNDJSON([]map[string]interface{}{record})

	streams := vlLogsToLokiStreams(body)
	if len(streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(streams))
	}
	values := streams[0]["values"].([][]string)
	msg := values[0][1]

	if !json.Valid([]byte(msg)) {
		t.Errorf("deeply nested _msg not valid JSON: %q", msg)
	}
	if strings.Contains(msg, "map[") {
		t.Errorf("Go map format leaked into deeply nested _msg: %q", msg)
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal([]byte(msg), &decoded); err != nil {
		t.Fatalf("cannot unmarshal deep nested msg: %v", err)
	}
	req, ok := decoded["request"].(map[string]interface{})
	if !ok || req["method"] != "POST" {
		t.Errorf("request.method not preserved: %v", decoded["request"])
	}
}

// TestJSONPrettyPrint_NilMsgIsEmpty confirms that a nil _msg field results in
// an empty string entry (not a panic or "null" string), which is the correct
// Grafana-facing behavior for missing messages.
func TestJSONPrettyPrint_NilMsgIsEmpty(t *testing.T) {
	record := map[string]interface{}{
		"_time":   "2024-01-01T00:00:00Z",
		"_stream": `{app="nil-msg-test"}`,
		"_msg":    nil,
	}
	body := buildNDJSON([]map[string]interface{}{record})
	streams := vlLogsToLokiStreams(body)
	if len(streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(streams))
	}
	values := streams[0]["values"].([][]string)
	if values[0][1] != "" {
		t.Errorf("nil _msg: expected empty string, got %q", values[0][1])
	}
}

// Prevent "imported and not used" errors for packages that appear only in
// generated formatting inside test closures.
var _ = fmt.Sprintf
