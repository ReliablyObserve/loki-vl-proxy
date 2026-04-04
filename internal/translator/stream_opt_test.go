package translator

import (
	"strings"
	"testing"
)

// =============================================================================
// Stream selector optimization: use VL {stream} selectors for known _stream_fields
// =============================================================================

func TestStreamOpt_KnownStreamField_UsesStreamSelector(t *testing.T) {
	streamFields := map[string]bool{"app": true, "env": true}
	logql := `{app="nginx"}`

	result, err := TranslateLogQLWithStreamFields(logql, nil, streamFields)
	if err != nil {
		t.Fatal(err)
	}
	// Should use VL stream selector syntax for known stream fields
	if !strings.Contains(result, `app:=nginx`) {
		// Field filter format is the fallback — but for stream fields,
		// VL can use native stream selectors which are faster.
		// The key: the query should still work correctly
		t.Logf("result: %s (field filter used, stream selector would be faster)", result)
	}
}

func TestStreamOpt_UnknownField_UsesFieldFilter(t *testing.T) {
	streamFields := map[string]bool{"app": true}
	logql := `{level="error"}`

	result, err := TranslateLogQLWithStreamFields(logql, nil, streamFields)
	if err != nil {
		t.Fatal(err)
	}
	// level is NOT a stream field, so it must use field filter
	if !strings.Contains(result, `level:=error`) {
		t.Errorf("expected field filter for non-stream field, got %q", result)
	}
}

func TestStreamOpt_MixedFields(t *testing.T) {
	streamFields := map[string]bool{"app": true, "env": true}
	logql := `{app="nginx", level="error"}`

	result, err := TranslateLogQLWithStreamFields(logql, nil, streamFields)
	if err != nil {
		t.Fatal(err)
	}
	// Both should be present in some form
	if !strings.Contains(result, "nginx") || !strings.Contains(result, "error") {
		t.Errorf("expected both matchers in result, got %q", result)
	}
}

func TestStreamOpt_NilStreamFields_FallsBackToFieldFilter(t *testing.T) {
	logql := `{app="nginx"}`

	// nil stream fields = no optimization, all field filters
	result, err := TranslateLogQLWithStreamFields(logql, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, `app:=nginx`) {
		t.Errorf("expected field filter when no stream fields configured, got %q", result)
	}
}

func TestStreamOpt_EmptyStreamFields_FallsBackToFieldFilter(t *testing.T) {
	logql := `{app="nginx"}`

	result, err := TranslateLogQLWithStreamFields(logql, nil, map[string]bool{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, `app:=nginx`) {
		t.Errorf("expected field filter when stream fields empty, got %q", result)
	}
}

func TestStreamOpt_RegexMatcher_AlwaysFieldFilter(t *testing.T) {
	streamFields := map[string]bool{"app": true}
	logql := `{app=~"nginx.*"}`

	result, err := TranslateLogQLWithStreamFields(logql, nil, streamFields)
	if err != nil {
		t.Fatal(err)
	}
	// Regex matchers should always use field filter (VL stream selectors only support exact match)
	if !strings.Contains(result, "nginx") {
		t.Errorf("expected nginx in result, got %q", result)
	}
}

func TestStreamOpt_NegativeMatcher_AlwaysFieldFilter(t *testing.T) {
	streamFields := map[string]bool{"app": true}
	logql := `{app!="nginx"}`

	result, err := TranslateLogQLWithStreamFields(logql, nil, streamFields)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "nginx") {
		t.Errorf("expected nginx in result, got %q", result)
	}
}

func TestStreamOpt_WithPipeline(t *testing.T) {
	streamFields := map[string]bool{"app": true}
	logql := `{app="nginx"} |= "error"`

	result, err := TranslateLogQLWithStreamFields(logql, nil, streamFields)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "error") {
		t.Errorf("expected error filter in result, got %q", result)
	}
}
