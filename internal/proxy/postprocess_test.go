package proxy

import (
	"strconv"
	"strings"
	"testing"
)

func TestDecolorizeStreams(t *testing.T) {
	streams := []map[string]interface{}{
		{
			"stream": map[string]string{"app": "test"},
			"values": [][]string{
				{"1000", "\x1b[31mERROR\x1b[0m: something failed"},
				{"1001", "\x1b[32mINFO\x1b[0m: \x1b[1mbold\x1b[0m text"},
				{"1002", "no ansi codes here"},
			},
		},
	}

	decolorizeStreams(streams)

	values := streams[0]["values"].([][]string)
	if values[0][1] != "ERROR: something failed" {
		t.Errorf("expected stripped ANSI, got %q", values[0][1])
	}
	if values[1][1] != "INFO: bold text" {
		t.Errorf("expected stripped ANSI, got %q", values[1][1])
	}
	if values[2][1] != "no ansi codes here" {
		t.Errorf("expected unchanged, got %q", values[2][1])
	}
}

func TestIPFilterStreams(t *testing.T) {
	streams := []map[string]interface{}{
		{
			"stream": map[string]string{"app": "web", "addr": "10.0.1.42"},
			"values": [][]string{{"1000", "log line 1"}},
		},
		{
			"stream": map[string]string{"app": "web", "addr": "192.168.1.100"},
			"values": [][]string{{"1001", "log line 2"}},
		},
		{
			"stream": map[string]string{"app": "web", "addr": "8.8.8.8"},
			"values": [][]string{{"1002", "log line 3"}},
		},
	}

	t.Run("CIDR_match", func(t *testing.T) {
		result := ipFilterStreams(streams, "addr", "10.0.0.0/8")
		if len(result) != 1 {
			t.Fatalf("expected 1 match for 10.0.0.0/8, got %d", len(result))
		}
		labels := result[0]["stream"].(map[string]string)
		if labels["addr"] != "10.0.1.42" {
			t.Errorf("expected 10.0.1.42, got %s", labels["addr"])
		}
	})

	t.Run("wider_CIDR", func(t *testing.T) {
		result := ipFilterStreams(streams, "addr", "0.0.0.0/0")
		if len(result) != 3 {
			t.Errorf("expected 3 matches for 0.0.0.0/0, got %d", len(result))
		}
	})

	t.Run("no_match", func(t *testing.T) {
		result := ipFilterStreams(streams, "addr", "172.16.0.0/12")
		if len(result) != 0 {
			t.Errorf("expected 0 matches, got %d", len(result))
		}
	})

	t.Run("single_IP", func(t *testing.T) {
		result := ipFilterStreams(streams, "addr", "8.8.8.8")
		if len(result) != 1 {
			t.Fatalf("expected 1 match for exact IP, got %d", len(result))
		}
	})

	t.Run("missing_label", func(t *testing.T) {
		result := ipFilterStreams(streams, "nonexistent", "10.0.0.0/8")
		if len(result) != 0 {
			t.Errorf("expected 0 matches for missing label, got %d", len(result))
		}
	})

	t.Run("invalid_CIDR", func(t *testing.T) {
		result := ipFilterStreams(streams, "addr", "not-a-cidr")
		if len(result) != 3 {
			t.Errorf("expected all streams returned for invalid CIDR, got %d", len(result))
		}
	})

	t.Run("private_range", func(t *testing.T) {
		result := ipFilterStreams(streams, "addr", "192.168.0.0/16")
		if len(result) != 1 {
			t.Fatalf("expected 1 match for 192.168.0.0/16, got %d", len(result))
		}
		labels := result[0]["stream"].(map[string]string)
		if labels["addr"] != "192.168.1.100" {
			t.Errorf("expected 192.168.1.100, got %s", labels["addr"])
		}
	})
}

func TestParseIPFilter(t *testing.T) {
	tests := []struct {
		query     string
		wantLabel string
		wantCIDR  string
		wantOK    bool
	}{
		{`{app="web"} | addr = ip("10.0.0.0/8")`, "addr", "10.0.0.0/8", true},
		{`{app="web"} | addr == ip("192.168.0.0/16")`, "addr", "192.168.0.0/16", true},
		{`{app="web"} | src_ip = ip("172.16.0.0/12")`, "src_ip", "172.16.0.0/12", true},
		{`{app="web"}`, "", "", false},
		{`{app="web"} | json`, "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			label, cidr, ok := parseIPFilter(tt.query)
			if ok != tt.wantOK {
				t.Errorf("ok = %v, want %v", ok, tt.wantOK)
			}
			if label != tt.wantLabel {
				t.Errorf("label = %q, want %q", label, tt.wantLabel)
			}
			if cidr != tt.wantCIDR {
				t.Errorf("cidr = %q, want %q", cidr, tt.wantCIDR)
			}
		})
	}
}

func TestApplyLineFormatTemplate(t *testing.T) {
	streams := []map[string]interface{}{
		{
			"stream": map[string]string{"app": "web", "status": "200", "method": "get"},
			"values": [][]string{
				{"1000", "original line"},
			},
		},
	}

	t.Run("simple_field", func(t *testing.T) {
		s := cloneStreams(streams)
		applyLineFormatTemplate(s, `{{.app}} {{.status}}`)
		val := s[0]["values"].([][]string)[0][1]
		if val != "web 200" {
			t.Errorf("expected 'web 200', got %q", val)
		}
	})

	t.Run("ToUpper", func(t *testing.T) {
		s := cloneStreams(streams)
		applyLineFormatTemplate(s, `{{.method | ToUpper}}`)
		val := s[0]["values"].([][]string)[0][1]
		if val != "GET" {
			t.Errorf("expected 'GET', got %q", val)
		}
	})

	t.Run("ToLower", func(t *testing.T) {
		s := cloneStreams(streams)
		s[0]["stream"] = map[string]string{"app": "WEB"}
		applyLineFormatTemplate(s, `{{.app | ToLower}}`)
		val := s[0]["values"].([][]string)[0][1]
		if val != "web" {
			t.Errorf("expected 'web', got %q", val)
		}
	})

	t.Run("default_func", func(t *testing.T) {
		s := cloneStreams(streams)
		applyLineFormatTemplate(s, `{{.missing | default "N/A"}}`)
		val := s[0]["values"].([][]string)[0][1]
		if val != "N/A" {
			t.Errorf("expected 'N/A', got %q", val)
		}
	})

	t.Run("invalid_template", func(t *testing.T) {
		s := cloneStreams(streams)
		applyLineFormatTemplate(s, `{{.unclosed`)
		val := s[0]["values"].([][]string)[0][1]
		if val != "original line" {
			t.Errorf("invalid template should leave line unchanged, got %q", val)
		}
	})
}

func TestExtractLineFormatTemplate(t *testing.T) {
	tests := []struct {
		query string
		want  string
	}{
		{`{app="web"} | line_format "{{.status}}"`, "{{.status}}"},
		{`{app="web"} | json | line_format "{{.method | ToUpper}}"`, "{{.method | ToUpper}}"},
		{`{app="web"}`, ""},
		{`{app="web"} | logfmt`, ""},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			got := extractLineFormatTemplate(tt.query)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestTokenizeToPattern(t *testing.T) {
	tests := []struct {
		line string
		want string
	}{
		{"GET /api/v1/users 200 15ms", "GET <_> <_> <_>"},
		{"10.0.1.42 - - [2026-04-04] GET /health 200", "<_> - - <_> GET /health <_>"},
		{"GET /health 200", "GET /health <_>"},
		{"error connecting to database at 192.168.1.100:5432", "error connecting to database at <_>"},
		{`{"method":"GET","path":"/api","status":200}`, `{method=<_> path=<_> status=<_>}`},
		{"simple static text", "simple static text"},
	}
	for _, tt := range tests {
		t.Run(tt.line[:min(20, len(tt.line))], func(t *testing.T) {
			got := tokenizeToPattern(tt.line)
			if got != tt.want {
				t.Errorf("tokenizeToPattern(%q)\n  got:  %q\n  want: %q", tt.line, got, tt.want)
			}
		})
	}
}

func TestIsVariableToken(t *testing.T) {
	variables := []string{"200", "15.3", "10.0.1.42", "abc123def", "2026-04-04T10:30:00Z", "123456"}
	for _, v := range variables {
		if !isVariableToken(v) {
			t.Errorf("expected %q to be variable", v)
		}
	}

	constants := []string{"GET", "POST", "error", "connecting", "to", "database"}
	for _, c := range constants {
		if isVariableToken(c) {
			t.Errorf("expected %q to be constant", c)
		}
	}
}

func TestExtractLogPatterns(t *testing.T) {
	vlBody := []byte(strings.Join([]string{
		`{"_time":"2026-04-04T10:00:00Z","_msg":"GET /api/users 200 15ms","app":"web"}`,
		`{"_time":"2026-04-04T10:00:01Z","_msg":"GET /api/users 200 22ms","app":"web"}`,
		`{"_time":"2026-04-04T10:00:02Z","_msg":"POST /api/orders 201 142ms","app":"web"}`,
		`{"_time":"2026-04-04T10:00:03Z","_msg":"GET /api/users 404 3ms","app":"web"}`,
	}, "\n"))

	patterns := extractLogPatterns(vlBody, "15s", 50)
	if len(patterns) == 0 {
		t.Fatal("expected at least 1 pattern")
	}

	first := patterns[0]
	if first["pattern"] == nil {
		t.Fatalf("expected pattern string in first result: %v", first)
	}
	samples, ok := first["samples"].([][]interface{})
	if !ok || len(samples) == 0 {
		t.Fatalf("expected non-empty time-bucket samples, got %v", first["samples"])
	}
}

func TestExtractLogPatterns_RespectsTopNLimit(t *testing.T) {
	vlBody := []byte(strings.Join([]string{
		`{"_time":"2026-04-04T10:00:00Z","_msg":"GET /api/users 200 15ms"}`,
		`{"_time":"2026-04-04T10:00:01Z","_msg":"GET /api/users 200 16ms"}`,
		`{"_time":"2026-04-04T10:00:02Z","_msg":"POST /api/orders 201 42ms"}`,
		`{"_time":"2026-04-04T10:00:03Z","_msg":"POST /api/orders 201 43ms"}`,
		`{"_time":"2026-04-04T10:00:04Z","_msg":"POST /api/orders 201 44ms"}`,
		`{"_time":"2026-04-04T10:00:05Z","_msg":"DELETE /api/users/42 204 5ms"}`,
	}, "\n"))

	patterns := extractLogPatterns(vlBody, "1m", 2)
	if len(patterns) != 2 {
		t.Fatalf("expected top 2 patterns, got %d", len(patterns))
	}
	if patterns[0]["pattern"] == patterns[1]["pattern"] {
		t.Fatalf("expected distinct top patterns, got %#v", patterns)
	}
}

func TestExtractLogPatternsRespectsLimit(t *testing.T) {
	vlBody := []byte(strings.Join([]string{
		`{"_time":"2026-04-04T10:00:00Z","_msg":"GET /api/users 200 15ms","level":"info"}`,
		`{"_time":"2026-04-04T10:00:01Z","_msg":"POST /api/orders 201 142ms","level":"info"}`,
		`{"_time":"2026-04-04T10:00:02Z","_msg":"DELETE /api/orders/123 403 3ms","level":"error"}`,
	}, "\n"))

	patterns := extractLogPatterns(vlBody, "15s", 1)
	if len(patterns) != 1 {
		t.Fatalf("expected a single pattern with limit=1, got %d", len(patterns))
	}
}

func TestExtractLogPatterns_DefaultAndMaxLimitPaths(t *testing.T) {
	lines := make([]string, 0, maxPatternResponseLimit+25)
	for i := 0; i < maxPatternResponseLimit+25; i++ {
		lines = append(lines, `{"_time":"2026-04-04T10:00:00Z","_msg":"tokA`+strconv.Itoa(i)+` tokB`+strconv.Itoa(i)+` tokC`+strconv.Itoa(i)+` tokD`+strconv.Itoa(i)+`","level":"info"}`)
	}
	vlBody := []byte(strings.Join(lines, "\n"))

	defaultLimited := extractLogPatterns(vlBody, "1m", 0)
	if len(defaultLimited) != 50 {
		t.Fatalf("expected default limit of 50, got %d", len(defaultLimited))
	}

	capped := extractLogPatterns(vlBody, "1m", maxPatternResponseLimit+500)
	if len(capped) != maxPatternResponseLimit {
		t.Fatalf("expected capped limit of %d, got %d", maxPatternResponseLimit, len(capped))
	}
}

func TestExtractLogPatterns_GroupsByBucketAndDetectedLevel(t *testing.T) {
	vlBody := []byte(strings.Join([]string{
		`{"_time":"2026-04-04T10:00:01Z","_msg":"GET /api/users 200 15ms","detected_level":"warn"}`,
		`{"_time":"2026-04-04T10:00:09Z","_msg":"GET /api/users 200 16ms","detected_level":"warn"}`,
		`{"_time":"2026-04-04T10:00:12Z","_msg":"GET /api/users 200 17ms","level":"info"}`,
	}, "\n"))

	patterns := extractLogPatterns(vlBody, "10s", 10)
	if len(patterns) != 2 {
		t.Fatalf("expected two grouped patterns, got %d", len(patterns))
	}

	first := patterns[0]
	if first["level"] != "warn" {
		t.Fatalf("expected detected_level to win, got %#v", first)
	}
	samples, ok := first["samples"].([][]interface{})
	if !ok || len(samples) != 1 {
		t.Fatalf("expected warn entries to collapse into one bucket, got %#v", first["samples"])
	}
	if got := samples[0][1]; got != 2 {
		t.Fatalf("expected warn bucket count 2, got %#v", got)
	}
}

func TestExtractLogPatterns_DrainLikeTemplateMerging(t *testing.T) {
	vlBody := []byte(strings.Join([]string{
		`{"_time":"2026-04-04T10:00:01Z","_msg":"level=info msg=request id=1 user=alice"}`,
		`{"_time":"2026-04-04T10:00:02Z","_msg":"level=info msg=request id=2 user=bob"}`,
		`{"_time":"2026-04-04T10:00:03Z","_msg":"level=info msg=request id=3 user=carol"}`,
	}, "\n"))

	patterns := extractLogPatterns(vlBody, "1m", 10)
	if len(patterns) != 1 {
		t.Fatalf("expected one merged drain-like pattern, got %d", len(patterns))
	}
	got, _ := patterns[0]["pattern"].(string)
	if got != "level=info msg=request id=<_> user=<_>" {
		t.Fatalf("unexpected merged pattern: %q", got)
	}
}

func TestExtractLogPatterns_SkipsTooShortMessagesLikeLokiDrain(t *testing.T) {
	vlBody := []byte(strings.Join([]string{
		`{"_time":"2026-04-04T10:00:01Z","_msg":"ok done"}`,
		`{"_time":"2026-04-04T10:00:02Z","_msg":"still ok"}`,
	}, "\n"))

	patterns := extractLogPatterns(vlBody, "1m", 10)
	if len(patterns) != 0 {
		t.Fatalf("expected no patterns for messages below minimum token length, got %d", len(patterns))
	}
}

func TestExtractLogPatternsFromWindowEntries(t *testing.T) {
	entries := []queryRangeWindowEntry{
		{
			Stream: map[string]string{"detected_level": "info"},
			Value:  []interface{}{"1712311201000000000", "GET /api/users 200 15ms"},
		},
		{
			Stream: map[string]string{"detected_level": "info"},
			Value:  []interface{}{"1712311202000000000", "GET /api/users 200 22ms"},
		},
		{
			Stream: map[string]string{"level": "error"},
			Value:  []interface{}{"1712311203000000000", "POST /api/orders 500 142ms"},
		},
	}

	patterns := extractLogPatternsFromWindowEntries(entries, "10s", 10)
	if len(patterns) != 2 {
		t.Fatalf("expected 2 patterns from window entries, got %d", len(patterns))
	}
	first := patterns[0]
	samples, ok := first["samples"].([][]interface{})
	if !ok || len(samples) == 0 {
		t.Fatalf("expected samples in first pattern, got %#v", first["samples"])
	}
}

func TestParsePatternUnixSeconds(t *testing.T) {
	tests := []struct {
		name string
		raw  interface{}
		want int64
	}{
		{name: "rfc3339", raw: "2026-04-04T10:00:00Z", want: 1775296800},
		{name: "seconds", raw: "1775296800", want: 1775296800},
		{name: "millis", raw: "1775296800000", want: 1775296800},
		{name: "micros", raw: "1775296800000000", want: 1775296800},
		{name: "nanos", raw: "1775296800000000000", want: 1775296800},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parsePatternUnixSeconds(tc.raw)
			if !ok {
				t.Fatalf("expected parse success for %v", tc.raw)
			}
			if got != tc.want {
				t.Fatalf("expected %d, got %d", tc.want, got)
			}
		})
	}
}

func TestExtractLogPatterns_AlternateMessageAndTimestampFields(t *testing.T) {
	vlBody := []byte(strings.Join([]string{
		`{"timestamp":"2026-04-04T10:00:00Z","message":"GET /v1/users 200 15ms","detected_level":"info"}`,
		`{"ts":"1775296801","msg":"GET /v1/users 200 19ms","level":"info"}`,
		`{"time":"1775296802","line":"POST /v1/orders 201 88ms","level":"info"}`,
	}, "\n"))

	patterns := extractLogPatterns(vlBody, "1m", 10)
	if len(patterns) == 0 {
		t.Fatalf("expected patterns for alternate message/timestamp field names, got %#v", patterns)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func cloneStreams(streams []map[string]interface{}) []map[string]interface{} {
	result := make([]map[string]interface{}, len(streams))
	for i, s := range streams {
		newStream := make(map[string]interface{})
		for k, v := range s {
			if labels, ok := v.(map[string]string); ok {
				newLabels := make(map[string]string, len(labels))
				for lk, lv := range labels {
					newLabels[lk] = lv
				}
				newStream[k] = newLabels
			} else if values, ok := v.([][]string); ok {
				newValues := make([][]string, len(values))
				for j, val := range values {
					newVal := make([]string, len(val))
					copy(newVal, val)
					newValues[j] = newVal
				}
				newStream[k] = newValues
			} else {
				newStream[k] = v
			}
		}
		result[i] = newStream
	}
	return result
}
