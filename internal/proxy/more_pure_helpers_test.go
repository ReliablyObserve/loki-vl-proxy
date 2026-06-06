package proxy

import (
	"math"
	"testing"
	"time"
)

func TestStripOuterLabelReplace(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"no_wrapper", `sum(rate({app="x"}[5m]))`, `sum(rate({app="x"}[5m]))`},
		{"single_wrap", `label_replace(rate({app="x"}[5m]), "a", "b", "c", "d")`, `rate({app="x"}[5m])`},
		{"double_wrap",
			`label_replace(label_replace(rate({app="x"}[5m]), "a", "b", "c", "d"), "e", "f", "g", "h")`,
			`rate({app="x"}[5m])`},
		{"nested_function_inside",
			`label_replace(sum by (level)(rate({app="x"}[5m])), "a", "b", "c", "d")`,
			`sum by (level)(rate({app="x"}[5m]))`},
		{"malformed_no_comma_breaks",
			`label_replace(rate({app="x"}[5m]))`,
			// no comma at depth-0 → loop bails, returns input unchanged
			`label_replace(rate({app="x"}[5m]))`},
		{"whitespace_handled",
			`  label_replace(  rate({app="x"}[5m])  ,  "a", "b", "c", "d"  )  `,
			`rate({app="x"}[5m])`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := stripOuterLabelReplace(tc.in)
			if got != tc.want {
				t.Errorf("stripOuterLabelReplace:\n  got  %q\n  want %q", got, tc.want)
			}
		})
	}
}

func TestInferUnwrapConv(t *testing.T) {
	cases := map[string]string{
		"sum_over_time({a=\"x\"} | unwrap duration(d) [5m])": "duration",
		"sum_over_time({a=\"x\"} | unwrap bytes(b) [5m])":    "bytes",
		"sum_over_time({a=\"x\"} | unwrap latency [5m])":     "",
		"":                                  "",
		"random text without conv keywords": "",
	}
	for in, want := range cases {
		got := inferUnwrapConv(in)
		if got != want {
			t.Errorf("inferUnwrapConv(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestFormatMetricSampleValue(t *testing.T) {
	tests := []struct {
		in   float64
		want string
	}{
		{0, "0"},
		{1, "1"},
		{42, "42"},
		{-5, "-5"},
		{1.5, "1.5"},
		{0.001, "0.001"},
		{math.NaN(), "NaN"},
		{math.Inf(1), "+Inf"},
		{math.Inf(-1), "-Inf"},
	}
	for _, tc := range tests {
		got := formatMetricSampleValue(tc.in)
		if got != tc.want {
			t.Errorf("formatMetricSampleValue(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestMetricWindowValue(t *testing.T) {
	tests := []struct {
		name   string
		fn     string
		total  float64
		window time.Duration
		want   float64
	}{
		{"rate_5m_300_events", "rate", 300, 5 * time.Minute, 1.0},
		{"rate_zero_window_safe", "rate", 100, 0, 0},
		{"rate_negative_window_safe", "rate", 100, -time.Second, 0},
		{"bytes_rate_60s_120_bytes", "bytes_rate", 120, time.Minute, 2.0},
		{"unknown_func_returns_total", "count_over_time", 42, 5 * time.Minute, 42},
		{"sum_over_time_returns_total", "sum_over_time", 99, time.Hour, 99},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := metricWindowValue(tc.fn, tc.total, tc.window)
			if math.Abs(got-tc.want) > 1e-9 {
				t.Errorf("metricWindowValue(%q, %v, %v) = %v, want %v",
					tc.fn, tc.total, tc.window, got, tc.want)
			}
		})
	}
}

func TestPatternLevelFromEntry(t *testing.T) {
	tests := []struct {
		name  string
		entry map[string]interface{}
		want  string
	}{
		{"detected_level_wins", map[string]interface{}{"detected_level": "error", "level": "info"}, "error"},
		{"level_when_no_detected", map[string]interface{}{"level": "warn"}, "warn"},
		{"empty_detected_falls_through", map[string]interface{}{"detected_level": "   ", "level": "info"}, "info"},
		{"trims_whitespace", map[string]interface{}{"level": "  error  "}, "error"},
		{"from_logfmt_msg",
			map[string]interface{}{"_msg": "ts=2024-01-01 level=error msg=oops"},
			"error"},
		{"from_json_msg",
			map[string]interface{}{"_msg": `{"level":"warn","msg":"oops"}`},
			"warn"},
		{"empty_entry", map[string]interface{}{}, ""},
		{"only_msg_no_level",
			map[string]interface{}{"_msg": "request handled in 4ms"},
			""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := patternLevelFromEntry(tc.entry)
			if got != tc.want {
				t.Errorf("patternLevelFromEntry(%v) = %q, want %q", tc.entry, got, tc.want)
			}
		})
	}
}

func TestParsePatternStepSeconds(t *testing.T) {
	tests := []struct {
		in   string
		want int64
	}{
		{"", 60},
		{"60", 60},
		{"60s", 60},
		{"5m", 300},
		{"1h", 3600},
		{"unparseable", 60}, // fallback to default
		{"0", 60},           // below 1s lower bound
	}
	for _, tc := range tests {
		got := parsePatternStepSeconds(tc.in)
		if got != tc.want {
			t.Errorf("parsePatternStepSeconds(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestExtractLevelFromMsg(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
		ok   bool
	}{
		{"empty", "", "", false},
		{"whitespace_only", "   ", "", false},
		{"json_level", `{"level":"error","msg":"oops"}`, "error", true},
		{"json_severity", `{"severity":"warn","msg":"x"}`, "warn", true},
		{"logfmt_level", `ts=2024 level=error msg=oops`, "error", true},
		{"logfmt_lvl", `lvl=warn other=stuff`, "warn", true},
		{"plain_no_level", `just a free text message`, "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := extractLevelFromMsg(tc.in)
			if ok != tc.ok {
				t.Errorf("extractLevelFromMsg(%q): ok=%v want %v", tc.in, ok, tc.ok)
			}
			if ok && got != tc.want {
				t.Errorf("extractLevelFromMsg(%q): got %q want %q", tc.in, got, tc.want)
			}
		})
	}
}
