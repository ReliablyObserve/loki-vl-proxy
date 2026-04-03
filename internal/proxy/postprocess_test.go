package proxy

import (
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
