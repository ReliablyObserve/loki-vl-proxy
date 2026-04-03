package proxy

import (
	"net"
	"regexp"
	"strings"
	"text/template"
)

// ansiEscapeRe matches ANSI escape sequences (color codes, cursor movement, etc.)
var ansiEscapeRe = regexp.MustCompile(`\x1b\[[0-9;]*[a-zA-Z]`)

// decolorizeStreams strips ANSI escape sequences from all log lines in streams.
// Implements Loki's `| decolorize` pipe at the proxy level.
// TODO: Remove when VL adds native decolorize support.
func decolorizeStreams(streams []map[string]interface{}) {
	for _, stream := range streams {
		values, ok := stream["values"].([][]string)
		if !ok {
			continue
		}
		for i, val := range values {
			if len(val) >= 2 {
				values[i][1] = ansiEscapeRe.ReplaceAllString(val[1], "")
			}
		}
	}
}

// ipFilterStreams filters log entries where a label value matches a CIDR range.
// Implements Loki's `| label = ip("CIDR")` filter at the proxy level.
// TODO: Remove when VL adds native IP range filtering.
func ipFilterStreams(streams []map[string]interface{}, labelName, cidr string) []map[string]interface{} {
	_, ipNet, err := net.ParseCIDR(cidr)
	if err != nil {
		// Not a valid CIDR — try as single IP
		ip := net.ParseIP(cidr)
		if ip == nil {
			return streams // invalid filter, return unfiltered
		}
		mask := net.CIDRMask(32, 32)
		if ip.To4() == nil {
			mask = net.CIDRMask(128, 128)
		}
		ipNet = &net.IPNet{IP: ip, Mask: mask}
	}

	result := make([]map[string]interface{}, 0, len(streams))
	for _, stream := range streams {
		labels, _ := stream["stream"].(map[string]string)
		if labels == nil {
			continue
		}

		ipStr, ok := labels[labelName]
		if !ok {
			continue
		}

		ip := net.ParseIP(ipStr)
		if ip != nil && ipNet.Contains(ip) {
			result = append(result, stream)
		}
	}
	return result
}

// parseIPFilter extracts the label name and CIDR from a LogQL ip() filter expression.
// Supports: `| addr = ip("192.168.0.0/16")` and `| addr == ip("10.0.0.0/8")`
func parseIPFilter(query string) (label, cidr string, ok bool) {
	// Match: label = ip("CIDR") or label == ip("CIDR")
	re := regexp.MustCompile(`(\w+)\s*==?\s*ip\(\s*"([^"]+)"\s*\)`)
	m := re.FindStringSubmatch(query)
	if m == nil {
		return "", "", false
	}
	return m[1], m[2], true
}

// applyLineFormatTemplate applies a Go text/template to log line values.
// Implements Loki's `| line_format "{{.status}} {{.method | ToUpper}}"` with full template support.
// TODO: Remove when VL adds equivalent template formatting.
func applyLineFormatTemplate(streams []map[string]interface{}, tmplStr string) {
	tmplStr = strings.Trim(tmplStr, `"`)

	// Register Loki-compatible template functions
	funcMap := template.FuncMap{
		"ToUpper":    strings.ToUpper,
		"ToLower":    strings.ToLower,
		"Title":      strings.Title,
		"Replace":    strings.Replace,
		"TrimSpace":  strings.TrimSpace,
		"TrimPrefix": strings.TrimPrefix,
		"TrimSuffix": strings.TrimSuffix,
		"HasPrefix":  strings.HasPrefix,
		"HasSuffix":  strings.HasSuffix,
		"Contains":   strings.Contains,
		"default": func(def string, val ...string) string {
			// In pipeline: {{.field | default "N/A"}} — val[0] is the piped value
			if len(val) > 0 && val[0] != "" {
				return val[0]
			}
			return def
		},
	}

	tmpl, err := template.New("line_format").Option("missingkey=zero").Funcs(funcMap).Parse(tmplStr)
	if err != nil {
		return // invalid template, leave lines unchanged
	}

	for _, stream := range streams {
		labels, _ := stream["stream"].(map[string]string)
		if labels == nil {
			labels = map[string]string{}
		}

		values, ok := stream["values"].([][]string)
		if !ok {
			continue
		}

		for i, val := range values {
			if len(val) < 2 {
				continue
			}

			// Build template data from labels + line
			data := make(map[string]string, len(labels)+1)
			for k, v := range labels {
				data[k] = v
			}
			data["_line"] = val[1]

			var buf strings.Builder
			if err := tmpl.Execute(&buf, data); err == nil {
				values[i][1] = buf.String()
			}
		}
	}
}

// extractLineFormatTemplate extracts the template string from a line_format query.
// Returns empty string if not found.
func extractLineFormatTemplate(query string) string {
	re := regexp.MustCompile(`\|\s*line_format\s+"((?:[^"\\]|\\.)*)"`)
	m := re.FindStringSubmatch(query)
	if m != nil {
		return m[1]
	}
	return ""
}
