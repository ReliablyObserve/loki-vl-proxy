package proxy

import (
	"fmt"
	"net/http"
	"strings"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// Settings of -logql-dotted-names and -label-browse-extensions.
const (
	CompatAuto   = "auto"
	CompatReject = "reject"
	CompatAccept = "accept"
	CompatOn     = "on"
	CompatOff    = "off"
)

// lokiProfile reports whether the label settings form the Loki-compatible
// profile: underscore label names and translated metadata fields, so every
// name a client sees is a valid Loki label name. "auto" settings follow it:
// Loki's grammar for LogQL names, Loki's label endpoint parameters. The hybrid
// and native metadata modes (and passthrough labels) expose dotted
// VictoriaLogs field names on purpose, so "auto" keeps their extensions on.
func lokiProfile(style LabelStyle, mode MetadataFieldMode) bool {
	return style == LabelStyleUnderscores && mode == MetadataFieldModeTranslated
}

// resolveDottedNames returns whether LogQL dotted names are rejected with
// Loki's parse error for -logql-dotted-names=setting.
func resolveDottedNames(setting string, style LabelStyle, mode MetadataFieldMode) (reject bool, err error) {
	switch strings.TrimSpace(setting) {
	case "", CompatAuto:
		return lokiProfile(style, mode), nil
	case CompatReject:
		return true, nil
	case CompatAccept:
		return false, nil
	}
	return false, fmt.Errorf("invalid -logql-dotted-names %q: want auto, reject or accept", setting)
}

// resolveLabelBrowse returns whether the label endpoints honour the proxy's
// browse parameters for -label-browse-extensions=setting.
func resolveLabelBrowse(setting string, style LabelStyle, mode MetadataFieldMode, indexedCache bool) (bool, error) {
	switch strings.TrimSpace(setting) {
	case "", CompatAuto:
		return !lokiProfile(style, mode) || indexedCache, nil
	case CompatOn:
		return true, nil
	case CompatOff:
		return false, nil
	}
	return false, fmt.Errorf("invalid -label-browse-extensions %q: want auto, on or off", setting)
}

// lokiNameError returns Loki's parse error for a LogQL name Loki's grammar
// rejects (a "." token), or "" when dotted names are accepted.
func (p *Proxy) lokiNameError(query string) string {
	if !p.rejectDottedNames {
		return ""
	}
	return logqlpkg.DottedNameError(query)
}

// lokiNameCheck returns lokiNameError for selector-parameter validation, or
// nil when dotted names are accepted.
func (p *Proxy) lokiNameCheck() func(string) string {
	if !p.rejectDottedNames {
		return nil
	}
	return logqlpkg.DottedNameError
}

// labelBrowseExtensions reports whether the label endpoints honour the
// proxy's browse parameters (search/q on /labels and /label/{name}/values,
// limit and offset on /label/{name}/values). Loki reads only start, end and
// query there (loghttp.ParseLabelQuery) and returns every name or value.
func (p *Proxy) labelBrowseExtensions() bool {
	return p.labelBrowse
}

// detectedFieldName is the detected_fields label of a key parsed from a JSON
// log line. Loki's json parser sanitizes keys (http.method -> http_method) and
// reports the original key in jsonPath; while dotted names are rejected in
// LogQL, the proxy does the same, so every field Drilldown offers is a name
// the proxy accepts. With dotted names accepted, the stored name is kept.
func (p *Proxy) detectedFieldName(key string) string {
	if !p.rejectDottedNames {
		return key
	}
	return lokiJSONKeyLabel(key)
}

// lokiJSONKeyLabel is Loki's sanitizeLabelKey for a top-level parsed key:
// surrounding space trimmed, a leading digit prefixed with an underscore, and
// every rune outside [A-Za-z0-9_] replaced by an underscore (no collapsing).
func lokiJSONKeyLabel(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return key
	}
	if key[0] >= '0' && key[0] <= '9' {
		key = "_" + key
	}
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return '_'
	}, key)
}

// labelSearchParam returns the browse search term (search, else q), or ""
// when the label endpoints ignore browse parameters.
func (p *Proxy) labelSearchParam(r *http.Request) string {
	if !p.labelBrowseExtensions() {
		return ""
	}
	if search := strings.TrimSpace(r.FormValue("search")); search != "" {
		return search
	}
	return strings.TrimSpace(r.FormValue("q"))
}

// labelLimitParam returns the client's limit on a label endpoint, or "" when
// the label endpoints ignore browse parameters.
func (p *Proxy) labelLimitParam(r *http.Request) string {
	if !p.labelBrowseExtensions() {
		return ""
	}
	return r.FormValue("limit")
}
