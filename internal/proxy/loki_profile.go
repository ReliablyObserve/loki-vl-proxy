package proxy

import (
	"net/http"
	"strings"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// lokiProfile reports whether the label settings form the Loki-compatible
// profile: underscore label names and translated metadata fields, so every
// name a client sees is a valid Loki label name. In that profile the proxy
// holds requests to Loki's contract too: LogQL names follow Loki's grammar
// (a dotted name is Loki's parse error) and the label-browse request
// extensions are ignored the way Loki ignores unknown parameters. The hybrid
// and native modes expose dotted VictoriaLogs field names on purpose, so they
// keep accepting them in queries.
func lokiProfile(style LabelStyle, mode MetadataFieldMode) bool {
	return style == LabelStyleUnderscores && mode == MetadataFieldModeTranslated
}

// lokiNameError returns Loki's parse error for a LogQL name Loki's grammar
// rejects (a "." token), or "" outside the Loki-compatible profile.
func (p *Proxy) lokiNameError(query string) string {
	if !p.lokiNames {
		return ""
	}
	return logqlpkg.DottedNameError(query)
}

// lokiNameCheck returns lokiNameError for selector-parameter validation, or
// nil outside the Loki-compatible profile.
func (p *Proxy) lokiNameCheck() func(string) string {
	if !p.lokiNames {
		return nil
	}
	return logqlpkg.DottedNameError
}

// labelBrowseExtensions reports whether the label endpoints honour the
// proxy's browse parameters (search/q on /labels and /label/{name}/values,
// limit and offset on /label/{name}/values). Loki reads only start, end and
// query there (loghttp.ParseLabelQuery) and returns every name or value, so
// the Loki-compatible profile ignores them unless the operator opted into
// the indexed browse cache (-label-values-indexed-cache), whose documented
// purpose is that windowed browse.
func (p *Proxy) labelBrowseExtensions() bool {
	return !p.lokiNames || p.labelValuesIndexedCache
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
