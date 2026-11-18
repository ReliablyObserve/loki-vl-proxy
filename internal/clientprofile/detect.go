package clientprofile

import (
	"net/http"
	"strings"
)

// The signals a request carries about its client, as captured on the wire from Grafana 13.3.1 with Logs Drilldown
// 3.2.0, Grafana 13.3.0 with Drilldown 3.1.0 and Grafana 12.4.14 with Drilldown 2.6.0 (testdata/captured-*.json):
//
//   - User-Agent: `Grafana/<grafana version> (<os>; <arch>) loki/<Loki datasource plugin version>` on every request
//     Grafana's backend sends for its Loki datasource.
//   - X-Query-Tags `Source=<source>` on query requests: grafana-lokiexplore-app for Logs Drilldown (on its resource
//     calls too), grafana-explore for an Explore query and logvolhist for Explore's log volume query.
//   - X-Grafana-Referer on resource calls (labels, label values, index/stats, detected_*, patterns, volume): the
//     browser page the call came from. Only its path prefix is read, against a closed set; the URL, which holds the
//     user's query and time range, is never kept.
//
// No header names the Logs Drilldown app's version (its plugin id reaches the proxy only as the Source tag), so
// Profile.Drilldown stays unknown on every captured release.

// sourceDrilldown is the Source tag of Logs Drilldown (grafana-lokiexplore-app); explore sources are Explore's.
const sourceDrilldown = "grafana-lokiexplore-app"

var exploreSources = [...]string{"grafana-explore", "logvolhist"}

// Detect reads the client profile of a request from its headers. It reads a bounded prefix of each header it
// looks at and keeps no text of them.
func Detect(h http.Header) Profile {
	ua := first(h["User-Agent"])
	p := Profile{Grafana: grafanaVersion(ua), Datasource: tokenVersion(ua, "loki/")}
	grafana := p.Grafana.Known || hasGrafanaHeader(h)
	switch source := querySource(h["X-Query-Tags"]); {
	case strings.EqualFold(source, sourceDrilldown):
		p.Surface = SurfaceDrilldown
	case isExploreSource(source):
		p.Surface = SurfaceExplore
	case !grafana:
		p.Surface = SurfaceAPI
	default:
		p.Surface = refererSurface(first(h["X-Grafana-Referer"]))
	}
	return p
}

// first is a header's first value; the names read are canonical, so the map is read directly.
func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

// maxHeaderScan bounds how much of a header the detection reads.
const maxHeaderScan = 512

func clip(s string) string {
	if len(s) > maxHeaderScan {
		return s[:maxHeaderScan]
	}
	return s
}

// grafanaVersion is the version after the `Grafana/` product token of a User-Agent (in any case).
func grafanaVersion(ua string) Version {
	return tokenVersion(ua, "grafana/")
}

// tokenVersion is the version that follows token (lower case) where it starts the User-Agent or follows a space.
func tokenVersion(ua, token string) Version {
	ua = clip(ua)
	for i := 0; i+len(token) <= len(ua); i++ {
		if (i == 0 || ua[i-1] == ' ') && strings.EqualFold(ua[i:i+len(token)], token) {
			return ParseVersion(ua[i+len(token):])
		}
	}
	return Version{}
}

// hasGrafanaHeader reports a header only Grafana's backend sends.
func hasGrafanaHeader(h http.Header) bool {
	for _, name := range [...]string{"X-Grafana-Org-Id", "X-Datasource-Uid", "X-Grafana-Referer", "X-Plugin-Id"} {
		if first(h[name]) != "" {
			return true
		}
	}
	return false
}

// querySource is the value of the Source tag of X-Query-Tags (comma-separated key=value pairs; the first Source
// wins), "" when there is none.
func querySource(values []string) string {
	for _, raw := range values {
		raw = clip(raw)
		for raw != "" {
			var part string
			part, raw, _ = strings.Cut(raw, ",")
			key, val, ok := strings.Cut(part, "=")
			if !ok || !strings.EqualFold(strings.TrimSpace(key), "source") {
				continue
			}
			if val = strings.Trim(strings.TrimSpace(val), `"`); val != "" {
				return val
			}
		}
	}
	return ""
}

func isExploreSource(source string) bool {
	for _, s := range exploreSources {
		if strings.EqualFold(source, s) {
			return true
		}
	}
	return false
}

// refererSurface is the surface of a Grafana request from the path of the page it came from: Logs Drilldown's app
// pages, Explore, or any other page (dashboards, alerting, the datasource settings). Grafana served under a sub path
// (root_url) puts it before these.
func refererSurface(referer string) Surface {
	path := clip(referer)
	if i := strings.Index(path, "://"); i >= 0 {
		path = path[i+3:]
		if j := strings.IndexByte(path, '/'); j >= 0 {
			path = path[j:]
		} else {
			path = "/"
		}
	}
	if i := strings.IndexAny(path, "?#"); i >= 0 {
		path = path[:i]
	}
	switch {
	case strings.Contains(path, "/a/"+sourceDrilldown+"/") || strings.HasSuffix(path, "/a/"+sourceDrilldown):
		return SurfaceDrilldown
	case strings.HasSuffix(path, "/explore") || strings.Contains(path, "/explore/"):
		return SurfaceExplore
	}
	return SurfaceDatasource
}
