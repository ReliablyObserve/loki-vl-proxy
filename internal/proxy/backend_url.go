package proxy

import (
	"net/url"
	"strings"
)

// backendRequestURL is the URL of path on a backend: path is appended to the path the backend URL was configured
// with, so a backend served under a prefix (-backend=https://host/logs, VictoriaLogs behind a reverse proxy that
// shares one hostname) keeps that prefix on every request. A trailing slash on the configured URL is ignored, so
// https://host/logs and https://host/logs/ reach the same paths. path is one of the proxy's own fixed paths.
func backendRequestURL(base *url.URL, path string) url.URL {
	u := *base
	u.Path = strings.TrimSuffix(base.Path, "/") + path
	if base.RawPath != "" {
		u.RawPath = strings.TrimSuffix(base.RawPath, "/") + path
	}
	return u
}
