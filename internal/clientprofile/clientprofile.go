// Package clientprofile is the typed profile of the client behind one request: which surface sent it (Grafana
// Explore, Logs Drilldown, another Grafana Loki datasource user such as a dashboard, or a plain API client) and the
// Grafana and Logs Drilldown versions the request names. It is read once per request from the request's headers and
// carried in its context; behaviour that depends on the client reads it from there.
//
// Every value is bounded: a version is major.minor.patch numbers or unknown, a surface one of a closed set, so a
// profile can label a metric or a log line without carrying any text a client sent.
package clientprofile

import (
	"context"
	"strconv"
)

// Surface is the client surface of a request.
type Surface uint8

const (
	// SurfaceUnknown is a request whose client the profile cannot tell.
	SurfaceUnknown Surface = iota
	// SurfaceAPI is a request from a client that is not Grafana (no Grafana User-Agent or header).
	SurfaceAPI
	// SurfaceExplore is a request of Grafana's Loki datasource from Explore.
	SurfaceExplore
	// SurfaceDrilldown is a request of the Logs Drilldown app (grafana-lokiexplore-app).
	SurfaceDrilldown
	// SurfaceDatasource is any other request of Grafana's Loki datasource: dashboards, alerting, the datasource
	// health check and its resource calls.
	SurfaceDatasource
)

var surfaceLabels = [...]string{"unknown", "api", "explore", "drilldown", "datasource"}

// Label is the surface's name for metrics and logs.
func (s Surface) Label() string {
	if int(s) < len(surfaceLabels) {
		return surfaceLabels[s]
	}
	return surfaceLabels[SurfaceUnknown]
}

// Grafana reports whether the surface is one of Grafana's.
func (s Surface) Grafana() bool {
	return s == SurfaceExplore || s == SurfaceDrilldown || s == SurfaceDatasource
}

// Version is a client release as major.minor.patch; the zero value is unknown.
type Version struct {
	Major, Minor, Patch uint16
	Known               bool
}

// maxVersionPart bounds every number of a version: a part with more digits is no version.
const maxVersionPart = 9999

// ParseVersion reads the release at the start of s: three dot-separated runs of 1 to 4 ASCII digits, each at most
// 9999, followed by the end of s or by a character that is not a digit or a dot (a pre-release or build suffix such
// as -beta1 or +security-01 is dropped). A leading v is accepted. Anything else is unknown.
func ParseVersion(s string) Version {
	if len(s) > 0 && (s[0] == 'v' || s[0] == 'V') {
		s = s[1:]
	}
	var parts [3]uint16
	i := 0
	for p := 0; p < 3; p++ {
		start := i
		n := 0
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			if i-start == 4 {
				return Version{}
			}
			n = n*10 + int(s[i]-'0')
			i++
		}
		if i == start || n > maxVersionPart {
			return Version{}
		}
		parts[p] = uint16(n)
		if p < 2 {
			if i >= len(s) || s[i] != '.' {
				return Version{}
			}
			i++
		}
	}
	if i < len(s) && (s[i] == '.' || (s[i] >= '0' && s[i] <= '9')) {
		return Version{} // a fourth part or a longer number
	}
	return Version{Major: parts[0], Minor: parts[1], Patch: parts[2], Known: true}
}

// Label is the version as major.minor.patch, or "unknown".
func (v Version) Label() string {
	if !v.Known {
		return "unknown"
	}
	b := make([]byte, 0, 14)
	b = strconv.AppendUint(b, uint64(v.Major), 10)
	b = append(b, '.')
	b = strconv.AppendUint(b, uint64(v.Minor), 10)
	b = append(b, '.')
	b = strconv.AppendUint(b, uint64(v.Patch), 10)
	return string(b)
}

// AtLeast reports whether v is known and at least major.minor.patch.
func (v Version) AtLeast(major, minor, patch uint16) bool {
	if !v.Known {
		return false
	}
	if v.Major != major {
		return v.Major > major
	}
	if v.Minor != minor {
		return v.Minor > minor
	}
	return v.Patch >= patch
}

// Profile is the client of one request.
type Profile struct {
	Surface Surface
	// Grafana is the Grafana release that sent the request (its User-Agent), unknown for another client.
	Grafana Version
	// Datasource is the release of Grafana's Loki datasource plugin that sent the request (the loki/ token of the
	// User-Agent; Grafana bundles it, and a release can update it on its own).
	Datasource Version
	// Drilldown is the Logs Drilldown app release when the request names it; unknown otherwise.
	Drilldown Version
}

type contextKey struct{}

// With returns ctx carrying p.
func With(ctx context.Context, p Profile) context.Context {
	return context.WithValue(ctx, contextKey{}, p)
}

// From returns the profile ctx carries, the zero (unknown) profile when it carries none.
func From(ctx context.Context) Profile {
	p, _ := ctx.Value(contextKey{}).(Profile)
	return p
}
