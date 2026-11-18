// Package clientprofile is the leaf type of what a request says about the client behind it: the surface it comes
// from (Grafana's Explore, Logs Drilldown, the Loki datasource, a plain API caller) and the versions it announces.
// The capability layer reads it per request (capability.Layer.For); nothing here decides anything, and the package
// imports only the standard library so every layer may depend on it.
//
// Detection (reading the headers into a Profile) is not part of this package yet: a request carries a Profile only
// where something has attached one with With.
package clientprofile

import (
	"context"
	"strconv"
)

// Surface is the client surface a request comes from. The set is closed: Label never returns anything else, so a
// metric label built from it stays bounded.
type Surface uint8

// The surfaces.
const (
	Unknown Surface = iota
	API
	Explore
	Drilldown
	Datasource
)

var surfaceLabels = [...]string{"unknown", "api", "explore", "drilldown", "datasource"}

// Label is the surface's name for metrics and logs.
func (s Surface) Label() string {
	if int(s) < len(surfaceLabels) {
		return surfaceLabels[s]
	}
	return surfaceLabels[Unknown]
}

// Version is a client version as major.minor.patch.
type Version struct {
	Major, Minor, Patch uint16
	Known               bool
}

// maxVersionPart bounds each part to four digits, so a crafted header cannot name an unbounded set of versions.
const maxVersionPart = 9999

// ParseVersion reads the leading major.minor.patch of s ("13.3.1", "13.3.1 (commit)", "3.2.0-rc1"). Anything that
// does not start with three dot-separated numbers of one to four digits is not a version.
func ParseVersion(s string) Version {
	var parts [3]uint16
	i := 0
	for n := 0; n < 3; n++ {
		start := i
		for i < len(s) && s[i] >= '0' && s[i] <= '9' && i-start < 5 {
			i++
		}
		if i == start || i-start > 4 {
			return Version{}
		}
		v, err := strconv.Atoi(s[start:i])
		if err != nil || v > maxVersionPart {
			return Version{}
		}
		parts[n] = uint16(v)
		if n < 2 {
			if i >= len(s) || s[i] != '.' {
				return Version{}
			}
			i++
		}
	}
	return Version{Major: parts[0], Minor: parts[1], Patch: parts[2], Known: true}
}

// Label is "M.m.p", or "unknown" for a version that was not read.
func (v Version) Label() string {
	if !v.Known {
		return "unknown"
	}
	return strconv.Itoa(int(v.Major)) + "." + strconv.Itoa(int(v.Minor)) + "." + strconv.Itoa(int(v.Patch))
}

// Profile is what a request says about its client.
type Profile struct {
	Surface    Surface
	Grafana    Version // the Grafana runtime
	Datasource Version // the bundled Loki datasource (Grafana's User-Agent carries it)
	Drilldown  Version // the Logs Drilldown plugin, when a request carries it
}

type ctxKey struct{}

// With returns ctx carrying p.
func With(ctx context.Context, p Profile) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

// From is the profile ctx carries, or the zero Profile (Unknown surface, no versions).
func From(ctx context.Context) Profile {
	p, _ := ctx.Value(ctxKey{}).(Profile)
	return p
}
