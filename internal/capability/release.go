package capability

import (
	"strconv"
	"strings"
	"time"
)

// Flavour is the build family of a VictoriaLogs release, read from the suffix of its version.
type Flavour uint8

// The flavours. FlavourUnknown is a version suffix the proxy does not recognise (a gate that lists the flavours it
// is in never counts such a build).
const (
	FlavourUnknown Flavour = iota
	FlavourOSS
	FlavourEnterprise
	FlavourEnterpriseFIPS
)

// String is the flavour's label.
func (f Flavour) String() string {
	switch f {
	case FlavourOSS:
		return "oss"
	case FlavourEnterprise:
		return "enterprise"
	case FlavourEnterpriseFIPS:
		return "enterprise-fips"
	}
	return "unknown"
}

// FlavourSet is a set of flavours; the zero value means every flavour.
type FlavourSet uint8

// Flavours builds a set.
func Flavours(fs ...Flavour) FlavourSet {
	var s FlavourSet
	for _, f := range fs {
		s |= 1 << f
	}
	return s
}

// Has reports whether f is in the set (every flavour is, for the zero set).
func (s FlavourSet) Has(f Flavour) bool { return s == 0 || s&(1<<f) != 0 }

// Source is where a release was read from.
type Source uint8

// The sources of a release.
const (
	SourceNone    Source = iota
	SourceMetrics        // the VictoriaLogs /metrics endpoint
	SourceHeader         // a response header
	SourceConfig         // the operator or a test named it
	numSources
)

var sourceLabels = [...]string{"none", "metrics", "header", "config"}

// String is the source's label.
func (s Source) String() string {
	if int(s) < len(sourceLabels) {
		return sourceLabels[s]
	}
	return sourceLabels[SourceNone]
}

// maxRaw bounds the raw text the layer keeps of a release: the value comes from a response the proxy does not
// control.
const maxRaw = 128

// maxSuffix bounds the unrecognised version suffix a release keeps.
const maxSuffix = 32

// Release is one VictoriaLogs build as observed.
type Release struct {
	Major, Minor, Patch int
	Flavour             Flavour
	// Suffix is the version suffix of a build whose flavour is not recognised ("rc1", "cluster"), at most
	// maxSuffix bytes: it keeps such a build apart from the plain release in the canonical name and the digest.
	Suffix string
	Known  bool
	Raw    string // as reported (vm_app_version, a header), at most maxRaw bytes
	Source Source
	SeenAt time.Time
}

// Compare orders releases by version, then flavour (an unrecognised build sorts lowest, so it is the one a mixed
// set is judged by). A release that is not Known sorts below every known one.
func (r Release) Compare(o Release) int {
	switch {
	case r.Known != o.Known:
		if r.Known {
			return 1
		}
		return -1
	case r.Major != o.Major:
		return r.Major - o.Major
	case r.Minor != o.Minor:
		return r.Minor - o.Minor
	case r.Patch != o.Patch:
		return r.Patch - o.Patch
	}
	if d := int(r.Flavour) - int(o.Flavour); d != 0 {
		return d
	}
	return strings.Compare(r.Suffix, o.Suffix)
}

// String is the canonical name of the release, "v1.52.2-enterprise" ("v1.54.0" for an open-source build), or
// "unknown".
func (r Release) String() string {
	if !r.Known {
		return "unknown"
	}
	s := "v" + strconv.Itoa(r.Major) + "." + strconv.Itoa(r.Minor) + "." + strconv.Itoa(r.Patch)
	switch r.Flavour {
	case FlavourEnterprise:
		s += "-enterprise"
	case FlavourEnterpriseFIPS:
		s += "-enterprise-fips"
	case FlavourUnknown:
		if r.Suffix != "" {
			s += "-" + r.Suffix
		}
	}
	return s
}

// ParseRelease reads a release the caller trusts to be VictoriaLogs ("v1.53.0", "1.52.1", "v1.52.2-enterprise",
// "v1.54.0-enterprise-fips"); the operator's -backend-min-version and tests use it. An unreadable text is not Known.
// What a backend says about itself goes through ParseBuild, which also checks the product.
func ParseRelease(s string) Release {
	s = strings.TrimSpace(s)
	if len(s) > maxRaw {
		return Release{}
	}
	r, ok := parseSemverSuffix(strings.TrimPrefix(s, "v"), true)
	if !ok {
		return Release{}
	}
	r.Raw = s
	r.Source = SourceConfig
	return r
}

// parseSemverSuffix reads "X.Y.Z" followed by the flavour suffix and the build counters
// ("-enterprise-fips-0-g6433dae586"): the version of a release string with the product name cut off.
func parseSemverSuffix(s string, allowTwo bool) (Release, bool) {
	var nums [3]int
	i := 0
	for n := 0; n < 3; n++ {
		start := i
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
		}
		if i == start || i-start > 4 {
			return Release{}, false
		}
		nums[n], _ = strconv.Atoi(s[start:i])
		if n < 2 {
			if i < len(s) && s[i] == '.' {
				i++
				continue
			}
			if n == 1 && allowTwo && (i == len(s) || s[i] == '-') {
				break // "1.50": the patch is zero
			}
			return Release{}, false
		}
	}
	r := Release{Major: nums[0], Minor: nums[1], Patch: nums[2], Known: true, Flavour: FlavourOSS}
	rest := s[i:]
	if rest == "" {
		return r, true
	}
	if rest[0] != '-' && rest[0] != '+' {
		return Release{}, false // "1.2.3x", "1.2.3.4": not a release version
	}
	r.Flavour = flavourOfSuffix(rest[1:])
	if r.Flavour == FlavourUnknown {
		var kept []string
		for _, part := range strings.Split(rest[1:], "-") {
			if part != "" && part != "dirty" && !isBuildCounter(part) {
				kept = append(kept, part)
			}
		}
		suffix := strings.Join(kept, "-")
		if len(suffix) > maxSuffix {
			suffix = suffix[:maxSuffix]
		}
		r.Suffix = sanitizeSuffix(suffix)
		if r.Suffix == "" {
			r.Suffix = "unrecognised"
		}
	}
	return r, true
}

// flavourOfSuffix reads the dash-separated tail of a version. Build counters ("0", "g6433dae586", "dirty") are
// not part of the flavour; "enterprise" and "enterprise-fips" are; anything else leaves the flavour unknown.
func flavourOfSuffix(tail string) Flavour {
	f := FlavourOSS
	parts := strings.Split(tail, "-")
	for i := 0; i < len(parts); i++ {
		switch p := parts[i]; {
		case p == "" || p == "dirty" || isBuildCounter(p):
		case p == "enterprise" && f == FlavourOSS:
			f = FlavourEnterprise
			if i+1 < len(parts) && parts[i+1] == "fips" {
				f = FlavourEnterpriseFIPS
				i++
			}
		default:
			return FlavourUnknown
		}
	}
	return f
}

// isBuildCounter reports a commit count ("0", "12") or an abbreviated commit ("g6433dae586").
func isBuildCounter(p string) bool {
	if p[0] == 'g' {
		p = p[1:]
		if p == "" {
			return false
		}
		for i := 0; i < len(p); i++ {
			c := p[i]
			if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
				return false
			}
		}
		return true
	}
	for i := 0; i < len(p); i++ {
		if p[i] < '0' || p[i] > '9' {
			return false
		}
	}
	return true
}

// ParseBuild reads what a backend says about itself and accepts it only when it names VictoriaLogs: the
// `victoria-logs-<date>-<time>-tags-vX.Y.Z[-flavour]-<n>-g<hash>` build string, `VictoriaLogs/vX.Y.Z`,
// `victoria-logs vX.Y.Z`. Other products of the same version library (vmauth, vlagent, vmagent, VictoriaMetrics, the
// stack's vmauth runs VictoriaMetrics v1.155.0) and a bare semver name no VictoriaLogs and are refused: a
// gate decided from a version of another program would enable features the backend lacks.
func ParseBuild(raw string, src Source) (Release, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" || len(raw) > maxRaw {
		return Release{}, false
	}
	rest, ok := cutProduct(raw)
	if !ok {
		return Release{}, false
	}
	r, ok := parseSemverSuffix(rest, false)
	if !ok {
		return Release{}, false
	}
	r.Raw = raw
	r.Source = src
	return r, true
}

// cutProduct removes the leading VictoriaLogs product name and the build stamp ("20261113-102924-tags-") and
// returns the version text that follows.
func cutProduct(s string) (string, bool) {
	const short, long = "victorialogs", "victoria-logs"
	switch {
	case hasPrefixFold(s, long):
		s = s[len(long):]
	case hasPrefixFold(s, short):
		s = s[len(short):]
	default:
		return "", false
	}
	if s == "" || !strings.ContainsRune("-_ /:=", rune(s[0])) {
		return "", false // "victoria-logsxyz", "victorialogs-proxy..." is not matched: a separator follows the name
	}
	s = strings.TrimLeft(s, "-_ /:=")
	// The build string: <yyyymmdd>-<hhmmss>-tags-v<version>-<n>-g<hash>.
	if i := strings.Index(s, "tags-"); i >= 0 && allBuildStamp(s[:i]) {
		s = s[i+len("tags-"):]
	}
	return strings.TrimPrefix(s, "v"), true
}

func allBuildStamp(s string) bool {
	for i := 0; i < len(s); i++ {
		if (s[i] < '0' || s[i] > '9') && s[i] != '-' {
			return false
		}
	}
	return true
}

func hasPrefixFold(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
}

// ParseMetrics reads the release out of a VictoriaLogs /metrics payload: the `version` label of the
// victorialogs_build_info series, or of the vm_app_version series when it names VictoriaLogs (the same endpoint behind
// vmauth is vmauth's own, whose vm_app_version names vmauth and is not a VictoriaLogs release).
func ParseMetrics(text string) (Release, bool) {
	for len(text) > 0 {
		line := text
		if i := strings.IndexByte(text, '\n'); i >= 0 {
			line, text = text[:i], text[i+1:]
		} else {
			text = ""
		}
		line = strings.TrimSpace(line)
		var named bool
		switch {
		case strings.HasPrefix(line, "victorialogs_build_info{"):
			named = true
		case strings.HasPrefix(line, "vm_app_version{"):
		default:
			continue
		}
		for _, label := range []string{`version="`, `short_version="`} {
			for from := 0; ; {
				i := strings.Index(line[from:], label)
				if i < 0 {
					break
				}
				i += from
				from = i + len(label)
				if i > 0 && isLabelNameByte(line[i-1]) {
					continue // "short_version" is not "version"
				}
				value := line[from:]
				j := strings.IndexByte(value, '"')
				if j < 0 {
					break
				}
				if r, ok := ParseBuild(value[:j], SourceMetrics); ok {
					return r, true
				}
				if named {
					// The series itself names VictoriaLogs: a bare version label is enough.
					if r := ParseRelease(value[:j]); r.Known {
						r.Source = SourceMetrics
						return r, true
					}
				}
			}
		}
	}
	return Release{}, false
}

// isLabelNameByte reports whether c continues a label name ("short_version" must not match "version").
func isLabelNameByte(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}

// AtLeast reports whether r's version (flavour aside) is at least o's. A release that is not Known is below every
// known one.
func (r Release) AtLeast(o Release) bool {
	if !o.Known {
		return true
	}
	return r.Known && cmp3(r.Major, r.Minor, r.Patch, [3]int{o.Major, o.Minor, o.Patch}) >= 0
}

// sanitizeSuffix keeps the characters of a version suffix and drops the rest (the suffix reaches a metric label and
// the canonical name).
func sanitizeSuffix(s string) string {
	b := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '-' || c == '.' || c == '_' {
			b = append(b, c)
		}
	}
	return string(b)
}
