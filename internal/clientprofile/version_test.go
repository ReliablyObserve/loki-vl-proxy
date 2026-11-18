package clientprofile

import (
	"context"
	"regexp"
	"strings"
	"testing"
)

func TestParseVersion(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
	}{
		{"13.3.1", "13.3.1"},
		{"v13.3.1", "13.3.1"},
		{"12.4.14", "12.4.14"},
		{"13.3.0-security-01", "13.3.0"},
		{"11.6.6+security-01", "11.6.6"},
		{"3.2.0", "3.2.0"},
		{"0.0.0", "0.0.0"},
		{"9999.9999.9999", "9999.9999.9999"},
		{"13.3.1 (linux/amd64)", "13.3.1"},
		{"13.3.1/abc", "13.3.1"},
		{"13.3", "unknown"},
		{"13", "unknown"},
		{"", "unknown"},
		{"v", "unknown"},
		{"13.3.1.4", "unknown"},
		{"13.3.10000", "unknown"},
		{"10000.0.0", "unknown"},
		{"13..1", "unknown"},
		{".13.3.1", "unknown"},
		{"13.3.x", "unknown"},
		{"x13.3.1", "unknown"},
		{"١٣.3.1", "unknown"}, // non-ASCII digits are no version
		{"13.3.1\x00", "13.3.1"},
		{"13.03.01", "13.3.1"},
		{strings.Repeat("9", 200), "unknown"},
	} {
		if got := ParseVersion(tc.in).Label(); got != tc.want {
			t.Errorf("ParseVersion(%q) = %s, want %s", tc.in, got, tc.want)
		}
	}
}

func TestVersionAtLeast(t *testing.T) {
	v := ParseVersion("13.3.1")
	for _, tc := range []struct {
		major, minor, patch uint16
		want                bool
	}{{13, 3, 1, true}, {13, 3, 0, true}, {13, 2, 9, true}, {12, 99, 99, true}, {13, 3, 2, false}, {13, 4, 0, false}, {14, 0, 0, false}} {
		if got := v.AtLeast(tc.major, tc.minor, tc.patch); got != tc.want {
			t.Errorf("13.3.1 AtLeast(%d.%d.%d) = %v", tc.major, tc.minor, tc.patch, got)
		}
	}
	if (Version{}).AtLeast(0, 0, 0) {
		t.Error("an unknown version is at least nothing")
	}
}

func TestSurfaceLabels(t *testing.T) {
	want := map[Surface]string{SurfaceUnknown: "unknown", SurfaceAPI: "api", SurfaceExplore: "explore", SurfaceDrilldown: "drilldown", SurfaceDatasource: "datasource", Surface(200): "unknown"}
	for s, label := range want {
		if s.Label() != label {
			t.Errorf("Surface(%d).Label() = %q, want %q", s, s.Label(), label)
		}
	}
}

func TestProfileContext(t *testing.T) {
	if p := From(context.Background()); p != (Profile{}) {
		t.Fatalf("empty context: %+v", p)
	}
	p := Profile{Surface: SurfaceDrilldown, Grafana: ParseVersion("13.3.1")}
	if got := From(With(context.Background(), p)); got != p {
		t.Fatalf("round trip: %+v, want %+v", got, p)
	}
}

var labelPattern = regexp.MustCompile(`^(unknown|[0-9]{1,4}\.[0-9]{1,4}\.[0-9]{1,4})$`)

// FuzzParseVersion: any input yields a label that is "unknown" or major.minor.patch of at most four digits each,
// and a known version prints back to itself.
func FuzzParseVersion(f *testing.F) {
	for _, seed := range []string{"13.3.1", "v12.4.14-security-01", "13.3", "", "9999.9999.9999", "10000.1.1", "1.2.3.4", "\xff", "13.3.1 x"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		v := ParseVersion(s)
		label := v.Label()
		if !labelPattern.MatchString(label) {
			t.Fatalf("ParseVersion(%q).Label() = %q", s, label)
		}
		if v.Known {
			if again := ParseVersion(label); again != v {
				t.Fatalf("%q: label %q reads back as %+v", s, label, again)
			}
		}
	})
}
