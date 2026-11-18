package clientprofile

import (
	"context"
	"testing"
)

func TestParseVersion(t *testing.T) {
	for in, want := range map[string]string{
		"13.3.1":                "13.3.1",
		"13.3.1 (abc123)":       "13.3.1",
		"3.2.0-rc1":             "3.2.0",
		"0.0.0":                 "0.0.0",
		"9999.9999.9999":        "9999.9999.9999",
		"13.3":                  "unknown",
		"13.3.":                 "unknown",
		"a.b.c":                 "unknown",
		"":                      "unknown",
		"10000.1.1":             "unknown",
		"1.2.99999":             "unknown",
		"1.2.3.4":               "1.2.3",
		"1..3":                  "unknown",
		" 1.2.3":                "unknown",
		"99999999999999999.1.1": "unknown",
	} {
		if got := ParseVersion(in).Label(); got != want {
			t.Errorf("ParseVersion(%q) = %s, want %s", in, got, want)
		}
	}
}

func TestSurfaceLabelsAreClosed(t *testing.T) {
	for s, want := range map[Surface]string{Unknown: "unknown", API: "api", Explore: "explore", Drilldown: "drilldown", Datasource: "datasource", Surface(200): "unknown"} {
		if got := s.Label(); got != want {
			t.Errorf("Surface(%d) = %s, want %s", s, got, want)
		}
	}
}

func TestWithFrom(t *testing.T) {
	if p := From(context.Background()); p != (Profile{}) {
		t.Fatalf("no profile attached: %+v", p)
	}
	want := Profile{Surface: Drilldown, Grafana: ParseVersion("13.3.1"), Datasource: ParseVersion("13.2.5")}
	if got := From(With(context.Background(), want)); got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}
