package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"
)

func TestHoldsFlags(t *testing.T) {
	main := []string{"-listen=:3100", "-label-style=underscores", "-label-values-indexed-cache=true"}
	plain := []string{"-listen=:3100", "-label-style=underscores"}
	explicit := []string{"-label-style=underscores", "-label-values-indexed-cache=false"}
	cases := []struct {
		name          string
		flags, wanted []string
		held          bool
	}{
		{"nothing wanted", main, nil, true},
		{"an exact flag", main, []string{"-label-style=underscores"}, true},
		{"another value", main, []string{"-label-style=passthrough"}, false},
		{"an unlisted flag", plain, []string{"-delete-api-enabled=true"}, false},
		{"off, listed on", main, []string{"-label-values-indexed-cache=false"}, false},
		{"off, not listed (default off)", plain, []string{"-label-values-indexed-cache=false"}, true},
		{"off, listed off", explicit, []string{"-label-values-indexed-cache=false"}, true},
		{"off by 0 and off", plain, []string{"-label-values-indexed-cache=0", "-label-values-indexed-cache=off"}, true},
		{"on, not listed", plain, []string{"-label-values-indexed-cache=true"}, false},
		{"on, bare", plain, []string{"-label-values-indexed-cache"}, false},
		{"off of a flag whose default is not known to be off", plain, []string{"-query-plan-lowering=false"}, false},
		{"off of an unknown flag", plain, []string{"-nope=false"}, false},
		{"off with the profile's other flags", plain, []string{"-label-style=underscores", "-label-values-indexed-cache=false"}, true},
	}
	for _, c := range cases {
		if got := holdsFlags(c.flags, c.wanted); got != c.held {
			t.Errorf("%s: %v", c.name, got)
		}
	}
}

// A case that names the Loki profile without the main proxy's browse-keeping index goes to the underscore variant even
// when other variants hold the same flags and come first.
func TestChooseVariantPrefersTheLokiProfile(t *testing.T) {
	def := []string{"-label-style=underscores", "-metadata-field-mode=translated", "-label-values-indexed-cache=true"}
	profile := []string{"-label-style=underscores", "-metadata-field-mode=translated", "-emit-structured-metadata=true"}
	variants := []Variant{
		{Name: "patterns-autodetect", URL: "http://a", Flags: profile},
		{Name: "native-metadata", URL: "http://n", Flags: []string{"-label-style=underscores", "-metadata-field-mode=native"}},
		{Name: "translated-metadata", URL: "http://t", Flags: profile},
		{Name: "underscore", URL: "http://u", Flags: profile},
		{Name: "vmauth", URL: "http://v", Flags: profile},
	}
	cases := []struct {
		name    string
		flags   []string
		variant string
		blocked bool
	}{
		{"the browse-keeping index off", []string{"-label-values-indexed-cache=false"}, "underscore", false},
		{"the Loki profile", []string{"-label-style=underscores", "-metadata-field-mode=translated", "-label-values-indexed-cache=false"}, "underscore", false},
		{"native metadata", []string{"-metadata-field-mode=native", "-label-values-indexed-cache=false"}, "native-metadata", false},
		{"the default has them", []string{"-label-values-indexed-cache=true"}, "", false},
		{"nothing runs them", []string{"-label-values-indexed-cache=false", "-delete-api-enabled=true"}, "", true},
	}
	for _, c := range cases {
		p := Probe{ProxyFlags: c.flags}
		ChooseVariant(&p, def, variants)
		if p.Variant != c.variant || (p.Blocked != "") != c.blocked {
			t.Errorf("%s: variant %q blocked %q", c.name, p.Variant, p.Blocked)
		}
	}
	// Without the preferred variant the first holder serves.
	p := Probe{ProxyFlags: []string{"-label-values-indexed-cache=false"}}
	ChooseVariant(&p, def, variants[:3])
	if p.Variant != "patterns-autodetect" {
		t.Errorf("first holder: %q", p.Variant)
	}
}

// The repository's stack: a profile case that names the index off runs on the underscore variant, a proxy with no
// index, and the default proxy (the index on) does not hold it.
func TestComposeRoutesTheLokiProfileCases(t *testing.T) {
	variants, def, err := VariantsFromCompose("../e2e-compat/docker-compose.yml", "127.0.0.1", "http://127.0.0.1:13100")
	if err != nil {
		t.Fatal(err)
	}
	if holdsFlags(def, []string{"-label-values-indexed-cache=false"}) {
		t.Fatalf("the main proxy runs the index: %v", def)
	}
	p := Probe{ProxyFlags: []string{"-label-style=underscores", "-label-values-indexed-cache=false"}}
	ChooseVariant(&p, def, variants)
	if p.Variant != "underscore" || p.Blocked != "" {
		t.Errorf("variant %q blocked %q (of %d variants)", p.Variant, p.Blocked, len(variants))
	}
}

// defaultOff names only flags the proxy declares off.
func TestDefaultOffFlagsAreDeclaredOff(t *testing.T) {
	src, err := os.ReadFile("../../cmd/proxy/main.go")
	if err != nil {
		t.Fatal(err)
	}
	for flag := range defaultOff {
		re := regexp.MustCompile(`fs\.Bool\("` + regexp.QuoteMeta(flag[1:]) + `", false,`)
		if !re.Match(src) {
			t.Errorf("%s is not declared as a bool flag defaulting to false in cmd/proxy/main.go", flag)
		}
	}
}

// A live run sends a case that names the index off to the underscore variant (not the first variant that has the flags),
// and its replay, given the same variants, picks the same one.
func TestRunRoutesAProfileCaseToTheUnderscoreVariant(t *testing.T) {
	hits := map[string]int{}
	srv := func(name string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/loki/api/v1/status/buildinfo":
				_, _ = io.WriteString(w, `{"version":"3.7.9"}`)
			case "/loki/api/v1/label/service_name/values":
				hits[name]++
				_, _ = io.WriteString(w, `{"status":"success","data":["a","b"]}`)
			case "/loki/api/v1/query":
				_, _ = io.WriteString(w, `{"data":{"resultType":"vector","result":[{"metric":{},"value":[1,"4"]}]}}`)
			default:
				if r.URL.Path == "/metrics" {
					_, _ = io.WriteString(w, "vm_app_version{short_version=\"v1.53.0\"} 1\n")
					return
				}
				_, _ = io.WriteString(w, `{"c":"4"}`+"\n")
			}
		}))
	}
	loki, def, first, underscore := srv("loki"), srv("default"), srv("patterns-autodetect"), srv("underscore")
	defer loki.Close()
	defer def.Close()
	defer first.Close()
	defer underscore.Close()
	dir := t.TempDir()
	profile := []string{"-label-style=underscores"}
	vf := filepath.Join(dir, "variants.json")
	raw, _ := json.Marshal([]Variant{{Name: "patterns-autodetect", URL: first.URL, Flags: profile}, {Name: "underscore", URL: underscore.URL, Flags: profile}})
	if err := os.WriteFile(vf, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	root := writeRegistry(t, map[string]string{"x/browse": "request: {method: GET, path: /loki/api/v1/label/service_name/values, params: {start: now-1h, end: now, limit: '2'}, " +
		"proxy_flags: ['-label-values-indexed-cache=false']}\ncompare: {mode: exact, fields: [data]}\ngap: {status: fixed, impact: edge, effort: S, area: a, loki: l, proxy: p}\n"})
	live := config{registry: root, loki: loki.URL, proxy: def.URL, vl: loki.URL, variants: vf, proxyFlags: "-label-style=underscores -label-values-indexed-cache=true",
		out: filepath.Join(dir, "live.json"), record: filepath.Join(dir, "a.jsonl"), tenant: "0", settle: minSettle, timeout: 5 * time.Second, noReuse: true,
		now: func() time.Time { return testNow }}
	var out, errs bytes.Buffer
	if code := run(live, &out, &errs); code != 0 {
		t.Fatalf("live: %d %s", code, errs.String())
	}
	if hits["underscore"] == 0 || hits["patterns-autodetect"] != 0 || hits["default"] != 0 {
		t.Errorf("the proxy asks: %v", hits)
	}
	if r := readFile(t, live.out).Cases["x/browse"]; r.Variant != "underscore" {
		t.Errorf("live: %+v", r)
	}
	replay := config{registry: root, replay: live.record, variants: vf, proxyFlags: live.proxyFlags, out: filepath.Join(dir, "replay.json"), tenant: "0",
		settle: minSettle, now: func() time.Time { return testNow.Add(2 * time.Hour) }}
	if code := run(replay, &out, &errs); code != 0 {
		t.Fatalf("replay: %d %s", code, errs.String())
	}
	if r := readFile(t, replay.out).Cases["x/browse"]; r.Variant != "underscore" {
		t.Errorf("replay: %+v", r)
	}
}
