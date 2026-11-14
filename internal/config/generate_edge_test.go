package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeFile puts a file under root, creating its directory.
func writeFile(t *testing.T, root, rel, content string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// conformance: operator-configurable-limits
// A flag's default is the value an operator sees: a constant of internal/proxy, constant arithmetic and a duration are
// resolved, and an expression that is none of them is shown as written.
func TestResolveDefault(t *testing.T) {
	consts := map[string]string{
		"DefaultLimit":   "1 << 20 // one MiB",
		"DefaultTimeout": "30*time.Second",
		"DefaultAlias":   "proxy.DefaultLimit",
		"DefaultName":    `"x"`,
	}
	consts["DefaultLimit"] = "1 << 20"
	for expr, want := range map[string]string{
		"proxy.DefaultLimit":       "1048576",
		"proxy.DefaultTimeout":     "30s",
		"proxy.DefaultAlias":       "1048576",
		"proxy.Missing":            "proxy.Missing",
		"proxy.DefaultName":        `"x"`,
		"time.Second":              "1s",
		"2*time.Hour":              "2h0m0s",
		"(1 + 2)*time.Millisecond": "3ms",
		"500*time.Millisecond":     "500ms",
		"10 * 1024 * 1024":         "10485760",
		"1 << 30":                  "1073741824",
		"100 >> 2":                 "25",
		"4096":                     "4096",
		`"text"`:                   `"text"`,
		"false":                    "false",
		"1.5":                      "1.5",
		"1 << x":                   "1 << x",
		"a *":                      "a *",
		"  7  ":                    "7",
		"":                         "",
	} {
		if got := ResolveDefault(expr, consts); got != want {
			t.Errorf("ResolveDefault(%q) = %q, want %q", expr, got, want)
		}
	}
}

// conformance: operator-configurable-limits
// The usage of a flag is its first string literal, with adjacent literals joined and line breaks as spaces.
func TestUsageAfter(t *testing.T) {
	for _, c := range []struct {
		rest string
		want string
		ok   bool
	}{
		{`"plain usage")`, "plain usage", true},
		{`"a" + "b")`, "ab", true},
		{"\"a\" +\n\t\"b\" + \"c\")", "abc", true},
		{`"say \"hi\"\nnow")`, `say "hi" now`, true},
		{`"a" + x)`, "a", true},
		{`no literal`, "", false},
		{`"unterminated`, "", false},
		{`"ends in escape\`, "", false},
	} {
		got, ok := usageAfter(c.rest)
		if got != c.want || ok != c.ok {
			t.Errorf("usageAfter(%q) = %q, %v; want %q, %v", c.rest, got, ok, c.want, c.ok)
		}
	}
}

// conformance: operator-configurable-limits
// ParseFlags reads each declaration of the binary and resolves its default through the constants of internal/proxy.
func TestParseFlags(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "internal/proxy/limits.go", "package proxy\n\nconst (\n\tDefaultCap = 8 << 20 // bytes\n\tDefaultWait = 15*time.Second\n)\n")
	writeFile(t, root, "internal/proxy/limits_test.go", "package proxy\n\nconst DefaultOnlyInTests = 1\n")
	writeFile(t, root, "internal/proxy/notes.txt", "DefaultText = 2\n")
	writeFile(t, root, "internal/proxy/dir.go/placeholder", "x")
	writeFile(t, root, "cmd/proxy/main.go", `package main

func parse(fs *flag.FlagSet) {
	cap := fs.Int64("max-cap", proxy.DefaultCap, "the cap "+
		"in bytes")
	wait := fs.Duration("wait", proxy.DefaultWait, "how long to wait")
	name := fs.String("name", "x", "a name")
	on := fs.Bool("zeta.on", false, "switch")
}
`)
	flags, err := ParseFlags(filepath.Join(root, MainGoPath))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, f := range flags {
		got = append(got, f.Name+"|"+f.Type+"|"+f.Default+"|"+f.Usage)
	}
	want := []string{
		"max-cap|Int64|8388608|the cap in bytes",
		"name|String|\"x\"|a name",
		"wait|Duration|15s|how long to wait",
		"zeta.on|Bool|false|switch",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("flags:\n got  %q\n want %q", got, want)
	}
}

// conformance: operator-configurable-limits
func TestParseFlagsErrors(t *testing.T) {
	if _, err := ParseFlags(filepath.Join(t.TempDir(), MainGoPath)); err == nil {
		t.Error("a missing main.go must fail")
	}
	root := t.TempDir()
	writeFile(t, root, "cmd/proxy/main.go", "package main\n")
	if _, err := ParseFlags(filepath.Join(root, MainGoPath)); err == nil {
		t.Error("a tree without internal/proxy must fail")
	}
	writeFile(t, root, "internal/proxy/x.go", "package proxy\n")
	writeFile(t, root, "cmd/proxy/main.go", "package main\nvar _ = fs.Int(\"broken\", 1, 2)\n")
	if _, err := ParseFlags(filepath.Join(root, MainGoPath)); err == nil || !strings.Contains(err.Error(), `flag "broken": cannot read its usage string`) {
		t.Errorf("a flag without a usage string: %v", err)
	}
	unreadable := t.TempDir()
	writeFile(t, unreadable, "cmd/proxy/main.go", "package main\n")
	writeFile(t, unreadable, "internal/proxy/a.go", "package proxy\n")
	if err := os.Chmod(filepath.Join(unreadable, "internal/proxy/a.go"), 0); err == nil && os.Geteuid() != 0 {
		if _, err := ParseFlags(filepath.Join(unreadable, MainGoPath)); err == nil {
			t.Error("an unreadable proxy source must fail")
		}
	}
}

// conformance: operator-configurable-limits
// The reference documents are generated from the flags the binary declares; a limit whose flag is gone is an error,
// never a silent omission.
func TestGenerateRejectsALimitWithoutItsFlag(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "internal/proxy/x.go", "package proxy\n")
	writeFile(t, root, "cmd/proxy/main.go", "package main\nvar _ = fs.Int(\"only-flag\", 1, \"the only flag\")\n")
	_, err := Generate(root)
	if err == nil || !strings.HasPrefix(err.Error(), "limits registry: flag -") || !strings.HasSuffix(err.Error(), "is not declared by the binary") {
		t.Errorf("Generate over a tree without the limit flags: %v", err)
	}
	for name, render := range map[string]func([]Flag) (string, error){
		"RenderLimitsRegistry": RenderLimitsRegistry,
		"RenderLimitsJSON":     RenderLimitsJSON,
	} {
		if _, err := render([]Flag{{Name: "only-flag", Type: "Int", Default: "1", Usage: "u"}}); err == nil || !strings.Contains(err.Error(), "is not declared by the binary") {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := Generate(t.TempDir()); err == nil {
		t.Error("an empty root must fail")
	}
}

// conformance: operator-configurable-limits
// The helm value that sets a flag, the category of a flag and the limit table index.
func TestFlagLookups(t *testing.T) {
	for name, want := range map[string]string{
		"peer-self":       "peerCache.* (chart-managed)",
		"peer-srv":        "peerCache.* (chart-managed)",
		"disk-cache-path": "persistence.* (chart-managed)",
		"max-concurrent":  "extraArgs.max-concurrent",
	} {
		if got := HelmKeyOf(name); got != want {
			t.Errorf("HelmKeyOf(%q) = %q, want %q", name, got, want)
		}
	}
	for name, want := range map[string]string{"peer-http-url": "peer cache", "cold-backend": "cold storage", "listen": "server", "unheard-of": "other"} {
		if got := CategoryOf(name); got != want {
			t.Errorf("CategoryOf(%q) = %q, want %q", name, got, want)
		}
	}
	byFlag := LimitByFlag()
	if len(byFlag) != len(Limits) {
		t.Errorf("LimitByFlag holds %d of %d limits: a flag is listed twice", len(byFlag), len(Limits))
	}
	for _, l := range Limits {
		if byFlag[l.Flag].Flag != l.Flag {
			t.Errorf("LimitByFlag misses %s", l.Flag)
		}
	}
}
