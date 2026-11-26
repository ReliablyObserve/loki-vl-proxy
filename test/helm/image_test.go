//go:build helm

package helm

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var imageDigest = "sha256:" + strings.Repeat("0123456789abcdef", 4)

func proxyImage(t *testing.T, args ...string) string {
	t.Helper()
	match := regexp.MustCompile(`(?m)^ +image: "([^"]*)"$`).FindAllStringSubmatch(renderChart(t, args...), -1)
	if len(match) != 1 {
		t.Fatalf("want one proxy image, got %q", match)
	}
	return match[0][1]
}

// conformance: release-image-pinned-by-digest
func TestProxyImageReference(t *testing.T) {
	appVersion := regexp.MustCompile(`app\.kubernetes\.io/version: "([^"]+)"`).FindStringSubmatch(renderChart(t))[1]
	const repo = "ghcr.io/reliablyobserve/loki-vl-proxy"
	for _, tc := range []struct {
		name, want string
		args       []string
	}{
		{"default", repo + ":" + appVersion, nil},
		{"tag", repo + ":1.2.3", []string{"--set", "image.tag=1.2.3"}},
		{"empty digest", repo + ":" + appVersion, []string{"--set", "image.digest="}},
		// "helm upgrade --reuse-values" from a chart without the key leaves it unset.
		{"digest unset", repo + ":" + appVersion, []string{"--set", "image.digest=null"}},
		{"digest", repo + "@" + imageDigest, []string{"--set", "image.digest=" + imageDigest}},
		{"tag and digest", repo + ":1.2.3@" + imageDigest, []string{"--set", "image.tag=1.2.3", "--set", "image.digest=" + imageDigest}},
		{"mirror", "registry.example.com:5000/mirror/loki-vl-proxy@" + imageDigest, []string{"--set", "image.repository=registry.example.com:5000/mirror/loki-vl-proxy", "--set", "image.digest=" + imageDigest}},
		{"digest written into the tag", repo + ":1.2.3@" + imageDigest, []string{"--set", "image.tag=1.2.3@" + imageDigest}},
	} {
		if got := proxyImage(t, tc.args...); got != tc.want {
			t.Errorf("%s: image %q, want %q", tc.name, got, tc.want)
		}
	}
}

// conformance: release-image-pinned-by-digest
func TestProxyImageDigestRejected(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		args       []string
	}{
		{"no algorithm", "is not sha256", []string{"--set", "image.digest=" + strings.TrimPrefix(imageDigest, "sha256:")}},
		{"short", "is not sha256", []string{"--set", "image.digest=sha256:abc"}},
		{"upper case", "is not sha256", []string{"--set", "image.digest=sha256:" + strings.Repeat("A", 64)}},
		{"trailing text", "is not sha256", []string{"--set", "image.digest=" + imageDigest + "x"}},
		{"with a reference", "is not sha256", []string{"--set", "image.digest=repo@" + imageDigest}},
		{"digest in the tag too", "must not hold a digest", []string{"--set", "image.digest=" + imageDigest, "--set", "image.tag=1.2.3@" + imageDigest}},
		{"digest in the repository too", "must not hold a digest", []string{"--set", "image.digest=" + imageDigest, "--set", "image.repository=r/x@" + imageDigest}},
	} {
		args := append([]string{"template", "scrape", "../../charts/loki-vl-proxy"}, tc.args...)
		if out, err := exec.Command("helm", args...).CombinedOutput(); err == nil || !strings.Contains(string(out), tc.want) {
			t.Errorf("%s: error %v, want %q in %s", tc.name, err, tc.want, out)
		}
	}
}

// Every image the chart's own templates name is built by one helper.
//
// conformance: release-image-pinned-by-digest
func TestEveryImageUsesTheHelper(t *testing.T) {
	var templates []string
	for _, pattern := range []string{"*.yaml", "tests/*.yaml"} {
		found, err := filepath.Glob("../../charts/loki-vl-proxy/templates/" + pattern)
		if err != nil {
			t.Fatal(err)
		}
		templates = append(templates, found...)
	}
	images := 0
	for _, path := range templates {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		testImage := strings.Contains(string(data), `$testImage := include "loki-vl-proxy.image" `)
		for _, line := range strings.Split(string(data), "\n") {
			value, found := strings.CutPrefix(strings.TrimLeft(line, " -"), "image: ")
			if !found {
				continue
			}
			images++
			if value != `"{{ include "loki-vl-proxy.proxyImage" . }}"` && (!testImage || value != "{{ $testImage }}") {
				t.Errorf("%s: image %q does not come from the image helper", path, value)
			}
		}
	}
	if images != 4 {
		t.Fatalf("found %d image lines, want the proxy and three test containers", images)
	}
}
