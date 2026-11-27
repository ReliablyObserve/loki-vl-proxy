//go:build helm

package helm

import (
	"bytes"
	"io"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func chartDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate chart")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "charts", "loki-vl-proxy")
}

// renderProxyPod renders the chart with args and returns the proxy pod spec.
func renderProxyPod(t *testing.T, args ...string) (map[string]any, error) {
	t.Helper()
	out, err := exec.Command("helm", append([]string{"template", "rel", chartDir(t)}, args...)...).CombinedOutput()
	if err != nil {
		return nil, &renderError{string(out)}
	}
	dec := yaml.NewDecoder(bytes.NewReader(out))
	for {
		var doc map[string]any
		if err := dec.Decode(&doc); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if doc["kind"] == "Deployment" || doc["kind"] == "StatefulSet" {
			return doc["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any), nil
		}
	}
	t.Fatal("no proxy workload rendered")
	return nil, nil
}

type renderError struct{ out string }

func (e *renderError) Error() string { return e.out }

func asList(v any) []any { l, _ := v.([]any); return l }

// clientAuth.existingSecret mounts the users file read-only from the Secret
// (its directory, so a rotated Secret reaches the pod), sets
// -server.auth-users-file to it, keeps the probes on their open paths, and
// cannot be combined with the plain flag.
//
// conformance: client-authentication
func TestClientAuthMountsTheUsersFile(t *testing.T) {
	pod, err := renderProxyPod(t, "--set", "clientAuth.existingSecret=lvp-users", "--set", "clientAuth.existingSecretKey=auth.yaml")
	if err != nil {
		t.Fatal(err)
	}
	container := asList(pod["containers"])[0].(map[string]any)
	var flag string
	for _, a := range asList(container["args"]) {
		if s := a.(string); strings.HasPrefix(s, "-server.auth-users-file=") {
			flag = s
		}
	}
	if flag != "-server.auth-users-file=/etc/loki-vl-proxy/auth/auth.yaml" {
		t.Fatalf("flag %q", flag)
	}
	var mounted bool
	for _, m := range asList(container["volumeMounts"]) {
		mm := m.(map[string]any)
		if mm["name"] != "client-auth-users" {
			continue
		}
		mounted = true
		if mm["mountPath"] != "/etc/loki-vl-proxy/auth" || mm["readOnly"] != true || mm["subPath"] != nil {
			t.Fatalf("mount %#v: want the directory, read-only, without subPath", mm)
		}
	}
	if !mounted {
		t.Fatal("users file not mounted")
	}
	var volume map[string]any
	for _, v := range asList(pod["volumes"]) {
		if vv := v.(map[string]any); vv["name"] == "client-auth-users" {
			volume = vv["secret"].(map[string]any)
		}
	}
	if volume == nil || volume["secretName"] != "lvp-users" {
		t.Fatalf("volume %#v", volume)
	}
	items := asList(volume["items"])
	if len(items) != 1 || items[0].(map[string]any)["key"] != "auth.yaml" || items[0].(map[string]any)["path"] != "auth.yaml" {
		t.Fatalf("items %#v", items)
	}
	// The probes keep their paths, which caller authentication leaves open.
	for _, probe := range []string{"livenessProbe", "readinessProbe"} {
		path := container[probe].(map[string]any)["httpGet"].(map[string]any)["path"]
		if path != "/alive" && path != "/ready" && path != "/health" {
			t.Fatalf("%s path %v is not an open probe path", probe, path)
		}
	}
	// No credential in the pod spec.
	for _, e := range asList(container["env"]) {
		if strings.Contains(strings.ToUpper(e.(map[string]any)["name"].(string)), "AUTH_USERS") {
			t.Fatalf("users file passed through env: %#v", e)
		}
	}

	if _, err := renderProxyPod(t, "--set", "clientAuth.existingSecret=s", "--set", `extraArgs.server\.auth-users-file=/x`); err == nil || !strings.Contains(err.Error(), "clientAuth.existingSecret cannot be combined") {
		t.Fatalf("both set: %v", err)
	}
	// Off by default: nothing mounted, no flag.
	off, err := renderProxyPod(t)
	if err != nil {
		t.Fatal(err)
	}
	c := asList(off["containers"])[0].(map[string]any)
	for _, a := range asList(c["args"]) {
		if strings.HasPrefix(a.(string), "-server.auth-users-file") {
			t.Fatal("flag rendered with clientAuth unset")
		}
	}
	for _, v := range asList(off["volumes"]) {
		if v.(map[string]any)["name"] == "client-auth-users" {
			t.Fatal("volume rendered with clientAuth unset")
		}
	}
}
