package main

import (
	"flag"
	"strings"
	"testing"
	"time"
)

func handoverScopeFlags(t *testing.T, args ...string) *flag.FlagSet {
	t.Helper()
	fs := flag.NewFlagSet("scope", flag.ContinueOnError)
	fs.String("max-stats-query-series", "1000", "")
	fs.String("peer-self", "", "")
	fs.String("peer-self-az", "", "")
	fs.String("backend-basic-auth", "", "")
	fs.String("backend-version-basic-auth", "", "")
	fs.String("cache-key-secret", "", "")
	fs.String("cold-backend", "", "")
	fs.String("otlp-headers", "", "")
	fs.String("peer-auth-token", "", "")
	fs.String("server.admin-auth-token", "", "")
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	return fs
}

// conformance: cache-restart-handover
func TestCacheHandoverScope(t *testing.T) {
	env := envConfig{backendURL: "http://vl:9428"}
	base := cacheHandoverScope(handoverScopeFlags(t), env, time.Second, 0)
	if base == "" || !strings.HasPrefix(base, version+"|"+revision+"|") {
		t.Fatalf("scope %q: want the build identity first", base)
	}
	if again := cacheHandoverScope(handoverScopeFlags(t), env, time.Second, 0); again != base {
		t.Fatal("the same build and configuration must give the same scope")
	}
	for name, args := range map[string][]string{
		"pod identity": {"-peer-self=10.0.0.7:3100", "-peer-self-az=zone-b"},
		"credentials":  {"-peer-auth-token=new", "-server.admin-auth-token=new", "-backend-basic-auth=user:pw", "-backend-version-basic-auth=ver:pw", "-cache-key-secret=new", "-otlp-headers=Authorization=Bearer x"},
	} {
		if got := cacheHandoverScope(handoverScopeFlags(t, args...), env, time.Second, 0); got != base {
			t.Fatalf("%s changed the scope: it decides no answer", name)
		}
	}
	if got := cacheHandoverScope(handoverScopeFlags(t), envConfig{backendURL: "http://vlreader:correct-horse@vl:9428"}, time.Second, 0); got != base {
		t.Fatal("credentials in the backend URL changed the scope: they decide no answer and are never hashed onto the disk")
	}
	if got := cacheHandoverScope(handoverScopeFlags(t, "-cold-backend=http://u:pw@cold:9428"), env, time.Second, 0); got != cacheHandoverScope(handoverScopeFlags(t, "-cold-backend=http://cold:9428"), env, time.Second, 0) {
		t.Fatal("credentials in a URL flag changed the scope")
	}
	if got := cacheHandoverScope(handoverScopeFlags(t, "-max-stats-query-series=500"), env, time.Second, 0); got == base {
		t.Fatal("a flag that decides answers must change the scope")
	}
	if got := cacheHandoverScope(handoverScopeFlags(t), envConfig{backendURL: "http://other:9428"}, 0, 1); got == base {
		t.Fatal("an environment override must change the scope")
	}
	if got := cacheHandoverScope(handoverScopeFlags(t), env, 0, 0); got != "" {
		t.Fatal("with the hand-over off no scope is computed")
	}
}

// conformance: cache-restart-handover
func TestCacheHandoverFlagsAndLazyScope(t *testing.T) {
	if err := validateCacheHandoverFlags(0, 0, time.Second); err != nil {
		t.Fatalf("defaults refused: %v", err)
	}
	for _, bad := range [][3]int64{{-1, 0, 1}, {0, -1, 1}, {0, 0, 0}} {
		if err := validateCacheHandoverFlags(time.Duration(bad[0]), bad[1], time.Duration(bad[2])); err == nil {
			t.Fatalf("%v accepted", bad)
		}
	}
	env := envConfig{backendURL: "http://vl:9428"}
	lazy := lazyCacheHandoverScope(handoverScopeFlags(t), env, time.Second, 0)
	if got, want := lazy(), cacheHandoverScope(handoverScopeFlags(t), env, time.Second, 0); got != want || got == "" {
		t.Fatalf("lazy scope %q, want %q", got, want)
	}
	digest, err := executableDigest()
	if err != nil || len(digest) != 32 {
		t.Fatalf("executable digest %q %v", digest, err)
	}
}
