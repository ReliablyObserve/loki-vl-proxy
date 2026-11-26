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
		"pod identity": {"-peer-self=10.0.0.7:3100"},
		"credentials":  {"-peer-auth-token=new", "-server.admin-auth-token=new"},
	} {
		if got := cacheHandoverScope(handoverScopeFlags(t, args...), env, time.Second, 0); got != base {
			t.Fatalf("%s changed the scope: it decides no answer", name)
		}
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
