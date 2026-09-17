package proxy

import "testing"

// `__tenant_id__` is a Loki LABEL matcher, so its regexp is anchored like every
// other one: Prometheus compiles a matcher as `^(?:v)$`. Unanchored, it decides
// which tenants a fan-out queries, so `=~"prod"` reached `prod-eu` and
// `nonprod` as well.
func TestTenantMatcherRegexpIsAnchored(t *testing.T) {
	tenants := []string{"prod", "prod-eu", "nonprod", "staging"}
	for _, tc := range []struct {
		op   string
		raw  string
		want []string
	}{
		{"=~", "prod", []string{"prod"}},
		{"=~", "prod.*", []string{"prod", "prod-eu"}},
		{"!~", "prod", []string{"prod-eu", "nonprod", "staging"}},
		{"=~", "prod|staging", []string{"prod", "staging"}},
	} {
		got, matched, err := applyTenantMatch(tenants, tc.op, tc.raw)
		if err != nil || !matched {
			t.Fatalf("%s %q: matched=%v err=%v", tc.op, tc.raw, matched, err)
		}
		if len(got) != len(tc.want) {
			t.Fatalf("%s %q = %v, want %v", tc.op, tc.raw, got, tc.want)
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Fatalf("%s %q = %v, want %v", tc.op, tc.raw, got, tc.want)
			}
		}
	}
}
