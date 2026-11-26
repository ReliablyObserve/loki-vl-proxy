package clientauth

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

func hashOf(t testing.TB, password string, cost int) string {
	t.Helper()
	h, err := bcrypt.GenerateFromPassword([]byte(password), cost)
	if err != nil {
		t.Fatal(err)
	}
	return string(h)
}

func tokenDigest(token string) string {
	d := sha256.Sum256([]byte(token))
	return hex.EncodeToString(d[:])
}

func basicHeader(user, password string) []string {
	return []string{"Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+password))}
}

// testUsers is grafana/secret (basic, tenant team-a), ci (bearer ci-token) and
// both (basic both-pw and bearer both-token).
func testUsers(t testing.TB) []byte {
	t.Helper()
	return fmt.Appendf(nil, `users:
  - name: grafana
    password_hash: %q
    tenant: team-a
  - name: ci
    token_sha256: %q
  - name: both
    password_hash: %q
    token_sha256: %q
`, hashOf(t, "secret", bcrypt.MinCost), tokenDigest("ci-token"), hashOf(t, "both-pw", bcrypt.MinCost), tokenDigest("both-token"))
}

func mustAuth(t testing.TB, data []byte, opts Options) *Authenticator {
	t.Helper()
	users, err := Parse(data, nil)
	if err != nil {
		t.Fatal(err)
	}
	a, err := New(users, opts)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// countingCompare counts bcrypt verifications.
func countingCompare(a *Authenticator) *atomic.Int64 {
	var n atomic.Int64
	inner := a.compare
	a.compare = func(h, p []byte) error { n.Add(1); return inner(h, p) }
	return &n
}

// conformance: client-authentication
func TestParse_AcceptsTheDocumentedShape(t *testing.T) {
	u, err := Parse(testUsers(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if u.Len() != 3 || !u.HasBasic() || !u.HasBearer() || u.Pins() != 1 || u.MixedCosts() || u.LowestCost() != bcrypt.MinCost {
		t.Fatalf("users = %d basic=%v bearer=%v pins=%d mixed=%v lowest=%d", u.Len(), u.HasBasic(), u.HasBearer(), u.Pins(), u.MixedCosts(), u.LowestCost())
	}
}

// conformance: client-authentication
func TestParse_RefusesBrokenFilesWithoutEchoingSecrets(t *testing.T) {
	hash := hashOf(t, "secret", bcrypt.MinCost)
	digest := tokenDigest("tok")
	cases := map[string]string{
		"empty":            "",
		"blank":            "  \n\t\n",
		"not yaml":         "users: [",
		"no users":         "users: []",
		"unknown field":    fmt.Sprintf("users:\n  - name: a\n    password: %q\n", hash),
		"top-level list":   fmt.Sprintf("- name: a\n  password_hash: %q\n", hash),
		"two documents":    fmt.Sprintf("users:\n  - name: a\n    password_hash: %q\n---\nusers: []\n", hash),
		"no name":          fmt.Sprintf("users:\n  - password_hash: %q\n", hash),
		"name with colon":  fmt.Sprintf("users:\n  - name: 'a:b'\n    password_hash: %q\n", hash),
		"name too long":    fmt.Sprintf("users:\n  - name: %s\n    password_hash: %q\n", strings.Repeat("a", MaxNameBytes+1), hash),
		"name with space":  fmt.Sprintf("users:\n  - name: 'a b'\n    password_hash: %q\n", hash),
		"duplicate name":   fmt.Sprintf("users:\n  - name: a\n    password_hash: %q\n  - name: a\n    token_sha256: %q\n", hash, digest),
		"no credential":    "users:\n  - name: a\n",
		"plain password":   "users:\n  - name: a\n    password_hash: hunter2hunter2\n",
		"cost too high":    "users:\n  - name: a\n    password_hash: '$2a$17$" + strings.Repeat("a", 53) + "'\n",
		"short digest":     "users:\n  - name: a\n    token_sha256: abcd\n",
		"non-hex digest":   "users:\n  - name: a\n    token_sha256: " + strings.Repeat("z", 64) + "\n",
		"duplicate token":  fmt.Sprintf("users:\n  - name: a\n    token_sha256: %q\n  - name: b\n    token_sha256: %q\n", digest, digest),
		"multi-tenant pin": fmt.Sprintf("users:\n  - name: a\n    password_hash: %q\n    tenant: 'x|y'\n", hash),
		"too large":        "users:\n" + strings.Repeat("#", MaxFileBytes),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(data), nil)
			if err == nil {
				t.Fatal("parsed")
			}
			msg := err.Error()
			for _, secret := range []string{hash, digest, "hunter2", "$2a$"} {
				if strings.Contains(msg, secret) {
					t.Fatalf("error %q carries %q", msg, secret)
				}
			}
		})
	}
	// The tenant check the proxy passes runs on every pin.
	_, err := Parse(fmt.Appendf(nil, "users:\n  - name: a\n    password_hash: %q\n    tenant: bad\n", hash), func(string) error { return errors.New("refused") })
	if err == nil || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("tenant check not applied: %v", err)
	}
}

// conformance: client-authentication
func TestParseAuthorization(t *testing.T) {
	enc := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	cases := []struct {
		name   string
		values []string
		want   Credential
		reason Reason
	}{
		{"absent", nil, Credential{}, ReasonMissing},
		{"empty", []string{""}, Credential{}, ReasonMissing},
		{"two headers", []string{"Bearer a", "Bearer b"}, Credential{}, ReasonMalformed},
		{"no scheme", []string{"token"}, Credential{}, ReasonMalformed},
		{"unknown scheme", []string{"Digest a=b"}, Credential{}, ReasonMalformed},
		{"basic", []string{"Basic " + enc("u:p")}, Credential{Method: MethodBasic, User: "u", Secret: "p"}, ReasonNone},
		{"basic lower case", []string{"basic " + enc("u:p")}, Credential{Method: MethodBasic, User: "u", Secret: "p"}, ReasonNone},
		{"basic password with colon", []string{"Basic " + enc("u:a:b")}, Credential{Method: MethodBasic, User: "u", Secret: "a:b"}, ReasonNone},
		{"basic empty password", []string{"Basic " + enc("u:")}, Credential{Method: MethodBasic, User: "u", Secret: ""}, ReasonNone},
		{"basic no colon", []string{"Basic " + enc("up")}, Credential{}, ReasonMalformed},
		{"basic no user", []string{"Basic " + enc(":p")}, Credential{}, ReasonMalformed},
		{"basic not base64", []string{"Basic !!!"}, Credential{}, ReasonMalformed},
		{"basic empty", []string{"Basic "}, Credential{}, ReasonMalformed},
		{"bearer", []string{"Bearer abc.def"}, Credential{Method: MethodBearer, Secret: "abc.def"}, ReasonNone},
		{"bearer upper case", []string{"BEARER abc"}, Credential{Method: MethodBearer, Secret: "abc"}, ReasonNone},
		{"bearer extra blanks", []string{"  Bearer   abc  "}, Credential{Method: MethodBearer, Secret: "abc"}, ReasonNone},
		{"bearer with blank", []string{"Bearer a b"}, Credential{}, ReasonMalformed},
		{"bearer control", []string{"Bearer a\x01"}, Credential{}, ReasonMalformed},
		{"bearer empty", []string{"Bearer "}, Credential{}, ReasonMalformed},
		{"too long", []string{"Bearer " + strings.Repeat("a", MaxHeaderBytes)}, Credential{}, ReasonMalformed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, reason := ParseAuthorization(c.values)
			if reason != c.reason || got != c.want {
				t.Fatalf("got %+v %v, want %+v %v", got, reason, c.want, c.reason)
			}
		})
	}
}

// conformance: client-authentication
func TestAuthenticate_BasicAndBearer(t *testing.T) {
	a := mustAuth(t, testUsers(t), Options{CacheTTL: time.Minute})
	ctx := context.Background()
	cases := []struct {
		name   string
		header []string
		want   Identity
		reason Reason
	}{
		{"basic", basicHeader("grafana", "secret"), Identity{Name: "grafana", Tenant: "team-a", Method: MethodBasic}, ReasonNone},
		{"basic wrong password", basicHeader("grafana", "nope"), Identity{}, ReasonInvalid},
		{"basic unknown user", basicHeader("nobody", "secret"), Identity{}, ReasonInvalid},
		{"basic bearer-only user", basicHeader("ci", "ci-token"), Identity{}, ReasonInvalid},
		{"basic other user's password", basicHeader("both", "secret"), Identity{}, ReasonInvalid},
		{"basic too long", basicHeader("grafana", strings.Repeat("x", 73)), Identity{}, ReasonInvalid},
		{"bearer", []string{"Bearer ci-token"}, Identity{Name: "ci", Method: MethodBearer}, ReasonNone},
		{"bearer of a dual user", []string{"Bearer both-token"}, Identity{Name: "both", Method: MethodBearer}, ReasonNone},
		{"basic of a dual user", basicHeader("both", "both-pw"), Identity{Name: "both", Method: MethodBasic}, ReasonNone},
		{"bearer unknown", []string{"Bearer secret"}, Identity{}, ReasonInvalid},
		{"missing", nil, Identity{}, ReasonMissing},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Twice: the second answer comes from the caches.
			for i := 0; i < 2; i++ {
				got, reason := a.Authenticate(ctx, c.header)
				if reason != c.reason || got != c.want {
					t.Fatalf("try %d: got %+v %v, want %+v %v", i, got, reason, c.want, c.reason)
				}
			}
		})
	}
}

// conformance: client-authentication
func TestAuthenticate_CachesSkipBcrypt(t *testing.T) {
	a := mustAuth(t, testUsers(t), Options{CacheTTL: time.Minute})
	n := countingCompare(a)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, r := a.Authenticate(ctx, basicHeader("grafana", "secret")); r != ReasonNone {
			t.Fatal(r)
		}
		if _, r := a.Authenticate(ctx, basicHeader("grafana", "wrong")); r != ReasonInvalid {
			t.Fatal(r)
		}
	}
	if got := n.Load(); got != 2 {
		t.Fatalf("bcrypt ran %d times, want 2 (one per distinct credential)", got)
	}
	// Unknown names are verified too (same cost as a known name), once.
	a.Authenticate(ctx, basicHeader("nobody", "x"))
	a.Authenticate(ctx, basicHeader("nobody", "x"))
	if got := n.Load(); got != 3 {
		t.Fatalf("bcrypt ran %d times, want 3", got)
	}
	// The cache keys hold no plaintext.
	a.ok.mu.Lock()
	for k := range a.ok.entries {
		if strings.Contains(string(k[:]), "secret") {
			t.Fatal("cache key holds the password")
		}
	}
	a.ok.mu.Unlock()

	// Off: every request verifies.
	off := mustAuth(t, testUsers(t), Options{})
	m := countingCompare(off)
	for i := 0; i < 3; i++ {
		off.Authenticate(ctx, basicHeader("grafana", "secret"))
	}
	if m.Load() != 3 {
		t.Fatalf("cache off: bcrypt ran %d times, want 3", m.Load())
	}
}

// conformance: client-authentication
func TestAuthenticate_CacheExpiresAndIsBounded(t *testing.T) {
	a := mustAuth(t, testUsers(t), Options{CacheTTL: time.Minute})
	n := countingCompare(a)
	now := time.Unix(1000, 0)
	a.now = func() time.Time { return now }
	ctx := context.Background()
	a.Authenticate(ctx, basicHeader("grafana", "secret"))
	now = now.Add(59 * time.Second)
	a.Authenticate(ctx, basicHeader("grafana", "secret"))
	if n.Load() != 1 {
		t.Fatalf("bcrypt ran %d times within the TTL", n.Load())
	}
	now = now.Add(2 * time.Second)
	a.Authenticate(ctx, basicHeader("grafana", "secret"))
	if n.Load() != 2 {
		t.Fatalf("bcrypt ran %d times after the TTL, want 2", n.Load())
	}
	// A flood of distinct wrong passwords never grows the failure cache past its bound.
	a.compare = func(_, _ []byte) error { return bcrypt.ErrMismatchedHashAndPassword }
	for i := 0; i < cacheMaxEntries+500; i++ {
		a.Authenticate(ctx, basicHeader("grafana", fmt.Sprint("guess", i)))
	}
	if got := a.fail.len(); got > cacheMaxEntries {
		t.Fatalf("failure cache holds %d entries, bound %d", got, cacheMaxEntries)
	}
}

// conformance: client-authentication
func TestAuthenticate_BusyWhenEverySlotIsTaken(t *testing.T) {
	a := mustAuth(t, testUsers(t), Options{CacheTTL: time.Minute, MaxConcurrentVerifications: 1, VerifyWait: 20 * time.Millisecond})
	ctx := context.Background()
	// A credential verified before the flood.
	if _, r := a.Authenticate(ctx, basicHeader("grafana", "secret")); r != ReasonNone {
		t.Fatal(r)
	}
	release := make(chan struct{})
	started := make(chan struct{})
	inner := a.compare
	a.compare = func(h, p []byte) error {
		close(started)
		<-release
		return inner(h, p)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); a.Authenticate(ctx, basicHeader("grafana", "slow")) }()
	<-started
	if _, r := a.Authenticate(ctx, basicHeader("grafana", "other")); r != ReasonBusy {
		t.Fatalf("got %v with every slot taken, want busy", r)
	}
	// The cached caller is answered without a slot.
	if _, r := a.Authenticate(ctx, basicHeader("grafana", "secret")); r != ReasonNone {
		t.Fatalf("cached credential waited for a slot: %v", r)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	a.wait = time.Hour
	if _, r := a.Authenticate(cctx, basicHeader("grafana", "third")); r != ReasonBusy {
		t.Fatalf("cancelled wait: %v", r)
	}
	close(release)
	wg.Wait()
}

// conformance: client-authentication
func TestPinTenant(t *testing.T) {
	pinned := Identity{Name: "g", Tenant: "team-a"}
	cases := []struct {
		name   string
		id     Identity
		values []string
		set    string
		ok     bool
	}{
		{"no pin, any tenant", Identity{Name: "g"}, []string{"other"}, "", true},
		{"no pin, none", Identity{Name: "g"}, nil, "", true},
		{"pin, none sent", pinned, nil, "team-a", true},
		{"pin, same", pinned, []string{"team-a"}, "team-a", true},
		{"pin, other", pinned, []string{"team-b"}, "", false},
		{"pin, multi-tenant", pinned, []string{"team-a|team-b"}, "", false},
		{"pin, multi-tenant of itself", pinned, []string{"team-a|team-a"}, "", false},
		{"pin, trailing bar", pinned, []string{"team-a|"}, "", false},
		{"pin, blanks", pinned, []string{" team-a"}, "", false},
		{"pin, empty value", pinned, []string{""}, "", false},
		{"pin, two lines", pinned, []string{"team-a", "team-b"}, "", false},
		{"pin, two same lines", pinned, []string{"team-a", "team-a"}, "", false},
		{"pin, case", pinned, []string{"TEAM-A"}, "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			set, ok := PinTenant(c.id, c.values)
			if set != c.set || ok != c.ok {
				t.Fatalf("got %q %v, want %q %v", set, ok, c.set, c.ok)
			}
		})
	}
}

// conformance: client-authentication
func TestWatcher_ReloadsAndKeepsTheLastGoodUsers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "users.yaml")
	first := testUsers(t)
	if err := os.WriteFile(path, first, 0o600); err != nil {
		t.Fatal(err)
	}
	users, err := LoadFile(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	a, err := New(users, Options{CacheTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	var reports []ReloadResult
	w := NewWatcher(a, path, nil, func(r ReloadResult) { reports = append(reports, r) })
	ctx := context.Background()

	w.Check() // unchanged content: nothing reported
	if len(reports) != 0 {
		t.Fatalf("unchanged file reported: %+v", reports)
	}
	if _, r := a.Authenticate(ctx, basicHeader("grafana", "secret")); r != ReasonNone {
		t.Fatal(r)
	}

	// A broken version keeps the last good users, and is reported once.
	for _, broken := range [][]byte{[]byte("users: ["), nil} {
		if err := os.WriteFile(path, broken, 0o600); err != nil {
			t.Fatal(err)
		}
		reports = nil
		w.Check()
		w.Check()
		if len(reports) != 1 || reports[0].Err == nil || reports[0].Users.Len() != 3 {
			t.Fatalf("broken %q: reports %+v", broken, reports)
		}
		if _, r := a.Authenticate(ctx, basicHeader("grafana", "secret")); r != ReasonNone {
			t.Fatalf("last good users lost: %v", r)
		}
	}
	// A deleted file too.
	_ = os.Remove(path)
	reports = nil
	w.Check()
	w.Check()
	if len(reports) != 1 || reports[0].Err == nil {
		t.Fatalf("missing file: reports %+v", reports)
	}

	// A good new version replaces the users; the old password stops working at once.
	second := fmt.Appendf(nil, "users:\n  - name: grafana\n    password_hash: %q\n", hashOf(t, "rotated", bcrypt.MinCost))
	if err := os.WriteFile(path, second, 0o600); err != nil {
		t.Fatal(err)
	}
	reports = nil
	w.Check()
	if len(reports) != 1 || reports[0].Err != nil || reports[0].Users.Len() != 1 {
		t.Fatalf("good reload: reports %+v", reports)
	}
	if _, r := a.Authenticate(ctx, basicHeader("grafana", "secret")); r != ReasonInvalid {
		t.Fatalf("old password after rotation: %v", r)
	}
	if id, r := a.Authenticate(ctx, basicHeader("grafana", "rotated")); r != ReasonNone || id.Tenant != "" {
		t.Fatalf("new password: %+v %v", id, r)
	}
	if _, r := a.Authenticate(ctx, []string{"Bearer ci-token"}); r != ReasonInvalid {
		t.Fatalf("removed token still works: %v", r)
	}

	// Run stops with its context.
	cctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { w.Run(cctx, time.Millisecond); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
	w.Run(ctx, 0) // no interval: returns at once
}

// conformance: client-authentication
func TestChallengeAndReasons(t *testing.T) {
	a := mustAuth(t, testUsers(t), Options{})
	if got := a.Challenge(); len(got) != 2 || !strings.HasPrefix(got[0], "Basic ") || !strings.HasPrefix(got[1], "Bearer ") {
		t.Fatalf("challenge %q", got)
	}
	bearerOnly := mustAuth(t, fmt.Appendf(nil, "users:\n  - name: ci\n    token_sha256: %q\n", tokenDigest("t")), Options{})
	if got := bearerOnly.Challenge(); len(got) != 1 || !strings.HasPrefix(got[0], "Bearer ") {
		t.Fatalf("bearer-only challenge %q", got)
	}
	// A bearer-only file refuses every basic credential without bcrypt.
	n := countingCompare(bearerOnly)
	if _, r := bearerOnly.Authenticate(context.Background(), basicHeader("ci", "t")); r != ReasonInvalid || n.Load() != 0 {
		t.Fatalf("basic against bearer-only users: %v, bcrypt %d", r, n.Load())
	}
	if got := strings.Join(Reasons(), ","); got != "missing,malformed,invalid,tenant_denied,busy" {
		t.Fatalf("reasons %s", got)
	}
	if Reason(200).String() != "unknown" || MethodNone.String() != "" || MethodBasic.String() != "basic_auth" || MethodBearer.String() != "bearer_token" {
		t.Fatal("names")
	}
	if _, ok := IdentityFrom(context.Background()); ok {
		t.Fatal("identity from an empty context")
	}
	id := Identity{Name: "x", Method: MethodBearer}
	if got, ok := IdentityFrom(WithIdentity(context.Background(), id)); !ok || got != id {
		t.Fatal("identity round trip")
	}
	if _, err := LoadFile(filepath.Join(t.TempDir(), "missing.yaml"), nil); err == nil {
		t.Fatal("missing file loaded")
	}
}

// conformance: client-authentication
func TestMixedCostsAreReported(t *testing.T) {
	data := fmt.Appendf(nil, "users:\n  - name: a\n    password_hash: %q\n  - name: b\n    password_hash: %q\n", hashOf(t, "x", bcrypt.MinCost), hashOf(t, "y", bcrypt.MinCost+1))
	u, err := Parse(data, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !u.MixedCosts() || u.LowestCost() != bcrypt.MinCost {
		t.Fatalf("mixed=%v lowest=%d", u.MixedCosts(), u.LowestCost())
	}
	// Unknown names are compared with the dearest hash.
	if c, _ := bcrypt.Cost(u.dummyHash); c != bcrypt.MinCost+1 {
		t.Fatalf("dummy hash cost %d", c)
	}
}
