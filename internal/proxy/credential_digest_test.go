package proxy

import (
	"context"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func credentialTestProxy(t testing.TB, mutate func(*Config)) *Proxy {
	t.Helper()
	cfg := Config{
		BackendURL:     "http://vl.example:9428",
		LogLevel:       "error",
		Cache:          cache.New(30*time.Second, 100),
		ForwardHeaders: []string{"Authorization"},
		ForwardCookies: []string{"session"},
	}
	if mutate != nil {
		mutate(&cfg)
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func credentialRequest(org, authorization, cookie string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil)
	if org != "" {
		r.Header.Set("X-Scope-OrgID", org)
	}
	if authorization != "" {
		r.Header.Set("Authorization", authorization)
	}
	if cookie != "" {
		r.Header.Set("Cookie", cookie)
	}
	return withOrgID(r)
}

// unkeyedFingerprintDigest is what anyone holding a stored key could recompute
// for a guessed credential if the fingerprint were a plain SHA-256 of its
// inputs (the construction before keyed digests).
func unkeyedFingerprintDigest(p *Proxy, r *http.Request) string {
	scope := p.scopeFingerprint(r.Context(), r.Header.Get("X-Scope-Orgid"))
	cookies := make([][2]string, 0)
	for _, c := range r.Cookies() {
		if p.forwardCookies["*"] || p.forwardCookies[c.Name] {
			cookies = append(cookies, [2]string{c.Name, c.Value})
		}
	}
	data, _ := json.Marshal([]any{scope, p.forwardedIdentityHeaders(r), cookies})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// A stored key must not confirm a guessed credential: recomputing the
// fingerprint from the request's inputs without the digest key finds nothing.
//
// conformance: cache-key-credential-digest
func TestCredentialFingerprintCannotBeRecomputedWithoutKey(t *testing.T) {
	p := credentialTestProxy(t, nil)
	for _, r := range []*http.Request{
		credentialRequest("team-a", "Basic YWxpY2U6Y29ycmVjdC1ob3JzZQ==", ""),
		credentialRequest("team-a", "", "session=s3ss10n"),
		credentialRequest("", "Bearer eyJhbGciOi.payload.sig", "session=s3ss10n"),
	} {
		fp := p.forwardedAuthFingerprint(r)
		if !strings.HasPrefix(fp, identityFingerprintPrefix) {
			t.Fatalf("credential fingerprint %q lost the %q marker peerPushAllowed reads", fp, identityFingerprintPrefix)
		}
		if guess := unkeyedFingerprintDigest(p, r); strings.Contains(fp, guess) {
			t.Fatalf("fingerprint %q is the unkeyed SHA-256 of its inputs: a stored key confirms a guessed credential", fp)
		}
		for _, key := range []string{
			p.responseKey("labels", r, r.URL.RawQuery),
			p.canonicalReadCacheKey("labels", getOrgID(r.Context()), r),
		} {
			if strings.Contains(key, unkeyedFingerprintDigest(p, r)) {
				t.Fatalf("cache key %q holds the unkeyed digest of the credentials", key)
			}
		}
	}
}

// The backend credentials (-backend-basic-auth) decide the routing namespace
// in every cache key; a stored key must not confirm a guessed password either.
//
// conformance: cache-key-credential-digest
func TestBackendCredentialsCannotBeRecomputedFromNamespace(t *testing.T) {
	p := credentialTestProxy(t, func(c *Config) {
		c.ForwardHeaders, c.ForwardCookies = nil, nil
		c.BackendBasicAuth = "vlreader:correct-horse"
	})
	var style LabelStyle
	var fields map[string]string
	var otel bool
	if p.labelTranslator != nil {
		style, fields, otel = p.labelTranslator.style, p.labelTranslator.vlToLoki, p.labelTranslator.translateOTel
	}
	// Everything but the password is known to whoever holds the keys.
	guess := func(password string) string {
		headers := map[string]string{"Authorization": "Basic " + base64Encode("vlreader:"+password)}
		data, _ := json.Marshal([]any{"scope-v3", p.backend.String(), "", headers, p.tenantMap, p.tenantLabel, style, fields, otel, p.forwardTenantHeader})
		sum := sha256.Sum256(data)
		return hex.EncodeToString(sum[:])
	}
	if got := p.routingNamespace; got == guess("correct-horse") {
		t.Fatal("the routing namespace is the unkeyed SHA-256 of the backend credentials: a stored key confirms a guessed password")
	}
	for _, org := range []string{"", "team-a"} {
		scope := p.scopeFingerprint(context.Background(), org)
		if want := guess("correct-horse"); scope == want || scope == hex.EncodeToString(sha256Of(want+org)) {
			t.Fatalf("org %q: scope fingerprint recomputable from the backend password", org)
		}
	}
	// Another backend credential still gives another namespace.
	other := credentialTestProxy(t, func(c *Config) {
		c.ForwardHeaders, c.ForwardCookies = nil, nil
		c.BackendBasicAuth = "vlreader:other"
	})
	if other.routingNamespace == p.routingNamespace {
		t.Fatal("two backend credentials share one routing namespace")
	}
}

func sha256Of(s string) []byte {
	sum := sha256.Sum256([]byte(s))
	return sum[:]
}

// Where the digest key comes from decides which replicas and restarts share
// credential-scoped entries; a different or missing key is always a miss.
//
// conformance: cache-key-credential-digest
func TestCredentialDigestKeySources(t *testing.T) {
	r := func() *http.Request { return credentialRequest("team-a", "Basic YWxpY2U6cHc=", "session=1") }
	fp := func(mutate func(*Config)) string {
		return credentialTestProxy(t, mutate).forwardedAuthFingerprint(r())
	}
	withSecret := func(s string) func(*Config) { return func(c *Config) { c.CacheKeySecret = s } }
	withPeerToken := func(s string) func(*Config) { return func(c *Config) { c.PeerAuthToken = s } }

	if a, b := fp(withSecret("fleet-1")), fp(withSecret("fleet-1")); a != b {
		t.Error("one -cache-key-secret: replicas and restarts must agree")
	}
	if fp(withSecret("fleet-1")) == fp(withSecret("fleet-2")) {
		t.Error("two -cache-key-secret values must give two keys (a miss)")
	}
	if a, b := fp(withPeerToken("peer-1")), fp(withPeerToken("peer-1")); a != b {
		t.Error("one -peer-auth-token: replicas and restarts must agree")
	}
	if fp(withPeerToken("peer-1")) == fp(withPeerToken("peer-2")) {
		t.Error("two peer tokens must give two keys (a miss)")
	}
	both := func(c *Config) { c.CacheKeySecret, c.PeerAuthToken = "fleet-1", "peer-1" }
	if fp(both) != fp(withSecret("fleet-1")) || fp(both) == fp(withPeerToken("peer-1")) {
		t.Error("-cache-key-secret must take precedence over -peer-auth-token")
	}
	if a, b := fp(nil), fp(nil); a == b {
		t.Error("without a secret every process must use its own key")
	}
	if fp(nil) == fp(withSecret("fleet-1")) || fp(nil) == fp(withPeerToken("peer-1")) {
		t.Error("a process without a key must miss a keyed replica's entries")
	}
	// The derived key is not the peer token used as is: knowing how the
	// digest is built and holding a key does not need, nor reveal, the token.
	in := appendField(appendField([]byte{credentialDomainHeader}, "A"), "B")
	got := newCredentialDigest("", "peer-1").headerValues(map[string]string{"A": "B"})["A"]
	if got == credentialDigestVersion+hmacHex([]byte("peer-1"), in) {
		t.Error("the peer token must not be the HMAC key itself")
	}
	derived, err := hkdf.Key(sha256.New, []byte("peer-1"), nil, credentialDigestInfo, sha256.Size)
	if err != nil {
		t.Fatal(err)
	}
	if got != credentialDigestVersion+hmacHex(derived, in) {
		t.Error("the digest key must be HKDF-SHA256 of the secret with credentialDigestInfo")
	}
}

func hmacHex(key, data []byte) string {
	m := hmac.New(sha256.New, key)
	_, _ = m.Write(data)
	return hex.EncodeToString(m.Sum(nil))
}

// Entries of a build that wrote unkeyed fingerprints are never read after the
// upgrade: not for the caller that wrote them, not for anyone else.
//
// conformance: cache-key-credential-digest
func TestUnkeyedFingerprintEntriesAreNeverServed(t *testing.T) {
	p := credentialTestProxy(t, func(c *Config) { c.CacheKeySecret = "fleet-1" })
	alice := credentialRequest("team-a", "Basic YWxpY2U6cHc=", "")
	bob := credentialRequest("team-a", "Basic Ym9iOnB3", "")
	oldAlice := identityFingerprintPrefix + unkeyedFingerprintDigest(p, alice)
	newAlice := p.forwardedAuthFingerprint(alice)
	keyAlice := p.responseKey("labels", alice, alice.URL.RawQuery)
	// The key an earlier build wrote for alice's answer.
	oldKey := strings.Replace(keyAlice, newAlice, oldAlice, 1)
	if oldKey == keyAlice {
		t.Fatal("old and new key are equal")
	}
	p.cache.Set(oldKey, []byte("alice's answer"))
	for name, r := range map[string]*http.Request{"alice": alice, "bob": bob} {
		key := p.responseKey("labels", r, r.URL.RawQuery)
		if _, ok := p.cache.Get(key); ok {
			t.Fatalf("%s read an entry written under the unkeyed fingerprint", name)
		}
	}
	// Structurally disjoint: an unkeyed fingerprint is the prefix and 64 hex
	// digits; a keyed one has the version (with a '.') after the prefix.
	tail := strings.TrimPrefix(newAlice, identityFingerprintPrefix)
	if !strings.HasPrefix(tail, credentialDigestVersion) || strings.Trim(tail, "0123456789abcdef") == "" {
		t.Fatalf("keyed fingerprint %q could equal an unkeyed one", newAlice)
	}
}

// Credential-scoped entries are still kept off the peers' push path.
//
// conformance: cache-key-credential-digest
func TestPeerPushRefusesKeyedCredentialEntries(t *testing.T) {
	p := credentialTestProxy(t, func(c *Config) { c.CacheKeySecret = "fleet-1" })
	r := credentialRequest("team-a", "Basic YWxpY2U6cHc=", "session=1")
	for _, key := range []string{
		p.responseKey("labels", r, r.URL.RawQuery),
		p.canonicalReadCacheKey("labels", "team-a", r),
		p.detectedFieldsCacheKey(context.WithValue(r.Context(), origRequestKey, r), `{app="a"}`, "1", "2", 10),
	} {
		if peerPushAllowed(key) {
			t.Fatalf("credential-scoped key %q may be pushed to peers", key)
		}
	}
	if !peerPushAllowed(p.responseKey("labels", credentialRequest("team-a", "", ""), "")) {
		t.Fatal("a key without credentials must stay pushable")
	}
}

// Reuse of credential-scoped keys by a second process (a restart reading the
// disk tier, another replica asking its peers), measured over many callers:
// the disk and peer tiers are addressed by the key text, so a reproduced key
// is a hit and any other key is a miss. Keys without credentials are always
// reproduced.
//
// conformance: cache-key-credential-digest
func TestCredentialScopedKeyReuseAcrossProcesses(t *testing.T) {
	const callers = 200
	reused := func(first, second func(*Config), credentials bool) int {
		a, b := credentialTestProxy(t, first), credentialTestProxy(t, second)
		n := 0
		for i := 0; i < callers; i++ {
			auth := ""
			if credentials {
				auth = fmt.Sprintf("Basic user-%d", i)
			}
			r := credentialRequest(fmt.Sprintf("org-%d", i%7), auth, "")
			if a.responseKey("labels", r, "") == b.responseKey("labels", r, "") {
				n++
			}
		}
		return n
	}
	secret := func(c *Config) { c.CacheKeySecret = "fleet-1" }
	peer := func(c *Config) { c.PeerAuthToken = "peer-1" }
	for _, tc := range []struct {
		name          string
		first, second func(*Config)
		credentials   bool
		want          int
	}{
		{"same -cache-key-secret", secret, secret, true, callers},
		{"same -peer-auth-token", peer, peer, true, callers},
		{"no key (per process)", nil, nil, true, 0},
		{"different keys", secret, peer, true, 0},
		{"no credentials, no key", nil, nil, false, callers},
		{"no credentials, different keys", secret, peer, false, callers},
	} {
		if got := reused(tc.first, tc.second, tc.credentials); got != tc.want {
			t.Errorf("%s: %d of %d keys reproduced, want %d", tc.name, got, callers, tc.want)
		} else {
			t.Logf("%s: %d/%d keys reproduced", tc.name, got, callers)
		}
	}
}

// Distinct credentials never share a cache key: two requests get one
// fingerprint exactly when their tenant scope, forwarded headers and
// forwarded cookies are the same bytes (invalid UTF-8 included), and a
// credential fingerprint is always keyed.
//
// conformance: cache-key-credential-digest
func FuzzCredentialFingerprintsNeverCollide(f *testing.F) {
	f.Add("team-a", "Basic YTpi", "session=1", "team-a", "Basic YTpj", "session=1")
	f.Add("team-a", "", "session=1", "team-b", "", "session=1")
	f.Add("", `a","b`, "", "", `a"`, `session=","b`)
	f.Add("a", "b", "session=c; session=d", "a", "b", "session=d; session=c")
	f.Add("x", "", "", "x", "", "")
	// Two different invalid UTF-8 header values: JSON spells both U+FFFD.
	f.Add("team-a", "Basic \xff", "", "team-a", "Basic \xfe", "")
	p := credentialTestProxy(f, func(c *Config) {
		c.CacheKeySecret = "fuzz"
		c.ForwardHeaders = []string{"Authorization", "X-Id-Token"}
		c.ForwardCookies = []string{"session", "sid"}
	})
	inputs := func(org, auth, cookie string) (*http.Request, string, bool) {
		r := credentialRequest(org, auth, cookie)
		if auth != "" {
			r.Header.Set("X-Id-Token", auth+"#id")
		}
		scope := p.scopeFingerprint(r.Context(), r.Header.Get("X-Scope-Orgid"))
		cookies := make([][2]string, 0)
		for _, c := range r.Cookies() {
			if p.forwardCookies[c.Name] {
				cookies = append(cookies, [2]string{c.Name, c.Value})
			}
		}
		// The oracle compares raw bytes (strconv.Quote is injective, also on
		// invalid UTF-8, where JSON writes U+FFFD for any bad byte).
		var in strings.Builder
		in.WriteString(strconv.Quote(scope))
		identity := p.forwardedIdentityHeaders(r)
		names := make([]string, 0, len(identity))
		for name := range identity {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			in.WriteString(" h" + strconv.Quote(name))
			for _, v := range identity[name] {
				in.WriteString("=" + strconv.Quote(v))
			}
		}
		for _, c := range cookies {
			in.WriteString(" c" + strconv.Quote(c[0]) + "=" + strconv.Quote(c[1]))
		}
		return r, in.String(), len(cookies) > 0 || p.forwardsCredentialHeader(r)
	}
	f.Fuzz(func(t *testing.T, org1, auth1, cookie1, org2, auth2, cookie2 string) {
		r1, in1, cred1 := inputs(org1, auth1, cookie1)
		r2, in2, cred2 := inputs(org2, auth2, cookie2)
		fp1, fp2 := p.forwardedAuthFingerprint(r1), p.forwardedAuthFingerprint(r2)
		if (in1 == in2) != (fp1 == fp2) {
			t.Fatalf("inputs equal=%v but fingerprints equal=%v\n%s -> %s\n%s -> %s", in1 == in2, fp1 == fp2, in1, fp1, in2, fp2)
		}
		for _, c := range []struct {
			r    *http.Request
			fp   string
			cred bool
		}{{r1, fp1, cred1}, {r2, fp2, cred2}} {
			if !c.cred {
				continue
			}
			if !strings.HasPrefix(c.fp, identityFingerprintPrefix+credentialDigestVersion) {
				t.Fatalf("credential fingerprint %q is not keyed", c.fp)
			}
			if strings.Contains(c.fp, unkeyedFingerprintDigest(p, c.r)) {
				t.Fatalf("credential fingerprint %q is the unkeyed digest", c.fp)
			}
		}
	})
}

// The pooled hashers are shared by concurrent requests: each gets its own
// answer.
//
// conformance: cache-key-credential-digest
func TestCredentialDigestConcurrentRequests(t *testing.T) {
	p := credentialTestProxy(t, func(c *Config) { c.CacheKeySecret = "fleet-1" })
	const n = 64
	want := make([]string, n)
	for i := range want {
		want[i] = p.forwardedAuthFingerprint(credentialRequest("team-a", fmt.Sprintf("Bearer %d", i), fmt.Sprintf("session=%d", i)))
	}
	errs := make(chan string, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			for j := 0; j < 50; j++ {
				if got := p.forwardedAuthFingerprint(credentialRequest("team-a", fmt.Sprintf("Bearer %d", i), fmt.Sprintf("session=%d", i))); got != want[i] {
					errs <- fmt.Sprintf("caller %d: %q, want %q", i, got, want[i])
					return
				}
			}
			errs <- ""
		}(i)
	}
	for i := 0; i < n; i++ {
		if e := <-errs; e != "" {
			t.Fatal(e)
		}
	}
}

// Header values that are not valid UTF-8 stay apart: two credentials that
// differ only in such bytes never share a fingerprint (JSON would spell both
// as U+FFFD).
//
// conformance: cache-key-credential-digest
func TestCredentialFingerprintKeepsInvalidUTF8Apart(t *testing.T) {
	p := credentialTestProxy(t, func(c *Config) { c.CacheKeySecret = "fleet-1" })
	a := p.forwardedAuthFingerprint(credentialRequest("team-a", "Basic \xff", ""))
	b := p.forwardedAuthFingerprint(credentialRequest("team-a", "Basic \xfe", ""))
	if a == b {
		t.Fatalf("two credentials differing in invalid UTF-8 bytes share fingerprint %q", a)
	}
	if again := p.forwardedAuthFingerprint(credentialRequest("team-a", "Basic \xff", "")); again != a {
		t.Fatalf("the same bytes gave %q and %q", a, again)
	}
}
