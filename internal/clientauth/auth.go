package clientauth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// Reason is why a request was refused. The set is closed: it labels
// loki_vl_proxy_auth_failures_total.
type Reason uint8

const (
	// ReasonNone: authenticated.
	ReasonNone Reason = iota
	// ReasonMissing: no Authorization header.
	ReasonMissing
	// ReasonMalformed: an Authorization header that is not one Basic or Bearer credential.
	ReasonMalformed
	// ReasonInvalid: an unknown user, a wrong password or an unknown token. One
	// reason for all three, so not even the metric tells names apart.
	ReasonInvalid
	// ReasonTenantDenied: a user pinned to a tenant sent another X-Scope-OrgID.
	ReasonTenantDenied
	// ReasonBusy: every password verification slot stayed taken for the wait.
	ReasonBusy
	reasonCount
)

var reasonNames = [reasonCount]string{"", "missing", "malformed", "invalid", "tenant_denied", "busy"}

func (r Reason) String() string {
	if r < reasonCount {
		return reasonNames[r]
	}
	return "unknown"
}

// Reasons is every failure reason, in order.
func Reasons() []string { return append([]string(nil), reasonNames[1:]...) }

// Method is how a caller authenticated.
type Method uint8

const (
	MethodNone Method = iota
	MethodBasic
	MethodBearer
)

func (m Method) String() string {
	switch m {
	case MethodBasic:
		return "basic_auth"
	case MethodBearer:
		return "bearer_token"
	default:
		return ""
	}
}

// Identity is an authenticated caller.
type Identity struct {
	Name   string
	Tenant string // the pinned tenant, "" when the user may send any
	Method Method
}

type identityKey struct{}

// WithIdentity stores the authenticated caller in ctx.
func WithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, identityKey{}, id)
}

// IdentityFrom is the authenticated caller ctx carries.
func IdentityFrom(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(identityKey{}).(Identity)
	return id, ok
}

// MaxHeaderBytes bounds the Authorization value read.
const MaxHeaderBytes = 8192

// maxPasswordBytes is bcrypt's input limit: it ignores bytes past 72, so a
// longer password is refused instead of matching on its prefix.
const maxPasswordBytes = 72

// Credential is a parsed Authorization header.
type Credential struct {
	Method Method
	User   string // basic only
	Secret string // the password or the token
}

// ParseAuthorization reads the request's Authorization header values: exactly
// one value holding a Basic (base64 user:password) or Bearer credential, the
// scheme in any case (RFC 9110). It never returns the secret in a reason.
func ParseAuthorization(values []string) (Credential, Reason) {
	if len(values) == 0 || (len(values) == 1 && strings.TrimSpace(values[0]) == "") {
		return Credential{}, ReasonMissing
	}
	if len(values) > 1 || len(values[0]) > MaxHeaderBytes {
		return Credential{}, ReasonMalformed
	}
	v := strings.TrimSpace(values[0])
	scheme, rest, ok := strings.Cut(v, " ")
	if !ok {
		return Credential{}, ReasonMalformed
	}
	rest = strings.TrimLeft(rest, " ")
	switch {
	case strings.EqualFold(scheme, "Basic"):
		raw, err := base64.StdEncoding.DecodeString(rest)
		if err != nil {
			return Credential{}, ReasonMalformed
		}
		name, password, ok := strings.Cut(string(raw), ":")
		if !ok || name == "" {
			return Credential{}, ReasonMalformed
		}
		return Credential{Method: MethodBasic, User: name, Secret: password}, ReasonNone
	case strings.EqualFold(scheme, "Bearer"):
		if rest == "" {
			return Credential{}, ReasonMalformed
		}
		for i := 0; i < len(rest); i++ {
			if c := rest[i]; c <= ' ' || c > '~' {
				return Credential{}, ReasonMalformed
			}
		}
		return Credential{Method: MethodBearer, Secret: rest}, ReasonNone
	default:
		return Credential{}, ReasonMalformed
	}
}

// PinTenant applies a user's tenant pin to the X-Scope-OrgID values a request
// carries: none gets the pin, exactly the pin keeps it, anything else (another
// tenant, a multi-tenant a|b value, several header lines, a spelling with
// blanks) is refused. It returns the value to set, or false.
func PinTenant(id Identity, values []string) (string, bool) {
	if id.Tenant == "" {
		return "", true
	}
	switch {
	case len(values) == 0:
		return id.Tenant, true
	case len(values) == 1 && values[0] == id.Tenant:
		return id.Tenant, true
	default:
		return "", false
	}
}

// Options tune the authenticator.
type Options struct {
	// MaxConcurrentVerifications bounds bcrypt verifications in flight; above
	// it a request waits up to VerifyWait, then is refused with ReasonBusy.
	MaxConcurrentVerifications int
	// CacheTTL is how long a verified basic credential skips bcrypt (and a
	// refused one is refused without it, for at most a minute). 0 turns both
	// caches off.
	CacheTTL time.Duration
	// VerifyWait is the longest a request waits for a verification slot.
	VerifyWait time.Duration
}

// DefaultMaxConcurrentVerifications, DefaultCacheTTL and DefaultVerifyWait
// are the defaults of the flags that set them.
const (
	DefaultMaxConcurrentVerifications = 4
	DefaultCacheTTL                   = 5 * time.Minute
	DefaultVerifyWait                 = time.Second
	// cacheMaxEntries bounds each credential cache.
	cacheMaxEntries = 4096
	// failCacheMaxTTL bounds how long a refused credential stays refused
	// without bcrypt, so a password just added to the file is not refused for
	// long by a cached failure (a reload also clears the caches).
	failCacheMaxTTL = time.Minute
)

// generation is the users set with the number that keys its cache entries.
type generation struct {
	users *Users
	gen   uint64
}

// Authenticator checks credentials against the current users set.
type Authenticator struct {
	cur     atomic.Pointer[generation]
	nextGen atomic.Uint64
	key     [32]byte
	sem     chan struct{}
	wait    time.Duration
	ok      *credCache
	fail    *credCache
	failTTL time.Duration
	// compare is bcrypt.CompareHashAndPassword; tests count calls through it.
	compare func(hash, password []byte) error
	now     func() time.Time
}

// New builds an authenticator over users.
func New(users *Users, opts Options) (*Authenticator, error) {
	if opts.MaxConcurrentVerifications <= 0 {
		opts.MaxConcurrentVerifications = DefaultMaxConcurrentVerifications
	}
	if opts.VerifyWait <= 0 {
		opts.VerifyWait = DefaultVerifyWait
	}
	a := &Authenticator{
		sem:     make(chan struct{}, opts.MaxConcurrentVerifications),
		wait:    opts.VerifyWait,
		compare: bcrypt.CompareHashAndPassword,
		now:     time.Now,
	}
	if _, err := rand.Read(a.key[:]); err != nil {
		return nil, err
	}
	if opts.CacheTTL > 0 {
		a.ok = newCredCache(opts.CacheTTL)
		a.failTTL = min(opts.CacheTTL, failCacheMaxTTL)
		a.fail = newCredCache(a.failTTL)
	}
	a.Swap(users)
	return a, nil
}

// Swap replaces the users set (a reload). Credentials verified against the old
// set are verified again.
func (a *Authenticator) Swap(users *Users) {
	a.cur.Store(&generation{users: users, gen: a.nextGen.Add(1)})
	a.ok.clear()
	a.fail.clear()
}

// Users is the current users set.
func (a *Authenticator) Users() *Users { return a.cur.Load().users }

// Challenge is the WWW-Authenticate values of a refusal: the schemes the
// users file holds.
func (a *Authenticator) Challenge() []string {
	u := a.Users()
	out := make([]string, 0, 2)
	if u.HasBasic() {
		out = append(out, `Basic realm="loki-vl-proxy", charset="UTF-8"`)
	}
	if u.HasBearer() {
		out = append(out, `Bearer realm="loki-vl-proxy"`)
	}
	return out
}

// Authenticate checks the request's Authorization header values.
func (a *Authenticator) Authenticate(ctx context.Context, authorization []string) (Identity, Reason) {
	cred, reason := ParseAuthorization(authorization)
	if reason != ReasonNone {
		return Identity{}, reason
	}
	g := a.cur.Load()
	if cred.Method == MethodBearer {
		return a.bearer(g.users, cred.Secret)
	}
	return a.basic(ctx, g, cred.User, cred.Secret)
}

// bearer compares the token's digest with every token user's digest in
// constant time, without stopping at a match, so the time spent does not
// depend on which entry (if any) matched.
func (a *Authenticator) bearer(users *Users, token string) (Identity, Reason) {
	digest := sha256.Sum256([]byte(token))
	var match *user
	for _, u := range users.tokens {
		if subtle.ConstantTimeCompare(u.tokenDigest[:], digest[:]) == 1 {
			match = u
		}
	}
	if match == nil {
		return Identity{}, ReasonInvalid
	}
	return Identity{Name: match.name, Tenant: match.tenant, Method: MethodBearer}, ReasonNone
}

// basic verifies a user name and password. A credential verified in the last
// CacheTTL (and refused in the last minute) is answered from the cache, before
// any verification slot is taken, so a flood of new credentials never delays
// callers already verified. An unknown name, or a user without a password
// hash, is compared with the dearest hash in the file, so it costs what a
// known name costs.
func (a *Authenticator) basic(ctx context.Context, g *generation, name, password string) (Identity, Reason) {
	key := a.cacheKey(g.gen, name, password)
	now := a.now()
	if id, ok := a.ok.get(key, now); ok {
		return id, ReasonNone
	}
	if _, ok := a.fail.get(key, now); ok {
		return Identity{}, ReasonInvalid
	}
	if len(password) > maxPasswordBytes || !g.users.HasBasic() {
		a.fail.put(key, Identity{}, now)
		return Identity{}, ReasonInvalid
	}
	if !a.acquire(ctx) {
		return Identity{}, ReasonBusy
	}
	u := g.users.byName[name]
	hash := g.users.dummyHash
	known := u != nil && u.passwordHash != nil
	if known {
		hash = u.passwordHash
	}
	err := a.compare(hash, []byte(password))
	<-a.sem
	if !known || err != nil {
		a.fail.put(key, Identity{}, a.now())
		return Identity{}, ReasonInvalid
	}
	id := Identity{Name: u.name, Tenant: u.tenant, Method: MethodBasic}
	a.ok.put(key, id, a.now())
	return id, ReasonNone
}

func (a *Authenticator) acquire(ctx context.Context) bool {
	select {
	case a.sem <- struct{}{}:
		return true
	default:
	}
	t := time.NewTimer(a.wait)
	defer t.Stop()
	select {
	case a.sem <- struct{}{}:
		return true
	case <-t.C:
		return false
	case <-ctx.Done():
		return false
	}
}

// cacheKey is HMAC-SHA256 over the generation, the name's length, the name and
// the password, with a key drawn at start: the cache holds no plaintext, and
// its keys are useless outside this process.
func (a *Authenticator) cacheKey(gen uint64, name, password string) [sha256.Size]byte {
	mac := hmac.New(sha256.New, a.key[:])
	var hdr [16]byte
	binary.BigEndian.PutUint64(hdr[:8], gen)
	binary.BigEndian.PutUint64(hdr[8:], uint64(len(name)))
	_, _ = mac.Write(hdr[:])
	_, _ = mac.Write([]byte(name))
	_, _ = mac.Write([]byte(password))
	var out [sha256.Size]byte
	mac.Sum(out[:0])
	return out
}

// credCache is a bounded map of credential keys to identities with a TTL. When
// full, an insert drops an arbitrary entry, so a flood of distinct credentials
// costs O(1) per insert and never grows it.
type credCache struct {
	mu      sync.Mutex
	ttl     time.Duration
	entries map[[sha256.Size]byte]credEntry
}

type credEntry struct {
	id      Identity
	expires time.Time
}

func newCredCache(ttl time.Duration) *credCache {
	return &credCache{ttl: ttl, entries: make(map[[sha256.Size]byte]credEntry)}
}

func (c *credCache) get(key [sha256.Size]byte, now time.Time) (Identity, bool) {
	if c == nil {
		return Identity{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok {
		return Identity{}, false
	}
	if !now.Before(e.expires) {
		delete(c.entries, key)
		return Identity{}, false
	}
	return e.id, true
}

func (c *credCache) put(key [sha256.Size]byte, id Identity, now time.Time) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.entries[key]; !ok && len(c.entries) >= cacheMaxEntries {
		for k := range c.entries {
			delete(c.entries, k)
			break
		}
	}
	c.entries[key] = credEntry{id: id, expires: now.Add(c.ttl)}
}

func (c *credCache) clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	clear(c.entries)
	c.mu.Unlock()
}

func (c *credCache) len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}
