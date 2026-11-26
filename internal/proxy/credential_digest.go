package proxy

import (
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"net/http"
	"net/url"
	"slices"
	"sync"
)

// Credential digests in cache keys.
//
// Cache keys are stored where the answers are: the disk tier, the restart
// hand-over, pattern and label-index snapshots, peers' copies, the peers' hot
// index and debug log lines. A key that names a request's credentials (a
// -forward-headers header such as Authorization, a -forward-cookies cookie) or
// the backend credentials (-backend-basic-auth) carries them only as an
// HMAC-SHA256 under a key derived at start, so a stored key cannot be used to
// test guesses of a credential without that key, which is never stored beside
// the cache.
//
// The key, in order of precedence:
//   - -cache-key-secret (file or environment): set it to the same value on
//     every replica to share credential-scoped entries across replicas and
//     restarts;
//   - else -peer-auth-token, which every replica of a peer-cache fleet shares
//     already (the chart generates one): a key derived from it, never the token
//     itself;
//   - else a random key per process: credential-scoped entries are then never
//     reused after a restart or by another replica.
//
// Replicas or builds with different keys compute different keys for the same
// credentials: a miss, never another caller's answer. Keys without any
// credential are byte for byte what they were.

// credentialDigestInfo separates the derived key from every other use of the
// secret it comes from (the peer token also authenticates /_cache/* calls).
const credentialDigestInfo = "loki-vl-proxy/cache-key-credential-digest/v1" // #nosec G101 -- a public HKDF context label, not a credential

// credentialDigestVersion follows identityFingerprintPrefix in a keyed
// fingerprint. '.' is not a hex digit, so a keyed fingerprint never equals the
// unkeyed SHA-256 fingerprint ("fwdid." + 64 hex digits) earlier builds wrote:
// entries of those builds are never read for anyone.
const credentialDigestVersion = "k1."

// Key sources, for the start-up log line.
const ( // #nosec G101 -- names of where the key comes from, not credentials
	credentialKeyFromSecret    = "cache-key-secret"
	credentialKeyFromPeerToken = "peer-auth-token"
	credentialKeyEphemeral     = "per-process"
)

type credentialDigest struct {
	source string
	pool   sync.Pool // of *credentialHasher
}

// credentialHasher is HMAC-SHA256 under the derived key with its scratch
// buffers, reused across requests.
type credentialHasher struct {
	h     hash.Hash
	in    []byte   // the encoded input; zeroed after each use
	names []string // sorted header names
	sum   [sha256.Size]byte
}

// newCredentialDigest derives the digest key from secret, else from
// peerToken, else from random bytes.
func newCredentialDigest(secret, peerToken string) *credentialDigest {
	var ikm []byte
	d := &credentialDigest{}
	switch {
	case secret != "":
		ikm, d.source = []byte(secret), credentialKeyFromSecret
	case peerToken != "":
		ikm, d.source = []byte(peerToken), credentialKeyFromPeerToken
	default:
		ikm, d.source = make([]byte, 32), credentialKeyEphemeral
		_, _ = rand.Read(ikm) // never fails (crypto/rand)
	}
	key, err := hkdf.Key(sha256.New, ikm, nil, credentialDigestInfo, sha256.Size)
	if err != nil {
		// Only an output length above 255 hash sizes fails.
		panic("credential digest key: " + err.Error())
	}
	d.pool.New = func() any { return &credentialHasher{h: hmac.New(sha256.New, key)} }
	return d
}

// ephemeralCredentialDigest serves a Proxy built without New (tests): a
// random key for the process.
var ephemeralCredentialDigest = sync.OnceValue(func() *credentialDigest { return newCredentialDigest("", "") })

func (p *Proxy) credentialDigester() *credentialDigest {
	if p.credentialDigest != nil {
		return p.credentialDigest
	}
	return ephemeralCredentialDigest()
}

// Input domains: a fingerprint and a backend header value never digest the
// same bytes.
const (
	credentialDomainFingerprint byte = 'f'
	credentialDomainHeader      byte = 'h'
)

// appendField appends s with its length first, so the encoding is
// unambiguous whatever bytes s holds.
func appendField(b []byte, s string) []byte {
	return append(binary.AppendUvarint(b, uint64(len(s))), s...)
}

// digest is the HMAC-SHA256 of in, hex encoded after head in one string.
func (d *credentialDigest) digest(e *credentialHasher, head string) string {
	e.h.Reset()
	_, _ = e.h.Write(e.in)
	clear(e.in)
	e.in = e.in[:0]
	sum := e.h.Sum(e.sum[:0])
	var out [len(identityFingerprintPrefix) + len(credentialDigestVersion) + 2*sha256.Size]byte
	n := copy(out[:], head)
	n += hex.Encode(out[n:], sum)
	return string(out[:n])
}

// fingerprint is the keyed auth fingerprint of a request that forwards
// credentials: its tenant scope, forwarded headers and forwarded cookies,
// identityFingerprintPrefix first, so peerPushAllowed still keeps such
// entries off the peers' push path.
func (d *credentialDigest) fingerprint(scope string, identity http.Header, cookies [][2]string) string {
	e := d.pool.Get().(*credentialHasher)
	defer d.pool.Put(e)
	e.in = appendField(append(e.in, credentialDomainFingerprint), scope)
	e.names = e.names[:0]
	for name := range identity {
		e.names = append(e.names, name)
	}
	slices.Sort(e.names)
	e.in = binary.AppendUvarint(e.in, uint64(len(e.names)))
	for _, name := range e.names {
		e.in = appendField(e.in, name)
		e.in = binary.AppendUvarint(e.in, uint64(len(identity[name])))
		for _, v := range identity[name] {
			e.in = appendField(e.in, v)
		}
	}
	e.in = binary.AppendUvarint(e.in, uint64(len(cookies)))
	for _, c := range cookies {
		e.in = appendField(appendField(e.in, c[0]), c[1])
	}
	return d.digest(e, identityFingerprintPrefix+credentialDigestVersion)
}

// headerValues returns headers with every value replaced by its keyed digest
// (name and value digested together), for the routing namespace: the backend
// credentials still decide the namespace without being guessable from it.
func (d *credentialDigest) headerValues(headers map[string]string) map[string]string {
	if len(headers) == 0 {
		return headers
	}
	e := d.pool.Get().(*credentialHasher)
	defer d.pool.Put(e)
	out := make(map[string]string, len(headers))
	for name, value := range headers {
		e.in = appendField(appendField(append(e.in, credentialDomainHeader), name), value)
		out[name] = d.digest(e, credentialDigestVersion)
	}
	return out
}

// credentialDomainURLUserinfo digests the user information of a backend URL.
const credentialDomainURLUserinfo byte = 'u'

// urlInKeys is how a backend URL enters cache keys: as it is when it holds no
// user information (unchanged keys), else without it and with its keyed
// digest after '#', so credentials in the URL still decide the key without
// being in it or guessable from it.
func (p *Proxy) urlInKeys(u *url.URL) string {
	if u == nil {
		return ""
	}
	if u.User == nil {
		return u.String()
	}
	bare := *u
	bare.User = nil
	d := p.credentialDigester()
	e := d.pool.Get().(*credentialHasher)
	defer d.pool.Put(e)
	e.in = appendField(append(e.in, credentialDomainURLUserinfo), u.User.String())
	return bare.String() + "#" + d.digest(e, credentialDigestVersion)
}

// backendInKeys is urlInKeys of the backend, computed once.
func (p *Proxy) backendInKeys() string {
	if p.backendKeyText != "" || p.backend == nil {
		return p.backendKeyText
	}
	return p.urlInKeys(p.backend)
}
