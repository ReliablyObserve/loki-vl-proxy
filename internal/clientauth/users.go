// Package clientauth authenticates the callers of the proxy's read API with
// HTTP Basic credentials (a bcrypt password hash per user) or bearer tokens
// (a SHA-256 digest per user), read from a users file the operator mounts
// (-server.auth-users-file). It is off unless that file is configured; with it
// on, a request without a valid credential is refused before any route runs.
//
// Nothing here keeps a plaintext credential: the users file holds hashes only,
// the verified-credential cache is keyed by an HMAC with a per-process random
// key, and no error, log line or metric carries a password, token or hash.
package clientauth

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"
)

const (
	// MaxFileBytes bounds the users file: it is read whole on every change.
	MaxFileBytes = 1 << 20
	// MaxUsers bounds the entries one file may hold.
	MaxUsers = 10000
	// MaxNameBytes bounds a user name. Metric labels keep 64 bytes of a client
	// identity, so a longer name could share a label with another user.
	MaxNameBytes = 64
	// minCost and maxCost bound a bcrypt hash's cost. htpasswd -B writes cost 5
	// by default, so low costs are accepted (and reported); above 16 one
	// verification takes seconds, which no per-request check can afford.
	minCost = bcrypt.MinCost
	maxCost = 16
)

// user is one entry of the users file, as the authenticator reads it.
type user struct {
	name         string
	tenant       string
	passwordHash []byte // nil: the user has no basic credential
	cost         int
	tokenDigest  [sha256.Size]byte
	hasToken     bool
}

// Users is a parsed users file. It is immutable once built.
type Users struct {
	byName map[string]*user
	tokens []*user
	// dummyHash is the bcrypt hash a basic credential with an unknown name is
	// compared with, so an unknown name costs what a known one costs. It is
	// the hash of the user with the highest cost.
	dummyHash []byte
	costs     map[int]int
	pins      int
	digest    [sha256.Size]byte // of the file content it was parsed from
}

// Len is the number of users.
func (u *Users) Len() int { return len(u.byName) }

// HasBasic reports whether any user has a password hash.
func (u *Users) HasBasic() bool { return u.dummyHash != nil }

// HasBearer reports whether any user has a token digest.
func (u *Users) HasBearer() bool { return len(u.tokens) > 0 }

// Pins is the number of users pinned to a tenant.
func (u *Users) Pins() int { return u.pins }

// MixedCosts reports bcrypt hashes of more than one cost. A wrong password for
// a cheaper user then answers faster than one for a dearer user, which tells a
// caller which cost class a known name has (not whether a name exists: unknown
// names are compared with the dearest hash).
func (u *Users) MixedCosts() bool { return len(u.costs) > 1 }

// LowestCost is the lowest bcrypt cost in the file, 0 without basic users.
func (u *Users) LowestCost() int {
	lowest := 0
	for c := range u.costs {
		if lowest == 0 || c < lowest {
			lowest = c
		}
	}
	return lowest
}

// fileSchema is the users file:
//
//	users:
//	  - name: grafana
//	    password_hash: "$2y$12$..."   # bcrypt (htpasswd -nbBC 12 grafana <password>)
//	    tenant: team-a                # optional: the only X-Scope-OrgID this user may send
//	  - name: ci
//	    token_sha256: "<64 hex>"      # sha256 of the bearer token
type fileSchema struct {
	Users []userSchema `yaml:"users"`
}

type userSchema struct {
	Name         string `yaml:"name"`
	PasswordHash string `yaml:"password_hash"`
	TokenSHA256  string `yaml:"token_sha256"`
	Tenant       string `yaml:"tenant"`
}

var yamlLineRe = regexp.MustCompile(`line (\d+)`)

// errYAML is the parse error: the YAML library's own text can quote the input,
// and the input holds hashes, so only the line number is kept.
func errYAML(err error) error {
	if m := yamlLineRe.FindStringSubmatch(err.Error()); m != nil {
		return fmt.Errorf("users file is not valid YAML of the expected shape (line %s): it must be a map with a users list of name, password_hash, token_sha256 and tenant entries", m[1])
	}
	return errors.New("users file is not valid YAML of the expected shape: it must be a map with a users list of name, password_hash, token_sha256 and tenant entries")
}

// validName is a user name the proxy can use as a metric label and log value
// as it is, and that a Basic credential can carry (no ':').
func validName(name string) bool {
	if name == "" || len(name) > MaxNameBytes {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '-', c == '_', c == '.', c == '@':
		default:
			return false
		}
	}
	return true
}

// Parse reads a users file. validTenant checks a tenant pin (nil accepts any
// non-empty pin without '|'). Errors name the entry by its position and name,
// never a hash or token.
func Parse(data []byte, validTenant func(string) error) (*Users, error) {
	if len(data) > MaxFileBytes {
		return nil, fmt.Errorf("users file is larger than %d bytes", MaxFileBytes)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, errors.New("users file is empty: it must list at least one user")
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var doc fileSchema
	if err := dec.Decode(&doc); err != nil {
		return nil, errYAML(err)
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("users file holds more than one YAML document")
	}
	if len(doc.Users) == 0 {
		return nil, errors.New("users file lists no users")
	}
	if len(doc.Users) > MaxUsers {
		return nil, fmt.Errorf("users file lists %d users, more than %d", len(doc.Users), MaxUsers)
	}
	out := &Users{byName: make(map[string]*user, len(doc.Users)), costs: map[int]int{}, digest: sha256.Sum256(data)}
	digests := make(map[[sha256.Size]byte]string)
	highest := -1
	for i, e := range doc.Users {
		at := fmt.Sprintf("users[%d]", i)
		if !validName(e.Name) {
			return nil, fmt.Errorf("%s: name must be 1-%d bytes of letters, digits, '-', '_', '.' or '@'", at, MaxNameBytes)
		}
		at = fmt.Sprintf("users[%d] (%s)", i, e.Name)
		if _, dup := out.byName[e.Name]; dup {
			return nil, fmt.Errorf("%s: the name is listed twice", at)
		}
		if e.PasswordHash == "" && e.TokenSHA256 == "" {
			return nil, fmt.Errorf("%s: needs password_hash (bcrypt) or token_sha256", at)
		}
		u := &user{name: e.Name}
		if e.PasswordHash != "" {
			hash := []byte(strings.TrimSpace(e.PasswordHash))
			cost, err := bcrypt.Cost(hash)
			if err != nil {
				return nil, fmt.Errorf("%s: password_hash is not a bcrypt hash (write one with htpasswd -nbBC 12 <name> <password>)", at)
			}
			if cost < minCost || cost > maxCost {
				return nil, fmt.Errorf("%s: password_hash has bcrypt cost %d, outside %d-%d", at, cost, minCost, maxCost)
			}
			u.passwordHash, u.cost = hash, cost
			out.costs[cost]++
			if cost > highest {
				highest, out.dummyHash = cost, hash
			}
		}
		if e.TokenSHA256 != "" {
			raw, err := hex.DecodeString(strings.TrimSpace(e.TokenSHA256))
			if err != nil || len(raw) != sha256.Size {
				return nil, fmt.Errorf("%s: token_sha256 must be the 64 hex digits of the token's SHA-256", at)
			}
			copy(u.tokenDigest[:], raw)
			if other, dup := digests[u.tokenDigest]; dup {
				return nil, fmt.Errorf("%s: token_sha256 is also the token of %s", at, other)
			}
			digests[u.tokenDigest] = e.Name
			u.hasToken = true
			out.tokens = append(out.tokens, u)
		}
		if e.Tenant != "" {
			if err := checkTenant(e.Tenant, validTenant); err != nil {
				return nil, fmt.Errorf("%s: tenant: %w", at, err)
			}
			u.tenant = e.Tenant
			out.pins++
		}
		out.byName[e.Name] = u
	}
	return out, nil
}

func checkTenant(t string, validTenant func(string) error) error {
	if strings.ContainsRune(t, '|') {
		return errors.New("a user is pinned to one tenant: '|' is not allowed")
	}
	if validTenant != nil {
		return validTenant(t)
	}
	return nil
}

// ReadFile reads at most MaxFileBytes+1 bytes of path, so Parse can refuse an
// oversized file without reading it whole.
func ReadFile(path string) ([]byte, error) {
	f, err := os.Open(path) //nolint:gosec // G304: the operator's -server.auth-users-file
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return io.ReadAll(io.LimitReader(f, MaxFileBytes+1))
}

// LoadFile reads and parses the users file at path.
func LoadFile(path string, validTenant func(string) error) (*Users, error) {
	data, err := ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read users file: %w", err)
	}
	users, err := Parse(data, validTenant)
	if err != nil {
		return nil, fmt.Errorf("users file %s: %w", path, err)
	}
	return users, nil
}
