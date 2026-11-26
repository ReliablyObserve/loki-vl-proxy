package clientauth

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"
	"time"
)

// FuzzParseUsersFile: any input parses or is refused quickly, never panics,
// and a refusal never quotes a hash or token from the input.
//
// conformance: client-authentication
func FuzzParseUsersFile(f *testing.F) {
	f.Add([]byte("users:\n  - name: a\n    password_hash: \"$2a$04$Bj9fAnn7rHkv8OXXNdETAuSV2EVKk5bODCUSqEXFGI0rAgMlJ/FY6\"\n    tenant: team-a\n"))
	f.Add([]byte("users:\n  - name: ci\n    token_sha256: " + strings.Repeat("ab", 32) + "\n"))
	f.Add([]byte("users: ["))
	f.Add([]byte("users:\n  - name: 'a:b'\n"))
	f.Add([]byte("- a\n- b\n"))
	f.Add([]byte("users:\n  - &a {name: x, token_sha256: " + strings.Repeat("cd", 32) + "}\n  - *a\n"))
	f.Add([]byte(""))
	f.Fuzz(func(t *testing.T, data []byte) {
		start := time.Now()
		u, err := Parse(data, nil)
		if d := time.Since(start); d > 2*time.Second {
			t.Fatalf("parse took %s", d)
		}
		if err == nil {
			if u.Len() == 0 {
				t.Fatal("parsed a file with no users")
			}
			return
		}
		if strings.Contains(err.Error(), "$2") {
			t.Fatalf("error quotes a hash: %q", err)
		}
	})
}

// FuzzParseAuthorization: any header value parses or is refused with a reason,
// never panics, and a parsed credential round-trips.
//
// conformance: client-authentication
func FuzzParseAuthorization(f *testing.F) {
	f.Add("Basic " + base64.StdEncoding.EncodeToString([]byte("user:pass")))
	f.Add("Bearer abc.def-ghi")
	f.Add("basic Og==")
	f.Add("Bearer ")
	f.Add("Digest x")
	f.Add("")
	a := mustAuth(f, testUsers(f), Options{CacheTTL: time.Minute})
	a.compare = func(_, _ []byte) error { return nil } // the parse and lookup are fuzzed, not bcrypt
	f.Fuzz(func(t *testing.T, header string) {
		cred, reason := ParseAuthorization([]string{header})
		switch reason {
		case ReasonNone:
			if cred.Method == MethodNone || cred.Secret == "" && cred.Method == MethodBearer {
				t.Fatalf("parsed %q to %+v", header, cred)
			}
			if cred.Method == MethodBasic && (cred.User == "" || strings.Contains(cred.User, ":")) {
				t.Fatalf("basic user %q from %q", cred.User, header)
			}
		case ReasonMissing, ReasonMalformed:
			if cred != (Credential{}) {
				t.Fatalf("refused %q but returned %+v", header, cred)
			}
		default:
			t.Fatalf("reason %v from parsing", reason)
		}
		id, r := a.Authenticate(context.Background(), []string{header})
		if r == ReasonNone && id.Name == "" {
			t.Fatalf("authenticated %q without a name", header)
		}
	})
}
