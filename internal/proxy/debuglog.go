package proxy

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

// redactQuery returns a deterministic fingerprint of s suitable for debug
// logging without leaking literal contents.
//
// Format: "sha256:<8 hex chars> len=<n>"
//
// When raw is true, s is returned verbatim. Intended for local dev only via
// the --debug-log-raw-queries flag.
//
// Tenant IDs / orgIDs are NOT routed through this helper; they remain in plain
// text in debug logs because they are operationally critical and not sensitive
// in this proxy's threat model.
func redactQuery(s string, raw bool) string {
	if raw {
		return s
	}
	sum := sha256.Sum256([]byte(s))
	return "sha256:" + hex.EncodeToString(sum[:])[:8] + " len=" + strconv.Itoa(len(s))
}
