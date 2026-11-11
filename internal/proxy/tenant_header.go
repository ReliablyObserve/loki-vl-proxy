package proxy

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
)

// Loki's answers to an X-Scope-OrgID it refuses, recorded from Loki 3.7.9 with
// auth_enabled: true (testdata/tenant_auth_loki_answers.jsonl):
//
//   - no header, an empty one or only spaces: 401 "no org id\n" on every route;
//   - an ID that is '.' or '..', longer than 150 bytes, or holds a byte outside
//     A-Z a-z 0-9 ! ' ( ) * - . : _ (one per member of a multi-tenant header,
//     in header order): 400 with the text below, no newline; query_range,
//     detected_fields and detected_field values put the querier's
//     "rpc error: code = Code(400) desc = " before it, tail answers 401 with a
//     newline and delete 400 with a newline;
//   - more than one distinct member on tail (400, no newline) or delete (400
//     with a newline, after checking only the first member): "multiple org IDs
//     present".
//
// The ruler's read routes answer in their Prometheus JSON instead: the Loki
// rules views 500 "no org id" for any refused or multi-tenant header, the
// Prometheus rules and alerts views 400 "too many org ids found" for a
// multi-tenant header (before its members are checked) and 500 "no valid org id
// found" for a refused ID.
//
// A header whose members are all the same ID ("a|a") is that one tenant; one
// that holds an empty member next to an ID ("a|") is a multi-tenant request
// over that ID, answered with __tenant_id__ like any other.
const (
	lokiNoOrgID            = "no org id"
	lokiMultipleOrgIDs     = "multiple org IDs present"
	lokiTenantIDMaxLength  = 150
	lokiQuerierErrorPrefix = "rpc error: code = Code(400) desc = "
)

// lokiTenantRefusalKind says which of Loki's tenant refusals an error is.
type lokiTenantRefusalKind uint8

const (
	lokiTenantNotRefused lokiTenantRefusalKind = iota
	lokiTenantMissing
	lokiTenantInvalid
	lokiTenantMultiple
)

// lokiTenantIDAllowed is the byte set Loki accepts in a tenant ID.
var lokiTenantIDAllowed = func() (set [256]bool) {
	for c := 'a'; c <= 'z'; c++ {
		set[c] = true
	}
	for c := 'A'; c <= 'Z'; c++ {
		set[c] = true
	}
	for c := '0'; c <= '9'; c++ {
		set[c] = true
	}
	for _, c := range "!'()*-.:_" {
		set[c] = true
	}
	return set
}()

// lokiTenantIDError is Loki's text for a tenant ID it refuses, or "" when it
// accepts the ID. The empty ID is accepted: Loki reads it only as a member of
// a multi-tenant header.
func lokiTenantIDError(id string) string {
	if id == "." || id == ".." {
		return "tenant ID is '.' or '..'"
	}
	for i := 0; i < len(id); i++ {
		if !lokiTenantIDAllowed[id[i]] {
			// Loki names the byte, not the character: 'é' is reported as 'Ã'.
			// The ID is quoted as sent, invalid UTF-8 included.
			return "tenant ID '" + id + "' contains unsupported character '" + string(rune(id[i])) + "'"
		}
	}
	// Loki checks the characters first: 151 bytes of 'é' are refused for
	// their first byte, not their length.
	if len(id) > lokiTenantIDMaxLength {
		return "tenant ID is too long: max 150 characters"
	}
	return ""
}

// tenantHeaderIgnored reports whether tenancy is turned off completely, the
// proxy then being Loki's single-tenant mode (auth_enabled: false):
// X-Scope-OrgID is ignored and every request reads the backend default
// tenant, as Loki serves tenant "fake" to everyone. Tenancy is off only when
// numeric passthrough is off (-tenant.numeric-passthrough=false) and nothing
// else names tenants: no -auth.enabled, no -require-tenant-header, no tenant
// map (static, or a -tenant-map-file even while it is empty) and no
// -tenant-label. Numeric passthrough is on by default, so the default
// deployment routes X-Scope-OrgID: 7 to account 7. The tenant map and label
// are read from the live routing, so a reload that adds or removes the map
// switches mode.
func (p *Proxy) tenantHeaderIgnored(routing requestRouting) bool {
	return p.strictTenantRouting && !p.authEnabled && !p.requireTenantHeader && !p.tenantMapFileSet &&
		routing.label == "" && len(routing.tenants) == 0
}

// lokiTenantMembers splits a header that holds '|' into its distinct members
// in header order, keeping empty ones, as Loki counts them. The members are
// deduplicated in place: a scan for the few members a real header holds, a map
// past that so a long crafted header stays linear.
func lokiTenantMembers(orgID string) []string {
	parts := strings.Split(orgID, "|")
	out := parts[:0]
	if len(parts) > 32 {
		seen := make(map[string]struct{}, len(parts))
		for _, part := range parts {
			if _, ok := seen[part]; !ok {
				seen[part] = struct{}{}
				out = append(out, part)
			}
		}
		return out
	}
	for _, part := range parts {
		if !slices.Contains(out, part) {
			out = append(out, part) // writes at or before the index read
		}
	}
	return out
}

// lokiTenantSingleMember reports the one ID of a header whose members are all
// that ID ("a|a"), without allocating.
func lokiTenantSingleMember(orgID string) (string, bool) {
	first, rest, _ := strings.Cut(orgID, "|")
	for {
		var part string
		var more bool
		part, rest, more = strings.Cut(rest, "|")
		if part != first {
			return "", false
		}
		if !more {
			return first, true
		}
	}
}

// normalizeTenantHeader rewrites X-Scope-OrgID into the form Loki reads: a
// blank header is no header, and a multi-tenant header whose members are all
// one ID is that ID.
func normalizeTenantHeader(r *http.Request) {
	raw, present := r.Header["X-Scope-Orgid"]
	if !present {
		return
	}
	orgID := ""
	if len(raw) > 0 {
		orgID = strings.TrimSpace(raw[0])
	}
	if strings.IndexByte(orgID, '|') >= 0 {
		if id, ok := lokiTenantSingleMember(orgID); ok {
			orgID = id
		}
	}
	if orgID == "" {
		r.Header.Del("X-Scope-OrgID")
		return
	}
	if len(raw) != 1 || raw[0] != orgID {
		r.Header.Set("X-Scope-OrgID", orgID)
	}
}

func lokiTenantRefusal(kind lokiTenantRefusalKind, msg string) *requestPolicyError {
	status := http.StatusBadRequest
	if kind == lokiTenantMissing {
		status = http.StatusUnauthorized
	}
	return &requestPolicyError{status: status, msg: msg, loki: kind}
}

// validateLokiTenantMembers checks the members of a multi-tenant header as
// Loki does on path: every member in order, then the count where the route
// takes one tenant; delete checks only its first member before the count.
func validateLokiTenantMembers(path string, members []string) error {
	check := members
	if path == "/loki/api/v1/delete" {
		check = members[:1]
	}
	for _, m := range check {
		if msg := lokiTenantIDError(m); msg != "" {
			return lokiTenantRefusal(lokiTenantInvalid, msg)
		}
	}
	return nil
}

// tenantRulerView says which ruler view path is: 1 the Loki rules views, 2 the
// Prometheus rules and alerts views, 0 none.
func tenantRulerView(path string) int {
	switch {
	case path == "/loki/api/v1/rules" || strings.HasPrefix(path, "/loki/api/v1/rules/") ||
		path == "/api/prom/rules" || strings.HasPrefix(path, "/api/prom/rules/"):
		return 1
	case path == "/prometheus/api/v1/rules" || path == "/prometheus/api/v1/alerts":
		return 2
	}
	return 0
}

// writeLokiTenantRefusal answers a refused X-Scope-OrgID in the shape Loki's
// route uses for it (see the comment at the top of this file).
func (p *Proxy) writeLokiTenantRefusal(w http.ResponseWriter, path string, e *requestPolicyError) {
	const tail, del = "/loki/api/v1/tail", "/loki/api/v1/delete"
	if view := tenantRulerView(path); view != 0 && e.loki != lokiTenantMissing {
		switch {
		case view == 1:
			p.writeTenantRefusal(w, http.StatusInternalServerError, lokiNoOrgID, writeLokiRulerError)
		case e.loki == lokiTenantMultiple:
			p.writeTenantRefusal(w, http.StatusBadRequest, "too many org ids found", writeLokiRulerError)
		default:
			p.writeTenantRefusal(w, http.StatusInternalServerError, "no valid org id found", writeLokiRulerError)
		}
		return
	}
	switch e.loki {
	case lokiTenantMissing:
		p.writeTenantRefusal(w, http.StatusUnauthorized, lokiNoOrgID, writeLokiHTTPError)
	case lokiTenantMultiple:
		shape := errorShape(writeLokiTenantErrorBody)
		if path == del {
			shape = writeLokiHTTPError
		}
		p.writeTenantRefusal(w, http.StatusBadRequest, lokiMultipleOrgIDs, shape)
	default:
		switch {
		case path == tail:
			p.writeTenantRefusal(w, http.StatusUnauthorized, e.msg, writeLokiHTTPError)
		case path == del:
			p.writeTenantRefusal(w, http.StatusBadRequest, e.msg, writeLokiHTTPError)
		case path == "/loki/api/v1/query_range" || path == "/loki/api/v1/detected_fields" ||
			strings.HasPrefix(path, "/loki/api/v1/detected_field/"):
			p.writeTenantRefusal(w, http.StatusBadRequest, lokiQuerierErrorPrefix+e.msg, writeLokiTenantErrorBody)
		default:
			p.writeTenantRefusal(w, http.StatusBadRequest, e.msg, writeLokiTenantErrorBody)
		}
	}
}

// tenantRefusalLogBytes bounds the refusal text in the log: the body quotes
// the header as Loki does, up to -http-max-header-bytes; the log keeps enough
// to see the reason.
const tenantRefusalLogBytes = 256

// writeTenantRefusal writes a tenant refusal in shape and logs it at WARN with
// its text cut to tenantRefusalLogBytes.
func (p *Proxy) writeTenantRefusal(w http.ResponseWriter, code int, msg string, shape errorShape) {
	if p.log != nil && p.log.Enabled(context.Background(), slog.LevelWarn) {
		logged := msg
		if len(logged) > tenantRefusalLogBytes {
			logged = logged[:tenantRefusalLogBytes] + "..."
		}
		p.log.Warn("request error", "code", code, "error", strings.ToValidUTF8(logged, "\uFFFD"))
	}
	shape(w, code, msg)
}

// writeLokiTenantErrorBody is writeLokiErrorBody without the UTF-8 repair: Loki
// quotes a refused tenant ID byte for byte, invalid UTF-8 included. The bytes
// are the client's own header, sent back as text/plain with nosniff.
func writeLokiTenantErrorBody(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	_, _ = io.WriteString(w, msg)
}
