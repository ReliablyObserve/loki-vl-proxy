package proxy

import (
	"errors"
	"net/http"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/clientauth"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
)

// RouteMux is what the Register*Routes helpers register on. *http.ServeMux is
// one; tests pass a recorder so every registered pattern can be checked.
type RouteMux interface {
	Handle(pattern string, handler http.Handler)
	HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request))
}

// clientAuthExemptPatterns are the registered patterns caller authentication
// (-server.auth-users-file) leaves open: the probes (kubelet and load balancers
// carry no credential), the Prometheus scrape, and the peer cache routes,
// which have their own shared token (-peer-auth-token). Every other pattern,
// on every listener, needs a credential. A new route is protected unless it is
// added here; TestClientAuth_EveryRegisteredRouteIsProtectedOrExempt checks
// every pattern every topology registers.
var clientAuthExemptPatterns = map[string]bool{
	"/alive":        true,
	"/livez":        true,
	"/health":       true,
	"/healthz":      true,
	"/ready":        true,
	"/metrics":      true,
	"/_cache/get":   true,
	"/_cache/set":   true,
	"/_cache/hot":   true,
	"/_cache/has":   true,
	"/_cache/peers": true,
	"/_cache/purge": true,
}

// ClientAuthExempt reports whether caller authentication leaves the registered
// pattern open.
func ClientAuthExempt(pattern string) bool { return clientAuthExemptPatterns[pattern] }

// The causes of a refusal, for the log line only: they name the reason, never
// the user or the credential.
var (
	errAuthMissing      = errors.New("caller authentication: no Authorization header (-server.auth-users-file)")
	errAuthMalformed    = errors.New("caller authentication: the Authorization header is not one Basic or Bearer credential (-server.auth-users-file)")
	errAuthInvalid      = errors.New("caller authentication: unknown user, wrong password or unknown token (-server.auth-users-file)")
	errAuthTenantDenied = errors.New("caller authentication: X-Scope-OrgID is not the tenant this user is pinned to (-server.auth-users-file)")
	errAuthBusy         = errors.New("caller authentication: every password verification slot stayed busy (-server.auth-max-concurrent-verifications)")
)

func clientAuthCause(r clientauth.Reason) error {
	switch r {
	case clientauth.ReasonMissing:
		return errAuthMissing
	case clientauth.ReasonMalformed:
		return errAuthMalformed
	case clientauth.ReasonTenantDenied:
		return errAuthTenantDenied
	case clientauth.ReasonBusy:
		return errAuthBusy
	default:
		return errAuthInvalid
	}
}

// WithClientAuth puts caller authentication in front of mux. With it off (no
// users file) it returns mux itself: nothing changes on the request path.
//
// The route is classified by the pattern mux itself matches (mux.Handler), so
// a spelling the mux cleans or redirects, or a path no pattern holds, is judged
// as the mux will serve it; only an exempt pattern skips the check. The
// request is then served by mux as usual, so path values are set as before.
func (p *Proxy) WithClientAuth(mux *http.ServeMux) http.Handler {
	if p.clientAuth == nil {
		return mux
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, pattern := mux.Handler(r); clientAuthExemptPatterns[pattern] {
			mux.ServeHTTP(w, r)
			return
		}
		id, reason := p.clientAuth.Authenticate(r.Context(), r.Header.Values("Authorization"))
		if reason == clientauth.ReasonNone {
			pinned, ok := clientauth.PinTenant(id, r.Header.Values("X-Scope-OrgID"))
			if !ok {
				reason = clientauth.ReasonTenantDenied
			} else if pinned != "" {
				r.Header.Set("X-Scope-OrgID", pinned)
			}
		}
		if reason != clientauth.ReasonNone {
			p.refuseClientAuth(w, r, reason)
			return
		}
		// The caller's credential is the proxy's to check, not VictoriaLogs':
		// it is removed unless the operator forwards Authorization
		// (-forward-authorization or -forward-headers=Authorization), in which
		// case cache keys carry its fingerprint as before.
		if !p.forwardsAuthorization {
			r.Header.Del("Authorization")
		}
		mux.ServeHTTP(w, r.WithContext(clientauth.WithIdentity(r.Context(), id)))
	})
}

func (p *Proxy) refuseClientAuth(w http.ResponseWriter, r *http.Request, reason clientauth.Reason) {
	p.metrics.RecordAuthFailure(reason.String())
	if reason == clientauth.ReasonBusy {
		p.writeFailure(w, r.Context(), lokierr.QueueFull(errAuthBusy), writeLokiHTTPError)
		return
	}
	for _, c := range p.clientAuth.Challenge() {
		w.Header().Add("WWW-Authenticate", c)
	}
	p.writeFailure(w, r.Context(), lokierr.Unauthorized(clientAuthCause(reason)), writeLokiHTTPError)
}

// ClientAuth is the caller authenticator, nil when caller authentication is off.
func (p *Proxy) ClientAuth() *clientauth.Authenticator { return p.clientAuth }

// ValidateTenantPin checks a users file tenant pin with Loki's tenant ID rules.
func ValidateTenantPin(id string) error {
	if msg := lokiTenantIDError(id); msg != "" {
		return errors.New(msg)
	}
	return nil
}

// forwardsAuthorizationHeader reports whether the operator forwards the
// Authorization header to VictoriaLogs.
func forwardsAuthorizationHeader(headers []string) bool {
	for _, h := range headers {
		if strings.EqualFold(strings.TrimSpace(h), "Authorization") {
			return true
		}
	}
	return false
}

// resolveClient is the client identity of the metrics' client label and the
// logs' enduser.id: the authenticated user when caller authentication is on,
// otherwise as before (trusted user headers, the tenant, the address).
func (p *Proxy) resolveClient(r *http.Request) (string, string) {
	if p.clientAuth != nil {
		if id, ok := clientauth.IdentityFrom(r.Context()); ok {
			return id.Name, "auth_user"
		}
	}
	return metrics.ResolveClientContext(r, p.metricsTrustProxyHeaders)
}

// resolveAuth is the request's authentication principal and its mechanism:
// the authenticated user when caller authentication is on (the header itself
// is gone by then), otherwise the Basic user name the client sent.
func (p *Proxy) resolveAuth(r *http.Request) (string, string) {
	if p.clientAuth != nil {
		if id, ok := clientauth.IdentityFrom(r.Context()); ok {
			return id.Name, id.Method.String()
		}
	}
	return metrics.ResolveAuthContext(r)
}
