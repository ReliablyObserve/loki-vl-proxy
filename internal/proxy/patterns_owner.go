package proxy

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"time"
)

// patternsForwardedHeader marks a /patterns request a peer forwarded to the pattern state's owner: the owner
// answers it from its own state, never forwarding it again. It counts only with the peers' token
// (X-Peer-Token).
const patternsForwardedHeader = "X-Loki-VL-Patterns-Forwarded"

// patternsOwnerTimeout bounds a forwarded /patterns request: the owner may build a state on it.
const patternsOwnerTimeout = 30 * time.Second

// patternStateKey names the pattern state of a request's tenant and selector across the fleet: its owner and
// replica in the peers' ring hold the state, so every peer answers the selector with the same patterns.
func (p *Proxy) patternStateKey(r *http.Request, scope patternQueryScope) string {
	sum := sha256.Sum256([]byte(p.patternStateTenantKey(r) + "\x00" + scope.filter.String()))
	return "patterns_state:" + hex.EncodeToString(sum[:])
}

// patternsForwardedToUs reports whether a peer forwarded the request to this proxy as the state's owner.
func (p *Proxy) patternsForwardedToUs(r *http.Request) bool {
	if r.Header.Get(patternsForwardedHeader) == "" || p.peerAuthToken == "" {
		return false
	}
	got := r.Header.Get("X-Peer-Token")
	return subtle.ConstantTimeEq(int32(len(got)), int32(len(p.peerAuthToken))) == 1 &&
		subtle.ConstantTimeCompare([]byte(got), []byte(p.peerAuthToken)) == 1
}

// patternsFromOwner answers a /patterns request from the peer that holds the selector's pattern state: the key's
// owner, then its replica, in ring order. The request goes to the peer's /loki/api/v1/patterns with the client's
// headers, so the peer reads it with the client's identity through its own middleware, and the forward mark, so
// it answers from its own state. ok is false when this proxy is the first of them that answers (it answers from
// its own state), the request was forwarded to it, there are no peers, or every peer before it fails: the
// caller answers locally.
func (p *Proxy) patternsFromOwner(r *http.Request, scope patternQueryScope) ([]patternResultEntry, bool) {
	if p.peerCache == nil || p.peerAuthToken == "" || p.patternsForwardedToUs(r) {
		return nil, false
	}
	self := p.peerCache.SelfAddr()
	key := p.patternStateKey(r, scope)
	for _, peer := range p.peerCache.Replicas(key) {
		if peer == self {
			return nil, false
		}
		started := time.Now()
		// The forward may wait patternsOwnerTimeout while the owner builds the state, so a peer that does not
		// answer a presence check within the peer timeout (stopped, frozen, unreachable) is skipped first.
		if !p.peerCache.Reachable(peer, key) {
			p.observeInternalOperation(r.Context(), "patterns_owner", "peer_unreachable", time.Since(started))
			continue
		}
		entries, err := p.forwardPatterns(r, peer)
		if err == nil {
			p.observeInternalOperation(r.Context(), "patterns_owner", "forwarded", time.Since(started))
			return entries, true
		}
		p.observeInternalOperation(r.Context(), "patterns_owner", "peer_failed", time.Since(started))
		p.log.Debug("patterns owner forward failed", "peer", peer, "error", err)
	}
	return nil, false
}

// forwardPatterns sends the client's /patterns request to a peer and reads its answer.
func (p *Proxy) forwardPatterns(r *http.Request, peer string) ([]patternResultEntry, error) {
	ctx, cancel := context.WithTimeout(r.Context(), patternsOwnerTimeout)
	defer cancel()
	target := url.URL{Scheme: "http", Host: peer, Path: "/loki/api/v1/patterns", RawQuery: r.URL.RawQuery}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, err
	}
	for name, values := range r.Header {
		switch http.CanonicalHeaderKey(name) {
		case "Connection", "Keep-Alive", "Transfer-Encoding", "Te", "Trailer", "Upgrade", "Proxy-Authorization", "Accept-Encoding", "Content-Length":
			continue
		}
		req.Header[name] = append([]string(nil), values...)
	}
	req.Header.Set("X-Peer-Token", p.peerAuthToken)
	req.Header.Set(patternsForwardedHeader, "1")
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil, errPatternCounts{"peer status " + http.StatusText(resp.StatusCode)}
	}
	body, err := readBodyLimited(resp.Body, maxPatternsPeerSnapshotBytes)
	if err != nil {
		return nil, err
	}
	var answer struct {
		Status string               `json:"status"`
		Data   []patternResultEntry `json:"data"`
	}
	if err := json.Unmarshal(body, &answer); err != nil {
		return nil, err
	}
	if answer.Status != "success" {
		return nil, errPatternCounts{"peer answered " + answer.Status}
	}
	return answer.Data, nil
}
