package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/capability"
)

// Where the VictoriaLogs release comes from besides the backend itself. Behind vmauth the backend's /metrics is
// vmauth's own, so the operator names another source: -backend-version-url (a VictoriaLogs /metrics endpoint, read
// instead of the backend's) or -backend-version (the release, written by hand). Neither is trusted on its own: the
// capability layer discards a named release a feature probe contradicts, and turns on only the gates a probe
// confirms (internal/capability, decide).
//
// The URL is operator configuration only. Its request carries nothing of a client request and nothing the proxy
// sends VictoriaLogs (no backend auth, headers or tenant): only -backend-version-basic-auth. Redirects are not
// followed, so that credential reaches no other address; the answer and the wait are bounded.

// maxProbeReason bounds the reason text of a failed version read: parts of it come from the server.
const maxProbeReason = 256

// versionSource is the parsed -backend-version-url with the client that reads it.
type versionSource struct {
	url    *url.URL
	auth   string // "user:password", sent to url only
	client *http.Client
}

// newVersionSource validates the flags that name the release's source: the version URL with its client (nil without
// -backend-version-url) and the declared release (not Known without -backend-version). Errors never repeat the URL
// or the credential.
func newVersionSource(cfg Config, transport *http.Transport) (*versionSource, capability.Release, error) {
	var declared capability.Release
	rawURL, name := strings.TrimSpace(cfg.BackendVersionURL), strings.TrimSpace(cfg.BackendVersion)
	if rawURL == "" && cfg.BackendVersionBasicAuth != "" {
		return nil, declared, errors.New("-backend-version-basic-auth needs -backend-version-url")
	}
	if rawURL == "" && name == "" {
		return nil, declared, nil
	}
	if cfg.BackendFeatureProbesDisabled {
		return nil, declared, errors.New("-backend-version-url and -backend-version need -backend-feature-probes=true: the probes check the release they name against the backend")
	}
	if name != "" {
		if declared = capability.ParseRelease(name); !declared.Known {
			return nil, declared, fmt.Errorf("invalid -backend-version %q: want a VictoriaLogs release such as v1.54.0", truncateText(name, 64))
		}
	}
	if rawURL == "" {
		return nil, declared, nil
	}
	u, err := url.Parse(rawURL)
	switch {
	case err != nil:
		return nil, declared, errors.New("invalid -backend-version-url: it is not a URL")
	case u.Scheme != "http" && u.Scheme != "https":
		return nil, declared, errors.New("invalid -backend-version-url: the scheme must be http or https")
	case u.Host == "":
		return nil, declared, errors.New("invalid -backend-version-url: it names no host")
	case u.User != nil:
		return nil, declared, errors.New("invalid -backend-version-url: put credentials in -backend-version-basic-auth, not in the URL")
	}
	t := transport.Clone()
	// A new connection per read: behind a load balancer each read then reaches any member, and the lowest release
	// of the window decides.
	t.DisableKeepAlives = true
	return &versionSource{url: u, auth: cfg.BackendVersionBasicAuth, client: &http.Client{
		Transport:     t,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, declared, nil
}

// fetchVersionMetrics reads at most limit bytes of the /metrics answer the release comes from.
func (p *Proxy) fetchVersionMetrics(ctx context.Context, limit int64) (int, []byte, error) {
	s := p.versionSource
	if s == nil {
		return p.backendProbeGet(ctx, "/metrics", nil, limit)
	}
	ctx, cancel := context.WithTimeout(ctx, p.probeTimeout())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url.String(), nil)
	if err != nil {
		return 0, nil, err
	}
	if user, password, _ := strings.Cut(s.auth, ":"); s.auth != "" {
		req.SetBasicAuth(user, password)
	}
	resp, err := s.client.Do(req) // #nosec G704 -- the URL is operator configuration (-backend-version-url), never request input
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	if err := decodeCompressedHTTPResponse(resp, limit); err != nil {
		return resp.StatusCode, nil, err
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	return resp.StatusCode, body, err
}

// probeBackendVersionFromMetrics reads the release from /metrics: the endpoint -backend-version-url names, else the
// backend's. Only a VictoriaLogs release counts: behind vmauth the backend's endpoint is often vmauth's own. A
// failed read changes nothing known; it returns why (empty on success), never with a URL or a credential.
func (p *Proxy) probeBackendVersionFromMetrics(ctx context.Context) string {
	if p == nil {
		return ""
	}
	reason := p.readBackendVersion(ctx)
	if reason != "" {
		p.log.Debug("backend version metrics probe failed", "reason", reason)
	}
	return reason
}

func (p *Proxy) readBackendVersion(ctx context.Context) string {
	endpoint := "metrics probe"
	if p.versionSource != nil {
		endpoint = "metrics probe of -backend-version-url"
	}
	// One byte past the bound tells a longer answer from one that ends at it.
	status, body, err := p.fetchVersionMetrics(ctx, maxMetricsProbeBytes+1)
	switch {
	case status >= http.StatusMultipleChoices && status < http.StatusBadRequest:
		return fmt.Sprintf("%s returned HTTP status %d (redirects are not followed)", endpoint, status)
	case status != 0 && (status < http.StatusOK || status >= http.StatusBadRequest):
		return fmt.Sprintf("%s returned HTTP status %d", endpoint, status)
	case err != nil:
		// The transport's error names the request URL, whose query may hold a key: only its cause is kept.
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return truncateText(fmt.Sprintf("%s failed: %v", endpoint, err), maxProbeReason)
	}
	longer := len(body) > maxMetricsProbeBytes
	if longer {
		body = body[:maxMetricsProbeBytes]
	}
	rel, ok := capability.ParseMetrics(string(body))
	if !ok {
		reason := fmt.Sprintf("%s found no VictoriaLogs release (%s)", endpoint, capability.DescribeMetrics(string(body)))
		if longer {
			reason += fmt.Sprintf("; only the first %d bytes of the answer are read", maxMetricsProbeBytes)
		}
		return reason
	}
	if p.versionSource != nil {
		rel.Source = capability.SourceVersionURL
	}
	p.caps.Observe(p.backendKey(), rel)
	return ""
}

func truncateText(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "")
}

// versionLog is what the version warnings last said, so a periodic round repeats none.
type versionLog struct {
	reason, named string
}

// warnVersionReadFailure logs why a periodic or traffic-triggered read found no release, once per distinct reason.
func (p *Proxy) warnVersionReadFailure(reason string) {
	if p.noteVersionReadReason(reason) && reason != "" {
		p.log.Warn("backend version read failed", "reason", reason)
	}
}

// noteVersionReadReason records the reason and reports whether it differs from the last one.
func (p *Proxy) noteVersionReadReason(reason string) bool {
	p.versionLogMu.Lock()
	defer p.versionLogMu.Unlock()
	changed := p.versionLogged.reason != reason
	p.versionLogged.reason = reason
	return changed
}

// warnNamedRelease logs, once per change, a named release (-backend-version, -backend-version-url) that a feature
// probe contradicts, one the probes show to be behind the backend, and a declared release that differs from the one
// read (the lower one decides). It returns the contradicted releases.
func (p *Proxy) warnNamedRelease() []string {
	status := p.caps.Status()
	var rejected, read []string
	declared := ""
	for _, r := range status.Releases {
		switch {
		case r.Rejected:
			rejected = append(rejected, r.Release+" ("+r.Source+")")
		case r.Source == capability.SourceDeclared.String():
			declared = r.Release
		case r.InWindow:
			read = append(read, r.Release)
		}
	}
	differs := false
	for _, r := range read {
		differs = differs || declared != "" && r != declared
	}
	state := strings.Join(rejected, ",")
	if differs {
		state += "|" + declared + "|" + strings.Join(read, ",")
	}
	if status.NamedBehind != "" {
		state += "|behind " + status.NamedBehind
	}
	p.versionLogMu.Lock()
	changed := p.versionLogged.named != state
	p.versionLogged.named = state
	p.versionLogMu.Unlock()
	if !changed {
		return rejected
	}
	if len(rejected) > 0 {
		p.log.Warn("named backend version contradicted by a feature probe; it is not used",
			"backend.version.rejected", rejected, "gates", status.Mismatches)
	}
	if differs {
		p.log.Warn("declared backend version differs from the version read; the lower one decides the gates",
			"backend.version.declared", declared, "backend.version.read", read, "backend.version.effective", status.Effective)
	}
	if status.NamedBehind != "" {
		p.log.Warn("the feature probes show a VictoriaLogs newer than the named release; the gates above it are off, update it",
			"backend.version.named", status.NamedBehind)
	}
	return rejected
}
