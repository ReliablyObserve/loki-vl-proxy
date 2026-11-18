package proxy

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/capability"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
)

// The capability layer's runtime: how the proxy learns the VictoriaLogs release (the /metrics endpoint, response
// headers) and what its probes observe, when it asks again, and how a change shows up (log line, flight key digest,
// metrics, the status route, the debug header). The decisions themselves live in internal/capability.
//
// Probe requests are the proxy's own: they use the backend client, its auth and TLS and its backend headers, and
// bypass the circuit breaker, the outage join and hold, the admission queue and the refusal prediction, and they are
// never counted in client or upstream request metrics. An answer that is not an explicit refusal (a timeout, a 5xx,
// a 401, a transport error, an open breaker) is no verdict, so an outage never flips a gate or moves the digest.

const (
	// DefaultBackendVersionReprobeInterval is how often the loop reads the release and probes the gates again.
	DefaultBackendVersionReprobeInterval = 5 * time.Minute
	// maxProbeBody bounds what a probe reads of an answer: a refusal message is a line.
	maxProbeBody = 16 << 10
	// maxMetricsProbeBytes bounds the /metrics read of the version probe.
	maxMetricsProbeBytes = 2 << 20
)

// minCapabilityLoopInterval is the shortest period the loop keeps, whatever -backend-version-reprobe-interval is
// (a variable so a test can shorten it).
var minCapabilityLoopInterval = 10 * time.Second

// backendVersionReprobeInterval spaces the metrics version probes that traffic triggers while the release is
// unknown, whatever the periodic interval is.
const backendVersionReprobeInterval = 5 * time.Minute

// maybeReprobeBackendVersion starts at most one background probe round per backendVersionReprobeInterval, bounded
// by the version check timeout, so a startup probe that failed (backend not ready, /metrics not routed) does not pin
// version-gated behaviour for the process lifetime. It never blocks the calling request.
func (p *Proxy) maybeReprobeBackendVersion(now time.Time) {
	last := p.backendVersionProbedAt.Load()
	if now.UnixNano()-last < int64(backendVersionReprobeInterval) || !p.backendVersionProbedAt.CompareAndSwap(last, now.UnixNano()) {
		return
	}
	p.startVersionProbe()
}

// startVersionProbe reads the release from /metrics in the background (a request that finds the release unknown
// triggers it, rate limited above); it stops with the proxy. Feature probes run at startup and in the loop only.
func (p *Proxy) startVersionProbe() {
	stop := p.keepWarmStop
	if stopped(stop) {
		return // shut down: no new goroutine joins the wait group
	}
	p.keepWarmWG.Add(1)
	go func() {
		defer p.keepWarmWG.Done()
		ctx, cancel := context.WithTimeout(context.Background(), p.probeRoundTimeout())
		defer cancel()
		if stop != nil {
			go func() {
				select {
				case <-stop:
					cancel()
				case <-ctx.Done():
				}
			}()
		}
		p.runCapabilityProbes(ctx, false)
	}()
}

func (p *Proxy) probeRoundTimeout() time.Duration {
	timeout := p.backendVersionCheckTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	// The release read and each gate's probe get their own timeout.
	return timeout * time.Duration(2+len(capability.Gates()))
}

func (p *Proxy) probeTimeout() time.Duration {
	if p.backendVersionCheckTimeout > 0 {
		return p.backendVersionCheckTimeout
	}
	return 5 * time.Second
}

// StartBackendCapabilityLoop starts the periodic re-probe (every -backend-version-reprobe-interval, jittered) that
// keeps the gate set in step with the backend through upgrades and rollbacks. It stops when the proxy shuts down. An
// interval of zero starts nothing. Calling it twice starts one loop.
func (p *Proxy) StartBackendCapabilityLoop() {
	if p == nil || p.backendReprobeInterval <= 0 || !p.capLoopStarted.CompareAndSwap(false, true) {
		return
	}
	stop := p.keepWarmStop
	if stopped(stop) {
		p.capLoopStarted.Store(false)
		return
	}
	p.keepWarmWG.Add(1)
	go func() {
		defer p.keepWarmWG.Done()
		interval := max(p.backendReprobeInterval, minCapabilityLoopInterval)
		timer := time.NewTimer(jittered(interval))
		defer timer.Stop()
		for {
			select {
			case <-stop:
				return
			case <-timer.C:
			}
			ctx, cancel := context.WithTimeout(context.Background(), p.probeRoundTimeout())
			go func() {
				select {
				case <-stop:
					cancel()
				case <-ctx.Done():
				}
			}()
			p.runCapabilityProbes(ctx, true)
			cancel()
			timer.Reset(jittered(interval))
		}
	}()
}

// stopped reports a closed stop channel.
func stopped(stop chan struct{}) bool {
	select {
	case <-stop:
		return true
	default:
		return false
	}
}

// jittered spreads the replicas' probes: d plus or minus 10%.
func jittered(d time.Duration) time.Duration {
	return d - d/10 + time.Duration(rand.Int64N(int64(d/5)+1))
}

// runCapabilityProbes is one round: read the release from /metrics, probe the gates that have a probe when features
// is set, and let the layer drop the releases that left its window. One round at a time per replica.
func (p *Proxy) runCapabilityProbes(ctx context.Context, features bool) {
	if !p.capProbing.CompareAndSwap(false, true) {
		return // single flight
	}
	defer p.capProbing.Store(false)
	p.probeBackendVersionFromMetrics(ctx)
	if features {
		p.probeBackendFeatures(ctx)
	}
	p.caps.Prune()
}

// backendProbeGet sends a probe request to the backend and reads at most limit bytes of the answer.
func (p *Proxy) backendProbeGet(ctx context.Context, path string, params url.Values, limit int64) (int, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, p.probeTimeout())
	defer cancel()
	u := backendRequestURL(p.backend, path)
	u.RawQuery = params.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return 0, nil, err
	}
	p.forwardTenantHeaders(req)
	p.applyBackendHeaders(req)
	resp, err := p.doBackendClient(p.client, req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	p.observeBackendVersionHeadersOnly(resp.Header)
	if err := decodeCompressedHTTPResponse(resp, limit); err != nil {
		return resp.StatusCode, nil, err
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, body, nil
}

// observeBackendVersionHeadersOnly reads a release out of a probe answer's headers without starting another probe.
func (p *Proxy) observeBackendVersionHeadersOnly(h http.Header) {
	for _, name := range [...]string{"Server", "X-App-Version", "X-Backend-Version", "X-Victoriametrics-Build"} {
		if v := h.Get(name); v != "" && p.caps.ObserveRaw(p.backendKey(), v, capability.SourceHeader) {
			return
		}
	}
}

// probeBackendVersionFromMetrics reads the release from the VictoriaLogs /metrics endpoint. Only a VictoriaLogs
// release counts: behind vmauth the endpoint is often vmauth's own.
func (p *Proxy) probeBackendVersionFromMetrics(ctx context.Context) {
	if p == nil {
		return
	}
	status, body, err := p.backendProbeGet(ctx, "/metrics", nil, maxMetricsProbeBytes)
	if err != nil {
		p.log.Debug("backend version metrics probe failed", "error", err)
		return
	}
	if status >= http.StatusBadRequest {
		p.log.Debug("backend version metrics probe returned non-success status", "http.response.status_code", status)
		return
	}
	rel, ok := capability.ParseMetrics(string(body))
	if !ok {
		p.log.Debug("backend version metrics probe found no VictoriaLogs release")
		return
	}
	p.caps.Observe(p.backendKey(), rel)
}

// probeBackendFeatures runs each gate's feature probe once and tells the layer the verdicts. Disabled by
// -backend-feature-probes=false.
func (p *Proxy) probeBackendFeatures(ctx context.Context) {
	if p == nil || p.backendFeatureProbesDisabled {
		return
	}
	for _, g := range capability.Gates() {
		if g.Probe == nil || g.Kind == capability.Safety {
			continue
		}
		if ctx.Err() != nil {
			return
		}
		verdict := capability.VerdictNone
		status, body, err := p.backendProbeGet(ctx, g.Probe.Path, g.Probe.Values(), maxProbeBody)
		if err == nil {
			verdict = g.Probe.Classify(status, body)
		} else if p.log.Enabled(ctx, slog.LevelDebug) {
			p.log.Debug("backend feature probe failed", "gate", g.Name, "error", err)
		}
		p.caps.ObserveProbe(g.ID, verdict)
	}
}

// onCapabilityChange is the layer's subscriber: it logs the change once, moves the flight key prefix to the new
// digest (a held failure is read only under the gate set it was answered in) and warns about an unsupported release.
func (p *Proxy) onCapabilityChange(old, nw capability.Set) {
	p.refreshFlightPrefix()
	flipped := capability.Flipped(old, nw)
	var on, off []string
	for _, id := range flipped {
		if nw.On(id) {
			on = append(on, id.Name())
		} else {
			off = append(off, id.Name())
		}
	}
	p.log.Info(
		"capability set changed",
		"capability.release.old", old.Release().String(),
		"capability.release.new", nw.Release().String(),
		"capability.release.raw", nw.Release().Raw,
		"capability.gates_on", on,
		"capability.gates_off", off,
		"capability.epoch", nw.Epoch(),
		"capability.digest", nw.Digest(),
		"backend.capability_profile", nw.Profile(),
		"service_name.derivation", serviceNameDerivationForm(nw.On(capability.PipeCoalesce)),
	)
	if r := nw.Release(); r.Known && !r.AtLeast(capability.ParseRelease("v1.49.0")) {
		// Independent of -backend-min-version: Helm values or an operator may carry an older floor, but the supported
		// lines start here.
		p.log.Warn(
			"backend version is below the supported VictoriaLogs lines",
			"backend.version.semver", r.String(),
			"minimum_supported_version", "v1.49.0",
			"note", "VictoriaLogs v1.49.0 and every v1.5x release are supported; this version is unsupported and untested",
		)
	}
	for _, id := range flipped {
		if nw.Why(id) != capability.ReasonMismatch {
			continue
		}
		p.log.Warn("capability mismatch", "gate", id.Name(), "release", nw.Release().String(), "probe_says", boolWord(nw.On(id)),
			"note", "a feature probe contradicts the release; the observed behaviour wins")
	}
}

func boolWord(on bool) string {
	if on {
		return "present"
	}
	return "absent"
}

// serviceNameDerivationForm names the LogsQL the proxy uses to derive service_name on this backend, so an operator
// can tell the two apart in the logs.
func serviceNameDerivationForm(coalesce bool) string {
	if coalesce {
		return "coalesce"
	}
	return "format"
}

// capabilityStats converts the layer's counters for the metrics exporters.
func (p *Proxy) capabilityStats() metrics.CapabilityStats {
	st := p.caps.Stats()
	out := metrics.CapabilityStats{Epoch: st.Epoch}
	for _, r := range st.Releases {
		if r.InWindow {
			out.Releases = append(out.Releases, metrics.CapabilityRelease{Version: r.Release, Flavour: r.Flavour, Source: r.Source})
		}
	}
	for i, g := range st.Gates {
		out.Gates = append(out.Gates, metrics.CapabilityGate{Gate: g.Name, Side: g.Side, Source: g.Source, On: g.On})
		out.Mismatches = append(out.Mismatches, metrics.CapabilityCount{Gate: g.Name, N: st.Mismatches[i]})
		for v := capability.VerdictNone; v <= capability.VerdictOff; v++ {
			out.Probes = append(out.Probes, metrics.CapabilityProbe{Gate: g.Name, Verdict: v.String(), N: st.Probes[i][v]})
		}
	}
	for _, c := range st.Clients {
		out.Clients = append(out.Clients, metrics.CapabilityClient{Surface: c.Surface, Grafana: c.Grafana, Datasource: c.Datasource, Drilldown: c.Drilldown, N: c.Requests})
	}
	return out
}

// handleCapabilities is GET /admin/capabilities (any other method is answered 405): every release seen, the effective one, every gate with what decided
// it, the mismatches and the epoch. It is a proxy-own route, protected like the other admin routes.
func (p *Proxy) handleCapabilities(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		p.fail(w, r, lokierr.MethodNotAllowed())
		return
	}
	st := p.caps.Status()
	st.Probing = !p.backendFeatureProbesDisabled
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(st)
}

// capabilitiesHeader is the debug header X-Loki-VL-Capabilities (only with -debug-vl-calls-header).
const capabilitiesHeader = "X-Loki-VL-Capabilities"

func (p *Proxy) setCapabilitiesHeader(ctx context.Context, h http.Header) {
	h.Set(capabilitiesHeader, strings.TrimSpace(p.caps.For(ctx).HeaderValue()))
}
