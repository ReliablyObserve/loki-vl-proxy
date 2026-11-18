package proxy

import (
	"net/http"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/capability"
)

// setReleaseForTest replaces what the proxy knows of its backend by exactly release ("" leaves nothing known): a
// fresh capability layer wired like the proxy's own, then the release observed. Tests call it before they serve.
func (p *Proxy) setReleaseForTest(release string) {
	p.caps = capability.New(capability.Options{})
	p.caps.SetWindow(capabilityWindowIntervals*p.backendReprobeInterval + p.probeRoundTimeout())
	p.caps.Subscribe(p.onCapabilityChange)
	p.refreshFlightPrefix()
	if release != "" {
		p.storeBackendVersion("test", release)
	}
}

// isCapabilityProbe reports one of the proxy's feature probes (the capability table's): a request a test backend that
// counts or checks the reads of a scenario passes on without counting.
func isCapabilityProbe(r *http.Request) bool {
	_ = r.ParseForm()
	for _, g := range capability.Gates() {
		if g.Probe != nil && r.URL.Path == g.Probe.Path && r.FormValue("query") == g.Probe.Query {
			return true
		}
	}
	return false
}

// storeBackendVersion records a release the caller trusts to be VictoriaLogs's (an operator-named or test-named
// version; what a backend reports goes through capability.ParseBuild). source is a label of where it came from.
func (p *Proxy) storeBackendVersion(source, semver string) {
	r := capability.ParseRelease(semver)
	if !r.Known {
		return
	}
	r.Raw = source
	if strings.HasPrefix(source, "metrics:") {
		r.Source = capability.SourceMetrics
	}
	p.caps.Observe(p.backendKey(), r)
}
