package proxy

import (
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/capability"
)

// setReleaseForTest replaces what the proxy knows of its backend by exactly release ("" leaves nothing known): a
// fresh capability layer wired like the proxy's own, then the release observed. Tests call it before they serve.
func (p *Proxy) setReleaseForTest(release string) {
	p.caps = capability.New(capability.Options{})
	p.caps.SetWindow(2*p.backendReprobeInterval + p.probeRoundTimeout())
	p.caps.Subscribe(p.onCapabilityChange)
	p.refreshFlightPrefix()
	if release != "" {
		p.storeBackendVersion("test", release)
	}
}
