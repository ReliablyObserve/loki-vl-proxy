package main

import (
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/registry"
)

// Probe is one registry case as the runner sees it: either a request it can send to both sides, or the
// reason it cannot.
type Probe struct {
	ID       string
	Status   string // the case's gap.status; "" when the case has no gap block
	Consumer string
	Mode     string
	Fields   []string
	Values   map[string]any

	Method  string
	Path    string
	Params  map[string]string
	Headers map[string]string
	// ProxyFlags are the proxy flags the case needs (request.proxy_flags); Variant is the proxy variant chosen
	// for them ("" is the default proxy).
	ProxyFlags []string
	Variant    string

	// NotExecutable is set for a scenario case (scope and entry): there is no request to send.
	NotExecutable string
	// Blocked is set for a request the runner does not send: a precondition it cannot set up, a placeholder
	// time, a write, a stream, a request through Grafana.
	Blocked string
}

// Expectation is what the registry says about a case: a fixed case or a case without a gap must match
// Loki; an open case differs; a documented, owner-kept or upstream case differs on purpose.
func (p Probe) Expectation() string {
	switch p.Status {
	case "", "fixed":
		return "match"
	case "open":
		return "differ"
	}
	return "accepted"
}

// Endpoints the runner never sends: they write, stream, or answer through another API.
var notSent = map[string]string{
	"/loki/api/v1/push":   "a write; the runner only reads",
	"/loki/api/v1/delete": "a delete; the runner only reads",
	"/loki/api/v1/tail":   "a websocket stream",
	"/api/ds/query":       "a request through Grafana's datasource API",
}

// Probes turns the loaded cases into probes, sorted by id.
func Probes(cases []registry.Case) []Probe {
	out := make([]Probe, 0, len(cases))
	for _, c := range cases {
		out = append(out, probeOf(c))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func stringMap(v any) map[string]string {
	m, _ := v.(map[string]any)
	out := make(map[string]string, len(m))
	for k, e := range m {
		out[k] = fmt.Sprint(e)
	}
	return out
}

func probeOf(c registry.Case) Probe {
	doc := c.Doc
	p := Probe{ID: c.ID}
	p.Consumer, _ = doc["consumer"].(string)
	if gap, ok := doc["gap"].(map[string]any); ok {
		p.Status, _ = gap["status"].(string)
	}
	cmp, _ := doc["compare"].(map[string]any)
	p.Mode, _ = cmp["mode"].(string)
	if list, ok := cmp["fields"].([]any); ok {
		for _, f := range list {
			p.Fields = append(p.Fields, fmt.Sprint(f))
		}
	}
	p.Values, _ = cmp["values"].(map[string]any)
	req, _ := doc["request"].(map[string]any)
	p.Method, _ = req["method"].(string)
	p.Path, _ = req["path"].(string)
	p.Params = stringMap(req["params"])
	p.Headers = stringMap(req["headers"])
	if list, ok := req["proxy_flags"].([]any); ok {
		for _, f := range list {
			p.ProxyFlags = append(p.ProxyFlags, fmt.Sprint(f))
		}
	}
	switch {
	case p.Method == "" || p.Path == "":
		p.NotExecutable = "a scenario case (scope and entry), not an HTTP request"
	case req["precondition"] != nil:
		p.Blocked = "needs a precondition the runner does not set up"
	case req["setup"] != nil:
		p.Blocked = "needs a setup step the runner does not run"
	case notSent[p.Path] != "":
		p.Blocked = notSent[p.Path]
	case strings.Contains(p.Path, "//"):
		p.Blocked = "a malformed path the runner does not send"
	}
	return p
}

// relative reads now, now-<n><unit> and now+<n><unit> (units s, m, h, d, w).
func relative(value string, now time.Time) (time.Time, bool) {
	if value == "now" {
		return now, true
	}
	rest, ok := strings.CutPrefix(value, "now")
	if !ok || len(rest) < 3 || (rest[0] != '-' && rest[0] != '+') {
		return time.Time{}, false
	}
	unit := map[byte]time.Duration{'s': time.Second, 'm': time.Minute, 'h': time.Hour, 'd': 24 * time.Hour, 'w': 7 * 24 * time.Hour}[rest[len(rest)-1]]
	n, err := strconv.Atoi(rest[1 : len(rest)-1])
	if unit == 0 || err != nil || n < 0 {
		return time.Time{}, false
	}
	d := time.Duration(n) * unit
	if rest[0] == '-' {
		d = -d
	}
	return now.Add(d), true
}

// timeParams are the parameters that hold a time.
var timeParams = map[string]bool{"start": true, "end": true, "time": true}

// Query resolves the probe's parameters at now: a relative time becomes Unix nanoseconds, so both sides get
// the same bytes. A parameter that is a placeholder (<start>, a named anchor) blocks the probe.
func (p Probe) Query(now time.Time) (url.Values, string) {
	now = p.anchor(now)
	q := url.Values{}
	keys := make([]string, 0, len(p.Params))
	for k := range p.Params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := p.Params[k]
		if timeParams[k] {
			if t, ok := relative(v, now); ok {
				v = strconv.FormatInt(t.UnixNano(), 10)
			} else if !absoluteTime(v) {
				return nil, fmt.Sprintf("the %s parameter %q is a placeholder the runner cannot resolve", k, v)
			}
		}
		q.Set(k, v)
	}
	return q, ""
}

// anchor aligns the run's settled "now" down to the case's step, so the window ends on a step boundary and no
// step of it reads data younger than the settle lag.
func (p Probe) anchor(now time.Time) time.Time {
	step, ok := stepOf(p.Params["step"])
	if !ok {
		return now
	}
	ns := now.UnixNano()
	return time.Unix(0, ns-ns%int64(step)).UTC()
}

// stepOf reads a step as Loki does: a number of seconds or a duration.
func stepOf(v string) (time.Duration, bool) {
	if v == "" {
		return 0, false
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		d := time.Duration(f * float64(time.Second))
		return d, d > 0
	}
	if d, ok := logql.ParseUnitDuration(v); ok {
		return d, true
	}
	return 0, false
}

// Variant is a running proxy: its URL and the flags it runs with.
type Variant struct {
	Name  string   `json:"name"`
	URL   string   `json:"url"`
	Flags []string `json:"flags"`
}

// ChooseVariant picks the proxy for a probe that names proxy flags: the default when its flags hold them all,
// else the first variant that does; none blocks the probe.
func ChooseVariant(p *Probe, defaultFlags []string, variants []Variant) {
	if len(p.ProxyFlags) == 0 || p.NotExecutable != "" || p.Blocked != "" {
		return
	}
	holds := func(flags []string) bool {
		have := map[string]bool{}
		for _, f := range flags {
			have[f] = true
		}
		for _, f := range p.ProxyFlags {
			if !have[f] {
				return false
			}
		}
		return true
	}
	if holds(defaultFlags) {
		return
	}
	for _, v := range variants {
		if holds(v.Flags) {
			p.Variant = v.Name
			return
		}
	}
	p.Blocked = "needs a proxy variant with " + strings.Join(p.ProxyFlags, " ")
}

// absoluteTime is a Unix time (seconds or nanoseconds, a fraction allowed) or an RFC 3339 time, which Loki
// and the proxy both read.
func absoluteTime(v string) bool {
	if _, err := strconv.ParseFloat(v, 64); err == nil {
		return true
	}
	_, err := time.Parse(time.RFC3339Nano, v)
	return err == nil
}
