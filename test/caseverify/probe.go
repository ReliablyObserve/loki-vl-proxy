package main

import (
	"fmt"
	"net/url"
	"regexp"
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
	// EmptyIsAnswer is why two empty answers prove the case (request.verify.empty_is_answer): the case is
	// that a query the proxy once mishandled is answered like Loki, with nothing and no error.
	EmptyIsAnswer string
	// EmptyRange is why the case's range holds no data on purpose (request.verify.empty_range): the case is the
	// answer to a range without data, so two empty counts are the data the case needs, not a verdict of `empty`.
	EmptyRange string
	// Control is a second request of the case (request.verify.control: params laid over the request's own) that
	// must return data on both sides. Where the case's own answers are both empty, a control that answers data on
	// both sides proves the data is there, and the two empty answers match; the control's own answers are not
	// compared.
	Control map[string]string

	// OwnerKept are the owner-kept deviation cases this case's answers are subject to (the case's owner_kept).
	OwnerKept []string

	// Blocked is set for a request the runner does not send: a precondition it cannot set up, a placeholder
	// time, a write, a stream, a request through Grafana. BlockedClass names the kind of block, so the daily
	// report counts the blocked cases by what they need (see the class constants).
	Blocked      string
	BlockedClass string
}

// Classes of blocked cases. The first group is set by the case (request.verify.blocked_by) or by its path, the
// second by what the answers showed.
const (
	classLoad         = "load"         // needs admission slots, queues or concurrent clients held
	classFault        = "fault"        // needs a backend failure injected
	classVolume       = "volume"       // needs more data or series than the stack holds, or a limit the stack does not run
	classVersion      = "version"      // needs another VictoriaLogs version
	classFixture      = "fixture"      // needs data the stack does not hold
	classProxyOnly    = "proxy_only"   // the expectation is the proxy's own behaviour or log, which Loki does not answer
	classMovedWindow  = "moved_window" // needs a second ask with a moved window and compares the two asks
	classWrite        = "write"
	classWebsocket    = "websocket"
	classGrafana      = "grafana"
	classPlaceholder  = "placeholder_time"
	classUnclassed    = "unclassified"
	classEmptyAnswer  = "empty_answer"  // both sides answered, with nothing to compare
	classNoAnswer     = "no_answer"     // a side did not answer, or answered more than the runner reads
	classLokiError    = "loki_error"    // Loki failed on a case that does not compare errors
	classUnreadFields = "unread_fields" // both answers are empty on what the runner reads; the case names fields it does not
	classNoVariant    = "no_variant"    // the case names proxy flags no running proxy has
)

// blockedByClasses are the classes a case may name in request.verify.blocked_by.
var blockedByClasses = map[string]bool{classLoad: true, classFault: true, classVolume: true, classVersion: true,
	classFixture: true, classProxyOnly: true, classMovedWindow: true, classWrite: true}

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
var notSent = map[string]struct{ reason, class string }{
	"/loki/api/v1/push":   {"a write; the runner only reads", classWrite},
	"/loki/api/v1/delete": {"a delete route; the case's expectation is the proxy's default (-delete-api-enabled unset), not what this stack's Loki answers", classProxyOnly},
	"/loki/api/v1/tail":   {"a websocket stream", classWebsocket},
	"/api/ds/query":       {"a request through Grafana's datasource API", classGrafana},
}

// Probes turns the loaded cases into probes, sorted by id.
func Probes(cases []registry.Case) []Probe {
	out := make([]Probe, 0, len(cases))
	ownerKept := map[string]bool{}
	for _, c := range cases {
		if gap, ok := c.Doc["gap"].(map[string]any); ok && gap["status"] == "owner-kept" {
			ownerKept[c.ID] = true
		}
		out = append(out, probeOf(c))
	}
	// A case may name only owner-kept cases: an annotation that points nowhere would excuse a difference
	// without saying why.
	for i := range out {
		for _, id := range out[i].OwnerKept {
			if !ownerKept[id] && out[i].Blocked == "" && out[i].NotExecutable == "" {
				out[i].Blocked = fmt.Sprintf("owner_kept names %q, which is not an owner-kept case", id)
			}
		}
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
	if list, ok := doc["owner_kept"].([]any); ok {
		for _, id := range list {
			p.OwnerKept = append(p.OwnerKept, fmt.Sprint(id))
		}
	}
	verify, _ := req["verify"].(map[string]any)
	metBy, _ := verify["met_by"].(string)
	evidence, _ := verify["evidence"].(string)
	blockedBy, _ := verify["blocked_by"].(string)
	reason, _ := verify["reason"].(string)
	p.EmptyIsAnswer, _ = verify["empty_is_answer"].(string)
	p.EmptyRange, _ = verify["empty_range"].(string)
	badControl := false
	if c, ok := verify["control"].(map[string]any); ok {
		p.Control = stringMap(c["params"])
		badControl = len(p.Control) == 0
	}
	block := func(class, reason string) {
		p.Blocked, p.BlockedClass = reason, class
	}
	// What the request needs that the runner does not set up: the case names it (verify.blocked_by) or, for a
	// precondition the stack's standing data meets, says so with its evidence (verify.met_by).
	unmet := ""
	switch {
	case req["setup"] != nil:
		unmet = "needs a setup step the runner does not run"
	case req["precondition"] != nil && (metBy != "stack" || evidence == ""):
		unmet = "needs a precondition the runner does not set up"
	}
	switch {
	case p.Method == "" || p.Path == "":
		p.NotExecutable = "a scenario case (scope and entry), not an HTTP request"
	case verify != nil && (metBy != "" && blockedBy != "" || metBy != "" && metBy != "stack" || blockedBy != "" && !blockedByClasses[blockedBy]):
		block(classUnclassed, fmt.Sprintf("request.verify is not valid (met_by %q, blocked_by %q)", metBy, blockedBy))
	case metBy != "" && evidence == "":
		block(classUnclassed, "request.verify.met_by needs its evidence")
	case badControl:
		block(classUnclassed, "request.verify.control needs params")
	case blockedBy != "":
		if reason == "" {
			reason = unmet
		}
		if reason == "" {
			reason = "needs " + blockedBy
		}
		block(blockedBy, reason)
	case unmet != "":
		block(classUnclassed, unmet)
	case notSent[p.Path].reason != "":
		block(notSent[p.Path].class, notSent[p.Path].reason)
	}
	return p
}

// withParams is the probe with the parameters laid over its own: the control request of a case.
func (p Probe) withParams(over map[string]string) Probe {
	params := make(map[string]string, len(p.Params)+len(over))
	for k, v := range p.Params {
		params[k] = v
	}
	for k, v := range over {
		params[k] = v
	}
	p.Params = params
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
// the same bytes. A placeholder time (<start>, <start+30m>, historical-aligned-hour-minus-2h) is resolved against
// the settled end (see placeholders); a parameter that is any other placeholder (<a row time in ns>) blocks the probe.
func (p Probe) Query(now time.Time) (url.Values, string) {
	now = p.anchor(now)
	ph := p.placeholders(now)
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
			} else if t, ok := ph.resolve(v); ok {
				v = strconv.FormatInt(t.UnixNano(), 10)
			} else if !absoluteTime(v) {
				return nil, fmt.Sprintf("the %s parameter %q is a placeholder the runner cannot resolve", k, v)
			}
		}
		q.Set(k, v)
	}
	return q, ""
}

// placeholderSpan is the window a case that names only its start gets: a case's own fixture decides what is in it,
// the runner only has to give both sides the same one.
const placeholderSpan = 30 * time.Minute

// Placeholder times of the registry's cases, resolved so the window the largest of them names ends on the settled
// end (now, already aligned to the step): the run never reads data younger than the settle lag, and both sides
// get the same bytes.
//
//	<start>, <start+N>                  a start S and S plus N (units s, m, h, d, w); S is the settled end less the
//	                                    largest N named by the request (at least placeholderSpan), on the step
//	historical-aligned-hour[-minus-Nh|-plus-Nh]
//	                                    an hour boundary H and H less or plus N hours; H is the newest boundary for
//	                                    which H plus the largest plus named by the request is not past the settled end
type placeholderTimes struct {
	start, hour time.Time
}

var (
	startPlaceholder = regexp.MustCompile(`^<start(?:\+(\d+)([smhdw]))?>$`)
	hourPlaceholder  = regexp.MustCompile(`^historical-aligned-hour(?:-(minus|plus)-(\d+)h)?$`)
)

var placeholderUnits = map[string]time.Duration{"s": time.Second, "m": time.Minute, "h": time.Hour, "d": 24 * time.Hour, "w": 7 * 24 * time.Hour}

func (p Probe) placeholders(now time.Time) placeholderTimes {
	offset, named, plus := time.Duration(0), false, time.Duration(0)
	for k := range timeParams {
		v := p.Params[k]
		if m := startPlaceholder.FindStringSubmatch(v); m != nil && m[1] != "" {
			n, err := strconv.Atoi(m[1])
			if err == nil {
				offset, named = max(offset, time.Duration(n)*placeholderUnits[m[2]]), true
			}
		}
		if m := hourPlaceholder.FindStringSubmatch(v); m != nil && m[1] == "plus" {
			if n, err := strconv.Atoi(m[2]); err == nil {
				plus = max(plus, time.Duration(n)*time.Hour)
			}
		}
	}
	if !named {
		offset = placeholderSpan
	}
	start := now.Add(-offset)
	if step, ok := stepOf(p.Params["step"]); ok {
		ns := start.UnixNano()
		start = time.Unix(0, ns-ns%int64(step)).UTC()
	}
	return placeholderTimes{start: start, hour: now.Add(-plus).Truncate(time.Hour)}
}

// resolve reads a placeholder time; false when v is not one the runner knows.
func (t placeholderTimes) resolve(v string) (time.Time, bool) {
	if m := startPlaceholder.FindStringSubmatch(v); m != nil {
		if m[1] == "" {
			return t.start, true
		}
		n, err := strconv.Atoi(m[1])
		if err != nil {
			return time.Time{}, false
		}
		return t.start.Add(time.Duration(n) * placeholderUnits[m[2]]), true
	}
	if m := hourPlaceholder.FindStringSubmatch(v); m != nil {
		if m[1] == "" {
			return t.hour, true
		}
		n, err := strconv.Atoi(m[2])
		if err != nil {
			return time.Time{}, false
		}
		d := time.Duration(n) * time.Hour
		if m[1] == "minus" {
			d = -d
		}
		return t.hour.Add(d), true
	}
	return time.Time{}, false
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

// lokiProfileVariant is the proxy variant that runs the Loki-compatible profile with no extension flags (the
// stack's underscore variant, the one Grafana's proxy datasource reads). Several variants may hold the flags a case
// names; this one is the reference they are measured against, so it is preferred.
const lokiProfileVariant = "underscore"

// defaultOff are the boolean flags that are off when a proxy does not list them (their declared default is false,
// cmd/proxy/main.go; TestDefaultOffFlagsAreDeclaredOff keeps the list honest). A case that names one with =false
// (say -label-values-indexed-cache=false, the Loki profile without the main proxy's browse-keeping index) is held
// by a proxy that lists it as false or does not list it.
var defaultOff = map[string]bool{"-label-values-indexed-cache": true}

// holdsFlags reports whether a proxy running with flags has every flag a case names.
func holdsFlags(flags, wanted []string) bool {
	have := map[string]bool{}
	listed := map[string]bool{}
	for _, f := range flags {
		have[f] = true
		name, _, _ := strings.Cut(f, "=")
		listed[name] = true
	}
	for _, f := range wanted {
		name, value, ok := strings.Cut(f, "=")
		off := ok && (value == "false" || value == "0" || value == "off")
		unlistedOff := off && defaultOff[name] && !listed[name]
		if !have[f] && !unlistedOff {
			return false
		}
	}
	return true
}

// ChooseVariant picks the proxy for a probe that names proxy flags: the default when its flags hold them all,
// else the Loki-profile variant if it does, else the first variant that does; none blocks the probe.
func ChooseVariant(p *Probe, defaultFlags []string, variants []Variant) {
	if len(p.ProxyFlags) == 0 || p.NotExecutable != "" || p.Blocked != "" {
		return
	}
	if holdsFlags(defaultFlags, p.ProxyFlags) {
		return
	}
	chosen := ""
	for _, v := range variants {
		if holdsFlags(v.Flags, p.ProxyFlags) && (chosen == "" || v.Name == lokiProfileVariant) {
			chosen = v.Name
		}
	}
	if chosen != "" {
		p.Variant = chosen
		return
	}
	p.Blocked, p.BlockedClass = "needs a proxy variant with "+strings.Join(p.ProxyFlags, " "), classNoVariant
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
