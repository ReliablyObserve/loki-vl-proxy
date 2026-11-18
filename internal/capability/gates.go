package capability

import (
	"net/url"
	"strconv"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql/probes"
)

// Kind says how a gate behaves when the proxy does not know whether the backend has what it names.
type Kind uint8

const (
	// Capability is a feature used when present: unknown means off, and what a probe observed wins over the version.
	Capability Kind = iota
	// Safety guards a crash or a wrong answer: an unknown release, an unknown flavour or a disagreement between the
	// release and a probe all mean guarded, and the more conservative reading wins. It has no probe (a crash cannot
	// be probed without crashing the backend) and is decided from the release alone. "On" means the release carries
	// the fix; off means the proxy must not send what the fix is needed for.
	Safety
	// Behaviour is a fact about how the backend answers (a refusal it gives from some release on): unknown means
	// undecided, never assumed either way. A prediction that reads it fires only when it is decided.
	Behaviour
)

var kindLabels = [...]string{"capability", "safety", "behaviour"}

// String is the kind's label.
func (k Kind) String() string { return kindLabels[k] }

// Side says whose behaviour a gate describes.
type Side uint8

const (
	// SideVictoriaLogs gates read the backend release(s).
	SideVictoriaLogs Side = iota
	// SideLoki gates read the Loki compatibility target (a hook: no such input exists yet).
	SideLoki
	// SideClient gates read the request's client profile (a hook: evaluated per request, never stored).
	SideClient
)

var sideLabels = [...]string{"victorialogs", "loki", "client"}

// String is the side's label.
func (s Side) String() string { return sideLabels[s] }

// GateID names a gate in the table; the same index is the bit of the gate in a Set.
type GateID uint8

// The gates. The table (gates below) holds each one's name, release and registry item.
const (
	DensePatternWindowing GateID = iota
	PipeCoalesce
	ColumnFieldValues
	StreamMetadataEndpoints
	StatsLimitAfterStats
	PipeJSONArrayConcat
	RangeTimeWritersRefused
	MathQuotedConstantClone
	ExtraFiltersLen
	numGates
)

// Patch is a release line that got a gate as a patch release (an LTS fix).
type Patch struct {
	Patch    int        // first patch of the line with the gate
	Flavours FlavourSet // builds of the line that carry it (zero: every build)
}

// Gate declares one capability: where it starts, the LTS lines that back-ported it, how to observe it and why.
type Gate struct {
	ID       GateID
	Name     string // vl.pipe_coalesce
	Kind     Kind
	Side     Side
	From     [3]int           // first release with it
	Lines    map[[2]int]Patch // release lines that got it in a patch
	Flavours FlavourSet       // builds that carry it (zero: every build)
	Probe    *Probe           // optional feature probe; never set on a Safety gate
	Registry string           // conformance/registry/vl/capabilities/<Registry>.yaml
	Citation string           // VictoriaLogs CHANGELOG or issue
	Default  bool             // the value when nothing is known (false for every gate after the v1.49.0 floor)
	// Reads says what in the proxy reads the gate, for the generated reference.
	Reads string
}

// In reports whether release r carries the gate: from the first release on, or, on an older line, from the first
// patch Lines names for it, and only on a build whose flavour the gate (or the line) lists. A release that is not
// Known carries nothing.
func (g Gate) In(r Release) bool {
	if !r.Known || !g.Flavours.Has(r.Flavour) {
		return false
	}
	if cmp3(r.Major, r.Minor, r.Patch, g.From) >= 0 {
		return true
	}
	if p, ok := g.Lines[[2]int{r.Major, r.Minor}]; ok {
		return r.Patch >= p.Patch && p.Flavours.Has(r.Flavour)
	}
	return false
}

func cmp3(maj, min, patch int, w [3]int) int {
	switch {
	case maj != w[0]:
		return maj - w[0]
	case min != w[1]:
		return min - w[1]
	}
	return patch - w[2]
}

// FromString is the first release as "v1.51.0".
func (g Gate) FromString() string {
	return "v" + strconv.Itoa(g.From[0]) + "." + strconv.Itoa(g.From[1]) + "." + strconv.Itoa(g.From[2])
}

// Verdict is what a probe says about a gate.
type Verdict uint8

const (
	// VerdictNone is no answer: a timeout, a 5xx, an authentication failure, a transport error. It keeps the
	// previous verdict, so an outage never flips a gate.
	VerdictNone Verdict = iota
	VerdictOn
	VerdictOff
)

var verdictLabels = [...]string{"inconclusive", "on", "off"}

// String is the verdict's label.
func (v Verdict) String() string { return verdictLabels[v] }

// Probe is a harmless request that tells "has it" from "has it not" without a version.
type Probe struct {
	Path  string // /select/logsql/query (never a write path)
	Query string // the LogsQL of a query path, or the query arg of a metadata path
	// Step is the step arg of stats_query_range; Time marks stats_query, which takes a time arg.
	Step string
	// Accepted is what a 2xx answer says: VerdictOn for a feature the backend runs, VerdictOff for a behaviour whose
	// presence is a refusal.
	Accepted Verdict
	// Refused is what a refusal says (the opposite of Accepted).
	Refused Verdict
	// RefusalStatus lists the statuses a refusal comes with, and RefusalText the texts of its message; an empty text
	// matches any message (a path that does not exist). Only a refusal in both is a verdict.
	RefusalStatus []int
	RefusalText   []string
	// CrashSafe names the releases whose oracle proved the probe does not crash VictoriaLogs (TestProbesCrashSafe).
	CrashSafe [2]string
}

// Values are the args of the probe's request: the fixed one-second window of the probes package (long before any
// retention: the probe reads no stored row), one row at most.
func (p *Probe) Values() url.Values {
	v := url.Values{}
	v.Set("query", p.Query)
	v.Set("start", probes.WindowStart)
	v.Set("end", probes.WindowEnd)
	switch p.Path {
	case pathStatsQuery:
		v.Set("time", probes.WindowEnd)
	case pathStatsQueryRange:
		v.Set("step", p.Step)
	case pathQuery:
		v.Set("limit", "1")
	}
	return v
}

// Classify reads the probe's answer. Only a success and an explicit refusal (a status the probe lists with a message
// naming the refusal) are verdicts; every other answer is none.
func (p *Probe) Classify(status int, body []byte) Verdict {
	if status >= 200 && status < 300 {
		return p.Accepted
	}
	listed := false
	for _, s := range p.RefusalStatus {
		if s == status {
			listed = true
			break
		}
	}
	if !listed {
		return VerdictNone
	}
	for _, text := range p.RefusalText {
		if containsFold(body, text) {
			return p.Refused
		}
	}
	return VerdictNone
}

func containsFold(body []byte, text string) bool {
	if text == "" {
		return true
	}
	if len(body) > 4096 {
		body = body[:4096]
	}
	n := len(text)
	for i := 0; i+n <= len(body); i++ {
		match := true
		for j := 0; j < n; j++ {
			a, b := body[i+j], text[j]
			if a >= 'A' && a <= 'Z' {
				a += 'a' - 'A'
			}
			if b >= 'A' && b <= 'Z' {
				b += 'a' - 'A'
			}
			if a != b {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// The VictoriaLogs paths a probe may use: reads only.
const (
	pathQuery           = "/select/logsql/query"
	pathStatsQuery      = "/select/logsql/stats_query"
	pathStatsQueryRange = "/select/logsql/stats_query_range"
	pathStreamFieldName = "/select/logsql/stream_field_names"
)

// gates is the one table of every input that turns an optimisation or a behaviour on or off. Everything else (the
// lowering, the error prediction, the handlers) asks a Set for a gate; nothing compares release strings.
var gates = [numGates]Gate{
	DensePatternWindowing: {
		Name: "vl.dense_pattern_windowing", Kind: Capability, From: [3]int{1, 50, 0},
		Registry: "dense-pattern-windowing", Citation: "VictoriaLogs v1.50.0 (stream fields in the column index)",
		Reads: "the patterns endpoint: dense windowing of the log sample (patterns.go)",
	},
	PipeCoalesce: {
		Name: "vl.pipe_coalesce", Kind: Capability, From: [3]int{1, 51, 0},
		Registry: "pipe-coalesce", Citation: "VictoriaLogs v1.51.0 (#690)",
		Probe: &Probe{Path: pathQuery, Query: probes.Coalesce, Accepted: VerdictOn, Refused: VerdictOff,
			RefusalStatus: []int{400}, RefusalText: []string{`unexpected pipe "coalesce"`}, CrashSafe: [2]string{"v1.49.0", "v1.54.0"}},
		Reads: "the service_name derivation (one coalesce or a format chain) and the refusal prediction",
	},
	ColumnFieldValues: {
		Name: "vl.column_field_values", Kind: Capability, From: [3]int{1, 50, 0},
		Registry: "column-field-values", Citation: "VictoriaLogs v1.50.0 (stream fields in the column index)",
		Reads: "label value lookups: field_values replaces stream_field_values (drilldown.go)",
	},
	StreamMetadataEndpoints: {
		Name: "vl.stream_metadata_endpoints", Kind: Capability, From: [3]int{1, 30, 0}, Default: true,
		Registry: "stream-metadata-endpoints", Citation: "VictoriaLogs v1.30.0 (stream_field_names, stream_field_values)",
		Probe: &Probe{Path: pathStreamFieldName, Query: probes.StreamFieldNames, Accepted: VerdictOn, Refused: VerdictOff,
			RefusalStatus: []int{400, 404}, RefusalText: []string{"unsupported path requested"}, CrashSafe: [2]string{"v1.49.0", "v1.54.0"}},
		Reads: "label and series metadata: the stream endpoints are tried first (label_metadata.go, label_handlers.go)",
	},
	StatsLimitAfterStats: {
		Name: "vl.stats_limit_after_stats", Kind: Capability, From: [3]int{1, 51, 0},
		Registry: "vl-stats-pipe-after-stats", Citation: "VictoriaLogs v1.51.0 (#1296)",
		Probe: &Probe{Path: pathStatsQuery, Query: probes.StatsLimitAfterStats, Accepted: VerdictOn, Refused: VerdictOff,
			RefusalStatus: []int{400, 422}, RefusalText: []string{"cannot be put after"}, CrashSafe: [2]string{"v1.49.0", "v1.54.0"}},
		Reads: "the refusal prediction of a limit or offset pipe after the last stats pipe",
	},
	PipeJSONArrayConcat: {
		Name: "vl.pipe_json_array_concat", Kind: Capability, From: [3]int{1, 52, 0},
		Registry: "vl-pipe-before-release", Citation: "VictoriaLogs v1.52.0 (#712)",
		Reads: "nothing: documentation only (v1.49 and v1.50 read the pipe as a filter and answer it, so it is never predicted)",
	},
	RangeTimeWritersRefused: {
		Name: "vl.range_time_writers_refused", Kind: Behaviour, From: [3]int{1, 53, 0},
		Registry: "vl-stats-range-pipe-before-stats", Citation: "VictoriaLogs v1.53.0 (canReturnLastNResults checks)",
		Probe: &Probe{Path: pathStatsQueryRange, Query: probes.RangeTimeWriter, Step: "1s", Accepted: VerdictOff, Refused: VerdictOn,
			RefusalStatus: []int{400, 422}, RefusalText: []string{"cannot be put in front of"}, CrashSafe: [2]string{"v1.49.0", "v1.54.0"}},
		Reads: "the refusal prediction of a stats_query_range pipe that writes _time ahead of the last stats pipe",
	},
	MathQuotedConstantClone: {
		Name: "vl.math_quoted_constant_clone", Kind: Safety, From: [3]int{1, 53, 0},
		Registry: "vl-math-quoted-constant-clone-crash", Citation: "VictoriaLogs v1.53.0 (math quoted constants keep their quotes when printed)",
		Reads: "the crash guard: a math pipe with a quoted number is never sent to a release without the fix (the v1.52 line stays guarded: its fixed releases are enterprise-only images)",
	},
	ExtraFiltersLen: {
		Name: "vl.extra_filters_len", Kind: Behaviour, From: [3]int{1, 54, 0},
		Registry: "vl-extra-filters-too-long", Citation: "VictoriaLogs v1.54.0 (#1923)",
		Reads: "the refusal prediction of extra_filters and extra_stream_filters longer than -search.maxQueryLen",
	},
}

// init stamps each table row with its index, so a row cannot name a different gate than its position.
func init() {
	for i := range gates {
		gates[i].ID = GateID(i)
	}
}

// Gates returns the table (a copy: the table never changes at run time).
func Gates() []Gate {
	out := make([]Gate, len(gates))
	copy(out, gates[:])
	return out
}

// Lookup returns the gate of a table name ("vl.pipe_coalesce").
func Lookup(name string) (Gate, bool) {
	for _, g := range gates {
		if g.Name == name {
			return g, true
		}
	}
	return Gate{}, false
}

// Name is the gate's table name; a GateID outside the table is "unknown".
func (id GateID) Name() string {
	if id < numGates {
		return gates[id].Name
	}
	return "unknown"
}

// String is the gate's table name.
func (id GateID) String() string { return id.Name() }
