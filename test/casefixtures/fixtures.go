// Package casefixtures holds the log streams the per-case live verifier (test/caseverify) needs on a stack to
// prove a registry case: a case that names a stream the stack never ingested answers "empty" on both sides, and
// an empty answer proves nothing.
//
// The streams are the ones the cases' own tests push (test/e2e-compat) or the visual stack seeds
// (bench/visual/stack.py), under stable names. Every stream goes to Loki and to VictoriaLogs with the same
// timestamps, lines, stream labels and structured metadata; the route into VictoriaLogs is the one the case is
// about (a JSON line, Loki's push with the line kept verbatim, Loki's push with the line parsed). The names are the
// ones the registry's queries already hold, so no case text changes (a case query is a row of the plan corpus).
//
// The seed command is `go run ./test/casefixtures/cmd/seed`. It is test support: it writes only to the Loki and
// VictoriaLogs URLs it is given.
package casefixtures

import (
	"fmt"
	"time"
)

// Route is how a stream reaches VictoriaLogs.
type Route string

const (
	// RouteJSONLine posts rows to /insert/jsonline: the line in _msg, the stream labels, the structured
	// metadata and the projected fields as plain fields, _stream_fields the label names (the e2e-compat
	// helper pushStreamForTenant).
	RouteJSONLine Route = "jsonline"
	// RouteLokiPushRaw posts Loki's own push body to /insert/loki/api/v1/push?disable_message_parsing=1: the
	// line stays in _msg verbatim and only the structured metadata is stored as fields.
	RouteLokiPushRaw Route = "loki-push-raw"
	// RouteLokiPush posts Loki's own push body to /insert/loki/api/v1/push: VictoriaLogs parses a JSON line
	// into fields and sets _msg itself.
	RouteLokiPush Route = "loki-push"
)

// Entry is one log line of a stream, written once per tick.
type Entry struct {
	Line string
	// Metadata is stored under the same name on both sides: Loki's structured metadata and a VictoriaLogs field.
	Metadata map[string]string
	// LokiMetadata is Loki's structured metadata alone, VLFields the VictoriaLogs fields alone: the two
	// spellings of a key the stores name differently (an OTel attribute k8s.pod.name is k8s_pod_name on Loki).
	LokiMetadata map[string]string
	VLFields     map[string]string
}

// Stream is one stream of one fixture: its labels, its entries and the route into VictoriaLogs.
type Stream struct {
	// Name identifies the stream in the seed command's output and in the gate test.
	Name string
	// Cases are the registry cases (their ids) whose request selects this stream.
	Cases []string
	// Decoys are the cases that must NOT select this stream but need it written: a selector's regexp is anchored,
	// and the stream a substring match would take is the proof.
	Decoys  []string
	Labels  map[string]string
	Entries []Entry
	Route   Route
}

// Defaults of the seed command: the span the backlog covers and the tick that spaces it. A tick holds every
// entry of every stream once, so a window of five minutes holds ten of them.
const (
	DefaultSpan  = 30 * time.Minute
	DefaultEvery = 30 * time.Second
)

// m is a label or metadata set.
type m = map[string]string

// ipLines are the lines of the IPv4 line filter's own e2e fixture (TestCompat_IPLineFilterLikeLoki) and the
// dot-run lines of semantics/ip-filter-ipv4-after-dot-run.
var ipLines = []string{
	"client 10.0.0.1 connected", "client 110.0.0.12 connected", "client 10.0.0.12 connected", "client 10.0.0.1:8080",
	"ip=10.0.0.1,next", "a10.0.0.1b", "10.0.0.1.5", "v1.10.0.0.1", "x-10.0.0.1-y", "10.0.0.255 end", "10.0.1.1 end",
	"10.0.0.3 mid", "10.0.0.9 high", "0010.0.0.1 padded", "10.00.0.1 zero", "1192.168.1.1 wide", "::ffff:10.0.0.1 mapped",
	"g:10.0.0.1 colon", "10.0.0.1. dot",
	"x ..10.0.0.1 y", "x a..10.0.0.1 y", "x 0..10.0.0.1 y", "x 1.2..10.0.0.1 y",
}

func lines(ls ...string) []Entry {
	out := make([]Entry, len(ls))
	for i, l := range ls {
		out[i] = Entry{Line: l}
	}
	return out
}

// Streams is every stream, in a fixed order.
func Streams() []Stream {
	var out []Stream
	out = append(out, visualStreams()...)
	out = append(out, profileStreams()...)
	out = append(out, semanticsStreams()...)
	for i := range out {
		out[i].Labels = withServiceName(out[i].Labels)
	}
	return out
}

// withServiceName gives a stream the service_name Loki would derive for it (the first non-empty of service, app,
// application, name, app_kubernetes_io_name, container, container_name, component, workload, job), as a label both
// sides hold: Loki adds it at ingest, VictoriaLogs stores no such field, and the verifier counts VictoriaLogs by the
// selector's field names, so a stream without it would be counted by `{service_name=~".+"}` on Loki only.
func withServiceName(labels map[string]string) map[string]string {
	if labels["service_name"] != "" {
		return labels
	}
	out := make(map[string]string, len(labels)+1)
	for k, v := range labels {
		out[k] = v
	}
	out["service_name"] = "unknown_service"
	for _, k := range []string{"service", "app", "application", "name", "app_kubernetes_io_name", "container", "container_name", "component", "workload", "job"} {
		if labels[k] != "" {
			out["service_name"] = labels[k]
			break
		}
	}
	return out
}

// visualStreams are the seeds of bench/visual/stack.py, the cases that read them by service_name. The stream
// carries service_name as a label on both sides: Loki derives it from app, VictoriaLogs holds no such field, and
// the verifier counts VictoriaLogs by the selector's field names.
func visualStreams() []Stream {
	return []Stream{
		{
			Name:   "visual-keep-metadata",
			Cases:  []string{"drilldown/detected-fields-metadata-reported-as-json"},
			Labels: m{"app": "visual-keep-metadata", "service_name": "visual-keep-metadata"},
			Entries: []Entry{{
				Line:         "metadata keep visual sample",
				LokiMetadata: m{"deployment_environment": "production"},
				VLFields:     m{"deployment.environment": "production"},
			}},
			Route: RouteJSONLine,
		},
		{
			Name:    "visual-detected-level-stage-metadata",
			Cases:   []string{"drilldown/detected-labels-lists-stored-field", "drilldown/volume-detected-level-not-normalised"},
			Labels:  m{"app": "visual-detected-level-stage", "case": "sm-level", "service_name": "visual-detected-level-stage"},
			Entries: []Entry{{Line: "stage metadata level sample", Metadata: m{"level": "Warning"}}},
			Route:   RouteJSONLine,
		},
		{
			Name:  "visual-detected-level-stage-label",
			Cases: []string{"drilldown/detected-labels-lists-stored-field", "drilldown/volume-detected-level-not-normalised"},
			Labels: m{"app": "visual-detected-level-stage", "case": "label-detected-level", "detected_level": "warn",
				"service_name": "visual-detected-level-stage"},
			Entries: lines("stage stream level sample"),
			Route:   RouteLokiPushRaw,
		},
	}
}

// profileStreams are the fixtures of the profile cases (cases/profiles).
func profileStreams() []Stream {
	// The svc values of the label filter case (`svc=~"api|api-gateway"`) ride in the streams that exist anyway, so no
	// stream carries app="api-gateway", the selector of twenty cases the generator's data already serves.
	apps := []struct{ app, svc string }{{"api", "api"}, {"web", "web"}, {"my-api", "my-api"}, {"webhook", "api-gateway"}}
	var anchors []Stream
	for _, a := range apps {
		// The selector app=~"api|web" is anchored: api and web are its streams, the others are what a substring
		// match would wrongly take.
		cases, decoys := []string{"profiles/label-filter-regexp-anchoring", "profiles/stream-selector-regexp-anchoring"}, []string(nil)
		if a.app != "api" && a.app != "web" {
			cases, decoys = cases[:1], cases[1:]
		}
		if a.app == "api" {
			// `{app="api"}` is also the selector of the line_format case: its stream reads this one too.
			cases = append(append([]string(nil), cases...), "semantics/line-format-template-error-keeps-entry")
		}
		anchors = append(anchors, Stream{
			Name:   "anchor-" + a.app,
			Cases:  cases,
			Decoys: decoys,
			Labels: m{"probe": "anchor<ts>", "app": a.app},
			Entries: lines(
				`{"svc":"`+a.svc+`","msg":"anchor `+a.app+`"}`,
				`{"svc":"no dotted keys here","msg":"anchor other"}`,
			),
			Route: RouteJSONLine,
		})
	}
	out := []Stream{
		{
			Name:   "compat-matrix",
			Cases:  []string{"profiles/detected-fields-dotted-json-keys"},
			Labels: m{"app": "compat-matrix-<ts>"},
			Entries: lines(
				`{"http.method":"GET","k8s.pod.name":"pod-a","svc":{"name":"api"},"msg":"dotted keys"}`,
				`{"http.method":"POST","k8s.pod.name":"pod-b","svc":{"name":"web"},"msg":"dotted keys"}`,
			),
			Route: RouteJSONLine,
		},
		{
			Name:   "line-fields",
			Cases:  []string{"profiles/detected-fields-structured-metadata-beside-json-line"},
			Labels: m{"app": "line-fields-<ts>"},
			Entries: []Entry{
				{Line: `{"msg":"hello","user":"u1","status":200}`, Metadata: m{"trace_id": "trace-1", "k8s_pod_name": "pod-a"}},
				{Line: `{"msg":"again","user":"u2","status":500}`, Metadata: m{"trace_id": "trace-2", "k8s_pod_name": "pod-b"}},
			},
			Route: RouteLokiPushRaw,
		},
		{
			Name:   "matrix-extraction-list-json",
			Cases:  []string{"profiles/json-extraction-list-exposes-only-listed"},
			Labels: m{"app": "matrix-extraction-list", "case": "json"},
			Entries: []Entry{
				{Line: `{"msg":"hello","level":"info"}`, VLFields: m{"level": "info"}},
			},
			Route: RouteJSONLine,
		},
		{
			Name:    "promtail-json",
			Cases:   []string{"profiles/json-line-stored-without-line"},
			Labels:  m{"app": "promtail-json-<ts>"},
			Entries: lines(`{"msg":"promtail line","level":"info","user":"u1"}`, `{"msg":"second line","level":"warn","user":"u2"}`),
			Route:   RouteLokiPush,
		},
		{
			Name:   "stage-fields-json",
			Cases:  []string{"profiles/label-filter-on-line-field", "profiles/stage-field-exposure"},
			Labels: m{"app": "stage-fields-<ts>-json"},
			Entries: lines(
				`{"msg":"m1","status":200,"svc":{"name":"api"},"user":"u1"}`,
				`{"msg":"m2","status":404,"svc":{"name":"web"},"user":"u2"}`,
				`{"msg":"m3","status":500,"svc":{"name":"api"},"user":"u1"}`,
			),
			Route: RouteLokiPushRaw,
		},
		{
			Name:   "sanitized-filter",
			Cases:  []string{"profiles/sanitized-json-key-filter"},
			Labels: m{"app": "compat-sanitized-<ts>"},
			Entries: lines(
				`{"probe.verb":"GET","probe.code":200,"msg":"sanitized"}`,
				`{"probe.verb":"POST","probe.code":500,"msg":"sanitized"}`,
			),
			Route: RouteJSONLine,
		},
		{
			Name:   "sanitized-grouping",
			Cases:  []string{"profiles/sanitized-json-key-grouping"},
			Labels: m{"app": "compat-grouping-<ts>"},
			Entries: lines(
				`{"gk.verb":"GET","msg":"grouping"}`,
				`{"gk.verb":"POST","msg":"grouping"}`,
				`{"gk.verb":"GET","msg":"grouping again"}`,
			),
			Route: RouteJSONLine,
		},
		{
			Name:   "structured-metadata-e2e",
			Cases:  []string{"profiles/structured-metadata-keys-per-profile"},
			Labels: m{"service_name": "structured-metadata-e2e", "level": "info"},
			Entries: []Entry{{
				Line: "otel record with attributes",
				// An OTel pipeline delivers the attributes to VictoriaLogs under their own dotted names and to
				// Loki under the sanitized ones.
				LokiMetadata: m{"k8s_cluster_name": "us-east-1", "k8s_namespace_name": "prod", "k8s_pod_name": "pod-a"},
				VLFields:     m{"k8s.cluster.name": "us-east-1", "k8s.namespace.name": "prod", "k8s.pod.name": "pod-a"},
			}},
			Route: RouteJSONLine,
		},
	}
	return append(out, anchors...)
}

// semanticsStreams are the fixtures of the semantics cases (cases/semantics).
func semanticsStreams() []Stream {
	// m and s are stream labels, not structured metadata: Loki splits a metric query's series by structured
	// metadata and the proxy does not (a difference of its own), and every metric query on {app="x"} (the fixed
	// nested aggregation case among them) reads these streams too. As labels both sides split alike.
	var labelled []Stream
	for i, v := range []string{"5.0", "5", "6", "abc", ""} {
		labels := m{"app": "x", "case": "numeric"}
		if v != "" {
			labels["m"] = v
		}
		labelled = append(labelled, Stream{
			Name:    fmt.Sprintf("x-numeric-%d", i),
			Cases:   []string{"semantics/label-filter-numeric-equality", "semantics/label-filter-numeric-error-label"},
			Labels:  labels,
			Entries: lines("numeric line " + fmt.Sprint(i)),
			Route:   RouteJSONLine,
		})
	}
	for i, v := range []string{"a detected_level b", "other text"} {
		labelled = append(labelled, Stream{
			Name:    fmt.Sprintf("x-quoted-%d", i),
			Cases:   []string{"semantics/translator-reads-quoted-text-as-syntax"},
			Labels:  m{"app": "x", "case": "quoted", "s": v},
			Entries: lines("quoted line " + fmt.Sprint(i)),
			Route:   RouteJSONLine,
		})
	}
	return append(labelled, semanticsStreamsRest()...)
}

func semanticsStreamsRest() []Stream {
	// The streams of app="x" are one selector for twelve cases (the case texts are corpus rows and stay as they
	// are); each stream holds the lines of the cases that read it.
	return []Stream{
		{
			Name:   "x-stream-level",
			Cases:  []string{"semantics/drop-keep-plain-name-after-parser", "semantics/plain-name-after-parser-reads-stream-label"},
			Labels: m{"app": "x", "level": "info"},
			Entries: lines(
				`{"level":"debug","msg":"parsed level differs from the stream label"}`,
				`{"level":"info","msg":"parsed level equals the stream label"}`,
			),
			Route: RouteJSONLine,
		},
		{
			Name:    "x-pod-zone",
			Cases:   []string{"semantics/nested-aggregation-outer-by-native-routes"},
			Labels:  m{"app": "x", "case": "zone-a", "pod": "pod-a1", "zone": "a"},
			Entries: lines("zone a first pod"),
			Route:   RouteJSONLine,
		},
		{
			Name:    "x-pod-zone-b",
			Cases:   []string{"semantics/nested-aggregation-outer-by-native-routes"},
			Labels:  m{"app": "x", "case": "zone-a2", "pod": "pod-a2", "zone": "a"},
			Entries: lines("zone a second pod", "zone a second pod again"),
			Route:   RouteJSONLine,
		},
		{
			Name:    "x-pod-zone-c",
			Cases:   []string{"semantics/nested-aggregation-outer-by-native-routes"},
			Labels:  m{"app": "x", "case": "zone-b", "pod": "pod-b1", "zone": "b"},
			Entries: lines("zone b pod"),
			Route:   RouteJSONLine,
		},
		{
			Name:   "x-repeated-filters",
			Cases:  []string{"semantics/repeated-label-filter-replaced-across-label-format"},
			Labels: m{"app": "x", "case": "repeated"},
			Entries: lines(
				`{"user":"info","msg":"m"}`,
				`{"user":"m","msg":"info"}`,
				`{"user":"other","msg":"m"}`,
			),
			Route: RouteJSONLine,
		},
		{
			Name:   "x-packed",
			Cases:  []string{"semantics/translator-writes-unpack-as-unpack-json"},
			Labels: m{"app": "x", "case": "packed"},
			Entries: lines(
				`{"_entry":"packed entry","a":"1","b":"2"}`,
				`{"_entry":"packed other","a":"2"}`,
				`{"a":"1","msg":"not packed"}`,
				"plain line",
			),
			Route: RouteJSONLine,
		},
		{
			Name:   "x-two-formats",
			Cases:  []string{"semantics/two-parsers-keep-one"},
			Labels: m{"app": "x", "case": "formats"},
			Entries: lines(
				`{"a":"1","b":"two"}`,
				"c=3 d=four",
				`{"a":"5","c":"six"}`,
				"c=7 b=eight",
			),
			Route: RouteJSONLine,
		},
		{
			Name:    "x-plain-rate",
			Cases:   []string{"semantics/translator-reads-blank-before-range-call-as-text", "semantics/translator-reads-parenthesized-metric-as-text"},
			Labels:  m{"app": "x", "case": "plain"},
			Entries: lines("plain one", "plain two", "plain three"),
			Route:   RouteJSONLine,
		},
		{
			Name:    "ip-line",
			Cases:   []string{"semantics/ip-line-filter-ipv4-like-loki", "semantics/ip-filter-ipv4-after-dot-run"},
			Labels:  m{"app": "ip-line", "probe": "seed"},
			Entries: lines(ipLines...),
			Route:   RouteJSONLine,
		},
		{
			Name:    "ipprobe-t",
			Cases:   []string{"semantics/line-filter-invalid-utf8"},
			Labels:  m{"app": "ipprobe-t", "probe": "seed"},
			Entries: lines("level=info msg=ok", "level=warn msg=slow b", `level=error msg="bad byte" b`),
			Route:   RouteJSONLine,
		},
		{
			// One stream for `{app="nginx"} | pattern "<_> <a><b"`; it also holds the IPv4 half of
			// semantics/ip-label-filter-embedded-address (its second corpus shape, `{service_name="nginx"}`), while that
			// case's request reads the ip-label-v6 stream.
			Name:   "nginx",
			Cases:  []string{"semantics/pattern-capture-before-bracket-text"},
			Labels: m{"app": "nginx", "service_name": "nginx"},
			Entries: lines(
				`addr=10.0.0.5 msg=ab<b`,
				`addr=10.0.0.5:443 msg=cd<b`,
				`addr="client 10.0.0.5" msg=ef<b`,
				`addr="[10.0.0.5]:80" msg=gh`,
				`addr=192.168.1.1 msg=ij`,
				`addr=192.168.1.1:80 msg=kl`,
				`addr=010.0.0.5 msg=mn`,
				`addr=10.0.0.256 msg=op`,
				`addr="no address" msg=qr`,
				`msg=no_addr_label`,
			),
			Route: RouteJSONLine,
		},
		{
			Name:    "api-line-format",
			Cases:   []string{"semantics/line-format-template-error-keeps-entry"},
			Labels:  m{"app": "api"},
			Entries: lines("a=1 b=2", "a=3 b=4"),
			Route:   RouteJSONLine,
		},
		{
			Name:   "a-numeric-metadata",
			Cases:  []string{"semantics/numeric-label-filter-not-equal-missing-label"},
			Labels: m{"app": "a"},
			Entries: []Entry{
				{Line: "metadata six", Metadata: m{"m": "6"}},
				{Line: "metadata four", Metadata: m{"m": "4"}},
				{Line: "metadata five", Metadata: m{"m": "5"}},
				{Line: "no metadata"},
			},
			Route: RouteJSONLine,
		},
		{
			Name:    "uwsum-lf-1",
			Cases:   []string{"semantics/without-instant-detected-level-unknown"},
			Labels:  m{"app": "uwsum-lf-1", "route": "r1"},
			Entries: lines("level-less line one", "level-less line two"),
			Route:   RouteJSONLine,
		},
		{
			Name:    "uwsum-lf-2",
			Cases:   []string{"semantics/without-instant-detected-level-unknown"},
			Labels:  m{"app": "uwsum-lf-2", "route": "r2"},
			Entries: lines("level-less line three"),
			Route:   RouteJSONLine,
		},
		{
			Name:   "ip-label-v6",
			Cases:  []string{"semantics/ip-label-filter-embedded-address"},
			Labels: m{"app": "ip-label-v6"},
			Entries: lines(
				`addr=2001:db8::1 msg=whole`,
				`addr="[2001:db8::1]:443" msg=bracketed`,
				`addr="client 2001:db8::1" msg=embedded`,
				`addr=fe80::1%eth0 msg=zoned`,
				`addr=2001:dc8::1 msg=other_prefix`,
				`addr="no address" msg=none`,
				`msg=no_addr_label`,
			),
			Route: RouteJSONLine,
		},
		{
			// Loki: `| json | ip=ip("10.2.0.0/16")` 4 lines (3 matches + the rejected one, __error__=JSONParserErr);
			// `ip!=ip(...)` 3 lines (2 + the rejected one); method="GET" 4; proxy drops the rejected line under both.
			Name:   "ip-json-parser-error",
			Cases:  []string{"semantics/ip-filter-keeps-parser-error-lines"},
			Labels: m{"app": "ip-json-parser-error"},
			Entries: lines(
				`{"ip":"10.2.3.4","method":"GET"}`,
				`{"ip":"10.2.5.6","method":"GET"}`,
				`{"ip":"10.2.7.8","method":"GET"}`,
				`{"ip":"192.168.1.1","method":"GET"}`,
				`{"ip":"172.16.0.1","method":"POST"}`,
				`not json ip=10.2.3.4`,
			),
			Route: RouteLokiPushRaw, // the line stays verbatim in _msg on both sides
		},
		{
			// A JSON object with an empty value, one with a value and one without the key (semantics/json-empty-value-label-series):
			// Loki answers the empty `a` as a series of its own, the proxy merges it into the series without the label.
			Name:    "json-empty",
			Cases:   []string{"semantics/json-empty-value-label-series"},
			Labels:  m{"app": "json-empty"},
			Entries: lines(`{"a":""}`, `{"a":"x"}`, `{"b":1}`),
			Route:   RouteLokiPushRaw,
		},
		{
			// 12 lines: Loki groups sum by (a) as `1`: 7 and none: 5; the proxy reads 5 groups (a=2, a="unterminated, a==1).
			Name:   "logfmt-malformed",
			Cases:  []string{"semantics/logfmt-malformed-lines"},
			Labels: m{"app": "logfmt-malformed"},
			Entries: lines(
				`a=1 msg=ok`, `a=1 msg=ok2`, `a=1 b=2`, `a=1 c=3`, `a=1 d=4`,
				`a=1 a=2`, `a=1 "b"=2`,
				`a="unterminated b=2`, `a==1 b=2`,
				`b=2`, `msg=hello`, `x=1 y=2`,
			),
			Route: RouteLokiPushRaw,
		},
		{
			// Three streams, each with a `pod` stream label that the logfmt extraction list `amount, pod` also writes:
			// Loki answers {app, pod=<stream pod>, pod_extracted=<parsed pod>}; the proxy the parsed pod under `pod`.
			// One stream per pod value a|b|c (Name suffix), the parsed pod p1 or p2 alternating by line.
			Name:   "uw-collision-a",
			Cases:  []string{"semantics/unwrap-sum-without-extracted-label-collision"},
			Labels: m{"app": "uw-collision", "namespace": "ns1", "pod": "a"},
			Entries: lines(
				`amount=10 pod=p1`,
				`amount=20 pod=p2`,
			),
			Route: RouteJSONLine,
		},
		{
			// Three streams, each with a `pod` stream label that the logfmt extraction list `amount, pod` also writes:
			// Loki answers {app, pod=<stream pod>, pod_extracted=<parsed pod>}; the proxy the parsed pod under `pod`.
			// One stream per pod value a|b|c (Name suffix), the parsed pod p1 or p2 alternating by line.
			Name:   "uw-collision-b",
			Cases:  []string{"semantics/unwrap-sum-without-extracted-label-collision"},
			Labels: m{"app": "uw-collision", "namespace": "ns1", "pod": "b"},
			Entries: lines(
				`amount=10 pod=p1`,
				`amount=20 pod=p2`,
			),
			Route: RouteJSONLine,
		},
		{
			// Three streams, each with a `pod` stream label that the logfmt extraction list `amount, pod` also writes:
			// Loki answers {app, pod=<stream pod>, pod_extracted=<parsed pod>}; the proxy the parsed pod under `pod`.
			// One stream per pod value a|b|c (Name suffix), the parsed pod p1 or p2 alternating by line.
			Name:   "uw-collision-c",
			Cases:  []string{"semantics/unwrap-sum-without-extracted-label-collision"},
			Labels: m{"app": "uw-collision", "namespace": "ns1", "pod": "c"},
			Entries: lines(
				`amount=10 pod=p1`,
				`amount=20 pod=p2`,
			),
			Route: RouteJSONLine,
		},
		{
			// Stream label level="info"; every JSON line says another level. VictoriaLogs' Loki push unpacks the line's key
			// over the stream label, so `sum by (level)` reads error/warn there and info on Loki.
			Name:   "level-shadow",
			Cases:  []string{"semantics/stream-level-shadowed-by-ingest-json-key"},
			Labels: m{"app": "level-shadow", "level": "info"},
			Entries: lines(
				`{"level":"error","msg":"stream info, line error"}`,
				`{"level":"warn","msg":"stream info, line warn"}`,
			),
			Route: RouteLokiPush, // VictoriaLogs parses the JSON line into fields after the stream labels
		},
	}
}
