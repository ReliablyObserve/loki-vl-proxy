package querycorpus_test

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// The Loki differential of -field-mapping-label-mode (semantics/field-mapping-label-modes). VictoriaLogs (the serve
// oracle, v1.54.0 and v1.49.0) holds lines under the OpenTelemetry names (k8s.pod.name, k8s.namespace.name) mapped to
// the aliases pod and namespace; Loki (the engine oracle for metric queries, the process oracle for each line of a log
// query and for the detected fields) holds the same lines the way the mode says a migration sees them:
//
//   - alias: a Loki that stored the pre-migration names (pod, namespace);
//   - both: a Loki that stored both names (pod and k8s_pod_name, namespace and k8s_namespace_name).
//
// The lines carry the keys pod, namespace and k8s_pod_name, so a parser yields keys named like a stream label of the
// entry, which Loki renames name_extracted. Per parser (logfmt, json) a service has four streams: two hold the mapped
// fields, one holds none (its parsed pod is the label pod), and one holds a stream field stored as pod itself (a
// stream that was never renamed). The flag unset and original are pinned byte for byte to the answers recorded at the
// base (fmUnchanged): the alias rules change nothing there.
//
// detected_fields and /detected_field/<name>/values have no Loki oracle of their own: their Loki answer is read from
// Loki's pipeline of `<selector> | <parser>` over every line (the process oracle), the parsed labels and structured
// metadata it keeps, which is where Loki's detected fields come from. Only the field names this differential is about
// (fmFieldNames) are compared.

const fmAnswersPath = "testdata/field-mapping-label-mode-loki-diff.jsonl.gz"

var fmParsers = []string{"logfmt", "json"}

// fmMappedService is a logfmt service whose streams all hold the mapped fields: its parsed pod is pod_extracted on
// every line, so Loki has no detected field pod there.
const fmMappedService = "fm-mapped"

// fmStream is one stream of the fixture: its class (mapped, plain, native) and its index.
type fmStream struct {
	Parser string `json:"parser"`
	Class  string `json:"class"`
	I      int    `json:"i"`
}

func (s fmStream) service() string {
	if s.Class == "only-mapped" {
		return fmMappedService
	}
	return "fm-" + s.Parser
}

// fmStreams are two mapped streams, one without the mapped fields and one storing pod itself, per parser, and the
// two streams of fmMappedService.
func fmStreams() []fmStream {
	var out []fmStream
	for _, p := range fmParsers {
		out = append(out, fmStream{p, "mapped", 0}, fmStream{p, "mapped", 1}, fmStream{p, "plain", 2}, fmStream{p, "native", 3})
	}
	return append(out, fmStream{"logfmt", "only-mapped", 4}, fmStream{"logfmt", "only-mapped", 5})
}

// vlStream is the stream as VictoriaLogs stores it.
func (s fmStream) vlStream() [][2]string {
	out := [][2]string{{"service.name", s.service()}}
	switch s.Class {
	case "mapped", "only-mapped":
		out = append(out, [2]string{"k8s.namespace.name", "ns-a"}, [2]string{"k8s.pod.name", fmt.Sprintf("pod-%d", s.I)})
	case "plain":
		out = append(out, [2]string{"zone", "z2"})
	case "native":
		out = append(out, [2]string{"pod", fmt.Sprintf("npod-%d", s.I)})
	}
	return out
}

// lokiLabels are the stream's labels in the Loki reference of the mode.
func (s fmStream) lokiLabels(ref string) map[string]string {
	out := map[string]string{"service_name": s.service()}
	switch s.Class {
	case "mapped", "only-mapped":
		out["pod"], out["namespace"] = fmt.Sprintf("pod-%d", s.I), "ns-a"
		if ref == "both" {
			out["k8s_pod_name"], out["k8s_namespace_name"] = out["pod"], out["namespace"]
		}
	case "plain":
		out["zone"] = "z2"
	case "native":
		out["pod"] = fmt.Sprintf("npod-%d", s.I)
	}
	return out
}

// fmEntry is one line.
type fmEntry struct {
	Stream int    `json:"stream"`
	TS     int64  `json:"ts"`
	Line   string `json:"line"`
}

// fmFixture is a line every 41 seconds per stream over the metric window and 20 minutes before it, less one of every
// i+2 for stream i, so the streams' counts differ and groupings rank without ties.
func fmFixture() []fmEntry {
	var out []fmEntry
	streams := fmStreams()
	span := metricWindow + 20*time.Minute
	first := metricEnd.Add(-span)
	const every = 41 * time.Second
	for s, st := range streams {
		for k := 0; ; k++ {
			at := first.Add(time.Duration(k)*every + time.Duration(s)*3*time.Second + 250*time.Microsecond)
			if !at.Before(metricEnd) {
				break
			}
			if k%(st.I+2) == 1 {
				continue
			}
			keys := [][2]string{{"level", "info"}, {"pod", fmt.Sprintf("line-%d-%d", st.I, k%2)}, {"namespace", fmt.Sprintf("lns-%d", k%3)}}
			if k%2 == 0 {
				keys = append(keys, [2]string{"k8s_pod_name", fmt.Sprintf("kl-%d-%d", st.I, k%4/2)})
			}
			keys = append(keys, [2]string{"n", strconv.Itoa(k)})
			var line string
			if st.Parser == "json" {
				parts := make([]string, len(keys))
				for i, kv := range keys {
					parts[i] = strconv.Quote(kv[0]) + ":" + strconv.Quote(kv[1])
				}
				line = "{" + strings.Join(parts, ",") + "}"
			} else {
				parts := make([]string, len(keys))
				for i, kv := range keys {
					parts[i] = kv[0] + "=" + kv[1]
				}
				line = strings.Join(parts, " ")
			}
			out = append(out, fmEntry{Stream: s, TS: at.UnixNano(), Line: line})
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].TS < out[j].TS })
	return out
}

// fmLokiStreams are the fixture as the Loki reference ref holds it.
func fmLokiStreams(entries []fmEntry, ref string) []oraclerun.EngineStream {
	streams := fmStreams()
	out := make([]oraclerun.EngineStream, len(streams))
	for i, s := range streams {
		out[i].Labels = s.lokiLabels(ref)
	}
	for _, e := range entries {
		out[e.Stream].Entries = append(out[e.Stream].Entries, oraclerun.EngineEntry{TS: e.TS, Line: e.Line, Metadata: map[string]string{"detected_level": "info"}})
	}
	return out
}

func fmVLRows(entries []fmEntry) []oraclerun.ServeRow {
	streams := fmStreams()
	out := make([]oraclerun.ServeRow, 0, len(entries))
	for _, e := range entries {
		out = append(out, oraclerun.ServeRow{TS: e.TS, Stream: streams[e.Stream].vlStream(), Fields: [][2]string{{"_msg", e.Line}}})
	}
	return out
}

// fmMode is a -field-mapping-label-mode and the Loki reference it is held to ("" for a mode pinned to the base).
type fmMode struct {
	mode proxy.FieldMappingLabelMode
	ref  string
}

func (m fmMode) name() string {
	if m.mode == proxy.FieldMappingLabelModeUnset {
		return "unset"
	}
	return string(m.mode)
}

func fmModes() []fmMode {
	return []fmMode{
		{proxy.FieldMappingLabelModeUnset, ""},
		{proxy.FieldMappingLabelModeOriginal, ""},
		{proxy.FieldMappingLabelModeAlias, "alias"},
		{proxy.FieldMappingLabelModeBoth, "both"},
	}
}

// fmRefs are the Loki references.
var fmRefs = []string{"alias", "both"}

// fmFieldNames are the detected fields compared.
var fmFieldNames = []string{"pod", "pod_extracted", "namespace", "namespace_extracted", "k8s_pod_name", "k8s_pod_name_extracted", "k8s_namespace_name", "k8s_namespace_name_extracted"}

// fmQuery is one request shape over one parser's service.
type fmQuery struct {
	q, parser string
	// kind is log, metric, fields (detected_fields) or values (/detected_field/<field>/values).
	kind  string
	field string
	reqs  []metricRequest
}

func (fq fmQuery) id() string {
	if fq.field != "" {
		return fq.kind + " " + fq.field + " " + fq.q
	}
	return fq.kind + " " + fq.q
}

func fmQueries() []fmQuery {
	var out []fmQuery
	for _, p := range fmParsers {
		sel := `{service_name="fm-` + p + `"}`
		q := sel + " | " + p
		for _, f := range []string{"", `pod="pod-0"`, `pod=~"npod-3|line-2-.*"`, `pod!="pod-1"`, `pod_extracted=~".+-1"`, `k8s_pod_name="kl-0-1"`,
			`k8s_pod_name_extracted!=""`, `namespace_extracted!=""`, `namespace="ns-a"`} {
			lq := q
			if f != "" {
				lq += " | " + f
			}
			out = append(out, fmQuery{q: lq, parser: p, kind: "log", reqs: []metricRequest{fmLogRequest(lq)}})
		}
		for _, m := range []string{
			`sum by (pod) (count_over_time(%s [5m]))`,
			`sum by (pod_extracted) (count_over_time(%s [5m]))`,
			`topk(2, sum by (pod) (count_over_time(%s [5m])))`,
			`sum by (pod) (count_over_time(%s | pod=~"pod-.+|npod-.+" [5m]))`,
			`sum by (pod_extracted) (count_over_time(%s | pod=~"pod-.+|npod-.+" [5m]))`,
			`sum by (k8s_pod_name) (count_over_time(%s [5m]))`,
			`sum by (k8s_pod_name_extracted) (count_over_time(%s [5m]))`,
			`sum by (pod, namespace_extracted) (count_over_time(%s | pod_extracted!="" [5m]))`,
			`sum by (pod_extracted) (count_over_time(%s | pod_extracted!="" [5m]))`,
			`sum by (k8s_pod_name_extracted) (count_over_time(%s | k8s_pod_name_extracted!="" [5m]))`,
		} {
			mq := fmt.Sprintf(m, q)
			reqs := metricRequests(mq, 5*time.Minute)
			if strings.HasPrefix(m, "topk") {
				// The per-step rankings tie between streams of equal counts, which either side picks arbitrarily.
				reqs = reqs[:1]
			}
			out = append(out, fmQuery{q: mq, parser: p, kind: "metric", reqs: reqs})
		}
		out = append(out, fmQuery{q: sel, parser: p, kind: "fields", reqs: []metricRequest{fmMetaRequest(sel)}})
		for _, f := range []string{"pod", "pod_extracted", "k8s_pod_name", "k8s_pod_name_extracted", "namespace"} {
			out = append(out, fmQuery{q: sel, parser: p, kind: "values", field: f, reqs: []metricRequest{fmMetaRequest(sel)}})
		}
	}
	sel := `{service_name="` + fmMappedService + `"}`
	out = append(out, fmQuery{q: sel + " | logfmt", parser: "logfmt", kind: "log", reqs: []metricRequest{fmLogRequest(sel + " | logfmt")}})
	out = append(out, fmQuery{q: sel, parser: "logfmt", kind: "fields", reqs: []metricRequest{fmMetaRequest(sel)}})
	for _, f := range []string{"pod", "pod_extracted", "namespace", "k8s_pod_name"} {
		out = append(out, fmQuery{q: sel, parser: "logfmt", kind: "values", field: f, reqs: []metricRequest{fmMetaRequest(sel)}})
	}
	return out
}

func fmLogRequest(q string) metricRequest {
	r := fmMetaRequest(q)
	r.kind = "log"
	r.form.Set("limit", "5000")
	r.form.Set("direction", "backward")
	return r
}

func fmMetaRequest(q string) metricRequest {
	return metricRequest{kind: "meta", form: url.Values{
		"query": {q},
		"start": {strconv.FormatInt(metricEnd.Add(-metricWindow-30*time.Minute).Unix(), 10)},
		"end":   {strconv.FormatInt(metricEnd.Unix(), 10)},
		"limit": {"1000"},
	}}
}

// fmRecord is a query's recorded answers: Loki's per reference and request kind, VictoriaLogs' per version by the
// hash of the request key, and the pinned answers of the modes without a reference, per version, mode and kind.
type fmRecord struct {
	ID     string                                  `json:"id"`
	Inputs string                                  `json:"inputs"`
	Loki   map[string]map[string]metricLokiAnswer  `json:"loki"`
	VL     map[string]map[string]metricVLAnswer    `json:"vl"`
	Pinned map[string]map[string]map[string]string `json:"pinned,omitempty"`
}

func (fq fmQuery) inputsHash(entries []fmEntry) string {
	b, _ := json.Marshal(struct {
		S []fmStream
		E []fmEntry
		M []proxy.FieldMapping
	}{fmStreams(), entries, fmMappings()})
	var s strings.Builder
	s.Write(b)
	s.WriteString("\x00" + fq.id())
	for _, r := range fq.reqs {
		s.WriteString("\x00" + r.kind + "\x00" + r.form.Encode())
	}
	return shortHash(s.String())
}

// fmMappings are a migration's mappings: the alias first, the -label-style name last (the answer name of the
// unset mode).
func fmMappings() []proxy.FieldMapping {
	return []proxy.FieldMapping{
		{VLField: "k8s.pod.name", LokiLabel: "pod"}, {VLField: "k8s.pod.name", LokiLabel: "k8s_pod_name"},
		{VLField: "k8s.namespace.name", LokiLabel: "namespace"}, {VLField: "k8s.namespace.name", LokiLabel: "k8s_namespace_name"},
	}
}

func readFMAnswers(t *testing.T) map[string]*fmRecord {
	t.Helper()
	f, err := os.Open(fmAnswersPath)
	if err != nil {
		t.Fatalf("%s: %v: LVP_RUN_ORACLES=1 records it", fmAnswersPath, err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]*fmRecord{}
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<28)
	for sc.Scan() {
		var r fmRecord
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		out[r.ID] = &r
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func writeFMAnswers(t *testing.T, recs []*fmRecord) {
	t.Helper()
	f, err := os.Create(fmAnswersPath)
	if err != nil {
		t.Fatal(err)
	}
	zw, err := gzip.NewWriterLevel(f, gzip.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	zw.ModTime = time.Time{}
	enc := json.NewEncoder(zw)
	enc.SetEscapeHTML(false)
	for _, r := range recs {
		if err := enc.Encode(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// fmLokiPipeline runs Loki's pipeline of q over every line of the reference: the kept entries, by time.
func fmLokiPipeline(t *testing.T, q, ref string, entries []fmEntry) (map[string]jlEntryAnswer, string) {
	t.Helper()
	streams := fmStreams()
	cases := make([]oraclerun.ProcessCase, len(entries))
	for i, e := range entries {
		cases[i] = oraclerun.ProcessCase{ID: strconv.Itoa(i), Query: q, Labels: streams[e.Stream].lokiLabels(ref), Line: e.Line,
			Metadata: map[string]string{"detected_level": "info"}, WithCategories: true}
	}
	answers, err := oraclerun.LokiProcess(filepath.Join("..", "..", "test", "oracle", "loki"), cases)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]jlEntryAnswer{}
	for i, a := range answers {
		if a.Error != "" {
			return nil, a.Error
		}
		if !a.Selected || !a.Keep {
			continue
		}
		out[strconv.FormatInt(entries[i].TS, 10)] = jlEntryAnswer{Line: a.Line, Stream: a.Stream, Metadata: jlWithoutLevel(a.StructuredMetadata), Parsed: jlWithoutLevel(a.Parsed)}
	}
	return out, ""
}

// fmLokiAnswer is Loki's answer to a log, fields or values request of the reference.
func fmLokiAnswer(t *testing.T, fq fmQuery, ref string, entries []fmEntry) metricLokiAnswer {
	t.Helper()
	q := fq.q
	if fq.kind != "log" {
		q += " | " + fq.parser
	}
	kept, errText := fmLokiPipeline(t, q, ref, entries)
	if errText != "" {
		return metricLokiAnswer{Status: 400, Error: errText}
	}
	var body []byte
	switch fq.kind {
	case "log":
		body, _ = json.Marshal(kept)
	case "fields":
		names := map[string]bool{}
		for _, e := range kept {
			for k := range e.Parsed {
				names[k] = true
			}
			for k := range e.Metadata {
				names[k] = true
			}
		}
		body, _ = json.Marshal(fmCompared(sortedKeysOf(names)))
	case "values":
		values := map[string]bool{}
		for _, e := range kept {
			if v, ok := e.Parsed[fq.field]; ok {
				values[v] = true
			} else if v, ok := e.Metadata[fq.field]; ok {
				values[v] = true
			}
		}
		body, _ = json.Marshal(sortedKeysOf(values))
	}
	return metricLokiAnswer{Status: 200, Body: body}
}

// fmCompared keeps the compared field names.
func fmCompared(names []string) []string {
	out := []string{}
	for _, n := range names {
		if slices.Contains(fmFieldNames, n) {
			out = append(out, n)
		}
	}
	return out
}

// newFMProxy starts a proxy of the profile and the mode in front of front, and returns what serves one request of
// the query through it; serving a query of no requests stops it.
func newFMProxy(t *testing.T, prof diffProfile, mode fmMode, front *metricFront) func(fmQuery, metricRequest) metricHandlerRun {
	t.Helper()
	srv := newMemoryFront(front)
	p, err := proxy.New(proxy.Config{
		BackendURL:             srv.URL,
		Cache:                  cache.New(time.Minute, 1000),
		LogLevel:               "error",
		LabelStyle:             prof.style,
		MetadataFieldMode:      prof.mode,
		EmitStructuredMetadata: true,
		FieldMappings:          fmMappings(),
		FieldMappingLabelMode:  mode.mode,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = p.ValidateBackendVersionCompatibility(context.Background())
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	return func(fq fmQuery, mr metricRequest) metricHandlerRun {
		if mr.form == nil {
			_ = p.Shutdown(context.Background())
			srv.Close()
			return metricHandlerRun{}
		}
		var path string
		switch {
		case fq.kind == "fields":
			path = "/loki/api/v1/detected_fields"
		case fq.kind == "values":
			path = "/loki/api/v1/detected_field/" + fq.field + "/values"
		case mr.instant:
			path = "/loki/api/v1/query"
		default:
			path = "/loki/api/v1/query_range"
		}
		req := httptest.NewRequest(http.MethodGet, path+"?"+mr.form.Encode(), nil)
		req.Header.Set("X-Scope-OrgID", "0")
		if mr.kind == "log" {
			req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
		}
		rec := httptest.NewRecorder()
		front.mu.Lock()
		front.sent, front.missing = nil, nil
		front.mu.Unlock()
		mux.ServeHTTP(rec, req)
		front.mu.Lock()
		defer front.mu.Unlock()
		return metricHandlerRun{status: rec.Code, body: rec.Body.String(), sent: uniqueSorted(front.sent), missing: uniqueSorted(front.missing)}
	}
}

func runFMHandler(t *testing.T, prof diffProfile, mode fmMode, fq fmQuery, mr metricRequest, front *metricFront) metricHandlerRun {
	t.Helper()
	serve := newFMProxy(t, prof, mode, front)
	defer serve(fmQuery{}, metricRequest{})
	return serve(fq, mr)
}

const fmTenantKey = "field-mapping-label-mode"

// recordFMAnswers runs the oracles: Loki over every request of both references, and every request the handler sends
// under every profile and mode through VictoriaLogs' query server of each version. The pinned answers of the modes
// without a reference are kept unless LVP_FIELD_MAPPING_PIN=1 records them again (at the base).
func recordFMAnswers(t *testing.T, queries []fmQuery, entries []fmEntry) {
	t.Helper()
	root := filepath.Join("..", "..")
	var prior map[string]*fmRecord
	if os.Getenv("LVP_FIELD_MAPPING_PIN") != "1" {
		prior = readFMAnswers(t)
	}
	recs := make([]*fmRecord, len(queries))
	for i, fq := range queries {
		recs[i] = &fmRecord{ID: fq.id(), Inputs: fq.inputsHash(entries), Loki: map[string]map[string]metricLokiAnswer{}, VL: map[string]map[string]metricVLAnswer{},
			Pinned: map[string]map[string]map[string]string{}}
	}
	for _, ref := range fmRefs {
		var sets []oraclerun.EngineDataSet
		for i, fq := range queries {
			recs[i].Loki[ref] = map[string]metricLokiAnswer{}
			if fq.kind != "metric" {
				recs[i].Loki[ref][fq.reqs[0].kind] = fmLokiAnswer(t, fq, ref, entries)
				continue
			}
			set := oraclerun.EngineDataSet{Streams: fmLokiStreams(entries, ref)}
			for _, r := range fq.reqs {
				set.Requests = append(set.Requests, oraclerun.EngineRequest{ID: strconv.Itoa(i) + "/" + r.kind, Instant: r.instant, Form: r.form.Encode()})
			}
			sets = append(sets, set)
		}
		loki, err := oraclerun.LokiEngine(filepath.Join(root, "test", "oracle", "loki"), sets)
		if err != nil {
			t.Fatal(err)
		}
		for i, fq := range queries {
			if fq.kind != "metric" {
				continue
			}
			for _, r := range fq.reqs {
				a := loki[strconv.Itoa(i)+"/"+r.kind]
				recs[i].Loki[ref][r.kind] = metricLokiAnswer{Status: a.Status, Body: a.Body, Error: a.Error}
			}
		}
	}
	tenant := diffTenant(fmTenantKey, fmTenantKey)
	for _, prof := range diffProfiles() {
		srv, err := oraclerun.StartVLServe(filepath.Join(root, "test", "oracle", "vl"), prof.modfile, []oraclerun.ServeDataSet{{Tenant: tenant, Rows: fmVLRows(entries)}})
		if err != nil {
			t.Fatal(err)
		}
		for i, fq := range queries {
			front := &metricFront{version: prof.version, upstream: srv.URL, tenant: tenant, recorded: map[string]metricVLAnswer{}}
			recs[i].Pinned[prof.version] = map[string]map[string]string{}
			for _, mode := range fmModes() {
				recs[i].Pinned[prof.version][mode.name()] = map[string]string{}
				for _, r := range fq.reqs {
					run := runFMHandler(t, prof, mode, fq, r, front)
					runFMHandler(t, prof, mode, fq, r, front)
					if mode.ref == "" {
						recs[i].Pinned[prof.version][mode.name()][r.kind] = canonicalJLBody(run.body)
					}
				}
			}
			recs[i].VL[prof.version] = front.recorded
		}
		srv.Stop()
	}
	for i := range recs {
		if p := prior[recs[i].ID]; p != nil && p.Inputs == recs[i].Inputs {
			recs[i].Pinned = p.Pinned
		}
	}
	writeFMAnswers(t, recs)
	t.Logf("recorded %s: %d queries", fmAnswersPath, len(recs))
}

// compareFMMeta holds a fields or values answer to Loki's derived one.
func compareFMMeta(fq fmQuery, loki metricLokiAnswer, run metricHandlerRun) metricDiff {
	d := metricDiff{classes: map[string]string{}}
	if run.status != loki.Status {
		d.add(fmt.Sprintf("answers status %d where Loki answers %d", run.status, loki.Status), proxyErrorText(run.body))
		return d
	}
	var want []string
	_ = json.Unmarshal(loki.Body, &want)
	var got []string
	if fq.kind == "fields" {
		var out struct {
			Fields []struct {
				Label string `json:"label"`
			} `json:"fields"`
		}
		if err := json.Unmarshal([]byte(run.body), &out); err != nil {
			d.add("the answer does not decode", err.Error())
			return d
		}
		for _, f := range out.Fields {
			got = append(got, f.Label)
		}
		got = fmCompared(got)
	} else {
		var out struct {
			Values []string `json:"values"`
		}
		if err := json.Unmarshal([]byte(run.body), &out); err != nil {
			d.add("the answer does not decode", err.Error())
			return d
		}
		got = out.Values
	}
	slices.Sort(got)
	if got == nil {
		got = []string{}
	}
	if !slices.Equal(got, want) {
		d.add("lists other "+fq.kind, fmt.Sprintf("%v, Loki %v", got, want))
	}
	return d
}

// The rows that still differ from Loki, by class (docs/KNOWN_ISSUES.md, registry case
// semantics/field-mapping-label-modes).
const (
	// fmKnownAliasBase: in the alias mode the -label-style name of a mapped field (k8s_pod_name) is a query name of
	// the field (it reads the field before and after a parser), where the pre-migration Loki has no such stream label:
	// its k8s_pod_name is the parsed key, and no k8s_pod_name_extracted exists. Changing it changes the documented
	// alias mode rule that every name of a mapping is a query name.
	fmKnownAliasBase = "alias mode: the -label-style name of a mapped field reads the field after a parser"
	// fmKnownBothNative: in the both mode a stream that stores pod itself (never renamed) also carries k8s_pod_name
	// in the answer (the both layer adds every name of the field the answer label pod reads); a Loki storing both
	// names holds pod alone on such a stream. Its parsed k8s_pod_name keeps its name (only a stream that stores the
	// mapped field collides with the -label-style name).
	fmKnownBothNative = "both mode: a stream storing the alias itself gains the -label-style name"
	// fmKnownBothBaseGrouping: in the both mode a grouping by the -label-style name fills the field from the parsed
	// alias key (pod) where the stream lacks the field, not from the parsed k8s_pod_name key. These rows (and the alias
	// mode's sum by (k8s_pod_name)) also hold a defect main has without a parser: a query_range at step = range
	// answers a grouping by the -label-style name under the answer name (pod), the other request kinds under
	// k8s_pod_name.
	fmKnownBothBaseGrouping = "both mode: a grouping by the -label-style name reads the parsed alias where the stream lacks the field"
)

// fmKnown are the rows that still differ from Loki, by mode and query id, with their class.
var fmKnown = map[string]string{
	`alias log {service_name="fm-json"} | json | k8s_pod_name="kl-0-1"`:                                                                     fmKnownAliasBase,
	`alias log {service_name="fm-json"} | json | k8s_pod_name_extracted!=""`:                                                                fmKnownAliasBase,
	`alias log {service_name="fm-logfmt"} | logfmt | k8s_pod_name="kl-0-1"`:                                                                 fmKnownAliasBase,
	`alias log {service_name="fm-logfmt"} | logfmt | k8s_pod_name_extracted!=""`:                                                            fmKnownAliasBase,
	`alias metric sum by (k8s_pod_name) (count_over_time({service_name="fm-json"} | json [5m]))`:                                            fmKnownAliasBase,
	`alias metric sum by (k8s_pod_name) (count_over_time({service_name="fm-logfmt"} | logfmt [5m]))`:                                        fmKnownAliasBase,
	`alias metric sum by (k8s_pod_name_extracted) (count_over_time({service_name="fm-json"} | json [5m]))`:                                  fmKnownAliasBase,
	`alias metric sum by (k8s_pod_name_extracted) (count_over_time({service_name="fm-json"} | json | k8s_pod_name_extracted!="" [5m]))`:     fmKnownAliasBase,
	`alias metric sum by (k8s_pod_name_extracted) (count_over_time({service_name="fm-logfmt"} | logfmt [5m]))`:                              fmKnownAliasBase,
	`alias metric sum by (k8s_pod_name_extracted) (count_over_time({service_name="fm-logfmt"} | logfmt | k8s_pod_name_extracted!="" [5m]))`: fmKnownAliasBase,
	`both log {service_name="fm-json"} | json`:                                                                                              fmKnownBothNative,
	`both log {service_name="fm-json"} | json | pod!="pod-1"`:                                                                               fmKnownBothNative,
	`both log {service_name="fm-json"} | json | pod=~"npod-3|line-2-.*"`:                                                                    fmKnownBothNative,
	`both log {service_name="fm-json"} | json | pod_extracted=~".+-1"`:                                                                      fmKnownBothNative,
	`both log {service_name="fm-logfmt"} | logfmt`:                                                                                          fmKnownBothNative,
	`both log {service_name="fm-logfmt"} | logfmt | pod!="pod-1"`:                                                                           fmKnownBothNative,
	`both log {service_name="fm-logfmt"} | logfmt | pod=~"npod-3|line-2-.*"`:                                                                fmKnownBothNative,
	`both log {service_name="fm-logfmt"} | logfmt | pod_extracted=~".+-1"`:                                                                  fmKnownBothNative,
	`both metric sum by (k8s_pod_name) (count_over_time({service_name="fm-json"} | json [5m]))`:                                             fmKnownBothBaseGrouping,
	`both metric sum by (k8s_pod_name) (count_over_time({service_name="fm-logfmt"} | logfmt [5m]))`:                                         fmKnownBothBaseGrouping,
}

// TestFieldMappingLabelModesMatchLoki replays the recorded answers through the handlers: the unset and original
// modes answer byte for byte what they answered at the base; alias and both answer what their Loki reference
// answers, apart from the rows listed in fmKnown.
//
// conformance: field-mapping-label-names, semantics/field-mapping-label-modes
func TestFieldMappingLabelModesMatchLoki(t *testing.T) {
	if testing.Short() && os.Getenv("CI") == "" && os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("runs every request on a fresh proxy; run without -short")
	}
	t.Cleanup(querycorpus.PauseRecording())
	entries := fmFixture()
	queries := fmQueries()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordFMAnswers(t, queries, entries)
	}
	answers := readFMAnswers(t)
	report := os.Getenv("LVP_FIELD_MAPPING_REPORT") == "1"
	matched, runs := map[string]int{}, map[string]int{}
	for _, fq := range queries {
		rec := answers[fq.id()]
		if rec == nil || rec.Inputs != fq.inputsHash(entries) {
			t.Fatalf("the recorded answers of %s are stale: record them again with LVP_RUN_ORACLES=1", fq.id())
		}
		for _, prof := range diffProfiles() {
			for _, mode := range fmModes() {
				differs := map[string]string{}
				for _, mr := range fq.reqs {
					run := runFMHandler(t, prof, mode, fq, mr, &metricFront{version: prof.version, answers: rec.VL[prof.version]})
					if missing := answerRequests(run.missing); len(missing) > 0 {
						t.Fatalf("%s %s %s %s: VictoriaLogs request %s was not recorded: record them again with LVP_RUN_ORACLES=1", prof.name, mode.name(), mr.kind, fq.id(), missing[0])
					}
					if os.Getenv("LVP_FIELD_MAPPING_SENT") == "1" && prof.name == diffProfiles()[0].name && mr.kind != "step<range" && mr.kind != "step>range" {
						for _, k := range run.sent {
							u, _ := url.ParseQuery(k[strings.Index(k, "?")+1:])
							if q := u.Get("query"); q != "" {
								if len(q) > 900 {
									q = "…" + q[len(q)-900:]
								}
								t.Logf("SENT %s %s %s | %s", mode.name(), mr.kind, fq.id(), q)
							}
						}
					}
					if mode.ref == "" {
						if want := rec.Pinned[prof.version][mode.name()][mr.kind]; canonicalJLBody(run.body) != want {
							t.Errorf("%s %s %s %s: the answer changed:\n got %.2000s\nwant %.2000s", prof.name, mode.name(), mr.kind, fq.id(), canonicalJLBody(run.body), want)
						}
						continue
					}
					var d metricDiff
					switch fq.kind {
					case "log":
						d = compareJLLog(rec.Loki[mode.ref][mr.kind], run)
					case "metric":
						d = compareMetric(rec.Loki[mode.ref][mr.kind], run)
					default:
						d = compareFMMeta(fq, rec.Loki[mode.ref][mr.kind], run)
					}
					for c, ex := range d.classes {
						differs[mr.kind+": "+c] = ex
					}
				}
				if mode.ref == "" {
					continue
				}
				key := mode.name()
				runs[key]++
				known := fmKnown[key+" "+fq.id()]
				switch {
				case len(differs) == 0:
					matched[key]++
					if known != "" && !report {
						t.Errorf("%s %s %s matches Loki: remove it from fmKnown (%s)", prof.name, key, fq.id(), known)
					}
				case report:
					for _, c := range sortedKeysOf(differs) {
						t.Logf("DIFF %s %s %s: %s: %.600s", prof.name, key, fq.id(), c, differs[c])
					}
				case known == "":
					for _, c := range sortedKeysOf(differs) {
						t.Errorf("%s %s %s: %s: %.600s", prof.name, key, fq.id(), c, differs[c])
					}
				}
			}
		}
	}
	for _, k := range sortedKeysOf(runs) {
		t.Logf("%s: %d of %d query x profile runs match Loki", k, matched[k], runs[k])
	}
}
