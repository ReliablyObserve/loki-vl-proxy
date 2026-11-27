//go:build e2e

package e2e_fleet

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"
)

// The mixed -field-mapping-label-mode ring (compose profile "labelmode"):
// a and b list aliases, c keeps the default listing (flag unset), d lists
// both names. Every replica has the same -field-mapping.
var labelModeRing = []struct {
	replica
	mode string
}{
	{replica{"proxy-labelmode-a", envOr("FLEET_PROXY_LABELMODE_A_URL", "http://localhost:3111")}, "alias"},
	{replica{"proxy-labelmode-b", envOr("FLEET_PROXY_LABELMODE_B_URL", "http://localhost:3112")}, "alias"},
	{replica{"proxy-labelmode-c", envOr("FLEET_PROXY_LABELMODE_C_URL", "http://localhost:3113")}, "original"},
	{replica{"proxy-labelmode-d", envOr("FLEET_PROXY_LABELMODE_D_URL", "http://localhost:3114")}, "both"},
}

// labelModeMapped are the names of the two mapped fields; which of them a
// listing holds depends on the mode.
var labelModeMapped = map[string][]string{
	"alias":    {"namespace", "pod"},
	"original": {"k8s_namespace_name", "k8s_pod_name"},
	"both":     {"k8s_namespace_name", "k8s_pod_name", "namespace", "pod"},
}

// labelModeAnswerName is the one name stream and series labels carry for
// k8s.namespace.name in each mode.
var labelModeAnswerName = map[string]string{"alias": "namespace", "original": "k8s_namespace_name", "both": "namespace"}

// TestFleet_FieldMappingLabelModesShareOnlyTheirOwnAnswers: replicas of a
// peer ring that list different names for the same field mappings (a rollout
// that changes -field-mapping-label-mode one replica at a time) never serve
// each other's answers, on any tier, and replicas of one mode share theirs:
//   - a (alias) answers /labels, label values by both names, detected_labels,
//     volume by the alias and a log query from VictoriaLogs; b (alias) then
//     answers the same with no VictoriaLogs call anywhere in the fleet and no
//     lookup that missed every tier (memory copy pushed to it, or a peer);
//   - c (default listing) and d (both) answer their own mode's names, never
//     a's, and make their own VictoriaLogs reads;
//   - the other way round, on a selector c answered first, a and b still
//     answer alias names;
//   - a replica that lost its memory and disk answers alias names again.
//
// conformance: field-mapping-label-names, semantics/field-mapping-label-modes
func TestFleet_FieldMappingLabelModesShareOnlyTheirOwnAnswers(t *testing.T) {
	services := make([]string, 0, len(labelModeRing))
	reps := make([]replica, 0, len(labelModeRing))
	for _, r := range labelModeRing {
		services = append(services, r.name)
		reps = append(reps, r.replica)
	}
	fleetCompose(t, append([]string{"--profile", "labelmode", "up", "-d", "--no-deps", "--no-build", "--wait", "--wait-timeout", "120"}, services...)...)
	t.Cleanup(func() {
		fleetCompose(t, append([]string{"--profile", "labelmode", "rm", "--stop", "--force", "--volumes"}, services...)...)
	})
	for _, r := range reps {
		waitProxyReady(t, r.addr)
	}
	factor := 0
	for _, r := range reps {
		f := int(scrapeMetrics(t, r.addr).sum("loki_vl_proxy_peer_cache_replication_factor", nil))
		factor = max(factor, f, 1)
	}

	// Rows older than -max-metadata-cache-freshness (24h), so every listing
	// is settled and kept, and older than the peer cache proof's fixture
	// (26-34h), whose windows they must stay out of: every request here is
	// scoped by its own service names anyway.
	run := fmt.Sprintf("%d", time.Now().UnixNano())
	end := time.Now().Add(-49 * time.Hour).Truncate(time.Hour)
	start := end.Add(-3 * time.Hour)
	namespaces := []string{"lm-" + run + "-x", "lm-" + run + "-y"}
	apps := []string{"lm-" + run + "-first-a", "lm-" + run + "-first-c"}
	var rows strings.Builder
	for i := 0; i < 60; i++ {
		for s, svc := range apps {
			row, _ := json.Marshal(map[string]string{
				"_time":              end.Add(-30*time.Minute + time.Duration(i)*30*time.Second).Format(time.RFC3339Nano),
				"_msg":               fmt.Sprintf("tick %d", i),
				"service.name":       svc,
				"k8s.namespace.name": namespaces[s],
				"k8s.pod.name":       fmt.Sprintf("%s-pod-%d", namespaces[s], i%3),
			})
			rows.Write(row)
			rows.WriteByte('\n')
		}
	}
	resp, err := http.Post(fleetVLURL+"/insert/jsonline?_stream_fields=service.name,k8s.namespace.name,k8s.pod.name",
		"application/stream+json", strings.NewReader(rows.String()))
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		t.Fatalf("ingest: %d", resp.StatusCode)
	}
	if resp, err := http.Post(fleetVLURL+"/internal/force_flush", "text/plain", nil); err == nil {
		resp.Body.Close()
	}
	window := fleetRange(start, end)
	deadline := time.Now().Add(60 * time.Second)
	for {
		v := url.Values{"query": {fmt.Sprintf(`"service.name":=%q | stats count() as n`, apps[1])}, "start": window["start"], "end": window["end"]}
		body := fleetGet(t, fleetVLURL, "/select/logsql/stats_query?"+v.Encode())
		if strings.Contains(string(body), `"60"`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("VictoriaLogs does not hold the rows of %s: %s", run, body)
		}
		time.Sleep(500 * time.Millisecond)
	}

	requests := func(svc string) map[string]string {
		w := fleetRange(start, end)
		w.Set("query", fmt.Sprintf(`{service_name=%q}`, svc))
		q := w.Encode()
		vol := fleetRange(start, end)
		vol.Set("query", fmt.Sprintf(`{service_name=%q}`, svc))
		vol.Set("targetLabels", "namespace")
		logs := fleetRange(start, end)
		logs.Set("query", fmt.Sprintf(`{service_name=%q}`, svc))
		logs.Set("limit", "1000")
		return map[string]string{
			"labels":                  "/loki/api/v1/labels?" + q,
			"namespace_values":        "/loki/api/v1/label/namespace/values?" + q,
			"k8s_namespace_values":    "/loki/api/v1/label/k8s_namespace_name/values?" + q,
			"detected_labels":         "/loki/api/v1/detected_labels?" + q,
			"volume_by_namespace":     "/loki/api/v1/index/volume?" + vol.Encode(),
			"logs_by_service_streams": "/loki/api/v1/query_range?" + logs.Encode(),
		}
	}
	names := []string{"labels", "namespace_values", "k8s_namespace_values", "detected_labels", "volume_by_namespace", "logs_by_service_streams"}

	type reply struct {
		body   string
		header string
		d      fleetCounters
		where  string // VictoriaLogs calls per replica and route, for messages
	}
	ask := func(rep replica, path string) reply {
		t.Helper()
		before := scrapeAll(t, reps)
		resp, err := fleetHTTP.Get(rep.addr + path)
		if err != nil {
			t.Fatalf("%s %s: %v", rep.name, path, err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s %s: %d %.400s", rep.name, path, resp.StatusCode, raw)
		}
		after := settleFleet(t, reps)
		d := after[rep.name].minus(before[rep.name])
		where := ""
		for _, r := range reps {
			f := after[r.name].minus(before[r.name])
			d.fleetVLSelect += f.vlSelect - f.vlBackground
			if f.vlSelect != 0 {
				where += fmt.Sprintf(" %s: %.0f (background %.0f) %v;", r.name, f.vlSelect, f.vlBackground, f.vlRoutes)
			}
		}
		return reply{canonicalJSON(t, rep.name+" "+path, raw), resp.Header.Get("X-Loki-VL-Backend-Calls"), d, where}
	}

	// checkMode holds an answer to the names of its replica's mode.
	checkMode := func(rep replica, mode, svc, name, body string) {
		t.Helper()
		label := fmt.Sprintf("%s (%s) %s of %s", rep.name, mode, name, svc)
		ns := namespaces[slices.Index(apps, svc)]
		switch name {
		case "labels":
			var out struct{ Data []string }
			_ = json.Unmarshal([]byte(body), &out)
			if got := mappedOf(out.Data); !slices.Equal(got, labelModeMapped[mode]) || !slices.Contains(out.Data, "service_name") {
				t.Errorf("%s: %v, want the mapped names %v and service_name", label, out.Data, labelModeMapped[mode])
			}
		case "namespace_values", "k8s_namespace_values":
			var out struct{ Data []string }
			_ = json.Unmarshal([]byte(body), &out)
			if !slices.Equal(out.Data, []string{ns}) {
				t.Errorf("%s: %v, want [%s]", label, out.Data, ns)
			}
		case "detected_labels":
			var out struct {
				DetectedLabels []struct{ Label string } `json:"detectedLabels"`
			}
			_ = json.Unmarshal([]byte(body), &out)
			var got []string
			for _, l := range out.DetectedLabels {
				got = append(got, l.Label)
			}
			sort.Strings(got)
			if m := mappedOf(got); !slices.Equal(m, labelModeMapped[mode]) {
				t.Errorf("%s: %v, want the mapped names %v", label, got, labelModeMapped[mode])
			}
		case "volume_by_namespace":
			var out struct {
				Data struct {
					Result []struct{ Metric map[string]string }
				}
			}
			_ = json.Unmarshal([]byte(body), &out)
			if mode == "original" {
				// docs/KNOWN_ISSUES.md: the default listing answers a volume by
				// a non-winning mapping name with no series, as on main.
				if len(out.Data.Result) != 0 {
					t.Errorf("%s: %v, want no series", label, out.Data.Result)
				}
				return
			}
			if len(out.Data.Result) != 1 || !reflect.DeepEqual(out.Data.Result[0].Metric, map[string]string{"namespace": ns}) {
				t.Errorf("%s: %v, want one series {namespace=%q}", label, out.Data.Result, ns)
			}
		case "logs_by_service_streams":
			var out struct {
				Data struct {
					Result []struct {
						Stream map[string]string
						Values [][]string
					}
				}
			}
			_ = json.Unmarshal([]byte(body), &out)
			lines := 0
			for _, s := range out.Data.Result {
				lines += len(s.Values)
				if s.Stream[labelModeAnswerName[mode]] != ns {
					t.Errorf("%s: stream %v lacks %s=%q", label, s.Stream, labelModeAnswerName[mode], ns)
				}
				for _, other := range []string{"namespace", "k8s_namespace_name"} {
					if _, ok := s.Stream[other]; ok && other != labelModeAnswerName[mode] {
						t.Errorf("%s: stream %v carries a second name of the field", label, s.Stream)
					}
				}
			}
			if len(out.Data.Result) != 3 || lines != 60 {
				t.Errorf("%s: %d streams, %d lines; want 3 and 60", label, len(out.Data.Result), lines)
			}
		}
	}

	a, b, c, d := labelModeRing[0], labelModeRing[1], labelModeRing[2], labelModeRing[3]

	// 1. a computes the alias answers; b, the same mode, reads them from the
	// fleet's cache tiers.
	first := map[string]string{}
	reqs := requests(apps[0])
	var peerReads float64
	for _, name := range names {
		got := ask(a.replica, reqs[name])
		checkMode(a.replica, a.mode, apps[0], name, got.body)
		if got.d.fleetVLSelect == 0 {
			t.Errorf("%s on %s: a cold request with no VictoriaLogs call", name, a.name)
		}
		first[name] = got.body
		t.Logf("cold %s on %s (%s): %s VictoriaLogs calls (header), %.0f in the fleet", name, a.name, a.mode, got.header, got.d.fleetVLSelect)
	}
	for _, name := range names {
		got := ask(b.replica, reqs[name])
		if got.body != first[name] {
			t.Errorf("%s on %s (alias) differs from a's:\n got: %.400s\nwant: %.400s", name, b.name, got.body, first[name])
		}
		if name == "volume_by_namespace" {
			// A volume answer is kept 10 s, under -peer-write-through-min-ttl
			// (30 s): it is never pushed, and another replica reuses it only
			// when the key's owner computed it. Its isolation is the answer.
			continue
		}
		if got.d.fleetVLSelect != 0 || got.header != "0" {
			t.Errorf("%s on %s: %.0f VictoriaLogs calls in the fleet (header %s); a's answer of the same mode is kept:%s", name, b.name, got.d.fleetVLSelect, got.header, got.where)
		}
		if got.d.missedAll != 0 {
			t.Errorf("%s on %s: %.0f lookups missed every tier (%s)", name, b.name, got.d.missedAll, got.d)
		}
		peerReads += got.d.peerHits
		t.Logf("same mode %s on %s: peer hits %.0f, memory hits %.0f, disk hits %.0f", name, b.name, got.d.peerHits, got.d.tierL1Hits, got.d.tierL2Hits)
	}
	if factor < 2 && peerReads < 1 {
		t.Errorf("b read none of a's answers from a peer; with one copy per key some of them are owned by c or d")
	}

	// 2. c (default listing) and d (both) answer their own names on the
	// same requests, never a's answers.
	for _, rep := range []struct {
		replica
		mode string
	}{c, d} {
		calls := 0.0
		for _, name := range names {
			got := ask(rep.replica, reqs[name])
			checkMode(rep.replica, rep.mode, apps[0], name, got.body)
			// Listings differ in every other mode; answers carry the alias in
			// both, so only the default listing's streams differ from a's.
			if name == "labels" || name == "detected_labels" || (name == "logs_by_service_streams" && rep.mode == "original") {
				if got.body == first[name] {
					t.Errorf("%s on %s (%s) answered a's alias answer", name, rep.name, rep.mode)
				}
			}
			calls += got.d.fleetVLSelect
		}
		t.Logf("other mode on %s (%s): %.0f VictoriaLogs calls in the fleet for the %d requests a answered", rep.name, rep.mode, calls, len(names))
	}

	// 3. The other way round: c answers a new selector first, then a and b.
	reqs2 := requests(apps[1])
	for _, name := range names {
		checkMode(c.replica, c.mode, apps[1], name, ask(c.replica, reqs2[name]).body)
	}
	second := map[string]string{}
	for _, name := range names {
		got := ask(a.replica, reqs2[name])
		checkMode(a.replica, a.mode, apps[1], name, got.body)
		second[name] = got.body
	}
	for _, name := range names {
		got := ask(b.replica, reqs2[name])
		if got.body != second[name] {
			t.Errorf("%s on %s after c and a differs from a's:\n got: %.400s\nwant: %.400s", name, b.name, got.body, second[name])
		}
		if name != "volume_by_namespace" && (got.d.fleetVLSelect != 0 || got.header != "0") {
			t.Errorf("%s on %s after c and a: %.0f VictoriaLogs calls in the fleet (header %s):%s", name, b.name, got.d.fleetVLSelect, got.header, got.where)
		}
	}

	// 4. a loses memory and disk: it answers alias names again (rebuilt from
	// its peers' alias copies or from VictoriaLogs, never c's or d's).
	fleetCompose(t, "--profile", "labelmode", "rm", "--stop", "--force", "--volumes", a.name)
	fleetCompose(t, "--profile", "labelmode", "up", "-d", "--no-deps", "--no-build", "--wait", "--wait-timeout", "120", a.name)
	waitProxyReady(t, a.addr)
	rebuilt := 0.0
	for _, name := range names {
		got := ask(a.replica, reqs[name])
		checkMode(a.replica, a.mode, apps[0], name, got.body)
		if got.body != first[name] {
			t.Errorf("%s on %s after the disk loss differs from its first answer:\n got: %.400s\nwant: %.400s", name, a.name, got.body, first[name])
		}
		rebuilt += got.d.fleetVLSelect
	}
	t.Logf("a after losing memory and disk: %.0f VictoriaLogs calls in the fleet for the %d requests (replication factor %d)", rebuilt, len(names), factor)
}

// mappedOf returns the names of the two mapped fields a listing holds, sorted.
func mappedOf(names []string) []string {
	var out []string
	for _, n := range names {
		switch n {
		case "namespace", "pod", "k8s_namespace_name", "k8s_pod_name":
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out
}
