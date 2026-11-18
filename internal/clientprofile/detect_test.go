package clientprofile

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// capturedSet is one testdata/captured-*.json file: the requests Grafana sent the proxy on the compose stack for
// every Explore and Logs Drilldown request kind, with the surface each one comes from.
type capturedSet struct {
	Grafana   string `json:"grafana"`
	Drilldown string `json:"drilldown"`
	Requests  []struct {
		Flow    string            `json:"flow"`
		Method  string            `json:"method"`
		Path    string            `json:"path"`
		Headers map[string]string `json:"headers"`
		Surface string            `json:"surface"`
	} `json:"requests"`
}

func readCaptured(t *testing.T) map[string]capturedSet {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("testdata", "captured-*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no captured header sets: %v", err)
	}
	out := map[string]capturedSet{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var s capturedSet
		if err := json.Unmarshal(b, &s); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		out[f] = s
	}
	return out
}

// Every request Grafana sent from Explore and from Logs Drilldown, on each captured release pair, gets its surface:
// Drilldown on every Drilldown request (queries and resource calls alike), never on an Explore one, and Explore on
// Explore's queries and resource calls. Grafana 12's Explore query carries no Source tag and no page, so it reads as
// another datasource request (its log volume query and resource calls still read as Explore). The Grafana and Loki
// datasource versions are the User-Agent's; no request names the Drilldown app's version.
//
// conformance: drilldown/client-detection-captured-header-sets
func TestDetectCapturedHeaderSets(t *testing.T) {
	sets := readCaptured(t)
	if len(sets) < 3 {
		t.Fatalf("%d captured release pairs, want the pinned, previous and previous-family ones", len(sets))
	}
	for file, set := range sets {
		grafana := ParseVersion(set.Grafana)
		if !grafana.Known {
			t.Fatalf("%s: Grafana version %q", file, set.Grafana)
		}
		surfaces := map[string]int{}
		for _, req := range set.Requests {
			h := http.Header{}
			for k, v := range req.Headers {
				h.Set(k, v)
			}
			got := Detect(h)
			if got.Surface.Label() != req.Surface {
				t.Errorf("%s %s %s %s: surface %s, want %s (headers %v)", file, req.Flow, req.Method, req.Path, got.Surface.Label(), req.Surface, req.Headers)
			}
			if got.Grafana != grafana {
				t.Errorf("%s %s %s: Grafana %s, want %s", file, req.Flow, req.Path, got.Grafana.Label(), set.Grafana)
			}
			if got.Drilldown.Known {
				t.Errorf("%s %s %s: a Drilldown version %s, but no captured header carries one", file, req.Flow, req.Path, got.Drilldown.Label())
			}
			if strings.Contains(req.Headers["User-Agent"], " loki/") != got.Datasource.Known {
				t.Errorf("%s %s %s: datasource version %s from %q", file, req.Flow, req.Path, got.Datasource.Label(), req.Headers["User-Agent"])
			}
			surfaces[req.Surface]++
		}
		if surfaces["drilldown"] == 0 || surfaces["explore"] == 0 {
			t.Errorf("%s: captured surfaces %v, want Drilldown and Explore requests", file, surfaces)
		}
	}
}

// The captured fixtures hold no secret and no user data: no X-Grafana-Id (a signed ID token), no Authorization or
// cookie, and of X-Grafana-Referer only the page's path class (the full URL holds the user's query and time range).
func TestCapturedHeaderSetsHoldNoSecrets(t *testing.T) {
	for file, set := range readCaptured(t) {
		for _, req := range set.Requests {
			for k, v := range req.Headers {
				switch lk := strings.ToLower(k); {
				case lk == "x-grafana-id", lk == "authorization", lk == "cookie", strings.Contains(lk, "token"), lk == "x-grafana-user":
					t.Errorf("%s: header %s kept", file, k)
				case lk == "x-grafana-referer" && strings.ContainsAny(v, "?#&"):
					t.Errorf("%s: a full referer kept: %q", file, v)
				}
				if strings.Contains(v, "eyJ") { // a JWT's base64 header
					t.Errorf("%s: %s holds a token: %q", file, k, v)
				}
			}
		}
	}
}

func TestDetectSurfaces(t *testing.T) {
	for _, tc := range []struct {
		name    string
		headers map[string]string
		want    string
	}{
		{"curl", map[string]string{"User-Agent": "curl/8.7.1"}, "api"},
		{"no headers", nil, "api"},
		{"logcli", map[string]string{"User-Agent": "loki-logcli/3.7.10"}, "api"},
		{"drilldown tag without UA", map[string]string{"X-Query-Tags": "Source=grafana-lokiexplore-app"}, "drilldown"},
		{"drilldown tag any case", map[string]string{"User-Agent": "Grafana/13.3.1", "X-Query-Tags": "source=Grafana-LokiExplore-App"}, "drilldown"},
		{"drilldown tag among others", map[string]string{"User-Agent": "Grafana/13.3.1", "X-Query-Tags": "Feature=x, Source=grafana-lokiexplore-app"}, "drilldown"},
		{"quoted tag", map[string]string{"User-Agent": "Grafana/13.3.1", "X-Query-Tags": `Source="grafana-explore"`}, "explore"},
		{"explore volume", map[string]string{"User-Agent": "Grafana/13.3.1", "X-Query-Tags": "Source=logvolhist"}, "explore"},
		{"explore resource call", map[string]string{"User-Agent": "Grafana/13.3.1", "X-Grafana-Referer": "http://g/explore?panes=x"}, "explore"},
		{"explore under a sub path", map[string]string{"User-Agent": "Grafana/13.3.1", "X-Grafana-Referer": "https://h/grafana/explore"}, "explore"},
		{"drilldown resource call by page", map[string]string{"User-Agent": "Grafana/13.3.1", "X-Grafana-Referer": "http://g/a/grafana-lokiexplore-app/explore/service/x/logs"}, "drilldown"},
		{"dashboard resource call", map[string]string{"User-Agent": "Grafana/13.3.1", "X-Grafana-Referer": "http://g/d/abc/my-dash?var-x=explore"}, "datasource"},
		{"dashboard named explore", map[string]string{"User-Agent": "Grafana/13.3.1", "X-Grafana-Referer": "http://g/d/abc/explore-things"}, "datasource"},
		{"alerting query", map[string]string{"User-Agent": "Grafana/13.3.1 (linux; amd64) loki/13.2.5"}, "datasource"},
		{"grafana header only", map[string]string{"X-Grafana-Org-Id": "1"}, "datasource"},
		{"unknown source", map[string]string{"User-Agent": "Grafana/13.3.1", "X-Query-Tags": "Source=grafana-dashboard"}, "datasource"},
		{"source tag from a script", map[string]string{"X-Query-Tags": "Source=my-script"}, "api"},
	} {
		h := http.Header{}
		for k, v := range tc.headers {
			h.Set(k, v)
		}
		if got := Detect(h).Surface.Label(); got != tc.want {
			t.Errorf("%s: surface %s, want %s", tc.name, got, tc.want)
		}
	}
}

func TestDetectVersions(t *testing.T) {
	for _, tc := range []struct {
		ua, grafana, datasource string
	}{
		{"Grafana/13.3.1 (linux; arm64) loki/13.2.5", "13.3.1", "13.2.5"},
		{"Grafana/12.4.14", "12.4.14", "unknown"},
		{"grafana/11.6.6+security-01", "11.6.6", "unknown"},
		{"Mozilla/5.0 Grafana/13.3.1", "13.3.1", "unknown"},
		{"NotGrafana/13.3.1", "unknown", "unknown"},
		{"Grafana/13.3.1 (linux; arm64) loki/13.2.5 Grafana/9.9.9", "13.3.1", "13.2.5"},
		{"Grafana/13.3", "unknown", "unknown"},
		{"Grafana/" + strings.Repeat("9", 100), "unknown", "unknown"},
		{"x loki/1.2.3", "unknown", "1.2.3"},
		{"xloki/1.2.3", "unknown", "unknown"},
	} {
		p := Detect(http.Header{"User-Agent": {tc.ua}})
		if p.Grafana.Label() != tc.grafana || p.Datasource.Label() != tc.datasource {
			t.Errorf("%q: Grafana %s, datasource %s; want %s, %s", tc.ua, p.Grafana.Label(), p.Datasource.Label(), tc.grafana, tc.datasource)
		}
	}
}

// Detect allocates nothing on the request path.
func TestDetectAllocatesNothing(t *testing.T) {
	h := http.Header{
		"User-Agent":        {"Grafana/13.3.1 (linux; arm64) loki/13.2.5"},
		"X-Query-Tags":      {"Source=grafana-lokiexplore-app"},
		"X-Grafana-Referer": {"http://g/explore?x=1"},
	}
	if n := testing.AllocsPerRun(100, func() { _ = Detect(h) }); n != 0 {
		t.Fatalf("Detect allocates %v times", n)
	}
}

func BenchmarkDetect(b *testing.B) {
	h := http.Header{
		"User-Agent":        {"Grafana/13.3.1 (linux; arm64) loki/13.2.5"},
		"X-Query-Tags":      {"Source=grafana-lokiexplore-app"},
		"X-Grafana-Org-Id":  {"1"},
		"X-Grafana-Referer": {"http://127.0.0.1:3002/a/grafana-lokiexplore-app/explore/service/api/logs?from=now-1h"},
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Detect(h)
	}
}

// FuzzDetect: any header values give a profile whose labels come from the closed sets (a surface name, unknown or
// major.minor.patch), so nothing a client sends reaches a metric label or a log field as text; and a request that
// names Drilldown in its Source tag is always Drilldown.
func FuzzDetect(f *testing.F) {
	f.Add("Grafana/13.3.1 (linux; arm64) loki/13.2.5", "Source=grafana-lokiexplore-app", "http://g/a/grafana-lokiexplore-app/explore")
	f.Add("Grafana/12.4.14", "", "http://g/explore?panes=x")
	f.Add("curl/8", "Source=x,Source=y", "")
	f.Add("Grafana/"+strings.Repeat("1.", 300), strings.Repeat("Source=,", 200), "://"+strings.Repeat("/", 600))
	surfaces := map[string]bool{"unknown": true, "api": true, "explore": true, "drilldown": true, "datasource": true}
	f.Fuzz(func(t *testing.T, ua, tags, referer string) {
		h := http.Header{"User-Agent": {ua}, "X-Query-Tags": {tags}, "X-Grafana-Referer": {referer}}
		p := Detect(h)
		if !surfaces[p.Surface.Label()] {
			t.Fatalf("surface %q", p.Surface.Label())
		}
		for _, v := range []Version{p.Grafana, p.Datasource, p.Drilldown} {
			if !labelPattern.MatchString(v.Label()) {
				t.Fatalf("version label %q", v.Label())
			}
		}
		if p.Drilldown.Known {
			t.Fatal("no header names the Drilldown version")
		}
		if strings.EqualFold(querySource([]string{tags}), sourceDrilldown) && p.Surface != SurfaceDrilldown {
			t.Fatalf("tags %q: surface %s", tags, p.Surface.Label())
		}
	})
}
