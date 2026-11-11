package proxy

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// tenantAuthLokiAnswers holds Loki's answers to requests that differ only in
// their X-Scope-OrgID (and Authorization) header, recorded from a Loki with
// auth_enabled: true and querier.multi_tenant_queries_enabled: true: the first
// line names the Loki version and the configuration, each later line is one
// request and Loki's status, Content-Type and error body. Record them again with
//
//	LVP_ORACLE_RECORD_TENANT_AUTH=http://127.0.0.1:23100 go test ./internal/proxy -run TestRecordTenantAuthLokiAnswers -count=1
//
// against that Loki (testdata/tenant_auth_loki_config.yaml; its data is two
// pushed lines, one per tenant team-a and team-b). The stack's Loki runs with
// auth_enabled: false and ignores the header, so it cannot answer these.
const tenantAuthLokiAnswersFile = "testdata/tenant_auth_loki_answers.jsonl"

// tenantAuthWindow is the fixed window every recorded request reads.
const (
	tenantAuthStart = "1762860000"
	tenantAuthEnd   = "1762863600"
)

// tenantAuthHeader is one value of X-Scope-OrgID; Absent sends no header.
type tenantAuthHeader struct {
	Name   string
	Value  string
	Absent bool
}

// tenantAuthCase is one recorded request.
type tenantAuthCase struct {
	ID     string            `json:"id"`
	Method string            `json:"method"`
	Target string            `json:"target"`
	Header map[string]string `json:"header,omitempty"` // nil value map: no X-Scope-OrgID
	NoOrg  bool              `json:"no_org,omitempty"`
}

// tenantAuthAnswer is Loki's answer to one case.
type tenantAuthAnswer struct {
	tenantAuthCase
	Status      int    `json:"status"`
	ContentType string `json:"content_type"`
	// Body is the error body, kept for a status other than 200 and 204;
	// BodyRaw holds it instead when it is not valid UTF-8 (Loki quotes the
	// header's bytes as sent), since JSON would replace those bytes.
	Body    string `json:"body,omitempty"`
	BodyRaw []byte `json:"body_raw,omitempty"`
	// TenantLabel is whether a successful labels answer lists __tenant_id__.
	TenantLabel bool `json:"tenant_label,omitempty"`
}

type tenantAuthMeta struct {
	Loki   string `json:"loki"`
	Config string `json:"config"`
}

// tenantAuthRoutes is every route Grafana or a Loki client reads, with a query
// that names a stream so the request reaches the tenant check on each.
func tenantAuthRoutes() []struct{ Name, Method, Target string } {
	sel := "%7Bapp%3D~%22.%2B%22%7D" // {app=~".+"}
	win := "&start=" + tenantAuthStart + "&end=" + tenantAuthEnd
	return []struct{ Name, Method, Target string }{
		{"query_range", http.MethodGet, "/loki/api/v1/query_range?query=" + sel + win},
		{"query", http.MethodGet, "/loki/api/v1/query?query=count_over_time(" + sel + "%5B1h%5D)&time=" + tenantAuthEnd},
		{"series", http.MethodGet, "/loki/api/v1/series?match%5B%5D=" + sel + win},
		{"labels", http.MethodGet, "/loki/api/v1/labels?start=" + tenantAuthStart + "&end=" + tenantAuthEnd},
		{"label_values", http.MethodGet, "/loki/api/v1/label/app/values?start=" + tenantAuthStart + "&end=" + tenantAuthEnd},
		{"index_stats", http.MethodGet, "/loki/api/v1/index/stats?query=" + sel + win},
		{"volume", http.MethodGet, "/loki/api/v1/index/volume?query=" + sel + win},
		{"volume_range", http.MethodGet, "/loki/api/v1/index/volume_range?query=" + sel + win + "&step=60"},
		{"detected_fields", http.MethodGet, "/loki/api/v1/detected_fields?query=" + sel + win},
		{"detected_field_values", http.MethodGet, "/loki/api/v1/detected_field/app/values?query=" + sel + win},
		{"detected_labels", http.MethodGet, "/loki/api/v1/detected_labels?query=" + sel + win},
		{"patterns", http.MethodGet, "/loki/api/v1/patterns?query=" + sel + win + "&step=60"},
		{"tail", http.MethodGet, "/loki/api/v1/tail?query=" + sel},
	}
}

// tenantAuthHeaders is every X-Scope-OrgID the routes are asked with.
func tenantAuthHeaders() []tenantAuthHeader {
	return []tenantAuthHeader{
		{Name: "absent", Absent: true},
		{Name: "empty", Value: ""},
		{Name: "blank", Value: "   "},
		{Name: "single", Value: "team-a"},
		{Name: "multi", Value: "team-a|team-b"},
		{Name: "multi-trailing-bar", Value: "team-a|"},
		{Name: "multi-leading-bar", Value: "|team-a"},
		{Name: "multi-empty-middle", Value: "team-a||team-b"},
		{Name: "multi-duplicate", Value: "team-a|team-a"},
		{Name: "multi-invalid-member", Value: "team-a|a b"},
		{Name: "multi-spaced", Value: "team-a | team-b"},
		{Name: "multi-dot-member", Value: "team-a|.."},
		{Name: "multi-long-member", Value: "team-a|" + strings.Repeat("b", 151)},
		{Name: "multi-three", Value: "team-a|team-b|team-c"},
		{Name: "multi-invalid-first", Value: "a b|team-a"},
		{Name: "multi-invalid-last", Value: "team-a|team-b|a b"},
		{Name: "multi-dot-first", Value: "..|team-a"},
		{Name: "multi-reordered", Value: "team-b|team-a"},
		{Name: "dot", Value: "."},
		{Name: "dotdot", Value: ".."},
		{Name: "len-150", Value: strings.Repeat("a", 150)},
		{Name: "len-151", Value: strings.Repeat("a", 151)},
		{Name: "len-151-with-space", Value: strings.Repeat("a", 150) + " "},
		{Name: "len-76-runes-152-bytes", Value: strings.Repeat("é", 76)},
		{Name: "len-152-with-space", Value: strings.Repeat("a", 151) + " b"},
		{Name: "space", Value: "a b"},
		{Name: "slash", Value: "../x"},
		{Name: "non-ascii", Value: "zé"},
		{Name: "star", Value: "*"},
	}
}

// tenantAuthCases is the recorded request set: every route with every header,
// /labels with one tenant per printable ASCII character, and a forwarded
// Authorization next to a tenant and without one.
func tenantAuthCases() []tenantAuthCase {
	var out []tenantAuthCase
	add := func(id, method, target string, h tenantAuthHeader, extra map[string]string) {
		c := tenantAuthCase{ID: id, Method: method, Target: target, NoOrg: h.Absent}
		c.Header = map[string]string{}
		if !h.Absent {
			c.Header["X-Scope-OrgID"] = h.Value
		}
		for k, v := range extra {
			c.Header[k] = v
		}
		out = append(out, c)
	}
	for _, r := range tenantAuthRoutes() {
		for _, h := range tenantAuthHeaders() {
			add(r.Name+"/"+h.Name, r.Method, r.Target, h, nil)
		}
	}
	labels := tenantAuthRoutes()[3]
	for c := 0x09; c <= 0xff; c++ {
		if c == '|' || (c > 0x09 && c < 0x20) || c == 0x7f {
			continue // the multi-tenant separator, asked above; control bytes cannot be sent
		}
		add(fmt.Sprintf("labels/char-%02x", c), labels.Method, labels.Target, tenantAuthHeader{Value: "x" + string([]byte{byte(c)}) + "x"}, nil)
	}
	bearer := map[string]string{"Authorization": "Bearer grafana-service-account-token"}
	basic := map[string]string{"Authorization": "Basic dXNlcjpwYXNz"}
	add("labels/authorization-bearer-with-tenant", labels.Method, labels.Target, tenantAuthHeader{Value: "team-a"}, bearer)
	add("labels/authorization-basic-with-tenant", labels.Method, labels.Target, tenantAuthHeader{Value: "team-a"}, basic)
	add("labels/authorization-bearer-without-tenant", labels.Method, labels.Target, tenantAuthHeader{Absent: true}, bearer)
	// The delete API: Loki's compactor serves it with retention enabled. The
	// proxy asks for its confirmation header and delete token on top, which the
	// replay adds; the tenant answers are Loki's.
	del := "/loki/api/v1/delete?query=%7Bapp%3D%22team-a-app%22%7D&start=" + tenantAuthStart + "&end=" + tenantAuthEnd
	for _, h := range tenantAuthHeaders() {
		add("delete/"+h.Name, http.MethodPost, del, h, nil)
	}
	// The ruler's read routes answer a refused tenant in their own JSON; only
	// the refusals are asked, since an admitted tenant reads a ruler backend.
	for _, route := range []string{"/loki/api/v1/rules", "/api/prom/rules", "/prometheus/api/v1/rules", "/prometheus/api/v1/alerts"} {
		for _, h := range tenantAuthHeaders() {
			switch h.Name {
			case "absent", "blank", "multi", "multi-invalid-member", "dotdot", "len-151", "space", "non-ascii":
				add("ruler"+route+"/"+h.Name, http.MethodGet, route, h, nil)
			}
		}
	}
	return out
}

func (c tenantAuthCase) request(base string) (*http.Request, error) {
	req, err := http.NewRequest(c.Method, base+c.Target, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range c.Header {
		// Set a header with an empty value too: Header.Set keeps it.
		req.Header[http.CanonicalHeaderKey(k)] = []string{v}
	}
	return req, nil
}

// TestRecordTenantAuthLokiAnswers records Loki's answers into
// tenantAuthLokiAnswersFile; it runs only with LVP_ORACLE_RECORD_TENANT_AUTH
// set to the base URL of an auth-enabled Loki.
func TestRecordTenantAuthLokiAnswers(t *testing.T) {
	base := os.Getenv("LVP_ORACLE_RECORD_TENANT_AUTH")
	if base == "" {
		t.Skip("set LVP_ORACLE_RECORD_TENANT_AUTH to an auth-enabled Loki to record its answers")
	}
	version := lokiBuildVersion(t, base)
	out, err := os.Create(tenantAuthLokiAnswersFile)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Close() }()
	w := bufio.NewWriter(out)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(tenantAuthMeta{Loki: version, Config: "testdata/tenant_auth_loki_config.yaml"}); err != nil {
		t.Fatal(err)
	}
	// Loki 3.7.9 never answers some headers (a lone "|" retries an empty org
	// until the client gives up), so a request that hangs fails the recording.
	client := &http.Client{Timeout: 20 * time.Second}
	for _, c := range tenantAuthCases() {
		req, err := c.request(base)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", c.ID, err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		a := tenantAuthAnswer{tenantAuthCase: c, Status: resp.StatusCode, ContentType: resp.Header.Get("Content-Type")}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
			a.setBody(body)
		} else if strings.HasPrefix(c.ID, "labels/") {
			a.TenantLabel = strings.Contains(string(body), `"__tenant_id__"`)
		}
		if err := enc.Encode(a); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
}

func lokiBuildVersion(t *testing.T, base string) string {
	t.Helper()
	resp, err := http.Get(base + "/loki/api/v1/status/buildinfo")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var info struct {
		Version string `json:"version"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		t.Fatal(err)
	}
	return info.Version
}

// readTenantAuthLokiAnswers reads the recorded answers.
func readTenantAuthLokiAnswers(t *testing.T) (tenantAuthMeta, []tenantAuthAnswer) {
	t.Helper()
	fh, err := os.Open(tenantAuthLokiAnswersFile)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fh.Close() }()
	sc := bufio.NewScanner(fh)
	sc.Buffer(make([]byte, 1<<16), 1<<20)
	var meta tenantAuthMeta
	var rows []tenantAuthAnswer
	for first := true; sc.Scan(); first = false {
		if first {
			if err := json.Unmarshal(sc.Bytes(), &meta); err != nil {
				t.Fatal(err)
			}
			continue
		}
		var a tenantAuthAnswer
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, a)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return meta, rows
}

// tenantAuthReplayToken is the delete token the replay sends on delete.
const tenantAuthReplayToken = "replay-admin-token"

// newTenantAuthReplayProxy is the proxy the recorded answers are replayed
// against: -auth.enabled (Loki's auth_enabled: true), the delete API on with an
// delete token, and every tenant Loki accepted mapped to its own VictoriaLogs
// account, since the proxy refuses an unmapped name of its own accord.
func newTenantAuthReplayProxy(t *testing.T, rows []tenantAuthAnswer, backend string, opts ...testProxyOption) *http.ServeMux {
	t.Helper()
	tenants := map[string]TenantMapping{}
	for _, row := range rows {
		if row.Status/100 != 2 || row.NoOrg {
			continue
		}
		for _, id := range strings.Split(row.Header["X-Scope-OrgID"], "|") {
			if id != "" {
				if _, ok := tenants[id]; !ok {
					tenants[id] = TenantMapping{AccountID: fmt.Sprint(len(tenants) + 1), ProjectID: "0"}
				}
			}
		}
	}
	base := []testProxyOption{withBackendURL(backend), func(c *Config) {
		c.AuthEnabled = true
		c.TenantMap = tenants
		c.DeleteAPIEnabled = true
		c.DeleteAPIAuthToken = tenantAuthReplayToken
	}}
	p := newTestProxyWithOptions(t, append(base, opts...)...)
	mux := http.NewServeMux()
	p.RegisterProxyRoutes(mux)
	return mux
}

func (a *tenantAuthAnswer) setBody(body []byte) {
	if utf8.Valid(body) {
		a.Body = string(body)
		return
	}
	a.BodyRaw = body
}

// body is the error body as sent.
func (a tenantAuthAnswer) body() string {
	if a.BodyRaw != nil {
		return string(a.BodyRaw)
	}
	return a.Body
}
