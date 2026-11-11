package proxy

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// Every request that differs only in X-Scope-OrgID gets Loki's answer: the
// recorded status, and for a refusal Loki's Content-Type and exact body (Loki
// 3.7.9, auth_enabled: true, multi-tenant queries on). With auth on, a missing
// or blank header is 401 "no org id"; an ID Loki's tenant check refuses ('.' or
// '..', longer than 150, a byte outside its set) is 400 with Loki's text on
// every route, before routing, the cache or any VictoriaLogs call.
//
// conformance: tenant-auth-follows-loki
func TestTenantAuth_AnswersLikeLoki(t *testing.T) {
	meta, rows := readTenantAuthLokiAnswers(t)
	if meta.Loki == "" || len(rows) < 300 {
		t.Fatalf("%s: %d rows recorded with Loki %q", tenantAuthLokiAnswersFile, len(rows), meta.Loki)
	}
	vl := httptest.NewServer(emptyVictoriaLogs())
	defer vl.Close()
	mux := newTenantAuthReplayProxy(t, rows, vl.URL)
	// The requests come from tenantAuthCases, not the file: JSON cannot carry
	// a header that is not valid UTF-8.
	cases := map[string]tenantAuthCase{}
	for _, c := range tenantAuthCases() {
		cases[c.ID] = c
	}
	if len(cases) != len(rows) {
		t.Fatalf("%d cases, %d recorded answers: record them again (TestRecordTenantAuthLokiAnswers)", len(cases), len(rows))
	}
	var diffs []string
	for _, row := range rows {
		c, ok := cases[row.ID]
		if !ok {
			t.Fatalf("recorded %s is no case: record the answers again", row.ID)
		}
		req, err := c.request("")
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest(row.Method, row.Target, nil)
		r.Header = req.Header
		if strings.HasPrefix(row.ID, "delete/") {
			r.Header.Set("X-Delete-Confirmation", "true")
			r.Header.Set("X-Delete-Token", tenantAuthReplayToken)
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		got := tenantAuthAnswer{Status: w.Code, ContentType: w.Header().Get("Content-Type")}
		if w.Code != http.StatusOK && w.Code != http.StatusNoContent {
			got.setBody(w.Body.Bytes())
		} else if strings.HasPrefix(row.ID, "labels/") {
			got.TenantLabel = strings.Contains(w.Body.String(), `"__tenant_id__"`)
		}
		if got.Status != row.Status || got.body() != row.body() || got.TenantLabel != row.TenantLabel ||
			(row.body() != "" && got.ContentType != row.ContentType) {
			diffs = append(diffs, row.ID+": proxy "+describeTenantAuth(got)+"\n      loki  "+describeTenantAuth(row))
		}
	}
	if len(diffs) > 0 {
		t.Fatalf("%d of %d answers differ from Loki %s:\n  %s", len(diffs), len(rows), meta.Loki, strings.Join(diffs, "\n  "))
	}
}

func describeTenantAuth(a tenantAuthAnswer) string {
	s := strconv.Itoa(a.Status)
	if b := a.body(); b != "" {
		s += " " + strconv.Quote(b) + " (" + a.ContentType + ")"
	}
	if a.TenantLabel {
		s += " +__tenant_id__"
	}
	return s
}

// emptyVictoriaLogs answers every VictoriaLogs read with an empty success, so a
// request the proxy admits answers 200 and only the tenant check can refuse it.
func emptyVictoriaLogs() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stats_query_range":
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
		case "/select/logsql/stats_query":
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
		case "/select/logsql/query", "/select/logsql/tail":
		case "/select/logsql/hits":
			_, _ = w.Write([]byte(`{"hits":[]}`))
		case "/select/logsql/delete":
			w.WriteHeader(http.StatusOK)
		default:
			_, _ = w.Write([]byte(`{"values":[]}`))
		}
	})
}
