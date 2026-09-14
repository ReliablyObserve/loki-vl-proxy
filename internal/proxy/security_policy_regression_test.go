package proxy

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestTenantScoping_WildcardPolicyInBothRoutingModes(t *testing.T) {
	for _, label := range []string{"", "org_id"} {
		for _, allow := range []bool{false, true} {
			for _, mapped := range []bool{false, true} {
				p := newTestProxyWithOptions(t, func(c *Config) {
					c.TenantLabel = label
					c.AllowGlobalTenant = allow
					if mapped {
						c.TenantMap = map[string]TenantMapping{"*": {AccountID: "10", ProjectID: "20"}}
					}
				})
				err := p.validateSingleTenantOrgID("*")
				if (err == nil) != (allow || mapped) {
					t.Fatalf("label=%q allow=%v mapped=%v: %v", label, allow, mapped, err)
				}
				for _, alias := range []string{"0", "fake", "default"} {
					if err := p.validateSingleTenantOrgID(alias); err != nil {
						t.Fatalf("alias %q: %v", alias, err)
					}
				}
				r := httptest.NewRequest("GET", "/loki/api/v1/labels", nil)
				r.Header.Set("X-Scope-OrgID", "*|10")
				if p.validateTenantHeader(r) == nil {
					t.Fatal("multi-tenant wildcard accepted")
				}
			}
		}
	}
}

func TestTailHardening_BoundedFragmentedInputPreservesControls(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if r.URL.Path == "/select/logsql/tail" {
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		}
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	srv := httptest.NewServer(http.HandlerFunc(p.handleTail))
	defer srv.Close()
	for _, size := range []int{128, 8192} {
		dialer := websocket.Dialer{WriteBufferSize: 256}
		ws, _, err := dialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http")+"?query={app%3D%22a%22}", nil)
		if err != nil {
			t.Fatal(err)
		}
		func() {
			defer ws.Close()
			_ = ws.SetWriteDeadline(time.Now().Add(3 * time.Second))
			writer, err := ws.NextWriter(websocket.BinaryMessage)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = writer.Write([]byte(strings.Repeat("x", size)))
			_ = writer.Close()
			pong := errors.New("pong received")
			ws.SetPongHandler(func(string) error { return pong })
			_ = ws.WriteControl(websocket.PingMessage, []byte("still alive"), time.Now().Add(time.Second))
			_ = ws.SetReadDeadline(time.Now().Add(3 * time.Second))
			_, _, err = ws.ReadMessage()
			if size <= 4096 {
				if !errors.Is(err, pong) {
					t.Fatalf("small message/control failed: %v", err)
				}
			} else if !websocket.IsCloseError(err, websocket.CloseMessageTooBig) {
				t.Fatalf("oversized fragmented message: expected close 1009, got %v", err)
			}
		}()
	}
}

func TestHardening_SecurityRegressionSelection(t *testing.T) {
	script, err := os.ReadFile("../../scripts/ci/run_security_regressions.sh")
	if err != nil {
		t.Fatal(err)
	}
	match := regexp.MustCompile(`-run '([^']+)'`).FindSubmatch(script)
	if len(match) != 2 {
		t.Fatal("missing unit security selection")
	}
	selection, err := regexp.Compile(string(match[1]))
	if err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, path := range files {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil {
				continue
			}
			name := fn.Name.Name
			if strings.HasPrefix(name, "TestHardening_") || strings.HasPrefix(name, "TestTenantScoping_") || strings.HasPrefix(name, "TestTailHardening_") {
				count++
				if !selection.MatchString(name) {
					t.Errorf("security lane omits %s", name)
				}
			}
		}
	}
	if count == 0 || selection.MatchString("TestUnrelated") {
		t.Fatal("invalid security inventory")
	}
}
