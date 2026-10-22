package main

import (
	"bytes"
	"io"
	"log/slog"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// runCapturingProxyCfg runs the CLI with args and returns the proxy config it built.
func runCapturingProxyCfg(t *testing.T, args ...string) runtimeOptions {
	t.Helper()
	reloadCh := make(chan os.Signal)
	close(reloadCh)
	shutdownCh := make(chan os.Signal, 1)
	shutdownCh <- syscall.SIGTERM
	var captured runtimeOptions
	err := run(
		append([]string{"-server.admin-auth-token=test-token"}, args...),
		func(string) string { return "" },
		io.Discard,
		func(chan<- os.Signal, ...os.Signal) {},
		func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher { return &fakeOTLPPusher{} },
		func(opts runtimeOptions, _ *slog.Logger, _ signalNotifier, _ otlpPusherFactory) (*runtimeState, error) {
			captured = opts
			return &runtimeState{
				proxy:        &proxy.Proxy{},
				server:       &fakeHTTPServer{},
				cacheCleanup: func() {},
				stopOTLP:     func() {},
				reloadCh:     reloadCh,
				shutdownCh:   shutdownCh,
			}, nil
		},
		func(httpServer, serverLoopOptions, *slog.Logger, func(string, ...any)) {},
		func(ch <-chan os.Signal, _ httpServer, _ time.Duration, _ *slog.Logger) { <-ch },
	)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	return captured
}

// conformance: tenant-numeric-routing
func TestTenantNumericPassthroughFlag(t *testing.T) {
	for _, tc := range []struct {
		args       []string
		wantStrict bool
	}{
		{nil, false},
		{[]string{"-tenant.numeric-passthrough=true"}, false},
		{[]string{"-tenant.numeric-passthrough=false"}, true},
		{[]string{"-tenant.numeric-passthrough=false", "-tenant.allow-global=true"}, true},
	} {
		got := runCapturingProxyCfg(t, tc.args...).proxyCfg
		if got.strictTenantRouting != tc.wantStrict {
			t.Fatalf("%v: strictTenantRouting = %v, want %v", tc.args, got.strictTenantRouting, tc.wantStrict)
		}
		cfg, err := buildProxyConfig(got)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.StrictTenantRouting != tc.wantStrict {
			t.Fatalf("%v: Config.StrictTenantRouting = %v, want %v", tc.args, cfg.StrictTenantRouting, tc.wantStrict)
		}
	}
}

// A deployed map value such as account_id "007" keeps working: VictoriaLogs
// reads it as 7, and only the org IDs (the map keys) follow the canonical rule.
//
// conformance: tenant-numeric-routing
func TestValidateTenantMap_KeepsAcceptingAnyUint32Spelling(t *testing.T) {
	for _, tc := range []struct {
		account, project string
		wantErr          bool
	}{
		{"7", "0", false}, {"007", "0", false}, {"7", "00", false}, {"4294967295", "0", false},
		{"+7", "0", true}, {"-1", "0", true}, {"4294967296", "0", true}, {"", "0", true}, {"abc", "0", true},
	} {
		err := validateTenantMap(map[string]proxy.TenantMapping{"org": {AccountID: tc.account, ProjectID: tc.project}})
		if (err != nil) != tc.wantErr {
			t.Errorf("%q/%q: error %v, want error=%v", tc.account, tc.project, err, tc.wantErr)
		}
	}
}

// The SIGHUP reload from the environment validates exactly what startup does.
//
// conformance: tenant-numeric-routing
func TestReloadDynamicConfig_ValidatesTenantMapLikeStartup(t *testing.T) {
	for _, tc := range []struct {
		raw       string
		installed bool
	}{
		{`{"team-a":{"account_id":"007","project_id":"0"}}`, true},
		{`{"team-a":{"account_id":"abc","project_id":"0"}}`, false},
		{`{"team-a":{"account_id":"4294967296","project_id":"0"}}`, false},
	} {
		buf := &bytes.Buffer{}
		fake := &fakeReloadableProxy{}
		reloadDynamicConfig(fake, func(key string) string { return map[string]string{"TENANT_MAP": tc.raw}[key] }, "", slog.New(slog.NewJSONHandler(buf, nil)))
		if (fake.tenantMap != nil) != tc.installed {
			t.Errorf("%s: installed=%v, want %v (%s)", tc.raw, fake.tenantMap != nil, tc.installed, buf.String())
		}
		if _, err := parseTenantMapJSON(tc.raw); (err == nil) != tc.installed {
			t.Errorf("%s: startup validation disagrees with the reload", tc.raw)
		}
	}
}

// conformance: tenant-numeric-routing
func TestLogTenantRouting_StatesTheActiveMode(t *testing.T) {
	for _, tc := range []struct {
		cfg  proxy.Config
		want []string
	}{
		{proxy.Config{}, []string{`"mode":"default"`, `"numeric_passthrough":true`, "403"}},
		{proxy.Config{StrictTenantRouting: true}, []string{`"mode":"strict"`, `"numeric_passthrough":false`, "tenant map"}},
		{proxy.Config{TenantLabel: "org"}, []string{`"mode":"tenant-label"`, `"tenant_label":"org"`}},
	} {
		buf := &bytes.Buffer{}
		logTenantRouting(slog.New(slog.NewJSONHandler(buf, nil)), tc.cfg)
		if strings.Count(buf.String(), "tenant routing") != 1 {
			t.Fatalf("want exactly one startup line, got %s", buf.String())
		}
		for _, w := range tc.want {
			if !strings.Contains(buf.String(), w) {
				t.Errorf("%+v: line lacks %s: %s", tc.cfg, w, buf.String())
			}
		}
	}
}
