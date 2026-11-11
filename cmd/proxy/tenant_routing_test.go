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
		{nil, true}, // tenancy off by default: strict routing with nothing to route ignores the header
		{[]string{"-tenant.numeric-passthrough=true"}, false},
		{[]string{"-auth.enabled=true"}, false},
		{[]string{"-require-tenant-header=true"}, false},
		{[]string{`-tenant-map={"a":{"account_id":"1","project_id":"0"}}`}, false},
		{[]string{"-tenant-label=org_id"}, false},
		{[]string{"-tenant.numeric-passthrough=false", `-tenant-map={"a":{"account_id":"1","project_id":"0"}}`}, true},
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

// Tenancy is off by default (Loki's single-tenant mode); any tenant setting,
// in flag or environment form, turns it on, and numeric passthrough then
// stays on unless set false explicitly. The tenancy-off configuration is the
// one TestTenantNoAuth_HeaderIgnoredLikeLoki (internal/proxy) replays Loki's
// auth_enabled: false answers against: strict routing with no map, file,
// label, -auth.enabled or -require-tenant-header.
//
// conformance: tenant-auth-follows-loki, tenant-numeric-routing
func TestResolveNumericPassthrough(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		flag, explicit, auth, require bool
		mapJSON, mapFile, label       string
		want                          bool
	}{
		{name: "nothing set: tenancy off", want: false},
		{name: "explicit true", flag: true, explicit: true, want: true},
		{name: "explicit false", explicit: true, want: false},
		{name: "explicit false with a map: strict", explicit: true, mapJSON: `{"a":{}}`, want: false},
		{name: "-auth.enabled", auth: true, want: true},
		{name: "-require-tenant-header", require: true, want: true},
		{name: "tenant map (flag or TENANT_MAP)", mapJSON: `{"a":{}}`, want: true},
		{name: "tenant map file (flag or TENANT_MAP_FILE)", mapFile: "/etc/t.yaml", want: true},
		{name: "tenant label (flag or TENANT_LABEL)", label: "org_id", want: true},
		{name: "blank values do not count", mapJSON: " ", mapFile: " ", label: " ", want: false},
	} {
		if got := resolveNumericPassthrough(tc.flag, tc.explicit, tc.auth, tc.require, tc.mapJSON, tc.mapFile, tc.label); got != tc.want {
			t.Fatalf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
}

// The environment forms of the tenant settings turn tenancy on like the flags.
//
// conformance: tenant-auth-follows-loki
func TestRun_TenantEnvTurnsTenancyOn(t *testing.T) {
	for _, env := range []map[string]string{
		{"TENANT_MAP": `{"a":{"account_id":"1","project_id":"0"}}`},
		{"TENANT_LABEL": "org_id"},
	} {
		reloadCh := make(chan os.Signal)
		close(reloadCh)
		shutdownCh := make(chan os.Signal, 1)
		shutdownCh <- syscall.SIGTERM
		var captured runtimeOptions
		err := run([]string{"-server.admin-auth-token=test-token"}, func(k string) string { return env[k] }, io.Discard,
			func(chan<- os.Signal, ...os.Signal) {},
			func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher { return &fakeOTLPPusher{} },
			func(opts runtimeOptions, _ *slog.Logger, _ signalNotifier, _ otlpPusherFactory) (*runtimeState, error) {
				captured = opts
				return &runtimeState{proxy: &proxy.Proxy{}, server: &fakeHTTPServer{}, cacheCleanup: func() {}, stopOTLP: func() {}, reloadCh: reloadCh, shutdownCh: shutdownCh}, nil
			},
			func(httpServer, serverLoopOptions, *slog.Logger, func(string, ...any)) {},
			func(ch <-chan os.Signal, _ httpServer, _ time.Duration, _ *slog.Logger) { <-ch })
		if err != nil {
			t.Fatalf("%v: %v", env, err)
		}
		if captured.proxyCfg.strictTenantRouting {
			t.Fatalf("%v: tenancy stayed off (strict routing with no map)", env)
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

// Per-tenant limits are keyed by X-Scope-OrgID, which tenancy-off ignores:
// they refuse to start without a tenancy setting, naming the flags that turn
// it on. Default limits apply to every request and stay allowed.
//
// conformance: tenant-auth-follows-loki
func TestTenantLimitsNeedTenancy(t *testing.T) {
	const limits = `{"team-a":{"max_query_series":10}}`
	for _, tc := range []struct {
		name    string
		s       tenancySettings
		wantErr bool
	}{
		{"no limits, tenancy off", tenancySettings{}, false},
		{"default limits only", tenancySettings{}, false},
		{"tenant limits, tenancy off", tenancySettings{tenantLimitsJSON: limits}, true},
		{"tenant limits, explicit passthrough false", tenancySettings{tenantLimitsJSON: limits, numericPassthroughExplicit: true}, true},
		{"tenant limits + numeric passthrough", tenancySettings{tenantLimitsJSON: limits, numericPassthrough: true, numericPassthroughExplicit: true}, false},
		{"tenant limits + tenant map", tenancySettings{tenantLimitsJSON: limits, tenantMapJSON: `{"team-a":{"account_id":"1","project_id":"0"}}`}, false},
		{"tenant limits + map file", tenancySettings{tenantLimitsJSON: limits, tenantMapFile: "/etc/t.yaml"}, false},
		{"tenant limits + label", tenancySettings{tenantLimitsJSON: limits, tenantLabel: "org_id"}, false},
		{"tenant limits + auth", tenancySettings{tenantLimitsJSON: limits, authEnabled: true}, false},
		{"tenant limits + require header", tenancySettings{tenantLimitsJSON: limits, requireTenantHeader: true}, false},
	} {
		_, err := resolveTenancy(tc.s)
		if (err != nil) != tc.wantErr {
			t.Fatalf("%s: err %v, want error %v", tc.name, err, tc.wantErr)
		}
		if err != nil {
			for _, flag := range []string{"-tenant-limits", "-tenant.numeric-passthrough=true", "-tenant-map", "-tenant-map-file", "-tenant-label", "-auth.enabled", "-require-tenant-header", "-tenant-default-limits"} {
				if !strings.Contains(err.Error(), flag) {
					t.Fatalf("%s: error does not name %s: %v", tc.name, flag, err)
				}
			}
		}
	}
	for _, env := range []map[string]string{{"TENANT_LIMITS": limits}, {}} {
		args := []string{"-server.admin-auth-token=test-token"}
		if len(env) == 0 {
			args = append(args, "-tenant-limits="+limits)
		}
		err := run(args, func(k string) string { return env[k] }, io.Discard, func(chan<- os.Signal, ...os.Signal) {},
			func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher { return &fakeOTLPPusher{} },
			func(runtimeOptions, *slog.Logger, signalNotifier, otlpPusherFactory) (*runtimeState, error) {
				t.Fatal("runtime builder must not run when per-tenant limits have no tenancy")
				return nil, nil
			},
			func(httpServer, serverLoopOptions, *slog.Logger, func(string, ...any)) {},
			func(<-chan os.Signal, httpServer, time.Duration, *slog.Logger) {})
		if err == nil || !strings.Contains(err.Error(), "tenancy is off") {
			t.Fatalf("env %v args %v: %v, want the tenancy refusal", env, args, err)
		}
	}
	// Default limits alone start.
	got := runCapturingProxyCfg(t, `-tenant-default-limits={"max_query_series":10}`).proxyCfg
	if !got.strictTenantRouting {
		t.Fatal("default limits turned tenancy on")
	}
}
