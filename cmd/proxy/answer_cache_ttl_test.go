package main

import (
	"errors"
	"flag"
	"io"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

var errAnswerTTLStop = errors.New("stop after flag parsing")

// runAnswerTTLFlags runs the binary's flag handling and returns the answer
// class TTLs it hands to the proxy, or the start-up error.
func runAnswerTTLFlags(t *testing.T, args []string, env map[string]string) (map[string]time.Duration, error) {
	t.Helper()
	var got map[string]time.Duration
	err := run(append([]string{"-backend", "http://backend.test"}, args...), func(key string) string { return env[key] }, io.Discard,
		func(chan<- os.Signal, ...os.Signal) {},
		func(metrics.OTLPConfig, *metrics.Metrics) otlpMetricsPusher { return &fakeOTLPPusher{} },
		func(opts runtimeOptions, _ *slog.Logger, _ signalNotifier, _ otlpPusherFactory) (*runtimeState, error) {
			got = opts.proxyCfg.answerCacheTTLs
			cfg, err := buildProxyConfig(opts.proxyCfg)
			if err != nil {
				return nil, err
			}
			if !reflect.DeepEqual(cfg.AnswerCacheTTLs, got) {
				t.Fatalf("proxy config %v, runtime %v", cfg.AnswerCacheTTLs, got)
			}
			return nil, errAnswerTTLStop
		},
		func(httpServer, serverLoopOptions, *slog.Logger, func(string, ...any)) {
			t.Fatal("server loop must not start")
		},
		func(<-chan os.Signal, httpServer, time.Duration, *slog.Logger) {})
	if errors.Is(err, errAnswerTTLStop) {
		return got, nil
	}
	return nil, err
}

// conformance: cache-answer-ttl-shorten-only
func TestAnswerCacheTTLFlagsAndEnv(t *testing.T) {
	if got, err := runAnswerTTLFlags(t, nil, nil); err != nil || got != nil {
		t.Fatalf("defaults: %v %v, want no setting", got, err)
	}
	got, err := runAnswerTTLFlags(t,
		[]string{"-cache-answer-ttl-series=5s", "-cache-answer-ttl-query-range=1m", "-cache-answer-ttl-detected-field-values=0"},
		map[string]string{"CACHE_ANSWER_TTL_SERIES": "1s", "CACHE_ANSWER_TTL_VOLUME_RANGE": "3s", "CACHE_ANSWER_TTL_LABELS": ""})
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]time.Duration{"series": 5 * time.Second, "query_range": time.Minute, "volume_range": 3 * time.Second}; !reflect.DeepEqual(got, want) {
		t.Fatalf("settings %v, want %v (a flag wins over its variable)", got, want)
	}
	for _, class := range proxy.AnswerCacheTTLClasses {
		builtin, _ := proxy.AnswerCacheTTLMax(class)
		name, env := answerCacheTTLFlag(class), answerCacheTTLEnv(class)
		if got, err := runAnswerTTLFlags(t, []string{"-" + name + "=" + builtin.String()}, nil); err != nil || got[class] != builtin {
			t.Fatalf("-%s at the built-in %s: %v %v", name, builtin, got, err)
		}
		for _, tc := range []struct {
			args []string
			env  map[string]string
		}{
			{[]string{"-" + name + "=" + (builtin + time.Second).String()}, nil},
			{[]string{"-" + name + "=-1s"}, nil},
			{nil, map[string]string{env: (builtin + time.Second).String()}},
		} {
			_, err := runAnswerTTLFlags(t, tc.args, tc.env)
			if err == nil {
				t.Fatalf("%v %v accepted", tc.args, tc.env)
			}
			for _, part := range []string{"-" + name, env, builtin.String()} {
				if !strings.Contains(err.Error(), part) {
					t.Fatalf("%v %v: error %q does not name %q", tc.args, tc.env, err, part)
				}
			}
		}
	}
	if _, err := runAnswerTTLFlags(t, nil, map[string]string{"CACHE_ANSWER_TTL_QUERY": "soon"}); err == nil || !strings.Contains(err.Error(), "CACHE_ANSWER_TTL_QUERY") {
		t.Fatalf("an unparsable variable: %v", err)
	}
}

// A setting given by its variable changes the restart hand-over scope like
// one given by its flag, so memory-only answers stored under another TTL are
// not restored.
//
// conformance: cache-answer-ttl-shorten-only, cache-restart-handover
func TestAnswerCacheTTLChangesTheHandoverScope(t *testing.T) {
	flags := func(args []string, env map[string]string) *flag.FlagSet {
		fs := handoverScopeFlags(t)
		for _, class := range proxy.AnswerCacheTTLClasses {
			fs.Duration(answerCacheTTLFlag(class), 0, "")
		}
		if err := fs.Parse(args); err != nil {
			t.Fatal(err)
		}
		explicit := map[string]bool{}
		fs.Visit(func(f *flag.Flag) { explicit[f.Name] = true })
		if _, err := resolveAnswerCacheTTLs(fs, explicit, func(key string) string { return env[key] }); err != nil {
			t.Fatal(err)
		}
		return fs
	}
	env := envConfig{backendURL: "http://vl:9428"}
	base := cacheHandoverScope(flags(nil, nil), env, time.Second, 0)
	byFlag := cacheHandoverScope(flags([]string{"-cache-answer-ttl-series=5s"}, nil), env, time.Second, 0)
	byEnv := cacheHandoverScope(flags(nil, map[string]string{"CACHE_ANSWER_TTL_SERIES": "5s"}), env, time.Second, 0)
	if base == "" || byFlag == base || byEnv != byFlag {
		t.Fatalf("scope unset %q, flag %q, env %q", base, byFlag, byEnv)
	}
}
