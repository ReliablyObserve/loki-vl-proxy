package main

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// answerCacheTTLFlag is the flag of a Loki answer class's TTL setting:
// -cache-answer-ttl-<class with dashes>.
func answerCacheTTLFlag(class string) string {
	return "cache-answer-ttl-" + strings.ReplaceAll(class, "_", "-")
}

// answerCacheTTLEnv is its environment variable, CACHE_ANSWER_TTL_<CLASS>.
func answerCacheTTLEnv(class string) string {
	return "CACHE_ANSWER_TTL_" + strings.ToUpper(class)
}

// parseFlagsSecretsAndAnswerTTLs is parseFlagsAndSecrets followed by
// resolveAnswerCacheTTLs.
func parseFlagsSecretsAndAnswerTTLs(fs *flag.FlagSet, args []string, getenv func(string) string, secrets []secretFlag) (map[string]bool, map[string]time.Duration, error) {
	explicit, err := parseFlagsAndSecrets(fs, args, getenv, secrets)
	if err != nil {
		return nil, nil, err
	}
	ttls, err := resolveAnswerCacheTTLs(fs, explicit, getenv)
	return explicit, ttls, err
}

// resolveAnswerCacheTTLs reads the -cache-answer-ttl-* settings: a flag given
// on the command line wins, else its environment variable is applied through
// the flag (so the restart hand-over scope, which hashes the flag values,
// sees it). Every setting is checked here, before any cache is opened: a
// negative value or one longer than the class's built-in TTL stops the start
// with an error naming the flag, the variable and the built-in TTL. Only
// non-zero settings are returned.
func resolveAnswerCacheTTLs(fs *flag.FlagSet, explicit map[string]bool, getenv func(string) string) (map[string]time.Duration, error) {
	var out map[string]time.Duration
	for _, class := range proxy.AnswerCacheTTLClasses {
		name, env := answerCacheTTLFlag(class), answerCacheTTLEnv(class)
		f := fs.Lookup(name)
		if f == nil {
			return nil, fmt.Errorf("flag -%s is not declared", name)
		}
		if v := strings.TrimSpace(getenv(env)); v != "" && !explicit[name] {
			if err := fs.Set(name, v); err != nil {
				return nil, fmt.Errorf("invalid %s=%q: %w", env, v, err)
			}
		}
		ttl := f.Value.(flag.Getter).Get().(time.Duration)
		if err := proxy.ValidateAnswerCacheTTL(class, ttl); err != nil {
			return nil, fmt.Errorf("invalid -%s=%s (env %s): %w", name, ttl, env, err)
		}
		if ttl == 0 {
			continue
		}
		if out == nil {
			out = make(map[string]time.Duration, len(proxy.AnswerCacheTTLClasses))
		}
		out[class] = ttl
	}
	return out, nil
}
