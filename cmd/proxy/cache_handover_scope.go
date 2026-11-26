package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

func validateCacheHandoverFlags(flushTimeout time.Duration, warmBytes int64, warmTimeout time.Duration) error {
	if flushTimeout < 0 || warmBytes < 0 || warmTimeout <= 0 {
		return fmt.Errorf("invalid restart hand-over: -cache-shutdown-flush-timeout and -cache-startup-warm-bytes must be >= 0, -cache-startup-warm-timeout > 0")
	}
	return nil
}

// cacheHandoverScope identifies the build and the configuration that decides
// answers, for the response cache's restart hand-over: memory-only answers a
// graceful shutdown kept are restored only by a process with the same scope
// (records of the disk tier carry their own key versions and are restored by
// any build, as a disk hit would serve them).
//
// The build part is the version and revision stamped at build time and a
// SHA-256 of the running executable, so two builds stamped alike (local
// builds) still differ. The configuration part is a SHA-256 of every flag
// value and the environment overrides. Left out: the per-pod identity
// (-peer-self, the service instance id), which changes when Kubernetes
// recreates a pod and decides no answer, and credentials (flag names holding
// token, password or secret), which decide no answer either and are never
// hashed onto the disk. It is computed only when the hand-over is on (hashing
// the executable reads it once); "" restores no memory-only answer.
func cacheHandoverScope(fs *flag.FlagSet, env envConfig, flushTimeout time.Duration, warmBytes int64) string {
	if flushTimeout <= 0 && warmBytes <= 0 {
		return ""
	}
	exe, err := executableDigest()
	if err != nil {
		return ""
	}
	h := sha256.New()
	fs.VisitAll(func(f *flag.Flag) {
		if !handoverScopeFlag(f.Name) {
			return
		}
		fmt.Fprintf(h, "flag %s=%s\n", f.Name, f.Value.String())
	})
	translateOTel := "unset"
	if env.translateOTel != nil {
		translateOTel = fmt.Sprint(*env.translateOTel)
	}
	for _, kv := range [][2]string{
		{"backend", env.backendURL}, {"ruler-backend", env.rulerBackendURL}, {"alerts-backend", env.alertsBackendURL},
		{"tenant-map", env.tenantMapJSON}, {"tenant-map-file", env.tenantMapFile},
		{"tenant-limits-allow", env.tenantLimitsAllow}, {"tenant-default", env.tenantDefaultJSON}, {"tenant-limits", env.tenantLimitsJSON},
		{"label-style", env.labelStyle}, {"field-mapping", env.fieldMappingJSON}, {"metadata-field-mode", env.metadataFieldMode},
		{"translate-otel", translateOTel}, {"extra-label-fields", env.extraLabelFields},
	} {
		fmt.Fprintf(h, "env %s=%s\n", kv[0], kv[1])
	}
	return version + "|" + revision + "|" + exe + "|" + hex.EncodeToString(h.Sum(nil)[:16])
}

func executableDigest() (string, error) {
	path, err := os.Executable()
	if err != nil {
		return "", err
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)[:16]), nil
}

func handoverScopeFlag(name string) bool {
	if name == "peer-self" {
		return false
	}
	lower := strings.ToLower(name)
	for _, secret := range []string{"token", "password", "secret"} {
		if strings.Contains(lower, secret) {
			return false
		}
	}
	return true
}
