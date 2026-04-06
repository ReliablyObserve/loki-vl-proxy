package proxy

import "testing"

func FuzzParseLegacyRulesPath(f *testing.F) {
	for _, seed := range []string{
		"/loki/api/v1/rules",
		"/loki/api/v1/rules/compat.rules",
		"/loki/api/v1/rules/compat.rules/loki-vl-e2e-alerts",
		"/api/prom/rules/prod/api",
		"/loki/api/v1/rules/%2e%2e/escape",
		"/loki/api/v1/rules/team%2Fa/api%20alerts",
		"/not/a/rules/path",
		"",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, path string) {
		_, _, _ = parseLegacyRulesPath(path)
	})
}

func FuzzExtractLogPatterns(f *testing.F) {
	for _, seed := range []struct {
		body  string
		step  string
		limit int
	}{
		{`{"_time":"2026-04-04T10:00:00Z","_msg":"GET /api/users 200 15ms","level":"info"}` + "\n", "15s", 10},
		{`{"_time":"2026-04-04T10:00:00Z","_msg":"POST /api/orders 201 142ms","level":"error"}` + "\n", "60", 1},
		{"", "1m", 50},
	} {
		f.Add(seed.body, seed.step, seed.limit)
	}

	f.Fuzz(func(t *testing.T, body string, step string, limit int) {
		patterns := extractLogPatterns([]byte(body), step, limit)
		if limit > 0 && len(patterns) > limit {
			t.Fatalf("expected patterns to respect limit=%d, got %d", limit, len(patterns))
		}
		for _, pattern := range patterns {
			samples, _ := pattern["samples"].([][]interface{})
			for _, sample := range samples {
				if len(sample) != 2 {
					t.Fatalf("expected [bucket,count] pairs, got %v", sample)
				}
			}
		}
	})
}
