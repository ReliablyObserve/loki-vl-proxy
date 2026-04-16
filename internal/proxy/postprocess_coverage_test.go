package proxy

import "testing"

func TestCollectPatternObservationsFromJSON_MixedShapes(t *testing.T) {
	miner := newPatternMiner()
	observed := 0
	decoded := map[string]interface{}{
		"_msg":  "root request completed successfully",
		"_time": "1712434830",
		"level": "debug",
		"results": []interface{}{
			map[string]interface{}{
				"_msg":  "database timeout threshold exceeded",
				"_time": "1712434800",
				"level": "error",
			},
		},
		"values": []interface{}{
			map[string]interface{}{
				"_msg":  "cache miss detected again",
				"_time": "1712434810",
				"level": "warn",
			},
		},
		"data": map[string]interface{}{
			"result": []interface{}{
				map[string]interface{}{
					"stream": map[string]interface{}{
						"level": "info",
					},
					"values": []interface{}{
						[]interface{}{"1712434820", "http request completed normally"},
						[]interface{}{"bad", "skip me"},
					},
				},
			},
		},
	}

	collectPatternObservationsFromJSON(miner, decoded, 60, "", &observed)

	if observed != 4 {
		t.Fatalf("expected 4 observed patterns, got %d", observed)
	}

	patterns := buildPatternResponse(miner, 10)
	if len(patterns) != 4 {
		t.Fatalf("expected 4 pattern buckets, got %d", len(patterns))
	}
	for _, pattern := range patterns {
		samples, ok := pattern["samples"].([][]interface{})
		if !ok || len(samples) != 1 {
			t.Fatalf("expected exactly one sample per collected pattern, got %#v", pattern["samples"])
		}
	}
}

func TestPostprocessHelperCoverage(t *testing.T) {
	if got := deduplicatePlaceholders("<_><_> constant <_><_><_>", patternVarPlaceholder); got != "<_> constant <_>" {
		t.Fatalf("unexpected deduplicated placeholders %q", got)
	}
	if got := deduplicatePlaceholders("constant only", patternVarPlaceholder); got != "constant only" {
		t.Fatalf("unexpected unchanged placeholders %q", got)
	}

	if got := tokenizeToPattern(`{"foo":1,"bar":"x"}`); got != "{bar=<_> foo=<_>}" {
		t.Fatalf("unexpected JSON pattern %q", got)
	}
	if got := tokenizeToPattern("{broken-json"); got != "<_>" {
		t.Fatalf("expected invalid JSON to collapse to placeholder, got %q", got)
	}

	for _, token := range []string{"1234", "10.0.0.1", "2026-04-06T20:21:22Z", "/a/b/c/d"} {
		if !isVariableToken(token) {
			t.Fatalf("expected %q to be treated as variable", token)
		}
	}
	for _, token := range []string{"INFO", "service=api"} {
		if isVariableToken(token) {
			t.Fatalf("expected %q to remain structural", token)
		}
	}

	if !isIPLike("192.168.0.1") {
		t.Fatal("expected dotted quad to be recognized as IP-like")
	}
	if isIPLike("192.168.0.x") {
		t.Fatal("expected malformed IP not to be recognized as IP-like")
	}
}
