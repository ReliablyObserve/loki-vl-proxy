package proxy

import (
	"slices"
	"testing"
)

// FuzzPatternMinerPlaceholdersAgree checks that the miner's checks give the same answers with each token's
// placeholder worked out once for the line (as Observe passes them) as when every check works it out again: the
// structure signature, the prefix check, the similarity and the merged template, for a template mined from one
// line and a second line. That the answers are the ones the miner gave before is
// TestPatternMinerKeepsTheRecordedPatterns'.
//
// conformance: loki_api_v1_patterns
func FuzzPatternMinerPlaceholdersAgree(f *testing.F) {
	for _, seed := range [][2]string{
		{"GET /api/v1/users/42 HTTP/1.1 status=200 duration_ms=12", "GET /api/v1/orders/7 HTTP/1.1 status=500 duration_ms=900"},
		{"connection from 10.0.0.1:8080 accepted for user alice", "connection from 192.168.1.20:443 accepted for user bob"},
		{"level=info ts=2026-11-06T10:00:00Z id=0b8f6a2e-7c1d-4e2f-9a3b-5c6d7e8f9a0b done", "level=warn ts=2026-11-06T11:30:00Z id=deadbeefcafe done"},
		{"trace_id=4bf92f3577b34da6a3ce929d0e0e4736 took 15ms", "trace_id=00f067aa0ba902b7 took 3000ms"},
		{"path /var/lib/db/1.db opened <_> times", "path /tmp/x.db opened 3 times"},
		{"a b c d", "a b c d"},
	} {
		f.Add(seed[0], seed[1])
	}
	tok := newPatternLineTokenizer()
	f.Fuzz(func(t *testing.T, first, second string) {
		templateTokens, _, ok := tok.Tokenize(first)
		if !ok {
			return
		}
		tokens, _, ok := tok.Tokenize(second)
		if !ok {
			return
		}
		placeholders := make([]string, len(tokens))
		for i, token := range tokens {
			placeholders[i] = patternPlaceholderForToken(token)
		}
		// A template the miner could hold: the first line merged with the second, which turns differing tokens
		// into placeholders.
		template := mergePatternTemplate(slices.Clone(templateTokens), tokens)
		for _, tmpl := range [][]string{templateTokens, template} {
			if got, want := patternPrefixCompatibleOf(tmpl, tokens, placeholders), patternPrefixCompatible(tmpl, tokens); got != want {
				t.Fatalf("prefix check of %q against %q = %v, want %v", tokens, tmpl, got, want)
			}
			gotSim, gotParams := getPatternSimilarityOf(tmpl, tokens, placeholders)
			wantSim, wantParams := getPatternSimilarity(tmpl, tokens)
			if gotSim != wantSim || gotParams != wantParams {
				t.Fatalf("similarity of %q to %q = %v %d, want %v %d", tokens, tmpl, gotSim, gotParams, wantSim, wantParams)
			}
			got := mergePatternTemplateOf(slices.Clone(tmpl), tokens, placeholders)
			if want := mergePatternTemplate(slices.Clone(tmpl), tokens); !slices.Equal(got, want) {
				t.Fatalf("merge of %q into %q = %q, want %q", tokens, tmpl, got, want)
			}
		}
		templatePlaceholders := make([]string, len(template))
		for i, token := range template {
			templatePlaceholders[i] = patternPlaceholderForToken(token)
		}
		for _, c := range []struct{ line, placeholders []string }{{tokens, placeholders}, {template, templatePlaceholders}} {
			if got, want := string(appendPatternStructureSignature(nil, c.line, c.placeholders)), patternStructureSignature(c.line); got != want {
				t.Fatalf("signature of %q = %q, want %q", c.line, got, want)
			}
		}
	})
}
