package translator

import (
	"regexp"
	"sort"
	"strings"
)

// NormalizeQuery canonicalizes a LogQL query for cache key consistency.
// Semantically identical queries produce the same normalized form.
func NormalizeQuery(query string) string {
	query = strings.TrimSpace(query)
	if query == "" {
		return "*"
	}

	// 1. Normalize whitespace: collapse runs of spaces
	query = collapseSpaces(query)

	// 2. Sort label matchers inside stream selectors {a="1",b="2"}
	query = normalizeStreamSelector(query)

	return query
}

var multiSpace = regexp.MustCompile(`\s+`)

func collapseSpaces(s string) string {
	return multiSpace.ReplaceAllString(s, " ")
}

// normalizeStreamSelector sorts comma-separated label matchers inside {}.
func normalizeStreamSelector(query string) string {
	start := strings.Index(query, "{")
	if start < 0 {
		return query
	}
	end := findMatchingBrace(query[start:])
	if end < 0 {
		return query
	}
	end += start

	prefix := query[:start]
	selector := query[start+1 : end]
	suffix := query[end+1:]

	// Split matchers, sort, rejoin
	matchers := splitMatchers(selector)
	sort.Strings(matchers)
	normalized := "{" + strings.Join(matchers, ",") + "}"

	return prefix + normalized + suffix
}

// splitMatchers splits "a=1,b=2" respecting quotes.
func splitMatchers(s string) []string {
	var matchers []string
	inQuote := false
	start := 0
	for i, c := range s {
		if c == '"' {
			inQuote = !inQuote
		}
		if c == ',' && !inQuote {
			m := strings.TrimSpace(s[start:i])
			if m != "" {
				matchers = append(matchers, m)
			}
			start = i + 1
		}
	}
	m := strings.TrimSpace(s[start:])
	if m != "" {
		matchers = append(matchers, m)
	}
	return matchers
}
