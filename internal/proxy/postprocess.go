package proxy

import (
	"container/heap"
	"encoding/json"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"time"
	"unicode"
)

// ansiEscapeRe matches ANSI escape sequences (color codes, cursor movement, etc.)
var ansiEscapeRe = regexp.MustCompile(`\x1b\[[0-9;]*[a-zA-Z]`)

const maxPatternResponseLimit = 1000
const (
	patternVarPlaceholder = "<_>"
	patternMinTokens      = 4
	patternMaxTokens      = 80
	patternSimThreshold   = 0.3
	patternMaxLineLength  = 3000
)

type patternBucket struct {
	pattern     string
	level       string
	buckets     map[int64]int
	total       int
	tokens      []string
	spacesAfter []int
}

// decolorizeStreams strips ANSI escape sequences from all log lines in streams.
// Implements Loki's `| decolorize` pipe at the proxy level.
// TODO: Remove when VL adds native decolorize support.
func decolorizeStreams(streams []map[string]interface{}) {
	for _, stream := range streams {
		values, ok := stream["values"].([][]string)
		if !ok {
			continue
		}
		for i, val := range values {
			if len(val) >= 2 {
				values[i][1] = ansiEscapeRe.ReplaceAllString(val[1], "")
			}
		}
	}
}

// ipFilterStreams filters log entries where a label value matches a CIDR range.
// Implements Loki's `| label = ip("CIDR")` filter at the proxy level.
// TODO: Remove when VL adds native IP range filtering.
func ipFilterStreams(streams []map[string]interface{}, labelName, cidr string) []map[string]interface{} {
	_, ipNet, err := net.ParseCIDR(cidr)
	if err != nil {
		// Not a valid CIDR — try as single IP
		ip := net.ParseIP(cidr)
		if ip == nil {
			return streams // invalid filter, return unfiltered
		}
		mask := net.CIDRMask(32, 32)
		if ip.To4() == nil {
			mask = net.CIDRMask(128, 128)
		}
		ipNet = &net.IPNet{IP: ip, Mask: mask}
	}

	result := make([]map[string]interface{}, 0, len(streams))
	for _, stream := range streams {
		labels, _ := stream["stream"].(map[string]string)
		if labels == nil {
			continue
		}

		ipStr, ok := labels[labelName]
		if !ok {
			continue
		}

		ip := net.ParseIP(ipStr)
		if ip != nil && ipNet.Contains(ip) {
			result = append(result, stream)
		}
	}
	return result
}

// parseIPFilter extracts the label name and CIDR from a LogQL ip() filter expression.
// Supports: `| addr = ip("192.168.0.0/16")` and `| addr == ip("10.0.0.0/8")`
func parseIPFilter(query string) (label, cidr string, ok bool) {
	// Match: label = ip("CIDR") or label == ip("CIDR")
	re := regexp.MustCompile(`(\w+)\s*==?\s*ip\(\s*"([^"]+)"\s*\)`)
	m := re.FindStringSubmatch(query)
	if m == nil {
		return "", "", false
	}
	return m[1], m[2], true
}

// applyLineFormatTemplate applies a Go text/template to log line values.
// Implements Loki's `| line_format "{{.status}} {{.method | ToUpper}}"` with full template support.
// TODO: Remove when VL adds equivalent template formatting.
func applyLineFormatTemplate(streams []map[string]interface{}, tmplStr string) {
	tmplStr = strings.Trim(tmplStr, `"`)

	// Register Loki-compatible template functions
	funcMap := template.FuncMap{
		"ToUpper":    strings.ToUpper,
		"ToLower":    strings.ToLower,
		"Title":      titleCase,
		"Replace":    strings.Replace,
		"TrimSpace":  strings.TrimSpace,
		"TrimPrefix": strings.TrimPrefix,
		"TrimSuffix": strings.TrimSuffix,
		"HasPrefix":  strings.HasPrefix,
		"HasSuffix":  strings.HasSuffix,
		"Contains":   strings.Contains,
		"default": func(def string, val ...string) string {
			// In pipeline: {{.field | default "N/A"}} — val[0] is the piped value
			if len(val) > 0 && val[0] != "" {
				return val[0]
			}
			return def
		},
	}

	tmpl, err := template.New("line_format").Option("missingkey=zero").Funcs(funcMap).Parse(tmplStr)
	if err != nil {
		return // invalid template, leave lines unchanged
	}

	for _, stream := range streams {
		labels, _ := stream["stream"].(map[string]string)
		if labels == nil {
			labels = map[string]string{}
		}

		values, ok := stream["values"].([][]string)
		if !ok {
			continue
		}

		for i, val := range values {
			if len(val) < 2 {
				continue
			}

			// Build template data from labels + line
			data := make(map[string]string, len(labels)+1)
			for k, v := range labels {
				data[k] = v
			}
			data["_line"] = val[1]

			var buf strings.Builder
			if err := tmpl.Execute(&buf, data); err == nil {
				values[i][1] = buf.String()
			}
		}
	}
}

// extractLineFormatTemplate extracts the template string from a line_format query.
// Returns empty string if not found.
func extractLineFormatTemplate(query string) string {
	re := regexp.MustCompile(`\|\s*line_format\s+"((?:[^"\\]|\\.)*)"`)
	m := re.FindStringSubmatch(query)
	if m != nil {
		return m[1]
	}
	return ""
}

// extractLogPatterns implements Loki-style Drain-inspired pattern extraction.
// It tokenizes lines with punctuation-aware splitting, clusters by similarity,
// and returns Grafana Logs Drilldown compatible pattern buckets.
func extractLogPatterns(vlBody []byte, step string, limit int) []map[string]interface{} {
	stepSeconds := int64(60)
	if step != "" {
		if d, err := time.ParseDuration(step); err == nil && d > 0 {
			stepSeconds = int64(d / time.Second)
		} else if seconds, err := strconv.Atoi(step); err == nil && seconds > 0 {
			stepSeconds = int64(seconds)
		}
	}

	lines := strings.Split(string(vlBody), "\n")
	miner := newPatternMiner()

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var entry map[string]interface{}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		msg, _ := entry["_msg"].(string)
		if msg == "" {
			continue
		}
		timeStr, _ := entry["_time"].(string)
		ts, err := time.Parse(time.RFC3339Nano, timeStr)
		if err != nil {
			if ts, err = time.Parse(time.RFC3339, timeStr); err != nil {
				continue
			}
		}
		labels := buildEntryLabels(entry)
		level := strings.TrimSpace(labels["detected_level"])
		if level == "" {
			level = strings.TrimSpace(labels["level"])
		}
		bucket := ts.Unix()
		if stepSeconds > 0 {
			bucket = (bucket / stepSeconds) * stepSeconds
		}
		miner.Observe(level, msg, bucket)
	}

	// Sort by count descending
	if limit <= 0 {
		limit = 50
	}
	if limit > maxPatternResponseLimit {
		limit = maxPatternResponseLimit
	}
	clusters := miner.AllClusters()
	entryCap := len(clusters)
	if entryCap > maxPatternResponseLimit {
		entryCap = maxPatternResponseLimit
	}
	entries := make([]*patternBucket, 0, entryCap)
	if len(clusters) <= limit {
		entries = append(entries, clusters...)
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].total > entries[j].total
		})
	} else {
		h := &patternBucketHeap{}
		heap.Init(h)
		for _, entry := range clusters {
			if h.Len() < limit {
				heap.Push(h, entry)
				continue
			}
			if (*h)[0].total < entry.total {
				(*h)[0] = entry
				heap.Fix(h, 0)
			}
		}
		for h.Len() > 0 {
			entries = append(entries, heap.Pop(h).(*patternBucket))
		}
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].total > entries[j].total
		})
	}

	result := make([]map[string]interface{}, 0, len(entries))
	for _, e := range entries {
		e.pattern = miner.tokenizer.Join(e.tokens, e.spacesAfter)
		sampleTimes := make([]int64, 0, len(e.buckets))
		for bucket := range e.buckets {
			sampleTimes = append(sampleTimes, bucket)
		}
		sort.Slice(sampleTimes, func(i, j int) bool { return sampleTimes[i] < sampleTimes[j] })

		samples := make([][]interface{}, 0, len(sampleTimes))
		for _, bucket := range sampleTimes {
			samples = append(samples, []interface{}{bucket, e.buckets[bucket]})
		}

		item := map[string]interface{}{
			"pattern": e.pattern,
			"samples": samples,
		}
		if e.level != "" {
			item["level"] = e.level
		}
		result = append(result, item)
	}
	return result
}

type patternMiner struct {
	tokenizer *patternLineTokenizer
	// level -> tokenCount -> clusters
	groups map[string]map[int][]*patternBucket
}

func newPatternMiner() *patternMiner {
	return &patternMiner{
		tokenizer: newPatternLineTokenizer(),
		groups:    make(map[string]map[int][]*patternBucket),
	}
}

func (m *patternMiner) Observe(level, line string, bucket int64) {
	tokens, spacesAfter, ok := m.tokenizer.Tokenize(line)
	if !ok {
		return
	}
	tokenCount := len(tokens)

	levelGroups := m.groups[level]
	if levelGroups == nil {
		levelGroups = make(map[int][]*patternBucket)
		m.groups[level] = levelGroups
	}
	candidates := levelGroups[tokenCount]
	cluster := bestPatternCluster(candidates, tokens)
	if cluster == nil {
		cluster = &patternBucket{
			level:       level,
			buckets:     make(map[int64]int),
			tokens:      cloneTokens(tokens),
			spacesAfter: cloneInts(spacesAfter),
		}
		levelGroups[tokenCount] = append(levelGroups[tokenCount], cluster)
	} else {
		cluster.tokens = mergePatternTemplate(cluster.tokens, tokens)
	}
	cluster.buckets[bucket]++
	cluster.total++
}

func (m *patternMiner) AllClusters() []*patternBucket {
	out := make([]*patternBucket, 0)
	for _, byCount := range m.groups {
		for _, clusters := range byCount {
			out = append(out, clusters...)
		}
	}
	return out
}

func bestPatternCluster(candidates []*patternBucket, tokens []string) *patternBucket {
	var match *patternBucket
	maxSim := -1.0
	maxParamCount := -1

	for _, cluster := range candidates {
		curSim, paramCount := getPatternSimilarity(cluster.tokens, tokens)
		if paramCount < 0 {
			continue
		}
		if curSim > maxSim || (curSim == maxSim && paramCount > maxParamCount) {
			maxSim = curSim
			maxParamCount = paramCount
			match = cluster
		}
	}
	if maxSim >= patternSimThreshold {
		return match
	}
	return nil
}

func getPatternSimilarity(templateTokens, tokens []string) (float64, int) {
	if len(templateTokens) != len(tokens) || len(templateTokens) == 0 {
		return 0, -1
	}
	simTokens := 0
	paramCount := 0
	for i := range templateTokens {
		switch templateTokens[i] {
		case patternVarPlaceholder:
			paramCount++
		case tokens[i]:
			simTokens++
		}
	}
	return float64(simTokens) / float64(len(templateTokens)), paramCount
}

func mergePatternTemplate(templateTokens, tokens []string) []string {
	if len(templateTokens) != len(tokens) {
		return templateTokens
	}
	for i := range templateTokens {
		if templateTokens[i] != tokens[i] {
			templateTokens[i] = patternVarPlaceholder
		}
	}
	return templateTokens
}

type patternLineTokenizer struct {
	includeDelimiters [128]rune
	excludeDelimiters [128]rune
}

func newPatternLineTokenizer() *patternLineTokenizer {
	var included [128]rune
	var excluded [128]rune
	included['='] = 1
	excluded['_'] = 1
	excluded['-'] = 1
	excluded['.'] = 1
	excluded[':'] = 1
	excluded['/'] = 1
	return &patternLineTokenizer{
		includeDelimiters: included,
		excludeDelimiters: excluded,
	}
}

func (p *patternLineTokenizer) Tokenize(line string) ([]string, []int, bool) {
	if len(line) == 0 || len(line) > patternMaxLineLength {
		return nil, nil, false
	}
	tokens := make([]string, 0, 128)
	spacesAfter := make([]int, 0, 64)

	start := 0
	for i, char := range line {
		if len(tokens) >= 127 {
			break
		}
		if unicode.IsLetter(char) || unicode.IsNumber(char) || (char < 128 && p.excludeDelimiters[char] != 0) {
			continue
		}
		included := char < 128 && p.includeDelimiters[char] != 0
		if char == ' ' || included || unicode.IsPunct(char) {
			if i > start {
				tokens = append(tokens, line[start:i])
			}
			if char == ' ' {
				spacesAfter = append(spacesAfter, len(tokens)-1)
			} else {
				tokens = append(tokens, line[i:i+1])
			}
			start = i + 1
		}
	}
	if start < len(line) {
		tokens = append(tokens, line[start:])
	}

	if len(tokens) < patternMinTokens || len(tokens) > patternMaxTokens {
		return nil, nil, false
	}
	return tokens, spacesAfter, true
}

func (p *patternLineTokenizer) Join(tokens []string, spacesAfter []int) string {
	if len(tokens) == 0 {
		return ""
	}
	strBuilder := strings.Builder{}
	spacesIdx := 0
	for i, token := range tokens {
		out := token
		if token == patternVarPlaceholder {
			out = patternVarPlaceholder
		}
		strBuilder.WriteString(out)
		for spacesIdx < len(spacesAfter) && i == spacesAfter[spacesIdx] {
			strBuilder.WriteByte(' ')
			spacesIdx++
		}
	}
	return deduplicatePlaceholders(strBuilder.String(), patternVarPlaceholder)
}

func deduplicatePlaceholders(line, placeholder string) string {
	first := strings.Index(line, placeholder+placeholder)
	if first == -1 {
		return line
	}
	var b strings.Builder
	b.Grow(len(line))
	i := 0
	for i < len(line) {
		if strings.HasPrefix(line[i:], placeholder+placeholder) {
			b.WriteString(placeholder)
			i += len(placeholder)
			for i < len(line) && strings.HasPrefix(line[i:], placeholder) {
				i += len(placeholder)
			}
			continue
		}
		b.WriteByte(line[i])
		i++
	}
	return b.String()
}

func cloneTokens(tokens []string) []string {
	out := make([]string, len(tokens))
	copy(out, tokens)
	return out
}

func cloneInts(src []int) []int {
	out := make([]int, len(src))
	copy(out, src)
	return out
}

type patternBucketHeap []*patternBucket

func (h patternBucketHeap) Len() int           { return len(h) }
func (h patternBucketHeap) Less(i, j int) bool { return h[i].total < h[j].total }
func (h patternBucketHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *patternBucketHeap) Push(x interface{}) {
	*h = append(*h, x.(*patternBucket))
}
func (h *patternBucketHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// tokenizeToPattern converts a log line into a pattern by replacing variable parts with <_>.
// This is a simplified version of the drain log pattern mining algorithm.
func tokenizeToPattern(line string) string {
	// Try JSON first
	if strings.HasPrefix(line, "{") {
		return tokenizeJSONPattern(line)
	}

	tokens := strings.Fields(line)
	result := make([]string, len(tokens))
	for i, token := range tokens {
		if isVariableToken(token) {
			result[i] = "<_>"
		} else {
			result[i] = token
		}
	}
	return strings.Join(result, " ")
}

// isVariableToken returns true if a token is likely a variable value (not a structural constant).
func isVariableToken(token string) bool {
	// Numbers (possibly with decimals, units)
	if len(token) > 0 && (token[0] >= '0' && token[0] <= '9') {
		return true
	}

	// UUIDs, hashes, IDs
	if len(token) >= 8 && isHexLike(token) {
		return true
	}

	// IP addresses
	if isIPLike(token) {
		return true
	}

	// Timestamps (ISO, RFC3339)
	if strings.Contains(token, "T") && strings.Contains(token, ":") {
		return true
	}

	// Paths with many segments
	if strings.HasPrefix(token, "/") && strings.Count(token, "/") > 2 {
		return true
	}

	// Contains mostly digits
	digitCount := 0
	for _, ch := range token {
		if unicode.IsDigit(ch) {
			digitCount++
		}
	}
	if len(token) > 3 && digitCount > len(token)/2 {
		return true
	}

	return false
}

// titleCase uppercases the first letter of each word (simple ASCII replacement for deprecated strings.Title).
func titleCase(s string) string {
	prev := ' '
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(rune(prev)) || prev == ' ' {
			prev = r
			return unicode.ToTitle(r)
		}
		prev = r
		return r
	}, s)
}

func isHexLike(s string) bool {
	s = strings.TrimPrefix(s, "0x")
	for _, ch := range s {
		isHex := (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F') || ch == '-'
		if !isHex {
			return false
		}
	}
	return true
}

func isIPLike(s string) bool {
	parts := strings.Split(s, ".")
	if len(parts) != 4 {
		return false
	}
	for _, p := range parts {
		if len(p) == 0 || len(p) > 3 {
			return false
		}
		for _, ch := range p {
			if ch < '0' || ch > '9' {
				return false
			}
		}
	}
	return true
}

func tokenizeJSONPattern(line string) string {
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(line), &data); err != nil {
		return "<_>"
	}
	// Create pattern from JSON keys (sorted)
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"=<_>")
	}
	return "{" + strings.Join(parts, " ") + "}"
}
