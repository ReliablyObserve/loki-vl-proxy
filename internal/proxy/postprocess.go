package proxy

import (
	"bufio"
	"bytes"
	stdjson "encoding/json"
	"net"
	"regexp"
	"sort"
	"strings"
	"sync"
	"text/template"
	"time"
	"unicode"
)

// ansiEscapeRe matches ANSI escape sequences (color codes, cursor movement, etc.)
var ansiEscapeRe = regexp.MustCompile(`\x1b\[[0-9;]*[a-zA-Z]`)

const maxPatternResponseLimit = 1000
const (
	patternVarPlaceholder  = "<_>"
	patternNumPlaceholder  = "<num>"
	patternUUIDPlaceholder = "<uuid>"
	patternIPPlaceholder   = "<ip>"
	patternPathPlaceholder = "<path>"
	patternHexPlaceholder  = "<hex>"
	patternTSPlaceholder   = "<ts>"
	patternMinTokens       = 4
	patternMaxTokens       = 80
	patternSimThreshold    = 0.3
	patternMaxLineLength   = 3000
	patternPrefixDepth     = 4
)

type patternBucket struct {
	pattern     string
	level       string
	buckets     map[int64]int
	total       int
	tokens      []string
	spacesAfter []int
}

type patternExtractionStats struct {
	scannedLines  int
	observedLines int
	patternCount  int
}

func (s patternExtractionStats) hitLimit(limit int) bool {
	return limit > 0 && s.scannedLines >= limit
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
			data := make(map[string]string, safeAddCap(len(labels), 1))
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
	patterns, _ := extractLogPatternsWithStats(vlBody, step, limit)
	return patterns
}

func extractLogPatternsWithStats(vlBody []byte, step string, limit int) ([]map[string]interface{}, patternExtractionStats) {
	stepSeconds := parsePatternStepSeconds(step)
	miner := newPatternMiner()
	stats := patternExtractionStats{}
	if len(vlBody) == 0 {
		return nil, stats
	}

	scanner := bufio.NewScanner(bytes.NewReader(vlBody))
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	entry := make(map[string]interface{}, 16)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		stats.scannedLines++
		for key := range entry {
			delete(entry, key)
		}
		if err := stdjson.Unmarshal(line, &entry); err != nil {
			continue
		}
		msg, ok := patternMessageFromEntry(entry)
		if !ok {
			continue
		}
		unixSeconds, ok := patternUnixSecondsFromEntry(entry)
		if !ok {
			continue
		}
		labels := buildEntryLabels(entry)
		level := strings.TrimSpace(labels["detected_level"])
		if level == "" {
			level = strings.TrimSpace(labels["level"])
		}
		bucket := unixSeconds
		if stepSeconds > 0 {
			bucket = (bucket / stepSeconds) * stepSeconds
		}
		miner.Observe(level, msg, bucket)
		stats.observedLines++
	}
	if stats.observedLines > 0 {
		patterns := buildPatternResponse(miner, limit)
		stats.patternCount = len(patterns)
		return patterns, stats
	}

	var decoded interface{}
	if err := stdjson.Unmarshal(vlBody, &decoded); err != nil {
		return nil, stats
	}
	collectPatternObservationsFromJSON(miner, decoded, stepSeconds, "", &stats.observedLines)
	if stats.observedLines == 0 {
		return nil, stats
	}
	patterns := buildPatternResponse(miner, limit)
	stats.patternCount = len(patterns)
	return patterns, stats
}

func extractLogPatternsFromWindowEntries(entries []queryRangeWindowEntry, step string, limit int) []map[string]interface{} {
	patterns, _ := extractLogPatternsFromWindowEntriesWithStats(entries, step, limit)
	return patterns
}

func extractLogPatternsFromWindowEntriesWithStats(entries []queryRangeWindowEntry, step string, limit int) ([]map[string]interface{}, patternExtractionStats) {
	if len(entries) == 0 {
		return nil, patternExtractionStats{}
	}
	stepSeconds := parsePatternStepSeconds(step)
	miner := newPatternMiner()
	stats := patternExtractionStats{}
	for _, entry := range entries {
		stats.scannedLines++
		if len(entry.Value) < 2 {
			continue
		}
		unixSeconds, ok := parsePatternUnixSeconds(entry.Value[0])
		if !ok {
			continue
		}
		msg, ok := stringifyEntryValue(entry.Value[1])
		if !ok || strings.TrimSpace(msg) == "" {
			continue
		}
		level := strings.TrimSpace(entry.Stream["detected_level"])
		if level == "" {
			level = strings.TrimSpace(entry.Stream["level"])
		}
		bucket := unixSeconds
		if stepSeconds > 0 {
			bucket = (bucket / stepSeconds) * stepSeconds
		}
		miner.Observe(level, msg, bucket)
		stats.observedLines++
	}
	patterns := buildPatternResponse(miner, limit)
	stats.patternCount = len(patterns)
	return patterns, stats
}

func parsePatternStepSeconds(step string) int64 {
	stepSeconds := int64(60)
	if step == "" {
		return stepSeconds
	}
	if d, ok := parsePositiveStepDuration(step); ok && d >= time.Second {
		return int64(d / time.Second)
	}
	return stepSeconds
}

func parsePatternUnixSeconds(raw interface{}) (int64, bool) {
	timeStr, ok := stringifyEntryValue(raw)
	if !ok {
		return 0, false
	}
	timeStr = strings.TrimSpace(timeStr)
	if timeStr == "" {
		return 0, false
	}
	if parsed, err := time.Parse(time.RFC3339Nano, timeStr); err == nil {
		return parsed.Unix(), true
	}
	if parsed, err := time.Parse(time.RFC3339, timeStr); err == nil {
		return parsed.Unix(), true
	}
	// Reuse Loki-compatible parser for numeric, RFC3339, and relative ("now-24h")
	// boundaries so pattern sampling/filling stays stable with Drilldown ranges.
	if ns, ok := parseLokiTimeToUnixNano(timeStr); ok {
		return ns / int64(time.Second), true
	}
	return 0, false
}

func patternMessageFromEntry(entry map[string]interface{}) (string, bool) {
	for _, key := range []string{"_msg", "message", "msg", "line", "log"} {
		msg, ok := stringifyEntryValue(entry[key])
		if ok && strings.TrimSpace(msg) != "" {
			return msg, true
		}
	}
	return "", false
}

func patternUnixSecondsFromEntry(entry map[string]interface{}) (int64, bool) {
	for _, key := range []string{"_time", "time", "timestamp", "ts"} {
		if unixSeconds, ok := parsePatternUnixSeconds(entry[key]); ok {
			return unixSeconds, true
		}
	}
	return 0, false
}

func collectPatternObservationsFromJSON(miner *patternMiner, decoded interface{}, stepSeconds int64, inheritedLevel string, observed *int) {
	switch value := decoded.(type) {
	case map[string]interface{}:
		if msg, ok := patternMessageFromEntry(value); ok {
			if unixSeconds, ok := patternUnixSecondsFromEntry(value); ok {
				level := inheritedLevel
				if level == "" {
					labels := buildEntryLabels(value)
					level = strings.TrimSpace(labels["detected_level"])
					if level == "" {
						level = strings.TrimSpace(labels["level"])
					}
				}
				bucket := unixSeconds
				if stepSeconds > 0 {
					bucket = (bucket / stepSeconds) * stepSeconds
				}
				miner.Observe(level, msg, bucket)
				*observed = *observed + 1
			}
		}
		if rows, ok := value["results"].([]interface{}); ok {
			for _, row := range rows {
				collectPatternObservationsFromJSON(miner, row, stepSeconds, inheritedLevel, observed)
			}
		}
		if rows, ok := value["values"].([]interface{}); ok {
			for _, row := range rows {
				collectPatternObservationsFromJSON(miner, row, stepSeconds, inheritedLevel, observed)
			}
		}
		if dataMap, ok := value["data"].(map[string]interface{}); ok {
			if resultRows, ok := dataMap["result"].([]interface{}); ok {
				for _, row := range resultRows {
					rowMap, ok := row.(map[string]interface{})
					if !ok {
						continue
					}
					level := inheritedLevel
					if stream, ok := rowMap["stream"].(map[string]interface{}); ok {
						if v, ok := stringifyEntryValue(stream["detected_level"]); ok && strings.TrimSpace(v) != "" {
							level = strings.TrimSpace(v)
						} else if v, ok := stringifyEntryValue(stream["level"]); ok && strings.TrimSpace(v) != "" {
							level = strings.TrimSpace(v)
						}
					}
					values, _ := rowMap["values"].([]interface{})
					for _, pairRaw := range values {
						pair, ok := pairRaw.([]interface{})
						if !ok || len(pair) < 2 {
							continue
						}
						unixSeconds, ok := parsePatternUnixSeconds(pair[0])
						if !ok {
							continue
						}
						msg, ok := stringifyEntryValue(pair[1])
						if !ok || strings.TrimSpace(msg) == "" {
							continue
						}
						bucket := unixSeconds
						if stepSeconds > 0 {
							bucket = (bucket / stepSeconds) * stepSeconds
						}
						miner.Observe(level, msg, bucket)
						*observed = *observed + 1
					}
				}
			}
		}
	case []interface{}:
		for _, item := range value {
			collectPatternObservationsFromJSON(miner, item, stepSeconds, inheritedLevel, observed)
		}
	}
}

func buildPatternResponse(miner *patternMiner, limit int) []map[string]interface{} {
	if limit <= 0 {
		limit = 50
	}
	if limit > maxPatternResponseLimit {
		limit = maxPatternResponseLimit
	}
	clusters := miner.AllClusters()
	entries := make([]*patternBucket, 0, len(clusters))
	entries = append(entries, clusters...)
	for _, e := range entries {
		e.pattern = miner.tokenizer.Join(e.tokens, e.spacesAfter)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].total != entries[j].total {
			return entries[i].total > entries[j].total
		}
		if entries[i].level != entries[j].level {
			return entries[i].level < entries[j].level
		}
		return entries[i].pattern < entries[j].pattern
	})
	if len(entries) > limit {
		entries = entries[:limit]
	}

	result := make([]map[string]interface{}, 0, len(entries))
	for _, e := range entries {
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
	// level -> tokenCount -> structural signature -> clusters
	groups map[string]map[int]map[string][]*patternBucket
}

func newPatternMiner() *patternMiner {
	return &patternMiner{
		tokenizer: newPatternLineTokenizer(),
		groups:    make(map[string]map[int]map[string][]*patternBucket),
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
		levelGroups = make(map[int]map[string][]*patternBucket)
		m.groups[level] = levelGroups
	}
	signatureGroups := levelGroups[tokenCount]
	if signatureGroups == nil {
		signatureGroups = make(map[string][]*patternBucket)
		levelGroups[tokenCount] = signatureGroups
	}
	signature := patternStructureSignature(tokens)
	candidates := signatureGroups[signature]
	cluster := bestPatternCluster(candidates, tokens)
	if cluster == nil {
		cluster = &patternBucket{
			level:       level,
			buckets:     make(map[int64]int),
			tokens:      cloneTokens(tokens),
			spacesAfter: cloneInts(spacesAfter),
		}
		signatureGroups[signature] = append(signatureGroups[signature], cluster)
	} else {
		cluster.tokens = mergePatternTemplate(cluster.tokens, tokens)
	}
	cluster.buckets[bucket]++
	cluster.total++
}

func (m *patternMiner) AllClusters() []*patternBucket {
	out := make([]*patternBucket, 0)
	for _, byCount := range m.groups {
		for _, bySignature := range byCount {
			for _, clusters := range bySignature {
				out = append(out, clusters...)
			}
		}
	}
	return out
}

func bestPatternCluster(candidates []*patternBucket, tokens []string) *patternBucket {
	var match *patternBucket
	maxSim := -1.0
	maxParamCount := -1

	for _, cluster := range candidates {
		if !patternPrefixCompatible(cluster.tokens, tokens) {
			continue
		}
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

func patternPrefixCompatible(templateTokens, tokens []string) bool {
	if len(templateTokens) != len(tokens) || len(tokens) == 0 {
		return false
	}
	depth := patternPrefixDepth
	if depth > len(tokens) {
		depth = len(tokens)
	}
	for i := 0; i < depth; i++ {
		templateToken := templateTokens[i]
		inputToken := tokens[i]
		if patternPlaceholderMatchesToken(templateToken, inputToken) || templateToken == inputToken {
			continue
		}
		if samePatternPlaceholderKind(templateToken, inputToken) || patternTokenHasDigits(templateToken) || patternTokenHasDigits(inputToken) {
			continue
		}
		return false
	}
	return true
}

func patternTokenHasDigits(token string) bool {
	for _, ch := range token {
		if unicode.IsDigit(ch) {
			return true
		}
	}
	return false
}

func getPatternSimilarity(templateTokens, tokens []string) (float64, int) {
	if len(templateTokens) != len(tokens) || len(templateTokens) == 0 {
		return 0, -1
	}
	simTokens := 0
	paramCount := 0
	for i := range templateTokens {
		switch {
		case patternPlaceholderMatchesToken(templateTokens[i], tokens[i]):
			paramCount++
		case templateTokens[i] == tokens[i]:
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
			templateTokens[i] = commonPatternPlaceholder(templateTokens[i], tokens[i])
		}
	}
	return templateTokens
}

type patternLineTokenizer struct {
	includeDelimiters [128]rune
	excludeDelimiters [128]rune
}

// tokenizerBuf holds pre-allocated slices reused across Tokenize calls via a pool.
// The caller must call pool.Put after it is done with the returned slices.
type tokenizerBuf struct {
	tokens      []string
	spacesAfter []int
}

var tokenizerBufPool = sync.Pool{
	New: func() interface{} {
		return &tokenizerBuf{
			tokens:      make([]string, 0, 128),
			spacesAfter: make([]int, 0, 64),
		}
	},
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
	buf := tokenizerBufPool.Get().(*tokenizerBuf)
	tokens := buf.tokens[:0]
	spacesAfter := buf.spacesAfter[:0]

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
		buf.tokens = tokens
		buf.spacesAfter = spacesAfter
		tokenizerBufPool.Put(buf)
		return nil, nil, false
	}
	// Copy results to fresh slices before returning the pool buffer.
	// The caller holds the returned slices across multiple Tokenize calls.
	outTokens := make([]string, len(tokens))
	copy(outTokens, tokens)
	outSpaces := make([]int, len(spacesAfter))
	copy(outSpaces, spacesAfter)
	buf.tokens = tokens
	buf.spacesAfter = spacesAfter
	tokenizerBufPool.Put(buf)
	return outTokens, outSpaces, true
}

func (p *patternLineTokenizer) Join(tokens []string, spacesAfter []int) string {
	if len(tokens) == 0 {
		return ""
	}
	strBuilder := strings.Builder{}
	spacesIdx := 0
	for i, token := range tokens {
		out := token
		if isPatternPlaceholder(token) {
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

func patternStructureSignature(tokens []string) string {
	if len(tokens) == 0 {
		return ""
	}
	parts := make([]string, len(tokens))
	for i, token := range tokens {
		if i > 0 {
			prev := tokens[i-1]
			if prev == "=" || prev == ":" {
				if placeholder := patternPlaceholderKind(token); placeholder != "" {
					parts[i] = placeholder
				} else {
					parts[i] = patternVarPlaceholder
				}
				continue
			}
		}
		if placeholder := patternPlaceholderKind(token); placeholder != "" {
			parts[i] = placeholder
			continue
		}
		parts[i] = token
	}
	return strings.Join(parts, "\x1e")
}

func isPatternPlaceholder(token string) bool {
	switch token {
	case patternVarPlaceholder, patternNumPlaceholder, patternUUIDPlaceholder, patternIPPlaceholder, patternPathPlaceholder, patternHexPlaceholder, patternTSPlaceholder:
		return true
	default:
		return false
	}
}

func patternPlaceholderKind(token string) string {
	if isPatternPlaceholder(token) {
		return token
	}
	return patternPlaceholderForToken(token)
}

func patternPlaceholderMatchesToken(placeholder, token string) bool {
	if placeholder == "" {
		return false
	}
	if placeholder == patternVarPlaceholder {
		return true
	}
	return placeholder == patternPlaceholderForToken(token)
}

func samePatternPlaceholderKind(left, right string) bool {
	leftKind := patternPlaceholderKind(left)
	rightKind := patternPlaceholderKind(right)
	return leftKind != "" && leftKind == rightKind
}

func commonPatternPlaceholder(left, right string) string {
	leftKind := patternPlaceholderKind(left)
	rightKind := patternPlaceholderKind(right)
	if leftKind != "" && leftKind == rightKind {
		return leftKind
	}
	return patternVarPlaceholder
}

func patternPlaceholderForToken(token string) string {
	token = strings.TrimSpace(strings.Trim(token, `"'()[]{}<>,;`))
	if token == "" {
		return ""
	}
	switch {
	case isIPLike(token):
		return patternIPPlaceholder
	case isUUIDLike(token):
		return patternUUIDPlaceholder
	case isTimestampLike(token):
		return patternTSPlaceholder
	case isPathLike(token):
		return patternPathPlaceholder
	case isNumericLike(token):
		return patternNumPlaceholder
	case len(token) >= 8 && isHexLike(token):
		return patternHexPlaceholder
	default:
		return ""
	}
}

func isUUIDLike(token string) bool {
	if len(token) != 36 {
		return false
	}
	for i, ch := range token {
		switch i {
		case 8, 13, 18, 23:
			if ch != '-' {
				return false
			}
		default:
			isHex := (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')
			if !isHex {
				return false
			}
		}
	}
	return true
}

func isTimestampLike(token string) bool {
	return strings.Contains(token, "T") && strings.Contains(token, ":")
}

func isPathLike(token string) bool {
	return strings.HasPrefix(token, "/") && strings.Count(token, "/") >= 2
}

func isNumericLike(token string) bool {
	if token == "" {
		return false
	}
	if token[0] >= '0' && token[0] <= '9' {
		return true
	}
	digitCount := 0
	for _, ch := range token {
		if unicode.IsDigit(ch) {
			digitCount++
		}
	}
	return len(token) > 3 && digitCount > len(token)/2
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
	if err := stdjson.Unmarshal([]byte(line), &data); err != nil {
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
