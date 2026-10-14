package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The pipes that derive detected_level (translator.BuildDetectedLevelChain)
// each pass every block of a window, so the full chain, which knows 16 stored
// level fields and 14 body keys, costs far more than a window that holds one
// field and one key needs. One VictoriaLogs stats query tells which of them
// the window holds, and the chain is built from those alone.
//
// The answer is a mask that may only over-report. A field or key reported
// present that is not costs a pipe; one reported absent that a line holds
// would be a wrong answer. So:
//   - the probe runs over the stats query's own base and window;
//   - a stored field counts as present when any row has it non-empty;
//   - a body key counts as present when its words occur in the line (the same
//     word index the guard probes use), and only rows that no definite stored
//     level field decides are looked at, because the chain unpacks no other;
//   - an escape sequence (\u) in such a line may spell a key (unpack_json
//     decodes `level`), which the word test cannot see, so one makes
//     every body key present;
//   - a probe error, or a response that is not the expected row, yields the
//     full chain.
//
// The mask of a window is cached per tenant and base like the guard probes'
// verdicts (coveredWindowProbe): only the part older than statsPushdownRiskSettle
// is remembered, the rest is probed on every request, so a key that first
// appears in recent lines is seen at once.

// levelPresenceUndecided lists the stored fields that do not decide a row's
// level by themselves; every other level field, when non-empty, does (the
// chain's stored pick for it has no further condition).
var levelPresenceUndecided = []string{"severity", "severity_text", "severity_number"}

// levelPresenceLayout is the order of the mask: the stored fields, the body
// keys, and the escape flag.
var (
	levelPresenceStored = translator.DetectedLevelFieldNames
	levelPresenceBody   = translator.DetectedLevelBodyKeys()
)

func levelPresenceMaskLen() int { return len(levelPresenceStored) + len(levelPresenceBody) + 1 }

// notLevelWordRune is true for the runes that end a word of VictoriaLogs'
// tokenizer in a level key (ASCII letters, digits and the underscore continue it).
func notLevelWordRune(r rune) bool {
	return r != '_' && (r < '0' || r > '9') && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z')
}

// levelPresenceQuery returns the probe: one row of counts, one per stored
// field, per body key and for the escape flag, in mask order.
func levelPresenceQuery(base string) string {
	var decided []string
	for _, name := range levelPresenceStored {
		if !containsString(levelPresenceUndecided, name) {
			decided = append(decided, quoteLogsQLIdent(name)+":*")
		}
	}
	counts := make([]string, 0, levelPresenceMaskLen())
	for i, name := range levelPresenceStored {
		counts = append(counts, "count() if ("+quoteLogsQLIdent(name)+":*) as p"+strconv.Itoa(i))
	}
	for i, key := range levelPresenceBody {
		words := strings.FieldsFunc(key, notLevelWordRune)
		for j, w := range words {
			words[j] = "_msg:" + strconv.Quote(w)
		}
		counts = append(counts, "count() if (-__p_h:* "+strings.Join(words, " ")+") as p"+strconv.Itoa(len(levelPresenceStored)+i))
	}
	counts = append(counts, `count() if (-__p_h:* _msg:~"\\\\u") as p`+strconv.Itoa(levelPresenceMaskLen()-1))
	return base + ` | format if (` + strings.Join(decided, " OR ") + `) "1" as __p_h | stats ` + strings.Join(counts, ", ")
}

// levelPresenceMask runs the probe over [start, end] and returns its mask.
func (p *Proxy) levelPresenceMask(ctx context.Context, base string, start, end time.Time) (string, error) {
	params := url.Values{"query": {levelPresenceQuery(base)}, "start": {start.UTC().Format(time.RFC3339Nano)}, "end": {end.Add(time.Nanosecond).UTC().Format(time.RFC3339Nano)}}
	resp, err := p.vlPost(ctx, "/select/logsql/query", params)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
	if err != nil {
		return "", err
	}
	if resp.StatusCode >= 400 {
		return "", p.redactedBackendStatusError("backend returned", resp.StatusCode, body)
	}
	var row map[string]string
	line, _, _ := strings.Cut(strings.TrimSpace(string(body)), "\n")
	if err := json.Unmarshal([]byte(line), &row); err != nil {
		return "", fmt.Errorf("level presence probe: %w", err)
	}
	mask := make([]byte, levelPresenceMaskLen())
	for i := range mask {
		n, ok := row["p"+strconv.Itoa(i)]
		if !ok {
			return "", fmt.Errorf("level presence probe: no count p%d", i)
		}
		mask[i] = '0'
		if v, err := strconv.ParseInt(n, 10, 64); err != nil || v > 0 {
			mask[i] = '1' // an unreadable count over-reports
		}
	}
	return string(mask), nil
}

// orMasks returns the union of two masks; an empty one is the identity.
func orMasks(a, b string) string {
	if a == "" || len(a) != len(b) {
		return b
	}
	out := []byte(a)
	for i := range out {
		if b[i] == '1' {
			out[i] = '1'
		}
	}
	return string(out)
}

// levelPresenceFromMask turns a mask into the chain's presence set.
func levelPresenceFromMask(mask string) *translator.DetectedLevelPresence {
	present := &translator.DetectedLevelPresence{}
	escaped := mask[len(mask)-1] == '1'
	for i, name := range levelPresenceStored {
		if mask[i] == '1' {
			present.Stored = append(present.Stored, name)
		}
	}
	for i, key := range levelPresenceBody {
		if escaped || mask[len(levelPresenceStored)+i] == '1' {
			present.BodyKeys = append(present.BodyKeys, key)
		}
	}
	return present
}

// fullLevelChain is the chain every route puts into a query that groups by or
// filters on detected_level; vlPostHTTP narrows it (pruneLevelChain).
var fullLevelChain = sync.OnceValue(func() string {
	return translator.BuildDetectedLevelChain(translator.DetectedLevelFieldNames, translator.DetectedLevelChainGrouping, nil)
})

// levelChainDelete ends the chain; the native route drops it from its query,
// so the chain is matched and replaced without it.
const levelChainDelete = " | delete __dl, __dl_*, __j_*, __l_*"

// pruneLevelChain narrows the detected_level chain of a stats_query_range
// request to the fields and keys its window holds. The pipes before the chain
// are the rows the chain reads, so they are the probe's base; the request's
// start and end are its window. Other requests (an instant query has no
// window to probe), a probe that fails, and a query without the chain keep
// the query as it is.
func (p *Proxy) pruneLevelChain(ctx context.Context, path string, params url.Values) url.Values {
	query := params.Get("query")
	full := strings.TrimSuffix(fullLevelChain(), levelChainDelete)
	// One chain only: its base is the probe's, and a second chain (a query of
	// several segments) may sit on a different one.
	at := strings.Index(query, full)
	if path != "/select/logsql/stats_query_range" || at < 0 || strings.Count(query, full) != 1 {
		return params
	}
	from, okFrom := parseFlexibleUnixNanos(params.Get("start"))
	to, okTo := parseFlexibleUnixNanos(params.Get("end"))
	if !okFrom || !okTo {
		return params
	}
	present := p.detectedLevelPresence(ctx, strings.TrimSpace(query[:at]), time.Unix(0, from), time.Unix(0, to))
	if present == nil {
		return params
	}
	out := cloneURLValues(params)
	pruned := translator.BuildDetectedLevelChain(translator.DetectedLevelFieldNames, translator.DetectedLevelChainGrouping, present)
	out.Set("query", strings.Replace(query, full, strings.TrimSuffix(pruned, levelChainDelete), 1))
	return out
}

// detectedLevelPresence returns the level fields and body keys the stats query
// over [start, end] on base can read, or nil (the full chain) when they
// cannot be told.
func (p *Proxy) detectedLevelPresence(ctx context.Context, base string, start, end time.Time) *translator.DetectedLevelPresence {
	key := "level-presence:v1:" + getOrgID(ctx) + ":" + base
	mask, _, err := p.coveredWindowProbe(ctx, key, start, end, func(from, to time.Time, acc string) (string, bool, error) {
		found, err := p.levelPresenceMask(ctx, base, from, to)
		return orMasks(acc, found), false, err
	})
	if err != nil || len(mask) != levelPresenceMaskLen() {
		return nil
	}
	return levelPresenceFromMask(mask)
}

// coveredWindowProbe runs check over the parts of [start, end] that the entry
// under key does not already cover, passing it what the entry (and the parts
// checked before) found, and returns what the last check returned; a check that
// stops, or fails, ends the run and nothing is remembered. The entry covers a
// window per tenant and key, so a refresh or a wider range checks only the
// uncovered part. It never covers time later than statsPushdownRiskSettle
// before now, whatever the window, so lines arriving late for recent
// timestamps are checked on every request, and it keeps the age of the check
// that covered each part: extending an entry leaves its expiry where it was,
// because a part checked long ago must not look checked now.
func (p *Proxy) coveredWindowProbe(ctx context.Context, key string, start, end time.Time, check func(from, to time.Time, acc string) (string, bool, error)) (string, bool, error) {
	if p.cache == nil {
		return check(start, end, "")
	}
	// What a window holds depends on what the caller may see: the tenant, the
	// routing and the forwarded identity, as for every cached response.
	key += ":scope:" + p.contextScopeFingerprint(ctx)
	type window struct{ from, to time.Time }
	todo := []window{{start, end}}
	coveredFrom, coveredTo := start, end
	acc, ttl := "", statsPushdownRiskCacheTTL
	if raw, remaining, ok := p.cache.GetWithTTL(key); ok {
		parts := strings.SplitN(string(raw), " ", 3)
		var fromNs, toNs int64
		var err error
		if len(parts) == 3 {
			fromNs, err = strconv.ParseInt(parts[0], 10, 64)
			if err == nil {
				toNs, err = strconv.ParseInt(parts[1], 10, 64)
			}
		}
		cachedFrom, cachedTo := time.Unix(0, fromNs), time.Unix(0, toNs)
		if len(parts) == 3 && err == nil && !start.After(cachedTo) && !end.Before(cachedFrom) {
			acc = parts[2]
			if remaining < ttl {
				ttl = remaining
			}
			todo = todo[:0]
			if start.Before(cachedFrom) {
				todo = append(todo, window{start, cachedFrom})
			} else {
				coveredFrom = cachedFrom
			}
			if end.After(cachedTo) {
				todo = append(todo, window{cachedTo, end})
			} else {
				coveredTo = cachedTo
			}
		}
	}
	for _, w := range todo {
		var stop bool
		var err error
		if acc, stop, err = check(w.from, w.to, acc); err != nil || stop {
			return acc, stop, err
		}
	}
	if settled := time.Now().Add(-statsPushdownRiskSettle); coveredTo.After(settled) {
		coveredTo = settled
	}
	if len(todo) > 0 && coveredTo.After(coveredFrom) {
		// Only a window that was actually checked refreshes the entry (see above).
		p.cache.SetLocalOnlyWithTTL(key, []byte(fmt.Sprintf("%d %d %s", coveredFrom.UnixNano(), coveredTo.UnixNano(), acc)), ttl)
	}
	return acc, false, nil
}

// The fields a plan reading detected_level groups by when the stored level
// fields alone decide it (orderedJSONStatsBucketsWithReason): copies of the
// stored detected_level and level, taken before any unpack so that a body key
// never looks stored.
const (
	storedDetectedLevelField = "__dl_d"
	storedLevelField         = "__dl_l"
)

// storedLevelSeries gives each series of a stats query grouped by the stored
// level fields its detected_level label, applies the filters on it, and reports
// false when a series is one the stored fields do not decide exactly: no
// stored level (the line decides), or a stored detected_level (a stream label
// keeps its stored spelling, structured metadata is normalised; the stats
// cannot tell them apart). A stored detected_level outranks level, which
// outranks every later field, so a series with a level needs no other field.
func storedLevelSeries(series map[string]manualSeriesSamples, filters []translator.DropCondition) (map[string]manualSeriesSamples, bool) {
	out := make(map[string]manualSeriesSamples, len(series))
	for key, entry := range series {
		stored := entry.Metric[storedLevelField]
		if stored == "" || entry.Metric[storedDetectedLevelField] != "" {
			return nil, false
		}
		level := translator.NormalizeStoredLevel(stored)
		keep := true
		for _, filter := range filters {
			keep = keep && filter.Matches(level)
		}
		if !keep {
			continue
		}
		metric := make(map[string]string, len(entry.Metric)+1)
		for k, v := range entry.Metric {
			metric[k] = v
		}
		metric["detected_level"] = level
		entry.Metric = metric
		out[key] = entry
	}
	return out, true
}

// levelNeedsChain reports whether a window of base held rows the stored level
// fields do not decide, so the next ask goes to the chain at once; the verdict
// expires with the guard verdicts, so data that changes is tried again.
func (p *Proxy) levelNeedsChain(ctx context.Context, base string) bool {
	if p.cache == nil {
		return false
	}
	_, _, ok := p.cache.GetWithTTL(levelNeedsChainKey(p, ctx, base))
	return ok
}

func (p *Proxy) markLevelNeedsChain(ctx context.Context, base string) {
	if p.cache != nil {
		p.cache.SetLocalOnlyWithTTL(levelNeedsChainKey(p, ctx, base), []byte("1"), statsPushdownRiskCacheTTL)
	}
}

// levelShapeSeparator follows the base in the verdict of one query shape, which
// a read past the group guard leaves (the same rows may answer a query with
// fewer groups from the stored fields).
const levelShapeSeparator = "\x00"

func levelNeedsChainKey(p *Proxy, ctx context.Context, base string) string {
	if at := strings.Index(base, levelShapeSeparator); at >= 0 {
		// the shape is long: its digest stands for it
		sum := sha256.Sum256([]byte(base[at+1:]))
		base = base[:at+1] + hex.EncodeToString(sum[:8])
	}
	return "level-needs-chain:v1:" + getOrgID(ctx) + ":" + p.contextScopeFingerprint(ctx) + ":" + base
}
