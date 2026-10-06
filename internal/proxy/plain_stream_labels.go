package proxy

import (
	"context"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// A parser stage never overwrites a label the entry has from its stream in
// Loki: the parsed key is renamed name_extracted and a later filter, grouping
// or unwrap on the plain name reads the stream value. VictoriaLogs' parsers
// overwrite the stored field, so the translator reads the stored value aside
// for the stream label names this returns (translator.WithStreamLabels). A
// query that names no stream label of the tenant after a parser keeps its
// plain translation and so the Drilldown fast paths that match it.

const (
	// streamLabelNamesWindow is the recent window the tenant's stream label
	// names are listed over (through the metadata inventory, whose sealed
	// buckets /labels shares). A stream label that only older data has keeps
	// the plain translation.
	streamLabelNamesWindow = time.Hour
	// streamLabelNamesTTL is how long the names are served before a refresh.
	streamLabelNamesTTL = time.Minute
	// streamLabelNamesRetry spaces refreshes after a failed or timed out one.
	streamLabelNamesRetry = 30 * time.Second
	// streamLabelNamesRefreshTimeout bounds one refresh, which runs detached
	// from the request that triggered it.
	streamLabelNamesRefreshTimeout = time.Second
	// streamLabelNamesMaxTenants bounds the per-tenant (and auth scope) entries.
	streamLabelNamesMaxTenants = 1024
)

// streamLabelNamesState holds the last known stream label names per tenant and
// auth scope. A query never waits for a listing: it reads the last known names
// (none before the first refresh finished, which is the plain translation)
// and, when they are older than the TTL, starts one single-flight background
// refresh.
type streamLabelNamesState struct {
	mu        sync.Mutex
	byKey     map[string]*streamLabelNamesEntry
	refreshes int // started, for tests
	base      context.Context
	stop      context.CancelFunc
	wg        sync.WaitGroup // running refreshes
}

// stopStreamLabelNames cancels the running refreshes, starts no more and waits
// for them (Proxy.Shutdown).
func (p *Proxy) stopStreamLabelNames() {
	st := &p.streamLabelNames
	st.mu.Lock()
	if st.base == nil {
		st.base, st.stop = context.WithCancel(context.Background())
	}
	st.stop()
	st.mu.Unlock()
	st.wg.Wait()
}

type streamLabelNamesEntry struct {
	names      []string
	fresh      time.Time // last successful refresh
	tried      time.Time // last refresh start
	refreshing bool
}

var (
	parserKeywordRE  = regexp.MustCompile(`\|\s*(json|logfmt|unpack|regexp|pattern)\b`)
	queryStringRE    = regexp.MustCompile("\"(?:[^\"\\\\]|\\\\.)*\"|`[^`]*`")
	queryTemplateRE  = regexp.MustCompile(`\{\{[^}]*\}\}`)
	queryStreamSelRE = regexp.MustCompile(`\{[^{}]*\}`)
	queryIdentRE     = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_.-]*`)
	plainLabelNameRE = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)
)

// plainStreamLabelNames returns the stream label names of the tenant that the
// query names outside its stream selectors and string literals, when the query
// has a parser stage (Loki-compatible profile, and only for a query a client
// request runs: the tenant is read from it).
func (p *Proxy) plainStreamLabelNames(ctx context.Context, query string) []string {
	if ctx.Value(origRequestKey) == nil || !parserKeywordRE.MatchString(query) || !p.lokiCompatibleProfile() {
		return nil
	}
	mentioned := map[string]bool{}
	for _, id := range queryIdentRE.FindAllString(queryStreamSelRE.ReplaceAllString(queryStringRE.ReplaceAllString(query, `""`), ""), -1) {
		mentioned[id] = true
	}
	// A label_format or line_format template reads a label as {{.name}}.
	for _, tmpl := range queryTemplateRE.FindAllString(query, -1) {
		for _, id := range queryIdentRE.FindAllString(tmpl, -1) {
			mentioned[id] = true
		}
	}
	known := p.tenantStreamLabelNames(ctx)
	var out []string
	for _, name := range known {
		if mentioned[name] {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// tenantStreamLabelNames returns the tenant's recent stream label names whose
// translation is the identity (a name VictoriaLogs stores under another name is
// not read as a stream value) and that are no derived label, as last known.
// It never blocks on VictoriaLogs.
func (p *Proxy) tenantStreamLabelNames(ctx context.Context) []string {
	// The names only change the query sent to VictoriaLogs, never what is
	// returned, so they are keyed by tenant and shared across auth scopes.
	key := "slnames:" + getOrgID(ctx)
	st := &p.streamLabelNames
	now := time.Now()
	st.mu.Lock()
	if st.byKey == nil {
		st.byKey = map[string]*streamLabelNamesEntry{}
	}
	if st.base == nil {
		st.base, st.stop = context.WithCancel(context.Background())
	}
	e := st.byKey[key]
	if e == nil {
		if len(st.byKey) >= streamLabelNamesMaxTenants {
			for k := range st.byKey {
				delete(st.byKey, k)
				break
			}
		}
		e = &streamLabelNamesEntry{}
		st.byKey[key] = e
	}
	names := e.names
	due := !e.refreshing && now.Sub(e.fresh) >= streamLabelNamesTTL && now.Sub(e.tried) >= streamLabelNamesRetry && st.base.Err() == nil
	if due {
		e.refreshing, e.tried = true, now
		st.refreshes++
		st.wg.Add(1)
	}
	cold := e.fresh.IsZero()
	st.mu.Unlock()
	if due {
		go p.refreshStreamLabelNames(context.WithoutCancel(ctx), key)
	}
	if cold {
		p.observeInternalOperation(ctx, "stream_label_names", "cold_fallback", 0)
	} else if due {
		p.observeInternalOperation(ctx, "stream_label_names", "stale", 0)
	}
	return names
}

// refreshStreamLabelNames lists the stream label names over the recent window
// within its own deadline and stores them for key.
func (p *Proxy) refreshStreamLabelNames(ctx context.Context, key string) {
	started := time.Now()
	defer p.streamLabelNames.wg.Done()
	ctx, cancel := context.WithTimeout(context.WithValue(ctx, quietUpstreamFailuresKey{}, true), streamLabelNamesRefreshTimeout)
	defer cancel()
	defer context.AfterFunc(p.streamLabelNames.base, cancel)()
	end := started.Truncate(time.Minute)
	params := url.Values{}
	params.Set("query", "*")
	params.Set("start", strconv.FormatInt(end.Add(-streamLabelNamesWindow).UnixNano(), 10))
	params.Set("end", strconv.FormatInt(end.UnixNano(), 10))
	names, err := p.fetchStreamFieldNamesCached(ctx, params)
	var usable []string
	for _, n := range names {
		if plainLabelNameRE.MatchString(n) && n != "service_name" && n != detectedLevelLabel && p.labelTranslator.ToVL(n) == n {
			usable = append(usable, n)
		}
	}
	st := &p.streamLabelNames
	st.mu.Lock()
	if e := st.byKey[key]; e != nil {
		e.refreshing = false
		if err == nil {
			e.names, e.fresh = usable, time.Now()
		}
	}
	st.mu.Unlock()
	outcome := "refresh"
	if err != nil {
		outcome = "refresh_error"
		p.log.Warn("stream label names refresh failed; queries keep the last known names", "error", err, "duration", time.Since(started))
	}
	p.observeInternalOperation(ctx, "stream_label_names", outcome, time.Since(started))
}

// quietUpstreamFailuresKey marks a context whose upstream failures are logged
// below warning level (the refresh logs one line for the whole listing).
type quietUpstreamFailuresKey struct{}

// responseNamesCacheKey is the part of a response cache key that stands for the
// stream label names a query was translated with: an answer made with the plain
// translation (names not known yet) must not be served once the names are known.
func (p *Proxy) responseNamesCacheKey(r *http.Request, query string) string {
	if names := p.plainStreamLabelNames(r.Context(), query); len(names) > 0 {
		return streamLabelsCacheKey(names)
	}
	return ""
}

// streamLabelsCacheKey keys a translation by the names it was made for.
func streamLabelsCacheKey(names []string) string {
	return "\x00sl:" + strings.Join(names, ",")
}
