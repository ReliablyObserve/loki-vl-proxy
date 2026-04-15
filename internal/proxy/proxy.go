package proxy

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	_ "net/http/pprof"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
	mw "github.com/ReliablyObserve/Loki-VL-proxy/internal/middleware"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
	"github.com/gorilla/websocket"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/sync/singleflight"
	"gopkg.in/yaml.v3"
)

// jsonBufPool pools bytes.Buffer for JSON encoding to reduce allocations.
// Capped at 64KB before returning to prevent pool bloat from large responses.
const maxPooledBufSize = 64 * 1024

const (
	patternDedupSourceMemory = "mem"
	patternDedupSourceDisk   = "disk"
	patternDedupSourcePeer   = "peer"
)

var jsonBufPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 4096))
	},
}

var backendSemverPattern = regexp.MustCompile(`v[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z\.-]+)?`)
var backendMetricsQuotedVersionPattern = regexp.MustCompile(`(?:^|[,{])(?:short_version|version)="(v[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z\.-]+)?)"`)

// marshalJSON encodes v to JSON using a pooled buffer, then writes to w.
// Reduces allocations vs json.NewEncoder(w).Encode() by reusing buffers.
func marshalJSON(w http.ResponseWriter, v interface{}) {
	buf := jsonBufPool.Get().(*bytes.Buffer)
	buf.Reset()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(buf).Encode(v); err != nil {
		// Fallback: encode directly (shouldn't happen for our types)
		json.NewEncoder(w).Encode(v)
	} else {
		w.Write(buf.Bytes())
	}

	if buf.Cap() <= maxPooledBufSize {
		jsonBufPool.Put(buf)
	}
	// Oversized buffers are left for GC (prevents pool bloat)
}

type ctxKey int

const (
	orgIDKey ctxKey = iota
	origRequestKey
	requestTelemetryKey
	requestRouteMetaKey
	requestGrafanaClientKey
)

type requestTelemetry struct {
	mu sync.Mutex

	cacheResult       string
	upstreamCalls     int
	upstreamDuration  time.Duration
	upstreamLastCode  int
	upstreamErrorSeen bool
}

type requestTelemetrySnapshot struct {
	cacheResult       string
	upstreamCalls     int
	upstreamDuration  time.Duration
	upstreamLastCode  int
	upstreamErrorSeen bool
}

type requestRouteMeta struct {
	endpoint string
	route    string
}

type grafanaClientProfile struct {
	surface           string
	sourceTag         string
	version           string
	runtimeMajor      int
	runtimeFamily     string
	drilldownProfile  string
	datasourceProfile string
}

var upstreamRequestTypeByRoute = map[string]string{
	"/select/logsql/query":               "select_logsql_query",
	"/select/logsql/stats_query":         "select_logsql_stats_query",
	"/select/logsql/stats_query_range":   "select_logsql_stats_query_range",
	"/select/logsql/streams":             "select_logsql_streams",
	"/select/logsql/hits":                "select_logsql_hits",
	"/select/logsql/field_names":         "select_logsql_field_names",
	"/select/logsql/stream_field_names":  "select_logsql_stream_field_names",
	"/select/logsql/field_values":        "select_logsql_field_values",
	"/select/logsql/stream_field_values": "select_logsql_stream_field_values",
	"/select/logsql/delete":              "select_logsql_delete",
	"/select/logsql/tail":                "select_logsql_tail",
}

func newRequestTelemetry() *requestTelemetry {
	return &requestTelemetry{cacheResult: "bypass"}
}

func getRequestTelemetry(ctx context.Context) *requestTelemetry {
	rt, _ := ctx.Value(requestTelemetryKey).(*requestTelemetry)
	return rt
}

func setCacheResult(ctx context.Context, result string) {
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return
	}
	rt.mu.Lock()
	rt.cacheResult = result
	rt.mu.Unlock()
}

func splitHostPortValue(addr string) (string, int) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", 0
	}
	host, portStr, err := net.SplitHostPort(addr)
	if err == nil {
		port, _ := strconv.Atoi(portStr)
		return strings.TrimSpace(host), port
	}
	return addr, 0
}

func forwardedClientAddress(r *http.Request, trustProxyHeaders bool) string {
	if trustProxyHeaders {
		if fwd := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); fwd != "" {
			if idx := strings.IndexByte(fwd, ','); idx > 0 {
				return strings.TrimSpace(fwd[:idx])
			}
			return fwd
		}
	}
	host, _ := splitHostPortValue(r.RemoteAddr)
	return host
}

func recordUpstreamCall(ctx context.Context, statusCode int, duration time.Duration, hadError bool) {
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return
	}
	rt.mu.Lock()
	rt.upstreamCalls++
	rt.upstreamDuration += duration
	rt.upstreamLastCode = statusCode
	rt.upstreamErrorSeen = rt.upstreamErrorSeen || hadError
	rt.mu.Unlock()
}

func snapshotTelemetry(ctx context.Context) requestTelemetrySnapshot {
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return requestTelemetrySnapshot{cacheResult: "bypass"}
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return requestTelemetrySnapshot{
		cacheResult:       rt.cacheResult,
		upstreamCalls:     rt.upstreamCalls,
		upstreamDuration:  rt.upstreamDuration,
		upstreamLastCode:  rt.upstreamLastCode,
		upstreamErrorSeen: rt.upstreamErrorSeen,
	}
}

func requestRouteMetaFromContext(ctx context.Context) requestRouteMeta {
	meta, _ := ctx.Value(requestRouteMetaKey).(requestRouteMeta)
	return meta
}

func grafanaClientProfileFromContext(ctx context.Context) grafanaClientProfile {
	profile, _ := ctx.Value(requestGrafanaClientKey).(grafanaClientProfile)
	return profile
}

// withOrgID stores the X-Scope-OrgID and original request in the request context (request-scoped, no shared state).
func withOrgID(r *http.Request) *http.Request {
	ctx := r.Context()
	// Store original request for header forwarding in vlGet/vlPost
	ctx = context.WithValue(ctx, origRequestKey, r)
	orgID := r.Header.Get("X-Scope-OrgID")
	if orgID != "" {
		ctx = context.WithValue(ctx, orgIDKey, orgID)
	}
	return r.WithContext(ctx)
}

// getOrgID retrieves the org ID from context.
func getOrgID(ctx context.Context) string {
	if v, ok := ctx.Value(orgIDKey).(string); ok {
		return v
	}
	return ""
}

// TenantMapping maps a string org ID to VL's numeric AccountID and ProjectID.
type TenantMapping struct {
	AccountID string `json:"account_id" yaml:"account_id"`
	ProjectID string `json:"project_id" yaml:"project_id"`
}

type TailMode string

const (
	TailModeAuto      TailMode = "auto"
	TailModeNative    TailMode = "native"
	TailModeSynthetic TailMode = "synthetic"
)

type Config struct {
	BackendURL        string
	RulerBackendURL   string
	AlertsBackendURL  string
	Cache             *cache.Cache
	CompatCache       *cache.Cache
	LogLevel          string
	MaxConcurrent     int                      // max concurrent backend queries (0=unlimited)
	RatePerSecond     float64                  // per-client rate limit (0=unlimited)
	RateBurst         int                      // per-client burst size
	CBFailThreshold   int                      // circuit breaker failure threshold
	CBOpenDuration    time.Duration            // circuit breaker open duration
	TenantMap         map[string]TenantMapping // string org ID → VL account/project
	AuthEnabled       bool
	AllowGlobalTenant bool

	// Grafana datasource compatibility
	MaxLines           int               // default max lines per query (0=1000)
	ForwardHeaders     []string          // HTTP headers to forward from client to VL backend
	ForwardCookies     []string          // Cookie names to forward from client to VL backend
	BackendHeaders     map[string]string // static headers to add to all VL requests
	BackendBasicAuth   string            // "user:password" for VL backend basic auth
	BackendCompression string            // upstream HTTP compression preference: auto, gzip, zstd, none
	// ClientResponseCompression controls downstream client-facing response
	// compression policy used by the compatibility cache hit path.
	ClientResponseCompression string
	// ClientResponseCompressionMinBytes is the downstream compression threshold
	// before the proxy spends CPU compressing a response.
	ClientResponseCompressionMinBytes int
	BackendTimeout                    time.Duration // bounded timeout for non-streaming backend requests
	BackendTLSSkip                    bool          // skip TLS verification for VL backend
	// BackendMinVersion defines the minimum VictoriaLogs version considered
	// fully supported at startup compatibility check time.
	BackendMinVersion string
	// BackendAllowUnsupportedVersion allows startup to continue when detected
	// backend version is lower than BackendMinVersion. Use at your own risk.
	BackendAllowUnsupportedVersion bool
	// BackendVersionCheckTimeout bounds startup backend version checks.
	BackendVersionCheckTimeout time.Duration
	DerivedFields              []DerivedField // derived fields for trace/link extraction
	StreamResponse             bool           // stream responses via chunked transfer (default: false)
	// EmitStructuredMetadata enables Loki 3-tuple stream values [ts, line, metadata].
	// Disabled by default for conservative datasource compatibility.
	EmitStructuredMetadata bool
	// PatternsEnabled controls /loki/api/v1/patterns availability.
	// Nil defaults to true for backward compatibility.
	PatternsEnabled *bool
	// PatternsAutodetectFromQueries passively extracts patterns from successful
	// log query/query_range responses and warms /patterns cache entries.
	PatternsAutodetectFromQueries bool
	// PatternsCustom is a static list of patterns always prepended to
	// /loki/api/v1/patterns responses.
	PatternsCustom []string
	// Query range windowing/cache options.
	// When enabled, eligible log query_range requests are split into time windows,
	// fetched with bounded parallelism, and merged in Loki-compatible direction order.
	QueryRangeWindowingEnabled         bool
	QueryRangeSplitInterval            time.Duration
	QueryRangeMaxParallel              int
	QueryRangeAdaptiveParallel         bool
	QueryRangeParallelMin              int
	QueryRangeParallelMax              int
	QueryRangeLatencyTarget            time.Duration
	QueryRangeLatencyBackoff           time.Duration
	QueryRangeAdaptiveCooldown         time.Duration
	QueryRangeErrorBackoffThreshold    float64
	QueryRangeFreshness                time.Duration
	QueryRangeRecentCacheTTL           time.Duration
	QueryRangeHistoryCacheTTL          time.Duration
	QueryRangePrefilterIndexStats      bool
	QueryRangePrefilterMinWindows      int
	QueryRangeStreamAwareBatching      bool
	QueryRangeExpensiveHitThreshold    int64
	QueryRangeExpensiveMaxParallel     int
	QueryRangeAlignWindows             bool
	QueryRangeWindowTimeout            time.Duration
	QueryRangePartialResponses         bool
	QueryRangeBackgroundWarm           bool
	QueryRangeBackgroundWarmMaxWindows int
	// RecentTailRefreshEnabled enables near-now cache freshness bypass for selected endpoints.
	// When enabled, stale cache hits near current time are bypassed so the proxy refetches
	// latest data while still caching historical ranges.
	RecentTailRefreshEnabled bool
	// RecentTailRefreshWindow defines how close query end time must be to "now" to be
	// considered near-now for freshness bypass.
	RecentTailRefreshWindow time.Duration
	// RecentTailRefreshMaxStaleness defines max acceptable cache age for near-now requests.
	// Older cache hits are bypassed and recomputed.
	RecentTailRefreshMaxStaleness time.Duration

	// Label translation
	LabelStyle        LabelStyle        // how to translate VL field names to Loki labels
	MetadataFieldMode MetadataFieldMode // how to expose non-label VL fields through field-oriented APIs
	FieldMappings     []FieldMapping    // custom VL↔Loki field name mappings

	// Stream optimization
	StreamFields []string // VL _stream_fields labels — use native stream selectors for these (faster)
	// ExtraLabelFields extends the auto-discovered label surface with explicit VL field names.
	// Values may be dotted or underscore aliases; they are normalized to VL-native names.
	ExtraLabelFields []string
	// LabelValuesIndexedCache enables indexed browsing for /label/{name}/values.
	// When enabled, empty-query browsing can return a hot subset first, with optional offset/limit/search.
	LabelValuesIndexedCache bool
	// LabelValuesHotLimit is the default number of values returned for empty-query browsing
	// when LabelValuesIndexedCache is enabled and request limit is not provided.
	LabelValuesHotLimit int
	// LabelValuesIndexMaxEntries caps in-memory indexed values per tenant+label.
	LabelValuesIndexMaxEntries int
	// LabelValuesIndexPersistPath enables periodic/final persistence of label-values index snapshots.
	// Empty disables disk persistence.
	LabelValuesIndexPersistPath string
	// LabelValuesIndexPersistInterval controls periodic snapshot persistence interval.
	LabelValuesIndexPersistInterval time.Duration
	// LabelValuesIndexStartupStale marks on-disk snapshots older than this threshold as stale.
	// Stale snapshots trigger peer warm fallback before serving.
	LabelValuesIndexStartupStale time.Duration
	// LabelValuesIndexPeerWarmTimeout bounds startup peer warm attempts when disk is stale/missing.
	LabelValuesIndexPeerWarmTimeout time.Duration
	// PatternsPersistPath enables periodic/final persistence of generated /patterns cache snapshots.
	// Empty disables disk persistence.
	PatternsPersistPath string
	// PatternsPersistInterval controls periodic snapshot persistence interval.
	PatternsPersistInterval time.Duration
	// PatternsStartupStale marks on-disk pattern snapshots older than this threshold as stale.
	// Stale snapshots trigger peer warm fallback before serving.
	PatternsStartupStale time.Duration
	// PatternsPeerWarmTimeout bounds startup peer warm attempts when disk is stale/missing.
	PatternsPeerWarmTimeout time.Duration

	// Peer cache (fleet distribution)
	PeerCache     *cache.PeerCache // optional peer cache for distributed fleet
	PeerAuthToken string

	// Admin/debug endpoints
	RegisterInstrumentation *bool
	EnablePprof             bool
	EnableQueryAnalytics    bool
	AdminAuthToken          string

	// Tail/WebSocket hardening
	TailAllowedOrigins []string
	TailMode           TailMode

	// Metrics/export hardening
	MetricsMaxTenants        int
	MetricsMaxClients        int
	MetricsTrustProxyHeaders bool

	// Tenant limits runtime exposure.
	// TenantLimitsAllowPublish controls which fields are exposed by
	// /config/tenant/v1/limits and /loki/api/v1/drilldown-limits.
	// If empty, default Loki-compatible allowlist is used.
	TenantLimitsAllowPublish []string
	// TenantDefaultLimits applies global published limits overrides.
	TenantDefaultLimits map[string]any
	// TenantLimits applies per-tenant published limits overrides keyed by X-Scope-OrgID.
	TenantLimits map[string]map[string]any
}

// DerivedField extracts a value from log lines and creates a link (e.g., to a trace backend).
// Matches Grafana Loki datasource "Derived fields" config.
type DerivedField struct {
	Name            string `json:"name" yaml:"name"`                       // field name (e.g., "traceID")
	MatcherRegex    string `json:"matcherRegex" yaml:"matcherRegex"`       // regex to extract value from log line
	URL             string `json:"url" yaml:"url"`                         // link template (e.g., "http://tempo:3200/trace/${__value.raw}")
	URLDisplayLabel string `json:"urlDisplayLabel" yaml:"urlDisplayLabel"` // display text for the link
	DatasourceUID   string `json:"datasourceUid" yaml:"datasourceUid"`     // Grafana datasource UID for internal link
}

const (
	// maxQueryLength limits the LogQL query string length to prevent abuse.
	maxQueryLength = 65536 // 64KB
	// maxLimitValue caps the number of results per query.
	maxLimitValue                     = 10000
	maxZeroFillBuckets                = 10000
	maxPatternBackendQueryLimit       = 20000
	maxUserDrivenSlicePrealloc        = 512
	tailWriteTimeout                  = 2 * time.Second
	maxMultiTenantFanout              = 64
	maxMultiTenantMergedResponseBytes = 32 << 20
	maxDetectedScanLines              = 2000
	maxSyntheticTailSeenEntries       = 4096
	labelValuesIndexSnapshotCacheKey  = "__label_values_index_snapshot:v1"
	patternsSnapshotCacheKey          = "__patterns_snapshot:v1"
	// Keep pattern cache entries effectively permanent; updates replace by cache key.
	patternsCacheRetention = 100 * 365 * 24 * time.Hour
)

// CacheTTLs defines per-endpoint cache TTLs.
var CacheTTLs = map[string]time.Duration{
	"labels":                2 * time.Minute,
	"label_values":          2 * time.Minute,
	"label_inventory":       5 * time.Minute,
	"series":                30 * time.Second,
	"detected_fields":       90 * time.Second,
	"detected_field_values": 90 * time.Second,
	"detected_labels":       90 * time.Second,
	"patterns":              patternsCacheRetention,
	"query_range":           10 * time.Second,
	"query":                 10 * time.Second,
	"index_stats":           10 * time.Second,
	"volume":                10 * time.Second,
	"volume_range":          10 * time.Second,
}

type Proxy struct {
	backend                               *url.URL
	rulerBackend                          *url.URL
	alertsBackend                         *url.URL
	client                                *http.Client
	tailClient                            *http.Client
	cache                                 *cache.Cache
	compatCache                           *cache.Cache
	log                                   *slog.Logger
	metrics                               *metrics.Metrics
	queryTracker                          *metrics.QueryTracker
	coalescer                             *mw.Coalescer
	limiter                               *mw.RateLimiter
	breaker                               *mw.CircuitBreaker
	configMu                              sync.RWMutex // protects tenantMap and labelTranslator
	tenantMap                             map[string]TenantMapping
	authEnabled                           bool
	allowGlobalTenant                     bool
	maxLines                              int
	forwardHeaders                        []string          // headers to copy from client request to VL
	forwardCookies                        map[string]bool   // cookie names to copy from client request to VL
	backendHeaders                        map[string]string // static headers on all VL requests
	backendCompression                    string
	clientResponseCompression             string
	clientResponseCompressionMinBytes     int
	backendMinVersion                     string
	backendAllowUnsupportedVersion        bool
	backendVersionCheckTimeout            time.Duration
	derivedFields                         []DerivedField
	streamResponse                        bool
	emitStructuredMetadata                bool
	patternsEnabled                       bool
	patternsAutodetectFromQueries         bool
	patternsCustom                        []string
	labelTranslator                       *LabelTranslator
	metadataFieldMode                     MetadataFieldMode
	streamFieldsMap                       map[string]bool  // known _stream_fields for VL stream selector optimization
	declaredLabelFields                   []string         // configured VL-native label fields (stream_fields + extras)
	peerCache                             *cache.PeerCache // L3 fleet peer cache
	peerAuthToken                         string
	registerInstrumentation               bool
	enablePprof                           bool
	enableQueryAnalytics                  bool
	adminAuthToken                        string
	tailAllowedOrigins                    map[string]struct{}
	tailMode                              TailMode
	metricsTrustProxyHeaders              bool
	tenantLimitsAllowPublish              []string
	tenantDefaultLimits                   map[string]any
	tenantLimits                          map[string]map[string]any
	translationCache                      *cache.Cache
	queryRangeWindowing                   bool
	queryRangeSplitInterval               time.Duration
	queryRangeMaxParallel                 int
	queryRangeAdaptiveParallel            bool
	queryRangeParallelMin                 int
	queryRangeParallelMax                 int
	queryRangeLatencyTarget               time.Duration
	queryRangeLatencyBackoff              time.Duration
	queryRangeAdaptiveCooldown            time.Duration
	queryRangeErrorBackoffThreshold       float64
	queryRangeAdaptiveMu                  sync.Mutex
	queryRangeParallelCurrent             int
	queryRangeLatencyEWMA                 time.Duration
	queryRangeErrorEWMA                   float64
	queryRangeAdaptiveLastAdjust          time.Time
	queryRangeFreshness                   time.Duration
	queryRangeRecentCacheTTL              time.Duration
	queryRangeHistoryCacheTTL             time.Duration
	queryRangePrefilterIndexStats         bool
	queryRangePrefilterMinWindows         int
	queryRangeStreamAwareBatching         bool
	queryRangeExpensiveWindowHitThreshold int64
	queryRangeExpensiveWindowMaxParallel  int
	queryRangeAlignWindows                bool
	queryRangeWindowTimeout               time.Duration
	queryRangePartialResponses            bool
	queryRangeBackgroundWarm              bool
	queryRangeBackgroundWarmMaxWindows    int
	recentTailRefreshEnabled              bool
	recentTailRefreshWindow               time.Duration
	recentTailRefreshMaxStaleness         time.Duration
	labelRefreshGroup                     singleflight.Group
	labelValuesIndexedCache               bool
	labelValuesHotLimit                   int
	labelValuesIndexMaxEntries            int
	labelValuesIndexPersistPath           string
	labelValuesIndexPersistInterval       time.Duration
	labelValuesIndexStartupStale          time.Duration
	labelValuesIndexPeerWarmTimeout       time.Duration
	patternsPersistPath                   string
	patternsPersistInterval               time.Duration
	patternsStartupStale                  time.Duration
	patternsPeerWarmTimeout               time.Duration
	patternsWarmReady                     atomic.Bool
	patternsPersistStarted                atomic.Bool
	patternsPersistDirty                  atomic.Bool
	patternsPersistStop                   chan struct{}
	patternsPersistDone                   chan struct{}
	patternsSnapshotMu                    sync.RWMutex
	patternsSnapshotEntries               map[string]patternSnapshotEntry
	patternsSnapshotPatternCount          int64
	patternsSnapshotPayloadBytes          int64
	backendVersionMu                      sync.RWMutex
	backendVersionRaw                     string
	backendVersionSemver                  string
	backendCapabilityProfile              string
	backendSupportsStreamMetadata         bool
	backendSupportsDensePatternWindowing  bool
	backendSupportsMetadataSubstring      bool
	backendVersionLogged                  bool
	labelValuesIndexWarmReady             atomic.Bool
	labelValuesIndexPersistStarted        atomic.Bool
	labelValuesIndexPersistDirty          atomic.Bool
	labelValuesIndexPersistStop           chan struct{}
	labelValuesIndexPersistDone           chan struct{}
	labelValuesIndexMu                    sync.RWMutex
	labelValuesIndex                      map[string]*labelValuesIndexState
}

var defaultTenantLimitsAllowPublish = []string{
	"discover_log_levels",
	"discover_service_name",
	"log_level_fields",
	"max_entries_limit_per_query",
	"max_line_size_truncate",
	"max_query_bytes_read",
	"max_query_length",
	"max_query_lookback",
	"max_query_range",
	"max_query_series",
	"metric_aggregation_enabled",
	"otlp_config",
	"pattern_persistence_enabled",
	"query_timeout",
	"retention_period",
	"retention_stream",
	"volume_enabled",
	"volume_max_series",
}

type labelValueIndexEntry struct {
	SeenCount uint32 `json:"seen_count"`
	LastSeen  int64  `json:"last_seen_unix_nano"`
}

type labelValuesIndexState struct {
	entries map[string]labelValueIndexEntry
	dirty   bool
	ordered []string
}

type labelValuesIndexSnapshot struct {
	Version         int                                        `json:"version"`
	SavedAtUnixNano int64                                      `json:"saved_at_unix_nano"`
	StatesByKey     map[string]map[string]labelValueIndexEntry `json:"states_by_key"`
}

type patternSnapshotEntry struct {
	Value             []byte `json:"value"`
	UpdatedAtUnixNano int64  `json:"updated_at_unix_nano"`
	PatternCount      int    `json:"pattern_count,omitempty"`
}

type patternsSnapshot struct {
	Version         int                             `json:"version"`
	SavedAtUnixNano int64                           `json:"saved_at_unix_nano"`
	EntriesByKey    map[string]patternSnapshotEntry `json:"entries_by_key"`
}

type tailConn interface {
	SetWriteDeadline(time.Time) error
	WriteMessage(int, []byte) error
	WriteControl(int, []byte, time.Time) error
}

type syntheticTailSeen struct {
	seen  map[string]struct{}
	order []string
	limit int
}

func cloneStringAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneTenantLimitsMap(in map[string]map[string]any) map[string]map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]map[string]any, len(in))
	for tenant, limits := range in {
		out[tenant] = cloneStringAnyMap(limits)
	}
	return out
}

func mergeStringAnyMap(dst, src map[string]any) {
	for k, v := range src {
		dst[k] = v
	}
}

func filterPublishedLimits(limits map[string]any, allowlist []string) map[string]any {
	if len(allowlist) == 0 {
		return limits
	}
	allow := make(map[string]struct{}, len(allowlist))
	for _, key := range allowlist {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		allow[key] = struct{}{}
	}
	filtered := make(map[string]any, len(allow))
	for key, value := range limits {
		if _, ok := allow[key]; ok {
			filtered[key] = value
		}
	}
	return filtered
}

func New(cfg Config) (*Proxy, error) {
	u, err := url.Parse(cfg.BackendURL)
	if err != nil {
		return nil, fmt.Errorf("invalid backend URL: %w", err)
	}
	var rulerURL *url.URL
	if strings.TrimSpace(cfg.RulerBackendURL) != "" {
		rulerURL, err = url.Parse(cfg.RulerBackendURL)
		if err != nil {
			return nil, fmt.Errorf("invalid ruler backend URL: %w", err)
		}
	}
	var alertsURL *url.URL
	if strings.TrimSpace(cfg.AlertsBackendURL) != "" {
		alertsURL, err = url.Parse(cfg.AlertsBackendURL)
		if err != nil {
			return nil, fmt.Errorf("invalid alerts backend URL: %w", err)
		}
	}

	level := slog.LevelInfo
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	baseLogger := slog.Default()
	if baseLogger == nil {
		baseLogger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	}
	logger := slog.New(NewRedactingHandler(&levelFilterHandler{
		inner: baseLogger.Handler(),
		min:   level,
	})).With("component", "proxy")

	maxConcurrent := cfg.MaxConcurrent
	if maxConcurrent < 0 {
		maxConcurrent = 0
	}
	ratePerSec := cfg.RatePerSecond
	if ratePerSec < 0 {
		ratePerSec = 0
	}
	rateBurst := cfg.RateBurst
	if rateBurst < 0 {
		rateBurst = 0
	}
	cbFail := cfg.CBFailThreshold
	if cbFail == 0 {
		cbFail = 5
	}
	cbOpen := cfg.CBOpenDuration
	if cbOpen == 0 {
		cbOpen = 10 * time.Second
	}

	// Build HTTP client optimized for high-concurrency single-backend proxying.
	// Go defaults (MaxIdleConnsPerHost=2) cause ephemeral port exhaustion under load.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 256                 // total idle connections across all hosts
	transport.MaxIdleConnsPerHost = 256          // VL is the only backend — all slots for it
	transport.MaxConnsPerHost = 0                // unlimited concurrent connections to VL
	transport.IdleConnTimeout = 90 * time.Second // reuse connections for 90s
	backendTimeout := cfg.BackendTimeout
	if backendTimeout <= 0 {
		backendTimeout = 120 * time.Second
	}
	backendMinVersion := normalizeSemverString(cfg.BackendMinVersion)
	if backendMinVersion == "" {
		backendMinVersion = "v1.30.0"
	}
	if _, _, _, ok := parseSemverTriplet(backendMinVersion); !ok {
		return nil, fmt.Errorf("invalid backend minimum version %q", backendMinVersion)
	}
	backendVersionCheckTimeout := cfg.BackendVersionCheckTimeout
	if backendVersionCheckTimeout <= 0 {
		backendVersionCheckTimeout = 5 * time.Second
	}
	transport.ResponseHeaderTimeout = backendTimeout
	transport.DisableCompression = false // accept gzip from VL if available
	if cfg.BackendTLSSkip {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	tailTransport := transport.Clone()
	tailTransport.ResponseHeaderTimeout = 30 * time.Second

	maxLines := cfg.MaxLines
	if maxLines <= 0 {
		maxLines = 1000
	}

	backendHeaders := cfg.BackendHeaders
	if backendHeaders == nil {
		backendHeaders = make(map[string]string)
	}
	if cfg.BackendBasicAuth != "" {
		encoded := base64Encode(cfg.BackendBasicAuth)
		backendHeaders["Authorization"] = "Basic " + encoded
	}
	registerInstrumentation := true
	if cfg.RegisterInstrumentation != nil {
		registerInstrumentation = *cfg.RegisterInstrumentation
	}
	forwardCookies := make(map[string]bool, len(cfg.ForwardCookies))
	for _, cookieName := range cfg.ForwardCookies {
		cookieName = strings.TrimSpace(cookieName)
		if cookieName == "" {
			continue
		}
		forwardCookies[cookieName] = true
	}
	tailAllowedOrigins := make(map[string]struct{}, len(cfg.TailAllowedOrigins))
	for _, origin := range cfg.TailAllowedOrigins {
		origin = strings.TrimSpace(origin)
		if origin == "" {
			continue
		}
		tailAllowedOrigins[origin] = struct{}{}
	}
	metadataFieldMode := normalizeMetadataFieldMode(cfg.MetadataFieldMode)
	queryRangeMaxParallel := cfg.QueryRangeMaxParallel
	if queryRangeMaxParallel <= 0 {
		queryRangeMaxParallel = 2
	}
	queryRangeParallelMax := cfg.QueryRangeParallelMax
	if queryRangeParallelMax <= 0 {
		queryRangeParallelMax = queryRangeMaxParallel
	}
	queryRangeParallelMin := cfg.QueryRangeParallelMin
	if queryRangeParallelMin <= 0 {
		queryRangeParallelMin = 2
	}
	if queryRangeParallelMax < 1 {
		queryRangeParallelMax = 1
	}
	if queryRangeParallelMin > queryRangeParallelMax {
		queryRangeParallelMin = queryRangeParallelMax
	}
	if queryRangeParallelMin < 1 {
		queryRangeParallelMin = 1
	}
	queryRangeLatencyTarget := cfg.QueryRangeLatencyTarget
	if queryRangeLatencyTarget <= 0 {
		queryRangeLatencyTarget = 1500 * time.Millisecond
	}
	queryRangeLatencyBackoff := cfg.QueryRangeLatencyBackoff
	if queryRangeLatencyBackoff <= 0 {
		queryRangeLatencyBackoff = 3 * time.Second
	}
	if queryRangeLatencyBackoff < queryRangeLatencyTarget {
		queryRangeLatencyBackoff = queryRangeLatencyTarget * 2
	}
	queryRangeAdaptiveCooldown := cfg.QueryRangeAdaptiveCooldown
	if queryRangeAdaptiveCooldown <= 0 {
		queryRangeAdaptiveCooldown = 30 * time.Second
	}
	queryRangeErrorBackoffThreshold := cfg.QueryRangeErrorBackoffThreshold
	if queryRangeErrorBackoffThreshold <= 0 || queryRangeErrorBackoffThreshold > 1 {
		queryRangeErrorBackoffThreshold = 0.02
	}
	queryRangeFreshness := cfg.QueryRangeFreshness
	if queryRangeFreshness <= 0 {
		queryRangeFreshness = 10 * time.Minute
	}
	queryRangeRecentCacheTTL := cfg.QueryRangeRecentCacheTTL
	if queryRangeRecentCacheTTL < 0 {
		queryRangeRecentCacheTTL = 0
	}
	queryRangeHistoryCacheTTL := cfg.QueryRangeHistoryCacheTTL
	if queryRangeHistoryCacheTTL < 0 {
		queryRangeHistoryCacheTTL = 0
	}
	queryRangePrefilterMinWindows := cfg.QueryRangePrefilterMinWindows
	if queryRangePrefilterMinWindows <= 0 {
		queryRangePrefilterMinWindows = 8
	}
	queryRangeExpensiveHitThreshold := cfg.QueryRangeExpensiveHitThreshold
	if queryRangeExpensiveHitThreshold <= 0 {
		queryRangeExpensiveHitThreshold = 2000
	}
	queryRangeExpensiveMaxParallel := cfg.QueryRangeExpensiveMaxParallel
	if queryRangeExpensiveMaxParallel <= 0 {
		queryRangeExpensiveMaxParallel = 1
	}
	if queryRangeExpensiveMaxParallel > queryRangeParallelMax {
		queryRangeExpensiveMaxParallel = queryRangeParallelMax
	}
	queryRangeWindowTimeout := cfg.QueryRangeWindowTimeout
	if queryRangeWindowTimeout < 0 {
		queryRangeWindowTimeout = 0
	}
	queryRangeBackgroundWarmMaxWindows := cfg.QueryRangeBackgroundWarmMaxWindows
	if queryRangeBackgroundWarmMaxWindows <= 0 {
		queryRangeBackgroundWarmMaxWindows = 24
	}
	recentTailRefreshWindow := cfg.RecentTailRefreshWindow
	if recentTailRefreshWindow <= 0 {
		recentTailRefreshWindow = 2 * time.Minute
	}
	recentTailRefreshMaxStaleness := cfg.RecentTailRefreshMaxStaleness
	if recentTailRefreshMaxStaleness <= 0 {
		recentTailRefreshMaxStaleness = 15 * time.Second
	}
	tailMode := cfg.TailMode
	if tailMode == "" {
		tailMode = TailModeAuto
	}
	switch tailMode {
	case TailModeAuto, TailModeNative, TailModeSynthetic:
	default:
		return nil, fmt.Errorf("invalid tail mode %q", tailMode)
	}
	labelValuesHotLimit := cfg.LabelValuesHotLimit
	if labelValuesHotLimit <= 0 {
		labelValuesHotLimit = 200
	}
	labelValuesIndexMaxEntries := cfg.LabelValuesIndexMaxEntries
	if labelValuesIndexMaxEntries <= 0 {
		labelValuesIndexMaxEntries = 200000
	}
	if labelValuesIndexMaxEntries < labelValuesHotLimit {
		labelValuesIndexMaxEntries = labelValuesHotLimit
	}
	labelValuesIndexPersistInterval := cfg.LabelValuesIndexPersistInterval
	if labelValuesIndexPersistInterval <= 0 {
		labelValuesIndexPersistInterval = 30 * time.Second
	}
	labelValuesIndexStartupStale := cfg.LabelValuesIndexStartupStale
	if labelValuesIndexStartupStale <= 0 {
		labelValuesIndexStartupStale = 60 * time.Second
	}
	labelValuesIndexPeerWarmTimeout := cfg.LabelValuesIndexPeerWarmTimeout
	if labelValuesIndexPeerWarmTimeout <= 0 {
		labelValuesIndexPeerWarmTimeout = 5 * time.Second
	}
	labelValuesPersistPath := strings.TrimSpace(cfg.LabelValuesIndexPersistPath)
	if err := ensureWritableSnapshotPath(labelValuesPersistPath); err != nil {
		return nil, fmt.Errorf("label-values index persistence path %q is not writable: %w", labelValuesPersistPath, err)
	}
	patternsPersistInterval := cfg.PatternsPersistInterval
	if patternsPersistInterval <= 0 {
		patternsPersistInterval = 30 * time.Second
	}
	patternsStartupStale := cfg.PatternsStartupStale
	if patternsStartupStale <= 0 {
		patternsStartupStale = 60 * time.Second
	}
	patternsPeerWarmTimeout := cfg.PatternsPeerWarmTimeout
	if patternsPeerWarmTimeout <= 0 {
		patternsPeerWarmTimeout = 5 * time.Second
	}
	patternsPersistPath := strings.TrimSpace(cfg.PatternsPersistPath)
	if err := ensureWritableSnapshotPath(patternsPersistPath); err != nil {
		return nil, fmt.Errorf("patterns persistence path %q is not writable: %w", patternsPersistPath, err)
	}

	labelTranslator := NewLabelTranslator(cfg.LabelStyle, cfg.FieldMappings)
	declaredLabelFields := buildDeclaredLabelFields(cfg.StreamFields, cfg.ExtraLabelFields, labelTranslator)
	patternsEnabled := true
	if cfg.PatternsEnabled != nil {
		patternsEnabled = *cfg.PatternsEnabled
	}
	tenantLimitsAllowPublish := append([]string(nil), cfg.TenantLimitsAllowPublish...)
	if len(tenantLimitsAllowPublish) == 0 {
		tenantLimitsAllowPublish = append([]string(nil), defaultTenantLimitsAllowPublish...)
	}
	tenantDefaultLimits := cloneStringAnyMap(cfg.TenantDefaultLimits)
	tenantLimits := cloneTenantLimitsMap(cfg.TenantLimits)
	patternsCustom := normalizeCustomPatterns(cfg.PatternsCustom)

	return &Proxy{
		backend:       u,
		rulerBackend:  rulerURL,
		alertsBackend: alertsURL,
		client: &http.Client{
			Timeout:   backendTimeout,
			Transport: transport,
		},
		tailClient: &http.Client{
			Transport: tailTransport,
		},
		cache:                                 cfg.Cache,
		compatCache:                           cfg.CompatCache,
		log:                                   logger,
		metrics:                               metrics.NewMetricsWithLimits(cfg.MetricsMaxTenants, cfg.MetricsMaxClients),
		queryTracker:                          metrics.NewQueryTracker(10000),
		coalescer:                             mw.NewCoalescer(),
		limiter:                               mw.NewRateLimiter(maxConcurrent, ratePerSec, rateBurst),
		breaker:                               mw.NewCircuitBreaker(cbFail, 3, cbOpen),
		tenantMap:                             cfg.TenantMap,
		authEnabled:                           cfg.AuthEnabled,
		allowGlobalTenant:                     cfg.AllowGlobalTenant,
		maxLines:                              maxLines,
		forwardHeaders:                        cfg.ForwardHeaders,
		forwardCookies:                        forwardCookies,
		backendHeaders:                        backendHeaders,
		backendCompression:                    normalizeBackendCompression(cfg.BackendCompression),
		clientResponseCompression:             cfg.ClientResponseCompression,
		clientResponseCompressionMinBytes:     cfg.ClientResponseCompressionMinBytes,
		backendMinVersion:                     backendMinVersion,
		backendAllowUnsupportedVersion:        cfg.BackendAllowUnsupportedVersion,
		backendVersionCheckTimeout:            backendVersionCheckTimeout,
		derivedFields:                         cfg.DerivedFields,
		streamResponse:                        cfg.StreamResponse,
		emitStructuredMetadata:                cfg.EmitStructuredMetadata,
		patternsEnabled:                       patternsEnabled,
		patternsAutodetectFromQueries:         cfg.PatternsAutodetectFromQueries,
		patternsCustom:                        patternsCustom,
		labelTranslator:                       labelTranslator,
		metadataFieldMode:                     metadataFieldMode,
		streamFieldsMap:                       buildStreamFieldsMap(cfg.StreamFields),
		declaredLabelFields:                   declaredLabelFields,
		peerCache:                             cfg.PeerCache,
		peerAuthToken:                         cfg.PeerAuthToken,
		registerInstrumentation:               registerInstrumentation,
		enablePprof:                           cfg.EnablePprof,
		enableQueryAnalytics:                  cfg.EnableQueryAnalytics,
		adminAuthToken:                        cfg.AdminAuthToken,
		tailAllowedOrigins:                    tailAllowedOrigins,
		tailMode:                              tailMode,
		metricsTrustProxyHeaders:              cfg.MetricsTrustProxyHeaders,
		tenantLimitsAllowPublish:              tenantLimitsAllowPublish,
		tenantDefaultLimits:                   tenantDefaultLimits,
		tenantLimits:                          tenantLimits,
		translationCache:                      cache.New(5*time.Minute, 5000),
		queryRangeWindowing:                   cfg.QueryRangeWindowingEnabled && cfg.QueryRangeSplitInterval > 0,
		queryRangeSplitInterval:               cfg.QueryRangeSplitInterval,
		queryRangeMaxParallel:                 queryRangeMaxParallel,
		queryRangeAdaptiveParallel:            cfg.QueryRangeAdaptiveParallel,
		queryRangeParallelMin:                 queryRangeParallelMin,
		queryRangeParallelMax:                 queryRangeParallelMax,
		queryRangeLatencyTarget:               queryRangeLatencyTarget,
		queryRangeLatencyBackoff:              queryRangeLatencyBackoff,
		queryRangeAdaptiveCooldown:            queryRangeAdaptiveCooldown,
		queryRangeErrorBackoffThreshold:       queryRangeErrorBackoffThreshold,
		queryRangeParallelCurrent:             queryRangeParallelMin,
		queryRangeFreshness:                   queryRangeFreshness,
		queryRangeRecentCacheTTL:              queryRangeRecentCacheTTL,
		queryRangeHistoryCacheTTL:             queryRangeHistoryCacheTTL,
		queryRangePrefilterIndexStats:         cfg.QueryRangePrefilterIndexStats,
		queryRangePrefilterMinWindows:         queryRangePrefilterMinWindows,
		queryRangeStreamAwareBatching:         cfg.QueryRangeStreamAwareBatching,
		queryRangeExpensiveWindowHitThreshold: queryRangeExpensiveHitThreshold,
		queryRangeExpensiveWindowMaxParallel:  queryRangeExpensiveMaxParallel,
		queryRangeAlignWindows:                cfg.QueryRangeAlignWindows,
		queryRangeWindowTimeout:               queryRangeWindowTimeout,
		queryRangePartialResponses:            cfg.QueryRangePartialResponses,
		queryRangeBackgroundWarm:              cfg.QueryRangeBackgroundWarm,
		queryRangeBackgroundWarmMaxWindows:    queryRangeBackgroundWarmMaxWindows,
		recentTailRefreshEnabled:              cfg.RecentTailRefreshEnabled,
		recentTailRefreshWindow:               recentTailRefreshWindow,
		recentTailRefreshMaxStaleness:         recentTailRefreshMaxStaleness,
		labelValuesIndexedCache:               cfg.LabelValuesIndexedCache,
		labelValuesHotLimit:                   labelValuesHotLimit,
		labelValuesIndexMaxEntries:            labelValuesIndexMaxEntries,
		labelValuesIndexPersistPath:           labelValuesPersistPath,
		labelValuesIndexPersistInterval:       labelValuesIndexPersistInterval,
		labelValuesIndexStartupStale:          labelValuesIndexStartupStale,
		labelValuesIndexPeerWarmTimeout:       labelValuesIndexPeerWarmTimeout,
		patternsPersistPath:                   patternsPersistPath,
		patternsPersistInterval:               patternsPersistInterval,
		patternsStartupStale:                  patternsStartupStale,
		patternsPeerWarmTimeout:               patternsPeerWarmTimeout,
		patternsPersistStop:                   make(chan struct{}),
		patternsPersistDone:                   make(chan struct{}),
		patternsSnapshotEntries:               make(map[string]patternSnapshotEntry),
		labelValuesIndexPersistStop:           make(chan struct{}),
		labelValuesIndexPersistDone:           make(chan struct{}),
		labelValuesIndex:                      make(map[string]*labelValuesIndexState),
	}, nil
}

func buildStreamFieldsMap(fields []string) map[string]bool {
	if len(fields) == 0 {
		return nil
	}
	m := make(map[string]bool, len(fields))
	for _, f := range fields {
		key := strings.TrimSpace(f)
		if key == "" {
			continue
		}
		m[key] = true
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

func ensureWritableSnapshotPath(path string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	dir := filepath.Dir(path)
	if dir == "" || dir == "." {
		dir = "."
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	return f.Close()
}

func buildDeclaredLabelFields(streamFields, extraLabelFields []string, lt *LabelTranslator) []string {
	if len(streamFields) == 0 && len(extraLabelFields) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(streamFields)+len(extraLabelFields))
	out := make([]string, 0, len(streamFields)+len(extraLabelFields))
	addField := func(raw string) {
		name := strings.TrimSpace(raw)
		if name == "" {
			return
		}
		if lt != nil {
			mapped := strings.TrimSpace(lt.ToVL(name))
			if mapped != "" {
				name = mapped
			}
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	for _, field := range streamFields {
		addField(field)
	}
	for _, field := range extraLabelFields {
		addField(field)
	}
	if len(out) == 0 {
		return nil
	}
	sort.Strings(out)
	return out
}

func normalizeCustomPatterns(patterns []string) []string {
	if len(patterns) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(patterns))
	out := make([]string, 0, len(patterns))
	for _, pattern := range patterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		if _, ok := seen[pattern]; ok {
			continue
		}
		seen[pattern] = struct{}{}
		out = append(out, pattern)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// Init wires cross-component dependencies after construction.
func (p *Proxy) Init() {
	p.metrics.SetCircuitBreakerFunc(p.breaker.State)
	p.labelValuesIndexWarmReady.Store(true)
	p.patternsWarmReady.Store(true)
	if p.labelValuesIndexedCache {
		p.warmLabelValuesIndexOnStartup()
		p.startLabelValuesIndexPersistenceLoop()
	}
	p.warmPatternsOnStartup()
	p.startPatternsPersistenceLoop()
}

// Shutdown flushes in-memory caches that should survive rolling restarts.
func (p *Proxy) Shutdown(ctx context.Context) error {
	p.stopPatternsPersistenceLoop(ctx)
	p.stopLabelValuesIndexPersistenceLoop(ctx)
	if err := p.persistPatternsNow("shutdown"); err != nil {
		return err
	}
	return p.persistLabelValuesIndexNow("shutdown")
}

// ReloadTenantMap hot-reloads tenant mappings (called on SIGHUP).
func (p *Proxy) ReloadTenantMap(m map[string]TenantMapping) {
	p.configMu.Lock()
	p.tenantMap = m
	if p.compatCache != nil {
		p.compatCache.InvalidatePrefix("")
	}
	p.configMu.Unlock()
	p.labelValuesIndexMu.Lock()
	p.labelValuesIndex = make(map[string]*labelValuesIndexState)
	p.labelValuesIndexMu.Unlock()
	p.labelValuesIndexPersistDirty.Store(true)
}

// ReloadFieldMappings hot-reloads field mappings and rebuilds the label translator.
func (p *Proxy) ReloadFieldMappings(mappings []FieldMapping) {
	p.configMu.Lock()
	p.labelTranslator = NewLabelTranslator(p.labelTranslator.style, mappings)
	if p.translationCache != nil {
		p.translationCache.InvalidatePrefix("")
	}
	if p.compatCache != nil {
		p.compatCache.InvalidatePrefix("")
	}
	p.configMu.Unlock()
	p.labelValuesIndexMu.Lock()
	p.labelValuesIndex = make(map[string]*labelValuesIndexState)
	p.labelValuesIndexMu.Unlock()
	p.labelValuesIndexPersistDirty.Store(true)
}

// GetMetrics returns the proxy's metrics instance for external telemetry exporters.
func (p *Proxy) GetMetrics() *metrics.Metrics { return p.metrics }

// GetQueryTracker returns the query analytics tracker.
func (p *Proxy) GetQueryTracker() *metrics.QueryTracker { return p.queryTracker }

// securityHeaders wraps a handler with security response headers.
func securityHeaders(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Cache-Control", "no-store")
		h.ServeHTTP(w, r)
	})
}

type requestPolicyError struct {
	status int
	msg    string
}

func (e *requestPolicyError) Error() string { return e.msg }

// validateTenantHeader applies Loki-style tenant presence checks and proxy-specific
// mapping validation before any backend call is made.
func (p *Proxy) validateTenantHeader(r *http.Request) error {
	orgID := strings.TrimSpace(r.Header.Get("X-Scope-OrgID"))
	if orgID == "" {
		if p.authEnabled {
			return &requestPolicyError{
				status: http.StatusUnauthorized,
				msg:    "missing X-Scope-OrgID header",
			}
		}
		return nil
	}
	if strings.Contains(orgID, "|") {
		if !isMultiTenantQueryPath(r.URL.Path) {
			return &requestPolicyError{
				status: http.StatusBadRequest,
				msg:    "multi-tenant X-Scope-OrgID values are only supported on query endpoints",
			}
		}
		tenantIDs := splitMultiTenantOrgIDs(orgID)
		if len(tenantIDs) < 2 {
			return &requestPolicyError{
				status: http.StatusBadRequest,
				msg:    "multi-tenant X-Scope-OrgID must include at least two tenant IDs",
			}
		}
		for _, tenantID := range tenantIDs {
			if tenantID == "*" {
				return &requestPolicyError{
					status: http.StatusBadRequest,
					msg:    `wildcard "*" is not supported inside multi-tenant X-Scope-OrgID values`,
				}
			}
			if err := p.validateSingleTenantOrgID(tenantID); err != nil {
				return err
			}
		}
		return nil
	}
	return p.validateSingleTenantOrgID(orgID)
}

func (p *Proxy) validateSingleTenantOrgID(orgID string) error {
	p.configMu.RLock()
	_, ok := p.tenantMap[orgID]
	p.configMu.RUnlock()
	if ok {
		return nil
	}

	if isDefaultTenantAlias(orgID) {
		return nil
	}
	if orgID == "*" {
		if p.globalTenantAllowed() {
			return nil
		}
		return &requestPolicyError{
			status: http.StatusForbidden,
			msg:    `global tenant bypass ("*") is disabled`,
		}
	}
	if _, err := strconv.Atoi(orgID); err == nil {
		return nil
	}

	return &requestPolicyError{
		status: http.StatusForbidden,
		msg:    fmt.Sprintf("unknown tenant %q", orgID),
	}
}

func splitMultiTenantOrgIDs(orgID string) []string {
	parts := strings.Split(orgID, "|")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if _, ok := seen[part]; ok {
			continue
		}
		seen[part] = struct{}{}
		out = append(out, part)
	}
	return out
}

func hasMultiTenantOrgID(orgID string) bool {
	return strings.IndexByte(orgID, '|') >= 0
}

func isMultiTenantQueryPath(path string) bool {
	switch {
	case path == "/loki/api/v1/query":
		return true
	case path == "/loki/api/v1/query_range":
		return true
	case path == "/loki/api/v1/series":
		return true
	case path == "/loki/api/v1/labels":
		return true
	case strings.HasPrefix(path, "/loki/api/v1/label/"):
		return true
	case path == "/loki/api/v1/index/stats":
		return true
	case path == "/loki/api/v1/index/volume":
		return true
	case path == "/loki/api/v1/index/volume_range":
		return true
	case path == "/loki/api/v1/detected_fields":
		return true
	case strings.HasPrefix(path, "/loki/api/v1/detected_field/"):
		return true
	case path == "/loki/api/v1/detected_labels":
		return true
	case path == "/loki/api/v1/patterns":
		return true
	default:
		return false
	}
}

func isDefaultTenantAlias(orgID string) bool {
	switch orgID {
	case "0", "fake", "default":
		return true
	default:
		return false
	}
}

func (p *Proxy) globalTenantAllowed() bool {
	if p.allowGlobalTenant {
		return true
	}
	p.configMu.RLock()
	defer p.configMu.RUnlock()
	return len(p.tenantMap) == 0
}

func (p *Proxy) tenantMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := p.validateTenantHeader(r); err != nil {
			if rpe, ok := err.(*requestPolicyError); ok {
				p.writeError(w, rpe.status, rpe.msg)
				return
			}
			p.writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (p *Proxy) adminMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p.adminAuthToken != "" {
			got := strings.TrimSpace(r.Header.Get("X-Admin-Token"))
			if got == "" {
				auth := strings.TrimSpace(r.Header.Get("Authorization"))
				if strings.HasPrefix(auth, "Bearer ") {
					got = strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
				}
			}
			if got != p.adminAuthToken {
				p.writeError(w, http.StatusUnauthorized, "admin authentication required")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func hostOnly(addr string) string {
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}

func (p *Proxy) isKnownPeerHost(host string) bool {
	if p.peerCache == nil {
		return false
	}
	for _, peer := range p.peerCache.Peers() {
		if hostOnly(peer) == host {
			return true
		}
	}
	return false
}

func (p *Proxy) peerCacheMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p.peerAuthToken != "" {
			if r.Header.Get("X-Peer-Token") != p.peerAuthToken {
				p.writeError(w, http.StatusUnauthorized, "peer authentication required")
				return
			}
			next.ServeHTTP(w, r)
			return
		}

		if !p.isKnownPeerHost(hostOnly(r.RemoteAddr)) {
			p.writeError(w, http.StatusForbidden, "peer cache endpoint is restricted to configured peers")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (p *Proxy) isAllowedTailOrigin(origin string) bool {
	origin = strings.TrimSpace(origin)
	if origin == "" {
		return true
	}
	if _, ok := p.tailAllowedOrigins["*"]; ok {
		return true
	}
	_, ok := p.tailAllowedOrigins[origin]
	return ok
}

func (p *Proxy) tailUpgrader() websocket.Upgrader {
	return websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return p.isAllowedTailOrigin(r.Header.Get("Origin"))
		},
	}
}

func compatCacheableEndpoint(endpoint string) bool {
	switch endpoint {
	case "query", "query_range", "series", "labels", "label_values", "index_stats", "volume", "volume_range", "detected_fields", "detected_field_values", "detected_labels", "patterns":
		return true
	default:
		return false
	}
}

func (p *Proxy) shouldUseCompatCache(endpoint string, r *http.Request) bool {
	if p.compatCache == nil || r.Method != http.MethodGet || !compatCacheableEndpoint(endpoint) {
		return false
	}
	if (endpoint == "query" || endpoint == "query_range") && p.streamResponse {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(r.Header.Get("Upgrade")), "websocket") {
		return false
	}
	return true
}

func (p *Proxy) compatCacheKey(endpoint string, r *http.Request) (string, bool) {
	if !p.shouldUseCompatCache(endpoint, r) {
		return "", false
	}
	return "compat:v1:" + endpoint + ":" + r.Header.Get("X-Scope-OrgID") + ":" + r.URL.Path + "?" + r.URL.RawQuery, true
}

func compatCacheVariantKey(baseKey, encoding string) string {
	return baseKey + ":enc:" + encoding
}

func (p *Proxy) compatCacheResponseEncoding(r *http.Request) (string, int) {
	return mw.PlanResponseCompression(r, mw.CompressionOptions{
		Mode:     p.clientResponseCompression,
		MinBytes: p.clientResponseCompressionMinBytes,
	})
}

func (p *Proxy) compatCacheEncodedVariant(cacheKey string, body []byte, ttl time.Duration, encoding string, minBytes int) ([]byte, string, bool) {
	if encoding == "" || len(body) == 0 || len(body) < minBytes {
		return body, "", true
	}
	variantKey := compatCacheVariantKey(cacheKey, encoding)
	if cachedVariant, ok := p.compatCache.Get(variantKey); ok {
		return cachedVariant, encoding, true
	}
	encoded, err := mw.EncodeResponseBody(encoding, body)
	if err != nil || len(encoded) >= len(body) {
		return body, "", false
	}
	p.compatCache.SetWithTTL(variantKey, encoded, ttl)
	return encoded, encoding, true
}

func compatCacheResponseAllowed(rec *httptest.ResponseRecorder) bool {
	if rec == nil || rec.Code != http.StatusOK || rec.Flushed {
		return false
	}
	if len(rec.Result().Cookies()) > 0 {
		return false
	}
	contentType := strings.ToLower(strings.TrimSpace(rec.Header().Get("Content-Type")))
	return contentType == "" || strings.Contains(contentType, "application/json")
}

type compatCacheCaptureWriter struct {
	http.ResponseWriter
	body    []byte
	code    int
	flushed bool
}

func (w *compatCacheCaptureWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *compatCacheCaptureWriter) Write(b []byte) (int, error) {
	if w.code == 0 {
		w.code = http.StatusOK
	}
	w.body = append(w.body, b...)
	return w.ResponseWriter.Write(b)
}

func (w *compatCacheCaptureWriter) Flush() {
	w.flushed = true
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func compatCacheCaptureAllowed(code int, flushed bool, header http.Header) bool {
	if code == 0 {
		code = http.StatusOK
	}
	if code != http.StatusOK || flushed {
		return false
	}
	if len(header.Values("Set-Cookie")) > 0 {
		return false
	}
	contentType := strings.ToLower(strings.TrimSpace(header.Get("Content-Type")))
	return contentType == "" || strings.Contains(contentType, "application/json")
}

func patternsPayloadEmpty(body []byte) bool {
	if len(body) == 0 {
		return true
	}
	var resp struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return false
	}
	return len(resp.Data) == 0
}

func (p *Proxy) compatCacheMiddleware(endpoint, route string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		cacheKey, cacheable := p.compatCacheKey(endpoint, r)
		if !cacheable {
			setCacheResult(r.Context(), "bypass")
			next(w, r)
			return
		}
		ttl := CacheTTLs[endpoint]
		if ttl <= 0 {
			setCacheResult(r.Context(), "bypass")
			next(w, r)
			return
		}
		if cached, remainingTTL, ok := p.compatCache.GetWithTTL(cacheKey); ok {
			setCacheResult(r.Context(), "hit")
			w.Header().Set("Content-Type", "application/json")
			body := cached
			if encoding, minBytes := p.compatCacheResponseEncoding(r); encoding != "" {
				if encodedBody, encodedAs, ok := p.compatCacheEncodedVariant(cacheKey, cached, remainingTTL, encoding, minBytes); ok && encodedAs != "" {
					w.Header().Set("Content-Encoding", encodedAs)
					w.Header().Set("Vary", "Accept-Encoding")
					body = encodedBody
				}
			}
			_, _ = w.Write(body)
			elapsed := time.Since(start)
			p.metrics.RecordRequestWithRoute(endpoint, route, http.StatusOK, elapsed)
			p.metrics.RecordCacheHit()
			if endpoint == "query" || endpoint == "query_range" {
				p.queryTracker.Record(endpoint, r.FormValue("query"), elapsed, false)
			}
			return
		}

		setCacheResult(r.Context(), "miss")
		var capture *compatCacheCaptureWriter
		if encoding, _ := p.compatCacheResponseEncoding(r); encoding != "" {
			_ = mw.RegisterEncodedResponseCapture(w, encoding, func(encodedAs string, encoded []byte) {
				if capture == nil || !compatCacheCaptureAllowed(capture.code, capture.flushed, w.Header()) {
					return
				}
				if endpoint == "patterns" && patternsPayloadEmpty(capture.body) {
					return
				}
				if len(encoded) == 0 || len(encoded) >= len(capture.body) {
					return
				}
				p.compatCache.SetWithTTL(compatCacheVariantKey(cacheKey, encodedAs), encoded, ttl)
			})
		}
		capture = &compatCacheCaptureWriter{ResponseWriter: w}
		next(capture, r)
		if compatCacheCaptureAllowed(capture.code, capture.flushed, w.Header()) {
			body := capture.body
			if endpoint == "patterns" && patternsPayloadEmpty(body) {
				return
			}
			p.compatCache.SetWithTTL(cacheKey, append([]byte(nil), body...), ttl)
		}
	}
}

func (p *Proxy) RegisterRoutes(mux *http.ServeMux) {
	// Rate-limited endpoints with security headers + request logging
	rl := func(endpoint, route string, h http.HandlerFunc) http.Handler {
		return securityHeaders(p.tenantMiddleware(p.limiter.Middleware(p.requestLogger(endpoint, route, p.compatCacheMiddleware(endpoint, route, h)))))
	}
	rlNoTenant := func(endpoint, route string, h http.HandlerFunc) http.Handler {
		return securityHeaders(p.limiter.Middleware(p.requestLogger(endpoint, route, h)))
	}

	// Loki API endpoints — data queries are rate-limited
	mux.Handle("/loki/api/v1/query_range", rl("query_range", "/loki/api/v1/query_range", p.handleQueryRange))
	mux.Handle("/loki/api/v1/query", rl("query", "/loki/api/v1/query", p.handleQuery))
	mux.Handle("/loki/api/v1/series", rl("series", "/loki/api/v1/series", p.handleSeries))

	// Metadata endpoints — rate-limited but cached
	mux.Handle("/loki/api/v1/labels", rl("labels", "/loki/api/v1/labels", p.handleLabels))
	mux.Handle("/loki/api/v1/label/", rl("label_values", "/loki/api/v1/label/{name}/values", p.handleLabelValues))
	mux.Handle("/loki/api/v1/detected_fields", rl("detected_fields", "/loki/api/v1/detected_fields", p.handleDetectedFields))
	mux.Handle("/loki/api/v1/detected_field/", rl("detected_field_values", "/loki/api/v1/detected_field/{name}/values", p.handleDetectedFieldValues))

	// Lighter endpoints — still rate-limited
	mux.Handle("/loki/api/v1/index/stats", rl("index_stats", "/loki/api/v1/index/stats", p.handleIndexStats))
	mux.Handle("/loki/api/v1/index/volume", rl("volume", "/loki/api/v1/index/volume", p.handleVolume))
	mux.Handle("/loki/api/v1/index/volume_range", rl("volume_range", "/loki/api/v1/index/volume_range", p.handleVolumeRange))
	mux.Handle("/loki/api/v1/patterns", rl("patterns", "/loki/api/v1/patterns", p.handlePatterns))
	mux.Handle("/loki/api/v1/tail", rl("tail", "/loki/api/v1/tail", p.handleTail))

	// Read-only API additions
	mux.Handle("/loki/api/v1/format_query", rl("format_query", "/loki/api/v1/format_query", p.handleFormatQuery))
	mux.Handle("/loki/api/v1/detected_labels", rl("detected_labels", "/loki/api/v1/detected_labels", p.handleDetectedLabels))
	mux.Handle("/loki/api/v1/drilldown-limits", rlNoTenant("drilldown_limits", "/loki/api/v1/drilldown-limits", p.handleDrilldownLimits))
	mux.Handle("/config/tenant/v1/limits", rlNoTenant("tenant_limits", "/config/tenant/v1/limits", p.handleTenantLimitsConfig))

	// Write endpoints — blocked (this is a read-only proxy)
	mux.HandleFunc("/loki/api/v1/push", p.handleWriteBlocked)

	// Delete endpoint — exception to read-only with strict safeguards
	mux.Handle("/loki/api/v1/delete", rl("delete", "/loki/api/v1/delete", p.handleDelete))

	// Alerting / ruler read endpoints
	alertRead := func(endpoint, route string, h http.HandlerFunc) http.Handler {
		return securityHeaders(p.tenantMiddleware(p.requestLogger(endpoint, route, h)))
	}
	mux.Handle("/loki/api/v1/rules", alertRead("rules", "/loki/api/v1/rules", p.handleRules))
	mux.Handle("/loki/api/v1/rules/", alertRead("rules_nested", "/loki/api/v1/rules/{namespace}", p.handleRules))
	mux.Handle("/api/prom/rules", alertRead("rules_prom", "/api/prom/rules", p.handleRules))
	mux.Handle("/api/prom/rules/", alertRead("rules_prom_nested", "/api/prom/rules/{namespace}", p.handleRules))
	mux.Handle("/prometheus/api/v1/rules", alertRead("rules_prometheus", "/prometheus/api/v1/rules", p.handleRules))
	mux.Handle("/loki/api/v1/alerts", alertRead("alerts", "/loki/api/v1/alerts", p.handleAlerts))
	mux.Handle("/api/prom/alerts", alertRead("alerts_prom", "/api/prom/alerts", p.handleAlerts))
	mux.Handle("/prometheus/api/v1/alerts", alertRead("alerts_prometheus", "/prometheus/api/v1/alerts", p.handleAlerts))
	mux.HandleFunc("/config", p.handleConfigStub)

	// Health / readiness — NOT rate-limited
	mux.HandleFunc("/alive", p.handleAlive)
	mux.HandleFunc("/livez", p.handleAlive)
	mux.HandleFunc("/health", p.handleHealth)
	mux.HandleFunc("/healthz", p.handleHealth)
	mux.HandleFunc("/ready", p.handleReady)
	mux.HandleFunc("/loki/api/v1/status/buildinfo", p.handleBuildInfo)

	if p.registerInstrumentation {
		// Prometheus metrics endpoint — NOT rate-limited
		mux.HandleFunc("/metrics", p.handleMetrics)
		if p.enablePprof {
			mux.Handle("/debug/pprof/cmdline", p.adminMiddleware(http.NotFoundHandler()))
			mux.Handle("/debug/pprof/", p.adminMiddleware(http.DefaultServeMux))
		}
	}

	if p.enableQueryAnalytics {
		mux.Handle("/debug/queries", securityHeaders(p.adminMiddleware(http.HandlerFunc(p.queryTracker.Handler))))
	}

	// Peer cache endpoint — internal, for sharded fleet cache
	if p.peerCache != nil {
		peerCacheHandler := securityHeaders(p.peerCacheMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p.peerCache.ServeHTTP(w, r, p.cache)
		})))
		mux.Handle("/_cache/get", peerCacheHandler)
		mux.Handle("/_cache/set", peerCacheHandler)
		mux.Handle("/_cache/hot", peerCacheHandler)
	}
}

func (p *Proxy) handleMetrics(w http.ResponseWriter, r *http.Request) {
	rec := httptest.NewRecorder()
	p.metrics.Handler(rec, r)

	for k, vals := range rec.Header() {
		for _, v := range vals {
			w.Header().Add(k, v)
		}
	}
	if p.peerCache != nil {
		_, _ = rec.Body.WriteString(p.peerCacheMetrics())
	}

	w.WriteHeader(rec.Code)
	_, _ = w.Write(rec.Body.Bytes())
}

func (p *Proxy) peerCacheMetrics() string {
	stats := p.peerCache.Stats()
	remotePeers, _ := stats["peers"].(int)
	hits, _ := stats["peer_hits"].(int64)
	misses, _ := stats["peer_misses"].(int64)
	errors, _ := stats["peer_errors"].(int64)
	wtPushes, _ := stats["wt_pushes"].(int64)
	wtErrors, _ := stats["wt_errors"].(int64)
	raHotRequests, _ := stats["ra_hot_requests"].(int64)
	raHotErrors, _ := stats["ra_hot_errors"].(int64)
	raPrefetches, _ := stats["ra_prefetches"].(int64)
	raPrefetchBytes, _ := stats["ra_prefetch_bytes"].(int64)
	raBudgetDrops, _ := stats["ra_budget_drops"].(int64)
	raTenantSkips, _ := stats["ra_tenant_skips"].(int64)
	clusterMembers := len(p.peerCache.Peers())

	return fmt.Sprintf(
		"# HELP loki_vl_proxy_peer_cache_peers Number of remote peers currently in the fleet cache ring.\n"+
			"# TYPE loki_vl_proxy_peer_cache_peers gauge\n"+
			"loki_vl_proxy_peer_cache_peers %d\n"+
			"# HELP loki_vl_proxy_peer_cache_cluster_members Number of total cache ring members including this proxy instance.\n"+
			"# TYPE loki_vl_proxy_peer_cache_cluster_members gauge\n"+
			"loki_vl_proxy_peer_cache_cluster_members %d\n"+
			"# HELP loki_vl_proxy_peer_cache_hits_total Successful peer-cache fetches.\n"+
			"# TYPE loki_vl_proxy_peer_cache_hits_total counter\n"+
			"loki_vl_proxy_peer_cache_hits_total %d\n"+
			"# HELP loki_vl_proxy_peer_cache_misses_total Peer-cache lookups that missed on the owner.\n"+
			"# TYPE loki_vl_proxy_peer_cache_misses_total counter\n"+
			"loki_vl_proxy_peer_cache_misses_total %d\n"+
			"# HELP loki_vl_proxy_peer_cache_errors_total Peer-cache fetch errors.\n"+
			"# TYPE loki_vl_proxy_peer_cache_errors_total counter\n"+
			"loki_vl_proxy_peer_cache_errors_total %d\n"+
			"# HELP loki_vl_proxy_peer_cache_write_through_pushes_total Successful owner write-through pushes from non-owner peers.\n"+
			"# TYPE loki_vl_proxy_peer_cache_write_through_pushes_total counter\n"+
			"loki_vl_proxy_peer_cache_write_through_pushes_total %d\n"+
			"# HELP loki_vl_proxy_peer_cache_write_through_errors_total Owner write-through push errors.\n"+
			"# TYPE loki_vl_proxy_peer_cache_write_through_errors_total counter\n"+
			"loki_vl_proxy_peer_cache_write_through_errors_total %d\n"+
			"# HELP loki_vl_proxy_peer_cache_hot_index_requests_total Peer hot-index requests.\n"+
			"# TYPE loki_vl_proxy_peer_cache_hot_index_requests_total counter\n"+
			"loki_vl_proxy_peer_cache_hot_index_requests_total %d\n"+
			"# HELP loki_vl_proxy_peer_cache_hot_index_errors_total Peer hot-index request errors.\n"+
			"# TYPE loki_vl_proxy_peer_cache_hot_index_errors_total counter\n"+
			"loki_vl_proxy_peer_cache_hot_index_errors_total %d\n"+
			"# HELP loki_vl_proxy_peer_cache_read_ahead_prefetches_total Successful hot read-ahead prefetches.\n"+
			"# TYPE loki_vl_proxy_peer_cache_read_ahead_prefetches_total counter\n"+
			"loki_vl_proxy_peer_cache_read_ahead_prefetches_total %d\n"+
			"# HELP loki_vl_proxy_peer_cache_read_ahead_prefetch_bytes_total Bytes prefetched by hot read-ahead.\n"+
			"# TYPE loki_vl_proxy_peer_cache_read_ahead_prefetch_bytes_total counter\n"+
			"loki_vl_proxy_peer_cache_read_ahead_prefetch_bytes_total %d\n"+
			"# HELP loki_vl_proxy_peer_cache_read_ahead_budget_drops_total Hot read-ahead candidates dropped by budget/size filters.\n"+
			"# TYPE loki_vl_proxy_peer_cache_read_ahead_budget_drops_total counter\n"+
			"loki_vl_proxy_peer_cache_read_ahead_budget_drops_total %d\n"+
			"# HELP loki_vl_proxy_peer_cache_read_ahead_tenant_skips_total Hot read-ahead candidates skipped by tenant fairness pass.\n"+
			"# TYPE loki_vl_proxy_peer_cache_read_ahead_tenant_skips_total counter\n"+
			"loki_vl_proxy_peer_cache_read_ahead_tenant_skips_total %d\n",
		remotePeers, clusterMembers, hits, misses, errors, wtPushes, wtErrors, raHotRequests, raHotErrors, raPrefetches, raPrefetchBytes, raBudgetDrops, raTenantSkips,
	)
}

// handleQueryRange translates Loki range queries.
// Loki: GET /loki/api/v1/query_range?query={...}&start=...&end=...&limit=...&step=...
// VL stats: POST /select/logsql/stats_query_range with query, start, end, step
// VL logs:  POST /select/logsql/query with query, start, end, limit
func (p *Proxy) handleQueryRange(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	logqlQuery := r.FormValue("query")
	if _, ok := p.validateQuery(w, logqlQuery, "query_range"); !ok {
		return
	}
	categorizedLabels := requestWantsCategorizedLabels(r)
	emitStructuredMetadata := p.shouldEmitStructuredMetadata(r)
	tupleMode := tupleModeForRequest(categorizedLabels, emitStructuredMetadata)
	if p.handleMultiTenantFanout(w, r, "query_range", p.handleQueryRange) {
		return
	}
	cacheKey := ""
	cacheable := !p.streamResponse
	if cacheable {
		cacheKey = p.queryRangeCacheKey(r, logqlQuery)
		if cached, remaining, ok := p.cache.GetWithTTL(cacheKey); ok {
			if !p.shouldBypassRecentTailCache("query_range", remaining, r) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(cached)
				elapsed := time.Since(start)
				p.metrics.RecordRequest("query_range", http.StatusOK, elapsed)
				p.metrics.RecordTupleMode(tupleMode)
				p.metrics.RecordCacheHit()
				p.queryTracker.Record("query_range", logqlQuery, elapsed, false)
				return
			}
		}
		p.metrics.RecordCacheMiss()
	}
	p.log.Debug("query_range request", "logql", logqlQuery)

	logqlQuery = p.preferWorkingParser(r.Context(), logqlQuery, r.FormValue("start"), r.FormValue("end"))

	logsqlQuery, err := p.translateQuery(logqlQuery)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		p.metrics.RecordRequest("query_range", http.StatusBadRequest, time.Since(start))
		return
	}
	// Extract without() labels for post-processing
	logsqlQuery, withoutLabels := translator.ParseWithoutMarker(logsqlQuery)
	p.log.Debug("translated query", "logsql", logsqlQuery, "without", withoutLabels)

	r = withOrgID(r)

	var (
		sc       = &statusCapture{ResponseWriter: w, code: 200}
		capture  *bufferedResponseWriter
		cacheTap *compatCacheCaptureWriter
	)
	if len(withoutLabels) > 0 {
		capture = &bufferedResponseWriter{header: make(http.Header)}
		sc = &statusCapture{ResponseWriter: capture, code: 200}
	} else if cacheable {
		cacheTap = &compatCacheCaptureWriter{ResponseWriter: w}
		sc = &statusCapture{ResponseWriter: cacheTap, code: 200}
	}

	// Check for subquery expression (e.g., max_over_time(rate(...)[1h:5m]))
	if outerFunc, innerQL, rng, step, ok := translator.ParseSubqueryExpr(logsqlQuery); ok {
		p.proxySubqueryRange(sc, r, outerFunc, innerQL, rng, step)
	} else if op, left, right, vm, ok := translator.ParseBinaryMetricExprFull(logsqlQuery); ok {
		// Binary metric expression (e.g., sum(rate(...)) / sum(rate(...)))
		p.proxyBinaryMetricQueryRangeVM(sc, r, op, left, right, vm)
	} else if isStatsQuery(logsqlQuery) {
		p.proxyStatsQueryRange(sc, r, logsqlQuery)
	} else {
		if !p.proxyLogQueryWindowed(sc, r, logsqlQuery) {
			p.proxyLogQuery(sc, r, logsqlQuery)
		}
	}

	if capture != nil {
		cacheOut := capture.body
		if len(withoutLabels) > 0 {
			cacheOut = applyWithoutGrouping(cacheOut, withoutLabels)
		}
		copyHeaders(w.Header(), capture.Header())
		if w.Header().Get("Content-Type") == "" {
			w.Header().Set("Content-Type", "application/json")
		}
		if sc.code != http.StatusOK {
			w.WriteHeader(sc.code)
		}
		_, _ = w.Write(cacheOut)
		if cacheable && sc.code == http.StatusOK {
			p.cache.SetWithTTL(cacheKey, cacheOut, CacheTTLs["query_range"])
		}
	} else if cacheTap != nil && cacheable && sc.code == http.StatusOK {
		p.cache.SetWithTTL(cacheKey, cacheTap.body, CacheTTLs["query_range"])
	}

	elapsed := time.Since(start)
	p.metrics.RecordRequest("query_range", sc.code, elapsed)
	p.queryTracker.Record("query_range", logqlQuery, elapsed, sc.code >= 400)
}

func (p *Proxy) queryRangeCacheKey(r *http.Request, logqlQuery string) string {
	rawQuery := r.URL.RawQuery
	if rawQuery == "" {
		var b strings.Builder
		b.Grow(len(logqlQuery) + 64)
		b.WriteString("query=")
		b.WriteString(url.QueryEscape(logqlQuery))
		for _, key := range []string{"start", "end", "step", "limit", "direction"} {
			if value := r.FormValue(key); value != "" {
				b.WriteByte('&')
				b.WriteString(key)
				b.WriteByte('=')
				b.WriteString(url.QueryEscape(value))
			}
		}
		rawQuery = b.String()
	}
	return "query_range:" + r.Header.Get("X-Scope-OrgID") + ":" + rawQuery + ":" + p.tupleModeCacheKey(r)
}

// handleQuery translates Loki instant queries.
func (p *Proxy) handleQuery(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	logqlQuery := r.FormValue("query")
	if _, ok := p.validateQuery(w, logqlQuery, "query"); !ok {
		return
	}
	p.log.Debug("query request", "logql", logqlQuery)

	if body, ok := evaluateConstantInstantVectorQuery(logqlQuery, r.FormValue("time")); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
		elapsed := time.Since(start)
		p.metrics.RecordRequest("query", http.StatusOK, elapsed)
		p.queryTracker.Record("query", logqlQuery, elapsed, false)
		return
	}
	if p.handleMultiTenantFanout(w, r, "query", p.handleQuery) {
		return
	}

	logqlQuery = p.preferWorkingParser(r.Context(), logqlQuery, r.FormValue("start"), r.FormValue("end"))

	logsqlQuery, err := p.translateQuery(logqlQuery)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		p.metrics.RecordRequest("query", http.StatusBadRequest, time.Since(start))
		return
	}

	// Extract without() labels for post-processing
	logsqlQuery, withoutLabels := translator.ParseWithoutMarker(logsqlQuery)

	r = withOrgID(r)

	// Wrap writer to capture actual status code for metrics
	sc := &statusCapture{ResponseWriter: w, code: 200}

	var bw *bufferedResponseWriter
	if len(withoutLabels) > 0 {
		bw = &bufferedResponseWriter{header: w.Header()}
		sc = &statusCapture{ResponseWriter: bw, code: 200}
	}

	if outerFunc, innerQL, rng, step, ok := translator.ParseSubqueryExpr(logsqlQuery); ok {
		p.proxySubquery(sc, r, outerFunc, innerQL, rng, step)
	} else if op, left, right, vm, ok := translator.ParseBinaryMetricExprFull(logsqlQuery); ok {
		p.proxyBinaryMetricQueryVM(sc, r, op, left, right, vm)
	} else if isStatsQuery(logsqlQuery) {
		p.proxyStatsQuery(sc, r, logsqlQuery)
	} else {
		p.proxyLogQuery(sc, r, logsqlQuery)
	}

	if bw != nil && len(withoutLabels) > 0 {
		result := applyWithoutGrouping(bw.body, withoutLabels)
		w.Header().Set("Content-Type", "application/json")
		w.Write(result)
	}

	elapsed := time.Since(start)
	p.metrics.RecordRequest("query", sc.code, elapsed)
	p.queryTracker.Record("query", logqlQuery, elapsed, sc.code >= 400)
}

var (
	vectorLiteralRE = regexp.MustCompile(`^\s*vector\(\s*([^)]+?)\s*\)\s*$`)
	vectorBinaryRE  = regexp.MustCompile(`^\s*vector\(\s*([^)]+?)\s*\)\s*([+\-*/])\s*vector\(\s*([^)]+?)\s*\)\s*$`)
)

func evaluateConstantInstantVectorQuery(expr, timeParam string) ([]byte, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, false
	}

	if matches := vectorLiteralRE.FindStringSubmatch(expr); len(matches) == 2 {
		value, err := strconv.ParseFloat(strings.TrimSpace(matches[1]), 64)
		if err != nil {
			return nil, false
		}
		return buildConstantVectorResponse(timeParam, value), true
	}

	if matches := vectorBinaryRE.FindStringSubmatch(expr); len(matches) == 4 {
		left, err := strconv.ParseFloat(strings.TrimSpace(matches[1]), 64)
		if err != nil {
			return nil, false
		}
		right, err := strconv.ParseFloat(strings.TrimSpace(matches[3]), 64)
		if err != nil {
			return nil, false
		}
		value, ok := applyConstantBinaryOp(left, right, matches[2])
		if !ok {
			return nil, false
		}
		return buildConstantVectorResponse(timeParam, value), true
	}

	return nil, false
}

func buildConstantVectorResponse(timeParam string, value float64) []byte {
	ts := parseInstantVectorTime(timeParam)
	result, _ := json.Marshal(map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "vector",
			"result": []map[string]interface{}{
				{
					"metric": map[string]string{},
					"value":  []interface{}{ts, strconv.FormatFloat(value, 'f', -1, 64)},
				},
			},
			"stats": map[string]interface{}{},
		},
	})
	return result
}

func parseInstantVectorTime(timeParam string) int64 {
	if timeParam == "" {
		return time.Now().UnixNano()
	}
	if t, err := time.Parse(time.RFC3339Nano, timeParam); err == nil {
		return t.UnixNano()
	}
	if i, err := strconv.ParseInt(timeParam, 10, 64); err == nil {
		if i < 1_000_000_000_000 {
			return i * int64(time.Second)
		}
		return i
	}
	if f, err := strconv.ParseFloat(timeParam, 64); err == nil {
		if f < 1_000_000_000_000 {
			return int64(f * float64(time.Second))
		}
		return int64(f)
	}
	return time.Now().UnixNano()
}

func applyConstantBinaryOp(left, right float64, op string) (float64, bool) {
	switch op {
	case "+":
		return left + right, true
	case "-":
		return left - right, true
	case "*":
		return left * right, true
	case "/":
		if right == 0 {
			return 0, false
		}
		return left / right, true
	default:
		return 0, false
	}
}

type vlAPIError struct {
	status int
	body   string
}

func (e *vlAPIError) Error() string {
	if strings.TrimSpace(e.body) == "" {
		return fmt.Sprintf("victorialogs api error: status %d", e.status)
	}
	return strings.TrimSpace(e.body)
}

func shouldFallbackToGenericMetadata(err error) bool {
	apiErr, ok := err.(*vlAPIError)
	if !ok {
		return false
	}
	return apiErr.status >= 400 && apiErr.status < 500
}

func decodeVLFieldHits(body []byte) ([]string, error) {
	var resp struct {
		Values []struct {
			Value string `json:"value"`
			Hits  int64  `json:"hits"`
		} `json:"values"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	values := make([]string, 0, len(resp.Values))
	for _, item := range resp.Values {
		values = append(values, item.Value)
	}
	return values, nil
}

func appendUniqueStrings(dst []string, values ...string) []string {
	if len(values) == 0 {
		return dst
	}
	seen := make(map[string]struct{}, len(dst)+len(values))
	for _, existing := range dst {
		seen[existing] = struct{}{}
	}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		dst = append(dst, value)
	}
	return dst
}

func (p *Proxy) snapshotDeclaredLabelFields() []string {
	if p == nil || len(p.declaredLabelFields) == 0 {
		return nil
	}
	out := make([]string, len(p.declaredLabelFields))
	copy(out, p.declaredLabelFields)
	return out
}

func (p *Proxy) vlGetMetadataCoalesced(ctx context.Context, path string, params url.Values) (int, []byte, error) {
	key := "vlmeta:get:" + getOrgID(ctx) + ":" + path + "?" + params.Encode()
	status, _, body, err := p.coalescer.Do(key, func() (*http.Response, error) {
		return p.vlGet(ctx, path, params)
	})
	if err != nil {
		return 0, nil, err
	}
	return status, body, nil
}

func (p *Proxy) fetchVLFieldNames(ctx context.Context, path string, params url.Values) ([]string, error) {
	status, body, err := p.vlGetMetadataCoalesced(ctx, path, params)
	if err != nil {
		return nil, err
	}
	if status >= 400 {
		return nil, &vlAPIError{status: status, body: string(body)}
	}
	fields, err := decodeVLFieldHits(body)
	if err != nil {
		return nil, err
	}
	p.labelTranslator.LearnFieldAliases(fields)
	return fields, nil
}

func (p *Proxy) fetchVLFieldValues(ctx context.Context, path string, params url.Values) ([]string, error) {
	status, body, err := p.vlGetMetadataCoalesced(ctx, path, params)
	if err != nil {
		return nil, err
	}
	if status >= 400 {
		return nil, &vlAPIError{status: status, body: string(body)}
	}
	return decodeVLFieldHits(body)
}

func (p *Proxy) fetchPreferredLabelNames(ctx context.Context, params url.Values) ([]string, error) {
	if p.supportsStreamMetadataEndpoints() {
		labels, err := p.fetchVLFieldNames(ctx, "/select/logsql/stream_field_names", params)
		if err == nil {
			labels = appendUniqueStrings(labels, p.snapshotDeclaredLabelFields()...)
			return labels, nil
		}
		if !shouldFallbackToGenericMetadata(err) {
			return nil, err
		}
	}
	fallback, fallbackErr := p.fetchVLFieldNames(ctx, "/select/logsql/field_names", params)
	if fallbackErr != nil {
		return nil, fallbackErr
	}
	fallback = appendUniqueStrings(fallback, p.snapshotDeclaredLabelFields()...)
	return fallback, nil
}

func (p *Proxy) fetchPreferredLabelNamesCached(ctx context.Context, params url.Values) ([]string, error) {
	if p == nil || p.cache == nil {
		return p.fetchPreferredLabelNames(ctx, params)
	}

	cacheKey := "label_inventory:" + getOrgID(ctx) + ":" + params.Encode()
	if cached, ok := p.cache.Get(cacheKey); ok {
		var labels []string
		if err := json.Unmarshal(cached, &labels); err == nil {
			return labels, nil
		}
	}

	labels, err := p.fetchPreferredLabelNames(ctx, params)
	if err != nil {
		return nil, err
	}
	if encoded, err := json.Marshal(labels); err == nil {
		p.cache.SetWithTTL(cacheKey, encoded, CacheTTLs["label_inventory"])
	}
	return labels, nil
}

func (p *Proxy) fetchPreferredLabelValues(ctx context.Context, labelName string, params url.Values) ([]string, error) {
	useStreamEndpoint := p.supportsStreamMetadataEndpoints()
	streamFields := []string{}
	if useStreamEndpoint {
		var err error
		streamFields, err = p.fetchVLFieldNames(ctx, "/select/logsql/stream_field_names", params)
		useStreamEndpoint = err == nil
		if err != nil && !shouldFallbackToGenericMetadata(err) {
			return nil, err
		}
	}
	streamFields = appendUniqueStrings(streamFields, p.snapshotDeclaredLabelFields()...)

	resolution := p.labelTranslator.ResolveLabelCandidates(labelName, streamFields)
	if len(resolution.candidates) == 0 && useStreamEndpoint {
		useStreamEndpoint = false
	}
	if len(resolution.candidates) == 0 && !useStreamEndpoint {
		resolution = p.labelTranslator.ResolveLabelCandidates(labelName, p.snapshotDeclaredLabelFields())
	}
	if len(resolution.candidates) == 0 {
		resolution = fieldResolution{candidates: []string{p.labelTranslator.ToVL(labelName)}}
	}
	if len(resolution.candidates) == 0 {
		return []string{}, nil
	}

	endpoint := "/select/logsql/field_values"
	if useStreamEndpoint {
		endpoint = "/select/logsql/stream_field_values"
	}

	seen := make(map[string]struct{}, 16)
	values := make([]string, 0, 16)
	for _, candidate := range resolution.candidates {
		queryParams := url.Values{}
		for key, items := range params {
			for _, item := range items {
				queryParams.Add(key, item)
			}
		}
		queryParams.Set("field", candidate)

		fieldValues, fieldErr := p.fetchVLFieldValues(ctx, endpoint, queryParams)
		if fieldErr != nil && endpoint == "/select/logsql/stream_field_values" && shouldFallbackToGenericMetadata(fieldErr) {
			endpoint = "/select/logsql/field_values"
			fieldValues, fieldErr = p.fetchVLFieldValues(ctx, endpoint, queryParams)
		}
		if fieldErr != nil {
			return nil, fieldErr
		}
		for _, value := range fieldValues {
			if _, ok := seen[value]; ok {
				continue
			}
			seen[value] = struct{}{}
			values = append(values, value)
		}
	}
	sort.Strings(values)
	return values, nil
}

func (p *Proxy) shouldRefreshLabelsInBackground(remaining, ttl time.Duration) bool {
	if remaining <= 0 || ttl <= 0 {
		return false
	}
	threshold := (ttl * 4) / 5
	if threshold <= 0 {
		threshold = ttl / 2
	}
	return remaining <= threshold
}

func (p *Proxy) requestEndsNearNow(r *http.Request) bool {
	if p == nil || r == nil || p.recentTailRefreshWindow <= 0 {
		return false
	}
	endRaw := strings.TrimSpace(firstNonEmpty(r.FormValue("end"), r.FormValue("to")))
	if endRaw == "" {
		// Loki defaults to "now" when end is omitted.
		return true
	}
	endNs, ok := parseLokiTimeToUnixNano(endRaw)
	if !ok {
		return false
	}
	nowNs := time.Now().UnixNano()
	if endNs > nowNs {
		endNs = nowNs
	}
	return nowNs-endNs <= p.recentTailRefreshWindow.Nanoseconds()
}

func (p *Proxy) shouldBypassRecentTailCache(endpoint string, remaining time.Duration, r *http.Request) bool {
	if p == nil || !p.recentTailRefreshEnabled {
		return false
	}
	ttl := CacheTTLs[endpoint]
	if ttl <= 0 || remaining <= 0 {
		return false
	}
	cacheAge := ttl - remaining
	if cacheAge < p.recentTailRefreshMaxStaleness {
		return false
	}
	return p.requestEndsNearNow(r)
}

func (p *Proxy) labelBackgroundTimeout() time.Duration {
	if p.client != nil && p.client.Timeout > 0 && p.client.Timeout < 10*time.Second {
		return p.client.Timeout
	}
	return 10 * time.Second
}

func (p *Proxy) refreshLabelsCacheAsync(orgID, cacheKey, rawQuery, start, end, search string) {
	refreshKey := "refresh:labels:" + cacheKey
	go func() {
		_, err, _ := p.labelRefreshGroup.Do(refreshKey, func() (interface{}, error) {
			ctx, cancel := context.WithTimeout(context.Background(), p.labelBackgroundTimeout())
			defer cancel()
			if orgID != "" {
				ctx = context.WithValue(ctx, orgIDKey, orgID)
			}

			params := url.Values{}
			if strings.TrimSpace(rawQuery) != "" {
				translated, terr := p.translateQuery(defaultQuery(rawQuery))
				if terr == nil {
					params.Set("query", translated)
				} else {
					params.Set("query", "*")
				}
			} else {
				params.Set("query", "*")
			}
			if strings.TrimSpace(start) != "" {
				params.Set("start", start)
			}
			if strings.TrimSpace(end) != "" {
				params.Set("end", end)
			}
			if search = strings.TrimSpace(search); search != "" && p.supportsMetadataSubstringFilter() {
				params.Set("q", search)
				params.Set("filter", "substring")
			}

			labels, fetchErr := p.fetchPreferredLabelNames(ctx, params)
			if fetchErr != nil {
				return nil, fetchErr
			}

			filtered := make([]string, 0, len(labels))
			for _, v := range labels {
				if isVLInternalField(v) {
					continue
				}
				filtered = append(filtered, v)
			}
			labels = p.labelTranslator.TranslateLabelsList(filtered)
			labels = appendSyntheticLabels(labels)
			p.cache.SetWithTTL(cacheKey, lokiLabelsResponse(labels), CacheTTLs["labels"])
			return nil, nil
		})
		if err != nil {
			p.log.Debug("background labels refresh failed", "cache_key", cacheKey, "error", err)
		}
	}()
}

func (p *Proxy) refreshLabelValuesCacheAsync(orgID, cacheKey, labelName, rawQuery, start, end, limit, search string) {
	refreshKey := "refresh:label_values:" + cacheKey
	go func() {
		_, err, _ := p.labelRefreshGroup.Do(refreshKey, func() (interface{}, error) {
			ctx, cancel := context.WithTimeout(context.Background(), p.labelBackgroundTimeout())
			defer cancel()
			if orgID != "" {
				ctx = context.WithValue(ctx, orgIDKey, orgID)
			}

			var (
				values   []string
				fetchErr error
			)

			if labelName == "service_name" {
				values, fetchErr = p.serviceNameValues(ctx, rawQuery, start, end)
			} else {
				params := url.Values{}
				if strings.TrimSpace(rawQuery) != "" {
					translated, terr := p.translateQuery(defaultQuery(rawQuery))
					if terr == nil {
						params.Set("query", translated)
					} else {
						params.Set("query", "*")
					}
				} else {
					params.Set("query", "*")
				}
				if strings.TrimSpace(start) != "" {
					params.Set("start", start)
				}
				if strings.TrimSpace(end) != "" {
					params.Set("end", end)
				}
				if strings.TrimSpace(limit) != "" {
					params.Set("limit", limit)
				}
				if search = strings.TrimSpace(search); search != "" && p.supportsMetadataSubstringFilter() {
					params.Set("q", search)
					params.Set("filter", "substring")
				}
				values, fetchErr = p.fetchPreferredLabelValues(ctx, labelName, params)
			}
			if fetchErr != nil {
				return nil, fetchErr
			}

			p.updateLabelValuesIndex(orgID, labelName, values)
			if p.labelValuesBrowseMode(rawQuery) {
				if indexedValues, ok := p.selectLabelValuesFromIndex(orgID, labelName, "", 0, p.defaultLabelValuesLimit(limit)); ok {
					values = indexedValues
				}
			}
			p.cache.SetWithTTL(cacheKey, lokiLabelsResponse(values), CacheTTLs["label_values"])
			return nil, nil
		})
		if err != nil {
			p.log.Debug("background label values refresh failed", "cache_key", cacheKey, "label", labelName, "error", err)
		}
	}()
}

func (p *Proxy) recordPatternSnapshotEntry(cacheKey string, payload []byte, now time.Time) {
	if !p.patternsEnabled {
		return
	}
	patternCount := patternCountFromPayload(payload)
	if patternCount > 0 {
		p.metrics.RecordPatternsStored(patternCount)
	}
	if strings.TrimSpace(p.patternsPersistPath) == "" {
		return
	}
	copied := append([]byte(nil), payload...)
	p.patternsSnapshotMu.Lock()
	if existing, ok := p.patternsSnapshotEntries[cacheKey]; ok {
		if bytes.Equal(existing.Value, copied) {
			if existing.UpdatedAtUnixNano < now.UnixNano() {
				existing.UpdatedAtUnixNano = now.UnixNano()
				p.patternsSnapshotEntries[cacheKey] = existing
			}
			p.updatePatternSnapshotMetricsLocked()
			p.patternsSnapshotMu.Unlock()
			return
		}
		p.patternsSnapshotPatternCount -= int64(existing.PatternCount)
		p.patternsSnapshotPayloadBytes -= int64(len(existing.Value))
	}
	p.patternsSnapshotEntries[cacheKey] = patternSnapshotEntry{
		Value:             copied,
		UpdatedAtUnixNano: now.UnixNano(),
		PatternCount:      patternCount,
	}
	p.patternsSnapshotPatternCount += int64(patternCount)
	p.patternsSnapshotPayloadBytes += int64(len(copied))
	droppedEntries, _ := p.compactPatternSnapshotEntriesLocked()
	p.updatePatternSnapshotMetricsLocked()
	p.patternsSnapshotMu.Unlock()
	p.patternsPersistDirty.Store(true)
	if droppedEntries > 0 {
		p.recordPatternSnapshotDedup(patternDedupSourceMemory, "update", droppedEntries)
	}
}

func patternCountFromPayload(payload []byte) int {
	if len(payload) == 0 {
		return 0
	}
	var resp patternsResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		return 0
	}
	return len(resp.Data)
}

func patternCountFromSnapshot(snapshot patternsSnapshot) int {
	total := 0
	for key, entry := range snapshot.EntriesByKey {
		if strings.TrimSpace(key) == "" || len(entry.Value) == 0 {
			continue
		}
		if entry.PatternCount > 0 {
			total += entry.PatternCount
			continue
		}
		total += patternCountFromPayload(entry.Value)
	}
	return total
}

func (p *Proxy) updatePatternSnapshotMetricsLocked() {
	if p == nil || p.metrics == nil {
		return
	}
	p.metrics.SetPatternsInMemory(
		int(p.patternsSnapshotPatternCount),
		len(p.patternsSnapshotEntries),
		p.patternsSnapshotPayloadBytes,
	)
}

func (p *Proxy) buildPatternsSnapshot(now time.Time) patternsSnapshot {
	p.patternsSnapshotMu.RLock()
	defer p.patternsSnapshotMu.RUnlock()

	entries := make(map[string]patternSnapshotEntry, len(p.patternsSnapshotEntries))
	for key, entry := range p.patternsSnapshotEntries {
		if strings.TrimSpace(key) == "" || len(entry.Value) == 0 {
			continue
		}
		patternCount := entry.PatternCount
		if patternCount <= 0 {
			patternCount = patternCountFromPayload(entry.Value)
		}
		entries[key] = patternSnapshotEntry{
			Value:             append([]byte(nil), entry.Value...),
			UpdatedAtUnixNano: entry.UpdatedAtUnixNano,
			PatternCount:      patternCount,
		}
	}

	return patternsSnapshot{
		Version:         1,
		SavedAtUnixNano: now.UnixNano(),
		EntriesByKey:    entries,
	}
}

func (p *Proxy) applyPatternsSnapshot(snapshot patternsSnapshot, source string) (int, int) {
	if p.cache == nil {
		return 0, 0
	}

	nowUnix := time.Now().UTC().UnixNano()
	appliedEntries := 0
	appliedPatterns := 0

	p.patternsSnapshotMu.Lock()

	for key, incoming := range snapshot.EntriesByKey {
		if strings.TrimSpace(key) == "" || len(incoming.Value) == 0 {
			continue
		}
		if incoming.UpdatedAtUnixNano <= 0 {
			incoming.UpdatedAtUnixNano = nowUnix
		}
		if existing, ok := p.patternsSnapshotEntries[key]; ok && existing.UpdatedAtUnixNano >= incoming.UpdatedAtUnixNano {
			continue
		}
		incomingPatternCount := incoming.PatternCount
		if incomingPatternCount <= 0 {
			incomingPatternCount = patternCountFromPayload(incoming.Value)
		}
		copied := append([]byte(nil), incoming.Value...)
		if existing, ok := p.patternsSnapshotEntries[key]; ok {
			p.patternsSnapshotPatternCount -= int64(existing.PatternCount)
			p.patternsSnapshotPayloadBytes -= int64(len(existing.Value))
		}
		p.patternsSnapshotEntries[key] = patternSnapshotEntry{
			Value:             copied,
			UpdatedAtUnixNano: incoming.UpdatedAtUnixNano,
			PatternCount:      incomingPatternCount,
		}
		p.patternsSnapshotPatternCount += int64(incomingPatternCount)
		p.patternsSnapshotPayloadBytes += int64(len(copied))
		p.cache.SetWithTTL(key, copied, patternsCacheRetention)
		appliedEntries++
		appliedPatterns += incomingPatternCount
	}
	droppedEntries, _ := p.compactPatternSnapshotEntriesLocked()
	p.updatePatternSnapshotMetricsLocked()
	p.patternsSnapshotMu.Unlock()

	if appliedEntries > 0 || droppedEntries > 0 {
		p.patternsPersistDirty.Store(true)
	}
	if droppedEntries > 0 {
		p.recordPatternSnapshotDedup(source, "merge", droppedEntries)
	}
	return appliedEntries, appliedPatterns
}

func patternSnapshotIdentityFromCacheKey(cacheKey string) string {
	cacheKey = strings.TrimSpace(cacheKey)
	if cacheKey == "" {
		return ""
	}
	parts := strings.SplitN(cacheKey, ":", 3)
	if len(parts) != 3 || parts[0] != "patterns" {
		return ""
	}
	orgID := strings.TrimSpace(parts[1])
	if orgID == "" {
		return ""
	}
	params, err := url.ParseQuery(parts[2])
	if err != nil {
		return ""
	}
	query := patternScopeQuery(params.Get("query"))
	if strings.TrimSpace(query) == "" {
		return ""
	}
	return orgID + "\x00" + query
}

func betterPatternSnapshotEntry(current, incoming patternSnapshotEntry, currentKey, incomingKey string) bool {
	if incoming.UpdatedAtUnixNano != current.UpdatedAtUnixNano {
		return incoming.UpdatedAtUnixNano > current.UpdatedAtUnixNano
	}
	if incoming.PatternCount != current.PatternCount {
		return incoming.PatternCount > current.PatternCount
	}
	if len(incoming.Value) != len(current.Value) {
		return len(incoming.Value) > len(current.Value)
	}
	// Deterministic tie-breaker.
	return incomingKey > currentKey
}

func (p *Proxy) compactPatternSnapshotEntriesLocked() (int, int) {
	if p == nil || len(p.patternsSnapshotEntries) <= 1 {
		return 0, 0
	}

	keysByIdentity := make(map[string][]string, len(p.patternsSnapshotEntries))
	for key := range p.patternsSnapshotEntries {
		identity := patternSnapshotIdentityFromCacheKey(key)
		if identity == "" {
			continue
		}
		keysByIdentity[identity] = append(keysByIdentity[identity], key)
	}

	dropped := make(map[string]struct{})
	for _, keys := range keysByIdentity {
		if len(keys) <= 1 {
			continue
		}
		keepKeys := make([]string, 0, len(keys))
		for _, key := range keys {
			entry := p.patternsSnapshotEntries[key]
			matched := -1
			for i, keepKey := range keepKeys {
				keepEntry := p.patternsSnapshotEntries[keepKey]
				if bytes.Equal(keepEntry.Value, entry.Value) {
					matched = i
					break
				}
			}
			if matched == -1 {
				keepKeys = append(keepKeys, key)
				continue
			}
			existingKey := keepKeys[matched]
			existingEntry := p.patternsSnapshotEntries[existingKey]
			if betterPatternSnapshotEntry(existingEntry, entry, existingKey, key) {
				keepKeys[matched] = key
				dropped[existingKey] = struct{}{}
			} else {
				dropped[key] = struct{}{}
			}
		}
	}

	if len(dropped) == 0 {
		return 0, 0
	}

	beforePatterns := p.patternsSnapshotPatternCount
	droppedEntries := 0
	for key := range dropped {
		if _, ok := p.patternsSnapshotEntries[key]; !ok {
			continue
		}
		delete(p.patternsSnapshotEntries, key)
		droppedEntries++
		if p.cache != nil {
			p.cache.Invalidate(key)
		}
	}
	p.recomputePatternSnapshotStatsLocked()
	droppedPatterns := int(beforePatterns - p.patternsSnapshotPatternCount)
	if droppedPatterns < 0 {
		droppedPatterns = 0
	}
	return droppedEntries, droppedPatterns
}

func (p *Proxy) compactPatternsSnapshot(source, reason string) (int, int) {
	p.patternsSnapshotMu.Lock()
	droppedEntries, droppedPatterns := p.compactPatternSnapshotEntriesLocked()
	p.updatePatternSnapshotMetricsLocked()
	p.patternsSnapshotMu.Unlock()
	if droppedEntries > 0 {
		p.patternsPersistDirty.Store(true)
		p.recordPatternSnapshotDedup(source, reason, droppedEntries)
	}
	return droppedEntries, droppedPatterns
}

func (p *Proxy) recordPatternSnapshotDedup(source, reason string, droppedEntries int) {
	if droppedEntries <= 0 {
		return
	}
	source = strings.ToLower(strings.TrimSpace(source))
	switch source {
	case patternDedupSourceDisk, patternDedupSourcePeer:
	default:
		source = patternDedupSourceMemory
	}
	if p.metrics != nil {
		p.metrics.RecordPatternsDeduplicated(source, droppedEntries)
	}
	p.log.Info(
		"patterns snapshot deduplicated",
		"component", "proxy",
		"source", source,
		"reason", reason,
		"duplicates_removed", droppedEntries,
	)
}

func (p *Proxy) recomputePatternSnapshotStatsLocked() {
	var patterns int64
	var payloadBytes int64
	for key, entry := range p.patternsSnapshotEntries {
		if strings.TrimSpace(key) == "" || len(entry.Value) == 0 {
			delete(p.patternsSnapshotEntries, key)
			continue
		}
		patternCount := entry.PatternCount
		if patternCount <= 0 {
			patternCount = patternCountFromPayload(entry.Value)
			entry.PatternCount = patternCount
			p.patternsSnapshotEntries[key] = entry
		}
		patterns += int64(patternCount)
		payloadBytes += int64(len(entry.Value))
	}
	p.patternsSnapshotPatternCount = patterns
	p.patternsSnapshotPayloadBytes = payloadBytes
}

func (p *Proxy) persistPatternsNow(reason string) error {
	if !p.patternsEnabled || strings.TrimSpace(p.patternsPersistPath) == "" {
		return nil
	}
	if reason == "periodic" && !p.patternsPersistDirty.Load() {
		return nil
	}

	snapshot := p.buildPatternsSnapshot(time.Now().UTC())
	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal patterns snapshot: %w", err)
	}

	path := p.patternsPersistPath
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create patterns snapshot directory: %w", err)
	}
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("write patterns snapshot temp file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename patterns snapshot temp file: %w", err)
	}

	if p.cache != nil {
		ttl := p.patternsStartupStale * 3
		minTTL := p.patternsPersistInterval * 2
		if ttl < minTTL {
			ttl = minTTL
		}
		if ttl <= 0 {
			ttl = 5 * time.Minute
		}
		p.cache.SetWithTTL(patternsSnapshotCacheKey, data, ttl)
	}

	entryCount := len(snapshot.EntriesByKey)
	patternCount := patternCountFromSnapshot(snapshot)
	p.log.Info(
		"patterns snapshot persisted",
		"reason", reason,
		"path", path,
		"entries", entryCount,
		"patterns", patternCount,
		"bytes", len(data),
	)
	p.metrics.SetPatternsPersistedDiskBytes(int64(len(data)))
	p.patternsPersistDirty.Store(false)
	return nil
}

func (p *Proxy) restorePatternsFromDisk() (bool, int64, error) {
	if !p.patternsEnabled || strings.TrimSpace(p.patternsPersistPath) == "" {
		return false, 0, nil
	}
	data, err := os.ReadFile(p.patternsPersistPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, 0, nil
		}
		return false, 0, fmt.Errorf("read patterns snapshot: %w", err)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return false, 0, nil
	}

	var snapshot patternsSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return false, 0, fmt.Errorf("decode patterns snapshot: %w", err)
	}
	if snapshot.Version != 1 {
		return false, 0, fmt.Errorf("unsupported patterns snapshot version: %d", snapshot.Version)
	}
	if snapshot.SavedAtUnixNano <= 0 {
		return false, 0, fmt.Errorf("invalid patterns snapshot timestamp: %d", snapshot.SavedAtUnixNano)
	}

	appliedEntries, appliedPatterns := p.applyPatternsSnapshot(snapshot, patternDedupSourceDisk)
	p.metrics.RecordPatternsRestoredFromDisk(appliedPatterns, appliedEntries)
	p.metrics.SetPatternsPersistedDiskBytes(int64(len(data)))
	if p.cache != nil {
		ttl := p.patternsStartupStale * 3
		if ttl <= 0 {
			ttl = 5 * time.Minute
		}
		p.cache.SetWithTTL(patternsSnapshotCacheKey, data, ttl)
	}
	p.log.Info(
		"patterns snapshot restored from disk",
		"path", p.patternsPersistPath,
		"saved_at", time.Unix(0, snapshot.SavedAtUnixNano).UTC().Format(time.RFC3339Nano),
		"entries_applied", appliedEntries,
		"patterns_applied", appliedPatterns,
		"bytes", len(data),
	)
	return true, snapshot.SavedAtUnixNano, nil
}

func (p *Proxy) fetchPatternsSnapshotFromPeer(peerAddr string, timeout time.Duration) (*patternsSnapshot, error) {
	if strings.TrimSpace(peerAddr) == "" {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	endpoint := fmt.Sprintf("http://%s/_cache/get?key=%s", peerAddr, url.QueryEscape(patternsSnapshotCacheKey))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	if p.peerAuthToken != "" {
		req.Header.Set("X-Cache-Auth", p.peerAuthToken)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("peer %s status %d: %s", peerAddr, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var snapshot patternsSnapshot
	if err := json.Unmarshal(body, &snapshot); err != nil {
		return nil, err
	}
	if snapshot.Version != 1 || snapshot.SavedAtUnixNano <= 0 {
		return nil, fmt.Errorf("invalid patterns snapshot metadata from peer %s", peerAddr)
	}
	return &snapshot, nil
}

func (p *Proxy) restorePatternsFromPeers(minSavedAt int64) (bool, int64, error) {
	if !p.patternsEnabled || p.peerCache == nil {
		return false, 0, nil
	}
	peers := p.peerCache.Peers()
	if len(peers) == 0 {
		return false, 0, nil
	}

	timeout := p.patternsPeerWarmTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	type peerResult struct {
		snapshot *patternsSnapshot
		err      error
		peer     string
	}
	resCh := make(chan peerResult, len(peers))
	for _, peerAddr := range peers {
		peerAddr := strings.TrimSpace(peerAddr)
		if peerAddr == "" {
			continue
		}
		go func(addr string) {
			snapshot, err := p.fetchPatternsSnapshotFromPeer(addr, timeout)
			resCh <- peerResult{snapshot: snapshot, err: err, peer: addr}
		}(peerAddr)
	}

	merged := patternsSnapshot{
		Version:         1,
		SavedAtUnixNano: time.Now().UTC().UnixNano(),
		EntriesByKey:    make(map[string]patternSnapshotEntry),
	}
	mergedSavedAt := minSavedAt

	p.patternsSnapshotMu.RLock()
	for key, entry := range p.patternsSnapshotEntries {
		merged.EntriesByKey[key] = patternSnapshotEntry{
			Value:             append([]byte(nil), entry.Value...),
			UpdatedAtUnixNano: entry.UpdatedAtUnixNano,
		}
	}
	p.patternsSnapshotMu.RUnlock()

	deadline := time.After(timeout)
	received := 0
	for received < len(peers) {
		select {
		case res := <-resCh:
			received++
			if res.err != nil {
				p.log.Debug("patterns peer warm fetch failed", "peer", res.peer, "error", res.err)
				continue
			}
			if res.snapshot == nil || len(res.snapshot.EntriesByKey) == 0 {
				continue
			}
			if res.snapshot.SavedAtUnixNano > mergedSavedAt {
				mergedSavedAt = res.snapshot.SavedAtUnixNano
			}
			for key, incoming := range res.snapshot.EntriesByKey {
				if strings.TrimSpace(key) == "" || len(incoming.Value) == 0 {
					continue
				}
				existing, ok := merged.EntriesByKey[key]
				if ok && existing.UpdatedAtUnixNano >= incoming.UpdatedAtUnixNano {
					continue
				}
				merged.EntriesByKey[key] = patternSnapshotEntry{
					Value:             append([]byte(nil), incoming.Value...),
					UpdatedAtUnixNano: incoming.UpdatedAtUnixNano,
				}
			}
		case <-deadline:
			received = len(peers)
		}
	}

	appliedEntries, appliedPatterns := p.applyPatternsSnapshot(merged, patternDedupSourcePeer)
	if appliedEntries == 0 || mergedSavedAt <= minSavedAt {
		return false, mergedSavedAt, nil
	}
	p.metrics.RecordPatternsRestoredFromPeers(appliedPatterns, appliedEntries)

	p.log.Info(
		"patterns snapshot warmed from peers",
		"saved_at", time.Unix(0, mergedSavedAt).UTC().Format(time.RFC3339Nano),
		"entries_applied", appliedEntries,
		"patterns_applied", appliedPatterns,
		"entries_updated", appliedEntries,
		"patterns_updated", appliedPatterns,
		"peers", len(peers),
	)
	return true, mergedSavedAt, nil
}

func (p *Proxy) warmPatternsOnStartup() {
	if !p.patternsEnabled || strings.TrimSpace(p.patternsPersistPath) == "" {
		return
	}
	p.patternsWarmReady.Store(false)
	defer p.patternsWarmReady.Store(true)

	var diskSavedAt int64
	loadedFromDisk, savedAt, err := p.restorePatternsFromDisk()
	if err != nil {
		p.log.Warn("patterns snapshot disk restore failed", "error", err)
	}
	if loadedFromDisk {
		diskSavedAt = savedAt
	}

	type peerWarmResult struct {
		ok      bool
		savedAt int64
		err     error
	}
	timeout := p.patternsPeerWarmTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	resCh := make(chan peerWarmResult, 1)
	go func() {
		ok, savedAt, peerErr := p.restorePatternsFromPeers(diskSavedAt)
		resCh <- peerWarmResult{ok: ok, savedAt: savedAt, err: peerErr}
	}()

	var res peerWarmResult
	select {
	case res = <-resCh:
	case <-time.After(timeout):
		p.log.Warn("patterns snapshot peer warm timed out", "timeout", timeout.String())
		res = peerWarmResult{ok: false}
	}
	if res.err != nil {
		p.log.Warn("patterns snapshot peer warm failed", "error", res.err)
	} else if res.ok {
		if persistErr := p.persistPatternsNow("startup_peer_warm"); persistErr != nil {
			p.log.Warn("patterns snapshot persistence after peer warm failed", "error", persistErr)
		}
	}
}

func (p *Proxy) startPatternsPersistenceLoop() {
	if !p.patternsEnabled || strings.TrimSpace(p.patternsPersistPath) == "" || p.patternsPersistInterval <= 0 {
		return
	}
	if p.patternsPersistStarted.Swap(true) {
		return
	}
	p.log.Info(
		"patterns snapshot backup enabled",
		"path", p.patternsPersistPath,
		"interval", p.patternsPersistInterval.String(),
	)

	go func() {
		defer close(p.patternsPersistDone)
		ticker := time.NewTicker(p.patternsPersistInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				p.compactPatternsSnapshot(patternDedupSourceMemory, "periodic")
				if err := p.persistPatternsNow("periodic"); err != nil {
					p.log.Warn("periodic patterns snapshot persistence failed", "error", err)
				}
			case <-p.patternsPersistStop:
				return
			}
		}
	}()
}

func (p *Proxy) stopPatternsPersistenceLoop(ctx context.Context) {
	if !p.patternsPersistStarted.Load() {
		return
	}
	select {
	case <-p.patternsPersistStop:
	default:
		close(p.patternsPersistStop)
	}

	if ctx == nil {
		<-p.patternsPersistDone
		return
	}
	select {
	case <-p.patternsPersistDone:
	case <-ctx.Done():
		p.log.Warn("timeout waiting for patterns persistence loop stop", "error", ctx.Err())
	}
}

func (p *Proxy) buildLabelValuesIndexSnapshot(now time.Time) labelValuesIndexSnapshot {
	p.labelValuesIndexMu.RLock()
	defer p.labelValuesIndexMu.RUnlock()

	states := make(map[string]map[string]labelValueIndexEntry, len(p.labelValuesIndex))
	for key, state := range p.labelValuesIndex {
		if state == nil || len(state.entries) == 0 {
			continue
		}
		entries := make(map[string]labelValueIndexEntry, len(state.entries))
		for value, entry := range state.entries {
			entries[value] = entry
		}
		states[key] = entries
	}

	return labelValuesIndexSnapshot{
		Version:         1,
		SavedAtUnixNano: now.UnixNano(),
		StatesByKey:     states,
	}
}

func (p *Proxy) applyLabelValuesIndexSnapshot(snapshot labelValuesIndexSnapshot) (states int, values int) {
	restored := make(map[string]*labelValuesIndexState, len(snapshot.StatesByKey))
	for key, entries := range snapshot.StatesByKey {
		if len(entries) == 0 {
			continue
		}
		copied := make(map[string]labelValueIndexEntry, len(entries))
		for value, entry := range entries {
			if strings.TrimSpace(value) == "" {
				continue
			}
			copied[value] = entry
			values++
		}
		if len(copied) == 0 {
			continue
		}
		restored[key] = &labelValuesIndexState{
			entries: copied,
			dirty:   true,
		}
		states++
	}

	p.labelValuesIndexMu.Lock()
	p.labelValuesIndex = restored
	p.labelValuesIndexMu.Unlock()
	p.labelValuesIndexPersistDirty.Store(false)
	return states, values
}

func (p *Proxy) persistLabelValuesIndexNow(reason string) error {
	if !p.labelValuesIndexedCache || strings.TrimSpace(p.labelValuesIndexPersistPath) == "" {
		return nil
	}
	if reason == "periodic" && !p.labelValuesIndexPersistDirty.Load() {
		return nil
	}

	snapshot := p.buildLabelValuesIndexSnapshot(time.Now().UTC())
	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal label index snapshot: %w", err)
	}

	path := p.labelValuesIndexPersistPath
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create label index snapshot directory: %w", err)
	}
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("write label index snapshot temp file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename label index snapshot temp file: %w", err)
	}

	if p.cache != nil {
		ttl := p.labelValuesIndexStartupStale * 3
		minTTL := p.labelValuesIndexPersistInterval * 2
		if ttl < minTTL {
			ttl = minTTL
		}
		if ttl <= 0 {
			ttl = 5 * time.Minute
		}
		p.cache.SetWithTTL(labelValuesIndexSnapshotCacheKey, data, ttl)
	}

	states, values := p.labelValuesIndexCardinality()
	if reason == "periodic" {
		p.log.Debug("label values index snapshot persisted", "path", path, "states", states, "values", values, "bytes", len(data))
	} else {
		p.log.Info("label values index snapshot persisted", "reason", reason, "path", path, "states", states, "values", values, "bytes", len(data))
	}
	p.labelValuesIndexPersistDirty.Store(false)
	return nil
}

func (p *Proxy) restoreLabelValuesIndexFromDisk() (bool, int64, error) {
	if !p.labelValuesIndexedCache || strings.TrimSpace(p.labelValuesIndexPersistPath) == "" {
		return false, 0, nil
	}

	data, err := os.ReadFile(p.labelValuesIndexPersistPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, 0, nil
		}
		return false, 0, fmt.Errorf("read label index snapshot: %w", err)
	}

	var snapshot labelValuesIndexSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return false, 0, fmt.Errorf("decode label index snapshot: %w", err)
	}
	if snapshot.Version != 1 {
		return false, 0, fmt.Errorf("unsupported label index snapshot version: %d", snapshot.Version)
	}
	if snapshot.SavedAtUnixNano <= 0 {
		return false, 0, fmt.Errorf("invalid label index snapshot timestamp: %d", snapshot.SavedAtUnixNano)
	}

	states, values := p.applyLabelValuesIndexSnapshot(snapshot)
	if p.cache != nil {
		ttl := p.labelValuesIndexStartupStale * 3
		if ttl <= 0 {
			ttl = 5 * time.Minute
		}
		p.cache.SetWithTTL(labelValuesIndexSnapshotCacheKey, data, ttl)
	}
	p.log.Info(
		"label values index restored from disk",
		"path", p.labelValuesIndexPersistPath,
		"saved_at", time.Unix(0, snapshot.SavedAtUnixNano).UTC().Format(time.RFC3339Nano),
		"states", states,
		"values", values,
	)
	return true, snapshot.SavedAtUnixNano, nil
}

func (p *Proxy) restoreLabelValuesIndexFromPeers(minSavedAt int64) (bool, int64, error) {
	if !p.labelValuesIndexedCache || p.cache == nil || p.peerCache == nil {
		return false, 0, nil
	}
	data, ok := p.cache.Get(labelValuesIndexSnapshotCacheKey)
	if !ok {
		return false, 0, nil
	}

	var snapshot labelValuesIndexSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return false, 0, fmt.Errorf("decode peer label index snapshot: %w", err)
	}
	if snapshot.Version != 1 || snapshot.SavedAtUnixNano <= 0 {
		return false, 0, fmt.Errorf("invalid peer label index snapshot metadata")
	}
	if snapshot.SavedAtUnixNano <= minSavedAt {
		return false, snapshot.SavedAtUnixNano, nil
	}

	states, values := p.applyLabelValuesIndexSnapshot(snapshot)
	p.log.Info(
		"label values index warmed from peers",
		"saved_at", time.Unix(0, snapshot.SavedAtUnixNano).UTC().Format(time.RFC3339Nano),
		"states", states,
		"values", values,
	)
	return true, snapshot.SavedAtUnixNano, nil
}

func (p *Proxy) warmLabelValuesIndexOnStartup() {
	p.labelValuesIndexWarmReady.Store(false)
	defer p.labelValuesIndexWarmReady.Store(true)

	var diskSavedAt int64
	loadedFromDisk, savedAt, err := p.restoreLabelValuesIndexFromDisk()
	if err != nil {
		p.log.Warn("label values index disk restore failed", "error", err)
	}
	if loadedFromDisk {
		diskSavedAt = savedAt
	}

	diskFresh := loadedFromDisk && time.Since(time.Unix(0, diskSavedAt)) <= p.labelValuesIndexStartupStale
	if !diskFresh {
		if p.cache != nil {
			p.cache.Invalidate(labelValuesIndexSnapshotCacheKey)
		}
		type peerWarmResult struct {
			ok      bool
			savedAt int64
			err     error
		}
		timeout := p.labelValuesIndexPeerWarmTimeout
		if timeout <= 0 {
			timeout = 5 * time.Second
		}
		resCh := make(chan peerWarmResult, 1)
		go func() {
			ok, savedAt, peerErr := p.restoreLabelValuesIndexFromPeers(diskSavedAt)
			resCh <- peerWarmResult{ok: ok, savedAt: savedAt, err: peerErr}
		}()

		var res peerWarmResult
		select {
		case res = <-resCh:
		case <-time.After(timeout):
			p.log.Warn("label values index peer warm timed out", "timeout", timeout.String())
			res = peerWarmResult{ok: false}
		}
		if res.err != nil {
			p.log.Warn("label values index peer warm failed", "error", res.err)
		} else if res.ok {
			diskSavedAt = res.savedAt
			if persistErr := p.persistLabelValuesIndexNow("startup_peer_warm"); persistErr != nil {
				p.log.Warn("label values index persistence after peer warm failed", "error", persistErr)
			}
		}
	}
}

func (p *Proxy) startLabelValuesIndexPersistenceLoop() {
	if !p.labelValuesIndexedCache || strings.TrimSpace(p.labelValuesIndexPersistPath) == "" || p.labelValuesIndexPersistInterval <= 0 {
		return
	}
	if p.labelValuesIndexPersistStarted.Swap(true) {
		return
	}
	go func() {
		defer close(p.labelValuesIndexPersistDone)
		ticker := time.NewTicker(p.labelValuesIndexPersistInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := p.persistLabelValuesIndexNow("periodic"); err != nil {
					p.log.Warn("periodic label values index persistence failed", "error", err)
				}
			case <-p.labelValuesIndexPersistStop:
				return
			}
		}
	}()
}

func (p *Proxy) stopLabelValuesIndexPersistenceLoop(ctx context.Context) {
	if !p.labelValuesIndexPersistStarted.Load() {
		return
	}
	select {
	case <-p.labelValuesIndexPersistStop:
	default:
		close(p.labelValuesIndexPersistStop)
	}

	if ctx == nil {
		<-p.labelValuesIndexPersistDone
		return
	}
	select {
	case <-p.labelValuesIndexPersistDone:
	case <-ctx.Done():
		p.log.Warn("timeout waiting for label values persistence loop stop", "error", ctx.Err())
	}
}

func (p *Proxy) labelValuesIndexCardinality() (states int, values int) {
	p.labelValuesIndexMu.RLock()
	defer p.labelValuesIndexMu.RUnlock()
	for _, state := range p.labelValuesIndex {
		if state == nil || len(state.entries) == 0 {
			continue
		}
		states++
		values += len(state.entries)
	}
	return states, values
}

func (p *Proxy) labelValuesBrowseMode(rawQuery string) bool {
	trimmed := strings.TrimSpace(rawQuery)
	return trimmed == "" || trimmed == "*"
}

func (p *Proxy) defaultLabelValuesLimit(limitRaw string) int {
	if strings.TrimSpace(limitRaw) != "" {
		limit := parsePositiveInt(limitRaw, p.labelValuesHotLimit)
		if limit > maxLimitValue {
			limit = maxLimitValue
		}
		return limit
	}
	limit := p.labelValuesHotLimit
	if limit <= 0 {
		limit = 200
	}
	if limit > maxLimitValue {
		limit = maxLimitValue
	}
	return limit
}

func parsePositiveInt(raw string, fallback int) int {
	value := strings.TrimSpace(raw)
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func parseNonNegativeInt(raw string, fallback int) int {
	value := strings.TrimSpace(raw)
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < 0 {
		return fallback
	}
	return n
}

func normalizeLabelValueSearch(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

func labelValuesIndexKey(orgID, labelName string) string {
	return orgID + "|" + strings.ToLower(strings.TrimSpace(labelName))
}

func labelValueIndexLess(aName string, a labelValueIndexEntry, bName string, b labelValueIndexEntry) bool {
	if a.SeenCount != b.SeenCount {
		return a.SeenCount > b.SeenCount
	}
	if a.LastSeen != b.LastSeen {
		return a.LastSeen > b.LastSeen
	}
	return aName < bName
}

func (p *Proxy) labelValueIndexEnsureOrderedLocked(index *labelValuesIndexState) {
	if index == nil {
		return
	}
	if !index.dirty && len(index.ordered) > 0 {
		return
	}
	ordered := make([]string, 0, len(index.entries))
	for value := range index.entries {
		ordered = append(ordered, value)
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		left := index.entries[ordered[i]]
		right := index.entries[ordered[j]]
		return labelValueIndexLess(ordered[i], left, ordered[j], right)
	})
	index.ordered = ordered
	index.dirty = false
}

func (p *Proxy) updateLabelValuesIndex(orgID, labelName string, values []string) {
	if !p.labelValuesIndexedCache || len(values) == 0 {
		return
	}
	key := labelValuesIndexKey(orgID, labelName)
	now := time.Now().UnixNano()

	p.labelValuesIndexMu.Lock()
	defer p.labelValuesIndexMu.Unlock()

	index, ok := p.labelValuesIndex[key]
	structuralChange := false
	if !ok {
		index = &labelValuesIndexState{entries: make(map[string]labelValueIndexEntry, len(values))}
		p.labelValuesIndex[key] = index
		structuralChange = true
	}

	for _, value := range values {
		if _, exists := index.entries[value]; !exists {
			structuralChange = true
		}
		entry := index.entries[value]
		if entry.SeenCount < ^uint32(0) {
			entry.SeenCount++
		}
		entry.LastSeen = now
		index.entries[value] = entry
	}
	index.dirty = true

	maxEntries := p.labelValuesIndexMaxEntries
	if maxEntries <= 0 || len(index.entries) <= maxEntries {
		if structuralChange {
			p.labelValuesIndexPersistDirty.Store(true)
		}
		return
	}
	p.labelValueIndexEnsureOrderedLocked(index)
	if len(index.ordered) <= maxEntries {
		if structuralChange {
			p.labelValuesIndexPersistDirty.Store(true)
		}
		return
	}
	keep := index.ordered[:maxEntries]
	pruned := make(map[string]labelValueIndexEntry, len(keep))
	for _, value := range keep {
		pruned[value] = index.entries[value]
	}
	index.entries = pruned
	index.ordered = append([]string(nil), keep...)
	index.dirty = false
	structuralChange = true
	if structuralChange {
		p.labelValuesIndexPersistDirty.Store(true)
	}
}

func (p *Proxy) selectLabelValuesFromIndex(orgID, labelName, search string, offset, limit int) ([]string, bool) {
	if !p.labelValuesIndexedCache {
		return nil, false
	}
	if limit <= 0 {
		limit = p.labelValuesHotLimit
	}
	if limit <= 0 {
		limit = 200
	}
	if limit > maxLimitValue {
		limit = maxLimitValue
	}
	if offset < 0 {
		offset = 0
	}

	key := labelValuesIndexKey(orgID, labelName)
	search = normalizeLabelValueSearch(search)

	p.labelValuesIndexMu.Lock()
	defer p.labelValuesIndexMu.Unlock()

	index, ok := p.labelValuesIndex[key]
	if !ok || len(index.entries) == 0 {
		return nil, false
	}
	p.labelValueIndexEnsureOrderedLocked(index)
	if len(index.ordered) == 0 {
		return nil, false
	}

	// Keep preallocation fixed-size so allocation cannot scale with request input.
	values := make([]string, 0, maxUserDrivenSlicePrealloc)
	seen := 0
	for _, candidate := range index.ordered {
		if search != "" && !strings.Contains(strings.ToLower(candidate), search) {
			continue
		}
		if seen < offset {
			seen++
			continue
		}
		values = append(values, candidate)
		if len(values) >= limit {
			break
		}
	}
	return values, true
}

func selectLabelValuesWindow(values []string, search string, offset, limit int) []string {
	if limit <= 0 {
		limit = maxLimitValue
	}
	if limit > maxLimitValue {
		limit = maxLimitValue
	}
	if offset < 0 {
		offset = 0
	}

	search = normalizeLabelValueSearch(search)
	// Keep preallocation fixed-size so allocation cannot scale with request input.
	out := make([]string, 0, maxUserDrivenSlicePrealloc)
	seen := 0
	for _, value := range values {
		if search != "" && !strings.Contains(strings.ToLower(value), search) {
			continue
		}
		if seen < offset {
			seen++
			continue
		}
		out = append(out, value)
		if len(out) >= limit {
			break
		}
	}
	return out
}

func (p *Proxy) setJSONCacheWithTTL(cacheKey string, ttl time.Duration, value interface{}) {
	if p == nil || p.cache == nil {
		return
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return
	}
	p.cache.SetWithTTL(cacheKey, encoded, ttl)
}

func (p *Proxy) refreshDetectedFieldsCacheAsync(orgID, cacheKey, query, start, end string, lineLimit int) {
	refreshKey := "refresh:detected_fields:" + cacheKey
	go func() {
		_, err, _ := p.labelRefreshGroup.Do(refreshKey, func() (interface{}, error) {
			ctx, cancel := context.WithTimeout(context.Background(), p.labelBackgroundTimeout())
			defer cancel()
			if orgID != "" {
				ctx = context.WithValue(ctx, orgIDKey, orgID)
			}
			fields, _, detectErr := p.detectFields(ctx, query, start, end, lineLimit)
			if detectErr != nil {
				return nil, detectErr
			}
			payload := map[string]interface{}{
				"status": "success",
				"data":   fields,
				"fields": fields,
				"limit":  lineLimit,
			}
			p.setJSONCacheWithTTL(cacheKey, CacheTTLs["detected_fields"], payload)
			return nil, nil
		})
		if err != nil {
			p.log.Debug("background detected_fields refresh failed", "cache_key", cacheKey, "error", err)
		}
	}()
}

func (p *Proxy) refreshDetectedLabelsCacheAsync(orgID, cacheKey, query, start, end string, lineLimit int) {
	refreshKey := "refresh:detected_labels:" + cacheKey
	go func() {
		_, err, _ := p.labelRefreshGroup.Do(refreshKey, func() (interface{}, error) {
			ctx, cancel := context.WithTimeout(context.Background(), p.labelBackgroundTimeout())
			defer cancel()
			if orgID != "" {
				ctx = context.WithValue(ctx, orgIDKey, orgID)
			}
			labels, _, detectErr := p.detectLabels(ctx, query, start, end, lineLimit)
			if detectErr != nil {
				return nil, detectErr
			}
			payload := map[string]interface{}{
				"status":         "success",
				"data":           labels,
				"detectedLabels": labels,
				"limit":          lineLimit,
			}
			p.setJSONCacheWithTTL(cacheKey, CacheTTLs["detected_labels"], payload)
			return nil, nil
		})
		if err != nil {
			p.log.Debug("background detected_labels refresh failed", "cache_key", cacheKey, "error", err)
		}
	}()
}

func (p *Proxy) refreshDetectedFieldValuesCacheAsync(orgID, cacheKey, fieldName, query, start, end string, lineLimit int) {
	refreshKey := "refresh:detected_field_values:" + cacheKey
	go func() {
		_, err, _ := p.labelRefreshGroup.Do(refreshKey, func() (interface{}, error) {
			ctx, cancel := context.WithTimeout(context.Background(), p.labelBackgroundTimeout())
			defer cancel()
			if orgID != "" {
				ctx = context.WithValue(ctx, orgIDKey, orgID)
			}

			var (
				values  []string
				errVals error
			)
			if nativeField, ok, resolveErr := p.resolveNativeDetectedField(ctx, query, start, end, fieldName); resolveErr == nil && ok {
				values, errVals = p.fetchNativeFieldValues(ctx, query, start, end, nativeField, lineLimit)
				if errVals == nil && len(values) == 0 {
					// Keep Drilldown UX non-empty for synthetic/derived labels when native values are empty.
					values = nil
				}
			}
			if values == nil && errVals == nil {
				_, fieldValues, detectErr := p.detectFields(ctx, query, start, end, lineLimit)
				if detectErr != nil {
					return nil, detectErr
				}
				values = fieldValues[fieldName]
				if values == nil && fieldName == "level" {
					values = fieldValues["detected_level"]
				}
			}
			if len(values) == 0 {
				values = p.detectedLabelValuesForField(ctx, fieldName, query, start, end, lineLimit)
			}
			if errVals != nil {
				return nil, errVals
			}
			if values == nil {
				values = []string{}
			}

			payload := map[string]interface{}{
				"status": "success",
				"data":   values,
				"values": values,
				"limit":  lineLimit,
			}
			p.setJSONCacheWithTTL(cacheKey, CacheTTLs["detected_field_values"], payload)
			return nil, nil
		})
		if err != nil {
			p.log.Debug("background detected_field_values refresh failed", "cache_key", cacheKey, "field", fieldName, "error", err)
		}
	}()
}

// handleLabels returns label names.
// Loki: GET /loki/api/v1/labels?start=...&end=...
// VL:   GET /select/logsql/stream_field_names?query=*&start=...&end=...
//
//	fallback /select/logsql/field_names for older backends
func (p *Proxy) handleLabels(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "labels", p.handleLabels) {
		return
	}
	orgID := r.Header.Get("X-Scope-OrgID")
	cacheKey := "labels:" + orgID + ":" + r.URL.RawQuery

	if cached, remaining, ok := p.cache.GetWithTTL(cacheKey); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(cached)
		p.metrics.RecordRequest("labels", http.StatusOK, time.Since(start))
		p.metrics.RecordCacheHit()
		if p.shouldRefreshLabelsInBackground(remaining, CacheTTLs["labels"]) {
			search := strings.TrimSpace(r.FormValue("search"))
			if search == "" {
				search = strings.TrimSpace(r.FormValue("q"))
			}
			p.refreshLabelsCacheAsync(orgID, cacheKey, r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), search)
		}
		return
	}
	p.metrics.RecordCacheMiss()
	r = withOrgID(r)

	params := url.Values{}
	// Forward the query param if provided (Loki uses it to scope label suggestions)
	if q := r.FormValue("query"); q != "" {
		translated, terr := p.translateQuery(defaultQuery(q))
		if terr == nil {
			params.Set("query", translated)
		} else {
			params.Set("query", "*")
		}
	} else {
		params.Set("query", "*")
	}
	if s := r.FormValue("start"); s != "" {
		params.Set("start", s)
	}
	if e := r.FormValue("end"); e != "" {
		params.Set("end", e)
	}
	search := strings.TrimSpace(r.FormValue("search"))
	if search == "" {
		search = strings.TrimSpace(r.FormValue("q"))
	}
	if search != "" && p.supportsMetadataSubstringFilter() {
		params.Set("q", search)
		params.Set("filter", "substring")
	}

	labels, err := p.fetchPreferredLabelNamesCached(r.Context(), params)
	if err != nil {
		status := statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
		p.metrics.RecordRequest("labels", status, time.Since(start))
		return
	}

	filtered := make([]string, 0, len(labels))
	for _, v := range labels {
		// Filter out VL internal fields — Loki doesn't expose these
		if isVLInternalField(v) {
			continue
		}
		filtered = append(filtered, v)
	}

	// Apply label name translation (e.g., dots → underscores)
	labels = p.labelTranslator.TranslateLabelsList(filtered)
	labels = appendSyntheticLabels(labels)

	result := lokiLabelsResponse(labels)
	p.cache.SetWithTTL(cacheKey, result, CacheTTLs["labels"])
	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
	p.metrics.RecordRequest("labels", http.StatusOK, time.Since(start))
}

// handleLabelValues returns values for a specific label.
// Loki: GET /loki/api/v1/label/{name}/values?start=...&end=...
// VL:   GET /select/logsql/stream_field_values?query=*&field={name}&start=...&end=...
//
//	fallback /select/logsql/field_values for older backends
func (p *Proxy) handleLabelValues(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	// Extract label name from URL: /loki/api/v1/label/{name}/values
	path := r.URL.Path
	parts := strings.Split(path, "/")
	if len(parts) < 7 || parts[6] != "values" {
		p.writeError(w, http.StatusBadRequest, "invalid label values URL")
		return
	}
	labelName := parts[5]
	if strings.Contains(r.Header.Get("X-Scope-OrgID"), "|") && labelName == "__tenant_id__" {
		values := splitMultiTenantOrgIDs(r.Header.Get("X-Scope-OrgID"))
		result := lokiLabelsResponse(values)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(result)
		p.metrics.RecordRequest("label_values", http.StatusOK, time.Since(start))
		return
	}
	if p.handleMultiTenantFanout(w, r, "label_values", p.handleLabelValues) {
		return
	}
	orgID := r.Header.Get("X-Scope-OrgID")
	cacheKey := "label_values:" + orgID + ":" + labelName + ":" + r.URL.RawQuery
	rawQuery := r.FormValue("query")
	rawLimit := r.FormValue("limit")
	rawOffset := r.FormValue("offset")
	search := r.FormValue("search")
	if strings.TrimSpace(search) == "" {
		search = r.FormValue("q")
	}
	offset := parseNonNegativeInt(rawOffset, 0)
	limit := p.defaultLabelValuesLimit(rawLimit)
	if rawLimit == "" && !p.labelValuesBrowseMode(rawQuery) {
		limit = maxLimitValue
	}

	if cached, remaining, ok := p.cache.GetWithTTL(cacheKey); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(cached)
		p.metrics.RecordRequest("label_values", http.StatusOK, time.Since(start))
		p.metrics.RecordCacheHit()
		if p.shouldRefreshLabelsInBackground(remaining, CacheTTLs["label_values"]) {
			p.refreshLabelValuesCacheAsync(
				orgID,
				cacheKey,
				labelName,
				r.FormValue("query"),
				r.FormValue("start"),
				r.FormValue("end"),
				r.FormValue("limit"),
				search,
			)
		}
		return
	}
	p.metrics.RecordCacheMiss()

	if p.labelValuesBrowseMode(rawQuery) {
		if indexedValues, ok := p.selectLabelValuesFromIndex(orgID, labelName, search, offset, limit); ok {
			result := lokiLabelsResponse(indexedValues)
			p.cache.SetWithTTL(cacheKey, result, CacheTTLs["label_values"])
			w.Header().Set("Content-Type", "application/json")
			w.Write(result)
			p.metrics.RecordRequest("label_values", http.StatusOK, time.Since(start))
			return
		}
	}

	if labelName == "service_name" {
		values, err := p.serviceNameValues(r.Context(), r.FormValue("query"), r.FormValue("start"), r.FormValue("end"))
		if err != nil {
			status := statusFromUpstreamErr(err)
			p.writeError(w, status, err.Error())
			p.metrics.RecordRequest("label_values", status, time.Since(start))
			return
		}
		p.updateLabelValuesIndex(orgID, labelName, values)
		if p.labelValuesBrowseMode(rawQuery) {
			if indexedValues, ok := p.selectLabelValuesFromIndex(orgID, labelName, search, offset, limit); ok {
				values = indexedValues
			} else {
				values = selectLabelValuesWindow(values, search, offset, limit)
			}
		}
		result := lokiLabelsResponse(values)
		p.cache.SetWithTTL(cacheKey, result, CacheTTLs["label_values"])
		w.Header().Set("Content-Type", "application/json")
		w.Write(result)
		p.metrics.RecordRequest("label_values", http.StatusOK, time.Since(start))
		return
	}

	params := url.Values{}
	if q := r.FormValue("query"); q != "" {
		translated, terr := p.translateQuery(defaultQuery(q))
		if terr == nil {
			params.Set("query", translated)
		} else {
			params.Set("query", "*")
		}
	} else {
		params.Set("query", "*")
	}
	if s := r.FormValue("start"); s != "" {
		params.Set("start", s)
	}
	if e := r.FormValue("end"); e != "" {
		params.Set("end", e)
	}
	if l := r.FormValue("limit"); l != "" {
		params.Set("limit", l)
	}
	if search != "" && p.supportsMetadataSubstringFilter() {
		params.Set("q", search)
		params.Set("filter", "substring")
	}

	values, err := p.fetchPreferredLabelValues(r.Context(), labelName, params)
	if err != nil {
		status := statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
		p.metrics.RecordRequest("label_values", status, time.Since(start))
		return
	}

	p.updateLabelValuesIndex(orgID, labelName, values)
	if p.labelValuesBrowseMode(rawQuery) {
		if indexedValues, ok := p.selectLabelValuesFromIndex(orgID, labelName, search, offset, limit); ok {
			values = indexedValues
		} else {
			values = selectLabelValuesWindow(values, search, offset, limit)
		}
	} else if search != "" || offset > 0 || rawLimit != "" {
		values = selectLabelValuesWindow(values, search, offset, limit)
	}

	result := lokiLabelsResponse(values)
	p.cache.SetWithTTL(cacheKey, result, CacheTTLs["label_values"])
	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
	p.metrics.RecordRequest("label_values", http.StatusOK, time.Since(start))
}

// handleSeries returns stream/series metadata.
// Loki: GET /loki/api/v1/series?match[]={...}&start=...&end=...
// VL:   GET /select/logsql/streams?query={...}&start=...&end=...
func (p *Proxy) handleSeries(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "series", p.handleSeries) {
		return
	}
	r = withOrgID(r)
	matchQueries := r.Form["match[]"]
	query := "*"
	if len(matchQueries) > 0 {
		translated, err := p.translateQuery(matchQueries[0])
		if err == nil {
			query = translated
		}
	}

	params := url.Values{}
	params.Set("query", query)
	if s := r.FormValue("start"); s != "" {
		params.Set("start", s)
	}
	if e := r.FormValue("end"); e != "" {
		params.Set("end", e)
	}

	resp, err := p.vlGet(r.Context(), "/select/logsql/streams", params)
	if err != nil {
		status := statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
		p.metrics.RecordRequest("series", status, time.Since(start))
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	// Propagate VL error status
	if resp.StatusCode >= 400 {
		p.writeError(w, resp.StatusCode, string(body))
		p.metrics.RecordRequest("series", resp.StatusCode, time.Since(start))
		return
	}

	// VL returns: {"values": [{"value": "{stream}", "hits": N}, ...]}
	// Loki expects: {"status": "success", "data": [{"label": "value", ...}, ...]}
	var vlResp struct {
		Values []struct {
			Value string `json:"value"`
			Hits  int64  `json:"hits"`
		} `json:"values"`
	}
	json.Unmarshal(body, &vlResp)

	series := make([]map[string]string, 0, len(vlResp.Values))
	for _, v := range vlResp.Values {
		labels := parseStreamLabels(v.Value)
		if len(labels) > 0 {
			labels = p.labelTranslator.TranslateLabelsMap(labels)
			ensureSyntheticServiceName(labels)
			series = append(series, labels)
		}
	}

	result, _ := json.Marshal(map[string]interface{}{
		"status": "success",
		"data":   series,
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
	p.metrics.RecordRequest("series", http.StatusOK, time.Since(start))
}

// handleIndexStats returns index statistics via VL /select/logsql/hits.
// Loki: GET /loki/api/v1/index/stats?query={...}&start=...&end=...
// Response: {"streams":N, "chunks":N, "entries":N, "bytes":N}
func (p *Proxy) handleIndexStats(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "index_stats", p.handleIndexStats) {
		return
	}
	r = withOrgID(r)
	query := r.FormValue("query")
	if query == "" {
		query = "*"
	}
	logsqlQuery, _ := p.translateQuery(query)

	params := url.Values{}
	params.Set("query", logsqlQuery)
	if s := r.FormValue("start"); s != "" {
		params.Set("start", formatVLTimestamp(s))
	}
	if e := r.FormValue("end"); e != "" {
		params.Set("end", formatVLTimestamp(e))
	}
	// VL v1.49+ requires step for hits — use large step to get one bucket (total count)
	if params.Get("step") == "" {
		params.Set("step", "1h")
	}

	resp, err := p.vlGet(r.Context(), "/select/logsql/hits", params)
	if err != nil {
		// Fallback to zeros on error
		p.writeJSON(w, map[string]interface{}{"streams": 0, "chunks": 0, "bytes": 0, "entries": 0})
		p.metrics.RecordRequest("index_stats", http.StatusOK, time.Since(start))
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	entries := sumHitsValues(body)
	hits := parseHits(body)
	streams := len(hits.Hits)
	if streams == 0 && entries > 0 {
		streams = 1
	}
	result, _ := json.Marshal(map[string]interface{}{
		"streams": streams,
		"chunks":  streams,
		"bytes":   entries * 100, // approximate — VL doesn't expose bytes
		"entries": entries,
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
	p.metrics.RecordRequest("index_stats", http.StatusOK, time.Since(start))
}

func (p *Proxy) resolveTargetLabelFields(ctx context.Context, targetLabels string, params url.Values) []string {
	fields := splitTargetLabels(targetLabels)
	if len(fields) == 0 {
		return nil
	}

	var (
		available []string
		fetchErr  error
	)

	needsInventory := false
	if p.labelTranslator != nil && p.labelTranslator.style == LabelStyleUnderscores {
		for _, field := range fields {
			translated := p.labelTranslator.ToVL(field)
			if translated == strings.TrimSpace(field) && strings.Contains(field, "_") {
				needsInventory = true
				break
			}
		}
	}

	if needsInventory {
		lookup := url.Values{}
		for _, key := range []string{"query", "start", "end"} {
			if value := strings.TrimSpace(params.Get(key)); value != "" {
				lookup.Set(key, value)
			}
		}
		available, fetchErr = p.fetchPreferredLabelNamesCached(ctx, lookup)
		if fetchErr != nil {
			p.log.Debug("target label inventory lookup failed; falling back to direct mapping", "error", fetchErr)
		}
	}

	if len(available) == 0 {
		available = p.snapshotDeclaredLabelFields()
	}
	available = appendUniqueStrings(available, p.snapshotDeclaredLabelFields()...)

	resolved := make([]string, 0, len(fields))
	for _, field := range fields {
		name := strings.TrimSpace(field)
		if name == "" {
			continue
		}
		mapped := p.labelTranslator.ToVL(name)
		if len(available) > 0 {
			candidates := p.labelTranslator.ResolveLabelCandidates(name, available)
			if len(candidates.candidates) > 0 {
				if candidates.ambiguous {
					p.log.Debug("ambiguous label alias for targetLabels; using first candidate",
						"label", name,
						"candidate_count", len(candidates.candidates),
					)
				}
				mapped = candidates.candidates[0]
			}
		}
		resolved = appendUniqueStrings(resolved, mapped)
	}
	return resolved
}

// handleVolume returns volume data via VL /select/logsql/hits with field grouping.
// Loki: GET /loki/api/v1/index/volume?query={...}&start=...&end=...
// Response: {"status":"success","data":{"resultType":"vector","result":[{"metric":{...},"value":[ts,"count"]}]}}
func (p *Proxy) handleVolume(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "volume", p.handleVolume) {
		return
	}
	r = withOrgID(r)
	orgID := r.Header.Get("X-Scope-OrgID")
	cacheKey := "volume:" + orgID + ":" + r.URL.RawQuery
	if cached, remaining, ok := p.cache.GetWithTTL(cacheKey); ok {
		if !p.shouldBypassRecentTailCache("volume", remaining, r) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(cached)
			p.metrics.RecordRequest("volume", http.StatusOK, time.Since(start))
			p.metrics.RecordCacheHit()
			if p.shouldRefreshLabelsInBackground(remaining, CacheTTLs["volume"]) {
				p.refreshVolumeCacheAsync(orgID, cacheKey, r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), r.FormValue("targetLabels"))
			}
			return
		}
	}
	p.metrics.RecordCacheMiss()

	result, err := p.computeVolumeResult(r.Context(), r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), r.FormValue("targetLabels"))
	if err != nil {
		result = map[string]interface{}{
			"status": "success",
			"data":   map[string]interface{}{"resultType": "vector", "result": []interface{}{}},
		}
	}
	p.setJSONCacheWithTTL(cacheKey, CacheTTLs["volume"], result)
	p.writeJSON(w, result)
	p.metrics.RecordRequest("volume", http.StatusOK, time.Since(start))
}

func (p *Proxy) computeVolumeResult(ctx context.Context, query, start, end, targetLabels string) (map[string]interface{}, error) {
	if query == "" {
		query = "*"
	}
	if targetLabels == "" {
		targetLabels = inferPrimaryTargetLabel(query)
	}
	if usesDerivedVolumeLabels(targetLabels) {
		result, err := p.volumeByDerivedLabels(ctx, query, start, end, targetLabels, "")
		if err == nil {
			return result, nil
		}
	}
	logsqlQuery, _ := p.translateQuery(query)

	params := url.Values{}
	params.Set("query", logsqlQuery)
	if s := start; s != "" {
		params.Set("start", formatVLTimestamp(s))
	}
	if e := end; e != "" {
		params.Set("end", formatVLTimestamp(e))
	}
	// VL v1.49+ requires step for hits
	if params.Get("step") == "" {
		params.Set("step", "1h")
	}
	// Request field-level grouping
	if targetLabels != "" {
		mappedFields := p.resolveTargetLabelFields(ctx, targetLabels, params)
		if len(mappedFields) > 0 {
			for _, field := range mappedFields {
				params.Add("field", field)
			}
		}
	}

	resp, err := p.vlGet(ctx, "/select/logsql/hits", params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	return p.hitsToVolumeVector(body), nil
}

func (p *Proxy) refreshVolumeCacheAsync(orgID, cacheKey, rawQuery, start, end, targetLabels string) {
	refreshKey := "refresh:volume:" + cacheKey
	go func() {
		_, err, _ := p.labelRefreshGroup.Do(refreshKey, func() (interface{}, error) {
			ctx, cancel := context.WithTimeout(context.Background(), p.labelBackgroundTimeout())
			defer cancel()
			if orgID != "" {
				ctx = context.WithValue(ctx, orgIDKey, orgID)
			}
			result, err := p.computeVolumeResult(ctx, rawQuery, start, end, targetLabels)
			if err == nil {
				p.setJSONCacheWithTTL(cacheKey, CacheTTLs["volume"], result)
			}
			return nil, err
		})
		if err != nil {
			p.log.Debug("background volume refresh failed", "cache_key", cacheKey, "error", err)
		}
	}()
}

// handleVolumeRange returns volume range data via VL /select/logsql/hits with step.
// Loki: GET /loki/api/v1/index/volume_range?query={...}&start=...&end=...&step=60
// Response: {"status":"success","data":{"resultType":"matrix","result":[{"metric":{...},"values":[[ts,"count"],...]}]}}
func (p *Proxy) handleVolumeRange(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "volume_range", p.handleVolumeRange) {
		return
	}
	r = withOrgID(r)
	orgID := r.Header.Get("X-Scope-OrgID")
	cacheKey := "volume_range:" + orgID + ":" + r.URL.RawQuery
	if cached, remaining, ok := p.cache.GetWithTTL(cacheKey); ok {
		if !p.shouldBypassRecentTailCache("volume_range", remaining, r) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(cached)
			p.metrics.RecordRequest("volume_range", http.StatusOK, time.Since(start))
			p.metrics.RecordCacheHit()
			if p.shouldRefreshLabelsInBackground(remaining, CacheTTLs["volume_range"]) {
				p.refreshVolumeRangeCacheAsync(orgID, cacheKey, r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), r.FormValue("step"), r.FormValue("targetLabels"))
			}
			return
		}
	}
	p.metrics.RecordCacheMiss()

	result, err := p.computeVolumeRangeResult(r.Context(), r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), r.FormValue("step"), r.FormValue("targetLabels"))
	if err != nil {
		result = map[string]interface{}{
			"status": "success",
			"data":   map[string]interface{}{"resultType": "matrix", "result": []interface{}{}},
		}
	}
	p.setJSONCacheWithTTL(cacheKey, CacheTTLs["volume_range"], result)
	p.writeJSON(w, result)
	p.metrics.RecordRequest("volume_range", http.StatusOK, time.Since(start))
}

func (p *Proxy) computeVolumeRangeResult(ctx context.Context, query, start, end, step, targetLabels string) (map[string]interface{}, error) {
	if query == "" {
		query = "*"
	}
	if targetLabels == "" {
		targetLabels = inferPrimaryTargetLabel(query)
	}
	if usesDerivedVolumeLabels(targetLabels) {
		result, err := p.volumeByDerivedLabels(ctx, query, start, end, targetLabels, step)
		if err == nil {
			return result, nil
		}
	}
	logsqlQuery, _ := p.translateQuery(query)

	params := url.Values{}
	params.Set("query", logsqlQuery+" | sort by (_time desc)")
	if s := start; s != "" {
		params.Set("start", formatVLTimestamp(s))
	}
	if e := end; e != "" {
		params.Set("end", formatVLTimestamp(e))
	}
	if step != "" {
		params.Set("step", formatVLStep(step))
	}
	// Forward targetLabels for field-level grouping (same as /volume)
	if targetLabels != "" {
		mappedFields := p.resolveTargetLabelFields(ctx, targetLabels, params)
		if len(mappedFields) > 0 {
			for _, field := range mappedFields {
				params.Add("field", field)
			}
		}
	}

	resp, err := p.vlGet(ctx, "/select/logsql/hits", params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	return p.hitsToVolumeMatrix(body, start, end, step), nil
}

func (p *Proxy) refreshVolumeRangeCacheAsync(orgID, cacheKey, rawQuery, start, end, step, targetLabels string) {
	refreshKey := "refresh:volume_range:" + cacheKey
	go func() {
		_, err, _ := p.labelRefreshGroup.Do(refreshKey, func() (interface{}, error) {
			ctx, cancel := context.WithTimeout(context.Background(), p.labelBackgroundTimeout())
			defer cancel()
			if orgID != "" {
				ctx = context.WithValue(ctx, orgIDKey, orgID)
			}
			result, err := p.computeVolumeRangeResult(ctx, rawQuery, start, end, step, targetLabels)
			if err == nil {
				p.setJSONCacheWithTTL(cacheKey, CacheTTLs["volume_range"], result)
			}
			return nil, err
		})
		if err != nil {
			p.log.Debug("background volume_range refresh failed", "cache_key", cacheKey, "error", err)
		}
	}()
}

// handleDetectedFields returns detected field names.
func (p *Proxy) handleDetectedFields(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "detected_fields", p.handleDetectedFields) {
		return
	}
	orgID := r.Header.Get("X-Scope-OrgID")
	cacheKey := "detected_fields:" + orgID + ":" + r.URL.RawQuery
	if cached, remaining, ok := p.cache.GetWithTTL(cacheKey); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(cached)
		p.metrics.RecordRequest("detected_fields", http.StatusOK, time.Since(start))
		p.metrics.RecordCacheHit()
		if p.shouldRefreshLabelsInBackground(remaining, CacheTTLs["detected_fields"]) {
			p.refreshDetectedFieldsCacheAsync(orgID, cacheKey, r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), parseDetectedLineLimit(r))
		}
		return
	}
	p.metrics.RecordCacheMiss()

	r = withOrgID(r)
	lineLimit := parseDetectedLineLimit(r)
	fields, _, err := p.detectFields(r.Context(), r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), lineLimit)
	if err != nil {
		payload := map[string]interface{}{
			"status": "success",
			"data":   []interface{}{},
			"fields": []interface{}{},
			"limit":  lineLimit,
		}
		p.writeJSON(w, payload)
		p.metrics.RecordRequest("detected_fields", http.StatusOK, time.Since(start))
		return
	}
	payload := map[string]interface{}{
		"status": "success",
		"data":   fields,
		"fields": fields,
		"limit":  lineLimit,
	}
	p.setJSONCacheWithTTL(cacheKey, CacheTTLs["detected_fields"], payload)
	p.writeJSON(w, payload)
	p.metrics.RecordRequest("detected_fields", http.StatusOK, time.Since(start))
}

// handleDetectedFieldValues returns values for a detected field.
// Loki: GET /loki/api/v1/detected_field/{name}/values?query=...
// Response: {"values":["debug","info","warn","error"],"limit":1000}
func (p *Proxy) handleDetectedFieldValues(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "detected_field_values", p.handleDetectedFieldValues) {
		return
	}
	orgID := r.Header.Get("X-Scope-OrgID")
	r = withOrgID(r)
	// Extract field name from URL: /loki/api/v1/detected_field/{name}/values
	path := r.URL.Path
	parts := strings.Split(path, "/")
	fieldName := ""
	for i, part := range parts {
		if part == "detected_field" && i+1 < len(parts) {
			fieldName = parts[i+1]
			break
		}
	}
	if fieldName == "" {
		p.writeError(w, http.StatusBadRequest, "missing field name in URL")
		return
	}

	lineLimit := parseDetectedLineLimit(r)
	cacheKey := "detected_field_values:" + orgID + ":" + fieldName + ":" + r.URL.RawQuery
	if cached, remaining, ok := p.cache.GetWithTTL(cacheKey); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(cached)
		p.metrics.RecordRequest("detected_field_values", http.StatusOK, time.Since(start))
		p.metrics.RecordCacheHit()
		if p.shouldRefreshLabelsInBackground(remaining, CacheTTLs["detected_field_values"]) {
			p.refreshDetectedFieldValuesCacheAsync(orgID, cacheKey, fieldName, r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), lineLimit)
		}
		return
	}
	p.metrics.RecordCacheMiss()

	if fieldName == "service_name" {
		if values, err := p.serviceNameValues(r.Context(), r.FormValue("query"), r.FormValue("start"), r.FormValue("end")); err == nil && len(values) > 0 {
			payload := map[string]interface{}{
				"status": "success",
				"data":   values,
				"values": values,
				"limit":  lineLimit,
			}
			p.setJSONCacheWithTTL(cacheKey, CacheTTLs["detected_field_values"], payload)
			p.writeJSON(w, payload)
			p.metrics.RecordRequest("detected_field_values", http.StatusOK, time.Since(start))
			return
		}
	}

	if nativeField, ok, err := p.resolveNativeDetectedField(r.Context(), r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), fieldName); err == nil && ok {
		if values, valuesErr := p.fetchNativeFieldValues(r.Context(), r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), nativeField, lineLimit); valuesErr == nil {
			if len(values) > 0 {
				payload := map[string]interface{}{
					"status": "success",
					"data":   values,
					"values": values,
					"limit":  lineLimit,
				}
				p.setJSONCacheWithTTL(cacheKey, CacheTTLs["detected_field_values"], payload)
				p.writeJSON(w, payload)
				p.metrics.RecordRequest("detected_field_values", http.StatusOK, time.Since(start))
				return
			}
		}
	}

	_, fieldValues, err := p.detectFields(r.Context(), r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), lineLimit)
	if err != nil {
		payload := map[string]interface{}{
			"status": "success",
			"data":   []string{},
			"values": []string{},
			"limit":  lineLimit,
		}
		p.writeJSON(w, payload)
		p.metrics.RecordRequest("detected_field_values", http.StatusOK, time.Since(start))
		return
	}
	values := fieldValues[fieldName]
	if values == nil && fieldName == "level" {
		values = fieldValues["detected_level"]
	}
	if len(values) == 0 {
		values = p.detectedLabelValuesForField(r.Context(), fieldName, r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), lineLimit)
	}
	if values == nil {
		values = []string{}
	}

	payload := map[string]interface{}{
		"status": "success",
		"data":   values,
		"values": values,
		"limit":  lineLimit,
	}
	p.setJSONCacheWithTTL(cacheKey, CacheTTLs["detected_field_values"], payload)
	p.writeJSON(w, payload)
	p.metrics.RecordRequest("detected_field_values", http.StatusOK, time.Since(start))
}

func (p *Proxy) detectedLabelValuesForField(ctx context.Context, fieldName, query, start, end string, lineLimit int) []string {
	_, summaries, err := p.detectLabels(ctx, query, start, end, lineLimit)
	if err != nil || summaries == nil {
		return nil
	}

	available := make([]string, 0, len(summaries))
	for name := range summaries {
		available = append(available, name)
	}
	resolution := p.labelTranslator.ResolveLabelCandidates(fieldName, available)
	if len(resolution.candidates) == 0 {
		resolution = fieldResolution{candidates: []string{fieldName}}
	}

	valueSet := make(map[string]struct{})
	for _, candidate := range resolution.candidates {
		summary := summaries[candidate]
		if summary == nil {
			continue
		}
		for value := range summary.values {
			if strings.TrimSpace(value) == "" {
				continue
			}
			valueSet[value] = struct{}{}
		}
	}
	if len(valueSet) == 0 {
		return nil
	}

	values := make([]string, 0, len(valueSet))
	for value := range valueSet {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

// handlePatterns returns log patterns for Grafana Logs Drilldown.
func (p *Proxy) handlePatterns(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if !p.patternsEnabled {
		p.writeError(w, http.StatusNotFound, "patterns endpoint is disabled")
		p.metrics.RecordRequest("patterns", http.StatusNotFound, time.Since(start))
		return
	}
	if p.handleMultiTenantFanout(w, r, "patterns", p.handlePatterns) {
		return
	}
	r = withOrgID(r)
	orgID := r.Header.Get("X-Scope-OrgID")
	query := patternScopeQuery(r.FormValue("query"))
	startParam := strings.TrimSpace(firstNonEmpty(r.FormValue("start"), r.FormValue("from")))
	endParam := strings.TrimSpace(firstNonEmpty(r.FormValue("end"), r.FormValue("to")))
	requestStepParam := strings.TrimSpace(r.FormValue("step"))
	stepParam := requestStepParam
	if stepParam == "" {
		stepParam = derivePatternStep(startParam, endParam)
	}
	patternLimit := parsePatternLimit(r.FormValue("limit"))
	sourceLimit := parsePatternSourceLineLimit(r.FormValue("line_limit"), startParam, endParam, stepParam)
	cacheLookupKeys := []string{p.patternsAutodetectCacheKey(orgID, query, startParam, endParam, requestStepParam)}
	if requestStepParam == "" {
		if derived := p.patternsAutodetectCacheKey(orgID, query, startParam, endParam, stepParam); derived != "" {
			cacheLookupKeys = append(cacheLookupKeys, derived)
		}
	}

	cacheWriteKey := cacheLookupKeys[0]
	if cacheWriteKey == "" {
		cacheWriteKey = "patterns:" + orgID + ":" + r.URL.RawQuery
	}

	for _, key := range cacheLookupKeys {
		if key == "" {
			continue
		}
		if cached, ok := p.cache.Get(key); ok {
			body := p.applyCustomPatternsToPayload(cached, startParam, endParam, stepParam, patternLimit)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(body)
			p.metrics.RecordRequest("patterns", http.StatusOK, time.Since(start))
			p.metrics.RecordCacheHit()
			return
		}
	}

	if fallbackCached, ok := p.cache.Get(cacheWriteKey); ok {
		body := p.applyCustomPatternsToPayload(fallbackCached, startParam, endParam, stepParam, patternLimit)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
		p.metrics.RecordRequest("patterns", http.StatusOK, time.Since(start))
		p.metrics.RecordCacheHit()
		return
	}
	p.metrics.RecordCacheMiss()
	if requestStepParam == "" {
		// When request step is omitted, keep cache compatibility with warm payloads
		// generated by query/query_range autodetect path (empty step key).
		cacheWriteKey = p.patternsAutodetectCacheKey(orgID, query, startParam, endParam, "")
		if cacheWriteKey == "" {
			cacheWriteKey = "patterns:" + orgID + ":" + r.URL.RawQuery
		}
	}
	derivedStepCacheKey := ""
	if requestStepParam == "" && stepParam != "" {
		derivedStepCacheKey = p.patternsAutodetectCacheKey(orgID, query, startParam, endParam, stepParam)
	}

	logsqlQuery, err := p.translatePatternQuery(query)
	if err != nil {
		p.writeJSON(w, map[string]interface{}{
			"status": "success",
			"data":   []interface{}{},
		})
		p.metrics.RecordRequest("patterns", http.StatusOK, time.Since(start))
		return
	}

	params := url.Values{}
	params.Set("query", logsqlQuery)
	if s := startParam; s != "" {
		params.Set("start", formatVLTimestamp(s))
	}
	if e := endParam; e != "" {
		params.Set("end", formatVLTimestamp(e))
	}
	if s := strings.TrimSpace(stepParam); s != "" {
		params.Set("step", formatVLStep(s))
	}
	params.Set("limit", strconv.Itoa(sourceLimit))

	fetchPatterns := func(endpoint string) ([]patternResultEntry, bool) {
		fetchFromParams := func(queryParams url.Values) ([]patternResultEntry, bool) {
			resp, err := p.vlPost(r.Context(), endpoint, queryParams)
			if err != nil {
				return nil, false
			}
			defer resp.Body.Close()
			if resp.StatusCode >= http.StatusBadRequest {
				return nil, false
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return nil, false
			}
			extracted := extractLogPatterns(body, stepParam, patternLimit)
			if len(extracted) == 0 {
				return nil, false
			}
			entries := patternResultEntriesFromMaps(extracted)
			if len(entries) == 0 {
				return nil, false
			}
			return entries, true
		}

		if endpoint == "/select/logsql/query" {
			denseWindowing := p.supportsDensePatternWindowingForRequest(r)
			startNs, endNs, splitInterval, perWindowLimit, ok := patternWindowedSamplingConfig(startParam, endParam, stepParam, sourceLimit, denseWindowing)
			if ok {
				windows := splitQueryRangeWindowsWithOptions(startNs, endNs, splitInterval, "backward", true)
				if len(windows) > 1 {
					windowEntries, windowSuccesses := p.fetchPatternsFromWindows(r, logsqlQuery, sourceLimit, perWindowLimit, windows, stepParam, patternLimit)
					if shouldAcceptWindowedPatternResults(windowSuccesses, len(windows), denseWindowing) && len(windowEntries) > 0 {
						// Prefer distributed stratified windows so dense ranges keep full-range
						// visibility instead of collapsing to a recent-tail sample.
						return windowEntries, true
					}
				}
			}
		}

		entries, ok := fetchFromParams(params)
		if !ok {
			return nil, false
		}
		return entries, true
	}
	params.Set("limit", strconv.Itoa(patternBackendQueryLimit(startParam, endParam, stepParam, patternLimit)))

	// Use /query with stratified windowing to preserve full selected-range buckets.
	entries, _ := fetchPatterns("/select/logsql/query")
	if entries == nil {
		entries = []patternResultEntry{}
	}
	p.metrics.RecordPatternsDetected(len(entries))
	entries = p.prependCustomPatternEntries(entries, startParam, stepParam, patternLimit)
	entries = fillPatternSamplesAcrossRequestedRange(entries, startParam, endParam, stepParam)
	resultBody, err := json.Marshal(patternsResponse{
		Status: "success",
		Data:   entries,
	})
	if err != nil {
		p.writeJSON(w, map[string]interface{}{"status": "success", "data": []interface{}{}})
		p.metrics.RecordRequest("patterns", http.StatusOK, time.Since(start))
		return
	}
	// Avoid sticky empty results: first-call empty probes should not poison long-lived pattern cache entries.
	if len(entries) > 0 {
		now := time.Now().UTC()
		p.cache.SetWithTTL(cacheWriteKey, resultBody, patternsCacheRetention)
		p.recordPatternSnapshotEntry(cacheWriteKey, resultBody, now)
		if derivedStepCacheKey != "" && derivedStepCacheKey != cacheWriteKey {
			p.cache.SetWithTTL(derivedStepCacheKey, resultBody, patternsCacheRetention)
			p.recordPatternSnapshotEntry(derivedStepCacheKey, resultBody, now)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(resultBody)
	p.metrics.RecordRequest("patterns", http.StatusOK, time.Since(start))
}

func (p *Proxy) fetchPatternsFromWindows(
	r *http.Request,
	logsqlQuery string,
	sourceLimit int,
	perWindowLimit int,
	windows []queryRangeWindow,
	stepParam string,
	patternLimit int,
) ([]patternResultEntry, int) {
	if len(windows) == 0 {
		return nil, 0
	}
	if perWindowLimit <= 0 {
		perWindowLimit = 1
	}
	effectiveLimit := perWindowLimit
	if sourceLimit > 0 && sourceLimit < effectiveLimit {
		effectiveLimit = sourceLimit
	}

	maxParallel := min(8, max(1, p.queryRangeWindowParallelLimit()))
	if maxParallel > len(windows) {
		maxParallel = len(windows)
	}

	sem := make(chan struct{}, maxParallel)
	var wg sync.WaitGroup

	baseParams := url.Values{}
	baseParams.Set("query", logsqlQuery)
	if s := strings.TrimSpace(stepParam); s != "" {
		baseParams.Set("step", formatVLStep(s))
	}

	collected := make([]patternResultEntry, 0, len(windows))
	successes := 0
	var mu sync.Mutex

	for _, window := range windows {
		window := window
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-r.Context().Done():
				return
			case sem <- struct{}{}:
			}
			defer func() { <-sem }()

			params := cloneURLValues(baseParams)
			params.Set("start", strconv.FormatInt(window.startNs, 10))
			params.Set("end", strconv.FormatInt(window.endNs, 10))
			params.Set("limit", strconv.Itoa(effectiveLimit))
			resp, err := p.vlPost(r.Context(), "/select/logsql/query", params)
			if err != nil {
				p.log.Debug(
					"patterns window fetch failed",
					"start_ns", window.startNs,
					"end_ns", window.endNs,
					"error", err,
				)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode >= http.StatusBadRequest {
				p.log.Debug(
					"patterns window fetch non-success status",
					"start_ns", window.startNs,
					"end_ns", window.endNs,
					"status_code", resp.StatusCode,
				)
				return
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return
			}
			extracted := extractLogPatterns(body, stepParam, patternLimit)
			if len(extracted) == 0 {
				return
			}
			entries := patternResultEntriesFromMaps(extracted)
			if len(entries) == 0 {
				return
			}

			mu.Lock()
			successes++
			collected = mergePatternResultEntries(collected, entries)
			mu.Unlock()
		}()
	}
	wg.Wait()

	if len(collected) == 0 {
		return nil, successes
	}
	return collected, successes
}

func patternScopeQuery(query string) string {
	query = strings.TrimSpace(query)
	if query == "" {
		return "*"
	}
	query = strings.TrimSpace(streamSelectorPrefix(query))
	if query == "" {
		return "*"
	}
	if query == "*" || strings.HasPrefix(query, "{") {
		return query
	}
	// Drilldown can emit LogsQL-like top-level filters (`field:=value`) without
	// braces. Keep those as-is and avoid wrapping into LogQL selector syntax.
	if looksLikeLogsQLQuery(query) {
		return query
	}
	return normalizeBareSelectorQuery(query)
}

func looksLikeLogsQLQuery(query string) bool {
	query = strings.TrimSpace(query)
	return strings.Contains(query, ":=") ||
		strings.Contains(query, ":~") ||
		strings.Contains(query, ":>") ||
		strings.Contains(query, ":<")
}

func (p *Proxy) translatePatternQuery(query string) (string, error) {
	scoped := patternScopeQuery(query)
	if scoped == "*" {
		return scoped, nil
	}
	if looksLikeLogsQLQuery(scoped) {
		return scoped, nil
	}
	return p.translateQuery(scoped)
}

func parsePatternLimit(raw string) int {
	patternLimit := 50
	if raw = strings.TrimSpace(raw); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			patternLimit = n
		}
	}
	if patternLimit > maxPatternResponseLimit {
		return maxPatternResponseLimit
	}
	return patternLimit
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func cloneURLValues(in url.Values) url.Values {
	out := make(url.Values, len(in))
	for k, vals := range in {
		copied := make([]string, len(vals))
		copy(copied, vals)
		out[k] = copied
	}
	return out
}

func parsePatternSourceLineLimit(raw, startParam, endParam, stepParam string) int {
	const (
		defaultPatternSourceLimit = 10_000
		minPatternSourceLimit     = 2_000
		maxPatternSourceLimit     = 50_000
		bucketSampleMultiplier    = 20
	)

	if raw = strings.TrimSpace(raw); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			if parsed > maxPatternSourceLimit {
				return maxPatternSourceLimit
			}
			return parsed
		}
	}

	startUnix, okStart := parsePatternUnixSeconds(startParam)
	endUnix, okEnd := parsePatternUnixSeconds(endParam)
	if !okStart || !okEnd || endUnix <= startUnix {
		return defaultPatternSourceLimit
	}

	stepSeconds := parsePatternStepSeconds(stepParam)
	if stepSeconds <= 0 {
		stepSeconds = 60
	}

	buckets := int(((endUnix - startUnix) / stepSeconds) + 1)
	estimate := buckets * bucketSampleMultiplier
	if estimate < minPatternSourceLimit {
		return minPatternSourceLimit
	}
	if estimate > maxPatternSourceLimit {
		return maxPatternSourceLimit
	}
	return estimate
}

func derivePatternStep(startParam, endParam string) string {
	startUnix, okStart := parsePatternUnixSeconds(startParam)
	endUnix, okEnd := parsePatternUnixSeconds(endParam)
	if !okStart || !okEnd || endUnix <= startUnix {
		return ""
	}

	spanSeconds := endUnix - startUnix
	const (
		targetBuckets = int64(120)
		minStep       = int64(1)
		maxStep       = int64(3600)
	)

	stepSeconds := spanSeconds / targetBuckets
	if stepSeconds < minStep {
		stepSeconds = minStep
	}
	if stepSeconds > maxStep {
		stepSeconds = maxStep
	}
	return strconv.FormatInt(stepSeconds, 10) + "s"
}

func patternWindowedSamplingConfig(startParam, endParam, stepParam string, sourceLimit int, denseWindowing bool) (int64, int64, time.Duration, int, bool) {
	startNs, endNs, ok := parseLokiTimeRangeToUnixNano(startParam, endParam)
	if !ok || sourceLimit <= 0 || endNs <= startNs {
		return 0, 0, 0, 0, false
	}

	span := time.Duration(endNs - startNs)
	minWindowedSpan := 20 * time.Minute
	targetWindowSpan := 20 * time.Minute
	minWindowSourceLimit := 200
	maxWindowSourceLimit := 1_000
	maxPatternWindowSamples := 96
	if denseWindowing {
		// Dense ranges can otherwise explode into hundreds of windows and produce
		// short/unstable tails under backend pressure. Keep fanout bounded.
		targetWindowSpan = 45 * time.Minute
		minWindowSourceLimit = 200
		maxWindowSourceLimit = 4_000
		maxPatternWindowSamples = 64
	}
	if span < minWindowedSpan {
		return 0, 0, 0, 0, false
	}

	windowCount := int(span/targetWindowSpan) + 1
	if !denseWindowing {
		if stepSeconds := parsePatternStepSeconds(stepParam); stepSeconds > 0 {
			stepNs := stepSeconds * int64(time.Second)
			if stepNs > 0 {
				stepBasedCount := int(((endNs - startNs) / stepNs) + 1)
				if stepBasedCount > windowCount {
					windowCount = stepBasedCount
				}
			}
		}
	}
	if windowCount < 2 {
		windowCount = 2
	}
	if windowCount > maxPatternWindowSamples {
		windowCount = maxPatternWindowSamples
	}

	intervalNs := (endNs - startNs) / int64(windowCount)
	if intervalNs <= 0 {
		return 0, 0, 0, 0, false
	}
	interval := time.Duration(intervalNs)

	perWindowLimit := sourceLimit / windowCount
	if perWindowLimit < minWindowSourceLimit {
		perWindowLimit = minWindowSourceLimit
	}
	if perWindowLimit > maxWindowSourceLimit {
		perWindowLimit = maxWindowSourceLimit
	}

	return startNs, endNs, interval, perWindowLimit, true
}

func shouldAcceptWindowedPatternResults(successes, total int, denseWindowing bool) bool {
	if successes <= 0 || total <= 0 {
		return false
	}
	if successes >= total {
		return true
	}
	if !denseWindowing {
		return true
	}
	// For dense mode, avoid returning highly partial window samples; they tend
	// to collapse to short recent tails and churn on refresh.
	return successes*2 >= total
}

func limitPatternPayload(payload []byte, limit int) []byte {
	if limit <= 0 || limit >= maxPatternResponseLimit || len(payload) == 0 {
		return payload
	}
	var resp patternsResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		return payload
	}
	if len(resp.Data) <= limit {
		return payload
	}
	resp.Data = resp.Data[:limit]
	encoded, err := json.Marshal(resp)
	if err != nil {
		return payload
	}
	return encoded
}

func customPatternSeedBucket(startParam, stepParam string) int64 {
	nowBucket := time.Now().UTC().Unix()
	if parsed, ok := parsePatternUnixSeconds(strings.TrimSpace(startParam)); ok {
		nowBucket = parsed
	}
	stepSeconds := parsePatternStepSeconds(stepParam)
	if stepSeconds > 0 {
		nowBucket = (nowBucket / stepSeconds) * stepSeconds
	}
	return nowBucket
}

func patternResultEntriesFromMaps(patterns []map[string]interface{}) []patternResultEntry {
	if len(patterns) == 0 {
		return nil
	}
	out := make([]patternResultEntry, 0, len(patterns))
	for _, item := range patterns {
		pattern, _ := item["pattern"].(string)
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		entry := patternResultEntry{
			Pattern: pattern,
			Samples: [][]interface{}{},
		}
		if level, ok := item["level"].(string); ok {
			entry.Level = strings.TrimSpace(level)
		}
		if rawSamples, ok := item["samples"].([][]interface{}); ok {
			entry.Samples = rawSamples
		} else if rawSamples, ok := item["samples"].([]interface{}); ok && len(rawSamples) > 0 {
			samples := make([][]interface{}, 0, len(rawSamples))
			for _, sample := range rawSamples {
				pair, ok := sample.([]interface{})
				if !ok || len(pair) != 2 {
					continue
				}
				samples = append(samples, pair)
			}
			entry.Samples = samples
		}
		out = append(out, entry)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func mergePatternResultEntries(base, extra []patternResultEntry) []patternResultEntry {
	if len(base) == 0 {
		return extra
	}
	if len(extra) == 0 {
		return base
	}

	type bucket struct {
		level   string
		pattern string
		samples map[int64]int
		total   int
	}

	merged := map[string]*bucket{}
	add := func(items []patternResultEntry) {
		for _, item := range items {
			pattern := strings.TrimSpace(item.Pattern)
			if pattern == "" {
				continue
			}
			key := item.Level + "\x00" + pattern
			b := merged[key]
			if b == nil {
				b = &bucket{
					level:   strings.TrimSpace(item.Level),
					pattern: pattern,
					samples: map[int64]int{},
				}
				merged[key] = b
			}
			for _, pair := range item.Samples {
				if len(pair) < 2 {
					continue
				}
				ts, okTS := numberToInt64(pair[0])
				count, okCount := numberToInt(pair[1])
				if !okTS || !okCount {
					continue
				}
				b.samples[ts] += count
				b.total += count
			}
		}
	}

	add(base)
	add(extra)

	items := make([]*bucket, 0, len(merged))
	for _, item := range merged {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].total != items[j].total {
			return items[i].total > items[j].total
		}
		if items[i].level != items[j].level {
			return items[i].level < items[j].level
		}
		return items[i].pattern < items[j].pattern
	})

	out := make([]patternResultEntry, 0, len(items))
	for _, item := range items {
		timestamps := make([]int64, 0, len(item.samples))
		for ts := range item.samples {
			timestamps = append(timestamps, ts)
		}
		sort.Slice(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] })
		samples := make([][]interface{}, 0, len(timestamps))
		for _, ts := range timestamps {
			samples = append(samples, []interface{}{ts, item.samples[ts]})
		}
		entry := patternResultEntry{Pattern: item.pattern, Samples: samples}
		if item.level != "" {
			entry.Level = item.level
		}
		out = append(out, entry)
	}

	return out
}

func (p *Proxy) prependCustomPatternEntries(patterns []patternResultEntry, startParam, stepParam string, limit int) []patternResultEntry {
	if len(p.patternsCustom) == 0 {
		if limit > 0 && len(patterns) > limit {
			return patterns[:limit]
		}
		return patterns
	}

	seedBucket := customPatternSeedBucket(startParam, stepParam)
	out := make([]patternResultEntry, 0, len(p.patternsCustom)+len(patterns))
	seen := make(map[string]struct{}, len(p.patternsCustom)+len(patterns))

	for _, customPattern := range p.patternsCustom {
		customPattern = strings.TrimSpace(customPattern)
		if customPattern == "" {
			continue
		}
		if _, ok := seen[customPattern]; ok {
			continue
		}
		seen[customPattern] = struct{}{}
		out = append(out, patternResultEntry{
			Pattern: customPattern,
			Samples: [][]interface{}{{seedBucket, 0}},
		})
	}

	for _, entry := range patterns {
		patternKey := strings.TrimSpace(entry.Pattern)
		if patternKey == "" {
			continue
		}
		if _, ok := seen[patternKey]; ok {
			continue
		}
		seen[patternKey] = struct{}{}
		out = append(out, entry)
	}

	if limit > 0 && len(out) > limit {
		return out[:limit]
	}
	return out
}

func (p *Proxy) applyCustomPatternsToPayload(payload []byte, startParam, endParam, stepParam string, limit int) []byte {
	if len(payload) == 0 {
		return payload
	}
	if len(p.patternsCustom) == 0 {
		return fillPatternPayloadSamples(payload, startParam, endParam, stepParam, limit)
	}

	var resp patternsResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		return fillPatternPayloadSamples(payload, startParam, endParam, stepParam, limit)
	}
	resp.Data = p.prependCustomPatternEntries(resp.Data, startParam, stepParam, limit)
	resp.Data = fillPatternSamplesAcrossRequestedRange(resp.Data, startParam, endParam, stepParam)
	encoded, err := json.Marshal(resp)
	if err != nil {
		return fillPatternPayloadSamples(payload, startParam, endParam, stepParam, limit)
	}
	return encoded
}

func (p *Proxy) patternsAutodetectCacheKey(orgID, query, start, end, step string) string {
	query = patternScopeQuery(query)
	if strings.TrimSpace(query) == "" {
		return ""
	}
	params := url.Values{}
	params.Set("query", strings.TrimSpace(query))
	normalizedStep := normalizePatternCacheStep(step)
	if trimmed := strings.TrimSpace(start); trimmed != "" {
		params.Set("start", normalizePatternCacheBoundary(trimmed, normalizedStep))
	}
	if trimmed := strings.TrimSpace(end); trimmed != "" {
		params.Set("end", normalizePatternCacheBoundary(trimmed, normalizedStep))
	}
	if trimmed := strings.TrimSpace(normalizedStep); trimmed != "" {
		params.Set("step", trimmed)
	}
	return "patterns:" + orgID + ":" + params.Encode()
}

func normalizePatternCacheBoundary(raw, normalizedStep string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	ts := formatVLTimestamp(raw)
	ns, ok := parseLokiTimeToUnixNano(raw)
	if !ok {
		return ts
	}
	isRelativeNow := raw == "now" || strings.HasPrefix(raw, "now-") || strings.HasPrefix(raw, "now+")
	if isRelativeNow {
		if stepSeconds := parsePatternStepSeconds(normalizedStep); stepSeconds > 0 {
			stepNs := stepSeconds * int64(time.Second)
			if stepNs > 0 {
				ns = (ns / stepNs) * stepNs
			}
		}
	}
	return strconv.FormatInt(ns, 10)
}

func fillPatternPayloadSamples(payload []byte, startParam, endParam, stepParam string, limit int) []byte {
	trimmed := limitPatternPayload(payload, limit)
	if len(trimmed) == 0 {
		return trimmed
	}
	var resp patternsResponse
	if err := json.Unmarshal(trimmed, &resp); err != nil {
		return trimmed
	}
	resp.Data = fillPatternSamplesAcrossRequestedRange(resp.Data, startParam, endParam, stepParam)
	encoded, err := json.Marshal(resp)
	if err != nil {
		return trimmed
	}
	return encoded
}

func fillPatternSamplesAcrossRequestedRange(entries []patternResultEntry, startParam, endParam, stepParam string) []patternResultEntry {
	if len(entries) == 0 {
		return entries
	}
	startUnix, okStart := parsePatternUnixSeconds(startParam)
	endUnix, okEnd := parsePatternUnixSeconds(endParam)
	if !okStart || !okEnd || endUnix < startUnix {
		return entries
	}
	stepSeconds := parsePatternStepSeconds(stepParam)
	if stepSeconds <= 0 {
		return entries
	}
	effectiveStepSeconds := stepSeconds
	startBucket := (startUnix / effectiveStepSeconds) * effectiveStepSeconds
	endBucket := (endUnix / effectiveStepSeconds) * effectiveStepSeconds
	if endBucket < startBucket {
		return entries
	}
	const maxPatternRangePoints = 11000
	points := int(((endBucket - startBucket) / effectiveStepSeconds) + 1)
	if points <= 0 {
		return entries
	}
	// For very long ranges with tiny steps (for example 1s over days), adaptively
	// coarsen bucket resolution instead of returning sparse short-tail samples.
	for points > maxPatternRangePoints {
		effectiveStepSeconds *= 2
		startBucket = (startUnix / effectiveStepSeconds) * effectiveStepSeconds
		endBucket = (endUnix / effectiveStepSeconds) * effectiveStepSeconds
		if endBucket < startBucket {
			return entries
		}
		points = int(((endBucket - startBucket) / effectiveStepSeconds) + 1)
	}
	for i := range entries {
		sampleMap := make(map[int64]int, len(entries[i].Samples))
		for _, pair := range entries[i].Samples {
			if len(pair) < 2 {
				continue
			}
			ts, okTS := numberToInt64(pair[0])
			count, okCount := numberToInt(pair[1])
			if !okTS || !okCount {
				continue
			}
			ts = (ts / effectiveStepSeconds) * effectiveStepSeconds
			sampleMap[ts] += count
		}
		filled := make([][]interface{}, 0, points)
		for ts := startBucket; ts <= endBucket; ts += effectiveStepSeconds {
			filled = append(filled, []interface{}{ts, sampleMap[ts]})
		}
		entries[i].Samples = filled
	}
	return entries
}

func normalizePatternCacheStep(step string) string {
	step = strings.TrimSpace(formatVLStep(step))
	if step == "" {
		return ""
	}
	dur, err := time.ParseDuration(step)
	if err != nil || dur <= 0 {
		return step
	}
	if dur%time.Second == 0 {
		return strconv.FormatInt(int64(dur/time.Second), 10) + "s"
	}
	return dur.String()
}

func (p *Proxy) maybeAutodetectPatternsFromNDJSON(orgID, query, start, end, step string, body []byte) {
	if !p.patternsEnabled || !p.patternsAutodetectFromQueries || len(body) == 0 {
		return
	}
	cacheKey := p.patternsAutodetectCacheKey(orgID, query, start, end, step)
	if cacheKey == "" {
		return
	}
	patterns := extractLogPatterns(body, step, maxPatternResponseLimit)
	if len(patterns) == 0 {
		return
	}
	p.metrics.RecordPatternsDetected(len(patterns))
	resultBody, err := json.Marshal(map[string]interface{}{
		"status": "success",
		"data":   patterns,
	})
	if err != nil {
		return
	}
	now := time.Now().UTC()
	p.cache.SetWithTTL(cacheKey, resultBody, patternsCacheRetention)
	p.recordPatternSnapshotEntry(cacheKey, resultBody, now)
}

func (p *Proxy) maybeAutodetectPatternsFromWindowEntries(orgID, query, start, end, step string, entries []queryRangeWindowEntry) {
	if !p.patternsEnabled || !p.patternsAutodetectFromQueries || len(entries) == 0 {
		return
	}
	cacheKey := p.patternsAutodetectCacheKey(orgID, query, start, end, step)
	if cacheKey == "" {
		return
	}
	patterns := extractLogPatternsFromWindowEntries(entries, step, maxPatternResponseLimit)
	if len(patterns) == 0 {
		return
	}
	p.metrics.RecordPatternsDetected(len(patterns))
	resultBody, err := json.Marshal(map[string]interface{}{
		"status": "success",
		"data":   patterns,
	})
	if err != nil {
		return
	}
	now := time.Now().UTC()
	p.cache.SetWithTTL(cacheKey, resultBody, patternsCacheRetention)
	p.recordPatternSnapshotEntry(cacheKey, resultBody, now)
}

// handleFormatQuery returns the query as-is (pretty-printing is client-side for LogQL).
func (p *Proxy) handleFormatQuery(w http.ResponseWriter, r *http.Request) {
	query := r.FormValue("query")
	p.writeJSON(w, map[string]interface{}{
		"status": "success",
		"data":   query,
	})
}

// handleDrilldownLimits returns Grafana Logs Drilldown limits metadata.
// Grafana uses this as a lightweight capability/bootstrap probe.
func (p *Proxy) handleDrilldownLimits(w http.ResponseWriter, r *http.Request) {
	patternIngesterEnabled := p.patternsEnabled && p.patternsAutodetectFromQueries
	limits := p.publishedTenantLimits(r)
	backendRaw, backendSemver, backendProfile := p.backendVersionState()
	profile := grafanaClientProfileFromContext(r.Context())
	if profile.surface == "" {
		profile = detectGrafanaClientProfile(r, "drilldown_limits", "/loki/api/v1/drilldown-limits")
	}
	resp := map[string]interface{}{
		"limits":                   limits,
		"pattern_ingester_enabled": patternIngesterEnabled,
		"version":                  "unknown",
		"maxDetectedFields":        1000,
		"maxDetectedValues":        1000,
		"maxLabelValues":           1000,
		"maxLines":                 p.maxLines,
	}
	if profile.surface != "" && profile.surface != "unknown" {
		resp["grafana_client_surface"] = profile.surface
	}
	if profile.version != "" {
		resp["grafana_runtime_version"] = profile.version
	}
	if profile.runtimeFamily != "" {
		resp["grafana_runtime_family"] = profile.runtimeFamily
	}
	if profile.drilldownProfile != "" {
		resp["drilldown_profile"] = profile.drilldownProfile
	}
	if profile.datasourceProfile != "" {
		resp["grafana_datasource_profile"] = profile.datasourceProfile
	}
	if backendRaw != "" {
		resp["backend_version_source"] = backendRaw
	}
	if backendSemver != "" {
		resp["backend_version_semver"] = backendSemver
	}
	if backendProfile != "" {
		resp["backend_capability_profile"] = backendProfile
	}
	p.writeJSON(w, resp)
}

func (p *Proxy) publishedTenantLimits(r *http.Request) map[string]any {
	orgID := strings.TrimSpace(r.Header.Get("X-Scope-OrgID"))
	if strings.Contains(orgID, "|") {
		orgID = ""
	}
	return p.publishedTenantLimitsForOrgID(orgID)
}

func (p *Proxy) publishedTenantLimitsForOrgID(orgID string) map[string]any {
	patternPersistenceEnabled := p.patternsEnabled && strings.TrimSpace(p.patternsPersistPath) != ""
	limits := map[string]any{
		"discover_log_levels":         true,
		"discover_service_name":       []string{"service", "app", "application", "app_name", "name", "app_kubernetes_io_name", "container", "container_name", "k8s_container_name", "component", "workload", "job", "k8s_job_name"},
		"log_level_fields":            []string{"level", "LEVEL", "Level", "log.level", "severity", "SEVERITY", "Severity", "SeverityText", "lvl", "LVL", "Lvl", "severity_text", "Severity_Text", "SEVERITY_TEXT"},
		"max_entries_limit_per_query": maxLimitValue,
		"max_line_size_truncate":      false,
		"max_query_bytes_read":        "0B",
		"max_query_length":            "30d1h",
		"max_query_lookback":          "0s",
		"max_query_range":             "0s",
		"max_query_series":            500,
		"metric_aggregation_enabled":  false,
		"otlp_config": map[string]any{
			"resource_attributes": map[string]any{
				"attributes_config": []map[string]any{
					{
						"action":     "index_label",
						"attributes": []string{"service.name", "service.namespace", "service.instance.id", "deployment.environment", "deployment.environment.name", "cloud.region", "cloud.availability_zone", "k8s.cluster.name", "k8s.namespace.name", "k8s.pod.name", "k8s.container.name", "container.name", "k8s.replicaset.name", "k8s.deployment.name", "k8s.statefulset.name", "k8s.daemonset.name", "k8s.cronjob.name", "k8s.job.name"},
					},
				},
			},
		},
		"pattern_persistence_enabled": patternPersistenceEnabled,
		"query_timeout":               p.client.Timeout.String(),
		"retention_period":            "0s",
		"retention_stream":            []any{},
		"volume_enabled":              true,
		"volume_max_series":           1000,
	}
	if p.client.Timeout <= 0 {
		limits["query_timeout"] = "0s"
	}

	p.configMu.RLock()
	allowlist := append([]string(nil), p.tenantLimitsAllowPublish...)
	defaultOverrides := cloneStringAnyMap(p.tenantDefaultLimits)
	tenantOverrides := cloneStringAnyMap(p.tenantLimits[orgID])
	p.configMu.RUnlock()

	if len(defaultOverrides) > 0 {
		mergeStringAnyMap(limits, defaultOverrides)
	}
	if len(tenantOverrides) > 0 {
		mergeStringAnyMap(limits, tenantOverrides)
	}
	return filterPublishedLimits(limits, allowlist)
}

func (p *Proxy) handleTenantLimitsConfig(w http.ResponseWriter, r *http.Request) {
	orgID := strings.TrimSpace(r.Header.Get("X-Scope-OrgID"))
	if strings.Contains(orgID, "|") {
		http.Error(w, "multi-tenant X-Scope-OrgID is not supported on this endpoint", http.StatusBadRequest)
		return
	}
	limits := p.publishedTenantLimitsForOrgID(orgID)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	data, err := yaml.Marshal(limits)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(data)
}

// handleDetectedLabels returns stream-level labels (similar to detected_fields but for stream labels).
func (p *Proxy) handleDetectedLabels(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "detected_labels", p.handleDetectedLabels) {
		return
	}
	orgID := r.Header.Get("X-Scope-OrgID")
	cacheKey := "detected_labels:" + orgID + ":" + r.URL.RawQuery
	if cached, remaining, ok := p.cache.GetWithTTL(cacheKey); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(cached)
		p.metrics.RecordRequest("detected_labels", http.StatusOK, time.Since(start))
		p.metrics.RecordCacheHit()
		if p.shouldRefreshLabelsInBackground(remaining, CacheTTLs["detected_labels"]) {
			p.refreshDetectedLabelsCacheAsync(orgID, cacheKey, r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), parseDetectedLineLimit(r))
		}
		return
	}
	p.metrics.RecordCacheMiss()

	r = withOrgID(r)
	lineLimit := parseDetectedLineLimit(r)
	detectedLabels, _, err := p.detectLabels(r.Context(), r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), lineLimit)
	if err != nil {
		payload := map[string]interface{}{
			"status":         "success",
			"data":           []interface{}{},
			"detectedLabels": []interface{}{},
			"limit":          lineLimit,
		}
		p.writeJSON(w, payload)
		p.metrics.RecordRequest("detected_labels", http.StatusOK, time.Since(start))
		return
	}

	payload := map[string]interface{}{
		"status":         "success",
		"data":           detectedLabels,
		"detectedLabels": detectedLabels,
		"limit":          lineLimit,
	}
	p.setJSONCacheWithTTL(cacheKey, CacheTTLs["detected_labels"], payload)
	p.writeJSON(w, payload)
	p.metrics.RecordRequest("detected_labels", http.StatusOK, time.Since(start))
}

// handleWriteBlocked rejects write requests — this is a read-only proxy.
func (p *Proxy) handleWriteBlocked(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusMethodNotAllowed)
	json.NewEncoder(w).Encode(map[string]string{
		"error": "write operations are not supported — this is a read-only proxy. Send logs directly to VictoriaLogs.",
	})
}

const (
	// maxDeleteTimeRange limits delete operations to 30 days for safety.
	maxDeleteTimeRange = 30 * 24 * time.Hour
)

// handleDelete is the sole write exception — proxies Loki delete requests to VL
// with strict safeguards: confirmation header, query validation, time range limits,
// tenant scoping, and audit logging.
//
// Loki: POST /loki/api/v1/delete?query={...}&start=...&end=...
// VL:   POST /select/logsql/delete?query=...&start=...&end=...
func (p *Proxy) handleDelete(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// Only POST allowed
	if r.Method != http.MethodPost {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "DELETE endpoint requires POST method",
		})
		return
	}

	// Safeguard 1: Require explicit confirmation header
	if r.Header.Get("X-Delete-Confirmation") != "true" {
		p.writeError(w, http.StatusBadRequest,
			"delete requires X-Delete-Confirmation: true header for safety")
		p.metrics.RecordRequest("delete", http.StatusBadRequest, time.Since(start))
		return
	}

	// Safeguard 2: Require non-empty, non-wildcard query
	query := r.FormValue("query")
	if query == "" {
		p.writeError(w, http.StatusBadRequest, "query parameter required for delete")
		p.metrics.RecordRequest("delete", http.StatusBadRequest, time.Since(start))
		return
	}
	trimmed := strings.TrimSpace(query)
	if trimmed == "{}" || trimmed == "*" || trimmed == "" {
		p.writeError(w, http.StatusBadRequest,
			"wildcard delete rejected — query must target specific streams (e.g., {app=\"nginx\"})")
		p.metrics.RecordRequest("delete", http.StatusBadRequest, time.Since(start))
		return
	}

	// Safeguard 3: Require time range
	startTS := r.FormValue("start")
	endTS := r.FormValue("end")
	if startTS == "" || endTS == "" {
		p.writeError(w, http.StatusBadRequest,
			"both start and end parameters required for delete")
		p.metrics.RecordRequest("delete", http.StatusBadRequest, time.Since(start))
		return
	}

	// Safeguard 4: Limit time range to maxDeleteTimeRange (30 days)
	startSec, err1 := strconv.ParseFloat(startTS, 64)
	endSec, err2 := strconv.ParseFloat(endTS, 64)
	if err1 == nil && err2 == nil {
		// Handle nanosecond timestamps (>1e15 is clearly nanoseconds)
		if startSec > 1e15 {
			startSec = startSec / 1e9
		}
		if endSec > 1e15 {
			endSec = endSec / 1e9
		}
		rangeDur := time.Duration(int64(endSec-startSec)) * time.Second
		if rangeDur > maxDeleteTimeRange {
			p.writeError(w, http.StatusBadRequest,
				fmt.Sprintf("delete time range too wide: %s exceeds maximum %s",
					rangeDur.Round(time.Hour), maxDeleteTimeRange))
			p.metrics.RecordRequest("delete", http.StatusBadRequest, time.Since(start))
			return
		}
		if rangeDur < 0 {
			p.writeError(w, http.StatusBadRequest, "end must be after start")
			p.metrics.RecordRequest("delete", http.StatusBadRequest, time.Since(start))
			return
		}
	}

	// Translate query
	logsqlQuery, err := p.translateQuery(query)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, "failed to translate query: "+err.Error())
		p.metrics.RecordRequest("delete", http.StatusBadRequest, time.Since(start))
		return
	}

	// Safeguard 5: Tenant scoping
	r = withOrgID(r)
	tenant := r.Header.Get("X-Scope-OrgID")

	// Audit log BEFORE executing delete
	p.log.Warn("DELETE request",
		"tenant", tenant,
		"query", query,
		"logsql", logsqlQuery,
		"start", startTS,
		"end", endTS,
		"client", r.RemoteAddr,
	)

	// Forward to VL delete endpoint
	params := url.Values{}
	params.Set("query", logsqlQuery)
	params.Set("start", formatVLTimestamp(startTS))
	params.Set("end", formatVLTimestamp(endTS))

	resp, err := p.vlPost(r.Context(), "/select/logsql/delete", params)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, "VL delete failed: "+err.Error())
		p.metrics.RecordRequest("delete", http.StatusBadGateway, time.Since(start))
		return
	}
	defer resp.Body.Close()

	// Propagate VL response
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		p.writeError(w, resp.StatusCode, string(body))
		p.metrics.RecordRequest("delete", resp.StatusCode, time.Since(start))
		return
	}

	// Audit log AFTER successful delete
	p.log.Warn("DELETE completed",
		"tenant", tenant,
		"query", query,
		"start", startTS,
		"end", endTS,
		"vl_status", resp.StatusCode,
		"duration_ms", time.Since(start).Milliseconds(),
	)

	w.WriteHeader(http.StatusNoContent)
	p.metrics.RecordRequest("delete", http.StatusNoContent, time.Since(start))
}

// handleTail bridges Loki's WebSocket tail to VL's NDJSON streaming tail.
// Loki: ws:///loki/api/v1/tail?query={...}&start=...&limit=...
// VL:   GET /select/logsql/tail?query=...
func (p *Proxy) handleTail(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	logqlQuery := r.FormValue("query")
	if logqlQuery == "" {
		p.writeError(w, http.StatusBadRequest, "query parameter required")
		p.metrics.RecordRequest("tail", http.StatusBadRequest, time.Since(start))
		return
	}

	logsqlQuery, err := p.translateQuery(logqlQuery)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		p.metrics.RecordRequest("tail", http.StatusBadRequest, time.Since(start))
		return
	}

	if origin := strings.TrimSpace(r.Header.Get("Origin")); origin != "" && !p.isAllowedTailOrigin(origin) {
		p.writeError(w, http.StatusForbidden, "tail origin not allowed")
		p.metrics.RecordRequest("tail", http.StatusForbidden, time.Since(start))
		return
	}

	r = withOrgID(r)

	tailCtx, tailCancel := context.WithCancel(r.Context())
	defer tailCancel()

	if statusCode, msg, ok := p.preflightTailAccess(tailCtx, logsqlQuery, r.FormValue("start")); ok {
		p.writeError(w, statusCode, msg)
		p.metrics.RecordRequest("tail", statusCode, time.Since(start))
		return
	}

	// Upgrade immediately after local validation so slow or blocking native tail
	// headers do not break the client handshake. Native tail remains a best-effort
	// path; if it stalls or isn't available, synthetic polling takes over.
	upgrader := p.tailUpgrader()
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		p.log.Error("websocket upgrade failed", "error", err)
		p.metrics.RecordRequest("tail", http.StatusBadRequest, time.Since(start))
		return
	}
	defer func() { _ = conn.Close() }()
	p.metrics.RecordRequest("tail", http.StatusOK, time.Since(start))

	// Start a read loop to detect client disconnect (WebSocket protocol requires it).
	// When client closes, this goroutine exits and wsCtx is canceled.
	wsCtx, wsCancel := context.WithCancel(tailCtx)
	defer wsCancel()
	go func() {
		defer tailCancel()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	pingTicker := time.NewTicker(time.Second)
	defer pingTicker.Stop()

	if p.tailMode == TailModeSynthetic {
		p.log.Debug("tail connected", "logql", logqlQuery, "logsql", logsqlQuery, "native", false, "fallback", "forced synthetic tail mode")
		p.streamSyntheticTail(wsCtx, conn, logsqlQuery, r.FormValue("start"))
		return
	}

	resp, nativeTail, fallbackReason := p.openNativeTailStream(wsCtx, logsqlQuery)
	p.log.Debug("tail connected", "logql", logqlQuery, "logsql", logsqlQuery, "native", nativeTail, "fallback", fallbackReason)
	if !nativeTail {
		if p.tailMode == TailModeNative {
			_ = p.writeTailControl(conn, websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, fallbackReason))
			return
		}
		p.streamSyntheticTail(wsCtx, conn, logsqlQuery, r.FormValue("start"))
		return
	}
	defer resp.Body.Close()

	// Read VL NDJSON stream and forward as Loki WebSocket frames
	lineCh := make(chan []byte)
	errCh := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 1024*1024), 1024*1024) // 1MB max line
		for scanner.Scan() {
			line := append([]byte(nil), scanner.Bytes()...)
			select {
			case lineCh <- line:
			case <-wsCtx.Done():
				return
			}
		}
		errCh <- scanner.Err()
	}()

	for {
		select {
		case <-wsCtx.Done():
			return
		case <-pingTicker.C:
			if err := p.writeTailMessage(conn, websocket.PingMessage, nil); err != nil {
				p.log.Debug("websocket ping failed, client disconnected", "error", err)
				return
			}
		case err := <-errCh:
			if err != nil && wsCtx.Err() == nil {
				p.log.Debug("tail stream ended with error", "error", err)
			}
			return
		case line := <-lineCh:
			if len(line) == 0 {
				continue
			}

			// Parse VL NDJSON line
			var vlLine map[string]interface{}
			if err := json.Unmarshal(line, &vlLine); err != nil {
				continue
			}

			// Convert to Loki tail frame
			frame := p.vlLineToTailFrame(vlLine)
			frameJSON, err := json.Marshal(frame)
			if err != nil {
				continue
			}

			if err := p.writeTailMessage(conn, websocket.TextMessage, frameJSON); err != nil {
				p.log.Debug("websocket write failed, client disconnected", "error", err)
				return
			}
		}
	}
}

func (p *Proxy) preflightTailAccess(parent context.Context, logsqlQuery, startHint string) (int, string, bool) {
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()

	windowStart := time.Now().Add(-5 * time.Second)
	if parsed, ok := parseEntryTime(startHint); ok {
		windowStart = parsed
	}

	params := url.Values{}
	params.Set("query", logsqlQuery+" | sort by (_time desc)")
	params.Set("start", formatVLTimestamp(windowStart.UTC().Format(time.RFC3339Nano)))
	params.Set("end", formatVLTimestamp(time.Now().UTC().Format(time.RFC3339Nano)))
	params.Set("limit", "1")

	resp, err := p.vlGet(ctx, "/select/logsql/query", params)
	if err != nil {
		p.log.Debug("tail preflight skipped", "error", err)
		return 0, "", false
	}
	defer resp.Body.Close()

	if resp.StatusCode < 400 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return 0, "", false
	}

	body, _ := io.ReadAll(resp.Body)
	msg := strings.TrimSpace(string(body))
	if msg == "" {
		msg = http.StatusText(resp.StatusCode)
	}
	return resp.StatusCode, msg, true
}

func (p *Proxy) openNativeTailStream(parent context.Context, logsqlQuery string) (*http.Response, bool, string) {
	ctx, cancel := context.WithTimeout(parent, 1500*time.Millisecond)
	defer cancel()

	vlURL := fmt.Sprintf("%s/select/logsql/tail?query=%s",
		p.backend.String(), url.QueryEscape(logsqlQuery))
	req, err := http.NewRequestWithContext(ctx, "GET", vlURL, nil)
	if err != nil {
		return nil, false, "failed to create native tail request"
	}
	p.applyBackendHeaders(req)
	p.forwardTenantHeaders(req)

	resp, err := p.tailClient.Do(req)
	if err != nil {
		return nil, false, err.Error()
	}
	if err := decodeCompressedHTTPResponse(resp); err != nil {
		_ = resp.Body.Close()
		return nil, false, fmt.Sprintf("backend tail decode error: %v", err)
	}
	if resp.StatusCode == http.StatusOK {
		return resp, true, ""
	}

	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	msg := strings.TrimSpace(string(body))
	if msg == "" {
		msg = http.StatusText(resp.StatusCode)
	}
	return nil, false, fmt.Sprintf("backend tail unavailable: %s", msg)
}

func (p *Proxy) streamSyntheticTail(ctx context.Context, conn tailConn, logsqlQuery, startHint string) {
	lastSeen := newSyntheticTailSeen(maxSyntheticTailSeenEntries)
	windowStart := time.Now().Add(-5 * time.Second)
	if parsed, ok := parseEntryTime(startHint); ok {
		windowStart = parsed
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		if err := p.writeSyntheticTailBatch(ctx, conn, logsqlQuery, &windowStart, lastSeen); err != nil {
			p.log.Debug("synthetic tail batch failed", "error", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (p *Proxy) writeSyntheticTailBatch(ctx context.Context, conn tailConn, logsqlQuery string, windowStart *time.Time, lastSeen *syntheticTailSeen) error {
	params := url.Values{}
	params.Set("query", logsqlQuery+" | sort by (_time)")
	params.Set("start", formatVLTimestamp(windowStart.UTC().Format(time.RFC3339Nano)))
	params.Set("end", formatVLTimestamp(time.Now().UTC().Format(time.RFC3339Nano)))
	params.Set("limit", "200")

	resp, err := p.vlGet(ctx, "/select/logsql/query", params)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("synthetic tail query failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	newest := *windowStart
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}

		var vlLine map[string]interface{}
		if err := json.Unmarshal(line, &vlLine); err != nil {
			continue
		}
		timeStr, _ := stringifyEntryValue(vlLine["_time"])
		msgStr, _ := stringifyEntryValue(vlLine["_msg"])
		streamStr, _ := stringifyEntryValue(vlLine["_stream"])
		seenKey := timeStr + "\x00" + streamStr + "\x00" + msgStr
		if lastSeen.Contains(seenKey) {
			continue
		}
		lastSeen.Add(seenKey)

		if entryTime, ok := parseEntryTime(timeStr); ok && entryTime.After(newest) {
			newest = entryTime
		}

		frameJSON, err := json.Marshal(p.vlLineToTailFrame(vlLine))
		if err != nil {
			continue
		}
		if err := p.writeTailMessage(conn, websocket.TextMessage, frameJSON); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	*windowStart = newest.Add(time.Nanosecond)
	return nil
}

func newSyntheticTailSeen(limit int) *syntheticTailSeen {
	if limit <= 0 {
		limit = maxSyntheticTailSeenEntries
	}
	return &syntheticTailSeen{
		seen:  make(map[string]struct{}, min(128, limit)),
		order: make([]string, 0, min(128, limit)),
		limit: limit,
	}
}

func (s *syntheticTailSeen) Contains(key string) bool {
	_, ok := s.seen[key]
	return ok
}

func (s *syntheticTailSeen) Add(key string) {
	if _, ok := s.seen[key]; ok {
		return
	}
	s.seen[key] = struct{}{}
	s.order = append(s.order, key)
	if len(s.order) <= s.limit {
		return
	}
	drop := len(s.order) - s.limit
	for _, oldKey := range s.order[:drop] {
		delete(s.seen, oldKey)
	}
	s.order = append([]string(nil), s.order[drop:]...)
}
func (p *Proxy) writeTailMessage(conn tailConn, messageType int, data []byte) error {
	if err := conn.SetWriteDeadline(time.Now().Add(tailWriteTimeout)); err != nil {
		return err
	}
	return conn.WriteMessage(messageType, data)
}

func (p *Proxy) writeTailControl(conn tailConn, messageType int, data []byte) error {
	deadline := time.Now().Add(tailWriteTimeout)
	if err := conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	return conn.WriteControl(messageType, data, deadline)
}

// vlLineToTailFrame converts a single VL NDJSON log line to a Loki tail WebSocket frame.
func (p *Proxy) vlLineToTailFrame(vlLine map[string]interface{}) map[string]interface{} {
	ts := ""
	msg := ""

	for k, v := range vlLine {
		sv := fmt.Sprintf("%v", v)
		switch k {
		case "_time":
			if t, err := time.Parse(time.RFC3339Nano, sv); err == nil {
				ts = fmt.Sprintf("%d", t.UnixNano())
			} else {
				ts = sv
			}
		case "_msg":
			msg = sv
		case "_stream":
			// Skip internal VL stream ID
		}
	}
	if ts == "" {
		ts = fmt.Sprintf("%d", time.Now().UnixNano())
	}

	labels := buildEntryLabels(vlLine)
	translatedLabels := labels
	if !p.labelTranslator.IsPassthrough() {
		translatedLabels = p.labelTranslator.TranslateLabelsMap(labels)
	}
	ensureDetectedLevel(translatedLabels)
	ensureSyntheticServiceName(translatedLabels)

	return map[string]interface{}{
		"streams": []map[string]interface{}{
			{
				"stream": translatedLabels,
				"values": [][]string{{ts, msg}},
			},
		},
	}
}

// handleReady returns readiness status.
func (p *Proxy) handleReady(w http.ResponseWriter, r *http.Request) {
	if p.labelValuesIndexedCache && !p.labelValuesIndexWarmReady.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("label values index warming"))
		return
	}
	if p.patternsEnabled && strings.TrimSpace(p.patternsPersistPath) != "" && !p.patternsWarmReady.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("patterns snapshot warming"))
		return
	}

	// Probe VL backend health
	readyReq := withOrgID(r)
	resp, err := p.vlGet(readyReq.Context(), "/health", nil)
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("backend not ready"))
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("backend not ready"))
		return
	}

	// Circuit breaker check
	if !p.breaker.Allow() {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("circuit breaker open"))
		return
	}

	w.Write([]byte("ready"))
}

// handleHealth returns process health status without backend dependency checks.
func (p *Proxy) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("ok"))
}

// handleAlive returns process liveness status without backend dependency checks.
func (p *Proxy) handleAlive(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("alive"))
}

func (p *Proxy) writeEmptyRules(w http.ResponseWriter) {
	p.writeJSON(w, map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"groups": []interface{}{},
		},
	})
}

func (p *Proxy) writeEmptyLegacyRules(w http.ResponseWriter) {
	data, err := yaml.Marshal(map[string][]legacyRuleGroup{})
	if err != nil {
		p.writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to marshal rules response: %v", err))
		return
	}
	w.Header().Set("Content-Type", "application/yaml")
	_, _ = w.Write(data)
}

func (p *Proxy) writeEmptyAlerts(w http.ResponseWriter) {
	p.writeJSON(w, map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"alerts": []interface{}{},
		},
	})
}

func (p *Proxy) handleRules(w http.ResponseWriter, r *http.Request) {
	if isLegacyRulesPath(r.URL.Path) {
		if p.rulerBackend != nil {
			p.handleLegacyRules(w, r)
			return
		}
		p.writeEmptyLegacyRules(w)
		return
	}
	if p.rulerBackend == nil {
		p.writeEmptyRules(w)
		return
	}
	p.proxyAlertingRead(w, r, p.rulerBackend, "/api/v1/rules")
}

func (p *Proxy) handleAlerts(w http.ResponseWriter, r *http.Request) {
	if p.alertsBackend == nil {
		p.writeEmptyAlerts(w)
		return
	}
	p.proxyAlertingRead(w, r, p.alertsBackend, "/api/v1/alerts")
}

// handleConfigStub returns a minimal config response.
func (p *Proxy) handleConfigStub(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/yaml")
	w.Write([]byte("# loki-vl-proxy\n"))
}

// handleBuildInfo returns fake build info for Grafana datasource detection.
func (p *Proxy) handleBuildInfo(w http.ResponseWriter, r *http.Request) {
	p.writeJSON(w, map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"version":   "2.9.0",
			"revision":  "loki-vl-proxy",
			"branch":    "main",
			"goVersion": "go1.26.1",
		},
	})
}

// --- Backend request helpers ---

func (p *Proxy) proxyAlertingRead(w http.ResponseWriter, r *http.Request, backend *url.URL, path string) {
	resp, err := p.alertingBackendGet(withOrgID(r), backend, path)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	defer resp.Body.Close()

	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (p *Proxy) alertingBackendGet(r *http.Request, backend *url.URL, path string) (*http.Response, error) {
	return p.alertingBackendGetWithParams(r, backend, path, r.URL.Query())
}

func (p *Proxy) alertingBackendGetWithParams(r *http.Request, backend *url.URL, path string, params url.Values) (*http.Response, error) {
	if backend == nil {
		return nil, fmt.Errorf("alerting backend not configured")
	}

	u := *backend
	u.Path = path
	u.RawQuery = params.Encode()

	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	p.forwardTenantHeaders(req)
	p.applyBackendHeaders(req)
	start := time.Now()
	resp, err := p.client.Do(req)
	duration := time.Since(start)
	serverPort, _ := strconv.Atoi(u.Port())
	if err != nil {
		mappedStatus := statusFromUpstreamErr(err)
		recordUpstreamCall(r.Context(), mappedStatus, duration, true)
		p.recordUpstreamObservation(r.Context(), "loki", http.MethodGet, path, u.Hostname(), serverPort, mappedStatus, duration, err)
		return nil, err
	}
	if err := decodeCompressedHTTPResponse(resp); err != nil {
		_ = resp.Body.Close()
		recordUpstreamCall(r.Context(), http.StatusBadGateway, duration, true)
		p.recordUpstreamObservation(r.Context(), "loki", http.MethodGet, path, u.Hostname(), serverPort, http.StatusBadGateway, duration, err)
		return nil, fmt.Errorf("decode backend response: %w", err)
	}
	recordUpstreamCall(r.Context(), resp.StatusCode, duration, false)
	p.recordUpstreamObservation(r.Context(), "loki", http.MethodGet, path, u.Hostname(), serverPort, resp.StatusCode, duration, nil)
	return resp, nil
}

type alertingRulesResponse struct {
	Status string `json:"status"`
	Data   struct {
		Groups []alertingRuleGroup `json:"groups"`
	} `json:"data"`
}

type alertingRuleGroup struct {
	Name     string                `json:"name"`
	File     string                `json:"file"`
	Interval float64               `json:"interval"`
	Rules    []alertingBackendRule `json:"rules"`
}

type alertingBackendRule struct {
	Name        string            `json:"name"`
	Query       string            `json:"query"`
	Type        string            `json:"type"`
	Duration    float64           `json:"duration"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

type legacyRuleGroup struct {
	Name     string            `yaml:"name"`
	Interval string            `yaml:"interval,omitempty"`
	Rules    []legacyRuleEntry `yaml:"rules"`
}

type legacyRuleEntry struct {
	Alert       string            `yaml:"alert,omitempty"`
	Record      string            `yaml:"record,omitempty"`
	Expr        string            `yaml:"expr"`
	For         string            `yaml:"for,omitempty"`
	Labels      map[string]string `yaml:"labels,omitempty"`
	Annotations map[string]string `yaml:"annotations,omitempty"`
}

func isLegacyRulesPath(path string) bool {
	return path == "/loki/api/v1/rules" ||
		strings.HasPrefix(path, "/loki/api/v1/rules/") ||
		path == "/api/prom/rules" ||
		strings.HasPrefix(path, "/api/prom/rules/")
}

func parseLegacyRulesPath(path string) (namespace, group string, err error) {
	var suffix string
	switch {
	case strings.HasPrefix(path, "/loki/api/v1/rules/"):
		suffix = strings.TrimPrefix(path, "/loki/api/v1/rules/")
	case strings.HasPrefix(path, "/api/prom/rules/"):
		suffix = strings.TrimPrefix(path, "/api/prom/rules/")
	default:
		return "", "", nil
	}
	if suffix == "" {
		return "", "", nil
	}
	parts := strings.Split(suffix, "/")
	if len(parts) > 2 {
		return "", "", fmt.Errorf("invalid legacy rules path")
	}
	namespace, err = url.PathUnescape(parts[0])
	if err != nil {
		return "", "", fmt.Errorf("invalid namespace: %w", err)
	}
	if !filepath.IsLocal(namespace) {
		return "", "", fmt.Errorf("invalid namespace: path traversal not allowed")
	}
	if len(parts) == 2 {
		group, err = url.PathUnescape(parts[1])
		if err != nil {
			return "", "", fmt.Errorf("invalid group name: %w", err)
		}
		if !filepath.IsLocal(group) {
			return "", "", fmt.Errorf("invalid group name: path traversal not allowed")
		}
	}
	return namespace, group, nil
}

func (p *Proxy) handleLegacyRules(w http.ResponseWriter, r *http.Request) {
	namespace, group, err := parseLegacyRulesPath(r.URL.Path)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	params := r.URL.Query()
	if namespace != "" {
		params["file[]"] = []string{namespace}
	}
	if group != "" {
		params["rule_group[]"] = []string{group}
	}

	resp, err := p.alertingBackendGetWithParams(withOrgID(r), p.rulerBackend, "/api/v1/rules", params)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		copyHeaders(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
		return
	}

	var decoded alertingRulesResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		p.writeError(w, http.StatusBadGateway, fmt.Sprintf("invalid ruler backend response: %v", err))
		return
	}

	if group != "" {
		if len(decoded.Data.Groups) == 0 {
			p.writeError(w, http.StatusNotFound, "no rule groups found")
			return
		}
		out := legacyRuleGroupFromBackend(decoded.Data.Groups[0])
		data, err := yaml.Marshal(out)
		if err != nil {
			p.writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to marshal rules response: %v", err))
			return
		}
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(data)
		return
	}

	formatted := make(map[string][]legacyRuleGroup)
	for _, g := range decoded.Data.Groups {
		formatted[g.File] = append(formatted[g.File], legacyRuleGroupFromBackend(g))
	}
	data, err := yaml.Marshal(formatted)
	if err != nil {
		p.writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to marshal rules response: %v", err))
		return
	}
	w.Header().Set("Content-Type", "application/yaml")
	_, _ = w.Write(data)
}

func legacyRuleGroupFromBackend(g alertingRuleGroup) legacyRuleGroup {
	out := legacyRuleGroup{
		Name:  g.Name,
		Rules: make([]legacyRuleEntry, 0, len(g.Rules)),
	}
	if g.Interval > 0 {
		out.Interval = (time.Duration(g.Interval * float64(time.Second))).String()
	}
	for _, r := range g.Rules {
		entry := legacyRuleEntry{
			Expr:        r.Query,
			Labels:      r.Labels,
			Annotations: r.Annotations,
		}
		switch r.Type {
		case "alerting":
			entry.Alert = r.Name
		default:
			entry.Record = r.Name
		}
		if r.Duration > 0 {
			entry.For = (time.Duration(r.Duration * float64(time.Second))).String()
		}
		out.Rules = append(out.Rules, entry)
	}
	return out
}

func deriveRequestType(endpoint, route string) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint != "" {
		return endpoint
	}
	route = strings.TrimSpace(route)
	if route == "" {
		return "unknown"
	}
	if requestType, ok := upstreamRequestTypeByRoute[route]; ok {
		return requestType
	}
	trimmed := strings.Trim(route, "/")
	if trimmed == "" {
		return "unknown"
	}
	trimmed = strings.ReplaceAll(trimmed, "/", "_")
	return trimmed
}

func normalizeObservedRoute(route string) string {
	route = strings.TrimSpace(route)
	if route == "" {
		return "/unknown"
	}
	return route
}

func (p *Proxy) recordUpstreamObservation(ctx context.Context, system, method, route, serverAddress string, serverPort int, statusCode int, duration time.Duration, err error) {
	meta := requestRouteMetaFromContext(ctx)
	requestType := deriveRequestType(meta.endpoint, route)
	observedRoute := normalizeObservedRoute(route)

	p.metrics.RecordUpstreamRequest(system, requestType, observedRoute, statusCode, duration)
	if system == "vl" {
		p.metrics.RecordBackendDurationWithRoute(requestType, observedRoute, duration)
	}

	level := slog.LevelInfo
	if err != nil || statusCode >= http.StatusInternalServerError {
		level = slog.LevelError
	} else if statusCode >= http.StatusBadRequest {
		level = slog.LevelWarn
	}
	if !p.log.Enabled(ctx, level) {
		return
	}

	logAttrs := []interface{}{
		"http.route", observedRoute,
		"url.path", observedRoute,
		"http.request.method", method,
		"http.response.status_code", statusCode,
		"loki.request.type", requestType,
		"loki.api.system", system,
		"proxy.direction", "upstream",
		"event.duration", duration.Nanoseconds(),
		"loki.tenant.id", getOrgID(ctx),
	}
	if serverAddress != "" {
		logAttrs = append(logAttrs, "server.address", serverAddress)
	}
	if serverPort > 0 {
		logAttrs = append(logAttrs, "server.port", serverPort)
	}
	if err != nil {
		logAttrs = append(logAttrs,
			"error.type", "transport",
			"error.message", err.Error(),
		)
	}
	p.log.Log(ctx, level, "upstream_request", logAttrs...)
}

func (p *Proxy) observeBackendVersionFromHeaders(headers http.Header) {
	if headers == nil {
		return
	}
	candidates := []string{
		headers.Get("Server"),
		headers.Get("X-App-Version"),
		headers.Get("X-Backend-Version"),
		headers.Get("X-Victoriametrics-Build"),
	}
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		semver := extractBackendSemver(candidate)
		if semver == "" {
			continue
		}
		p.storeBackendVersion(candidate, semver)
		return
	}
}

func extractBackendSemver(raw string) string {
	return backendSemverPattern.FindString(raw)
}

func extractBackendSemverFromMetrics(metricsText string) string {
	metricsText = strings.TrimSpace(metricsText)
	if metricsText == "" {
		return ""
	}
	for _, line := range strings.Split(metricsText, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.Contains(line, "vm_app_version{") && !strings.Contains(line, "victorialogs_build_info{") {
			continue
		}
		if match := backendMetricsQuotedVersionPattern.FindStringSubmatch(line); len(match) == 2 {
			return match[1]
		}
		if fallback := extractBackendSemver(line); fallback != "" {
			return fallback
		}
	}
	return ""
}

func parseSemverTriplet(version string) (int, int, int, bool) {
	version = strings.TrimSpace(strings.TrimPrefix(version, "v"))
	if version == "" {
		return 0, 0, 0, false
	}
	parts := strings.Split(version, ".")
	if len(parts) < 3 {
		return 0, 0, 0, false
	}
	major, err := strconv.Atoi(numericPrefix(parts[0]))
	if err != nil {
		return 0, 0, 0, false
	}
	minor, err := strconv.Atoi(numericPrefix(parts[1]))
	if err != nil {
		return 0, 0, 0, false
	}
	patch, err := strconv.Atoi(numericPrefix(parts[2]))
	if err != nil {
		return 0, 0, 0, false
	}
	return major, minor, patch, true
}

func normalizeSemverString(version string) string {
	version = strings.TrimSpace(version)
	if version == "" {
		return ""
	}
	if version[0] >= '0' && version[0] <= '9' {
		return "v" + version
	}
	return version
}

func numericPrefix(in string) string {
	in = strings.TrimSpace(in)
	if in == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range in {
		if r < '0' || r > '9' {
			break
		}
		b.WriteRune(r)
	}
	return b.String()
}

func semverAtLeastSemver(version, minVersion string) bool {
	vMaj, vMin, vPatch, vok := parseSemverTriplet(version)
	mMaj, mMin, mPatch, mok := parseSemverTriplet(minVersion)
	if !vok || !mok {
		return false
	}
	if vMaj != mMaj {
		return vMaj > mMaj
	}
	if vMin != mMin {
		return vMin > mMin
	}
	return vPatch >= mPatch
}

func semverAtLeast(semver string, major, minor, patch int) bool {
	mj, mn, pt, ok := parseSemverTriplet(semver)
	if !ok {
		return false
	}
	if mj != major {
		return mj > major
	}
	if mn != minor {
		return mn > minor
	}
	return pt >= patch
}

func deriveBackendCapabilities(semver string) (profile string, supportsStreamMetadata bool, supportsDensePatternWindowing bool, supportsMetadataSubstring bool) {
	switch {
	case semverAtLeast(semver, 1, 50, 0):
		return "vl-v1.50-plus", true, true, true
	case semverAtLeast(semver, 1, 49, 0):
		return "vl-v1.49-plus", true, false, true
	case semverAtLeast(semver, 1, 30, 0):
		return "vl-v1.30-plus", true, false, false
	default:
		return "legacy-pre-v1.30", false, false, false
	}
}

func (p *Proxy) storeBackendVersion(raw, semver string) {
	if semver == "" {
		return
	}
	p.backendVersionMu.Lock()
	defer p.backendVersionMu.Unlock()
	// Keep first detected version to avoid churn from intermediaries rewriting headers.
	if p.backendVersionSemver != "" {
		return
	}
	profile, supportsStreamMetadata, supportsDensePatternWindowing, supportsMetadataSubstring := deriveBackendCapabilities(semver)
	p.backendVersionRaw = raw
	p.backendVersionSemver = semver
	p.backendCapabilityProfile = profile
	p.backendSupportsStreamMetadata = supportsStreamMetadata
	p.backendSupportsDensePatternWindowing = supportsDensePatternWindowing
	p.backendSupportsMetadataSubstring = supportsMetadataSubstring
	if !p.backendVersionLogged {
		p.backendVersionLogged = true
		p.log.Info(
			"backend version detected",
			"backend.version.raw", raw,
			"backend.version.semver", semver,
			"backend.capability_profile", p.backendCapabilityProfile,
			"metadata.stream_endpoints", p.backendSupportsStreamMetadata,
			"metadata.substring_filter", p.backendSupportsMetadataSubstring,
			"patterns.dense_windowing", p.backendSupportsDensePatternWindowing,
		)
	}
}

func (p *Proxy) supportsStreamMetadataEndpoints() bool {
	p.backendVersionMu.RLock()
	defer p.backendVersionMu.RUnlock()
	// Before first backend version observation, optimistically keep stream-first
	// behavior so we don't regress newer deployments.
	if p.backendVersionSemver == "" {
		return true
	}
	return p.backendSupportsStreamMetadata
}

func (p *Proxy) supportsDensePatternWindowing() bool {
	p.backendVersionMu.RLock()
	defer p.backendVersionMu.RUnlock()
	return p.backendSupportsDensePatternWindowing
}

func (p *Proxy) supportsDensePatternWindowingForRequest(r *http.Request) bool {
	if !p.supportsDensePatternWindowing() {
		return false
	}
	if r == nil {
		return true
	}
	profile := grafanaClientProfileFromContext(r.Context())
	// Keep legacy Drilldown runtime family on conservative windowing even when backend
	// can do denser extraction. This mirrors 1.x behavior and avoids over-rendering
	// surprises on older Grafana runtimes.
	if profile.surface == "grafana_drilldown" && profile.runtimeMajor > 0 && profile.runtimeMajor < 12 {
		return false
	}
	return true
}

func (p *Proxy) supportsMetadataSubstringFilter() bool {
	p.backendVersionMu.RLock()
	defer p.backendVersionMu.RUnlock()
	return p.backendSupportsMetadataSubstring
}

func (p *Proxy) backendVersionState() (raw, semver, profile string) {
	p.backendVersionMu.RLock()
	defer p.backendVersionMu.RUnlock()
	return p.backendVersionRaw, p.backendVersionSemver, p.backendCapabilityProfile
}

func (p *Proxy) storeBackendCapabilityProbe(source string, supportsStreamMetadata, supportsMetadataSubstring bool) {
	if p == nil {
		return
	}
	p.backendVersionMu.Lock()
	defer p.backendVersionMu.Unlock()
	if p.backendVersionSemver != "" {
		return
	}
	p.backendVersionRaw = source
	switch {
	case supportsStreamMetadata && supportsMetadataSubstring:
		p.backendCapabilityProfile = "vl-probed-v1.49-plus"
	case supportsStreamMetadata:
		p.backendCapabilityProfile = "vl-probed-v1.30-plus"
	default:
		p.backendCapabilityProfile = "legacy-probed-pre-v1.30"
	}
	p.backendSupportsStreamMetadata = supportsStreamMetadata
	p.backendSupportsMetadataSubstring = supportsMetadataSubstring
	// Keep dense-windowing conservative without explicit semver evidence.
	p.backendSupportsDensePatternWindowing = false
	if !p.backendVersionLogged {
		p.backendVersionLogged = true
		p.log.Info(
			"backend capability profile inferred",
			"backend.version.source", source,
			"backend.capability_profile", p.backendCapabilityProfile,
			"metadata.stream_endpoints", p.backendSupportsStreamMetadata,
			"metadata.substring_filter", p.backendSupportsMetadataSubstring,
			"patterns.dense_windowing", p.backendSupportsDensePatternWindowing,
		)
	}
}

func (p *Proxy) probeBackendVersionFromMetrics(ctx context.Context) {
	if p == nil {
		return
	}
	resp, err := p.vlGet(ctx, "/metrics", nil)
	if err != nil {
		p.log.Debug("backend version metrics probe failed", "error", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		p.log.Debug("backend version metrics probe returned non-success status", "http.response.status_code", resp.StatusCode)
		return
	}

	const maxMetricsProbeBytes = 2 << 20 // 2 MiB
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxMetricsProbeBytes))
	if err != nil {
		p.log.Debug("backend version metrics probe read failed", "error", err)
		return
	}
	semver := extractBackendSemverFromMetrics(string(body))
	if semver == "" {
		p.log.Debug("backend version metrics probe found no semver")
		return
	}
	p.storeBackendVersion("metrics:"+semver, semver)
}

func (p *Proxy) probeBackendEndpointSupport(ctx context.Context, path string, params url.Values) bool {
	if p == nil {
		return false
	}
	resp, err := p.vlGet(ctx, path, params)
	if err != nil {
		p.log.Debug("backend endpoint probe failed", "path", path, "error", err)
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	return resp.StatusCode < http.StatusBadRequest
}

func (p *Proxy) probeBackendCapabilitiesFromEndpoints(ctx context.Context) {
	if p == nil {
		return
	}
	q := url.Values{}
	q.Set("query", "*")
	supportsStreamMetadata := p.probeBackendEndpointSupport(ctx, "/select/logsql/stream_field_names", q)

	sub := url.Values{}
	sub.Set("query", "*")
	sub.Set("q", "a")
	sub.Set("filter", "substring")
	supportsMetadataSubstring := p.probeBackendEndpointSupport(ctx, "/select/logsql/field_names", sub)

	p.storeBackendCapabilityProbe("endpoint-probe", supportsStreamMetadata, supportsMetadataSubstring)
}

// ValidateBackendVersionCompatibility checks backend version support at startup.
// It blocks startup only when a detected backend version is below the configured
// minimum and the unsafe override is not enabled.
func (p *Proxy) ValidateBackendVersionCompatibility(ctx context.Context) error {
	if p == nil {
		return nil
	}
	timeout := p.backendVersionCheckTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	checkCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resp, err := p.vlGet(checkCtx, "/health", nil)
	if err != nil {
		p.log.Warn(
			"backend version compatibility check skipped",
			"reason", "health probe failed",
			"error", err,
			"minimum_supported_version", p.backendMinVersion,
		)
		return nil
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		p.log.Warn(
			"backend version compatibility check skipped",
			"reason", "health probe returned non-success status",
			"http.response.status_code", resp.StatusCode,
			"minimum_supported_version", p.backendMinVersion,
		)
		return nil
	}

	raw, semver, profile := p.backendVersionState()
	if semver == "" {
		p.probeBackendVersionFromMetrics(checkCtx)
		raw, semver, profile = p.backendVersionState()
	}
	if semver == "" {
		p.probeBackendCapabilitiesFromEndpoints(checkCtx)
		raw, semver, profile = p.backendVersionState()
	}
	if semver == "" && profile != "" {
		p.log.Warn(
			"backend version compatibility check partial",
			"reason", "explicit backend semver unavailable; inferred capability profile from runtime endpoint probes",
			"backend.version.source", raw,
			"backend.capability_profile", profile,
			"minimum_supported_version", p.backendMinVersion,
		)
		return nil
	}
	if semver == "" {
		p.log.Warn(
			"backend version compatibility check unavailable",
			"reason", "backend did not expose version in response headers or /metrics payload",
			"minimum_supported_version", p.backendMinVersion,
		)
		return nil
	}
	if !semverAtLeastSemver(semver, p.backendMinVersion) {
		if p.backendAllowUnsupportedVersion {
			p.log.Warn(
				"unsupported backend version allowed by override",
				"backend.version.raw", raw,
				"backend.version.semver", semver,
				"backend.capability_profile", profile,
				"minimum_supported_version", p.backendMinVersion,
				"flag", "backend-allow-unsupported-version",
			)
			return nil
		}
		return fmt.Errorf(
			"detected backend version %s (%s) is below minimum supported %s; compatibility may be incomplete. Set -backend-allow-unsupported-version=true to bypass at your own risk",
			semver,
			raw,
			p.backendMinVersion,
		)
	}

	p.log.Info(
		"backend version compatibility check passed",
		"backend.version.raw", raw,
		"backend.version.semver", semver,
		"backend.capability_profile", profile,
		"minimum_supported_version", p.backendMinVersion,
	)
	return nil
}

func (p *Proxy) vlGet(ctx context.Context, path string, params url.Values) (*http.Response, error) {
	if !p.breaker.Allow() {
		return nil, fmt.Errorf("circuit breaker open — backend unavailable")
	}

	u := *p.backend
	u.Path = path
	u.RawQuery = params.Encode()

	p.log.Debug("VL request", "method", "GET", "url", u.String())
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	p.forwardTenantHeaders(req)
	p.applyBackendHeaders(req)
	start := time.Now()
	resp, err := p.client.Do(req)
	duration := time.Since(start)
	serverPort, _ := strconv.Atoi(u.Port())
	if err != nil {
		mappedStatus := statusFromUpstreamErr(err)
		recordUpstreamCall(ctx, mappedStatus, duration, true)
		p.recordUpstreamObservation(ctx, "vl", http.MethodGet, path, u.Hostname(), serverPort, mappedStatus, duration, err)
		if shouldRecordBreakerFailure(err) {
			p.breaker.RecordFailure()
		}
		return nil, err
	}
	p.observeBackendVersionFromHeaders(resp.Header)
	if err := decodeCompressedHTTPResponse(resp); err != nil {
		_ = resp.Body.Close()
		recordUpstreamCall(ctx, http.StatusBadGateway, duration, true)
		p.recordUpstreamObservation(ctx, "vl", http.MethodGet, path, u.Hostname(), serverPort, http.StatusBadGateway, duration, err)
		return nil, fmt.Errorf("decode backend response: %w", err)
	}
	recordUpstreamCall(ctx, resp.StatusCode, duration, false)
	p.recordUpstreamObservation(ctx, "vl", http.MethodGet, path, u.Hostname(), serverPort, resp.StatusCode, duration, nil)
	// Any completed HTTP response proves backend reachability; keep breaker for transport failures only.
	p.breaker.RecordSuccess()
	return resp, nil
}

func (p *Proxy) vlPost(ctx context.Context, path string, params url.Values) (*http.Response, error) {
	if !p.breaker.Allow() {
		return nil, fmt.Errorf("circuit breaker open — backend unavailable")
	}

	u := *p.backend
	u.Path = path

	p.log.Debug("VL request", "method", "POST", "url", u.String(), "params", params.Encode())
	req, err := http.NewRequestWithContext(ctx, "POST", u.String(), strings.NewReader(params.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	p.forwardTenantHeaders(req)
	p.applyBackendHeaders(req)
	start := time.Now()
	resp, err := p.client.Do(req)
	duration := time.Since(start)
	serverPort, _ := strconv.Atoi(u.Port())
	if err != nil {
		mappedStatus := statusFromUpstreamErr(err)
		recordUpstreamCall(ctx, mappedStatus, duration, true)
		p.recordUpstreamObservation(ctx, "vl", http.MethodPost, path, u.Hostname(), serverPort, mappedStatus, duration, err)
		if shouldRecordBreakerFailure(err) {
			p.breaker.RecordFailure()
		}
		return nil, err
	}
	p.observeBackendVersionFromHeaders(resp.Header)
	if err := decodeCompressedHTTPResponse(resp); err != nil {
		_ = resp.Body.Close()
		recordUpstreamCall(ctx, http.StatusBadGateway, duration, true)
		p.recordUpstreamObservation(ctx, "vl", http.MethodPost, path, u.Hostname(), serverPort, http.StatusBadGateway, duration, err)
		return nil, fmt.Errorf("decode backend response: %w", err)
	}
	recordUpstreamCall(ctx, resp.StatusCode, duration, false)
	p.recordUpstreamObservation(ctx, "vl", http.MethodPost, path, u.Hostname(), serverPort, resp.StatusCode, duration, nil)
	// Any completed HTTP response proves backend reachability; keep breaker for transport failures only.
	p.breaker.RecordSuccess()
	return resp, nil
}

// vlGetCoalesced wraps vlGet with request coalescing.
func (p *Proxy) vlGetCoalesced(ctx context.Context, key, path string, params url.Values) ([]byte, error) {
	status, body, err := p.vlGetCoalescedWithStatus(ctx, key, path, params)
	if err != nil {
		return nil, err
	}
	if status >= http.StatusBadRequest {
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = fmt.Sprintf("VL backend returned %d", status)
		}
		return nil, errors.New(msg)
	}
	return body, nil
}

// vlGetCoalescedWithStatus wraps vlGet with request coalescing and returns status + body.
func (p *Proxy) vlGetCoalescedWithStatus(ctx context.Context, key, path string, params url.Values) (int, []byte, error) {
	status, _, body, err := p.coalescer.Do(key, func() (*http.Response, error) {
		return p.vlGet(ctx, path, params)
	})
	if err != nil {
		return 0, nil, err
	}
	return status, body, nil
}

// vlPostCoalesced wraps vlPost with request coalescing and returns status + body.
func (p *Proxy) vlPostCoalesced(ctx context.Context, key, path string, params url.Values) (int, []byte, error) {
	status, _, body, err := p.coalescer.Do(key, func() (*http.Response, error) {
		return p.vlPost(ctx, path, params)
	})
	if err != nil {
		return 0, nil, err
	}
	return status, body, nil
}

// --- Stats query proxying ---

func (p *Proxy) proxyStatsQueryRange(w http.ResponseWriter, r *http.Request, logsqlQuery string) {
	params := url.Values{}
	params.Set("query", logsqlQuery)
	if s := r.FormValue("start"); s != "" {
		params.Set("start", formatVLTimestamp(s))
	}
	if e := r.FormValue("end"); e != "" {
		params.Set("end", formatVLTimestamp(e))
	}
	if step := r.FormValue("step"); step != "" {
		params.Set("step", formatVLStep(step))
	}

	resp, err := p.vlPost(r.Context(), "/select/logsql/stats_query_range", params)
	if err != nil {
		p.writeError(w, statusFromUpstreamErr(err), err.Error())
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	// Propagate VL error status
	if resp.StatusCode >= 400 {
		p.writeError(w, resp.StatusCode, string(body))
		return
	}

	// VL stats_query_range returns Prometheus-compatible format.
	// Just wrap it in Loki's envelope.
	// Translate label names (e.g., dots → underscores) in metric labels.
	body = p.translateStatsResponseLabels(body, r.FormValue("query"))
	w.Header().Set("Content-Type", "application/json")
	w.Write(wrapAsLokiResponse(body, "matrix"))
}

func (p *Proxy) proxyStatsQuery(w http.ResponseWriter, r *http.Request, logsqlQuery string) {
	params := url.Values{}
	params.Set("query", logsqlQuery)
	if t := r.FormValue("time"); t != "" {
		params.Set("time", formatVLTimestamp(t))
	}

	resp, err := p.vlPost(r.Context(), "/select/logsql/stats_query", params)
	if err != nil {
		p.writeError(w, statusFromUpstreamErr(err), err.Error())
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	// Propagate VL error status
	if resp.StatusCode >= 400 {
		p.writeError(w, resp.StatusCode, string(body))
		return
	}

	body = p.translateStatsResponseLabels(body, r.FormValue("query"))
	w.Header().Set("Content-Type", "application/json")
	w.Write(wrapAsLokiResponse(body, "vector"))
}

// proxyBinaryMetricQueryRangeVM evaluates with vector matching (on/ignoring/group_left/group_right).
func (p *Proxy) proxyBinaryMetricQueryRangeVM(w http.ResponseWriter, r *http.Request, op, leftQL, rightQL string, vm *translator.VectorMatchInfo) {
	p.proxyBinaryMetricVM(w, r, op, leftQL, rightQL, "stats_query_range", "matrix", vm)
}

func (p *Proxy) proxyBinaryMetricQueryVM(w http.ResponseWriter, r *http.Request, op, leftQL, rightQL string, vm *translator.VectorMatchInfo) {
	p.proxyBinaryMetricVM(w, r, op, leftQL, rightQL, "stats_query", "vector", vm)
}

func (p *Proxy) proxyBinaryMetricVM(w http.ResponseWriter, r *http.Request, op, leftQL, rightQL, vlEndpoint, resultType string, vm *translator.VectorMatchInfo) {
	// If no vector matching, fall back to default behavior
	if vm == nil || (len(vm.On) == 0 && len(vm.Ignoring) == 0 && len(vm.GroupLeft) == 0 && len(vm.GroupRight) == 0) {
		p.proxyBinaryMetric(w, r, op, leftQL, rightQL, vlEndpoint, resultType)
		return
	}

	isRange := vlEndpoint == "stats_query_range"
	buildParams := func(query string) url.Values {
		params := url.Values{"query": {query}}
		if isRange {
			if s := r.FormValue("start"); s != "" {
				params.Set("start", formatVLTimestamp(s))
			}
			if e := r.FormValue("end"); e != "" {
				params.Set("end", formatVLTimestamp(e))
			}
			if step := r.FormValue("step"); step != "" {
				params.Set("step", formatVLStep(step))
			}
		} else {
			if t := r.FormValue("time"); t != "" {
				params.Set("time", formatVLTimestamp(t))
			}
		}
		return params
	}

	leftIsScalar := translator.IsScalar(leftQL)
	rightIsScalar := translator.IsScalar(rightQL)

	var leftBody, rightBody []byte
	if leftIsScalar {
		leftBody = []byte(`{"status":"success","data":{"resultType":"scalar","result":[0,"` + leftQL + `"]}}`)
	} else {
		resp, e := p.vlPost(r.Context(), "/select/logsql/"+vlEndpoint, buildParams(leftQL))
		if e != nil {
			p.writeError(w, statusFromUpstreamErr(e), "left query: "+e.Error())
			return
		}
		defer resp.Body.Close()
		leftBody, _ = io.ReadAll(resp.Body)
	}

	if rightIsScalar {
		rightBody = []byte(`{"status":"success","data":{"resultType":"scalar","result":[0,"` + rightQL + `"]}}`)
	} else {
		resp, e := p.vlPost(r.Context(), "/select/logsql/"+vlEndpoint, buildParams(rightQL))
		if e != nil {
			p.writeError(w, statusFromUpstreamErr(e), "right query: "+e.Error())
			return
		}
		defer resp.Body.Close()
		rightBody, _ = io.ReadAll(resp.Body)
	}

	// Apply vector matching: on(), ignoring(), group_left(), group_right()
	var result []byte
	if len(vm.On) > 0 {
		result = applyOnMatching(leftBody, rightBody, op, vm.On, resultType)
	} else if len(vm.Ignoring) > 0 {
		result = applyIgnoringMatching(leftBody, rightBody, op, vm.Ignoring, resultType)
	} else {
		// group_left/group_right without on/ignoring — use default matching
		result = combineBinaryMetricResults(leftBody, rightBody, op, resultType, leftIsScalar, rightIsScalar, leftQL, rightQL)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
}

func (p *Proxy) proxyBinaryMetric(w http.ResponseWriter, r *http.Request, op, leftQL, rightQL, vlEndpoint, resultType string) {
	isRange := vlEndpoint == "stats_query_range"

	buildParams := func(query string) url.Values {
		params := url.Values{"query": {query}}
		if isRange {
			if s := r.FormValue("start"); s != "" {
				params.Set("start", formatVLTimestamp(s))
			}
			if e := r.FormValue("end"); e != "" {
				params.Set("end", formatVLTimestamp(e))
			}
			if step := r.FormValue("step"); step != "" {
				params.Set("step", formatVLStep(step))
			}
		} else {
			if t := r.FormValue("time"); t != "" {
				params.Set("time", formatVLTimestamp(t))
			}
		}
		return params
	}

	// Check if either side is a scalar (number)
	leftIsScalar := translator.IsScalar(leftQL)
	rightIsScalar := translator.IsScalar(rightQL)

	var leftBody, rightBody []byte

	if leftIsScalar {
		leftBody = []byte(`{"status":"success","data":{"resultType":"scalar","result":[0,"` + leftQL + `"]}}`)
	} else {
		resp, e := p.vlPost(r.Context(), "/select/logsql/"+vlEndpoint, buildParams(leftQL))
		if e != nil {
			p.writeError(w, statusFromUpstreamErr(e), "left query: "+e.Error())
			return
		}
		defer resp.Body.Close()
		leftBody, _ = io.ReadAll(resp.Body)
	}

	if rightIsScalar {
		rightBody = []byte(`{"status":"success","data":{"resultType":"scalar","result":[0,"` + rightQL + `"]}}`)
	} else {
		resp, e := p.vlPost(r.Context(), "/select/logsql/"+vlEndpoint, buildParams(rightQL))
		if e != nil {
			p.writeError(w, statusFromUpstreamErr(e), "right query: "+e.Error())
			return
		}
		defer resp.Body.Close()
		rightBody, _ = io.ReadAll(resp.Body)
	}

	// Combine results with arithmetic at proxy level
	result := combineBinaryMetricResults(leftBody, rightBody, op, resultType, leftIsScalar, rightIsScalar, leftQL, rightQL)

	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
}

// combineBinaryMetricResults applies arithmetic op to two VL stats results.
func combineBinaryMetricResults(leftBody, rightBody []byte, op, resultType string, leftScalar, rightScalar bool, leftQL, rightQL string) []byte {
	// For scalar operations (e.g., rate(...) * 100), apply to each value
	if rightScalar {
		scalar := parseScalar(rightQL)
		return applyScalarOp(leftBody, op, scalar, resultType)
	}
	if leftScalar {
		scalar := parseScalar(leftQL)
		return applyScalarOpReverse(rightBody, op, scalar, resultType)
	}

	// Both sides are metric results — combine point-by-point
	// This is a simplified implementation that handles the common case
	// of matching time series (same labels, same timestamps)
	return combineMetricResults(leftBody, rightBody, op, resultType)
}

func parseScalar(s string) float64 {
	f, _ := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return f
}

func applyScalarOp(body []byte, op string, scalar float64, resultType string) []byte {
	var vlResp map[string]interface{}
	if err := json.Unmarshal(body, &vlResp); err != nil {
		return wrapAsLokiResponse(body, resultType)
	}

	// VL stats_query_range returns {"results": [{"metric":{}, "values": [[ts, val]...]}...]}
	results, _ := vlResp["results"].([]interface{})
	for _, r := range results {
		rm, _ := r.(map[string]interface{})
		values, _ := rm["values"].([]interface{})
		for i, v := range values {
			point, _ := v.([]interface{})
			if len(point) >= 2 {
				valStr, _ := point[1].(string)
				val, _ := strconv.ParseFloat(valStr, 64)
				newVal := applyOp(val, scalar, op)
				point[1] = strconv.FormatFloat(newVal, 'f', -1, 64)
				values[i] = point
			}
		}
	}

	result, _ := json.Marshal(vlResp)
	return wrapAsLokiResponse(result, resultType)
}

func applyScalarOpReverse(body []byte, op string, scalar float64, resultType string) []byte {
	var vlResp map[string]interface{}
	if err := json.Unmarshal(body, &vlResp); err != nil {
		return wrapAsLokiResponse(body, resultType)
	}

	results, _ := vlResp["results"].([]interface{})
	for _, r := range results {
		rm, _ := r.(map[string]interface{})
		values, _ := rm["values"].([]interface{})
		for i, v := range values {
			point, _ := v.([]interface{})
			if len(point) >= 2 {
				valStr, _ := point[1].(string)
				val, _ := strconv.ParseFloat(valStr, 64)
				newVal := applyOp(scalar, val, op)
				point[1] = strconv.FormatFloat(newVal, 'f', -1, 64)
				values[i] = point
			}
		}
	}

	result, _ := json.Marshal(vlResp)
	return wrapAsLokiResponse(result, resultType)
}

func combineMetricResults(leftBody, rightBody []byte, op, resultType string) []byte {
	// Parse both results
	var leftResp, rightResp map[string]interface{}
	json.Unmarshal(leftBody, &leftResp)
	json.Unmarshal(rightBody, &rightResp)

	leftResults, _ := leftResp["results"].([]interface{})
	rightResults, _ := rightResp["results"].([]interface{})

	// Build a map of right results by metric labels for joining
	rightMap := make(map[string][]interface{})
	for _, r := range rightResults {
		rm, _ := r.(map[string]interface{})
		metric, _ := rm["metric"].(map[string]interface{})
		key := metricKey(metric)
		values, _ := rm["values"].([]interface{})
		rightMap[key] = values
	}

	// Combine: for each left result, find matching right result and apply op
	for _, r := range leftResults {
		rm, _ := r.(map[string]interface{})
		metric, _ := rm["metric"].(map[string]interface{})
		key := metricKey(metric)
		leftValues, _ := rm["values"].([]interface{})
		rightValues := rightMap[key]

		if len(rightValues) > 0 {
			// Build timestamp→value index for right side
			rightIdx := make(map[string]float64)
			for _, v := range rightValues {
				point, _ := v.([]interface{})
				if len(point) >= 2 {
					ts := fmt.Sprintf("%v", point[0])
					valStr, _ := point[1].(string)
					val, _ := strconv.ParseFloat(valStr, 64)
					rightIdx[ts] = val
				}
			}

			// Apply op point-by-point
			for i, v := range leftValues {
				point, _ := v.([]interface{})
				if len(point) >= 2 {
					ts := fmt.Sprintf("%v", point[0])
					leftValStr, _ := point[1].(string)
					leftVal, _ := strconv.ParseFloat(leftValStr, 64)
					if rightVal, ok := rightIdx[ts]; ok {
						newVal := applyOp(leftVal, rightVal, op)
						point[1] = strconv.FormatFloat(newVal, 'f', -1, 64)
					}
					leftValues[i] = point
				}
			}
		}
	}

	result, _ := json.Marshal(leftResp)
	return wrapAsLokiResponse(result, resultType)
}

func applyOp(a, b float64, op string) float64 {
	switch op {
	case "/":
		if b == 0 {
			return 0 // avoid division by zero, return 0 like Prometheus
		}
		return a / b
	case "*":
		return a * b
	case "+":
		return a + b
	case "-":
		return a - b
	case "%":
		if b == 0 {
			return 0
		}
		return math.Mod(a, b)
	case "^":
		return math.Pow(a, b)
	case "==":
		if a == b {
			return 1
		}
		return 0
	case "!=":
		if a != b {
			return 1
		}
		return 0
	case ">":
		if a > b {
			return 1
		}
		return 0
	case "<":
		if a < b {
			return 1
		}
		return 0
	case ">=":
		if a >= b {
			return 1
		}
		return 0
	case "<=":
		if a <= b {
			return 1
		}
		return 0
	}
	return a
}

func metricKey(metric map[string]interface{}) string {
	if metric == nil {
		return "{}"
	}
	parts := make([]string, 0, len(metric))
	for k, v := range metric {
		parts = append(parts, fmt.Sprintf("%s=%v", k, v))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// proxyLogQuery fetches log lines from VictoriaLogs.
func (p *Proxy) proxyLogQuery(w http.ResponseWriter, r *http.Request, logsqlQuery string) {
	// Loki direction param: "forward" = oldest first, "backward" (default) = newest first
	direction := r.FormValue("direction")
	if direction == "forward" {
		logsqlQuery += " | sort by (_time)"
	} else {
		logsqlQuery += " | sort by (_time desc)"
	}

	params := url.Values{}
	params.Set("query", logsqlQuery)
	if s := r.FormValue("start"); s != "" {
		params.Set("start", formatVLTimestamp(s))
	}
	if e := r.FormValue("end"); e != "" {
		params.Set("end", formatVLTimestamp(e))
	}
	limit := r.FormValue("limit")
	if limit == "" {
		limit = strconv.Itoa(p.maxLines)
	}
	params.Set("limit", sanitizeLimit(limit))

	resp, err := p.vlPost(r.Context(), "/select/logsql/query", params)
	if err != nil {
		p.writeError(w, statusFromUpstreamErr(err), err.Error())
		return
	}
	defer resp.Body.Close()

	// Propagate VL error status to the client
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		errMsg := string(body)
		if errMsg == "" {
			errMsg = fmt.Sprintf("VL backend returned %d", resp.StatusCode)
		}
		p.writeError(w, resp.StatusCode, errMsg)
		return
	}

	// Chunked streaming: flush partial results as they arrive from VL
	categorizedLabels := requestWantsCategorizedLabels(r)
	emitStructuredMetadata := p.shouldEmitStructuredMetadata(r)
	p.metrics.RecordTupleMode(tupleModeForRequest(categorizedLabels, emitStructuredMetadata))
	if p.streamResponse {
		p.streamLogQuery(w, resp, r.FormValue("query"), categorizedLabels, emitStructuredMetadata)
		return
	}

	body, _ := io.ReadAll(resp.Body)
	p.maybeAutodetectPatternsFromNDJSON(
		r.Header.Get("X-Scope-OrgID"),
		r.FormValue("query"),
		r.FormValue("start"),
		r.FormValue("end"),
		r.FormValue("step"),
		body,
	)

	// VL returns newline-delimited JSON, each line is a log entry.
	// Loki expects: {"status":"success","data":{"resultType":"streams","result":[...]}}
	streams := p.vlLogsToLokiStreams(body, r.FormValue("query"), categorizedLabels, emitStructuredMetadata)

	// Apply derived fields (extract trace_id etc. from log lines)
	if len(p.derivedFields) > 0 {
		p.applyDerivedFields(streams)
	}

	// Apply proxy-side post-processing for Loki features VL doesn't natively support.
	// These are applied after VL returns results, implementing Loki behavior at the proxy.
	// TODO: Remove each when VL adds native equivalents.
	logqlQuery := r.FormValue("query")
	if strings.Contains(logqlQuery, "decolorize") {
		decolorizeStreams(streams)
	}
	if label, cidr, ok := parseIPFilter(logqlQuery); ok {
		streams = ipFilterStreams(streams, label, cidr)
	}
	if tmpl := extractLineFormatTemplate(logqlQuery); tmpl != "" {
		applyLineFormatTemplate(streams, tmpl)
	}

	result, _ := json.Marshal(map[string]interface{}{
		"status": "success",
		"data": func() map[string]interface{} {
			data := map[string]interface{}{
				"resultType": "streams",
				"result":     streams,
				"stats":      map[string]interface{}{},
			}
			if categorizedLabels {
				data["encodingFlags"] = []string{"categorize-labels"}
			}
			return data
		}(),
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
}

// streamLogQuery streams VL NDJSON response as chunked Loki-compatible JSON.
func (p *Proxy) streamLogQuery(w http.ResponseWriter, resp *http.Response, originalQuery string, categorizedLabels bool, emitStructuredMetadata bool) {
	flusher, canFlush := w.(http.Flusher)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Transfer-Encoding", "chunked")

	// Write opening envelope
	openEnvelope := `{"status":"success","data":{"resultType":"streams"`
	if categorizedLabels {
		openEnvelope += `,"encodingFlags":["categorize-labels"]`
	}
	openEnvelope += `,"result":[`
	w.Write([]byte(openEnvelope))
	if canFlush {
		flusher.Flush()
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	first := true
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var entry map[string]interface{}
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}

		timeStr, ok := stringifyEntryValue(entry["_time"])
		if !ok || timeStr == "" {
			continue
		}
		msg, _ := stringifyEntryValue(entry["_msg"])

		tsNanos, ok := formatEntryTimestamp(timeStr)
		if !ok {
			continue
		}

		labels, structuredMetadata, parsedFields := p.classifyEntryFields(entry, originalQuery)
		translatedLabels := labels
		if !p.labelTranslator.IsPassthrough() {
			translatedLabels = p.labelTranslator.TranslateLabelsMap(labels)
		}
		ensureDetectedLevel(translatedLabels)
		ensureSyntheticServiceName(translatedLabels)

		stream := map[string]interface{}{
			"stream": translatedLabels,
			"values": buildStreamValues(tsNanos, msg, structuredMetadata, parsedFields, emitStructuredMetadata, categorizedLabels),
		}

		chunk, _ := json.Marshal(stream)
		if !first {
			w.Write([]byte(","))
		}
		w.Write(chunk)
		first = false

		if canFlush {
			flusher.Flush()
		}
	}

	// Close envelope
	w.Write([]byte(`],"stats":{}}}`))
	if canFlush {
		flusher.Flush()
	}
}

// applyDerivedFields extracts values from log lines using regex and adds them as labels.
// This enables Grafana's "Derived fields" feature for trace linking.
func (p *Proxy) applyDerivedFields(streams []map[string]interface{}) {
	// Pre-compile regexes
	type compiledDF struct {
		name string
		re   *regexp.Regexp
		url  string
	}
	compiled := make([]compiledDF, 0, len(p.derivedFields))
	for _, df := range p.derivedFields {
		re, err := regexp.Compile(df.MatcherRegex)
		if err != nil {
			p.log.Warn("invalid derived field regex", "name", df.Name, "error", err)
			continue
		}
		compiled = append(compiled, compiledDF{name: df.Name, re: re, url: df.URL})
	}

	for _, stream := range streams {
		values, ok := stream["values"].([]interface{})
		if !ok {
			continue
		}
		labels := streamStringMap(stream["stream"])
		if labels == nil {
			continue
		}

		for _, val := range values {
			pair, ok := val.([]interface{})
			if !ok || len(pair) < 2 {
				continue
			}
			line, ok := pair[1].(string)
			if !ok {
				continue
			}
			for _, cdf := range compiled {
				matches := cdf.re.FindStringSubmatch(line)
				if len(matches) > 1 {
					// Use first capture group as the value
					labels[cdf.name] = matches[1]
				} else if len(matches) == 1 {
					// Full match, no capture group
					labels[cdf.name] = matches[0]
				}
			}
		}
	}
}

func streamStringMap(value interface{}) map[string]string {
	switch labels := value.(type) {
	case map[string]string:
		return labels
	case map[string]interface{}:
		result := make(map[string]string, len(labels))
		for k, v := range labels {
			if s, ok := v.(string); ok {
				result[k] = s
			}
		}
		return result
	default:
		return nil
	}
}

// --- Response converters ---

func lokiLabelsResponse(labels []string) []byte {
	result, _ := json.Marshal(map[string]interface{}{
		"status": "success",
		"data":   labels,
	})
	return result
}

// vlEntryPool pools map[string]interface{} to reduce GC pressure in NDJSON parsing.
var vlEntryPool = sync.Pool{
	New: func() interface{} {
		return make(map[string]interface{}, 8)
	},
}

// vlLogsToLokiStreams converts VL newline-delimited JSON logs to Loki streams format.
// Optimized: byte scanning, pooled maps, pre-allocated slices.
func vlLogsToLokiStreams(body []byte) []map[string]interface{} {
	type streamEntry struct {
		Labels map[string]string
		Values [][]string // [[timestamp_ns, line], ...]
	}
	// Estimate line count from body size (~200 bytes/line average)
	estimatedLines := len(body)/200 + 1
	streamMap := make(map[string]*streamEntry, estimatedLines/10+1)

	// Scan lines without copying the entire body to a string
	start := 0
	for i := 0; i <= len(body); i++ {
		if i < len(body) && body[i] != '\n' {
			continue
		}
		line := body[start:i]
		start = i + 1

		// Trim whitespace (avoid bytes.TrimSpace allocation)
		for len(line) > 0 && (line[0] == ' ' || line[0] == '\t' || line[0] == '\r') {
			line = line[1:]
		}
		for len(line) > 0 && (line[len(line)-1] == ' ' || line[len(line)-1] == '\t' || line[len(line)-1] == '\r') {
			line = line[:len(line)-1]
		}
		if len(line) == 0 {
			continue
		}

		// Use pooled map to reduce allocations
		entry := vlEntryPool.Get().(map[string]interface{})
		for k := range entry {
			delete(entry, k) // clear reused map
		}
		if err := json.Unmarshal(line, &entry); err != nil {
			vlEntryPool.Put(entry)
			continue
		}

		// Extract _time, _msg, _stream, and remaining fields as labels
		timeStr, _ := entry["_time"].(string)
		msg, _ := entry["_msg"].(string)
		if timeStr == "" {
			vlEntryPool.Put(entry)
			continue
		}

		// Parse time to nanoseconds
		ts, err := time.Parse(time.RFC3339Nano, timeStr)
		if err != nil {
			vlEntryPool.Put(entry)
			continue
		}
		tsNanos := strconv.FormatInt(ts.UnixNano(), 10)

		labels := buildEntryLabels(entry)
		streamKey := canonicalLabelsKey(labels)

		se, ok := streamMap[streamKey]
		if !ok {
			se = &streamEntry{
				Labels: labels,
				Values: make([][]string, 0),
			}
			streamMap[streamKey] = se
		}

		// Return pooled entry after extracting all needed data
		vlEntryPool.Put(entry)

		se.Values = append(se.Values, []string{tsNanos, msg})
	}

	result := make([]map[string]interface{}, 0, len(streamMap))
	for _, se := range streamMap {
		result = append(result, map[string]interface{}{
			"stream": se.Labels,
			"values": se.Values,
		})
	}
	return result
}

func (p *Proxy) vlLogsToLokiStreams(body []byte, originalQuery string, categorizedLabels bool, emitStructuredMetadata bool) []map[string]interface{} {
	type streamEntry struct {
		Labels map[string]string
		Values []interface{}
	}

	streamMap := make(map[string]*streamEntry, len(body)/256+1)
	start := 0
	for i := 0; i <= len(body); i++ {
		if i < len(body) && body[i] != '\n' {
			continue
		}
		line := body[start:i]
		start = i + 1

		for len(line) > 0 && (line[0] == ' ' || line[0] == '\t' || line[0] == '\r') {
			line = line[1:]
		}
		for len(line) > 0 && (line[len(line)-1] == ' ' || line[len(line)-1] == '\t' || line[len(line)-1] == '\r') {
			line = line[:len(line)-1]
		}
		if len(line) == 0 {
			continue
		}

		entry := vlEntryPool.Get().(map[string]interface{})
		for k := range entry {
			delete(entry, k)
		}
		if err := json.Unmarshal(line, &entry); err != nil {
			vlEntryPool.Put(entry)
			continue
		}

		timeStr, ok := stringifyEntryValue(entry["_time"])
		if !ok || timeStr == "" {
			vlEntryPool.Put(entry)
			continue
		}
		tsNanos, ok := formatEntryTimestamp(timeStr)
		if !ok {
			vlEntryPool.Put(entry)
			continue
		}
		msg, _ := stringifyEntryValue(entry["_msg"])

		labels, structuredMetadata, parsedFields := p.classifyEntryFields(entry, originalQuery)
		streamKey := canonicalLabelsKey(labels)
		se, ok := streamMap[streamKey]
		if !ok {
			translatedLabels := labels
			if !p.labelTranslator.IsPassthrough() {
				translatedLabels = p.labelTranslator.TranslateLabelsMap(labels)
			}
			ensureDetectedLevel(translatedLabels)
			ensureSyntheticServiceName(translatedLabels)

			se = &streamEntry{
				Labels: translatedLabels,
				Values: make([]interface{}, 0, 8),
			}
			streamMap[streamKey] = se
		}
		se.Values = append(se.Values, buildStreamValue(tsNanos, msg, structuredMetadata, parsedFields, emitStructuredMetadata, categorizedLabels))
		vlEntryPool.Put(entry)
	}

	result := make([]map[string]interface{}, 0, len(streamMap))
	for _, se := range streamMap {
		result = append(result, map[string]interface{}{
			"stream": se.Labels,
			"values": se.Values,
		})
	}
	return result
}

func stringifyEntryValue(value interface{}) (string, bool) {
	if value == nil {
		return "", false
	}
	switch v := value.(type) {
	case string:
		return v, true
	default:
		return fmt.Sprintf("%v", v), true
	}
}

func formatEntryTimestamp(timeStr string) (string, bool) {
	if parsed, err := time.Parse(time.RFC3339Nano, timeStr); err == nil {
		return strconv.FormatInt(parsed.UnixNano(), 10), true
	}
	if parsed, err := time.Parse(time.RFC3339, timeStr); err == nil {
		return strconv.FormatInt(parsed.UnixNano(), 10), true
	}
	if _, err := strconv.ParseInt(timeStr, 10, 64); err == nil {
		return timeStr, true
	}
	return "", false
}

func buildStreamValues(ts, msg string, structuredMetadata map[string]string, parsedFields map[string]string, emitStructuredMetadata bool, categorizedLabels bool) []interface{} {
	return []interface{}{buildStreamValue(ts, msg, structuredMetadata, parsedFields, emitStructuredMetadata, categorizedLabels)}
}

func buildStreamValue(ts, msg string, structuredMetadata map[string]string, parsedFields map[string]string, emitStructuredMetadata bool, categorizedLabels bool) interface{} {
	if !categorizedLabels {
		return []interface{}{ts, msg}
	}

	metadata := map[string]interface{}{}
	if emitStructuredMetadata {
		if len(structuredMetadata) > 0 {
			metadata["structuredMetadata"] = metadataFieldMap(structuredMetadata)
		}
		if len(parsedFields) > 0 {
			metadata["parsed"] = metadataFieldMap(parsedFields)
		}
	}
	return []interface{}{ts, msg, metadata}
}

func metadataFieldMap(fields map[string]string) map[string]string {
	if len(fields) == 0 {
		return nil
	}
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	pairs := make(map[string]string, len(keys))
	for _, key := range keys {
		pairs[key] = fields[key]
	}
	return pairs
}

func (p *Proxy) classifyEntryFields(entry map[string]interface{}, originalQuery string) (map[string]string, map[string]string, map[string]string) {
	labels := parseStreamLabels(asString(entry["_stream"]))
	if value, ok := stringifyEntryValue(entry["level"]); ok && strings.TrimSpace(value) != "" {
		labels["level"] = value
	}
	ensureDetectedLevel(labels)
	ensureSyntheticServiceName(labels)

	var (
		parsedFields             map[string]string
		structuredMetadataFields map[string]string
	)
	classifyAsParsed := hasParserStage(originalQuery, "json") || hasParserStage(originalQuery, "logfmt")

	for key, value := range entry {
		if isVLInternalField(key) || key == "_stream_id" || key == "level" {
			continue
		}
		if _, exists := labels[key]; exists {
			continue
		}
		stringValue, ok := stringifyEntryValue(value)
		if !ok || strings.TrimSpace(stringValue) == "" {
			continue
		}
		for _, exposure := range p.metadataFieldExposures(key) {
			if _, exists := labels[exposure.name]; exists && !exposure.isAlias {
				continue
			}
			if classifyAsParsed {
				if parsedFields == nil {
					parsedFields = make(map[string]string, 4)
				}
				parsedFields[exposure.name] = stringValue
				continue
			}
			if structuredMetadataFields == nil {
				structuredMetadataFields = make(map[string]string, 4)
			}
			structuredMetadataFields[exposure.name] = stringValue
		}
	}

	return labels, structuredMetadataFields, parsedFields
}

// parseStreamLabels parses {key="value",key2="value2"} into a map.
func parseStreamLabels(s string) map[string]string {
	labels := make(map[string]string)
	s = strings.Trim(s, "{}")
	if s == "" {
		return labels
	}

	// Simple parser for key="value" pairs
	for _, pair := range splitLabelPairs(s) {
		pair = strings.TrimSpace(pair)
		eqIdx := strings.Index(pair, "=")
		if eqIdx <= 0 {
			continue
		}
		key := strings.TrimSpace(pair[:eqIdx])
		val := strings.TrimSpace(pair[eqIdx+1:])
		val = strings.Trim(val, `"`)
		labels[key] = val
	}
	return labels
}

func splitLabelPairs(s string) []string {
	var pairs []string
	inQuote := false
	start := 0
	for i, c := range s {
		if c == '"' {
			inQuote = !inQuote
		}
		if c == ',' && !inQuote {
			pairs = append(pairs, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		pairs = append(pairs, s[start:])
	}
	return pairs
}

func wrapAsLokiResponse(vlBody []byte, resultType string) []byte {
	// VL stats endpoints return Prometheus-compatible format already.
	// Try to parse and re-wrap in Loki envelope.
	var promResp map[string]interface{}
	if err := json.Unmarshal(vlBody, &promResp); err != nil {
		// If we can't parse, return as-is with wrapper
		result, _ := json.Marshal(map[string]interface{}{
			"status": "success",
			"data": map[string]interface{}{
				"resultType": resultType,
				"result":     []interface{}{},
			},
		})
		return result
	}

	// Check for VL error responses and translate to Loki error format.
	// VL may return: {"error":"message"} or {"status":"error","msg":"message"}
	if errMsg, ok := promResp["error"].(string); ok {
		result, _ := json.Marshal(map[string]interface{}{
			"status":    "error",
			"errorType": "bad_request",
			"error":     errMsg,
		})
		return result
	}
	if promResp["status"] == "error" {
		errMsg := ""
		if msg, ok := promResp["msg"].(string); ok {
			errMsg = msg
		} else if msg, ok := promResp["message"].(string); ok {
			errMsg = msg
		} else if msg, ok := promResp["error"].(string); ok {
			errMsg = msg
		}
		result, _ := json.Marshal(map[string]interface{}{
			"status":    "error",
			"errorType": "bad_request",
			"error":     errMsg,
		})
		return result
	}

	// If VL already returned status/data format, pass through
	if _, ok := promResp["data"]; ok {
		result, _ := json.Marshal(map[string]interface{}{
			"status": "success",
			"data":   promResp["data"],
		})
		return result
	}

	result, _ := json.Marshal(map[string]interface{}{
		"status": "success",
		"data":   promResp,
	})
	return result
}

// --- VL hits response conversion helpers ---

type vlHitsResponse struct {
	Hits []struct {
		Fields     map[string]string `json:"fields"`
		Timestamps []vlTimestamp     `json:"timestamps"`
		Values     []int             `json:"values"`
	} `json:"hits"`
}

type vlTimestamp string

func (t *vlTimestamp) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || string(data) == "null" {
		*t = ""
		return nil
	}
	if data[0] == '"' {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		*t = vlTimestamp(s)
		return nil
	}
	var n json.Number
	if err := json.Unmarshal(data, &n); err != nil {
		return err
	}
	*t = vlTimestamp(n.String())
	return nil
}

// parseTimestampToUnix converts a VL timestamp (RFC3339 string or numeric) to Unix seconds.
func parseTimestampToUnix(ts string) float64 {
	if parsed, ok := parseFlexibleUnixSeconds(ts); ok {
		return float64(parsed)
	}
	return float64(time.Now().Unix())
}

func parseHits(body []byte) vlHitsResponse {
	var resp vlHitsResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return vlHitsResponse{}
	}
	return resp
}

type requestedBucketRange struct {
	start int64
	end   int64
	step  int64
	count int
}

func parseFlexibleUnixSeconds(raw string) (int64, bool) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return 0, false
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed.Unix(), true
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed.Unix(), true
	}
	if integer, err := strconv.ParseInt(value, 10, 64); err == nil {
		return normalizeUnixSeconds(integer), true
	}
	if floating, err := strconv.ParseFloat(value, 64); err == nil {
		abs := math.Abs(floating)
		switch {
		case abs >= 1_000_000_000_000_000_000:
			return int64(floating / 1_000_000_000), true
		case abs >= 1_000_000_000_000_000:
			return int64(floating / 1_000_000), true
		case abs >= 1_000_000_000_000:
			return int64(floating / 1_000), true
		default:
			return int64(floating), true
		}
	}
	return 0, false
}

func normalizeUnixSeconds(v int64) int64 {
	abs := v
	if abs < 0 {
		abs = -abs
	}
	switch {
	case abs >= 1_000_000_000_000_000_000:
		return v / 1_000_000_000
	case abs >= 1_000_000_000_000_000:
		return v / 1_000_000
	case abs >= 1_000_000_000_000:
		return v / 1_000
	default:
		return v
	}
}

func parseStepSeconds(step string) (int64, bool) {
	value := strings.TrimSpace(step)
	if value == "" {
		return 0, false
	}
	if d, err := time.ParseDuration(value); err == nil && d > 0 {
		return int64(d / time.Second), true
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
		return int64(seconds), true
	}
	return 0, false
}

func parseRequestedBucketRange(start, end, step string) (requestedBucketRange, bool) {
	startSeconds, ok := parseFlexibleUnixSeconds(start)
	if !ok {
		return requestedBucketRange{}, false
	}
	endSeconds, ok := parseFlexibleUnixSeconds(end)
	if !ok || endSeconds < startSeconds {
		return requestedBucketRange{}, false
	}
	stepSeconds, ok := parseStepSeconds(step)
	if !ok || stepSeconds <= 0 {
		return requestedBucketRange{}, false
	}
	count := int(((endSeconds - startSeconds) / stepSeconds) + 1)
	if count <= 0 || count > maxZeroFillBuckets {
		return requestedBucketRange{}, false
	}
	return requestedBucketRange{
		start: startSeconds,
		end:   endSeconds,
		step:  stepSeconds,
		count: count,
	}, true
}

func (br requestedBucketRange) bucketFor(ts int64) (int64, bool) {
	if ts < br.start || ts > br.end {
		return 0, false
	}
	offset := ts - br.start
	return br.start + ((offset / br.step) * br.step), true
}

func patternBackendQueryLimit(start, end, step string, patternLimit int) int {
	if patternLimit <= 0 {
		patternLimit = 50
	}
	if patternLimit > maxPatternResponseLimit {
		patternLimit = maxPatternResponseLimit
	}
	factor := 20
	if bucketRange, ok := parseRequestedBucketRange(start, end, step); ok {
		factor = bucketRange.count
		if factor < 20 {
			factor = 20
		}
		if factor > 200 {
			factor = 200
		}
	}
	limit := patternLimit * factor
	if limit < 1000 {
		limit = 1000
	}
	if limit > maxPatternBackendQueryLimit {
		limit = maxPatternBackendQueryLimit
	}
	return limit
}

func sumHitsValues(body []byte) int {
	hits := parseHits(body)
	total := 0
	for _, h := range hits.Hits {
		for _, v := range h.Values {
			total += v
		}
	}
	return total
}

func (p *Proxy) translateVolumeMetric(fields map[string]string) map[string]string {
	if fields == nil {
		return nil
	}
	translated := fields
	if p != nil && p.labelTranslator != nil && !p.labelTranslator.IsPassthrough() {
		translated = p.labelTranslator.TranslateLabelsMap(fields)
	}
	if translated == nil {
		return nil
	}
	ensureSyntheticServiceName(translated)
	return translated
}

func (p *Proxy) hitsToVolumeVector(body []byte) map[string]interface{} {
	hits := parseHits(body)
	result := make([]map[string]interface{}, 0, len(hits.Hits))
	for _, h := range hits.Hits {
		total := 0
		var lastTS float64
		for i, v := range h.Values {
			total += v
			if i < len(h.Timestamps) {
				lastTS = parseTimestampToUnix(string(h.Timestamps[i]))
			}
		}
		result = append(result, map[string]interface{}{
			"metric": p.translateVolumeMetric(h.Fields),
			"value":  []interface{}{lastTS, strconv.Itoa(total)},
		})
	}
	return map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "vector",
			"result":     result,
		},
	}
}

func (p *Proxy) hitsToVolumeMatrix(body []byte, start, end, step string) map[string]interface{} {
	hits := parseHits(body)
	bucketRange, fillMissing := parseRequestedBucketRange(start, end, step)
	result := make([]map[string]interface{}, 0, len(hits.Hits))
	for _, h := range hits.Hits {
		seriesFill := fillMissing
		if seriesFill {
			counts := make(map[int64]int, len(h.Timestamps))
			mappedSamples := 0
			for i, ts := range h.Timestamps {
				if i >= len(h.Values) {
					continue
				}
				parsedTS, ok := parseFlexibleUnixSeconds(string(ts))
				if !ok {
					continue
				}
				bucket, ok := bucketRange.bucketFor(parsedTS)
				if !ok {
					continue
				}
				counts[bucket] += h.Values[i]
				mappedSamples++
			}
			if mappedSamples == 0 && len(h.Timestamps) > 0 {
				seriesFill = false
			}
			if seriesFill {
				values := make([][]interface{}, 0, bucketRange.count)
				for ts := bucketRange.start; ts <= bucketRange.end; ts += bucketRange.step {
					values = append(values, []interface{}{float64(ts), strconv.Itoa(counts[ts])})
				}
				result = append(result, map[string]interface{}{
					"metric": p.translateVolumeMetric(h.Fields),
					"values": values,
				})
				continue
			}
		}

		values := make([][]interface{}, 0, len(h.Timestamps))
		for i, ts := range h.Timestamps {
			val := 0
			if i < len(h.Values) {
				val = h.Values[i]
			}
			values = append(values, []interface{}{parseTimestampToUnix(string(ts)), strconv.Itoa(val)})
		}
		result = append(result, map[string]interface{}{
			"metric": p.translateVolumeMetric(h.Fields),
			"values": values,
		})
	}
	return map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "matrix",
			"result":     result,
		},
	}
}

// isStatsQuery returns true if the LogsQL query contains a stats pipe.
// It only matches top-level pipes, not strings inside quoted filter values
// (e.g., ~"stats query" must NOT trigger this).
func isStatsQuery(logsqlQuery string) bool {
	// Walk the query, skipping quoted regions
	inQuote := false
	for i := 0; i < len(logsqlQuery); i++ {
		if logsqlQuery[i] == '"' {
			inQuote = !inQuote
			continue
		}
		if inQuote {
			continue
		}
		rest := logsqlQuery[i:]
		if strings.HasPrefix(rest, "| stats ") ||
			strings.HasPrefix(rest, "| rate(") ||
			strings.HasPrefix(rest, "| count(") {
			return true
		}
	}
	return false
}

func formatVLTimestamp(ts string) string {
	// Loki sends Unix timestamps (seconds or nanoseconds).
	// Grafana drilldown resource endpoints send RFC3339 timestamps, while
	// query endpoints usually send numeric Unix values. Normalize RFC3339 to
	// Unix nanoseconds so every VL endpoint sees the same time format.
	if _, err := strconv.ParseFloat(ts, 64); err == nil {
		// Already numeric — preserve caller precision.
		return ts
	}
	if parsed, err := time.Parse(time.RFC3339Nano, ts); err == nil {
		return strconv.FormatInt(parsed.UnixNano(), 10)
	}
	if parsed, err := time.Parse(time.RFC3339, ts); err == nil {
		return strconv.FormatInt(parsed.UnixNano(), 10)
	}
	return ts
}

// formatVLStep converts Loki's step parameter to VL duration format.
// Loki/Prometheus sends step as seconds (numeric string like "60") or duration ("1m").
// VL requires duration strings (e.g., "60s", "1m", "1h").
func formatVLStep(step string) string {
	step = strings.TrimSpace(step)
	if step == "" {
		return step
	}
	// If it's already a duration string (contains letter), pass through
	for _, ch := range step {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') {
			return step
		}
	}
	// Numeric-only: treat as seconds, append "s"
	if _, err := strconv.ParseFloat(step, 64); err == nil {
		return step + "s"
	}
	return step
}

func requestWantsCategorizedLabels(r *http.Request) bool {
	if r == nil {
		return false
	}
	for _, raw := range r.Header.Values("X-Loki-Response-Encoding-Flags") {
		for _, part := range strings.Split(raw, ",") {
			if strings.EqualFold(strings.TrimSpace(part), "categorize-labels") {
				return true
			}
		}
	}
	return false
}

func tupleModeForRequest(categorizedLabels bool, emitStructuredMetadata bool) string {
	if emitStructuredMetadata && categorizedLabels {
		return "categorize_labels_3tuple"
	}
	return "default_2tuple"
}

func (p *Proxy) shouldEmitStructuredMetadata(r *http.Request) bool {
	return p.emitStructuredMetadata && requestWantsCategorizedLabels(r)
}

func (p *Proxy) handleMultiTenantFanout(w http.ResponseWriter, r *http.Request, endpoint string, single func(http.ResponseWriter, *http.Request)) bool {
	orgID := strings.TrimSpace(r.Header.Get("X-Scope-OrgID"))
	if !hasMultiTenantOrgID(orgID) {
		return false
	}
	tenantIDs := splitMultiTenantOrgIDs(orgID)
	if len(tenantIDs) < 2 {
		return false
	}
	if len(tenantIDs) > maxMultiTenantFanout {
		p.writeError(w, http.StatusBadRequest, fmt.Sprintf("multi-tenant fanout exceeds limit of %d tenants", maxMultiTenantFanout))
		return true
	}
	filteredReq, filteredTenants, err := p.applyTenantSelectorFilter(r, tenantIDs)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		return true
	}
	if len(filteredTenants) == 0 {
		p.writeJSON(w, emptyMultiTenantResponse(endpoint))
		return true
	}

	if cacheKey, cacheable := p.multiTenantCacheKey(filteredReq, endpoint); cacheable {
		if cached, ok := p.cache.Get(cacheKey); ok {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(cached)
			return true
		}
	}

	switch endpoint {
	case "detected_fields":
		body, contentType, err := p.multiTenantDetectedFieldsResponse(filteredReq, filteredTenants)
		if err != nil {
			p.writeError(w, http.StatusInternalServerError, "failed to merge multi-tenant response: "+err.Error())
			return true
		}
		w.Header().Set("Content-Type", contentType)
		_, _ = w.Write(body)
		if cacheKey, cacheable := p.multiTenantCacheKey(filteredReq, endpoint); cacheable {
			p.cache.SetWithTTL(cacheKey, body, CacheTTLs[endpoint])
		}
		return true
	case "detected_labels":
		body, contentType, err := p.multiTenantDetectedLabelsResponse(filteredReq, filteredTenants)
		if err != nil {
			p.writeError(w, http.StatusInternalServerError, "failed to merge multi-tenant response: "+err.Error())
			return true
		}
		w.Header().Set("Content-Type", contentType)
		_, _ = w.Write(body)
		if cacheKey, cacheable := p.multiTenantCacheKey(filteredReq, endpoint); cacheable {
			p.cache.SetWithTTL(cacheKey, body, CacheTTLs[endpoint])
		}
		return true
	}

	recorders := make([]*httptest.ResponseRecorder, 0, len(filteredTenants))
	for _, tenantID := range filteredTenants {
		subReq := filteredReq.Clone(filteredReq.Context())
		subReq.Header = filteredReq.Header.Clone()
		subReq.Header.Set("X-Scope-OrgID", tenantID)

		rec := httptest.NewRecorder()
		single(rec, subReq)
		if rec.Code >= 400 {
			copyHeaders(w.Header(), rec.Header())
			if w.Header().Get("Content-Type") == "" {
				w.Header().Set("Content-Type", "application/json")
			}
			w.WriteHeader(rec.Code)
			_, _ = w.Write(rec.Body.Bytes())
			return true
		}
		recorders = append(recorders, rec)
	}

	body, contentType, err := mergeMultiTenantResponses(endpoint, filteredTenants, recorders)
	if err != nil {
		p.writeError(w, http.StatusInternalServerError, "failed to merge multi-tenant response: "+err.Error())
		return true
	}
	if len(body) > maxMultiTenantMergedResponseBytes {
		p.writeError(w, http.StatusRequestEntityTooLarge, "multi-tenant merged response exceeds configured safety limit")
		return true
	}
	if contentType == "" {
		contentType = "application/json"
	}
	w.Header().Set("Content-Type", contentType)
	_, _ = w.Write(body)
	if cacheKey, cacheable := p.multiTenantCacheKey(filteredReq, endpoint); cacheable {
		p.cache.SetWithTTL(cacheKey, body, CacheTTLs[endpoint])
	}
	return true
}

type lokiStringListResponse struct {
	Status string   `json:"status"`
	Data   []string `json:"data"`
}

type lokiSeriesResponse struct {
	Status string              `json:"status"`
	Data   []map[string]string `json:"data"`
}

type lokiQueryResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType    string          `json:"resultType"`
		Result        json.RawMessage `json:"result"`
		Stats         json.RawMessage `json:"stats,omitempty"`
		EncodingFlags []string        `json:"encodingFlags,omitempty"`
	} `json:"data"`
}

type lokiStreamResult struct {
	Stream map[string]string `json:"stream"`
	Values [][]interface{}   `json:"values"`
}

type lokiVectorResult struct {
	Metric map[string]string `json:"metric"`
	Value  []interface{}     `json:"value"`
}

type lokiMatrixResult struct {
	Metric map[string]string `json:"metric"`
	Values [][]interface{}   `json:"values"`
}

type detectedFieldResponse struct {
	Status string                `json:"status"`
	Data   []detectedFieldRecord `json:"data"`
	Fields []detectedFieldRecord `json:"fields"`
}

type detectedFieldRecord struct {
	Label       string        `json:"label"`
	Type        string        `json:"type"`
	Cardinality int           `json:"cardinality"`
	Parsers     []string      `json:"parsers"`
	JSONPath    []interface{} `json:"jsonPath,omitempty"`
}

type detectedFieldValuesResponse struct {
	Status string   `json:"status"`
	Data   []string `json:"data"`
	Values []string `json:"values"`
}

type detectedLabelResponse struct {
	Status         string                `json:"status"`
	Data           []detectedLabelRecord `json:"data"`
	DetectedLabels []detectedLabelRecord `json:"detectedLabels"`
}

type detectedLabelRecord struct {
	Label       string `json:"label"`
	Cardinality int    `json:"cardinality"`
}

type patternsResponse struct {
	Status string               `json:"status"`
	Data   []patternResultEntry `json:"data"`
}

type patternResultEntry struct {
	Pattern string          `json:"pattern"`
	Level   string          `json:"level,omitempty"`
	Samples [][]interface{} `json:"samples"`
}

func parseDetectedLineLimit(r *http.Request) int {
	lineLimit := 1000
	if value := strings.TrimSpace(r.FormValue("line_limit")); value != "" {
		if n, err := strconv.Atoi(value); err == nil && n > 0 {
			lineLimit = n
		}
	}
	if value := strings.TrimSpace(r.FormValue("limit")); value != "" {
		if n, err := strconv.Atoi(value); err == nil && n > 0 {
			lineLimit = n
		}
	}
	return lineLimit
}

func (p *Proxy) multiTenantCacheKey(r *http.Request, endpoint string) (string, bool) {
	if r.Method != http.MethodGet {
		return "", false
	}
	if endpoint == "patterns" || endpoint == "index_stats" || endpoint == "volume" || endpoint == "volume_range" || endpoint == "detected_labels" || endpoint == "detected_fields" || endpoint == "detected_field_values" || endpoint == "series" || endpoint == "labels" || endpoint == "label_values" || endpoint == "query" || endpoint == "query_range" {
		key := "mt:" + endpoint + ":" + r.Header.Get("X-Scope-OrgID") + ":" + r.URL.RawQuery
		if endpoint == "query" || endpoint == "query_range" {
			key += ":" + p.tupleModeCacheKey(r)
		}
		return key, true
	}
	return "", false
}

func (p *Proxy) tupleModeCacheKey(r *http.Request) string {
	categorizedLabels := requestWantsCategorizedLabels(r)
	if p.emitStructuredMetadata && categorizedLabels {
		return "categorize_labels_3tuple"
	}
	return "default_2tuple"
}

func emptyMultiTenantResponse(endpoint string) map[string]interface{} {
	switch endpoint {
	case "labels", "label_values":
		return map[string]interface{}{"status": "success", "data": []string{}}
	case "series":
		return map[string]interface{}{"status": "success", "data": []interface{}{}}
	case "query":
		return map[string]interface{}{"status": "success", "data": map[string]interface{}{"resultType": "streams", "result": []interface{}{}, "stats": map[string]interface{}{}}}
	case "query_range":
		return map[string]interface{}{"status": "success", "data": map[string]interface{}{"resultType": "streams", "result": []interface{}{}, "stats": map[string]interface{}{}}}
	case "index_stats":
		return map[string]interface{}{"streams": 0, "chunks": 0, "bytes": 0, "entries": 0}
	case "volume":
		return map[string]interface{}{"status": "success", "data": map[string]interface{}{"resultType": "vector", "result": []interface{}{}}}
	case "volume_range":
		return map[string]interface{}{"status": "success", "data": map[string]interface{}{"resultType": "matrix", "result": []interface{}{}}}
	case "detected_fields":
		return map[string]interface{}{"status": "success", "data": []interface{}{}, "fields": []interface{}{}}
	case "detected_field_values":
		return map[string]interface{}{"status": "success", "data": []string{}, "values": []string{}}
	case "detected_labels":
		return map[string]interface{}{"status": "success", "data": []interface{}{}, "detectedLabels": []interface{}{}}
	case "patterns":
		return map[string]interface{}{"status": "success", "data": []interface{}{}}
	default:
		return map[string]interface{}{"status": "success"}
	}
}

func (p *Proxy) applyTenantSelectorFilter(r *http.Request, tenantIDs []string) (*http.Request, []string, error) {
	filteredReq := r.Clone(r.Context())
	filteredReq.Header = r.Header.Clone()
	queryValues := filteredReq.URL.Query()

	switch {
	case queryValues.Get("query") != "":
		query, filtered, err := filterTenantMatchers(queryValues.Get("query"), tenantIDs)
		if err != nil {
			return nil, nil, err
		}
		queryValues.Set("query", query)
		filteredReq.URL.RawQuery = queryValues.Encode()
		filteredReq.Form = queryValues
		filteredReq.PostForm = queryValues
		filteredReq.Header.Set("X-Scope-OrgID", strings.Join(filtered, "|"))
		return filteredReq, filtered, nil
	case len(queryValues["match[]"]) > 0:
		filtered := tenantIDs
		matchers := queryValues["match[]"]
		for i, expr := range matchers {
			updated, narrowed, err := filterTenantMatchers(expr, filtered)
			if err != nil {
				return nil, nil, err
			}
			matchers[i] = updated
			filtered = narrowed
		}
		queryValues["match[]"] = matchers
		filteredReq.URL.RawQuery = queryValues.Encode()
		filteredReq.Form = queryValues
		filteredReq.PostForm = queryValues
		filteredReq.Header.Set("X-Scope-OrgID", strings.Join(filtered, "|"))
		return filteredReq, filtered, nil
	default:
		return filteredReq, tenantIDs, nil
	}
}

func filterTenantMatchers(query string, tenantIDs []string) (string, []string, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return query, tenantIDs, nil
	}
	selector, rest, ok := splitLeadingSelector(query)
	if !ok {
		return query, tenantIDs, nil
	}
	matchers := splitSelectorMatchers(selector[1 : len(selector)-1])
	filteredMatchers := make([]string, 0, len(matchers))
	filteredTenants := append([]string(nil), tenantIDs...)
	for _, matcher := range matchers {
		nextTenants, handled, err := filterTenantsByMatcher(filteredTenants, matcher)
		if err != nil {
			return "", nil, err
		}
		if handled {
			filteredTenants = nextTenants
			continue
		}
		filteredMatchers = append(filteredMatchers, matcher)
	}
	var rebuilt string
	switch {
	case len(filteredMatchers) == 0 && strings.TrimSpace(rest) == "":
		rebuilt = "*"
	case len(filteredMatchers) == 0:
		rebuilt = "* " + strings.TrimSpace(rest)
	case strings.TrimSpace(rest) == "":
		rebuilt = "{" + strings.Join(filteredMatchers, ",") + "}"
	default:
		rebuilt = "{" + strings.Join(filteredMatchers, ",") + "} " + strings.TrimSpace(rest)
	}
	return strings.TrimSpace(rebuilt), filteredTenants, nil
}

func splitLeadingSelector(query string) (selector, rest string, ok bool) {
	query = strings.TrimSpace(query)
	if query == "" || query[0] != '{' {
		return "", "", false
	}
	end := findMatchingBraceLocal(query)
	if end < 0 {
		return "", "", false
	}
	return query[:end+1], strings.TrimSpace(query[end+1:]), true
}

func findMatchingBraceLocal(s string) int {
	depth := 0
	inQuote := false
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			if inQuote && i+1 < len(s) {
				i++
			}
		case '"':
			inQuote = !inQuote
		case '{':
			if !inQuote {
				depth++
			}
		case '}':
			if !inQuote {
				depth--
				if depth == 0 {
					return i
				}
			}
		}
	}
	return -1
}

func splitSelectorMatchers(s string) []string {
	var matchers []string
	inQuote := false
	start := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			if inQuote && i+1 < len(s) {
				i++
			}
		case '"':
			inQuote = !inQuote
		case ',':
			if !inQuote {
				part := strings.TrimSpace(s[start:i])
				if part != "" {
					matchers = append(matchers, part)
				}
				start = i + 1
			}
		}
	}
	part := strings.TrimSpace(s[start:])
	if part != "" {
		matchers = append(matchers, part)
	}
	return matchers
}

func filterTenantsByMatcher(tenantIDs []string, matcher string) ([]string, bool, error) {
	for _, op := range []string{"!~", "=~", "!=", "="} {
		if idx := strings.Index(matcher, op); idx > 0 {
			label := strings.TrimSpace(matcher[:idx])
			if label != "__tenant_id__" {
				return tenantIDs, false, nil
			}
			raw := strings.TrimSpace(matcher[idx+len(op):])
			raw = strings.Trim(raw, "\"`")
			return applyTenantMatch(tenantIDs, op, raw)
		}
	}
	return tenantIDs, false, nil
}

func applyTenantMatch(tenantIDs []string, op, raw string) ([]string, bool, error) {
	out := make([]string, 0, len(tenantIDs))
	switch op {
	case "=":
		for _, tenantID := range tenantIDs {
			if tenantID == raw {
				out = append(out, tenantID)
			}
		}
	case "!=":
		for _, tenantID := range tenantIDs {
			if tenantID != raw {
				out = append(out, tenantID)
			}
		}
	case "=~":
		re, err := regexp.Compile(raw)
		if err != nil {
			return nil, true, fmt.Errorf("invalid __tenant_id__ regex: %w", err)
		}
		for _, tenantID := range tenantIDs {
			if re.MatchString(tenantID) {
				out = append(out, tenantID)
			}
		}
	case "!~":
		re, err := regexp.Compile(raw)
		if err != nil {
			return nil, true, fmt.Errorf("invalid __tenant_id__ regex: %w", err)
		}
		for _, tenantID := range tenantIDs {
			if !re.MatchString(tenantID) {
				out = append(out, tenantID)
			}
		}
	default:
		return tenantIDs, false, nil
	}
	return out, true, nil
}

func mergeMultiTenantResponses(endpoint string, tenantIDs []string, recorders []*httptest.ResponseRecorder) ([]byte, string, error) {
	switch endpoint {
	case "labels":
		values := make([]string, 0)
		for _, rec := range recorders {
			var resp lokiStringListResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				return nil, "", err
			}
			values = append(values, resp.Data...)
		}
		values = append(values, "__tenant_id__")
		return lokiLabelsResponse(uniqueSortedStrings(values)), "application/json", nil
	case "label_values":
		values := make([]string, 0)
		for _, rec := range recorders {
			var resp lokiStringListResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				return nil, "", err
			}
			values = append(values, resp.Data...)
		}
		return lokiLabelsResponse(uniqueSortedStrings(values)), "application/json", nil
	case "series":
		return mergeSeriesResponses(tenantIDs, recorders)
	case "query", "query_range", "volume", "volume_range":
		return mergeLokiQueryResponses(tenantIDs, recorders)
	case "index_stats":
		return mergeIndexStatsResponses(recorders)
	case "detected_fields":
		return mergeDetectedFieldsResponses(recorders)
	case "detected_field_values":
		return mergeDetectedFieldValuesResponses(recorders)
	case "detected_labels":
		return mergeDetectedLabelsResponses(tenantIDs, recorders)
	case "patterns":
		return mergePatternsResponses(recorders)
	default:
		return nil, "", fmt.Errorf("unsupported multi-tenant endpoint %q", endpoint)
	}
}

func uniqueSortedStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func injectTenantLabel(labels map[string]string, tenantID string) map[string]string {
	if labels == nil {
		labels = map[string]string{}
	}
	if existing, ok := labels["__tenant_id__"]; ok {
		if _, exists := labels["original___tenant_id__"]; !exists {
			labels["original___tenant_id__"] = existing
		}
	}
	labels["__tenant_id__"] = tenantID
	return labels
}

func interfaceStringMap(v interface{}) map[string]string {
	switch m := v.(type) {
	case map[string]string:
		out := make(map[string]string, len(m))
		for k, val := range m {
			out[k] = val
		}
		return out
	case map[string]interface{}:
		out := make(map[string]string, len(m))
		for k, val := range m {
			out[k] = fmt.Sprintf("%v", val)
		}
		return out
	default:
		return map[string]string{}
	}
}

func mergeSeriesResponses(tenantIDs []string, recorders []*httptest.ResponseRecorder) ([]byte, string, error) {
	seen := map[string]map[string]string{}
	for i, rec := range recorders {
		var resp lokiSeriesResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			return nil, "", err
		}
		for _, item := range resp.Data {
			labels := injectTenantLabel(item, tenantIDs[i])
			seen[canonicalLabelsKey(labels)] = labels
		}
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]map[string]string, 0, len(keys))
	for _, key := range keys {
		result = append(result, seen[key])
	}
	body, err := json.Marshal(map[string]interface{}{"status": "success", "data": result})
	return body, "application/json", err
}

func mergeLokiQueryResponses(tenantIDs []string, recorders []*httptest.ResponseRecorder) ([]byte, string, error) {
	var (
		resultType string
		stats      json.RawMessage
		streams    []lokiStreamResult
		vectors    []lokiVectorResult
		matrixes   []lokiMatrixResult
	)
	encodingFlagsSet := make(map[string]struct{})
	for i, rec := range recorders {
		var resp lokiQueryResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			return nil, "", err
		}
		for _, flag := range resp.Data.EncodingFlags {
			flag = strings.TrimSpace(flag)
			if flag == "" {
				continue
			}
			encodingFlagsSet[flag] = struct{}{}
		}
		if resp.Data.ResultType != "" && resultType == "" {
			resultType = resp.Data.ResultType
		}
		if len(resp.Data.Stats) > 0 {
			stats = resp.Data.Stats
		}
		switch resp.Data.ResultType {
		case "streams":
			var items []lokiStreamResult
			if err := json.Unmarshal(resp.Data.Result, &items); err != nil {
				return nil, "", err
			}
			for _, item := range items {
				item.Stream = injectTenantLabel(item.Stream, tenantIDs[i])
				normalizeMetadataPairTuples(item.Values)
				if streamValuesHaveCategorizedMetadata(item.Values) {
					encodingFlagsSet["categorize-labels"] = struct{}{}
				}
				streams = append(streams, item)
			}
		case "vector":
			var items []lokiVectorResult
			if err := json.Unmarshal(resp.Data.Result, &items); err != nil {
				return nil, "", err
			}
			for _, item := range items {
				item.Metric = injectTenantLabel(item.Metric, tenantIDs[i])
				vectors = append(vectors, item)
			}
		case "matrix":
			var items []lokiMatrixResult
			if err := json.Unmarshal(resp.Data.Result, &items); err != nil {
				return nil, "", err
			}
			for _, item := range items {
				item.Metric = injectTenantLabel(item.Metric, tenantIDs[i])
				matrixes = append(matrixes, item)
			}
		}
	}
	if resultType == "streams" {
		sort.SliceStable(streams, func(i, j int) bool {
			return latestStreamTimestampStrings(streams[i].Values) > latestStreamTimestampStrings(streams[j].Values)
		})
	}
	data := map[string]interface{}{"resultType": resultType}
	switch resultType {
	case "streams":
		data["result"] = streams
	case "vector":
		data["result"] = vectors
	case "matrix":
		data["result"] = matrixes
	default:
		data["result"] = []interface{}{}
	}
	if len(stats) > 0 {
		data["stats"] = json.RawMessage(stats)
	} else {
		data["stats"] = map[string]interface{}{}
	}
	if len(encodingFlagsSet) > 0 {
		encodingFlags := make([]string, 0, len(encodingFlagsSet))
		for flag := range encodingFlagsSet {
			encodingFlags = append(encodingFlags, flag)
		}
		sort.Strings(encodingFlags)
		data["encodingFlags"] = encodingFlags
	}
	body, err := json.Marshal(map[string]interface{}{"status": "success", "data": data})
	return body, "application/json", err
}

func latestStreamTimestampStrings(values [][]interface{}) string {
	if len(values) == 0 {
		return ""
	}
	if len(values[0]) == 0 {
		return ""
	}
	return fmt.Sprintf("%v", values[0][0])
}

// normalizeMetadataPairTuples rewrites tuple metadata to Loki categorized-label object maps.
// It normalizes legacy pair-object arrays ({name,value}) and pair tuples ([[k,v], ...]) into {"k":"v", ...}.
func normalizeMetadataPairTuples(values [][]interface{}) {
	for i := range values {
		if len(values[i]) < 3 {
			continue
		}
		meta, ok := values[i][2].(map[string]interface{})
		if !ok || len(meta) == 0 {
			continue
		}
		if raw, ok := meta["structuredMetadata"]; ok {
			if normalized := normalizeMetadataPairs(raw); len(normalized) > 0 {
				meta["structuredMetadata"] = normalized
			} else {
				delete(meta, "structuredMetadata")
			}
		}
		if raw, ok := meta["parsed"]; ok {
			if normalized := normalizeMetadataPairs(raw); len(normalized) > 0 {
				meta["parsed"] = normalized
			} else {
				delete(meta, "parsed")
			}
		}
		values[i][2] = meta
	}
}

func normalizeMetadataPairs(raw interface{}) map[string]string {
	switch typed := raw.(type) {
	case nil:
		return nil
	case map[string]string:
		out := make(map[string]string, len(typed))
		for key, value := range typed {
			trimmed := strings.TrimSpace(key)
			if trimmed == "" {
				continue
			}
			out[trimmed] = value
		}
		return out
	case []interface{}:
		out := make(map[string]string, len(typed))
		for _, item := range typed {
			switch pair := item.(type) {
			case []interface{}:
				if len(pair) < 2 {
					continue
				}
				key := fmt.Sprintf("%v", pair[0])
				if strings.TrimSpace(key) == "" {
					continue
				}
				out[key] = fmt.Sprintf("%v", pair[1])
			case []string:
				if len(pair) < 2 {
					continue
				}
				key := strings.TrimSpace(pair[0])
				if key == "" {
					continue
				}
				out[key] = pair[1]
			case map[string]interface{}:
				name := strings.TrimSpace(fmt.Sprintf("%v", pair["name"]))
				if name == "" {
					continue
				}
				out[name] = normalizeMetadataStringValue(pair["value"])
			}
		}
		return out
	case map[string]interface{}:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out := make(map[string]string, len(keys))
		for _, key := range keys {
			trimmed := strings.TrimSpace(key)
			if trimmed == "" {
				continue
			}
			out[trimmed] = normalizeMetadataStringValue(typed[key])
		}
		return out
	default:
		return nil
	}
}

func normalizeMetadataStringValue(raw interface{}) string {
	if raw == nil {
		return ""
	}
	return fmt.Sprintf("%v", raw)
}

func streamValuesHaveCategorizedMetadata(values [][]interface{}) bool {
	for _, tuple := range values {
		if len(tuple) < 3 {
			continue
		}
		if _, ok := tuple[2].(map[string]interface{}); ok {
			return true
		}
	}
	return false
}

func mergeIndexStatsResponses(recorders []*httptest.ResponseRecorder) ([]byte, string, error) {
	totals := map[string]int{"streams": 0, "chunks": 0, "bytes": 0, "entries": 0}
	for _, rec := range recorders {
		var resp map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			return nil, "", err
		}
		for key := range totals {
			switch v := resp[key].(type) {
			case float64:
				totals[key] += int(v)
			case int:
				totals[key] += v
			}
		}
	}
	body, err := json.Marshal(totals)
	return body, "application/json", err
}

func mergeDetectedFieldsResponses(recorders []*httptest.ResponseRecorder) ([]byte, string, error) {
	type mergedField struct {
		Label       string
		Type        string
		Cardinality int
		Parsers     map[string]struct{}
		JSONPath    []interface{}
	}
	merged := map[string]*mergedField{}
	for _, rec := range recorders {
		var resp detectedFieldResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			return nil, "", err
		}
		for _, item := range resp.Fields {
			label := item.Label
			if label == "" {
				continue
			}
			mf := merged[label]
			if mf == nil {
				mf = &mergedField{Label: label, Type: item.Type, Parsers: map[string]struct{}{}, JSONPath: item.JSONPath}
				merged[label] = mf
			}
			mf.Cardinality += item.Cardinality
			for _, parser := range item.Parsers {
				mf.Parsers[parser] = struct{}{}
			}
		}
	}
	labels := make([]string, 0, len(merged))
	for label := range merged {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	out := make([]detectedFieldRecord, 0, len(labels))
	for _, label := range labels {
		mf := merged[label]
		parsers := make([]string, 0, len(mf.Parsers))
		for parser := range mf.Parsers {
			parsers = append(parsers, parser)
		}
		sort.Strings(parsers)
		item := detectedFieldRecord{
			Label:       mf.Label,
			Type:        mf.Type,
			Cardinality: mf.Cardinality,
			Parsers:     parsers,
		}
		if len(mf.JSONPath) > 0 {
			item.JSONPath = mf.JSONPath
		}
		out = append(out, item)
	}
	body, err := json.Marshal(detectedFieldResponse{Status: "success", Data: out, Fields: out})
	return body, "application/json", err
}

func (p *Proxy) multiTenantDetectedFieldsResponse(r *http.Request, tenantIDs []string) ([]byte, string, error) {
	lineLimit := parseDetectedLineLimit(r)
	type mergedField struct {
		Label            string
		Type             string
		Parsers          map[string]struct{}
		JSONPath         []interface{}
		Values           map[string]struct{}
		CardinalityFloor int
	}
	merged := map[string]*mergedField{}
	for _, tenantID := range tenantIDs {
		subReq := r.Clone(r.Context())
		subReq.Header = r.Header.Clone()
		subReq.Header.Set("X-Scope-OrgID", tenantID)
		subReq = withOrgID(subReq)

		fields, fieldValues, err := p.detectFields(subReq.Context(), subReq.FormValue("query"), subReq.FormValue("start"), subReq.FormValue("end"), lineLimit)
		if err != nil {
			return nil, "", err
		}
		for _, item := range fields {
			label, _ := item["label"].(string)
			if label == "" {
				continue
			}
			mf := merged[label]
			if mf == nil {
				mf = &mergedField{
					Label:   label,
					Type:    fmt.Sprintf("%v", item["type"]),
					Parsers: map[string]struct{}{},
					Values:  map[string]struct{}{},
				}
				if jp, ok := item["jsonPath"].([]interface{}); ok {
					mf.JSONPath = jp
				}
				merged[label] = mf
			}
			if card, ok := numberToInt(item["cardinality"]); ok && card > mf.CardinalityFloor {
				mf.CardinalityFloor = card
			}
			if parsers, ok := item["parsers"].([]interface{}); ok {
				for _, parser := range parsers {
					mf.Parsers[fmt.Sprintf("%v", parser)] = struct{}{}
				}
			}
			for _, value := range fieldValues[label] {
				mf.Values[value] = struct{}{}
			}
		}
	}

	labels := make([]string, 0, len(merged))
	for label := range merged {
		labels = append(labels, label)
	}
	sort.Strings(labels)

	out := make([]map[string]interface{}, 0, len(labels))
	for _, label := range labels {
		mf := merged[label]
		parsers := make([]string, 0, len(mf.Parsers))
		for parser := range mf.Parsers {
			parsers = append(parsers, parser)
		}
		sort.Strings(parsers)
		cardinality := len(mf.Values)
		if mf.CardinalityFloor > cardinality {
			cardinality = mf.CardinalityFloor
		}
		item := map[string]interface{}{
			"label":       mf.Label,
			"type":        mf.Type,
			"cardinality": cardinality,
			"parsers":     parsers,
		}
		if len(mf.JSONPath) > 0 {
			item["jsonPath"] = mf.JSONPath
		}
		out = append(out, item)
	}
	body, err := json.Marshal(map[string]interface{}{"status": "success", "data": out, "fields": out, "limit": lineLimit})
	return body, "application/json", err
}

func mergeDetectedFieldValuesResponses(recorders []*httptest.ResponseRecorder) ([]byte, string, error) {
	values := make([]string, 0)
	for _, rec := range recorders {
		var resp detectedFieldValuesResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			return nil, "", err
		}
		values = append(values, resp.Values...)
	}
	values = uniqueSortedStrings(values)
	body, err := json.Marshal(detectedFieldValuesResponse{Status: "success", Data: values, Values: values})
	return body, "application/json", err
}

func (p *Proxy) multiTenantDetectedLabelsResponse(r *http.Request, tenantIDs []string) ([]byte, string, error) {
	lineLimit := parseDetectedLineLimit(r)
	merged := map[string]*detectedLabelSummary{
		"__tenant_id__": {
			label:  "__tenant_id__",
			values: map[string]struct{}{},
		},
	}
	for _, tenantID := range tenantIDs {
		merged["__tenant_id__"].values[tenantID] = struct{}{}

		subReq := r.Clone(r.Context())
		subReq.Header = r.Header.Clone()
		subReq.Header.Set("X-Scope-OrgID", tenantID)
		subReq = withOrgID(subReq)

		_, summaries, err := p.detectLabels(subReq.Context(), subReq.FormValue("query"), subReq.FormValue("start"), subReq.FormValue("end"), lineLimit)
		if err != nil {
			return nil, "", err
		}
		for label, summary := range summaries {
			existing := merged[label]
			if existing == nil {
				existing = &detectedLabelSummary{
					label:  summary.label,
					values: map[string]struct{}{},
				}
				merged[label] = existing
			}
			for value := range summary.values {
				existing.values[value] = struct{}{}
			}
		}
	}

	out := formatDetectedLabelSummaries(merged)
	body, err := json.Marshal(map[string]interface{}{"status": "success", "data": out, "detectedLabels": out, "limit": lineLimit})
	return body, "application/json", err
}

func mergeDetectedLabelsResponses(tenantIDs []string, recorders []*httptest.ResponseRecorder) ([]byte, string, error) {
	cardinality := map[string]int{"__tenant_id__": len(tenantIDs)}
	for _, rec := range recorders {
		var resp detectedLabelResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			return nil, "", err
		}
		for _, item := range resp.DetectedLabels {
			label := item.Label
			if label == "" {
				continue
			}
			cardinality[label] += item.Cardinality
		}
	}
	labels := make([]string, 0, len(cardinality))
	for label := range cardinality {
		labels = append(labels, label)
	}
	sort.Slice(labels, func(i, j int) bool {
		if labels[i] == "service_name" {
			return true
		}
		if labels[j] == "service_name" {
			return false
		}
		return labels[i] < labels[j]
	})
	out := make([]detectedLabelRecord, 0, len(labels))
	for _, label := range labels {
		out = append(out, detectedLabelRecord{Label: label, Cardinality: cardinality[label]})
	}
	body, err := json.Marshal(detectedLabelResponse{Status: "success", Data: out, DetectedLabels: out})
	return body, "application/json", err
}

func mergePatternsResponses(recorders []*httptest.ResponseRecorder) ([]byte, string, error) {
	type bucket struct {
		level   string
		pattern string
		samples map[int64]int
		total   int
	}
	merged := map[string]*bucket{}
	for _, rec := range recorders {
		var resp patternsResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			return nil, "", err
		}
		for _, item := range resp.Data {
			pattern := item.Pattern
			level := item.Level
			key := level + "\x00" + pattern
			b := merged[key]
			if b == nil {
				b = &bucket{level: level, pattern: pattern, samples: map[int64]int{}}
				merged[key] = b
			}
			for _, pair := range item.Samples {
				if len(pair) < 2 {
					continue
				}
				ts, ok := numberToInt64(pair[0])
				if !ok {
					continue
				}
				count, ok := numberToInt(pair[1])
				if !ok {
					continue
				}
				b.samples[ts] += count
				b.total += count
			}
		}
	}
	items := make([]*bucket, 0, len(merged))
	for _, item := range merged {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].total > items[j].total })
	out := make([]patternResultEntry, 0, len(items))
	for _, item := range items {
		timestamps := make([]int64, 0, len(item.samples))
		for ts := range item.samples {
			timestamps = append(timestamps, ts)
		}
		sort.Slice(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] })
		samples := make([][]interface{}, 0, len(timestamps))
		for _, ts := range timestamps {
			samples = append(samples, []interface{}{ts, item.samples[ts]})
		}
		respItem := patternResultEntry{Pattern: item.pattern, Samples: samples}
		if item.level != "" {
			respItem.Level = item.level
		}
		out = append(out, respItem)
	}
	body, err := json.Marshal(patternsResponse{Status: "success", Data: out})
	return body, "application/json", err
}

func numberToInt64(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	case json.Number:
		out, err := n.Int64()
		return out, err == nil
	default:
		return 0, false
	}
}

func numberToInt(v interface{}) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), true
	case int:
		return n, true
	case int64:
		return int(n), true
	case json.Number:
		out, err := n.Int64()
		return int(out), err == nil
	default:
		return 0, false
	}
}

// --- Multitenancy ---

// forwardTenantHeaders maps Loki's X-Scope-OrgID to VL's AccountID/ProjectID.
// Reads orgID from the request context (set by withOrgID).
// tenantMap is protected by configMu (written by ReloadTenantMap on SIGHUP).
func (p *Proxy) forwardTenantHeaders(req *http.Request) {
	orgID := getOrgID(req.Context())
	if orgID == "" {
		// No tenant header → default VL tenant (0:0), serves all data
		return
	}

	// Check tenant map first for string→int mapping (read-lock for SIGHUP safety)
	p.configMu.RLock()
	tm := p.tenantMap
	p.configMu.RUnlock()

	if tm != nil {
		if mapping, ok := tm[orgID]; ok {
			req.Header.Set("AccountID", mapping.AccountID)
			req.Header.Set("ProjectID", mapping.ProjectID)
			return
		}
	}

	// Default-tenant aliases keep Loki single-tenant compatibility while still
	// targeting VictoriaLogs' built-in 0:0 tenant.
	if isDefaultTenantAlias(orgID) {
		return
	}

	// Wildcard bypass is proxy-specific and remains opt-in.
	if orgID == "*" {
		if p.globalTenantAllowed() {
			return
		}
		return
	}

	// Try numeric passthrough: "42" → AccountID: 42
	if _, err := strconv.Atoi(orgID); err == nil {
		req.Header.Set("AccountID", orgID)
		req.Header.Set("ProjectID", "0")
	}
}

// --- Error / JSON helpers ---

// validateQuery checks query string length and returns a sanitized version.
func (p *Proxy) validateQuery(w http.ResponseWriter, query string, endpoint string) (string, bool) {
	if len(query) > maxQueryLength {
		p.writeError(w, http.StatusBadRequest, fmt.Sprintf("query exceeds max length (%d > %d)", len(query), maxQueryLength))
		p.metrics.RecordRequest(endpoint, http.StatusBadRequest, 0)
		return "", false
	}
	return query, true
}

// sanitizeLimit caps and validates the limit parameter.
func sanitizeLimit(limitStr string) string {
	if limitStr == "" {
		return "1000"
	}
	n, err := strconv.Atoi(limitStr)
	if err != nil || n <= 0 {
		return "1000"
	}
	if n > maxLimitValue {
		return strconv.Itoa(maxLimitValue)
	}
	return limitStr
}

func (p *Proxy) writeError(w http.ResponseWriter, code int, msg string) {
	p.log.Error("request error", "code", code, "error", msg)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "error",
		"errorType": lokiErrorType(code),
		"error":     msg,
	})
}

func statusFromUpstreamErr(err error) int {
	if err == nil {
		return http.StatusBadGateway
	}
	if isCanceledErr(err) {
		return 499
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return http.StatusGatewayTimeout
	}
	lower := strings.ToLower(err.Error())
	if strings.Contains(lower, "deadline exceeded") || strings.Contains(lower, "timeout") {
		return http.StatusGatewayTimeout
	}
	return http.StatusBadGateway
}

func isCanceledErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "context canceled")
}

func shouldRecordBreakerFailure(err error) bool {
	if err == nil {
		return false
	}
	if isCanceledErr(err) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return false
	}
	lower := strings.ToLower(err.Error())
	if strings.Contains(lower, "context canceled") ||
		strings.Contains(lower, "deadline exceeded") ||
		strings.Contains(lower, "timeout") {
		return false
	}
	return true
}

// lokiErrorType returns the Loki/Prometheus-style errorType for an HTTP status code.
// Matches the exact errorType strings from Loki's Prometheus API handler:
// vendor/github.com/prometheus/prometheus/web/api/v1/api.go
func lokiErrorType(code int) string {
	switch code {
	case 400:
		return "bad_data"
	case 404:
		return "not_found"
	case 406:
		return "not_acceptable"
	case 422:
		return "execution"
	case 499:
		return "canceled"
	case 500:
		return "internal"
	case 502:
		return "unavailable"
	case 503:
		return "timeout" // Loki maps 503 to ErrQueryTimeout
	case 504:
		return "timeout"
	default:
		if code >= 400 && code < 500 {
			return "bad_data"
		}
		return "internal"
	}
}

func (p *Proxy) writeJSON(w http.ResponseWriter, data interface{}) {
	marshalJSON(w, data)
}

func base64Encode(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

func copyHeaders(dst, src http.Header) {
	for key, values := range src {
		dst.Del(key)
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

var trustedIdentityHeaders = []string{
	"X-Grafana-User",
	"X-Forwarded-User",
	"X-Webauth-User",
	"X-Auth-Request-User",
}

var trustedProxyForwardHeaders = []string{
	"X-Forwarded-For",
	"X-Forwarded-Proto",
	"X-Forwarded-Host",
	"X-Real-Ip",
	"Forwarded",
}

// isVLInternalField returns true for VictoriaLogs internal field names
// that should not be exposed in Loki-compatible label responses.
func isVLInternalField(name string) bool {
	return name == "_time" || name == "_msg" || name == "_stream" || name == "_stream_id"
}

// applyBackendHeaders adds static backend headers and forwarded client headers to a VL request.
func (p *Proxy) applyBackendHeaders(vlReq *http.Request) {
	for k, v := range p.backendHeaders {
		vlReq.Header.Set(k, v)
	}
	if vlReq.Header.Get("Accept-Encoding") == "" {
		switch p.backendCompression {
		case "none":
			vlReq.Header.Set("Accept-Encoding", "identity")
		case "gzip":
			vlReq.Header.Set("Accept-Encoding", "gzip")
		case "zstd":
			vlReq.Header.Set("Accept-Encoding", "zstd")
		default:
			vlReq.Header.Set("Accept-Encoding", "zstd, gzip")
		}
	}
	if origReq, ok := vlReq.Context().Value(origRequestKey).(*http.Request); ok && origReq != nil {
		clientID, clientSource := metrics.ResolveClientContext(origReq, p.metricsTrustProxyHeaders)
		vlReq.Header.Set("X-Loki-VL-Client-ID", clientID)
		vlReq.Header.Set("X-Loki-VL-Client-Source", clientSource)
		gp := grafanaClientProfileFromContext(origReq.Context())
		if gp.surface != "" {
			vlReq.Header.Set("X-Loki-VL-Grafana-Surface", gp.surface)
		}
		if gp.version != "" {
			vlReq.Header.Set("X-Loki-VL-Grafana-Version", gp.version)
		}
		if gp.runtimeFamily != "" {
			vlReq.Header.Set("X-Loki-VL-Grafana-Runtime-Family", gp.runtimeFamily)
		}
		if gp.drilldownProfile != "" {
			vlReq.Header.Set("X-Loki-VL-Drilldown-Profile", gp.drilldownProfile)
		}
		if authUser, authSource := metrics.ResolveAuthContext(origReq); authUser != "" {
			vlReq.Header.Set("X-Loki-VL-Auth-User", authUser)
			vlReq.Header.Set("X-Loki-VL-Auth-Source", authSource)
		}
		if p.metricsTrustProxyHeaders {
			for _, headerName := range trustedIdentityHeaders {
				if value := strings.TrimSpace(origReq.Header.Get(headerName)); value != "" {
					vlReq.Header.Set(headerName, value)
				}
			}
			for _, headerName := range trustedProxyForwardHeaders {
				if value := strings.TrimSpace(origReq.Header.Get(headerName)); value != "" {
					vlReq.Header.Set(headerName, value)
				}
			}
		}
		// Forward configured client headers from the original request
		if len(p.forwardHeaders) > 0 {
			for _, hdr := range p.forwardHeaders {
				if val := origReq.Header.Get(hdr); val != "" {
					vlReq.Header.Set(hdr, val)
				}
			}
		}
		for _, cookie := range origReq.Cookies() {
			if p.forwardCookies["*"] || p.forwardCookies[cookie.Name] {
				vlReq.AddCookie(cookie)
			}
		}
	}
}

func normalizeBackendCompression(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "auto":
		return "auto"
	case "none", "gzip", "zstd":
		return strings.ToLower(strings.TrimSpace(mode))
	default:
		return "auto"
	}
}

func decodeCompressedHTTPResponse(resp *http.Response) error {
	encoding := strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Encoding")))
	switch encoding {
	case "", "identity":
		return nil
	case "gzip", "x-gzip":
		zr, err := gzip.NewReader(resp.Body)
		if err != nil {
			return err
		}
		resp.Body = &readCloserChain{Reader: zr, closers: []io.Closer{zr, resp.Body}}
	case "zstd":
		zr, err := zstd.NewReader(resp.Body)
		if err != nil {
			return err
		}
		resp.Body = &readCloserChain{
			Reader: zr,
			closers: []io.Closer{
				closerFunc(func() error {
					zr.Close()
					return nil
				}),
				resp.Body,
			},
		}
	default:
		return nil
	}
	resp.Header.Del("Content-Encoding")
	resp.Header.Del("Content-Length")
	resp.ContentLength = -1
	resp.Uncompressed = true
	return nil
}

type readCloserChain struct {
	io.Reader
	closers []io.Closer
}

func (r *readCloserChain) Close() error {
	var firstErr error
	for _, closer := range r.closers {
		if err := closer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

type closerFunc func() error

func (f closerFunc) Close() error {
	return f()
}

// statusCapture wraps ResponseWriter to capture the status code and bytes written.
type statusCapture struct {
	http.ResponseWriter
	code         int
	bytesWritten int
}

func (sc *statusCapture) WriteHeader(code int) {
	sc.code = code
	sc.ResponseWriter.WriteHeader(code)
}

func (sc *statusCapture) Write(b []byte) (int, error) {
	n, err := sc.ResponseWriter.Write(b)
	sc.bytesWritten += n
	return n, err
}

// Flush implements http.Flusher for chunked streaming support.
func (sc *statusCapture) Flush() {
	if f, ok := sc.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (sc *statusCapture) Unwrap() http.ResponseWriter {
	return sc.ResponseWriter
}

// Hijack implements http.Hijacker for WebSocket upgrade support.
func (sc *statusCapture) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := sc.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, fmt.Errorf("hijack not supported")
}

// requestLogger wraps a handler with structured logging and route-aware metrics.
func (p *Proxy) requestLogger(endpoint, route string, next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		tenant := r.Header.Get("X-Scope-OrgID")
		query := r.FormValue("query")
		clientID, clientSource := metrics.ResolveClientContext(r, p.metricsTrustProxyHeaders)
		p.metrics.RecordClientInflight(clientID, 1)
		defer p.metrics.RecordClientInflight(clientID, -1)

		rt := newRequestTelemetry()
		ctx := context.WithValue(r.Context(), requestTelemetryKey, rt)
		ctx = context.WithValue(ctx, requestRouteMetaKey, requestRouteMeta{endpoint: endpoint, route: route})
		grafanaProfile := detectGrafanaClientProfile(r, endpoint, route)
		ctx = context.WithValue(ctx, requestGrafanaClientKey, grafanaProfile)
		reqWithTelemetry := r.WithContext(ctx)
		sc := &statusCapture{ResponseWriter: w, code: 200}
		next.ServeHTTP(sc, reqWithTelemetry)

		elapsed := time.Since(start)
		telemetry := snapshotTelemetry(reqWithTelemetry.Context())
		proxyOverhead := elapsed - telemetry.upstreamDuration
		if proxyOverhead < 0 {
			proxyOverhead = 0
		}

		// Per-tenant metrics
		p.metrics.RecordTenantRequestWithRoute(tenant, endpoint, route, sc.code, elapsed)

		// Per-client identity metrics (Grafana user > tenant > IP)
		p.metrics.RecordClientIdentityWithRoute(clientID, endpoint, route, elapsed, int64(sc.bytesWritten))
		p.metrics.RecordClientStatusWithRoute(clientID, endpoint, route, sc.code)
		p.metrics.RecordClientQueryLengthWithRoute(clientID, endpoint, route, len(query))

		// Client error categorization
		if sc.code >= 400 && sc.code < 500 {
			reason := "bad_request"
			switch sc.code {
			case 400:
				reason = "bad_request"
			case 429:
				reason = "rate_limited"
			case 404:
				reason = "not_found"
			case 413:
				reason = "body_too_large"
			}
			p.metrics.RecordClientErrorWithRoute(endpoint, route, reason)
		}

		// Structured request log — includes tenant, query, status, latency, cache info
		logLevel := slog.LevelInfo
		if sc.code >= 500 {
			logLevel = slog.LevelError
		} else if sc.code >= 400 {
			logLevel = slog.LevelWarn
		}
		reqCtx := reqWithTelemetry.Context()
		if !p.log.Enabled(reqCtx, logLevel) {
			return
		}

		authUser, authSource := metrics.ResolveAuthContext(r)
		clientAddr := forwardedClientAddress(r, p.metricsTrustProxyHeaders)
		peerAddr, _ := splitHostPortValue(r.RemoteAddr)
		grafanaSurface := grafanaProfile.surface
		grafanaSourceTag := grafanaProfile.sourceTag
		grafanaVersion := grafanaProfile.version
		logAttrs := []interface{}{
			"http.route", route,
			"url.path", r.URL.Path,
			"http.request.method", r.Method,
			"http.response.status_code", sc.code,
			"loki.request.type", endpoint,
			"loki.api.system", "loki",
			"proxy.direction", "downstream",
			"event.duration", elapsed.Nanoseconds(),
			"loki.tenant.id", tenant,
			"loki.query", truncateQuery(query, 200),
			"enduser.id", clientID,
			"enduser.source", clientSource,
			"cache.result", telemetry.cacheResult,
			"proxy.duration_ms", elapsed.Milliseconds(),
			"proxy.overhead_ms", proxyOverhead.Milliseconds(),
			"upstream.calls", telemetry.upstreamCalls,
			"upstream.duration_ms", telemetry.upstreamDuration.Milliseconds(),
			"upstream.status_code", telemetry.upstreamLastCode,
			"upstream.error", telemetry.upstreamErrorSeen,
		}
		if clientAddr != "" {
			logAttrs = append(logAttrs, "client.address", clientAddr)
		}
		if peerAddr != "" {
			logAttrs = append(logAttrs, "network.peer.address", peerAddr)
		}
		if userAgent := strings.TrimSpace(r.Header.Get("User-Agent")); userAgent != "" {
			logAttrs = append(logAttrs, "user_agent.original", userAgent)
		}
		if grafanaVersion != "" {
			logAttrs = append(logAttrs, "grafana.version", grafanaVersion)
		}
		if grafanaSourceTag != "" {
			logAttrs = append(logAttrs, "grafana.client.source_tag", grafanaSourceTag)
		}
		if grafanaSurface != "unknown" {
			logAttrs = append(logAttrs, "grafana.client.surface", grafanaSurface)
		}
		if grafanaProfile.runtimeFamily != "" {
			logAttrs = append(logAttrs, "grafana.runtime.family", grafanaProfile.runtimeFamily)
		}
		if grafanaProfile.drilldownProfile != "" {
			logAttrs = append(logAttrs, "grafana.drilldown.profile", grafanaProfile.drilldownProfile)
		}
		if grafanaProfile.datasourceProfile != "" {
			logAttrs = append(logAttrs, "grafana.datasource.profile", grafanaProfile.datasourceProfile)
		}
		if enduserName := deriveEnduserName(clientID, clientSource); enduserName != "" {
			logAttrs = append(logAttrs, "enduser.name", enduserName)
		}
		if authUser != "" {
			logAttrs = append(logAttrs,
				"auth.principal", authUser,
				"auth.source", authSource,
			)
		}
		p.log.Log(reqCtx, logLevel, "request", logAttrs...)
	})
}

func truncateQuery(q string, maxLen int) string {
	if len(q) <= maxLen {
		return q
	}
	return q[:maxLen] + "..."
}

func detectGrafanaClientProfile(r *http.Request, endpoint, route string) grafanaClientProfile {
	version := parseGrafanaVersionFromUserAgent(r.Header.Get("User-Agent"))
	sourceTag := parseGrafanaSourceTag(r.Header.Values("X-Query-Tags"))
	surface := "unknown"

	sourceLower := strings.ToLower(sourceTag)
	switch {
	case strings.Contains(sourceLower, "lokiexplore"), strings.Contains(sourceLower, "drilldown"):
		surface = "grafana_drilldown"
	case strings.Contains(sourceLower, "loki"):
		surface = "grafana_loki_datasource"
	}

	// Fallback: infer surface from endpoint family when request is known to come from Grafana.
	if surface == "unknown" && version != "" {
		switch endpoint {
		case "patterns", "detected_fields", "detected_labels", "volume", "volume_range", "drilldown_limits":
			surface = "grafana_drilldown"
		default:
			if strings.HasPrefix(route, "/loki/api/") {
				surface = "grafana_loki_datasource"
			}
		}
	}

	major := parseGrafanaRuntimeMajor(version)
	runtimeFamily := ""
	switch {
	case major >= 12:
		runtimeFamily = "12.x+"
	case major == 11:
		runtimeFamily = "11.x"
	case major > 0:
		runtimeFamily = strconv.Itoa(major) + ".x"
	}

	drilldownProfile := ""
	if surface == "grafana_drilldown" {
		switch {
		case major >= 12:
			drilldownProfile = "drilldown-v2"
		case major == 11:
			drilldownProfile = "drilldown-v1"
		}
	}

	datasourceProfile := ""
	if surface == "grafana_loki_datasource" {
		switch {
		case major >= 12:
			datasourceProfile = "grafana-datasource-v12"
		case major == 11:
			datasourceProfile = "grafana-datasource-v11"
		}
	}

	return grafanaClientProfile{
		surface:           surface,
		sourceTag:         sourceTag,
		version:           version,
		runtimeMajor:      major,
		runtimeFamily:     runtimeFamily,
		drilldownProfile:  drilldownProfile,
		datasourceProfile: datasourceProfile,
	}
}

func parseGrafanaSourceTag(values []string) string {
	for _, raw := range values {
		for _, part := range strings.Split(raw, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			key, val, ok := strings.Cut(part, "=")
			if !ok {
				continue
			}
			if !strings.EqualFold(strings.TrimSpace(key), "source") {
				continue
			}
			clean := strings.Trim(strings.TrimSpace(val), `"`)
			if clean != "" {
				return clean
			}
		}
	}
	return ""
}

func parseGrafanaVersionFromUserAgent(userAgent string) string {
	userAgent = strings.TrimSpace(userAgent)
	if userAgent == "" {
		return ""
	}
	lower := strings.ToLower(userAgent)
	idx := strings.Index(lower, "grafana/")
	if idx < 0 {
		return ""
	}
	rest := userAgent[idx+len("grafana/"):]
	end := 0
	for end < len(rest) {
		ch := rest[end]
		if (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '.' || ch == '-' {
			end++
			continue
		}
		break
	}
	version := strings.Trim(rest[:end], ".-")
	return version
}

func parseGrafanaRuntimeMajor(version string) int {
	if version == "" {
		return 0
	}
	majorPart := version
	if idx := strings.IndexByte(majorPart, '.'); idx >= 0 {
		majorPart = majorPart[:idx]
	}
	major, err := strconv.Atoi(majorPart)
	if err != nil {
		return 0
	}
	return major
}

func deriveEnduserName(clientID, clientSource string) string {
	switch clientSource {
	case "grafana_user", "forwarded_user", "webauth_user", "auth_request_user":
		return clientID
	default:
		return ""
	}
}

// translateQuery translates a LogQL query to LogsQL, applying label name translation.
func (p *Proxy) translateQuery(logql string) (string, error) {
	normalized := strings.TrimSpace(logql)
	switch normalized {
	case "", "*", `"*"`, "`*`":
		return "*", nil
	}
	if p.translationCache != nil {
		if cached, ok := p.translationCache.Get(normalized); ok {
			return string(cached), nil
		}
	}

	p.configMu.RLock()
	labelFn := p.labelTranslator.ToVL
	streamFieldsMap := p.streamFieldsMap
	p.configMu.RUnlock()
	var (
		translated string
		err        error
	)
	if streamFieldsMap != nil {
		translated, err = translator.TranslateLogQLWithStreamFields(logql, labelFn, streamFieldsMap)
	} else {
		translated, err = translator.TranslateLogQLWithLabels(logql, labelFn)
	}
	if err != nil {
		return "", err
	}
	trimmed := strings.TrimSpace(translated)
	if strings.HasPrefix(trimmed, "|") {
		translated = "* " + trimmed
	}
	if p.translationCache != nil {
		p.translationCache.SetWithTTL(normalized, []byte(translated), 5*time.Minute)
	}
	return translated, nil
}

var (
	jsonParserStageRE   = regexp.MustCompile(`\|\s*json(?:\s+[^|]+)?`)
	logfmtParserStageRE = regexp.MustCompile(`\|\s*logfmt(?:\s+[^|]+)?`)
)

func hasParserStage(logql, parser string) bool {
	re := jsonParserStageRE
	if parser == "logfmt" {
		re = logfmtParserStageRE
	}
	return re.MatchString(logql)
}

func removeParserStage(logql, parser string) string {
	re := jsonParserStageRE
	if parser == "logfmt" {
		re = logfmtParserStageRE
	}
	logql = re.ReplaceAllString(logql, "")
	for strings.Contains(logql, "  ") {
		logql = strings.ReplaceAll(logql, "  ", " ")
	}
	return strings.TrimSpace(logql)
}

func (p *Proxy) preferWorkingParser(ctx context.Context, logql, start, end string) string {
	if !hasParserStage(logql, "json") || !hasParserStage(logql, "logfmt") {
		return logql
	}

	baseQuery := extractParserProbeQuery(logql)
	if baseQuery == "" {
		baseQuery = logql
	}
	baseQuery = defaultFieldDetectionQuery(baseQuery)
	logsqlQuery, err := p.translateQuery(baseQuery)
	if err != nil {
		return logql
	}

	params := url.Values{}
	params.Set("query", logsqlQuery+" | sort by (_time desc)")
	params.Set("limit", "25")
	if start != "" {
		params.Set("start", formatVLTimestamp(start))
	}
	if end != "" {
		params.Set("end", formatVLTimestamp(end))
	}

	resp, err := p.vlPost(ctx, "/select/logsql/query", params)
	if err != nil {
		return logql
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil || len(body) == 0 {
		return logql
	}

	jsonHits := 0
	logfmtHits := 0
	startIdx := 0
	for i := 0; i <= len(body); i++ {
		if i < len(body) && body[i] != '\n' {
			continue
		}
		line := strings.TrimSpace(string(body[startIdx:i]))
		startIdx = i + 1
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

		var parsedJSON map[string]interface{}
		if json.Unmarshal([]byte(msg), &parsedJSON) == nil && len(parsedJSON) > 0 {
			jsonHits++
		}
		if fields := parseLogfmtFields(msg); len(fields) > 0 {
			logfmtHits++
		}
	}

	switch {
	case jsonHits == 0 && logfmtHits == 0:
		return logql
	case jsonHits >= logfmtHits:
		return removeParserStage(logql, "logfmt")
	default:
		return removeParserStage(logql, "json")
	}
}

var metricParserProbeRE = regexp.MustCompile(`(?s)(?:count_over_time|bytes_over_time|rate|bytes_rate|sum_over_time|avg_over_time|max_over_time|min_over_time|first_over_time|last_over_time|stddev_over_time|stdvar_over_time|quantile_over_time)\((.*?)\[[^][]+\]\)`)

func extractParserProbeQuery(logql string) string {
	matches := metricParserProbeRE.FindStringSubmatch(logql)
	if len(matches) == 2 {
		return strings.TrimSpace(matches[1])
	}
	return strings.TrimSpace(logql)
}

// translateStatsResponseLabels translates label names in VL stats responses
// (both vector and matrix result types) from VL field names (dots) to
// Loki-compatible label names (underscores).
func (p *Proxy) translateStatsResponseLabels(body []byte, originalQuery string) []byte {
	var resp map[string]interface{}
	if err := json.Unmarshal(body, &resp); err != nil {
		return body
	}

	// Handle both direct results and nested data.result
	var results []interface{}
	if data, ok := resp["data"].(map[string]interface{}); ok {
		if r, ok := data["result"].([]interface{}); ok {
			results = r
		}
	}
	if r, ok := resp["result"].([]interface{}); ok {
		results = r
	}
	if r, ok := resp["results"].([]interface{}); ok {
		results = r
	}

	if len(results) == 0 {
		return body
	}

	for _, r := range results {
		entry, ok := r.(map[string]interface{})
		if !ok {
			continue
		}
		// Translate "metric" labels map
		if metricRaw, ok := entry["metric"]; ok {
			if metric, ok := metricRaw.(map[string]interface{}); ok {
				translated := make(map[string]interface{}, len(metric))
				for k, v := range metric {
					if k == "__name__" {
						continue
					}
					lokiKey := k
					if !p.labelTranslator.IsPassthrough() {
						lokiKey = p.labelTranslator.ToLoki(k)
					}
					translated[lokiKey] = v
				}
				if strings.Contains(originalQuery, "detected_level") {
					if _, ok := translated["detected_level"]; !ok {
						if value, ok := translated["level"]; ok {
							translated["detected_level"] = value
							delete(translated, "level")
						}
					}
				}
				entry["metric"] = translated
			}
		}
	}

	result, err := json.Marshal(resp)
	if err != nil {
		return body
	}
	return result
}
