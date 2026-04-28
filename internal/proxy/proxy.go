package proxy

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
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

// compat cache capture buffers are pooled separately because large query_range
// misses can otherwise spend most heap growth on append churn before the entry
// is even admitted to cache.
const (
	maxPooledCompatCaptureBufSize = 64 * 1024
	defaultCompatCaptureBufSize   = 32 * 1024
)

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

type pooledCompatCaptureBuf struct {
	data []byte
}

var compatCaptureBufPool = sync.Pool{
	New: func() interface{} {
		return &pooledCompatCaptureBuf{data: make([]byte, 0, defaultCompatCaptureBufSize)}
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

	cacheResult            string
	upstreamCalls          int
	upstreamDuration       time.Duration
	upstreamLastCode       int
	upstreamErrorSeen      bool
	upstreamCallsByType    map[string]int
	upstreamDurationByType map[string]time.Duration
	internalOpsByType      map[string]int
	internalDurationByType map[string]time.Duration
}

type requestTelemetrySnapshot struct {
	cacheResult            string
	upstreamCalls          int
	upstreamDuration       time.Duration
	upstreamLastCode       int
	upstreamErrorSeen      bool
	upstreamCallsByType    map[string]int
	upstreamDurationByType map[string]time.Duration
	internalOpsByType      map[string]int
	internalDurationByType map[string]time.Duration
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

func upstreamBreakdownKey(system, requestType string) string {
	system = strings.TrimSpace(system)
	requestType = strings.TrimSpace(requestType)
	if requestType == "" {
		requestType = "unknown"
	}
	if system == "" {
		return requestType
	}
	return system + ":" + requestType
}

func internalOperationBreakdownKey(operation, outcome string) string {
	operation = strings.TrimSpace(operation)
	outcome = strings.TrimSpace(outcome)
	if operation == "" {
		operation = "unknown"
	}
	if outcome == "" {
		outcome = "unknown"
	}
	return operation + ":" + outcome
}

func recordUpstreamCall(ctx context.Context, system, requestType string, statusCode int, duration time.Duration, hadError bool) {
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return
	}
	rt.mu.Lock()
	rt.upstreamCalls++
	rt.upstreamDuration += duration
	rt.upstreamLastCode = statusCode
	rt.upstreamErrorSeen = rt.upstreamErrorSeen || hadError
	if requestType != "" {
		key := upstreamBreakdownKey(system, requestType)
		if rt.upstreamCallsByType == nil {
			rt.upstreamCallsByType = make(map[string]int)
		}
		if rt.upstreamDurationByType == nil {
			rt.upstreamDurationByType = make(map[string]time.Duration)
		}
		rt.upstreamCallsByType[key]++
		rt.upstreamDurationByType[key] += duration
	}
	rt.mu.Unlock()
}

func recordInternalOperation(ctx context.Context, operation, outcome string, duration time.Duration) {
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return
	}
	if duration < 0 {
		duration = 0
	}
	rt.mu.Lock()
	key := internalOperationBreakdownKey(operation, outcome)
	if rt.internalOpsByType == nil {
		rt.internalOpsByType = make(map[string]int)
	}
	if rt.internalDurationByType == nil {
		rt.internalDurationByType = make(map[string]time.Duration)
	}
	rt.internalOpsByType[key]++
	rt.internalDurationByType[key] += duration
	rt.mu.Unlock()
}

func snapshotTelemetry(ctx context.Context) requestTelemetrySnapshot {
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return requestTelemetrySnapshot{cacheResult: "bypass"}
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	upstreamCallsByType := make(map[string]int, len(rt.upstreamCallsByType))
	for key, value := range rt.upstreamCallsByType {
		upstreamCallsByType[key] = value
	}
	upstreamDurationByType := make(map[string]time.Duration, len(rt.upstreamDurationByType))
	for key, value := range rt.upstreamDurationByType {
		upstreamDurationByType[key] = value
	}
	internalOpsByType := make(map[string]int, len(rt.internalOpsByType))
	for key, value := range rt.internalOpsByType {
		internalOpsByType[key] = value
	}
	internalDurationByType := make(map[string]time.Duration, len(rt.internalDurationByType))
	for key, value := range rt.internalDurationByType {
		internalDurationByType[key] = value
	}
	return requestTelemetrySnapshot{
		cacheResult:            rt.cacheResult,
		upstreamCalls:          rt.upstreamCalls,
		upstreamDuration:       rt.upstreamDuration,
		upstreamLastCode:       rt.upstreamLastCode,
		upstreamErrorSeen:      rt.upstreamErrorSeen,
		upstreamCallsByType:    upstreamCallsByType,
		upstreamDurationByType: upstreamDurationByType,
		internalOpsByType:      internalOpsByType,
		internalDurationByType: internalDurationByType,
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
	CBFailThreshold   int                      // circuit breaker: failures within window before opening
	CBOpenDuration    time.Duration            // circuit breaker: how long to stay open before probing
	CBWindowDuration  time.Duration            // circuit breaker: sliding window for failure counting (default 30s)
	CoalescerDisabled bool                     // disable singleflight coalescing; every request makes its own backend call
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
	MetricsMaxTenants            int
	MetricsMaxClients            int
	MetricsTrustProxyHeaders     bool
	MetricsExportSensitiveLabels bool
	MetricsMaxConcurrency        int

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
	maxZeroFillBuckets                = 32768
	maxPatternBackendQueryLimit       = 20000
	maxPatternSecondPassLineLimit     = 8000
	maxPatternSecondPassWindows       = 8
	maxUserDrivenSlicePrealloc        = 512
	tailWriteTimeout                  = 2 * time.Second
	maxMultiTenantFanout              = 64
	maxMultiTenantMergedResponseBytes = 32 << 20
	maxDetectedScanLines              = 2000
	maxSyntheticTailSeenEntries       = 4096
	maxBufferedBackendBodyBytes       = 64 << 20
	maxPatternsPeerSnapshotBytes      = 8 << 20
	maxLabelValuesPeerSnapshotBytes   = 8 << 20
	maxUpstreamErrorBodyBytes         = 4 << 10
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
	metricsConcurrencyLimiter             chan struct{}
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
	patternsPersistDigest                 [sha256.Size]byte
	patternsPersistDigestReady            bool
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
	labelValuesIndexPersistDigest         [sha256.Size]byte
	labelValuesIndexPersistDigestReady    bool
	readCacheKeyMemoMu                    sync.RWMutex
	readCacheKeyMemo                      map[canonicalReadCacheMemoKey]string
}

const maxReadCacheKeyMemoEntries = 16384

type canonicalReadCacheMemoKey struct {
	endpoint string
	orgID    string
	extra    string
	rawQuery string
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

func newCoalescer(disabled bool) *mw.Coalescer {
	if disabled {
		return mw.NewCoalescerDisabled()
	}
	return mw.NewCoalescer()
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
		// 2 s is short enough that users barely notice a trip while still damping
		// burst storms. Coalescing and caching protect the backend between probes.
		cbOpen = 2 * time.Second
	}
	cbWindow := cfg.CBWindowDuration
	if cbWindow <= 0 {
		cbWindow = 30 * time.Second
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
	proxyMetrics := metrics.NewMetricsWithOptions(cfg.MetricsMaxTenants, cfg.MetricsMaxClients, cfg.MetricsExportSensitiveLabels)
	if cfg.Cache != nil {
		proxyMetrics.SetCacheStatsProvider(cfg.Cache.Stats)
	}

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
		metrics:                               proxyMetrics,
		queryTracker:                          metrics.NewQueryTracker(10000),
		coalescer:                             newCoalescer(cfg.CoalescerDisabled),
		limiter:                               mw.NewRateLimiter(maxConcurrent, ratePerSec, rateBurst),
		breaker:                               mw.NewCircuitBreaker(cbFail, 3, cbOpen, cbWindow),
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
		metricsConcurrencyLimiter:             buildConcurrencyLimiter(cfg.MetricsMaxConcurrency),
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
		readCacheKeyMemo:                      make(map[canonicalReadCacheMemoKey]string, 2048),
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
	if p.limiter != nil {
		p.limiter.Stop()
	}
	if p.peerCache != nil {
		p.peerCache.Close()
	}
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

func setSecurityHeaders(header http.Header) {
	if strings.TrimSpace(header.Get("X-Content-Type-Options")) == "" {
		header.Set("X-Content-Type-Options", "nosniff")
	}
	if strings.TrimSpace(header.Get("X-Frame-Options")) == "" {
		header.Set("X-Frame-Options", "DENY")
	}
	if strings.TrimSpace(header.Get("Cross-Origin-Resource-Policy")) == "" {
		header.Set("Cross-Origin-Resource-Policy", "same-origin")
	}
	if strings.TrimSpace(header.Get("Cache-Control")) == "" {
		header.Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
	}
	if strings.TrimSpace(header.Get("Pragma")) == "" {
		header.Set("Pragma", "no-cache")
	}
	if strings.TrimSpace(header.Get("Expires")) == "" {
		header.Set("Expires", "0")
	}
}

// securityHeaders wraps a handler with baseline security response headers.
func securityHeaders(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setSecurityHeaders(w.Header())
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
	return p.allowGlobalTenant
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
	key := "compat:v1:" + endpoint + ":" + r.Header.Get("X-Scope-OrgID") + ":" + r.URL.Path + "?" + r.URL.RawQuery
	if fp := p.forwardedAuthFingerprint(r); fp != "" {
		key += ":auth:" + fp
	}
	return key, true
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
	body       []byte
	bufHolder  *pooledCompatCaptureBuf
	code       int
	flushed    bool
	limit      int
	overflowed bool
}

func newCompatCacheCaptureWriter(w http.ResponseWriter, limit int) *compatCacheCaptureWriter {
	body, holder := acquireCompatCaptureBuf(limit)
	return &compatCacheCaptureWriter{
		ResponseWriter: w,
		body:           body,
		bufHolder:      holder,
		limit:          limit,
	}
}

func (w *compatCacheCaptureWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *compatCacheCaptureWriter) Write(b []byte) (int, error) {
	if w.code == 0 {
		w.code = http.StatusOK
	}
	if strings.TrimSpace(w.Header().Get("Content-Type")) == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	if strings.TrimSpace(w.Header().Get("X-Content-Type-Options")) == "" {
		w.Header().Set("X-Content-Type-Options", "nosniff")
	}
	w.capture(b)
	return w.ResponseWriter.Write(b)
}

func (w *compatCacheCaptureWriter) Flush() {
	w.flushed = true
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *compatCacheCaptureWriter) capture(b []byte) {
	if w.overflowed || len(b) == 0 {
		return
	}
	if w.limit > 0 && len(w.body)+len(b) > w.limit {
		w.overflow()
		return
	}
	w.body = append(w.body, b...)
}

func (w *compatCacheCaptureWriter) overflow() {
	if w.overflowed {
		return
	}
	w.overflowed = true
	releaseCompatCaptureBuf(w.body, w.bufHolder)
	w.body = nil
	w.bufHolder = nil
}

func (w *compatCacheCaptureWriter) CapturedBody() []byte {
	if w.overflowed {
		return nil
	}
	return w.body
}

func (w *compatCacheCaptureWriter) Release() {
	releaseCompatCaptureBuf(w.body, w.bufHolder)
	w.body = nil
	w.bufHolder = nil
}

func acquireCompatCaptureBuf(limit int) ([]byte, *pooledCompatCaptureBuf) {
	size := defaultCompatCaptureBufSize
	if limit > 0 {
		size = min(size, limit)
	}
	holder := compatCaptureBufPool.Get().(*pooledCompatCaptureBuf)
	if cap(holder.data) < size {
		holder.data = make([]byte, 0, size)
	}
	holder.data = holder.data[:0]
	return holder.data, holder
}

func releaseCompatCaptureBuf(buf []byte, holder *pooledCompatCaptureBuf) {
	if holder == nil {
		return
	}
	if buf != nil && cap(buf) <= maxPooledCompatCaptureBufSize {
		holder.data = buf[:0]
	} else {
		holder.data = make([]byte, 0, defaultCompatCaptureBufSize)
	}
	compatCaptureBufPool.Put(holder)
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
		var (
			capture            *compatCacheCaptureWriter
			captureAllowed     bool
			capturePatternsNil bool
			captureBodyLen     int
		)
		if encoding, _ := p.compatCacheResponseEncoding(r); encoding != "" {
			_ = mw.RegisterEncodedResponseCapture(w, encoding, func(encodedAs string, encoded []byte) {
				if !captureAllowed || capturePatternsNil || captureBodyLen == 0 {
					return
				}
				if len(encoded) == 0 || len(encoded) >= captureBodyLen {
					return
				}
				p.compatCache.SetWithTTL(compatCacheVariantKey(cacheKey, encodedAs), encoded, ttl)
			})
		}
		capture = newCompatCacheCaptureWriter(w, p.compatCache.MaxEntrySizeBytes())
		next(capture, r)
		if compatCacheCaptureAllowed(capture.code, capture.flushed, w.Header()) {
			body := capture.CapturedBody()
			captureBodyLen = len(body)
			captureAllowed = captureBodyLen > 0
			if endpoint == "patterns" && captureAllowed {
				capturePatternsNil = patternsPayloadEmpty(body)
				if capturePatternsNil {
					capture.Release()
					return
				}
			}
			if captureAllowed {
				p.compatCache.SetWithTTL(cacheKey, append([]byte(nil), body...), ttl)
			}
		}
		capture.Release()
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
		// Prometheus metrics endpoint — security headers plus bounded scrape concurrency.
		mux.Handle("/metrics", securityHeaders(http.HandlerFunc(p.handleMetrics)))
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
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if limiter := p.metricsConcurrencyLimiter; limiter != nil {
		select {
		case limiter <- struct{}{}:
			defer func() { <-limiter }()
		default:
			w.Header().Set("Retry-After", "1")
			http.Error(w, "metrics scrape already in progress", http.StatusTooManyRequests)
			return
		}
	}
	p.metrics.Handler(w, r)
	if p.peerCache != nil {
		_, _ = io.WriteString(w, p.peerCacheMetrics())
	}
}

func (p *Proxy) peerCacheMetrics() string {
	stats := p.peerCache.Stats()
	remotePeers, _ := stats["peers"].(int)
	hits, _ := stats["peer_hits"].(int64)
	misses, _ := stats["peer_misses"].(int64)
	errors, _ := stats["peer_errors"].(int64)
	errorReasons, _ := stats["peer_error_reasons"].(map[string]int64)
	wtPushes, _ := stats["wt_pushes"].(int64)
	wtErrors, _ := stats["wt_errors"].(int64)
	raHotRequests, _ := stats["ra_hot_requests"].(int64)
	raHotErrors, _ := stats["ra_hot_errors"].(int64)
	raPrefetches, _ := stats["ra_prefetches"].(int64)
	raPrefetchBytes, _ := stats["ra_prefetch_bytes"].(int64)
	raBudgetDrops, _ := stats["ra_budget_drops"].(int64)
	raTenantSkips, _ := stats["ra_tenant_skips"].(int64)
	clusterMembers := len(p.peerCache.Peers())
	var reasonLines strings.Builder
	if len(errorReasons) > 0 {
		reasons := make([]string, 0, len(errorReasons))
		for reason := range errorReasons {
			reasons = append(reasons, reason)
		}
		sort.Strings(reasons)
		reasonLines.WriteString("# HELP loki_vl_proxy_peer_cache_error_reason_total Peer-cache fetch errors by reason.\n")
		reasonLines.WriteString("# TYPE loki_vl_proxy_peer_cache_error_reason_total counter\n")
		for _, reason := range reasons {
			fmt.Fprintf(&reasonLines, "loki_vl_proxy_peer_cache_error_reason_total{reason=%q} %d\n", reason, errorReasons[reason])
		}
	}

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
			"%s"+
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
		remotePeers, clusterMembers, hits, misses, errors, reasonLines.String(), wtPushes, wtErrors, raHotRequests, raHotErrors, raPrefetches, raPrefetchBytes, raBudgetDrops, raTenantSkips,
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

	logqlQuery = resolveGrafanaRangeTemplateTokens(logqlQuery, r.FormValue("start"), r.FormValue("end"), r.FormValue("step"))
	logqlQuery = p.preferWorkingParser(r.Context(), logqlQuery, r.FormValue("start"), r.FormValue("end"))

	if spec, ok := parseBareParserMetricCompatSpec(logqlQuery); ok {
		resolvedSpec, resolved := resolveBareParserMetricRangeWindow(spec, r.FormValue("start"), r.FormValue("end"), r.FormValue("step"))
		if !resolved {
			p.writeError(w, http.StatusBadRequest, "invalid range selector")
			p.metrics.RecordRequest("query_range", http.StatusBadRequest, time.Since(start))
			return
		}
		p.proxyBareParserMetricQueryRange(w, r, start, logqlQuery, resolvedSpec)
		return
	}

	if postAgg, ok := parseInstantMetricPostAggQuery(logqlQuery); ok {
		p.handleRangeMetricPostAggregation(w, r, start, logqlQuery, postAgg)
		return
	}

	logsqlQuery, err := p.translateQueryWithContext(r.Context(), logqlQuery)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		p.metrics.RecordRequest("query_range", http.StatusBadRequest, time.Since(start))
		return
	}
	// Extract without() labels and label-transform markers for post-processing.
	logsqlQuery, withoutLabels := translator.ParseWithoutMarker(logsqlQuery)
	logsqlQuery, isGroupQuery := translator.ParseGroupMarker(logsqlQuery)
	logsqlQuery, labelReplaceSpec := translator.ParseLabelReplaceMarker(logsqlQuery)
	logsqlQuery, labelJoinSpec := translator.ParseLabelJoinMarker(logsqlQuery)
	logsqlQuery = preserveMetricStreamIdentity(logqlQuery, logsqlQuery, withoutLabels)
	if isBareMetricFunctionQuery(strings.TrimSpace(logqlQuery)) && !isStatsQuery(logsqlQuery) {
		p.writeError(w, http.StatusBadRequest, "unsupported metric query: range aggregations require compatible unwrap or translator support")
		p.metrics.RecordRequest("query_range", http.StatusBadRequest, time.Since(start))
		return
	}
	p.log.Debug("translated query", "logsql", logsqlQuery, "without", withoutLabels)

	r = withOrgID(r)

	needsCapture := len(withoutLabels) > 0 || isGroupQuery || labelReplaceSpec != nil || labelJoinSpec != nil
	var (
		sc       = &statusCapture{ResponseWriter: w, code: 200}
		capture  *bufferedResponseWriter
		cacheTap *compatCacheCaptureWriter
	)
	if needsCapture {
		capture = &bufferedResponseWriter{header: make(http.Header)}
		sc = &statusCapture{ResponseWriter: capture, code: 200}
	} else if cacheable {
		cacheTap = newCompatCacheCaptureWriter(w, p.cache.MaxEntrySizeBytes())
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
		if isGroupQuery {
			cacheOut = applyGroupNormalization(cacheOut)
		}
		if labelReplaceSpec != nil {
			cacheOut = applyLabelReplace(cacheOut, *labelReplaceSpec)
		}
		if labelJoinSpec != nil {
			cacheOut = applyLabelJoin(cacheOut, *labelJoinSpec)
		}
		copyBackendHeaders(w.Header(), capture.Header())
		if w.Header().Get("Content-Type") == "" {
			w.Header().Set("Content-Type", "application/json")
		}
		if sc.code != http.StatusOK {
			w.WriteHeader(sc.code)
		}
		_, _ = w.Write(cacheOut)
		if cacheable && sc.code == http.StatusOK {
			p.setLocalReadCacheWithTTL(cacheKey, append([]byte(nil), cacheOut...), CacheTTLs["query_range"])
		}
	} else if cacheTap != nil {
		if cacheable && sc.code == http.StatusOK {
			if body := cacheTap.CapturedBody(); len(body) > 0 {
				p.setLocalReadCacheWithTTL(cacheKey, append([]byte(nil), body...), CacheTTLs["query_range"])
			}
		}
		cacheTap.Release()
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

	logqlQuery = resolveGrafanaRangeTemplateTokens(logqlQuery, r.FormValue("start"), r.FormValue("end"), r.FormValue("step"))
	logqlQuery = p.preferWorkingParser(r.Context(), logqlQuery, r.FormValue("start"), r.FormValue("end"))

	if spec, ok := parseBareParserMetricCompatSpec(logqlQuery); ok {
		resolvedSpec, resolved := resolveBareParserMetricRangeWindow(spec, r.FormValue("start"), r.FormValue("end"), r.FormValue("step"))
		if !resolved {
			p.writeError(w, http.StatusBadRequest, "invalid range selector")
			p.metrics.RecordRequest("query", http.StatusBadRequest, time.Since(start))
			return
		}
		p.proxyBareParserMetricQuery(w, r, start, logqlQuery, resolvedSpec)
		return
	}
	if spec, ok := parseAbsentOverTimeCompatSpec(logqlQuery); ok {
		p.proxyAbsentOverTimeQuery(w, r, start, logqlQuery, spec)
		return
	}

	if postAgg, ok := parseInstantMetricPostAggQuery(logqlQuery); ok {
		p.handleInstantMetricPostAggregation(w, r, start, logqlQuery, postAgg)
		return
	}

	logsqlQuery, err := p.translateQueryWithContext(r.Context(), logqlQuery)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		p.metrics.RecordRequest("query", http.StatusBadRequest, time.Since(start))
		return
	}

	// Extract without() labels and label-transform markers for post-processing.
	logsqlQuery, withoutLabels := translator.ParseWithoutMarker(logsqlQuery)
	logsqlQuery, isGroupQuery := translator.ParseGroupMarker(logsqlQuery)
	logsqlQuery, labelReplaceSpec := translator.ParseLabelReplaceMarker(logsqlQuery)
	logsqlQuery, labelJoinSpec := translator.ParseLabelJoinMarker(logsqlQuery)
	logsqlQuery = preserveMetricStreamIdentity(logqlQuery, logsqlQuery, withoutLabels)
	if isBareMetricFunctionQuery(strings.TrimSpace(logqlQuery)) && !isStatsQuery(logsqlQuery) {
		p.writeError(w, http.StatusBadRequest, "unsupported metric query: range aggregations require compatible unwrap or translator support")
		p.metrics.RecordRequest("query", http.StatusBadRequest, time.Since(start))
		return
	}

	r = withOrgID(r)

	// Wrap writer to capture actual status code for metrics
	sc := &statusCapture{ResponseWriter: w, code: 200}

	needsCapture := len(withoutLabels) > 0 || isGroupQuery || labelReplaceSpec != nil || labelJoinSpec != nil
	var bw *bufferedResponseWriter
	if needsCapture {
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
		p.writeError(sc, http.StatusBadRequest, "log queries are not supported as an instant query type, please change your query to a range query type")
	}

	if bw != nil && needsCapture {
		result := bw.body
		if len(withoutLabels) > 0 {
			result = applyWithoutGrouping(result, withoutLabels)
		}
		if isGroupQuery {
			result = applyGroupNormalization(result)
		}
		if labelReplaceSpec != nil {
			result = applyLabelReplace(result, *labelReplaceSpec)
		}
		if labelJoinSpec != nil {
			result = applyLabelJoin(result, *labelJoinSpec)
		}
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

type instantMetricPostAgg struct {
	name  string
	inner string
	k     int
}

func parseInstantMetricPostAggQuery(logql string) (instantMetricPostAgg, bool) {
	logql = strings.TrimSpace(logql)
	for _, name := range []string{"sort_desc", "sort"} {
		prefix := name + "("
		if !strings.HasPrefix(logql, prefix) || !strings.HasSuffix(logql, ")") {
			continue
		}
		inner := strings.TrimSpace(logql[len(prefix) : len(logql)-1])
		if inner == "" {
			return instantMetricPostAgg{}, false
		}
		return instantMetricPostAgg{name: name, inner: inner}, true
	}
	for _, name := range []string{"topk", "bottomk"} {
		prefix := name + "("
		if !strings.HasPrefix(logql, prefix) || !strings.HasSuffix(logql, ")") {
			continue
		}
		args := strings.TrimSpace(logql[len(prefix) : len(logql)-1])
		comma := topLevelCommaIndex(args)
		if comma <= 0 {
			return instantMetricPostAgg{}, false
		}
		k, err := strconv.Atoi(strings.TrimSpace(args[:comma]))
		if err != nil || k <= 0 {
			return instantMetricPostAgg{}, false
		}
		inner := strings.TrimSpace(args[comma+1:])
		if inner == "" {
			return instantMetricPostAgg{}, false
		}
		return instantMetricPostAgg{name: name, inner: inner, k: k}, true
	}
	return instantMetricPostAgg{}, false
}

func topLevelCommaIndex(s string) int {
	depth := 0
	inQuote := false
	for i, r := range s {
		switch r {
		case '"':
			inQuote = !inQuote
		case '(':
			if !inQuote {
				depth++
			}
		case ')':
			if !inQuote && depth > 0 {
				depth--
			}
		case ',':
			if !inQuote && depth == 0 {
				return i
			}
		}
	}
	return -1
}

func preserveMetricStreamIdentity(originalLogQL, translatedLogsQL string, withoutLabels []string) string {
	if !isStatsQuery(translatedLogsQL) {
		return translatedLogsQL
	}
	if strings.Contains(translatedLogsQL, "| stats by (") {
		return translatedLogsQL
	}
	if len(withoutLabels) > 0 || isBareMetricFunctionQuery(strings.TrimSpace(originalLogQL)) {
		return addStatsByStreamClause(translatedLogsQL)
	}
	return translatedLogsQL
}

func isBareMetricFunctionQuery(logql string) bool {
	for _, prefix := range []string{
		"rate(",
		"rate_counter(",
		"count_over_time(",
		"bytes_over_time(",
		"bytes_rate(",
		"sum_over_time(",
		"avg_over_time(",
		"max_over_time(",
		"min_over_time(",
		"first_over_time(",
		"last_over_time(",
		"stddev_over_time(",
		"stdvar_over_time(",
		"absent_over_time(",
		"quantile_over_time(",
	} {
		if strings.HasPrefix(logql, prefix) {
			return true
		}
	}
	return false
}

func addStatsByStreamClause(logsqlQuery string) string {
	idx := strings.Index(logsqlQuery, "| stats ")
	if idx < 0 {
		return logsqlQuery
	}
	statsStart := idx + len("| stats ")
	return logsqlQuery[:statsStart] + "by (_stream, level) " + logsqlQuery[statsStart:]
}

func (p *Proxy) handleInstantMetricPostAggregation(w http.ResponseWriter, r *http.Request, start time.Time, originalQuery string, postAgg instantMetricPostAgg) {
	translatedInner, err := p.translateQueryWithContext(r.Context(), postAgg.inner)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		p.metrics.RecordRequest("query", http.StatusBadRequest, time.Since(start))
		return
	}
	translatedInner, withoutLabels := translator.ParseWithoutMarker(translatedInner)
	translatedInner = preserveMetricStreamIdentity(postAgg.inner, translatedInner, withoutLabels)

	r = withOrgID(r)

	bw := &bufferedResponseWriter{header: make(http.Header)}
	sc := &statusCapture{ResponseWriter: bw, code: 200}
	if outerFunc, innerQL, rng, step, ok := translator.ParseSubqueryExpr(translatedInner); ok {
		p.proxySubquery(sc, r, outerFunc, innerQL, rng, step)
	} else if op, left, right, vm, ok := translator.ParseBinaryMetricExprFull(translatedInner); ok {
		p.proxyBinaryMetricQueryVM(sc, r, op, left, right, vm)
	} else if isStatsQuery(translatedInner) {
		p.proxyStatsQuery(sc, r, translatedInner)
	} else {
		p.writeError(w, http.StatusBadRequest, "unsupported instant aggregation target")
		p.metrics.RecordRequest("query", http.StatusBadRequest, time.Since(start))
		return
	}

	if len(withoutLabels) > 0 {
		bw.body = applyWithoutGrouping(bw.body, withoutLabels)
	}

	if sc.code >= http.StatusBadRequest {
		copyHeaders(w.Header(), bw.Header())
		if w.Header().Get("Content-Type") == "" {
			w.Header().Set("Content-Type", "application/json")
		}
		w.WriteHeader(sc.code)
		_, _ = w.Write(bw.body)
		elapsed := time.Since(start)
		p.metrics.RecordRequest("query", sc.code, elapsed)
		p.queryTracker.Record("query", originalQuery, elapsed, true)
		return
	}

	result := applyInstantVectorPostAggregation(bw.body, postAgg)
	copyHeaders(w.Header(), bw.Header())
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	_, _ = w.Write(result)
	elapsed := time.Since(start)
	p.metrics.RecordRequest("query", http.StatusOK, elapsed)
	p.queryTracker.Record("query", originalQuery, elapsed, false)
}

// handleRangeMetricPostAggregation handles topk/bottomk/sort at /query_range by
// fetching the full matrix from VL and then trimming to the requested K series.
func (p *Proxy) handleRangeMetricPostAggregation(w http.ResponseWriter, r *http.Request, start time.Time, originalQuery string, postAgg instantMetricPostAgg) {
	translatedInner, err := p.translateQueryWithContext(r.Context(), postAgg.inner)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		p.metrics.RecordRequest("query_range", http.StatusBadRequest, time.Since(start))
		return
	}
	translatedInner, withoutLabels := translator.ParseWithoutMarker(translatedInner)
	translatedInner = preserveMetricStreamIdentity(postAgg.inner, translatedInner, withoutLabels)

	r = withOrgID(r)

	bw := &bufferedResponseWriter{header: make(http.Header)}
	sc := &statusCapture{ResponseWriter: bw, code: 200}
	p.proxyStatsQueryRange(sc, r, translatedInner)

	if len(withoutLabels) > 0 {
		bw.body = applyWithoutGrouping(bw.body, withoutLabels)
	}

	if sc.code >= http.StatusBadRequest {
		copyHeaders(w.Header(), bw.Header())
		if w.Header().Get("Content-Type") == "" {
			w.Header().Set("Content-Type", "application/json")
		}
		w.WriteHeader(sc.code)
		_, _ = w.Write(bw.body)
		elapsed := time.Since(start)
		p.metrics.RecordRequest("query_range", sc.code, elapsed)
		p.queryTracker.Record("query_range", originalQuery, elapsed, true)
		return
	}

	result := applyMatrixPostAggregation(bw.body, postAgg)
	copyHeaders(w.Header(), bw.Header())
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	_, _ = w.Write(result)
	elapsed := time.Since(start)
	p.metrics.RecordRequest("query_range", http.StatusOK, elapsed)
	p.queryTracker.Record("query_range", originalQuery, elapsed, false)
}

// applyMatrixPostAggregation applies topk/bottomk/sort to a matrix (query_range) result.
// It ranks series by their last value and trims to the requested K.
func applyMatrixPostAggregation(body []byte, postAgg instantMetricPostAgg) []byte {
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]interface{} `json:"metric"`
				Values [][]interface{}        `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Status != "success" || resp.Data.ResultType != "matrix" {
		return body
	}

	// Rank by last value of each series
	type ranked struct {
		idx  int
		last float64
	}
	ranks := make([]ranked, len(resp.Data.Result))
	for i, s := range resp.Data.Result {
		ranks[i].idx = i
		if len(s.Values) > 0 {
			last := s.Values[len(s.Values)-1]
			if len(last) >= 2 {
				if v, err := parseFloat(last[1]); err == nil {
					ranks[i].last = v
				}
			}
		}
	}

	sort.SliceStable(ranks, func(i, j int) bool {
		li, lj := ranks[i].last, ranks[j].last
		switch postAgg.name {
		case "bottomk":
			if li == lj {
				return ranks[i].idx < ranks[j].idx
			}
			return li < lj
		default: // topk, sort_desc, sort
			if li == lj {
				return ranks[i].idx < ranks[j].idx
			}
			return li > lj
		}
	})

	// Ensure topk size is safe: bounded by min(requested, max constant, available)
	const maxTopK = 10000
	safeSize := postAgg.k
	if safeSize < 0 {
		safeSize = 0
	}
	// Use min to create an allocation size that's clearly bounded
	allocSize := safeSize
	if allocSize > maxTopK {
		allocSize = maxTopK
	}
	if allocSize > len(ranks) {
		allocSize = len(ranks)
	}

	// Pre-allocate with safe maximum size to avoid CodeQL taint analysis issues
	// with user-provided allocation sizes. Use a fixed-size allocation and populate
	// only the needed elements.
	const preallocSize = 10000
	selected := make([]struct {
		Metric map[string]interface{} `json:"metric"`
		Values [][]interface{}        `json:"values"`
	}, preallocSize)

	// Only populate the needed number of results
	resultCount := allocSize
	if resultCount > len(selected) {
		resultCount = len(selected)
	}
	for i := 0; i < resultCount; i++ {
		selected[i] = resp.Data.Result[ranks[i].idx]
	}
	resp.Data.Result = selected[:resultCount]

	out, err := json.Marshal(resp)
	if err != nil {
		return body
	}
	return out
}

func parseFloat(v interface{}) (float64, error) {
	switch t := v.(type) {
	case float64:
		return t, nil
	case string:
		return strconv.ParseFloat(t, 64)
	default:
		return 0, fmt.Errorf("not a number")
	}
}

func applyInstantVectorPostAggregation(body []byte, postAgg instantMetricPostAgg) []byte {
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]interface{} `json:"metric"`
				Value  []interface{}          `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Status != "success" || resp.Data.ResultType != "vector" {
		return body
	}

	sort.SliceStable(resp.Data.Result, func(i, j int) bool {
		left := vectorPointValue(resp.Data.Result[i].Value)
		right := vectorPointValue(resp.Data.Result[j].Value)
		switch postAgg.name {
		case "sort", "bottomk":
			if left == right {
				return metricKey(resp.Data.Result[i].Metric) < metricKey(resp.Data.Result[j].Metric)
			}
			return left < right
		default:
			if left == right {
				return metricKey(resp.Data.Result[i].Metric) < metricKey(resp.Data.Result[j].Metric)
			}
			return left > right
		}
	})

	if (postAgg.name == "topk" || postAgg.name == "bottomk") && postAgg.k < len(resp.Data.Result) {
		resp.Data.Result = resp.Data.Result[:postAgg.k]
	}

	out, err := json.Marshal(resp)
	if err != nil {
		return body
	}
	return out
}

func vectorPointValue(value []interface{}) float64 {
	if len(value) < 2 {
		return 0
	}
	switch raw := value[1].(type) {
	case string:
		parsed, _ := strconv.ParseFloat(raw, 64)
		return parsed
	case float64:
		return raw
	default:
		return 0
	}
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
	seen := make(map[string]struct{}, safeAddCap(len(dst), len(values)))
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

func (p *Proxy) metadataQueryParams(ctx context.Context, candidate, start, end, limit, search string) (url.Values, error) {
	params := url.Values{}
	translated, err := p.translateQueryWithContext(ctx, candidate)
	if err != nil {
		return nil, err
	}
	params.Set("query", translated)
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
	return params, nil
}

func (p *Proxy) fetchScopedLabelNames(ctx context.Context, rawQuery, start, end, search string, useInventoryCache bool) ([]string, error) {
	candidates := metadataQueryCandidates(rawQuery)
	var lastErr error
	for i, candidate := range candidates {
		params, err := p.metadataQueryParams(ctx, candidate, start, end, "", search)
		if err != nil {
			lastErr = err
			if i+1 < len(candidates) {
				p.observeInternalOperation(ctx, "discovery_fallback", "label_names_relaxed_after_error", 0)
			}
			continue
		}
		var labels []string
		if useInventoryCache {
			labels, err = p.fetchPreferredLabelNamesCached(ctx, params)
		} else {
			labels, err = p.fetchPreferredLabelNames(ctx, params)
		}
		if err != nil {
			lastErr = err
			if i+1 < len(candidates) {
				p.observeInternalOperation(ctx, "discovery_fallback", "label_names_relaxed_after_error", 0)
			}
			continue
		}
		if len(labels) == 0 && i+1 < len(candidates) {
			p.observeInternalOperation(ctx, "discovery_fallback", "label_names_empty_primary", 0)
		}
		return labels, nil
	}
	return nil, lastErr
}

func (p *Proxy) fetchScopedLabelValues(ctx context.Context, labelName, rawQuery, start, end, limit, search string) ([]string, error) {
	candidates := metadataQueryCandidates(rawQuery)
	var lastErr error
	for i, candidate := range candidates {
		params, err := p.metadataQueryParams(ctx, candidate, start, end, limit, search)
		if err != nil {
			lastErr = err
			if i+1 < len(candidates) {
				p.observeInternalOperation(ctx, "discovery_fallback", "label_values_relaxed_after_error", 0)
			}
			continue
		}
		values, err := p.fetchPreferredLabelValues(ctx, labelName, params)
		if err != nil {
			lastErr = err
			if i+1 < len(candidates) {
				p.observeInternalOperation(ctx, "discovery_fallback", "label_values_relaxed_after_error", 0)
			}
			continue
		}
		if len(values) == 0 && i+1 < len(candidates) {
			p.observeInternalOperation(ctx, "discovery_fallback", "label_values_empty_primary", 0)
		}
		return values, nil
	}
	return nil, lastErr
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
	// Include per-user auth fingerprint to prevent cross-user coalescing when
	// forwarded auth headers/cookies are configured.
	if origReq, ok := ctx.Value(origRequestKey).(*http.Request); ok && origReq != nil {
		if fp := p.forwardedAuthFingerprint(origReq); fp != "" {
			key += ":auth:" + fp
		}
	}
	status, _, body, err := p.coalescer.DoWithGuard(key, p.breaker.Allow, func() (*http.Response, error) {
		return p.vlGetInner(ctx, path, params)
	})
	if errors.Is(err, mw.ErrGuardRejected) {
		return 0, nil, fmt.Errorf("circuit breaker open — backend unavailable")
	}
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

			labels, fetchErr := p.fetchScopedLabelNames(ctx, rawQuery, start, end, search, false)
			if fetchErr != nil {
				return nil, fetchErr
			}

			filtered := make([]string, 0, len(labels))
			for _, v := range labels {
				// Filter internal VL fields only (before translation)
				if isVLInternalField(v) || v == "detected_level" {
					continue
				}
				filtered = append(filtered, v)
			}
			// Translate dots to underscores
			labels = p.labelTranslator.TranslateLabelsList(filtered)
			labels = appendSyntheticLabels(labels)
			p.setEndpointReadCacheWithTTL("labels", cacheKey, lokiLabelsResponse(labels), CacheTTLs["labels"])
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
				values, fetchErr = p.fetchScopedLabelValues(ctx, labelName, rawQuery, start, end, limit, search)
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
			p.setEndpointReadCacheWithTTL("label_values", cacheKey, lokiLabelsResponse(values), CacheTTLs["label_values"])
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
	canonicalKey := canonicalPatternSnapshotCacheKey(cacheKey)
	if canonicalKey != "" {
		cacheKey = canonicalKey
	}
	payload = normalizePatternSnapshotPayload(payload)
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

func recordPatternResponseMetrics(m *metrics.Metrics, payload []byte) {
	if m == nil {
		return
	}
	m.SetPatternsLastResponse(patternCountFromPayload(payload), int64(len(payload)))
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

func (p *Proxy) cacheSnapshotBlobLocally(key string, data []byte, ttl time.Duration) {
	if p.cache == nil || strings.TrimSpace(key) == "" || len(data) == 0 || ttl <= 0 {
		return
	}
	p.cache.SetLocalOnlyWithTTL(key, data, ttl)
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
		canonicalKey := canonicalPatternSnapshotCacheKey(key)
		if strings.TrimSpace(canonicalKey) == "" {
			canonicalKey = key
		}
		incoming.Value = normalizePatternSnapshotPayload(incoming.Value)
		if incoming.UpdatedAtUnixNano <= 0 {
			incoming.UpdatedAtUnixNano = nowUnix
		}
		if existing, ok := p.patternsSnapshotEntries[canonicalKey]; ok && existing.UpdatedAtUnixNano >= incoming.UpdatedAtUnixNano {
			continue
		}
		incomingPatternCount := incoming.PatternCount
		if incomingPatternCount <= 0 {
			incomingPatternCount = patternCountFromPayload(incoming.Value)
		}
		copied := append([]byte(nil), incoming.Value...)
		if existing, ok := p.patternsSnapshotEntries[canonicalKey]; ok {
			p.patternsSnapshotPatternCount -= int64(existing.PatternCount)
			p.patternsSnapshotPayloadBytes -= int64(len(existing.Value))
		}
		p.patternsSnapshotEntries[canonicalKey] = patternSnapshotEntry{
			Value:             copied,
			UpdatedAtUnixNano: incoming.UpdatedAtUnixNano,
			PatternCount:      incomingPatternCount,
		}
		p.patternsSnapshotPatternCount += int64(incomingPatternCount)
		p.patternsSnapshotPayloadBytes += int64(len(copied))
		p.cache.SetWithTTL(canonicalKey, copied, patternsCacheRetention)
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

func canonicalPatternSnapshotCacheKey(cacheKey string) string {
	cacheKey = strings.TrimSpace(cacheKey)
	if cacheKey == "" {
		return ""
	}
	parts := strings.SplitN(cacheKey, ":", 3)
	if len(parts) != 3 || parts[0] != "patterns" {
		return cacheKey
	}
	orgID := strings.TrimSpace(parts[1])
	params, err := url.ParseQuery(parts[2])
	if err != nil {
		return cacheKey
	}
	query := patternScopeQuery(params.Get("query"))
	if strings.TrimSpace(query) == "" {
		return cacheKey
	}
	canonical := url.Values{}
	canonical.Set("query", query)
	return "patterns:" + orgID + ":" + canonical.Encode()
}

func normalizePatternSnapshotPayload(payload []byte) []byte {
	if len(payload) == 0 {
		return payload
	}
	var resp patternsResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		return payload
	}
	changed := false
	for i := range resp.Data {
		entry := resp.Data[i]
		if len(entry.Samples) == 0 {
			continue
		}
		compacted := compactPatternSnapshotSamples(entry.Samples)
		if len(compacted) != len(entry.Samples) {
			changed = true
		}
		entry.Samples = compacted
		resp.Data[i] = entry
	}
	if !changed {
		return payload
	}
	encoded, err := json.Marshal(resp)
	if err != nil {
		return payload
	}
	return encoded
}

func compactPatternSnapshotSamples(samples [][]interface{}) [][]interface{} {
	if len(samples) == 0 {
		return samples
	}
	byTimestamp := make(map[int64]int, len(samples))
	order := make([]int64, 0, len(samples))
	for _, pair := range samples {
		if len(pair) < 2 {
			continue
		}
		ts, okTS := numberToInt64(pair[0])
		count, okCount := numberToInt(pair[1])
		if !okTS || !okCount || count <= 0 {
			continue
		}
		if _, seen := byTimestamp[ts]; !seen {
			order = append(order, ts)
		}
		if count > byTimestamp[ts] {
			byTimestamp[ts] = count
		}
	}
	if len(byTimestamp) == 0 {
		return [][]interface{}{}
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })
	compacted := make([][]interface{}, 0, len(order))
	for _, ts := range order {
		compacted = append(compacted, []interface{}{ts, byTimestamp[ts]})
	}
	return compacted
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

func (p *Proxy) latestPatternSnapshotPayload(cacheKey string) ([]byte, bool) {
	if p == nil || !p.patternsEnabled || strings.TrimSpace(p.patternsPersistPath) == "" {
		return nil, false
	}
	identity := patternSnapshotIdentityFromCacheKey(cacheKey)
	if identity == "" {
		return nil, false
	}

	p.patternsSnapshotMu.RLock()
	defer p.patternsSnapshotMu.RUnlock()

	var (
		bestKey   string
		bestEntry patternSnapshotEntry
	)
	for key, entry := range p.patternsSnapshotEntries {
		if patternSnapshotIdentityFromCacheKey(key) != identity {
			continue
		}
		if entry.PatternCount <= 0 || len(entry.Value) == 0 {
			continue
		}
		if bestKey == "" || betterPatternSnapshotEntry(bestEntry, entry, bestKey, key) {
			bestKey = key
			bestEntry = entry
		}
	}
	if bestKey == "" {
		return nil, false
	}
	return append([]byte(nil), bestEntry.Value...), true
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

func writeSnapshotFileIfChanged(path string, data []byte, compareDigest [sha256.Size]byte, digest *[sha256.Size]byte, ready *bool) (bool, error) {
	if *ready && *digest == compareDigest {
		return false, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return false, err
	}
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return false, err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return false, err
	}
	*digest = compareDigest
	*ready = true
	return true, nil
}

func mustMarshalSnapshot(v interface{}) []byte {
	out, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return out
}

func (p *Proxy) persistPatternsNow(reason string) error {
	if !p.patternsEnabled || strings.TrimSpace(p.patternsPersistPath) == "" {
		return nil
	}
	if reason == "periodic" && !p.patternsPersistDirty.Load() {
		return nil
	}
	startedAt := time.Now()

	snapshot := p.buildPatternsSnapshot(time.Now().UTC())
	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal patterns snapshot: %w", err)
	}
	snapshotForDigest := snapshot
	snapshotForDigest.SavedAtUnixNano = 0
	snapshotDigest := sha256.Sum256(mustMarshalSnapshot(snapshotForDigest))

	path := p.patternsPersistPath
	wrote, err := writeSnapshotFileIfChanged(path, data, snapshotDigest, &p.patternsPersistDigest, &p.patternsPersistDigestReady)
	if err != nil {
		return fmt.Errorf("persist patterns snapshot: %w", err)
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
		p.cacheSnapshotBlobLocally(patternsSnapshotCacheKey, data, ttl)
	}

	entryCount := len(snapshot.EntriesByKey)
	patternCount := patternCountFromSnapshot(snapshot)
	p.log.Info(
		"patterns snapshot persisted",
		"reason", reason,
		"path", path,
		"wrote_disk", wrote,
		"entries", entryCount,
		"patterns", patternCount,
		"bytes", len(data),
		"duration_ms", time.Since(startedAt).Milliseconds(),
	)
	p.metrics.SetPatternsPersistedDiskState(patternCount, entryCount, int64(len(data)))
	if wrote {
		p.metrics.RecordPatternsPersistWrite(int64(len(data)))
	}
	p.patternsPersistDirty.Store(false)
	return nil
}

func (p *Proxy) restorePatternsFromDisk() (bool, int64, error) {
	if !p.patternsEnabled || strings.TrimSpace(p.patternsPersistPath) == "" {
		return false, 0, nil
	}
	startedAt := time.Now()
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
	snapshotForDigest := snapshot
	snapshotForDigest.SavedAtUnixNano = 0
	p.patternsPersistDigest = sha256.Sum256(mustMarshalSnapshot(snapshotForDigest))
	p.patternsPersistDigestReady = true

	snapshotEntryCount := len(snapshot.EntriesByKey)
	snapshotPatternCount := patternCountFromSnapshot(snapshot)
	appliedEntries, appliedPatterns := p.applyPatternsSnapshot(snapshot, patternDedupSourceDisk)
	p.metrics.RecordPatternsRestoredFromDisk(appliedPatterns, appliedEntries)
	p.metrics.RecordPatternsRestoreBytes("disk", int64(len(data)))
	p.metrics.SetPatternsPersistedDiskState(snapshotPatternCount, snapshotEntryCount, int64(len(data)))
	if p.cache != nil {
		ttl := p.patternsStartupStale * 3
		if ttl <= 0 {
			ttl = 5 * time.Minute
		}
		p.cacheSnapshotBlobLocally(patternsSnapshotCacheKey, data, ttl)
	}
	p.log.Info(
		"patterns snapshot restored from disk",
		"path", p.patternsPersistPath,
		"saved_at", time.Unix(0, snapshot.SavedAtUnixNano).UTC().Format(time.RFC3339Nano),
		"entries_applied", appliedEntries,
		"patterns_applied", appliedPatterns,
		"bytes", len(data),
		"duration_ms", time.Since(startedAt).Milliseconds(),
	)
	return true, snapshot.SavedAtUnixNano, nil
}

func (p *Proxy) fetchPatternsSnapshotFromPeer(peerAddr string, timeout time.Duration) (*patternsSnapshot, int, error) {
	if strings.TrimSpace(peerAddr) == "" {
		return nil, 0, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	endpoint := fmt.Sprintf("http://%s/_cache/get?key=%s", peerAddr, url.QueryEscape(patternsSnapshotCacheKey))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, 0, err
	}
	if p.peerAuthToken != "" {
		req.Header.Set("X-Peer-Token", p.peerAuthToken)
	}
	req.Header.Set("Accept-Encoding", "zstd, gzip")

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, 0, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, 0, fmt.Errorf("peer %s status %d: %s", peerAddr, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	if err := decodeCompressedHTTPResponse(resp); err != nil {
		return nil, 0, err
	}
	body, err := readBodyLimited(resp.Body, maxPatternsPeerSnapshotBytes)
	if err != nil {
		return nil, 0, err
	}
	var snapshot patternsSnapshot
	if err := json.Unmarshal(body, &snapshot); err != nil {
		return nil, 0, err
	}
	if snapshot.Version != 1 || snapshot.SavedAtUnixNano <= 0 {
		return nil, 0, fmt.Errorf("invalid patterns snapshot metadata from peer %s", peerAddr)
	}
	return &snapshot, len(body), nil
}

func (p *Proxy) restorePatternsFromPeers(minSavedAt int64) (bool, int64, error) {
	if !p.patternsEnabled || p.peerCache == nil {
		return false, 0, nil
	}
	startedAt := time.Now()
	peers := p.peerCache.Peers()
	if len(peers) == 0 {
		return false, 0, nil
	}

	timeout := p.patternsPeerWarmTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	type peerResult struct {
		snapshot  *patternsSnapshot
		bodyBytes int
		err       error
		peer      string
	}
	resCh := make(chan peerResult, len(peers))
	for _, peerAddr := range peers {
		peerAddr := strings.TrimSpace(peerAddr)
		if peerAddr == "" {
			continue
		}
		go func(addr string) {
			snapshot, bodyBytes, err := p.fetchPatternsSnapshotFromPeer(addr, timeout)
			resCh <- peerResult{snapshot: snapshot, bodyBytes: bodyBytes, err: err, peer: addr}
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
	totalPeerBytes := int64(0)
	for received < len(peers) {
		select {
		case res := <-resCh:
			received++
			if res.err != nil {
				p.log.Debug("patterns peer warm fetch failed", "peer", res.peer, "error", res.err)
				continue
			}
			if res.bodyBytes > 0 {
				totalPeerBytes += int64(res.bodyBytes)
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
	p.metrics.RecordPatternsRestoreBytes("peer", totalPeerBytes)

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
		"bytes", totalPeerBytes,
		"duration_ms", time.Since(startedAt).Milliseconds(),
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
	startedAt := time.Now()

	snapshot := p.buildLabelValuesIndexSnapshot(time.Now().UTC())
	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal label index snapshot: %w", err)
	}
	snapshotForDigest := snapshot
	snapshotForDigest.SavedAtUnixNano = 0
	snapshotDigest := sha256.Sum256(mustMarshalSnapshot(snapshotForDigest))

	path := p.labelValuesIndexPersistPath
	wrote, err := writeSnapshotFileIfChanged(path, data, snapshotDigest, &p.labelValuesIndexPersistDigest, &p.labelValuesIndexPersistDigestReady)
	if err != nil {
		return fmt.Errorf("persist label index snapshot: %w", err)
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
		p.cacheSnapshotBlobLocally(labelValuesIndexSnapshotCacheKey, data, ttl)
	}

	states, values := p.labelValuesIndexCardinality()
	if reason == "periodic" {
		p.log.Debug("label values index snapshot persisted", "path", path, "wrote_disk", wrote, "states", states, "values", values, "bytes", len(data), "duration_ms", time.Since(startedAt).Milliseconds())
	} else {
		p.log.Info("label values index snapshot persisted", "reason", reason, "path", path, "wrote_disk", wrote, "states", states, "values", values, "bytes", len(data), "duration_ms", time.Since(startedAt).Milliseconds())
	}
	p.labelValuesIndexPersistDirty.Store(false)
	return nil
}

func (p *Proxy) restoreLabelValuesIndexFromDisk() (bool, int64, error) {
	if !p.labelValuesIndexedCache || strings.TrimSpace(p.labelValuesIndexPersistPath) == "" {
		return false, 0, nil
	}
	startedAt := time.Now()

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
	snapshotForDigest := snapshot
	snapshotForDigest.SavedAtUnixNano = 0
	p.labelValuesIndexPersistDigest = sha256.Sum256(mustMarshalSnapshot(snapshotForDigest))
	p.labelValuesIndexPersistDigestReady = true

	states, values := p.applyLabelValuesIndexSnapshot(snapshot)
	if p.cache != nil {
		ttl := p.labelValuesIndexStartupStale * 3
		if ttl <= 0 {
			ttl = 5 * time.Minute
		}
		p.cacheSnapshotBlobLocally(labelValuesIndexSnapshotCacheKey, data, ttl)
	}
	p.log.Info(
		"label values index restored from disk",
		"path", p.labelValuesIndexPersistPath,
		"saved_at", time.Unix(0, snapshot.SavedAtUnixNano).UTC().Format(time.RFC3339Nano),
		"states", states,
		"values", values,
		"bytes", len(data),
		"duration_ms", time.Since(startedAt).Milliseconds(),
	)
	return true, snapshot.SavedAtUnixNano, nil
}

func (p *Proxy) fetchLabelValuesIndexSnapshotFromPeer(peerAddr string, timeout time.Duration) (*labelValuesIndexSnapshot, int, error) {
	if strings.TrimSpace(peerAddr) == "" {
		return nil, 0, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	endpoint := fmt.Sprintf("http://%s/_cache/get?key=%s", peerAddr, url.QueryEscape(labelValuesIndexSnapshotCacheKey))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, 0, err
	}
	if p.peerAuthToken != "" {
		req.Header.Set("X-Peer-Token", p.peerAuthToken)
	}
	req.Header.Set("Accept-Encoding", "zstd, gzip")

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, 0, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, 0, fmt.Errorf("peer %s status %d: %s", peerAddr, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	if err := decodeCompressedHTTPResponse(resp); err != nil {
		return nil, 0, err
	}
	body, err := readBodyLimited(resp.Body, maxLabelValuesPeerSnapshotBytes)
	if err != nil {
		return nil, 0, err
	}
	var snapshot labelValuesIndexSnapshot
	if err := json.Unmarshal(body, &snapshot); err != nil {
		return nil, 0, err
	}
	if snapshot.Version != 1 || snapshot.SavedAtUnixNano <= 0 {
		return nil, 0, fmt.Errorf("invalid label-values snapshot metadata from peer %s", peerAddr)
	}
	return &snapshot, len(body), nil
}

func mergeLabelValuesIndexEntry(existing, incoming labelValueIndexEntry) labelValueIndexEntry {
	if incoming.SeenCount > existing.SeenCount {
		existing.SeenCount = incoming.SeenCount
	}
	if incoming.LastSeen > existing.LastSeen {
		existing.LastSeen = incoming.LastSeen
	}
	return existing
}

func mergeLabelValuesIndexSnapshot(dst *labelValuesIndexSnapshot, src *labelValuesIndexSnapshot) {
	if dst == nil || src == nil {
		return
	}
	if dst.StatesByKey == nil {
		dst.StatesByKey = make(map[string]map[string]labelValueIndexEntry)
	}
	if src.SavedAtUnixNano > dst.SavedAtUnixNano {
		dst.SavedAtUnixNano = src.SavedAtUnixNano
	}
	for key, entries := range src.StatesByKey {
		if len(entries) == 0 {
			continue
		}
		dstEntries, ok := dst.StatesByKey[key]
		if !ok {
			dstEntries = make(map[string]labelValueIndexEntry, len(entries))
			dst.StatesByKey[key] = dstEntries
		}
		for value, entry := range entries {
			if strings.TrimSpace(value) == "" {
				continue
			}
			if existing, exists := dstEntries[value]; exists {
				dstEntries[value] = mergeLabelValuesIndexEntry(existing, entry)
				continue
			}
			dstEntries[value] = entry
		}
	}
}

func (p *Proxy) restoreLabelValuesIndexFromPeers(minSavedAt int64) (bool, int64, error) {
	if !p.labelValuesIndexedCache || p.peerCache == nil {
		return false, 0, nil
	}
	startedAt := time.Now()
	peers := p.peerCache.Peers()
	if len(peers) == 0 {
		return false, 0, nil
	}

	timeout := p.labelValuesIndexPeerWarmTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	type peerResult struct {
		snapshot  *labelValuesIndexSnapshot
		bodyBytes int
		err       error
		peer      string
	}
	resCh := make(chan peerResult, len(peers))
	for _, peerAddr := range peers {
		peerAddr := strings.TrimSpace(peerAddr)
		if peerAddr == "" {
			continue
		}
		go func(addr string) {
			snapshot, bodyBytes, err := p.fetchLabelValuesIndexSnapshotFromPeer(addr, timeout)
			resCh <- peerResult{snapshot: snapshot, bodyBytes: bodyBytes, err: err, peer: addr}
		}(peerAddr)
	}

	merged := p.buildLabelValuesIndexSnapshot(time.Now().UTC())
	if merged.Version == 0 {
		merged.Version = 1
	}
	if merged.StatesByKey == nil {
		merged.StatesByKey = make(map[string]map[string]labelValueIndexEntry)
	}
	mergedSavedAt := minSavedAt
	sawNewer := false
	totalPeerBytes := 0
	deadline := time.After(timeout)
	received := 0
	for received < len(peers) {
		select {
		case res := <-resCh:
			received++
			if res.err != nil {
				p.log.Debug("label values peer warm fetch failed", "peer", res.peer, "error", res.err)
				continue
			}
			totalPeerBytes += res.bodyBytes
			if res.snapshot == nil || len(res.snapshot.StatesByKey) == 0 || res.snapshot.SavedAtUnixNano <= minSavedAt {
				continue
			}
			sawNewer = true
			if res.snapshot.SavedAtUnixNano > mergedSavedAt {
				mergedSavedAt = res.snapshot.SavedAtUnixNano
			}
			mergeLabelValuesIndexSnapshot(&merged, res.snapshot)
		case <-deadline:
			received = len(peers)
		}
	}
	if !sawNewer {
		return false, mergedSavedAt, nil
	}

	states, values := p.applyLabelValuesIndexSnapshot(merged)
	p.log.Info(
		"label values index warmed from peers",
		"saved_at", time.Unix(0, mergedSavedAt).UTC().Format(time.RFC3339Nano),
		"states", states,
		"values", values,
		"peers", len(peers),
		"bytes", totalPeerBytes,
		"duration_ms", time.Since(startedAt).Milliseconds(),
	)
	return true, mergedSavedAt, nil
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

func (p *Proxy) endpointUsesSharedReadCache(endpoint string) bool {
	switch endpoint {
	case "labels", "label_values", "index_stats", "volume", "volume_range", "detected_fields", "detected_field_values", "detected_labels":
		return true
	default:
		return false
	}
}

func endpointForReadCacheKey(cacheKey string) string {
	switch {
	case strings.HasPrefix(cacheKey, "labels:"):
		return "labels"
	case strings.HasPrefix(cacheKey, "label_values:"):
		return "label_values"
	case strings.HasPrefix(cacheKey, "index_stats:"):
		return "index_stats"
	case strings.HasPrefix(cacheKey, "volume_range:"):
		return "volume_range"
	case strings.HasPrefix(cacheKey, "volume:"):
		return "volume"
	case strings.HasPrefix(cacheKey, "detected_fields:"):
		return "detected_fields"
	case strings.HasPrefix(cacheKey, "detected_field_values:"):
		return "detected_field_values"
	case strings.HasPrefix(cacheKey, "detected_labels:"):
		return "detected_labels"
	default:
		return ""
	}
}

func (p *Proxy) canonicalReadCacheKey(endpoint, orgID string, r *http.Request, extraParts ...string) string {
	// Include the per-user auth fingerprint so requests with different forwarded
	// credentials land in different cache namespaces.
	if r != nil {
		if fp := p.forwardedAuthFingerprint(r); fp != "" {
			extraParts = append(extraParts, "auth:"+fp)
		}
	}
	if memoKey, ok := buildCanonicalReadCacheMemoKey(endpoint, orgID, r, extraParts); ok && p != nil {
		p.readCacheKeyMemoMu.RLock()
		if cached, hit := p.readCacheKeyMemo[memoKey]; hit {
			p.readCacheKeyMemoMu.RUnlock()
			return cached
		}
		p.readCacheKeyMemoMu.RUnlock()

		computed := computeCanonicalReadCacheKey(endpoint, orgID, r, extraParts...)
		p.readCacheKeyMemoMu.Lock()
		if p.readCacheKeyMemo == nil || len(p.readCacheKeyMemo) >= maxReadCacheKeyMemoEntries {
			p.readCacheKeyMemo = make(map[canonicalReadCacheMemoKey]string, 2048)
		}
		p.readCacheKeyMemo[memoKey] = computed
		p.readCacheKeyMemoMu.Unlock()
		return computed
	}
	return computeCanonicalReadCacheKey(endpoint, orgID, r, extraParts...)
}

func buildCanonicalReadCacheMemoKey(endpoint, orgID string, r *http.Request, extraParts []string) (canonicalReadCacheMemoKey, bool) {
	if r == nil || len(extraParts) > 1 {
		return canonicalReadCacheMemoKey{}, false
	}
	key := canonicalReadCacheMemoKey{
		endpoint: endpoint,
		orgID:    orgID,
		rawQuery: r.URL.RawQuery,
	}
	if len(extraParts) == 1 {
		key.extra = strings.TrimSpace(extraParts[0])
	}
	return key, true
}

func computeCanonicalReadCacheKey(endpoint, orgID string, r *http.Request, extraParts ...string) string {
	params := r.URL.Query()
	normalizeReadCacheParams(endpoint, params)
	switch endpoint {
	case "detected_fields", "detected_field_values", "detected_labels":
		params.Set("limit", strconv.Itoa(parseDetectedLineLimit(r)))
	}
	if endpoint == "volume" || endpoint == "volume_range" {
		query := strings.TrimSpace(params.Get("query"))
		if query == "" {
			query = "*"
		}
		if strings.TrimSpace(params.Get("targetLabels")) == "" {
			if inferred := inferPrimaryTargetLabel(query); inferred != "" {
				params.Set("targetLabels", inferred)
			}
		}
		if endpoint == "volume_range" {
			if step := strings.TrimSpace(params.Get("step")); step != "" {
				params.Set("step", formatVLStep(step))
			}
		}
	}

	parts := make([]string, 0, 3+len(extraParts))
	parts = append(parts, endpoint, orgID)
	for _, part := range extraParts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		parts = append(parts, part)
	}
	parts = append(parts, params.Encode())
	return strings.Join(parts, ":")
}

func normalizeReadCacheParams(endpoint string, params url.Values) {
	if params == nil {
		return
	}
	if start := strings.TrimSpace(firstNonEmpty(params.Get("start"), params.Get("from"))); start != "" {
		params.Set("start", start)
	}
	params.Del("from")
	if end := strings.TrimSpace(firstNonEmpty(params.Get("end"), params.Get("to"))); end != "" {
		params.Set("end", end)
	}
	params.Del("to")

	if search := strings.TrimSpace(firstNonEmpty(params.Get("search"), params.Get("q"))); search != "" {
		params.Set("search", search)
	}
	params.Del("q")
	params.Del("line_limit")

	switch endpoint {
	case "labels", "label_values", "index_stats", "volume", "volume_range", "detected_fields", "detected_field_values", "detected_labels":
		if query := strings.TrimSpace(params.Get("query")); query == "" {
			params.Set("query", "*")
		} else {
			params.Set("query", query)
		}
	}
}

func (p *Proxy) setJSONCacheWithTTL(cacheKey string, ttl time.Duration, value interface{}) {
	p.setEndpointJSONCacheWithTTL(endpointForReadCacheKey(cacheKey), cacheKey, ttl, value)
}

func (p *Proxy) setEndpointJSONCacheWithTTL(endpoint, cacheKey string, ttl time.Duration, value interface{}) {
	if p == nil || p.cache == nil {
		return
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return
	}
	p.setEndpointReadCacheWithTTL(endpoint, cacheKey, encoded, ttl)
}

// setLocalReadCacheWithTTL stores response bodies for handlers that only read
// cache entries via GetWithTTL, which is intentionally L1-only. Writing those
// entries to disk or peer write-through adds churn without creating a fallback
// path the handlers actually use.
func (p *Proxy) setLocalReadCacheWithTTL(cacheKey string, value []byte, ttl time.Duration) {
	if p == nil || p.cache == nil {
		return
	}
	p.cache.SetLocalOnlyWithTTL(cacheKey, value, ttl)
}

func (p *Proxy) setEndpointReadCacheWithTTL(endpoint, cacheKey string, value []byte, ttl time.Duration) {
	if p == nil || p.cache == nil {
		return
	}
	if p.endpointUsesSharedReadCache(endpoint) {
		p.cache.SetLocalAndDiskWithTTL(cacheKey, value, ttl)
		return
	}
	p.cache.SetLocalOnlyWithTTL(cacheKey, value, ttl)
}

func (p *Proxy) endpointReadCacheEntry(endpoint, cacheKey string) ([]byte, time.Duration, string, bool) {
	if p == nil || p.cache == nil || strings.TrimSpace(cacheKey) == "" {
		return nil, 0, "", false
	}
	if p.endpointUsesSharedReadCache(endpoint) {
		return p.cache.GetSharedWithTTL(cacheKey)
	}
	body, ttl, ok := p.cache.GetWithTTL(cacheKey)
	if !ok {
		return nil, 0, "", false
	}
	return body, ttl, "l1_memory", true
}

func (p *Proxy) staleEndpointCacheEntry(endpoint, cacheKey string) ([]byte, time.Duration, string, bool) {
	if p == nil || p.cache == nil || strings.TrimSpace(cacheKey) == "" {
		return nil, 0, "", false
	}
	if p.endpointUsesSharedReadCache(endpoint) {
		return p.cache.GetRecoverableStaleWithTTL(cacheKey)
	}
	body, ttl, ok := p.cache.GetStaleWithTTL(cacheKey)
	if !ok {
		return nil, 0, "", false
	}
	return body, ttl, "l1_memory", true
}

func (p *Proxy) serveStaleReadCacheOnError(w http.ResponseWriter, endpoint, cacheKey string, started time.Time, err error) bool {
	body, remaining, tier, ok := p.staleEndpointCacheEntry(endpoint, cacheKey)
	if !ok || len(body) == 0 {
		return false
	}
	if strings.TrimSpace(w.Header().Get("Content-Type")) == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	_, _ = w.Write(body)
	if p.metrics != nil {
		p.metrics.RecordRequest(endpoint, http.StatusOK, time.Since(started))
	}
	staleFor := time.Duration(0)
	if remaining < 0 {
		staleFor = -remaining
	}
	p.log.Warn(
		"serving stale cached response after backend failure",
		"endpoint", endpoint,
		"cache_key", cacheKey,
		"cache_tier", tier,
		"stale_for", staleFor.String(),
		"error", err,
	)
	return true
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
			p.setEndpointJSONCacheWithTTL("detected_fields", cacheKey, CacheTTLs["detected_fields"], payload)
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
			p.setEndpointJSONCacheWithTTL("detected_labels", cacheKey, CacheTTLs["detected_labels"], payload)
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
			values, err := p.resolveDetectedFieldValues(ctx, fieldName, query, start, end, lineLimit, true)
			if err != nil {
				return nil, err
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
			p.setEndpointJSONCacheWithTTL("detected_field_values", cacheKey, CacheTTLs["detected_field_values"], payload)
			return nil, nil
		})
		if err != nil {
			p.log.Debug("background detected_field_values refresh failed", "cache_key", cacheKey, "field", fieldName, "error", err)
		}
	}()
}

func (p *Proxy) resolveDetectedFieldValues(ctx context.Context, fieldName, query, start, end string, lineLimit int, relaxOnEmpty bool) ([]string, error) {
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
	if relaxOnEmpty && len(values) == 0 {
		if relaxed := relaxedFieldDetectionQuery(query); relaxed != "" && relaxed != query {
			p.observeInternalOperation(ctx, "discovery_fallback", "detected_field_values_relaxed_after_empty", 0)
			return p.resolveDetectedFieldValues(ctx, fieldName, relaxed, start, end, lineLimit, false)
		}
	}
	// Last resort: field is inside JSON or logfmt _msg (not VL-indexed) and was
	// not found by scan or relaxed-query. Only reached when no relaxed query is
	// available (relaxOnEmpty=false or query already relaxed).
	if len(values) == 0 {
		values, _ = p.fetchUnpackedFieldValues(ctx, query, start, end, fieldName, lineLimit)
	}
	if values == nil {
		values = []string{}
	}
	return values, nil
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
	cacheKey := p.canonicalReadCacheKey("labels", orgID, r)

	if cached, remaining, _, ok := p.endpointReadCacheEntry("labels", cacheKey); ok {
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

	search := strings.TrimSpace(r.FormValue("search"))
	if search == "" {
		search = strings.TrimSpace(r.FormValue("q"))
	}

	labels, err := p.fetchScopedLabelNames(r.Context(), r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), search, true)
	if err != nil {
		status := statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
		p.metrics.RecordRequest("labels", status, time.Since(start))
		return
	}

	filtered := make([]string, 0, len(labels))
	for _, v := range labels {
		// Filter VL internal fields only (before translation)
		if isVLInternalField(v) || v == "detected_level" {
			continue
		}
		filtered = append(filtered, v)
	}

	// Apply label name translation (e.g., dots → underscores)
	labels = p.labelTranslator.TranslateLabelsList(filtered)
	labels = appendSyntheticLabels(labels)

	result := lokiLabelsResponse(labels)
	p.setEndpointReadCacheWithTTL("labels", cacheKey, result, CacheTTLs["labels"])
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
	cacheKey := p.canonicalReadCacheKey("label_values", orgID, r, labelName)
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

	if cached, remaining, _, ok := p.endpointReadCacheEntry("label_values", cacheKey); ok {
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
			p.setEndpointReadCacheWithTTL("label_values", cacheKey, result, CacheTTLs["label_values"])
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
		p.setEndpointReadCacheWithTTL("label_values", cacheKey, result, CacheTTLs["label_values"])
		w.Header().Set("Content-Type", "application/json")
		w.Write(result)
		p.metrics.RecordRequest("label_values", http.StatusOK, time.Since(start))
		return
	}

	values, err := p.fetchScopedLabelValues(r.Context(), labelName, rawQuery, r.FormValue("start"), r.FormValue("end"), r.FormValue("limit"), search)
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
	p.setEndpointReadCacheWithTTL("label_values", cacheKey, result, CacheTTLs["label_values"])
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
		translated, err := p.translateQueryWithContext(r.Context(), matchQueries[0])
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

	body, _ := readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)

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
		stream := parseStreamLabels(v.Value)
		if len(stream) == 0 {
			continue
		}
		// Copy before translation/mutation — parseStreamLabels returns a cached map.
		labels := make(map[string]string, len(stream))
		for k, val := range stream {
			labels[k] = val
		}
		labels = p.labelTranslator.TranslateLabelsMap(labels)
		ensureSyntheticServiceName(labels)
		series = append(series, labels)
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
	orgID := r.Header.Get("X-Scope-OrgID")
	cacheKey := p.canonicalReadCacheKey("index_stats", orgID, r)
	if cached, _, _, ok := p.endpointReadCacheEntry("index_stats", cacheKey); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(cached)
		p.metrics.RecordRequest("index_stats", http.StatusOK, time.Since(start))
		p.metrics.RecordCacheHit()
		return
	}
	p.metrics.RecordCacheMiss()

	r = withOrgID(r)
	result, err := p.computeIndexStatsResult(r.Context(), r.FormValue("query"), r.FormValue("start"), r.FormValue("end"))
	if err != nil {
		if p.serveStaleReadCacheOnError(w, "index_stats", cacheKey, start, err) {
			return
		}
		status := statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
		p.metrics.RecordRequest("index_stats", status, time.Since(start))
		return
	}
	p.setEndpointReadCacheWithTTL("index_stats", cacheKey, result, CacheTTLs["index_stats"])
	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
	p.metrics.RecordRequest("index_stats", http.StatusOK, time.Since(start))
}

func (p *Proxy) computeIndexStatsResult(ctx context.Context, query, start, end string) ([]byte, error) {
	if query == "" {
		query = "*"
	}
	logsqlQuery, _ := p.translateQueryWithContext(ctx, query)

	params := url.Values{}
	params.Set("query", logsqlQuery)
	if s := start; s != "" {
		params.Set("start", formatVLTimestamp(s))
	}
	if e := end; e != "" {
		params.Set("end", formatVLTimestamp(e))
	}
	if params.Get("step") == "" {
		params.Set("step", "1h")
	}

	resp, err := p.vlGet(ctx, "/select/logsql/hits", params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
	if resp.StatusCode >= http.StatusBadRequest {
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = fmt.Sprintf("VL backend returned %d", resp.StatusCode)
		}
		return nil, fmt.Errorf("%s", msg)
	}

	entries := sumHitsValues(body)
	hits := parseHits(body)
	streams := len(hits.Hits)
	if streams == 0 && entries > 0 {
		streams = 1
	}
	result, _ := json.Marshal(map[string]interface{}{
		"streams": streams,
		"chunks":  streams,
		"bytes":   entries * 100,
		"entries": entries,
	})
	return result, nil
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

func normalizeDrilldownGroupingLabel(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	switch strings.ToLower(raw) {
	case "$__all", "__all":
		return ""
	default:
		return raw
	}
}

func requestedVolumeTargetLabels(r *http.Request) string {
	if r == nil {
		return ""
	}
	if direct := strings.TrimSpace(r.FormValue("targetLabels")); direct != "" {
		return direct
	}
	for _, key := range []string{"drillDownLabel", "fieldBy", "labelBy", "var-fieldBy", "var-labelBy"} {
		if candidate := normalizeDrilldownGroupingLabel(r.FormValue(key)); candidate != "" {
			return candidate
		}
	}
	return ""
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
	query := r.FormValue("query")
	startParam := strings.TrimSpace(firstNonEmpty(r.FormValue("start"), r.FormValue("from")))
	endParam := strings.TrimSpace(firstNonEmpty(r.FormValue("end"), r.FormValue("to")))
	targetLabels := requestedVolumeTargetLabels(r)
	cacheKey := p.canonicalReadCacheKey("volume", orgID, r)
	if cached, remaining, _, ok := p.endpointReadCacheEntry("volume", cacheKey); ok {
		if !p.shouldBypassRecentTailCache("volume", remaining, r) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(cached)
			p.metrics.RecordRequest("volume", http.StatusOK, time.Since(start))
			p.metrics.RecordCacheHit()
			if p.shouldRefreshLabelsInBackground(remaining, CacheTTLs["volume"]) {
				p.refreshVolumeCacheAsync(orgID, cacheKey, query, startParam, endParam, targetLabels)
			}
			return
		}
	}
	p.metrics.RecordCacheMiss()

	result, err := p.computeVolumeResult(r.Context(), query, startParam, endParam, targetLabels)
	if err != nil {
		if p.serveStaleReadCacheOnError(w, "volume", cacheKey, start, err) {
			return
		}
		status := statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
		p.metrics.RecordRequest("volume", status, time.Since(start))
		return
	}
	p.setEndpointJSONCacheWithTTL("volume", cacheKey, CacheTTLs["volume"], result)
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
	logsqlQuery, _ := p.translateQueryWithContext(ctx, query)

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
	body, _ := readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
	if resp.StatusCode >= http.StatusBadRequest {
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = fmt.Sprintf("VL backend returned %d", resp.StatusCode)
		}
		return nil, fmt.Errorf("%s", msg)
	}

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
				p.setEndpointJSONCacheWithTTL("volume", cacheKey, CacheTTLs["volume"], result)
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
	query := r.FormValue("query")
	startParam := strings.TrimSpace(firstNonEmpty(r.FormValue("start"), r.FormValue("from")))
	endParam := strings.TrimSpace(firstNonEmpty(r.FormValue("end"), r.FormValue("to")))
	stepParam := r.FormValue("step")
	targetLabels := requestedVolumeTargetLabels(r)
	cacheKey := p.canonicalReadCacheKey("volume_range", orgID, r)
	if cached, remaining, _, ok := p.endpointReadCacheEntry("volume_range", cacheKey); ok {
		if !p.shouldBypassRecentTailCache("volume_range", remaining, r) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(cached)
			p.metrics.RecordRequest("volume_range", http.StatusOK, time.Since(start))
			p.metrics.RecordCacheHit()
			if p.shouldRefreshLabelsInBackground(remaining, CacheTTLs["volume_range"]) {
				p.refreshVolumeRangeCacheAsync(orgID, cacheKey, query, startParam, endParam, stepParam, targetLabels)
			}
			return
		}
	}
	p.metrics.RecordCacheMiss()

	result, err := p.computeVolumeRangeResult(r.Context(), query, startParam, endParam, stepParam, targetLabels)
	if err != nil {
		if p.serveStaleReadCacheOnError(w, "volume_range", cacheKey, start, err) {
			return
		}
		status := statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
		p.metrics.RecordRequest("volume_range", status, time.Since(start))
		return
	}
	p.setEndpointJSONCacheWithTTL("volume_range", cacheKey, CacheTTLs["volume_range"], result)
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
	logsqlQuery, _ := p.translateQueryWithContext(ctx, query)

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
	body, _ := readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
	if resp.StatusCode >= http.StatusBadRequest {
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = fmt.Sprintf("VL backend returned %d", resp.StatusCode)
		}
		return nil, fmt.Errorf("%s", msg)
	}

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
				p.setEndpointJSONCacheWithTTL("volume_range", cacheKey, CacheTTLs["volume_range"], result)
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
	cacheKey := p.canonicalReadCacheKey("detected_fields", orgID, r)
	if cached, remaining, _, ok := p.endpointReadCacheEntry("detected_fields", cacheKey); ok {
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
		if p.serveStaleReadCacheOnError(w, "detected_fields", cacheKey, start, err) {
			return
		}
		status := statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
		p.metrics.RecordRequest("detected_fields", status, time.Since(start))
		return
	}
	payload := map[string]interface{}{
		"status": "success",
		"data":   fields,
		"fields": fields,
		"limit":  lineLimit,
	}
	p.setEndpointJSONCacheWithTTL("detected_fields", cacheKey, CacheTTLs["detected_fields"], payload)
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
	cacheKey := p.canonicalReadCacheKey("detected_field_values", orgID, r, fieldName)
	if cached, remaining, _, ok := p.endpointReadCacheEntry("detected_field_values", cacheKey); ok {
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
			p.setEndpointJSONCacheWithTTL("detected_field_values", cacheKey, CacheTTLs["detected_field_values"], payload)
			p.writeJSON(w, payload)
			p.metrics.RecordRequest("detected_field_values", http.StatusOK, time.Since(start))
			return
		}
	}

	values, err := p.resolveDetectedFieldValues(r.Context(), fieldName, r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), lineLimit, true)
	if err != nil {
		if p.serveStaleReadCacheOnError(w, "detected_field_values", cacheKey, start, err) {
			return
		}
		status := statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
		p.metrics.RecordRequest("detected_field_values", status, time.Since(start))
		return
	}

	payload := map[string]interface{}{
		"status": "success",
		"data":   values,
		"values": values,
		"limit":  lineLimit,
	}
	p.setEndpointJSONCacheWithTTL("detected_field_values", cacheKey, CacheTTLs["detected_field_values"], payload)
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

type patternFetchDiagnostics struct {
	sourceLinesRequested int
	sourceLinesScanned   int
	sourceLinesObserved  int
	windowAttempts       int
	windowAccepted       int
	windowCapped         int
	secondPassWindows    int
	minedPreMerge        int
	minedPostMerge       int
	lowCoverage          bool
}

func (d *patternFetchDiagnostics) recordExtraction(limit int, stats patternExtractionStats, windowed bool) {
	if limit > 0 {
		d.sourceLinesRequested += limit
	}
	d.sourceLinesScanned += stats.scannedLines
	d.sourceLinesObserved += stats.observedLines
	d.minedPreMerge += stats.patternCount
	if windowed && stats.hitLimit(limit) {
		d.windowCapped++
	}
}

func (d *patternFetchDiagnostics) markLowCoverage() {
	d.lowCoverage = true
}

func (d patternFetchDiagnostics) likelyLowCoverage() bool {
	if d.lowCoverage {
		return true
	}
	if d.windowAttempts > 0 {
		if d.windowAccepted == 0 {
			return true
		}
		if d.windowAccepted*2 < d.windowAttempts {
			return true
		}
		if d.windowCapped > 0 && d.minedPostMerge <= max(1, d.windowAccepted/2) {
			return true
		}
	}
	return false
}

func patternSecondPassLimit(baseLimit, sourceLimit int) int {
	if baseLimit <= 0 {
		return 0
	}
	boosted := max(baseLimit*4, baseLimit+500)
	if sourceLimit > 0 && sourceLimit > baseLimit && boosted > sourceLimit {
		boosted = sourceLimit
	}
	if boosted > maxPatternSecondPassLineLimit {
		boosted = maxPatternSecondPassLineLimit
	}
	if boosted <= baseLimit {
		return 0
	}
	return boosted
}

func (p *Proxy) recordPatternFetchDiagnostics(diag patternFetchDiagnostics) {
	if p == nil || p.metrics == nil {
		return
	}
	p.metrics.RecordPatternsQuality(
		diag.sourceLinesRequested,
		diag.sourceLinesScanned,
		diag.sourceLinesObserved,
		diag.windowAttempts,
		diag.windowAccepted,
		diag.windowCapped,
		diag.secondPassWindows,
		diag.minedPreMerge,
		diag.minedPostMerge,
		diag.likelyLowCoverage(),
	)
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
		cacheWriteKey = "patterns:" + orgID + ":" + r.URL.Query().Encode()
	}

	for _, key := range cacheLookupKeys {
		if key == "" {
			continue
		}
		if cached, ok := p.cache.Get(key); ok {
			body := p.applyCustomPatternsToPayload(cached, startParam, endParam, stepParam, patternLimit)
			recordPatternResponseMetrics(p.metrics, body)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(body)
			p.metrics.RecordRequest("patterns", http.StatusOK, time.Since(start))
			p.metrics.RecordCacheHit()
			return
		}
	}

	if fallbackCached, ok := p.cache.Get(cacheWriteKey); ok {
		body := p.applyCustomPatternsToPayload(fallbackCached, startParam, endParam, stepParam, patternLimit)
		recordPatternResponseMetrics(p.metrics, body)
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
			cacheWriteKey = "patterns:" + orgID + ":" + r.URL.Query().Encode()
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
	params.Set("limit", strconv.Itoa(sourceLimit))
	diag := patternFetchDiagnostics{}

	fetchPatterns := func(endpoint string) ([]patternResultEntry, bool) {
		fetchFromParams := func(queryParams url.Values) ([]patternResultEntry, bool) {
			requestedLimit, _ := strconv.Atoi(strings.TrimSpace(queryParams.Get("limit")))
			resp, err := p.vlPost(r.Context(), endpoint, queryParams)
			if err != nil {
				return nil, false
			}
			defer resp.Body.Close()
			if resp.StatusCode >= http.StatusBadRequest {
				return nil, false
			}
			body, err := readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
			if err != nil {
				return nil, false
			}
			extracted, stats := extractLogPatternsWithStats(body, stepParam, patternLimit)
			diag.recordExtraction(requestedLimit, stats, false)
			if len(extracted) == 0 {
				if stats.hitLimit(requestedLimit) {
					diag.markLowCoverage()
				}
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
			startNs, endNs, splitInterval, perWindowLimit, secondPassCap, ok := patternWindowedSamplingConfig(startParam, endParam, stepParam, sourceLimit, denseWindowing)
			if ok {
				windows := splitQueryRangeWindowsWithOptions(startNs, endNs, splitInterval, "backward", true)
				if len(windows) > 1 {
					windowEntries, windowSuccesses, windowDiag := p.fetchPatternsFromWindows(r, logsqlQuery, sourceLimit, perWindowLimit, secondPassCap, windows, stepParam, patternLimit)
					diag.sourceLinesRequested += windowDiag.sourceLinesRequested
					diag.sourceLinesScanned += windowDiag.sourceLinesScanned
					diag.sourceLinesObserved += windowDiag.sourceLinesObserved
					diag.windowAttempts += windowDiag.windowAttempts
					diag.windowAccepted += windowDiag.windowAccepted
					diag.windowCapped += windowDiag.windowCapped
					diag.secondPassWindows += windowDiag.secondPassWindows
					diag.minedPreMerge += windowDiag.minedPreMerge
					if windowDiag.lowCoverage {
						diag.markLowCoverage()
					}
					if len(windowEntries) > diag.minedPostMerge {
						diag.minedPostMerge = len(windowEntries)
					}
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
	if len(entries) == 0 {
		if fallbackPayload, ok := p.latestPatternSnapshotPayload(cacheWriteKey); ok {
			p.metrics.RecordPatternsSnapshotHit(true)
			p.recordPatternFetchDiagnostics(diag)
			resultBody := p.applyCustomPatternsToPayload(fallbackPayload, startParam, endParam, stepParam, patternLimit)
			recordPatternResponseMetrics(p.metrics, resultBody)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(resultBody)
			p.metrics.RecordRequest("patterns", http.StatusOK, time.Since(start))
			return
		}
		p.metrics.RecordPatternsSnapshotMiss()
		diag.markLowCoverage()
	}
	if entries == nil {
		entries = []patternResultEntry{}
	}
	if len(entries) > diag.minedPostMerge {
		diag.minedPostMerge = len(entries)
	}
	p.recordPatternFetchDiagnostics(diag)
	p.metrics.RecordPatternsDetected(len(entries))
	snapshotBody := []byte(nil)
	if len(entries) > 0 {
		rawSnapshotBody, marshalErr := json.Marshal(patternsResponse{
			Status: "success",
			Data:   entries,
		})
		if marshalErr == nil {
			snapshotBody = rawSnapshotBody
		}
	}
	entries = p.prependCustomPatternEntries(entries, startParam, stepParam, patternLimit)
	entries = fillPatternSamplesAcrossRequestedRange(entries, startParam, endParam, stepParam)
	resultBody, err := json.Marshal(patternsResponse{
		Status: "success",
		Data:   entries,
	})
	if err != nil {
		p.metrics.SetPatternsLastResponse(0, 0)
		p.writeJSON(w, map[string]interface{}{"status": "success", "data": []interface{}{}})
		p.metrics.RecordRequest("patterns", http.StatusOK, time.Since(start))
		return
	}
	recordPatternResponseMetrics(p.metrics, resultBody)
	// Avoid sticky empty results: first-call empty probes should not poison long-lived pattern cache entries.
	if len(entries) > 0 {
		now := time.Now().UTC()
		snapshotPayload := snapshotBody
		if len(snapshotPayload) == 0 {
			snapshotPayload = resultBody
		}
		p.cache.SetWithTTL(cacheWriteKey, resultBody, patternsCacheRetention)
		p.recordPatternSnapshotEntry(cacheWriteKey, snapshotPayload, now)
		if derivedStepCacheKey != "" && derivedStepCacheKey != cacheWriteKey {
			p.cache.SetWithTTL(derivedStepCacheKey, resultBody, patternsCacheRetention)
			p.recordPatternSnapshotEntry(derivedStepCacheKey, snapshotPayload, now)
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
	maxSecondPassWindows int,
	windows []queryRangeWindow,
	stepParam string,
	patternLimit int,
) ([]patternResultEntry, int, patternFetchDiagnostics) {
	if len(windows) == 0 {
		return nil, 0, patternFetchDiagnostics{}
	}
	if perWindowLimit <= 0 {
		perWindowLimit = 1
	}
	if maxSecondPassWindows <= 0 {
		maxSecondPassWindows = maxPatternSecondPassWindows
	}
	effectiveLimit := perWindowLimit
	if sourceLimit > 0 && sourceLimit < effectiveLimit {
		effectiveLimit = sourceLimit
	}
	diag := patternFetchDiagnostics{windowAttempts: len(windows)}

	maxParallel := min(8, max(1, p.queryRangeWindowParallelLimit()))
	if maxParallel > len(windows) {
		maxParallel = len(windows)
	}

	sem := make(chan struct{}, maxParallel)
	var wg sync.WaitGroup

	baseParams := url.Values{}
	baseParams.Set("query", logsqlQuery)
	// Bucketization for patterns is proxy-side. Forwarding step to VictoriaLogs
	// raw log queries makes split subrequests align backward to prior step
	// boundaries, which duplicates adjacent window buckets on deterministic data.
	type windowResult struct {
		window  queryRangeWindow
		entries []patternResultEntry
		stats   patternExtractionStats
	}
	results := make([]windowResult, 0, len(windows))
	var mu sync.Mutex

	fetchWindow := func(window queryRangeWindow, limit int) ([]patternResultEntry, patternExtractionStats, bool) {
		params := cloneURLValues(baseParams)
		params.Set("start", strconv.FormatInt(window.startNs, 10))
		endNs := window.endNs
		if len(windows) > 0 && window.endNs == windows[len(windows)-1].endNs {
			stepNs := int64(parsePatternStepSeconds(stepParam)) * int64(time.Second)
			if stepNs <= 0 {
				stepNs = 1
			}
			if math.MaxInt64-endNs < stepNs {
				endNs = math.MaxInt64
			} else {
				endNs += stepNs
			}
		}
		params.Set("end", strconv.FormatInt(endNs, 10))
		params.Set("limit", strconv.Itoa(limit))
		resp, err := p.vlPost(r.Context(), "/select/logsql/query", params)
		if err != nil {
			p.log.Debug(
				"patterns window fetch failed",
				"start_ns", window.startNs,
				"end_ns", window.endNs,
				"error", err,
			)
			return nil, patternExtractionStats{}, false
		}
		defer resp.Body.Close()
		if resp.StatusCode >= http.StatusBadRequest {
			p.log.Debug(
				"patterns window fetch non-success status",
				"start_ns", window.startNs,
				"end_ns", window.endNs,
				"status_code", resp.StatusCode,
			)
			return nil, patternExtractionStats{}, false
		}
		body, err := readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
		if err != nil {
			return nil, patternExtractionStats{}, false
		}
		extracted, stats := extractLogPatternsWithStats(body, stepParam, patternLimit)
		if len(extracted) == 0 {
			return nil, stats, false
		}
		entries := patternResultEntriesFromMaps(extracted)
		if len(entries) == 0 {
			return nil, stats, false
		}
		return entries, stats, true
	}

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
			entries, stats, ok := fetchWindow(window, effectiveLimit)

			mu.Lock()
			diag.recordExtraction(effectiveLimit, stats, true)
			if ok {
				diag.windowAccepted++
				results = append(results, windowResult{
					window:  window,
					entries: entries,
					stats:   stats,
				})
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	collected := make([]patternResultEntry, 0, len(results))
	cappedResults := make([]windowResult, 0, len(results))
	for _, result := range results {
		collected = mergePatternResultEntries(collected, result.entries)
		if result.stats.hitLimit(effectiveLimit) {
			cappedResults = append(cappedResults, result)
		}
	}
	if boostedLimit := patternSecondPassLimit(effectiveLimit, sourceLimit); boostedLimit > 0 && len(cappedResults) > 0 {
		rerunCount := min(len(cappedResults), maxSecondPassWindows)
		diag.secondPassWindows += rerunCount
		for i := 0; i < rerunCount; i++ {
			result := cappedResults[i]
			entries, stats, ok := fetchWindow(result.window, boostedLimit)
			diag.recordExtraction(boostedLimit, stats, true)
			if !ok || len(entries) == 0 {
				continue
			}
			collected = mergePatternResultEntries(collected, entries)
		}
	}
	diag.minedPostMerge = len(collected)
	if len(collected) == 0 || diag.windowAccepted == 0 || (diag.windowCapped > 0 && len(collected) <= max(1, diag.windowAccepted/2)) {
		diag.markLowCoverage()
	}
	if len(collected) == 0 {
		return nil, diag.windowAccepted, diag
	}
	return collected, diag.windowAccepted, diag
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

func patternWindowedSamplingConfig(startParam, endParam, stepParam string, sourceLimit int, denseWindowing bool) (int64, int64, time.Duration, int, int, bool) {
	startNs, endNs, ok := parseLokiTimeRangeToUnixNano(startParam, endParam)
	if !ok || sourceLimit <= 0 || endNs <= startNs {
		return 0, 0, 0, 0, 0, false
	}

	span := time.Duration(endNs - startNs)
	minWindowedSpan := 20 * time.Minute
	targetWindowSpan := 20 * time.Minute
	minWindowSourceLimit := 200
	maxWindowSourceLimit := 1_000
	maxPatternWindowSamples := 96
	secondPassCap := maxPatternSecondPassWindows
	shortDenseSpanThreshold := 6 * time.Hour
	// The aligned splitter adds an extra boundary window, so keep the internal
	// cap at 20 to hold the effective backend fanout to about 21 windows while
	// still preserving enough bucket coverage for short Drilldown ranges.
	shortDenseWindowCap := 20
	shortDenseSecondPassCap := 4
	if denseWindowing {
		// Dense ranges can otherwise explode into hundreds of windows and produce
		// short/unstable tails under backend pressure. Keep fanout bounded so
		// short dense ranges don't turn a single /patterns refresh into dozens
		// of raw-log backend fetches.
		targetWindowSpan = 45 * time.Minute
		minWindowSourceLimit = 200
		maxWindowSourceLimit = 4_000
		maxPatternWindowSamples = 64
		// Long selected ranges need enough source lines per window to cover the
		// full window rather than a single 5m bucket from each 45m sample.
		if span >= 24*time.Hour {
			minWindowSourceLimit = maxWindowSourceLimit
		}
	}
	if span < minWindowedSpan {
		return 0, 0, 0, 0, 0, false
	}

	windowCount := int(span/targetWindowSpan) + 1
	stepSeconds := parsePatternStepSeconds(stepParam)
	if denseWindowing && stepSeconds > 0 && span <= shortDenseSpanThreshold {
		stepNs := stepSeconds * int64(time.Second)
		if stepNs > 0 {
			stepBasedCount := int(((endNs - startNs) / stepNs) + 1)
			if stepBasedCount > windowCount {
				windowCount = stepBasedCount
			}
		}
		if windowCount > shortDenseWindowCap {
			windowCount = shortDenseWindowCap
		}
		secondPassCap = shortDenseSecondPassCap
	}
	if !denseWindowing {
		if stepSeconds > 0 {
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
		return 0, 0, 0, 0, 0, false
	}
	interval := time.Duration(intervalNs)

	perWindowLimit := sourceLimit / windowCount
	if perWindowLimit < minWindowSourceLimit {
		perWindowLimit = minWindowSourceLimit
	}
	if perWindowLimit > maxWindowSourceLimit {
		perWindowLimit = maxWindowSourceLimit
	}

	return startNs, endNs, interval, perWindowLimit, secondPassCap, true
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

type mergedPatternBucket struct {
	level       string
	tokens      []string
	spacesAfter []int
	samples     map[int64]int
	total       int
}

const patternResultPlaceholderSentinel = "__lvp_pattern_placeholder__"

func mergePatternResultEntries(base, extra []patternResultEntry) []patternResultEntry {
	if len(base) == 0 {
		return extra
	}
	if len(extra) == 0 {
		return base
	}

	tokenizer := newPatternLineTokenizer()
	merged := map[string][]*mergedPatternBucket{}
	add := func(items []patternResultEntry) {
		for _, item := range items {
			pattern := strings.TrimSpace(item.Pattern)
			if pattern == "" {
				continue
			}
			tokens, spacesAfter, ok := tokenizePatternResultPattern(tokenizer, pattern)
			if !ok {
				tokens = []string{pattern}
				spacesAfter = nil
			}
			signature := strings.Join([]string{strings.TrimSpace(item.Level), strconv.Itoa(len(tokens))}, "\x00")
			candidates := merged[signature]
			b := bestMergedPatternBucket(candidates, tokens)
			if b == nil {
				b = &mergedPatternBucket{
					level:       strings.TrimSpace(item.Level),
					tokens:      cloneTokens(tokens),
					spacesAfter: cloneInts(spacesAfter),
					samples:     map[int64]int{},
				}
				merged[signature] = append(merged[signature], b)
			} else {
				b.tokens = mergePatternTemplate(b.tokens, tokens)
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
				if count > b.samples[ts] {
					b.total += count - b.samples[ts]
					b.samples[ts] = count
				}
			}
		}
	}

	add(base)
	add(extra)

	items := make([]*mergedPatternBucket, 0, len(base)+len(extra))
	for _, group := range merged {
		items = append(items, group...)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].total != items[j].total {
			return items[i].total > items[j].total
		}
		if items[i].level != items[j].level {
			return items[i].level < items[j].level
		}
		return tokenizer.Join(items[i].tokens, items[i].spacesAfter) < tokenizer.Join(items[j].tokens, items[j].spacesAfter)
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
		entry := patternResultEntry{
			Pattern: tokenizer.Join(item.tokens, item.spacesAfter),
			Samples: samples,
		}
		if item.level != "" {
			entry.Level = item.level
		}
		out = append(out, entry)
	}

	return out
}

func tokenizePatternResultPattern(tokenizer *patternLineTokenizer, pattern string) ([]string, []int, bool) {
	normalized := strings.ReplaceAll(pattern, patternVarPlaceholder, patternResultPlaceholderSentinel)
	tokens, spacesAfter, ok := tokenizer.Tokenize(normalized)
	if !ok {
		return nil, nil, false
	}
	for i := range tokens {
		if tokens[i] == patternResultPlaceholderSentinel {
			tokens[i] = patternVarPlaceholder
		}
	}
	return tokens, spacesAfter, true
}

func bestMergedPatternBucket(candidates []*mergedPatternBucket, tokens []string) *mergedPatternBucket {
	var match *mergedPatternBucket
	maxSim := -1.0
	maxParamCount := -1

	for _, candidate := range candidates {
		if !patternPrefixCompatible(candidate.tokens, tokens) {
			continue
		}
		curSim, paramCount := getPatternSimilarity(candidate.tokens, tokens)
		if paramCount < 0 {
			continue
		}
		if curSim > maxSim || (curSim == maxSim && paramCount > maxParamCount) {
			maxSim = curSim
			maxParamCount = paramCount
			match = candidate
		}
	}
	if maxSim >= patternSimThreshold {
		return match
	}
	return nil
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
		firstObservedBucket := int64(0)
		lastObservedBucket := int64(0)
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
			if firstObservedBucket == 0 || ts < firstObservedBucket {
				firstObservedBucket = ts
			}
			if ts > lastObservedBucket {
				lastObservedBucket = ts
			}
		}
		if firstObservedBucket == 0 && lastObservedBucket == 0 {
			continue
		}

		fillStartBucket := startBucket
		if firstObservedBucket > fillStartBucket {
			fillStartBucket = firstObservedBucket
		}
		fillEndBucket := endBucket
		if lastObservedBucket < fillEndBucket {
			fillEndBucket = lastObservedBucket
		}
		if fillEndBucket < fillStartBucket {
			continue
		}

		filledPoints := int(((fillEndBucket - fillStartBucket) / effectiveStepSeconds) + 1)
		filled := make([][]interface{}, 0, filledPoints)
		for ts := fillStartBucket; ts <= fillEndBucket; ts += effectiveStepSeconds {
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

func (p *Proxy) storeAutodetectedPatterns(orgID, query, start, end, step string, patterns []map[string]interface{}) {
	if !p.patternsEnabled || !p.patternsAutodetectFromQueries || len(patterns) == 0 {
		return
	}
	cacheKey := p.patternsAutodetectCacheKey(orgID, query, start, end, step)
	if cacheKey == "" {
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
	patterns := extractLogPatternsFromWindowEntries(entries, step, maxPatternResponseLimit)
	p.storeAutodetectedPatterns(orgID, query, start, end, step, patterns)
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
	cacheKey := p.canonicalReadCacheKey("detected_labels", orgID, r)
	if cached, remaining, _, ok := p.endpointReadCacheEntry("detected_labels", cacheKey); ok {
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
		if p.serveStaleReadCacheOnError(w, "detected_labels", cacheKey, start, err) {
			return
		}
		status := statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
		p.metrics.RecordRequest("detected_labels", status, time.Since(start))
		return
	}

	payload := map[string]interface{}{
		"status":         "success",
		"data":           detectedLabels,
		"detectedLabels": detectedLabels,
		"limit":          lineLimit,
	}
	p.setEndpointJSONCacheWithTTL("detected_labels", cacheKey, CacheTTLs["detected_labels"], payload)
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

	// Safeguard 4: Limit time range to maxDeleteTimeRange (30 days).
	// Use the unified parser so RFC3339 timestamps are also subject to the cap.
	startNS, err1 := parseDeleteTimestamp(startTS)
	endNS, err2 := parseDeleteTimestamp(endTS)
	if err1 != nil {
		p.writeError(w, http.StatusBadRequest, "invalid start timestamp: "+err1.Error())
		p.metrics.RecordRequest("delete", http.StatusBadRequest, time.Since(start))
		return
	}
	if err2 != nil {
		p.writeError(w, http.StatusBadRequest, "invalid end timestamp: "+err2.Error())
		p.metrics.RecordRequest("delete", http.StatusBadRequest, time.Since(start))
		return
	}
	rangeDur := time.Duration(endNS - startNS)
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

	// Translate query
	logsqlQuery, err := p.translateQueryWithContext(r.Context(), query)
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
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
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

	logsqlQuery, err := p.translateQueryWithContext(r.Context(), logqlQuery)
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

	body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
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

	body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
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
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
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
	n := copy(s.order, s.order[drop:])
	s.order = s.order[:n]
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

	copyBackendHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (p *Proxy) alertingBackendGet(r *http.Request, backend *url.URL, path string) (*http.Response, error) {
	return p.alertingBackendGetWithParams(r, backend, path, r.URL.Query())
}

// applyAlertingBackendHeaders sets only the Accept-Encoding header on requests
// to alerting/ruler backends. It intentionally does NOT apply p.backendHeaders
// (which carry VL credentials) and does NOT forward client auth headers or
// cookies, so VictoriaLogs credentials and per-user tokens are never sent to a
// different backend service.
func (p *Proxy) applyAlertingBackendHeaders(req *http.Request) {
	if req.Header.Get("Accept-Encoding") == "" {
		switch p.backendCompression {
		case "none":
			req.Header.Set("Accept-Encoding", "identity")
		case "gzip":
			req.Header.Set("Accept-Encoding", "gzip")
		case "zstd":
			req.Header.Set("Accept-Encoding", "zstd")
		default:
			req.Header.Set("Accept-Encoding", "zstd, gzip")
		}
	}
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
	p.applyAlertingBackendHeaders(req)
	start := time.Now()
	resp, err := p.client.Do(req)
	duration := time.Since(start)
	serverPort, _ := strconv.Atoi(u.Port())
	if err != nil {
		mappedStatus := statusFromUpstreamErr(err)
		p.recordUpstreamObservation(r.Context(), "loki", http.MethodGet, path, u.Hostname(), serverPort, mappedStatus, duration, err)
		return nil, err
	}
	if err := decodeCompressedHTTPResponse(resp); err != nil {
		_ = resp.Body.Close()
		p.recordUpstreamObservation(r.Context(), "loki", http.MethodGet, path, u.Hostname(), serverPort, http.StatusBadGateway, duration, err)
		return nil, fmt.Errorf("decode backend response: %w", err)
	}
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

func deriveUpstreamRequestType(route string) string {
	return deriveRequestType("", route)
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
	requestType := deriveUpstreamRequestType(route)
	observedRoute := normalizeObservedRoute(route)
	parentRequestType := deriveRequestType(meta.endpoint, meta.route)

	recordUpstreamCall(ctx, system, requestType, statusCode, duration, err != nil)

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
	if parentRequestType != "" && parentRequestType != "unknown" {
		logAttrs = append(logAttrs, "loki.parent_request.type", parentRequestType)
	}
	if parentRoute := strings.TrimSpace(meta.route); parentRoute != "" {
		logAttrs = append(logAttrs, "http.parent_route", parentRoute)
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

func (p *Proxy) observeInternalOperation(ctx context.Context, operation, outcome string, duration time.Duration) {
	if duration < 0 {
		duration = 0
	}
	recordInternalOperation(ctx, operation, outcome, duration)
	if p != nil && p.metrics != nil {
		p.metrics.RecordInternalOperation(operation, outcome, duration)
	}
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

// vlGetInner executes a GET against VL without checking the circuit breaker.
// Callers must either hold a breaker.Allow() token or use DoWithGuard (which
// enforces the guard before fn is called).
func (p *Proxy) vlGetInner(ctx context.Context, path string, params url.Values) (*http.Response, error) {
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
		p.recordUpstreamObservation(ctx, "vl", http.MethodGet, path, u.Hostname(), serverPort, mappedStatus, duration, err)
		if shouldRecordBreakerFailure(err) {
			p.breaker.RecordFailure()
		}
		return nil, err
	}
	p.observeBackendVersionFromHeaders(resp.Header)
	if err := decodeCompressedHTTPResponse(resp); err != nil {
		_ = resp.Body.Close()
		p.recordUpstreamObservation(ctx, "vl", http.MethodGet, path, u.Hostname(), serverPort, http.StatusBadGateway, duration, err)
		return nil, fmt.Errorf("decode backend response: %w", err)
	}
	p.recordUpstreamObservation(ctx, "vl", http.MethodGet, path, u.Hostname(), serverPort, resp.StatusCode, duration, nil)
	// Any completed HTTP response proves backend reachability; keep breaker for transport failures only.
	p.breaker.RecordSuccess()
	return resp, nil
}

func (p *Proxy) vlGet(ctx context.Context, path string, params url.Values) (*http.Response, error) {
	if !p.breaker.Allow() {
		return nil, fmt.Errorf("circuit breaker open — backend unavailable")
	}
	return p.vlGetInner(ctx, path, params)
}

// vlPostInner executes a POST against VL without checking the circuit breaker.
// Callers must either hold a breaker.Allow() token or use DoWithGuard.
func (p *Proxy) vlPostInner(ctx context.Context, path string, params url.Values) (*http.Response, error) {
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
		p.recordUpstreamObservation(ctx, "vl", http.MethodPost, path, u.Hostname(), serverPort, mappedStatus, duration, err)
		if shouldRecordBreakerFailure(err) {
			p.breaker.RecordFailure()
		}
		return nil, err
	}
	p.observeBackendVersionFromHeaders(resp.Header)
	if err := decodeCompressedHTTPResponse(resp); err != nil {
		_ = resp.Body.Close()
		p.recordUpstreamObservation(ctx, "vl", http.MethodPost, path, u.Hostname(), serverPort, http.StatusBadGateway, duration, err)
		return nil, fmt.Errorf("decode backend response: %w", err)
	}
	p.recordUpstreamObservation(ctx, "vl", http.MethodPost, path, u.Hostname(), serverPort, resp.StatusCode, duration, nil)
	// Any completed HTTP response proves backend reachability; keep breaker for transport failures only.
	p.breaker.RecordSuccess()
	return resp, nil
}

func (p *Proxy) vlPost(ctx context.Context, path string, params url.Values) (*http.Response, error) {
	if !p.breaker.Allow() {
		return nil, fmt.Errorf("circuit breaker open — backend unavailable")
	}
	return p.vlPostInner(ctx, path, params)
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

// vlGetCoalescedWithStatus wraps vlGetInner with request coalescing and a CB guard.
// When the circuit breaker is open and a request for the same key is already
// in-flight, this call joins the in-flight rather than failing immediately.
func (p *Proxy) vlGetCoalescedWithStatus(ctx context.Context, key, path string, params url.Values) (int, []byte, error) {
	status, _, body, err := p.coalescer.DoWithGuard(key, p.breaker.Allow, func() (*http.Response, error) {
		return p.vlGetInner(ctx, path, params)
	})
	if errors.Is(err, mw.ErrGuardRejected) {
		return 0, nil, fmt.Errorf("circuit breaker open — backend unavailable")
	}
	if err != nil {
		return 0, nil, err
	}
	return status, body, nil
}

// vlPostCoalesced wraps vlPostInner with request coalescing and a CB guard.
func (p *Proxy) vlPostCoalesced(ctx context.Context, key, path string, params url.Values) (int, []byte, error) {
	status, _, body, err := p.coalescer.DoWithGuard(key, p.breaker.Allow, func() (*http.Response, error) {
		return p.vlPostInner(ctx, path, params)
	})
	if errors.Is(err, mw.ErrGuardRejected) {
		return 0, nil, fmt.Errorf("circuit breaker open — backend unavailable")
	}
	if err != nil {
		return 0, nil, err
	}
	return status, body, nil
}

// --- Stats query proxying ---

func (p *Proxy) proxyStatsQueryRange(w http.ResponseWriter, r *http.Request, logsqlQuery string) {
	originalLogql := resolveGrafanaRangeTemplateTokens(r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), r.FormValue("step"))
	if p.handleStatsCompatRange(w, r, originalLogql, logsqlQuery) {
		return
	}

	// Keep metric query_range as a single backend request. Window splitting and
	// window-level cache reuse are for raw log queries only.
	params := buildStatsQueryRangeParams(logsqlQuery, r.FormValue("start"), r.FormValue("end"), r.FormValue("step"))

	resp, err := p.vlPost(r.Context(), "/select/logsql/stats_query_range", params)
	if err != nil {
		p.writeError(w, statusFromUpstreamErr(err), err.Error())
		return
	}
	defer resp.Body.Close()

	body, _ := readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)

	// Propagate VL error status
	if resp.StatusCode >= 400 {
		p.writeError(w, resp.StatusCode, string(body))
		return
	}

	body = trimStatsQueryRangeResponseToEnd(body, r.FormValue("end"))

	// VL stats_query_range returns Prometheus-compatible format.
	// Just wrap it in Loki's envelope.
	// Translate label names (e.g., dots → underscores) in metric labels.
	body = p.translateStatsResponseLabelsWithContext(r.Context(), body, r.FormValue("query"))
	w.Header().Set("Content-Type", "application/json")
	w.Write(wrapAsLokiResponse(body, "matrix"))
}

type statsQueryRangeSeries struct {
	Metric map[string]interface{} `json:"metric"`
	Values [][]interface{}        `json:"values"`
}

type statsQueryRangeResponse struct {
	Data struct {
		Result []statsQueryRangeSeries `json:"result"`
	} `json:"data"`
	Results []statsQueryRangeSeries `json:"results"`
}

func buildStatsQueryRangeParams(logsqlQuery, startRaw, endRaw, stepRaw string) url.Values {
	params := url.Values{}
	params.Set("query", logsqlQuery)
	if s := strings.TrimSpace(startRaw); s != "" {
		params.Set("start", formatVLTimestamp(s))
	}
	if e := strings.TrimSpace(endRaw); e != "" {
		if extendedEnd, ok := extendStatsQueryRangeEnd(e, stepRaw); ok {
			params.Set("end", extendedEnd)
		} else {
			params.Set("end", formatVLTimestamp(e))
		}
	}
	if step := strings.TrimSpace(stepRaw); step != "" {
		params.Set("step", formatVLStep(step))
	}
	return params
}

func extendStatsQueryRangeEnd(endRaw, stepRaw string) (string, bool) {
	endNs, ok := parseLokiTimeToUnixNano(endRaw)
	if !ok {
		return "", false
	}
	stepDur, ok := parsePositiveStepDuration(stepRaw)
	if !ok || stepDur <= 0 {
		return "", false
	}
	return strconv.FormatInt(endNs+stepDur.Nanoseconds(), 10), true
}

func trimStatsQueryRangeResponseToEnd(body []byte, endRaw string) []byte {
	endNs, ok := parseLokiTimeToUnixNano(endRaw)
	if !ok {
		return body
	}

	var resp statsQueryRangeResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return body
	}

	results := resp.Results
	target := &resp.Results
	if len(results) == 0 {
		results = resp.Data.Result
		target = &resp.Data.Result
	}
	if len(results) == 0 {
		return body
	}

	changed := false
	trimmed := make([]statsQueryRangeSeries, 0, len(results))
	for _, series := range results {
		filtered := make([][]interface{}, 0, len(series.Values))
		for _, point := range series.Values {
			if statsQueryRangePointUnixNano(point) <= endNs {
				filtered = append(filtered, point)
				continue
			}
			changed = true
		}
		series.Values = filtered
		trimmed = append(trimmed, series)
	}
	if !changed {
		return body
	}
	*target = trimmed
	encoded, err := json.Marshal(resp)
	if err != nil {
		return body
	}
	return encoded
}

func statsQueryRangePointUnixNano(point []interface{}) int64 {
	if len(point) == 0 {
		return 0
	}
	switch ts := point[0].(type) {
	case float64:
		return normalizeLokiNumericTimeToUnixNano(ts)
	case float32:
		return normalizeLokiNumericTimeToUnixNano(float64(ts))
	case int:
		return normalizeLokiIntTimeToUnixNano(int64(ts))
	case int64:
		return normalizeLokiIntTimeToUnixNano(ts)
	case int32:
		return normalizeLokiIntTimeToUnixNano(int64(ts))
	case json.Number:
		if value, err := ts.Float64(); err == nil {
			return normalizeLokiNumericTimeToUnixNano(value)
		}
	case string:
		if value, ok := parseLokiTimeToUnixNano(ts); ok {
			return value
		}
	}
	return 0
}

func (p *Proxy) proxyStatsQuery(w http.ResponseWriter, r *http.Request, logsqlQuery string) {
	originalLogql := resolveGrafanaRangeTemplateTokens(r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), r.FormValue("step"))
	if p.handleStatsCompatInstant(w, r, originalLogql, logsqlQuery) {
		return
	}

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

	body, _ := readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)

	// Propagate VL error status
	if resp.StatusCode >= 400 {
		p.writeError(w, resp.StatusCode, string(body))
		return
	}

	body = p.translateStatsResponseLabelsWithContext(r.Context(), body, r.FormValue("query"))
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
		leftBody, _ = readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
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
		rightBody, _ = readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
	}

	// Apply vector matching: on(), ignoring(), group_left(), group_right()
	var result []byte
	if len(vm.On) > 0 {
		result = applyOnMatching(leftBody, rightBody, op, vm.On, resultType)
	} else if len(vm.Ignoring) > 0 {
		if err := validateVectorMatchCardinality(leftBody, rightBody, nil, vm.Ignoring, len(vm.GroupLeft) > 0, len(vm.GroupRight) > 0); err != nil {
			p.writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
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
		leftBody, _ = readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
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
		rightBody, _ = readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
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

	results, _ := extractMetricResults(vlResp)
	for _, r := range results {
		rm, _ := r.(map[string]interface{})
		applyScalarToSample(rm, scalar, op, false)
	}

	result, _ := json.Marshal(vlResp)
	return wrapAsLokiResponse(result, resultType)
}

func applyScalarOpReverse(body []byte, op string, scalar float64, resultType string) []byte {
	var vlResp map[string]interface{}
	if err := json.Unmarshal(body, &vlResp); err != nil {
		return wrapAsLokiResponse(body, resultType)
	}

	results, _ := extractMetricResults(vlResp)
	for _, r := range results {
		rm, _ := r.(map[string]interface{})
		applyScalarToSample(rm, scalar, op, true)
	}

	result, _ := json.Marshal(vlResp)
	return wrapAsLokiResponse(result, resultType)
}

func parsePointValue(raw interface{}) float64 {
	switch v := raw.(type) {
	case float64:
		return v
	case string:
		parsed, _ := strconv.ParseFloat(v, 64)
		return parsed
	default:
		return 0
	}
}

func combineMetricResults(leftBody, rightBody []byte, op, resultType string) []byte {
	// Parse both results
	var leftResp, rightResp map[string]interface{}
	json.Unmarshal(leftBody, &leftResp)
	json.Unmarshal(rightBody, &rightResp)

	leftResults, _ := extractMetricResults(leftResp)
	rightResults, _ := extractMetricResults(rightResp)

	// Build a map of right results by metric labels for joining
	rightMap := make(map[string]map[string]float64)
	for _, r := range rightResults {
		rm, _ := r.(map[string]interface{})
		metric, _ := rm["metric"].(map[string]interface{})
		key := metricKey(metric)
		rightMap[key] = samplePointIndex(rm)
	}

	// Combine: for each left result, find matching right result and apply op
	for _, r := range leftResults {
		rm, _ := r.(map[string]interface{})
		metric, _ := rm["metric"].(map[string]interface{})
		key := metricKey(metric)
		rightIdx := rightMap[key]
		if len(rightIdx) > 0 {
			applyBinaryToSample(rm, rightIdx, op)
		}
	}

	result, _ := json.Marshal(leftResp)
	return wrapAsLokiResponse(result, resultType)
}

func extractMetricResults(payload map[string]interface{}) ([]interface{}, bool) {
	if results, ok := payload["results"].([]interface{}); ok {
		return results, true
	}
	if data, ok := payload["data"].(map[string]interface{}); ok {
		if result, ok := data["result"].([]interface{}); ok {
			return result, true
		}
	}
	if result, ok := payload["result"].([]interface{}); ok {
		return result, true
	}
	return nil, false
}

func applyScalarToSample(sample map[string]interface{}, scalar float64, op string, reverse bool) {
	values, _ := sample["values"].([]interface{})
	for i, raw := range values {
		point, _ := raw.([]interface{})
		if len(point) < 2 {
			continue
		}
		val := parsePointValue(point[1])
		newVal := applyOp(val, scalar, op)
		if reverse {
			newVal = applyOp(scalar, val, op)
		}
		point[1] = strconv.FormatFloat(newVal, 'f', -1, 64)
		values[i] = point
	}
	if len(values) > 0 {
		sample["values"] = values
	}

	if value, ok := sample["value"].([]interface{}); ok && len(value) >= 2 {
		val := parsePointValue(value[1])
		newVal := applyOp(val, scalar, op)
		if reverse {
			newVal = applyOp(scalar, val, op)
		}
		value[1] = strconv.FormatFloat(newVal, 'f', -1, 64)
		sample["value"] = value
	}
}

func samplePointIndex(sample map[string]interface{}) map[string]float64 {
	index := map[string]float64{}

	values, _ := sample["values"].([]interface{})
	for _, raw := range values {
		point, _ := raw.([]interface{})
		if len(point) < 2 {
			continue
		}
		index[fmt.Sprintf("%v", point[0])] = parsePointValue(point[1])
	}

	if value, ok := sample["value"].([]interface{}); ok && len(value) >= 2 {
		index[fmt.Sprintf("%v", value[0])] = parsePointValue(value[1])
	}

	return index
}

func applyBinaryToSample(sample map[string]interface{}, rightIndex map[string]float64, op string) {
	values, _ := sample["values"].([]interface{})
	for i, raw := range values {
		point, _ := raw.([]interface{})
		if len(point) < 2 {
			continue
		}
		ts := fmt.Sprintf("%v", point[0])
		rightVal, ok := rightIndex[ts]
		if !ok {
			continue
		}
		leftVal := parsePointValue(point[1])
		point[1] = strconv.FormatFloat(applyOp(leftVal, rightVal, op), 'f', -1, 64)
		values[i] = point
	}
	if len(values) > 0 {
		sample["values"] = values
	}

	if value, ok := sample["value"].([]interface{}); ok && len(value) >= 2 {
		ts := fmt.Sprintf("%v", value[0])
		if rightVal, ok := rightIndex[ts]; ok {
			leftVal := parsePointValue(value[1])
			value[1] = strconv.FormatFloat(applyOp(leftVal, rightVal, op), 'f', -1, 64)
			sample["value"] = value
		}
	}
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
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
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

	collectPatterns := p.patternsEnabled && p.patternsAutodetectFromQueries
	streams, patterns, err := p.vlReaderToLokiStreams(
		resp.Body,
		r.FormValue("query"),
		r.FormValue("step"),
		categorizedLabels,
		emitStructuredMetadata,
		collectPatterns,
	)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	p.storeAutodetectedPatterns(
		r.Header.Get("X-Scope-OrgID"),
		r.FormValue("query"),
		r.FormValue("start"),
		r.FormValue("end"),
		r.FormValue("step"),
		patterns,
	)

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

	p.writeJSON(w, map[string]interface{}{
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
	streamOrder := make([]string, 0, estimatedLines/10+1)

	// Scan lines without copying the entire body to a string.
	start := 0
	for start < len(body) {
		end := start
		for end < len(body) && body[end] != '\n' {
			end++
		}
		line := body[start:end]
		if end < len(body) {
			start = end + 1
		} else {
			start = len(body)
		}

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
		msg, _ := stringifyEntryValue(entry["_msg"])
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
			streamOrder = append(streamOrder, streamKey)
		}

		// Return pooled entry after extracting all needed data
		vlEntryPool.Put(entry)

		se.Values = append(se.Values, []string{tsNanos, msg})
	}

	// Sort streams by key for stable cross-stream ordering. VL returns entries
	// in non-deterministic stream order; without this, same-timestamp entries
	// from different streams reorder between requests.
	sort.Strings(streamOrder)
	result := make([]map[string]interface{}, 0, len(streamMap))
	for _, key := range streamOrder {
		se := streamMap[key]
		// Only tie-break entries with identical nanosecond timestamps by message
		// content. Different-timestamp entries are left in VL's returned order
		// (which already reflects the requested direction: ascending/descending).
		sort.SliceStable(se.Values, func(i, j int) bool {
			ti, tj := se.Values[i][0], se.Values[j][0]
			if ti != tj {
				return false
			}
			return se.Values[i][1] < se.Values[j][1]
		})
		result = append(result, map[string]interface{}{
			"stream": se.Labels,
			"values": se.Values,
		})
	}
	return result
}

type cachedLogQueryStreamDescriptor struct {
	key              string
	rawLabels        map[string]string
	translatedLabels map[string]string
}

func (p *Proxy) vlReaderToLokiStreams(r io.Reader, originalQuery, step string, categorizedLabels bool, emitStructuredMetadata bool, collectPatterns bool) ([]map[string]interface{}, []map[string]interface{}, error) {
	type streamEntry struct {
		Labels map[string]string
		Values []interface{}
	}

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	streamMap := make(map[string]*streamEntry, 32)
	streamOrder := make([]string, 0, 32)
	streamDescriptorCache := make(map[string]cachedLogQueryStreamDescriptor, 16)
	streamLabelCache := make(map[string]map[string]string, 16)
	exposureCache := make(map[string][]metadataFieldExposure, 16)
	classifyAsParsed := hasParserStage(originalQuery, "json") || hasParserStage(originalQuery, "logfmt")

	var (
		miner        *patternMiner
		stepSeconds  int64
		patternCount int
	)
	if collectPatterns {
		miner = newPatternMiner()
		stepSeconds = parsePatternStepSeconds(step)
	}

	for scanner.Scan() {
		line := scanner.Bytes()
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
		rawStream := asString(entry["_stream"])
		level, _ := stringifyEntryValue(entry["level"])

		desc := p.logQueryStreamDescriptor(rawStream, level, streamLabelCache, streamDescriptorCache)
		structuredMetadata, parsedFields := p.classifyEntryMetadataFields(entry, desc.rawLabels, classifyAsParsed, exposureCache)
		se, ok := streamMap[desc.key]
		if !ok {
			se = &streamEntry{
				Labels: desc.translatedLabels,
				Values: make([]interface{}, 0, 8),
			}
			streamMap[desc.key] = se
			streamOrder = append(streamOrder, desc.key)
		}
		se.Values = append(se.Values, buildStreamValue(tsNanos, msg, structuredMetadata, parsedFields, emitStructuredMetadata, categorizedLabels))

		if miner != nil {
			levelValue := strings.TrimSpace(desc.rawLabels["detected_level"])
			if levelValue == "" {
				levelValue = strings.TrimSpace(desc.rawLabels["level"])
			}
			if unixSeconds, ok := parseFlexibleUnixSeconds(timeStr); ok {
				bucket := unixSeconds
				if stepSeconds > 0 {
					bucket = (bucket / stepSeconds) * stepSeconds
				}
				miner.Observe(levelValue, msg, bucket)
				patternCount++
			}
		}

		vlEntryPool.Put(entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}

	sort.Strings(streamOrder)
	result := make([]map[string]interface{}, 0, len(streamMap))
	for _, key := range streamOrder {
		se := streamMap[key]
		// Only tie-break entries with identical nanosecond timestamps by message
		// content. Different-timestamp entries are left in VL's returned order
		// (which already reflects the requested direction: ascending/descending).
		sort.SliceStable(se.Values, func(i, j int) bool {
			vi, _ := se.Values[i].([]interface{})
			vj, _ := se.Values[j].([]interface{})
			if len(vi) < 2 || len(vj) < 2 {
				return false
			}
			ti, _ := vi[0].(string)
			tj, _ := vj[0].(string)
			if ti != tj {
				return false
			}
			msgi, _ := vi[1].(string)
			msgj, _ := vj[1].(string)
			return msgi < msgj
		})
		result = append(result, map[string]interface{}{
			"stream": se.Labels,
			"values": se.Values,
		})
	}

	var patterns []map[string]interface{}
	if miner != nil && patternCount > 0 {
		patterns = buildPatternResponse(miner, maxPatternResponseLimit)
	}
	return result, patterns, nil
}

func (p *Proxy) logQueryStreamDescriptor(rawStream, level string, streamLabelCache map[string]map[string]string, descriptorCache map[string]cachedLogQueryStreamDescriptor) cachedLogQueryStreamDescriptor {
	cacheKey := rawStream + "\x00" + strings.TrimSpace(level)
	if desc, ok := descriptorCache[cacheKey]; ok {
		return desc
	}

	baseLabels, ok := streamLabelCache[rawStream]
	if !ok {
		baseLabels = parseStreamLabels(rawStream)
		streamLabelCache[rawStream] = baseLabels
	}

	rawLabels := cloneStringMap(baseLabels)
	if trimmed := strings.TrimSpace(level); trimmed != "" {
		rawLabels["level"] = trimmed
	}
	ensureDetectedLevel(rawLabels)
	ensureSyntheticServiceName(rawLabels)

	translatedLabels := rawLabels
	if p != nil && p.labelTranslator != nil && !p.labelTranslator.IsPassthrough() {
		translatedLabels = p.labelTranslator.TranslateLabelsMap(rawLabels)
		ensureDetectedLevel(translatedLabels)
		ensureSyntheticServiceName(translatedLabels)
	}

	desc := cachedLogQueryStreamDescriptor{
		key:              canonicalLabelsKey(rawLabels),
		rawLabels:        rawLabels,
		translatedLabels: translatedLabels,
	}
	descriptorCache[cacheKey] = desc
	return desc
}

func (p *Proxy) classifyEntryMetadataFields(entry map[string]interface{}, streamLabels map[string]string, classifyAsParsed bool, exposureCache map[string][]metadataFieldExposure) (map[string]string, map[string]string) {
	var (
		parsedFields             map[string]string
		structuredMetadataFields map[string]string
	)

	for key, value := range entry {
		if isVLInternalField(key) || key == "_stream_id" || key == "level" {
			continue
		}
		if _, exists := streamLabels[key]; exists {
			continue
		}
		stringValue, ok := stringifyEntryValue(value)
		if !ok || strings.TrimSpace(stringValue) == "" {
			continue
		}
		exposures := p.metadataFieldExposuresCached(key, exposureCache)
		for _, exposure := range exposures {
			if _, exists := streamLabels[exposure.name]; exists && !exposure.isAlias {
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

	return structuredMetadataFields, parsedFields
}

func (p *Proxy) metadataFieldExposuresCached(vlField string, exposureCache map[string][]metadataFieldExposure) []metadataFieldExposure {
	if len(exposureCache) == 0 {
		return p.metadataFieldExposures(vlField)
	}
	if exposures, ok := exposureCache[vlField]; ok {
		return exposures
	}
	exposures := p.metadataFieldExposures(vlField)
	exposureCache[vlField] = exposures
	return exposures
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(src))
	for key, value := range src {
		out[key] = value
	}
	return out
}

func stringifyEntryValue(value interface{}) (string, bool) {
	if value == nil {
		return "", false
	}
	switch v := value.(type) {
	case string:
		return v, true
	case map[string]interface{}, []interface{}:
		// VL may parse JSON-looking _msg fields into objects; re-serialize to
		// preserve valid JSON so Grafana can pretty-print the log line.
		b, err := json.Marshal(v)
		if err == nil {
			return string(b), true
		}
		return fmt.Sprintf("%v", v), true
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

var emptyCategorizedMetadata = map[string]interface{}{}

func buildStreamValue(ts, msg string, structuredMetadata map[string]string, parsedFields map[string]string, emitStructuredMetadata bool, categorizedLabels bool) interface{} {
	if !categorizedLabels {
		return []interface{}{ts, msg}
	}

	if emitStructuredMetadata {
		metadata := make(map[string]interface{}, 2)
		if len(structuredMetadata) > 0 {
			metadata["structuredMetadata"] = metadataFieldMap(structuredMetadata)
		}
		if len(parsedFields) > 0 {
			metadata["parsed"] = metadataFieldMap(parsedFields)
		}
		if len(metadata) > 0 {
			return []interface{}{ts, msg, metadata}
		}
	}
	return []interface{}{ts, msg, emptyCategorizedMetadata}
}

func metadataFieldMap(fields map[string]string) map[string]string {
	if len(fields) == 0 {
		return nil
	}
	pairs := make(map[string]string, len(fields))
	for key, value := range fields {
		pairs[key] = value
	}
	return pairs
}

func (p *Proxy) classifyEntryFields(entry map[string]interface{}, originalQuery string) (map[string]string, map[string]string, map[string]string) {
	stream := parseStreamLabels(asString(entry["_stream"]))
	labels := make(map[string]string, len(stream))
	for k, v := range stream {
		labels[k] = v
	}
	if value, ok := stringifyEntryValue(entry["level"]); ok && strings.TrimSpace(value) != "" {
		labels["level"] = value
	}
	// Mirror Loki's ingest-time level detection: if VL did not surface level as
	// a top-level field (native field or OTel severity), try to extract it from
	// the raw _msg string (JSON or logfmt) so detected_level matches Loki.
	if labels["level"] == "" && labels["detected_level"] == "" {
		if msgStr, ok := entry["_msg"].(string); ok {
			if lvl, ok := extractLevelFromMsg(msgStr); ok {
				labels["level"] = lvl
			}
		}
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

// streamLabelsCacheMu guards streamLabelsCache to prevent concurrent map writes.
// The cache avoids re-parsing the same _stream value (identical for all log lines
// in a stream), which was the top allocation hot path in pprof (10-15% of totals).
var (
	streamLabelsCacheMu sync.RWMutex
	streamLabelsCache   = make(map[string]map[string]string, 256)
)

// parseStreamLabels parses {key="value",key2="value2"} into a map.
// Results are cached by the raw string — _stream values repeat heavily across
// a result set, so the cache eliminates redundant allocations.
// Callers must not mutate the returned map.
func parseStreamLabels(s string) map[string]string {
	streamLabelsCacheMu.RLock()
	if m, ok := streamLabelsCache[s]; ok {
		streamLabelsCacheMu.RUnlock()
		return m
	}
	streamLabelsCacheMu.RUnlock()

	m := parseStreamLabelsUncached(s)

	streamLabelsCacheMu.Lock()
	// Bound cache size to avoid unbounded growth.
	if len(streamLabelsCache) < 4096 {
		streamLabelsCache[s] = m
	}
	streamLabelsCacheMu.Unlock()
	return m
}

// parseStreamLabelsUncached parses without cache lookup. Inlines the split loop
// to avoid allocating an intermediate []string of pairs.
func parseStreamLabelsUncached(s string) map[string]string {
	s = strings.Trim(s, "{}")
	if s == "" {
		return map[string]string{}
	}
	labels := make(map[string]string, 4)
	inQuote := false
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '"' {
			inQuote = !inQuote
		}
		if c == ',' && !inQuote {
			appendLabelPair(s[start:i], labels)
			start = i + 1
		}
	}
	appendLabelPair(s[start:], labels)
	return labels
}

// appendLabelPair parses a single key="value" token into dst.
func appendLabelPair(pair string, dst map[string]string) {
	pair = strings.TrimSpace(pair)
	eq := strings.IndexByte(pair, '=')
	if eq <= 0 {
		return
	}
	k := strings.TrimSpace(pair[:eq])
	v := strings.TrimSpace(pair[eq+1:])
	v = strings.Trim(v, `"`)
	dst[k] = v
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
	if rawData, ok := promResp["data"]; ok {
		if dataMap, ok := rawData.(map[string]interface{}); ok {
			normalizeLokiResultDataShape(dataMap, resultType)
			result, _ := json.Marshal(map[string]interface{}{
				"status": "success",
				"data":   dataMap,
			})
			return result
		}
		result, _ := json.Marshal(map[string]interface{}{
			"status": "success",
			"data": map[string]interface{}{
				"resultType": resultType,
				"result":     []interface{}{},
			},
		})
		return result
	}

	normalizeLokiResultDataShape(promResp, resultType)

	result, _ := json.Marshal(map[string]interface{}{
		"status": "success",
		"data":   promResp,
	})
	return result
}

func normalizeLokiResultDataShape(data map[string]interface{}, defaultResultType string) {
	if data == nil {
		return
	}

	if _, hasResult := data["result"]; !hasResult {
		if rawResults, ok := data["results"]; ok {
			data["result"] = rawResults
		} else {
			data["result"] = []interface{}{}
		}
	}

	currentResultType, _ := data["resultType"].(string)
	if strings.TrimSpace(currentResultType) == "" && strings.TrimSpace(defaultResultType) != "" {
		data["resultType"] = defaultResultType
	}
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

type bareParserMetricCompatSpec struct {
	funcName        string
	baseQuery       string
	rangeWindow     time.Duration
	rangeWindowExpr string
	unwrapField     string
	quantile        float64
}

type bareParserMetricSample struct {
	tsNanos int64
	value   float64
}

type bareParserMetricSeries struct {
	metric  map[string]string
	samples []bareParserMetricSample
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

func parseFlexibleUnixNanos(raw string) (int64, bool) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return 0, false
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed.UnixNano(), true
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed.UnixNano(), true
	}
	if integer, err := strconv.ParseInt(value, 10, 64); err == nil {
		return normalizeUnixNanos(integer), true
	}
	if floating, err := strconv.ParseFloat(value, 64); err == nil {
		abs := math.Abs(floating)
		switch {
		case abs >= 1_000_000_000_000_000_000:
			return int64(floating), true
		case abs >= 1_000_000_000_000_000:
			return int64(floating * 1_000), true
		case abs >= 1_000_000_000_000:
			return int64(floating * 1_000_000), true
		default:
			return int64(floating * float64(time.Second)), true
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

func normalizeUnixNanos(v int64) int64 {
	abs := v
	if abs < 0 {
		abs = -abs
	}
	switch {
	case abs >= 1_000_000_000_000_000_000:
		return v
	case abs >= 1_000_000_000_000_000:
		return v * 1_000
	case abs >= 1_000_000_000_000:
		return v * 1_000_000
	default:
		return v * int64(time.Second)
	}
}

var (
	prometheusDurationPartRE = regexp.MustCompile(`(?i)([0-9]*\.?[0-9]+)(ns|us|µs|ms|s|m|h|d|w|y)`)
	grafanaRangeTokens       = []string{
		"$__rate_interval_ms",
		"$__interval_ms",
		"$__range_ms",
		"$__rate_interval",
		"$__range_s",
		"$__interval",
		"$__range",
		"$__auto",
	}
)

func parsePositiveStepDuration(step string) (time.Duration, bool) {
	value := strings.TrimSpace(step)
	if value == "" {
		return 0, false
	}
	if d, err := time.ParseDuration(value); err == nil && d > 0 {
		return d, true
	}
	if d, ok := parsePrometheusStyleDuration(value); ok {
		return d, true
	}
	seconds, err := strconv.ParseFloat(value, 64)
	if err != nil || seconds <= 0 {
		return 0, false
	}
	nanos := seconds * float64(time.Second)
	if nanos > float64(math.MaxInt64) {
		return 0, false
	}
	d := time.Duration(nanos)
	if d <= 0 {
		return 0, false
	}
	return d, true
}

func parsePrometheusStyleDuration(value string) (time.Duration, bool) {
	parts := prometheusDurationPartRE.FindAllStringSubmatch(value, -1)
	if len(parts) == 0 {
		return 0, false
	}

	totalSeconds := 0.0
	var consumed strings.Builder
	for _, part := range parts {
		numeric, err := strconv.ParseFloat(part[1], 64)
		if err != nil || numeric <= 0 {
			return 0, false
		}

		switch strings.ToLower(part[2]) {
		case "ns":
			totalSeconds += numeric / 1e9
		case "us", "µs":
			totalSeconds += numeric / 1e6
		case "ms":
			totalSeconds += numeric / 1e3
		case "s":
			totalSeconds += numeric
		case "m":
			totalSeconds += numeric * 60
		case "h":
			totalSeconds += numeric * 3600
		case "d":
			totalSeconds += numeric * 86400
		case "w":
			totalSeconds += numeric * 7 * 86400
		case "y":
			totalSeconds += numeric * 365 * 86400
		default:
			return 0, false
		}

		consumed.WriteString(part[0])
	}

	if consumed.String() != value {
		return 0, false
	}

	nanos := totalSeconds * float64(time.Second)
	if nanos <= 0 || nanos > float64(math.MaxInt64) {
		return 0, false
	}

	d := time.Duration(nanos)
	if d <= 0 {
		return 0, false
	}
	return d, true
}

func resolveGrafanaRangeTemplateTokens(query, start, end, step string) string {
	if !strings.Contains(query, "$__") && !strings.Contains(query, "${__") {
		return query
	}

	replacements := map[string]string{}
	for _, token := range grafanaRangeTokens {
		if duration, ok := resolveGrafanaTemplateTokenDuration(token, start, end, step); ok {
			replacements[token] = formatLogQLDuration(duration)
		}
	}

	if len(replacements) == 0 {
		return query
	}

	normalized := query
	for _, token := range grafanaRangeTokens {
		replacement, ok := replacements[token]
		if !ok {
			continue
		}
		braced := "${" + strings.TrimPrefix(token, "$") + "}"
		normalized = strings.ReplaceAll(normalized, braced, replacement)
		normalized = strings.ReplaceAll(normalized, token, replacement)
	}
	return normalized
}

func resolveGrafanaTemplateTokenDuration(token, start, end, step string) (time.Duration, bool) {
	canonicalToken, ok := canonicalGrafanaRangeToken(token)
	if !ok {
		return 0, false
	}

	stepDur, stepOK := parsePositiveStepDuration(step)
	if !stepOK {
		stepDur = time.Minute
	}

	rangeDur := stepDur
	startNanos, startOK := parseFlexibleUnixNanos(start)
	endNanos, endOK := parseFlexibleUnixNanos(end)
	if startOK && endOK && endNanos > startNanos {
		rangeDur = time.Duration(endNanos - startNanos)
	}
	if rangeDur <= 0 {
		rangeDur = stepDur
	}

	switch canonicalToken {
	case "$__auto", "$__interval", "$__interval_ms":
		return stepDur, true
	case "$__rate_interval", "$__rate_interval_ms":
		rateInterval := stepDur * 4
		if rateInterval < time.Minute {
			rateInterval = time.Minute
		}
		return rateInterval, true
	case "$__range", "$__range_s", "$__range_ms":
		return rangeDur, true
	default:
		return 0, false
	}
}

func canonicalGrafanaRangeToken(token string) (string, bool) {
	normalized := strings.ToLower(strings.TrimSpace(token))
	if normalized == "" {
		return "", false
	}
	if strings.HasPrefix(normalized, "${") && strings.HasSuffix(normalized, "}") {
		normalized = "$" + strings.TrimSuffix(strings.TrimPrefix(normalized, "${"), "}")
	}
	if strings.HasPrefix(normalized, "__") {
		normalized = "$" + normalized
	}

	switch normalized {
	case "$__auto",
		"$__interval",
		"$__interval_ms",
		"$__rate_interval",
		"$__rate_interval_ms",
		"$__range",
		"$__range_s",
		"$__range_ms":
		return normalized, true
	default:
		return "", false
	}
}

func formatLogQLDuration(d time.Duration) string {
	if d <= 0 {
		return "1s"
	}
	if d%time.Hour == 0 {
		return strconv.FormatInt(int64(d/time.Hour), 10) + "h"
	}
	if d%time.Minute == 0 {
		return strconv.FormatInt(int64(d/time.Minute), 10) + "m"
	}
	if d%time.Second == 0 {
		return strconv.FormatInt(int64(d/time.Second), 10) + "s"
	}
	if d%time.Millisecond == 0 {
		return strconv.FormatInt(int64(d/time.Millisecond), 10) + "ms"
	}
	return strconv.FormatFloat(d.Seconds(), 'f', -1, 64) + "s"
}

func parseStepSeconds(step string) (int64, bool) {
	d, ok := parsePositiveStepDuration(step)
	if !ok || d < time.Second {
		return 0, false
	}
	return int64(d / time.Second), true
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
	serviceSignal := hasServiceSignal(translated)
	ensureSyntheticServiceName(translated)
	if !serviceSignal && strings.TrimSpace(translated["service_name"]) == unknownServiceName {
		delete(translated, "service_name")
	}
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

// parseDeleteTimestamp parses a timestamp string used in delete requests and
// returns Unix nanoseconds. It accepts float seconds, float nanoseconds (>1e15),
// RFC3339Nano, and RFC3339. Returns an error for unrecognized formats so callers
// can reject them rather than forwarding an unbounded time range.
func parseDeleteTimestamp(ts string) (int64, error) {
	if f, err := strconv.ParseFloat(ts, 64); err == nil {
		if f > 1e15 {
			// Already nanoseconds — keep as-is (truncated to int64).
			return int64(f), nil
		}
		// Seconds — multiply to nanoseconds.
		return int64(f * 1e9), nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, ts); err == nil {
		return parsed.UnixNano(), nil
	}
	if parsed, err := time.Parse(time.RFC3339, ts); err == nil {
		return parsed.UnixNano(), nil
	}
	return 0, fmt.Errorf("unrecognized timestamp format: %q", ts)
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
	if seconds, err := strconv.ParseFloat(step, 64); err == nil && seconds > 0 {
		return strconv.FormatFloat(seconds, 'f', -1, 64) + "s"
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
		switch endpoint {
		case "labels", "index_stats", "volume", "volume_range", "detected_labels", "detected_fields":
			key = "mt:" + p.canonicalReadCacheKey(endpoint, r.Header.Get("X-Scope-OrgID"), r)
		case "label_values":
			parts := strings.Split(r.URL.Path, "/")
			if len(parts) >= 7 {
				key = "mt:" + p.canonicalReadCacheKey(endpoint, r.Header.Get("X-Scope-OrgID"), r, parts[5])
			}
		case "detected_field_values":
			parts := strings.Split(r.URL.Path, "/")
			for i, part := range parts {
				if part == "detected_field" && i+1 < len(parts) {
					key = "mt:" + p.canonicalReadCacheKey(endpoint, r.Header.Get("X-Scope-OrgID"), r, parts[i+1])
					break
				}
			}
		case "patterns":
			key = "mt:" + endpoint + ":" + r.Header.Get("X-Scope-OrgID") + ":" + r.URL.Query().Encode()
		}
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
	if err := validateLogQLSyntax(query); err != "" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprint(w, err)
		p.metrics.RecordRequest(endpoint, http.StatusBadRequest, 0)
		return "", false
	}
	return query, true
}

// validateLogQLSyntax performs lightweight LogQL syntax validation to catch
// queries that Loki would reject with a parse error. Returns the error message
// (matching Loki's format) or empty string if valid.
//
// This catches:
// - Empty queries
// - Queries without a stream selector (e.g., "| json")
// - Malformed stream selectors (e.g., "{app=}")
// - Binary operations on log/pipeline expressions (e.g., "{app=...} == 2")
func validateLogQLSyntax(query string) string {
	query = strings.TrimSpace(query)

	// Empty query
	if query == "" {
		return "parse error : syntax error: unexpected $end"
	}

	// No stream selector — starts with pipeline
	if strings.HasPrefix(query, "|") {
		return "parse error at line 1, col 1: syntax error: unexpected |"
	}

	// Malformed selector: unclosed or empty values
	if strings.HasPrefix(query, "{") {
		braceDepth := 0
		selectorEnd := -1
		for i, ch := range query {
			if ch == '{' {
				braceDepth++
			} else if ch == '}' {
				braceDepth--
				if braceDepth == 0 {
					selectorEnd = i
					break
				}
			}
		}
		if selectorEnd < 0 {
			return "parse error at line 1, col 1: syntax error: unexpected end of input"
		}
		selector := query[:selectorEnd+1]
		// Check for empty values like {app=} or {app= }
		if emptyValueRe.MatchString(selector) {
			return "parse error at line 1, col " + strconv.Itoa(strings.Index(selector, "=}")+2) + ": syntax error: unexpected }, expecting STRING"
		}

		// Check for binary operations on log queries
		// Pattern: {selector} [| pipeline...] <binary_op> <number>
		// This is invalid unless wrapped in a metric function
		rest := strings.TrimSpace(query[selectorEnd+1:])
		if isBinaryOpOnLogQuery(rest) {
			op := extractBinaryOp(rest)
			exprType := "*syntax.MatchersExpr"
			if strings.Contains(rest, "|") {
				exprType = "*syntax.PipelineExpr"
			}
			return "parse error : unexpected type for left leg of binary operation (" + op + "): " + exprType
		}
	}

	return ""
}

var emptyValueRe = regexp.MustCompile(`[=!~]=?\s*[,}]`)

// binaryOpOnLogRe matches a binary operator followed by a number at the end
// of a pipeline expression (not inside a metric function).
var binaryOpOnLogRe = regexp.MustCompile(`(?:^|\s)\s*(==|!=|<=|>=|<|>|\+|-|\*|/|%|\^)\s*\d+\s*$`)

// labelFilterRe matches a standalone label filter stage: identifier <op> number.
var labelFilterRe = regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_.]*)\s*(?:==|!=|<=|>=|<|>)\s*\d+(\.\d+)?\s*$`)

// logParserKeywords is the set of LogQL parser stage keywords that cannot be field names
// in a label filter comparison (e.g. "| json == 2" is invalid; "| status >= 400" is valid).
var logParserKeywords = map[string]bool{
	"json": true, "logfmt": true, "unpack": true, "regexp": true,
	"pattern": true, "decolorize": true,
}

func isBinaryOpOnLogQuery(rest string) bool {
	if rest == "" {
		return false
	}
	if !binaryOpOnLogRe.MatchString(rest) {
		return false
	}
	// If the last pipe-separated segment is a label filter (field op number) where
	// the field is not a parser keyword, it's a valid stage — e.g. "| json | status >= 400".
	lastPipe := strings.LastIndex(rest, "|")
	lastSeg := strings.TrimSpace(rest)
	if lastPipe >= 0 {
		lastSeg = strings.TrimSpace(rest[lastPipe+1:])
	}
	if m := labelFilterRe.FindStringSubmatch(lastSeg); m != nil {
		if !logParserKeywords[m[1]] {
			return false
		}
	}
	return true
}

func extractBinaryOp(rest string) string {
	m := binaryOpOnLogRe.FindStringSubmatch(rest)
	if len(m) >= 2 {
		return m[1]
	}
	return "?"
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
	level := slog.LevelInfo
	switch {
	case code >= http.StatusInternalServerError:
		level = slog.LevelError
	case code >= http.StatusBadRequest:
		level = slog.LevelWarn
	}
	if p.log != nil && p.log.Enabled(context.Background(), level) {
		p.log.Log(context.Background(), level, "request error", "code", code, "error", msg)
	}
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
	if errors.Is(err, mw.ErrGuardRejected) {
		return http.StatusServiceUnavailable
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
	if strings.Contains(lower, "circuit breaker") {
		return http.StatusServiceUnavailable
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

// proxyControlledResponseHeaders is the set of response headers that the proxy
// sets via withSecurityHeaders. copyBackendHeaders must not overwrite them with
// backend values, otherwise the security posture set by the proxy is silently
// erased by whatever the backend returns.
var proxyControlledResponseHeaders = map[string]bool{
	"X-Content-Type-Options":       true,
	"X-Frame-Options":              true,
	"Cross-Origin-Resource-Policy": true,
	"Cache-Control":                true,
	"Pragma":                       true,
	"Expires":                      true,
}

// copyBackendHeaders copies backend response headers to dst while skipping
// headers in proxyControlledResponseHeaders that the proxy itself manages.
// Use this (instead of copyHeaders) when copying a backend response to the
// client so proxy-set security headers are not overwritten.
func copyBackendHeaders(dst, src http.Header) {
	for key, values := range src {
		if proxyControlledResponseHeaders[http.CanonicalHeaderKey(key)] {
			continue
		}
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

// isVLInternalField returns true for VictoriaLogs core internal field names
// that should never be exposed in Loki-compatible responses.
func isVLInternalField(name string) bool {
	return name == "_time" || name == "_msg" || name == "_stream" || name == "_stream_id"
}

// isVLNonLokiLabelField returns true for fields that VictoriaLogs exposes in
// its field_names endpoint but that should not appear in the Loki /labels API.
// This includes OTel semantic convention fields that VL stores as regular log
// fields — Loki never surfaces these as label names.
func isVLNonLokiLabelField(name string) bool {
	if isVLInternalField(name) {
		return true
	}
	// VL auto-derives this from log content; Loki never exposes it as a label.
	if name == "detected_level" {
		return true
	}
	return false
}

// ShouldFilterTranslatedLabel returns true if a label should be filtered from Loki-compatible
// responses. Only VL-internal fields and detected_level are filtered; user/system fields
// (including those with OTel-like naming patterns) are preserved. Declared fields are
// always kept even if they match filter criteria.
//
// This is exported for testing purposes to validate label filtering logic.
func (p *Proxy) ShouldFilterTranslatedLabel(name string) bool {
	// VL internal fields should be filtered
	if isVLNonLokiLabelField(name) {
		// But respect explicitly declared fields
		for _, declared := range p.declaredLabelFields {
			if declared == name {
				return false
			}
			if strings.Contains(declared, ".") && strings.ReplaceAll(declared, ".", "_") == name {
				return false
			}
		}
		return true
	}
	return false
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

// forwardedAuthFingerprint returns a short hash (16 hex chars) of the
// per-user auth context forwarded with a request (configured forward headers
// and cookies). Returns "" when no forwarding is configured, so callers can
// skip the extra allocation when the cache namespace is already user-agnostic.
func (p *Proxy) forwardedAuthFingerprint(r *http.Request) string {
	if len(p.forwardHeaders) == 0 && len(p.forwardCookies) == 0 {
		return ""
	}
	var b strings.Builder
	for _, hdr := range p.forwardHeaders {
		if val := r.Header.Get(hdr); val != "" {
			b.WriteString(hdr)
			b.WriteByte('=')
			b.WriteString(val)
			b.WriteByte(';')
		}
	}
	for _, cookie := range r.Cookies() {
		if p.forwardCookies["*"] || p.forwardCookies[cookie.Name] {
			b.WriteString("cookie:")
			b.WriteString(cookie.Name)
			b.WriteByte('=')
			b.WriteString(cookie.Value)
			b.WriteByte(';')
		}
	}
	if b.Len() == 0 {
		return ""
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])[:16]
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
	if sc.ResponseWriter.Header().Get("Content-Type") == "" {
		sc.ResponseWriter.Header().Set("Content-Type", "text/plain; charset=utf-8")
	}
	sc.ResponseWriter.Header().Set("X-Content-Type-Options", "nosniff")
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

func buildConcurrencyLimiter(limit int) chan struct{} {
	if limit <= 0 {
		return nil
	}
	return make(chan struct{}, limit)
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
		p.metrics.RecordUpstreamCallsPerRequestWithRoute(endpoint, route, telemetry.upstreamCalls)

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
		upstreamDurationByTypeMs := make(map[string]int64, len(telemetry.upstreamDurationByType))
		for key, value := range telemetry.upstreamDurationByType {
			upstreamDurationByTypeMs[key] = value.Milliseconds()
		}
		internalDurationByTypeMs := make(map[string]int64, len(telemetry.internalDurationByType))
		for key, value := range telemetry.internalDurationByType {
			internalDurationByTypeMs[key] = value.Milliseconds()
		}
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
		if len(telemetry.upstreamCallsByType) > 0 {
			logAttrs = append(logAttrs, "upstream.call_types", len(telemetry.upstreamCallsByType))
		}
		if len(upstreamDurationByTypeMs) > 0 {
			logAttrs = append(logAttrs, "upstream.duration_types", len(upstreamDurationByTypeMs))
		}
		if len(telemetry.internalOpsByType) > 0 {
			logAttrs = append(logAttrs, "proxy.operation_types", len(telemetry.internalOpsByType))
		}
		if len(internalDurationByTypeMs) > 0 {
			logAttrs = append(logAttrs, "proxy.operation_duration_types", len(internalDurationByTypeMs))
		}
		if p.log.Enabled(reqCtx, slog.LevelDebug) {
			if len(telemetry.upstreamCallsByType) > 0 {
				logAttrs = append(logAttrs, "upstream.calls_by_type", telemetry.upstreamCallsByType)
			}
			if len(upstreamDurationByTypeMs) > 0 {
				logAttrs = append(logAttrs, "upstream.duration_ms_by_type", upstreamDurationByTypeMs)
			}
			if len(telemetry.internalOpsByType) > 0 {
				logAttrs = append(logAttrs, "proxy.operations_by_type", telemetry.internalOpsByType)
			}
			if len(internalDurationByTypeMs) > 0 {
				logAttrs = append(logAttrs, "proxy.operation_duration_ms_by_type", internalDurationByTypeMs)
			}
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
	return p.translateQueryWithContext(context.Background(), logql)
}

func (p *Proxy) translateQueryWithContext(ctx context.Context, logql string) (string, error) {
	start := time.Now()
	normalized := strings.TrimSpace(logql)
	switch normalized {
	case "", "*", `"*"`, "`*`":
		p.observeInternalOperation(ctx, "translate_query", "passthrough", time.Since(start))
		return "*", nil
	}
	if p.translationCache != nil {
		if cached, ok := p.translationCache.Get(normalized); ok {
			p.observeInternalOperation(ctx, "translate_query", "cache_hit", time.Since(start))
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
		if p.metrics != nil {
			p.metrics.RecordTranslationError()
		}
		p.observeInternalOperation(ctx, "translate_query", "error", time.Since(start))
		return "", err
	}
	trimmed := strings.TrimSpace(translated)
	if strings.HasPrefix(trimmed, "|") {
		translated = "* " + trimmed
	}
	if p.translationCache != nil {
		p.translationCache.SetWithTTL(normalized, []byte(translated), 5*time.Minute)
	}
	if p.metrics != nil {
		p.metrics.RecordTranslation()
	}
	p.observeInternalOperation(ctx, "translate_query", "translated", time.Since(start))
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
	opStart := time.Now()
	if !hasParserStage(logql, "json") || !hasParserStage(logql, "logfmt") {
		p.observeInternalOperation(ctx, "prefer_working_parser", "bypass", time.Since(opStart))
		return logql
	}

	baseQuery := extractParserProbeQuery(logql)
	if baseQuery == "" {
		baseQuery = logql
	}
	baseQuery = defaultFieldDetectionQuery(baseQuery)
	logsqlQuery, err := p.translateQueryWithContext(ctx, baseQuery)
	if err != nil {
		p.observeInternalOperation(ctx, "prefer_working_parser", "probe_translate_error", time.Since(opStart))
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
		p.observeInternalOperation(ctx, "prefer_working_parser", "probe_upstream_error", time.Since(opStart))
		return logql
	}
	defer resp.Body.Close()

	body, err := readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
	if err != nil || len(body) == 0 {
		p.observeInternalOperation(ctx, "prefer_working_parser", "probe_empty", time.Since(opStart))
		return logql
	}

	jsonHits := 0
	logfmtHits := 0
	startIdx := 0
	for startIdx < len(body) {
		endIdx := startIdx
		for endIdx < len(body) && body[endIdx] != '\n' {
			endIdx++
		}
		line := strings.TrimSpace(string(body[startIdx:endIdx]))
		if endIdx < len(body) {
			startIdx = endIdx + 1
		} else {
			startIdx = len(body)
		}
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
		p.observeInternalOperation(ctx, "prefer_working_parser", "no_parser_signal", time.Since(opStart))
		return logql
	case jsonHits >= logfmtHits:
		p.observeInternalOperation(ctx, "prefer_working_parser", "prefer_json", time.Since(opStart))
		return removeParserStage(logql, "logfmt")
	default:
		p.observeInternalOperation(ctx, "prefer_working_parser", "prefer_logfmt", time.Since(opStart))
		return removeParserStage(logql, "json")
	}
}

var metricParserProbeRE = regexp.MustCompile(`(?s)(?:count_over_time|bytes_over_time|rate|bytes_rate|rate_counter|sum_over_time|avg_over_time|max_over_time|min_over_time|first_over_time|last_over_time|stddev_over_time|stdvar_over_time|quantile_over_time)\((.*?)\[[^][]+\]\)`)

var (
	absentOverTimeCompatRE         = regexp.MustCompile(`(?s)^\s*absent_over_time\(\s*(.*)\[([^][]+)\]\s*\)\s*$`)
	bareParserMetricCompatRE       = regexp.MustCompile(`(?s)^\s*(count_over_time|bytes_over_time|rate|bytes_rate|rate_counter|sum_over_time|avg_over_time|max_over_time|min_over_time|first_over_time|last_over_time|stddev_over_time|stdvar_over_time)\((.*)\[([^][]+)\]\)\s*$`)
	bareParserQuantileCompatRE     = regexp.MustCompile(`(?s)^\s*quantile_over_time\(\s*([0-9.]+)\s*,\s*(.*)\[([^][]+)\]\)\s*$`)
	bareParserUnwrapFieldRE        = regexp.MustCompile(`\|\s*unwrap\s+(?:(?:duration|bytes)\(([^)]+)\)|([A-Za-z0-9_.-]+))`)
	regexpExtractingParserStageRE  = regexp.MustCompile(`\|\s*regexp(?:\s+[^|]+)?`)
	patternExtractingParserStageRE = regexp.MustCompile(`\|\s*pattern(?:\s+[^|]+)?`)
	otherExtractingParserStageRE   = regexp.MustCompile(`\|\s*(?:unpack|extract|extract_regexp)(?:\s+[^|]+)?`)
)

func extractParserProbeQuery(logql string) string {
	unquote := func(s string) string {
		s = strings.TrimSpace(s)
		if len(s) < 2 {
			return s
		}
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			if unquoted, err := strconv.Unquote(s); err == nil {
				return strings.TrimSpace(unquoted)
			}
		}
		return s
	}

	matches := metricParserProbeRE.FindStringSubmatch(logql)
	if len(matches) == 2 {
		return unquote(matches[1])
	}
	return unquote(logql)
}

func hasExtractingParserStage(logql string) bool {
	for _, re := range []*regexp.Regexp{
		jsonParserStageRE,
		logfmtParserStageRE,
		regexpExtractingParserStageRE,
		patternExtractingParserStageRE,
		otherExtractingParserStageRE,
	} {
		if re.MatchString(logql) {
			return true
		}
	}
	return false
}

func parseBareParserMetricCompatSpec(logql string) (bareParserMetricCompatSpec, bool) {
	if matches := bareParserQuantileCompatRE.FindStringSubmatch(strings.TrimSpace(logql)); len(matches) == 4 {
		baseQuery := strings.TrimSpace(matches[2])
		if baseQuery == "" || !hasExtractingParserStage(baseQuery) {
			return bareParserMetricCompatSpec{}, false
		}
		windowRaw := strings.TrimSpace(matches[3])
		window, ok := parsePositiveStepDuration(windowRaw)
		if !ok && !isGrafanaRangeTemplateSelector(windowRaw) {
			return bareParserMetricCompatSpec{}, false
		}
		phi, err := strconv.ParseFloat(matches[1], 64)
		if err != nil || phi < 0 || phi > 1 {
			return bareParserMetricCompatSpec{}, false
		}
		unwrapField := extractBareParserUnwrapField(baseQuery)
		if unwrapField == "" {
			return bareParserMetricCompatSpec{}, false
		}
		return bareParserMetricCompatSpec{
			funcName:        "quantile_over_time",
			baseQuery:       baseQuery,
			rangeWindow:     window,
			rangeWindowExpr: windowRaw,
			unwrapField:     unwrapField,
			quantile:        phi,
		}, true
	}

	matches := bareParserMetricCompatRE.FindStringSubmatch(strings.TrimSpace(logql))
	if len(matches) != 4 {
		return bareParserMetricCompatSpec{}, false
	}
	baseQuery := strings.TrimSpace(matches[2])
	if baseQuery == "" || !hasExtractingParserStage(baseQuery) {
		return bareParserMetricCompatSpec{}, false
	}
	windowRaw := strings.TrimSpace(matches[3])
	window, ok := parsePositiveStepDuration(windowRaw)
	if !ok && !isGrafanaRangeTemplateSelector(windowRaw) {
		return bareParserMetricCompatSpec{}, false
	}
	unwrapField := ""
	switch matches[1] {
	case "rate_counter", "sum_over_time", "avg_over_time", "max_over_time", "min_over_time", "first_over_time", "last_over_time", "stddev_over_time", "stdvar_over_time":
		unwrapField = extractBareParserUnwrapField(baseQuery)
		if unwrapField == "" {
			return bareParserMetricCompatSpec{}, false
		}
	}
	return bareParserMetricCompatSpec{
		funcName:        matches[1],
		baseQuery:       baseQuery,
		rangeWindow:     window,
		rangeWindowExpr: windowRaw,
		unwrapField:     unwrapField,
	}, true
}

func isGrafanaRangeTemplateSelector(window string) bool {
	_, ok := canonicalGrafanaRangeToken(window)
	return ok
}

func resolveBareParserMetricRangeWindow(spec bareParserMetricCompatSpec, start, end, step string) (bareParserMetricCompatSpec, bool) {
	if spec.rangeWindow > 0 {
		return spec, true
	}
	if strings.TrimSpace(spec.rangeWindowExpr) == "" {
		return spec, false
	}
	duration, ok := resolveGrafanaTemplateTokenDuration(spec.rangeWindowExpr, start, end, step)
	if !ok || duration <= 0 {
		return spec, false
	}
	spec.rangeWindow = duration
	return spec, true
}

func extractBareParserUnwrapField(query string) string {
	matches := bareParserUnwrapFieldRE.FindStringSubmatch(query)
	if len(matches) != 3 {
		return ""
	}
	if field := strings.TrimSpace(matches[1]); field != "" {
		return field
	}
	return strings.TrimSpace(matches[2])
}

func formatMetricSampleValue(v float64) string {
	if math.IsNaN(v) {
		return "NaN"
	}
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	if math.IsInf(v, -1) {
		return "-Inf"
	}
	if math.Mod(v, 1) == 0 {
		return strconv.FormatInt(int64(v), 10)
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func metricWindowValue(funcName string, total float64, rangeWindow time.Duration) float64 {
	switch funcName {
	case "rate", "bytes_rate":
		if rangeWindow <= 0 {
			return 0
		}
		return total / rangeWindow.Seconds()
	default:
		return total
	}
}

func (p *Proxy) fetchBareParserMetricSeries(ctx context.Context, originalQuery string, spec bareParserMetricCompatSpec, start, end string) ([]bareParserMetricSeries, error) {
	logsqlQuery, err := p.translateQueryWithContext(ctx, spec.baseQuery)
	if err != nil {
		return nil, err
	}

	params := url.Values{}
	params.Set("query", logsqlQuery+" | sort by (_time)")
	if start != "" {
		params.Set("start", formatVLTimestamp(start))
	}
	if end != "" {
		params.Set("end", formatVLTimestamp(end))
	}

	resp, err := p.vlPost(ctx, "/select/logsql/query", params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = fmt.Sprintf("VL backend returned %d", resp.StatusCode)
		}
		return nil, &vlAPIError{status: resp.StatusCode, body: msg}
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	seriesByKey := make(map[string]*bareParserMetricSeries, 16)
	streamLabelCache := make(map[string]map[string]string, 16)
	streamDescriptorCache := make(map[string]cachedLogQueryStreamDescriptor, 16)
	exposureCache := make(map[string][]metadataFieldExposure, 16)

	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		entry := vlEntryPool.Get().(map[string]interface{})
		for key := range entry {
			delete(entry, key)
		}
		if err := json.Unmarshal(line, &entry); err != nil {
			vlEntryPool.Put(entry)
			continue
		}

		tsNanos, ok := parseFlexibleUnixNanos(asString(entry["_time"]))
		if !ok {
			vlEntryPool.Put(entry)
			continue
		}
		msg, _ := stringifyEntryValue(entry["_msg"])
		desc := p.logQueryStreamDescriptor(asString(entry["_stream"]), asString(entry["level"]), streamLabelCache, streamDescriptorCache)
		_, parsedFields := p.classifyEntryMetadataFields(entry, desc.rawLabels, true, exposureCache)
		metric := cloneStringMap(desc.translatedLabels)
		for key, value := range parsedFields {
			if spec.unwrapField != "" && key == spec.unwrapField {
				continue
			}
			metric[key] = value
		}
		seriesKey := canonicalLabelsKey(metric)
		series, ok := seriesByKey[seriesKey]
		if !ok {
			series = &bareParserMetricSeries{
				metric:  metric,
				samples: make([]bareParserMetricSample, 0, 8),
			}
			seriesByKey[seriesKey] = series
		}
		weight := 1.0
		if spec.unwrapField != "" {
			rawValue, ok := stringifyEntryValue(entry[spec.unwrapField])
			if !ok {
				vlEntryPool.Put(entry)
				continue
			}
			parsedValue, err := strconv.ParseFloat(rawValue, 64)
			if err != nil {
				vlEntryPool.Put(entry)
				continue
			}
			weight = parsedValue
		} else if spec.funcName == "bytes_over_time" || spec.funcName == "bytes_rate" {
			weight = float64(len(msg))
		}
		series.samples = append(series.samples, bareParserMetricSample{tsNanos: tsNanos, value: weight})
		vlEntryPool.Put(entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	result := make([]bareParserMetricSeries, 0, len(seriesByKey))
	for _, series := range seriesByKey {
		result = append(result, *series)
	}
	sort.Slice(result, func(i, j int) bool {
		return canonicalLabelsKey(result[i].metric) < canonicalLabelsKey(result[j].metric)
	})
	return result, nil
}

func bareParserMetricWindowValue(funcName string, window []bareParserMetricSample, spec bareParserMetricCompatSpec) float64 {
	if len(window) == 0 {
		return 0
	}
	switch funcName {
	case "count_over_time", "rate", "bytes_over_time", "bytes_rate", "sum_over_time":
		total := 0.0
		for _, sample := range window {
			total += sample.value
		}
		return metricWindowValue(funcName, total, spec.rangeWindow)
	case "rate_counter":
		if len(window) < 2 || spec.rangeWindow <= 0 {
			return 0
		}
		increase := 0.0
		prev := window[0].value
		for _, sample := range window[1:] {
			if sample.value >= prev {
				increase += sample.value - prev
			} else {
				// Counter reset: treat current value as the post-reset increase.
				increase += sample.value
			}
			prev = sample.value
		}
		return increase / spec.rangeWindow.Seconds()
	case "avg_over_time":
		total := 0.0
		for _, sample := range window {
			total += sample.value
		}
		return total / float64(len(window))
	case "max_over_time":
		maxValue := window[0].value
		for _, sample := range window[1:] {
			if sample.value > maxValue {
				maxValue = sample.value
			}
		}
		return maxValue
	case "min_over_time":
		minValue := window[0].value
		for _, sample := range window[1:] {
			if sample.value < minValue {
				minValue = sample.value
			}
		}
		return minValue
	case "first_over_time":
		return window[0].value
	case "last_over_time":
		return window[len(window)-1].value
	case "stddev_over_time", "stdvar_over_time":
		mean := 0.0
		for _, sample := range window {
			mean += sample.value
		}
		mean /= float64(len(window))
		variance := 0.0
		for _, sample := range window {
			delta := sample.value - mean
			variance += delta * delta
		}
		variance /= float64(len(window))
		if funcName == "stddev_over_time" {
			return math.Sqrt(variance)
		}
		return variance
	case "quantile_over_time":
		values := make([]float64, 0, len(window))
		for _, sample := range window {
			values = append(values, sample.value)
		}
		sort.Float64s(values)
		if len(values) == 1 {
			return values[0]
		}
		pos := spec.quantile * float64(len(values)-1)
		lower := int(math.Floor(pos))
		upper := int(math.Ceil(pos))
		if lower == upper {
			return values[lower]
		}
		weight := pos - float64(lower)
		return values[lower] + ((values[upper] - values[lower]) * weight)
	default:
		return 0
	}
}

func buildBareParserMetricMatrix(series []bareParserMetricSeries, startNanos, endNanos, stepNanos int64, spec bareParserMetricCompatSpec) map[string]interface{} {
	result := make([]lokiMatrixResult, 0, len(series))
	windowNanos := int64(spec.rangeWindow)
	for _, seriesItem := range series {
		values := make([][]interface{}, 0, int(((endNanos-startNanos)/stepNanos)+1))
		samples := seriesItem.samples
		left := 0
		right := 0
		for eval := startNanos; eval <= endNanos; eval += stepNanos {
			lower := eval - windowNanos
			for right < len(samples) && samples[right].tsNanos <= eval {
				right++
			}
			for left < right && samples[left].tsNanos < lower {
				left++
			}
			window := samples[left:right]
			values = append(values, []interface{}{float64(eval) / float64(time.Second), formatMetricSampleValue(bareParserMetricWindowValue(spec.funcName, window, spec))})
		}
		result = append(result, lokiMatrixResult{Metric: seriesItem.metric, Values: values})
	}
	return map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "matrix",
			"result":     result,
		},
	}
}

func buildBareParserMetricVector(series []bareParserMetricSeries, evalNanos int64, spec bareParserMetricCompatSpec) map[string]interface{} {
	result := make([]lokiVectorResult, 0, len(series))
	windowNanos := int64(spec.rangeWindow)
	for _, seriesItem := range series {
		lower := evalNanos - windowNanos
		window := make([]bareParserMetricSample, 0, len(seriesItem.samples))
		for _, sample := range seriesItem.samples {
			if sample.tsNanos >= lower && sample.tsNanos <= evalNanos {
				window = append(window, sample)
			}
		}
		result = append(result, lokiVectorResult{
			Metric: seriesItem.metric,
			Value:  []interface{}{float64(evalNanos) / float64(time.Second), formatMetricSampleValue(bareParserMetricWindowValue(spec.funcName, window, spec))},
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

type absentOverTimeCompatSpec struct {
	baseQuery   string
	rangeWindow time.Duration
}

func parseAbsentOverTimeCompatSpec(logql string) (absentOverTimeCompatSpec, bool) {
	matches := absentOverTimeCompatRE.FindStringSubmatch(strings.TrimSpace(logql))
	if len(matches) != 3 {
		return absentOverTimeCompatSpec{}, false
	}
	window, ok := parsePositiveStepDuration(matches[2])
	if !ok {
		return absentOverTimeCompatSpec{}, false
	}
	baseQuery := strings.TrimSpace(matches[1])
	if baseQuery == "" {
		return absentOverTimeCompatSpec{}, false
	}
	return absentOverTimeCompatSpec{baseQuery: baseQuery, rangeWindow: window}, true
}

func extractAbsentMetricLabels(query string) map[string]string {
	selector, _, ok := splitLeadingSelector(strings.TrimSpace(query))
	if !ok || len(selector) < 2 {
		return map[string]string{}
	}
	matchers := splitSelectorMatchers(selector[1 : len(selector)-1])
	labels := make(map[string]string, len(matchers))
	for _, matcher := range matchers {
		matcher = strings.TrimSpace(matcher)
		if strings.Contains(matcher, "!=") || strings.Contains(matcher, "=~") || strings.Contains(matcher, "!~") {
			continue
		}
		idx := strings.Index(matcher, "=")
		if idx <= 0 {
			continue
		}
		label := strings.TrimSpace(matcher[:idx])
		value := strings.TrimSpace(matcher[idx+1:])
		value = strings.Trim(value, "\"`")
		if label == "" || value == "" {
			continue
		}
		labels[label] = value
	}
	return labels
}

func statsResponseIsEmpty(body []byte) bool {
	var resp struct {
		Data struct {
			Result []lokiVectorResult `json:"result"`
		} `json:"data"`
		Results []lokiVectorResult `json:"results"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return false
	}
	results := resp.Results
	if len(results) == 0 {
		results = resp.Data.Result
	}
	if len(results) == 0 {
		return true
	}
	for _, item := range results {
		if len(item.Value) < 2 {
			continue
		}
		raw := fmt.Sprint(item.Value[1])
		raw = strings.Trim(raw, "\"")
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return false
		}
		if value != 0 {
			return false
		}
	}
	return true
}

func buildAbsentInstantVector(evalRaw string, metric map[string]string) map[string]interface{} {
	evalNs, ok := parseFlexibleUnixNanos(evalRaw)
	if !ok {
		evalNs = time.Now().UnixNano()
	}
	return map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "vector",
			"result": []lokiVectorResult{{
				Metric: metric,
				Value:  []interface{}{float64(evalNs) / float64(time.Second), "1"},
			}},
		},
	}
}

func (p *Proxy) proxyAbsentOverTimeQuery(w http.ResponseWriter, r *http.Request, start time.Time, originalQuery string, spec absentOverTimeCompatSpec) {
	logsqlQuery, err := p.translateQueryWithContext(r.Context(), originalQuery)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		p.metrics.RecordRequest("query", http.StatusBadRequest, time.Since(start))
		return
	}

	params := url.Values{}
	params.Set("query", logsqlQuery)
	if t := r.FormValue("time"); t != "" {
		params.Set("time", formatVLTimestamp(t))
	}

	resp, err := p.vlPost(r.Context(), "/select/logsql/stats_query", params)
	if err != nil {
		status := statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
		p.metrics.RecordRequest("query", status, time.Since(start))
		p.queryTracker.Record("query", originalQuery, time.Since(start), true)
		return
	}
	defer resp.Body.Close()

	body, _ := readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
	if resp.StatusCode >= http.StatusBadRequest {
		p.writeError(w, resp.StatusCode, string(body))
		p.metrics.RecordRequest("query", resp.StatusCode, time.Since(start))
		p.queryTracker.Record("query", originalQuery, time.Since(start), true)
		return
	}

	body = p.translateStatsResponseLabelsWithContext(r.Context(), body, originalQuery)
	var out []byte
	if statsResponseIsEmpty(body) {
		out, _ = json.Marshal(buildAbsentInstantVector(r.FormValue("time"), extractAbsentMetricLabels(spec.baseQuery)))
	} else {
		out = wrapAsLokiResponse([]byte(`{"result":[]}`), "vector")
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(out)
	elapsed := time.Since(start)
	p.metrics.RecordRequest("query", http.StatusOK, elapsed)
	p.queryTracker.Record("query", originalQuery, elapsed, false)
}

func (p *Proxy) proxyBareParserMetricQueryRange(w http.ResponseWriter, r *http.Request, start time.Time, originalQuery string, spec bareParserMetricCompatSpec) {
	startNanos, ok := parseFlexibleUnixNanos(r.FormValue("start"))
	if !ok {
		p.writeError(w, http.StatusBadRequest, "invalid start timestamp")
		p.metrics.RecordRequest("query_range", http.StatusBadRequest, time.Since(start))
		return
	}
	endNanos, ok := parseFlexibleUnixNanos(r.FormValue("end"))
	if !ok || endNanos < startNanos {
		p.writeError(w, http.StatusBadRequest, "invalid end timestamp")
		p.metrics.RecordRequest("query_range", http.StatusBadRequest, time.Since(start))
		return
	}
	stepDur, ok := parsePositiveStepDuration(r.FormValue("step"))
	if !ok {
		p.writeError(w, http.StatusBadRequest, "invalid step")
		p.metrics.RecordRequest("query_range", http.StatusBadRequest, time.Since(start))
		return
	}
	series, err := p.fetchBareParserMetricSeries(r.Context(), originalQuery, spec, r.FormValue("start"), r.FormValue("end"))
	if err != nil {
		status := statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
		p.metrics.RecordRequest("query_range", status, time.Since(start))
		p.queryTracker.Record("query_range", originalQuery, time.Since(start), true)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	marshalJSON(w, buildBareParserMetricMatrix(series, startNanos, endNanos, int64(stepDur), spec))
	elapsed := time.Since(start)
	p.metrics.RecordRequest("query_range", http.StatusOK, elapsed)
	p.queryTracker.Record("query_range", originalQuery, elapsed, false)
}

func (p *Proxy) proxyBareParserMetricQuery(w http.ResponseWriter, r *http.Request, start time.Time, originalQuery string, spec bareParserMetricCompatSpec) {
	evalNanos, ok := parseFlexibleUnixNanos(r.FormValue("time"))
	if !ok {
		evalNanos = time.Now().UnixNano()
	}
	startWindow := strconv.FormatInt(evalNanos-int64(spec.rangeWindow), 10)
	endWindow := strconv.FormatInt(evalNanos, 10)
	series, err := p.fetchBareParserMetricSeries(r.Context(), originalQuery, spec, startWindow, endWindow)
	if err != nil {
		status := statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
		p.metrics.RecordRequest("query", status, time.Since(start))
		p.queryTracker.Record("query", originalQuery, time.Since(start), true)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	marshalJSON(w, buildBareParserMetricVector(series, evalNanos, spec))
	elapsed := time.Since(start)
	p.metrics.RecordRequest("query", http.StatusOK, elapsed)
	p.queryTracker.Record("query", originalQuery, elapsed, false)
}

func (p *Proxy) translateStatsResponseLabelsWithContext(ctx context.Context, body []byte, originalQuery string) []byte {
	start := time.Now()
	var resp map[string]interface{}
	if err := json.Unmarshal(body, &resp); err != nil {
		p.observeInternalOperation(ctx, "translate_stats_response_labels", "decode_error", time.Since(start))
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
		p.observeInternalOperation(ctx, "translate_stats_response_labels", "no_results", time.Since(start))
		return body
	}

	// Allocate once and reuse across result entries to avoid per-result map
	// allocations (was the #3 hot path in pprof: ~81 GB cumulative).
	translated := make(map[string]interface{}, 8)
	syntheticLabels := make(map[string]string, 8)

	translatedMetrics := 0
	for _, r := range results {
		entry, ok := r.(map[string]interface{})
		if !ok {
			continue
		}
		// Translate "metric" labels map
		if metricRaw, ok := entry["metric"]; ok {
			if metric, ok := metricRaw.(map[string]interface{}); ok {
				// Clear reused maps instead of allocating new ones each iteration.
				for k := range translated {
					delete(translated, k)
				}
				changed := false
				for k, v := range metric {
					if k == "__name__" {
						changed = true
						continue
					}
					if k == "_stream" {
						if rawStream, ok := v.(string); ok {
							for streamKey, streamValue := range parseStreamLabels(rawStream) {
								lokiKey := streamKey
								if !p.labelTranslator.IsPassthrough() {
									lokiKey = p.labelTranslator.ToLoki(streamKey)
								}
								translated[lokiKey] = streamValue
							}
							changed = true
							continue
						}
					}
					lokiKey := k
					if !p.labelTranslator.IsPassthrough() {
						lokiKey = p.labelTranslator.ToLoki(k)
					}
					if lokiKey != k {
						changed = true
					}
					translated[lokiKey] = v
				}
				for k := range syntheticLabels {
					delete(syntheticLabels, k)
				}
				for key, value := range translated {
					if s, ok := value.(string); ok {
						syntheticLabels[key] = s
					}
				}
				serviceSignal := hasServiceSignal(syntheticLabels)
				beforeSyntheticCount := len(syntheticLabels)
				hadLevel := syntheticLabels["level"] != ""
				ensureDetectedLevel(syntheticLabels)
				// If detected_level was synthesized from level, remove the raw level label.
				// Metric aggregations like "sum by (detected_level)" translate to VL's
				// "sum by (level)" and back — the result should only carry detected_level,
				// matching Loki's behavior where both labels don't coexist in metric output.
				if hadLevel && syntheticLabels["detected_level"] != "" {
					delete(syntheticLabels, "level")
					delete(translated, "level")
				}
				ensureSyntheticServiceName(syntheticLabels)
				if !serviceSignal && strings.TrimSpace(syntheticLabels["service_name"]) == unknownServiceName {
					delete(syntheticLabels, "service_name")
				}
				if len(syntheticLabels) != beforeSyntheticCount {
					changed = true
				}
				for key, value := range syntheticLabels {
					if existing, ok := translated[key]; ok && existing == value {
						continue
					}
					translated[key] = value
					changed = true
				}
				if changed {
					translatedMetrics++
				}
				// entry["metric"] must point to a new map — the reused translated map
				// is mutated on the next iteration. Copy it for the JSON encoder.
				out := make(map[string]interface{}, len(translated))
				for k, v := range translated {
					out[k] = v
				}
				entry["metric"] = out
			}
		}
	}

	result, err := json.Marshal(resp)
	if err != nil {
		p.observeInternalOperation(ctx, "translate_stats_response_labels", "encode_error", time.Since(start))
		return body
	}
	outcome := "noop"
	if translatedMetrics > 0 {
		outcome = "translated"
	}
	p.observeInternalOperation(ctx, "translate_stats_response_labels", outcome, time.Since(start))
	return result
}
