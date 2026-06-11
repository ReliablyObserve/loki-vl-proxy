package proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
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
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
	mw "github.com/ReliablyObserve/Loki-VL-proxy/internal/middleware"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/observability"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
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
	BackendURL          string
	RulerBackendURL     string
	AlertsBackendURL    string
	Cache               *cache.Cache
	CompatCache         *cache.Cache
	LogLevel            string
	MaxConcurrent       int                      // max concurrent backend queries (0=unlimited)
	RatePerSecond       float64                  // per-client rate limit (0=unlimited)
	RateBurst           int                      // per-client burst size
	CBFailThreshold     int                      // circuit breaker: failures within window before opening
	CBOpenDuration      time.Duration            // circuit breaker: how long to stay open before probing
	CBWindowDuration    time.Duration            // circuit breaker: sliding window for failure counting (default 30s)
	CoalescerDisabled   bool                     // disable singleflight coalescing; every request makes its own backend call
	TenantMap           map[string]TenantMapping // string org ID → VL account/project
	TenantLabel         string                   // VL field name for label-based tenant routing (alternative to AccountID/ProjectID headers)
	AuthEnabled         bool
	RequireTenantHeader bool
	AllowGlobalTenant   bool
	ForwardTenantHeader bool // forward per-tenant X-Scope-OrgID to upstream (safe for VL, needed for Victoria Lakehouse)

	// Grafana datasource compatibility
	MaxLines int // default max lines per query (0=1000)
	// RangeMetricRowLimit caps the number of log rows fetched per collectRangeMetricSamples
	// call used by manual range-metric compatibility (rate, count_over_time, etc.).
	// 0 means use the built-in default of 1,000,000. Lower values bound memory at the cost
	// of potential result truncation for very high-cardinality queries.
	RangeMetricRowLimit int
	ForwardHeaders      []string          // HTTP headers to forward from client to VL backend
	ForwardCookies      []string          // Cookie names to forward from client to VL backend
	BackendHeaders      map[string]string // static headers to add to all VL requests
	BackendBasicAuth    string            // "user:password" for VL backend basic auth
	BackendCompression  string            // upstream HTTP compression preference: auto, gzip, zstd, none
	// ClientResponseCompression controls downstream client-facing response
	// compression policy used by the compatibility cache hit path.
	ClientResponseCompression string
	// ClientResponseCompressionMinBytes is the downstream compression threshold
	// before the proxy spends CPU compressing a response.
	ClientResponseCompressionMinBytes int
	BackendTimeout                    time.Duration // bounded timeout for non-streaming backend requests
	BackendTLSSkip                    bool          // skip TLS verification for VL backend
	// BackendReadBufferSize is the per-connection read buffer size for VL HTTP responses.
	// Default 65536 (64 KB) — larger buffers reduce syscall count for multi-KB responses.
	// Set to 0 to use the Go default (4096).
	BackendReadBufferSize int `yaml:"backend_read_buffer_size"`
	// BackendWriteBufferSize is the per-connection write buffer size for VL HTTP requests.
	// Default 65536 (64 KB).
	// Set to 0 to use the Go default (4096).
	BackendWriteBufferSize int `yaml:"backend_write_buffer_size"`
	// BackendMinVersion defines the minimum VictoriaLogs version considered
	// fully supported at startup compatibility check time.
	BackendMinVersion string
	// BackendAllowUnsupportedVersion allows startup to continue when detected
	// backend version is lower than BackendMinVersion. Use at your own risk.
	BackendAllowUnsupportedVersion bool
	// BackendVersionCheckTimeout bounds startup backend version checks.
	BackendVersionCheckTimeout time.Duration
	// BackendVersionStrict, when true, promotes /health unreachability, non-2xx
	// responses, and missing/sub-min backend semver to a hard startup error
	// from ValidateBackendVersionCompatibility. Default false preserves the
	// historical soft (warn-and-continue) behavior.
	BackendVersionStrict bool
	DerivedFields        []DerivedField // derived fields for trace/link extraction
	StreamResponse       bool           // stream responses via chunked transfer (default: false)
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

	// Label cache
	// LabelCacheTTL overrides the default TTL for /labels and /label/{name}/values
	// cache entries. Defaults to 5 minutes when unset. Keep-warm interval and skip
	// threshold are derived automatically from this value.
	LabelCacheTTL time.Duration
	// WarmupMaxJitter is the upper bound of the random delay added before the
	// startup label cache warmup begins. Spreading the delay across a fleet of
	// proxies (all restarting at once) prevents a thundering-herd of concurrent
	// wide-range stream_field_names queries hitting VL simultaneously.
	// Default 0 (no jitter). Recommended: 5–15s for fleets of ≥3 instances.
	WarmupMaxJitter time.Duration

	// Label translation
	LabelStyle        LabelStyle        // how to translate VL field names to Loki labels
	MetadataFieldMode MetadataFieldMode // how to expose non-label VL fields through field-oriented APIs
	FieldMappings     []FieldMapping    // custom VL↔Loki field name mappings
	TranslateOTel     *bool

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
	// PeerInsecureIPAllowlist, when true, falls back to source-IP membership
	// check on /_cache/* endpoints if no token is provided. Default false:
	// requests without a valid X-Peer-Token are rejected with 401. Operators
	// who want the legacy IP-only behavior must opt in explicitly via
	// --peer-insecure-ip-allowlist=true.
	PeerInsecureIPAllowlist bool

	// Cold storage backend (Victoria Lakehouse)
	ColdBackend ColdBackendConfig

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

	// LogRequestSampleRate controls per-request access log sampling for successful (2xx)
	// requests. 0 or 1 logs every request (default). N > 1 logs 1 in every N requests,
	// skipping the expensive log-attribute assembly for the rest.
	// 4xx/5xx requests are always logged regardless of this setting.
	LogRequestSampleRate int

	LogStatsInterval time.Duration
	LogRateThreshold int

	// Tenant limits runtime exposure.
	// TenantLimitsAllowPublish controls which fields are exposed by
	// /config/tenant/v1/limits and /loki/api/v1/drilldown-limits.
	// If empty, default Loki-compatible allowlist is used.
	TenantLimitsAllowPublish []string
	// TenantDefaultLimits applies global published limits overrides.
	TenantDefaultLimits map[string]any
	// TenantLimits applies per-tenant published limits overrides keyed by X-Scope-OrgID.
	TenantLimits map[string]map[string]any
	// DefaultMaxQueryLength is the default maximum allowed query time range enforced
	// for all tenants unless overridden by per-tenant limits. 0 means unlimited.
	DefaultMaxQueryLength time.Duration
	// MaxStatsQuerySeries caps the number of series returned by stats_query_range
	// (count_over_time, rate, bytes_rate with explicit by()). Matches Loki's
	// max_query_series behaviour. 0 means use the built-in default (500).
	MaxStatsQuerySeries int
	// StatsQueryRangeConcurrency limits the number of concurrent
	// stats_query_range calls the proxy makes to VL. Each Drilldown Fields
	// page load fires ~30 such calls simultaneously; without a cap they all
	// hit VL at once, causing a CPU storm. 0 means use the built-in default (4).
	StatsQueryRangeConcurrency int
	// DrilldownBurstWindowMs is the time window in milliseconds during which
	// concurrent per-field count_over_time queries from Grafana Drilldown Fields
	// are coalesced into a single fused VL conditional-stats call.
	// 0 disables the burst coalescer. Default when enabled: 50.
	DrilldownBurstWindowMs int
	// DrilldownBurstMaxFields caps the number of fields per coalesced VL call.
	// Excess fields form a second call. Default: 30.
	DrilldownBurstMaxFields int
	// DrilldownFieldBatchWindowMs is the accumulation window in milliseconds for
	// the multi-field stats batcher. Concurrent per-field stats_query_range calls
	// arriving within this window are folded into one multi-field VL query whose
	// result is marginalized back into per-field responses. 0 disables batching.
	// Default when enabled: 25.
	DrilldownFieldBatchWindowMs int
	// DrilldownFieldBatchMaxFields caps fields per batched VL call. When the batch
	// window closes with more than this many fields, the excess form a second batch.
	// Default: 6.
	DrilldownFieldBatchMaxFields int
	// StatsQueryRangeInterQueryDelayMs is the minimum pause in milliseconds between
	// consecutive VL stats_query_range calls on the individual (non-batched) path.
	// After each call the semaphore slot is held for this duration before release,
	// spreading the query burst over time and giving VL CPU headroom. 0 disables.
	// Default: 200.
	StatsQueryRangeInterQueryDelayMs int

	// DebugLogRawQueries, when true, disables redaction of LogQL/LogsQL and
	// backend query params in debug-level logs. Intended for local development
	// only. Default is false (redacted).
	DebugLogRawQueries bool

	// MetadataDefaultLookback is the default time window applied to /labels,
	// /label/{name}/values, and /series when the client omits both start and
	// end. 0 disables (unbounded scan, prior behavior).
	MetadataDefaultLookback time.Duration

	// DrilldownScanTimeout caps the time a single detected_fields /
	// detected_field_values request will spend in the slow log-scan
	// fallback path (parser pipeline + high-cardinality field). 0 disables.
	DrilldownScanTimeout time.Duration
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

// CacheTTLs defines per-endpoint cache TTLs. "labels" and "label_values" default
// to 5 minutes but can be overridden per-Proxy via Config.LabelCacheTTL;
// read p.cacheTTLLabels / p.cacheTTLLabelValues directly for those two keys.
var CacheTTLs = map[string]time.Duration{
	"labels":          5 * time.Minute,
	"label_values":    5 * time.Minute,
	"label_inventory": 5 * time.Minute,
	"series":          30 * time.Second,
	// Drilldown field/label inventory TTL. Kept at 90s to honor the
	// freshness contract asserted by TestDrilldown_GrafanaResourceContracts/
	// parsed_only_fields_refresh_after_new_logs_arrive (new fields must surface
	// within ~120s of ingest). A longer TTL (tried 5m) starves that refresh —
	// background refresh fires at half-TTL, so 5m delays a new field to ~2.5m,
	// past the contract window. The real Drilldown speedups on this branch come
	// from the column-indexed level filter + window-sampled /hits, not this TTL.
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
	tenantLabel                           string
	authEnabled                           bool
	requireTenantHeader                   bool
	allowGlobalTenant                     bool
	forwardTenantHeader                   bool
	maxLines                              int
	forwardHeaders                        []string          // headers to copy from client request to VL
	forwardCookies                        map[string]bool   // cookie names to copy from client request to VL
	backendHeaders                        map[string]string // static headers on all VL requests
	backendCompression                    string
	backendLoopback                       bool // true when backend host resolves to a loopback address
	clientResponseCompression             string
	clientResponseCompressionMinBytes     int
	backendMinVersion                     string
	backendAllowUnsupportedVersion        bool
	backendVersionCheckTimeout            time.Duration
	backendVersionStrict                  bool
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
	peerInsecureIPAllowlist               bool // gate the legacy IP-allowlist fallback (default false: token required)
	coldRouter                            *ColdRouter
	registerInstrumentation               bool
	enablePprof                           bool
	enableQueryAnalytics                  bool
	adminAuthToken                        string
	metricsConcurrencyLimiter             chan struct{}
	rangeMetricRowLimit                   int           // max rows fetched per collectRangeMetricSamples call (0=1_000_000)
	maxStatsQuerySeries                   int           // max series returned by collectRangeMetricHits (0=5000)
	statsQueryRangeSem                    chan struct{} // limits concurrent VL stats_query_range calls (nil=unlimited)
	statsQueryRangeInterQueryDelay        time.Duration // min pause between consecutive individual VL stats calls
	drilldownCoalescer                    *DrilldownBurstCoalescer
	drilldownFieldBatcher                 *drilldownFieldBatcher
	drilldownCardCache                    *drilldownCardinalityCache
	tailAllowedOrigins                    map[string]struct{}
	tailMode                              TailMode
	metricsTrustProxyHeaders              bool
	tenantLimitsAllowPublish              []string
	tenantDefaultLimits                   map[string]any
	tenantLimits                          map[string]map[string]any
	defaultMaxQueryLength                 time.Duration // 0 = unlimited
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
	warmupMaxJitter                       time.Duration
	labelRefreshGroup                     singleflight.Group
	parserProbeGroup                      singleflight.Group
	translationGroup                      singleflight.Group
	streamFieldNamesCache                 *cache.Cache // short-lived internal cache for stream_field_names routing decisions
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
	backendSupportsColumnFieldValues      bool
	backendVersionLogged                  bool
	labelValuesIndexWarmReady             atomic.Bool
	labelValuesIndexPersistStarted        atomic.Bool
	labelValuesIndexPersistDirty          atomic.Bool
	labelValuesIndexPersistStop           chan struct{}
	labelValuesIndexPersistDone           chan struct{}
	keepWarmStop                          chan struct{}
	labelValuesIndexMu                    sync.RWMutex
	labelValuesIndex                      map[string]*labelValuesIndexState
	labelValuesIndexPersistDigest         [sha256.Size]byte
	labelValuesIndexPersistDigestReady    bool
	readCacheKeyMemoMu                    sync.RWMutex
	readCacheKeyMemo                      map[canonicalReadCacheMemoKey]string
	logSampleN                            uint64 // 0 = log all; N>1 = log 1 in N successful requests
	logSampleCount                        atomic.Uint64
	requestSampler                        *observability.RequestSampler
	cacheTTLLabels                        time.Duration // per-instance TTL for labels endpoint (from Config.LabelCacheTTL)
	cacheTTLLabelValues                   time.Duration // per-instance TTL for label_values endpoint
	debugLogRawQueries                    bool          // when true, debug logs include raw LogQL/LogsQL and backend params
	metadataDefaultLookback               time.Duration // default lookback for /labels, /label/{name}/values, /series when client omits start+end; 0 disables
	// drilldownScanTimeout caps the time a single detected_fields /
	// detected_field_values request will spend scanning logs with a parser
	// filter. VL has no internal timeout, so without this cap a Drilldown
	// request fanned across 20+ panels can each block for 15s+ on slow
	// combos (parser pipeline + high-cardinality field), freezing the UI.
	// On timeout the proxy returns an empty result, which (thanks to the
	// empty-cache guard) is not persisted — the next request retries with
	// a fresh budget.
	drilldownScanTimeout time.Duration
	// handler is the decomposed view of this Proxy's deps + config + state.
	// Populated alongside the existing fields during the Task 9 migration.
	handler *Handler
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

//nolint:gocyclo // constructor wires many optional config knobs (URLs, levels, multi-tenant, caches, ruler/alerts, telemetry); complexity is inherent to top-level assembly.
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
	transport.DisableCompression = true // proxy owns Accept-Encoding via applyBackendHeaders
	if cfg.BackendTLSSkip {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // #nosec G402 -- intentional, opt-in via BackendTLSSkip; nosemgrep: problem-based-packs.insecure-transport.go-stdlib.bypass-tls-verification,go.lang.security.audit.crypto.missing-ssl-minversion.missing-ssl-minversion
	}
	readBuf := cfg.BackendReadBufferSize
	if readBuf <= 0 {
		readBuf = 64 * 1024
	}
	writeBuf := cfg.BackendWriteBufferSize
	if writeBuf <= 0 {
		writeBuf = 64 * 1024
	}
	transport.ReadBufferSize = readBuf
	transport.WriteBufferSize = writeBuf
	tailTransport := transport.Clone()
	// Use a short ResponseHeaderTimeout so that openNativeTailStream fails fast when
	// VL's tail endpoint is unavailable or slow to send headers (triggering synthetic
	// fallback). Unlike a request-context timeout, ResponseHeaderTimeout only guards
	// the header phase; subsequent body reads (NDJSON streaming) are not affected, so
	// the forwarded WebSocket session can live as long as the client remains connected.
	tailTransport.ResponseHeaderTimeout = 5 * time.Second

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
	warmupMaxJitter := cfg.WarmupMaxJitter
	if warmupMaxJitter < 0 {
		warmupMaxJitter = 0
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

	labelCacheTTL := CacheTTLs["labels"]
	if cfg.LabelCacheTTL > 0 {
		labelCacheTTL = cfg.LabelCacheTTL
	}

	labelTranslator := NewLabelTranslator(cfg.LabelStyle, cfg.FieldMappings)
	if cfg.TranslateOTel != nil {
		labelTranslator.SetTranslateOTel(*cfg.TranslateOTel)
	}
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

	coldRouter, err := NewColdRouter(cfg.ColdBackend, logger)
	if err != nil {
		return nil, fmt.Errorf("cold backend: %w", err)
	}

	p := &Proxy{
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
		tenantLabel:                           cfg.TenantLabel,
		authEnabled:                           cfg.AuthEnabled,
		requireTenantHeader:                   cfg.RequireTenantHeader,
		allowGlobalTenant:                     cfg.AllowGlobalTenant,
		forwardTenantHeader:                   cfg.ForwardTenantHeader,
		maxLines:                              maxLines,
		rangeMetricRowLimit:                   cfg.RangeMetricRowLimit,
		maxStatsQuerySeries:                   cfg.MaxStatsQuerySeries,
		statsQueryRangeSem:                    makeStatsQueryRangeSem(cfg.StatsQueryRangeConcurrency),
		statsQueryRangeInterQueryDelay:        time.Duration(cfg.StatsQueryRangeInterQueryDelayMs) * time.Millisecond,
		drilldownCoalescer:                    makeDrilldownBurstCoalescer(cfg.DrilldownBurstWindowMs, cfg.DrilldownBurstMaxFields),
		drilldownCardCache:                    newDrilldownCardinalityCache(),
		forwardHeaders:                        cfg.ForwardHeaders,
		forwardCookies:                        forwardCookies,
		backendHeaders:                        backendHeaders,
		backendCompression:                    normalizeBackendCompression(cfg.BackendCompression),
		backendLoopback:                       computeBackendLoopback(u),
		clientResponseCompression:             cfg.ClientResponseCompression,
		clientResponseCompressionMinBytes:     cfg.ClientResponseCompressionMinBytes,
		backendMinVersion:                     backendMinVersion,
		backendAllowUnsupportedVersion:        cfg.BackendAllowUnsupportedVersion,
		backendVersionCheckTimeout:            backendVersionCheckTimeout,
		backendVersionStrict:                  cfg.BackendVersionStrict,
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
		peerInsecureIPAllowlist:               cfg.PeerInsecureIPAllowlist,
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
		defaultMaxQueryLength:                 cfg.DefaultMaxQueryLength,
		translationCache:                      cache.New(5*time.Minute, 5000),
		streamFieldNamesCache:                 cache.New(30*time.Second, 500),
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
		warmupMaxJitter:                       warmupMaxJitter,
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
		keepWarmStop:                          make(chan struct{}),
		labelValuesIndex:                      make(map[string]*labelValuesIndexState),
		readCacheKeyMemo:                      make(map[canonicalReadCacheMemoKey]string, 2048),
		coldRouter:                            coldRouter,
		cacheTTLLabels:                        labelCacheTTL,
		cacheTTLLabelValues:                   labelCacheTTL,
		debugLogRawQueries:                    cfg.DebugLogRawQueries,
		metadataDefaultLookback:               cfg.MetadataDefaultLookback,
		drilldownScanTimeout:                  cfg.DrilldownScanTimeout,
	}
	if cfg.DrilldownFieldBatchWindowMs > 0 {
		maxFields := cfg.DrilldownFieldBatchMaxFields
		if maxFields <= 0 {
			maxFields = 6
		}
		p.drilldownFieldBatcher = newDrilldownFieldBatcher(p, time.Duration(cfg.DrilldownFieldBatchWindowMs)*time.Millisecond, maxFields)
	}
	if cfg.LogRequestSampleRate > 1 {
		p.logSampleN = uint64(cfg.LogRequestSampleRate)
	}
	p.requestSampler = observability.NewRequestSampler()
	if cfg.LogStatsInterval > 0 {
		p.requestSampler.DigestInterval = cfg.LogStatsInterval
	}
	if cfg.LogRateThreshold > 0 {
		p.requestSampler.QuietThreshold = int64(cfg.LogRateThreshold)
	}

	// Wire the decomposed Handler: copy deps (shared pointers), immutable config, and
	// initialise a fresh State.  The Proxy struct still holds the live mutable fields;
	// State on the Handler is the canonical home for those fields once receiver
	// migration completes in a follow-on PR.
	p.handler = &Handler{
		Deps: Deps{
			backend:               p.backend,
			rulerBackend:          p.rulerBackend,
			alertsBackend:         p.alertsBackend,
			client:                p.client,
			tailClient:            p.tailClient,
			cache:                 p.cache,
			compatCache:           p.compatCache,
			translationCache:      p.translationCache,
			streamFieldNamesCache: p.streamFieldNamesCache,
			peerCache:             p.peerCache,
			log:                   p.log,
			metrics:               p.metrics,
			queryTracker:          p.queryTracker,
			coalescer:             p.coalescer,
			limiter:               p.limiter,
			breaker:               p.breaker,
		},
		Cfg: &HandlerConfig{
			tenantLabel:                           p.tenantLabel,
			authEnabled:                           p.authEnabled,
			requireTenantHeader:                   p.requireTenantHeader,
			allowGlobalTenant:                     p.allowGlobalTenant,
			forwardTenantHeader:                   p.forwardTenantHeader,
			maxLines:                              p.maxLines,
			forwardHeaders:                        p.forwardHeaders,
			forwardCookies:                        p.forwardCookies,
			backendHeaders:                        p.backendHeaders,
			backendCompression:                    p.backendCompression,
			backendLoopback:                       p.backendLoopback,
			clientResponseCompression:             p.clientResponseCompression,
			clientResponseCompressionMinBytes:     p.clientResponseCompressionMinBytes,
			backendMinVersion:                     p.backendMinVersion,
			backendAllowUnsupportedVersion:        p.backendAllowUnsupportedVersion,
			backendVersionCheckTimeout:            p.backendVersionCheckTimeout,
			derivedFields:                         p.derivedFields,
			streamResponse:                        p.streamResponse,
			emitStructuredMetadata:                p.emitStructuredMetadata,
			patternsEnabled:                       p.patternsEnabled,
			patternsAutodetectFromQueries:         p.patternsAutodetectFromQueries,
			patternsCustom:                        p.patternsCustom,
			metadataFieldMode:                     p.metadataFieldMode,
			streamFieldsMap:                       p.streamFieldsMap,
			declaredLabelFields:                   p.declaredLabelFields,
			registerInstrumentation:               p.registerInstrumentation,
			enablePprof:                           p.enablePprof,
			enableQueryAnalytics:                  p.enableQueryAnalytics,
			adminAuthToken:                        p.adminAuthToken,
			rangeMetricRowLimit:                   p.rangeMetricRowLimit,
			tailAllowedOrigins:                    p.tailAllowedOrigins,
			tailMode:                              p.tailMode,
			metricsTrustProxyHeaders:              p.metricsTrustProxyHeaders,
			tenantLimitsAllowPublish:              p.tenantLimitsAllowPublish,
			tenantDefaultLimits:                   p.tenantDefaultLimits,
			tenantLimits:                          p.tenantLimits,
			defaultMaxQueryLength:                 p.defaultMaxQueryLength,
			queryRangeWindowing:                   p.queryRangeWindowing,
			queryRangeSplitInterval:               p.queryRangeSplitInterval,
			queryRangeMaxParallel:                 p.queryRangeMaxParallel,
			queryRangeAdaptiveParallel:            p.queryRangeAdaptiveParallel,
			queryRangeParallelMin:                 p.queryRangeParallelMin,
			queryRangeParallelMax:                 p.queryRangeParallelMax,
			queryRangeLatencyTarget:               p.queryRangeLatencyTarget,
			queryRangeLatencyBackoff:              p.queryRangeLatencyBackoff,
			queryRangeAdaptiveCooldown:            p.queryRangeAdaptiveCooldown,
			queryRangeErrorBackoffThreshold:       p.queryRangeErrorBackoffThreshold,
			queryRangeFreshness:                   p.queryRangeFreshness,
			queryRangeRecentCacheTTL:              p.queryRangeRecentCacheTTL,
			queryRangeHistoryCacheTTL:             p.queryRangeHistoryCacheTTL,
			queryRangePrefilterIndexStats:         p.queryRangePrefilterIndexStats,
			queryRangePrefilterMinWindows:         p.queryRangePrefilterMinWindows,
			queryRangeStreamAwareBatching:         p.queryRangeStreamAwareBatching,
			queryRangeExpensiveWindowHitThreshold: p.queryRangeExpensiveWindowHitThreshold,
			queryRangeExpensiveWindowMaxParallel:  p.queryRangeExpensiveWindowMaxParallel,
			queryRangeAlignWindows:                p.queryRangeAlignWindows,
			queryRangeWindowTimeout:               p.queryRangeWindowTimeout,
			queryRangePartialResponses:            p.queryRangePartialResponses,
			queryRangeBackgroundWarm:              p.queryRangeBackgroundWarm,
			queryRangeBackgroundWarmMaxWindows:    p.queryRangeBackgroundWarmMaxWindows,
			recentTailRefreshEnabled:              p.recentTailRefreshEnabled,
			recentTailRefreshWindow:               p.recentTailRefreshWindow,
			recentTailRefreshMaxStaleness:         p.recentTailRefreshMaxStaleness,
			warmupMaxJitter:                       p.warmupMaxJitter,
			labelValuesIndexedCache:               p.labelValuesIndexedCache,
			labelValuesHotLimit:                   p.labelValuesHotLimit,
			labelValuesIndexMaxEntries:            p.labelValuesIndexMaxEntries,
			labelValuesIndexPersistPath:           p.labelValuesIndexPersistPath,
			labelValuesIndexPersistInterval:       p.labelValuesIndexPersistInterval,
			labelValuesIndexStartupStale:          p.labelValuesIndexStartupStale,
			labelValuesIndexPeerWarmTimeout:       p.labelValuesIndexPeerWarmTimeout,
			patternsPersistPath:                   p.patternsPersistPath,
			patternsPersistInterval:               p.patternsPersistInterval,
			patternsStartupStale:                  p.patternsStartupStale,
			patternsPeerWarmTimeout:               p.patternsPeerWarmTimeout,
			peerAuthToken:                         p.peerAuthToken,
			cacheTTLLabels:                        p.cacheTTLLabels,
			cacheTTLLabelValues:                   p.cacheTTLLabelValues,
			logSampleN:                            p.logSampleN,
		},
		// State shares the exact same mutex instances and map/channel references as
		// Proxy so that Handler.State and Proxy never diverge.  Mutex fields are held
		// by pointer (required: sync.Mutex/sync.RWMutex must not be copied); atomic
		// fields are similarly held by pointer.  Map and channel fields are reference
		// types in Go and are shared directly.
		State: &State{
			configMu:                             &p.configMu,
			tenantMap:                            p.tenantMap,
			labelTranslator:                      p.labelTranslator,
			queryRangeAdaptiveMu:                 &p.queryRangeAdaptiveMu,
			queryRangeParallelCurrent:            &p.queryRangeParallelCurrent,
			queryRangeLatencyEWMA:                &p.queryRangeLatencyEWMA,
			queryRangeErrorEWMA:                  &p.queryRangeErrorEWMA,
			queryRangeAdaptiveLastAdjust:         &p.queryRangeAdaptiveLastAdjust,
			patternsSnapshotMu:                   &p.patternsSnapshotMu,
			patternsSnapshotEntries:              p.patternsSnapshotEntries,
			patternsSnapshotPatternCount:         &p.patternsSnapshotPatternCount,
			patternsSnapshotPayloadBytes:         &p.patternsSnapshotPayloadBytes,
			patternsPersistDigest:                &p.patternsPersistDigest,
			patternsPersistDigestReady:           &p.patternsPersistDigestReady,
			patternsWarmReady:                    &p.patternsWarmReady,
			patternsPersistStarted:               &p.patternsPersistStarted,
			patternsPersistDirty:                 &p.patternsPersistDirty,
			patternsPersistStop:                  p.patternsPersistStop,
			patternsPersistDone:                  p.patternsPersistDone,
			backendVersionMu:                     &p.backendVersionMu,
			backendVersionRaw:                    &p.backendVersionRaw,
			backendVersionSemver:                 &p.backendVersionSemver,
			backendCapabilityProfile:             &p.backendCapabilityProfile,
			backendSupportsStreamMetadata:        &p.backendSupportsStreamMetadata,
			backendSupportsDensePatternWindowing: &p.backendSupportsDensePatternWindowing,
			backendSupportsMetadataSubstring:     &p.backendSupportsMetadataSubstring,
			backendVersionLogged:                 &p.backendVersionLogged,
			labelValuesIndexMu:                   &p.labelValuesIndexMu,
			labelValuesIndex:                     p.labelValuesIndex,
			labelValuesIndexPersistDigest:        &p.labelValuesIndexPersistDigest,
			labelValuesIndexPersistDigestReady:   &p.labelValuesIndexPersistDigestReady,
			labelValuesIndexWarmReady:            &p.labelValuesIndexWarmReady,
			labelValuesIndexPersistStarted:       &p.labelValuesIndexPersistStarted,
			labelValuesIndexPersistDirty:         &p.labelValuesIndexPersistDirty,
			labelValuesIndexPersistStop:          p.labelValuesIndexPersistStop,
			labelValuesIndexPersistDone:          p.labelValuesIndexPersistDone,
			keepWarmStop:                         p.keepWarmStop,
			readCacheKeyMemoMu:                   &p.readCacheKeyMemoMu,
			readCacheKeyMemo:                     p.readCacheKeyMemo,
			labelRefreshGroup:                    &p.labelRefreshGroup,
			logSampleCount:                       &p.logSampleCount,
			metricsConcurrencyLimiter:            p.metricsConcurrencyLimiter,
			coldRouter:                           p.coldRouter,
		},
	}

	return p, nil
}

// computeBackendLoopback returns true when u's host is a loopback address or
// the hostname "localhost". The result is computed once at startup and cached in
// Proxy.backendLoopback so that applyBackendHeaders never parses URLs per-request.
func makeStatsQueryRangeSem(concurrency int) chan struct{} {
	const defaultConcurrency = 4
	if concurrency <= 0 {
		concurrency = defaultConcurrency
	}
	sem := make(chan struct{}, concurrency)
	for range concurrency {
		sem <- struct{}{}
	}
	return sem
}

// makeDrilldownBurstCoalescer creates a DrilldownBurstCoalescer if windowMs > 0.
// Returns nil when windowMs == 0 (disabled); callers must nil-check before use.
func makeDrilldownBurstCoalescer(windowMs, maxFields int) *DrilldownBurstCoalescer {
	if windowMs == 0 {
		return nil
	}
	return newDrilldownBurstCoalescer(windowMs, maxFields)
}

func computeBackendLoopback(u *url.URL) bool {
	if u == nil {
		return false
	}
	host := u.Hostname() // strips port
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// isBackendLoopback reports whether the primary VictoriaLogs backend is
// co-located (loopback address). Used by applyBackendHeaders to pick the
// best default Accept-Encoding when backendCompression is "auto".
func (p *Proxy) isBackendLoopback() bool {
	return p.backendLoopback
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
	p.warmMetadataCacheOnStartup()
	p.startLabelCacheKeepWarmLoop()
	if p.coldRouter != nil {
		p.coldRouter.Start(context.Background())
		p.log.Info("cold storage routing enabled",
			"backend", p.coldRouter.coldBackend.String(),
			"boundary", p.coldRouter.boundary,
		)
	}
}

// ColdRouter returns the cold storage router for testing.
func (p *Proxy) ColdRouter() *ColdRouter { return p.coldRouter }

// Shutdown flushes in-memory caches that should survive rolling restarts.
func (p *Proxy) Shutdown(ctx context.Context) error {
	if p.keepWarmStop != nil {
		select {
		case <-p.keepWarmStop:
		default:
			close(p.keepWarmStop)
		}
	}
	if p.limiter != nil {
		p.limiter.Stop()
	}
	if p.peerCache != nil {
		p.peerCache.Close()
	}
	if p.coldRouter != nil {
		p.coldRouter.Stop(ctx)
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
	prevTranslateOTel := p.labelTranslator.translateOTel
	p.labelTranslator = NewLabelTranslator(p.labelTranslator.style, mappings)
	p.labelTranslator.SetTranslateOTel(prevTranslateOTel)
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

// routeHandler wraps h with the standard per-route middleware chain:
// security headers → tenant auth → rate limiter → request logger → compat cache.
func (p *Proxy) routeHandler(endpoint, route string, h http.HandlerFunc) http.Handler {
	return securityHeaders(
		p.tenantMiddleware(
			p.limiter.Middleware(
				p.requestLogger(endpoint, route,
					p.compatCacheMiddleware(endpoint, route, h)))))
}

// RegisterRoutes wires every proxy, admin, debug, and metrics route onto the
// supplied mux. Use this when running a single HTTP listener; for split-listener
// deployments (loopback admin / dedicated metrics port) call the more granular
// RegisterProxyRoutes / RegisterAdminRoutes / RegisterMetricsRoute helpers
// instead.
func (p *Proxy) RegisterRoutes(mux *http.ServeMux) {
	p.RegisterProxyRoutes(mux)
	if p.registerInstrumentation {
		p.RegisterMetricsRoute(mux)
		p.RegisterAdminRoutes(mux)
	} else if p.enableQueryAnalytics {
		// /debug/queries is admin-tier but gated independently of
		// registerInstrumentation; expose it on the same mux for back-compat.
		p.RegisterAdminRoutes(mux)
	}
}

// RegisterProxyRoutes registers Loki-compatible proxy routes, health endpoints,
// and the internal peer-cache endpoints. It deliberately omits /metrics,
// /admin/*, and /debug/* so callers can host those on dedicated listeners.
func (p *Proxy) RegisterProxyRoutes(mux *http.ServeMux) {
	rlNoTenant := func(endpoint, route string, h http.HandlerFunc) http.Handler {
		return securityHeaders(p.limiter.Middleware(p.requestLogger(endpoint, route, h)))
	}

	// Loki API endpoints — data queries are rate-limited
	mux.Handle("/loki/api/v1/query_range", p.routeHandler("query_range", "/loki/api/v1/query_range", p.handleQueryRange))
	mux.Handle("/loki/api/v1/query", p.routeHandler("query", "/loki/api/v1/query", p.handleQuery))
	mux.Handle("/loki/api/v1/series", p.routeHandler("series", "/loki/api/v1/series", p.handleSeries))

	// Metadata endpoints — rate-limited but cached
	mux.Handle("/loki/api/v1/labels", p.routeHandler("labels", "/loki/api/v1/labels", p.handleLabels))
	mux.Handle("/loki/api/v1/label/", p.routeHandler("label_values", "/loki/api/v1/label/{name}/values", p.handleLabelValues))
	mux.Handle("/loki/api/v1/detected_fields", p.routeHandler("detected_fields", "/loki/api/v1/detected_fields", p.handleDetectedFields))
	mux.Handle("/loki/api/v1/detected_field/", p.routeHandler("detected_field_values", "/loki/api/v1/detected_field/{name}/values", p.handleDetectedFieldValues))

	// Lighter endpoints — still rate-limited
	mux.Handle("/loki/api/v1/index/stats", p.routeHandler("index_stats", "/loki/api/v1/index/stats", p.handleIndexStats))
	mux.Handle("/loki/api/v1/index/volume", p.routeHandler("volume", "/loki/api/v1/index/volume", p.handleVolume))
	mux.Handle("/loki/api/v1/index/volume_range", p.routeHandler("volume_range", "/loki/api/v1/index/volume_range", p.handleVolumeRange))
	mux.Handle("/loki/api/v1/patterns", p.routeHandler("patterns", "/loki/api/v1/patterns", p.handlePatterns))
	mux.Handle("/loki/api/v1/tail", p.routeHandler("tail", "/loki/api/v1/tail", p.handleTail))

	// Read-only API additions
	mux.Handle("/loki/api/v1/format_query", p.routeHandler("format_query", "/loki/api/v1/format_query", p.handleFormatQuery))
	mux.Handle("/loki/api/v1/detected_labels", p.routeHandler("detected_labels", "/loki/api/v1/detected_labels", p.handleDetectedLabels))
	mux.Handle("/loki/api/v1/drilldown-limits", rlNoTenant("drilldown_limits", "/loki/api/v1/drilldown-limits", p.handleDrilldownLimits))
	mux.Handle("/config/tenant/v1/limits", rlNoTenant("tenant_limits", "/config/tenant/v1/limits", p.handleTenantLimitsConfig))

	// Write endpoints — blocked (this is a read-only proxy)
	mux.HandleFunc("/loki/api/v1/push", p.handleWriteBlocked)

	// Delete endpoint — exception to read-only with strict safeguards
	mux.Handle("/loki/api/v1/delete", p.routeHandler("delete", "/loki/api/v1/delete", p.handleDelete))

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

	// Peer cache endpoint — internal, for sharded fleet cache. Stays on the
	// main proxy listener because peer-to-peer traffic must be reachable from
	// other proxy pods, not constrained to the loopback admin listener.
	if p.peerCache != nil {
		peerCacheHandler := securityHeaders(p.peerCacheMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p.peerCache.ServeHTTP(w, r, p.cache)
		})))
		mux.Handle("/_cache/get", peerCacheHandler)
		mux.Handle("/_cache/set", peerCacheHandler)
		mux.Handle("/_cache/hot", peerCacheHandler)
		mux.Handle("/_cache/has", peerCacheHandler)
		mux.Handle("/_cache/peers", peerCacheHandler)
		// Peer-side cache purge (fanout target of /admin/cache/flush?peers=1).
		// Same shared-token gate; purges this node's caches only (no re-fanout).
		mux.Handle("/_cache/purge", securityHeaders(p.peerCacheMiddleware(http.HandlerFunc(p.handlePeerCachePurge))))
	}
}

// RegisterMetricsRoute registers only the Prometheus /metrics endpoint. Use
// with a dedicated metrics listener (--metrics-listen) when you want scrapers
// to hit a separate port from proxy traffic.
//
// No-op when registerInstrumentation is false on the Proxy config.
func (p *Proxy) RegisterMetricsRoute(mux *http.ServeMux) {
	if !p.registerInstrumentation {
		return
	}
	// Prometheus metrics endpoint — security headers plus bounded scrape concurrency.
	mux.Handle("/metrics", securityHeaders(http.HandlerFunc(p.handleMetrics)))
}

// RegisterAdminRoutes registers /admin/*, /debug/pprof/*, and /debug/queries
// routes. Intended for a dedicated admin listener (--admin-listen) when no
// -server.admin-auth-token is configured, so the binary boots safely with
// admin surfaces only reachable from loopback. Honors registerInstrumentation
// (cache flush + pprof) and enableQueryAnalytics independently.
func (p *Proxy) RegisterAdminRoutes(mux *http.ServeMux) {
	if p.registerInstrumentation {
		if p.enablePprof {
			mux.Handle("/debug/pprof/cmdline", p.adminMiddleware(http.NotFoundHandler()))
			mux.Handle("/debug/pprof/", p.adminMiddleware(http.DefaultServeMux))
		}
		// Cache flush — POST /admin/cache/flush clears all in-memory cache entries.
		// Useful for benchmarking (ensures each run starts cold) and debugging.
		// Protected by the admin auth token.
		mux.Handle("/admin/cache/flush", p.adminMiddleware(http.HandlerFunc(p.handleCacheFlush)))
	}
	if p.enableQueryAnalytics {
		mux.Handle("/debug/queries", securityHeaders(p.adminMiddleware(http.HandlerFunc(p.queryTracker.Handler))))
	}
}

// purgeLocalCaches clears this instance's cache tiers: L0 hot-key index,
// L1 in-memory, and L2 on-disk (the disk store is backed by p.cache). Shared by
// the admin flush endpoint and the peer-side purge endpoint.
func (p *Proxy) purgeLocalCaches() {
	if p.cache != nil {
		p.cache.PurgeAll()
	}
	if p.compatCache != nil {
		p.compatCache.PurgeAll()
	}
}

func (p *Proxy) handleCacheFlush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	p.purgeLocalCaches()

	resp := map[string]any{
		"status": "ok",
		"local":  "purged (L0 hot index + L1 memory + L2 disk)",
	}
	// Ring-wide fanout: POST /admin/cache/flush?peers=1 (or bare ?peers) also
	// purges every peer in the L3 ring, authenticated with the shared
	// X-Peer-Token (the same auth the peer cache uses for get/set/has). The
	// operator clears the whole fleet by hitting one admin-token-protected
	// instance. Peers purge LOCAL only — they never re-fan-out — so there is no
	// broadcast storm. Without ?peers, behavior is unchanged (local only). Down
	// peers are reported, not fatal.
	if q := r.URL.Query(); q.Has("peers") && !isFalsyFlag(q.Get("peers")) {
		resp["peers"] = p.purgePeerCaches(r.Context())
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// isFalsyFlag reports whether a query-param value explicitly disables a flag
// (0/false/no/off, case-insensitive). A bare `?peers` (empty value) is truthy.
func isFalsyFlag(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "0", "false", "no", "off":
		return true
	default:
		return false
	}
}

// purgePeerCaches fans out a cache purge to every peer in the ring concurrently
// and returns a per-peer result map (addr -> "purged" or "error: ..."). A peer
// being unreachable must not block clearing the rest of the ring, so failures
// are collected, not propagated.
func (p *Proxy) purgePeerCaches(ctx context.Context) map[string]string {
	results := map[string]string{}
	if p.peerCache == nil {
		results["_note"] = "peer cache disabled; purged local instance only"
		return results
	}
	peers := p.peerCache.Peers()
	self := p.peerCache.SelfAddr()

	// Cap concurrent peer purges so a large discovered ring doesn't burst one
	// HTTP request to every peer at once. errgroup.SetLimit bounds the number of
	// in-flight goroutines; results are collected over a buffered channel (a peer
	// being unreachable is reported, never fatal).
	type peerResult struct{ addr, status string }
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(peerPurgeMaxConcurrency)
	ch := make(chan peerResult, len(peers))
	dispatched := 0
	for _, peerAddr := range peers {
		addr := strings.TrimSpace(peerAddr)
		if addr == "" || addr == self {
			continue // already purged locally; skip the redundant self round-trip
		}
		dispatched++
		g.Go(func() error {
			pctx, cancel := context.WithTimeout(gctx, 5*time.Second)
			defer cancel()
			ch <- peerResult{addr: addr, status: p.purgeOnePeer(pctx, addr)}
			return nil
		})
	}
	if dispatched == 0 {
		results["_note"] = "no peers configured; purged local instance only"
		return results
	}
	go func() { _ = g.Wait(); close(ch) }()
	for pr := range ch {
		results[pr.addr] = pr.status
	}
	return results
}

// peerPurgeMaxConcurrency bounds the cache-purge fanout so a large peer ring
// cannot make one admin purge open a request to every peer simultaneously.
const peerPurgeMaxConcurrency = 16

// purgeOnePeer POSTs a token-authenticated purge to a single peer's
// /_cache/purge endpoint and returns a short status string.
func (p *Proxy) purgeOnePeer(ctx context.Context, addr string) string {
	endpoint := fmt.Sprintf("http://%s/_cache/purge", addr)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return "error: " + err.Error()
	}
	if p.peerAuthToken != "" {
		req.Header.Set("X-Peer-Token", p.peerAuthToken)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return "error: " + err.Error()
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode == http.StatusOK {
		return "purged"
	}
	return fmt.Sprintf("error: HTTP %d", resp.StatusCode)
}

// handlePeerCachePurge is the peer-side endpoint hit by purgePeerCaches. Gated by
// peerCacheMiddleware (shared X-Peer-Token), it purges THIS node's caches only —
// it never fans out, preventing recursion across the ring.
func (p *Proxy) handlePeerCachePurge(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	p.purgeLocalCaches()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok","message":"peer cache purged (L0 hot index + L1 memory + L2 disk)"}`))
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
//
//nolint:gocyclo // dispatches across cache, multi-tenant fanout, windowing, stats vs logs, streaming and tuple modes; branching is inherent to Loki query_range parity.
func (p *Proxy) handleQueryRange(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	logqlQuery := r.FormValue("query")
	// Strip incomplete | unwrap stubs (no field name) that Grafana's metric builder
	// emits while the unwrap field picker is open. These are syntactically invalid
	// and would return 400, leaving the field picker empty.
	logqlQuery = stripIncompleteUnwrapStubs(logqlQuery)
	logqlQuery, ok := p.validateQuery(w, logqlQuery, "query_range")
	if !ok {
		return
	}

	// Suppress the Grafana querySplitting RESIDUAL chunk (see isQuerySplitResidual)
	// for METRIC (matrix) range queries. This MUST run here, at the entry, while
	// r.URL still carries start/end/step and the ORIGINAL logqlQuery still parses as
	// a metric expression: downstream, withOrgID/injectAuthFingerprint reset r.Form
	// and the URL/query are rewritten for VL, so the deeper stats/hits guards see an
	// empty range and silently no-op. A sub-step (range < step) by() residual yields
	// a single-bucket, multi-series frame that Grafana's mergeFrames collapses onto
	// ONE edge of the merged chart (a right-edge spike, or a left-edge "all data at
	// the beginning" cluster). Scoped to metric exprs via the AST so a log query
	// (streams, not a matrix) is never blanked.
	if isQuerySplitResidual(r) {
		if parsed, perr := logqlpkg.Parse(logqlQuery); perr == nil {
			switch parsed.(type) {
			case *logqlpkg.RangeAggregation, *logqlpkg.VectorAggregation, *logqlpkg.BinOpExpr, *logqlpkg.OpaqueMetricExpr, *logqlpkg.LiteralExpr:
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Proxy-Drilldown-Path", "hits-leftover-suppressed")
				_, _ = w.Write(emptyLokiMatrix)
				p.metrics.RecordRequest("query_range", http.StatusOK, time.Since(start))
				return
			}
		}
	}

	categorizedLabels := requestWantsCategorizedLabels(r)
	emitStructuredMetadata := p.shouldEmitStructuredMetadata(r)
	tupleMode := tupleModeForRequest(categorizedLabels, emitStructuredMetadata)
	if p.handleMultiTenantFanout(w, r, "query_range") {
		return
	}
	cacheKey := ""
	// Skip inner p.cache when compatCacheMiddleware is active — it handles the
	// outer response capture and store, making a nested inner cache redundant.
	cacheable := !p.streamResponse && !compatCacheIsActive(r.Context())
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
	p.log.Debug("query_range request", "logql", redactQuery(logqlQuery, p.debugLogRawQueries))

	// withOrgID must precede any vlGet/vlPost call (preferWorkingParser, bare-parser
	// paths, post-agg paths) so that the tenant context and forwarded auth headers
	// are available for all upstream requests made on this request's behalf.
	r = withOrgID(r)
	r = p.injectAuthFingerprint(r)

	logqlQuery = resolveGrafanaRangeTemplateTokens(logqlQuery, r.FormValue("start"), r.FormValue("end"), r.FormValue("step"))

	// Extract and apply LogQL offset: strip the offset clause and shift start/end
	// backward so preferWorkingParser probes the historical window where the offset
	// data actually lives. All downstream dispatch paths see the shifted times.
	//
	// Exception: binary metric expressions where only SOME sides carry an offset
	// ("mixed" case). Loki evaluates each side independently so it accepts these;
	// the proxy lets them through to the binary routing path which dispatches
	// separate VL queries per side. The translator silently drops offset clauses,
	// so time-shifting is not applied — results may differ from Loki for offset
	// values but the proxy returns 200 rather than incorrectly rejecting the query.
	// Expressions with multiple *different* offsets still return 400 (same as Loki).
	{
		offsetDur, strippedQuery, offsetErr := extractLogQLOffset(logqlQuery)
		if offsetErr != nil {
			// Allow mixed-offset binary expressions: bypass only when the failure is
			// "some range vectors have offset, others do not" AND the query is a
			// top-level binary op (Loki handles these by evaluating sides independently).
			isMixedOffset := strings.Contains(offsetErr.Error(), "loki-vl-proxy requires a uniform offset")
			parsedForOffset, _ := logqlpkg.Parse(logqlQuery)
			_, isBinaryForOffset := parsedForOffset.(*logqlpkg.BinOpExpr)
			if !isMixedOffset || !isBinaryForOffset {
				p.writeError(w, http.StatusBadRequest, offsetErr.Error())
				p.metrics.RecordRequest("query_range", http.StatusBadRequest, time.Since(start))
				return
			}
			// Mixed-offset binary: proceed without time-shifting; binary routing handles it.
		} else {
			logqlQuery = strippedQuery
			if offsetDur != 0 {
				// r.ParseForm() allocates a new map on the post-WithContext shallow copy —
				// it does not alias the map captured by withOrgID's origRequestKey reference.
				_ = r.ParseForm()
				if startNs, ok := parseLokiTimeToUnixNano(r.FormValue("start")); ok {
					r.Form.Set("start", nanosToVLTimestamp(startNs-offsetDur.Nanoseconds()))
				}
				if endNs, ok := parseLokiTimeToUnixNano(r.FormValue("end")); ok {
					r.Form.Set("end", nanosToVLTimestamp(endNs-offsetDur.Nanoseconds()))
				}
			}
		}
	}

	// Enforce max query length AFTER offset is applied (start/end reflect shifted range).
	if startNs, okS := parseLokiTimeToUnixNano(r.FormValue("start")); okS {
		if endNs, okE := parseLokiTimeToUnixNano(r.FormValue("end")); okE {
			if errMsg := p.checkQueryRangeLength(r.Context(), startNs, endNs); errMsg != "" {
				p.writeError(w, http.StatusBadRequest, errMsg)
				p.metrics.RecordRequest("query_range", http.StatusBadRequest, time.Since(start))
				return
			}
		}
	}

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

	if spec, ok := parseAbsentOverTimeCompatSpec(logqlQuery); ok {
		p.proxyAbsentOverTimeQueryRange(w, r, start, logqlQuery, spec)
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
	logsqlQuery, labelReplaceSpecs := translator.ParseAllLabelReplaceMarkers(logsqlQuery)
	logsqlQuery, labelJoinSpec := translator.ParseLabelJoinMarker(logsqlQuery)
	for _, spec := range labelReplaceSpecs {
		if _, err := regexp.Compile("^(?:" + spec.Regex + ")$"); err != nil {
			p.writeError(w, http.StatusBadRequest, fmt.Sprintf("parse error : invalid regex in label_replace: %v", err))
			p.metrics.RecordRequest("query_range", http.StatusBadRequest, time.Since(start))
			return
		}
	}
	logsqlQuery = preserveMetricStreamIdentity(logqlQuery, logsqlQuery, withoutLabels)
	if isBareMetricFunctionQuery(strings.TrimSpace(logqlQuery)) && !isStatsQuery(logsqlQuery) {
		p.writeError(w, http.StatusBadRequest, "unsupported metric query: range aggregations require compatible unwrap or translator support")
		p.metrics.RecordRequest("query_range", http.StatusBadRequest, time.Since(start))
		return
	}
	p.log.Debug("translated query", "logsql", redactQuery(logsqlQuery, p.debugLogRawQueries), "without", withoutLabels)

	needsCapture := len(withoutLabels) > 0 || isGroupQuery || len(labelReplaceSpecs) > 0 || labelJoinSpec != nil
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

	// Route using the original LogQL AST — more reliable than re-parsing translated markers.
	parsedForRouting, _ := logqlpkg.Parse(logqlQuery)
	if ra, ok := parsedForRouting.(*logqlpkg.RangeAggregation); ok && ra.Step != "" {
		// Subquery: max_over_time(rate(...)[1h:5m])
		innerLogsql, innerErr := p.translateQueryWithContext(r.Context(), ra.Inner.String())
		if innerErr != nil {
			p.writeError(sc, http.StatusBadRequest, innerErr.Error())
		} else {
			p.proxySubqueryRange(sc, r, string(ra.Op), innerLogsql, ra.Range, ra.Step)
		}
	} else if binOp, ok := parsedForRouting.(*logqlpkg.BinOpExpr); ok {
		// Binary metric expression: sum(rate(...)) / sum(rate(...))
		// translateBinOpSide handles scalar literals (e.g. * 100) without translation.
		leftLogsql, leftErr := p.translateBinOpSide(r.Context(), binOp.Left)
		rightLogsql, rightErr := p.translateBinOpSide(r.Context(), binOp.Right)
		if leftErr != nil {
			p.writeError(sc, http.StatusBadRequest, leftErr.Error())
		} else if rightErr != nil {
			p.writeError(sc, http.StatusBadRequest, rightErr.Error())
		} else {
			p.proxyBinaryMetricQueryRangeVM(sc, r, binOp.Op, leftLogsql, rightLogsql, binOpExprToVMInfo(binOp))
		}
	} else if isStatsQuery(logsqlQuery) {
		p.proxyStatsQueryRange(sc, r, logsqlQuery)
	} else {
		if !p.proxyLogQueryWindowed(sc, r, logsqlQuery) {
			if p.coldRouter != nil {
				p.proxyLogQueryWithCold(sc, r, logsqlQuery)
			} else {
				p.proxyLogQuery(sc, r, logsqlQuery)
			}
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
		for _, spec := range labelReplaceSpecs {
			cacheOut = applyLabelReplace(cacheOut, spec)
		}
		if labelJoinSpec != nil {
			cacheOut = applyLabelJoin(cacheOut, *labelJoinSpec)
		}
		copyBackendHeaders(w.Header(), capture.Header())
		if w.Header().Get("Content-Type") == "" {
			w.Header().Set("Content-Type", "application/json")
		}
		if sc.code != http.StatusOK {
			// Rewrite VL error body to Loki-compliant format (Loki's errorType field
			// uses strings like "execution", "timeout", etc.; VL may use "unavailable").
			p.writeError(w, sc.code, p.redactBackendError(cacheOut))
		} else {
			_, _ = w.Write(cacheOut)
		}
		if cacheable && sc.code == http.StatusOK {
			p.setLocalReadCacheWithTTL(cacheKey, append([]byte(nil), cacheOut...), 5*time.Minute)
		}
	} else if cacheTap != nil {
		if cacheable && sc.code == http.StatusOK {
			if body := cacheTap.CapturedBody(); len(body) > 0 {
				p.setLocalReadCacheWithTTL(cacheKey, append([]byte(nil), body...), 5*time.Minute)
			}
		}
		cacheTap.Release()
	}

	elapsed := time.Since(start)
	p.metrics.RecordRequest("query_range", sc.code, elapsed)
	p.queryTracker.Record("query_range", logqlQuery, elapsed, sc.code >= 400)
}

// queryRangeBucket returns the cache-key bucket size for a query_range request.
// Bucket = max(5 min, step), capped at 1 hour so very large steps don't produce
// multi-day cache entries that hold stale data too long.
func queryRangeBucket(r *http.Request) time.Duration {
	const (
		minBucket = 5 * time.Minute
		maxBucket = time.Hour
	)
	stepRaw := r.FormValue("step")
	if stepRaw == "" {
		return minBucket
	}
	d, ok := parsePositiveStepDuration(stepRaw)
	if !ok || d <= minBucket {
		return minBucket
	}
	if d > maxBucket {
		return maxBucket
	}
	return d
}

func (p *Proxy) queryRangeCacheKey(r *http.Request, logqlQuery string) string {
	// Build a stable key by bucketing both `start` and `end` to the step granularity.
	// Grafana's sliding time window ("from=now-2d&to=now") resolves to absolute
	// nanosecond timestamps that advance every second, so both start and end change on
	// every panel refresh. Bucketing only `end` (the previous behaviour) still produced
	// a unique key on each tick because the raw `start` value was included verbatim.
	//
	// With both endpoints bucketed to max(5min, step), the cache key is stable for the
	// full bucket duration. A 2-day window with step=1h now produces one VL call per
	// field per hour instead of one per 10 seconds (~360x fewer upstream calls).
	bucket := queryRangeBucket(r)
	startBucketed := bucketTimestampString(r.FormValue("start"), bucket)
	endBucketed := bucketTimestampString(r.FormValue("end"), bucket)

	var b strings.Builder
	b.Grow(len(logqlQuery) + 128)
	b.WriteString("query=")
	b.WriteString(url.QueryEscape(logqlQuery))
	for _, key := range []string{"step", "limit", "direction"} {
		if value := r.FormValue(key); value != "" {
			b.WriteByte('&')
			b.WriteString(key)
			b.WriteByte('=')
			b.WriteString(url.QueryEscape(value))
		}
	}
	if startBucketed != "" {
		b.WriteString("&start=")
		b.WriteString(url.QueryEscape(startBucketed))
	}
	if endBucketed != "" {
		b.WriteString("&end=")
		b.WriteString(url.QueryEscape(endBucketed))
	}
	key := "query_range:" + r.Header.Get("X-Scope-OrgID") + ":" + b.String() + ":" + p.tupleModeCacheKey(r)
	if fp := p.fingerprintFromCtx(r.Context(), r); fp != "" {
		key += ":auth:" + fp
	}
	return key
}

// handleQuery translates Loki instant queries.
//
//nolint:gocyclo // dispatches across cache, multi-tenant fanout, stats vs logs, subquery, binary metric, and streaming modes; branching is inherent to Loki instant query parity.
func (p *Proxy) handleQuery(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	logqlQuery := r.FormValue("query")
	logqlQuery, ok := p.validateQuery(w, logqlQuery, "query")
	if !ok {
		return
	}
	p.log.Debug("query request", "logql", redactQuery(logqlQuery, p.debugLogRawQueries))

	if body, ok := evaluateConstantInstantVectorQuery(logqlQuery, r.FormValue("time")); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
		elapsed := time.Since(start)
		p.metrics.RecordRequest("query", http.StatusOK, elapsed)
		p.queryTracker.Record("query", logqlQuery, elapsed, false)
		return
	}
	if p.handleMultiTenantFanout(w, r, "query") {
		return
	}

	// withOrgID must precede any vlGet/vlPost call (preferWorkingParser and all
	// early-return compat paths) so that tenant context is set for upstream requests.
	r = withOrgID(r)
	r = p.injectAuthFingerprint(r)

	logqlQuery = resolveGrafanaRangeTemplateTokens(logqlQuery, r.FormValue("start"), r.FormValue("end"), r.FormValue("step"))

	// Extract and apply LogQL offset: strip the offset clause and shift the eval
	// time backward so preferWorkingParser probes the historical window where the
	// offset data actually lives. All downstream dispatch paths see the shifted time.
	//
	// Mixed-offset binary expressions are treated the same as in query_range above.
	{
		offsetDur, strippedQuery, offsetErr := extractLogQLOffset(logqlQuery)
		if offsetErr != nil {
			isMixedOffset := strings.Contains(offsetErr.Error(), "loki-vl-proxy requires a uniform offset")
			parsedForOffset, _ := logqlpkg.Parse(logqlQuery)
			_, isBinaryForOffset := parsedForOffset.(*logqlpkg.BinOpExpr)
			if !isMixedOffset || !isBinaryForOffset {
				p.writeError(w, http.StatusBadRequest, offsetErr.Error())
				p.metrics.RecordRequest("query", http.StatusBadRequest, time.Since(start))
				return
			}
		} else {
			logqlQuery = strippedQuery
			if offsetDur != 0 {
				// r.ParseForm() allocates a new map on the post-WithContext shallow copy —
				// it does not alias the map captured by withOrgID's origRequestKey reference.
				_ = r.ParseForm()
				rawTime := r.FormValue("time")
				if rawTime == "" {
					// Loki allows omitting "time"; it defaults to now.
					// We must materialise that default here so the offset shift is applied.
					rawTime = strconv.FormatInt(time.Now().UnixNano(), 10)
				}
				if timeNs, ok := parseLokiTimeToUnixNano(rawTime); ok {
					r.Form.Set("time", nanosToVLTimestamp(timeNs-offsetDur.Nanoseconds()))
				}
			}
		}
	}

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
	logsqlQuery, labelReplaceSpecs := translator.ParseAllLabelReplaceMarkers(logsqlQuery)
	logsqlQuery, labelJoinSpec := translator.ParseLabelJoinMarker(logsqlQuery)
	for _, spec := range labelReplaceSpecs {
		if _, err := regexp.Compile("^(?:" + spec.Regex + ")$"); err != nil {
			p.writeError(w, http.StatusBadRequest, fmt.Sprintf("parse error : invalid regex in label_replace: %v", err))
			p.metrics.RecordRequest("query", http.StatusBadRequest, time.Since(start))
			return
		}
	}
	logsqlQuery = preserveMetricStreamIdentity(logqlQuery, logsqlQuery, withoutLabels)
	if isBareMetricFunctionQuery(strings.TrimSpace(logqlQuery)) && !isStatsQuery(logsqlQuery) {
		p.writeError(w, http.StatusBadRequest, "unsupported metric query: range aggregations require compatible unwrap or translator support")
		p.metrics.RecordRequest("query", http.StatusBadRequest, time.Since(start))
		return
	}

	// Wrap writer to capture actual status code for metrics
	sc := &statusCapture{ResponseWriter: w, code: 200}

	needsCapture := len(withoutLabels) > 0 || isGroupQuery || len(labelReplaceSpecs) > 0 || labelJoinSpec != nil
	var bw *bufferedResponseWriter
	if needsCapture {
		bw = &bufferedResponseWriter{header: w.Header()}
		sc = &statusCapture{ResponseWriter: bw, code: 200}
	}

	// Route using the original LogQL AST — more reliable than re-parsing translated markers.
	parsedForRoutingQ, _ := logqlpkg.Parse(logqlQuery)
	if ra, ok := parsedForRoutingQ.(*logqlpkg.RangeAggregation); ok && ra.Step != "" {
		innerLogsql, innerErr := p.translateQueryWithContext(r.Context(), ra.Inner.String())
		if innerErr != nil {
			p.writeError(sc, http.StatusBadRequest, innerErr.Error())
		} else {
			p.proxySubquery(sc, r, string(ra.Op), innerLogsql, ra.Range, ra.Step)
		}
	} else if binOp, ok := parsedForRoutingQ.(*logqlpkg.BinOpExpr); ok {
		leftLogsql, leftErr := p.translateBinOpSide(r.Context(), binOp.Left)
		rightLogsql, rightErr := p.translateBinOpSide(r.Context(), binOp.Right)
		if leftErr != nil {
			p.writeError(sc, http.StatusBadRequest, leftErr.Error())
		} else if rightErr != nil {
			p.writeError(sc, http.StatusBadRequest, rightErr.Error())
		} else {
			p.proxyBinaryMetricQueryVM(sc, r, binOp.Op, leftLogsql, rightLogsql, binOpExprToVMInfo(binOp))
		}
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
		for _, spec := range labelReplaceSpecs {
			result = applyLabelReplace(result, spec)
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
