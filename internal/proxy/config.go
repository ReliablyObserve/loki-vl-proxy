package proxy

import "time"

// HandlerConfig holds immutable startup configuration derived from Config.
// Fields are set once during New; never written after that.
// These mirror the corresponding fields in Proxy and are extracted here
// to support future decomposition of the Proxy struct.
//
// Note: Config (the public constructor input type) already exists in proxy.go.
// HandlerConfig represents the post-parse, normalised view stored on Proxy.
type HandlerConfig struct {
	tenantLabel                           string
	authEnabled                           bool
	requireTenantHeader                   bool
	allowGlobalTenant                     bool
	forwardTenantHeader                   bool
	maxLines                              int
	forwardHeaders                        []string
	forwardCookies                        map[string]bool
	backendHeaders                        map[string]string
	backendCompression                    string
	backendLoopback                       bool
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
	metadataFieldMode                     MetadataFieldMode
	streamFieldsMap                       map[string]bool
	declaredLabelFields                   []string
	registerInstrumentation               bool
	enablePprof                           bool
	enableQueryAnalytics                  bool
	adminAuthToken                        string
	rangeMetricRowLimit                   int
	tailAllowedOrigins                    map[string]struct{}
	tailMode                              TailMode
	metricsTrustProxyHeaders              bool
	tenantLimitsAllowPublish              []string
	tenantDefaultLimits                   map[string]any
	tenantLimits                          map[string]map[string]any
	defaultMaxQueryLength                 time.Duration
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
	peerAuthToken                         string
	cacheTTLLabels                        time.Duration
	cacheTTLLabelValues                   time.Duration
	logSampleN                            uint64
}
