package proxy

import (
	"crypto/sha256"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

// State holds internal mutable runtime state.
// All guarded fields are protected by the mutex listed alongside them.
// These mirror the corresponding fields in Proxy and are extracted here
// to support future decomposition of the Proxy struct.
type State struct {
	// configMu guards tenantMap and labelTranslator.
	configMu      sync.RWMutex
	tenantMap     map[string]TenantMapping
	labelTranslator *LabelTranslator

	// queryRangeAdaptiveMu guards adaptive parallel-tuning state.
	queryRangeAdaptiveMu        sync.Mutex
	queryRangeParallelCurrent   int
	queryRangeLatencyEWMA       time.Duration
	queryRangeErrorEWMA         float64
	queryRangeAdaptiveLastAdjust time.Time

	// patternsSnapshotMu guards in-memory pattern snapshot entries.
	patternsSnapshotMu          sync.RWMutex
	patternsSnapshotEntries     map[string]patternSnapshotEntry
	patternsSnapshotPatternCount int64
	patternsSnapshotPayloadBytes int64

	// patternsPersistDigest is the SHA-256 of the last successfully written snapshot.
	patternsPersistDigest      [sha256.Size]byte
	patternsPersistDigestReady bool

	// Patterns lifecycle signals.
	patternsWarmReady        atomic.Bool
	patternsPersistStarted   atomic.Bool
	patternsPersistDirty     atomic.Bool
	patternsPersistStop      chan struct{}
	patternsPersistDone      chan struct{}

	// backendVersionMu guards the discovered backend version fields.
	backendVersionMu                    sync.RWMutex
	backendVersionRaw                   string
	backendVersionSemver                string
	backendCapabilityProfile            string
	backendSupportsStreamMetadata       bool
	backendSupportsDensePatternWindowing bool
	backendSupportsMetadataSubstring    bool
	backendVersionLogged                bool

	// labelValuesIndexMu guards the per-tenant label-values indexed cache.
	labelValuesIndexMu          sync.RWMutex
	labelValuesIndex            map[string]*labelValuesIndexState

	// labelValuesIndexPersistDigest is the SHA-256 of the last successfully written index snapshot.
	labelValuesIndexPersistDigest      [sha256.Size]byte
	labelValuesIndexPersistDigestReady bool

	// Label-values index lifecycle signals.
	labelValuesIndexWarmReady        atomic.Bool
	labelValuesIndexPersistStarted   atomic.Bool
	labelValuesIndexPersistDirty     atomic.Bool
	labelValuesIndexPersistStop      chan struct{}
	labelValuesIndexPersistDone      chan struct{}

	// keepWarmStop signals the label cache keep-warm goroutine to stop.
	keepWarmStop chan struct{}

	// readCacheKeyMemoMu guards the read-cache key memoization map.
	readCacheKeyMemoMu sync.RWMutex
	readCacheKeyMemo   map[canonicalReadCacheMemoKey]string

	// labelRefreshGroup deduplicates concurrent label refresh calls.
	labelRefreshGroup singleflight.Group

	// logSampleCount is the rolling counter for access-log sampling.
	logSampleCount atomic.Uint64

	// metricsConcurrencyLimiter is the bounded channel for backend concurrency.
	metricsConcurrencyLimiter chan struct{}

	// coldRouter handles cold/warm storage routing.
	coldRouter *ColdRouter
}

// newState allocates a zero-value State with required maps and channels initialised.
func newState() *State {
	return &State{
		tenantMap:               make(map[string]TenantMapping),
		patternsSnapshotEntries: make(map[string]patternSnapshotEntry),
		labelValuesIndex:        make(map[string]*labelValuesIndexState),
		readCacheKeyMemo:        make(map[canonicalReadCacheMemoKey]string),
	}
}
