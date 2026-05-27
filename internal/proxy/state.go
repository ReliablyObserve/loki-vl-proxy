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
//
// Mutex fields are stored as pointers so that Handler.State can share the
// exact same mutex instance as Proxy (value copies of sync.Mutex/sync.RWMutex
// are forbidden by go vet and would create independent locks).
// Atomic fields (atomic.Bool, atomic.Uint64) are likewise stored as pointers
// for the same reason — atomic types must not be copied.
type State struct {
	// configMu guards tenantMap and labelTranslator.
	configMu      *sync.RWMutex
	tenantMap     map[string]TenantMapping
	labelTranslator *LabelTranslator

	// queryRangeAdaptiveMu guards adaptive parallel-tuning state.
	queryRangeAdaptiveMu        *sync.Mutex
	queryRangeParallelCurrent   *int
	queryRangeLatencyEWMA       *time.Duration
	queryRangeErrorEWMA         *float64
	queryRangeAdaptiveLastAdjust *time.Time

	// patternsSnapshotMu guards in-memory pattern snapshot entries.
	patternsSnapshotMu          *sync.RWMutex
	patternsSnapshotEntries     map[string]patternSnapshotEntry
	patternsSnapshotPatternCount *int64
	patternsSnapshotPayloadBytes *int64

	// patternsPersistDigest is the SHA-256 of the last successfully written snapshot.
	patternsPersistDigest      *[sha256.Size]byte
	patternsPersistDigestReady *bool

	// Patterns lifecycle signals.
	patternsWarmReady        *atomic.Bool
	patternsPersistStarted   *atomic.Bool
	patternsPersistDirty     *atomic.Bool
	patternsPersistStop      chan struct{}
	patternsPersistDone      chan struct{}

	// backendVersionMu guards the discovered backend version fields.
	backendVersionMu                    *sync.RWMutex
	backendVersionRaw                   *string
	backendVersionSemver                *string
	backendCapabilityProfile            *string
	backendSupportsStreamMetadata       *bool
	backendSupportsDensePatternWindowing *bool
	backendSupportsMetadataSubstring    *bool
	backendVersionLogged                *bool

	// labelValuesIndexMu guards the per-tenant label-values indexed cache.
	labelValuesIndexMu          *sync.RWMutex
	labelValuesIndex            map[string]*labelValuesIndexState

	// labelValuesIndexPersistDigest is the SHA-256 of the last successfully written index snapshot.
	labelValuesIndexPersistDigest      *[sha256.Size]byte
	labelValuesIndexPersistDigestReady *bool

	// Label-values index lifecycle signals.
	labelValuesIndexWarmReady        *atomic.Bool
	labelValuesIndexPersistStarted   *atomic.Bool
	labelValuesIndexPersistDirty     *atomic.Bool
	labelValuesIndexPersistStop      chan struct{}
	labelValuesIndexPersistDone      chan struct{}

	// keepWarmStop signals the label cache keep-warm goroutine to stop.
	keepWarmStop chan struct{}

	// readCacheKeyMemoMu guards the read-cache key memoization map.
	readCacheKeyMemoMu *sync.RWMutex
	readCacheKeyMemo   map[canonicalReadCacheMemoKey]string

	// labelRefreshGroup deduplicates concurrent label refresh calls.
	// singleflight.Group must not be copied, so it is held by pointer.
	labelRefreshGroup *singleflight.Group

	// logSampleCount is the rolling counter for access-log sampling.
	// atomic.Uint64 must not be copied, so it is held by pointer.
	logSampleCount *atomic.Uint64

	// metricsConcurrencyLimiter is the bounded channel for backend concurrency.
	metricsConcurrencyLimiter chan struct{}

	// coldRouter handles cold/warm storage routing.
	coldRouter *ColdRouter
}

// newState allocates a zero-value State with required maps and channels
// initialised.  It is used only as a fallback when a Proxy instance is not
// available to supply the live shared pointers (e.g. unit tests).
func newState() *State {
	return &State{
		configMu:                           &sync.RWMutex{},
		tenantMap:                          make(map[string]TenantMapping),
		queryRangeAdaptiveMu:               &sync.Mutex{},
		queryRangeParallelCurrent:          new(int),
		queryRangeLatencyEWMA:              new(time.Duration),
		queryRangeErrorEWMA:                new(float64),
		queryRangeAdaptiveLastAdjust:       new(time.Time),
		patternsSnapshotMu:                 &sync.RWMutex{},
		patternsSnapshotEntries:            make(map[string]patternSnapshotEntry),
		patternsSnapshotPatternCount:       new(int64),
		patternsSnapshotPayloadBytes:       new(int64),
		patternsPersistDigest:              new([sha256.Size]byte),
		patternsPersistDigestReady:         new(bool),
		patternsWarmReady:                  &atomic.Bool{},
		patternsPersistStarted:             &atomic.Bool{},
		patternsPersistDirty:               &atomic.Bool{},
		backendVersionMu:                   &sync.RWMutex{},
		backendVersionRaw:                  new(string),
		backendVersionSemver:               new(string),
		backendCapabilityProfile:           new(string),
		backendSupportsStreamMetadata:      new(bool),
		backendSupportsDensePatternWindowing: new(bool),
		backendSupportsMetadataSubstring:   new(bool),
		backendVersionLogged:               new(bool),
		labelValuesIndexMu:                 &sync.RWMutex{},
		labelValuesIndex:                   make(map[string]*labelValuesIndexState),
		labelValuesIndexPersistDigest:      new([sha256.Size]byte),
		labelValuesIndexPersistDigestReady: new(bool),
		labelValuesIndexWarmReady:          &atomic.Bool{},
		labelValuesIndexPersistStarted:     &atomic.Bool{},
		labelValuesIndexPersistDirty:       &atomic.Bool{},
		readCacheKeyMemoMu:                 &sync.RWMutex{},
		readCacheKeyMemo:                   make(map[canonicalReadCacheMemoKey]string),
		labelRefreshGroup:                  &singleflight.Group{},
		logSampleCount:                     &atomic.Uint64{},
	}
}
