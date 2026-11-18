package proxy

import (
	"sync"
	"sync/atomic"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// A query longer than validationCacheMaxQueryBytes is read by many steps of one request: the validation (the
// parameter check, the time bounds, the empty-answer check, the handler), the request rewrite, the plan and the
// handlers' parses. The memos of short queries bound their memory by a count of entries, which a long query would
// make large; longQueryMemo keeps what they keep for a long query, bounded by bytes, so that one parse and one
// validation serve the whole request (and its repeats) whatever the query's length.

// longQueryMemoBytes is what one generation of longQueryMemo holds: the cost of its entries, each its query's length
// times its weight (a parse holds a tree several times the size of its text). Two generations live: when the
// current one is full it becomes the previous one, and a hit there moves the entry to the current one.
const longQueryMemoBytes = 4 << 20

// The weights of longQueryMemo's entries: a validation result or a respelled text holds about its query's length (the
// query is its key), a parse a tree of a few times that.
const (
	longMemoTextWeight  = 2
	longMemoParseWeight = 8
	longMemoPlanWeight  = 16
)

// longQuery reports a query the memos of short queries do not keep and longQueryMemo does: longer than
// validationCacheMaxQueryBytes, within Loki's input size limit (a longer one is rejected before it is parsed).
func longQuery(query string) bool {
	return len(query) > validationCacheMaxQueryBytes && len(query) < logqlpkg.MaxInputSize
}

// longMemoCost is the cost of an entry of query of the given weight.
func longMemoCost(query string, weight int64) int64 { return int64(len(query)) * weight }

// Keys of longQueryMemo, one type per kind of entry.
type (
	longValidationKey  string
	longRequestTextKey string
	longPlanKey        string
)

type longMemoGeneration struct {
	entries sync.Map // key -> longMemoEntry
	cost    atomic.Int64
}

// longMemoEntry is a value of the memo and its cost.
type longMemoEntry struct {
	v    any
	cost int64
}

// textMemo is a memo by query text bounded by the cost of its entries, in two generations.
type textMemo struct {
	limit     int64
	mu        sync.Mutex
	cur, prev atomic.Pointer[longMemoGeneration]
}

func newTextMemo(limit int64) *textMemo {
	m := &textMemo{limit: limit}
	m.cur.Store(&longMemoGeneration{})
	return m
}

var longQueryMemo = newTextMemo(longQueryMemoBytes)

// load is the entry of key, from either generation.
func (m *textMemo) load(key any) (any, bool) {
	if e, ok := m.cur.Load().entries.Load(key); ok {
		return e.(longMemoEntry).v, true
	}
	if prev := m.prev.Load(); prev != nil {
		if e, ok := prev.entries.Load(key); ok {
			entry := e.(longMemoEntry)
			m.store(key, entry.cost, entry.v, false)
			return entry.v, true
		}
	}
	return nil, false
}

// store keeps v under key at the given cost, starting a new generation when the current one cannot take it; an
// entry costing more than a generation is not kept. With replace a present entry is replaced, otherwise kept.
func (m *textMemo) store(key any, cost int64, v any, replace bool) {
	if cost > m.limit {
		return
	}
	cur := m.cur.Load()
	if cur.cost.Load()+cost > m.limit {
		m.mu.Lock()
		if m.cur.Load() == cur {
			m.prev.Store(cur)
			m.cur.Store(&longMemoGeneration{})
		}
		m.mu.Unlock()
		cur = m.cur.Load()
	}
	e := longMemoEntry{v: v, cost: cost}
	if replace {
		if old, loaded := cur.entries.Swap(key, e); !loaded {
			cur.cost.Add(cost)
		} else {
			cur.cost.Add(cost - old.(longMemoEntry).cost)
		}
		return
	}
	if _, loaded := cur.entries.LoadOrStore(key, e); !loaded {
		cur.cost.Add(cost)
	}
}
