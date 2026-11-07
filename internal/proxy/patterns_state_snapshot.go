package proxy

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"sort"
	"time"
)

// patternStateSnapshotEvery is how often at most the holder of a selector's pattern state writes it to the cache
// as it reads new lines (it writes it at once when the state is built): a peer that takes the selector over, or the
// holder after a restart, continues from it, reading only the lines after it, with the same patterns.
const patternStateSnapshotEvery = time.Minute

// patternStateSnapshotVersion is the format of a snapshot; one of another version is not read.
const patternStateSnapshotVersion = 1

// patternStateSnapshot is a selector's pattern state as its holder writes it to the cache: everything a peer needs
// to continue it exactly, the clusterers' prefix trees included, so the lines it reads next join the same
// clusters as they would on the holder.
type patternStateSnapshot struct {
	V        int                                `json:"v"`
	FromNs   int64                              `json:"from"`
	ToNs     int64                              `json:"to"`
	Streams  []string                           `json:"streams"`
	Newest   map[string]int64                   `json:"newest"`
	AtNewest map[string]map[uint64]int          `json:"at_newest,omitempty"`
	Miners   []patternClustererSnapshot         `json:"miners"`
	Folded   map[string][]patternFoldedSnapshot `json:"folded,omitempty"`
}

type patternClustererSnapshot struct {
	Stream   string                   `json:"s"`
	Level    string                   `json:"l"`
	Clock    uint64                   `json:"clock"`
	Admitted int                      `json:"admitted"`
	Evicted  int                      `json:"evicted"`
	Clusters []patternClusterSnapshot `json:"clusters"`
	Trees    []patternTreeSnapshot    `json:"trees"`
}

type patternClusterSnapshot struct {
	Tokens  []string      `json:"t"`
	Spaces  []int         `json:"sp,omitempty"`
	Format  patternFormat `json:"f"`
	Buckets map[int64]int `json:"b"`
	Total   int           `json:"n"`
	Used    uint64        `json:"u"`
}

type patternTreeSnapshot struct {
	Format patternFormat       `json:"f"`
	Tokens int                 `json:"t"`
	Root   patternNodeSnapshot `json:"root"`
}

// patternNodeSnapshot is a node of a prefix tree: its children by key (the first one the node's first child) and
// the indexes, among the clusterer's clusters, of the clusters registered at it, in their order.
type patternNodeSnapshot struct {
	Keys     []string              `json:"k,omitempty"`
	Children []patternNodeSnapshot `json:"c,omitempty"`
	Clusters []int                 `json:"cl,omitempty"`
}

type patternFoldedSnapshot struct {
	Level   string        `json:"l"`
	Pattern string        `json:"p"`
	Samples map[int64]int `json:"s"`
}

// snapshot is the tenant state's selector s as a snapshot; the caller holds the state's lock.
func (t *patternTenantState) snapshot(s *patternSelectorState) patternStateSnapshot {
	snap := patternStateSnapshot{V: patternStateSnapshotVersion, FromNs: s.fromNs, ToNs: s.toNs, Newest: t.newest, AtNewest: t.atNewest, Folded: map[string][]patternFoldedSnapshot{}}
	for stream := range s.streams {
		snap.Streams = append(snap.Streams, stream)
	}
	sort.Strings(snap.Streams)
	keyOf := make(map[*patternClusterer]patternMinerKey, len(t.miner.miners))
	for mk, c := range t.miner.miners {
		keyOf[c] = mk
	}
	for _, c := range t.miner.order {
		mk := keyOf[c]
		cs := patternClustererSnapshot{Stream: mk.stream, Level: c.level, Clock: c.clock, Admitted: c.admitted, Evicted: c.evicted}
		index := make(map[*patternBucket]int, len(c.live))
		for i, cluster := range c.live {
			index[cluster] = i
			cs.Clusters = append(cs.Clusters, patternClusterSnapshot{Tokens: cluster.tokens, Spaces: cluster.spacesAfter, Format: cluster.format, Buckets: cluster.buckets, Total: cluster.total, Used: cluster.used})
		}
		keys := make([]patternTreeKey, 0, len(c.trees))
		for key := range c.trees {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].format != keys[j].format {
				return keys[i].format < keys[j].format
			}
			return keys[i].tokens < keys[j].tokens
		})
		for _, key := range keys {
			cs.Trees = append(cs.Trees, patternTreeSnapshot{Format: key.format, Tokens: key.tokens, Root: snapshotPatternNode(c.trees[key], index)})
		}
		snap.Miners = append(snap.Miners, cs)
	}
	for stream, clusters := range t.folded {
		for _, f := range clusters {
			snap.Folded[stream] = append(snap.Folded[stream], patternFoldedSnapshot{Level: f.level, Pattern: f.pattern, Samples: f.samples})
		}
	}
	return snap
}

func snapshotPatternNode(n *patternTreeNode, index map[*patternBucket]int) patternNodeSnapshot {
	var out patternNodeSnapshot
	for _, cluster := range n.clusters {
		if i, ok := index[cluster]; ok {
			out.Clusters = append(out.Clusters, i)
		}
	}
	if n.first == nil {
		return out
	}
	out.Keys = append(out.Keys, n.firstKey)
	out.Children = append(out.Children, snapshotPatternNode(n.first, index))
	more := make([]string, 0, len(n.more))
	for key := range n.more {
		more = append(more, key)
	}
	sort.Strings(more)
	for _, key := range more {
		out.Keys = append(out.Keys, key)
		out.Children = append(out.Children, snapshotPatternNode(n.more[key], index))
	}
	return out
}

// restore makes the tenant state the snapshot's, its selector the given one; the caller holds the state's lock.
func (t *patternTenantState) restore(selector string, snap patternStateSnapshot) *patternSelectorState {
	t.miner = newPatternMiner()
	t.newest = snap.Newest
	if t.newest == nil {
		t.newest = map[string]int64{}
	}
	t.atNewest = snap.AtNewest
	if t.atNewest == nil {
		t.atNewest = map[string]map[uint64]int{}
	}
	for _, cs := range snap.Miners {
		c := newPatternClusterer(cs.Level)
		c.clock, c.admitted, c.evicted = cs.Clock, cs.Admitted, cs.Evicted
		clusters := make([]*patternBucket, len(cs.Clusters))
		for i, cl := range cs.Clusters {
			buckets := cl.Buckets
			if buckets == nil {
				buckets = map[int64]int{}
			}
			clusters[i] = &patternBucket{level: cs.Level, buckets: buckets, total: cl.Total, tokens: cl.Tokens, spacesAfter: cl.Spaces, format: cl.Format, used: cl.Used}
		}
		c.live = clusters
		for _, tree := range cs.Trees {
			c.trees[patternTreeKey{format: tree.Format, tokens: tree.Tokens}] = restorePatternNode(tree.Root, clusters)
		}
		t.miner.miners[patternMinerKey{stream: cs.Stream, level: cs.Level}] = c
		t.miner.order = append(t.miner.order, c)
	}
	t.folded = map[string][]patternFoldedCluster{}
	for stream, clusters := range snap.Folded {
		for _, f := range clusters {
			t.folded[stream] = append(t.folded[stream], patternFoldedCluster{level: f.Level, pattern: f.Pattern, samples: f.Samples})
		}
	}
	s := &patternSelectorState{fromNs: snap.FromNs, toNs: snap.ToNs, streams: make(map[string]struct{}, len(snap.Streams))}
	for _, stream := range snap.Streams {
		s.streams[stream] = struct{}{}
	}
	t.selectors = map[string]*patternSelectorState{selector: s}
	return s
}

func restorePatternNode(ns patternNodeSnapshot, clusters []*patternBucket) *patternTreeNode {
	n := &patternTreeNode{}
	for _, i := range ns.Clusters {
		if i >= 0 && i < len(clusters) {
			n.clusters = append(n.clusters, clusters[i])
			clusters[i].leaf = n
		}
	}
	for i, key := range ns.Keys {
		if i >= len(ns.Children) {
			break
		}
		child := restorePatternNode(ns.Children[i], clusters)
		if i == 0 {
			n.firstKey, n.first = key, child
			continue
		}
		if n.more == nil {
			n.more = make(map[string]*patternTreeNode, len(ns.Keys)-1)
		}
		n.more[key] = child
	}
	return n
}

// encodePatternStateSnapshot is the snapshot gzipped JSON, as it is cached.
func encodePatternStateSnapshot(snap patternStateSnapshot) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if err := json.NewEncoder(zw).Encode(snap); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodePatternStateSnapshot(data []byte, maxBytes int64) (patternStateSnapshot, bool) {
	var snap patternStateSnapshot
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return snap, false
	}
	defer zr.Close()
	if err := json.NewDecoder(io.LimitReader(zr, maxBytes)).Decode(&snap); err != nil || snap.V != patternStateSnapshotVersion {
		return patternStateSnapshot{}, false
	}
	return snap, true
}

// patternStateSnapshotTTL keeps a snapshot as long as the samples it holds matter: Loki's retention and the hour
// the state starts at.
const patternStateSnapshotTTL = lokiPatternRetention + time.Hour

// writePatternStateSnapshot writes the selector's state to the cache under its key (memory, disk and its replicas
// on the peers), at most every patternStateSnapshotEvery unless now; the caller holds the state's lock.
func (p *Proxy) writePatternStateSnapshot(ctx context.Context, key string, t *patternTenantState, s *patternSelectorState, now bool) {
	if p.cache == nil || key == "" || (!now && time.Since(t.snapshotAt) < patternStateSnapshotEvery) {
		return
	}
	data, err := encodePatternStateSnapshot(t.snapshot(s))
	if err != nil {
		return
	}
	t.snapshotAt = time.Now()
	p.cache.SetWithTTL(key, data, patternStateSnapshotTTL)
	p.observeInternalOperation(ctx, "patterns_state_snapshot", "written", 0)
}

// loadPatternStateSnapshot restores the selector's state from the cache (memory, disk, then the key's replicas on
// the peers) when its holder wrote one; nil when there is none or it is unreadable. The caller holds the state's
// lock.
func (p *Proxy) loadPatternStateSnapshot(ctx context.Context, key, selector string, t *patternTenantState) *patternSelectorState {
	if p.cache == nil || key == "" {
		return nil
	}
	data, ok := p.cache.Get(key)
	if !ok {
		return nil
	}
	snap, ok := decodePatternStateSnapshot(data, 1<<30)
	if !ok {
		return nil
	}
	t.snapshotAt = time.Now()
	p.observeInternalOperation(ctx, "patterns_state_snapshot", "restored", 0)
	return t.restore(selector, snap)
}
