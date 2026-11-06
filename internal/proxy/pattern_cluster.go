package proxy

import (
	"slices"
	"unicode"
)

const (
	// patternTreeMaxDepth is how many of a line's leading tokens lead to its candidate clusters.
	patternTreeMaxDepth = 28
	// patternTreeMaxChildren is how many children a node of the tree has at most before new tokens share its
	// variable child.
	patternTreeMaxChildren = 15
	// patternMaxClusters is how many clusters one miner (the lines of one stream at one level) keeps: a cluster
	// made when it holds that many replaces the one used least recently, and its samples go with it. Loki's
	// pattern ingester keeps no more per stream either.
	patternMaxClusters = 300
	// patternMaxEvictionShare is the share of the lines a miner admitted that its evictions may reach: once they
	// exceed it, the lines are too varied to be worth mining and the miner admits no more lines for the rest of
	// the mining pass, as Loki's pattern ingester stops mining such a stream.
	patternMaxEvictionShare = 0.25
)

// patternTreeNode is a node of the prefix tree that leads a line to the clusters it may join. A child is keyed by
// the token that leads to it; the child keyed patternVarPlaceholder is the node's variable child, taken by tokens
// no other child holds. Clusters are registered at the node their first line's tokens lead to.
type patternTreeNode struct {
	children map[string]*patternTreeNode
	clusters []*patternBucket
}

// patternTreeKey names one tree of a miner: lines only share clusters with lines of their format and token count.
type patternTreeKey struct {
	format patternFormat
	tokens int
}

// patternClusterer groups the lines of one stream at one level into clusters, each cluster a template of tokens,
// as the lines arrive: a line joins the cluster it is most like, if it is like one, and otherwise starts a
// cluster of its own. It keeps at most patternMaxClusters, and stops admitting lines once it has had to evict too
// many.
type patternClusterer struct {
	level string
	trees map[patternTreeKey]*patternTreeNode
	// live is the clusters kept, in the order they were made, which keeps answers the same from one run to the next.
	live []*patternBucket
	// clock stamps each use of a cluster; the least recently used has the lowest stamp.
	clock uint64
	// admitted is the lines trained, evicted the clusters replaced.
	admitted, evicted int
}

func newPatternClusterer(level string) *patternClusterer {
	return &patternClusterer{level: level, trees: make(map[patternTreeKey]*patternTreeNode)}
}

// train reads one line's tokens and returns the cluster it joined or started, or nil when the clusterer admits no
// more lines. The clusterer keeps tokens and spacesAfter for a cluster it starts: the caller hands them over.
func (c *patternClusterer) train(tokens []string, spacesAfter []int, format patternFormat) *patternBucket {
	if float64(c.evicted) > patternMaxEvictionShare*float64(c.admitted) {
		return nil
	}
	c.admitted++
	c.clock++
	key := patternTreeKey{format: format, tokens: len(tokens)}
	root := c.trees[key]
	if root == nil {
		root = &patternTreeNode{}
		c.trees[key] = root
	}
	depth := max(0, min(patternTreeMaxDepth, len(tokens)-1))
	if cluster := bestPatternCluster(root.leaf(tokens, depth).candidates(), tokens); cluster != nil {
		for i, token := range tokens {
			if cluster.tokens[i] != token {
				cluster.tokens[i] = patternVarPlaceholder
			}
		}
		cluster.used = c.clock
		return cluster
	}
	if len(c.live) >= patternMaxClusters {
		c.evictLeastRecentlyUsed()
	}
	node := root.place(tokens, depth)
	cluster := &patternBucket{
		level:       c.level,
		buckets:     make(map[int64]int),
		tokens:      tokens,
		spacesAfter: spacesAfter,
		format:      format,
		leaf:        node,
		used:        c.clock,
	}
	node.clusters = append(node.clusters, cluster)
	c.live = append(c.live, cluster)
	return cluster
}

// evictLeastRecentlyUsed drops the cluster used least recently, the one made first among equals, with its samples.
func (c *patternClusterer) evictLeastRecentlyUsed() {
	oldest := 0
	for i, cluster := range c.live {
		if cluster.used < c.live[oldest].used {
			oldest = i
		}
	}
	gone := c.live[oldest]
	c.live = slices.Delete(c.live, oldest, oldest+1)
	if at := slices.Index(gone.leaf.clusters, gone); at >= 0 {
		gone.leaf.clusters = slices.Delete(gone.leaf.clusters, at, at+1)
	}
	c.evicted++
}

func (n *patternTreeNode) candidates() []*patternBucket {
	if n == nil {
		return nil
	}
	return n.clusters
}

// leaf is the node a line's first depth tokens lead to, or nil when it has no such node: at each level the child
// of the line's token, else the variable child.
func (n *patternTreeNode) leaf(tokens []string, depth int) *patternTreeNode {
	for _, token := range tokens[:depth] {
		next := n.children[token]
		if next == nil {
			next = n.children[patternVarPlaceholder]
		}
		if next == nil {
			return nil
		}
		n = next
	}
	return n
}

// place is the node a new cluster's first depth tokens lead to, made as needed. A token that has a node of its own
// follows it. One with a number in it is a value that changes: it takes the variable child. Any other token gets a
// node of its own while the node has room for it, and the variable child once it does not, the last room going to
// the variable child itself.
func (n *patternTreeNode) place(tokens []string, depth int) *patternTreeNode {
	for _, token := range tokens[:depth] {
		next := n.children[token]
		if next == nil {
			variable := n.children[patternVarPlaceholder]
			children := len(n.children)
			switch {
			case patternTokenHasNumber(token):
				next = n.child(patternVarPlaceholder)
			case variable != nil && children < patternTreeMaxChildren,
				variable == nil && children+1 < patternTreeMaxChildren:
				next = n.child(token)
			default:
				next = n.child(patternVarPlaceholder)
			}
		}
		n = next
	}
	return n
}

// child is the node's child for a key, made when it has none.
func (n *patternTreeNode) child(key string) *patternTreeNode {
	if next := n.children[key]; next != nil {
		return next
	}
	if n.children == nil {
		n.children = make(map[string]*patternTreeNode)
	}
	next := &patternTreeNode{}
	n.children[key] = next
	return next
}

// patternTokenHasNumber reports whether a token holds a digit or any other numeric character.
func patternTokenHasNumber(token string) bool {
	for _, r := range token {
		if unicode.IsNumber(r) {
			return true
		}
	}
	return false
}

// bestPatternCluster is the candidate the line's tokens join, if any. A candidate scores the share of the line's
// tokens it holds as they are, its variables never counting; the best score wins, then the candidate with more
// variables, then the one made first. The line joins only a candidate that scores patternSimThreshold or more.
func bestPatternCluster(candidates []*patternBucket, tokens []string) *patternBucket {
	var best *patternBucket
	bestSame, bestVariables := -1, -1
	for _, cluster := range candidates {
		same, variables := 0, 0
		for i, token := range cluster.tokens {
			switch token {
			case patternVarPlaceholder:
				variables++
			case tokens[i]:
				same++
			}
		}
		if same > bestSame || same == bestSame && variables > bestVariables {
			best, bestSame, bestVariables = cluster, same, variables
		}
	}
	if best == nil || float64(bestSame)/float64(len(tokens)) < patternSimThreshold {
		return nil
	}
	return best
}
