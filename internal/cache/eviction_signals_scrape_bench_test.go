package cache

// benchScrape reads what a metrics scrape reads from one cache: the layer
// snapshots and the eviction signals.
func benchScrape(c *Cache) {
	_ = c.LayerStats()
	_ = c.SignalStats()
}
