package proxy

import (
	"sync"
	"time"
)

const (
	// mixedCertMemoTTL bounds how long a derived certificate is reused. It only limits how stale the measured
	// lengths can get (a row longer than its bound goes to the exact complement, never to a wrong answer): the
	// field names behind it are re-read after every scan, whatever the age.
	mixedCertMemoTTL = time.Minute
	// mixedCertMemoMax bounds the entries: one per tenant, selector, class label and window class.
	mixedCertMemoMax = 256
)

// mixedCertEntry is what one window derivation settled: the stored field names it listed and the certified class
// it made of them, nil when the names or lengths do not certify (the request then runs the class itself).
type mixedCertEntry struct {
	names     []string
	certified *mixedJSONRowClass
	expires   time.Time
}

// mixedCertMemo remembers derived certificates so a repeated window, or the next window of the same selector,
// skips the field-name listing and the length measurement before the scan. The scan still ends with a fresh
// field-name listing that must stay within the remembered names, so a name that appeared since the entry was made
// sends the request to the class and drops the entry. A certified class is immutable once built and is shared.
// The zero value is ready to use.
type mixedCertMemo struct {
	mu sync.Mutex
	m  map[string]mixedCertEntry
}

func (c *mixedCertMemo) get(key string, now time.Time) (mixedCertEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[key]
	if !ok || !now.Before(e.expires) {
		delete(c.m, key)
		return mixedCertEntry{}, false
	}
	return e, true
}

func (c *mixedCertMemo) put(key string, e mixedCertEntry, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil {
		c.m = make(map[string]mixedCertEntry, 16)
	}
	if _, present := c.m[key]; !present && len(c.m) >= mixedCertMemoMax {
		for k, old := range c.m {
			if !now.Before(old.expires) {
				delete(c.m, k)
			}
		}
		for k := range c.m {
			if len(c.m) < mixedCertMemoMax {
				break
			}
			delete(c.m, k)
		}
	}
	e.expires = now.Add(mixedCertMemoTTL)
	c.m[key] = e
}

func (c *mixedCertMemo) drop(key string) {
	c.mu.Lock()
	delete(c.m, key)
	c.mu.Unlock()
}

// mixedCertWindowClass names the window length class a certificate is remembered for, so lengths measured over
// an hour are not reused for a day.
func mixedCertWindowClass(window time.Duration) string {
	switch {
	case window <= time.Hour:
		return "1h"
	case window <= 6*time.Hour:
		return "6h"
	case window <= 24*time.Hour:
		return "24h"
	default:
		return "long"
	}
}
