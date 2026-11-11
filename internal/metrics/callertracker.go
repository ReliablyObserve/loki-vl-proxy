package metrics

import (
	"sort"
	"sync"
	"time"
)

// CallerTracker answers "who is loading the proxy or VictoriaLogs" for /debug/queries: the heaviest callers
// (tenant and user) of the last window by VictoriaLogs time and bytes, and the most recent client requests with
// their caller and cost. Memory is bounded by construction: at most callerTrackerMaxCallers callers per window
// half (the rest are summed under one overflow caller) and callerTrackerRecent requests. It is per replica and in
// memory only; users never label a metric.
type CallerTracker struct {
	mu         sync.Mutex
	window     time.Duration
	maxCallers int
	start      time.Time // start of the current half
	cur, prev  map[callerKey]*CallerSummary
	recent     []CallerRequest // ring
	next       int
	now        func() time.Time
}

const (
	// callerTrackerWindow is one half of the top-callers window: the view covers the current half and the one
	// before it, so between one and two windows of traffic.
	callerTrackerWindow = 15 * time.Minute
	// callerTrackerMaxCallers bounds the distinct callers kept per half.
	callerTrackerMaxCallers = 1000
	// callerTrackerRecent is the number of recent requests kept.
	callerTrackerRecent = 256
	// callerTrackerQueryBytes caps a recent request's query text.
	callerTrackerQueryBytes = 512
	// CallerOverflow names the caller that sums every caller past callerTrackerMaxCallers.
	CallerOverflow = "__overflow__"
)

type callerKey struct{ tenant, user string }

// CallerCost is what client requests cost: the requests, their errors and slow ones, their time, and the
// VictoriaLogs calls they made with the time, bytes and log rows those took.
type CallerCost struct {
	Requests   int64 `json:"requests"`
	Errors     int64 `json:"errors"`
	Slow       int64 `json:"slow"`
	DurationMs int64 `json:"duration_ms"`
	VLCalls    int64 `json:"vl_calls"`
	VLMs       int64 `json:"vl_ms"`
	VLBytes    int64 `json:"vl_bytes"`
	VLRows     int64 `json:"vl_rows"`
}

func (c *CallerCost) add(o CallerCost) {
	c.Requests += o.Requests
	c.Errors += o.Errors
	c.Slow += o.Slow
	c.DurationMs += o.DurationMs
	c.VLCalls += o.VLCalls
	c.VLMs += o.VLMs
	c.VLBytes += o.VLBytes
	c.VLRows += o.VLRows
}

// CallerRequest is one finished client request: who sent it, what it asked and what it cost.
type CallerRequest struct {
	Time          int64  `json:"time"` // unix millis
	Tenant        string `json:"tenant"`
	User          string `json:"user"`
	UserSource    string `json:"user_source"`
	GrafanaSource string `json:"grafana_source,omitempty"`
	Endpoint      string `json:"endpoint"`
	Route         string `json:"route"`
	Status        int    `json:"status"`
	Query         string `json:"query,omitempty"`
	CallerCost
}

// CallerSummary is one caller's cost over the window.
type CallerSummary struct {
	Tenant            string `json:"tenant"`
	User              string `json:"user"`
	UserSource        string `json:"user_source"`
	LastGrafanaSource string `json:"last_grafana_source,omitempty"`
	LastSeen          int64  `json:"last_seen"` // unix millis
	CallerCost
}

// CallerView is the callers part of /debug/queries.
type CallerView struct {
	WindowSeconds  int64           `json:"window_seconds"`
	Since          int64           `json:"since"` // unix millis: the oldest traffic the top lists cover
	Callers        int             `json:"callers"`
	Overflowed     bool            `json:"overflowed"`
	TopByVLMs      []CallerSummary `json:"top_by_vl_ms"`
	TopByVLBytes   []CallerSummary `json:"top_by_vl_bytes"`
	RecentRequests []CallerRequest `json:"recent_requests"`
}

// NewCallerTracker returns an empty tracker.
func NewCallerTracker() *CallerTracker {
	return newCallerTracker(callerTrackerWindow, callerTrackerMaxCallers, callerTrackerRecent, time.Now)
}

func newCallerTracker(window time.Duration, maxCallers, recent int, now func() time.Time) *CallerTracker {
	return &CallerTracker{
		window:     window,
		maxCallers: maxCallers,
		start:      now(),
		cur:        make(map[callerKey]*CallerSummary),
		recent:     make([]CallerRequest, 0, recent),
		now:        now,
	}
}

// Record adds a finished client request.
func (ct *CallerTracker) Record(req CallerRequest) {
	if ct == nil {
		return
	}
	if len(req.Query) > callerTrackerQueryBytes {
		req.Query = req.Query[:callerTrackerQueryBytes] + "..."
	}
	now := ct.now()
	req.Time = now.UnixMilli()
	req.Requests = 1
	ct.mu.Lock()
	defer ct.mu.Unlock()
	ct.rotate(now)
	key := callerKey{req.Tenant, req.User}
	s, ok := ct.cur[key]
	if !ok {
		if len(ct.cur) >= ct.maxCallers {
			key = callerKey{CallerOverflow, CallerOverflow}
			s = ct.cur[key]
		}
		if s == nil {
			s = &CallerSummary{Tenant: key.tenant, User: key.user, UserSource: req.UserSource}
			if key.tenant == CallerOverflow {
				s.UserSource = CallerOverflow
			}
			ct.cur[key] = s
		}
	}
	s.CallerCost.add(req.CallerCost)
	s.LastSeen = req.Time
	if req.GrafanaSource != "" {
		s.LastGrafanaSource = req.GrafanaSource
	}
	if len(ct.recent) < cap(ct.recent) {
		ct.recent = append(ct.recent, req)
	} else if len(ct.recent) > 0 {
		ct.recent[ct.next] = req
	}
	if cap(ct.recent) > 0 {
		ct.next = (ct.next + 1) % cap(ct.recent)
	}
}

// rotate starts a new half once the current one is a window old; the view forgets the half before.
func (ct *CallerTracker) rotate(now time.Time) {
	age := now.Sub(ct.start)
	if age < ct.window {
		return
	}
	if age >= 2*ct.window {
		ct.prev = nil
	} else {
		ct.prev = ct.cur
	}
	ct.cur = make(map[callerKey]*CallerSummary, len(ct.cur))
	ct.start = now
}

// View returns the top n callers by VictoriaLogs time and by bytes and the recent requests, newest first, of the
// callers matching tenant and user ("" matches every one).
func (ct *CallerTracker) View(tenant, user string, n int) CallerView {
	if ct == nil {
		return CallerView{}
	}
	ct.mu.Lock()
	ct.rotate(ct.now())
	since := ct.start
	if ct.prev != nil {
		since = ct.start.Add(-ct.window)
	}
	merged := make(map[callerKey]*CallerSummary, len(ct.cur)+len(ct.prev))
	for _, half := range []map[callerKey]*CallerSummary{ct.prev, ct.cur} {
		for k, s := range half {
			if m, ok := merged[k]; ok {
				m.CallerCost.add(s.CallerCost)
				m.LastSeen = max(m.LastSeen, s.LastSeen)
				if s.LastGrafanaSource != "" {
					m.LastGrafanaSource = s.LastGrafanaSource
				}
				continue
			}
			c := *s
			merged[k] = &c
		}
	}
	recent := make([]CallerRequest, 0, len(ct.recent))
	for i := 0; i < len(ct.recent); i++ {
		// Newest first: the ring's last write is just before next.
		r := ct.recent[(ct.next-1-i+2*len(ct.recent))%len(ct.recent)]
		if callerMatches(r.Tenant, r.User, tenant, user) {
			recent = append(recent, r)
		}
	}
	ct.mu.Unlock()

	view := CallerView{WindowSeconds: int64(2 * ct.window / time.Second), Since: since.UnixMilli(), RecentRequests: recent}
	all := make([]CallerSummary, 0, len(merged))
	for k, s := range merged {
		if k.tenant == CallerOverflow {
			view.Overflowed = true
		}
		if callerMatches(s.Tenant, s.User, tenant, user) {
			all = append(all, *s)
		}
	}
	view.Callers = len(all)
	view.TopByVLMs = topCallers(all, n, func(a, b CallerSummary) bool { return a.VLMs > b.VLMs })
	view.TopByVLBytes = topCallers(all, n, func(a, b CallerSummary) bool { return a.VLBytes > b.VLBytes })
	return view
}

func callerMatches(tenant, user, wantTenant, wantUser string) bool {
	return (wantTenant == "" || tenant == wantTenant) && (wantUser == "" || user == wantUser)
}

func topCallers(all []CallerSummary, n int, more func(a, b CallerSummary) bool) []CallerSummary {
	sorted := append([]CallerSummary(nil), all...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if more(sorted[i], sorted[j]) {
			return true
		}
		if more(sorted[j], sorted[i]) {
			return false
		}
		return sorted[i].DurationMs > sorted[j].DurationMs
	})
	if n < len(sorted) {
		sorted = sorted[:n]
	}
	return sorted
}
