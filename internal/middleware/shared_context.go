package middleware

import (
	"context"
	"sync"
)

// SharedContext is the context of one upstream call that several waiters
// share. It keeps the values of the context that started the call (tenant,
// headers, telemetry) but is cancelled only when the last waiter leaves, so
// the upstream scan stops with its last client and never because the first
// waiter, the one that happened to start the call, went away.
type SharedContext struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	waiters int
	left    bool // the last waiter left; the call is cancelled and takes no more waiters
}

// NewSharedContext starts a shared call with first as its only waiter. When
// keepDeadline is set the call inherits first's deadline, so a caller that
// bounds its request keeps bounding the call.
func NewSharedContext(first context.Context, keepDeadline bool) *SharedContext {
	base := context.WithoutCancel(first)
	s := &SharedContext{waiters: 1}
	if d, ok := first.Deadline(); ok && keepDeadline {
		s.ctx, s.cancel = context.WithDeadline(base, d)
	} else {
		s.ctx, s.cancel = context.WithCancel(base)
	}
	return s
}

// Context is the context to run the shared upstream call with.
func (s *SharedContext) Context() context.Context { return s.ctx }

// Join adds a waiter. It reports false when the last waiter already left: the
// call is cancelled, so the caller must start a call of its own.
func (s *SharedContext) Join() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.left {
		return false
	}
	s.waiters++
	return true
}

// Leave removes a waiter that stopped waiting and cancels the call when it was
// the last one.
func (s *SharedContext) Leave() {
	s.mu.Lock()
	s.waiters--
	last := s.waiters == 0
	if last {
		s.left = true
	}
	s.mu.Unlock()
	if last {
		s.cancel()
	}
}

// Close releases the call's resources once it has finished.
func (s *SharedContext) Close() {
	s.mu.Lock()
	s.left = true
	s.mu.Unlock()
	s.cancel()
}
