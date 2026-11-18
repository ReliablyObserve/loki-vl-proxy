package cache

import (
	"container/list"
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// ErrorClass is the class of a failed answer, which decides how long it may be
// held. The query plan says which classes a request can fail in; the code
// that failed says which one it did (by the error's type, never its text).
type ErrorClass uint8

const (
	// ErrorNever is a failure that is never held or shared: a client's own
	// cancellation or deadline, a refusal by the proxy's own load shedding, an
	// auth failure, a proxy defect, or a failure the plan says the request
	// cannot have.
	ErrorNever ErrorClass = iota
	// ErrorTransient is a failure of the backend (a 5xx, a timeout, a refused
	// or broken connection, an overloaded backend): held for seconds, to absorb
	// a storm of identical requests, and answered again as soon as it expires.
	ErrorTransient
	// ErrorData is a failure read from the data in the request's window (a
	// pipeline error over the lines read, a series or line limit reached):
	// the same request can answer once the data changes, so it is held short.
	ErrorData
	// ErrorRequest is a failure the request alone decides (a query or a
	// parameter the proxy refuses as Loki does, whatever the data).
	ErrorRequest
	// ErrorRefusal is VictoriaLogs refusing the query the proxy wrote before
	// reading any data (a parse error, a stats shape it does not run, a query
	// over its length limit): the same text on the same VictoriaLogs release
	// always gets it, so it is held long, keyed by that release.
	ErrorRefusal
	errorClassCount
)

var errorClassNames = [errorClassCount]string{"never", "transient", "data", "request", "vl_refusal"}

// String is the class's name, for metrics and logs.
func (c ErrorClass) String() string {
	if c < errorClassCount {
		return errorClassNames[c]
	}
	return "never"
}

// ErrorClasses lists the classes that can be held, for metrics.
func ErrorClasses() []ErrorClass {
	return []ErrorClass{ErrorTransient, ErrorData, ErrorRequest, ErrorRefusal}
}

// Policy is how long a failed answer of one request may be held, per class.
// It is built from the request's plan (which classes the request can fail in)
// and the operator's TTLs; a zero TTL holds nothing of that class, and a class
// the plan rules out has a zero TTL.
type Policy struct {
	ttl      [errorClassCount]time.Duration
	excluded [errorClassCount]bool
}

// NewPolicy is a policy holding each class for its TTL.
func NewPolicy(transient, data, request time.Duration) Policy {
	var p Policy
	p.ttl[ErrorTransient], p.ttl[ErrorData], p.ttl[ErrorRequest] = transient, data, request
	return p
}

// WithRefusal is p holding VictoriaLogs' refusals (ErrorRefusal) for ttl.
func (p Policy) WithRefusal(ttl time.Duration) Policy {
	p.ttl[ErrorRefusal] = ttl
	return p
}

// TTL is how long an answer of class c may be held; zero when it is not held.
func (p Policy) TTL(c ErrorClass) time.Duration {
	if c == ErrorNever || c >= errorClassCount || p.ttl[c] < 0 {
		return 0
	}
	return p.ttl[c]
}

// Only is p without the classes outside allowed.
func (p Policy) Only(allowed func(ErrorClass) bool) Policy {
	for c := ErrorTransient; c < errorClassCount; c++ {
		if !allowed(c) {
			p.ttl[c], p.excluded[c] = 0, true
		}
	}
	return p
}

// Shares reports whether a failure of class c may be handed to the requests
// waiting on the same call. Every class but ErrorNever is: the followers asked
// the same thing at the same moment, so a zero TTL (nothing held afterwards)
// does not stop them sharing the call they waited for. A class the plan rules
// out is held by no policy; it is shared only when the caller allows it.
func (p Policy) Shares(c ErrorClass) bool {
	return c != ErrorNever && c < errorClassCount && !p.excluded[c]
}

// Answer is an answer exactly as the client read it: the status, the headers
// the request wrote (not the ones each request writes for itself), and the
// body.
type Answer struct {
	Status int
	Header http.Header
	Body   []byte
	Class  ErrorClass
	// Key, when set, is the exact key of the request that produced the
	// answer, for a flight joined on a coarser key: a follower takes the
	// answer only when its own exact key is the same, and a failure is held
	// under it.
	Key string
}

func (a *Answer) size(key string) int {
	n := len(key) + len(a.Body) + 64
	for name, values := range a.Header {
		n += len(name)
		for _, v := range values {
			n += len(v) + 16
		}
	}
	return n
}

// Outcome is how a flight ended, for the requests that waited on it.
type Outcome uint8

const (
	// OutcomeUnshared: the leader's answer is not for the followers (it was
	// cancelled or ran out of its own time, or failed in a class never
	// shared); a follower starts the request again, as the leader of a new
	// flight or a follower of one another follower started.
	OutcomeUnshared Outcome = iota
	// OutcomeReleased: the leader succeeded with an answer that cannot be
	// shared (it streamed, or outgrew what is kept); each follower runs the
	// request on its own, without a flight.
	OutcomeReleased
	// OutcomeStored: the leader's success is in the response cache; a follower
	// reads it there.
	OutcomeStored
	// OutcomeSucceeded: the leader succeeded with an answer shared in memory
	// (a request the response cache does not keep); never held.
	OutcomeSucceeded
	// OutcomeFailed: the leader failed with a shared answer.
	OutcomeFailed
)

// Flight is one request running for every identical request that arrives
// while it runs. The zero value is a flight not started; a caller may embed
// one and hand it to Join, so leading costs no allocation of its own.
type Flight struct {
	mu      sync.Mutex
	done    chan struct{} // made by the first waiter
	ended   bool
	outcome Outcome
	answer  *Answer
	waiters atomic.Int64
	// joined counts the requests that joined the flight (Lookup), so the
	// leader builds a shared answer only when someone will read it.
	joined atomic.Int64
}

// Wait blocks until the leader ends or ctx is done; ok is false when ctx ended
// first.
func (f *Flight) Wait(ctx context.Context) (Outcome, *Answer, bool) {
	f.waiters.Add(1)
	f.mu.Lock()
	if f.ended {
		o, a := f.outcome, f.answer
		f.mu.Unlock()
		return o, a, true
	}
	if f.done == nil {
		f.done = make(chan struct{})
	}
	done := f.done
	f.mu.Unlock()
	select {
	case <-done:
		f.mu.Lock()
		o, a := f.outcome, f.answer
		f.mu.Unlock()
		return o, a, true
	case <-ctx.Done():
		return OutcomeUnshared, nil, false
	}
}

// Waiters is the number of requests that waited on f.
func (f *Flight) Waiters() int64 { return f.waiters.Load() }

// Joined is the number of requests that joined f so far.
func (f *Flight) Joined() int64 { return f.joined.Load() }

// Release lets the followers go before the leader ends: the leader succeeds
// with an answer that cannot be shared (it started streaming, or outgrew what
// can be shared), so each follower runs on its own.
func (f *Flight) Release() { f.end(OutcomeReleased, nil) }

func (f *Flight) end(o Outcome, a *Answer) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ended {
		return
	}
	f.ended, f.outcome, f.answer = true, o, a
	if f.done != nil {
		close(f.done)
	}
}

// ErrorFlights coalesces identical requests in flight and holds failed
// answers for their class's TTL. It is bounded by entries and bytes; the
// oldest held answer is evicted first. It runs no goroutine: an expired answer
// is dropped when it is read or when room is needed.
type ErrorFlights struct {
	mu         sync.Mutex
	flights    map[string]*Flight
	held       map[string]*list.Element
	order      *list.List // of *heldAnswer, oldest first
	bytes      int
	maxEntries int
	maxBytes   int
	// MaxAnswerBytes is the largest body held or shared.
	maxAnswerBytes int
	// join lets identical requests wait on the one in flight; without it every
	// request leads its own flight and only held answers are shared.
	join bool
}

type heldAnswer struct {
	key     string
	answer  *Answer
	expires time.Time
	size    int
}

// NewErrorFlights is an ErrorFlights holding at most maxEntries answers and
// maxBytes bytes, each body at most maxAnswerBytes long; join lets identical
// requests wait on the one in flight.
func NewErrorFlights(maxEntries, maxBytes, maxAnswerBytes int, join bool) *ErrorFlights {
	return &ErrorFlights{
		flights:        make(map[string]*Flight),
		held:           make(map[string]*list.Element),
		order:          list.New(),
		maxEntries:     maxEntries,
		maxBytes:       maxBytes,
		maxAnswerBytes: maxAnswerBytes,
		join:           join,
	}
}

// Lookup returns the answer held for key, or the flight to wait on, or a new
// flight that the caller leads (lead) and must end with Finish.
func (e *ErrorFlights) Lookup(key string) (held *Answer, f *Flight, lead bool) {
	return e.Join(key, nil)
}

// Join is Lookup with own as the flight to lead (a zero Flight the caller
// keeps); a new one is allocated when own is nil.
func (e *ErrorFlights) Join(key string, own *Flight) (held *Answer, f *Flight, lead bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if el, ok := e.held[key]; ok {
		h := el.Value.(*heldAnswer)
		if clockNow().Before(h.expires) {
			return h.answer, nil, false
		}
		e.removeLocked(el)
	}
	if f = e.flights[key]; f != nil {
		f.joined.Add(1)
		return nil, f, false
	}
	f = own
	if f == nil {
		f = new(Flight)
	}
	if e.join {
		e.flights[key] = f
	}
	return nil, f, true
}

// Peek returns the answer held for key, if any, without starting a flight.
func (e *ErrorFlights) Peek(key string) *Answer {
	e.mu.Lock()
	defer e.mu.Unlock()
	el, ok := e.held[key]
	if !ok {
		return nil
	}
	h := el.Value.(*heldAnswer)
	if clockNow().Before(h.expires) {
		return h.answer
	}
	e.removeLocked(el)
	return nil
}

// Hold holds a for key for ttl (a second key for an answer a flight already
// ended with); it returns when the answer expires, or the zero time when it
// is not held.
func (e *ErrorFlights) Hold(key string, a *Answer, ttl time.Duration) time.Time {
	if a == nil || ttl <= 0 || len(a.Body) > e.maxAnswerBytes {
		return time.Time{}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.holdLocked(key, a, ttl)
	if el, ok := e.held[key]; ok {
		return el.Value.(*heldAnswer).expires
	}
	return time.Time{}
}

// HeldHint marks a key: it is never answered.
var HeldHint = &Answer{}

// HeldHintKey is where Finish marks a flight key whose failure is held under
// the leader's exact key (Answer.Key).
func HeldHintKey(join string) string { return join + "\x00held" }

// Drop removes the answer held for key, if any.
func (e *ErrorFlights) Drop(key string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if el, ok := e.held[key]; ok {
		e.removeLocked(el)
	}
}

// Finish ends the flight f for key with the leader's outcome; build makes
// the answer to share, and is called only when a request joined the flight or
// a failure may be held. A failed answer is held for its class's TTL under
// pol. Finish returns the shared answer and how long it is held (zero when it
// is not).
// AnswerBuilder builds the answer a flight ends with, when it is needed.
type AnswerBuilder interface {
	BuildAnswer() *Answer
}

func (e *ErrorFlights) Finish(key string, f *Flight, o Outcome, build AnswerBuilder, pol Policy) (*Answer, time.Duration) {
	var a *Answer
	var ttl time.Duration
	if o == OutcomeFailed {
		// The failure is built while the flight still takes joins, and held in
		// the same step that removes the flight: a request arriving at any
		// moment waits on the flight or reads the held failure, never leads a
		// second one.
		if build != nil {
			a = build.BuildAnswer()
		}
		if a == nil || len(a.Body) > e.maxAnswerBytes || !pol.Shares(a.Class) {
			o, a = OutcomeUnshared, nil
		} else {
			ttl = pol.TTL(a.Class)
		}
	}
	e.mu.Lock()
	if e.flights[key] == f {
		delete(e.flights, key) // no request joins after this
	}
	if ttl > 0 {
		held := key
		if a.Key != "" {
			held = a.Key
		}
		e.holdLocked(held, a, ttl)
		if held != key {
			// Requests joining on the coarser key look for the held failure
			// under their exact key while it is held.
			e.holdLocked(HeldHintKey(key), HeldHint, ttl)
		}
	}
	e.mu.Unlock()
	if o == OutcomeSucceeded {
		if f.joined.Load() > 0 && build != nil {
			a = build.BuildAnswer()
		}
		if a == nil {
			o = OutcomeReleased
		}
	}
	f.end(o, a)
	return a, ttl
}

func (e *ErrorFlights) holdLocked(key string, a *Answer, ttl time.Duration) {
	if el, ok := e.held[key]; ok {
		e.removeLocked(el)
	}
	h := &heldAnswer{key: key, answer: a, expires: clockNow().Add(ttl), size: a.size(key)}
	if h.size > e.maxBytes || e.maxEntries <= 0 {
		return
	}
	for e.order.Len() > 0 && (e.order.Len() >= e.maxEntries || e.bytes+h.size > e.maxBytes) {
		e.removeLocked(e.order.Front())
	}
	e.held[key] = e.order.PushBack(h)
	e.bytes += h.size
}

func (e *ErrorFlights) removeLocked(el *list.Element) {
	h := e.order.Remove(el).(*heldAnswer)
	delete(e.held, h.key)
	e.bytes -= h.size
}

// Clear drops every held answer (a configuration change may answer
// differently). Flights in progress are kept.
func (e *ErrorFlights) Clear() {
	e.mu.Lock()
	e.held = make(map[string]*list.Element)
	e.order.Init()
	e.bytes = 0
	e.mu.Unlock()
}

// Len is the number of held answers and their bytes.
func (e *ErrorFlights) Len() (entries, bytes int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.order.Len(), e.bytes
}

// Waiting is the number of requests that joined the flights in progress.
func (e *ErrorFlights) Waiting() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	var n int64
	for _, f := range e.flights {
		n += f.waiters.Load()
	}
	return n
}
