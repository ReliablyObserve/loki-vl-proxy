package proxy

// Handler is the HTTP handler for all Loki-compatible endpoints.
// It carries the three concern groups: external deps, immutable config, mutable state.
// Handler methods can be constructed in tests with mocked Deps without a full Proxy.
//
// In this PR, Handler is defined but handler methods still have *Proxy receivers.
// Task 9 completes the receiver migration.
type Handler struct {
	Deps
	Cfg   *HandlerConfig
	State *State
}
