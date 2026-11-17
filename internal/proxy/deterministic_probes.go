package proxy

import "context"

// deterministicProbesKey marks a request context whose concurrent probes all run out before a verdict is read.
// Where the proxy races probes (anyRisk, the checks beside an ordered-JSON stats attempt, the logfmt candidate
// probe), the first risky verdict normally cancels the others, so which of them reach VictoriaLogs depends on
// timing: against an answer as fast as a test stub's, a cancel can overtake a sibling not yet sent. A marked
// request waits for every probe it started and reads the verdicts in the probes' order, so its VictoriaLogs calls
// are the same on every run: every probe the request starts, as against a VictoriaLogs whose answers take longer
// than sending the siblings. The verdict is the same either way (risky when any probe is risky). Only the route
// parity harness marks a request (plan_route_parity_test.go); nothing in the proxy does.
type deterministicProbesKey struct{}

// deterministicProbes reports whether ctx is marked with deterministicProbesKey.
func deterministicProbes(ctx context.Context) bool {
	marked, _ := ctx.Value(deterministicProbesKey{}).(bool)
	return marked
}
