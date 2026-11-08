//go:build race

package proxy

// routeParityRace is true in a -race build: route parity then runs a sample (the full corpus takes minutes).
const routeParityRace = true
