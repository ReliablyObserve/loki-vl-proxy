//go:build !corpuscapture

package querycorpus

import "net/http"

// Record does nothing without the corpuscapture build tag.
func Record(Kind, string) {}

// Handler returns h unchanged without the corpuscapture build tag.
func Handler(h http.Handler) http.Handler { return h }

// RecordRequest does nothing without the corpuscapture build tag.
func RecordRequest(*http.Request, string) {}
