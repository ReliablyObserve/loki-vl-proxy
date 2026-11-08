//go:build !corpuscapture

package querycorpus

import "net/http"

// Record does nothing without the corpuscapture build tag.
func Record(Kind, string) {}

// PauseRecording does nothing without the corpuscapture build tag; the returned function does nothing either.
func PauseRecording() (resume func()) { return func() {} }

// Handler returns h unchanged without the corpuscapture build tag.
func Handler(h http.Handler) http.Handler { return h }

// RecordRequest does nothing without the corpuscapture build tag.
func RecordRequest(*http.Request, string) {}

// RecordBackend does nothing without the corpuscapture build tag.
func RecordBackend(string, string) {}
