package proxy

import "testing"

// BenchmarkPeerPushAllowed measures the push filter every write-through runs,
// on a long response key without credentials and on a credential-scoped one.
func BenchmarkPeerPushAllowed(b *testing.B) {
	plain := "compat:v2:query_range:line-v5:team-a:/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D+%7C%3D+%22x%22&start=1700000000&end=1700003600&step=60:auth:6b336325b4434ca72acdee58f2189119957aa04a5639cc8b802d611cdfa8873c:profile:default_2tuple/dl2/unknown///ms-1000:qs3:"
	scoped := "compat:v2:query_range:line-v5:team-a:/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=1&end=2&step=60:auth:fwdid.k1.2c7f297801108300503db5b7c5e30e73fedcaec294cacf5bf60fbf39e6a1b2c3:profile:default_2tuple/dl2/unknown///ms-1000:qs3:"
	for _, bc := range []struct{ name, key string }{{"no-credentials", plain}, {"credential-scoped", scoped}} {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = peerPushAllowed(bc.key)
			}
		})
	}
}
