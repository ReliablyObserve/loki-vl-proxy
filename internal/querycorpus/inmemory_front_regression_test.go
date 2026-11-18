package querycorpus_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// TestReplayHarnessesUseNoNetwork holds the invariant behind the flaky differentials: a request replayed through a
// harness (a fresh proxy in front of a fake VictoriaLogs) opens no TCP connection. Over the network every replayed run
// held a port and a client socket in TIME_WAIT, a few thousand runs used up the machine's ephemeral ports and the
// rows after them answered 500 "can't assign requested address" (TestUnwrapDynamicValueFormsMatchLoki and
// TestUnwrapIdentityKeyOrderMatchLoki, whichever ran when the pool was empty).
func TestReplayHarnessesUseNoNetwork(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	prof := diffProfiles()[0]
	before := realFrontDials.Load()

	const q = `sum by (app) (rate({job="vf"} [1m]))`
	p, err := planOfText(q)
	if err != nil {
		t.Fatal(err)
	}
	mr := metricRequests(q, firstRangeOf(p.Root))[0]
	for range 3 {
		run := runMetricHandler(t, prof, mr, &metricFront{version: prof.version, answers: map[string]metricVLAnswer{}})
		if len(run.sent) == 0 {
			t.Fatalf("runMetricHandler: the fake VictoriaLogs saw no request, the proxy did not reach it")
		}
	}

	run := runDiffHandler(t, prof, `{job="vf"}`, []map[string]string{{"_msg": "x", "_time": "2026-01-01T00:00:00Z", "job": "vf"}}, false)
	if len(run.sent) == 0 {
		t.Fatalf("runDiffHandler: the fake VictoriaLogs saw no request, the proxy did not reach it")
	}

	serve := newJLProxy(t, prof, jlVariants[0], jlModes()[0], &metricFront{version: prof.version, answers: map[string]metricVLAnswer{}})
	if got := serve(mr); len(got.sent) == 0 {
		t.Fatalf("newJLProxy: the fake VictoriaLogs saw no request, the proxy did not reach it")
	}
	serve(metricRequest{})

	if dials := realFrontDials.Load() - before; dials != 0 {
		t.Fatalf("the replay harnesses opened %d TCP connection(s); every fake VictoriaLogs must be served in memory (newMemoryFront)", dials)
	}
}
