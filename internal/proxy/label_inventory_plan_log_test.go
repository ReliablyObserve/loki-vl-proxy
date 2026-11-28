package proxy

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"
)

// inventoryPlanLogProxy is a labels endpoint over a fake VictoriaLogs whose request lines go to the returned buffer.
func inventoryPlanLogProxy(t *testing.T, p *Proxy) (http.Handler, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	p.log = slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	return p.requestLogger("labels", "/loki/api/v1/labels", p.handleLabels), &buf
}

func inventoryPlanLabels(t *testing.T, h http.Handler, start, end time.Time) {
	t.Helper()
	q := url.Values{"start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels?"+q.Encode(), nil)
	req.Header.Set("X-Scope-OrgID", "0")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/labels: %d %s", rec.Code, rec.Body.String())
	}
}

// lastRequestLine is the last request line written to buf.
func lastRequestLine(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	lines := slowLogLines(t, buf, "request")
	if len(lines) == 0 {
		t.Fatalf("no request line in\n%s", buf)
	}
	return lines[len(lines)-1]
}

// inventoryPlanCounts reads the flow.inventory fields of a request line; present reports whether it has any.
func inventoryPlanCounts(t *testing.T, line map[string]any) (counts map[string]int, present bool) {
	t.Helper()
	counts = map[string]int{}
	for _, name := range flowInventoryNames {
		v, ok := line[name]
		if !ok {
			continue
		}
		f, isNum := v.(float64)
		if !isNum {
			t.Fatalf("%s is %T in %v", name, v, line)
		}
		counts[name] = int(f)
		present = true
	}
	if present && len(counts) != len(flowInventoryNames) {
		t.Fatalf("the request line has only some of the bucket plan fields: %v", counts)
	}
	return counts, present
}

// The request line of /labels carries the bucket plan of its metadata inventory listing (issue #1821 item 3): how
// many listings the inventory planned, whether one was answered from memory, and how many of its buckets came from
// the cache, were confirmed by a row count or were read from VictoriaLogs, and how many unaligned edges it read. A
// cold window reads every piece from VictoriaLogs, a window moved inside it reuses its cached buckets, and a window
// the memory inventory answers says so; the pieces read are the VictoriaLogs listing calls the request made.
//
// conformance: observability-query-flow, semantics/label-browser-cold-first-listing, loki_api_v1_labels
func TestLabelsRequestLineCarriesTheInventoryBucketPlan(t *testing.T) {
	end0 := labelBrowserEnd(12, 26)
	now := pinnedNow(end0)
	vl := &fakeVictoriaLogs{}
	seedStableLabels(vl, now)
	const listing = "/select/logsql/stream_field_names"
	num := func(c map[string]int, name string) int { return c["flow.inventory."+name] }

	t.Run("cold window, then a window moved inside it", func(t *testing.T) {
		p := newPinnedLabelProxy(t, vl, 0, now) // no memory answers: every listing reads its buckets
		h, buf := inventoryPlanLogProxy(t, p)
		_, _ = vl.reset()
		inventoryPlanLabels(t, h, end0.Add(-6*time.Hour), end0)
		calls, _ := vl.reset()
		cold, ok := inventoryPlanCounts(t, lastRequestLine(t, buf))
		if !ok {
			t.Fatal("a /labels listing over 6h left no bucket plan on its request line")
		}
		if num(cold, "listings") != 1 || num(cold, "memory") != 0 || num(cold, "buckets_cached") != 0 || num(cold, "buckets_revalidated") != 0 {
			t.Fatalf("cold window: %v", cold)
		}
		if num(cold, "buckets_scanned") == 0 || num(cold, "edges") == 0 {
			t.Fatalf("cold window read no bucket or no edge: %v", cold)
		}
		if got := num(cold, "buckets_scanned") + num(cold, "edges"); got != calls[listing] {
			t.Fatalf("cold window: %d pieces read, %d %s calls (%v)", got, calls[listing], listing, cold)
		}

		inventoryPlanLabels(t, h, end0.Add(-6*time.Hour+5*time.Minute), end0.Add(-5*time.Minute))
		calls, _ = vl.reset()
		moved, _ := inventoryPlanCounts(t, lastRequestLine(t, buf))
		if num(moved, "listings") != 1 || num(moved, "buckets_cached") == 0 {
			t.Fatalf("moved window reused no cached bucket: %v", moved)
		}
		if got := num(moved, "buckets_scanned") + num(moved, "edges"); got != calls[listing] {
			t.Fatalf("moved window: %d pieces read, %d %s calls (%v)", got, calls[listing], listing, moved)
		}
	})

	t.Run("window answered from memory", func(t *testing.T) {
		p := newPinnedLabelProxy(t, vl, 10*time.Second, now)
		h, buf := inventoryPlanLogProxy(t, p)
		inventoryPlanLabels(t, h, end0.Add(-6*time.Hour), end0)
		settleBackground(t, p)
		_, _ = vl.reset()
		end := end0.Add(-317 * time.Second)
		inventoryPlanLabels(t, h, end.Add(-6*time.Hour), end)
		calls, _ := vl.reset()
		mem, _ := inventoryPlanCounts(t, lastRequestLine(t, buf))
		if num(mem, "listings") != 1 || num(mem, "memory") != 1 || calls[listing] != 0 {
			t.Fatalf("moved window from memory: %v, %d %s calls", mem, calls[listing], listing)
		}
		if num(mem, "buckets_scanned")+num(mem, "edges")+num(mem, "buckets_cached")+num(mem, "buckets_revalidated") != 0 {
			t.Fatalf("a memory answer reads no piece: %v", mem)
		}
	})

	t.Run("no inventory listing, no fields", func(t *testing.T) {
		p := newPinnedLabelProxy(t, vl, 0, now)
		h, buf := inventoryPlanLogProxy(t, p)
		// Shorter than the smallest window the inventory splits: one VictoriaLogs call, no bucket plan.
		inventoryPlanLabels(t, h, end0.Add(-5*time.Minute), end0)
		if c, ok := inventoryPlanCounts(t, lastRequestLine(t, buf)); ok {
			t.Fatalf("a listing the inventory did not plan logged a bucket plan: %v", c)
		}
	})
}

// The counts saturate instead of wrapping.
//
// conformance: observability-query-flow
func TestFlowInventoryCountsSaturate(t *testing.T) {
	var f flowInventory
	f.add(flowInventoryScanned, 300)
	f.add(flowInventoryScanned, 1)
	f.add(flowInventoryEdges, -3)
	if f[flowInventoryScanned] != 255 || f[flowInventoryEdges] != 0 {
		t.Fatalf("counts %v", f)
	}
}
