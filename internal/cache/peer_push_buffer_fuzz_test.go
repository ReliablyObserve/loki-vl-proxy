package cache

import (
	"fmt"
	"testing"
	"time"
)

// FuzzPeerPushBufferInvariants drives random add, take, requeue, finish and
// peer-removal sequences through the push buffer and checks it against a
// model after every step: the budget holds, totals equal the backlogs' sums,
// one entry per peer and key holding the newest sequence, FIFO order kept,
// and every push is either buffered, taken, dropped or lost to a newer value.
//
// conformance: peer-cache-push-replay
func FuzzPeerPushBufferInvariants(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 0, 5, 1, 0, 2, 4, 3, 1, 0, 0, 0, 0, 0})
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2})
	f.Add([]byte{0, 7, 15, 0, 7, 15, 0, 7, 15, 3, 0, 4, 1, 2, 1, 5, 6, 2})
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) > 4096 {
			ops = ops[:4096]
		}
		const maxEntries, maxBytes = 6, 40
		b := newPushBuffer(maxEntries, maxBytes, nil)
		base := time.Now().UnixNano()
		now := base
		var seq uint64
		model := map[string]map[string]uint64{} // peer -> key -> seq buffered
		taken := map[string][]*bufferedPush{}   // peer -> in flight
		next := func(i *int) byte {
			if *i >= len(ops) {
				return 0
			}
			v := ops[*i]
			*i++
			return v
		}
		drop := func(drops []pushDrop) {
			for _, d := range drops {
				if got, ok := model[d.addr][d.entry.key]; !ok || got != d.entry.seq {
					t.Fatalf("dropped %s/%s seq %d that was not buffered (model %d %v)", d.addr, d.entry.key, d.entry.seq, got, ok)
				}
				delete(model[d.addr], d.entry.key)
			}
		}
		for i := 0; i < len(ops); {
			op := next(&i) % 6
			peer := fmt.Sprintf("p%d", next(&i)%3)
			if model[peer] == nil {
				model[peer] = map[string]uint64{}
			}
			switch op {
			case 0: // add
				key := fmt.Sprintf("k%d", next(&i)%8)
				size := int(next(&i)%16) + 1
				s := seq + 1
				if next(&i)%4 == 0 && seq > 0 {
					s = seq - 1 // a late, older value
				} else {
					seq++
				}
				ttl := time.Duration(next(&i)%60+1) * time.Second
				prev, had := model[peer][key]
				coalesced, _, drops := b.add(peer, &bufferedPush{key: key, value: make([]byte, size), seq: s, expiresAt: now + int64(ttl)}, now)
				if coalesced != had {
					t.Fatalf("add %s/%s: coalesced %v, buffered before %v", peer, key, coalesced, had)
				}
				if !had || s >= prev {
					model[peer][key] = s
				}
				drop(drops)
			case 1: // take
				n := int(next(&i)%5) + 1
				batch, drops := b.take(peer, n, now)
				for _, d := range drops {
					if d.reason != droppedExpired {
						t.Fatalf("take dropped for %d", d.reason)
					}
				}
				drop(drops)
				for _, p := range batch {
					if model[peer][p.key] != p.seq {
						t.Fatalf("took %s seq %d, model %d", p.key, p.seq, model[peer][p.key])
					}
					delete(model[peer], p.key)
				}
				taken[peer] = append(taken[peer], batch...)
			case 2: // requeue what is in flight
				items := taken[peer]
				taken[peer] = nil
				for _, p := range items {
					if cur, ok := model[peer][p.key]; !ok || cur < p.seq {
						model[peer][p.key] = p.seq
					}
				}
				drop(b.requeue(peer, items, now))
			case 3: // finish
				empty := len(model[peer]) == 0
				if done := b.finish(peer); done != empty {
					t.Fatalf("finish %s: %v with %d buffered", peer, done, len(model[peer]))
				}
			case 4: // peer removed
				for _, p := range b.removePeer(peer) {
					if model[peer][p.key] != p.seq {
						t.Fatalf("removed %s seq %d, model %d", p.key, p.seq, model[peer][p.key])
					}
				}
				model[peer] = map[string]uint64{}
			case 5: // time passes
				now += int64(next(&i)) * int64(time.Second)
			}
			checkPushBuffer(t, b, model)
		}
	})
}

func checkPushBuffer(t *testing.T, b *pushBuffer, model map[string]map[string]uint64) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.entries > b.maxEntries || b.bytes > b.maxBytes {
		t.Fatalf("budget exceeded: %d entries, %d bytes", b.entries, b.bytes)
	}
	entries, bytes := 0, int64(0)
	for peer, bl := range b.peers {
		if bl.order.Len() != len(bl.byKey) || int64(bl.order.Len()) != bl.count.Load() {
			t.Fatalf("%s: %d in order, %d by key, count %d", peer, bl.order.Len(), len(bl.byKey), bl.count.Load())
		}
		var blBytes int64
		for el := bl.order.Front(); el != nil; el = el.Next() {
			p := el.Value.(*bufferedPush)
			if bl.byKey[p.key] != el {
				t.Fatalf("%s: index of %s points elsewhere", peer, p.key)
			}
			if model[peer][p.key] != p.seq {
				t.Fatalf("%s/%s: seq %d, model %d", peer, p.key, p.seq, model[peer][p.key])
			}
			blBytes += int64(len(p.value))
		}
		if blBytes != bl.bytes {
			t.Fatalf("%s: %d bytes counted, %d held", peer, bl.bytes, blBytes)
		}
		if len(model[peer]) != bl.order.Len() {
			t.Fatalf("%s: %d buffered, model %d", peer, bl.order.Len(), len(model[peer]))
		}
		entries += bl.order.Len()
		bytes += bl.bytes
	}
	for peer, keys := range model {
		if b.peers[peer] == nil && len(keys) != 0 {
			t.Fatalf("%s: model holds %d, buffer none", peer, len(keys))
		}
	}
	if entries != b.entries || bytes != b.bytes {
		t.Fatalf("totals %d/%d, sums %d/%d", b.entries, b.bytes, entries, bytes)
	}
}
