package cache

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

type capacityModelRecord struct {
	key     string
	value   []byte
	expires time.Time
}

// This is a cache safety model, not a linearizable key/value database model.
// Every write has a fresh generation key. Misses after eviction or faults are
// valid; any hit must have the exact key's payload and must not cross expiry.
// Every publication is quiesced before the next operation is checked.
func runPeerCapacitySequence(t testing.TB, rf int, sequence []byte) int64 {
	t.Helper()
	f := newCapacityFleet(t, rf, 16, 2048)
	var records []capacityModelRecord
	serial := 0
	ensureUp := func() {
		for node := range f.nodes {
			if !f.alive[node] {
				f.startBounded(node)
			}
		}
	}
	write := func(frontend int) {
		ensureUp()
		key := fmt.Sprintf("seq-tenant%d-gen%03d", frontend, serial)
		serial++
		value := bytes.Repeat([]byte{byte(serial)}, 128)
		value = append(value, []byte(key)...)
		records = append(records, capacityModelRecord{key: key, value: value, expires: clockNow().Add(time.Minute)})
		f.nodes[frontend].c.SetWithTTL(key, value, time.Minute)
		f.quiesce()
	}
	read := func(frontend, index int) {
		if len(records) == 0 {
			return
		}
		if !f.alive[frontend] {
			for i := range f.nodes {
				if f.alive[i] {
					frontend = i
					break
				}
			}
		}
		record := records[index%len(records)]
		value, ttl, _, _, hit := f.nodes[frontend].c.GetSharedWithAge(record.key)
		if !hit {
			return
		} // Explicit safe miss: no zero-backend-read claim under arbitrary faults.
		if !bytes.Equal(value, record.value) {
			t.Fatalf("RF%d returned another key/generation's payload for%s", rf, record.key)
		}
		if clockNow().After(record.expires) || ttl <= 0 {
			t.Fatalf("RF%d returned expired%s ttl=%s", rf, record.key, ttl)
		}
	}
	write(0)
	if len(sequence) > 16 {
		sequence = sequence[:16]
	}
	for _, raw := range sequence {
		op, parameter := int(raw%6), int(raw/6)
		node := parameter % 3
		switch op {
		case 0:
			write(node)
		case 1:
			read(node, parameter)
		case 2:
			f.quiesce()
			restore := AdvanceClockForTesting(2 * time.Minute)
			defer restore()
		case 3:
			// Bursts eventually pressure both entry and byte budgets. All values
			// individually fit the production per-entry admission ceiling.
			for i := 0; i < 5; i++ {
				write((node + i) % 3)
			}
		case 4:
			f.quiesce()
			f.restartBounded(node)
		case 5:
			f.quiesce()
			if !f.alive[node] {
				f.startBounded(node)
			} else {
				up := 0
				for _, alive := range f.alive {
					if alive {
						up++
					}
				}
				if up > 1 {
					f.stop(node)
				}
			}
		}
		f.quiesce()
		f.assertBudgets()
		for frontend, alive := range f.alive {
			if alive {
				for index := range records {
					read(frontend, index)
				}
			}
		}
	}
	var diskEvictions int64
	for _, node := range f.nodes {
		diskEvictions += node.dc.Evictions.Load()
	}
	return diskEvictions
}

func TestPeerCapacitySequencesExerciseCapacityEviction(t *testing.T) {
	for _, rf := range []int{1, 2} {
		t.Run(fmt.Sprintf("rf=%d", rf), func(t *testing.T) {
			//61 distinct generations exceed the fleet's48 record slots even atRF1.
			if evicted := runPeerCapacitySequence(t, rf, bytes.Repeat([]byte{3}, 12)); evicted == 0 {
				t.Fatal("capacity sequence did not exercise actual disk eviction")
			}
		})
	}
}

func TestPeerCapacityOperationPairs(t *testing.T) {
	for _, rf := range []int{1, 2} {
		for first := byte(0); first < 6; first++ {
			for second := byte(0); second < 6; second++ {
				t.Run(fmt.Sprintf("rf=%d/ops=%d,%d", rf, first, second), func(t *testing.T) {
					runPeerCapacitySequence(t, rf, []byte{first + 6, second + 12})
				})
			}
		}
	}
}

func FuzzPeerCapacitySequences(f *testing.F) {
	for _, seed := range [][]byte{
		{0, 0, 1, 2, 1, 4, 1, 5, 1},
		{1, 0, 6, 12, 3, 9, 15, 4, 1, 5, 1},
		{0, 5, 6, 2, 4, 1, 3, 3, 1},
		{1, 5, 11, 4, 10, 3, 2, 1},
		{0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3},
		{1, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3},
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 {
			return
		}
		runPeerCapacitySequence(t, 1+int(data[0]&1), data[1:])
	})
}
