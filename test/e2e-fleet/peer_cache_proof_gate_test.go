package e2e_fleet

import (
	"fmt"
	"testing"
)

// metadataDiskLeg decides what the metadata answers of a replica that
// restarted with its disk must show.
//
// A replica keeps a copy of a key on its disk only when the ring makes it a
// replica of that key (a copy it fetched from a peer while not a replica stays
// in memory). Ring placement depends on the run's probe, so a replica may be a
// replica of none of the keys: heldByB is how many of the keys it already held
// a pushed copy of before the restart. A copy it read from a peer in the same
// phase is on its disk too (a push still in flight), so a disk answer is no
// failure when heldByB is 0. Then:
//
//   - heldByB > 0: at least one answer must come from its own disk;
//   - heldByB == 0: its disk may have had nothing to serve, so the disk leg is
//     not exercised, but every one of the keys must still be answered by a
//     tier (disk or a peer), never by VictoriaLogs.
//
// served is how many of the keys were answered without missing every tier.
// It returns whether the disk leg was exercised, and a problem to report.
func metadataDiskLeg(heldByB, keys, served, fromDisk int) (exercised bool, problem string) {
	if heldByB > 0 {
		if fromDisk < 1 {
			return true, fmt.Sprintf("B served no metadata answer from its own disk after a restart although it held %d of %d keys", heldByB, keys)
		}
		return true, ""
	}
	if served != keys {
		return false, fmt.Sprintf("B held no metadata key before the restart, so all %d answers must come from its peers or disk, but only %d did", keys, served)
	}
	return false, ""
}

func TestMetadataDiskLeg(t *testing.T) {
	for _, c := range []struct {
		name                            string
		heldByB, keys, served, fromDisk int
		wantExercised, wantProblem      bool
	}{
		{"held keys served from disk", 4, 12, 12, 4, true, false},
		{"one held key served from disk", 1, 12, 12, 1, true, false},
		{"held keys but nothing from disk", 4, 12, 12, 0, true, true},
		{"no held key, all from peers", 0, 12, 12, 0, false, false},
		{"no held key, a push raced and the copy is on disk", 0, 12, 12, 2, false, false},
		{"no held key, one answer missed every tier", 0, 12, 11, 0, false, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			exercised, problem := metadataDiskLeg(c.heldByB, c.keys, c.served, c.fromDisk)
			if exercised != c.wantExercised || (problem != "") != c.wantProblem {
				t.Fatalf("metadataDiskLeg(%d, %d, %d, %d) = %v, %q; want exercised=%v problem=%v", c.heldByB, c.keys, c.served, c.fromDisk, exercised, problem, c.wantExercised, c.wantProblem)
			}
		})
	}
}
