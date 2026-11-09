package translate

import (
	"reflect"
	"strings"
	"sync"
	"testing"
)

func detectedLevelChainArgSets() []struct {
	fields  []string
	mode    DetectedLevelChainMode
	present *DetectedLevelPresence
	shared  *SharedUnpack
} {
	type args = struct {
		fields  []string
		mode    DetectedLevelChainMode
		present *DetectedLevelPresence
		shared  *SharedUnpack
	}
	var out []args
	for _, mode := range []DetectedLevelChainMode{DetectedLevelChainGroup, DetectedLevelChainLogs, DetectedLevelChainGrouping} {
		for _, fields := range [][]string{nil, {}, DetectedLevelFieldNames, {"level"}, {"severity_number"}, {"level", "severity"}} {
			for _, present := range []*DetectedLevelPresence{nil, {}, {Stored: []string{"level"}}, {BodyKeys: []string{"lvl"}},
				{Stored: []string{"severity_number"}, BodyKeys: []string{"level", "lvl"}}} {
				for _, shared := range []*SharedUnpack{nil, {}, {Keys: []string{"a"}}, {Keys: []string{"a", "b"}, Cond: "-x:*"}} {
					out = append(out, args{fields, mode, present, shared})
				}
			}
		}
	}
	return out
}

// The memoised chain is the chain built from the same arguments, for every combination of modes, field lists,
// presence and shared unpacks, on the first call and on a repeat.
// conformance: severity-detected-level-derivation
func TestDetectedLevelChainMemoEqualsBuild(t *testing.T) {
	for _, a := range detectedLevelChainArgSets() {
		want := buildDetectedLevelChainShared(a.fields, a.mode, a.present, a.shared)
		for i := 0; i < 2; i++ {
			if got := BuildDetectedLevelChainShared(a.fields, a.mode, a.present, a.shared); got != want {
				t.Fatalf("call %d for %#v: memo %q, build %q", i, a, got, want)
			}
		}
	}
}

// Two argument sets share a key only when they build the same chain: a nil presence or shared unpack differs from
// an empty one, and a separator or length in a field name cannot run into the next one. (A nil and an empty list
// share a key; the builder reads both as no items.)
// conformance: severity-detected-level-derivation
func TestDetectedLevelChainKeyIsInjective(t *testing.T) {
	sets := detectedLevelChainArgSets()
	tricky := []struct {
		fields  []string
		present *DetectedLevelPresence
	}{
		{[]string{"a:b"}, nil}, {[]string{"a", "b"}, nil}, {[]string{"1:a"}, nil}, {[]string{";"}, nil},
		{[]string{"a"}, &DetectedLevelPresence{Stored: []string{"b"}}}, {[]string{"a", "b"}, &DetectedLevelPresence{}},
	}
	for _, x := range tricky {
		sets = append(sets, struct {
			fields  []string
			mode    DetectedLevelChainMode
			present *DetectedLevelPresence
			shared  *SharedUnpack
		}{x.fields, DetectedLevelChainGroup, x.present, nil})
	}
	seen := map[string]int{}
	for i, a := range sets {
		key := detectedLevelChainKey(a.fields, a.mode, a.present, a.shared)
		if j, dup := seen[key]; dup {
			b := sets[j]
			if buildDetectedLevelChainShared(a.fields, a.mode, a.present, a.shared) != buildDetectedLevelChainShared(b.fields, b.mode, b.present, b.shared) {
				t.Fatalf("argument sets %#v and %#v share the key %q and build different chains", b, a, key)
			}
		}
		seen[key] = i
	}
}

// FuzzDetectedLevelChainKey: argument sets with the same key are the same arguments.
func FuzzDetectedLevelChainKey(f *testing.F) {
	f.Add("a,b", "c", "", true, false, 0)
	f.Add("a:b", "", "x;y", false, true, 2)
	f.Fuzz(func(t *testing.T, fields, stored, keys string, withPresent, withShared bool, mode int) {
		split := func(s string) []string {
			if s == "" {
				return nil
			}
			return strings.Split(s, ",")
		}
		a := []any{split(fields), DetectedLevelChainMode(mode % 3), (*DetectedLevelPresence)(nil), (*SharedUnpack)(nil)}
		if withPresent {
			a[2] = &DetectedLevelPresence{Stored: split(stored)}
		}
		if withShared {
			a[3] = &SharedUnpack{Keys: split(keys)}
		}
		// The same text read as other lists: the first field moved into the presence, or split at another comma.
		b := []any{split(fields + "," + stored), a[1], a[2], a[3]}
		ka := detectedLevelChainKey(a[0].([]string), a[1].(DetectedLevelChainMode), a[2].(*DetectedLevelPresence), a[3].(*SharedUnpack))
		kb := detectedLevelChainKey(b[0].([]string), b[1].(DetectedLevelChainMode), b[2].(*DetectedLevelPresence), b[3].(*SharedUnpack))
		build := func(x []any) string {
			return buildDetectedLevelChainShared(x[0].([]string), x[1].(DetectedLevelChainMode), x[2].(*DetectedLevelPresence), x[3].(*SharedUnpack))
		}
		if ka == kb && !reflect.DeepEqual(a, b) && build(a) != build(b) {
			t.Fatalf("%#v and %#v share the key %q and build different chains", a, b, ka)
		}
	})
}

// Builds from many goroutines agree and leave the memo within its bound.
// conformance: severity-detected-level-derivation
func TestDetectedLevelChainMemoConcurrentAndBounded(t *testing.T) {
	sets := detectedLevelChainArgSets()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, a := range sets {
				if got, want := BuildDetectedLevelChainShared(a.fields, a.mode, a.present, a.shared),
					buildDetectedLevelChainShared(a.fields, a.mode, a.present, a.shared); got != want {
					t.Errorf("concurrent memo %q, build %q", got, want)
					return
				}
			}
		}()
	}
	wg.Wait()
	// Distinct shared keys beyond the bound are built and not kept.
	for i := 0; i < maxDetectedLevelChains+10; i++ {
		_ = BuildDetectedLevelChainShared(nil, DetectedLevelChainGrouping, nil, &SharedUnpack{Keys: []string{strings.Repeat("k", i%50) + string(rune('a'+i%26)) + strings.Repeat("z", i/1300)}})
	}
	n := 0
	detectedLevelChains.Range(func(_, _ any) bool { n++; return true })
	if n > maxDetectedLevelChains {
		t.Fatalf("the memo holds %d chains, more than its bound %d", n, maxDetectedLevelChains)
	}
}
