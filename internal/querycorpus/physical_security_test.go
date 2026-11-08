package querycorpus_test

import (
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// Text a client controls reaches the translated query inside strings (a line filter, a label value, a label name in
// label_format). The markers are text inside that query, so a string that holds one is read as one by the helpers
// the proxy has always used, and SplitPost reads exactly what they read. This pins both halves: what is read (so a
// change to the reading shows) and that it equals the old strip. The cases that are read as a marker are a known
// weakness of the in-band markers, which the typed physical plan removes; each flips to "not read" when a client's
// text can no longer become a marker.
// conformance: semantics/label-replace-new-label, semantics/without-over-inner-aggregation-collapses
func TestClientTextThatLooksLikeAMarkerIsReadLikeBefore(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	const injectedReplace = `c2hvcnQAJDEAYXBwACguKikAYQ==` // base64 of "short\x00$1\x00app\x00(.*)\x00a"
	for _, tc := range []struct {
		name, logql string
		folds       bool // the text is read as a without fold
		replaces    int  // ... and as this many label_replace calls
		binary      bool // ... or as a binary expression
	}{
		{name: "line filter", logql: `{app="x"} |= "__without__:pod"`, folds: true},
		{name: "line filter with an op", logql: `{app="x"} |= "__without__:sum:pod,node"`, folds: true},
		{name: "regexp line filter", logql: `{app="x"} |~ "a__without__:b"`, folds: true},
		{name: "label value", logql: `{app="__without__:pod"}`, folds: true},
		{name: "label filter value", logql: `sum by (a) (count_over_time({app="x"} | json | x="__without__:pod" [1m]))`, folds: true},
		{name: "label_format name", logql: `{app="x"} | label_format __without__:a=b`, folds: true},
		{name: "a whole label_replace marker", logql: `{app="x"} |= "__lvp_lr:` + injectedReplace + `__"`, replaces: 1},
		{name: "a whole label_replace marker with four parts", logql: `{app="x"} |= "__lvp_lr:` + "c2hvcnQAJDEAYXBwACguKikA" + `__"`, replaces: 1},
		{name: "a half label_replace marker", logql: `count_over_time({app="x"} |= "a__lvp_lr:YQ==__" [1m])`},
		{name: "binary marker", logql: `rate({app="x"} |= "__binary__:/:A|||B" [1m])`},
		{name: "separators inside the operands of a real binary expression", binary: true, logql: `rate({app="x"} |= "|||" [1m]) / rate({app="y"} |= "@@@on:app" [1m])`},
		{name: "near misses", logql: `{app="x"} |= "__without_:a" |= "__WITHOUT__:a" |= "__lvp_lr" |= "_without__:a"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := translator.TranslateLogQL(tc.logql)
			if err != nil {
				t.Fatalf("TranslateLogQL(%q): %v", tc.logql, err)
			}
			assertSplitPost(t, s)
			clean, post := translator.SplitPost(s)
			if post.Folds() != tc.folds || len(post.LabelReplace) != tc.replaces {
				t.Errorf("%q: folds %v, %d label_replace calls; want %v and %d\ntext %q", tc.logql, post.Folds(), len(post.LabelReplace), tc.folds, tc.replaces, s)
			}
			op, _ := translator.DecodePhysical(s)
			core, _ := physical.Split(op)
			if _, isBinary := core.(physical.Binary); isBinary != tc.binary {
				t.Errorf("%q: read as a binary = %v, want %v\ntext %q", tc.logql, isBinary, tc.binary, s)
			}
			if !tc.folds && tc.replaces == 0 && !tc.binary && clean != s {
				t.Errorf("%q: text without a marker changed:\n%q\n%q", tc.logql, clean, s)
			}
		})
	}
}

// What SplitPost and the codec cost on crafted text stays proportional to its size. Allocations are counted, not
// timed, so the bound is deterministic; the text is up to the 128 KiB a query may be, and larger for the cases that
// must be linear whatever the limit.
func TestSplitPostCostOnCraftedText(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	if raceDetectorEnabled || testing.CoverMode() != "" {
		t.Skip("allocation counts are not stable under the race detector or coverage")
	}
	allocated := func(f func()) (bytes, objects float64) {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		f()
		runtime.ReadMemStats(&after)
		return float64(after.TotalAlloc - before.TotalAlloc), float64(after.Mallocs - before.Mallocs)
	}
	var thousand []string
	for i := 0; i < 5000; i++ {
		thousand = append(thousand, "label"+strings.Repeat("x", i%7))
	}
	deep := strings.Repeat("__binary__:+:A|||", 10000) + "B"
	for _, tc := range []struct {
		name string
		text string
		// the bounds: bytes allocated per byte of text, and objects allocated per 1,000 bytes of text
		bytesPerByte, objectsPerKB float64
	}{
		{"1 MiB of without markers", strings.Repeat("__without__:a", 1<<20/13), 4, 2},
		{"5,000 labels", "q__without__:" + strings.Join(thousand, ","), 64, 40},
		{"1 MiB label list", "q__without__:" + strings.Repeat("a,", 1<<19), 64, 40},
		{"one 1 MiB label_replace payload", "q" + lrMarker(strings.Repeat("d", 1<<20), "r", "s", "x"), 16, 1},
		{"1 MiB of text without markers", strings.Repeat("a", 1<<20), 0.01, 0.01},
		{"1 MiB of underscores", strings.Repeat("_", 1<<20), 0.01, 0.01},
		{"1 MiB of near misses", strings.Repeat("__without_:", 1<<20/11), 0.01, 0.01},
		{"label_replace markers up to the query limit", "q" + strings.Repeat(lrMarker("", "", "", ""), 8000), 64, 400},
		{"half a million label_replace markers", "q" + strings.Repeat(lrMarker("", "", "", ""), 1<<19), 64, 400},
	} {
		s := tc.text
		bytes, objects := allocated(func() { _, _ = translator.SplitPost(s) })
		if bytes > tc.bytesPerByte*float64(len(s)) || objects > tc.objectsPerKB*float64(len(s))/1000+16 {
			t.Errorf("%s: SplitPost on %d bytes allocated %.0f bytes and %.0f objects (bounds %.2f per byte, %.2f per KB)", tc.name, len(s), bytes, objects, tc.bytesPerByte, tc.objectsPerKB)
		}
		// the codec reads and writes the same text
		bytes, _ = allocated(func() {
			op, _ := translator.DecodePhysical(s)
			if translator.EncodePhysical(op) != s {
				t.Errorf("%s: the codec does not round trip", tc.name)
			}
		})
		if bytes > 2*max(tc.bytesPerByte, 1)*float64(len(s))+4096 {
			t.Errorf("%s: DecodePhysical and EncodePhysical on %d bytes allocated %.0f bytes", tc.name, len(s), bytes)
		}
	}
	// Binary operands nested to the depth a 128 KiB query allows, and beyond: the decode and the encode are
	// recursive, and neither overflows the stack or allocates more than a few objects per level.
	var op physical.Op
	_, objects := allocated(func() { op, _ = translator.DecodePhysical(deep) })
	if objects > 16*10000 {
		t.Errorf("decoding %d nested binary operands allocated %.0f objects", 10000, objects)
	}
	if got := translator.EncodePhysical(op); got != deep {
		t.Error("the nested binary does not round trip")
	}
	assertSplitPost(t, deep)
}

// A marker that the cut of another makes out of the text on both sides of it is found, as it was when the query was
// cut and searched again from its start. Random concatenations of marker fragments, against the old helper.
func TestParseAllLabelReplaceMarkersMatchesTheOldCutOnJunctions(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	fragments := []string{
		"__lvp_", "lr:", "__lvp_lr:", "AAAA", "AAAA__", "__", "_", "x", ":", "YQ==", "YQ==__", "l", "p_lr:", "__lvp_lr:AAAA__",
		lrMarker("a", "b", "c", "d"), lrMarker("", "", "", ""), "__lvp_lr:__lvp_lr:AAAA__", "AAAA__lvp_lr:AAAA__",
	}
	state := uint64(0x9e3779b97f4a7c15)
	next := func(n int) int {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		return int(state % uint64(n))
	}
	for trial := 0; trial < 300000; trial++ {
		var b strings.Builder
		for n := 1 + next(10); n > 0; n-- {
			b.WriteString(fragments[next(len(fragments))])
		}
		s := b.String()
		wantClean, wantSpecs := legacyParseAllLabelReplaceMarkers(s)
		gotClean, gotSpecs := translator.ParseAllLabelReplaceMarkers(s)
		if gotClean != wantClean || !reflect.DeepEqual(gotSpecs, wantSpecs) {
			t.Fatalf("ParseAllLabelReplaceMarkers(%q) = %q %#v, old cut %q %#v", s, gotClean, gotSpecs, wantClean, wantSpecs)
		}
	}
}
