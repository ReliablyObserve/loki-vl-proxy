package proxy

import (
	"cmp"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sort"
	"testing"
	"time"
)

// lokiQuantileOfWindow is Loki's quantile_over_time (pkg/logql/range_vector.go:449-487)
// written apart from the slider and the count arithmetic: the values of the
// buckets whose label is in [start, end), each held count times, sorted (NaN
// first), interpolated at rank phi*(n-1). false: no value in the window (Loki
// leaves the step out). computed is false when the window holds more values
// than the reference expands.
func lokiQuantileOfWindow(phi float64, samples []quantileSample, start, end int64) (value float64, nonEmpty, computed bool) {
	var values []float64
	for _, s := range samples {
		if s.ts < start || s.ts >= end {
			continue
		}
		if len(values)+int(s.count) > 8192 {
			return 0, true, false
		}
		for range int(s.count) {
			values = append(values, s.value)
		}
	}
	if len(values) == 0 {
		return 0, false, true
	}
	return lokiQuantile(phi, values), true, true
}

// sameQuantileBits: the same bits, any two NaN as one (their payload means nothing).
func sameQuantileBits(a, b float64) bool {
	return math.Float64bits(a) == math.Float64bits(b) || math.IsNaN(a) && math.IsNaN(b)
}

// checkQuantileSlider reads data as three bytes a bucket: the step to its label
// (0 to 30 ns, so labels repeat), its value (a quarter step up to 7.75, NaN, a
// negative one, +Inf, -Inf; never -0, which ties with 0 in either order) and its
// count (1 to 8, or up to 255,000 when the label's high bit is set), the way
// VictoriaLogs' count() per value gives whole counts of one or more. The slider
// moves its window of one length along the walk (a signed byte in 5 ns: forward,
// back, jumps) and must give, at every end, the bits of the window sort
// (quantileWindow), and, where the window holds few enough values to expand
// them, those of Loki's quantile over the expanded values.
func checkQuantileSlider(t testing.TB, data []byte, phi float64, window uint8, walk []byte) {
	t.Helper()
	if math.IsNaN(phi) {
		return
	}
	var samples []quantileSample
	ts := int64(0)
	for i := 0; i+2 < len(data) && len(samples) < 256; i += 3 {
		b0, b1, b2 := data[i], data[i+1], data[i+2]
		ts += int64(b0&3) * 10
		v := float64(b1&31) / 4
		switch b1 >> 5 {
		case 4:
			v = math.NaN()
		case 5:
			v = -v - 0.25
		case 6:
			v = math.Inf(1)
		case 7:
			v = math.Inf(-1)
		}
		count := 1 + int(b2%8)
		if b0&0x80 != 0 {
			count = 1 + int(b2)*1000
		}
		samples = append(samples, quantileSample{ts: ts, value: v, count: float64(count)})
	}
	length := int64(10 * (1 + window%8))
	w := newQuantileSlider(samples)
	var scratch []quantileSample
	end := int64(-20)
	for i, move := range walk {
		if i >= 64 {
			break
		}
		end += int64(int8(move)) * 5
		if i > 0 {
			end += 10
		}
		want, wantOK := quantileWindow(phi, samples, end-length, end, &scratch)
		got, gotOK := w.quantile(phi, end-length, end)
		if gotOK != wantOK || wantOK && !sameQuantileBits(got, want) {
			t.Fatalf("phi %v window [%d, %d): slider %v %v, window sort %v %v (%d buckets)", phi, end-length, end, got, gotOK, want, wantOK, len(samples))
		}
		if ref, nonEmpty, computed := lokiQuantileOfWindow(phi, samples, end-length, end); nonEmpty != gotOK || computed && !sameQuantileBits(got, ref) {
			t.Fatalf("phi %v window [%d, %d): slider %v %v, Loki %v %v (%d buckets)", phi, end-length, end, got, gotOK, ref, nonEmpty, len(samples))
		}
	}
}

// TestQuantileSliderMatchesLoki: the sliding quantile gives Loki's value (and
// the window sort's bits) over generated buckets with repeated labels and
// values, NaN, infinities, large counts, phi inside and outside [0, 1] and at
// the rank edges, and windows that move forward, back, and jump.
//
// conformance: semantics/unwrap-quantile-response-limit, quality/query-range-settled-steps
func TestQuantileSliderMatchesLoki(t *testing.T) {
	rnd := rand.New(rand.NewSource(7))
	phis := []float64{0, 0.25, 0.5, 0.9, 0.95, 0.99, 1, -0.1, 1.5, math.Inf(1), math.Inf(-1), math.SmallestNonzeroFloat64, 1 - 1e-16}
	for c := 0; c < 600; c++ {
		data := make([]byte, 3*rnd.Intn(120))
		rnd.Read(data)
		if c%3 == 0 { // few distinct values: ties are the case
			for i := 1; i < len(data); i += 3 {
				data[i] &= 0xE3
			}
		}
		walk := make([]byte, 1+rnd.Intn(64))
		rnd.Read(walk)
		for i := range walk {
			if rnd.Intn(3) != 0 { // mostly a step forward or two
				walk[i] = byte(rnd.Intn(5))
			}
		}
		phi := rnd.Float64()
		if c%2 == 0 {
			phi = phis[rnd.Intn(len(phis))]
		}
		checkQuantileSlider(t, data, phi, uint8(rnd.Intn(8)), walk)
	}
}

// FuzzQuantileSlider: the sliding quantile is, bit for bit, the sort of the
// window's buckets and Loki's quantile (checkQuantileSlider), for any buckets,
// phi and walk of the window.
//
// conformance: semantics/unwrap-quantile-response-limit, quality/query-range-settled-steps
func FuzzQuantileSlider(f *testing.F) {
	f.Add([]byte{1, 4, 2, 1, 4, 3, 2, 9, 1, 0, 9, 5, 1, 0, 7, 3, 130, 2}, 0.5, uint8(2), []byte{1, 1, 1, 1, 1, 251, 40, 0, 1})
	f.Add([]byte{0, 0x80, 1, 0, 0x80, 1, 1, 0xC0, 1, 1, 0xE0, 2}, 0.95, uint8(0), []byte{2, 2, 2, 2})
	f.Add([]byte{0, 0, 1, 0, 0, 1}, 0.0, uint8(7), []byte{0, 0, 0})
	f.Add([]byte{1, 0xA0, 3, 1, 0x01, 4, 2, 0x02, 5, 3, 0x03, 6}, 1.0, uint8(3), []byte{3, 3, 3, 3, 3, 3})
	f.Add([]byte{1, 0x84, 3, 1, 0x01, 4, 2, 0x02, 5}, -0.5, uint8(1), []byte{1, 1, 1})
	f.Add([]byte{1, 0xC0, 3, 1, 0x01, 4, 2, 0x02, 5}, 1.5, uint8(1), []byte{1, 1, 1})
	f.Fuzz(func(t *testing.T, data []byte, phi float64, window uint8, walk []byte) {
		if len(data) > 3*256 || len(walk) > 256 {
			t.Skip()
		}
		checkQuantileSlider(t, data, phi, window, walk)
	})
}

// checkSortQuantileSamples: sortQuantileSamples orders by time and keeps every
// sample, the samples of one time as a multiset (the windows rank them by
// value), for runs on the bucket grid, off it and spread wider than the
// counting sort takes.
func checkSortQuantileSamples(t *testing.T, data []byte, bucketSec uint8, offGrid bool) {
	t.Helper()
	bucket := time.Duration(1+int(bucketSec)%90) * time.Second
	start := time.Unix(1_800_000_000, 0)
	var in []quantileSample
	for i := 0; i+2 < len(data); i += 3 {
		ts := start.Add(time.Duration(int(data[i])-64) * bucket).UnixNano()
		if offGrid && data[i+1]%5 == 0 {
			ts += int64(data[i+1])
		}
		if data[i+2] == 255 {
			ts += int64(1000 * 1000 * bucket) // far out: the comparison sort answers
		}
		in = append(in, quantileSample{ts: ts, value: float64(data[i+1]), count: float64(data[i+2]%7 + 1)})
	}
	want := slices.Clone(in)
	slices.SortStableFunc(want, func(a, b quantileSample) int { return cmp.Compare(a.ts, b.ts) })
	got := sortQuantileSamples(slices.Clone(in), start, bucket)
	if len(got) != len(want) {
		t.Fatalf("%d samples, want %d", len(got), len(want))
	}
	key := func(s []quantileSample) []string {
		out := make([]string, len(s))
		for i, x := range s {
			out[i] = fmt.Sprintf("%d/%v/%v", x.ts, x.value, x.count)
		}
		return out
	}
	for i := 1; i < len(got); i++ {
		if got[i].ts < got[i-1].ts {
			t.Fatalf("not in time order at %d: %v", i, key(got))
		}
	}
	g, w := key(got), key(want)
	sort.Strings(g)
	sort.Strings(w)
	if !slices.Equal(g, w) {
		t.Fatalf("samples changed:\n%v\n%v", g, w)
	}
}

// TestSortQuantileSamplesMatchesSort: the counting sort of collectQuantileHistogram
// is the time sort of the samples.
//
// conformance: semantics/unwrap-quantile-response-limit
func TestSortQuantileSamplesMatchesSort(t *testing.T) {
	rnd := rand.New(rand.NewSource(5))
	for c := 0; c < 500; c++ {
		data := make([]byte, 3*rnd.Intn(200))
		rnd.Read(data)
		checkSortQuantileSamples(t, data, uint8(rnd.Intn(256)), c%3 == 0)
	}
}

// FuzzSortQuantileSamples: the counting sort is the time sort for any samples.
//
// conformance: semantics/unwrap-quantile-response-limit
func FuzzSortQuantileSamples(f *testing.F) {
	f.Add([]byte{64, 1, 1, 63, 2, 2, 65, 3, 3, 64, 4, 255}, uint8(59), false)
	f.Add([]byte{70, 5, 1, 10, 10, 2, 70, 15, 3}, uint8(0), true)
	f.Fuzz(func(t *testing.T, data []byte, bucket uint8, offGrid bool) {
		if len(data) > 3*512 {
			t.Skip()
		}
		checkSortQuantileSamples(t, data, bucket, offGrid)
	})
}
