package proxy

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func statsRetentionFixture(points int, varied bool) *statsExtentChunk {
	c := &statsExtentChunk{from: 170000100, to: 170000100 + int64(points), readAt: 1700001000000000000, names: []string{"c"}}
	if points == 0 {
		return c
	}
	s := &statsExtentSeries{name: "c", metric: []byte(`{"__name__":"c","app":"a"}`), labelKey: []byte(`"app":"a"`)}
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < points; i++ {
		value := "1"
		if varied {
			value = strconv.FormatUint(rng.Uint64(), 10)
		}
		k := c.from + int64(i)
		s.points = append(s.points, statsExtentPoint{k: k, raw: []byte(fmt.Sprintf(`[%d,%q]`, k*10, value))})
	}
	c.series = []*statsExtentSeries{s}
	return c
}

// conformance: quality/query-range-extent-cache
func TestStatsExtentOwnedEncodingSurvivesDiskQueueAndRestart(t *testing.T) {
	for _, codec := range []string{"raw", "s2", "zstd"} {
		t.Run(codec, func(t *testing.T) {
			fixture := statsRetentionFixture(10000, false)
			encoded, err := encodeStatsExtentChunk(fixture)
			if err != nil {
				t.Fatal(err)
			}
			cfg := cache.DiskCacheConfig{Path: filepath.Join(t.TempDir(), "cache.db"),
				Compression: true, Codec: codec, FlushInterval: time.Hour, MaxBytes: 4 << 20}
			disk, err := cache.NewDiskCache(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = disk.Close() })
			key := statsExtentKeyPrefix + "persisted"
			disk.Set(key, encoded, time.Hour)
			pending, ok := disk.Get(key)
			if !ok || !bytes.Equal(pending, encoded) || cap(pending) != len(pending) {
				t.Fatalf("queued extent not compact or changed: found=%v len=%d cap=%d", ok, len(pending), cap(pending))
			}
			if err := disk.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := cache.NewDiskCache(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			stored, ttl, ok := reopened.GetWithTTL(key)
			if !ok || !bytes.Equal(stored, encoded) || ttl <= 0 || ttl > time.Hour {
				t.Fatalf("persisted bytes or deadline changed: found=%v ttl=%s", ok, ttl)
			}
			back, err := decodeStatsExtentChunk(stored)
			if err != nil || !reflect.DeepEqual(back, fixture) {
				t.Fatalf("restart changed metadata or points: %v", err)
			}
		})
	}
}

func FuzzStatsExtentOwnedEncoding(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 1, 255})
	f.Add(bytes.Repeat([]byte{1}, 256))
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 512 {
			t.Skip()
		}
		fixture := statsRetentionFixture(len(input), true)
		for i, value := range input {
			fixture.series[0].points[i].raw = []byte(fmt.Sprintf(`[%d,%q]`, (fixture.from+int64(i))*10, strconv.Itoa(int(value))))
		}
		encoded, err := encodeStatsExtentChunk(fixture)
		if err != nil || cap(encoded) != len(encoded) {
			t.Fatalf("owned encoding: %v len=%d cap=%d", err, len(encoded), cap(encoded))
		}
		back, err := decodeStatsExtentChunk(encoded)
		if err != nil || !reflect.DeepEqual(back, fixture) {
			t.Fatalf("round trip changed metadata or points: %v", err)
		}
	})
}

// A byte budget must not retain S2's worst-case output allocation behind a
// much smaller compressed slice. Slicing capacity alone cannot release it.
// conformance: quality/query-range-extent-cache
func TestStatsExtentEncodingRetainsOnlyCompressedCapacity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		points int
		varied bool
	}{
		{"empty", 0, false},
		{"small", 10, false},
		{"compressible", 10000, false},
		{"varied-values", 4000, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := statsRetentionFixture(tc.points, tc.varied)
			encoded, err := encodeStatsExtentChunk(fixture)
			if err != nil {
				t.Fatal(err)
			}
			c := cache.NewWithMaxBytes(time.Hour, 10, 16<<20)
			defer c.Close()
			key := statsExtentKeyPrefix + tc.name
			c.SetWithTTL(key, encoded, time.Hour)
			stored, ok := c.Get(key)
			if !ok {
				t.Fatal("encoded extent was not retained")
			}
			if cap(stored) != len(stored) {
				t.Errorf("cache retains %d backing bytes for %d encoded bytes", cap(stored), len(stored))
			}
			back, err := decodeStatsExtentChunk(stored)
			if err != nil || !reflect.DeepEqual(back, fixture) {
				t.Fatalf("cached round trip changed metadata or points: %v", err)
			}
			want, ok := renderStatsExtentPieces([]*statsExtentFragment{fixture})
			if !ok {
				t.Fatal("invalid fixture")
			}
			got, ok := renderStatsExtentPieces([]*statsExtentFragment{back})
			if !ok || !bytes.Equal(got, want) {
				t.Fatal("cached encoding changed the rendered backend response")
			}
			// A later encoding must not overwrite a value already owned by cache.
			if _, err := encodeStatsExtentChunk(statsRetentionFixture(tc.points+1, true)); err != nil {
				t.Fatal(err)
			}
			again, err := decodeStatsExtentChunk(stored)
			if err != nil || !reflect.DeepEqual(again, fixture) {
				t.Fatal("a subsequent encoding mutated an admitted value")
			}
		})
	}
}

func BenchmarkStatsExtentEncodeOwned(b *testing.B) {
	for _, tc := range []struct {
		name   string
		points int
		varied bool
	}{{"empty", 0, false}, {"small", 10, false}, {"compressible", 10000, false}, {"varied-values", 4000, true}} {
		b.Run(tc.name, func(b *testing.B) {
			fixture := statsRetentionFixture(tc.points, tc.varied)
			b.ReportAllocs()
			var encoded []byte
			for b.Loop() {
				var err error
				encoded, err = encodeStatsExtentChunk(fixture)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(encoded)), "encoded-B/op")
			b.ReportMetric(float64(cap(encoded)), "retained-cap-B/op")
			if dir := os.Getenv("S2_ENCODED_PROOF_DIR"); dir != "" {
				if err := os.WriteFile(filepath.Join(dir, tc.name+".s2"), encoded, 0600); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
