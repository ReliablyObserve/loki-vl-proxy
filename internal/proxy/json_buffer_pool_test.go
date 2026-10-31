package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	fj "github.com/valyala/fastjson"
)

func TestResetJSONBufferBoundsRetainedCapacity(t *testing.T) {
	for _, size := range []int{0, 4096, maxPooledBufSize, maxPooledBufSize + 1, 8 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			buf := bytes.NewBuffer(make([]byte, size))
			if size > 0 {
				buf.Next(size / 2) // Reset must also discard the reader offset.
			}
			resetJSONBufferForReuse(buf)
			if buf.Len() != 0 || buf.Cap() > maxPooledBufSize {
				t.Fatalf("released buffer len=%d cap=%d", buf.Len(), buf.Cap())
			}
			if size <= maxPooledBufSize && buf.Cap() != size {
				t.Fatalf("small allocation discarded: cap=%d want=%d", buf.Cap(), size)
			}
		})
	}
}

func TestResetFJMarshalScratchBoundsRetainedCapacity(t *testing.T) {
	for _, size := range []int{0, 4096, maxPooledBufSize, maxPooledBufSize + 1, 8 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			scratch := make([]byte, size)
			resetFJMarshalScratchForReuse(&scratch)
			if len(scratch) != 0 || cap(scratch) > maxPooledBufSize {
				t.Fatalf("released scratch len=%d cap=%d", len(scratch), cap(scratch))
			}
			if size <= maxPooledBufSize && cap(scratch) != size {
				t.Fatalf("small allocation discarded: cap=%d want=%d", cap(scratch), size)
			}
		})
	}
}

func TestResetJSONPoolEmptyOversizedBacking(t *testing.T) {
	buf := bytes.NewBuffer(make([]byte, 0, maxPooledBufSize+1))
	resetJSONBufferForReuse(buf)
	if buf.Cap() != 0 {
		t.Fatalf("empty oversized buffer retained backing: cap=%d", buf.Cap())
	}
	scratch := make([]byte, 0, maxPooledBufSize+1)
	resetFJMarshalScratchForReuse(&scratch)
	if cap(scratch) != 0 {
		t.Fatalf("empty oversized scratch retained backing: cap=%d", cap(scratch))
	}
}

func TestWindowedStreamsOutputIndependentOfPoolReuse(t *testing.T) {
	for _, categorized := range []bool{false, true} {
		t.Run(fmt.Sprint(categorized), func(t *testing.T) {
			streams := []map[string]interface{}{{
				"stream": map[string]string{"app": "test"},
				"values": []interface{}{[]interface{}{"123", strings.Repeat("large", maxPooledBufSize)}},
			}}
			out := marshalWindowedStreamsResult(streams, categorized)
			want := bytes.Clone(out)
			for range 8 {
				streams[0]["values"] = []interface{}{[]interface{}{"456", "small"}}
				small := marshalWindowedStreamsResult(streams, categorized)
				if !bytes.Contains(small, []byte("small")) || bytes.Contains(small, []byte("large")) {
					t.Fatal("large response leaked into small response")
				}
			}
			if !bytes.Equal(out, want) {
				t.Fatal("returned output changed after pool reuse")
			}
		})
	}
}

func TestResetFJParserDropsLargeInputState(t *testing.T) {
	for _, size := range []int{maxPooledBufSize, 128 << 10, maxPooledStatsParserInputSize, maxPooledStatsParserInputSize + 1, 8 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			parser := &fj.Parser{}
			// Include a nested array/object so the parser owns both copied input
			// and cached values, rather than only a top-level string.
			body := `{"nested":[{"value":"` + strings.Repeat("x", size-25) + `"}]}`
			if len(body) != size {
				t.Fatalf("fixture length=%d want=%d", len(body), size)
			}
			if _, err := parser.Parse(body); err != nil {
				t.Fatal(err)
			}
			resetFJParserForReuse(parser, len(body))
			cleared := reflect.ValueOf(*parser).IsZero()
			if cleared != (size > maxPooledStatsParserInputSize) {
				t.Fatalf("parser cleared=%v for input length=%d", cleared, size)
			}
			value, err := parser.Parse(`{"small":42}`)
			if err != nil || value.GetInt("small") != 42 {
				t.Fatalf("parse after reset: value=%v err=%v", value, err)
			}
		})
	}
}

func TestResetFJParserClearsFailedParse(t *testing.T) {
	parser := &fj.Parser{}
	body := `{"bad":"` + strings.Repeat("x", 8<<20)
	if _, err := parser.Parse(body); err == nil {
		t.Fatal("expected invalid JSON")
	}
	resetFJParserForReuse(parser, len(body))
	if !reflect.ValueOf(*parser).IsZero() {
		t.Fatal("failed parse retained large input state")
	}
}

func TestResetFJParserUsesLargestInputAfterSmallParse(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprintf("first_parse_failed=%v", failed), func(t *testing.T) {
			parser := &fj.Parser{}
			large := `{"large":"` + strings.Repeat("x", maxPooledStatsParserInputSize+1)
			if !failed {
				large += `"}`
			}
			largest := len(large)
			if _, err := parser.Parse(large); (err != nil) != failed {
				t.Fatalf("first parse err=%v, want failure=%v", err, failed)
			}
			if _, err := parser.Parse(`{"small":42}`); err != nil {
				t.Fatal(err)
			}
			// Fastjson keeps its previous working-copy capacity after a small
			// parse. Returning by the last input's length would retain it.
			if reflect.ValueOf(*parser).FieldByName("b").Cap() <= maxPooledStatsParserInputSize {
				t.Fatal("fixture must retain oversized parser storage after the small parse")
			}
			resetFJParserForReuse(parser, largest)
			if !reflect.ValueOf(*parser).IsZero() {
				t.Fatal("maximum input size must clear state even after a later small parse")
			}
		})
	}
}

func TestStatsJSONOutputIndependentOfPoolReuse(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	p.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, nil)
	query := `sum by (service_name) (count_over_time({app="test"}[60s]))`
	keep := func(ts int64) bool { return ts > 1700000000*int64(1e9) }
	transforms := map[string]func([]byte) []byte{
		"trim": func(body []byte) []byte {
			return trimStatsQRByTimeFJShifted(body, keep, nil)
		},
		"translate": func(body []byte) []byte {
			return p.translateStatsResponseLabelsWithContext(context.Background(), body, query)
		},
		"trim_translate": func(body []byte) []byte {
			return p.trimAndTranslateStatsQRFJ(context.Background(), body, keep, query)
		},
	}
	for name, transform := range transforms {
		t.Run(name, func(t *testing.T) {
			body := buildStatsMatrixBody(100, 60)
			out := transform(body)
			if len(out) <= maxPooledBufSize || !json.Valid(out) {
				t.Fatal("expected valid oversized response")
			}
			if bytes.Equal(body, out) {
				t.Fatal("fixture must exercise the response rebuild path")
			}
			want := bytes.Clone(out)
			for range 8 {
				small := transform(buildStatsMatrixBody(1, 3))
				if !json.Valid(small) || len(small) >= maxPooledBufSize {
					t.Fatal("invalid small response after large response")
				}
			}
			if !bytes.Equal(out, want) {
				t.Fatal("returned JSON changed after parser and buffer reuse")
			}
		})
	}
}

// This complements the common 100-series benchmark with an input above the
// parser retention cutoff, where bounded pooling trades reuse for collection.
func BenchmarkStatsJSONOversizedParserInput(b *testing.B) {
	p := newUnderscokeProxy(b)
	body := buildStatsMatrixBody(1000, 60)
	if len(body) <= maxPooledStatsParserInputSize {
		b.Fatal("fixture must exceed the parser input retention cutoff")
	}
	query := `sum by (service_name) (count_over_time({app="test"}[60s]))`
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.translateStatsResponseLabelsWithContext(b.Context(), body, query)
	}
}
