package proxy

import (
	"bytes"
	"testing"
)

// Timestamps in the test JSON use 10-digit Unix seconds, which
// normalizeLokiIntTimeToUnixNano converts to nanoseconds (×1e9).
//
//	1748000000 → 1748000000000000000 ns
//	1748000001 → 1748000001000000000 ns
const (
	tsNs1 int64 = 1748000000000000000
	tsNs2 int64 = 1748000001000000000
	tsNs3 int64 = 1748000002000000000
)

func TestWriteFilteredValuesRaw_AllPass(t *testing.T) {
	raw := []byte(`[[1748000000,"1.0"],[1748000001,"2.0"],[1748000002,"3.0"]]`)
	buf := &bytes.Buffer{}
	ok := writeFilteredValuesRaw(buf, raw, func(ts int64) bool { return true })
	if !ok {
		t.Fatal("expected ok=true")
	}
	got := buf.String()
	want := `[[1748000000,"1.0"],[1748000001,"2.0"],[1748000002,"3.0"]]`
	if got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}
}

func TestWriteFilteredValuesRaw_FilterLast(t *testing.T) {
	raw := []byte(`[[1748000000,"1.0"],[1748000001,"2.0"],[1748000002,"3.0"]]`)
	buf := &bytes.Buffer{}
	ok := writeFilteredValuesRaw(buf, raw, func(ts int64) bool { return ts <= tsNs2 })
	if !ok {
		t.Fatal("expected ok=true")
	}
	got := buf.String()
	want := `[[1748000000,"1.0"],[1748000001,"2.0"]]`
	if got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}
}

func TestWriteFilteredValuesRaw_FilterFirst(t *testing.T) {
	raw := []byte(`[[1748000000,"1.0"],[1748000001,"2.0"],[1748000002,"3.0"]]`)
	buf := &bytes.Buffer{}
	ok := writeFilteredValuesRaw(buf, raw, func(ts int64) bool { return ts >= tsNs2 })
	if !ok {
		t.Fatal("expected ok=true")
	}
	got := buf.String()
	want := `[[1748000001,"2.0"],[1748000002,"3.0"]]`
	if got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}
}

func TestWriteFilteredValuesRaw_FilterAll(t *testing.T) {
	raw := []byte(`[[1748000000,"1.0"],[1748000001,"2.0"]]`)
	buf := &bytes.Buffer{}
	ok := writeFilteredValuesRaw(buf, raw, func(ts int64) bool { return false })
	if !ok {
		t.Fatal("expected ok=true")
	}
	if buf.String() != "[]" {
		t.Errorf("got %q want %q", buf.String(), "[]")
	}
}

func TestWriteFilteredValuesRaw_Empty(t *testing.T) {
	buf := &bytes.Buffer{}
	ok := writeFilteredValuesRaw(buf, []byte(`[]`), func(ts int64) bool { return true })
	if !ok {
		t.Fatal("expected ok=true")
	}
	if buf.String() != "[]" {
		t.Errorf("got %q", buf.String())
	}
}

func TestWriteFilteredValuesRaw_FloatTimestamp(t *testing.T) {
	// Fractional-second timestamp: 1748000000.5 s → 1748000000500000000 ns ≤ tsNs2 → kept.
	raw := []byte(`[[1748000000.5,"9.9"]]`)
	buf := &bytes.Buffer{}
	ok := writeFilteredValuesRaw(buf, raw, func(ts int64) bool { return ts <= tsNs2 })
	if !ok {
		t.Fatal("expected ok=true")
	}
	if buf.String() != `[[1748000000.5,"9.9"]]` {
		t.Errorf("got %q", buf.String())
	}
}

func TestWriteFilteredValuesRaw_NilKeep(t *testing.T) {
	raw := []byte(`[[1748000000,"1.0"],[1748000001,"2.0"]]`)
	buf := &bytes.Buffer{}
	ok := writeFilteredValuesRaw(buf, raw, nil)
	if !ok {
		t.Fatal("expected ok=true")
	}
	got := buf.String()
	want := `[[1748000000,"1.0"],[1748000001,"2.0"]]`
	if got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}
}

func TestWriteFilteredValuesRaw_BadInput(t *testing.T) {
	buf := &bytes.Buffer{}
	if writeFilteredValuesRaw(buf, []byte(`{}`), func(ts int64) bool { return true }) {
		t.Error("expected ok=false for non-array input")
	}
	buf.Reset()
	if writeFilteredValuesRaw(buf, []byte(`not json`), func(ts int64) bool { return true }) {
		t.Error("expected ok=false for invalid JSON")
	}
}

func TestRawBytesToInt64(t *testing.T) {
	cases := []struct {
		b    []byte
		want int64
	}{
		{[]byte("0"), 0},
		{[]byte("123"), 123},
		{[]byte("-456"), -456},
		{[]byte("1748000000000000000"), 1748000000000000000},
	}
	for _, c := range cases {
		got := rawBytesToInt64(c.b)
		if got != c.want {
			t.Errorf("rawBytesToInt64(%q) = %d, want %d", c.b, got, c.want)
		}
	}
}
