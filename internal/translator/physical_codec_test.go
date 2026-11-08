package translator

import (
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// scalarValue turns a query away before strconv.ParseFloat copies it into an error; the answer must stay the one
// TrimSpace and ParseFloat give.
func TestScalarValueMatchesParseFloat(t *testing.T) {
	ref := func(s string) (float64, bool) {
		s = strings.TrimSpace(s)
		if s == "" {
			return 0, false
		}
		v, err := strconv.ParseFloat(s, 64)
		return v, err == nil
	}
	check := func(s string) {
		t.Helper()
		gv, gok := scalarValue(s)
		wv, wok := ref(s)
		if gok != wok || (gok && gv != wv && !(math.IsNaN(gv) && math.IsNaN(wv))) {
			t.Fatalf("scalarValue(%q) = %v, %v, want %v, %v", s, gv, gok, wv, wok)
		}
	}
	for _, s := range []string{"", " ", "1", "-42", "+1.5", "1e5", "1.5e-3", ".5", "5.", "inf", "-Inf", "+INF", "infinity", "NaN", "nan",
		"0x1p-2", "0X1.8P3", "0x_1p0", "1_000", "0x1_0p0", " 5 ", "\t5\n", " 5", "5 ", "1e", "e1", "abc", "nginx", "app:=x", "{app=\"x\"}",
		"nan(1)", "infinitx", "1 2", "0x", "--1", "1e400", "-1e400"} {
		check(s)
	}
	for b := 0; b < 256; b++ {
		check(string([]byte{byte(b)}))
		check("1" + string([]byte{byte(b)}))
		check(string([]byte{byte(b)}) + "1")
	}
	alphabet := "0123456789+-._ eExXpPaAfFiInNtTyYgz\t"
	for _, a := range alphabet {
		for _, b := range alphabet {
			for _, c := range alphabet {
				check(string([]rune{a, b, c}))
			}
		}
	}
}

// trimWithoutMarker takes the marker off the end of a text, and says no when the end is anything else.
func TestTrimWithoutMarker(t *testing.T) {
	for _, tc := range []struct {
		name, s, op string
		labels      []string
		rest        string
		ok          bool
	}{
		{name: "no labels", s: "q", rest: "q", ok: true},
		{name: "labels", s: "q__without__:a,b", labels: []string{"a", "b"}, rest: "q", ok: true},
		{name: "an op", s: "q__without__:sum:a", op: "sum", labels: []string{"a"}, rest: "q", ok: true},
		{name: "shorter than the marker", s: "a,b", labels: []string{"a", "b"}},
		{name: "another marker", s: "q__WITHOUT__:a", labels: []string{"a"}},
		{name: "another op", s: "q__without__:max:a", op: "sum", labels: []string{"a"}},
		{name: "no colon after the op", s: "q__without__:sum;a", op: "sum", labels: []string{"a"}},
		{name: "another separator", s: "q__without__:a;b", labels: []string{"a", "b"}},
		{name: "another label", s: "q__without__:a,c", labels: []string{"a", "b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rest, ok := trimWithoutMarker(tc.s, tc.op, tc.labels)
			if ok != tc.ok || (ok && rest != tc.rest) {
				t.Errorf("trimWithoutMarker(%q, %q, %q) = %q, %v; want %q, %v", tc.s, tc.op, tc.labels, rest, ok, tc.rest, tc.ok)
			}
			// what appendWithoutMarker writes is what is taken off
			if ok && len(tc.labels) > 0 && appendWithoutMarker(rest, tc.op, tc.labels) != tc.s {
				t.Errorf("appendWithoutMarker(%q, %q, %q) != %q", rest, tc.op, tc.labels, tc.s)
			}
		})
	}
}

// The operators with no text of their own write nothing.
func TestEncodePhysicalOfOperatorsWithoutText(t *testing.T) {
	for _, op := range []physical.Op{physical.VLStats{}, physical.RankStep{}, nil} {
		if got := EncodePhysical(op); got != "" {
			t.Errorf("EncodePhysical(%#v) = %q", op, got)
		}
	}
}
