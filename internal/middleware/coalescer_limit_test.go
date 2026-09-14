package middleware

import (
	"strings"
	"testing"
)

func TestReadBodyPooledRejectsOverflow(t *testing.T) {
	for _, size := range []int{31, 32, 33} {
		body, err := readBodyPooled(strings.NewReader(strings.Repeat("x", size)), 32)
		if size > 32 {
			if err == nil || body != nil {
				t.Fatal("oversized response silently truncated")
			}
		} else if err != nil || len(body) != size {
			t.Fatalf("within limit: len=%d err=%v", len(body), err)
		}
	}
}
