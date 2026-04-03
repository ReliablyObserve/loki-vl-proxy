package cache

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkCache_Set(b *testing.B) {
	c := New(60*time.Second, 100000)
	value := []byte(`{"status":"success","data":["app","env","level"]}`)
	for b.Loop() {
		c.Set(fmt.Sprintf("key-%d", b.N), value)
	}
}

func BenchmarkCache_Get_Hit(b *testing.B) {
	c := New(60*time.Second, 100000)
	value := []byte(`{"status":"success","data":["app","env","level"]}`)
	c.Set("bench-key", value)
	for b.Loop() {
		c.Get("bench-key")
	}
}

func BenchmarkCache_Get_Miss(b *testing.B) {
	c := New(60*time.Second, 100000)
	for b.Loop() {
		c.Get("nonexistent")
	}
}

func BenchmarkCache_SetWithTTL(b *testing.B) {
	c := New(60*time.Second, 100000)
	value := []byte(`{"data":"test"}`)
	for b.Loop() {
		c.SetWithTTL(fmt.Sprintf("key-%d", b.N), value, 30*time.Second)
	}
}
