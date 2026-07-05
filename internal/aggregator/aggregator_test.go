package aggregator

import (
	"sync"
	"testing"
	"time"
)

func TestAdd(t *testing.T) {
	a := NewAggregator(time.Hour, 4, 100, 1000)
	key := ComboKey{UserID: 1, AnchorID: 1, GiftID: 1}

	result, w := a.Add(key, 100)
	if result != WindowCreated {
		t.Fatalf("expected WindowCreated, got %d", result)
	}
	if w.ComboCount != 1 || w.TotalAmount != 100 {
		t.Fatalf("combo: count=%d amount=%d", w.ComboCount, w.TotalAmount)
	}

	result, w = a.Add(key, 100)
	if result != HitAdded {
		t.Fatalf("expected HitAdded, got %d", result)
	}
	if w.ComboCount != 2 || w.TotalAmount != 200 {
		t.Fatalf("combo: count=%d amount=%d", w.ComboCount, w.TotalAmount)
	}

	key2 := ComboKey{UserID: 1, AnchorID: 2, GiftID: 1}
	result, w2 := a.Add(key2, 100)
	if result != WindowCreated {
		t.Fatalf("expected WindowCreated for new key, got %d", result)
	}
	if w2.ComboCount != 1 {
		t.Fatalf("independent window should start at 1, got %d", w2.ComboCount)
	}
}

func TestWindowExpiryTriggersFlush(t *testing.T) {
	a := NewAggregator(30*time.Millisecond, 4, 100, 1000)
	key := ComboKey{UserID: 10, AnchorID: 5, GiftID: 2}

	// open a window
	a.Add(key, 200)

	// wait for it to expire
	time.Sleep(50 * time.Millisecond)

	// Add same key again: Add() sees the old window expired,
	// pushes it to flushCh, and creates a new one
	result, w := a.Add(key, 300)
	if result != WindowCreated {
		t.Fatalf("expected WindowCreated after expiry, got %d", result)
	}
	if w.ComboCount != 1 || w.TotalAmount != 300 {
		t.Fatalf("fresh window should have count=1 amount=300, got count=%d amount=%d",
			w.ComboCount, w.TotalAmount)
	}

	// the old window should be in flushCh
	select {
	case flushed := <-a.FlushCh():
		if flushed.TotalAmount != 200 || flushed.ComboCount != 1 {
			t.Fatalf("flushed window: amount=%d count=%d", flushed.TotalAmount, flushed.ComboCount)
		}
	case <-time.After(time.Second):
		t.Fatal("expired window not in flush channel within 1s")
	}
}

func TestConcurrentAdd(t *testing.T) {
	a := NewAggregator(time.Hour, 8, 100, 1000)

	var wg sync.WaitGroup
	n := 1000

	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := ComboKey{UserID: int64(i % 10), AnchorID: 1, GiftID: 1}
			a.Add(key, 10)
		}(i)
	}
	wg.Wait()

	for uid := range 10 {
		s := a.getShard(ComboKey{UserID: int64(uid), AnchorID: 1, GiftID: 1})
		s.mu.Lock()
		w, exists := s.windows[ComboKey{UserID: int64(uid), AnchorID: 1, GiftID: 1}]
		s.mu.Unlock()
		if !exists {
			t.Fatalf("user %d window should exist", uid)
		}
		if w.ComboCount < 50 || w.ComboCount > 150 {
			t.Fatalf("user %d unexpected combo count: %d", uid, w.ComboCount)
		}
	}
}

func TestShutdownFlushesAll(t *testing.T) {
	a := NewAggregator(time.Hour, 4, 100, 1000)
	for uid := range 10 {
		a.Add(ComboKey{UserID: int64(uid), AnchorID: 1, GiftID: 1}, 100)
	}

	a.Shutdown()

	count := 0
	for range a.FlushCh() {
		count++
	}
	if count != 10 {
		t.Fatalf("expected 10 flushed windows, got %d", count)
	}
}

func TestFlusherWorkers(t *testing.T) {
	a := NewAggregator(10*time.Millisecond, 4, 100, 1000)

	var mu sync.Mutex
	var flushed []*ComboWindow

	f := NewFlusher(a, func(w *ComboWindow) {
		mu.Lock()
		flushed = append(flushed, w)
		mu.Unlock()
	}, 2)
	f.Start()

	for uid := range 5 {
		result, _ := a.Add(ComboKey{UserID: int64(uid), AnchorID: 1, GiftID: 1}, 100)
		if result != WindowCreated {
			t.Fatalf("expected WindowCreated, got %d", result)
		}
	}

	a.Shutdown()
	f.Shutdown()

	if len(flushed) != 5 {
		t.Fatalf("expected 5 flushed windows, got %d", len(flushed))
	}
}
