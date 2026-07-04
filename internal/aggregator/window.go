package aggregator

import (
	"sync"
	"time"
)

const defaultNumShards = 64

// AddResult flags what the caller should do after Add.
const (
	HitAdded      = iota // combo counted in existing window, no action needed
	WindowCreated        // new window created, caller must pre-deduct balance
)

type ComboKey struct {
	UserID   int64
	AnchorID int64
	GiftID   int
}

type ComboWindow struct {
	Key         ComboKey
	ComboCount  int32
	TotalAmount int64
	WindowStart time.Time
}

type shard struct {
	mu      sync.Mutex
	windows map[ComboKey]*ComboWindow
}

type Aggregator struct {
	shards    []*shard
	flushCh   chan *ComboWindow
	closeCh   chan struct{}
	windowTTL time.Duration
	once      sync.Once
}

// NewAggregator creates a sharded sliding-window aggregator.
// Flusher workers should be started separately via StartFlushers.
func NewAggregator(windowTTL time.Duration, numShards int) *Aggregator {
	if numShards <= 0 {
		numShards = defaultNumShards
	}
	s := make([]*shard, numShards)
	for i := 0; i < numShards; i++ {
		s[i] = &shard{windows: make(map[ComboKey]*ComboWindow)}
	}
	return &Aggregator{
		shards:    s,
		flushCh:   make(chan *ComboWindow, 4096),
		closeCh:   make(chan struct{}),
		windowTTL: windowTTL,
	}
}

func (a *Aggregator) getShard(key ComboKey) *shard {
	h := fnv64(key)
	return a.shards[h%uint64(len(a.shards))]
}

// Add records a combo hit and returns whether a new window was created.
//
// Result meanings:
//   - WindowCreated: this is the first hit of a new window. The caller must
//     pre-deduct the user's balance (Redis) before the window is counted.
//   - HitAdded: the hit was accumulated into an existing window. No balance
//     check needed.
//
// In either case, the caller should return a success response to the client
// immediately. The actual DB flush happens asynchronously.
func (a *Aggregator) Add(key ComboKey, price int64) (int, *ComboWindow) {
	s := a.getShard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	w, exists := s.windows[key]

	if !exists {
		w = &ComboWindow{
			Key:         key,
			ComboCount:  1,
			TotalAmount: price,
			WindowStart: now,
		}
		s.windows[key] = w
		return WindowCreated, w
	}

	if now.Sub(w.WindowStart) >= a.windowTTL {
		old := *w
		a.tryFlush(&old)
		w.ComboCount = 1
		w.TotalAmount = price
		w.WindowStart = now
		return WindowCreated, w
	}

	w.ComboCount++
	w.TotalAmount += price
	return HitAdded, w
}

// FlushCh returns the channel that expired windows are sent to.
func (a *Aggregator) FlushCh() <-chan *ComboWindow {
	return a.flushCh
}

// Shutdown gracefully stops the aggregator. It drains all shards and
// flushes remaining windows before closing the flush channel.
func (a *Aggregator) Shutdown() {
	a.once.Do(func() {
		close(a.closeCh)
		a.flushAll()
		close(a.flushCh)
	})
}

// tryFlush sends a window to the flush channel, dropping it if full.
// The gc loop will pick up stragglers.
func (a *Aggregator) tryFlush(w *ComboWindow) {
	select {
	case a.flushCh <- w:
	default:
	}
}

// flushAll drains every shard's windows into the flush channel.
// Called during graceful shutdown.
func (a *Aggregator) flushAll() {
	for _, s := range a.shards {
		s.mu.Lock()
		for key, w := range s.windows {
			a.flushCh <- w
			delete(s.windows, key)
		}
		s.mu.Unlock()
	}
}

func (a *Aggregator) gc() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-a.closeCh:
			return
		case <-ticker.C:
			a.scanExpired()
		}
	}
}

func (a *Aggregator) scanExpired() {
	for _, s := range a.shards {
		s.mu.Lock()
		now := time.Now()
		for key, w := range s.windows {
			if now.Sub(w.WindowStart) >= a.windowTTL {
				a.tryFlush(w)
				delete(s.windows, key)
			}
		}
		s.mu.Unlock()
	}
}

// StartGC starts the background expired-window scanner.
// Should be called once after NewAggregator.
func (a *Aggregator) StartGC() {
	go a.gc()
}

// -- helpers --

// fnv64 is a fast non-cryptographic hash for ComboKey.
func fnv64(k ComboKey) uint64 {
	h := uint64(k.UserID)
	h ^= uint64(k.AnchorID) * 0x9e3779b97f4a7c15
	h ^= uint64(k.GiftID) * 0xbf58476d1ce4e5b9
	return h
}
