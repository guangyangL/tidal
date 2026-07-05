package aggregator

import (
	"sync"
	"time"
)

const (
	defaultNumShards      = 64
	defaultMaxComboCount  = 100
	defaultMaxWindows     = 100000
)

// AddResult flags what the caller should do after Add.
const (
	HitAdded      = iota // combo counted in existing window, no action needed
	WindowCreated        // new window created, caller must pre-deduct balance
	WindowDropped        // shard overloaded, entry rejected (caller should still deduct)
)

type ComboKey struct {
	UserID   int64
	AnchorID int64
	GiftID   int64
	RoomID   int64
}

func GenComboKey(userID, anchorID, giftID, roomID int64) ComboKey {
	return ComboKey{
		UserID:   userID,
		AnchorID: anchorID,
		GiftID:   giftID,
		RoomID:   roomID,
	}
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
	shards            []*shard
	flushCh           chan *ComboWindow
	closeCh           chan struct{}
	windowTTL         time.Duration
	maxComboCount     int32
	maxWindowsPerShard int
	once              sync.Once
}

// NewAggregator creates a sharded sliding-window aggregator.
func NewAggregator(windowTTL time.Duration, numShards int, maxComboCount int32, maxWindowsPerShard int) *Aggregator {
	if numShards <= 0 {
		numShards = defaultNumShards
	}
	if maxComboCount <= 0 {
		maxComboCount = defaultMaxComboCount
	}
	if maxWindowsPerShard <= 0 {
		maxWindowsPerShard = defaultMaxWindows
	}
	s := make([]*shard, numShards)
	for i := 0; i < numShards; i++ {
		s[i] = &shard{windows: make(map[ComboKey]*ComboWindow, maxWindowsPerShard/numShards)}
	}
	return &Aggregator{
		shards:            s,
		flushCh:           make(chan *ComboWindow, 4096),
		closeCh:           make(chan struct{}),
		windowTTL:         windowTTL,
		maxComboCount:     maxComboCount,
		maxWindowsPerShard: maxWindowsPerShard,
	}
}

func (a *Aggregator) getShard(key ComboKey) *shard {
	h := fnv64(key)
	return a.shards[h%uint64(len(a.shards))]
}

// AddResult meanings:
//   - WindowCreated: first hit of a new window. Caller must pre-deduct balance.
//   - HitAdded: accumulated into an existing window. No balance check needed.
//   - WindowDropped: shard overloaded, entry not counted. Caller should still
//     deduct and handle independently (this is a safety valve, rare in practice).
func (a *Aggregator) Add(key ComboKey, price int64) (int, *ComboWindow) {
	s := a.getShard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	w, exists := s.windows[key]

	if !exists {
		// safety valve: prevent OOM from unique-key flood
		if len(s.windows) >= a.maxWindowsPerShard {
			return WindowDropped, nil
		}
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

	// force-flush if combo count exceeds limit
	if w.ComboCount >= a.maxComboCount {
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
func (a *Aggregator) tryFlush(w *ComboWindow) {
	select {
	case a.flushCh <- w:
	default:
	}
}

// Remove deletes a window from the aggregator so it won't be flushed.
func (a *Aggregator) Remove(key ComboKey) {
	s := a.getShard(key)
	s.mu.Lock()
	delete(s.windows, key)
	s.mu.Unlock()
}

// flushAll drains every shard's windows into the flush channel.
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
func (a *Aggregator) StartGC() {
	go a.gc()
}

// fnv64 is a fast non-cryptographic hash for ComboKey.
func fnv64(k ComboKey) uint64 {
	h := uint64(k.UserID)
	h ^= uint64(k.AnchorID) * 0x9e3779b97f4a7c15
	h ^= uint64(k.GiftID) * 0xbf58476d1ce4e5b9
	h ^= uint64(k.RoomID) * 0x9e3779b97f4a7c15
	return h
}
