package aggregator

import (
	"sync"
)

// FlushHandler processes an expired combo window (persist to DB, update leaderboard, etc.).
type FlushHandler func(w *ComboWindow)

// Flusher reads expired windows from the aggregator's flush channel and
// dispatches them to registered handlers via multiple worker goroutines.
type Flusher struct {
	agg     *Aggregator
	handler FlushHandler
	wg      sync.WaitGroup
	workers int
}

// NewFlusher creates a flusher with the given number of worker goroutines.
func NewFlusher(agg *Aggregator, handler FlushHandler, workers int) *Flusher {
	if workers <= 0 {
		workers = 4
	}
	return &Flusher{
		agg:     agg,
		handler: handler,
		workers: workers,
	}
}

// Start launches worker goroutines that read from the aggregator's flush channel.
func (f *Flusher) Start() {
	for i := 0; i < f.workers; i++ {
		f.wg.Add(1)
		go f.worker()
	}
}

// Shutdown waits for in-flight workers to finish.
func (f *Flusher) Shutdown() {
	f.wg.Wait()
}

func (f *Flusher) worker() {
	defer f.wg.Done()
	for w := range f.agg.FlushCh() {
		f.handler(w)
	}
}
