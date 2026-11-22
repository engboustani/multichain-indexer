package worker

import (
	"context"
	"sync"
	"time"

	"github.com/fystack/multichain-indexer/pkg/common/config"
	"github.com/fystack/multichain-indexer/pkg/common/logger"
	"gorm.io/gorm"
)

// Chains with on_demand: true are read only while someone is waiting for a payment on them. Whoever hands out a
// deposit address (Treasury) writes a watch window to the watch_windows table: the network and when the payment is
// due. A chain is active until its latest window plus its idle_grace, so payments that come a little late are still
// seen; after that it's idle and makes no RPC calls at all.

// WatchWindowLoader reads the latest watch window of every network.
type WatchWindowLoader interface {
	LoadWatchWindows(ctx context.Context) (map[string]time.Time, error)
}

// DBWatchWindowLoader reads the watch_windows table (network_id text primary key, watch_until timestamptz).
type DBWatchWindowLoader struct{ db *gorm.DB }

func NewDBWatchWindowLoader(db *gorm.DB) *DBWatchWindowLoader { return &DBWatchWindowLoader{db: db} }

func (l *DBWatchWindowLoader) LoadWatchWindows(ctx context.Context) (map[string]time.Time, error) {
	var rows []struct {
		NetworkID  string
		WatchUntil time.Time
	}
	if err := l.db.WithContext(ctx).Raw("SELECT network_id, watch_until FROM watch_windows").Scan(&rows).Error; err != nil {
		return nil, err
	}
	out := make(map[string]time.Time, len(rows))
	for _, r := range rows {
		out[r.NetworkID] = r.WatchUntil
	}
	return out, nil
}

// ChainActivity keeps the watch windows in memory, reloaded every interval, and answers whether a chain is active.
type ChainActivity struct {
	loader   WatchWindowLoader
	interval time.Duration
	now      func() time.Time

	mu      sync.RWMutex
	windows map[string]time.Time
	loaded  bool
}

func NewChainActivity(loader WatchWindowLoader, interval time.Duration) *ChainActivity {
	if interval <= 0 {
		interval = time.Second
	}
	return &ChainActivity{loader: loader, interval: interval, now: time.Now, windows: map[string]time.Time{}}
}

// Start loads the windows once, then keeps them fresh until ctx ends.
func (a *ChainActivity) Start(ctx context.Context) {
	a.refresh(ctx)
	go func() {
		ticker := time.NewTicker(a.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				a.refresh(ctx)
			}
		}
	}()
}

func (a *ChainActivity) refresh(ctx context.Context) {
	windows, err := a.loader.LoadWatchWindows(ctx)
	if err != nil {
		logger.Warn("Loading watch windows failed; keeping the previous ones", "error", err)
		return
	}
	a.mu.Lock()
	a.windows = windows
	a.loaded = true
	a.mu.Unlock()
}

// State reports whether cfg's chain should be read now, and until when it's active (zero when it isn't on demand).
// Until the windows have loaded once, every chain is active: a missing table must not stop indexing.
func (a *ChainActivity) State(cfg config.ChainConfig) (active bool, until time.Time) {
	if a == nil || !cfg.IsOnDemand() {
		return true, time.Time{}
	}
	a.mu.RLock()
	window, loaded := a.windows[cfg.NetworkId], a.loaded
	a.mu.RUnlock()
	if !loaded {
		return true, time.Time{}
	}
	if window.IsZero() {
		return false, time.Time{}
	}
	until = window.Add(cfg.IdleGraceOrDefault())
	return a.now().Before(until), until
}
