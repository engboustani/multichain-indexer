package worker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/fystack/multichain-indexer/internal/indexer"
	"github.com/fystack/multichain-indexer/pkg/common/config"
	"github.com/fystack/multichain-indexer/pkg/common/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type staticWindows struct {
	windows map[string]time.Time
	err     error
}

func (s staticWindows) LoadWatchWindows(context.Context) (map[string]time.Time, error) {
	return s.windows, s.err
}

var activityNow = time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

func onDemandChain(network string, grace time.Duration) config.ChainConfig {
	onDemand := true
	return config.ChainConfig{NetworkId: network, OnDemand: &onDemand, IdleGrace: grace}
}

func loadedActivity(t *testing.T, windows map[string]time.Time) *ChainActivity {
	t.Helper()
	a := NewChainActivity(staticWindows{windows: windows}, time.Second)
	a.now = func() time.Time { return activityNow }
	a.refresh(context.Background())
	return a
}

func TestChainActivity_AlwaysOnChainsAreActive(t *testing.T) {
	a := loadedActivity(t, nil)

	active, _ := a.State(config.ChainConfig{NetworkId: "bsc_testnet"})

	assert.True(t, active)
}

func TestChainActivity_OnDemandChainWithoutWindowIsIdle(t *testing.T) {
	a := loadedActivity(t, map[string]time.Time{})

	active, _ := a.State(onDemandChain("bsc_testnet", time.Hour))

	assert.False(t, active)
}

func TestChainActivity_ActiveUntilWindowPlusGrace(t *testing.T) {
	expires := activityNow.Add(-30 * time.Minute) // the order expired half an hour ago
	a := loadedActivity(t, map[string]time.Time{"bsc_testnet": expires, "solana_devnet": expires})

	active, until := a.State(onDemandChain("bsc_testnet", time.Hour))
	assert.True(t, active, "inside the late-payment grace")
	assert.Equal(t, expires.Add(time.Hour), until)

	active, _ = a.State(onDemandChain("solana_devnet", 15*time.Minute))
	assert.False(t, active, "its shorter grace has passed")
}

func TestChainActivity_UnreadableWindowsKeepChainsActive(t *testing.T) {
	a := NewChainActivity(staticWindows{err: errors.New("relation \"watch_windows\" does not exist")}, time.Second)
	a.refresh(context.Background())

	active, _ := a.State(onDemandChain("bsc_testnet", time.Hour))

	assert.True(t, active, "a missing table must not stop indexing")
}

func TestChainActivity_NilIsAlwaysActive(t *testing.T) {
	var a *ChainActivity

	active, _ := a.State(onDemandChain("bsc_testnet", time.Hour))

	assert.True(t, active)
}

func TestRegularWorker_IdleChainMakesNoCalls(t *testing.T) {
	chain := &stubIndexer{name: "BSC_TESTNET", internalCode: "BSC_TESTNET", latestErr: errors.New("must not be called")}
	rw := newTestRegularWorker(chain, &stubBlockStore{}, 1000, 10)
	makeOnDemand(rw, time.Hour)
	rw.activity = loadedActivity(t, map[string]time.Time{})

	require.NoError(t, rw.processRegularBlocks())

	assert.True(t, rw.idle)
	assert.Equal(t, uint64(1000), rw.currentBlock)
}

func TestRegularWorker_ResumesNearHeadWithoutBackfill(t *testing.T) {
	chain := &stubIndexer{name: "BSC_TESTNET", internalCode: "BSC_TESTNET", latest: 50_000}
	store := &stubBlockStore{}
	rw := newTestRegularWorker(chain, store, 1000, 10)
	makeOnDemand(rw, time.Hour)
	rw.idle = true
	rw.activity = loadedActivity(t, map[string]time.Time{"bsc_testnet": activityNow.Add(15 * time.Minute)})
	chain.getBlocksFunc = func(_ context.Context, from, to uint64, _ bool) ([]indexer.BlockResult, error) {
		return blockResults(from, to), nil
	}

	require.NoError(t, rw.processRegularBlocks())

	assert.False(t, rw.idle)
	assert.Empty(t, store.savedCatchupRanges, "the idle stretch isn't backfilled")
	assert.Contains(t, store.savedLatest, uint64(50_000-resumeMarginBlocks))
	assert.Equal(t, uint64(50_000-resumeMarginBlocks+1+10), rw.currentBlock, "read one batch from the margin")
}

func makeOnDemand(rw *RegularWorker, grace time.Duration) {
	onDemand := true
	rw.config.NetworkId = "bsc_testnet"
	rw.config.OnDemand = &onDemand
	rw.config.IdleGrace = grace
}

func blockResults(from, to uint64) []indexer.BlockResult {
	out := make([]indexer.BlockResult, 0, to-from+1)
	for n := from; n <= to; n++ {
		out = append(out, indexer.BlockResult{Number: n, Block: &types.Block{
			Number: n, Hash: fmt.Sprintf("0x%d", n), ParentHash: fmt.Sprintf("0x%d", n-1),
		}})
	}
	return out
}

func TestRegularWorker_RestartWhileIdleQueuesNoGap(t *testing.T) {
	chain := &stubIndexer{name: "BSC_TESTNET", internalCode: "BSC_TESTNET", latestErr: errors.New("must not be called")}
	store := &stubBlockStore{latestBlock: 1000}
	rw := newTestRegularWorker(chain, store, 0, 10)
	rw.idle = true

	assert.Equal(t, uint64(1000), rw.determineStartingBlock())
	assert.Empty(t, store.savedCatchupRanges)
}

func TestRescannerWorker_IdleChainRetriesNothing(t *testing.T) {
	rw := &RescannerWorker{BaseWorker: &BaseWorker{config: onDemandChain("bsc_testnet", time.Hour)}, interval: time.Millisecond}
	rw.activity = loadedActivity(t, map[string]time.Time{})

	require.NoError(t, rw.processRescan(), "returns before touching the store or the chain")
}
