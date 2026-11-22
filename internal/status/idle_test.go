package status

import (
	"testing"
	"time"

	"github.com/fystack/multichain-indexer/pkg/common/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshot_ReportsIdleAndActiveUntil(t *testing.T) {
	r := NewRegistry()
	r.RegisterChain("BSC_TESTNET", "bsc_testnet", config.ChainConfig{NetworkId: "bsc_testnet"})
	r.RegisterChain("SOLANA_DEVNET", "solana_devnet", config.ChainConfig{NetworkId: "solana_devnet"})
	until := time.Date(2026, 11, 21, 13, 0, 0, 0, time.UTC)
	r.SetActivitySource("BSC_TESTNET", func() (bool, bool, time.Time) { return true, false, time.Time{} })
	r.SetActivitySource("SOLANA_DEVNET", func() (bool, bool, time.Time) { return true, true, until })

	snap := r.Snapshot("test")

	require.Len(t, snap.Networks, 2)
	bsc, sol := snap.Networks[0], snap.Networks[1]
	assert.Equal(t, HealthIdle, bsc.Health)
	assert.True(t, bsc.Idle)
	assert.Nil(t, bsc.ActiveUntil)
	assert.Equal(t, HealthHealthy, sol.Health)
	assert.False(t, sol.Idle)
	require.NotNil(t, sol.ActiveUntil)
	assert.Equal(t, until, *sol.ActiveUntil)
}
