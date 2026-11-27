package app

import (
	"crypto/sha256"
	"testing"

	"cosmossdk.io/log"
	dbm "github.com/cosmos/cosmos-db"
	"github.com/cosmos/cosmos-sdk/client/flags"
	"github.com/evmos/ethermint/debank/replay"
	"github.com/stretchr/testify/require"
)

func dbDigest(t *testing.T, db dbm.DB) [32]byte {
	it, err := db.Iterator(nil, nil)
	require.NoError(t, err)
	defer it.Close()
	h := sha256.New()
	for ; it.Valid(); it.Next() {
		h.Write(it.Key())
		h.Write(it.Value())
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// The archive already contains both versions, as it does in historical replay.
func TestReplayConsecutiveBlocks(t *testing.T) {
	db, original, finalize, block2, senders := newReplayChain(t)
	parentHash := original.LastCommitID().Hash
	want2, err := original.FinalizeBlock(block2)
	require.NoError(t, err)
	_, err = original.Commit()
	require.NoError(t, err)
	block3 := finalize(3)
	want3, err := original.FinalizeBlock(block3)
	require.NoError(t, err)
	_, err = original.Commit()
	require.NoError(t, err)
	before := dbDigest(t, db)

	a, err := OpenReplayApp(db, t.TempDir(), TestAppChainID, 1)
	require.NoError(t, err)
	first, err := a.ReplayNextBlock(replay.Request{ChainID: TestAppChainID, ParentHash: parentHash, Request: block2})
	require.NoError(t, err)
	require.Equal(t, want2.AppHash, first.Finalize.AppHash)
	require.Len(t, first.Transactions, senders)

	next := replay.Request{ChainID: TestAppChainID, ParentHash: want2.AppHash, Request: block3}
	incorrect := next
	incorrect.ParentHash = make([]byte, 32)
	_, err = a.ReplayNextBlock(incorrect)
	require.ErrorContains(t, err, "parent commit")
	got, err := a.ReplayNextBlock(next)
	require.NoError(t, err)
	require.Equal(t, want3.AppHash, got.Finalize.AppHash)
	require.Empty(t, got.Transactions)

	// A single-block app over the archive gives the same result.
	fresh := NewReplayApp(log.NewNopLogger(), db, MinimalOptionsMap{flags.FlagHome: t.TempDir(), FlagDisableOptimisticExecution: true}, TestAppChainID)
	require.NoError(t, fresh.LoadReplayParent(2))
	freshTxs := ObserveReplayTransactions(fresh.EvmKeeper)
	freshRes, err := fresh.FinalizeBlock(block3)
	require.NoError(t, err)
	require.Equal(t, freshRes, got.Finalize)
	require.Empty(t, *freshTxs)
	require.Equal(t, before, dbDigest(t, db), "replay must not write the archive")
}
