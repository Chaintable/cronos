package app

import (
	"testing"
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cosmos/cosmos-sdk/baseapp"
	"github.com/evmos/ethermint/debank/replay"
	"github.com/stretchr/testify/require"
)

// The archive already contains both versions, as it does in historical replay.
func TestReplayConsecutiveBlocks(t *testing.T) {
	f := newReplayFixture(t, DefaultConsensusParams.Block.MaxGas)
	header := f.req.BeginBlock.Header
	header.Height++
	header.Time = header.Time.Add(time.Second)
	header.AppHash = f.wantHash
	begin := &abci.RequestBeginBlock{Header: header}
	next := replay.Request{ChainID: f.req.ChainID, ParentHash: f.wantHash, BeginBlock: begin}
	original := New(log.NewNopLogger(), f.db, nil, true, true, map[int64]bool{}, t.TempDir(), 0, MakeEncodingConfig(), EmptyAppOptions{}, baseapp.SetChainID(f.req.ChainID))
	original.BeginBlock(*begin)
	original.EndBlock(abci.RequestEndBlock{Height: header.Height})
	want := original.Commit().Data
	before := dbDigest(t, f.db)

	mark := func(string) {}
	a, err := OpenReplayApp(f.db, t.TempDir(), f.req.ChainID, f.req.BeginBlock.Header.Height-1, mark)
	require.NoError(t, err)
	first, err := a.ReplayNextBlock(f.req, mark)
	require.NoError(t, err)
	require.Equal(t, f.wantHash, first.AppHash)
	require.NotEmpty(t, first.Transactions)
	incorrect := next
	incorrect.ParentHash = make([]byte, 32)
	_, err = a.ReplayNextBlock(incorrect, mark)
	require.Error(t, err)

	got, err := a.ReplayNextBlock(next, mark)
	require.NoError(t, err)
	require.Equal(t, want, got.AppHash)
	require.Empty(t, got.Transactions)
	fresh, err := ReplayOnDB(f.db, t.TempDir(), next, mark)
	require.NoError(t, err)
	require.Equal(t, fresh, got)
	require.Equal(t, before, dbDigest(t, f.db))
}
