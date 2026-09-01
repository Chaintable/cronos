package cmd

import (
	"fmt"
	"testing"
	"time"

	dbm "github.com/cometbft/cometbft-db"
	cmtcrypto "github.com/cometbft/cometbft/crypto"
	"github.com/cometbft/cometbft/crypto/tmhash"
	cmtstateproto "github.com/cometbft/cometbft/proto/tendermint/state"
	cmtversion "github.com/cometbft/cometbft/proto/tendermint/version"
	cmtstate "github.com/cometbft/cometbft/state"
	cmtstore "github.com/cometbft/cometbft/store"
	cmttypes "github.com/cometbft/cometbft/types"
	"github.com/cometbft/cometbft/version"
	"github.com/stretchr/testify/require"
)

func TestRewindCometBFT(t *testing.T) {
	blockStore := cmtstore.NewBlockStore(dbm.NewMemDB())
	stateStore := cmtstate.NewStore(dbm.NewMemDB(), cmtstate.StoreOptions{})
	validatorSet, _ := cmttypes.RandValidatorSet(1, 10)
	params := cmttypes.DefaultConsensusParams()

	var lastBlockID cmttypes.BlockID
	for height := int64(1); height <= 5; height++ {
		block := &cmttypes.Block{
			Header: cmttypes.Header{
				Version:            cmtversion.Consensus{Block: version.BlockProtocol, App: 1},
				ChainID:            "rewind-test",
				Height:             height,
				Time:               time.Unix(height, 0).UTC(),
				LastBlockID:        lastBlockID,
				LastCommitHash:     cmtcrypto.CRandBytes(tmhash.Size),
				DataHash:           cmtcrypto.CRandBytes(tmhash.Size),
				ValidatorsHash:     validatorSet.Hash(),
				NextValidatorsHash: validatorSet.Hash(),
				ConsensusHash:      params.Hash(),
				AppHash:            testAppHash(height - 1),
				LastResultsHash:    tmhash.Sum([]byte(fmt.Sprintf("results-%d", height-1))),
				EvidenceHash:       cmtcrypto.CRandBytes(tmhash.Size),
				ProposerAddress:    cmtcrypto.CRandBytes(cmtcrypto.AddressSize),
			},
			LastCommit: &cmttypes.Commit{Height: height - 1},
		}
		parts, err := block.MakePartSet(cmttypes.BlockPartSizeBytes)
		require.NoError(t, err)
		blockStore.SaveBlock(block, parts, &cmttypes.Commit{Height: height})
		lastBlockID = blockStore.LoadBlockMeta(height).BlockID

		state := cmtstate.State{
			Version: cmtstateproto.Version{
				Consensus: block.Version,
				Software:  version.TMCoreSemVer,
			},
			ChainID:                          block.ChainID,
			InitialHeight:                    1,
			LastBlockHeight:                  height,
			LastBlockID:                      lastBlockID,
			LastBlockTime:                    block.Time,
			NextValidators:                   validatorSet,
			Validators:                       validatorSet,
			LastValidators:                   validatorSet,
			LastHeightValidatorsChanged:      2,
			ConsensusParams:                  *params,
			LastHeightConsensusParamsChanged: 2,
			LastResultsHash:                  tmhash.Sum([]byte(fmt.Sprintf("results-%d", height))),
			AppHash:                          testAppHash(height),
		}
		if height == 1 {
			require.NoError(t, stateStore.Bootstrap(state))
		} else {
			require.NoError(t, stateStore.Save(state))
		}
	}

	var progressCalls int
	height, hash, removed, err := rewindCometBFT(blockStore, stateStore, 2, func(_, _ int64) {
		progressCalls++
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, height)
	require.Equal(t, testAppHash(2), hash)
	require.EqualValues(t, 3, removed)
	require.Equal(t, 3, progressCalls)
	require.EqualValues(t, 2, blockStore.Height())
	require.Nil(t, blockStore.LoadBlockMeta(3))

	state, err := stateStore.Load()
	require.NoError(t, err)
	require.EqualValues(t, 2, state.LastBlockHeight)
	require.Equal(t, testAppHash(2), state.AppHash)
}

func TestRewindCometBFTRejectsFutureTarget(t *testing.T) {
	blockStore := cmtstore.NewBlockStore(dbm.NewMemDB())
	stateStore := cmtstate.NewStore(dbm.NewMemDB(), cmtstate.StoreOptions{})

	_, _, _, err := rewindCometBFT(blockStore, stateStore, 2, nil)
	require.ErrorContains(t, err, "no CometBFT state found")
}

func testAppHash(height int64) []byte {
	return tmhash.Sum([]byte(fmt.Sprintf("app-%d", height)))
}
