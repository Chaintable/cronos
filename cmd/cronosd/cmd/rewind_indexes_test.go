package cmd

import (
	"testing"

	cmtdb "github.com/cometbft/cometbft-db"
	cmtabci "github.com/cometbft/cometbft/abci/types"
	cmtquery "github.com/cometbft/cometbft/libs/pubsub/query"
	cmtblockidxkv "github.com/cometbft/cometbft/state/indexer/block/kv"
	cmttxidxkv "github.com/cometbft/cometbft/state/txindex/kv"
	cmttypes "github.com/cometbft/cometbft/types"
	cosmosdb "github.com/cosmos/cosmos-db"
	"github.com/ethereum/go-ethereum/common"
	ethindexer "github.com/evmos/ethermint/indexer"
	"github.com/stretchr/testify/require"
)

func TestTrimCometIndexes(t *testing.T) {
	db := cmtdb.NewMemDB()
	txIndexer := cmttxidxkv.NewTxIndex(db)
	blockIndexer := cmtblockidxkv.New(cmtdb.NewPrefixDB(db, blockEventPrefix))

	lowTx := indexTestTxResult(10, "low/value")
	highTx := indexTestTxResult(11, "high/value")
	require.NoError(t, txIndexer.Index(lowTx))
	require.NoError(t, txIndexer.Index(highTx))
	require.NoError(t, blockIndexer.Index(indexTestBlockEvents(10)))
	require.NoError(t, blockIndexer.Index(indexTestBlockEvents(11)))

	stats, err := trimCometIndexes(db, 10, nil)
	require.NoError(t, err)
	require.EqualValues(t, 2, stats.TxEventKeys)
	require.EqualValues(t, 1, stats.TxHashKeys)
	require.EqualValues(t, 2, stats.BlockEventKeys)

	result, err := txIndexer.Get(cmttypes.Tx(lowTx.Tx).Hash())
	require.NoError(t, err)
	require.NotNil(t, result)
	result, err = txIndexer.Get(cmttypes.Tx(highTx.Tx).Hash())
	require.NoError(t, err)
	require.Nil(t, result)

	lowBlocks, err := blockIndexer.Search(t.Context(), cmtquery.MustCompile("block.height = 10"))
	require.NoError(t, err)
	require.Equal(t, []int64{10}, lowBlocks)
	highBlocks, err := blockIndexer.Search(t.Context(), cmtquery.MustCompile("block.height = 11"))
	require.NoError(t, err)
	require.Empty(t, highBlocks)
}

func TestTrimEVMIndexes(t *testing.T) {
	db := cosmosdb.NewMemDB()
	lowHash := common.HexToHash("0x10")
	highHash := common.HexToHash("0x11")

	require.NoError(t, db.Set(ethindexer.TxIndexKey(10, 0), lowHash.Bytes()))
	require.NoError(t, db.Set(ethindexer.TxHashKey(lowHash), []byte("low")))
	require.NoError(t, db.Set(ethindexer.TxIndexKey(11, 0), highHash.Bytes()))
	require.NoError(t, db.Set(ethindexer.TxHashKey(highHash), []byte("high")))

	stats, err := trimEVMIndexes(db, 10)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.EVMTxIndexKeys)
	require.EqualValues(t, 1, stats.EVMTxHashKeys)

	require.True(t, dbHas(t, db, ethindexer.TxIndexKey(10, 0)))
	require.True(t, dbHas(t, db, ethindexer.TxHashKey(lowHash)))
	require.False(t, dbHas(t, db, ethindexer.TxIndexKey(11, 0)))
	require.False(t, dbHas(t, db, ethindexer.TxHashKey(highHash)))
}

func indexTestTxResult(height int64, value string) *cmtabci.TxResult {
	return &cmtabci.TxResult{
		Height: height,
		Tx:     []byte(value),
		Result: cmtabci.ExecTxResult{
			Events: []cmtabci.Event{{
				Type: "test",
				Attributes: []cmtabci.EventAttribute{{
					Key: "value", Value: value, Index: true,
				}},
			}},
		},
	}
}

func indexTestBlockEvents(height int64) cmttypes.EventDataNewBlockEvents {
	return cmttypes.EventDataNewBlockEvents{
		Height: height,
		Events: []cmtabci.Event{{
			Type: "test",
			Attributes: []cmtabci.EventAttribute{{
				Key: "height", Value: "indexed", Index: true,
			}},
		}},
	}
}

func dbHas(t *testing.T, db cosmosdb.DB, key []byte) bool {
	t.Helper()
	found, err := db.Has(key)
	require.NoError(t, err)
	return found
}
