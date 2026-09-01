package cmd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"

	cmtdb "github.com/cometbft/cometbft-db"
	cmtabci "github.com/cometbft/cometbft/abci/types"
	cmtcfg "github.com/cometbft/cometbft/config"
	cosmosdb "github.com/cosmos/cosmos-db"
	"github.com/cosmos/cosmos-sdk/server"
	ethermintserver "github.com/evmos/ethermint/server"
	"github.com/spf13/cobra"

	"github.com/cosmos/gogoproto/proto"
)

const (
	deleteBatchSize   = 10_000
	indexScanInterval = 1_000_000
)

var (
	blockEventPrefix = []byte("block_events")
	evmTxHashPrefix  = byte(1)
	evmTxIndexPrefix = byte(2)
)

type indexTrimStats struct {
	Scanned        int64
	TxEventKeys    int64
	TxHashKeys     int64
	BlockEventKeys int64
	EVMTxIndexKeys int64
	EVMTxHashKeys  int64
}

// RewindIndexesCmd removes transaction and block-event index entries above a
// target height. It is idempotent and must be run with cronosd stopped.
func RewindIndexesCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "rewind-indexes [height]",
		Short: "Remove CometBFT and EVM index entries above a specific height",
		Long: "Remove CometBFT transaction/block-event and EVM transaction index " +
			"entries above a specific height. Stop cronosd before running this command.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			target, err := strconv.ParseInt(args[0], 10, 64)
			if err != nil {
				return fmt.Errorf("parse target height: %w", err)
			}
			if target <= 0 {
				return fmt.Errorf("target height must be positive: %d", target)
			}

			ctx := server.GetServerContextFromCmd(cmd)
			cfg := ctx.Config

			txIndexDB, err := cmtcfg.DefaultDBProvider(&cmtcfg.DBContext{ID: "tx_index", Config: cfg})
			if err != nil {
				return fmt.Errorf("open CometBFT index database: %w", err)
			}
			stats, err := trimCometIndexes(txIndexDB, target, func(scanned int64) {
				if scanned%indexScanInterval == 0 {
					cmd.Printf("CometBFT index scan progress keys=%d\n", scanned)
				}
			})
			closeErr := txIndexDB.Close()
			if err != nil {
				return err
			}
			if closeErr != nil {
				return fmt.Errorf("close CometBFT index database: %w", closeErr)
			}

			evmIndexDB, err := ethermintserver.OpenIndexerDB(cfg.RootDir, server.GetAppDBBackend(ctx.Viper))
			if err != nil {
				return fmt.Errorf("open EVM index database: %w", err)
			}
			evmStats, err := trimEVMIndexes(evmIndexDB, target)
			closeErr = evmIndexDB.Close()
			if err != nil {
				return err
			}
			if closeErr != nil {
				return fmt.Errorf("close EVM index database: %w", closeErr)
			}

			stats.EVMTxIndexKeys = evmStats.EVMTxIndexKeys
			stats.EVMTxHashKeys = evmStats.EVMTxHashKeys
			cmd.Printf(
				"rewound indexes to height %d scanned=%d tx_events=%d tx_hashes=%d block_events=%d evm_tx_indexes=%d evm_tx_hashes=%d\n",
				target,
				stats.Scanned,
				stats.TxEventKeys,
				stats.TxHashKeys,
				stats.BlockEventKeys,
				stats.EVMTxIndexKeys,
				stats.EVMTxHashKeys,
			)
			return nil
		},
	}
}

func trimCometIndexes(db cmtdb.DB, target int64, progress func(scanned int64)) (indexTrimStats, error) {
	var stats indexTrimStats
	var deleteKeys [][]byte
	txHashes := make(map[string][]byte)

	iterator, err := db.Iterator(nil, nil)
	if err != nil {
		return stats, fmt.Errorf("iterate CometBFT indexes: %w", err)
	}
	for ; iterator.Valid(); iterator.Next() {
		stats.Scanned++
		if progress != nil {
			progress(stats.Scanned)
		}

		key := iterator.Key()
		value := iterator.Value()
		if bytes.HasPrefix(key, blockEventPrefix) {
			height, n := binary.Varint(value)
			if n <= 0 {
				_ = iterator.Close()
				return stats, fmt.Errorf("decode block-event index height key=%X value=%X", key, value)
			}
			if height > target {
				deleteKeys = append(deleteKeys, bytes.Clone(key))
				stats.BlockEventKeys++
			}
			continue
		}

		height, ok := txEventHeight(key, value)
		if ok && height > target {
			deleteKeys = append(deleteKeys, bytes.Clone(key))
			txHashes[string(value)] = bytes.Clone(value)
			stats.TxEventKeys++
		}
	}
	if err := iterator.Error(); err != nil {
		_ = iterator.Close()
		return stats, fmt.Errorf("scan CometBFT indexes: %w", err)
	}
	if err := iterator.Close(); err != nil {
		return stats, fmt.Errorf("close CometBFT index iterator: %w", err)
	}

	for _, hash := range txHashes {
		value, err := db.Get(hash)
		if err != nil {
			return stats, fmt.Errorf("read transaction index %X: %w", hash, err)
		}
		if value == nil {
			continue
		}
		var result cmtabci.TxResult
		if err := proto.Unmarshal(value, &result); err != nil {
			return stats, fmt.Errorf("decode transaction index %X: %w", hash, err)
		}
		if result.Height > target {
			deleteKeys = append(deleteKeys, hash)
			stats.TxHashKeys++
		}
	}

	if err := writeDeleteBatches(func() deleteBatch { return db.NewBatch() }, deleteKeys); err != nil {
		return stats, fmt.Errorf("delete CometBFT index suffix: %w", err)
	}
	return stats, nil
}

func trimEVMIndexes(db cosmosdb.DB, target int64) (indexTrimStats, error) {
	var stats indexTrimStats
	var deleteKeys [][]byte

	iterator, err := db.Iterator([]byte{evmTxIndexPrefix}, []byte{evmTxIndexPrefix + 1})
	if err != nil {
		return stats, fmt.Errorf("iterate EVM indexes: %w", err)
	}
	for ; iterator.Valid(); iterator.Next() {
		key := iterator.Key()
		value := iterator.Value()
		if len(key) != 17 || len(value) != 32 {
			_ = iterator.Close()
			return stats, fmt.Errorf("invalid EVM index entry key=%X value_length=%d", key, len(value))
		}
		if int64(binary.BigEndian.Uint64(key[1:9])) > target {
			deleteKeys = append(deleteKeys, bytes.Clone(key))
			deleteKeys = append(deleteKeys, append([]byte{evmTxHashPrefix}, value...))
			stats.EVMTxIndexKeys++
			stats.EVMTxHashKeys++
		}
	}
	if err := iterator.Error(); err != nil {
		_ = iterator.Close()
		return stats, fmt.Errorf("scan EVM indexes: %w", err)
	}
	if err := iterator.Close(); err != nil {
		return stats, fmt.Errorf("close EVM index iterator: %w", err)
	}

	if err := writeDeleteBatches(func() deleteBatch { return db.NewBatch() }, deleteKeys); err != nil {
		return stats, fmt.Errorf("delete EVM index suffix: %w", err)
	}
	return stats, nil
}

func txEventHeight(key, value []byte) (int64, bool) {
	if len(value) != 32 {
		return 0, false
	}
	lastSlash := bytes.LastIndexByte(key, '/')
	if lastSlash <= 0 {
		return 0, false
	}
	previousSlash := bytes.LastIndexByte(key[:lastSlash], '/')
	if previousSlash <= 0 || bytes.LastIndexByte(key[:previousSlash], '/') < 0 {
		return 0, false
	}
	if !validEventIndexSuffix(key[lastSlash+1:]) {
		return 0, false
	}

	height, err := strconv.ParseInt(string(key[previousSlash+1:lastSlash]), 10, 64)
	if err != nil || height <= 0 {
		return 0, false
	}
	return height, true
}

func validEventIndexSuffix(value []byte) bool {
	parts := bytes.Split(value, []byte("$es$"))
	if len(parts) != 2 || len(parts[0]) == 0 || len(parts[1]) == 0 {
		return false
	}
	for _, part := range parts {
		for _, char := range part {
			if char < '0' || char > '9' {
				return false
			}
		}
	}
	return true
}

type deleteBatch interface {
	Delete(key []byte) error
	WriteSync() error
	Close() error
}

func writeDeleteBatches(newBatch func() deleteBatch, keys [][]byte) error {
	for start := 0; start < len(keys); start += deleteBatchSize {
		end := start + deleteBatchSize
		if end > len(keys) {
			end = len(keys)
		}

		batch := newBatch()
		for _, key := range keys[start:end] {
			if err := batch.Delete(key); err != nil {
				_ = batch.Close()
				return err
			}
		}
		if err := batch.WriteSync(); err != nil {
			_ = batch.Close()
			return err
		}
		if err := batch.Close(); err != nil {
			return err
		}
	}
	return nil
}
