package cmd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
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
	txHeightPrefix   = []byte("tx.height/")
	evmTxHashPrefix  = byte(1)
	evmTxIndexPrefix = byte(2)
)

type indexTrimStats struct {
	Scanned        int64
	TxHeightKeys   int64
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
			stats, err := trimCometIndexes(txIndexDB, target, func(stage string, scanned int64) {
				if scanned%indexScanInterval == 0 {
					cmd.Printf("CometBFT %s scan progress keys=%d\n", stage, scanned)
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
				"rewound indexes to height %d scanned=%d tx_heights=%d tx_events=%d tx_hashes=%d block_events=%d evm_tx_indexes=%d evm_tx_hashes=%d\n",
				target,
				stats.Scanned,
				stats.TxHeightKeys,
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

type indexedTransaction struct {
	height    int64
	heightKey []byte
	hash      []byte
}

func trimCometIndexes(
	db cmtdb.DB,
	target int64,
	progress func(stage string, scanned int64),
) (indexTrimStats, error) {
	var stats indexTrimStats
	var deleteKeys [][]byte
	var transactions []indexedTransaction

	txIterator, err := db.Iterator(txHeightPrefix, prefixEnd(txHeightPrefix))
	if err != nil {
		return stats, fmt.Errorf("iterate CometBFT transaction heights: %w", err)
	}
	var txScanned int64
	for ; txIterator.Valid(); txIterator.Next() {
		txScanned++
		stats.Scanned++
		if progress != nil {
			progress("tx-height", txScanned)
		}

		height, ok := txEventHeight(txIterator.Key(), txIterator.Value())
		if !ok || height <= target {
			continue
		}
		transactions = append(transactions, indexedTransaction{
			height:    height,
			heightKey: bytes.Clone(txIterator.Key()),
			hash:      bytes.Clone(txIterator.Value()),
		})
		stats.TxHeightKeys++
	}
	if err := txIterator.Error(); err != nil {
		_ = txIterator.Close()
		return stats, fmt.Errorf("scan CometBFT transaction heights: %w", err)
	}
	if err := txIterator.Close(); err != nil {
		return stats, fmt.Errorf("close CometBFT transaction-height iterator: %w", err)
	}

	for _, transaction := range transactions {
		value, err := db.Get(transaction.hash)
		if err != nil {
			return stats, fmt.Errorf("read transaction index %X: %w", transaction.hash, err)
		}
		if value == nil {
			return stats, fmt.Errorf("transaction index %X is missing", transaction.hash)
		}
		var result cmtabci.TxResult
		if err := proto.Unmarshal(value, &result); err != nil {
			return stats, fmt.Errorf("decode transaction index %X: %w", transaction.hash, err)
		}
		if result.Height != transaction.height {
			return stats, fmt.Errorf(
				"transaction index %X height mismatch: height key=%d result=%d",
				transaction.hash,
				transaction.height,
				result.Height,
			)
		}

		deleteKeys = append(deleteKeys, transaction.heightKey, transaction.hash)
		stats.TxHashKeys++
		for _, eventPrefix := range txEventPrefixes(&result) {
			eventIterator, err := db.Iterator(eventPrefix, prefixEnd(eventPrefix))
			if err != nil {
				return stats, fmt.Errorf("iterate transaction event prefix %X: %w", eventPrefix, err)
			}
			for ; eventIterator.Valid(); eventIterator.Next() {
				if !bytes.Equal(eventIterator.Value(), transaction.hash) {
					_ = eventIterator.Close()
					return stats, fmt.Errorf(
						"transaction event %X points to unexpected hash %X",
						eventIterator.Key(),
						eventIterator.Value(),
					)
				}
				deleteKeys = append(deleteKeys, bytes.Clone(eventIterator.Key()))
				stats.TxEventKeys++
			}
			if err := eventIterator.Error(); err != nil {
				_ = eventIterator.Close()
				return stats, fmt.Errorf("scan transaction event prefix %X: %w", eventPrefix, err)
			}
			if err := eventIterator.Close(); err != nil {
				return stats, fmt.Errorf("close transaction event iterator: %w", err)
			}
		}
	}

	blockIterator, err := db.Iterator(blockEventPrefix, prefixEnd(blockEventPrefix))
	if err != nil {
		return stats, fmt.Errorf("iterate CometBFT block events: %w", err)
	}
	var blockScanned int64
	for ; blockIterator.Valid(); blockIterator.Next() {
		blockScanned++
		stats.Scanned++
		if progress != nil {
			progress("block-event", blockScanned)
		}
		height, n := binary.Varint(blockIterator.Value())
		if n <= 0 {
			_ = blockIterator.Close()
			return stats, fmt.Errorf(
				"decode block-event index height key=%X value=%X",
				blockIterator.Key(),
				blockIterator.Value(),
			)
		}
		if height > target {
			deleteKeys = append(deleteKeys, bytes.Clone(blockIterator.Key()))
			stats.BlockEventKeys++
		}
	}
	if err := blockIterator.Error(); err != nil {
		_ = blockIterator.Close()
		return stats, fmt.Errorf("scan CometBFT block events: %w", err)
	}
	if err := blockIterator.Close(); err != nil {
		return stats, fmt.Errorf("close CometBFT block-event iterator: %w", err)
	}

	if err := writeDeleteBatches(func() deleteBatch { return db.NewBatch() }, deleteKeys); err != nil {
		return stats, fmt.Errorf("delete CometBFT index suffix: %w", err)
	}
	return stats, nil
}

func trimEVMIndexes(db cosmosdb.DB, target int64) (indexTrimStats, error) {
	var stats indexTrimStats
	var deleteKeys [][]byte
	if target == math.MaxInt64 {
		return stats, nil
	}

	start := make([]byte, 9)
	start[0] = evmTxIndexPrefix
	binary.BigEndian.PutUint64(start[1:], uint64(target)+1)
	iterator, err := db.Iterator(start, []byte{evmTxIndexPrefix + 1})
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

func txEventPrefixes(result *cmtabci.TxResult) [][]byte {
	prefixes := make(map[string][]byte)
	for _, event := range result.Result.Events {
		if len(event.Type) == 0 {
			continue
		}
		for _, attribute := range event.Attributes {
			if len(attribute.Key) == 0 || !attribute.GetIndex() {
				continue
			}
			prefix := []byte(fmt.Sprintf(
				"%s.%s/%s/%d/%d$es$",
				event.Type,
				attribute.Key,
				attribute.Value,
				result.Height,
				result.Index,
			))
			prefixes[string(prefix)] = prefix
		}
	}

	resultPrefixes := make([][]byte, 0, len(prefixes))
	for _, prefix := range prefixes {
		resultPrefixes = append(resultPrefixes, prefix)
	}
	return resultPrefixes
}

func prefixEnd(prefix []byte) []byte {
	end := bytes.Clone(prefix)
	for index := len(end) - 1; index >= 0; index-- {
		if end[index] != math.MaxUint8 {
			end[index]++
			return end[:index+1]
		}
	}
	return nil
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
