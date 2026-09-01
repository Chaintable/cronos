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
	cmtstateproto "github.com/cometbft/cometbft/proto/tendermint/state"
	cmtstate "github.com/cometbft/cometbft/state"
	cmttypes "github.com/cometbft/cometbft/types"
	cosmosdb "github.com/cosmos/cosmos-db"
	"github.com/cosmos/cosmos-sdk/server"
	ethermintserver "github.com/evmos/ethermint/server"
	"github.com/google/orderedcode"
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
	Scanned          int64
	TxHeightKeys     int64
	TxEventKeys      int64
	TxHashKeys       int64
	TxHashesRestored int64
	BlockEventKeys   int64
	EVMTxIndexKeys   int64
	EVMTxHashKeys    int64
}

type stateTrimStats struct {
	ABCIResponses   int64
	Validators      int64
	ConsensusParams int64
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
			stateDB, err := cmtcfg.DefaultDBProvider(&cmtcfg.DBContext{ID: "state", Config: cfg})
			if err != nil {
				_ = txIndexDB.Close()
				return fmt.Errorf("open CometBFT state database: %w", err)
			}
			stateStore := cmtstate.NewStore(stateDB, cmtstate.StoreOptions{
				DiscardABCIResponses: cfg.Storage.DiscardABCIResponses,
			})
			currentState, err := stateStore.Load()
			if err != nil {
				_ = txIndexDB.Close()
				_ = stateDB.Close()
				return fmt.Errorf("load CometBFT state: %w", err)
			}
			if currentState.LastBlockHeight != target {
				_ = txIndexDB.Close()
				_ = stateDB.Close()
				return fmt.Errorf(
					"CometBFT state height %d does not match target %d",
					currentState.LastBlockHeight,
					target,
				)
			}
			sourceHeight, err := lastFinalizeBlockHeight(stateDB)
			if err != nil {
				_ = txIndexDB.Close()
				_ = stateDB.Close()
				return err
			}
			targetResponse, err := stateStore.LoadFinalizeBlockResponse(target)
			if err != nil {
				_ = txIndexDB.Close()
				_ = stateDB.Close()
				return fmt.Errorf("load target FinalizeBlockResponse at height %d: %w", target, err)
			}

			stats, err := trimCometIndexes(txIndexDB, target, sourceHeight, stateStore.LoadFinalizeBlockResponse, func(stage string, scanned int64) {
				if stage == "block-height" && scanned%rewindProgressInterval == 0 ||
					stage != "block-height" && scanned%indexScanInterval == 0 {
					cmd.Printf("CometBFT %s scan progress keys=%d\n", stage, scanned)
				}
			})
			txCloseErr := txIndexDB.Close()
			if err != nil {
				_ = stateDB.Close()
				return err
			}
			if txCloseErr != nil {
				_ = stateDB.Close()
				return fmt.Errorf("close CometBFT index database: %w", txCloseErr)
			}

			evmIndexDB, err := ethermintserver.OpenIndexerDB(cfg.RootDir, server.GetAppDBBackend(ctx.Viper))
			if err != nil {
				_ = stateDB.Close()
				return fmt.Errorf("open EVM index database: %w", err)
			}
			evmStats, err := trimEVMIndexes(evmIndexDB, target)
			closeErr := evmIndexDB.Close()
			if err != nil {
				_ = stateDB.Close()
				return err
			}
			if closeErr != nil {
				_ = stateDB.Close()
				return fmt.Errorf("close EVM index database: %w", closeErr)
			}

			stateStats, err := trimCometStateHistory(stateDB, target, sourceHeight, targetResponse)
			stateCloseErr := stateDB.Close()
			if err != nil {
				return err
			}
			if stateCloseErr != nil {
				return fmt.Errorf("close CometBFT state database: %w", stateCloseErr)
			}

			stats.EVMTxIndexKeys = evmStats.EVMTxIndexKeys
			stats.EVMTxHashKeys = evmStats.EVMTxHashKeys
			cmd.Printf(
				"rewound indexes and state history to height %d scanned=%d tx_heights=%d tx_events=%d tx_hashes=%d tx_hashes_restored=%d block_events=%d evm_tx_indexes=%d evm_tx_hashes=%d state_abci_responses=%d state_validators=%d state_consensus_params=%d\n",
				target,
				stats.Scanned,
				stats.TxHeightKeys,
				stats.TxEventKeys,
				stats.TxHashKeys,
				stats.TxHashesRestored,
				stats.BlockEventKeys,
				stats.EVMTxIndexKeys,
				stats.EVMTxHashKeys,
				stateStats.ABCIResponses,
				stateStats.Validators,
				stateStats.ConsensusParams,
			)
			return nil
		},
	}
}

func trimCometStateHistory(
	db cmtdb.DB,
	target int64,
	sourceHeight int64,
	targetResponse *cmtabci.ResponseFinalizeBlock,
) (stateTrimStats, error) {
	var stats stateTrimStats
	if sourceHeight < target {
		return stats, fmt.Errorf("source height %d is below target %d", sourceHeight, target)
	}
	if targetResponse == nil {
		return stats, fmt.Errorf("target FinalizeBlockResponse is nil")
	}

	var deleteKeys [][]byte
	appendIfPresent := func(key []byte, count *int64) error {
		present, err := db.Has(key)
		if err != nil {
			return err
		}
		if present {
			deleteKeys = append(deleteKeys, key)
			*count = *count + 1
		}
		return nil
	}

	for height := target + 1; height <= sourceHeight; height++ {
		if err := appendIfPresent(cometStateHeightKey("abciResponsesKey", height), &stats.ABCIResponses); err != nil {
			return stats, fmt.Errorf("inspect FinalizeBlockResponse at height %d: %w", height, err)
		}
	}
	// Saving CometBFT state at height H writes validator metadata for H+2 and
	// consensus parameters for H+1. The final hard rollback recreated those
	// boundary entries, so only keys beyond them belong to the discarded suffix.
	for height := target + 3; height <= sourceHeight+2; height++ {
		if err := appendIfPresent(cometStateHeightKey("validatorsKey", height), &stats.Validators); err != nil {
			return stats, fmt.Errorf("inspect validators at height %d: %w", height, err)
		}
	}
	for height := target + 2; height <= sourceHeight+1; height++ {
		if err := appendIfPresent(cometStateHeightKey("consensusParamsKey", height), &stats.ConsensusParams); err != nil {
			return stats, fmt.Errorf("inspect consensus params at height %d: %w", height, err)
		}
	}

	if err := writeDeleteBatches(func() indexBatch { return db.NewBatch() }, deleteKeys); err != nil {
		return stats, fmt.Errorf("delete CometBFT state history suffix: %w", err)
	}
	for _, key := range deleteKeys {
		present, err := db.Has(key)
		if err != nil {
			return stats, fmt.Errorf("verify deleted CometBFT state key %q: %w", key, err)
		}
		if present {
			return stats, fmt.Errorf("CometBFT state key %q still exists after deletion", key)
		}
	}

	responseInfo := &cmtstateproto.ABCIResponsesInfo{
		ResponseFinalizeBlock: targetResponse,
		Height:                target,
	}
	value, err := responseInfo.Marshal()
	if err != nil {
		return stats, fmt.Errorf("encode last FinalizeBlockResponse at height %d: %w", target, err)
	}
	if err := db.SetSync([]byte("lastABCIResponseKey"), value); err != nil {
		return stats, fmt.Errorf("set last FinalizeBlockResponse at height %d: %w", target, err)
	}
	lastHeight, err := lastFinalizeBlockHeight(db)
	if err != nil {
		return stats, err
	}
	if lastHeight != target {
		return stats, fmt.Errorf("last FinalizeBlockResponse height %d does not match target %d", lastHeight, target)
	}
	return stats, nil
}

func cometStateHeightKey(prefix string, height int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", prefix, height))
}

type indexedTransaction struct {
	height    int64
	index     uint32
	heightKey []byte
	hash      []byte
}

type indexValue struct {
	key   []byte
	value []byte
}

func trimCometIndexes(
	db cmtdb.DB,
	target int64,
	sourceHeight int64,
	loadFinalizeBlock func(int64) (*cmtabci.ResponseFinalizeBlock, error),
	progress func(stage string, scanned int64),
) (indexTrimStats, error) {
	var stats indexTrimStats
	if sourceHeight < target {
		return stats, fmt.Errorf("source height %d is below target %d", sourceHeight, target)
	}
	var deleteDataKeys [][]byte
	var deleteHeightKeys [][]byte
	var restoreValues []indexValue
	var transactions []indexedTransaction
	highHashes := make(map[string]struct{})

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

		height, index, err := txHeightOccurrence(txIterator.Key(), txIterator.Value())
		if err != nil {
			_ = txIterator.Close()
			return stats, err
		}
		if height <= target {
			continue
		}
		transactions = append(transactions, indexedTransaction{
			height:    height,
			index:     index,
			heightKey: bytes.Clone(txIterator.Key()),
			hash:      bytes.Clone(txIterator.Value()),
		})
		highHashes[string(txIterator.Value())] = struct{}{}
		stats.TxHeightKeys++
	}
	if err := txIterator.Error(); err != nil {
		_ = txIterator.Close()
		return stats, fmt.Errorf("scan CometBFT transaction heights: %w", err)
	}
	if err := txIterator.Close(); err != nil {
		return stats, fmt.Errorf("close CometBFT transaction-height iterator: %w", err)
	}

	retainedTransactions := make(map[string]indexedTransaction)
	if len(highHashes) > 0 {
		retainedIterator, err := db.Iterator(txHeightPrefix, prefixEnd(txHeightPrefix))
		if err != nil {
			return stats, fmt.Errorf("iterate retained CometBFT transaction heights: %w", err)
		}
		var retainedScanned int64
		for ; retainedIterator.Valid(); retainedIterator.Next() {
			retainedScanned++
			stats.Scanned++
			if progress != nil {
				progress("tx-retained", retainedScanned)
			}

			height, index, err := txHeightOccurrence(retainedIterator.Key(), retainedIterator.Value())
			if err != nil {
				_ = retainedIterator.Close()
				return stats, err
			}
			hashKey := string(retainedIterator.Value())
			if height > target {
				continue
			}
			if _, found := highHashes[hashKey]; !found {
				continue
			}

			retained, found := retainedTransactions[hashKey]
			if !found || height > retained.height || height == retained.height && index > retained.index {
				retainedTransactions[hashKey] = indexedTransaction{
					height: height,
					index:  index,
					hash:   bytes.Clone(retainedIterator.Value()),
				}
			}
		}
		if err := retainedIterator.Error(); err != nil {
			_ = retainedIterator.Close()
			return stats, fmt.Errorf("scan retained CometBFT transaction heights: %w", err)
		}
		if err := retainedIterator.Close(); err != nil {
			return stats, fmt.Errorf("close retained CometBFT transaction-height iterator: %w", err)
		}
	}

	if sourceHeight > target && loadFinalizeBlock == nil {
		return stats, fmt.Errorf("load FinalizeBlockResponse callback is required")
	}
	var cachedHeight int64 = -1
	var cachedResponse *cmtabci.ResponseFinalizeBlock
	loadTransactionResult := func(transaction indexedTransaction, tx []byte) (*cmtabci.TxResult, error) {
		if cachedHeight != transaction.height {
			response, err := loadFinalizeBlock(transaction.height)
			if err != nil {
				return nil, fmt.Errorf("load FinalizeBlockResponse at height %d: %w", transaction.height, err)
			}
			cachedHeight = transaction.height
			cachedResponse = response
		}
		resultCount := 0
		if cachedResponse != nil {
			resultCount = len(cachedResponse.TxResults)
		}
		if uint64(transaction.index) >= uint64(resultCount) {
			return nil, fmt.Errorf(
				"transaction index %d is outside FinalizeBlockResponse at height %d (results=%d)",
				transaction.index,
				transaction.height,
				resultCount,
			)
		}
		execResult := cachedResponse.TxResults[transaction.index]
		if execResult == nil {
			return nil, fmt.Errorf("transaction result %d is nil at height %d", transaction.index, transaction.height)
		}
		return &cmtabci.TxResult{
			Height: transaction.height,
			Index:  transaction.index,
			Tx:     bytes.Clone(tx),
			Result: *execResult,
		}, nil
	}

	processedHashes := make(map[string]struct{})
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
		if !bytes.Equal(cmttypes.Tx(result.Tx).Hash(), transaction.hash) {
			return stats, fmt.Errorf(
				"transaction index %X contains transaction with hash %X",
				transaction.hash,
				cmttypes.Tx(result.Tx).Hash(),
			)
		}
		indexedResult, err := loadTransactionResult(transaction, result.Tx)
		if err != nil {
			return stats, err
		}

		deleteHeightKeys = append(deleteHeightKeys, transaction.heightKey)
		for _, eventPrefix := range txEventPrefixes(indexedResult) {
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
				deleteDataKeys = append(deleteDataKeys, bytes.Clone(eventIterator.Key()))
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

		hashKey := string(transaction.hash)
		if _, processed := processedHashes[hashKey]; processed {
			continue
		}
		processedHashes[hashKey] = struct{}{}
		if retained, found := retainedTransactions[hashKey]; found {
			retainedResult, err := loadTransactionResult(retained, result.Tx)
			if err != nil {
				return stats, err
			}
			encoded, err := proto.Marshal(retainedResult)
			if err != nil {
				return stats, fmt.Errorf("encode retained transaction index %X: %w", transaction.hash, err)
			}
			restoreValues = append(restoreValues, indexValue{
				key:   bytes.Clone(transaction.hash),
				value: encoded,
			})
			stats.TxHashesRestored++
		} else {
			deleteDataKeys = append(deleteDataKeys, bytes.Clone(transaction.hash))
			stats.TxHashKeys++
		}
	}

	for height := target + 1; height <= sourceHeight; height++ {
		response, err := loadFinalizeBlock(height)
		if err != nil {
			return stats, fmt.Errorf("load FinalizeBlockResponse at height %d for block indexes: %w", height, err)
		}
		keys, err := indexedBlockEventKeys(db, height, response.GetEvents())
		if err != nil {
			return stats, err
		}
		deleteDataKeys = append(deleteDataKeys, keys...)
		stats.BlockEventKeys += int64(len(keys))
		stats.Scanned += int64(len(keys))
		if progress != nil {
			progress("block-height", height-target)
		}
	}

	if err := writeSetBatches(func() indexBatch { return db.NewBatch() }, restoreValues); err != nil {
		return stats, fmt.Errorf("restore retained CometBFT transaction indexes: %w", err)
	}
	if err := writeDeleteBatches(func() indexBatch { return db.NewBatch() }, deleteDataKeys); err != nil {
		return stats, fmt.Errorf("delete CometBFT transaction and block-event indexes: %w", err)
	}
	if err := writeDeleteBatches(func() indexBatch { return db.NewBatch() }, deleteHeightKeys); err != nil {
		return stats, fmt.Errorf("delete CometBFT transaction-height indexes: %w", err)
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

	if err := writeDeleteBatches(func() indexBatch { return db.NewBatch() }, deleteKeys); err != nil {
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

func indexedBlockEventKeys(db cmtdb.DB, height int64, events []cmtabci.Event) ([][]byte, error) {
	var keys [][]byte
	heightKey, err := orderedcode.Append(nil, cmttypes.BlockHeightKey, height)
	if err != nil {
		return nil, fmt.Errorf("encode block-height index at height %d: %w", height, err)
	}
	heightKey = append(bytes.Clone(blockEventPrefix), heightKey...)
	value, err := db.Get(heightKey)
	if err != nil {
		return nil, fmt.Errorf("read block-height index at height %d: %w", height, err)
	}
	if value != nil {
		if err := validateBlockEventHeight(heightKey, value, height); err != nil {
			return nil, err
		}
		keys = append(keys, heightKey)
	}

	prefixes, err := blockEventPrefixes(events, height)
	if err != nil {
		return nil, err
	}
	for _, prefix := range prefixes {
		prefix = append(bytes.Clone(blockEventPrefix), prefix...)
		iterator, err := db.Iterator(prefix, prefixEnd(prefix))
		if err != nil {
			return nil, fmt.Errorf("iterate block-event prefix %X: %w", prefix, err)
		}
		for ; iterator.Valid(); iterator.Next() {
			if err := validateBlockEventHeight(iterator.Key(), iterator.Value(), height); err != nil {
				_ = iterator.Close()
				return nil, err
			}
			keys = append(keys, bytes.Clone(iterator.Key()))
		}
		if err := iterator.Error(); err != nil {
			_ = iterator.Close()
			return nil, fmt.Errorf("scan block-event prefix %X: %w", prefix, err)
		}
		if err := iterator.Close(); err != nil {
			return nil, fmt.Errorf("close block-event iterator: %w", err)
		}
	}
	return keys, nil
}

func blockEventPrefixes(events []cmtabci.Event, height int64) ([][]byte, error) {
	prefixes := make(map[string][]byte)
	for _, event := range events {
		if len(event.Type) == 0 {
			continue
		}
		for _, attribute := range event.Attributes {
			if len(attribute.Key) == 0 || !attribute.GetIndex() {
				continue
			}
			compositeKey := fmt.Sprintf("%s.%s", event.Type, attribute.Key)
			if compositeKey == cmttypes.BlockHeightKey {
				return nil, fmt.Errorf("block event uses reserved index key %q at height %d", compositeKey, height)
			}
			prefix, err := orderedcode.Append(nil, compositeKey, attribute.Value, height)
			if err != nil {
				return nil, fmt.Errorf("encode block-event index at height %d: %w", height, err)
			}
			prefixes[string(prefix)] = prefix
		}
	}

	result := make([][]byte, 0, len(prefixes))
	for _, prefix := range prefixes {
		result = append(result, prefix)
	}
	return result, nil
}

func validateBlockEventHeight(key, value []byte, expected int64) error {
	height, n := binary.Varint(value)
	if n <= 0 || height != expected {
		return fmt.Errorf(
			"block-event index %X has height %d (value=%X), expected %d",
			key,
			height,
			value,
			expected,
		)
	}
	return nil
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

func lastFinalizeBlockHeight(db cmtdb.DB) (int64, error) {
	value, err := db.Get([]byte("lastABCIResponseKey"))
	if err != nil {
		return 0, fmt.Errorf("read last FinalizeBlockResponse: %w", err)
	}
	if value == nil {
		return 0, fmt.Errorf("last FinalizeBlockResponse is missing")
	}
	var info cmtstateproto.ABCIResponsesInfo
	if err := info.Unmarshal(value); err != nil {
		return 0, fmt.Errorf("decode last FinalizeBlockResponse: %w", err)
	}
	if info.Height <= 0 {
		return 0, fmt.Errorf("last FinalizeBlockResponse has invalid height %d", info.Height)
	}
	return info.Height, nil
}

func txHeightOccurrence(key, value []byte) (int64, uint32, error) {
	if len(value) != 32 {
		return 0, 0, fmt.Errorf("transaction-height index %X has invalid hash length %d", key, len(value))
	}
	if !bytes.HasPrefix(key, txHeightPrefix) {
		return 0, 0, fmt.Errorf("transaction-height index has invalid prefix: %X", key)
	}
	parts := bytes.Split(key[len(txHeightPrefix):], []byte{'/'})
	if len(parts) != 3 {
		return 0, 0, fmt.Errorf("transaction-height index has invalid key: %X", key)
	}
	if !validEventIndexSuffix(parts[2]) {
		return 0, 0, fmt.Errorf("transaction-height index has invalid event suffix: %X", key)
	}

	valueHeight, err := strconv.ParseInt(string(parts[0]), 10, 64)
	if err != nil || valueHeight <= 0 {
		return 0, 0, fmt.Errorf("transaction-height index has invalid value height: %X", key)
	}
	eventHeight, err := strconv.ParseInt(string(parts[1]), 10, 64)
	if err != nil || eventHeight <= 0 {
		return 0, 0, fmt.Errorf("transaction-height index has invalid event height: %X", key)
	}
	if valueHeight != eventHeight {
		return 0, 0, fmt.Errorf(
			"transaction-height index has inconsistent heights value=%d event=%d: %X",
			valueHeight,
			eventHeight,
			key,
		)
	}
	suffixParts := bytes.Split(parts[2], []byte("$es$"))
	index, err := strconv.ParseUint(string(suffixParts[0]), 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("transaction-height index has invalid transaction index: %X", key)
	}
	return eventHeight, uint32(index), nil
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

type indexBatch interface {
	Set(key, value []byte) error
	Delete(key []byte) error
	WriteSync() error
	Close() error
}

func writeSetBatches(newBatch func() indexBatch, values []indexValue) error {
	for start := 0; start < len(values); start += deleteBatchSize {
		end := start + deleteBatchSize
		if end > len(values) {
			end = len(values)
		}

		batch := newBatch()
		for _, value := range values[start:end] {
			if err := batch.Set(value.key, value.value); err != nil {
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

func writeDeleteBatches(newBatch func() indexBatch, keys [][]byte) error {
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
