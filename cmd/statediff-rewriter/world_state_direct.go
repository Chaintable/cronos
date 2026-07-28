package main

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cosmos/iavl"
	"github.com/evmos/ethermint/debank/statediff"
	"golang.org/x/sync/errgroup"
)

const defaultWorldStateRunHeights = int64(64)

type worldStateSources struct {
	accounts keyRangeStateChangeSource
	balances keyRangeStateChangeSource
	evm      keyRangeStateChangeSource
}

type worldStateShardResult struct {
	directShardTask
	deltas []worldStateDelta
}

func iterateArchiveWorldStateDeltasContext(
	ctx context.Context,
	archive *archiveReader,
	evmDenom string,
	cacheSize, concurrency int,
	runHeights int64,
	first, last int64,
	callback func(worldStateDelta) error,
) error {
	return iterateArchiveWorldStateDeltas(
		ctx, archive, evmDenom, cacheSize, concurrency, runHeights,
		first, last, false, callback,
	)
}

func iterateArchiveWorldStateDeltasWithStorageContext(
	ctx context.Context,
	archive *archiveReader,
	evmDenom string,
	cacheSize, concurrency int,
	runHeights int64,
	first, last int64,
	callback func(worldStateDelta) error,
) error {
	return iterateArchiveWorldStateDeltas(
		ctx, archive, evmDenom, cacheSize, concurrency, runHeights,
		first, last, true, callback,
	)
}

func iterateArchiveWorldStateDeltas(
	ctx context.Context,
	archive *archiveReader,
	evmDenom string,
	cacheSize, concurrency int,
	runHeights int64,
	first, last int64,
	includeStorage bool,
	callback func(worldStateDelta) error,
) error {
	if archive == nil {
		return fmt.Errorf("world-state archive is required")
	}
	legacyLatest := make(map[string]int64, 3)
	for _, storeName := range []string{"acc", "bank", "evm"} {
		version, err := archive.legacyLatestVersion(storeName)
		if err != nil {
			return fmt.Errorf("resolve latest legacy %s IAVL version: %w", storeName, err)
		}
		legacyLatest[storeName] = version
	}
	if err := verifyWorldStateStoreRoots(archive, first, last, legacyLatest); err != nil {
		return err
	}
	return iterateParallelWorldStateDeltasContext(
		ctx,
		func() worldStateSources {
			return worldStateSources{
				accounts: archive.stateChangeSource("acc", cacheSize),
				balances: archive.stateChangeSource("bank", cacheSize),
				evm:      archive.stateChangeSource("evm", cacheSize),
			}
		},
		evmDenom,
		[]int64{legacyLatest["acc"], legacyLatest["bank"], legacyLatest["evm"]},
		concurrency, runHeights, first, last, includeStorage, callback,
	)
}

func verifyWorldStateStoreRoots(
	archive *archiveReader,
	first, last int64,
	legacyLatest map[string]int64,
) error {
	versions := map[int64]struct{}{first: {}, last: {}}
	if first > 1 {
		versions[first-1] = struct{}{}
	}
	for _, version := range legacyLatest {
		if version >= first-1 && version <= last {
			versions[version] = struct{}{}
			if version < last {
				versions[version+1] = struct{}{}
			}
		}
	}
	for _, storeName := range []string{"acc", "bank", "evm"} {
		source := archive.stateChangeSource(storeName, 0)
		for version := range versions {
			got, err := source.VersionHash(version)
			if err != nil {
				return fmt.Errorf("resolve %s IAVL root at version %d: %w", storeName, version, err)
			}
			info, err := archive.commitInfo(version)
			if err != nil {
				return err
			}
			var want []byte
			for _, storeInfo := range info.StoreInfos {
				if storeInfo.Name == storeName {
					want = storeInfo.GetHash()
					break
				}
			}
			if want == nil {
				return fmt.Errorf("commit info %d has no %s store", version, storeName)
			}
			if !bytes.Equal(got, want) {
				return fmt.Errorf(
					"%s IAVL root at version %d is %x, commit info has %x",
					storeName, version, got, want,
				)
			}
		}
	}
	return nil
}

func iterateParallelWorldStateDeltasContext(
	ctx context.Context,
	newSources func() worldStateSources,
	evmDenom string,
	legacyBoundaries []int64,
	concurrency int,
	runHeights int64,
	first, last int64,
	includeStorage bool,
	callback func(worldStateDelta) error,
) error {
	if ctx == nil || newSources == nil || callback == nil {
		return fmt.Errorf("world-state traversal context, source factory, and callback are required")
	}
	if concurrency < 1 || concurrency > maximumDirectIAVLConcurrency {
		return fmt.Errorf("iavl-concurrency must be between 1 and %d", maximumDirectIAVLConcurrency)
	}
	if runHeights < directIAVLShardSize || runHeights%directIAVLShardSize != 0 {
		return fmt.Errorf("iavl-run-heights must be a positive multiple of %d", directIAVLShardSize)
	}
	if first < 2 || last < first {
		return fmt.Errorf("invalid world-state traversal range %d-%d", first, last)
	}
	for _, boundary := range legacyBoundaries {
		if boundary < 0 {
			return fmt.Errorf("invalid latest legacy version %d", boundary)
		}
	}
	decoder, err := newWorldStateDecoder(evmDenom)
	if err != nil {
		return err
	}

	pipelineCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	group, groupCtx := errgroup.WithContext(pipelineCtx)
	jobs := make(chan directShardTask, concurrency)
	results := make(chan worldStateShardResult, concurrency)
	tokens := make(chan struct{}, concurrency)

	group.Go(func() error {
		defer close(jobs)
		ordinal := uint64(1)
		for shardFirst := first; ; ordinal++ {
			shardLast := shardFirst + runHeights - 1
			if shardLast < shardFirst || shardLast > last {
				shardLast = last
			}
			for _, boundary := range legacyBoundaries {
				if boundary >= shardFirst && boundary < shardLast {
					shardLast = boundary
				}
			}
			task := directShardTask{ordinal: ordinal, first: shardFirst, last: shardLast}
			select {
			case tokens <- struct{}{}:
			case <-groupCtx.Done():
				return groupCtx.Err()
			}
			select {
			case jobs <- task:
			case <-groupCtx.Done():
				<-tokens
				return groupCtx.Err()
			}
			if shardLast == last {
				return nil
			}
			shardFirst = shardLast + 1
		}
	})

	for range concurrency {
		group.Go(func() error {
			sources := newSources()
			if sources.accounts == nil || sources.balances == nil || sources.evm == nil {
				return fmt.Errorf("world-state source factory returned nil")
			}
			for task := range jobs {
				result := worldStateShardResult{
					directShardTask: task,
					deltas:          make([]worldStateDelta, task.last-task.first+1),
				}
				for index := range result.deltas {
					result.deltas[index].height = task.first + int64(index)
				}
				if err := fillWorldStateShard(groupCtx, sources.accounts, task, result.deltas, []byte{0x01}, []byte{0x02},
					func(delta *worldStateDelta, changeSet *iavl.ChangeSet) error {
						decoded, err := decoder.decodeAccounts(changeSet)
						if err != nil {
							return err
						}
						delta.accounts = decoded
						return nil
					}); err != nil {
					return fmt.Errorf("account %w", err)
				}
				if err := fillWorldStateShard(groupCtx, sources.balances, task, result.deltas, []byte{0x02}, []byte{0x03},
					func(delta *worldStateDelta, changeSet *iavl.ChangeSet) error {
						decoded, err := decoder.decodeBalances(changeSet)
						if err != nil {
							return err
						}
						delta.balances = decoded
						return nil
					}); err != nil {
					return fmt.Errorf("balance %w", err)
				}
				evmEnd := []byte{0x02}
				if includeStorage {
					evmEnd = []byte{0x03}
				}
				if err := fillWorldStateShard(groupCtx, sources.evm, task, result.deltas, []byte{0x01}, evmEnd,
					func(delta *worldStateDelta, changeSet *iavl.ChangeSet) error {
						codeChanges := &iavl.ChangeSet{Pairs: make([]*iavl.KVPair, 0, len(changeSet.Pairs))}
						for _, pair := range changeSet.Pairs {
							if pair != nil && len(pair.Key) != 0 && pair.Key[0] == evmCodePrefix {
								codeChanges.Pairs = append(codeChanges.Pairs, pair)
							}
						}
						decoded, err := decodeCodes(codeChanges)
						if err != nil {
							return err
						}
						delta.codes = decoded
						if includeStorage {
							delta.storage, err = statediff.CanonicalStorageDiff(changeSet)
							if err != nil {
								return err
							}
						}
						return nil
					}); err != nil {
					return fmt.Errorf("EVM %w", err)
				}
				select {
				case results <- result:
				case <-groupCtx.Done():
					return groupCtx.Err()
				}
			}
			return nil
		})
	}

	groupDone := make(chan error, 1)
	go func() {
		groupDone <- group.Wait()
		close(results)
	}()

	expectedOrdinal := uint64(1)
	expectedHeight := first
	pending := make(map[uint64]worldStateShardResult, concurrency)
	var callbackErr error
	for result := range results {
		if callbackErr != nil {
			continue
		}
		if result.ordinal < expectedOrdinal {
			callbackErr = fmt.Errorf("world-state shard result %d precedes expected %d", result.ordinal, expectedOrdinal)
			cancel()
			continue
		}
		if _, found := pending[result.ordinal]; found {
			callbackErr = fmt.Errorf("duplicate world-state shard result %d", result.ordinal)
			cancel()
			continue
		}
		pending[result.ordinal] = result
		for {
			next, found := pending[expectedOrdinal]
			if !found {
				break
			}
			if next.first != expectedHeight || int64(len(next.deltas)) != next.last-next.first+1 {
				callbackErr = fmt.Errorf("world-state shard %d does not continuously cover height %d", next.ordinal, expectedHeight)
				cancel()
				break
			}
			for _, delta := range next.deltas {
				if delta.height != expectedHeight {
					callbackErr = fmt.Errorf("world-state shard %d emitted height %d, want %d", next.ordinal, delta.height, expectedHeight)
					cancel()
					break
				}
				if err := callback(delta); err != nil {
					callbackErr = err
					cancel()
					break
				}
				expectedHeight++
			}
			if callbackErr != nil {
				break
			}
			delete(pending, expectedOrdinal)
			<-tokens
			expectedOrdinal++
		}
	}
	groupErr := <-groupDone
	if callbackErr != nil {
		return callbackErr
	}
	if groupErr != nil {
		return groupErr
	}
	if expectedHeight != last+1 || len(pending) != 0 || len(tokens) != 0 {
		return fmt.Errorf("world-state traversal ended at height %d, want %d", expectedHeight-1, last)
	}
	return nil
}

func fillWorldStateShard(
	ctx context.Context,
	source keyRangeStateChangeSource,
	task directShardTask,
	deltas []worldStateDelta,
	start, end []byte,
	decode func(*worldStateDelta, *iavl.ChangeSet) error,
) error {
	return iterateDirectStateChangesWithContext(
		ctx,
		source,
		func(first, last int64, callback func(int64, *iavl.ChangeSet) error) error {
			return source.TraverseStateChangesInKeyRange(first, last, start, end, callback)
		},
		task.first,
		task.last,
		func(height int64, changeSet *iavl.ChangeSet) error {
			index := height - task.first
			if index < 0 || index >= int64(len(deltas)) {
				return fmt.Errorf("height %d is outside shard %d-%d", height, task.first, task.last)
			}
			return decode(&deltas[index], changeSet)
		},
	)
}
