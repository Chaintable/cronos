package main

import (
	"context"
	"fmt"

	"github.com/cosmos/iavl"
	"github.com/evmos/ethermint/debank/statediff"
	dtypes "github.com/evmos/ethermint/debank/types"
	"golang.org/x/sync/errgroup"
)

const (
	defaultDirectIAVLConcurrency = 8
	maximumDirectIAVLConcurrency = 64
	directIAVLShardSize          = int64(64)
	directResultBaseBytes        = int64(1 << 20)
	directResultItemBytes        = int64(128)
)

type directStorageChange struct {
	height    int64
	canonical []dtypes.AccountStorageDiff
}

type directShardTask struct {
	ordinal uint64
	first   int64
	last    int64
}

type directShardResult struct {
	directShardTask
	changes []directStorageChange
}

func iterateArchiveDirectStorageDiffsContext(
	ctx context.Context,
	archive *archiveReader,
	cacheSize, concurrency int,
	first, last int64,
	callback func(int64, []dtypes.AccountStorageDiff) error,
) error {
	if archive == nil {
		return fmt.Errorf("direct archive is required")
	}
	legacyLatest, err := archive.evmLegacyLatestVersion()
	if err != nil {
		return fmt.Errorf("resolve latest legacy EVM IAVL version: %w", err)
	}
	return iterateParallelDirectStorageDiffsContext(
		ctx,
		func() stateChangeSource { return archive.evmStateChangeSource(cacheSize) },
		legacyLatest, concurrency, first, last, callback,
	)
}

func iterateParallelDirectStorageDiffsContext(
	ctx context.Context,
	newSource func() stateChangeSource,
	legacyLatest int64,
	concurrency int,
	first, last int64,
	callback func(int64, []dtypes.AccountStorageDiff) error,
) error {
	if ctx == nil || newSource == nil || callback == nil {
		return fmt.Errorf("parallel direct traversal context, source factory, and callback are required")
	}
	if concurrency < 1 || concurrency > maximumDirectIAVLConcurrency {
		return fmt.Errorf("iavl-concurrency must be between 1 and %d", maximumDirectIAVLConcurrency)
	}
	if legacyLatest < 0 {
		return fmt.Errorf("invalid latest legacy version %d", legacyLatest)
	}
	if first < 2 || last < first {
		return fmt.Errorf("invalid parallel direct traversal range %d-%d", first, last)
	}

	pipelineCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	group, groupCtx := errgroup.WithContext(pipelineCtx)
	jobs := make(chan directShardTask, concurrency)
	results := make(chan directShardResult, concurrency)
	tokens := make(chan struct{}, concurrency)

	group.Go(func() error {
		defer close(jobs)
		ordinal := uint64(1)
		for shardFirst := first; ; ordinal++ {
			shardLast := shardFirst + directIAVLShardSize - 1
			if shardLast < shardFirst || shardLast > last {
				shardLast = last
			}
			if legacyLatest >= shardFirst && legacyLatest < shardLast {
				shardLast = legacyLatest
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
			source := newSource()
			if source == nil {
				return fmt.Errorf("direct state change source factory returned nil")
			}
			for task := range jobs {
				retainedBytes := int64(0)
				result := directShardResult{
					directShardTask: task,
					changes:         make([]directStorageChange, 0, task.last-task.first+1),
				}
				err := iterateDirectStateChangesContext(groupCtx, source, task.first, task.last, func(height int64, changeSet *iavl.ChangeSet) error {
					canonical, err := statediff.CanonicalStorageDiff(changeSet)
					if err != nil {
						return fmt.Errorf("height %d canonical storage: %w", height, err)
					}
					retainedBytes, err = addDirectShardRetainedBytes(retainedBytes, canonical)
					if err != nil {
						return fmt.Errorf("height %d canonical storage: %w", height, err)
					}
					result.changes = append(result.changes, directStorageChange{height: height, canonical: canonical})
					return nil
				})
				if err != nil {
					return fmt.Errorf("direct shard %d versions %d-%d: %w", task.ordinal, task.first, task.last, err)
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
	pending := make(map[uint64]directShardResult, concurrency)
	var callbackErr error
	for result := range results {
		if callbackErr != nil {
			continue
		}
		if result.ordinal < expectedOrdinal {
			callbackErr = fmt.Errorf("direct shard result %d precedes expected %d", result.ordinal, expectedOrdinal)
			cancel()
			continue
		}
		if _, found := pending[result.ordinal]; found {
			callbackErr = fmt.Errorf("duplicate direct shard result %d", result.ordinal)
			cancel()
			continue
		}
		pending[result.ordinal] = result
		for {
			next, found := pending[expectedOrdinal]
			if !found {
				break
			}
			if next.first != expectedHeight || int64(len(next.changes)) != next.last-next.first+1 {
				callbackErr = fmt.Errorf("direct shard %d does not continuously cover height %d", next.ordinal, expectedHeight)
				cancel()
				break
			}
			for _, change := range next.changes {
				if change.height != expectedHeight {
					callbackErr = fmt.Errorf("direct shard %d emitted height %d, want %d", next.ordinal, change.height, expectedHeight)
					cancel()
					break
				}
				if err := callback(change.height, change.canonical); err != nil {
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
		return fmt.Errorf("parallel direct traversal ended at height %d, want %d", expectedHeight-1, last)
	}
	return nil
}

func addDirectShardRetainedBytes(current int64, storage []dtypes.AccountStorageDiff) (int64, error) {
	if current < 0 || current > maxObjectSize {
		return 0, fmt.Errorf("invalid retained canonical shard bytes %d", current)
	}
	estimated, err := estimatedCanonicalStorageBytes(storage)
	if err != nil {
		return 0, err
	}
	// The 1 MiB base belongs to the outer object task. A buffered shard retains
	// only this height entry and the canonical account/slot values.
	retained := directResultItemBytes
	if estimated != 0 {
		retained += estimated - directResultBaseBytes
	}
	if retained > maxObjectSize-current {
		return 0, fmt.Errorf("retained canonical shard exceeds %d bytes", maxObjectSize)
	}
	return current + retained, nil
}

func estimatedCanonicalStorageBytes(storage []dtypes.AccountStorageDiff) (int64, error) {
	if len(storage) == 0 {
		return 0, nil
	}
	remaining := (maxObjectSize - directResultBaseBytes) / directResultItemBytes
	items := int64(0)
	for _, account := range storage {
		if remaining < 1 {
			return 0, fmt.Errorf("retained canonical storage exceeds %d bytes", maxObjectSize)
		}
		remaining--
		items++
		if int64(len(account.Values)) > remaining {
			return 0, fmt.Errorf("retained canonical storage exceeds %d bytes", maxObjectSize)
		}
		remaining -= int64(len(account.Values))
		items += int64(len(account.Values))
	}
	return directResultBaseBytes + items*directResultItemBytes, nil
}

func canonicalObjectOperationBytes(storage []dtypes.AccountStorageDiff, equalRoot bool) (int64, error) {
	if equalRoot {
		return 1 << 20, nil
	}
	retained, err := estimatedCanonicalStorageBytes(storage)
	if err != nil {
		return 0, err
	}
	return maxObjectOperationBytes + retained, nil
}
