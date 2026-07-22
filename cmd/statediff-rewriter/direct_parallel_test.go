package main

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/cosmos/iavl"
	dtypes "github.com/evmos/ethermint/debank/types"
	"github.com/stretchr/testify/require"
)

type parallelDirectFake struct {
	mu sync.Mutex

	ranges      [][2]int64
	completed   [][2]int64
	active      int
	maxActive   int
	workerError error
	errorFirst  int64

	holdFirst   int64
	release     chan struct{}
	releaseOnce sync.Once

	barrierTarget int
	barrier       chan struct{}
	barrierOnce   sync.Once
}

type parallelDirectFakeSource struct {
	fake *parallelDirectFake
}

func (source *parallelDirectFakeSource) VersionHash(version int64) ([]byte, error) {
	return bytes.Repeat([]byte{byte(version)}, legacyHashLength), nil
}

func (source *parallelDirectFakeSource) TraverseStateChanges(
	first, last int64,
	callback func(int64, *iavl.ChangeSet) error,
) error {
	fake := source.fake
	fake.mu.Lock()
	fake.ranges = append(fake.ranges, [2]int64{first, last})
	fake.active++
	if fake.active > fake.maxActive {
		fake.maxActive = fake.active
	}
	if fake.barrierTarget > 0 && fake.active == fake.barrierTarget {
		fake.barrierOnce.Do(func() { close(fake.barrier) })
	}
	fake.mu.Unlock()
	defer func() {
		fake.mu.Lock()
		fake.active--
		fake.mu.Unlock()
	}()

	if fake.barrierTarget > 0 {
		select {
		case <-fake.barrier:
		case <-time.After(2 * time.Second):
			return errors.New("parallel direct fake barrier timed out")
		}
	}
	if first == fake.errorFirst && fake.workerError != nil {
		return fake.workerError
	}
	if first == fake.holdFirst {
		select {
		case <-fake.release:
		case <-time.After(2 * time.Second):
			return errors.New("parallel direct fake release timed out")
		}
	}
	for version := first; version <= last; version++ {
		if err := callback(version, &iavl.ChangeSet{}); err != nil {
			return err
		}
	}
	fake.mu.Lock()
	fake.completed = append(fake.completed, [2]int64{first, last})
	fake.mu.Unlock()
	if first != fake.holdFirst && fake.release != nil {
		fake.releaseOnce.Do(func() { close(fake.release) })
	}
	return nil
}

func (fake *parallelDirectFake) source() stateChangeSource {
	return &parallelDirectFakeSource{fake: fake}
}

func (fake *parallelDirectFake) snapshot() (ranges, completed [][2]int64, maxActive int) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return append([][2]int64(nil), fake.ranges...), append([][2]int64(nil), fake.completed...), fake.maxActive
}

func TestIterateParallelDirectStorageDiffsContextOrdersShardsAndSplitsLegacyBoundary(t *testing.T) {
	const (
		first        = int64(2)
		legacyLatest = int64(100)
		last         = int64(228)
		concurrency  = 3
	)
	fake := &parallelDirectFake{
		holdFirst: first,
		release:   make(chan struct{}),
	}
	var heights []int64

	err := iterateParallelDirectStorageDiffsContext(
		context.Background(), fake.source, legacyLatest, concurrency, first, last,
		func(height int64, storage []dtypes.AccountStorageDiff) error {
			heights = append(heights, height)
			require.Empty(t, storage)
			return nil
		},
	)
	require.NoError(t, err)
	require.Len(t, heights, int(last-first+1))
	for index, height := range heights {
		require.Equal(t, first+int64(index), height)
	}

	ranges, completed, maxActive := fake.snapshot()
	sort.Slice(ranges, func(i, j int) bool { return ranges[i][0] < ranges[j][0] })
	require.Equal(t, [][2]int64{
		{2, 65},
		{66, legacyLatest},
		{legacyLatest + 1, 164},
		{165, last},
	}, ranges)
	require.NotEmpty(t, completed)
	require.NotEqual(t, first, completed[0][0], "the held first shard must complete out of order")
	require.LessOrEqual(t, maxActive, concurrency)
}

func TestIterateParallelDirectStorageDiffsContextBoundsConcurrency(t *testing.T) {
	const concurrency = 3
	fake := &parallelDirectFake{
		barrierTarget: concurrency,
		barrier:       make(chan struct{}),
	}

	err := iterateParallelDirectStorageDiffsContext(
		context.Background(), fake.source, 0, concurrency, 2, 2+directIAVLShardSize*4-1,
		func(int64, []dtypes.AccountStorageDiff) error { return nil },
	)
	require.NoError(t, err)
	_, _, maxActive := fake.snapshot()
	require.Equal(t, concurrency, maxActive)
}

func TestIterateParallelDirectStorageDiffsContextRejectsNilFactoryResult(t *testing.T) {
	err := iterateParallelDirectStorageDiffsContext(
		context.Background(), func() stateChangeSource { return nil }, 0, 1, 2, 2,
		func(int64, []dtypes.AccountStorageDiff) error { return nil },
	)
	require.ErrorContains(t, err, "direct state change source factory returned nil")
}

func TestIterateParallelDirectStorageDiffsContextPropagatesWorkerError(t *testing.T) {
	wantErr := errors.New("worker failed")
	fake := &parallelDirectFake{workerError: wantErr, errorFirst: 2}

	err := iterateParallelDirectStorageDiffsContext(
		context.Background(), fake.source, 0, 2, 2, 129,
		func(int64, []dtypes.AccountStorageDiff) error { return nil },
	)
	require.ErrorIs(t, err, wantErr)
	require.ErrorContains(t, err, "direct shard 1 versions 2-65")
}

func TestIterateParallelDirectStorageDiffsContextPropagatesCallbackError(t *testing.T) {
	wantErr := errors.New("callback failed")
	fake := &parallelDirectFake{}

	err := iterateParallelDirectStorageDiffsContext(
		context.Background(), fake.source, 0, 2, 2, 129,
		func(height int64, _ []dtypes.AccountStorageDiff) error {
			if height == 7 {
				return wantErr
			}
			return nil
		},
	)
	require.ErrorIs(t, err, wantErr)
}

func TestIterateParallelDirectStorageDiffsContextHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fake := &parallelDirectFake{}

	err := iterateParallelDirectStorageDiffsContext(
		ctx, fake.source, 0, 2, 2, 129,
		func(int64, []dtypes.AccountStorageDiff) error { return nil },
	)
	require.ErrorIs(t, err, context.Canceled)
}

type postCheckMismatchSource struct {
	traversed bool
}

func (source *postCheckMismatchSource) VersionHash(version int64) ([]byte, error) {
	value := byte(version)
	if source.traversed {
		value++
	}
	return bytes.Repeat([]byte{value}, legacyHashLength), nil
}

func (source *postCheckMismatchSource) TraverseStateChanges(
	first, last int64,
	callback func(int64, *iavl.ChangeSet) error,
) error {
	for version := first; version <= last; version++ {
		if err := callback(version, &iavl.ChangeSet{}); err != nil {
			return err
		}
	}
	source.traversed = true
	return nil
}

func TestIterateParallelDirectStorageDiffsContextPublishesOnlyPostCheckedShards(t *testing.T) {
	callbacks := 0
	err := iterateParallelDirectStorageDiffsContext(
		context.Background(),
		func() stateChangeSource { return &postCheckMismatchSource{} },
		0, 1, 2, 2+directIAVLShardSize-1,
		func(int64, []dtypes.AccountStorageDiff) error {
			callbacks++
			return nil
		},
	)
	require.ErrorContains(t, err, "root changed during traversal")
	require.Zero(t, callbacks)
}

func TestIterateParallelDirectStorageDiffsContextRejectsInvalidLegacyBoundary(t *testing.T) {
	fake := &parallelDirectFake{}
	err := iterateParallelDirectStorageDiffsContext(
		context.Background(), fake.source, -1, 1, 2, 2,
		func(int64, []dtypes.AccountStorageDiff) error { return nil },
	)
	require.ErrorContains(t, err, "invalid latest legacy version -1")
}

func TestEstimatedCanonicalStorageBytesBoundsRetainedMemory(t *testing.T) {
	got, err := estimatedCanonicalStorageBytes(nil)
	require.NoError(t, err)
	require.Zero(t, got)

	one := []dtypes.AccountStorageDiff{{Values: []dtypes.IndexValuePair{{}}}}
	got, err = estimatedCanonicalStorageBytes(one)
	require.NoError(t, err)
	require.Equal(t, directResultBaseBytes+2*directResultItemBytes, got)

	remaining := (maxObjectSize - directResultBaseBytes) / directResultItemBytes
	values := make([]dtypes.IndexValuePair, int(remaining))
	atLimit := []dtypes.AccountStorageDiff{{Values: values[:remaining-1]}}
	got, err = estimatedCanonicalStorageBytes(atLimit)
	require.NoError(t, err)
	require.Equal(t, directResultBaseBytes+remaining*directResultItemBytes, got)

	tooLarge := []dtypes.AccountStorageDiff{{Values: values}}
	_, err = estimatedCanonicalStorageBytes(tooLarge)
	require.ErrorContains(t, err, "retained canonical storage exceeds")

	weight, err := canonicalObjectOperationBytes(one, false)
	require.NoError(t, err)
	require.Equal(t, maxObjectOperationBytes+directResultBaseBytes+2*directResultItemBytes, weight)
	weight, err = canonicalObjectOperationBytes(nil, true)
	require.NoError(t, err)
	require.Equal(t, int64(1<<20), weight)
}

func TestAddDirectShardRetainedBytesEnforcesCumulativeLimit(t *testing.T) {
	one := []dtypes.AccountStorageDiff{{Values: []dtypes.IndexValuePair{{}}}}
	retained, err := addDirectShardRetainedBytes(0, nil)
	require.NoError(t, err)
	require.Equal(t, directResultItemBytes, retained)

	retained, err = addDirectShardRetainedBytes(retained, one)
	require.NoError(t, err)
	require.Equal(t, 4*directResultItemBytes, retained)

	oneBytes := int64(3) * directResultItemBytes
	retained, err = addDirectShardRetainedBytes(maxObjectSize-oneBytes, one)
	require.NoError(t, err)
	require.Equal(t, maxObjectSize, retained)

	_, err = addDirectShardRetainedBytes(maxObjectSize-oneBytes+1, one)
	require.ErrorContains(t, err, "retained canonical shard exceeds")
}
