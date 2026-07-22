package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	dtypes "github.com/evmos/ethermint/debank/types"
	"github.com/stretchr/testify/require"

	storetypes "cosmossdk.io/store/types"
)

func TestResolvePlanOutputBeforeDumpSeal(t *testing.T) {
	parent := t.TempDir()
	_, _, err := resolvePlanOutput(filepath.Join(parent, "plan.invalid"))
	require.ErrorContains(t, err, "must end in .staging")

	sealed := filepath.Join(parent, "plan.sealed")
	require.NoError(t, os.WriteFile(sealed, []byte("occupied"), 0o600))
	_, _, err = resolvePlanOutput(filepath.Join(parent, "plan.staging"))
	require.ErrorContains(t, err, "sealed plan must be a non-symlink directory")
	body, readErr := os.ReadFile(sealed)
	require.NoError(t, readErr)
	require.Equal(t, []byte("occupied"), body)

	require.NoError(t, os.Remove(sealed))
	require.NoError(t, os.Mkdir(filepath.Join(parent, "plan.staging"), 0o755))
	_, _, err = resolvePlanOutput(filepath.Join(parent, "plan.staging"))
	require.ErrorContains(t, err, "without a resumable checkpoint")
}

func TestReuseSealedPlanValidatesIdentity(t *testing.T) {
	manifestPath, manifest, _, _ := makeSealedPlanRecords(t, 2)
	sealedPath := filepath.Dir(manifestPath)
	loaded, gotPath, found, err := reuseSealedPlanContext(context.Background(), sealedPath, manifest)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, manifestPath, gotPath)
	require.Equal(t, manifest.RunID, loaded.RunID)

	different := manifest
	different.Prefix = "different"
	_, _, found, err = reuseSealedPlanContext(context.Background(), sealedPath, different)
	require.ErrorContains(t, err, "identity differs")
	require.False(t, found)
}

func TestExtendPlanPrefixDigestBindsEveryInput(t *testing.T) {
	previous := common.BytesToHash([]byte{1})
	root := common.BytesToHash([]byte{2})
	parent := common.BytesToHash([]byte{3})
	canonical := []dtypes.AccountStorageDiff{testStorage(1, 1, 2)}
	base, err := extendPlanPrefixDigest(previous, 10, root, parent, canonical)
	require.NoError(t, err)
	variants := []struct {
		previous  common.Hash
		height    uint64
		root      common.Hash
		parent    common.Hash
		canonical []dtypes.AccountStorageDiff
	}{
		{common.BytesToHash([]byte{9}), 10, root, parent, canonical},
		{previous, 11, root, parent, canonical},
		{previous, 10, common.BytesToHash([]byte{9}), parent, canonical},
		{previous, 10, root, common.BytesToHash([]byte{9}), canonical},
		{previous, 10, root, parent, []dtypes.AccountStorageDiff{testStorage(1, 1, 3)}},
	}
	for _, variant := range variants {
		got, err := extendPlanPrefixDigest(variant.previous, variant.height, variant.root, variant.parent, variant.canonical)
		require.NoError(t, err)
		require.NotEqual(t, base, got)
	}
}

type fakeCommitInfoReader struct {
	infos map[int64]*storetypes.CommitInfo
	calls []int64
}

func (r *fakeCommitInfoReader) commitInfo(version int64) (*storetypes.CommitInfo, error) {
	r.calls = append(r.calls, version)
	info, ok := r.infos[version]
	if !ok {
		return nil, fmt.Errorf("unexpected commit info version %d", version)
	}
	return info, nil
}

func TestResolvePlanRange(t *testing.T) {
	full, err := resolvePlanRange(planOptions{}, 200)
	require.NoError(t, err)
	require.Equal(t, planRange{
		FirstHeight: 2, FinalHeight: 200, DumpFirstVersion: 1,
		ManifestSchema: manifestSchema, DumpSchema: dumpManifestSchema,
	}, full)

	pilot, err := resolvePlanRange(planOptions{Pilot: true, PilotFirstHeight: 100, PilotFinalHeight: 109}, 200)
	require.NoError(t, err)
	require.Equal(t, planRange{
		FirstHeight: 100, FinalHeight: 109, DumpFirstVersion: 100,
		ManifestSchema: pilotManifestSchema, DumpSchema: pilotDumpManifestSchema,
	}, pilot)

	for _, options := range []planOptions{
		{Pilot: true, PilotFirstHeight: 1, PilotFinalHeight: 10},
		{Pilot: true, PilotFirstHeight: 100, PilotFinalHeight: 99},
		{Pilot: true, PilotFirstHeight: 100, PilotFinalHeight: 201},
		{PilotFirstHeight: 100, PilotFinalHeight: 109},
	} {
		_, err := resolvePlanRange(options, 200)
		require.Error(t, err)
	}
}

func TestArchiveRootsUsesZeroParentAtHeightTwo(t *testing.T) {
	info := &storetypes.CommitInfo{Version: 1}
	reader := &fakeCommitInfoReader{infos: map[int64]*storetypes.CommitInfo{1: info}}
	root, parent, err := archiveRoots(reader, 2)
	require.NoError(t, err)
	require.Equal(t, common.BytesToHash(info.Hash()), root)
	require.Equal(t, common.Hash{}, parent)
}

func TestArchiveRootCursorReadsEachSequentialVersionOnce(t *testing.T) {
	infos := make(map[int64]*storetypes.CommitInfo)
	for version := int64(1); version <= 3; version++ {
		infos[version] = &storetypes.CommitInfo{Version: version}
	}
	reader := &fakeCommitInfoReader{infos: infos}
	cursor, err := newArchiveRootCursor(reader, 2)
	require.NoError(t, err)
	var previous common.Hash
	for height := int64(2); height <= 4; height++ {
		root, parent, err := cursor.Next(height)
		require.NoError(t, err)
		require.Equal(t, previous, parent)
		previous = root
	}
	require.Equal(t, []int64{1, 2, 3}, reader.calls)
}

func TestArchiveRootCursorPilotReadsOnePredecessorThenSequentialVersions(t *testing.T) {
	infos := map[int64]*storetypes.CommitInfo{
		98:  {Version: 98},
		99:  {Version: 99},
		100: {Version: 100},
	}
	reader := &fakeCommitInfoReader{infos: infos}
	cursor, err := newArchiveRootCursor(reader, 100)
	require.NoError(t, err)
	root, parent, err := cursor.Next(100)
	require.NoError(t, err)
	require.Equal(t, common.BytesToHash(infos[99].Hash()), root)
	require.Equal(t, common.BytesToHash(infos[98].Hash()), parent)
	root, parent, err = cursor.Next(101)
	require.NoError(t, err)
	require.Equal(t, common.BytesToHash(infos[100].Hash()), root)
	require.Equal(t, common.BytesToHash(infos[99].Hash()), parent)
	require.Equal(t, []int64{99, 98, 100}, reader.calls)
}

func TestMakePackRecord(t *testing.T) {
	root := common.BytesToHash([]byte{2})
	parent := common.BytesToHash([]byte{1})
	old := dtypes.BlockStorageDiff{
		Hash: root, ParentHash: parent, NewAccounts: []dtypes.NewAccount{}, DeletedAccounts: []common.Hash{},
		StorageDiff: []dtypes.AccountStorageDiff{testStorage(1, 1, 1)}, NewCodes: []dtypes.NewCode{},
	}
	body, err := rlp.EncodeToBytes(old)
	require.NoError(t, err)
	object := storedObject{Body: body, ETag: "etag"}
	canonical := []dtypes.AccountStorageDiff{testStorage(1, 1, 2)}
	record, changed, err := makePackRecord(2, "key", object, root, parent, canonical)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, sha256Hash(body), record.OldSHA256)
	require.NotEqual(t, record.OldSHA256, record.NewSHA256)
	require.NoError(t, verifyRecordTarget(record, record.NewBody, record.NewSHA256))

	_, changed, err = makePackRecord(2, "key", storedObject{Body: record.NewBody}, root, parent, canonical)
	require.NoError(t, err)
	require.False(t, changed)
}

func TestMakePackRecordRejectsInvalidObject(t *testing.T) {
	root := common.BytesToHash([]byte{2})
	parent := common.BytesToHash([]byte{1})
	canonical := []dtypes.AccountStorageDiff{testStorage(1, 1, 2)}

	_, _, err := makePackRecord(2, "key", storedObject{Body: []byte{0xff}}, root, parent, canonical)
	require.ErrorContains(t, err, "decode state diff")

	diff := dtypes.BlockStorageDiff{Hash: common.BytesToHash([]byte{3}), ParentHash: parent}
	body, encodeErr := rlp.EncodeToBytes(diff)
	require.NoError(t, encodeErr)
	_, _, err = makePackRecord(2, "key", storedObject{Body: body}, root, parent, canonical)
	require.ErrorContains(t, err, "root mismatch")

	valid := dtypes.BlockStorageDiff{Hash: root, ParentHash: parent}
	body, encodeErr = rlp.EncodeToBytes(valid)
	require.NoError(t, encodeErr)
	body = append(body, 0x80)
	_, _, err = makePackRecord(2, "key", storedObject{Body: body}, root, parent, canonical)
	require.Error(t, err)
}

func TestPlanObjectAnalysisReachesTargetRateAtTwentyMillisecondLatency(t *testing.T) {
	_, _, records, objects := makeSealedPlanRecords(t, 1000)
	tasks := make([]planTask, 0, len(records))
	for _, record := range records {
		var target dtypes.BlockStorageDiff
		require.NoError(t, rlp.DecodeBytes(record.NewBody, &target))
		tasks = append(tasks, planTask{
			height: record.Height, root: target.Hash, parent: target.ParentHash,
			key: record.Key, canonical: target.StorageDiff, bytes: maxObjectOperationBytes,
		})
	}
	store := &fakeObjectStore{objects: objects, getDelay: 20 * time.Millisecond}
	options := defaultParallelOptions()
	options.Concurrency = 64
	options.Window = 256
	limiter, err := newByteLimiter(options.MaxInFlightBytes)
	require.NoError(t, err)
	started := time.Now()
	err = runOrderedPipeline(context.Background(), 2, options.Concurrency, options.Window,
		func(ctx context.Context, emit func(uint64, planTask) error) error {
			for _, task := range tasks {
				if err := limiter.Acquire(ctx, task.bytes); err != nil {
					return err
				}
				if err := emit(task.height, task); err != nil {
					limiter.Release(task.bytes)
					return err
				}
			}
			return nil
		},
		func(ctx context.Context, task planTask) (planOutcome, error) {
			outcome, err := processPlanTask(ctx, defaultBucket, task, store)
			if err != nil {
				limiter.Release(task.bytes)
				return planOutcome{}, err
			}
			outcome.bytes = task.bytes
			return outcome, nil
		},
		func(_ uint64, outcome planOutcome) error {
			limiter.Release(outcome.bytes)
			require.True(t, outcome.changed)
			return nil
		},
	)
	require.NoError(t, err)
	rate := float64(len(tasks)) / time.Since(started).Seconds()
	t.Logf("plan analysis rate %.2f objects/s", rate)
	require.GreaterOrEqual(t, rate, float64(1000))
}
