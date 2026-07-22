package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	dtypes "github.com/evmos/ethermint/debank/types"
	"github.com/holiman/uint256"
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

func planTestBlockStorageDiff(root, parent common.Hash, storage []dtypes.AccountStorageDiff) dtypes.BlockStorageDiff {
	return dtypes.BlockStorageDiff{
		Hash: root, ParentHash: parent,
		NewAccounts: []dtypes.NewAccount{{
			Address: common.BytesToHash([]byte{10}), Balance: uint256.NewInt(11), Nonce: 12,
			CodeHash: common.BytesToHash([]byte{13}),
		}},
		DeletedAccounts: []common.Hash{common.BytesToHash([]byte{14})},
		StorageDiff:     storage,
		NewCodes: []dtypes.NewCode{{
			CodeHash: common.BytesToHash([]byte{15}), Code: []byte{0x60, 0x00},
		}},
	}
}

func requireChangedPackRecord(
	t *testing.T,
	old dtypes.BlockStorageDiff,
	object storedObject,
	record packRecord,
	canonical []dtypes.AccountStorageDiff,
) {
	t.Helper()
	require.Equal(t, packSchema, record.Schema)
	require.Equal(t, uint64(2), record.Height)
	require.Equal(t, "key", record.Key)
	require.Equal(t, object.ETag, record.OldETag)
	require.Equal(t, object.Body, record.OldBody)
	require.Equal(t, object.Headers, record.Headers)
	expectedNewBody, err := replaceStorageDiffRLP(object.Body, canonical)
	require.NoError(t, err)
	require.Equal(t, expectedNewBody, record.NewBody)
	require.False(t, bytes.Equal(record.OldBody, record.NewBody))
	require.Equal(t, sha256Hash(record.OldBody), record.OldSHA256)
	require.Equal(t, sha256Hash(record.NewBody), record.NewSHA256)
	retained, err := retainedDigest(old)
	require.NoError(t, err)
	require.Equal(t, retained, record.RetainedSHA256)
	require.Zero(t, record.SlotsAdded)
	require.Zero(t, record.SlotsRemoved)
	require.Zero(t, record.SlotsChanged)
	require.False(t, record.NoncanonicalOld)
	require.False(t, record.ConflictingOld)

	oldFields, err := blockStorageDiffRawFields(record.OldBody)
	require.NoError(t, err)
	newFields, err := blockStorageDiffRawFields(record.NewBody)
	require.NoError(t, err)
	for index := range oldFields {
		if index == storageDiffFieldIndex {
			require.False(t, bytes.Equal(oldFields[index], newFields[index]))
			continue
		}
		require.True(t, bytes.Equal(oldFields[index], newFields[index]), "raw RLP field %d changed", index)
	}

	var decoded dtypes.BlockStorageDiff
	require.NoError(t, rlp.DecodeBytes(record.NewBody, &decoded))
	decodedStorage, err := rlp.EncodeToBytes(decoded.StorageDiff)
	require.NoError(t, err)
	canonicalStorage, err := rlp.EncodeToBytes(canonical)
	require.NoError(t, err)
	require.Equal(t, canonicalStorage, decodedStorage)
}

func TestMakePackRecordReplacesStorageWithoutSemanticComparison(t *testing.T) {
	root := common.BytesToHash([]byte{2})
	parent := common.BytesToHash([]byte{1})
	canonical := []dtypes.AccountStorageDiff{testStorage(1, 1, 2), testStorage(2, 2, 3)}
	tests := []struct {
		name string
		old  []dtypes.AccountStorageDiff
	}{
		{name: "missing", old: []dtypes.AccountStorageDiff{testStorage(1, 1, 2)}},
		{name: "extra", old: []dtypes.AccountStorageDiff{testStorage(1, 1, 2), testStorage(2, 2, 3), testStorage(3, 3, 4)}},
		{name: "wrong", old: []dtypes.AccountStorageDiff{testStorage(1, 1, 99), testStorage(2, 2, 3)}},
		{name: "duplicate", old: []dtypes.AccountStorageDiff{testStorage(1, 1, 2), testStorage(1, 1, 99), testStorage(2, 2, 3)}},
		{name: "same slots different order", old: []dtypes.AccountStorageDiff{testStorage(2, 2, 3), testStorage(1, 1, 2)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			old := planTestBlockStorageDiff(root, parent, test.old)
			body, err := rlp.EncodeToBytes(old)
			require.NoError(t, err)
			object := storedObject{
				Body: body, ETag: "etag",
				Headers: objectHeaders{ContentType: "application/octet-stream", Metadata: []byte(`{"source":"test"}`)},
			}
			record, changed, err := makePackRecord(2, "key", object, root, parent, canonical)
			require.NoError(t, err)
			require.True(t, changed)
			requireChangedPackRecord(t, old, object, record, canonical)
		})
	}
}

func TestMakePackRecordNoopRequiresIdenticalBody(t *testing.T) {
	root := common.BytesToHash([]byte{2})
	parent := common.BytesToHash([]byte{1})
	canonical := []dtypes.AccountStorageDiff{testStorage(1, 1, 2), testStorage(2, 2, 3)}
	old := planTestBlockStorageDiff(root, parent, canonical)
	body, err := rlp.EncodeToBytes(old)
	require.NoError(t, err)
	record, changed, err := makePackRecord(2, "key", storedObject{Body: body}, root, parent, canonical)
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, packRecord{}, record)
}

func TestMakePackRecordTreatsNilAndEmptyStorageAsSameRLP(t *testing.T) {
	root := common.BytesToHash([]byte{2})
	parent := common.BytesToHash([]byte{1})
	tests := []struct {
		name      string
		old       []dtypes.AccountStorageDiff
		canonical []dtypes.AccountStorageDiff
	}{
		{name: "nil old empty canonical", old: nil, canonical: []dtypes.AccountStorageDiff{}},
		{name: "empty old nil canonical", old: []dtypes.AccountStorageDiff{}, canonical: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			old := planTestBlockStorageDiff(root, parent, test.old)
			body, err := rlp.EncodeToBytes(old)
			require.NoError(t, err)
			record, changed, err := makePackRecord(2, "key", storedObject{Body: body}, root, parent, test.canonical)
			require.NoError(t, err)
			require.False(t, changed)
			require.Equal(t, packRecord{}, record)
		})
	}
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
