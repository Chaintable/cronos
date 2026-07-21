package main

import (
	"fmt"
	"testing"

	storetypes "cosmossdk.io/store/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/stretchr/testify/require"

	dtypes "github.com/evmos/ethermint/debank/types"
)

type fakeCommitInfoReader struct {
	infos map[int64]*storetypes.CommitInfo
}

func (r fakeCommitInfoReader) commitInfo(version int64) (*storetypes.CommitInfo, error) {
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
	root, parent, err := archiveRoots(fakeCommitInfoReader{infos: map[int64]*storetypes.CommitInfo{1: info}}, 2)
	require.NoError(t, err)
	require.Equal(t, common.BytesToHash(info.Hash()), root)
	require.Equal(t, common.Hash{}, parent)
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
