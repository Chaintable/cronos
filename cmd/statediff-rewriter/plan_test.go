package main

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/stretchr/testify/require"

	dtypes "github.com/evmos/ethermint/debank/types"
)

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
