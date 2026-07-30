package main

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	dtypes "github.com/evmos/ethermint/debank/types"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestReplaceStorageDiffRLPPreservesEveryOtherRawField(t *testing.T) {
	old := dtypes.BlockStorageDiff{
		Hash: common.HexToHash("0x01"), ParentHash: common.HexToHash("0x02"),
		NewAccounts: []dtypes.NewAccount{{
			Address: common.HexToHash("0x03"), Balance: uint256.NewInt(4), Nonce: 5,
			CodeHash: common.HexToHash("0x06"),
		}},
		DeletedAccounts: []common.Hash{common.HexToHash("0x07")},
		StorageDiff: []dtypes.AccountStorageDiff{{
			Address: common.HexToHash("0x08"),
			Values:  []dtypes.IndexValuePair{{Index: common.HexToHash("0x09"), Value: uint256.NewInt(10)}},
		}},
		NewCodes: []dtypes.NewCode{{CodeHash: common.HexToHash("0x0b"), Code: []byte{0x60, 0x00}}},
	}
	oldBody, err := rlp.EncodeToBytes(old)
	require.NoError(t, err)
	canonical := []dtypes.AccountStorageDiff{{
		Address: common.HexToHash("0x0c"),
		Values:  []dtypes.IndexValuePair{{Index: common.HexToHash("0x0d"), Value: uint256.NewInt(14)}},
	}}
	newBody, err := replaceStorageDiffRLP(oldBody, canonical)
	require.NoError(t, err)
	require.NoError(t, verifyRawRetainedFields(oldBody, newBody))

	oldFields, err := blockStorageDiffRawFields(oldBody)
	require.NoError(t, err)
	newFields, err := blockStorageDiffRawFields(newBody)
	require.NoError(t, err)
	for index := range oldFields {
		if index == storageDiffFieldIndex {
			require.False(t, bytes.Equal(oldFields[index], newFields[index]))
			continue
		}
		require.Equal(t, []byte(oldFields[index]), []byte(newFields[index]))
	}
	var decoded dtypes.BlockStorageDiff
	require.NoError(t, rlp.DecodeBytes(newBody, &decoded))
	require.Equal(t, canonical, decoded.StorageDiff)
}

func TestVerifyRawRetainedFieldsRejectsRetainedMutation(t *testing.T) {
	old := dtypes.BlockStorageDiff{Hash: common.HexToHash("0x01"), ParentHash: common.HexToHash("0x02")}
	oldBody, err := rlp.EncodeToBytes(old)
	require.NoError(t, err)
	changed := old
	changed.ParentHash = common.HexToHash("0x03")
	changedBody, err := rlp.EncodeToBytes(changed)
	require.NoError(t, err)
	require.ErrorContains(t, verifyRawRetainedFields(oldBody, changedBody), "field 1 changed")
}

func TestReplaceWorldStateRLPPreservesRootsAndReplacesEmptyFields(t *testing.T) {
	old := dtypes.BlockStorageDiff{
		Hash: common.HexToHash("0x01"), ParentHash: common.HexToHash("0x02"),
		NewAccounts:     []dtypes.NewAccount{{Address: common.HexToHash("0x03"), Balance: uint256.NewInt(4)}},
		DeletedAccounts: []common.Hash{common.HexToHash("0x05")},
		StorageDiff:     []dtypes.AccountStorageDiff{{Address: common.HexToHash("0x06")}},
		NewCodes:        []dtypes.NewCode{{CodeHash: common.HexToHash("0x07"), Code: []byte{0x60}}},
	}
	oldBody, err := rlp.EncodeToBytes(old)
	require.NoError(t, err)

	newBody, err := replaceWorldStateRLP(
		oldBody,
		expectedWorldState{},
		refillNewAccounts|refillDeletedAccounts|refillStorage|refillNewCodes,
	)
	require.NoError(t, err)
	requireWorldStateRootsRawUnchanged(t, oldBody, newBody)

	var got dtypes.BlockStorageDiff
	require.NoError(t, rlp.DecodeBytes(newBody, &got))
	require.Equal(t, old.Hash, got.Hash)
	require.Equal(t, old.ParentHash, got.ParentHash)
	require.Empty(t, got.NewAccounts)
	require.Empty(t, got.DeletedAccounts)
	require.Empty(t, got.StorageDiff)
	require.Empty(t, got.NewCodes)

	withoutStorage, err := replaceWorldStateRLP(
		oldBody,
		expectedWorldState{},
		refillNewAccounts|refillDeletedAccounts|refillNewCodes,
	)
	require.NoError(t, err)
	oldFields, err := blockStorageDiffRawFields(oldBody)
	require.NoError(t, err)
	newFields, err := blockStorageDiffRawFields(withoutStorage)
	require.NoError(t, err)
	require.Equal(t, []byte(oldFields[storageDiffFieldIndex]), []byte(newFields[storageDiffFieldIndex]))
}

func requireWorldStateRootsRawUnchanged(t *testing.T, oldBody, newBody []byte) {
	t.Helper()
	oldFields, err := blockStorageDiffRawFields(oldBody)
	require.NoError(t, err)
	newFields, err := blockStorageDiffRawFields(newBody)
	require.NoError(t, err)
	require.Equal(t, []byte(oldFields[0]), []byte(newFields[0]))
	require.Equal(t, []byte(oldFields[1]), []byte(newFields[1]))
}
