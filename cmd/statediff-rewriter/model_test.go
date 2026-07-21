package main

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	dtypes "github.com/evmos/ethermint/debank/types"
)

func testStorage(address, index byte, value uint64) dtypes.AccountStorageDiff {
	return dtypes.AccountStorageDiff{
		Address: common.BytesToHash([]byte{address}),
		Values:  []dtypes.IndexValuePair{{Index: common.BytesToHash([]byte{index}), Value: uint256.NewInt(value)}},
	}
}

func TestCompareStorage(t *testing.T) {
	canonical := []dtypes.AccountStorageDiff{testStorage(1, 1, 2)}
	equal, noncanonical, conflict, stats, err := compareStorage(canonical, canonical)
	require.NoError(t, err)
	require.True(t, equal)
	require.False(t, noncanonical)
	require.False(t, conflict)
	require.Equal(t, slotStats{}, stats)

	duplicate := []dtypes.AccountStorageDiff{testStorage(1, 1, 2), testStorage(1, 1, 2)}
	equal, noncanonical, conflict, _, err = compareStorage(duplicate, canonical)
	require.NoError(t, err)
	require.False(t, equal)
	require.True(t, noncanonical)
	require.False(t, conflict)

	duplicate[1].Values[0].Value = uint256.NewInt(3)
	equal, noncanonical, conflict, _, err = compareStorage(duplicate, canonical)
	require.NoError(t, err)
	require.False(t, equal)
	require.True(t, noncanonical)
	require.True(t, conflict)

	conflictingStorage := []dtypes.AccountStorageDiff{testStorage(1, 1, 2), testStorage(1, 1, 3)}
	normalized, err := normalizeStorage(conflictingStorage)
	require.NoError(t, err)
	require.True(t, normalized.Noncanonical)
	require.True(t, normalized.Conflict)

	empty := []dtypes.AccountStorageDiff{{Address: common.BytesToHash([]byte{1})}}
	equal, noncanonical, conflict, _, err = compareStorage(empty, nil)
	require.NoError(t, err)
	require.False(t, equal)
	require.True(t, noncanonical)
	require.False(t, conflict)
}
