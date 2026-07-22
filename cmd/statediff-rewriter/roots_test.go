package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

func writeRootIndex(t *testing.T, path string, records ...rootRecord) {
	t.Helper()
	file, err := os.Create(path)
	require.NoError(t, err)
	for _, record := range records {
		require.NoError(t, writeRootRecord(file, record.Root, record.Height))
	}
	require.NoError(t, file.Close())
}

func TestCheckDuplicateRoots(t *testing.T) {
	t.Run("adjacent equal roots", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "roots.raw")
		root := common.BytesToHash([]byte{1})
		writeRootIndex(t, path, rootRecord{Root: root, Height: 3}, rootRecord{Root: root, Height: 2})
		name, hash, err := checkDuplicateRoots(path, dir)
		require.NoError(t, err)
		require.Equal(t, "roots.sorted", name)
		require.NotEmpty(t, hash)
	})

	t.Run("non-adjacent reused root", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "roots.raw")
		root := common.BytesToHash([]byte{1})
		writeRootIndex(t, path, rootRecord{Root: root, Height: 2}, rootRecord{Root: common.BytesToHash([]byte{2}), Height: 3}, rootRecord{Root: root, Height: 4})
		_, _, err := checkDuplicateRoots(path, dir)
		require.ErrorContains(t, err, "non-adjacent heights")
	})
}

func TestRootMultisetCommitmentIgnoresOrderButPreservesMultiplicity(t *testing.T) {
	dir := t.TempDir()
	first := filepath.Join(dir, "first")
	second := filepath.Join(dir, "second")
	third := filepath.Join(dir, "third")
	a := rootRecord{Root: common.BytesToHash([]byte{1}), Height: 2}
	b := rootRecord{Root: common.BytesToHash([]byte{2}), Height: 3}
	writeRootIndex(t, first, a, b, a)
	writeRootIndex(t, second, b, a, a)
	writeRootIndex(t, third, b, a, b)

	firstHash, firstCount, err := rootMultisetSHA256(first)
	require.NoError(t, err)
	secondHash, secondCount, err := rootMultisetSHA256(second)
	require.NoError(t, err)
	thirdHash, thirdCount, err := rootMultisetSHA256(third)
	require.NoError(t, err)
	require.Equal(t, firstCount, secondCount)
	require.Equal(t, firstHash, secondHash)
	require.Equal(t, firstCount, thirdCount)
	require.NotEqual(t, firstHash, thirdHash)
}
