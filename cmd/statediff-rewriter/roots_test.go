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
