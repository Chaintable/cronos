package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadRocksDBIdentity(t *testing.T) {
	home := t.TempDir()
	dir := filepath.Join(home, "data", "application.db")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "IDENTITY"), []byte("db-123\n"), 0o600))

	identity, err := readRocksDBIdentity(home)
	require.NoError(t, err)
	require.Equal(t, "db-123", identity)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "IDENTITY"), []byte("db-123\ndb-456\n"), 0o600))
	_, err = readRocksDBIdentity(home)
	require.ErrorContains(t, err, "exactly one value")
}
