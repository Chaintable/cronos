package main

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/cosmos/iavl"
	"github.com/stretchr/testify/require"
)

func encodeChangeSet(t *testing.T, version int64, pairs ...*iavl.KVPair) []byte {
	t.Helper()
	var payload bytes.Buffer
	for _, pair := range pairs {
		if pair.Delete {
			payload.WriteByte(1)
		} else {
			payload.WriteByte(0)
		}
		var scratch [10]byte
		n := binary.PutUvarint(scratch[:], uint64(len(pair.Key)))
		payload.Write(scratch[:n])
		payload.Write(pair.Key)
		if !pair.Delete {
			n = binary.PutUvarint(scratch[:], uint64(len(pair.Value)))
			payload.Write(scratch[:n])
			payload.Write(pair.Value)
		}
	}
	var result bytes.Buffer
	var header [16]byte
	binary.LittleEndian.PutUint64(header[:8], uint64(version))
	binary.LittleEndian.PutUint64(header[8:], uint64(payload.Len()))
	result.Write(header[:])
	result.Write(payload.Bytes())
	return result.Bytes()
}

func zlibBody(t *testing.T, body []byte) []byte {
	t.Helper()
	var compressed bytes.Buffer
	writer := zlib.NewWriter(&compressed)
	_, err := writer.Write(body)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return compressed.Bytes()
}

func writeDumpFile(t *testing.T, staging, name string, body []byte) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(staging, "evm"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(staging, "evm", name), body, 0o644))
}

func TestSealDumpSortsByFirstVersion(t *testing.T) {
	staging := filepath.Join(t.TempDir(), "dump.staging")
	var versions2To9 []byte
	for version := int64(2); version <= 9; version++ {
		versions2To9 = append(versions2To9, encodeChangeSet(t, version)...)
	}
	writeDumpFile(t, staging, "block-10.zz", zlibBody(t, encodeChangeSet(t, 10)))
	writeDumpFile(t, staging, "block-2.zz", zlibBody(t, versions2To9))
	writeDumpFile(t, staging, "block-1.zz", zlibBody(t, encodeChangeSet(t, 1)))

	sealed, manifest, hash, err := sealDump(staging)
	require.NoError(t, err)
	require.Equal(t, int64(1), manifest.FirstVersion)
	require.Equal(t, int64(10), manifest.LastVersion)
	require.Equal(t, "evm/block-1.zz", manifest.Files[0].Path)
	require.Equal(t, "evm/block-2.zz", manifest.Files[1].Path)
	require.Equal(t, "evm/block-10.zz", manifest.Files[2].Path)
	require.NotEmpty(t, hash)
	require.DirExists(t, sealed)
	require.NoDirExists(t, staging)
}

func TestScanZlibChangeSetsRejectsTruncationAndCorruption(t *testing.T) {
	for length := 1; length <= 15; length++ {
		t.Run("partial-header-"+strconv.Itoa(length), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "partial.zz")
			require.NoError(t, os.WriteFile(path, zlibBody(t, make([]byte, length)), 0o644))
			_, err := scanZlibChangeSets(path, nil)
			require.Error(t, err)
		})
	}

	t.Run("short payload", func(t *testing.T) {
		var header [16]byte
		binary.LittleEndian.PutUint64(header[:8], 1)
		binary.LittleEndian.PutUint64(header[8:], 10)
		path := filepath.Join(t.TempDir(), "short.zz")
		require.NoError(t, os.WriteFile(path, zlibBody(t, append(header[:], 1)), 0o644))
		_, err := scanZlibChangeSets(path, nil)
		require.Error(t, err)
	})

	t.Run("bad checksum", func(t *testing.T) {
		body := zlibBody(t, encodeChangeSet(t, 1))
		body[len(body)-1] ^= 0xff
		path := filepath.Join(t.TempDir(), "checksum.zz")
		require.NoError(t, os.WriteFile(path, body, 0o644))
		_, err := scanZlibChangeSets(path, nil)
		require.Error(t, err)
	})

	t.Run("missing trailer", func(t *testing.T) {
		body := zlibBody(t, encodeChangeSet(t, 1))
		path := filepath.Join(t.TempDir(), "truncated.zz")
		require.NoError(t, os.WriteFile(path, body[:len(body)-2], 0o644))
		_, err := scanZlibChangeSets(path, nil)
		require.Error(t, err)
	})

	t.Run("trailing data", func(t *testing.T) {
		body := append(zlibBody(t, encodeChangeSet(t, 1)), []byte("garbage")...)
		path := filepath.Join(t.TempDir(), "trailing.zz")
		require.NoError(t, os.WriteFile(path, body, 0o644))
		_, err := scanZlibChangeSets(path, nil)
		require.ErrorContains(t, err, "trailing data")
	})
}

func TestSealDumpRejectsGenesisStorage(t *testing.T) {
	staging := filepath.Join(t.TempDir(), "dump.staging")
	key := make([]byte, 53)
	key[0] = 0x02
	writeDumpFile(t, staging, "block-1.zz", zlibBody(t, encodeChangeSet(t, 1, &iavl.KVPair{Key: key, Value: []byte{1}})))
	_, _, _, err := sealDump(staging)
	require.ErrorContains(t, err, "genesis changeset contains EVM storage")
}
