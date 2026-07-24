package main

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/cosmos/iavl"
	"github.com/stretchr/testify/require"
)

func rangeChangeSetBody(version int64) []byte {
	var header [16]byte
	binary.LittleEndian.PutUint64(header[:8], uint64(version))
	return header[:]
}

func rangeZlibBody(t *testing.T, body []byte) []byte {
	t.Helper()
	var compressed bytes.Buffer
	writer := zlib.NewWriter(&compressed)
	_, err := writer.Write(body)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return compressed.Bytes()
}

func writeRangeDumpFile(t *testing.T, staging, name string, body []byte) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(staging, "evm"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(staging, "evm", name), body, 0o600))
}

func sealedRangeDump(t *testing.T) (string, dumpManifest) {
	t.Helper()
	staging := filepath.Join(t.TempDir(), "dump.staging")
	for _, blockRange := range []versionRange{{First: 1, Last: 4}, {First: 5, Last: 8}, {First: 9, Last: 12}} {
		var body []byte
		for version := blockRange.First; version <= blockRange.Last; version++ {
			body = append(body, rangeChangeSetBody(version)...)
		}
		writeRangeDumpFile(t, staging, "block-"+strconv.FormatInt(blockRange.First, 10)+".zz", rangeZlibBody(t, body))
	}
	sealed, manifest, _, err := sealDump(staging)
	require.NoError(t, err)
	return sealed, manifest
}

func TestIterateSealedDumpRangeContextOnlyScansOverlappingFiles(t *testing.T) {
	sealed, manifest := sealedRangeDump(t)
	for _, name := range []string{"block-1.zz", "block-9.zz"} {
		require.NoError(t, os.WriteFile(filepath.Join(sealed, "evm", name), []byte("not zlib"), 0o600))
	}

	var versions []int64
	err := iterateSealedDumpRangeContext(
		context.Background(), sealed, manifest, 6, 7,
		func(version int64, _ *iavl.ChangeSet) error {
			versions = append(versions, version)
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []int64{6, 7}, versions)
}

func TestIterateSealedDumpRangeContextCrossesFiles(t *testing.T) {
	sealed, manifest := sealedRangeDump(t)
	var versions []int64
	err := iterateSealedDumpRangeContext(
		context.Background(), sealed, manifest, 3, 10,
		func(version int64, _ *iavl.ChangeSet) error {
			versions = append(versions, version)
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []int64{3, 4, 5, 6, 7, 8, 9, 10}, versions)
}

func TestIterateSealedDumpRangeContextValidatesTouchedFiles(t *testing.T) {
	tests := []struct {
		name      string
		corrupt   func(*testing.T, string, *dumpManifest)
		wantError string
	}{
		{
			name: "sha256",
			corrupt: func(t *testing.T, _ string, manifest *dumpManifest) {
				t.Helper()
				manifest.Files[1].SHA256 = strings.Repeat("0", 64)
			},
			wantError: "sealed dump file changed",
		},
		{
			name: "size",
			corrupt: func(t *testing.T, _ string, manifest *dumpManifest) {
				t.Helper()
				manifest.Files[1].Size++
			},
			wantError: "sealed dump file changed",
		},
		{
			name: "version",
			corrupt: func(t *testing.T, _ string, manifest *dumpManifest) {
				t.Helper()
				manifest.Files[1].FirstVersion++
			},
			wantError: "sealed dump file changed",
		},
		{
			name: "trailer",
			corrupt: func(t *testing.T, sealed string, manifest *dumpManifest) {
				t.Helper()
				path := filepath.Join(sealed, manifest.Files[1].Path)
				body, err := os.ReadFile(path)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(path, append(body, []byte("trailing")...), 0o600))
			},
			wantError: "trailing data",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sealed, manifest := sealedRangeDump(t)
			test.corrupt(t, sealed, &manifest)
			err := iterateSealedDumpRangeContext(
				context.Background(), sealed, manifest, 6, 7,
				func(int64, *iavl.ChangeSet) error { return nil },
			)
			require.ErrorContains(t, err, test.wantError)
		})
	}
}

func TestIterateSealedDumpRangeContextRequiresCoveredRange(t *testing.T) {
	sealed, manifest := sealedRangeDump(t)
	for _, blockRange := range []versionRange{{First: 0, Last: 1}, {First: 1, Last: 13}, {First: 8, Last: 7}} {
		err := iterateSealedDumpRangeContext(
			context.Background(), sealed, manifest, blockRange.First, blockRange.Last,
			func(int64, *iavl.ChangeSet) error { return nil },
		)
		require.Error(t, err)
	}
}
