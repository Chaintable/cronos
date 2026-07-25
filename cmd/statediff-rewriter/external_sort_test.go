package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func byteLess(left, right []byte) bool {
	return bytes.Compare(left, right) < 0
}

func collectExternalRecords(t *testing.T, reader *externalMergeReader) [][]byte {
	t.Helper()
	var records [][]byte
	for {
		record, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return records
		}
		require.NoError(t, err)
		records = append(records, record)
	}
}

func TestExternalSorterMultipleChunks(t *testing.T) {
	sorter, err := newExternalSorter(t.TempDir(), 70, 10, byteLess)
	require.NoError(t, err)
	defer sorter.Close()

	input := [][]byte{[]byte("g"), []byte("a"), []byte("d"), []byte("b"), []byte("f"), []byte("c"), []byte("e")}
	for _, record := range input {
		require.NoError(t, sorter.Feed(record))
	}
	require.GreaterOrEqual(t, len(sorter.chunks), 3)

	reader, err := sorter.Finalize()
	require.NoError(t, err)
	require.Equal(t, [][]byte{
		[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"), []byte("f"), []byte("g"),
	}, collectExternalRecords(t, reader))
	require.NoError(t, reader.Close())
}

func TestExternalSorterUsesInjectedComparison(t *testing.T) {
	comparisons := 0
	descending := func(left, right []byte) bool {
		comparisons++
		return bytes.Compare(left, right) > 0
	}
	sorter, err := newExternalSorter(t.TempDir(), 68, 10, descending)
	require.NoError(t, err)
	defer sorter.Close()
	for _, record := range [][]byte{[]byte("a"), []byte("c"), []byte("b"), []byte("b")} {
		require.NoError(t, sorter.Feed(record))
	}
	reader, err := sorter.Finalize()
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("c"), []byte("b"), []byte("b"), []byte("a")}, collectExternalRecords(t, reader))
	require.Positive(t, comparisons)
	require.NoError(t, reader.Close())
}

func TestExternalSorterCopiesFedRecords(t *testing.T) {
	sorter, err := newExternalSorter(t.TempDir(), 64, 2, byteLess)
	require.NoError(t, err)
	defer sorter.Close()
	record := []byte("a")
	require.NoError(t, sorter.Feed(record))
	record[0] = 'z'
	reader, err := sorter.Finalize()
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a")}, collectExternalRecords(t, reader))
	require.NoError(t, reader.Close())
}

func TestExternalSorterReservesMinFreeForEveryOutput(t *testing.T) {
	sorter, err := newExternalSorterWithContext(
		context.Background(), t.TempDir(), externalRecordMemoryBytes+1, 2, 100, byteLess,
	)
	require.NoError(t, err)
	defer sorter.Close()
	sorter.allocationUnit = 4096
	var requirements []uint64
	sorter.ensureSpace = func(path string, minimum uint64) error {
		require.Equal(t, sorter.workDir, path)
		requirements = append(requirements, minimum)
		return nil
	}
	require.NoError(t, sorter.Feed([]byte("b")))
	require.NoError(t, sorter.Feed([]byte("a")))
	reader, err := sorter.Finalize()
	require.NoError(t, err)
	require.Equal(t, []uint64{12_388, 12_388}, requirements)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, collectExternalRecords(t, reader))
	require.NoError(t, reader.Close())
}

func TestExternalSorterFreeSpaceFailureIsSticky(t *testing.T) {
	sorter, err := newExternalSorterWithContext(
		context.Background(), t.TempDir(), externalRecordMemoryBytes+1, 2, 100, byteLess,
	)
	require.NoError(t, err)
	defer sorter.Close()
	want := errors.New("insufficient external sort space")
	sorter.ensureSpace = func(string, uint64) error { return want }
	require.NoError(t, sorter.Feed([]byte("a")))
	require.ErrorIs(t, sorter.Feed([]byte("b")), want)
	_, err = sorter.Finalize()
	require.ErrorIs(t, err, want)
	require.Empty(t, sorter.chunks)
}

func TestExternalSorterHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sorter, err := newExternalSorterWithContext(ctx, t.TempDir(), 64, 2, 0, byteLess)
	require.NoError(t, err)
	defer sorter.Close()
	require.NoError(t, sorter.Feed([]byte("a")))
	cancel()
	require.ErrorIs(t, sorter.Feed([]byte("b")), context.Canceled)
	_, err = sorter.Finalize()
	require.ErrorIs(t, err, context.Canceled)
}

func TestExternalSorterMultiPassFanInTwo(t *testing.T) {
	sorter, err := newExternalSorter(t.TempDir(), externalRecordMemoryBytes+1, 2, byteLess)
	require.NoError(t, err)
	defer sorter.Close()
	for _, record := range [][]byte{
		[]byte("i"), []byte("h"), []byte("g"), []byte("f"), []byte("e"),
		[]byte("d"), []byte("c"), []byte("b"), []byte("a"),
	} {
		require.NoError(t, sorter.Feed(record))
	}
	require.Len(t, sorter.chunks, 8)
	initialChunks := append([]string(nil), sorter.chunks...)

	reader, err := sorter.Finalize()
	require.NoError(t, err)
	require.LessOrEqual(t, len(sorter.chunks), 2)
	for _, path := range initialChunks {
		_, statErr := os.Stat(path)
		require.ErrorIs(t, statErr, os.ErrNotExist)
	}
	require.Equal(t, [][]byte{
		[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"),
		[]byte("f"), []byte("g"), []byte("h"), []byte("i"),
	}, collectExternalRecords(t, reader))
	require.NoError(t, reader.Close())
}

func TestExternalSorterFailedMergeKeepsUnreplacedInputsAndCleansPartialOutput(t *testing.T) {
	sorter, err := newExternalSorter(t.TempDir(), externalRecordMemoryBytes+1, 2, byteLess)
	require.NoError(t, err)
	workDir := sorter.workDir
	defer sorter.Close()
	for _, record := range [][]byte{[]byte("e"), []byte("d"), []byte("c"), []byte("b"), []byte("a")} {
		require.NoError(t, sorter.Feed(record))
	}
	require.Len(t, sorter.chunks, 4)
	firstGroup := append([]string(nil), sorter.chunks[:2]...)
	failedInput := sorter.chunks[2]
	require.NoError(t, os.Truncate(sorter.chunks[2], 0))

	_, err = sorter.Finalize()
	require.Error(t, err)
	require.NotErrorIs(t, err, io.EOF)
	entries, readErr := os.ReadDir(workDir)
	require.NoError(t, readErr)
	require.NotEmpty(t, entries)
	for _, entry := range entries {
		require.False(t, entry.IsDir())
		require.NotContains(t, entry.Name(), ".partial")
	}
	for _, path := range firstGroup {
		_, statErr := os.Stat(path)
		require.ErrorIs(t, statErr, os.ErrNotExist)
	}
	require.FileExists(t, failedInput)

	require.NoError(t, sorter.Close())
	_, err = os.Stat(workDir)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestExternalSorterRejectsOversizedRecord(t *testing.T) {
	sorter, err := newExternalSorter(t.TempDir(), externalRecordMemoryBytes+3, 2, byteLess)
	require.NoError(t, err)
	workDir := sorter.workDir
	err = sorter.Feed([]byte("four"))
	require.ErrorContains(t, err, "exceeding chunk limit")
	require.EqualError(t, sorter.Feed([]byte("a")), err.Error())
	_, finalizeErr := sorter.Finalize()
	require.EqualError(t, finalizeErr, err.Error())
	require.NoError(t, sorter.Close())
	_, statErr := os.Stat(workDir)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestNewExternalSorterRejectsInvalidOptions(t *testing.T) {
	dir := t.TempDir()
	tests := []struct {
		name          string
		dir           string
		maxChunkBytes int64
		maxChunks     int
		less          func([]byte, []byte) bool
	}{
		{name: "empty directory", maxChunkBytes: 64, maxChunks: 1, less: byteLess},
		{name: "small chunk", dir: dir, maxChunkBytes: externalRecordMemoryBytes - 1, maxChunks: 1, less: byteLess},
		{name: "zero chunks", dir: dir, maxChunkBytes: 64, less: byteLess},
		{name: "single chunk", dir: dir, maxChunkBytes: 64, maxChunks: 1, less: byteLess},
		{name: "nil comparison", dir: dir, maxChunkBytes: 64, maxChunks: 1},
		{name: "missing parent", dir: filepath.Join(dir, "missing"), maxChunkBytes: 64, maxChunks: 1, less: byteLess},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sorter, err := newExternalSorter(test.dir, test.maxChunkBytes, test.maxChunks, test.less)
			require.Error(t, err)
			require.Nil(t, sorter)
		})
	}
}

func TestExternalSorterCloseCleansTemporaryDirectory(t *testing.T) {
	t.Run("before finalize", func(t *testing.T) {
		sorter, err := newExternalSorter(t.TempDir(), 64, 2, byteLess)
		require.NoError(t, err)
		workDir := sorter.workDir
		require.NoError(t, sorter.Feed([]byte("record")))
		require.NoError(t, sorter.Close())
		require.NoError(t, sorter.Close())
		_, err = os.Stat(workDir)
		require.ErrorIs(t, err, os.ErrNotExist)
	})

	t.Run("merge reader owns cleanup", func(t *testing.T) {
		sorter, err := newExternalSorter(t.TempDir(), 64, 2, byteLess)
		require.NoError(t, err)
		workDir := sorter.workDir
		require.NoError(t, sorter.Feed([]byte("record")))
		reader, err := sorter.Finalize()
		require.NoError(t, err)
		require.NoError(t, reader.Close())
		require.NoError(t, reader.Close())
		require.NoError(t, sorter.Close())
		_, err = os.Stat(workDir)
		require.ErrorIs(t, err, os.ErrNotExist)
	})
}

func TestExternalSorterDetectsDamagedChunk(t *testing.T) {
	tests := []struct {
		name   string
		damage func(*testing.T, string)
	}{
		{
			name: "corrupt",
			damage: func(t *testing.T, path string) {
				t.Helper()
				body, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Greater(t, len(body), 1)
				body[len(body)-1] ^= 0xff
				require.NoError(t, os.WriteFile(path, body, 0o600))
			},
		},
		{
			name: "truncated",
			damage: func(t *testing.T, path string) {
				t.Helper()
				info, err := os.Stat(path)
				require.NoError(t, err)
				require.NoError(t, os.Truncate(path, info.Size()-1))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sorter, err := newExternalSorter(t.TempDir(), externalRecordMemoryBytes+1, 4, byteLess)
			require.NoError(t, err)
			defer sorter.Close()
			require.NoError(t, sorter.Feed([]byte("b")))
			require.NoError(t, sorter.Feed([]byte("a")))
			require.Len(t, sorter.chunks, 1)
			test.damage(t, sorter.chunks[0])

			reader, finalizeErr := sorter.Finalize()
			if finalizeErr == nil {
				defer reader.Close()
				for {
					_, finalizeErr = reader.Next()
					if finalizeErr != nil {
						break
					}
				}
			}
			require.Error(t, finalizeErr)
			require.NotErrorIs(t, finalizeErr, io.EOF)
		})
	}
}

func TestReadExternalRecordRejectsShortOrInvalidRecords(t *testing.T) {
	t.Run("short header", func(t *testing.T) {
		_, err := readExternalRecord(bytes.NewReader([]byte{0, 0, 0}), 64)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})

	t.Run("short body", func(t *testing.T) {
		body := make([]byte, externalRecordLengthBytes+2)
		binary.BigEndian.PutUint32(body, 3)
		_, err := readExternalRecord(bytes.NewReader(body), 64)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})

	t.Run("declared length exceeds limit", func(t *testing.T) {
		body := make([]byte, externalRecordLengthBytes)
		binary.BigEndian.PutUint32(body, 33)
		_, err := readExternalRecord(bytes.NewReader(body), 64)
		require.ErrorContains(t, err, "exceeds external sort chunk limit")
	})
}

func TestExternalSorterEmpty(t *testing.T) {
	sorter, err := newExternalSorter(t.TempDir(), 64, 2, byteLess)
	require.NoError(t, err)
	reader, err := sorter.Finalize()
	require.NoError(t, err)
	_, err = reader.Next()
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, reader.Close())
}
