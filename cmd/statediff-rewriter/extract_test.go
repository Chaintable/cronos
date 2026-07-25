package main

import (
	"bytes"
	"compress/zlib"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cosmos/iavl"
	iavldb "github.com/cosmos/iavl/db"
	"github.com/stretchr/testify/require"

	"cosmossdk.io/log"
)

type fakeTraverser struct {
	versions    []int64
	changes     map[int64]*iavl.ChangeSet
	err         error
	start       int64
	end         int64
	hashResults map[int64][][]byte
	hashCalls   map[int64]int
}

type failingLegacySource struct{ calls int }

func (source *failingLegacySource) InspectRoots(func(iavl.LegacyRootRecord) error) error {
	source.calls++
	return errors.New("legacy source must not be scanned")
}

func (source *failingLegacySource) InspectNodes(func(iavl.LegacyNodeRecord) error) error {
	source.calls++
	return errors.New("legacy source must not be scanned")
}

func (source *failingLegacySource) InspectOrphans(func(iavl.LegacyOrphanRecord) error) error {
	source.calls++
	return errors.New("legacy source must not be scanned")
}

func (f *fakeTraverser) TraverseStateChanges(start, end int64, callback func(int64, *iavl.ChangeSet) error) error {
	f.start, f.end = start, end
	for _, version := range f.versions {
		changeSet := f.changes[version]
		if changeSet == nil {
			changeSet = &iavl.ChangeSet{}
		}
		if err := callback(version, changeSet); err != nil {
			return err
		}
	}
	return f.err
}

func (f *fakeTraverser) VersionHash(version int64) ([]byte, error) {
	if f.hashCalls == nil {
		f.hashCalls = make(map[int64]int)
	}
	call := f.hashCalls[version]
	f.hashCalls[version]++
	if results := f.hashResults[version]; call < len(results) {
		if results[call] == nil {
			return nil, iavl.ErrVersionDoesNotExist
		}
		return append([]byte(nil), results[call]...), nil
	}
	return bytes.Repeat([]byte{byte(version)}, legacyHashLength), nil
}

func TestSplitDumpRanges(t *testing.T) {
	ranges, err := splitDumpRanges(100, 199, 8, 1_000_000)
	require.NoError(t, err)
	require.Equal(t, []versionRange{
		{First: 100, Last: 112},
		{First: 113, Last: 125},
		{First: 126, Last: 138},
		{First: 139, Last: 151},
		{First: 152, Last: 164},
		{First: 165, Last: 177},
		{First: 178, Last: 190},
		{First: 191, Last: 199},
	}, ranges)

	ranges, err = splitDumpRanges(1, 80_000_000, 8, 1_000_000)
	require.NoError(t, err)
	require.Len(t, ranges, 80)
	require.Equal(t, versionRange{First: 1, Last: 1_000_000}, ranges[0])
	require.Equal(t, versionRange{First: 79_000_001, Last: 80_000_000}, ranges[79])

	for _, testCase := range []struct {
		first       int64
		last        int64
		concurrency int
		chunkSize   int64
	}{
		{first: 0, last: 1, concurrency: 1, chunkSize: 1},
		{first: 2, last: 1, concurrency: 1, chunkSize: 1},
		{first: 1, last: 1, concurrency: 0, chunkSize: 1},
		{first: 1, last: 1, concurrency: 1, chunkSize: 0},
	} {
		_, err := splitDumpRanges(testCase.first, testCase.last, testCase.concurrency, testCase.chunkSize)
		require.Error(t, err)
	}
}

func TestSplitDumpRangesAtLegacyBoundary(t *testing.T) {
	legacy, modern, all, err := splitDumpRangesAtLegacy(1, 10, 4, 2, 3)
	require.NoError(t, err)
	require.Equal(t, []versionRange{{First: 1, Last: 2}, {First: 3, Last: 4}}, legacy)
	require.Equal(t, []versionRange{{First: 5, Last: 7}, {First: 8, Last: 10}}, modern)
	require.Equal(t, append(append([]versionRange{}, legacy...), modern...), all)
	require.Equal(t, int64(6), countVersionRanges(modern))

	legacy, modern, all, err = splitDumpRangesAtLegacy(6, 8, 4, 2, 3)
	require.NoError(t, err)
	require.Empty(t, legacy)
	require.Equal(t, []versionRange{{First: 6, Last: 7}, {First: 8, Last: 8}}, modern)
	require.Equal(t, modern, all)

	legacy, modern, all, err = splitDumpRangesAtLegacy(2, 4, 10, 2, 3)
	require.NoError(t, err)
	require.Equal(t, []versionRange{{First: 2, Last: 3}, {First: 4, Last: 4}}, legacy)
	require.Empty(t, modern)
	require.Equal(t, legacy, all)
}

func TestValidateLegacyPreparation(t *testing.T) {
	legacy := []versionRange{{First: 1, Last: 4}}
	modern := []versionRange{{First: 5, Last: 10}}
	require.NoError(t, validateLegacyPreparation(
		dumpOptions{FirstVersion: 1, LastVersion: 10, StopAfterLegacy: true}, 10, legacy, modern,
	))

	for _, testCase := range []struct {
		name    string
		options dumpOptions
		latest  int64
		legacy  []versionRange
		modern  []versionRange
		message string
	}{
		{
			name: "partial range", options: dumpOptions{FirstVersion: 2, LastVersion: 10, StopAfterLegacy: true},
			latest: 10, legacy: legacy, modern: modern, message: "full archive range",
		},
		{
			name: "not latest", options: dumpOptions{FirstVersion: 1, LastVersion: 9, StopAfterLegacy: true},
			latest: 10, legacy: legacy, modern: modern, message: "full archive range",
		},
		{
			name: "no legacy", options: dumpOptions{FirstVersion: 1, LastVersion: 10, StopAfterLegacy: true},
			latest: 10, modern: modern, message: "requires legacy versions",
		},
		{
			name: "no modern", options: dumpOptions{FirstVersion: 1, LastVersion: 10, StopAfterLegacy: true},
			latest: 10, legacy: legacy, message: "requires a modern suffix",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := validateLegacyPreparation(
				testCase.options, testCase.latest, testCase.legacy, testCase.modern,
			)
			require.ErrorContains(t, err, testCase.message)
		})
	}

	require.NoError(t, validateLegacyPreparation(dumpOptions{}, 0, nil, nil))
	require.ErrorContains(t, validateLegacyPreparation(
		dumpOptions{LegacyTrustNodeSet: true}, 10, nil, modern,
	), "legacy-trust-node-set requires legacy versions")
}

func TestSequentialDumpWriterWritesAndReusesRanges(t *testing.T) {
	evmDir := t.TempDir()
	ranges := []versionRange{{First: 1, Last: 2}, {First: 3, Last: 4}}
	key := legacyStorageKey(1)
	changes := map[int64]*iavl.ChangeSet{
		2: {Pairs: []*iavl.KVPair{{Key: key, Value: []byte{1}}}},
		4: {Pairs: []*iavl.KVPair{{Delete: true, Key: key}}},
	}

	writer, err := newSequentialDumpWriter(evmDir, ranges, zlib.BestSpeed, 1)
	require.NoError(t, err)
	for version := int64(1); version <= 4; version++ {
		changeSet := changes[version]
		if changeSet == nil {
			changeSet = &iavl.ChangeSet{}
		}
		require.NoError(t, writer.write(version, changeSet))
	}
	require.NoError(t, writer.finish())
	require.Equal(t, int64(2), writer.generatedFiles)
	require.Equal(t, int64(4), writer.generatedRecords)
	for _, blockRange := range ranges {
		require.NoError(t, validateDumpRangeFile(dumpRangePath(evmDir, blockRange), blockRange))
	}

	reused, err := newSequentialDumpWriter(evmDir, ranges, zlib.BestSpeed, 1)
	require.NoError(t, err)
	for version := int64(1); version <= 4; version++ {
		require.NoError(t, reused.write(version, &iavl.ChangeSet{}))
	}
	require.NoError(t, reused.finish())
	require.Equal(t, int64(2), reused.reusedFiles)
	require.Equal(t, int64(4), reused.reusedRecords)
	require.Zero(t, reused.generatedFiles)
}

func TestWriteLegacyDumpRangesSkipsScanWhenAllRangesReusable(t *testing.T) {
	evmDir := t.TempDir()
	ranges := []versionRange{{First: 1, Last: 2}, {First: 3, Last: 4}}
	writer, err := newSequentialDumpWriter(evmDir, ranges, zlib.BestSpeed, 1)
	require.NoError(t, err)
	for version := int64(1); version <= 4; version++ {
		require.NoError(t, writer.write(version, &iavl.ChangeSet{}))
	}
	require.NoError(t, writer.finish())
	stalePartial := dumpRangePath(evmDir, ranges[0]) + ".partial"
	require.NoError(t, os.WriteFile(stalePartial, []byte("stale"), 0o600))

	source := &failingLegacySource{}
	result, err := writeLegacyDumpRanges(
		context.Background(), source,
		legacyScanOptions{
			TempDir: t.TempDir(), FirstVersion: 1, LastVersion: 4,
			SortChunkSize: externalRecordMemoryBytes, MaxSortChunks: 2, MinFree: 1,
		},
		evmDir, ranges, zlib.BestSpeed, 1,
	)
	require.NoError(t, err)
	require.Zero(t, source.calls)
	require.False(t, result.Scanned)
	require.Equal(t, int64(2), result.ReusedFiles)
	require.Equal(t, int64(4), result.ReusedRecords)
	require.Zero(t, result.GeneratedFiles)
	require.NoFileExists(t, stalePartial)

	require.NoError(t, os.Remove(dumpRangePath(evmDir, ranges[1])))
	source = &failingLegacySource{}
	_, err = writeLegacyDumpRanges(
		context.Background(), source,
		legacyScanOptions{
			TempDir: t.TempDir(), FirstVersion: 1, LastVersion: 4,
			SortChunkSize: externalRecordMemoryBytes, MaxSortChunks: 2, MinFree: 1,
		},
		evmDir, ranges, zlib.BestSpeed, 1,
	)
	require.ErrorContains(t, err, "legacy source must not be scanned")
	require.Equal(t, 1, source.calls)
}

func TestWriteDumpRangeUsesExistingChangeSetFormat(t *testing.T) {
	key := append([]byte{0x02}, make([]byte, 52)...)
	tree := &fakeTraverser{
		versions: []int64{100, 101, 102},
		changes: map[int64]*iavl.ChangeSet{
			100: {Pairs: []*iavl.KVPair{{Key: key, Value: []byte{1}}}},
			101: {Pairs: []*iavl.KVPair{{Delete: true, Key: key}}},
		},
	}
	path := filepath.Join(t.TempDir(), "block-100.zz")
	require.NoError(t, writeDumpRange(context.Background(), path, tree, versionRange{First: 100, Last: 102}, zlib.BestSpeed))
	require.Equal(t, int64(100), tree.start)
	require.Equal(t, int64(102), tree.end)
	require.FileExists(t, path)
	require.NoFileExists(t, path+".partial")

	var versions []int64
	var changeSets []*iavl.ChangeSet
	manifest, err := scanZlibChangeSets(path, func(version int64, changeSet *iavl.ChangeSet) error {
		versions = append(versions, version)
		changeSets = append(changeSets, changeSet)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []int64{100, 101, 102}, versions)
	require.Equal(t, int64(3), manifest.Records)
	require.Equal(t, tree.changes[100], changeSets[0])
	require.Equal(t, tree.changes[101], changeSets[1])
	require.Empty(t, changeSets[2].Pairs)
}

func TestWriteDumpRangeTraversesRealIAVLUpdateDeleteAndEqualRoot(t *testing.T) {
	database := iavldb.NewMemDB()
	tree := iavl.NewMutableTree(database, 100, true, log.NewNopLogger())
	keyA := append([]byte{0x02}, make([]byte, 52)...)
	keyB := append([]byte{0x02}, make([]byte, 52)...)
	keyB[len(keyB)-1] = 1

	_, err := tree.Set(keyA, []byte{1})
	require.NoError(t, err)
	_, err = tree.Set(keyB, []byte{2})
	require.NoError(t, err)
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)
	require.Equal(t, int64(1), version)

	_, err = tree.Set(keyA, []byte{3})
	require.NoError(t, err)
	_, version, err = tree.SaveVersion()
	require.NoError(t, err)
	require.Equal(t, int64(2), version)

	_, removed, err := tree.Remove(keyB)
	require.NoError(t, err)
	require.True(t, removed)
	_, version, err = tree.SaveVersion()
	require.NoError(t, err)
	require.Equal(t, int64(3), version)

	_, version, err = tree.SaveVersion()
	require.NoError(t, err)
	require.Equal(t, int64(4), version)

	immutable := iavl.NewImmutableTree(database, 100, true, log.NewNopLogger())
	path := filepath.Join(t.TempDir(), "block-2.zz")
	require.NoError(t, writeDumpRange(context.Background(), path, immutable, versionRange{First: 2, Last: 4}, zlib.BestSpeed))

	changes := map[int64]*iavl.ChangeSet{}
	_, err = scanZlibChangeSets(path, func(version int64, changeSet *iavl.ChangeSet) error {
		changes[version] = changeSet
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []*iavl.KVPair{{Key: keyA, Value: []byte{3}}}, changes[2].Pairs)
	require.Equal(t, []*iavl.KVPair{{Delete: true, Key: keyB}}, changes[3].Pairs)
	require.Empty(t, changes[4].Pairs)
}

func TestWriteChangeSetMatchesGoldenEncoding(t *testing.T) {
	pairs := []*iavl.KVPair{
		{Key: []byte("key"), Value: []byte("value")},
		{Delete: true, Key: []byte("deleted")},
	}
	var compressedBuffer bytes.Buffer
	writer := zlib.NewWriter(&compressedBuffer)
	require.NoError(t, writeChangeSet(writer, 42, &iavl.ChangeSet{Pairs: pairs}))
	require.NoError(t, writer.Close())
	reader, err := zlib.NewReader(&compressedBuffer)
	require.NoError(t, err)
	actual, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, encodeChangeSet(t, 42, pairs...), actual)
}

func TestWriteDumpRangeRejectsIncompleteOrOutOfOrderTraversal(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		versions []int64
		message  string
	}{
		{name: "missing final", versions: []int64{10, 11}, message: "ended at version 11"},
		{name: "out of order", versions: []int64{10, 12}, message: "returned version 12, want 11"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "block-10.zz")
			err := writeDumpRange(context.Background(), path, &fakeTraverser{versions: testCase.versions}, versionRange{First: 10, Last: 12}, zlib.BestSpeed)
			require.ErrorContains(t, err, testCase.message)
			require.NoFileExists(t, path)
			require.FileExists(t, path+".partial")
		})
	}
}

func TestWriteDumpRangeRequiresStablePredecessorAndBoundaryRoots(t *testing.T) {
	t.Run("missing predecessor", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "block-10.zz")
		tree := &fakeTraverser{
			versions: []int64{10}, hashResults: map[int64][][]byte{9: {nil}},
		}
		err := writeDumpRange(context.Background(), path, tree, versionRange{First: 10, Last: 10}, zlib.BestSpeed)
		require.ErrorContains(t, err, "version 9 root before traversal")
		require.Zero(t, tree.start)
		require.NoFileExists(t, path)
	})

	t.Run("predecessor changed", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "block-10.zz")
		tree := &fakeTraverser{
			versions: []int64{10},
			hashResults: map[int64][][]byte{9: {
				bytes.Repeat([]byte{1}, legacyHashLength), bytes.Repeat([]byte{2}, legacyHashLength),
			}},
		}
		err := writeDumpRange(context.Background(), path, tree, versionRange{First: 10, Last: 10}, zlib.BestSpeed)
		require.ErrorContains(t, err, "version 9 root changed during traversal")
		require.NoFileExists(t, path)
	})
}

func TestWriteDumpRangePropagatesTraversalErrorAndCancellation(t *testing.T) {
	t.Run("traversal error", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "block-1.zz")
		boom := errors.New("boom")
		err := writeDumpRange(context.Background(), path, &fakeTraverser{err: boom}, versionRange{First: 1, Last: 1}, zlib.BestSpeed)
		require.ErrorIs(t, err, boom)
	})

	t.Run("canceled", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "block-1.zz")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := writeDumpRange(ctx, path, &fakeTraverser{versions: []int64{1}}, versionRange{First: 1, Last: 1}, zlib.BestSpeed)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestValidateDumpRangeFileRejectsWrongRangeAndCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "block-5.zz")
	tree := &fakeTraverser{versions: []int64{5, 6}}
	require.NoError(t, writeDumpRange(context.Background(), path, tree, versionRange{First: 5, Last: 6}, zlib.BestSpeed))
	require.NoError(t, validateDumpRangeFile(path, versionRange{First: 5, Last: 6}))
	require.Error(t, validateDumpRangeFile(path, versionRange{First: 5, Last: 7}))
	require.NoError(t, os.WriteFile(path, []byte("corrupt"), 0o600))
	require.Error(t, validateDumpRangeFile(path, versionRange{First: 5, Last: 6}))
}
