package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/cosmos/iavl"
	iavldb "github.com/cosmos/iavl/db"
	"github.com/stretchr/testify/require"
)

type fakeLegacyRecordSource struct {
	roots      []iavl.LegacyRootRecord
	nodes      []iavl.LegacyNodeRecord
	orphans    []iavl.LegacyOrphanRecord
	rootsErr   error
	nodesErr   error
	orphansErr error
}

type countingLegacyRecordSource struct {
	fakeLegacyRecordSource
	nodeInspections int
}

func (source *countingLegacyRecordSource) InspectNodes(callback func(iavl.LegacyNodeRecord) error) error {
	source.nodeInspections++
	return source.fakeLegacyRecordSource.InspectNodes(callback)
}

func (source fakeLegacyRecordSource) InspectRoots(callback func(iavl.LegacyRootRecord) error) error {
	for _, record := range source.roots {
		if err := callback(record); err != nil {
			return err
		}
	}
	return source.rootsErr
}

func (source fakeLegacyRecordSource) InspectNodes(callback func(iavl.LegacyNodeRecord) error) error {
	for _, record := range source.nodes {
		if err := callback(record); err != nil {
			return err
		}
	}
	return source.nodesErr
}

func (source fakeLegacyRecordSource) InspectOrphans(callback func(iavl.LegacyOrphanRecord) error) error {
	for _, record := range source.orphans {
		if err := callback(record); err != nil {
			return err
		}
	}
	return source.orphansErr
}

func legacyRoots(last int64) []iavl.LegacyRootRecord {
	records := make([]iavl.LegacyRootRecord, 0, last)
	for version := int64(1); version <= last; version++ {
		records = append(records, iavl.LegacyRootRecord{Version: version})
	}
	return records
}

func legacyRootsWithHash(last int64, hash []byte) []iavl.LegacyRootRecord {
	records := legacyRoots(last)
	for index := range records {
		records[index].Hash = append([]byte(nil), hash...)
	}
	return records
}

func legacyHash(marker byte) []byte { return bytes.Repeat([]byte{marker}, legacyHashLength) }

func legacyStorageNode(hashMarker byte, version int64, keyMarker byte, value ...byte) iavl.LegacyNodeRecord {
	return iavl.LegacyNodeRecord{
		Hash: legacyHash(hashMarker), Version: version, Height: 0, Size: 1,
		Key: legacyStorageKey(keyMarker), Value: value,
	}
}

func testLegacyScanOptions(t *testing.T, first, last int64) legacyScanOptions {
	t.Helper()
	return legacyScanOptions{
		TempDir: t.TempDir(), FirstVersion: first, LastVersion: last,
		SortChunkSize: 512, MaxSortChunks: 32,
	}
}

func TestLegacyLatestRootVersion(t *testing.T) {
	db := iavldb.NewMemDB()
	version, err := legacyLatestRootVersion(db)
	require.NoError(t, err)
	require.Zero(t, version)

	for _, current := range []int64{1, 10, 3} {
		key := make([]byte, 9)
		key[0] = 'r'
		binary.BigEndian.PutUint64(key[1:], uint64(current))
		require.NoError(t, db.Set(key, bytes.Repeat([]byte{byte(current)}, legacyHashLength)))
	}
	version, err = legacyLatestRootVersion(db)
	require.NoError(t, err)
	require.Equal(t, int64(10), version)
}

func TestScanLegacyChangeSetsReconstructsStorageUpdates(t *testing.T) {
	keyA, keyC := legacyStorageKey(1), legacyStorageKey(3)
	roots := legacyRoots(5)
	roots[1].Hash = legacyHash(1)
	roots[2].Hash = legacyHash(2)
	roots[4].Hash = legacyHash(3)
	source := fakeLegacyRecordSource{
		roots: roots,
		nodes: []iavl.LegacyNodeRecord{
			legacyStorageNode(1, 2, 1, 1),
			legacyStorageNode(2, 3, 1, 2),
			legacyStorageNode(3, 5, 3, 4),
		},
		// Raw legacy orphan order is by to-version, not node hash. The scanner
		// must external-sort these before joining them to hash-ordered nodes.
		orphans: []iavl.LegacyOrphanRecord{
			{Hash: legacyHash(1), FromVersion: 2, ToVersion: 2},
			{Hash: legacyHash(2), FromVersion: 3, ToVersion: 3},
		},
	}

	changes := make(map[int64]*iavl.ChangeSet)
	report, err := scanLegacyChangeSets(
		context.Background(), source, testLegacyScanOptions(t, 1, 5),
		func(version int64, changeSet *iavl.ChangeSet) error {
			changes[version] = changeSet
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, legacyScanReport{
		LegacyFirstVersion: 1, LegacyLastVersion: 5, Roots: 5,
		Nodes: 3, Leaves: 3, StorageLeaves: 3, Orphans: 2, StorageOrphans: 2,
		GraphNodes: 3, CoverageRecords: 6, Validation: legacyValidationGraph,
	}, report)
	require.Empty(t, changes[1].Pairs)
	require.Equal(t, []*iavl.KVPair{{Key: keyA, Value: []byte{1}}}, changes[2].Pairs)
	require.Equal(t, []*iavl.KVPair{{Key: keyA, Value: []byte{2}}}, changes[3].Pairs)
	require.Equal(t, []*iavl.KVPair{{Delete: true, Key: keyA}}, changes[4].Pairs)
	require.Equal(t, []*iavl.KVPair{{Key: keyC, Value: []byte{4}}}, changes[5].Pairs)
}

func TestScanLegacyChangeSetsTrustNodeSetSkipsGraphPass(t *testing.T) {
	keyA, keyC := legacyStorageKey(1), legacyStorageKey(3)
	roots := legacyRoots(5)
	roots[1].Hash = legacyHash(1)
	roots[2].Hash = legacyHash(2)
	roots[4].Hash = legacyHash(3)
	fixture := fakeLegacyRecordSource{
		roots: roots,
		nodes: []iavl.LegacyNodeRecord{
			legacyStorageNode(1, 2, 1, 1),
			legacyStorageNode(2, 3, 1, 2),
			legacyStorageNode(3, 5, 3, 4),
		},
		orphans: []iavl.LegacyOrphanRecord{
			{Hash: legacyHash(1), FromVersion: 2, ToVersion: 2},
			{Hash: legacyHash(2), FromVersion: 3, ToVersion: 3},
		},
	}
	run := func(t *testing.T, trust bool) (*countingLegacyRecordSource, legacyScanReport, map[int64]*iavl.ChangeSet) {
		t.Helper()
		source := &countingLegacyRecordSource{fakeLegacyRecordSource: fixture}
		options := testLegacyScanOptions(t, 1, 5)
		options.TrustNodeSet = trust
		changes := make(map[int64]*iavl.ChangeSet)
		report, err := scanLegacyChangeSets(
			context.Background(), source, options,
			func(version int64, changeSet *iavl.ChangeSet) error {
				changes[version] = changeSet
				return nil
			},
		)
		require.NoError(t, err)
		return source, report, changes
	}

	fullSource, fullReport, fullChanges := run(t, false)
	trustedSource, trustedReport, trustedChanges := run(t, true)

	require.Equal(t, 2, fullSource.nodeInspections)
	require.Equal(t, legacyValidationGraph, fullReport.Validation)
	require.NotZero(t, fullReport.GraphNodes)
	require.Equal(t, 1, trustedSource.nodeInspections)
	require.Equal(t, legacyValidationTrustedSet, trustedReport.Validation)
	require.Zero(t, trustedReport.GraphNodes)
	require.Zero(t, trustedReport.CoverageRecords)
	require.Equal(t, fullChanges, trustedChanges)
	require.Equal(t, []*iavl.KVPair{{Key: keyA, Value: []byte{1}}}, trustedChanges[2].Pairs)
	require.Equal(t, []*iavl.KVPair{{Key: keyA, Value: []byte{2}}}, trustedChanges[3].Pairs)
	require.Equal(t, []*iavl.KVPair{{Delete: true, Key: keyA}}, trustedChanges[4].Pairs)
	require.Equal(t, []*iavl.KVPair{{Key: keyC, Value: []byte{4}}}, trustedChanges[5].Pairs)
}

func TestScanLegacyChangeSetsTrustNodeSetHandlesOrphanedBranch(t *testing.T) {
	branch := iavl.LegacyNodeRecord{
		Hash: legacyHash(1), Version: 1, Height: 1, Size: 2, Key: []byte{0x03},
		LeftHash: legacyHash(3), RightHash: legacyHash(4),
	}
	storage := legacyStorageNode(2, 2, 5, 6)
	source := &countingLegacyRecordSource{fakeLegacyRecordSource: fakeLegacyRecordSource{
		roots: legacyRootsWithHash(2, storage.Hash),
		nodes: []iavl.LegacyNodeRecord{branch, storage},
		orphans: []iavl.LegacyOrphanRecord{
			{Hash: branch.Hash, FromVersion: 1, ToVersion: 1},
		},
	}}
	options := testLegacyScanOptions(t, 1, 2)
	options.TrustNodeSet = true
	changes := make(map[int64]*iavl.ChangeSet)

	report, err := scanLegacyChangeSets(
		context.Background(), source, options,
		func(version int64, changeSet *iavl.ChangeSet) error {
			changes[version] = changeSet
			return nil
		},
	)

	require.NoError(t, err)
	require.Equal(t, 1, source.nodeInspections)
	require.Equal(t, int64(1), report.Branches)
	require.Equal(t, int64(1), report.Leaves)
	require.Equal(t, int64(1), report.Orphans)
	require.Zero(t, report.StorageOrphans)
	require.Zero(t, report.GraphNodes)
	require.Zero(t, report.CoverageRecords)
	require.Empty(t, changes[1].Pairs)
	require.Equal(t, []*iavl.KVPair{{Key: legacyStorageKey(5), Value: []byte{6}}}, changes[2].Pairs)
}

func TestScanLegacyChangeSetsSupportsSubrangeAndEmptySorts(t *testing.T) {
	source := fakeLegacyRecordSource{
		roots: legacyRootsWithHash(4, legacyHash(1)),
		nodes: []iavl.LegacyNodeRecord{
			{Hash: legacyHash(1), Version: 1, Height: 0, Size: 1, Key: []byte{0x01}, Value: []byte{1}},
		},
	}
	var versions []int64
	_, err := scanLegacyChangeSets(
		context.Background(), source, testLegacyScanOptions(t, 2, 4),
		func(version int64, changeSet *iavl.ChangeSet) error {
			versions = append(versions, version)
			require.Empty(t, changeSet.Pairs)
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []int64{2, 3, 4}, versions)
}

func TestScanLegacyChangeSetsFailsClosed(t *testing.T) {
	validNode := legacyStorageNode(2, 2, 1, 1)
	tests := []struct {
		name   string
		source fakeLegacyRecordSource
		want   string
	}{
		{
			name: "root missing node",
			source: func() fakeLegacyRecordSource {
				roots := legacyRoots(2)
				roots[1].Hash = legacyHash(2)
				return fakeLegacyRecordSource{roots: roots}
			}(),
			want: "reachability references missing node",
		},
		{
			name:   "root gap",
			source: fakeLegacyRecordSource{roots: []iavl.LegacyRootRecord{{Version: 1}, {Version: 3}}},
			want:   "root version 3, want 2",
		},
		{
			name: "node order",
			source: fakeLegacyRecordSource{roots: legacyRoots(2), nodes: []iavl.LegacyNodeRecord{
				legacyStorageNode(3, 2, 1, 1), validNode,
			}},
			want: "not strictly increasing",
		},
		{
			name: "bad storage value",
			source: fakeLegacyRecordSource{roots: legacyRoots(2), nodes: []iavl.LegacyNodeRecord{
				legacyStorageNode(2, 2, 1),
			}},
			want: "value length 0",
		},
		{
			name: "orphan missing node",
			source: fakeLegacyRecordSource{
				roots:   legacyRoots(2),
				orphans: []iavl.LegacyOrphanRecord{{Hash: legacyHash(2), FromVersion: 1, ToVersion: 1}},
			},
			want: "references a missing node",
		},
		{
			name: "orphan creation mismatch",
			source: fakeLegacyRecordSource{
				roots: legacyRoots(3), nodes: []iavl.LegacyNodeRecord{validNode},
				orphans: []iavl.LegacyOrphanRecord{{Hash: legacyHash(2), FromVersion: 1, ToVersion: 2}},
			},
			want: "created at 2 but orphan lifetime starts at 1",
		},
		{
			name: "orphan reaches latest root",
			source: fakeLegacyRecordSource{
				roots: legacyRoots(2), nodes: []iavl.LegacyNodeRecord{validNode},
				orphans: []iavl.LegacyOrphanRecord{{Hash: legacyHash(2), FromVersion: 2, ToVersion: 2}},
			},
			want: "not before latest legacy root",
		},
		{
			name: "multiple orphan lifetimes",
			source: fakeLegacyRecordSource{
				roots: legacyRoots(3), nodes: []iavl.LegacyNodeRecord{validNode},
				orphans: []iavl.LegacyOrphanRecord{
					{Hash: legacyHash(2), FromVersion: 2, ToVersion: 2},
					{Hash: legacyHash(2), FromVersion: 2, ToVersion: 3},
				},
			},
			want: "multiple orphan lifetimes",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := scanLegacyChangeSets(
				context.Background(), test.source, testLegacyScanOptions(t, 1, 2),
				func(int64, *iavl.ChangeSet) error { return nil },
			)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestScanLegacyChangeSetsValidatesReachableBranch(t *testing.T) {
	left := iavl.LegacyNodeRecord{
		Hash: legacyHash(1), Version: 1, Height: 0, Size: 1, Key: []byte{0x01}, Value: []byte{1},
	}
	right := iavl.LegacyNodeRecord{
		Hash: legacyHash(2), Version: 1, Height: 0, Size: 1, Key: []byte{0x03}, Value: []byte{2},
	}
	branch := iavl.LegacyNodeRecord{
		Hash: legacyHash(3), Version: 1, Height: 1, Size: 2, Key: []byte{0x03},
		LeftHash: left.Hash, RightHash: right.Hash,
	}
	report, err := scanLegacyChangeSets(
		context.Background(),
		fakeLegacyRecordSource{roots: legacyRootsWithHash(1, branch.Hash), nodes: []iavl.LegacyNodeRecord{left, right, branch}},
		testLegacyScanOptions(t, 1, 1), func(int64, *iavl.ChangeSet) error { return nil },
	)
	require.NoError(t, err)
	require.Equal(t, int64(1), report.Branches)
	require.Equal(t, int64(3), report.GraphNodes)
	require.Equal(t, int64(6), report.CoverageRecords)
}

func TestScanLegacyChangeSetsRejectsDisconnectedAndIncompleteGraphs(t *testing.T) {
	leaf := iavl.LegacyNodeRecord{
		Hash: legacyHash(1), Version: 1, Height: 0, Size: 1, Key: []byte{0x01}, Value: []byte{1},
	}
	branch := iavl.LegacyNodeRecord{
		Hash: legacyHash(3), Version: 1, Height: 1, Size: 2, Key: []byte{0x03},
		LeftHash: leaf.Hash, RightHash: legacyHash(2),
	}
	tests := []struct {
		name   string
		source fakeLegacyRecordSource
		want   string
	}{
		{
			name:   "disconnected leaf",
			source: fakeLegacyRecordSource{roots: legacyRoots(1), nodes: []iavl.LegacyNodeRecord{leaf}},
			want:   "reachability ends at 0, want 1",
		},
		{
			name: "missing branch child",
			source: fakeLegacyRecordSource{
				roots: legacyRootsWithHash(1, branch.Hash), nodes: []iavl.LegacyNodeRecord{leaf, branch},
			},
			want: "reachability references missing node",
		},
		{
			name: "lifetime gap",
			source: fakeLegacyRecordSource{
				roots: []iavl.LegacyRootRecord{{Version: 1, Hash: leaf.Hash}, {Version: 2}},
				nodes: []iavl.LegacyNodeRecord{leaf},
			},
			want: "reachability ends at 1, want 2",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := scanLegacyChangeSets(
				context.Background(), test.source, testLegacyScanOptions(t, 1, int64(len(test.source.roots))),
				func(int64, *iavl.ChangeSet) error { return nil },
			)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestScanLegacyChangeSetsRejectsOverlappingParentReferences(t *testing.T) {
	left := iavl.LegacyNodeRecord{
		Hash: legacyHash(1), Version: 1, Height: 0, Size: 1, Key: []byte{0x01}, Value: []byte{1},
	}
	right := iavl.LegacyNodeRecord{
		Hash: legacyHash(2), Version: 1, Height: 0, Size: 1, Key: []byte{0x03}, Value: []byte{2},
	}
	root := iavl.LegacyNodeRecord{
		Hash: legacyHash(3), Version: 1, Height: 1, Size: 2, Key: []byte{0x03},
		LeftHash: left.Hash, RightHash: right.Hash,
	}
	duplicateParent := root
	duplicateParent.Hash = legacyHash(4)
	_, err := scanLegacyChangeSets(
		context.Background(),
		fakeLegacyRecordSource{
			roots: legacyRootsWithHash(1, root.Hash),
			nodes: []iavl.LegacyNodeRecord{left, right, root, duplicateParent},
		},
		testLegacyScanOptions(t, 1, 1), func(int64, *iavl.ChangeSet) error { return nil },
	)
	require.ErrorContains(t, err, "overlapping reachability")
}

func TestScanLegacyChangeSetsReturnsSourceContextAndEmitErrors(t *testing.T) {
	boom := errors.New("boom")
	_, err := scanLegacyChangeSets(
		context.Background(), fakeLegacyRecordSource{roots: legacyRoots(2), nodesErr: boom},
		testLegacyScanOptions(t, 1, 2), func(int64, *iavl.ChangeSet) error { return nil },
	)
	require.ErrorIs(t, err, boom)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = scanLegacyChangeSets(
		canceled, fakeLegacyRecordSource{roots: legacyRoots(2)},
		testLegacyScanOptions(t, 1, 2), func(int64, *iavl.ChangeSet) error { return nil },
	)
	require.ErrorIs(t, err, context.Canceled)

	_, err = scanLegacyChangeSets(
		context.Background(), fakeLegacyRecordSource{roots: legacyRoots(2)},
		testLegacyScanOptions(t, 1, 2), func(int64, *iavl.ChangeSet) error { return boom },
	)
	require.ErrorIs(t, err, boom)
}
