package main

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/cosmos/iavl"
	"github.com/stretchr/testify/require"
)

type directPlanEmission struct {
	version   int64
	changeSet *iavl.ChangeSet
}

type directPlanFakeSource struct {
	emissions        []directPlanEmission
	versionHashErrs  map[int64]error
	versionHashCalls []int64
	traversalCalls   [][2]int64
}

func (source *directPlanFakeSource) VersionHash(version int64) ([]byte, error) {
	source.versionHashCalls = append(source.versionHashCalls, version)
	if err := source.versionHashErrs[version]; err != nil {
		return nil, err
	}
	return bytes.Repeat([]byte{byte(version)}, legacyHashLength), nil
}

func (source *directPlanFakeSource) TraverseStateChanges(
	first, last int64,
	callback func(int64, *iavl.ChangeSet) error,
) error {
	source.traversalCalls = append(source.traversalCalls, [2]int64{first, last})
	for _, emission := range source.emissions {
		if err := callback(emission.version, emission.changeSet); err != nil {
			return err
		}
	}
	return nil
}

func TestIterateDirectStateChangesContext(t *testing.T) {
	changes := []*iavl.ChangeSet{{}, {}, {}}
	source := &directPlanFakeSource{emissions: []directPlanEmission{
		{version: 2, changeSet: changes[0]},
		{version: 3, changeSet: changes[1]},
		{version: 4, changeSet: changes[2]},
	}}
	var versions []int64
	var received []*iavl.ChangeSet

	err := iterateDirectStateChangesContext(
		context.Background(), source, 2, 4,
		func(version int64, changeSet *iavl.ChangeSet) error {
			versions = append(versions, version)
			received = append(received, changeSet)
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []int64{2, 3, 4}, versions)
	require.Equal(t, changes, received)
	require.Equal(t, [][2]int64{{2, 4}}, source.traversalCalls)
	require.Equal(t, []int64{1, 2, 4, 1, 2, 4}, source.versionHashCalls)
}

func TestIterateDirectStateChangesContextRejectsInvalidEmissions(t *testing.T) {
	changeSet := &iavl.ChangeSet{}
	testCases := []struct {
		name      string
		emissions []directPlanEmission
		wantError string
	}{
		{
			name: "missing final version",
			emissions: []directPlanEmission{
				{version: 2, changeSet: changeSet},
				{version: 3, changeSet: changeSet},
			},
			wantError: "direct traversal ended at version 3, want 4",
		},
		{
			name: "duplicate version",
			emissions: []directPlanEmission{
				{version: 2, changeSet: changeSet},
				{version: 2, changeSet: changeSet},
				{version: 3, changeSet: changeSet},
				{version: 4, changeSet: changeSet},
			},
			wantError: "direct traversal emitted version 2, want 3",
		},
		{
			name: "out of order version",
			emissions: []directPlanEmission{
				{version: 2, changeSet: changeSet},
				{version: 4, changeSet: changeSet},
				{version: 3, changeSet: changeSet},
			},
			wantError: "direct traversal emitted version 4, want 3",
		},
		{
			name: "nil changeset",
			emissions: []directPlanEmission{
				{version: 2},
			},
			wantError: "direct traversal emitted nil changeset at version 2",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			source := &directPlanFakeSource{emissions: testCase.emissions}
			err := iterateDirectStateChangesContext(
				context.Background(), source, 2, 4,
				func(int64, *iavl.ChangeSet) error { return nil },
			)
			require.ErrorContains(t, err, testCase.wantError)
		})
	}
}

func TestIterateDirectStateChangesContextRejectsMissingPredecessor(t *testing.T) {
	wantErr := errors.New("predecessor was pruned")
	source := &directPlanFakeSource{versionHashErrs: map[int64]error{1: wantErr}}

	err := iterateDirectStateChangesContext(
		context.Background(), source, 2, 2,
		func(int64, *iavl.ChangeSet) error { return nil },
	)
	require.ErrorIs(t, err, wantErr)
	require.ErrorContains(t, err, "resolve IAVL version 1 root before traversal")
	require.Empty(t, source.traversalCalls)
}

func TestIterateDirectStateChangesContextHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	source := &directPlanFakeSource{emissions: []directPlanEmission{
		{version: 2, changeSet: &iavl.ChangeSet{}},
	}}
	callbackCalls := 0

	err := iterateDirectStateChangesContext(ctx, source, 2, 2, func(int64, *iavl.ChangeSet) error {
		callbackCalls++
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, callbackCalls)
}

func TestEffectivePlanSourceModeAndIdentity(t *testing.T) {
	legacyDump := directPlanIdentityFixture()
	legacyDump.SourceMode = ""
	explicitDump := legacyDump
	explicitDump.SourceMode = planSourceDumpV1
	require.Equal(t, planSourceDumpV1, effectivePlanSourceMode(legacyDump))
	require.Equal(t, planSourceDumpV1, effectivePlanSourceMode(explicitDump))
	require.True(t, samePlanIdentity(legacyDump, explicitDump))

	direct := legacyDump
	direct.SourceMode = planSourceDirectV1
	direct.DumpPath = ""
	direct.DumpManifestHash = ""
	require.Equal(t, planSourceDirectV1, effectivePlanSourceMode(direct))
	require.True(t, samePlanIdentity(direct, direct))
	require.False(t, samePlanIdentity(legacyDump, direct))

	otherDirectArchive := direct
	otherDirectArchive.ArchiveIdentity.DatabaseIdentity = "other-db"
	require.False(t, samePlanIdentity(direct, otherDirectArchive))

	otherDump := explicitDump
	otherDump.DumpManifestHash = "other-dump-hash"
	require.False(t, samePlanIdentity(explicitDump, otherDump))
}

func TestLoadPlanManifestAcceptsDirectSource(t *testing.T) {
	manifestPath, manifest, _, _ := makeSealedPlanRecords(t, 1)
	manifest.SourceMode = planSourceDirectV1
	manifest.DumpPath = ""
	manifest.DumpManifestHash = ""
	_, err := atomicJSON(manifestPath, manifest)
	require.NoError(t, err)

	loaded, _, err := loadPlanManifest(manifestPath)
	require.NoError(t, err)
	require.Equal(t, planSourceDirectV1, effectivePlanSourceMode(loaded))
	require.Empty(t, loaded.DumpPath)
	require.Empty(t, loaded.DumpManifestHash)
}

func TestLoadPlanManifestRejectsDumpIdentityForDirectSource(t *testing.T) {
	testCases := []struct {
		name     string
		keepPath bool
		keepHash bool
	}{
		{name: "dump path", keepPath: true},
		{name: "dump manifest hash", keepHash: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			manifestPath, manifest, _, _ := makeSealedPlanRecords(t, 1)
			manifest.SourceMode = planSourceDirectV1
			if !testCase.keepPath {
				manifest.DumpPath = ""
			}
			if !testCase.keepHash {
				manifest.DumpManifestHash = ""
			}
			_, err := atomicJSON(manifestPath, manifest)
			require.NoError(t, err)

			_, _, err = loadPlanManifest(manifestPath)
			require.ErrorContains(t, err, "manifest direct IAVL identity is invalid")
		})
	}
}

func directPlanIdentityFixture() planManifest {
	return planManifest{
		Schema:           manifestSchema,
		Sealed:           false,
		Bucket:           defaultBucket,
		Prefix:           defaultPrefix,
		Region:           defaultRegion,
		FirstHeight:      2,
		FinalHeight:      10,
		CronosCommit:     "cronos",
		EthermintCommit:  "ethermint",
		IAVLCommit:       "iavl",
		SnapshotID:       "snapshot",
		ImageDigest:      "image",
		BuildTags:        "rocksdb,objstore",
		DumpPath:         "/dump.sealed",
		DumpManifestHash: "dump-hash",
		ArchiveIdentity: archiveIdentity{
			Home:             "/archive",
			DatabaseIdentity: "db",
			LatestVersion:    10,
			FinalCommitHash:  "commit",
		},
	}
}
