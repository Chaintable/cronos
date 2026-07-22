package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/cosmos/iavl"
	"github.com/stretchr/testify/require"
)

type versionMapTraverser map[int64]*iavl.ChangeSet

func (traverser versionMapTraverser) VersionHash(version int64) ([]byte, error) {
	return bytes.Repeat([]byte{byte(version)}, legacyHashLength), nil
}

func (traverser versionMapTraverser) TraverseStateChanges(
	start, end int64,
	callback func(int64, *iavl.ChangeSet) error,
) error {
	for version := start; version <= end; version++ {
		changeSet := traverser[version]
		if changeSet == nil {
			changeSet = &iavl.ChangeSet{}
		}
		if err := callback(version, changeSet); err != nil {
			return err
		}
	}
	return nil
}

func TestLegacySampleVersionsIncludesBoundaries(t *testing.T) {
	versions, err := legacySampleVersions(10, 20, 3)
	require.NoError(t, err)
	require.Equal(t, []int64{10, 15, 20}, versions)
	versions, err = legacySampleVersions(10, 12, 10)
	require.NoError(t, err)
	require.Equal(t, []int64{10, 11, 12}, versions)
}

func TestVerifyLegacyDumpSamplesComparesCanonicalStorageOnly(t *testing.T) {
	evmDir := t.TempDir()
	blockRange := versionRange{First: 1, Last: 5}
	storageKey := legacyStorageKey(1)
	nonStorageKey := []byte{0x01, 0x02}
	actual := versionMapTraverser{
		3: {Pairs: []*iavl.KVPair{{Key: storageKey, Value: []byte{3}}}},
		5: {Pairs: []*iavl.KVPair{{Delete: true, Key: storageKey}}},
	}
	reference := versionMapTraverser{
		1: {Pairs: []*iavl.KVPair{{Key: nonStorageKey, Value: []byte{1}}}},
		3: {Pairs: []*iavl.KVPair{
			{Key: nonStorageKey, Value: []byte{2}}, {Key: storageKey, Value: []byte{3}},
		}},
		5: {Pairs: []*iavl.KVPair{{Delete: true, Key: storageKey}}},
	}
	require.NoError(t, writeDumpRange(
		context.Background(), dumpRangePath(evmDir, blockRange), actual, blockRange, 1,
	))
	verified, err := verifyLegacyDumpSamples(
		context.Background(), evmDir, []versionRange{blockRange}, reference, 3,
	)
	require.NoError(t, err)
	require.Equal(t, 3, verified)

	reference[5] = &iavl.ChangeSet{}
	_, err = verifyLegacyDumpSamples(context.Background(), evmDir, []versionRange{blockRange}, reference, 3)
	require.ErrorContains(t, err, "mismatch at version 5")
}
