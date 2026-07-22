package main

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testCronosCommit    = "1111111111111111111111111111111111111111"
	testEthermintCommit = "222222222222"
	testIAVLCommit      = "333333333333"
	testBuildTags       = "rocksdb,grocksdb_clean_link"
)

var testImageDigest = "sha256:" + strings.Repeat("4", 64)

func useTestRuntimeBuildIdentity(t *testing.T) {
	t.Helper()
	previous := currentBuildIdentity
	currentBuildIdentity = func() buildIdentity {
		return buildIdentity{
			CronosCommit: testCronosCommit, EthermintCommit: testEthermintCommit,
			IAVLCommit: testIAVLCommit, BuildTags: testBuildTags, ImageDigest: testImageDigest,
		}
	}
	t.Cleanup(func() { currentBuildIdentity = previous })
}

func TestValidateBuildIdentity(t *testing.T) {
	require.NoError(t, validateBuildIdentity(
		testCronosCommit, testEthermintCommit, testIAVLCommit, testImageDigest, testBuildTags,
	))
	require.Error(t, validateBuildIdentity("unknown", testEthermintCommit, testIAVLCommit, testImageDigest, testBuildTags))
	require.Error(t, validateBuildIdentity(strings.Repeat("0", 40), testEthermintCommit, testIAVLCommit, testImageDigest, testBuildTags))
	require.Error(t, validateBuildIdentity(testCronosCommit, testEthermintCommit, testIAVLCommit, "sha256:test", testBuildTags))
	require.Error(t, validateBuildIdentity(testCronosCommit, testEthermintCommit, testIAVLCommit, testImageDigest, "rocksdb"))
}

func TestRequireRuntimeBuildIdentityRejectsEveryMismatch(t *testing.T) {
	manifest := planManifest{
		CronosCommit: testCronosCommit, EthermintCommit: testEthermintCommit, IAVLCommit: testIAVLCommit,
		BuildTags: testBuildTags, ImageDigest: testImageDigest,
	}
	base := buildIdentity{
		CronosCommit: testCronosCommit, EthermintCommit: testEthermintCommit, IAVLCommit: testIAVLCommit,
		BuildTags: testBuildTags, ImageDigest: testImageDigest,
	}
	tests := []struct {
		name   string
		mutate func(*buildIdentity)
	}{
		{name: "Cronos", mutate: func(identity *buildIdentity) { identity.CronosCommit = strings.Repeat("a", 40) }},
		{name: "Ethermint", mutate: func(identity *buildIdentity) { identity.EthermintCommit = strings.Repeat("a", 12) }},
		{name: "IAVL", mutate: func(identity *buildIdentity) { identity.IAVLCommit = strings.Repeat("a", 12) }},
		{name: "build tags", mutate: func(identity *buildIdentity) { identity.BuildTags = "rocksdb" }},
		{name: "image", mutate: func(identity *buildIdentity) { identity.ImageDigest = "sha256:" + strings.Repeat("a", 64) }},
		{name: "missing runtime image", mutate: func(identity *buildIdentity) { identity.ImageDigest = "" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			identity := base
			test.mutate(&identity)
			previous := currentBuildIdentity
			currentBuildIdentity = func() buildIdentity { return identity }
			defer func() { currentBuildIdentity = previous }()
			require.Error(t, requireRuntimeBuildIdentity(manifest))
		})
	}
}

func TestRuntimeIdentityMismatchPrecedesObjectAccess(t *testing.T) {
	manifestPath, _, record, object := makeSealedPlan(t)
	allowTestReadonlyFilesystems(t)
	previous := currentBuildIdentity
	currentBuildIdentity = func() buildIdentity {
		return buildIdentity{
			CronosCommit: testCronosCommit, EthermintCommit: testEthermintCommit,
			IAVLCommit: testIAVLCommit, BuildTags: testBuildTags,
			ImageDigest: "sha256:" + strings.Repeat("a", 64),
		}
	}
	t.Cleanup(func() { currentBuildIdentity = previous })
	store := &fakeObjectStore{objects: map[string]storedObject{record.Key: object}}
	_, err := runWriteMode(
		context.Background(), manifestPath, filepath.Join(t.TempDir(), "rollback.json"), "", "rollback", store,
	)
	require.ErrorContains(t, err, "runtime build identity differs")
	require.Empty(t, store.gets)
	require.Zero(t, store.puts)
}
