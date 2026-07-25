package main

import (
	"context"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	dtypes "github.com/evmos/ethermint/debank/types"
	"github.com/stretchr/testify/require"
)

func TestResolveProductionSegmentRange(t *testing.T) {
	segment, err := resolvePlanRange(planOptions{
		Segment: true, SegmentFirstHeight: 100, SegmentFinalHeight: 10_099,
	}, 20_000)
	require.NoError(t, err)
	require.Equal(t, int64(100), segment.FirstHeight)
	require.Equal(t, int64(10_099), segment.FinalHeight)
	require.Equal(t, segmentManifestSchema, segment.ManifestSchema)

	for _, test := range []struct {
		name    string
		options planOptions
	}{
		{
			name: "more than ten thousand blocks",
			options: planOptions{
				Segment: true, SegmentFirstHeight: 100, SegmentFinalHeight: 10_100,
			},
		},
		{
			name: "first height below block two",
			options: planOptions{
				Segment: true, SegmentFirstHeight: 1, SegmentFinalHeight: 100,
			},
		},
		{
			name: "final height above archive",
			options: planOptions{
				Segment: true, SegmentFirstHeight: 19_999, SegmentFinalHeight: 20_001,
			},
		},
		{
			name: "segment heights without segment mode",
			options: planOptions{
				SegmentFirstHeight: 100, SegmentFinalHeight: 200,
			},
		},
		{
			name: "pilot and production segment are mutually exclusive",
			options: planOptions{
				Pilot: true, PilotFirstHeight: 100, PilotFinalHeight: 200,
				Segment: true, SegmentFirstHeight: 100, SegmentFinalHeight: 200,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := resolvePlanRange(test.options, 20_000)
			require.Error(t, err)
		})
	}
}

func TestPlanCheckpointIdentityBindsInitialParentRoot(t *testing.T) {
	left := testPlanCheckpointManifest()
	left.Schema = segmentManifestSchema
	left.FirstHeight = 100
	left.FinalHeight = 109
	left.InitialParentRoot = common.BigToHash(big.NewInt(98))
	right := left
	right.InitialParentRoot = common.BigToHash(big.NewInt(99))

	require.False(t, samePlanIdentity(left, right))
	require.False(t, samePlanIdentity(right, left))
}

func TestPrepareProductionSegmentAdoptsSupersetDumpProducer(t *testing.T) {
	staging := filepath.Join(t.TempDir(), "dump.staging")
	var body []byte
	for version := int64(100); version <= 119; version++ {
		body = append(body, encodeChangeSet(t, version)...)
	}
	writeDumpFile(t, staging, "block-100.zz", zlibBody(t, body))
	identity := archiveIdentity{
		Home: "/archive", DatabaseIdentity: "db-old-producer", LatestVersion: 200, FinalCommitHash: "0x200",
	}
	producer := dumpContext{
		Schema: pilotDumpManifestSchema, FirstVersion: 100, LastVersion: 119,
		SnapshotID: "snap-old-producer", ArchiveIdentity: identity,
		CronosCommit: testCronosCommit, EthermintCommit: testEthermintCommit, IAVLCommit: testIAVLCommit,
		ImageDigest: testImageDigest, BuildTags: testBuildTags,
	}
	writeTestDumpSource(t, staging, producer)

	sealed, manifest, manifestHash, adopted, err := prepareSegmentDumpContext(
		context.Background(), staging, producer.SnapshotID, identity, 105, 114,
	)
	require.NoError(t, err)
	require.Equal(t, producer, adopted)
	require.Equal(t, int64(100), manifest.FirstVersion)
	require.Equal(t, int64(119), manifest.LastVersion)
	require.NotEmpty(t, manifestHash)
	require.DirExists(t, sealed)
	require.NoDirExists(t, staging)

	gotPath, gotManifest, gotHash, gotProducer, err := prepareSegmentDumpContext(
		context.Background(), staging, producer.SnapshotID, identity, 105, 114,
	)
	require.NoError(t, err)
	require.Equal(t, sealed, gotPath)
	require.Equal(t, manifest, gotManifest)
	require.Equal(t, manifestHash, gotHash)
	require.Equal(t, producer, gotProducer)

	badPath, badManifest, badHash, badProducer, err := prepareSegmentDumpContext(
		context.Background(), sealed, "different-snapshot", identity, 105, 114,
	)
	require.ErrorContains(t, err, "differs from plan snapshot")
	require.Empty(t, badPath)
	require.Equal(t, dumpManifest{}, badManifest)
	require.Empty(t, badHash)
	require.Equal(t, dumpContext{}, badProducer)

	badPath, badManifest, badHash, badProducer, err = prepareSegmentDumpContext(
		context.Background(), sealed, producer.SnapshotID, identity, 99, 114,
	)
	require.ErrorContains(t, err, "outside dump source range")
	require.Empty(t, badPath)
	require.Equal(t, dumpManifest{}, badManifest)
	require.Empty(t, badHash)
	require.Equal(t, dumpContext{}, badProducer)
}

func TestProductionSegmentManifestRequiresDumpProducer(t *testing.T) {
	manifestPath, manifest, _, _ := makeSealedSegmentPlan(t)
	manifest.DumpProducer = nil
	_, err := atomicJSON(manifestPath, manifest)
	require.NoError(t, err)

	_, _, err = loadPlanManifest(manifestPath)
	require.ErrorContains(t, err, "no dump producer identity")
}

func TestProductionSegmentPreflightUsesSealedInitialParentRoot(t *testing.T) {
	manifestPath, manifest, _, _ := makeSealedSegmentPlan(t)
	loaded, _, err := loadPlanManifest(manifestPath)
	require.NoError(t, err)
	require.Equal(t, manifest.InitialParentRoot, loaded.InitialParentRoot)
	require.NoError(t, validatePlanForWriteContext(context.Background(), filepath.Dir(manifestPath), loaded))

	loaded.InitialParentRoot = common.BigToHash(big.NewInt(999))
	err = validatePlanForWriteContext(context.Background(), filepath.Dir(manifestPath), loaded)
	require.ErrorContains(t, err, "root mismatch")
}

func TestProductionSegmentApplyVerifyRollbackLifecycle(t *testing.T) {
	manifestPath, manifest, record, oldObject := makeSealedSegmentPlan(t)
	_, manifestHash, err := loadPlanManifest(manifestPath)
	require.NoError(t, err)
	backup := writeBackupProof(t, manifestPath, manifestHash)
	store := &fakeObjectStore{objects: map[string]storedObject{record.Key: oldObject}}

	apply, err := runWriteMode(
		context.Background(), manifestPath, runtimeArtifactPath(manifestPath, "segment-apply.json"),
		backup, applyMode, store,
	)
	require.NoError(t, err)
	require.Equal(t, int64(1), apply.PUTs)
	require.True(t, store.targetVisible(record))

	verify, err := runVerify(
		context.Background(), manifestPath, runtimeArtifactPath(manifestPath, "segment-verify"), store,
	)
	require.NoError(t, err)
	require.Equal(t, manifest.Processed, verify.Processed)
	require.Equal(t, manifest.Changed, verify.VerifiedChanged)
	require.Equal(t, int64(1), verify.S3GETs)

	rollback, err := runWriteMode(
		context.Background(), manifestPath, runtimeArtifactPath(manifestPath, "segment-rollback.json"),
		"", rollbackMode, store,
	)
	require.NoError(t, err)
	require.Equal(t, int64(1), rollback.PUTs)
	require.True(t, store.oldVisible(record))
}

func makeSealedSegmentPlan(t *testing.T) (string, planManifest, packRecord, storedObject) {
	t.Helper()
	manifestPath, manifest, records, objects := makeSealedPlanRecords(t, 2)
	dir := filepath.Dir(manifestPath)
	for _, chunk := range manifest.Chunks {
		require.NoError(t, os.Remove(filepath.Join(dir, chunk.Pack)))
		require.NoError(t, os.Remove(filepath.Join(dir, chunk.Index)))
	}

	record := records[1]
	record.Ordinal = 1
	writer, err := newPackWriter(dir, 1<<30)
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	chunks, err := writer.Close()
	require.NoError(t, err)

	var diff dtypes.BlockStorageDiff
	require.NoError(t, rlp.DecodeBytes(record.NewBody, &diff))
	require.NotEqual(t, common.Hash{}, diff.ParentHash)
	root := rootRecord{Root: diff.Hash, Height: record.Height}
	writeRootIndex(t, filepath.Join(dir, manifest.HeightRootIndex), root)
	writeRootIndex(t, filepath.Join(dir, manifest.RootIndex), root)
	manifest.HeightRootIndexSHA256, _, err = hashFile(filepath.Join(dir, manifest.HeightRootIndex))
	require.NoError(t, err)
	manifest.RootIndexSHA256, _, err = hashFile(filepath.Join(dir, manifest.RootIndex))
	require.NoError(t, err)
	manifest.RootMultisetSHA256, _, err = rootMultisetSHA256(filepath.Join(dir, manifest.RootIndex))
	require.NoError(t, err)

	manifest.Schema = segmentManifestSchema
	manifest.FirstHeight = int64(record.Height)
	manifest.FinalHeight = int64(record.Height)
	manifest.InitialParentRoot = diff.ParentHash
	manifest.DumpProducer = &dumpContext{
		Schema: dumpManifestSchema, FirstVersion: 1, LastVersion: manifest.ArchiveIdentity.LatestVersion,
		SnapshotID: manifest.SnapshotID, ArchiveIdentity: manifest.ArchiveIdentity,
		CronosCommit: manifest.CronosCommit, EthermintCommit: manifest.EthermintCommit,
		IAVLCommit:  manifest.IAVLCommit,
		ImageDigest: manifest.ImageDigest, BuildTags: manifest.BuildTags,
	}
	manifest.Processed = 1
	manifest.Changed = 1
	manifest.ChangedCanonical = 0
	manifest.ChangedConflict = 0
	manifest.Unchanged = 0
	manifest.SkippedEqualRoot = 0
	manifest.SlotsAdded = record.SlotsAdded
	manifest.SlotsRemoved = record.SlotsRemoved
	manifest.SlotsChanged = record.SlotsChanged
	manifest.OldBytes = int64(len(record.OldBody))
	manifest.NewBytes = int64(len(record.NewBody))
	manifest.Chunks = chunks
	_, err = atomicJSON(manifestPath, manifest)
	require.NoError(t, err)

	return manifestPath, manifest, record, objects[record.Key]
}
