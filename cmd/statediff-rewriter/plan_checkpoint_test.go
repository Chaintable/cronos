package main

import (
	"bufio"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

func testPlanCheckpointManifest() planManifest {
	return planManifest{
		Schema: manifestSchema, Sealed: true, RunID: "run", CreatedAt: "2026-07-22T00:00:00Z",
		Bucket: defaultBucket, Prefix: defaultPrefix, Region: defaultRegion,
		FirstHeight: 2, FinalHeight: 10,
		CronosCommit: testCronosCommit, EthermintCommit: testEthermintCommit, IAVLCommit: testIAVLCommit,
		DumpPath: "/dump.sealed", DumpManifestHash: "dump-hash",
		ArchiveIdentity: archiveIdentity{
			Home: "/archive", DatabaseIdentity: "db-test", LatestVersion: 10, FinalCommitHash: "hash",
		},
		SnapshotID: "snap", ImageDigest: testImageDigest, BuildTags: testBuildTags,
	}
}

func TestPlanCheckpointFlushAndResumeTruncateFiles(t *testing.T) {
	_, _, records, _ := makeSealedPlanRecords(t, 3)
	dir := filepath.Join(t.TempDir(), "plan.staging")
	require.NoError(t, os.Mkdir(dir, 0o755))
	writer, err := newPackWriter(dir, 1<<30)
	require.NoError(t, err)
	rootPath := filepath.Join(dir, "roots.by-height.tmp")
	rootFile, err := os.OpenFile(rootPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	rootBuffer := bufio.NewWriter(rootFile)
	saver, err := newPlanCheckpointSaver(filepath.Join(dir, "plan.checkpoint.json"), rootBuffer, rootFile, writer, 1, 2, time.Hour)
	require.NoError(t, err)
	manifest := testPlanCheckpointManifest()

	for index := 0; index < 2; index++ {
		height := uint64(index + 2)
		require.NoError(t, writeRootRecord(rootBuffer, common.BytesToHash([]byte{1}), height))
		require.NoError(t, writer.Write(records[index]))
		manifest.Processed++
		manifest.Changed++
		manifest.SlotsAdded += records[index].SlotsAdded
		manifest.SlotsRemoved += records[index].SlotsRemoved
		manifest.SlotsChanged += records[index].SlotsChanged
		manifest.OldBytes += int64(len(records[index].OldBody))
		manifest.NewBytes += int64(len(records[index].NewBody))
		if records[index].NoncanonicalOld {
			manifest.ChangedCanonical++
		}
		if records[index].ConflictingOld {
			manifest.ChangedConflict++
		}
		require.NoError(t, saver.Advance(height, manifest, common.BytesToHash([]byte{byte(height)})))
	}
	checkpoint, found, err := loadPlanCheckpoint(filepath.Join(dir, "plan.checkpoint.json"), testPlanCheckpointManifest())
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(3), checkpoint.Frontier)
	require.Equal(t, int64(2), checkpoint.Manifest.Changed)
	wantPrefix := common.BytesToHash([]byte{3})
	require.Equal(t, hex.EncodeToString(wantPrefix[:]), checkpoint.PrefixSHA)
	require.ErrorContains(t, saver.Advance(3, manifest, wantPrefix), "does not advance")

	require.NoError(t, writeRootRecord(rootBuffer, common.BytesToHash([]byte{2}), 4))
	require.NoError(t, writer.Write(records[2]))
	require.NoError(t, rootBuffer.Flush())
	require.NoError(t, rootFile.Close())
	require.NoError(t, writer.Abort())

	restoredRoot, err := restorePlanRootIndex(dir, checkpoint.RootBytes)
	require.NoError(t, err)
	stat, err := os.Stat(restoredRoot)
	require.NoError(t, err)
	require.Equal(t, int64(2*rootRecordSize), stat.Size())
	resumed, err := resumePackWriter(dir, 1<<30, checkpoint.Pack, checkpoint.Manifest)
	require.NoError(t, err)
	state, err := resumed.Sync()
	require.NoError(t, err)
	require.Equal(t, int64(2), state.Records)
	require.NoError(t, resumed.Abort())
}

func TestPlanCheckpointInitializePersistsZeroFrontier(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "plan.staging")
	require.NoError(t, os.Mkdir(dir, 0o755))
	writer, err := newPackWriter(dir, 1<<30)
	require.NoError(t, err)
	rootFile, err := os.OpenFile(filepath.Join(dir, "roots.by-height.tmp"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	rootBuffer := bufio.NewWriter(rootFile)
	checkpointPath := filepath.Join(dir, "plan.checkpoint.json")
	saver, err := newPlanCheckpointSaver(checkpointPath, rootBuffer, rootFile, writer, 1, 2, time.Hour)
	require.NoError(t, err)
	manifest := testPlanCheckpointManifest()

	require.NoError(t, saver.Initialize(manifest))
	checkpoint, found, err := loadPlanCheckpoint(checkpointPath, manifest)
	require.NoError(t, err)
	require.True(t, found)
	require.Zero(t, checkpoint.Frontier)
	require.Zero(t, checkpoint.RootBytes)
	require.Zero(t, checkpoint.Manifest.Processed)
	require.NoError(t, rootFile.Close())
	require.NoError(t, writer.Abort())
}

func TestRestorePlanRootIndexRejectsSymlinkWithoutTruncatingTarget(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "plan.staging")
	require.NoError(t, os.Mkdir(dir, 0o755))
	target := filepath.Join(base, "outside")
	require.NoError(t, os.WriteFile(target, []byte("keep"), 0o600))
	require.NoError(t, os.Symlink(target, filepath.Join(dir, "roots.by-height.tmp")))

	_, err := restorePlanRootIndex(dir, 0)
	require.ErrorContains(t, err, "regular non-symlink")
	body, readErr := os.ReadFile(target)
	require.NoError(t, readErr)
	require.Equal(t, []byte("keep"), body)
}

func TestRestorePlanRootIndexRecoversNoReplaceLinkWindow(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "plan.staging")
	require.NoError(t, os.Mkdir(dir, 0o700))
	partial := filepath.Join(dir, "roots.by-height.tmp")
	final := filepath.Join(dir, "roots.by-height")
	require.NoError(t, os.WriteFile(partial, make([]byte, rootRecordSize), 0o600))
	require.NoError(t, os.Link(partial, final))
	restored, err := restorePlanRootIndex(dir, rootRecordSize)
	require.NoError(t, err)
	require.Equal(t, partial, restored)
	_, err = os.Lstat(final)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestRestorePlanRootIndexRejectsConflictingPartialAndFinal(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "plan.staging")
	require.NoError(t, os.Mkdir(dir, 0o700))
	partial := filepath.Join(dir, "roots.by-height.tmp")
	final := filepath.Join(dir, "roots.by-height")
	require.NoError(t, os.WriteFile(partial, []byte("partial"), 0o600))
	require.NoError(t, os.WriteFile(final, []byte("final"), 0o600))
	_, err := restorePlanRootIndex(dir, 0)
	require.ErrorContains(t, err, "conflicting inodes")
	partialBody, err := os.ReadFile(partial)
	require.NoError(t, err)
	finalBody, err := os.ReadFile(final)
	require.NoError(t, err)
	require.Equal(t, []byte("partial"), partialBody)
	require.Equal(t, []byte("final"), finalBody)
}

func TestLoadPlanCheckpointRejectsNumericMutation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "plan.checkpoint.json")
	manifest := testPlanCheckpointManifest()
	manifest.Processed = 1
	manifest.Unchanged = 1
	checkpoint := planCheckpoint{
		Schema: planCheckpointSchema, Frontier: 2, PrefixSHA: strings.Repeat("0", 64), RootBytes: rootRecordSize,
		Manifest: manifest,
	}
	checksum, err := planCheckpointChecksum(checkpoint)
	require.NoError(t, err)
	checkpoint.Checksum = checksum
	body, err := atomicJSON(path, checkpoint)
	require.NoError(t, err)

	mutated := strings.Replace(string(body), `"old_bytes": 0`, `"old_bytes": 1`, 1)
	require.NotEqual(t, string(body), mutated)
	require.NoError(t, os.WriteFile(path, []byte(mutated), 0o600))

	_, found, err := loadPlanCheckpoint(path, testPlanCheckpointManifest())
	require.ErrorContains(t, err, "checksum mismatch")
	require.False(t, found)
}

func TestLoadPlanCheckpointRejectsChecksummedInvalidState(t *testing.T) {
	validChunk := chunkManifest{
		Number: 0, Pack: "chunk-000000.pack", Index: "chunk-000000.idx",
		PackSHA256: strings.Repeat("1", 64), IndexSHA256: strings.Repeat("2", 64), PackSize: 1, Records: 1,
	}
	tests := []struct {
		name   string
		mutate func(*planCheckpoint)
		want   string
	}{
		{name: "negative counter", mutate: func(checkpoint *planCheckpoint) {
			checkpoint.Manifest.OldBytes = -1
		}, want: "counters are inconsistent"},
		{name: "chunk count mismatch", mutate: func(checkpoint *planCheckpoint) {
			checkpoint.Pack.Chunk = 1
		}, want: "pack state is invalid"},
		{name: "current offset missing", mutate: func(checkpoint *planCheckpoint) {
			checkpoint.Pack.PackOffset = 0
		}, want: "pack state is invalid"},
		{name: "invalid completed path", mutate: func(checkpoint *planCheckpoint) {
			checkpoint.Pack = packWriterState{Chunk: 1, Chunks: []chunkManifest{validChunk}}
			checkpoint.Pack.Chunks[0].Pack = "../chunk.pack"
		}, want: "completed chunk 0 is invalid"},
		{name: "zero-record completed chunk", mutate: func(checkpoint *planCheckpoint) {
			checkpoint.Pack = packWriterState{Chunk: 1, Chunks: []chunkManifest{validChunk}}
			checkpoint.Pack.Chunks[0].Records = 0
		}, want: "completed chunk 0 is invalid"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manifest := testPlanCheckpointManifest()
			manifest.Processed = 1
			manifest.Changed = 1
			checkpoint := planCheckpoint{
				Schema: planCheckpointSchema, Frontier: 2, PrefixSHA: strings.Repeat("1", 64), RootBytes: rootRecordSize,
				Manifest: manifest, Pack: packWriterState{PackOffset: 1, IndexOffset: 1, Records: 1},
			}
			test.mutate(&checkpoint)
			checksum, err := planCheckpointChecksum(checkpoint)
			require.NoError(t, err)
			checkpoint.Checksum = checksum
			path := filepath.Join(t.TempDir(), "plan.checkpoint.json")
			_, err = atomicJSON(path, checkpoint)
			require.NoError(t, err)
			_, found, err := loadPlanCheckpoint(path, testPlanCheckpointManifest())
			require.ErrorContains(t, err, test.want)
			require.False(t, found)
		})
	}
}
