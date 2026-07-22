package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func manifestForResumableRecords(records []packRecord) planManifest {
	manifest := planManifest{
		FirstHeight: int64(records[0].Height), FinalHeight: int64(records[len(records)-1].Height),
		Changed: int64(len(records)),
	}
	for _, record := range records {
		manifest.SlotsAdded += record.SlotsAdded
		manifest.SlotsRemoved += record.SlotsRemoved
		manifest.SlotsChanged += record.SlotsChanged
		manifest.OldBytes += int64(len(record.OldBody))
		manifest.NewBytes += int64(len(record.NewBody))
		if record.NoncanonicalOld {
			manifest.ChangedCanonical++
		}
		if record.ConflictingOld {
			manifest.ChangedConflict++
		}
	}
	return manifest
}

func checkpointStatisticRecords(t *testing.T) []packRecord {
	t.Helper()
	_, _, records, _ := makeSealedPlanRecords(t, 2)
	records[0].SlotsAdded, records[0].SlotsRemoved, records[0].SlotsChanged = 2, 3, 5
	records[0].NoncanonicalOld = true
	records[1].SlotsAdded, records[1].SlotsRemoved, records[1].SlotsChanged = 7, 11, 13
	records[1].ConflictingOld = true
	return records
}

func TestTruncateCheckpointFileRejectsSymlink(t *testing.T) {
	base := t.TempDir()
	target := filepath.Join(base, "outside")
	require.NoError(t, os.WriteFile(target, []byte("keep"), 0o600))
	link := filepath.Join(base, "chunk.pack.tmp")
	require.NoError(t, os.Symlink(target, link))

	require.Error(t, truncateCheckpointFile(link, 0))
	body, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), body)
}

func TestPackWriterResumeTruncatesUncheckpointedRecords(t *testing.T) {
	_, _, records, _ := makeSealedPlanRecords(t, 10)
	dir := t.TempDir()
	writer, err := newPackWriter(dir, 1<<30)
	require.NoError(t, err)
	for _, record := range records[:5] {
		require.NoError(t, writer.Write(record))
	}
	state, err := writer.Sync()
	require.NoError(t, err)
	for _, record := range records[5:8] {
		require.NoError(t, writer.Write(record))
	}
	require.NoError(t, writer.Abort())

	checkpointManifest := manifestForResumableRecords(records[:5])
	checkpointManifest.FinalHeight = 11
	resumed, err := resumePackWriter(dir, 1<<30, state, checkpointManifest)
	require.NoError(t, err)
	for _, record := range records[5:] {
		require.NoError(t, resumed.Write(record))
	}
	chunks, err := resumed.Close()
	require.NoError(t, err)

	finalManifest := planManifest{FirstHeight: 2, FinalHeight: 11, Changed: 10, Chunks: chunks}
	var heights []uint64
	require.NoError(t, iteratePack(dir, finalManifest, func(record packRecord) error {
		heights = append(heights, record.Height)
		return nil
	}))
	require.Equal(t, []uint64{2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, heights)
}

func TestPackWriterResumeRestoresChunkFinalizedAfterCheckpoint(t *testing.T) {
	_, _, records, _ := makeSealedPlanRecords(t, 5)
	dir := t.TempDir()
	writer, err := newPackWriter(dir, 1<<30)
	require.NoError(t, err)
	for _, record := range records {
		require.NoError(t, writer.Write(record))
	}
	state, err := writer.Sync()
	require.NoError(t, err)
	_, err = writer.Close()
	require.NoError(t, err)

	manifest := manifestForResumableRecords(records)
	resumed, err := resumePackWriter(dir, 1<<30, state, manifest)
	require.NoError(t, err)
	chunks, err := resumed.Close()
	require.NoError(t, err)
	manifest.Chunks = chunks
	var count int
	require.NoError(t, iteratePack(dir, manifest, func(packRecord) error {
		count++
		return nil
	}))
	require.Equal(t, 5, count)
}

func TestPackWriterResumeRecoversNoReplaceLinkWindow(t *testing.T) {
	_, _, records, _ := makeSealedPlanRecords(t, 2)
	dir := t.TempDir()
	writer, err := newPackWriter(dir, 1<<30)
	require.NoError(t, err)
	for _, record := range records {
		require.NoError(t, writer.Write(record))
	}
	state, err := writer.Sync()
	require.NoError(t, err)
	require.NoError(t, writer.Abort())
	for _, suffix := range []string{"pack", "idx"} {
		partial := filepath.Join(dir, "chunk-000000."+suffix+".tmp")
		final := filepath.Join(dir, "chunk-000000."+suffix)
		require.NoError(t, os.Link(partial, final))
	}
	resumed, err := resumePackWriter(dir, 1<<30, state, manifestForResumableRecords(records))
	require.NoError(t, err)
	for _, suffix := range []string{"pack", "idx"} {
		_, err := os.Lstat(filepath.Join(dir, "chunk-000000."+suffix))
		require.ErrorIs(t, err, os.ErrNotExist)
	}
	require.NoError(t, resumed.Abort())
}

func TestPackWriterResumeRejectsPairConflictBeforeMutation(t *testing.T) {
	_, _, records, _ := makeSealedPlanRecords(t, 2)
	dir := t.TempDir()
	writer, err := newPackWriter(dir, 1<<30)
	require.NoError(t, err)
	for _, record := range records {
		require.NoError(t, writer.Write(record))
	}
	state, err := writer.Sync()
	require.NoError(t, err)
	require.NoError(t, writer.Abort())
	packPartial := filepath.Join(dir, "chunk-000000.pack.tmp")
	packFinal := filepath.Join(dir, "chunk-000000.pack")
	require.NoError(t, os.Link(packPartial, packFinal))
	indexFinal := filepath.Join(dir, "chunk-000000.idx")
	require.NoError(t, os.WriteFile(indexFinal, []byte("conflict"), 0o600))

	_, err = resumePackWriter(dir, 1<<30, state, manifestForResumableRecords(records))
	require.ErrorContains(t, err, "conflicting inodes")
	packPartialInfo, err := os.Stat(packPartial)
	require.NoError(t, err)
	packFinalInfo, err := os.Stat(packFinal)
	require.NoError(t, err)
	require.True(t, os.SameFile(packPartialInfo, packFinalInfo))
}

func TestResumePackWriterHonorsCanceledContextBeforeRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := resumePackWriterContext(ctx, t.TempDir(), 1<<30, packWriterState{}, planManifest{})
	require.ErrorIs(t, err, context.Canceled)
}

func TestPackWriterResumeValidatesCheckpointStatistics(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*planManifest)
	}{
		{name: "slots added", mutate: func(manifest *planManifest) { manifest.SlotsAdded++ }},
		{name: "slots removed", mutate: func(manifest *planManifest) { manifest.SlotsRemoved++ }},
		{name: "slots changed", mutate: func(manifest *planManifest) { manifest.SlotsChanged++ }},
		{name: "new bytes", mutate: func(manifest *planManifest) { manifest.NewBytes++ }},
		{name: "noncanonical", mutate: func(manifest *planManifest) { manifest.ChangedCanonical++ }},
		{name: "conflict", mutate: func(manifest *planManifest) { manifest.ChangedConflict++ }},
		{name: "old bytes below changed bodies", mutate: func(manifest *planManifest) { manifest.OldBytes-- }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			records := checkpointStatisticRecords(t)
			dir := t.TempDir()
			writer, err := newPackWriter(dir, 1<<30)
			require.NoError(t, err)
			for _, record := range records {
				require.NoError(t, writer.Write(record))
			}
			state, err := writer.Sync()
			require.NoError(t, err)
			require.NoError(t, writer.Abort())
			manifest := manifestForResumableRecords(records)
			test.mutate(&manifest)

			_, err = resumePackWriter(dir, 1<<30, state, manifest)
			require.Error(t, err)
		})
	}
}

func TestPackWriterResumeAllowsOldBytesFromUnchangedObjects(t *testing.T) {
	records := checkpointStatisticRecords(t)
	dir := t.TempDir()
	writer, err := newPackWriter(dir, 1<<30)
	require.NoError(t, err)
	for _, record := range records {
		require.NoError(t, writer.Write(record))
	}
	state, err := writer.Sync()
	require.NoError(t, err)
	require.NoError(t, writer.Abort())
	manifest := manifestForResumableRecords(records)
	manifest.OldBytes += 123

	resumed, err := resumePackWriter(dir, 1<<30, state, manifest)
	require.NoError(t, err)
	require.NoError(t, resumed.Abort())
}

func TestPackWriterResumeValidatesCompletedChunkStatistics(t *testing.T) {
	records := checkpointStatisticRecords(t)
	dir := t.TempDir()
	writer, err := newPackWriter(dir, 1<<30)
	require.NoError(t, err)
	for _, record := range records {
		require.NoError(t, writer.Write(record))
	}
	require.NoError(t, writer.closeChunk())
	state, err := writer.Sync()
	require.NoError(t, err)
	require.Zero(t, state.Records)
	require.NoError(t, writer.Abort())
	manifest := manifestForResumableRecords(records)
	manifest.NewBytes++

	_, err = resumePackWriter(dir, 1<<30, state, manifest)
	require.ErrorContains(t, err, "statistics differ")
}
