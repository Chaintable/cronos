package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWorldAuditCheckpointRestoresAndTruncatesAppendLogs(t *testing.T) {
	dir := t.TempDir()
	findingsPath := filepath.Join(dir, worldAuditFindingsName)
	codesPath := filepath.Join(dir, worldAuditCodesName)
	checkpointPath := filepath.Join(dir, worldAuditCheckpointName)
	findings, err := createAppendAuditLog(findingsPath)
	require.NoError(t, err)
	codes, err := createAppendAuditLog(codesPath)
	require.NoError(t, err)
	require.NoError(t, findings.Append([]byte("{\"height\":2}\n")))
	require.NoError(t, codes.Append(make([]byte, commonHashLength)))

	checkpoint := worldAuditCheckpoint{
		Schema:         worldAuditCheckpointSchema,
		Implementation: currentWorldAuditImplementationIdentity(),
		ArchiveIdentity: archiveIdentity{
			Home: "/archive", DatabaseIdentity: "db", LatestVersion: 3, FinalCommitHash: "0x01",
		},
		Bucket: "bucket", Prefix: "prefix", Region: "region", EVMDenom: "basecro",
		FirstHeight: 2, FinalHeight: 3, FindingsPath: findingsPath, CodesPath: codesPath,
		Summary: newWorldAuditSummary(),
	}
	saver := worldAuditCheckpointSaver{
		path: checkpointPath, findings: findings, codes: codes, checkpoint: checkpoint,
		every: 1, interval: time.Hour, lastPersist: time.Now(),
	}
	summary := newWorldAuditSummary()
	summary.GenesisAudited = true
	summary.Processed = 1
	summary.Frontier = 2
	require.NoError(t, saver.Advance(2, summary))
	require.NoError(t, findings.Append([]byte("uncommitted")))
	require.NoError(t, codes.Append(make([]byte, commonHashLength)))
	require.NoError(t, findings.Close())
	require.NoError(t, codes.Close())

	loaded, found, err := loadWorldAuditCheckpoint(checkpointPath, checkpoint)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(2), loaded.Summary.Frontier)
	require.Equal(t, int64(commonHashLength), loaded.CodesBytes)

	restoredFindings, err := restoreAppendAuditLog(findingsPath, loaded.FindingsBytes, loaded.FindingsSHA256)
	require.NoError(t, err)
	restoredCodes, err := restoreAppendAuditLog(codesPath, loaded.CodesBytes, loaded.CodesSHA256)
	require.NoError(t, err)
	require.NoError(t, restoredFindings.Close())
	require.NoError(t, restoredCodes.Close())
	info, err := os.Stat(findingsPath)
	require.NoError(t, err)
	require.Equal(t, loaded.FindingsBytes, info.Size())
	info, err = os.Stat(codesPath)
	require.NoError(t, err)
	require.Equal(t, loaded.CodesBytes, info.Size())
}

func TestWorldAuditCheckpointRejectsChecksumMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, worldAuditCheckpointName)
	checkpoint := worldAuditCheckpoint{
		Schema: worldAuditCheckpointSchema, Checksum: "bad",
		Implementation: currentWorldAuditImplementationIdentity(),
		ArchiveIdentity: archiveIdentity{
			Home: "/archive", DatabaseIdentity: "db", LatestVersion: 2, FinalCommitHash: "0x01",
		},
		Bucket: "bucket", Prefix: "prefix", Region: "region", EVMDenom: "basecro",
		FirstHeight: 2, FinalHeight: 2,
		FindingsPath: filepath.Join(dir, worldAuditFindingsName),
		CodesPath:    filepath.Join(dir, worldAuditCodesName),
		Summary:      newWorldAuditSummary(),
	}
	_, err := atomicJSON(path, checkpoint)
	require.NoError(t, err)
	_, _, err = loadWorldAuditCheckpoint(path, checkpoint)
	require.ErrorContains(t, err, "checksum mismatch")
}

func TestWorldAuditCheckpointRestoresBeforeGenesisAudit(t *testing.T) {
	dir := t.TempDir()
	findingsPath := filepath.Join(dir, worldAuditFindingsName)
	codesPath := filepath.Join(dir, worldAuditCodesName)
	checkpointPath := filepath.Join(dir, worldAuditCheckpointName)
	findings, err := createAppendAuditLog(findingsPath)
	require.NoError(t, err)
	codes, err := createAppendAuditLog(codesPath)
	require.NoError(t, err)
	checkpoint := worldAuditCheckpoint{
		Schema:         worldAuditCheckpointSchema,
		Implementation: currentWorldAuditImplementationIdentity(),
		ArchiveIdentity: archiveIdentity{
			Home: "/archive", DatabaseIdentity: "db", LatestVersion: 2, FinalCommitHash: "0x01",
		},
		Bucket: "bucket", Prefix: "prefix", Region: "region", EVMDenom: "basecro",
		FirstHeight: 2, FinalHeight: 2, FindingsPath: findingsPath, CodesPath: codesPath,
		Summary: newWorldAuditSummary(),
	}
	saver := worldAuditCheckpointSaver{
		path: checkpointPath, findings: findings, codes: codes, checkpoint: checkpoint,
		every: 1, interval: time.Hour, lastPersist: time.Now(),
	}
	require.NoError(t, saver.Flush(false))
	require.NoError(t, findings.Close())
	require.NoError(t, codes.Close())

	loaded, found, err := loadWorldAuditCheckpoint(checkpointPath, checkpoint)
	require.NoError(t, err)
	require.True(t, found)
	require.False(t, loaded.Summary.GenesisAudited)
	require.False(t, loaded.Completed)
}

func TestWorldAuditCheckpointRejectsDifferentImplementation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, worldAuditCheckpointName)
	checkpoint := worldAuditCheckpoint{
		Schema:         worldAuditCheckpointSchema,
		Implementation: currentWorldAuditImplementationIdentity(),
		ArchiveIdentity: archiveIdentity{
			Home: "/archive", DatabaseIdentity: "db", LatestVersion: 2, FinalCommitHash: "0x01",
		},
		Bucket: "bucket", Prefix: "prefix", Region: "region", EVMDenom: "basecro",
		FirstHeight: 2, FinalHeight: 2,
		FindingsPath: filepath.Join(dir, worldAuditFindingsName),
		CodesPath:    filepath.Join(dir, worldAuditCodesName),
		Summary:      newWorldAuditSummary(),
	}
	checksum, err := worldAuditCheckpointChecksum(checkpoint)
	require.NoError(t, err)
	checkpoint.Checksum = checksum
	_, err = atomicJSON(path, checkpoint)
	require.NoError(t, err)

	expected := checkpoint
	expected.Implementation.ScannerVersion = "different"
	_, _, err = loadWorldAuditCheckpoint(path, expected)
	require.ErrorContains(t, err, "does not match")
}

func TestValidateCompletedAuditLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log")
	body := []byte("complete")
	require.NoError(t, os.WriteFile(path, body, 0o600))
	digest := sha256Hex(body)
	require.NoError(t, validateCompletedAuditLog(path, int64(len(body)), digest))

	require.NoError(t, os.WriteFile(path, append(body, '!'), 0o600))
	require.ErrorContains(t, validateCompletedAuditLog(path, int64(len(body)), digest), "exactly")
	require.NoError(t, os.WriteFile(path, []byte("corrupt!"), 0o600))
	require.ErrorContains(t, validateCompletedAuditLog(path, int64(len(body)), digest), "hash")
}
