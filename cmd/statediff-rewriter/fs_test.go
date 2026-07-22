package main

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenLockedPartialRejectsAliasesAndNonRegularFiles(t *testing.T) {
	for _, kind := range []string{"symlink", "hardlink", "fifo"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "artifact.partial")
			target := filepath.Join(dir, "target")
			require.NoError(t, os.WriteFile(target, []byte("keep"), 0o600))
			switch kind {
			case "symlink":
				require.NoError(t, os.Symlink(target, path))
			case "hardlink":
				require.NoError(t, os.Link(target, path))
			case "fifo":
				require.NoError(t, syscall.Mkfifo(path, 0o600))
			}

			file, err := openLockedPartial(path)
			require.ErrorContains(t, err, "regular non-symlink file with one link")
			require.Nil(t, file)
			body, err := os.ReadFile(target)
			require.NoError(t, err)
			require.Equal(t, []byte("keep"), body)
		})
	}
}

func TestOpenLockedPartialReplacesStaleRegularFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifact.partial")
	require.NoError(t, os.WriteFile(path, []byte("stale"), 0o600))
	file, err := openLockedPartial(path)
	require.NoError(t, err)
	_, err = file.WriteString("fresh")
	require.NoError(t, err)
	require.NoError(t, file.Close())
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("fresh"), body)
}

func TestAcquireStagingLocksIsExclusive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plan.staging")
	first, err := acquireStagingLocks(path)
	require.NoError(t, err)
	_, err = acquireStagingLocks(path)
	require.ErrorContains(t, err, "acquire staging lock")
	require.NoError(t, releaseStagingLocks(first))
	third, err := acquireStagingLocks(path)
	require.NoError(t, err)
	require.NoError(t, releaseStagingLocks(third))
}

func TestCommitFileNoReplacePreservesExistingTarget(t *testing.T) {
	dir := t.TempDir()
	partial := filepath.Join(dir, "artifact.partial")
	target := filepath.Join(dir, "artifact")
	require.NoError(t, os.WriteFile(partial, []byte("new"), 0o600))
	require.NoError(t, os.WriteFile(target, []byte("old"), 0o600))
	require.Error(t, commitFileNoReplace(partial, target))
	body, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, []byte("old"), body)
	body, err = os.ReadFile(partial)
	require.NoError(t, err)
	require.Equal(t, []byte("new"), body)
}

func TestCommitFileNoReplaceCommitsNewTarget(t *testing.T) {
	dir := t.TempDir()
	partial := filepath.Join(dir, "artifact.partial")
	target := filepath.Join(dir, "artifact")
	require.NoError(t, os.WriteFile(partial, []byte("new"), 0o600))
	require.NoError(t, commitFileNoReplace(partial, target))
	body, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, []byte("new"), body)
	_, err = os.Lstat(partial)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestRenameNoReplacePreservesConflictingDirectories(t *testing.T) {
	base := t.TempDir()
	source := filepath.Join(base, "plan.staging")
	target := filepath.Join(base, "plan.sealed")
	require.NoError(t, os.Mkdir(source, 0o700))
	require.NoError(t, os.Mkdir(target, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(source, "source"), []byte("source"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(target, "target"), []byte("target"), 0o600))
	require.Error(t, renameNoReplace(source, target))
	_, err := os.Stat(filepath.Join(source, "source"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(target, "target"))
	require.NoError(t, err)
}

func TestMutableStateLoadersRejectSymlinkAndHardlinkAliases(t *testing.T) {
	for _, kind := range []string{"symlink", "hardlink"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			checkpointTarget := filepath.Join(dir, "checkpoint-target.json")
			checkpointAlias := filepath.Join(dir, "checkpoint-alias.json")
			cp := checkpoint{RunID: "run", ManifestHash: "manifest", Mode: verifyMode}
			require.NoError(t, saveCheckpoint(checkpointTarget, cp))
			linkTestArtifact(t, kind, checkpointTarget, checkpointAlias)
			_, err := loadCheckpoint(checkpointAlias, cp.RunID, cp.ManifestHash, cp.Mode)
			require.Error(t, err)
			require.Error(t, saveCheckpoint(checkpointAlias, cp))

			journalTarget := filepath.Join(dir, "journal-target.json")
			journalAlias := filepath.Join(dir, "journal-alias.json")
			journal := testWriteJournal()
			require.NoError(t, saveWriteJournal(journalTarget, journal))
			linkTestArtifact(t, kind, journalTarget, journalAlias)
			_, _, err = loadWriteJournal(journalAlias, journal.RunID, journal.ManifestHash, journal.Operation)
			require.Error(t, err)
			require.Error(t, saveWriteJournal(journalAlias, journal))
		})
	}
}

func TestFilesystemSpaceRejectsOverflow(t *testing.T) {
	previous := statFilesystem
	statFilesystem = func(_ string, stat *syscall.Statfs_t) error {
		stat.Bsize = 2
		stat.Bavail = ^uint64(0)
		return nil
	}
	t.Cleanup(func() { statFilesystem = previous })
	_, _, err := filesystemSpace(t.TempDir())
	require.ErrorContains(t, err, "overflows uint64")
}

func TestAtomicJSONMinFreeFailurePreservesExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint.json")
	require.NoError(t, os.WriteFile(path, []byte("keep"), 0o600))
	_, err := atomicJSONWithMinFree(path, map[string]string{"new": "value"}, ^uint64(0))
	require.ErrorContains(t, err, "overflows uint64")
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), body)
}

func linkTestArtifact(t *testing.T, kind, target, alias string) {
	t.Helper()
	if kind == "symlink" {
		require.NoError(t, os.Symlink(target, alias))
		return
	}
	require.NoError(t, os.Link(target, alias))
}
