package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

func TestStatArtifactFilesystemDetectsWritableTestVolume(t *testing.T) {
	status, err := statArtifactFilesystem(t.TempDir())
	require.NoError(t, err)
	require.False(t, status.ReadOnly)
}

func TestLoadPlanManifestRejectsArtifactPathEscapeAndSymlinks(t *testing.T) {
	t.Run("path escape", func(t *testing.T) {
		manifestPath, manifest, _, _ := makeSealedPlanRecords(t, 1)
		manifest.RootIndex = "../roots.sorted"
		_, err := atomicJSON(manifestPath, manifest)
		require.NoError(t, err)
		_, _, err = loadPlanManifest(manifestPath)
		require.ErrorContains(t, err, "path must be a basename")
	})

	t.Run("artifact symlink", func(t *testing.T) {
		manifestPath, manifest, _, _ := makeSealedPlanRecords(t, 1)
		rootPath := filepath.Join(filepath.Dir(manifestPath), manifest.RootIndex)
		target := filepath.Join(t.TempDir(), "roots.sorted")
		body, err := os.ReadFile(rootPath)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(target, body, 0o600))
		require.NoError(t, os.Remove(rootPath))
		require.NoError(t, os.Symlink(target, rootPath))
		_, _, err = loadPlanManifest(manifestPath)
		require.ErrorContains(t, err, "regular non-symlink file")
	})

	t.Run("manifest symlink", func(t *testing.T) {
		manifestPath, _, _, _ := makeSealedPlanRecords(t, 1)
		target := filepath.Join(t.TempDir(), "manifest.v1.json")
		body, err := os.ReadFile(manifestPath)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(target, body, 0o600))
		require.NoError(t, os.Remove(manifestPath))
		require.NoError(t, os.Symlink(target, manifestPath))
		_, _, err = loadPlanManifest(manifestPath)
		require.ErrorContains(t, err, "regular non-symlink file")
	})
}

func TestLoadPlanManifestProvesRootIndexesHaveSameMultiset(t *testing.T) {
	manifestPath, manifest, _, _ := makeSealedPlanRecords(t, 2)
	rootPath := filepath.Join(filepath.Dir(manifestPath), manifest.RootIndex)
	writeRootIndex(t, rootPath,
		rootRecord{Root: common.BytesToHash([]byte{10}), Height: 2},
		rootRecord{Root: common.BytesToHash([]byte{11}), Height: 3},
	)
	var err error
	manifest.RootIndexSHA256, _, err = hashFile(rootPath)
	require.NoError(t, err)
	_, err = atomicJSON(manifestPath, manifest)
	require.NoError(t, err)
	_, _, err = loadPlanManifest(manifestPath)
	require.ErrorContains(t, err, "sealed multiset")
}

func TestLoadPlanManifestRejectsInvalidCountsAndChunkStructure(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*planManifest)
		want   string
	}{
		{
			name: "negative count",
			mutate: func(manifest *planManifest) {
				manifest.Changed, manifest.Unchanged, manifest.SkippedEqualRoot = 1, 1, -1
			},
			want: "block counts are inconsistent",
		},
		{
			name: "chunk number",
			mutate: func(manifest *planManifest) {
				manifest.Chunks[0].Number = 1
			},
			want: "pack chunk 0 metadata is invalid",
		},
		{
			name: "chunk record count",
			mutate: func(manifest *planManifest) {
				manifest.Chunks[0].Records++
			},
			want: "records exceed manifest changed count",
		},
		{
			name: "reused root artifact",
			mutate: func(manifest *planManifest) {
				manifest.RootIndex = manifest.HeightRootIndex
				manifest.RootIndexSHA256 = manifest.HeightRootIndexSHA256
			},
			want: "manifest artifact",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manifestPath, manifest, _, _ := makeSealedPlanRecords(t, 1)
			test.mutate(&manifest)
			_, err := atomicJSON(manifestPath, manifest)
			require.NoError(t, err)
			_, _, err = loadPlanManifest(manifestPath)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestSealedDumpRejectsEscapingAndSymlinkFiles(t *testing.T) {
	t.Run("path escape", func(t *testing.T) {
		_, manifest, _, _ := makeSealedPlanRecords(t, 1)
		body, err := os.ReadFile(filepath.Join(manifest.DumpPath, "dump-manifest.v1.json"))
		require.NoError(t, err)
		var dump dumpManifest
		require.NoError(t, decodeStrictJSON(body, &dump, "dump manifest"))
		dump.Files[0].Path = "../outside.zz"
		err = iterateSealedDump(manifest.DumpPath, dump, nil)
		require.ErrorContains(t, err, "evm/<basename>")
	})

	t.Run("file symlink", func(t *testing.T) {
		_, manifest, _, _ := makeSealedPlanRecords(t, 1)
		body, err := os.ReadFile(filepath.Join(manifest.DumpPath, "dump-manifest.v1.json"))
		require.NoError(t, err)
		var dump dumpManifest
		require.NoError(t, decodeStrictJSON(body, &dump, "dump manifest"))
		path := filepath.Join(manifest.DumpPath, dump.Files[0].Path)
		target := filepath.Join(t.TempDir(), "dump.zz")
		fileBody, err := os.ReadFile(path)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(target, fileBody, 0o600))
		require.NoError(t, os.Remove(path))
		require.NoError(t, os.Symlink(target, path))
		err = iterateSealedDump(manifest.DumpPath, dump, nil)
		require.ErrorContains(t, err, "regular non-symlink file")
	})
}

func TestSealedArtifactSetsRejectRogueFiles(t *testing.T) {
	t.Run("plan root", func(t *testing.T) {
		manifestPath, _, _, _ := makeSealedPlanRecords(t, 1)
		require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(manifestPath), "rogue.bin"), []byte("rogue"), 0o600))
		_, _, err := loadPlanManifest(manifestPath)
		require.ErrorContains(t, err, "unexpected sealed plan artifact")
	})

	t.Run("invalid plan checkpoint", func(t *testing.T) {
		manifestPath, _, _, _ := makeSealedPlanRecords(t, 1)
		require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(manifestPath), "plan.checkpoint.json"), []byte("{}"), 0o600))
		_, _, err := loadPlanManifest(manifestPath)
		require.ErrorContains(t, err, "plan checkpoint")
	})

	for _, location := range []string{"dump root", "dump evm"} {
		t.Run(location, func(t *testing.T) {
			_, plan, _, _ := makeSealedPlanRecords(t, 1)
			body, err := os.ReadFile(filepath.Join(plan.DumpPath, "dump-manifest.v1.json"))
			require.NoError(t, err)
			var dump dumpManifest
			require.NoError(t, decodeStrictJSON(body, &dump, "dump manifest"))
			rogueDir := plan.DumpPath
			if location == "dump evm" {
				rogueDir = filepath.Join(rogueDir, "evm")
			}
			require.NoError(t, os.WriteFile(filepath.Join(rogueDir, "rogue.bin"), []byte("rogue"), 0o600))
			err = iterateSealedDump(plan.DumpPath, dump, nil)
			require.ErrorContains(t, err, "artifact")
		})
	}
}

func TestWriteModesRequireReadOnlySealedPlan(t *testing.T) {
	manifestPath, _, record, object := makeSealedPlan(t)
	previous := inspectArtifactFilesystem
	inspectArtifactFilesystem = func(string) (filesystemStatus, error) {
		return filesystemStatus{Device: 1, ReadOnly: false}, nil
	}
	t.Cleanup(func() { inspectArtifactFilesystem = previous })
	store := &fakeObjectStore{objects: map[string]storedObject{record.Key: object}}
	_, err := runWriteMode(context.Background(), manifestPath, filepath.Join(t.TempDir(), "rollback.json"), "", "rollback", store)
	require.ErrorContains(t, err, "active sealed plan filesystem must be mounted read-only")
	_, err = runVerify(context.Background(), manifestPath, filepath.Join(t.TempDir(), "verify"), store)
	require.ErrorContains(t, err, "active sealed plan filesystem must be mounted read-only")
	require.Zero(t, store.puts)
}

func TestWriteModesRejectWritableOrForeignPlanArtifacts(t *testing.T) {
	tests := []struct {
		name   string
		status filesystemStatus
		want   string
	}{
		{name: "writable child mount", status: filesystemStatus{Device: 1, ReadOnly: false}, want: "filesystem must be mounted read-only"},
		{name: "foreign child mount", status: filesystemStatus{Device: 2, ReadOnly: true}, want: "must be on the plan filesystem"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manifestPath, manifest, record, object := makeSealedPlan(t)
			previous := inspectArtifactFilesystem
			inspectArtifactFilesystem = func(path string) (filesystemStatus, error) {
				if filepath.Base(path) == manifest.Chunks[0].Pack {
					return test.status, nil
				}
				return filesystemStatus{Device: 1, ReadOnly: true}, nil
			}
			t.Cleanup(func() { inspectArtifactFilesystem = previous })
			store := &fakeObjectStore{objects: map[string]storedObject{record.Key: object}}
			_, err := runWriteMode(
				context.Background(), manifestPath, filepath.Join(t.TempDir(), "rollback.json"), "", "rollback", store,
			)
			require.ErrorContains(t, err, test.want)
			require.Empty(t, store.gets)
			require.Zero(t, store.puts)
		})
	}
}

func TestVerifyRequiresReadOnlySealedDumpBeforeS3(t *testing.T) {
	manifestPath, manifest, record, object := makeSealedPlan(t)
	previous := inspectArtifactFilesystem
	inspectArtifactFilesystem = func(path string) (filesystemStatus, error) {
		readOnly := path != manifest.DumpPath && !strings.HasPrefix(path, manifest.DumpPath+string(filepath.Separator))
		return filesystemStatus{Device: 1, ReadOnly: readOnly}, nil
	}
	t.Cleanup(func() { inspectArtifactFilesystem = previous })
	store := &fakeObjectStore{objects: map[string]storedObject{record.Key: object}}
	_, err := runVerify(context.Background(), manifestPath, filepath.Join(t.TempDir(), "verify"), store)
	require.ErrorContains(t, err, "sealed dump filesystem must be mounted read-only")
	require.Empty(t, store.gets)
	require.Zero(t, store.puts)
}

func TestApplyRequiresIndependentReadOnlyBackupFilesystem(t *testing.T) {
	manifestPath, _, record, object := makeSealedPlan(t)
	_, manifestHash, err := loadPlanManifest(manifestPath)
	require.NoError(t, err)
	proofPath := writeBackupProof(t, manifestPath, manifestHash)
	store := &fakeObjectStore{objects: map[string]storedObject{record.Key: object}}

	t.Run("same filesystem device", func(t *testing.T) {
		previous := inspectArtifactFilesystem
		inspectArtifactFilesystem = func(string) (filesystemStatus, error) {
			return filesystemStatus{Device: 1, ReadOnly: true}, nil
		}
		defer func() { inspectArtifactFilesystem = previous }()
		_, err := runWriteMode(context.Background(), manifestPath, filepath.Join(t.TempDir(), "apply.json"), proofPath, "apply", store)
		require.ErrorContains(t, err, "same filesystem device")
	})

	t.Run("writable backup filesystem", func(t *testing.T) {
		previous := inspectArtifactFilesystem
		inspectArtifactFilesystem = func(path string) (filesystemStatus, error) {
			if filepath.Base(path) == "backup.sealed" {
				return filesystemStatus{Device: 2, ReadOnly: false}, nil
			}
			return filesystemStatus{Device: 1, ReadOnly: true}, nil
		}
		defer func() { inspectArtifactFilesystem = previous }()
		_, err := runWriteMode(context.Background(), manifestPath, filepath.Join(t.TempDir(), "apply.json"), proofPath, "apply", store)
		require.ErrorContains(t, err, "restored backup filesystem must be mounted read-only")
	})
}

func TestBackupProofRequiresSchemaSnapshotAndIndependentRestore(t *testing.T) {
	manifestPath, manifest, record, object := makeSealedPlan(t)
	_, manifestHash, err := loadPlanManifest(manifestPath)
	require.NoError(t, err)
	backup := copyPlanDirectory(t, filepath.Dir(manifestPath))
	proofPath := filepath.Join(t.TempDir(), "backup.json")
	proof := backupProof{
		Schema: backupProofSchema, ManifestSHA256: manifestHash, Kind: "ebs-snapshot-restore",
		Location: backup, Status: "completed",
	}
	body, err := json.Marshal(proof)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(proofPath, body, 0o600))
	store := &fakeObjectStore{objects: map[string]storedObject{record.Key: object}}
	_, err = runWriteMode(context.Background(), manifestPath, filepath.Join(t.TempDir(), "apply.json"), proofPath, "apply", store)
	require.ErrorContains(t, err, "backup proof is incomplete")

	proof.Independent = true
	proof.SnapshotID = manifest.SnapshotID
	body, err = json.Marshal(proof)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(proofPath, body, 0o600))
	_, err = runWriteMode(context.Background(), manifestPath, filepath.Join(t.TempDir(), "apply-source-snapshot.json"), proofPath, "apply", store)
	require.ErrorContains(t, err, "backup proof is incomplete")

	proof.SnapshotID = "snap-backup"
	backupLink := filepath.Join(t.TempDir(), "backup-link.sealed")
	require.NoError(t, os.Symlink(backup, backupLink))
	proof.Location = backupLink
	body, err = json.Marshal(proof)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(proofPath, body, 0o600))
	_, err = runWriteMode(context.Background(), manifestPath, filepath.Join(t.TempDir(), "apply-link.json"), proofPath, "apply", store)
	require.ErrorContains(t, err, "non-symlink directory")

	proof.Location = backup
	body, err = json.Marshal(map[string]any{
		"schema": proof.Schema, "manifest_sha256": proof.ManifestSHA256, "kind": proof.Kind,
		"snapshot_id": proof.SnapshotID, "location": proof.Location, "status": proof.Status,
		"independent_restore": proof.Independent, "unknown": true,
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(proofPath, body, 0o600))
	_, err = runWriteMode(context.Background(), manifestPath, filepath.Join(t.TempDir(), "apply-unknown.json"), proofPath, "apply", store)
	require.ErrorContains(t, err, "unknown field")
}
