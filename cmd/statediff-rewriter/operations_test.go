package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/stretchr/testify/require"

	dtypes "github.com/evmos/ethermint/debank/types"
)

type fakeObjectStore struct {
	objects      map[string]storedObject
	puts         int
	uncertainPut bool
}

func (s *fakeObjectStore) Get(_ context.Context, _, key string) (storedObject, error) {
	object, ok := s.objects[key]
	if !ok {
		return storedObject{}, errors.New("not found")
	}
	object.Body = append([]byte(nil), object.Body...)
	return object, nil
}

func (s *fakeObjectStore) Put(_ context.Context, _, key string, body []byte, ifMatch string, headers objectHeaders) error {
	object, ok := s.objects[key]
	if !ok {
		return errors.New("not found")
	}
	if object.ETag != ifMatch {
		return errors.New("precondition failed")
	}
	s.puts++
	s.objects[key] = storedObject{Body: append([]byte(nil), body...), ETag: fmt.Sprintf("etag-%d", s.puts), Headers: headers}
	if s.uncertainPut {
		s.uncertainPut = false
		return errors.New("timeout after write")
	}
	return nil
}

func makeSealedPlan(t *testing.T) (string, planManifest, packRecord, storedObject) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "plan.sealed")
	require.NoError(t, os.Mkdir(dir, 0o755))
	root := common.BytesToHash([]byte{2})
	parent := common.BytesToHash([]byte{1})
	oldDiff := dtypes.BlockStorageDiff{Hash: root, ParentHash: parent, StorageDiff: []dtypes.AccountStorageDiff{testStorage(1, 1, 1)}}
	newDiff := oldDiff
	newDiff.StorageDiff = []dtypes.AccountStorageDiff{testStorage(1, 1, 2)}
	oldBody, err := rlp.EncodeToBytes(oldDiff)
	require.NoError(t, err)
	newBody, err := rlp.EncodeToBytes(newDiff)
	require.NoError(t, err)
	retained, err := retainedDigest(oldDiff)
	require.NoError(t, err)
	metadata, err := canonicalMetadata(map[string]string{"source": "test"})
	require.NoError(t, err)
	record := packRecord{
		Schema: packSchema, Ordinal: 1, Height: 2, Key: "25/4cc51d2e/root/stateDiff", OldETag: "etag-old",
		OldBody: oldBody, NewBody: newBody, OldSHA256: sha256Hash(oldBody), NewSHA256: sha256Hash(newBody),
		Headers: objectHeaders{Metadata: metadata, ContentType: "application/octet-stream"}, RetainedSHA256: retained,
	}
	writer, err := newPackWriter(dir, 128)
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	chunks, err := writer.Close()
	require.NoError(t, err)
	manifest := planManifest{
		Schema: manifestSchema, Sealed: true, RunID: "test-run", Bucket: defaultBucket, Prefix: defaultPrefix, Region: defaultRegion,
		Changed: 1, Chunks: chunks, RootIndex: "roots.sorted", SnapshotID: "snap-test", ImageDigest: "sha256:test",
		CronosCommit: "cronos", EthermintCommit: "ethermint",
	}
	rootFile := filepath.Join(dir, manifest.RootIndex)
	writeRootIndex(t, rootFile, rootRecord{Root: root, Height: 2})
	manifest.RootIndexSHA256, _, err = hashFile(rootFile)
	require.NoError(t, err)
	manifestPath := filepath.Join(dir, "manifest.v1.json")
	_, err = atomicJSON(manifestPath, manifest)
	require.NoError(t, err)
	return manifestPath, manifest, record, storedObject{Body: oldBody, ETag: "etag-old", Headers: record.Headers}
}

func copyPlanDirectory(t *testing.T, source string) string {
	t.Helper()
	target := filepath.Join(t.TempDir(), "backup.sealed")
	require.NoError(t, filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		destination := filepath.Join(target, relative)
		if info.IsDir() {
			return os.MkdirAll(destination, info.Mode())
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(destination, body, info.Mode())
	}))
	return target
}

func writeBackupProof(t *testing.T, path, manifestHash string) string {
	t.Helper()
	proofPath := filepath.Join(filepath.Dir(path), "backup.json")
	backup := copyPlanDirectory(t, filepath.Dir(path))
	body, err := json.Marshal(backupProof{ManifestSHA256: manifestHash, Kind: "ebs-snapshot-restore", Location: backup, Status: "completed"})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(proofPath, body, 0o644))
	return proofPath
}

func TestApplyRejectsBackupWithoutIndependentReadableCopy(t *testing.T) {
	manifestPath, _, record, oldObject := makeSealedPlan(t)
	_, manifestHash, err := loadPlanManifest(manifestPath)
	require.NoError(t, err)
	proofPath := filepath.Join(filepath.Dir(manifestPath), "backup.json")
	body, err := json.Marshal(backupProof{
		ManifestSHA256: manifestHash, Kind: "ebs-snapshot", Location: filepath.Dir(manifestPath), Status: "completed",
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(proofPath, body, 0o644))
	store := &fakeObjectStore{objects: map[string]storedObject{record.Key: oldObject}}
	_, err = runWriteMode(context.Background(), manifestPath, filepath.Join(filepath.Dir(manifestPath), "apply.json"), proofPath, "apply", store)
	require.ErrorContains(t, err, "active plan directory")
}

func TestApplyAndRollbackLifecycle(t *testing.T) {
	manifestPath, manifest, record, oldObject := makeSealedPlan(t)
	_, manifestHash, err := loadPlanManifest(manifestPath)
	require.NoError(t, err)
	backup := writeBackupProof(t, manifestPath, manifestHash)
	store := &fakeObjectStore{objects: map[string]storedObject{record.Key: oldObject}, uncertainPut: true}

	applyCheckpoint := filepath.Join(filepath.Dir(manifestPath), "apply.json")
	report, err := runWriteMode(context.Background(), manifestPath, applyCheckpoint, backup, "apply", store)
	require.NoError(t, err)
	require.Equal(t, int64(1), report.PlannedChanged)
	require.Equal(t, int64(1), report.AppliedChanged)
	require.Equal(t, 1, store.puts)
	require.Equal(t, record.NewSHA256, sha256Hash(store.objects[record.Key].Body))
	require.Equal(t, record.Headers, store.objects[record.Key].Headers)

	report, err = runWriteMode(context.Background(), manifestPath, filepath.Join(filepath.Dir(manifestPath), "apply-again.json"), backup, "apply", store)
	require.NoError(t, err)
	require.Equal(t, int64(1), report.AlreadyTarget)
	require.Equal(t, 1, store.puts)

	report, err = runWriteMode(context.Background(), manifestPath, filepath.Join(filepath.Dir(manifestPath), "rollback.json"), "", "rollback", store)
	require.NoError(t, err)
	require.Equal(t, manifest.Changed, report.AppliedChanged)
	require.Equal(t, record.OldSHA256, sha256Hash(store.objects[record.Key].Body))
}

func TestApplyRejectsConflictAndThirdContent(t *testing.T) {
	manifestPath, _, record, oldObject := makeSealedPlan(t)
	_, manifestHash, err := loadPlanManifest(manifestPath)
	require.NoError(t, err)
	backup := writeBackupProof(t, manifestPath, manifestHash)

	oldObject.ETag = "different-etag"
	store := &fakeObjectStore{objects: map[string]storedObject{record.Key: oldObject}}
	_, err = runWriteMode(context.Background(), manifestPath, filepath.Join(filepath.Dir(manifestPath), "conflict.json"), backup, "apply", store)
	require.ErrorContains(t, err, "changed ETag")

	store.objects[record.Key] = storedObject{Body: []byte("third"), ETag: "third"}
	_, err = runWriteMode(context.Background(), manifestPath, filepath.Join(filepath.Dir(manifestPath), "third.json"), backup, "apply", store)
	require.ErrorContains(t, err, "third-party content")
}

func TestCheckpointRejectsCorruptionAndBindingMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint.json")
	require.NoError(t, os.WriteFile(path, []byte("{"), 0o644))
	_, err := loadCheckpoint(path, "run", "hash", "apply")
	require.Error(t, err)

	require.NoError(t, saveCheckpoint(path, checkpoint{RunID: "other", ManifestHash: "hash", Mode: "apply"}))
	_, err = loadCheckpoint(path, "run", "hash", "apply")
	require.ErrorContains(t, err, "does not match")
}
