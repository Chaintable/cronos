package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cosmos/iavl"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/evmos/ethermint/debank/statediff"
	dtypes "github.com/evmos/ethermint/debank/types"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

type fakeObjectStore struct {
	mu               sync.Mutex
	objects          map[string]storedObject
	puts             int
	uncertainPut     bool
	getDelay         time.Duration
	putDelay         time.Duration
	getGates         map[string]<-chan struct{}
	putGates         map[string]<-chan struct{}
	getErrors        map[string]error
	putErrors        map[string]error
	commitOnPutError map[string]bool
	gets             map[string]int
	activeGets       int
	maxActiveGets    int
}

func allowTestReadonlyFilesystems(t *testing.T) {
	t.Helper()
	previous := inspectArtifactFilesystem
	inspectArtifactFilesystem = func(path string) (filesystemStatus, error) {
		device := uint64(1)
		if strings.Contains(path, "backup.sealed") {
			device = 2
		}
		return filesystemStatus{Device: device, ReadOnly: true}, nil
	}
	t.Cleanup(func() { inspectArtifactFilesystem = previous })
}

func (s *fakeObjectStore) Get(ctx context.Context, _, key string) (storedObject, error) {
	s.mu.Lock()
	if s.gets == nil {
		s.gets = make(map[string]int)
	}
	s.gets[key]++
	s.activeGets++
	if s.activeGets > s.maxActiveGets {
		s.maxActiveGets = s.activeGets
	}
	delay, gate, injectedErr := s.getDelay, s.getGates[key], s.getErrors[key]
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.activeGets--
		s.mu.Unlock()
	}()
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return storedObject{}, ctx.Err()
		}
	}
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return storedObject{}, ctx.Err()
		}
	}
	if injectedErr != nil {
		return storedObject{}, injectedErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	object, ok := s.objects[key]
	if !ok {
		return storedObject{}, errors.New("not found")
	}
	return cloneStoredObject(object), nil
}

func (s *fakeObjectStore) getStats() (int, int, map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	counts := make(map[string]int, len(s.gets))
	var total int
	for key, count := range s.gets {
		counts[key] = count
		total += count
	}
	return total, s.maxActiveGets, counts
}

func (s *fakeObjectStore) Put(ctx context.Context, _, key string, body []byte, ifMatch string, headers objectHeaders) error {
	s.mu.Lock()
	delay, gate := s.putDelay, s.putGates[key]
	s.mu.Unlock()
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	object, ok := s.objects[key]
	if !ok {
		return errors.New("not found")
	}
	if object.ETag != ifMatch {
		return errors.New("precondition failed")
	}
	s.puts++
	injectedErr := s.putErrors[key]
	if injectedErr == nil || s.commitOnPutError[key] {
		s.objects[key] = storedObject{Body: append([]byte(nil), body...), ETag: fmt.Sprintf("etag-%d", s.puts), Headers: cloneObjectHeaders(headers)}
	}
	if injectedErr != nil {
		return injectedErr
	}
	if s.uncertainPut {
		s.uncertainPut = false
		return errors.New("timeout after write")
	}
	return nil
}

func cloneStoredObject(object storedObject) storedObject {
	object.Body = append([]byte(nil), object.Body...)
	object.Headers = cloneObjectHeaders(object.Headers)
	return object
}

func cloneObjectHeaders(headers objectHeaders) objectHeaders {
	headers.Metadata = append([]byte(nil), headers.Metadata...)
	return headers
}

func (s *fakeObjectStore) targetVisible(record packRecord) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	object, ok := s.objects[record.Key]
	return ok && sha256Hash(object.Body) == record.NewSHA256 && reflect.DeepEqual(object.Headers, record.Headers)
}

func (s *fakeObjectStore) oldVisible(record packRecord) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	object, ok := s.objects[record.Key]
	return ok && sha256Hash(object.Body) == record.OldSHA256 && reflect.DeepEqual(object.Headers, record.Headers)
}

func makeSealedPlan(t *testing.T) (string, planManifest, packRecord, storedObject) {
	t.Helper()
	manifestPath, manifest, records, objects := makeSealedPlanRecords(t, 1)
	return manifestPath, manifest, records[0], objects[records[0].Key]
}

func makeSealedPlanRecords(t *testing.T, count int) (string, planManifest, []packRecord, map[string]storedObject) {
	t.Helper()
	allowTestReadonlyFilesystems(t)
	useTestRuntimeBuildIdentity(t)
	base := t.TempDir()
	dir := filepath.Join(base, "plan.sealed")
	require.NoError(t, os.Mkdir(dir, 0o755))
	metadata, err := canonicalMetadata(map[string]string{"source": "test"})
	require.NoError(t, err)
	writer, err := newPackWriter(dir, 1<<30)
	require.NoError(t, err)
	records := make([]packRecord, 0, count)
	objects := make(map[string]storedObject, count)
	rootRecords := make([]rootRecord, 0, count)
	dumpBody := encodeChangeSet(t, 1)
	for index := 0; index < count; index++ {
		height := uint64(index + 2)
		root := common.BigToHash(new(big.Int).SetUint64(height))
		parent := common.Hash{}
		if height > 2 {
			parent = common.BigToHash(new(big.Int).SetUint64(height - 1))
		}
		key := make([]byte, 53)
		key[0], key[20], key[52] = 0x02, byte(index%251+1), 1
		oldCanonical, canonicalErr := statediff.CanonicalStorageDiff(&iavl.ChangeSet{Pairs: []*iavl.KVPair{{Key: key, Value: []byte{1}}}})
		require.NoError(t, canonicalErr)
		newCanonical, canonicalErr := statediff.CanonicalStorageDiff(&iavl.ChangeSet{Pairs: []*iavl.KVPair{{Key: key, Value: []byte{2}}}})
		require.NoError(t, canonicalErr)
		oldDiff := dtypes.BlockStorageDiff{Hash: root, ParentHash: parent, StorageDiff: oldCanonical}
		newDiff := oldDiff
		newDiff.StorageDiff = newCanonical
		oldBody, encodeErr := rlp.EncodeToBytes(oldDiff)
		require.NoError(t, encodeErr)
		newBody, encodeErr := rlp.EncodeToBytes(newDiff)
		require.NoError(t, encodeErr)
		retained, digestErr := retainedDigest(oldDiff)
		require.NoError(t, digestErr)
		objectKey := fmt.Sprintf("%s/%s/stateDiff", defaultPrefix, root.Hex())
		record := packRecord{
			Schema: packSchema, Ordinal: uint64(index + 1), Height: height, Key: objectKey, OldETag: fmt.Sprintf("etag-old-%d", height),
			OldBody: oldBody, NewBody: newBody, OldSHA256: sha256Hash(oldBody), NewSHA256: sha256Hash(newBody),
			Headers: objectHeaders{Metadata: metadata, ContentType: "application/octet-stream"}, RetainedSHA256: retained,
			SlotsChanged: 1,
		}
		require.NoError(t, writer.Write(record))
		records = append(records, record)
		objects[objectKey] = storedObject{Body: oldBody, ETag: record.OldETag, Headers: cloneObjectHeaders(record.Headers)}
		rootRecords = append(rootRecords, rootRecord{Root: root, Height: height})
		dumpBody = append(dumpBody, encodeChangeSet(t, int64(height), &iavl.KVPair{Key: key, Value: []byte{2}})...)
	}
	chunks, err := writer.Close()
	require.NoError(t, err)
	manifest := planManifest{
		Schema: manifestSchema, Sealed: true, RunID: "test-run", Bucket: defaultBucket, Prefix: defaultPrefix, Region: defaultRegion,
		FirstHeight: 2, FinalHeight: int64(count + 1), Processed: int64(count), Changed: int64(count), Chunks: chunks,
		CreatedAt:       time.Now().UTC().Format(time.RFC3339Nano),
		HeightRootIndex: "roots.by-height", RootIndex: "roots.sorted", SnapshotID: "snap-test", ImageDigest: testImageDigest,
		CronosCommit: testCronosCommit, EthermintCommit: testEthermintCommit, IAVLCommit: testIAVLCommit,
		ArchiveIdentity: archiveIdentity{
			Home: "/archive", DatabaseIdentity: "db-test",
			LatestVersion: int64(count + 1), FinalCommitHash: fmt.Sprintf("0x%02x", count+1),
		},
		BuildTags: testBuildTags,
	}
	for _, record := range records {
		manifest.OldBytes += int64(len(record.OldBody))
		manifest.NewBytes += int64(len(record.NewBody))
		manifest.SlotsChanged += record.SlotsChanged
	}
	dumpStaging := filepath.Join(base, "dump.staging")
	writeDumpFile(t, dumpStaging, "all.zz", zlibBody(t, dumpBody))
	dumpContext := dumpContext{
		Schema: dumpManifestSchema, FirstVersion: 1, LastVersion: manifest.FinalHeight,
		SnapshotID: manifest.SnapshotID, ArchiveIdentity: manifest.ArchiveIdentity,
		CronosCommit: manifest.CronosCommit, EthermintCommit: manifest.EthermintCommit,
		IAVLCommit:  manifest.IAVLCommit,
		ImageDigest: manifest.ImageDigest, BuildTags: manifest.BuildTags,
	}
	writeTestDumpSource(t, dumpStaging, dumpContext)
	manifest.DumpPath, _, manifest.DumpManifestHash, err = sealDump(dumpStaging, dumpContext)
	require.NoError(t, err)
	heightRootFile := filepath.Join(dir, manifest.HeightRootIndex)
	writeRootIndex(t, heightRootFile, rootRecords...)
	manifest.HeightRootIndexSHA256, _, err = hashFile(heightRootFile)
	require.NoError(t, err)
	rootFile := filepath.Join(dir, manifest.RootIndex)
	writeRootIndex(t, rootFile, rootRecords...)
	manifest.RootIndexSHA256, _, err = hashFile(rootFile)
	require.NoError(t, err)
	manifest.RootMultisetSHA256, _, err = rootMultisetSHA256(rootFile)
	require.NoError(t, err)
	manifestPath := filepath.Join(dir, "manifest.v1.json")
	_, err = atomicJSON(manifestPath, manifest)
	require.NoError(t, err)
	return manifestPath, manifest, records, objects
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
	proofPath := filepath.Join(t.TempDir(), "backup.json")
	backup := copyPlanDirectory(t, filepath.Dir(path))
	body, err := json.Marshal(backupProof{
		Schema: backupProofSchema, ManifestSHA256: manifestHash, Kind: "ebs-snapshot-restore",
		SnapshotID: "snap-backup", Location: backup, Status: "completed", Independent: true,
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(proofPath, body, 0o600))
	return proofPath
}

func runtimeArtifactPath(manifestPath, name string) string {
	return filepath.Join(filepath.Dir(filepath.Dir(manifestPath)), name)
}

func TestApplyRejectsBackupWithoutIndependentReadableCopy(t *testing.T) {
	manifestPath, _, record, oldObject := makeSealedPlan(t)
	_, manifestHash, err := loadPlanManifest(manifestPath)
	require.NoError(t, err)
	proofPath := runtimeArtifactPath(manifestPath, "backup.json")
	body, err := json.Marshal(backupProof{
		Schema: backupProofSchema, ManifestSHA256: manifestHash, Kind: "ebs-snapshot-restore", SnapshotID: "snap-backup",
		Location: filepath.Dir(manifestPath), Status: "completed", Independent: true,
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(proofPath, body, 0o600))
	store := &fakeObjectStore{objects: map[string]storedObject{record.Key: oldObject}}
	_, err = runWriteMode(context.Background(), manifestPath, runtimeArtifactPath(manifestPath, "apply.json"), proofPath, "apply", store)
	require.ErrorContains(t, err, "active plan directory")
}

func TestWriteModesRejectPilotPlan(t *testing.T) {
	manifestPath, manifest, _, _ := makeSealedPlan(t)
	manifest.Schema = pilotManifestSchema
	_, err := atomicJSON(manifestPath, manifest)
	require.NoError(t, err)

	store := &fakeObjectStore{objects: map[string]storedObject{}}
	_, err = runWriteMode(context.Background(), manifestPath, runtimeArtifactPath(manifestPath, "apply.json"), "", "apply", store)
	require.ErrorContains(t, err, "pilot plans support plan only")
	require.Zero(t, store.puts)

	_, err = runWriteMode(context.Background(), manifestPath, runtimeArtifactPath(manifestPath, "rollback.json"), "", "rollback", store)
	require.ErrorContains(t, err, "pilot plans support plan only")
	require.Zero(t, store.puts)

	_, err = runVerify(context.Background(), manifestPath, runtimeArtifactPath(manifestPath, "verify"), store)
	require.ErrorContains(t, err, "pilot plans support plan only")
	require.Zero(t, store.puts)
}

func TestApplyAndRollbackLifecycle(t *testing.T) {
	manifestPath, manifest, record, oldObject := makeSealedPlan(t)
	_, manifestHash, err := loadPlanManifest(manifestPath)
	require.NoError(t, err)
	backup := writeBackupProof(t, manifestPath, manifestHash)
	store := &fakeObjectStore{objects: map[string]storedObject{record.Key: oldObject}, uncertainPut: true}

	applyCheckpoint := runtimeArtifactPath(manifestPath, "apply.json")
	report, err := runWriteMode(context.Background(), manifestPath, applyCheckpoint, backup, "apply", store)
	require.NoError(t, err)
	require.Equal(t, int64(1), report.PlannedChanged)
	require.Equal(t, int64(1), report.AppliedChanged)
	require.Equal(t, 1, store.puts)
	require.Equal(t, record.NewSHA256, sha256Hash(store.objects[record.Key].Body))
	require.Equal(t, record.Headers, store.objects[record.Key].Headers)
	applyCP, err := loadCheckpoint(applyCheckpoint, manifest.RunID, manifestHash, "apply")
	require.NoError(t, err)
	require.Equal(t, uint64(1), applyCP.PUTAttempts)
	require.Equal(t, uint64(1), applyCP.UncertainPUTs)
	require.Zero(t, applyCP.AlreadyTarget)

	report, err = runWriteMode(context.Background(), manifestPath, runtimeArtifactPath(manifestPath, "apply-again.json"), backup, "apply", store)
	require.NoError(t, err)
	require.Equal(t, int64(1), report.AlreadyTarget)
	require.Equal(t, 1, store.puts)

	report, err = runWriteMode(context.Background(), manifestPath, runtimeArtifactPath(manifestPath, "rollback.json"), "", "rollback", store)
	require.NoError(t, err)
	require.Equal(t, manifest.Changed, report.AppliedChanged)
	require.Equal(t, record.OldSHA256, sha256Hash(store.objects[record.Key].Body))
}

func TestCompletedCheckpointsCannotMaskLaterStateChanges(t *testing.T) {
	t.Run("apply checkpoint after rollback", func(t *testing.T) {
		manifestPath, manifest, record, oldObject := makeSealedPlan(t)
		_, manifestHash, err := loadPlanManifest(manifestPath)
		require.NoError(t, err)
		backup := writeBackupProof(t, manifestPath, manifestHash)
		store := &fakeObjectStore{objects: map[string]storedObject{record.Key: oldObject}}
		applyCheckpoint := runtimeArtifactPath(manifestPath, "apply-complete.json")
		_, err = runWriteMode(context.Background(), manifestPath, applyCheckpoint, backup, "apply", store)
		require.NoError(t, err)
		_, err = runWriteMode(
			context.Background(), manifestPath, runtimeArtifactPath(manifestPath, "rollback-after-apply.json"), "", "rollback", store,
		)
		require.NoError(t, err)
		require.True(t, store.oldVisible(record))
		getsBefore := store.gets[record.Key]
		_, err = runWriteMode(context.Background(), manifestPath, applyCheckpoint, backup, "apply", store)
		require.ErrorContains(t, err, "checkpoint is already complete")
		require.Equal(t, getsBefore, store.gets[record.Key])
		checkpoint, err := loadCheckpoint(applyCheckpoint, manifest.RunID, manifestHash, "apply")
		require.NoError(t, err)
		require.Equal(t, uint64(manifest.Changed), checkpoint.Frontier)
	})

	t.Run("verify checkpoint after object mutation", func(t *testing.T) {
		manifestPath, _, record, oldObject := makeSealedPlan(t)
		store := &fakeObjectStore{objects: map[string]storedObject{
			record.Key: {Body: record.NewBody, ETag: "etag-new", Headers: cloneObjectHeaders(record.Headers)},
		}}
		checkpointDir := runtimeArtifactPath(manifestPath, "verify-complete")
		_, err := runVerify(context.Background(), manifestPath, checkpointDir, store)
		require.NoError(t, err)
		store.objects[record.Key] = oldObject
		getsBefore := store.gets[record.Key]
		_, err = runVerify(context.Background(), manifestPath, checkpointDir, store)
		require.ErrorContains(t, err, "checkpoint is already complete")
		require.Equal(t, getsBefore, store.gets[record.Key])
	})
}

func TestPartialWriteCheckpointRevalidatesPrefix(t *testing.T) {
	t.Run("rejects prefix reverted to source", func(t *testing.T) {
		manifestPath, manifest, records, objects := makeSealedPlanRecords(t, 3)
		_, manifestHash, err := loadPlanManifest(manifestPath)
		require.NoError(t, err)
		backup := writeBackupProof(t, manifestPath, manifestHash)
		checkpointPath := runtimeArtifactPath(manifestPath, "apply-partial.json")
		require.NoError(t, saveCheckpoint(checkpointPath, checkpoint{
			RunID: manifest.RunID, ManifestHash: manifestHash, Mode: "apply",
			Frontier: 1, Height: records[0].Height, AlreadyTarget: 1,
		}))
		store := &fakeObjectStore{objects: objects}

		report, err := runWriteModeWithOptions(
			context.Background(), manifestPath, checkpointPath, backup, "apply", defaultParallelOptions(), store,
		)
		require.ErrorContains(t, err, "checkpoint prefix")
		require.Zero(t, store.puts)
		require.Zero(t, report.VerifiedThisRun)
		require.True(t, store.oldVisible(records[0]))
		require.Equal(t, 1, store.gets[records[0].Key])
	})

	t.Run("revalidates target prefix before continuing", func(t *testing.T) {
		manifestPath, manifest, records, objects := makeSealedPlanRecords(t, 3)
		_, manifestHash, err := loadPlanManifest(manifestPath)
		require.NoError(t, err)
		backup := writeBackupProof(t, manifestPath, manifestHash)
		checkpointPath := runtimeArtifactPath(manifestPath, "apply-partial.json")
		require.NoError(t, saveCheckpoint(checkpointPath, checkpoint{
			RunID: manifest.RunID, ManifestHash: manifestHash, Mode: "apply",
			Frontier: 1, Height: records[0].Height, AlreadyTarget: 1,
		}))
		objects[records[0].Key] = storedObject{
			Body: records[0].NewBody, ETag: "etag-new", Headers: cloneObjectHeaders(records[0].Headers),
		}
		store := &fakeObjectStore{objects: objects}

		report, err := runWriteModeWithOptions(
			context.Background(), manifestPath, checkpointPath, backup, "apply", defaultParallelOptions(), store,
		)
		require.NoError(t, err)
		require.Equal(t, manifest.Changed, report.VerifiedThisRun)
		require.Equal(t, int64(1), report.AlreadyTarget)
		require.Equal(t, 2, store.puts)
		for _, record := range records {
			require.True(t, store.targetVisible(record), "height %d", record.Height)
		}
	})
}

func TestPartialVerifyCheckpointRevalidatesPrefix(t *testing.T) {
	t.Run("detects mutated prefix", func(t *testing.T) {
		manifestPath, manifest, records, objects := makeSealedPlanRecords(t, 3)
		for _, record := range records {
			objects[record.Key] = storedObject{
				Body: record.NewBody, ETag: "etag-new", Headers: cloneObjectHeaders(record.Headers),
			}
		}
		_, manifestHash, err := loadPlanManifest(manifestPath)
		require.NoError(t, err)
		checkpointDir := runtimeArtifactPath(manifestPath, "verify-partial")
		checkpointPath := filepath.Join(checkpointDir, "verify.json")
		require.NoError(t, saveCheckpoint(checkpointPath, checkpoint{
			RunID: manifest.RunID, ManifestHash: manifestHash, Mode: "verify",
			Frontier: records[1].Height, Height: records[1].Height, Changed: 2,
		}))
		objects[records[0].Key] = storedObject{
			Body: records[0].OldBody, ETag: records[0].OldETag, Headers: cloneObjectHeaders(records[0].Headers),
		}
		store := &fakeObjectStore{objects: objects}

		_, err = runVerify(context.Background(), manifestPath, checkpointDir, store)
		require.ErrorContains(t, err, "storage is not canonical")
		require.Equal(t, 1, store.gets[records[0].Key])
		persisted, err := loadCheckpoint(checkpointPath, manifest.RunID, manifestHash, "verify")
		require.NoError(t, err)
		require.Equal(t, records[1].Height, persisted.Frontier)
	})

	t.Run("counts the full current verification", func(t *testing.T) {
		manifestPath, manifest, records, objects := makeSealedPlanRecords(t, 3)
		for _, record := range records {
			objects[record.Key] = storedObject{
				Body: record.NewBody, ETag: "etag-new", Headers: cloneObjectHeaders(record.Headers),
			}
		}
		_, manifestHash, err := loadPlanManifest(manifestPath)
		require.NoError(t, err)
		checkpointDir := runtimeArtifactPath(manifestPath, "verify-partial")
		require.NoError(t, saveCheckpoint(filepath.Join(checkpointDir, "verify.json"), checkpoint{
			RunID: manifest.RunID, ManifestHash: manifestHash, Mode: "verify",
			Frontier: records[1].Height, Height: records[1].Height, Changed: 2,
		}))
		store := &fakeObjectStore{objects: objects}

		report, err := runVerify(context.Background(), manifestPath, checkpointDir, store)
		require.NoError(t, err)
		require.Equal(t, manifest.Processed, report.Processed)
		require.Equal(t, manifest.Changed, report.VerifiedChanged)
		require.Equal(t, manifest.Changed, report.S3GETs)
		for _, record := range records {
			require.Equal(t, 1, store.gets[record.Key])
		}
	})
}

func TestVerifyParallelUsesOneGETPerObjectAtTargetRate(t *testing.T) {
	manifestPath, manifest, records, objects := makeSealedPlanRecords(t, 1000)
	for _, record := range records {
		objects[record.Key] = storedObject{
			Body: record.NewBody, ETag: "etag-new", Headers: cloneObjectHeaders(record.Headers),
		}
	}
	store := &fakeObjectStore{objects: objects, getDelay: 20 * time.Millisecond}
	options := defaultParallelOptions()
	options.Concurrency = 64
	options.Window = 256
	report, err := runVerifyWithOptions(
		context.Background(), manifestPath, runtimeArtifactPath(manifestPath, "verify-rate"), options, store,
	)
	require.NoError(t, err)
	require.Equal(t, manifest.Processed, report.Processed)
	require.Equal(t, manifest.Changed, report.VerifiedChanged)
	require.Equal(t, int64(1000), report.S3GETs)
	t.Logf("verify rate %.2f objects/s", report.ObjectsPerSecond)
	require.GreaterOrEqual(t, report.ObjectsPerSecond, float64(1000))
	total, maximum, counts := store.getStats()
	require.Equal(t, 1000, total)
	require.GreaterOrEqual(t, maximum, 32)
	require.LessOrEqual(t, maximum, options.Concurrency)
	for _, record := range records {
		require.Equal(t, 1, counts[record.Key])
	}
}

func TestVerifyRejectsTargetMetadataDrift(t *testing.T) {
	manifestPath, _, records, objects := makeSealedPlanRecords(t, 1)
	record := records[0]
	objects[record.Key] = storedObject{Body: record.NewBody, ETag: "etag-new", Headers: cloneObjectHeaders(record.Headers)}
	object := objects[record.Key]
	object.Headers.ContentType = "text/plain"
	objects[record.Key] = object
	store := &fakeObjectStore{objects: objects}
	_, err := runVerify(context.Background(), manifestPath, runtimeArtifactPath(manifestPath, "verify-metadata"), store)
	require.ErrorContains(t, err, "object metadata changed")
}

func TestVerifyRejectsCheckpointChangedHeightMismatch(t *testing.T) {
	manifestPath, manifest, records, objects := makeSealedPlanRecords(t, 16)
	for _, record := range records {
		objects[record.Key] = storedObject{Body: record.NewBody, ETag: "etag-new", Headers: cloneObjectHeaders(record.Headers)}
	}
	_, manifestHash, err := loadPlanManifest(manifestPath)
	require.NoError(t, err)
	cpPath := filepath.Join(runtimeArtifactPath(manifestPath, "verify-invalid"), "verify.json")
	require.NoError(t, saveCheckpoint(cpPath, checkpoint{
		RunID: manifest.RunID, ManifestHash: manifestHash, Mode: "verify",
		Frontier: 10, Height: 10, Changed: 2,
	}))
	_, err = runVerify(context.Background(), manifestPath, filepath.Dir(cpPath), &fakeObjectStore{objects: objects})
	require.ErrorContains(t, err, "contains 9 changed records, not 2")
}

func TestApplyParallelizesAtTwentyMillisecondObjectLatency(t *testing.T) {
	manifestPath, _, _, objects := makeSealedPlanRecords(t, 1000)
	_, manifestHash, err := loadPlanManifest(manifestPath)
	require.NoError(t, err)
	backup := writeBackupProof(t, manifestPath, manifestHash)
	store := &fakeObjectStore{objects: objects, getDelay: 20 * time.Millisecond, putDelay: 20 * time.Millisecond}
	options := defaultParallelOptions()
	options.Concurrency = 96
	options.Window = 384
	report, err := runWriteModeWithOptions(
		context.Background(), manifestPath, runtimeArtifactPath(manifestPath, "apply-rate.json"), backup, "apply", options, store,
	)
	require.NoError(t, err)
	require.Equal(t, int64(1000), report.VerifiedThisRun)
	require.Equal(t, int64(1000), report.PUTs)
	t.Logf("apply rate %.2f objects/s", report.ObjectsPerSecond)
	total, maximum, _ := store.getStats()
	require.Equal(t, 2000, total)
	require.GreaterOrEqual(t, maximum, 32)
}

func TestConcurrentApplyCheckpointNeverCrossesFailureGap(t *testing.T) {
	manifestPath, manifest, records, objects := makeSealedPlanRecords(t, 160)
	_, manifestHash, err := loadPlanManifest(manifestPath)
	require.NoError(t, err)
	backup := writeBackupProof(t, manifestPath, manifestHash)
	gate := make(chan struct{})
	boom := errors.New("blocked ordinal failed")
	store := &fakeObjectStore{
		objects:   objects,
		putGates:  map[string]<-chan struct{}{records[4].Key: gate},
		putErrors: map[string]error{records[4].Key: boom},
	}
	checkpointPath := runtimeArtifactPath(manifestPath, "apply-concurrent.json")
	options := defaultParallelOptions()
	options.Concurrency = 8
	options.Window = 160
	options.CheckpointEvery = 160
	options.CheckpointInterval = time.Hour
	type result struct {
		report writeReport
		err    error
	}
	done := make(chan result, 1)
	go func() {
		report, runErr := runWriteModeWithOptions(context.Background(), manifestPath, checkpointPath, backup, "apply", options, store)
		done <- result{report: report, err: runErr}
	}()
	require.Eventually(t, func() bool { return store.targetVisible(records[9]) }, 5*time.Second, 10*time.Millisecond)
	close(gate)
	failed := <-done
	require.ErrorIs(t, failed.err, boom)

	cp, err := loadCheckpoint(checkpointPath, manifest.RunID, manifestHash, "apply")
	require.NoError(t, err)
	require.Zero(t, cp.Frontier)
	require.Zero(t, cp.Height)
	require.Zero(t, failed.report.AppliedChanged)
	journal, found, err := loadWriteJournal(writeJournalPath(checkpointPath), manifest.RunID, manifestHash, "apply")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, writeJournalIssued, journal.State)
	require.Equal(t, uint64(1), journal.Start)
	require.Equal(t, uint64(160), journal.End)
	require.Len(t, journal.Intents, 160)
	require.Equal(t, "apply", journal.Intents[4].Operation)
	require.Equal(t, records[4].Ordinal, journal.Intents[4].Ordinal)
	require.Equal(t, records[4].Key, journal.Intents[4].Key)
	require.Equal(t, records[4].OldETag, journal.Intents[4].IfMatch)
	require.Equal(t, hex.EncodeToString(records[4].OldSHA256[:]), journal.Intents[4].OldSHA256)
	require.Equal(t, hex.EncodeToString(records[4].NewSHA256[:]), journal.Intents[4].NewSHA256)
	require.NotEmpty(t, journal.Results)
	for _, result := range journal.Results {
		require.NotEmpty(t, result.PostPUTETag)
		require.Equal(t, hex.EncodeToString(records[result.Ordinal-1].NewSHA256[:]), result.ObservedSHA256)
	}

	store.mu.Lock()
	store.putGates = nil
	store.putErrors = nil
	store.mu.Unlock()
	options.CheckpointEvery = 8
	options.MaxInFlightBytes = maxObjectOperationBytes
	getsBefore, _, _ := store.getStats()
	store.mu.Lock()
	putsBefore := store.puts
	store.mu.Unlock()
	_, err = runWriteModeWithOptions(context.Background(), manifestPath, checkpointPath, backup, "apply", options, store)
	require.ErrorContains(t, err, "write journal batch needs")
	getsAfter, _, _ := store.getStats()
	require.Equal(t, getsBefore, getsAfter)
	store.mu.Lock()
	require.Equal(t, putsBefore, store.puts)
	store.mu.Unlock()

	options.MaxInFlightBytes = defaultParallelOptions().MaxInFlightBytes
	resumed, err := runWriteModeWithOptions(context.Background(), manifestPath, checkpointPath, backup, "apply", options, store)
	require.NoError(t, err)
	require.Equal(t, manifest.Changed, resumed.AppliedChanged)
	require.Positive(t, resumed.AlreadyTarget)
	for _, record := range records {
		require.True(t, store.targetVisible(record), "height %d", record.Height)
	}
	completed, err := loadCheckpoint(checkpointPath, manifest.RunID, manifestHash, "apply")
	require.NoError(t, err)
	require.Greater(t, completed.PUTAttempts, uint64(manifest.Changed))
	require.Equal(t, uint64(manifest.Changed), completed.ConfirmedPUTs)
}

func TestApplyRecoveryRejectsThirdContentBeforeRetryingIssuedBatch(t *testing.T) {
	manifestPath, manifest, records, objects := makeSealedPlanRecords(t, 8)
	_, manifestHash, err := loadPlanManifest(manifestPath)
	require.NoError(t, err)
	backup := writeBackupProof(t, manifestPath, manifestHash)
	gate := make(chan struct{})
	boom := errors.New("interrupted PUT")
	store := &fakeObjectStore{
		objects: objects, putGates: map[string]<-chan struct{}{records[0].Key: gate},
		putErrors: map[string]error{records[0].Key: boom},
	}
	checkpointPath := runtimeArtifactPath(manifestPath, "apply-third-recovery.json")
	options := defaultParallelOptions()
	options.Concurrency, options.Window, options.CheckpointEvery = 8, 8, 8
	done := make(chan error, 1)
	go func() {
		_, runErr := runWriteModeWithOptions(context.Background(), manifestPath, checkpointPath, backup, "apply", options, store)
		done <- runErr
	}()
	require.Eventually(t, func() bool { return store.targetVisible(records[3]) }, 5*time.Second, 10*time.Millisecond)
	close(gate)
	require.ErrorIs(t, <-done, boom)

	store.mu.Lock()
	store.putGates = nil
	store.putErrors = nil
	store.objects[records[0].Key] = storedObject{Body: []byte("third"), ETag: "third"}
	putsBeforeResume := store.puts
	store.mu.Unlock()
	_, err = runWriteModeWithOptions(context.Background(), manifestPath, checkpointPath, backup, "apply", options, store)
	require.ErrorContains(t, err, "third-party content")
	store.mu.Lock()
	require.Equal(t, putsBeforeResume, store.puts)
	store.mu.Unlock()
	cp, err := loadCheckpoint(checkpointPath, manifest.RunID, manifestHash, "apply")
	require.NoError(t, err)
	require.Zero(t, cp.Frontier)
	_, found, err := loadWriteJournal(writeJournalPath(checkpointPath), manifest.RunID, manifestHash, "apply")
	require.NoError(t, err)
	require.True(t, found)
}

func TestWriteRecoveryRejectsSourceAfterPersistedPUTOutcome(t *testing.T) {
	for _, outcome := range []string{"confirmed", "uncertain"} {
		t.Run(outcome, func(t *testing.T) {
			manifestPath, manifest, record, oldObject := makeSealedPlan(t)
			_, manifestHash, err := loadPlanManifest(manifestPath)
			require.NoError(t, err)
			backup := writeBackupProof(t, manifestPath, manifestHash)
			checkpointPath := runtimeArtifactPath(manifestPath, "apply-"+outcome+"-source.json")
			journal := newIssuedWriteJournal(manifest, manifestHash, applyMode, []preparedWrite{{
				record: record, needsPUT: true, ifMatch: record.OldETag,
			}})
			journal.Intents[0].PUTAttempts = 1
			if outcome == "confirmed" {
				journal.Intents[0].ConfirmedPUTs = 1
			} else {
				journal.Intents[0].UncertainPUTs = 1
			}
			require.NoError(t, saveWriteJournal(writeJournalPath(checkpointPath), journal))

			store := &fakeObjectStore{objects: map[string]storedObject{record.Key: oldObject}}
			_, err = runWriteMode(context.Background(), manifestPath, checkpointPath, backup, applyMode, store)
			require.ErrorContains(t, err, "persisted PUT outcome")
			require.Zero(t, store.puts)
		})
	}
}

func TestWriteModeRejectsConcurrentWorkDirectoryLockBeforeS3(t *testing.T) {
	manifestPath, _, record, object := makeSealedPlan(t)
	workDir := t.TempDir()
	locks, err := acquireStagingLocks(filepath.Join(workDir, "statediff-rewriter-write"))
	require.NoError(t, err)
	defer func() { require.NoError(t, releaseStagingLocks(locks)) }()
	store := &fakeObjectStore{objects: map[string]storedObject{record.Key: object}}
	_, err = runWriteMode(
		context.Background(), manifestPath, filepath.Join(workDir, "rollback.json"), "", rollbackMode, store,
	)
	require.ErrorContains(t, err, "acquire staging lock")
	total, _, _ := store.getStats()
	require.Zero(t, total)
	require.Zero(t, store.puts)
}

func TestWriteModeRejectsObservedJournalBehindCheckpoint(t *testing.T) {
	manifestPath, manifest, records, objects := makeSealedPlanRecords(t, 3)
	_, manifestHash, err := loadPlanManifest(manifestPath)
	require.NoError(t, err)
	checkpointPath := filepath.Join(t.TempDir(), "rollback.json")
	require.NoError(t, saveCheckpoint(checkpointPath, checkpoint{
		RunID: manifest.RunID, ManifestHash: manifestHash, Mode: "rollback",
		Frontier: 3, Height: records[2].Height, AlreadyTarget: 3,
	}))
	journal := newIssuedWriteJournal(manifest, manifestHash, "rollback", []preparedWrite{{
		record: records[0], needsPUT: true, ifMatch: "etag-new",
	}})
	journal.State = writeJournalObserved
	journal.Intents[0].PUTAttempts = 1
	journal.Intents[0].ConfirmedPUTs = 1
	journal.Results = []writeObservedResult{{
		Ordinal: records[0].Ordinal, ObservedSHA256: hex.EncodeToString(records[0].OldSHA256[:]),
		PostPUTETag: "etag-old", Outcome: "confirmed",
	}}
	require.NoError(t, saveWriteJournal(writeJournalPath(checkpointPath), journal))

	_, err = runWriteModeWithOptions(
		context.Background(), manifestPath, checkpointPath, "", "rollback", defaultParallelOptions(),
		&fakeObjectStore{objects: objects},
	)
	require.ErrorContains(t, err, "behind checkpoint")
}

func TestConcurrentRollbackRecoversIssuedBatchSymmetrically(t *testing.T) {
	manifestPath, manifest, records, objects := makeSealedPlanRecords(t, 16)
	_, manifestHash, err := loadPlanManifest(manifestPath)
	require.NoError(t, err)
	backup := writeBackupProof(t, manifestPath, manifestHash)
	store := &fakeObjectStore{objects: objects}
	_, err = runWriteMode(
		context.Background(), manifestPath, runtimeArtifactPath(manifestPath, "apply-before-rollback.json"), backup, "apply", store,
	)
	require.NoError(t, err)

	gate := make(chan struct{})
	boom := errors.New("interrupted rollback PUT")
	store.mu.Lock()
	store.putGates = map[string]<-chan struct{}{records[4].Key: gate}
	store.putErrors = map[string]error{records[4].Key: boom}
	store.mu.Unlock()
	checkpointPath := runtimeArtifactPath(manifestPath, "rollback-concurrent.json")
	options := defaultParallelOptions()
	options.Concurrency, options.Window, options.CheckpointEvery = 8, 16, 16
	done := make(chan error, 1)
	go func() {
		_, runErr := runWriteModeWithOptions(context.Background(), manifestPath, checkpointPath, "", "rollback", options, store)
		done <- runErr
	}()
	require.Eventually(t, func() bool { return store.oldVisible(records[9]) }, 5*time.Second, 10*time.Millisecond)
	close(gate)
	require.ErrorIs(t, <-done, boom)

	cp, err := loadCheckpoint(checkpointPath, manifest.RunID, manifestHash, "rollback")
	require.NoError(t, err)
	require.Zero(t, cp.Frontier)
	journal, found, err := loadWriteJournal(writeJournalPath(checkpointPath), manifest.RunID, manifestHash, "rollback")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, writeJournalIssued, journal.State)
	require.Equal(t, "rollback", journal.Intents[4].Operation)
	require.NotEqual(t, records[4].OldETag, journal.Intents[4].IfMatch)

	store.mu.Lock()
	store.putGates = nil
	store.putErrors = nil
	store.mu.Unlock()
	report, err := runWriteModeWithOptions(context.Background(), manifestPath, checkpointPath, "", "rollback", options, store)
	require.NoError(t, err)
	require.Equal(t, manifest.Changed, report.AppliedChanged)
	require.Positive(t, report.AlreadyTarget)
	for _, record := range records {
		require.True(t, store.oldVisible(record), "height %d", record.Height)
	}
}

func TestApplyConditionalConflictStopsEvenWhenTargetIsVisible(t *testing.T) {
	manifestPath, _, record, oldObject := makeSealedPlan(t)
	_, manifestHash, err := loadPlanManifest(manifestPath)
	require.NoError(t, err)
	backup := writeBackupProof(t, manifestPath, manifestHash)
	store := &fakeObjectStore{
		objects:          map[string]storedObject{record.Key: oldObject},
		putErrors:        map[string]error{record.Key: errObjectConflict},
		commitOnPutError: map[string]bool{record.Key: true},
	}
	checkpointPath := runtimeArtifactPath(manifestPath, "apply-conflict-target.json")
	_, err = runWriteMode(context.Background(), manifestPath, checkpointPath, backup, "apply", store)
	require.ErrorContains(t, err, "conditional conflict")
	require.True(t, store.targetVisible(record))
	_, statErr := os.Stat(checkpointPath)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestApplyRejectsSourceMetadataDrift(t *testing.T) {
	manifestPath, _, record, oldObject := makeSealedPlan(t)
	_, manifestHash, err := loadPlanManifest(manifestPath)
	require.NoError(t, err)
	backup := writeBackupProof(t, manifestPath, manifestHash)
	oldObject.Headers.ContentType = "changed/type"
	store := &fakeObjectStore{objects: map[string]storedObject{record.Key: oldObject}}
	_, err = runWriteMode(context.Background(), manifestPath, runtimeArtifactPath(manifestPath, "metadata.json"), backup, "apply", store)
	require.ErrorContains(t, err, "source object metadata changed")
}

func TestApplyRejectsConflictAndThirdContent(t *testing.T) {
	manifestPath, _, record, oldObject := makeSealedPlan(t)
	_, manifestHash, err := loadPlanManifest(manifestPath)
	require.NoError(t, err)
	backup := writeBackupProof(t, manifestPath, manifestHash)

	oldObject.ETag = "different-etag"
	store := &fakeObjectStore{objects: map[string]storedObject{record.Key: oldObject}}
	_, err = runWriteMode(context.Background(), manifestPath, runtimeArtifactPath(manifestPath, "conflict.json"), backup, "apply", store)
	require.ErrorContains(t, err, "changed ETag")

	store.objects[record.Key] = storedObject{Body: []byte("third"), ETag: "third"}
	_, err = runWriteMode(context.Background(), manifestPath, runtimeArtifactPath(manifestPath, "third.json"), backup, "apply", store)
	require.ErrorContains(t, err, "third-party content")
}

func TestCheckpointRejectsCorruptionAndBindingMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint.json")
	require.NoError(t, os.WriteFile(path, []byte("{"), 0o600))
	_, err := loadCheckpoint(path, "run", "hash", "apply")
	require.Error(t, err)

	require.NoError(t, saveCheckpoint(path, checkpoint{RunID: "other", ManifestHash: "hash", Mode: "apply"}))
	_, err = loadCheckpoint(path, "run", "hash", "apply")
	require.ErrorContains(t, err, "does not match")

	err = saveCheckpoint(path, checkpoint{
		RunID: "run", ManifestHash: "hash", Mode: "apply", Frontier: 1, Height: 2,
	})
	require.ErrorContains(t, err, "audit counters are inconsistent")
	loaded, err := loadCheckpoint(path, "other", "hash", "apply")
	require.NoError(t, err)
	require.Equal(t, "other", loaded.RunID)
}

func TestCheckpointRejectsChecksumMutationUnknownFieldsAndTrailingData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint.json")
	want := checkpoint{
		RunID: "run", ManifestHash: "hash", Mode: "verify",
		Frontier: 10, Height: 10, Changed: 4,
	}
	require.NoError(t, saveCheckpoint(path, want))
	loaded, err := loadCheckpoint(path, want.RunID, want.ManifestHash, want.Mode)
	require.NoError(t, err)
	require.Equal(t, want.Frontier, loaded.Frontier)

	body, err := os.ReadFile(path)
	require.NoError(t, err)
	var fields map[string]any
	require.NoError(t, json.Unmarshal(body, &fields))
	fields["frontier"] = float64(11)
	mutated, err := json.Marshal(fields)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, mutated, 0o600))
	_, err = loadCheckpoint(path, want.RunID, want.ManifestHash, want.Mode)
	require.ErrorContains(t, err, "checksum mismatch")

	require.NoError(t, saveCheckpoint(path, want))
	body, err = os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(body, &fields))
	fields["unexpected"] = true
	withUnknown, err := json.Marshal(fields)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, withUnknown, 0o600))
	_, err = loadCheckpoint(path, want.RunID, want.ManifestHash, want.Mode)
	require.ErrorContains(t, err, "unknown field")

	require.NoError(t, saveCheckpoint(path, want))
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = file.WriteString("{}\n")
	require.NoError(t, err)
	require.NoError(t, file.Close())
	_, err = loadCheckpoint(path, want.RunID, want.ManifestHash, want.Mode)
	require.ErrorContains(t, err, "trailing JSON")
}

func TestApplyPreflightsEntirePackBeforeFirstPUT(t *testing.T) {
	manifestPath, manifest, _, objects := makeSealedPlanRecords(t, 3)
	chunk := &manifest.Chunks[0]
	indexPath := filepath.Join(filepath.Dir(manifestPath), chunk.Index)
	index, err := os.OpenFile(indexPath, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = index.WriteString("{}\n")
	require.NoError(t, err)
	require.NoError(t, index.Close())
	chunk.IndexSHA256, _, err = hashFile(indexPath)
	require.NoError(t, err)
	_, err = atomicJSON(manifestPath, manifest)
	require.NoError(t, err)

	_, manifestHash, err := loadPlanManifest(manifestPath)
	require.NoError(t, err)
	backup := writeBackupProof(t, manifestPath, manifestHash)
	store := &fakeObjectStore{objects: objects}
	_, err = runWriteMode(
		context.Background(), manifestPath, runtimeArtifactPath(manifestPath, "apply-preflight.json"), backup, "apply", store,
	)
	require.ErrorContains(t, err, "pack index")
	require.Zero(t, store.puts)
}

func TestWritePreflightRejectsPackTargetThatDiffersFromDump(t *testing.T) {
	manifestPath, manifest, records, objects := makeSealedPlanRecords(t, 1)
	record := records[0]
	var target dtypes.BlockStorageDiff
	require.NoError(t, rlp.DecodeBytes(record.NewBody, &target))
	target.StorageDiff[0].Values[0].Value = uint256.NewInt(3)
	newBody, err := rlp.EncodeToBytes(target)
	require.NoError(t, err)
	manifest.NewBytes += int64(len(newBody) - len(record.NewBody))
	record.NewBody = newBody
	record.NewSHA256 = sha256Hash(newBody)

	dir := filepath.Dir(manifestPath)
	for _, chunk := range manifest.Chunks {
		require.NoError(t, os.Remove(filepath.Join(dir, chunk.Pack)))
		require.NoError(t, os.Remove(filepath.Join(dir, chunk.Index)))
	}
	writer, err := newPackWriter(dir, 1<<30)
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	manifest.Chunks, err = writer.Close()
	require.NoError(t, err)
	_, err = atomicJSON(manifestPath, manifest)
	require.NoError(t, err)

	loaded, _, err := loadPlanManifest(manifestPath)
	require.NoError(t, err)
	require.NoError(t, validatePlanForWrite(dir, loaded))
	store := &fakeObjectStore{objects: objects}
	_, err = runWriteMode(
		context.Background(), manifestPath, runtimeArtifactPath(manifestPath, "rollback-tampered.json"), "", rollbackMode, store,
	)
	require.ErrorContains(t, err, "target storage differs from sealed dump")
	total, _, _ := store.getStats()
	require.Zero(t, total)
	require.Zero(t, store.puts)
}
