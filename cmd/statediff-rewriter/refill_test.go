package main

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cosmos/iavl"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/evmos/ethermint/debank/statediff"
	dtypes "github.com/evmos/ethermint/debank/types"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	storetypes "cosmossdk.io/store/types"
)

type refillTestRoots struct {
	infos map[int64]*storetypes.CommitInfo
}

type refillTestPutCounter struct {
	*fakeObjectStore
	attemptsByKey map[string]int
}

func (store *refillTestPutCounter) Put(
	ctx context.Context,
	bucket, key string,
	body []byte,
	ifMatch string,
	headers objectHeaders,
) (putResult, error) {
	store.attemptsByKey[key]++
	return store.fakeObjectStore.Put(ctx, bucket, key, body, ifMatch, headers)
}

func (roots *refillTestRoots) commitInfo(version int64) (*storetypes.CommitInfo, error) {
	info, found := roots.infos[version]
	if !found {
		return nil, fmt.Errorf("missing test commit info %d", version)
	}
	return info, nil
}

func newRefillTestRoots(first, last int64) *refillTestRoots {
	infos := make(map[int64]*storetypes.CommitInfo, last-first+2)
	for version := first - 2; version < last; version++ {
		var hash common.Hash
		binary.BigEndian.PutUint64(hash[common.HashLength-8:], uint64(version))
		infos[version] = &storetypes.CommitInfo{
			Version: version,
			StoreInfos: []storetypes.StoreInfo{{
				Name: "evm",
				CommitId: storetypes.CommitID{
					Version: version,
					Hash:    hash[:],
				},
			}},
		}
	}
	return &refillTestRoots{infos: infos}
}

func refillTestPair(height int64, value byte) *iavl.KVPair {
	key := make([]byte, 53)
	key[0] = 0x02
	binary.BigEndian.PutUint64(key[13:21], uint64(height))
	binary.BigEndian.PutUint64(key[45:53], uint64(height*2))
	return &iavl.KVPair{Key: key, Value: []byte{value}}
}

func refillTestValue(height int64) byte {
	return byte(height%200 + 1)
}

func refillTestCanonical(t *testing.T, height int64, value byte) []dtypes.AccountStorageDiff {
	t.Helper()
	canonical, err := statediff.CanonicalStorageDiff(&iavl.ChangeSet{
		Pairs: []*iavl.KVPair{refillTestPair(height, value)},
	})
	require.NoError(t, err)
	return canonical
}

func writeRefillTestZZ(t *testing.T, first, last int64) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "block.zz")
	writeRefillTestZZFile(t, path, first, last)
	return path
}

func writeRefillTestZZFile(t *testing.T, path string, first, last int64) {
	t.Helper()
	var body []byte
	for height := first; height <= last; height++ {
		body = append(body, encodeChangeSet(t, height, refillTestPair(height, refillTestValue(height)))...)
	}
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, zlibBody(t, body), 0o600))
}

func refillTestRoot(roots *refillTestRoots, height int64) common.Hash {
	return common.BytesToHash(roots.infos[height-1].Hash())
}

func refillTestBody(t *testing.T, height int64, storage []dtypes.AccountStorageDiff) []byte {
	t.Helper()
	diff := dtypes.BlockStorageDiff{
		Hash:       common.BytesToHash([]byte{0x11, byte(height)}),
		ParentHash: common.BytesToHash([]byte{0x22, byte(height)}),
		NewAccounts: []dtypes.NewAccount{{
			Address:  common.BytesToHash([]byte{0x33, byte(height)}),
			Balance:  uint256.NewInt(uint64(height + 1)),
			Nonce:    uint64(height + 2),
			CodeHash: common.BytesToHash([]byte{0x44, byte(height)}),
		}},
		DeletedAccounts: []common.Hash{common.BytesToHash([]byte{0x55, byte(height)})},
		StorageDiff:     storage,
		NewCodes: []dtypes.NewCode{{
			CodeHash: common.BytesToHash([]byte{0x66, byte(height)}),
			Code:     []byte{0x60, byte(height), 0x00},
		}},
	}
	body, err := rlp.EncodeToBytes(diff)
	require.NoError(t, err)
	return body
}

func refillTestObjects(
	t *testing.T,
	roots *refillTestRoots,
	prefix string,
	first, last int64,
	alreadyCanonical map[int64]bool,
) (map[string]storedObject, map[int64]string, map[int64][]dtypes.AccountStorageDiff) {
	t.Helper()
	objects := make(map[string]storedObject, last-first+1)
	keys := make(map[int64]string, last-first+1)
	canonicalByHeight := make(map[int64][]dtypes.AccountStorageDiff, last-first+1)
	for height := first; height <= last; height++ {
		canonical := refillTestCanonical(t, height, refillTestValue(height))
		oldStorage := refillTestCanonical(t, height, refillTestValue(height)^0xff)
		if alreadyCanonical[height] {
			oldStorage = canonical
		}
		key := fmt.Sprintf("%s/%s/stateDiff", prefix, strings.ToLower(refillTestRoot(roots, height).Hex()))
		keys[height] = key
		canonicalByHeight[height] = canonical
		objects[key] = storedObject{
			Body: refillTestBody(t, height, oldStorage),
			ETag: fmt.Sprintf("etag-%d", height),
			Headers: objectHeaders{
				Metadata:             []byte(`{"source":"refill-test"}`),
				CacheControl:         "max-age=60",
				ContentDisposition:   "attachment",
				ContentEncoding:      "identity",
				ContentLanguage:      "en",
				ContentType:          "application/octet-stream",
				StorageClass:         "EXPRESS_ONEZONE",
				ServerSideEncryption: "AES256",
			},
		}
	}
	return objects, keys, canonicalByHeight
}

func refillTestOptions(t *testing.T, zz string, first, last int64, write bool) refillOptions {
	t.Helper()
	options := refillOptions{
		ZZFile: zz, ArchiveHome: filepath.Join(t.TempDir(), "archive"),
		FirstHeight: first, FinalHeight: last, Write: write,
		Bucket: "bucket", Prefix: "prefix", Region: "region",
		Concurrency: 1, Window: 1,
	}
	if write {
		options.Checkpoint = filepath.Join(t.TempDir(), "refill-checkpoint.json")
	}
	return options
}

func refillTestDirectOptions(t *testing.T, first, last int64, write bool) refillOptions {
	t.Helper()
	options := refillTestOptions(t, "", first, last, write)
	options.Direct = true
	options.IAVLCacheSize = 10
	options.IAVLConcurrency = 2
	options.IAVLRunHeights = defaultDirectIAVLRunHeights
	options.ArchiveIdentity = archiveIdentity{
		Home:             options.ArchiveHome,
		DatabaseIdentity: "refill-test-db",
		LatestVersion:    last,
		FinalCommitHash:  "0x1234",
	}
	return options
}

func refillTestDirectIterator(
	t *testing.T,
	first, last int64,
	starts *[]int64,
) refillStorageIterator {
	t.Helper()
	canonical := make(map[int64][]dtypes.AccountStorageDiff, last-first+1)
	for height := first; height <= last; height++ {
		canonical[height] = refillTestCanonical(t, height, refillTestValue(height))
	}
	return func(
		ctx context.Context,
		rangeFirst, rangeLast int64,
		callback func(int64, []dtypes.AccountStorageDiff) error,
	) error {
		*starts = append(*starts, rangeFirst)
		for height := rangeFirst; height <= rangeLast; height++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := callback(height, canonical[height]); err != nil {
				return err
			}
		}
		return nil
	}
}

func TestIterateZZRangeFastStopsAtTargetBeforeBadTrailer(t *testing.T) {
	body := append(encodeChangeSet(t, 2), encodeChangeSet(t, 3)...)
	trailing := make([]byte, 64<<10)
	_, err := rand.New(rand.NewSource(1)).Read(trailing)
	require.NoError(t, err)
	body = append(body, trailing...)
	compressed := zlibBody(t, body)
	compressed[len(compressed)-1] ^= 0xff

	fullReader, err := zlib.NewReader(bytes.NewReader(compressed))
	require.NoError(t, err)
	_, fullReadErr := io.ReadAll(io.LimitReader(fullReader, int64(len(body)+1)))
	require.Error(t, fullReadErr, "the fixture must have a bad zlib trailer")
	_ = fullReader.Close()

	path := filepath.Join(t.TempDir(), "block.zz")
	require.NoError(t, os.WriteFile(path, compressed, 0o600))
	var delivered []int64
	err = iterateZZRangeFastContext(context.Background(), path, 2, 3, func(height int64, _ *iavl.ChangeSet) error {
		delivered = append(delivered, height)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []int64{2, 3}, delivered)
}

func TestIterateZZDirectoryRangeFastSortsFilesNumerically(t *testing.T) {
	dir := t.TempDir()
	writeRefillTestZZFile(t, filepath.Join(dir, "block-10.zz"), 10, 10)
	writeRefillTestZZFile(t, filepath.Join(dir, "block-2.zz"), 2, 9)

	var delivered []int64
	err := iterateZZDirectoryRangeFastContext(
		context.Background(), dir, 2, 10,
		func(height int64, _ *iavl.ChangeSet) error {
			delivered = append(delivered, height)
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []int64{2, 3, 4, 5, 6, 7, 8, 9, 10}, delivered)
}

func TestIterateZZDirectoryRangeFastStartsAndStopsInsideFiles(t *testing.T) {
	dir := t.TempDir()
	writeRefillTestZZFile(t, filepath.Join(dir, "block-100.zz"), 100, 104)
	writeRefillTestZZFile(t, filepath.Join(dir, "block-105.zz"), 105, 109)
	writeRefillTestZZFile(t, filepath.Join(dir, "block-110.zz"), 110, 114)

	var delivered []int64
	err := iterateZZDirectoryRangeFastContext(
		context.Background(), dir, 102, 112,
		func(height int64, _ *iavl.ChangeSet) error {
			delivered = append(delivered, height)
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []int64{102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112}, delivered)
}

func TestRunRefillGetOnlyNeverPuts(t *testing.T) {
	const first, last = int64(100), int64(102)
	zz := writeRefillTestZZ(t, first, last)
	roots := newRefillTestRoots(first, last)
	objects, keys, _ := refillTestObjects(t, roots, "prefix", first, last, nil)
	original := make(map[string]storedObject, len(objects))
	for key, object := range objects {
		original[key] = cloneStoredObject(object)
	}
	store := &fakeObjectStore{objects: objects}
	options := refillTestOptions(t, zz, first, last, false)

	report, err := runRefillWithReaders(context.Background(), options, roots, store)
	require.NoError(t, err)
	require.Equal(t, "get-only", report.Mode)
	require.Equal(t, last, report.Frontier)
	require.Equal(t, uint64(last-first+1), report.Processed)
	require.Equal(t, uint64(last-first+1), report.Gets)
	require.Equal(t, uint64(last-first+1), report.WouldPUTs)
	require.Zero(t, report.Puts)

	totalGets, _, getsByKey := store.getStats()
	require.Equal(t, int(last-first+1), totalGets)
	puts, _ := store.putStats()
	require.Zero(t, puts)
	for height := first; height <= last; height++ {
		require.Equal(t, 1, getsByKey[keys[height]])
		require.Equal(t, original[keys[height]], store.objects[keys[height]])
	}
}

func TestRunRefillWriteAlwaysPutsOnlyReplacedStorage(t *testing.T) {
	const first, last = int64(100), int64(101)
	zz := writeRefillTestZZ(t, first, last)
	roots := newRefillTestRoots(first, last)
	objects, keys, canonical := refillTestObjects(t, roots, "prefix", first, last, map[int64]bool{last: true})
	original := make(map[string]storedObject, len(objects))
	for key, object := range objects {
		original[key] = cloneStoredObject(object)
	}
	store := &fakeObjectStore{objects: objects}
	options := refillTestOptions(t, zz, first, last, true)

	report, err := runRefillWithReaders(context.Background(), options, roots, store)
	require.NoError(t, err)
	require.Equal(t, "write", report.Mode)
	require.Equal(t, uint64(2), report.Gets)
	require.Equal(t, uint64(2), report.WouldPUTs)
	require.Equal(t, uint64(2), report.Puts)
	puts, _ := store.putStats()
	require.Equal(t, 2, puts, "an already canonical storage field must still be PUT")

	for height := first; height <= last; height++ {
		key := keys[height]
		got := store.objects[key]
		require.Equal(t, original[key].Headers, got.Headers)
		require.NotEqual(t, original[key].ETag, got.ETag)

		oldFields, err := blockStorageDiffRawFields(original[key].Body)
		require.NoError(t, err)
		newFields, err := blockStorageDiffRawFields(got.Body)
		require.NoError(t, err)
		for index := range oldFields {
			if index != storageDiffFieldIndex {
				require.Equal(t, []byte(oldFields[index]), []byte(newFields[index]), "height %d field %d", height, index)
			}
		}
		wantStorage, err := rlp.EncodeToBytes(canonical[height])
		require.NoError(t, err)
		require.Equal(t, wantStorage, []byte(newFields[storageDiffFieldIndex]))
		if height == first {
			require.NotEqual(t, []byte(oldFields[storageDiffFieldIndex]), []byte(newFields[storageDiffFieldIndex]))
		}
	}

	checkpoint, found, err := loadRefillCheckpoint(options.Checkpoint, options)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, last, checkpoint.Frontier)
	require.Equal(t, uint64(2), checkpoint.Processed)
	require.Equal(t, uint64(2), checkpoint.Puts)
}

func TestRunRefillDirectWriteUsesCanonicalIterator(t *testing.T) {
	const first, last = int64(100), int64(102)
	roots := newRefillTestRoots(first, last)
	objects, keys, canonical := refillTestObjects(t, roots, "prefix", first, last, nil)
	store := &fakeObjectStore{objects: objects}
	options := refillTestDirectOptions(t, first, last, true)
	var starts []int64

	report, err := runRefillWithReadersAndIterator(
		context.Background(), options, roots, store,
		refillTestDirectIterator(t, first, last, &starts),
	)
	require.NoError(t, err)
	require.True(t, report.Direct)
	require.Equal(t, []int64{first}, starts)
	require.Equal(t, uint64(last-first+1), report.Gets)
	require.Equal(t, uint64(last-first+1), report.Puts)

	for height := first; height <= last; height++ {
		fields, err := blockStorageDiffRawFields(store.objects[keys[height]].Body)
		require.NoError(t, err)
		wantStorage, err := rlp.EncodeToBytes(canonical[height])
		require.NoError(t, err)
		require.Equal(t, wantStorage, []byte(fields[storageDiffFieldIndex]))
	}
}

func TestRunRefillDirectWorldStateReplacesAllCanonicalFields(t *testing.T) {
	const first, last = int64(100), int64(101)
	roots := newRefillTestRoots(first, last)
	objects, keys, canonical := refillTestObjects(t, roots, "prefix", first, last, nil)
	original := make(map[string]storedObject, len(objects))
	for key, object := range objects {
		original[key] = cloneStoredObject(object)
	}
	store := &fakeObjectStore{objects: objects}
	options := refillTestDirectOptions(t, first, last, true)
	options.WorldState = true
	options.EVMDenom = "basecro"
	address := common.HexToHash("0x1234")
	worldByHeight := map[int64]expectedWorldState{
		first: {
			newAccounts: []dtypes.NewAccount{{
				Address: address, Balance: uint256.NewInt(9), Nonce: 10,
				CodeHash: common.HexToHash("0x5678"),
			}},
			deletedAccounts: []common.Hash{common.HexToHash("0x90")},
			storage:         canonical[first],
			codeWrites:      []dtypes.NewCode{{CodeHash: common.HexToHash("0xab"), Code: []byte{0x60}}},
		},
		last: {},
	}
	var starts []int64
	iterator := func(
		ctx context.Context,
		rangeFirst, rangeLast int64,
		callback func(int64, expectedWorldState) error,
	) error {
		starts = append(starts, rangeFirst)
		for height := rangeFirst; height <= rangeLast; height++ {
			if err := callback(height, worldByHeight[height]); err != nil {
				return err
			}
		}
		return ctx.Err()
	}

	report, err := runRefillWithReadersAndIterators(
		context.Background(), options, roots, store, nil, iterator,
	)
	require.NoError(t, err)
	require.True(t, report.WorldState)
	require.Equal(t, "basecro", report.EVMDenom)
	require.Equal(t, []int64{first}, starts)
	for height := first; height <= last; height++ {
		require.Equal(t, original[keys[height]].Headers, store.objects[keys[height]].Headers)
		requireWorldStateRootsRawUnchanged(t,
			original[keys[height]].Body, store.objects[keys[height]].Body,
		)
		var got dtypes.BlockStorageDiff
		require.NoError(t, rlp.DecodeBytes(store.objects[keys[height]].Body, &got))
		if height == last {
			require.Empty(t, got.NewAccounts)
			require.Empty(t, got.DeletedAccounts)
			require.Empty(t, got.StorageDiff)
			require.Empty(t, got.NewCodes)
		} else {
			require.Equal(t, worldByHeight[height].newAccounts, got.NewAccounts)
			require.Equal(t, worldByHeight[height].deletedAccounts, got.DeletedAccounts)
			require.Equal(t, worldByHeight[height].storage, got.StorageDiff)
			require.Equal(t, worldByHeight[height].codeWrites, got.NewCodes)
		}
	}

	checkpoint, found, err := loadRefillCheckpoint(options.Checkpoint, options)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, refillWorldStateCheckpointSchema, checkpoint.Schema)
	require.Equal(t, "basecro", checkpoint.EVMDenom)
	otherDenom := options
	otherDenom.EVMDenom = "other"
	_, _, err = loadRefillCheckpoint(options.Checkpoint, otherDenom)
	require.ErrorContains(t, err, "checkpoint does not match")
}

func TestRefillOptionsRejectsMoreThanTenThousandHeights(t *testing.T) {
	options := refillOptions{
		ZZFile: "/tmp/block.zz", ArchiveHome: "/tmp/archive",
		FirstHeight: 2, FinalHeight: 10_001,
		Bucket: "bucket", Prefix: "prefix", Region: "region",
		Concurrency: 1, Window: 1,
	}
	require.NoError(t, options.prepare())

	options.FinalHeight++
	require.ErrorContains(t, options.prepare(), "refill range exceeds 10000 heights")

	options.ZZFile = ""
	options.ZZDir = "/tmp/changesets"
	require.NoError(t, options.prepare(), "directory mode accepts a full refill range")

	options.ZZFile = "/tmp/block.zz"
	require.ErrorContains(t, options.prepare(), "exactly one refill source")

	options.ZZFile, options.ZZDir = "", ""
	options.Direct = true
	options.IAVLCacheSize = 0
	options.IAVLConcurrency = 1
	options.IAVLRunHeights = defaultDirectIAVLRunHeights
	options.FinalHeight = 20_000
	require.NoError(t, options.prepare(), "direct mode accepts a full refill range")

	options.ZZFile = "/tmp/block.zz"
	require.ErrorContains(t, options.prepare(), "exactly one refill source")

	options.ZZFile = ""
	options.IAVLConcurrency = 0
	require.ErrorContains(t, options.prepare(), "iavl-concurrency")

	options.IAVLConcurrency = 1
	options.IAVLRunHeights = directIAVLShardSize + 1
	require.ErrorContains(t, options.prepare(), "iavl-run-heights")

	options.IAVLRunHeights = defaultDirectIAVLRunHeights
	options.Direct = false
	options.WorldState = true
	options.EVMDenom = "basecro"
	options.ZZFile = "/tmp/block.zz"
	require.ErrorContains(t, options.prepare(), "world-state refill requires direct")

	options.ZZFile = ""
	options.Direct = true
	options.EVMDenom = ""
	require.ErrorContains(t, options.prepare(), "requires an EVM denom")
}

func TestRunRefillWriteResumesFromPersistedCheckpointAfterFailure(t *testing.T) {
	const first, last = int64(100), int64(1_101)
	const failedHeight = int64(1_100)
	zz := writeRefillTestZZ(t, first, last)
	roots := newRefillTestRoots(first, last)
	objects, keys, _ := refillTestObjects(t, roots, "prefix", first, last, nil)
	wantErr := errors.New("injected PUT failure")
	store := &fakeObjectStore{
		objects:   objects,
		putErrors: map[string]error{keys[failedHeight]: wantErr},
	}
	options := refillTestOptions(t, zz, first, last, true)

	_, err := runRefillWithReaders(context.Background(), options, roots, store)
	require.ErrorIs(t, err, wantErr)
	checkpoint, found, err := loadRefillCheckpoint(options.Checkpoint, options)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, first+int64(refillCheckpointEvery)-1, checkpoint.Frontier)
	require.Equal(t, refillCheckpointEvery, checkpoint.Processed)
	require.Equal(t, refillCheckpointEvery, checkpoint.Puts)

	delete(store.putErrors, keys[failedHeight])
	report, err := runRefillWithReaders(context.Background(), options, roots, store)
	require.NoError(t, err)
	require.Equal(t, last, report.Frontier)
	require.Equal(t, uint64(last-first+1), report.Processed)
	require.Equal(t, uint64(2), report.InvocationProcessed)
	require.Equal(t, uint64(last-first+1), report.Puts)

	_, _, getsByKey := store.getStats()
	require.Equal(t, 1, getsByKey[keys[first]])
	require.Equal(t, 1, getsByKey[keys[first+int64(refillCheckpointEvery)-1]])
	require.Equal(t, 2, getsByKey[keys[failedHeight]])
	require.Equal(t, 1, getsByKey[keys[last]])

	checkpoint, found, err = loadRefillCheckpoint(options.Checkpoint, options)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, last, checkpoint.Frontier)
	require.Equal(t, uint64(last-first+1), checkpoint.Processed)
	require.Equal(t, uint64(last-first+1), checkpoint.Puts)
}

func TestRunRefillDirectWriteResumesIteratorAtPersistedFrontier(t *testing.T) {
	const first, last = int64(100), int64(1_101)
	const failedHeight = int64(1_100)
	roots := newRefillTestRoots(first, last)
	objects, keys, _ := refillTestObjects(t, roots, "prefix", first, last, nil)
	wantErr := errors.New("injected direct PUT failure")
	store := &fakeObjectStore{
		objects:   objects,
		putErrors: map[string]error{keys[failedHeight]: wantErr},
	}
	options := refillTestDirectOptions(t, first, last, true)
	var starts []int64
	iterator := refillTestDirectIterator(t, first, last, &starts)

	_, err := runRefillWithReadersAndIterator(
		context.Background(), options, roots, store, iterator,
	)
	require.ErrorIs(t, err, wantErr)
	checkpoint, found, err := loadRefillCheckpoint(options.Checkpoint, options)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, failedHeight-1, checkpoint.Frontier)
	require.Equal(t, refillCheckpointEvery, checkpoint.Processed)

	otherSource := options
	otherSource.ArchiveIdentity.DatabaseIdentity = "other-db"
	_, _, err = loadRefillCheckpoint(options.Checkpoint, otherSource)
	require.ErrorContains(t, err, "checkpoint does not match")

	delete(store.putErrors, keys[failedHeight])
	report, err := runRefillWithReadersAndIterator(
		context.Background(), options, roots, store, iterator,
	)
	require.NoError(t, err)
	require.Equal(t, []int64{first, failedHeight}, starts)
	require.Equal(t, uint64(2), report.InvocationProcessed)
	require.Equal(t, last, report.Frontier)
}

func TestRunRefillWriteResumesAcrossZZFileBoundaryWithoutRepeatingCompletedFile(t *testing.T) {
	const first, boundary, last = int64(100), int64(1_100), int64(1_101)
	dir := t.TempDir()
	writeRefillTestZZFile(t, filepath.Join(dir, "block-100.zz"), first, boundary-1)
	writeRefillTestZZFile(t, filepath.Join(dir, "block-1100.zz"), boundary, last)

	roots := newRefillTestRoots(first, last)
	objects, keys, _ := refillTestObjects(t, roots, "prefix", first, last, nil)
	wantErr := errors.New("injected PUT failure")
	store := &refillTestPutCounter{
		fakeObjectStore: &fakeObjectStore{
			objects:   objects,
			putErrors: map[string]error{keys[boundary]: wantErr},
		},
		attemptsByKey: make(map[string]int),
	}
	options := refillOptions{
		ZZDir: dir, ArchiveHome: filepath.Join(t.TempDir(), "archive"),
		Checkpoint:  filepath.Join(t.TempDir(), "refill-checkpoint.json"),
		FirstHeight: first, FinalHeight: last, Write: true,
		Bucket: "bucket", Prefix: "prefix", Region: "region",
		Concurrency: 1, Window: 1,
	}

	_, err := runRefillWithReaders(context.Background(), options, roots, store)
	require.ErrorIs(t, err, wantErr)
	checkpoint, found, err := loadRefillCheckpoint(options.Checkpoint, options)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, boundary-1, checkpoint.Frontier)

	delete(store.putErrors, keys[boundary])
	report, err := runRefillWithReaders(context.Background(), options, roots, store)
	require.NoError(t, err)
	require.Equal(t, last, report.Frontier)
	require.Equal(t, uint64(2), report.InvocationProcessed)

	_, _, getsByKey := store.getStats()
	require.Equal(t, 1, getsByKey[keys[first]])
	require.Equal(t, 1, getsByKey[keys[boundary-1]])
	require.Equal(t, 2, getsByKey[keys[boundary]])
	require.Equal(t, 1, getsByKey[keys[last]])
	require.Equal(t, 1, store.attemptsByKey[keys[first]])
	require.Equal(t, 1, store.attemptsByKey[keys[boundary-1]])
	require.Equal(t, 2, store.attemptsByKey[keys[boundary]])
	require.Equal(t, 1, store.attemptsByKey[keys[last]])
}
