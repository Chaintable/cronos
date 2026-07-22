package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/cosmos/iavl"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/evmos/ethermint/debank/statediff"
	dtypes "github.com/evmos/ethermint/debank/types"

	storetypes "cosmossdk.io/store/types"

	"github.com/cosmos/cosmos-sdk/version"
)

type planOptions struct {
	DumpStaging      string
	Direct           bool
	IAVLCacheSize    int
	IAVLConcurrency  int
	ArchiveHome      string
	Output           string
	Bucket           string
	Prefix           string
	Region           string
	MinFree          uint64
	SnapshotID       string
	ImageDigest      string
	Pilot            bool
	PilotFirstHeight int64
	PilotFinalHeight int64
	Parallel         parallelOptions
}

const (
	planBufferedWriteReserve = uint64(3 << 20)
	planInitialWriteReserve  = uint64(4 << 20)
	directTraversalChunkSize = int64(100_000)
)

type planRange struct {
	FirstHeight      int64
	FinalHeight      int64
	DumpFirstVersion int64
	ManifestSchema   string
	DumpSchema       string
}

type archiveRootCursor struct {
	reader     commitInfoReader
	nextHeight int64
	previous   common.Hash
	started    bool
}

func newArchiveRootCursor(reader commitInfoReader, firstHeight int64) (*archiveRootCursor, error) {
	if firstHeight < 2 {
		return nil, fmt.Errorf("first block height must be at least 2")
	}
	return &archiveRootCursor{reader: reader, nextHeight: firstHeight}, nil
}

func (cursor *archiveRootCursor) Next(height int64) (common.Hash, common.Hash, error) {
	if height != cursor.nextHeight {
		return common.Hash{}, common.Hash{}, fmt.Errorf("root cursor got height %d, want %d", height, cursor.nextHeight)
	}
	rootInfo, err := cursor.reader.commitInfo(height - 1)
	if err != nil {
		return common.Hash{}, common.Hash{}, err
	}
	root := common.BytesToHash(rootInfo.Hash())
	parent := cursor.previous
	if !cursor.started && height > 2 {
		parentInfo, err := cursor.reader.commitInfo(height - 2)
		if err != nil {
			return common.Hash{}, common.Hash{}, err
		}
		parent = common.BytesToHash(parentInfo.Hash())
	}
	cursor.previous = root
	cursor.nextHeight++
	cursor.started = true
	return root, parent, nil
}

type planTask struct {
	height    uint64
	root      common.Hash
	parent    common.Hash
	key       string
	canonical []dtypes.AccountStorageDiff
	prefixSHA common.Hash
	bytes     int64
}

type planOutcome struct {
	height    uint64
	root      common.Hash
	prefixSHA common.Hash
	skipped   bool
	changed   bool
	oldBytes  int64
	record    packRecord
	bytes     int64
}

type planPrefixRecord struct {
	Previous     common.Hash
	Height       uint64
	Root         common.Hash
	Parent       common.Hash
	CanonicalSHA common.Hash
}

func resolvePlanRange(options planOptions, latestVersion int64) (planRange, error) {
	if latestVersion < 2 {
		return planRange{}, fmt.Errorf("archive latest version %d is below block 2", latestVersion)
	}
	if !options.Pilot {
		if options.PilotFirstHeight != 0 || options.PilotFinalHeight != 0 {
			return planRange{}, fmt.Errorf("pilot heights require explicit pilot mode")
		}
		return planRange{
			FirstHeight: 2, FinalHeight: latestVersion, DumpFirstVersion: 1,
			ManifestSchema: manifestSchema, DumpSchema: dumpManifestSchema,
		}, nil
	}
	if options.PilotFirstHeight < 2 || options.PilotFinalHeight < options.PilotFirstHeight || options.PilotFinalHeight > latestVersion {
		return planRange{}, fmt.Errorf("invalid pilot range %d-%d for archive latest %d", options.PilotFirstHeight, options.PilotFinalHeight, latestVersion)
	}
	return planRange{
		FirstHeight: options.PilotFirstHeight, FinalHeight: options.PilotFinalHeight,
		DumpFirstVersion: options.PilotFirstHeight, ManifestSchema: pilotManifestSchema, DumpSchema: pilotDumpManifestSchema,
	}, nil
}

func iterateDirectStateChangesContext(
	ctx context.Context,
	source stateChangeSource,
	first, last int64,
	callback func(int64, *iavl.ChangeSet) error,
) error {
	if ctx == nil || source == nil || callback == nil {
		return fmt.Errorf("direct traversal context, source, and callback are required")
	}
	if first < 2 || last < first {
		return fmt.Errorf("invalid direct traversal range %d-%d", first, last)
	}
	for chunkFirst := first; ; {
		chunkLast := chunkFirst + directTraversalChunkSize - 1
		if chunkLast < chunkFirst || chunkLast > last {
			chunkLast = last
		}
		expected := chunkFirst
		err := traverseVerifiedStateChanges(source, chunkFirst, chunkLast, func(version int64, changeSet *iavl.ChangeSet) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if version != expected {
				return fmt.Errorf("direct traversal emitted version %d, want %d", version, expected)
			}
			if changeSet == nil {
				return fmt.Errorf("direct traversal emitted nil changeset at version %d", version)
			}
			expected++
			return callback(version, changeSet)
		})
		if err != nil {
			return fmt.Errorf("direct traversal versions %d-%d: %w", chunkFirst, chunkLast, err)
		}
		if expected != chunkLast+1 {
			return fmt.Errorf("direct traversal ended at version %d, want %d", expected-1, chunkLast)
		}
		if chunkLast == last {
			return nil
		}
		chunkFirst = chunkLast + 1
	}
}

func runPlan(ctx context.Context, options planOptions, objects objectStore) (planManifest, string, error) {
	if err := options.Parallel.validate(true); err != nil {
		return planManifest{}, "", err
	}
	if options.MinFree == 0 {
		return planManifest{}, "", fmt.Errorf("min-free-bytes must be greater than zero")
	}
	if options.Parallel.MaxInFlightBytes < maxObjectOperationBytes+maxObjectSize {
		return planManifest{}, "", fmt.Errorf("plan max-inflight-bytes must be at least %d", maxObjectOperationBytes+maxObjectSize)
	}
	if options.Direct {
		if options.DumpStaging != "" {
			return planManifest{}, "", fmt.Errorf("direct and dump sources are mutually exclusive")
		}
		if options.IAVLCacheSize < 0 {
			return planManifest{}, "", fmt.Errorf("iavl-cache-size must not be negative")
		}
		if options.IAVLConcurrency < 1 || options.IAVLConcurrency > maximumDirectIAVLConcurrency {
			return planManifest{}, "", fmt.Errorf("iavl-concurrency must be between 1 and %d", maximumDirectIAVLConcurrency)
		}
		if !filepath.IsAbs(options.ArchiveHome) {
			return planManifest{}, "", fmt.Errorf("direct archive home must be absolute")
		}
	} else if options.DumpStaging == "" {
		return planManifest{}, "", fmt.Errorf("exactly one plan source is required: --direct or --dump")
	}
	output, sealedPlan, err := resolvePlanOutput(options.Output)
	if err != nil {
		return planManifest{}, "", err
	}
	options.Output = output
	lockPaths := []string{options.Output}
	if !options.Direct {
		dumpPath, err := canonicalDumpArgument(options.DumpStaging)
		if err != nil {
			return planManifest{}, "", err
		}
		options.DumpStaging = dumpPath
		if strings.HasSuffix(options.DumpStaging, ".staging") {
			lockPaths = append(lockPaths, options.DumpStaging)
		}
	}
	locks, err := acquireStagingLocks(lockPaths...)
	if err != nil {
		return planManifest{}, "", err
	}
	defer func() { _ = releaseStagingLocks(locks) }()
	if checkedOutput, checkedSealed, err := resolvePlanOutput(options.Output); err != nil {
		return planManifest{}, "", err
	} else if checkedOutput != options.Output || checkedSealed != sealedPlan {
		return planManifest{}, "", fmt.Errorf("plan output identity changed while acquiring its staging lock")
	}
	archive, err := openArchive(options.ArchiveHome)
	if err != nil {
		return planManifest{}, "", err
	}
	defer archive.Close()
	identity, err := archive.identity()
	if err != nil {
		return planManifest{}, "", err
	}
	scope, err := resolvePlanRange(options, identity.LatestVersion)
	if err != nil {
		return planManifest{}, "", err
	}
	cronosCommit, ethermintCommit, iavlCommit := buildCommits()
	if err := requireRuntimeBuild(cronosCommit, ethermintCommit, iavlCommit, options.ImageDigest, version.BuildTags); err != nil {
		return planManifest{}, "", err
	}
	var sealedDump, dumpHash string
	var dumpInfo dumpManifest
	sourceMode := planSourceDirectV1
	if !options.Direct {
		sourceMode = planSourceDumpV1
		sealedDump, dumpInfo, dumpHash, err = prepareDumpContext(ctx, options.DumpStaging, dumpContext{
			Schema: scope.DumpSchema, FirstVersion: scope.DumpFirstVersion, LastVersion: scope.FinalHeight,
			SnapshotID: options.SnapshotID, ArchiveIdentity: identity, CronosCommit: cronosCommit,
			EthermintCommit: ethermintCommit, IAVLCommit: iavlCommit,
			ImageDigest: options.ImageDigest, BuildTags: version.BuildTags,
		})
		if err != nil {
			return planManifest{}, "", err
		}
	}
	expectedManifest := planManifest{
		Schema: scope.ManifestSchema, Sealed: true,
		Bucket: options.Bucket, Prefix: strings.TrimSuffix(options.Prefix, "/"), Region: options.Region,
		FirstHeight: scope.FirstHeight, FinalHeight: scope.FinalHeight,
		CronosCommit: cronosCommit, EthermintCommit: ethermintCommit, IAVLCommit: iavlCommit,
		SourceMode: sourceMode, DumpPath: sealedDump, DumpManifestHash: dumpHash, ArchiveIdentity: identity,
		SnapshotID: options.SnapshotID, ImageDigest: options.ImageDigest, BuildTags: version.BuildTags,
	}
	if existing, manifestPath, found, err := reuseSealedPlanContext(ctx, sealedPlan, expectedManifest); err != nil {
		return planManifest{}, "", err
	} else if found {
		return existing, manifestPath, nil
	}
	checkpointPath := filepath.Join(options.Output, "plan.checkpoint.json")
	rootIndexPath := filepath.Join(options.Output, "roots.by-height.tmp")
	var manifest planManifest
	var writer *packWriter
	var rootIndex *os.File
	var resumeRootFile *os.File
	var resumeRootReader *bufio.Reader
	var resumeFrontier uint64
	var prefixDigest common.Hash
	var freshOutput bool
	outputInfo, outputErr := os.Lstat(options.Output)
	switch {
	case errors.Is(outputErr, os.ErrNotExist):
		initialMinimum, addErr := checkedAddUint64(options.MinFree, planInitialWriteReserve, "initial plan free-space requirement")
		if addErr != nil {
			return planManifest{}, "", addErr
		}
		if err := ensureFreeSpace(filepath.Dir(options.Output), initialMinimum); err != nil {
			return planManifest{}, "", fmt.Errorf("initialize plan staging: %w", err)
		}
		if objects == nil {
			objects, err = newS3ObjectStore(ctx, options.Region, options.Parallel.Concurrency)
			if err != nil {
				return planManifest{}, "", err
			}
		}
		if err := os.Mkdir(options.Output, 0o755); err != nil {
			return planManifest{}, "", err
		}
		freshOutput = true
		defer func() {
			if freshOutput {
				_ = os.RemoveAll(options.Output)
			}
		}()
		now := time.Now().UTC()
		manifest = expectedManifest
		manifest.RunID = fmt.Sprintf("bugb-%d", now.UnixNano())
		manifest.CreatedAt = now.Format(time.RFC3339Nano)
		writer, err = newPackWriterContext(ctx, options.Output, chunkSize)
		if err != nil {
			return planManifest{}, "", err
		}
		rootIndex, err = os.OpenFile(rootIndexPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			_ = writer.Abort()
			return planManifest{}, "", err
		}
	case outputErr == nil:
		if !outputInfo.IsDir() || outputInfo.Mode()&os.ModeSymlink != 0 {
			return planManifest{}, "", fmt.Errorf("plan staging must be a non-symlink directory: %s", options.Output)
		}
		if err := requireRegularFile(checkpointPath, "plan checkpoint"); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return planManifest{}, "", fmt.Errorf("plan staging directory exists without a resumable checkpoint: %s", options.Output)
			}
			return planManifest{}, "", err
		}
		checkpoint, found, loadErr := loadPlanCheckpoint(checkpointPath, expectedManifest)
		if loadErr != nil {
			return planManifest{}, "", loadErr
		}
		if !found {
			return planManifest{}, "", fmt.Errorf("plan staging directory exists without a resumable checkpoint: %s", options.Output)
		}
		manifest = checkpoint.Manifest
		resumeFrontier = checkpoint.Frontier
		prefixBytes, decodeErr := hex.DecodeString(checkpoint.PrefixSHA)
		if decodeErr != nil || len(prefixBytes) != common.HashLength {
			return planManifest{}, "", fmt.Errorf("decode plan checkpoint prefix hash")
		}
		copy(prefixDigest[:], prefixBytes)
		rootIndexPath, err = restorePlanRootIndex(options.Output, checkpoint.RootBytes)
		if err != nil {
			return planManifest{}, "", err
		}
		resumeRootFile, err = os.Open(rootIndexPath)
		if err != nil {
			return planManifest{}, "", err
		}
		resumeRootReader = bufio.NewReaderSize(resumeRootFile, 1<<20)
		writer, err = resumePackWriterContext(ctx, options.Output, chunkSize, checkpoint.Pack, manifest)
		if err != nil {
			_ = resumeRootFile.Close()
			return planManifest{}, "", err
		}
		rootIndex, err = os.OpenFile(rootIndexPath, os.O_WRONLY|os.O_APPEND|syscall.O_NOFOLLOW, 0o600)
		if err != nil {
			_ = writer.Abort()
			_ = resumeRootFile.Close()
			return planManifest{}, "", err
		}
	default:
		return planManifest{}, "", outputErr
	}
	writerOpen := true
	defer func() {
		if writerOpen {
			_ = writer.Abort()
		}
	}()
	rootOpen := true
	defer func() {
		if rootOpen {
			_ = rootIndex.Close()
		}
		if resumeRootFile != nil {
			_ = resumeRootFile.Close()
		}
	}()
	rootBuffer := bufio.NewWriterSize(rootIndex, 1<<20)
	rootCursor, err := newArchiveRootCursor(archive, scope.FirstHeight)
	if err != nil {
		_ = rootIndex.Close()
		return planManifest{}, "", err
	}
	limiter, err := newByteLimiter(options.Parallel.MaxInFlightBytes)
	if err != nil {
		_ = rootIndex.Close()
		return planManifest{}, "", err
	}
	saver, err := newPlanCheckpointSaver(
		checkpointPath, rootBuffer, rootIndex, writer, options.MinFree,
		options.Parallel.CheckpointEvery, options.Parallel.CheckpointInterval,
	)
	if err != nil {
		return planManifest{}, "", err
	}
	saver.prefixSHA = prefixDigest
	if freshOutput {
		if err := saver.Initialize(manifest); err != nil {
			return planManifest{}, "", fmt.Errorf("persist initial plan checkpoint: %w", err)
		}
		freshOutput = false
	}
	if objects == nil {
		objects, err = newS3ObjectStore(ctx, manifest.Region, options.Parallel.Concurrency)
		if err != nil {
			return planManifest{}, "", err
		}
	}
	ordinal := uint64(manifest.Changed)
	firstSequence := uint64(scope.FirstHeight)
	if resumeFrontier != 0 {
		firstSequence = resumeFrontier + 1
	}
	rootCheckpointMatched := resumeFrontier == 0
	iteratePlanSource := func(iterCtx context.Context, callback func(int64, []dtypes.AccountStorageDiff) error) error {
		if options.Direct {
			return iterateArchiveDirectStorageDiffsContext(
				iterCtx, archive, options.IAVLCacheSize, options.IAVLConcurrency,
				scope.FirstHeight, scope.FinalHeight, callback,
			)
		}
		return iterateSealedDumpContext(iterCtx, sealedDump, dumpInfo, func(height int64, changeSet *iavl.ChangeSet) error {
			if height == 1 && scope.FirstHeight == 2 {
				return callback(height, nil)
			}
			canonical, err := statediff.CanonicalStorageDiff(changeSet)
			if err != nil {
				return fmt.Errorf("height %d canonical storage: %w", height, err)
			}
			return callback(height, canonical)
		})
	}
	err = runOrderedPipeline(ctx, firstSequence, options.Parallel.Concurrency, options.Parallel.Window,
		func(pipelineCtx context.Context, emit func(uint64, planTask) error) error {
			rollingDigest := common.Hash{}
			scanErr := iteratePlanSource(pipelineCtx, func(height int64, canonical []dtypes.AccountStorageDiff) error {
				if height == 1 && scope.FirstHeight == 2 {
					return nil
				}
				if height < scope.FirstHeight || height > scope.FinalHeight {
					return fmt.Errorf("source height %d is outside plan range %d-%d", height, scope.FirstHeight, scope.FinalHeight)
				}
				root, parent, err := rootCursor.Next(height)
				if err != nil {
					return err
				}
				if root == parent && len(canonical) != 0 {
					return fmt.Errorf("height %d has storage changes behind an equal-root short circuit", height)
				}
				rollingDigest, err = extendPlanPrefixDigest(rollingDigest, uint64(height), root, parent, canonical)
				if err != nil {
					return fmt.Errorf("hash plan source prefix at height %d: %w", height, err)
				}
				if uint64(height) <= resumeFrontier {
					record, err := readRootRecord(resumeRootReader)
					if err != nil {
						return fmt.Errorf("read resumable root height %d: %w", height, err)
					}
					if record.Height != uint64(height) || record.Root != root {
						return fmt.Errorf("resumable root index differs at height %d", height)
					}
					if uint64(height) == resumeFrontier {
						if rollingDigest != prefixDigest {
							return fmt.Errorf("plan checkpoint prefix differs from current source at height %d", resumeFrontier)
						}
						rootCheckpointMatched = true
					}
					return nil
				}
				if uint64(height) > resumeFrontier && !rootCheckpointMatched {
					return fmt.Errorf("plan checkpoint height %d was not found", resumeFrontier)
				}
				weight, err := canonicalObjectOperationBytes(canonical, root == parent)
				if err != nil {
					return fmt.Errorf("height %d canonical storage reservation: %w", height, err)
				}
				if err := limiter.Acquire(pipelineCtx, weight); err != nil {
					return err
				}
				task := planTask{
					height: uint64(height), root: root, parent: parent,
					key:       fmt.Sprintf("%s/%s/stateDiff", manifest.Prefix, strings.ToLower(root.Hex())),
					canonical: canonical, prefixSHA: rollingDigest, bytes: weight,
				}
				if err := emit(uint64(height), task); err != nil {
					limiter.Release(weight)
					return err
				}
				return nil
			})
			if scanErr != nil {
				return scanErr
			}
			if !rootCheckpointMatched {
				return fmt.Errorf("plan checkpoint height %d was not found", resumeFrontier)
			}
			if resumeRootReader != nil {
				if _, err := resumeRootReader.Peek(1); !errors.Is(err, io.EOF) {
					if err == nil {
						return fmt.Errorf("resumable root index has records after checkpoint height %d", resumeFrontier)
					}
					return err
				}
			}
			return nil
		},
		func(workerCtx context.Context, task planTask) (planOutcome, error) {
			outcome, err := processPlanTask(workerCtx, manifest.Bucket, task, objects)
			if err != nil {
				limiter.Release(task.bytes)
				return planOutcome{}, err
			}
			outcome.bytes = task.bytes
			return outcome, nil
		},
		func(_ uint64, outcome planOutcome) error {
			defer limiter.Release(outcome.bytes)
			writeReserve := planBufferedWriteReserve + uint64(rootRecordSize)
			if outcome.changed {
				recordBytes := estimatedPackRecordBytes(outcome.record)
				if recordBytes <= 0 || uint64(recordBytes) > ^uint64(0)-writeReserve {
					return fmt.Errorf("plan record write reservation overflows uint64")
				}
				writeReserve += uint64(recordBytes)
			}
			if options.MinFree > ^uint64(0)-writeReserve {
				return fmt.Errorf("plan free-space requirement overflows uint64")
			}
			if err := ensureFreeSpace(options.Output, options.MinFree+writeReserve); err != nil {
				return err
			}
			if err := writeRootRecord(rootBuffer, outcome.root, outcome.height); err != nil {
				return err
			}
			if outcome.changed {
				ordinal++
				outcome.record.Ordinal = ordinal
				if err := writer.Write(outcome.record); err != nil {
					return err
				}
			}
			accumulatePlanOutcome(&manifest, outcome)
			return saver.Advance(outcome.height, manifest, outcome.prefixSHA)
		},
	)
	flushErr := saver.Flush()
	if err != nil || flushErr != nil {
		return planManifest{}, "", errors.Join(err, flushErr)
	}
	expectedProcessed := scope.FinalHeight - scope.FirstHeight + 1
	if manifest.Processed != expectedProcessed {
		_ = rootIndex.Close()
		return planManifest{}, "", fmt.Errorf("processed %d heights, want %d", manifest.Processed, expectedProcessed)
	}
	if err := rootBuffer.Flush(); err != nil {
		_ = rootIndex.Close()
		return planManifest{}, "", err
	}
	if err := rootIndex.Sync(); err != nil {
		_ = rootIndex.Close()
		return planManifest{}, "", err
	}
	if err := rootIndex.Close(); err != nil {
		return planManifest{}, "", err
	}
	rootOpen = false
	if resumeRootFile != nil {
		if err := resumeRootFile.Close(); err != nil {
			return planManifest{}, "", err
		}
		resumeRootFile = nil
	}
	heightRootName := "roots.by-height"
	heightRootPath := filepath.Join(options.Output, heightRootName)
	if err := commitFileNoReplace(rootIndexPath, heightRootPath); err != nil {
		return planManifest{}, "", err
	}
	manifest.HeightRootIndex = heightRootName
	manifest.HeightRootIndexSHA256, _, err = hashFileContext(ctx, heightRootPath)
	if err != nil {
		return planManifest{}, "", err
	}
	rootStat, err := os.Stat(heightRootPath)
	if err != nil {
		return planManifest{}, "", err
	}
	if rootStat.Size() < 0 || uint64(rootStat.Size()) > ^uint64(0)/2 {
		return planManifest{}, "", fmt.Errorf("root index external sort reservation overflows uint64")
	}
	sortReserver, err := newFreeSpaceReserver(options.Output, options.MinFree)
	if err != nil {
		return planManifest{}, "", err
	}
	releaseSort, err := sortReserver.reserve(uint64(rootStat.Size()) * 2)
	if err != nil {
		return planManifest{}, "", fmt.Errorf("root index external sort: %w", err)
	}
	manifest.RootIndex, manifest.RootIndexSHA256, err = checkDuplicateRootsContext(ctx, heightRootPath, options.Output)
	releaseSort()
	if err != nil {
		return planManifest{}, "", err
	}
	heightMultiset, heightCount, err := rootMultisetSHA256Context(ctx, heightRootPath)
	if err != nil {
		return planManifest{}, "", err
	}
	sortedMultiset, sortedCount, err := rootMultisetSHA256Context(ctx, filepath.Join(options.Output, manifest.RootIndex))
	if err != nil {
		return planManifest{}, "", err
	}
	if heightCount != manifest.Processed || sortedCount != manifest.Processed || heightMultiset != sortedMultiset {
		return planManifest{}, "", fmt.Errorf("root indexes do not contain the same multiset")
	}
	manifest.RootMultisetSHA256 = heightMultiset
	manifest.Chunks, err = writer.Close()
	if err != nil {
		return planManifest{}, "", err
	}
	writerOpen = false
	finalIdentity, err := archive.identity()
	if err != nil {
		return planManifest{}, "", err
	}
	if finalIdentity != identity {
		return planManifest{}, "", fmt.Errorf("archive identity changed during plan")
	}
	if !options.Direct {
		if err := validateSealedDumpManifest(sealedDump, dumpInfo, dumpHash); err != nil {
			return planManifest{}, "", err
		}
		if err := validateSealedDumpArtifactsContext(ctx, sealedDump, dumpInfo); err != nil {
			return planManifest{}, "", fmt.Errorf("sealed dump changed during plan: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return planManifest{}, "", err
	}
	if _, err := atomicJSONWithMinFree(filepath.Join(options.Output, "manifest.v1.json"), manifest, options.MinFree); err != nil {
		return planManifest{}, "", err
	}
	if _, err := os.Lstat(sealedPlan); err == nil {
		return planManifest{}, "", fmt.Errorf("sealed plan already exists: %s", sealedPlan)
	} else if !errors.Is(err, os.ErrNotExist) {
		return planManifest{}, "", err
	}
	if err := syncDir(options.Output); err != nil {
		return planManifest{}, "", err
	}
	if err := ctx.Err(); err != nil {
		return planManifest{}, "", err
	}
	if err := renameNoReplace(options.Output, sealedPlan); err != nil {
		return planManifest{}, "", err
	}
	if err := syncDir(filepath.Dir(sealedPlan)); err != nil {
		return planManifest{}, "", err
	}
	return manifest, filepath.Join(sealedPlan, "manifest.v1.json"), nil
}

func reuseSealedPlanContext(ctx context.Context, sealedPath string, expected planManifest) (planManifest, string, bool, error) {
	if ctx == nil {
		return planManifest{}, "", false, fmt.Errorf("reuse sealed plan context is required")
	}
	if _, err := os.Lstat(sealedPath); errors.Is(err, os.ErrNotExist) {
		return planManifest{}, "", false, nil
	} else if err != nil {
		return planManifest{}, "", false, err
	}
	manifestPath := filepath.Join(sealedPath, "manifest.v1.json")
	manifest, _, err := loadPlanManifestContext(ctx, manifestPath)
	if err != nil {
		return planManifest{}, "", false, fmt.Errorf("validate existing sealed plan: %w", err)
	}
	if !samePlanIdentity(manifest, expected) {
		return planManifest{}, "", false, fmt.Errorf("existing sealed plan identity differs from this run")
	}
	return manifest, manifestPath, true, nil
}

func resolvePlanOutput(path string) (string, string, error) {
	path = filepath.Clean(path)
	if !strings.HasSuffix(path, ".staging") {
		return "", "", fmt.Errorf("plan output must end in .staging")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", "", err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return "", "", fmt.Errorf("resolve plan output parent: %w", err)
	}
	staging := filepath.Join(parent, filepath.Base(absolute))
	sealed := strings.TrimSuffix(staging, ".staging") + ".sealed"
	sealedInfo, sealedErr := os.Lstat(sealed)
	if sealedErr != nil && !errors.Is(sealedErr, os.ErrNotExist) {
		return "", "", sealedErr
	}
	stagingInfo, stagingErr := os.Lstat(staging)
	if stagingErr != nil && !errors.Is(stagingErr, os.ErrNotExist) {
		return "", "", stagingErr
	}
	if sealedErr == nil && stagingErr == nil {
		return "", "", fmt.Errorf("both staging and sealed plans exist: %s and %s", staging, sealed)
	}
	if sealedErr == nil && (!sealedInfo.IsDir() || sealedInfo.Mode()&os.ModeSymlink != 0) {
		return "", "", fmt.Errorf("sealed plan must be a non-symlink directory: %s", sealed)
	}
	if stagingErr == nil {
		if !stagingInfo.IsDir() || stagingInfo.Mode()&os.ModeSymlink != 0 {
			return "", "", fmt.Errorf("plan staging must be a non-symlink directory: %s", staging)
		}
		checkpointPath := filepath.Join(staging, "plan.checkpoint.json")
		if err := requireRegularFile(checkpointPath, "plan checkpoint"); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return "", "", fmt.Errorf("plan staging directory exists without a resumable checkpoint: %s", staging)
			}
			return "", "", err
		}
	}
	return staging, sealed, nil
}

func canonicalDumpArgument(path string) (string, error) {
	path = filepath.Clean(path)
	if !strings.HasSuffix(path, ".staging") && !strings.HasSuffix(path, ".sealed") {
		return "", fmt.Errorf("dump path must end in .staging or .sealed")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return "", fmt.Errorf("resolve dump parent: %w", err)
	}
	return filepath.Join(parent, filepath.Base(absolute)), nil
}

func processPlanTask(ctx context.Context, bucket string, task planTask, objects objectStore) (planOutcome, error) {
	outcome := planOutcome{height: task.height, root: task.root, prefixSHA: task.prefixSHA}
	if task.root == task.parent {
		if len(task.canonical) != 0 {
			return planOutcome{}, fmt.Errorf("height %d has storage changes behind an equal-root short circuit", task.height)
		}
		outcome.skipped = true
		return outcome, nil
	}
	object, err := objects.Get(ctx, bucket, task.key)
	if err != nil {
		return planOutcome{}, fmt.Errorf("get height %d key %s: %w", task.height, task.key, err)
	}
	record, changed, err := makePackRecord(task.height, task.key, object, task.root, task.parent, task.canonical)
	if err != nil {
		return planOutcome{}, fmt.Errorf("analyze height %d: %w", task.height, err)
	}
	outcome.changed = changed
	outcome.oldBytes = int64(len(object.Body))
	outcome.record = record
	return outcome, nil
}

func extendPlanPrefixDigest(
	previous common.Hash,
	height uint64,
	root, parent common.Hash,
	canonical []dtypes.AccountStorageDiff,
) (common.Hash, error) {
	canonicalBody, err := rlp.EncodeToBytes(canonical)
	if err != nil {
		return common.Hash{}, err
	}
	record := planPrefixRecord{
		Previous: previous, Height: height, Root: root, Parent: parent, CanonicalSHA: sha256Hash(canonicalBody),
	}
	body, err := rlp.EncodeToBytes(record)
	if err != nil {
		return common.Hash{}, err
	}
	return sha256Hash(body), nil
}

func accumulatePlanOutcome(manifest *planManifest, outcome planOutcome) {
	manifest.Processed++
	if outcome.skipped {
		manifest.SkippedEqualRoot++
		return
	}
	manifest.OldBytes += outcome.oldBytes
	if !outcome.changed {
		manifest.Unchanged++
		return
	}
	manifest.Changed++
	manifest.SlotsAdded += outcome.record.SlotsAdded
	manifest.SlotsRemoved += outcome.record.SlotsRemoved
	manifest.SlotsChanged += outcome.record.SlotsChanged
	manifest.NewBytes += int64(len(outcome.record.NewBody))
	if outcome.record.NoncanonicalOld {
		manifest.ChangedCanonical++
	}
	if outcome.record.ConflictingOld {
		manifest.ChangedConflict++
	}
}

func makePackRecord(height uint64, key string, object storedObject, root, parent common.Hash, canonical []dtypes.AccountStorageDiff) (packRecord, bool, error) {
	var old dtypes.BlockStorageDiff
	if err := rlp.DecodeBytes(object.Body, &old); err != nil {
		return packRecord{}, false, fmt.Errorf("decode state diff: %w", err)
	}
	if old.Hash != root || old.ParentHash != parent {
		return packRecord{}, false, fmt.Errorf("root mismatch: object %s/%s archive %s/%s", old.Hash, old.ParentHash, root, parent)
	}
	newBody, err := replaceStorageDiffRLP(object.Body, canonical)
	if err != nil {
		return packRecord{}, false, err
	}
	if err := verifyReencoded(old, canonical, newBody); err != nil {
		return packRecord{}, false, err
	}
	if err := verifyRawRetainedFields(object.Body, newBody); err != nil {
		return packRecord{}, false, err
	}
	if bytes.Equal(object.Body, newBody) {
		return packRecord{}, false, nil
	}
	retained, err := retainedDigest(old)
	if err != nil {
		return packRecord{}, false, err
	}
	return packRecord{
		Schema: packSchema, Height: height, Key: key, OldETag: object.ETag,
		OldBody: object.Body, NewBody: newBody, OldSHA256: sha256Hash(object.Body), NewSHA256: sha256Hash(newBody),
		Headers: object.Headers, RetainedSHA256: retained,
	}, true, nil
}

type commitInfoReader interface {
	commitInfo(int64) (*storetypes.CommitInfo, error)
}

func archiveRoots(archive commitInfoReader, height int64) (common.Hash, common.Hash, error) {
	rootInfo, err := archive.commitInfo(height - 1)
	if err != nil {
		return common.Hash{}, common.Hash{}, err
	}
	root := common.BytesToHash(rootInfo.Hash())
	if height == 2 {
		return root, common.Hash{}, nil
	}
	parentInfo, err := archive.commitInfo(height - 2)
	if err != nil {
		return common.Hash{}, common.Hash{}, err
	}
	return root, common.BytesToHash(parentInfo.Hash()), nil
}

func verifyReencoded(old dtypes.BlockStorageDiff, canonical []dtypes.AccountStorageDiff, body []byte) error {
	var decoded dtypes.BlockStorageDiff
	if err := rlp.DecodeBytes(body, &decoded); err != nil {
		return err
	}
	decodedStorage, err := rlp.EncodeToBytes(decoded.StorageDiff)
	if err != nil {
		return err
	}
	canonicalStorage, err := rlp.EncodeToBytes(canonical)
	if err != nil {
		return err
	}
	if !bytes.Equal(decodedStorage, canonicalStorage) {
		return fmt.Errorf("replacement storage is not canonical")
	}
	oldRetained, err := retainedDigest(old)
	if err != nil {
		return err
	}
	newRetained, err := retainedDigest(decoded)
	if err != nil {
		return err
	}
	if oldRetained != newRetained {
		return fmt.Errorf("replacement changed retained fields")
	}
	return nil
}

func ensureFreeSpace(path string, minimum uint64) error {
	if minimum == 0 {
		return fmt.Errorf("min-free-bytes must be greater than zero")
	}
	available, _, err := filesystemSpace(path)
	if err != nil {
		return err
	}
	if available < minimum {
		return fmt.Errorf("filesystem free space %d is below min-free-bytes %d", available, minimum)
	}
	return nil
}

func buildCommits() (string, string, string) {
	cronosCommit := version.Commit
	ethermintCommit, iavlCommit := unknownBuildIdentity, unknownBuildIdentity
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" && cronosCommit == "" {
				cronosCommit = setting.Value
			}
		}
		for _, dependency := range info.Deps {
			switch dependency.Path {
			case "github.com/evmos/ethermint":
				ethermintCommit = moduleVersionCommit(dependency)
			case "github.com/cosmos/iavl":
				iavlCommit = moduleVersionCommit(dependency)
			}
		}
	}
	if cronosCommit == "" {
		cronosCommit = unknownBuildIdentity
	}
	return cronosCommit, ethermintCommit, iavlCommit
}

func moduleVersionCommit(dependency *debug.Module) string {
	if dependency == nil {
		return unknownBuildIdentity
	}
	version := dependency.Version
	if dependency.Replace != nil {
		version = dependency.Replace.Version
	}
	parts := strings.Split(version, "-")
	if len(parts) < 3 || parts[len(parts)-1] == "" {
		return unknownBuildIdentity
	}
	return parts[len(parts)-1]
}
