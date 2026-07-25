package main

import (
	"bytes"
	"compress/zlib"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	dbm "github.com/cosmos/cosmos-db"
	"github.com/cosmos/iavl"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"cosmossdk.io/log"
	"cosmossdk.io/store/rootmulti"
	"cosmossdk.io/store/wrapper"

	"github.com/cosmos/cosmos-sdk/version"
)

const (
	defaultDumpConcurrency = 8
	defaultDumpChunkSize   = int64(1_000_000)
	defaultDumpCacheSize   = 10_000
	evmIAVLPrefix          = "s/k:evm/"
)

type dumpOptions struct {
	Home                   string
	Output                 string
	FirstVersion           int64
	LastVersion            int64
	Concurrency            int
	ChunkSize              int64
	CacheSize              int
	ZlibLevel              int
	MinFree                uint64
	LegacyTempDir          string
	LegacySortChunkSize    int64
	LegacyMaxSortChunks    int
	LegacyReferenceSamples int
	SnapshotID             string
	ImageDigest            string
	StopAfterLegacy        bool
	LegacyTrustNodeSet     bool
}

type dumpReport struct {
	Phase                    string            `json:"phase"`
	Output                   string            `json:"output"`
	FirstVersion             int64             `json:"first_version"`
	LastVersion              int64             `json:"last_version"`
	CompletedLastVersion     int64             `json:"completed_last_version"`
	TargetRecords            int64             `json:"target_records"`
	Files                    int               `json:"files"`
	GeneratedFiles           int64             `json:"generated_files"`
	ReusedFiles              int64             `json:"reused_files"`
	Records                  int64             `json:"records"`
	GeneratedRecords         int64             `json:"generated_records"`
	ReusedRecords            int64             `json:"reused_records"`
	Concurrency              int               `json:"concurrency"`
	CacheSize                int               `json:"cache_size_per_worker"`
	ZlibLevel                int               `json:"zlib_level"`
	DurationSeconds          float64           `json:"duration_seconds"`
	HandledBlocksPerSecond   float64           `json:"handled_blocks_per_second"`
	GeneratedBlocksPerSecond float64           `json:"generated_blocks_per_second"`
	LegacyLatestVersion      int64             `json:"legacy_latest_version"`
	LegacyScan               *legacyScanReport `json:"legacy_scan,omitempty"`
	ModernRecords            int64             `json:"modern_records"`
	LegacyReferenceSamples   int               `json:"legacy_reference_samples"`
}

type versionRange struct {
	First int64
	Last  int64
}

type stateChangeTraverser interface {
	TraverseStateChanges(int64, int64, func(int64, *iavl.ChangeSet) error) error
}

type stateChangeSource interface {
	stateChangeTraverser
	VersionHash(int64) ([]byte, error)
}

func newDumpCommand() *cobra.Command {
	options := dumpOptions{
		Concurrency:            defaultConcurrency(),
		ChunkSize:              defaultDumpChunkSize,
		CacheSize:              defaultDumpCacheSize,
		ZlibLevel:              zlib.BestSpeed,
		LegacySortChunkSize:    defaultLegacySortChunkBytes,
		LegacyMaxSortChunks:    defaultLegacySortMaxChunks,
		LegacyReferenceSamples: 256,
	}
	command := &cobra.Command{
		Use:   "dump",
		Short: "Extract an explicit EVM IAVL version range without scanning the full node prefix",
		RunE: func(command *cobra.Command, _ []string) error {
			report, err := runDump(command.Context(), options)
			if err != nil {
				return err
			}
			return printJSON(report)
		},
	}
	command.Flags().StringVar(&options.Home, "home", "", "stopped Cronos node home with a read-only RocksDB application.db")
	command.Flags().StringVar(&options.Output, "output", "", "changeset dump directory ending in .staging")
	command.Flags().Int64Var(&options.FirstVersion, "first-version", 0, "first IAVL version to extract, inclusive")
	command.Flags().Int64Var(&options.LastVersion, "last-version", 0, "last IAVL version to extract, inclusive")
	command.Flags().IntVar(&options.Concurrency, "concurrency", options.Concurrency, "number of IAVL traversal workers")
	command.Flags().Int64Var(&options.ChunkSize, "chunk-size", options.ChunkSize, "maximum versions per output file")
	command.Flags().IntVar(&options.CacheSize, "iavl-cache-size", options.CacheSize, "IAVL node-cache entries per worker")
	command.Flags().IntVar(&options.ZlibLevel, "zlib-level", options.ZlibLevel, "zlib compression level (0-9)")
	command.Flags().Uint64Var(&options.MinFree, "min-free-bytes", 0, "stop before filesystem free space falls below this value")
	command.Flags().StringVar(&options.LegacyTempDir, "legacy-temp-dir", "", "temporary directory for bounded legacy external sorts; defaults to the dump parent")
	command.Flags().Int64Var(&options.LegacySortChunkSize, "legacy-sort-chunk-bytes", options.LegacySortChunkSize, "maximum in-memory bytes per legacy external-sort run")
	command.Flags().IntVar(&options.LegacyMaxSortChunks, "legacy-max-sort-chunks", options.LegacyMaxSortChunks, "maximum input files opened by each legacy external-sort merge")
	command.Flags().StringVar(&options.SnapshotID, "snapshot-id", "", "snapshot or immutable clone identity used to create this archive")
	command.Flags().StringVar(&options.ImageDigest, "image-digest", "", "immutable rewriter image digest")
	command.Flags().IntVar(&options.LegacyReferenceSamples, "legacy-reference-samples", options.LegacyReferenceSamples, "evenly spaced legacy versions compared with reference traversal")
	command.Flags().BoolVar(&options.StopAfterLegacy, "stop-after-legacy", false, "prepare the legacy prefix of a full dump and stop before modern traversal")
	command.Flags().BoolVar(&options.LegacyTrustNodeSet, "legacy-trust-node-set", false, "trust the frozen legacy node set and skip graph reachability validation")
	_ = command.MarkFlagRequired("home")
	_ = command.MarkFlagRequired("output")
	_ = command.MarkFlagRequired("first-version")
	_ = command.MarkFlagRequired("last-version")
	_ = command.MarkFlagRequired("min-free-bytes")
	_ = command.MarkFlagRequired("snapshot-id")
	_ = command.MarkFlagRequired("image-digest")
	return command
}

func runDump(ctx context.Context, options dumpOptions) (dumpReport, error) {
	started := time.Now()
	if options.FirstVersion < 1 || options.LastVersion < options.FirstVersion {
		return dumpReport{}, fmt.Errorf("invalid version range %d-%d", options.FirstVersion, options.LastVersion)
	}
	if options.Concurrency < 1 {
		return dumpReport{}, fmt.Errorf("concurrency must be positive")
	}
	if options.ChunkSize < 1 {
		return dumpReport{}, fmt.Errorf("chunk-size must be positive")
	}
	if options.LegacyReferenceSamples < 0 {
		return dumpReport{}, fmt.Errorf("legacy-reference-samples must not be negative")
	}
	if options.CacheSize < 0 {
		return dumpReport{}, fmt.Errorf("iavl-cache-size must not be negative")
	}
	if options.ZlibLevel < zlib.NoCompression || options.ZlibLevel > zlib.BestCompression {
		return dumpReport{}, fmt.Errorf("zlib-level must be between %d and %d", zlib.NoCompression, zlib.BestCompression)
	}
	if options.SnapshotID == "" || options.ImageDigest == "" {
		return dumpReport{}, fmt.Errorf("snapshot-id and image-digest are required")
	}
	cronosCommit, ethermintCommit, iavlCommit := buildCommits()
	if err := requireRuntimeBuild(cronosCommit, ethermintCommit, iavlCommit, options.ImageDigest, version.BuildTags); err != nil {
		return dumpReport{}, err
	}
	output, err := resolveDumpOutput(options.Output)
	if err != nil {
		return dumpReport{}, err
	}
	locks, err := acquireStagingLocks(output)
	if err != nil {
		return dumpReport{}, err
	}
	defer func() { _ = releaseStagingLocks(locks) }()
	if _, err := resolveDumpOutput(output); err != nil {
		return dumpReport{}, err
	}
	archive, err := openArchive(options.Home)
	if err != nil {
		return dumpReport{}, err
	}
	defer archive.Close()
	latest := rootmulti.GetLatestVersion(archive.db)
	if options.LastVersion > latest {
		return dumpReport{}, fmt.Errorf("last-version %d exceeds archive latest version %d", options.LastVersion, latest)
	}
	identity, err := archive.identity()
	if err != nil {
		return dumpReport{}, err
	}
	if identity.LatestVersion != latest {
		return dumpReport{}, fmt.Errorf("archive latest version changed while reading identity: %d to %d", latest, identity.LatestVersion)
	}
	dumpSchema := pilotDumpManifestSchema
	if options.FirstVersion == 1 && options.LastVersion == latest {
		dumpSchema = dumpManifestSchema
	}
	prefix := []byte(evmIAVLPrefix)
	evmDB := wrapper.NewDBWrapper(dbm.NewPrefixDB(archive.db, prefix))
	legacyLatest, err := legacyLatestRootVersion(evmDB)
	if err != nil {
		return dumpReport{}, err
	}
	legacyRanges, modernRanges, ranges, err := splitDumpRangesAtLegacy(
		options.FirstVersion, options.LastVersion, legacyLatest, options.Concurrency, options.ChunkSize,
	)
	if err != nil {
		return dumpReport{}, err
	}
	if err := validateLegacyPreparation(options, latest, legacyRanges, modernRanges); err != nil {
		return dumpReport{}, err
	}
	if len(legacyRanges) != 0 && options.LegacyReferenceSamples < 1 {
		return dumpReport{}, fmt.Errorf("legacy-reference-samples must be positive when extracting legacy versions")
	}
	evmDir, err := ensureDumpOutput(output)
	if err != nil {
		return dumpReport{}, err
	}
	if err := ensureFreeSpace(output, options.MinFree); err != nil {
		return dumpReport{}, err
	}
	space, err := newFreeSpaceReserver(output, options.MinFree)
	if err != nil {
		return dumpReport{}, err
	}
	sourceContext := dumpContext{
		Schema: dumpSchema, FirstVersion: options.FirstVersion, LastVersion: options.LastVersion,
		SnapshotID: options.SnapshotID, ArchiveIdentity: identity,
		CronosCommit: cronosCommit, EthermintCommit: ethermintCommit, IAVLCommit: iavlCommit,
		ImageDigest: options.ImageDigest, BuildTags: version.BuildTags,
	}
	if options.LegacyTrustNodeSet {
		sourceContext.LegacyValidation = legacyValidationTrustedSet
	}
	sourceHash, err := ensureDumpSource(output, sourceContext)
	if err != nil {
		return dumpReport{}, err
	}
	if err := validateDumpOutput(evmDir, ranges); err != nil {
		return dumpReport{}, err
	}

	var generatedFiles, reusedFiles, generatedRecords, reusedRecords atomic.Int64
	var legacyReport *legacyScanReport
	legacyReferenceSamples := 0
	if len(legacyRanges) != 0 {
		tempDir := options.LegacyTempDir
		if tempDir == "" {
			tempDir = filepath.Dir(output)
		}
		result, err := writeLegacyDumpRanges(
			ctx,
			iavlLegacyRecordSource{db: evmDB},
			legacyScanOptions{
				TempDir: tempDir, FirstVersion: legacyRanges[0].First,
				LastVersion:   legacyRanges[len(legacyRanges)-1].Last,
				SortChunkSize: options.LegacySortChunkSize, MaxSortChunks: options.LegacyMaxSortChunks,
				MinFree: options.MinFree, TrustNodeSet: options.LegacyTrustNodeSet,
			},
			evmDir, legacyRanges, options.ZlibLevel, options.MinFree,
		)
		if err != nil {
			return dumpReport{}, fmt.Errorf("extract legacy versions: %w", err)
		}
		if result.Scanned {
			legacyReport = &result.Scan
		}
		generatedFiles.Add(result.GeneratedFiles)
		reusedFiles.Add(result.ReusedFiles)
		generatedRecords.Add(result.GeneratedRecords)
		reusedRecords.Add(result.ReusedRecords)
		if options.LegacyReferenceSamples > 0 {
			reference := iavl.NewImmutableTree(evmDB, options.CacheSize, true, log.NewNopLogger())
			legacyReferenceSamples, err = verifyLegacyDumpSamples(
				ctx, evmDir, legacyRanges, reference, options.LegacyReferenceSamples,
			)
			if err != nil {
				return dumpReport{}, err
			}
		}
	}
	if options.StopAfterLegacy {
		if err := validateDumpOutputComplete(evmDir, legacyRanges); err != nil {
			return dumpReport{}, err
		}
		if err := validateDumpSourceUnchanged(archive, identity, output, sourceContext, sourceHash); err != nil {
			return dumpReport{}, err
		}
		duration := time.Since(started)
		records := countVersionRanges(legacyRanges)
		return dumpReport{
			Phase: "legacy-prepared", Output: output, FirstVersion: options.FirstVersion, LastVersion: options.LastVersion,
			CompletedLastVersion: legacyRanges[len(legacyRanges)-1].Last,
			TargetRecords:        options.LastVersion - options.FirstVersion + 1,
			Files:                len(legacyRanges), GeneratedFiles: generatedFiles.Load(), ReusedFiles: reusedFiles.Load(), Records: records,
			GeneratedRecords: generatedRecords.Load(), ReusedRecords: reusedRecords.Load(), Concurrency: 1,
			CacheSize: options.CacheSize, ZlibLevel: options.ZlibLevel, DurationSeconds: duration.Seconds(),
			HandledBlocksPerSecond:   float64(records) / duration.Seconds(),
			GeneratedBlocksPerSecond: float64(generatedRecords.Load()) / duration.Seconds(),
			LegacyLatestVersion:      legacyLatest, LegacyScan: legacyReport,
			ModernRecords: countVersionRanges(modernRanges), LegacyReferenceSamples: legacyReferenceSamples,
		}, nil
	}

	workerCount := options.Concurrency
	if workerCount > len(modernRanges) {
		workerCount = len(modernRanges)
	}
	if workerCount > 0 {
		jobs := make(chan versionRange)
		group, groupCtx := errgroup.WithContext(ctx)
		for range workerCount {
			group.Go(func() error {
				tree := iavl.NewImmutableTree(
					wrapper.NewDBWrapper(dbm.NewPrefixDB(archive.db, prefix)),
					options.CacheSize,
					true,
					log.NewNopLogger(),
				)
				for blockRange := range jobs {
					if err := groupCtx.Err(); err != nil {
						return err
					}
					path := dumpRangePath(evmDir, blockRange)
					if _, err := os.Lstat(path); err == nil {
						if err := validateReusableDumpRange(groupCtx, path, blockRange); err != nil {
							return err
						}
						reusedFiles.Add(1)
						reusedRecords.Add(blockRange.Last - blockRange.First + 1)
						continue
					} else if !errors.Is(err, os.ErrNotExist) {
						return err
					}
					if err := ensureFreeSpace(output, options.MinFree); err != nil {
						return err
					}
					if err := writeDumpRange(groupCtx, path, tree, blockRange, options.ZlibLevel, space); err != nil {
						return fmt.Errorf("extract versions %d-%d: %w", blockRange.First, blockRange.Last, err)
					}
					generatedFiles.Add(1)
					generatedRecords.Add(blockRange.Last - blockRange.First + 1)
				}
				return nil
			})
		}
		group.Go(func() error {
			defer close(jobs)
			for _, blockRange := range modernRanges {
				select {
				case jobs <- blockRange:
				case <-groupCtx.Done():
					return groupCtx.Err()
				}
			}
			return nil
		})
		if err := group.Wait(); err != nil {
			return dumpReport{}, err
		}
	}
	if err := validateDumpOutputComplete(evmDir, ranges); err != nil {
		return dumpReport{}, err
	}
	if err := validateDumpSourceUnchanged(archive, identity, output, sourceContext, sourceHash); err != nil {
		return dumpReport{}, err
	}

	duration := time.Since(started)
	records := options.LastVersion - options.FirstVersion + 1
	return dumpReport{
		Phase: "complete", Output: output, FirstVersion: options.FirstVersion, LastVersion: options.LastVersion,
		CompletedLastVersion: options.LastVersion, TargetRecords: records,
		Files: len(ranges), GeneratedFiles: generatedFiles.Load(), ReusedFiles: reusedFiles.Load(), Records: records,
		GeneratedRecords: generatedRecords.Load(), ReusedRecords: reusedRecords.Load(),
		Concurrency: workerCount, CacheSize: options.CacheSize, ZlibLevel: options.ZlibLevel,
		DurationSeconds: duration.Seconds(), HandledBlocksPerSecond: float64(records) / duration.Seconds(),
		GeneratedBlocksPerSecond: float64(generatedRecords.Load()) / duration.Seconds(),
		LegacyLatestVersion:      legacyLatest, LegacyScan: legacyReport,
		ModernRecords: countVersionRanges(modernRanges), LegacyReferenceSamples: legacyReferenceSamples,
	}, nil
}

func validateLegacyPreparation(
	options dumpOptions,
	latest int64,
	legacyRanges, modernRanges []versionRange,
) error {
	if options.LegacyTrustNodeSet && len(legacyRanges) == 0 {
		return fmt.Errorf("legacy-trust-node-set requires legacy versions")
	}
	if !options.StopAfterLegacy {
		return nil
	}
	if options.FirstVersion != 1 || options.LastVersion != latest {
		return fmt.Errorf(
			"stop-after-legacy requires the full archive range 1-%d, got %d-%d",
			latest, options.FirstVersion, options.LastVersion,
		)
	}
	if len(legacyRanges) == 0 {
		return fmt.Errorf("stop-after-legacy requires legacy versions")
	}
	if len(modernRanges) == 0 {
		return fmt.Errorf("stop-after-legacy requires a modern suffix")
	}
	return nil
}

func validateDumpSourceUnchanged(
	archive *archiveReader,
	expectedIdentity archiveIdentity,
	output string,
	expectedContext dumpContext,
	expectedHash string,
) error {
	identity, err := archive.identity()
	if err != nil {
		return err
	}
	if identity != expectedIdentity {
		return fmt.Errorf("archive identity changed during extraction: got %+v, want %+v", identity, expectedIdentity)
	}
	source, sourceHash, found, err := loadDumpSource(filepath.Join(output, dumpSourceFileName))
	if err != nil {
		return err
	}
	if !found || source.Context != expectedContext || sourceHash != expectedHash {
		return fmt.Errorf("dump source identity changed during extraction")
	}
	return nil
}

func resolveDumpOutput(path string) (string, error) {
	path = filepath.Clean(path)
	if !strings.HasSuffix(path, ".staging") {
		return "", fmt.Errorf("output must end in .staging")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return "", fmt.Errorf("resolve dump output parent: %w", err)
	}
	output := filepath.Join(parent, filepath.Base(absolute))
	if info, err := os.Lstat(output); err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("dump staging must be a non-symlink directory: %s", output)
		}
		evmDir := filepath.Join(output, "evm")
		if evmInfo, evmErr := os.Lstat(evmDir); evmErr == nil {
			if !evmInfo.IsDir() || evmInfo.Mode()&os.ModeSymlink != 0 {
				return "", fmt.Errorf("dump EVM output must be a non-symlink directory: %s", evmDir)
			}
		} else if !errors.Is(evmErr, os.ErrNotExist) {
			return "", evmErr
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	return output, nil
}

func ensureDumpOutput(output string) (string, error) {
	if info, err := os.Lstat(output); errors.Is(err, os.ErrNotExist) {
		if err := os.Mkdir(output, 0o755); err != nil {
			return "", err
		}
	} else if err != nil {
		return "", err
	} else if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("dump staging must be a non-symlink directory: %s", output)
	}
	evmDir := filepath.Join(output, "evm")
	if info, err := os.Lstat(evmDir); errors.Is(err, os.ErrNotExist) {
		if err := os.Mkdir(evmDir, 0o755); err != nil {
			return "", err
		}
	} else if err != nil {
		return "", err
	} else if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("dump EVM output must be a non-symlink directory: %s", evmDir)
	}
	return evmDir, nil
}

func splitDumpRangesAtLegacy(
	first, last, legacyLatest int64,
	concurrency int,
	maxChunkSize int64,
) (legacy, modern, all []versionRange, returnErr error) {
	if first < 1 || last < first {
		return nil, nil, nil, fmt.Errorf("invalid version range %d-%d", first, last)
	}
	if legacyLatest < 0 {
		return nil, nil, nil, fmt.Errorf("invalid latest legacy version %d", legacyLatest)
	}
	if first <= legacyLatest {
		legacyEnd := last
		if legacyEnd > legacyLatest {
			legacyEnd = legacyLatest
		}
		var err error
		legacy, err = splitDumpRanges(first, legacyEnd, concurrency, maxChunkSize)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	modernFirst := first
	if modernFirst <= legacyLatest {
		modernFirst = legacyLatest + 1
	}
	if modernFirst <= last {
		var err error
		modern, err = splitDumpRanges(modernFirst, last, concurrency, maxChunkSize)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	all = append(append(make([]versionRange, 0, len(legacy)+len(modern)), legacy...), modern...)
	return legacy, modern, all, nil
}

func countVersionRanges(ranges []versionRange) int64 {
	var count int64
	for _, blockRange := range ranges {
		count += blockRange.Last - blockRange.First + 1
	}
	return count
}

func splitDumpRanges(first, last int64, concurrency int, maxChunkSize int64) ([]versionRange, error) {
	if first < 1 || last < first {
		return nil, fmt.Errorf("invalid version range %d-%d", first, last)
	}
	if concurrency < 1 {
		return nil, fmt.Errorf("concurrency must be positive")
	}
	if maxChunkSize < 1 {
		return nil, fmt.Errorf("chunk-size must be positive")
	}
	total := last - first + 1
	chunkSize := (total-1)/int64(concurrency) + 1
	if chunkSize > maxChunkSize {
		chunkSize = maxChunkSize
	}
	ranges := make([]versionRange, 0, (total-1)/chunkSize+1)
	for begin := first; begin <= last; begin += chunkSize {
		end := begin + chunkSize - 1
		if end > last {
			end = last
		}
		ranges = append(ranges, versionRange{First: begin, Last: end})
	}
	return ranges, nil
}

func dumpRangePath(evmDir string, blockRange versionRange) string {
	return filepath.Join(evmDir, fmt.Sprintf("block-%d.zz", blockRange.First))
}

func validateDumpOutput(evmDir string, ranges []versionRange) error {
	allowed := make(map[string]struct{}, len(ranges)*2)
	for _, blockRange := range ranges {
		name := filepath.Base(dumpRangePath(evmDir, blockRange))
		allowed[name] = struct{}{}
		allowed[name+".partial"] = struct{}{}
	}
	entries, err := os.ReadDir(evmDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if _, ok := allowed[entry.Name()]; !ok {
			return fmt.Errorf("unexpected file in dump output: %s", entry.Name())
		}
		path := filepath.Join(evmDir, entry.Name())
		if err := requireRegularFile(path, "dump output entry"); err != nil {
			return err
		}
	}
	return nil
}

func validateDumpOutputComplete(evmDir string, ranges []versionRange) error {
	for _, blockRange := range ranges {
		path := dumpRangePath(evmDir, blockRange)
		if err := requireRegularFile(path, "dump output"); err != nil {
			return err
		}
		if _, err := os.Lstat(path + ".partial"); err == nil {
			return fmt.Errorf("partial dump remains after extraction: %s", path+".partial")
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func validateDumpRangeFile(path string, blockRange versionRange) error {
	return validateDumpRangeFileContext(context.Background(), path, blockRange)
}

func validateDumpRangeFileContext(ctx context.Context, path string, blockRange versionRange) error {
	manifest, err := scanZlibChangeSetsContext(ctx, path, nil)
	if err != nil {
		return fmt.Errorf("validate existing dump %s: %w", path, err)
	}
	wantRecords := blockRange.Last - blockRange.First + 1
	if manifest.FirstVersion != blockRange.First || manifest.LastVersion != blockRange.Last || manifest.Records != wantRecords {
		return fmt.Errorf(
			"dump %s contains versions %d-%d (%d records), want %d-%d (%d records)",
			path, manifest.FirstVersion, manifest.LastVersion, manifest.Records,
			blockRange.First, blockRange.Last, wantRecords,
		)
	}
	return nil
}

func validateReusableDumpRange(ctx context.Context, path string, blockRange versionRange) error {
	if err := requireRegularFile(path, "existing dump output"); err != nil {
		return err
	}
	if err := validateDumpRangeFileContext(ctx, path, blockRange); err != nil {
		return err
	}
	partial := path + ".partial"
	if err := requireRegularFile(partial, "stale partial dump"); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if err := os.Remove(partial); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func writeDumpRange(
	ctx context.Context,
	path string,
	tree stateChangeSource,
	blockRange versionRange,
	zlibLevel int,
	reservations ...*freeSpaceReserver,
) (returnErr error) {
	partial := path + ".partial"
	file, err := openLockedPartial(partial)
	if err != nil {
		return err
	}
	fileOpen := true
	defer func() {
		if fileOpen {
			if err := file.Close(); returnErr == nil {
				returnErr = err
			}
		}
	}()
	var output io.Writer = file
	if len(reservations) > 1 {
		return fmt.Errorf("write dump range has multiple free-space reservers")
	}
	if len(reservations) == 1 {
		if reservations[0] == nil {
			return fmt.Errorf("write dump range has a nil free-space reserver")
		}
		output = reservedWriter{writer: file, reserver: reservations[0]}
	}
	writer, err := zlib.NewWriterLevel(output, zlibLevel)
	if err != nil {
		return err
	}
	writerOpen := true
	defer func() {
		if writerOpen {
			if err := writer.Close(); returnErr == nil {
				returnErr = err
			}
		}
	}()

	expected := blockRange.First
	err = traverseVerifiedStateChanges(tree, blockRange.First, blockRange.Last, func(version int64, changeSet *iavl.ChangeSet) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if version != expected {
			return fmt.Errorf("traversal returned version %d, want %d", version, expected)
		}
		if err := writeChangeSet(writer, version, changeSet); err != nil {
			return err
		}
		expected++
		return nil
	})
	if err != nil {
		return err
	}
	if expected != blockRange.Last+1 {
		return fmt.Errorf("traversal ended at version %d, want %d", expected-1, blockRange.Last)
	}
	if err := writer.Close(); err != nil {
		return err
	}
	writerOpen = false
	if err := file.Sync(); err != nil {
		return err
	}
	if err := commitFileNoReplace(partial, path); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	fileOpen = false
	return nil
}

func traverseVerifiedStateChanges(
	source stateChangeSource,
	first, last int64,
	callback func(int64, *iavl.ChangeSet) error,
) error {
	if source == nil {
		return fmt.Errorf("state change source and callback are required")
	}
	return traverseVerifiedStateChangesWith(source, first, last, source.TraverseStateChanges, callback)
}

func traverseVerifiedStateChangesWith(
	source stateChangeSource,
	first, last int64,
	traverse func(int64, int64, func(int64, *iavl.ChangeSet) error) error,
	callback func(int64, *iavl.ChangeSet) error,
) error {
	if source == nil || traverse == nil || callback == nil {
		return fmt.Errorf("state change source and callback are required")
	}
	if first < 1 || last < first {
		return fmt.Errorf("invalid verified traversal range %d-%d", first, last)
	}
	versions := make([]int64, 0, 3)
	if first > 1 {
		versions = append(versions, first-1)
	}
	versions = append(versions, first)
	if last != first {
		versions = append(versions, last)
	}
	hashes := make(map[int64][]byte, len(versions))
	for _, version := range versions {
		hash, err := source.VersionHash(version)
		if err != nil {
			return fmt.Errorf("resolve IAVL version %d root before traversal: %w", version, err)
		}
		if len(hash) != legacyHashLength {
			return fmt.Errorf("IAVL version %d root hash has length %d", version, len(hash))
		}
		hashes[version] = hash
	}
	if err := traverse(first, last, callback); err != nil {
		return err
	}
	for _, version := range versions {
		hash, err := source.VersionHash(version)
		if err != nil {
			return fmt.Errorf("resolve IAVL version %d root after traversal: %w", version, err)
		}
		if !bytes.Equal(hash, hashes[version]) {
			return fmt.Errorf("IAVL version %d root changed during traversal", version)
		}
	}
	return nil
}

func defaultConcurrency() int {
	if runtime.NumCPU() < defaultDumpConcurrency {
		return runtime.NumCPU()
	}
	return defaultDumpConcurrency
}
