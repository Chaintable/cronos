package main

import (
	"bufio"
	"compress/zlib"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cosmos/iavl"
	"github.com/ethereum/go-ethereum/common"
	"github.com/evmos/ethermint/debank/statediff"
	dtypes "github.com/evmos/ethermint/debank/types"
	"github.com/spf13/cobra"
)

const (
	refillCheckpointSchema          = "statediff-rewriter-refill-checkpoint/v1"
	refillDirectoryCheckpointSchema = "statediff-rewriter-refill-directory-checkpoint/v1"
	maxRefillHeights                = int64(10_000)
	refillCheckpointEvery           = uint64(1_000)
	refillProgressEvery             = uint64(10_000)
)

type refillOptions struct {
	ZZFile      string
	ZZDir       string
	ArchiveHome string
	Checkpoint  string
	FirstHeight int64
	FinalHeight int64
	Write       bool
	Bucket      string
	Prefix      string
	Region      string
	Concurrency int
	Window      int
}

type refillTask struct {
	height    int64
	key       string
	canonical []dtypes.AccountStorageDiff
	skip      bool
}

type refillOutcome struct {
	height    int64
	gets      uint64
	puts      uint64
	wouldPUTs uint64
	skipped   uint64
	oldBytes  uint64
	newBytes  uint64
}

type refillCheckpoint struct {
	Schema      string `json:"schema"`
	Mode        string `json:"mode"`
	ZZFile      string `json:"zz_file,omitempty"`
	ZZDir       string `json:"zz_dir,omitempty"`
	ArchiveHome string `json:"archive_home"`
	FirstHeight int64  `json:"first_height"`
	FinalHeight int64  `json:"final_height"`
	Frontier    int64  `json:"frontier"`
	Processed   uint64 `json:"processed"`
	Gets        uint64 `json:"gets"`
	Puts        uint64 `json:"puts"`
	WouldPUTs   uint64 `json:"would_puts"`
	Skipped     uint64 `json:"skipped_equal_root"`
	OldBytes    uint64 `json:"old_bytes"`
	NewBytes    uint64 `json:"new_bytes"`
}

type refillReport struct {
	Mode                string  `json:"mode"`
	ZZFile              string  `json:"zz_file,omitempty"`
	ZZDir               string  `json:"zz_dir,omitempty"`
	ArchiveHome         string  `json:"archive_home"`
	FirstHeight         int64   `json:"first_height"`
	FinalHeight         int64   `json:"final_height"`
	Frontier            int64   `json:"frontier"`
	Processed           uint64  `json:"processed"`
	InvocationProcessed uint64  `json:"invocation_processed"`
	Gets                uint64  `json:"gets"`
	Puts                uint64  `json:"puts"`
	WouldPUTs           uint64  `json:"would_puts"`
	SkippedEqualRoot    uint64  `json:"skipped_equal_root"`
	OldBytes            uint64  `json:"old_bytes"`
	NewBytes            uint64  `json:"new_bytes"`
	StartedAt           string  `json:"started_at"`
	FinishedAt          string  `json:"finished_at"`
	ElapsedSeconds      float64 `json:"elapsed_seconds"`
	BlocksPerSecond     float64 `json:"blocks_per_second"`
}

func newRefillCommand() *cobra.Command {
	options := refillOptions{
		Bucket: defaultBucket, Prefix: defaultPrefix, Region: defaultRegion,
		Concurrency: defaultObjectConcurrency, Window: defaultObjectWindow,
	}
	command := &cobra.Command{
		Use:   "refill",
		Short: "Directly replace stateDiff storage from changeset files",
		RunE: func(command *cobra.Command, _ []string) error {
			report, err := runRefill(command.Context(), options, nil)
			if err != nil {
				return err
			}
			return printJSON(report)
		},
	}
	command.Flags().StringVar(&options.ZZFile, "zz", "", "changeset block-*.zz file")
	command.Flags().StringVar(&options.ZZDir, "zz-dir", "", "directory of numerically ordered block-*.zz files")
	command.Flags().StringVar(&options.ArchiveHome, "home", "", "frozen Cronos archive home")
	command.Flags().Int64Var(&options.FirstHeight, "first-height", 0, "first block height")
	command.Flags().Int64Var(&options.FinalHeight, "final-height", 0, "final block height; one-file mode allows at most 10,000 blocks")
	command.Flags().BoolVar(&options.Write, "write", false, "PUT replaced objects; the default only performs GET and replacement")
	command.Flags().StringVar(&options.Checkpoint, "checkpoint", "", "write-mode checkpoint path")
	command.Flags().IntVar(&options.Concurrency, "concurrency", options.Concurrency, "maximum concurrent S3 operations")
	command.Flags().IntVar(&options.Window, "window", options.Window, "maximum emitted but not continuously completed operations")
	_ = command.MarkFlagRequired("home")
	_ = command.MarkFlagRequired("first-height")
	_ = command.MarkFlagRequired("final-height")
	return command
}

func (options *refillOptions) prepare() error {
	if (options.ZZFile == "") == (options.ZZDir == "") {
		return fmt.Errorf("exactly one of zz and zz-dir is required")
	}
	if !filepath.IsAbs(options.ArchiveHome) ||
		(options.ZZFile != "" && !filepath.IsAbs(options.ZZFile)) ||
		(options.ZZDir != "" && !filepath.IsAbs(options.ZZDir)) {
		return fmt.Errorf("zz or zz-dir and home must be absolute paths")
	}
	if options.ZZFile != "" {
		options.ZZFile = filepath.Clean(options.ZZFile)
	} else {
		options.ZZDir = filepath.Clean(options.ZZDir)
	}
	options.ArchiveHome = filepath.Clean(options.ArchiveHome)
	options.Prefix = strings.TrimSuffix(options.Prefix, "/")
	if options.FirstHeight < 2 || options.FinalHeight < options.FirstHeight {
		return fmt.Errorf("invalid refill range %d-%d", options.FirstHeight, options.FinalHeight)
	}
	if options.ZZFile != "" && options.FinalHeight-options.FirstHeight+1 > maxRefillHeights {
		return fmt.Errorf("refill range exceeds %d heights", maxRefillHeights)
	}
	if options.Bucket == "" || options.Prefix == "" || options.Region == "" {
		return fmt.Errorf("refill S3 target is required")
	}
	if options.Concurrency < 1 || options.Concurrency > maximumObjectConcurrency {
		return fmt.Errorf("concurrency must be between 1 and %d", maximumObjectConcurrency)
	}
	if options.Window < options.Concurrency || options.Window > maximumOrderedPipelineWindow {
		return fmt.Errorf("window must be between concurrency (%d) and %d", options.Concurrency, maximumOrderedPipelineWindow)
	}
	if options.Write {
		if !filepath.IsAbs(options.Checkpoint) {
			return fmt.Errorf("write mode requires an absolute checkpoint path")
		}
		options.Checkpoint = filepath.Clean(options.Checkpoint)
	} else if options.Checkpoint != "" {
		return fmt.Errorf("checkpoint is only used with write mode")
	}
	return nil
}

func (options refillOptions) checkpointSchema() string {
	if options.ZZDir != "" {
		return refillDirectoryCheckpointSchema
	}
	return refillCheckpointSchema
}

func runRefill(ctx context.Context, options refillOptions, objects objectStore) (refillReport, error) {
	if err := options.prepare(); err != nil {
		return refillReport{}, err
	}
	archive, err := openArchive(options.ArchiveHome)
	if err != nil {
		return refillReport{}, err
	}
	defer archive.Close()
	if objects == nil {
		objects, err = newRefillS3ObjectStore(ctx, options.Region, options.Concurrency)
		if err != nil {
			return refillReport{}, err
		}
	}
	return runRefillWithReaders(ctx, options, archive, objects)
}

func runRefillWithReaders(
	ctx context.Context,
	options refillOptions,
	roots commitInfoReader,
	objects objectStore,
) (refillReport, error) {
	if err := options.prepare(); err != nil {
		return refillReport{}, err
	}
	if ctx == nil || roots == nil || objects == nil {
		return refillReport{}, fmt.Errorf("refill context, archive, and object store are required")
	}
	mode := "get-only"
	checkpoint := refillCheckpoint{
		Schema: options.checkpointSchema(), Mode: mode,
		ZZFile: options.ZZFile, ZZDir: options.ZZDir, ArchiveHome: options.ArchiveHome,
		FirstHeight: options.FirstHeight, FinalHeight: options.FinalHeight,
	}
	if options.Write {
		mode = "write"
		checkpoint.Mode = mode
		loaded, found, err := loadRefillCheckpoint(options.Checkpoint, options)
		if err != nil {
			return refillReport{}, err
		}
		if found {
			checkpoint = loaded
		} else if err := saveRefillCheckpoint(options.Checkpoint, checkpoint); err != nil {
			return refillReport{}, err
		}
	}

	started := time.Now()
	initialProcessed := checkpoint.Processed
	startHeight := options.FirstHeight
	if checkpoint.Frontier != 0 {
		startHeight = checkpoint.Frontier + 1
	}
	if startHeight <= options.FinalHeight {
		var previousHeight int64
		var previousRoot common.Hash
		if startHeight > 2 {
			parentInfo, err := roots.commitInfo(startHeight - 2)
			if err != nil {
				return refillReport{}, err
			}
			previousHeight = startHeight - 1
			previousRoot = common.BytesToHash(parentInfo.Hash())
		}
		firstSequence := checkpoint.Processed + 1
		nextSequence := firstSequence
		err := runOrderedPipeline(ctx, firstSequence, options.Concurrency, options.Window,
			func(pipelineCtx context.Context, emit func(uint64, refillTask) error) error {
				iterate := func(
					iteratorCtx context.Context,
					fn func(int64, *iavl.ChangeSet) error,
				) error {
					if options.ZZDir != "" {
						return iterateZZDirectoryRangeFastContext(
							iteratorCtx, options.ZZDir, startHeight, options.FinalHeight, fn,
						)
					}
					return iterateZZRangeFastContext(
						iteratorCtx, options.ZZFile, startHeight, options.FinalHeight, fn,
					)
				}
				return iterate(
					pipelineCtx,
					func(height int64, changeSet *iavl.ChangeSet) error {
						canonical, err := statediff.CanonicalStorageDiff(changeSet)
						if err != nil {
							return fmt.Errorf("height %d canonical storage: %w", height, err)
						}
						rootInfo, err := roots.commitInfo(height - 1)
						if err != nil {
							return err
						}
						root := common.BytesToHash(rootInfo.Hash())
						parent := common.Hash{}
						if height > 2 {
							if previousHeight == height-1 {
								parent = previousRoot
							} else {
								parentInfo, err := roots.commitInfo(height - 2)
								if err != nil {
									return err
								}
								parent = common.BytesToHash(parentInfo.Hash())
							}
						}
						task := refillTask{
							height: height, canonical: canonical, skip: root == parent,
							key: fmt.Sprintf("%s/%s/stateDiff", options.Prefix, strings.ToLower(root.Hex())),
						}
						sequence := nextSequence
						nextSequence++
						previousHeight, previousRoot = height, root
						return emit(sequence, task)
					},
				)
			},
			func(workerCtx context.Context, task refillTask) (refillOutcome, error) {
				return processRefillTask(workerCtx, options, task, objects)
			},
			func(_ uint64, outcome refillOutcome) error {
				checkpoint.Frontier = outcome.height
				checkpoint.Processed++
				checkpoint.Gets += outcome.gets
				checkpoint.Puts += outcome.puts
				checkpoint.WouldPUTs += outcome.wouldPUTs
				checkpoint.Skipped += outcome.skipped
				checkpoint.OldBytes += outcome.oldBytes
				checkpoint.NewBytes += outcome.newBytes
				if options.Write && (checkpoint.Processed%refillCheckpointEvery == 0 ||
					outcome.height == options.FinalHeight) {
					if err := saveRefillCheckpoint(options.Checkpoint, checkpoint); err != nil {
						return err
					}
				}
				if checkpoint.Processed%refillProgressEvery == 0 ||
					outcome.height == options.FinalHeight {
					fmt.Fprintf(
						os.Stderr,
						"refill progress frontier=%d processed=%d gets=%d puts=%d skipped_equal_root=%d\n",
						checkpoint.Frontier, checkpoint.Processed, checkpoint.Gets,
						checkpoint.Puts, checkpoint.Skipped,
					)
				}
				return nil
			},
		)
		if err != nil {
			return refillReport{}, err
		}
	}
	finished := time.Now()
	invocationProcessed := checkpoint.Processed - initialProcessed
	elapsed := finished.Sub(started).Seconds()
	rate := float64(0)
	if elapsed > 0 {
		rate = float64(invocationProcessed) / elapsed
	}
	return refillReport{
		Mode: mode, ZZFile: options.ZZFile, ZZDir: options.ZZDir, ArchiveHome: options.ArchiveHome,
		FirstHeight: options.FirstHeight, FinalHeight: options.FinalHeight,
		Frontier: checkpoint.Frontier, Processed: checkpoint.Processed,
		InvocationProcessed: invocationProcessed,
		Gets:                checkpoint.Gets, Puts: checkpoint.Puts, WouldPUTs: checkpoint.WouldPUTs,
		SkippedEqualRoot: checkpoint.Skipped, OldBytes: checkpoint.OldBytes, NewBytes: checkpoint.NewBytes,
		StartedAt: started.UTC().Format(time.RFC3339Nano), FinishedAt: finished.UTC().Format(time.RFC3339Nano),
		ElapsedSeconds: elapsed, BlocksPerSecond: rate,
	}, nil
}

type refillZZFile struct {
	path  string
	first int64
}

func listRefillZZFiles(dir string) ([]refillZZFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files := make([]refillZZFile, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "block-") || !strings.HasSuffix(name, ".zz") {
			continue
		}
		value := strings.TrimSuffix(strings.TrimPrefix(name, "block-"), ".zz")
		first, err := strconv.ParseInt(value, 10, 64)
		if err != nil || first < 1 || strconv.FormatInt(first, 10) != value {
			return nil, fmt.Errorf("invalid changeset filename %q", name)
		}
		path := filepath.Join(dir, name)
		if err := requireRegularFile(path, "refill changeset"); err != nil {
			return nil, err
		}
		files = append(files, refillZZFile{path: path, first: first})
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no block-*.zz files in %s", dir)
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].first < files[j].first
	})
	for index := 1; index < len(files); index++ {
		if files[index].first == files[index-1].first {
			return nil, fmt.Errorf("duplicate changeset start height %d", files[index].first)
		}
	}
	return files, nil
}

func iterateZZDirectoryRangeFastContext(
	ctx context.Context,
	dir string,
	first, last int64,
	fn func(int64, *iavl.ChangeSet) error,
) error {
	files, err := listRefillZZFiles(dir)
	if err != nil {
		return err
	}
	startIndex := sort.Search(len(files), func(index int) bool {
		return files[index].first > first
	}) - 1
	if startIndex < 0 {
		return fmt.Errorf("no changeset file covers height %d", first)
	}
	expected := first
	for index := startIndex; index < len(files) && expected <= last; index++ {
		fileFirst := expected
		fileLast := last
		if index+1 < len(files) && files[index+1].first <= last {
			fileLast = files[index+1].first - 1
		}
		err := iterateZZRangeFastContext(
			ctx, files[index].path, fileFirst, fileLast,
			func(height int64, changeSet *iavl.ChangeSet) error {
				if height != expected {
					return fmt.Errorf("changeset height %d follows %d", height, expected-1)
				}
				expected++
				return fn(height, changeSet)
			},
		)
		if err != nil {
			return err
		}
	}
	if expected != last+1 {
		return fmt.Errorf("changeset directory ended at height %d, want %d", expected-1, last)
	}
	return nil
}

func processRefillTask(
	ctx context.Context,
	options refillOptions,
	task refillTask,
	objects objectStore,
) (refillOutcome, error) {
	outcome := refillOutcome{height: task.height}
	if task.skip {
		outcome.skipped = 1
		return outcome, nil
	}
	object, err := objects.Get(ctx, options.Bucket, task.key)
	if err != nil {
		return refillOutcome{}, fmt.Errorf("get height %d key %s: %w", task.height, task.key, err)
	}
	newBody, err := replaceStorageDiffRLP(object.Body, task.canonical)
	if err != nil {
		return refillOutcome{}, fmt.Errorf("replace height %d storage: %w", task.height, err)
	}
	outcome.gets, outcome.wouldPUTs = 1, 1
	outcome.oldBytes, outcome.newBytes = uint64(len(object.Body)), uint64(len(newBody))
	if options.Write {
		if _, err := objects.Put(ctx, options.Bucket, task.key, newBody, object.ETag, object.Headers); err != nil {
			return refillOutcome{}, fmt.Errorf("put height %d key %s: %w", task.height, task.key, err)
		}
		outcome.puts = 1
	}
	return outcome, nil
}

func iterateZZRangeFastContext(
	ctx context.Context,
	path string,
	first, last int64,
	fn func(int64, *iavl.ChangeSet) error,
) error {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	zreader, err := zlib.NewReader(contextReader{ctx: ctx, reader: bufio.NewReader(file)})
	if err != nil {
		return fmt.Errorf("open zlib %s: %w", path, err)
	}
	defer zreader.Close()
	reader := bufio.NewReader(zreader)
	var delivered int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		version, changeSet, err := readChangeSet(reader)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read changeset %s: %w", path, err)
		}
		if version < first {
			continue
		}
		if version > last {
			break
		}
		if err := fn(version, changeSet); err != nil {
			return err
		}
		delivered++
		if version == last {
			break
		}
	}
	if delivered != last-first+1 {
		return fmt.Errorf("changeset file delivered %d records for range %d-%d", delivered, first, last)
	}
	return nil
}

func loadRefillCheckpoint(path string, options refillOptions) (refillCheckpoint, bool, error) {
	body, found, err := readMutableStateFile(path, "refill checkpoint")
	if err != nil || !found {
		return refillCheckpoint{}, found, err
	}
	var checkpoint refillCheckpoint
	if err := decodeStrictJSON(body, &checkpoint, "refill checkpoint"); err != nil {
		return refillCheckpoint{}, false, err
	}
	if checkpoint.Schema != options.checkpointSchema() || checkpoint.Mode != "write" ||
		checkpoint.ZZFile != options.ZZFile || checkpoint.ZZDir != options.ZZDir ||
		checkpoint.ArchiveHome != options.ArchiveHome ||
		checkpoint.FirstHeight != options.FirstHeight || checkpoint.FinalHeight != options.FinalHeight {
		return refillCheckpoint{}, false, fmt.Errorf("refill checkpoint does not match this run")
	}
	if checkpoint.Processed > uint64(options.FinalHeight-options.FirstHeight+1) ||
		(checkpoint.Processed == 0) != (checkpoint.Frontier == 0) ||
		(checkpoint.Frontier != 0 && (checkpoint.Frontier < options.FirstHeight ||
			checkpoint.Frontier > options.FinalHeight)) ||
		(checkpoint.Processed > 0 &&
			checkpoint.Frontier != options.FirstHeight+int64(checkpoint.Processed)-1) {
		return refillCheckpoint{}, false, fmt.Errorf("refill checkpoint frontier is invalid")
	}
	return checkpoint, true, nil
}

func saveRefillCheckpoint(path string, checkpoint refillCheckpoint) error {
	if err := validateMutableStatePath(path, "refill checkpoint"); err != nil {
		return err
	}
	_, err := atomicJSON(path, checkpoint)
	return err
}
