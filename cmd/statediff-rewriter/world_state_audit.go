package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	dtypes "github.com/evmos/ethermint/debank/types"
	"github.com/spf13/cobra"
)

const (
	defaultCronosEVMDenom    = "basecro"
	worldAuditProgressEvery  = uint64(100_000)
	worldAuditFindingsName   = "findings.ndjson"
	worldAuditCodesName      = "available-codes.bin"
	worldAuditCheckpointName = "checkpoint.v1.json"
	worldAuditSummaryName    = "summary.v1.json"
	defaultWorldAuditWorkers = 8
	defaultWorldAuditWindow  = 16
	maximumWorldAuditWindow  = 32
)

var (
	cronosGenesisStateRoot = common.HexToHash("0xe3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	cronosGenesisAccounts  = []common.Address{
		common.HexToAddress("0x0780adef7832a7f7682b757a5ec5bd9fe7c38b4b"),
		common.HexToAddress("0x81e3e543647e466a5abc824f5844ab0a091b6c6c"),
		common.HexToAddress("0xca5cf03d081197be24ef707081fbd7f3f11eb02d"),
		common.HexToAddress("0x4f87a3f99bd1e58d01de1c38b7f83cb967e816c2"),
		common.HexToAddress("0xef4d07d0e1b40603e0d1b3e633334f0aba5c7a60"),
		common.HexToAddress("0xf428fe419f1d0b1aac6a49a1980ce5b556e5ed54"),
		common.HexToAddress("0x5f61bc1a230051fdc3a96afcc27e706db1124be2"),
		common.HexToAddress("0xf6d4fecb1a6fb7c2ca350169a050d483bd87b883"),
	}
)

type worldAuditOptions struct {
	ArchiveHome     string
	Output          string
	Bucket          string
	Prefix          string
	Region          string
	EVMDenom        string
	FirstHeight     int64
	FinalHeight     int64
	Partial         bool
	IAVLCacheSize   int
	IAVLConcurrency int
	IAVLRunHeights  int64
	Parallel        parallelOptions
}

type worldAuditTask struct {
	height   int64
	root     common.Hash
	parent   common.Hash
	key      string
	skip     bool
	expected expectedWorldState
}

type worldAuditFetch struct {
	task           worldAuditTask
	actual         normalizedWorldState
	get            bool
	objectBytes    uint64
	missingObject  bool
	contentFinding *worldAuditFinding
}

type worldAuditReport struct {
	Schema              string                           `json:"schema"`
	Implementation      worldAuditImplementationIdentity `json:"implementation"`
	ArchiveIdentity     archiveIdentity                  `json:"archive_identity"`
	Initialization      worldStateInitialization         `json:"initialization"`
	FirstHeight         int64                            `json:"first_height"`
	FinalHeight         int64                            `json:"final_height"`
	Summary             worldAuditSummary                `json:"summary"`
	FindingsPath        string                           `json:"findings_path"`
	FindingsSHA256      string                           `json:"findings_sha256"`
	AvailableCodesPath  string                           `json:"available_codes_path"`
	AvailableCodesCount int                              `json:"available_codes_count"`
	CheckpointPath      string                           `json:"checkpoint_path"`
	Completed           bool                             `json:"completed"`
	DurationSeconds     float64                          `json:"duration_seconds"`
	BlocksPerSecond     float64                          `json:"blocks_per_second"`
	GenesisAudited      bool                             `json:"genesis_audited"`
	CodeHistoryComplete bool                             `json:"code_history_complete"`
}

func newWorldStateAuditCommand() *cobra.Command {
	options := worldAuditOptions{
		Bucket: defaultBucket, Prefix: defaultPrefix, Region: defaultRegion,
		EVMDenom: defaultCronosEVMDenom, FirstHeight: 2,
		IAVLCacheSize: defaultDumpCacheSize, IAVLConcurrency: defaultDirectIAVLConcurrency,
		IAVLRunHeights: defaultWorldStateRunHeights,
		Parallel: parallelOptions{
			Concurrency: defaultWorldAuditWorkers, Window: defaultWorldAuditWindow,
			MaxInFlightBytes: defaultMaxInFlightBytes,
			CheckpointEvery:  defaultCheckpointEvery, CheckpointInterval: defaultCheckpointInterval,
		},
	}
	command := &cobra.Command{
		Use:   "audit-world-state",
		Short: "Read-only audit S3 account, deletion, and code fields against acc/bank/evm IAVL",
		RunE: func(command *cobra.Command, _ []string) error {
			report, err := runWorldStateAudit(command.Context(), options, nil)
			if err != nil {
				return err
			}
			return printJSON(report)
		},
	}
	command.Flags().StringVar(&options.ArchiveHome, "home", "", "frozen Cronos archive home")
	command.Flags().StringVar(&options.Output, "output", "", "audit output directory")
	command.Flags().StringVar(&options.Bucket, "bucket", options.Bucket, "S3 bucket")
	command.Flags().StringVar(&options.Prefix, "prefix", options.Prefix, "S3 chain prefix")
	command.Flags().StringVar(&options.Region, "region", options.Region, "AWS region")
	command.Flags().StringVar(&options.EVMDenom, "evm-denom", options.EVMDenom, "native EVM bank denom")
	command.Flags().Int64Var(&options.FirstHeight, "first-height", options.FirstHeight, "first block height, inclusive")
	command.Flags().Int64Var(&options.FinalHeight, "final-height", 0, "final block height, inclusive; defaults to archive latest")
	command.Flags().BoolVar(
		&options.Partial, "partial", false,
		"allow an isolated fixture range starting after block 2; code-history completeness will be false",
	)
	command.Flags().IntVar(&options.IAVLCacheSize, "iavl-cache-size", options.IAVLCacheSize, "IAVL node-cache entries per store and worker")
	command.Flags().IntVar(&options.IAVLConcurrency, "iavl-concurrency", options.IAVLConcurrency, "parallel IAVL shards")
	command.Flags().Int64Var(&options.IAVLRunHeights, "iavl-run-heights", options.IAVLRunHeights, "heights retained per ordered IAVL shard")
	command.Flags().IntVar(
		&options.Parallel.Concurrency, "concurrency", options.Parallel.Concurrency,
		"maximum concurrent S3 GET operations",
	)
	command.Flags().IntVar(
		&options.Parallel.Window, "window", options.Parallel.Window,
		"maximum emitted but not continuously committed heights (maximum 32)",
	)
	command.Flags().Uint64Var(
		&options.Parallel.CheckpointEvery, "checkpoint-every", options.Parallel.CheckpointEvery,
		"persist a continuous checkpoint after this many heights",
	)
	command.Flags().DurationVar(
		&options.Parallel.CheckpointInterval, "checkpoint-interval", options.Parallel.CheckpointInterval,
		"maximum time between continuous checkpoint writes",
	)
	_ = command.MarkFlagRequired("home")
	_ = command.MarkFlagRequired("output")
	return command
}

func runWorldStateAudit(
	ctx context.Context,
	options worldAuditOptions,
	objects objectStore,
) (report worldAuditReport, returnErr error) {
	started := time.Now()
	if ctx == nil {
		return report, fmt.Errorf("audit context is required")
	}
	if options.FirstHeight < 2 || options.Output == "" || options.ArchiveHome == "" ||
		options.Bucket == "" || options.Prefix == "" || options.Region == "" || options.EVMDenom == "" {
		return report, fmt.Errorf("audit home, output, S3 identity, denom, and first height >= 2 are required")
	}
	if options.FirstHeight != 2 && !options.Partial {
		return report, fmt.Errorf("complete code-history audit must start at height 2; use --partial only for isolated fixtures")
	}
	if options.IAVLCacheSize < 0 {
		return report, fmt.Errorf("iavl-cache-size cannot be negative")
	}
	if err := options.Parallel.validate(true); err != nil {
		return report, err
	}
	if options.Parallel.Window > maximumWorldAuditWindow {
		return report, fmt.Errorf("audit window cannot exceed %d", maximumWorldAuditWindow)
	}
	output, err := filepath.Abs(options.Output)
	if err != nil {
		return report, err
	}
	if err := os.MkdirAll(output, 0o755); err != nil {
		return report, err
	}
	options.Output = output
	locks, err := acquireStagingLocks(filepath.Join(output, "world-state-audit"))
	if err != nil {
		return report, err
	}
	defer func() {
		if lockErr := releaseStagingLocks(locks); returnErr == nil && lockErr != nil {
			returnErr = lockErr
		}
	}()

	archive, err := openArchive(options.ArchiveHome)
	if err != nil {
		return report, err
	}
	defer func() {
		if closeErr := archive.Close(); returnErr == nil && closeErr != nil {
			returnErr = closeErr
		}
	}()
	identity, err := archive.identity()
	if err != nil {
		return report, err
	}
	if options.FinalHeight == 0 {
		options.FinalHeight = identity.LatestVersion
	}
	if options.FinalHeight < options.FirstHeight || options.FinalHeight > identity.LatestVersion {
		return report, fmt.Errorf(
			"invalid audit range %d-%d for archive latest %d",
			options.FirstHeight, options.FinalHeight, identity.LatestVersion,
		)
	}

	findingsPath := filepath.Join(output, worldAuditFindingsName)
	codesPath := filepath.Join(output, worldAuditCodesName)
	checkpointPath := filepath.Join(output, worldAuditCheckpointName)
	implementation := currentWorldAuditImplementationIdentity()
	expectedCheckpoint := worldAuditCheckpoint{
		Schema: worldAuditCheckpointSchema, Implementation: implementation, ArchiveIdentity: identity,
		Bucket: options.Bucket, Prefix: options.Prefix, Region: options.Region, EVMDenom: options.EVMDenom,
		FirstHeight: options.FirstHeight, FinalHeight: options.FinalHeight, Partial: options.Partial,
		FindingsPath: findingsPath, CodesPath: codesPath, Summary: newWorldAuditSummary(),
	}
	checkpoint, resumed, err := loadWorldAuditCheckpoint(checkpointPath, expectedCheckpoint)
	if err != nil {
		return report, err
	}
	if resumed && checkpoint.Completed {
		if err := validateCompletedAuditLog(
			findingsPath, checkpoint.FindingsBytes, checkpoint.FindingsSHA256,
		); err != nil {
			return report, err
		}
		if err := validateCompletedAuditLog(
			codesPath, checkpoint.CodesBytes, checkpoint.CodesSHA256,
		); err != nil {
			return report, err
		}
		report = worldAuditReport{
			Schema: worldAuditCheckpointSchema, Implementation: implementation, ArchiveIdentity: identity,
			FirstHeight: options.FirstHeight, FinalHeight: options.FinalHeight, Summary: checkpoint.Summary,
			FindingsPath: findingsPath, FindingsSHA256: checkpoint.FindingsSHA256,
			AvailableCodesPath: codesPath, AvailableCodesCount: int(checkpoint.CodesBytes / commonHashLength),
			CheckpointPath: checkpointPath, Completed: true, GenesisAudited: checkpoint.Summary.GenesisAudited,
			CodeHistoryComplete: !options.Partial && options.FirstHeight == 2,
		}
		if _, err := atomicJSON(filepath.Join(output, worldAuditSummaryName), report); err != nil {
			return worldAuditReport{}, err
		}
		return report, nil
	}

	decoder, err := newWorldStateDecoder(options.EVMDenom)
	if err != nil {
		return report, err
	}
	projection, initialCodes, initialization, err := initializeWorldStateProjection(
		archive, decoder, options.FirstHeight-1, options.IAVLCacheSize,
	)
	if err != nil {
		return report, err
	}

	var findingsLog, codesLog *appendAuditLog
	availableCodes := make(map[common.Hash]struct{})
	unavailableReported := make(map[common.Hash]struct{})
	if resumed {
		findingsLog, err = restoreAppendAuditLog(findingsPath, checkpoint.FindingsBytes, checkpoint.FindingsSHA256)
		if err == nil {
			codesLog, err = restoreAppendAuditLog(codesPath, checkpoint.CodesBytes, checkpoint.CodesSHA256)
		}
		if err == nil {
			err = loadAvailableCodeLog(codesPath, checkpoint.CodesBytes, availableCodes)
		}
		if err == nil {
			err = loadUnavailableCodeFindings(findingsPath, checkpoint.FindingsBytes, unavailableReported)
		}
		if err != nil {
			if findingsLog != nil {
				_ = findingsLog.Close()
			}
			if codesLog != nil {
				_ = codesLog.Close()
			}
			return report, err
		}
	} else {
		findingsLog, err = createAppendAuditLog(findingsPath)
		if err == nil {
			codesLog, err = createAppendAuditLog(codesPath)
		}
		if err != nil {
			if findingsLog != nil {
				_ = findingsLog.Close()
			}
			return report, err
		}
		hashes := make([]common.Hash, 0, len(initialCodes))
		for codeHash := range initialCodes {
			hashes = append(hashes, codeHash)
		}
		sort.Slice(hashes, func(i, j int) bool {
			return bytes.Compare(hashes[i][:], hashes[j][:]) < 0
		})
		for _, codeHash := range hashes {
			if err := appendAvailableCode(codesLog, availableCodes, codeHash); err != nil {
				_ = findingsLog.Close()
				_ = codesLog.Close()
				return report, err
			}
		}
		checkpoint = expectedCheckpoint
	}
	defer func() {
		findingsErr := findingsLog.Close()
		codesErr := codesLog.Close()
		if returnErr == nil {
			if findingsErr != nil {
				returnErr = findingsErr
			} else if codesErr != nil {
				returnErr = codesErr
			}
		}
	}()

	saver := &worldAuditCheckpointSaver{
		path: checkpointPath, findings: findingsLog, codes: codesLog, checkpoint: checkpoint,
		every: options.Parallel.CheckpointEvery, interval: options.Parallel.CheckpointInterval,
		lastPersist: time.Now(),
	}
	if !resumed {
		if err := saver.Flush(false); err != nil {
			return report, err
		}
	}
	summary := checkpoint.Summary
	if !summary.GenesisAudited {
		genesisProjection := projection
		if initialization.Version != 1 {
			genesisProjection, _, _, err = initializeWorldStateProjection(
				archive, decoder, 1, options.IAVLCacheSize,
			)
			if err != nil {
				return report, fmt.Errorf("initialize genesis world state: %w", err)
			}
		}
		genesisExpected, err := cronosGenesisExpectedWorldState(
			archive, genesisProjection, options.IAVLCacheSize,
		)
		if err != nil {
			return report, err
		}
		if objects == nil {
			objects, err = newRefillS3ObjectStore(ctx, options.Region, options.Parallel.Concurrency)
			if err != nil {
				return report, err
			}
		}
		genesisTask := worldAuditTask{
			height: 1,
			root:   cronosGenesisStateRoot,
			parent: ethtypes.EmptyRootHash,
			key: fmt.Sprintf(
				"%s/%s/stateDiff",
				options.Prefix,
				strings.ToLower(cronosGenesisStateRoot.Hex()),
			),
			expected: genesisExpected,
		}
		outcome, err := fetchWorldAuditTask(ctx, options.Bucket, genesisTask, objects)
		if err != nil {
			return report, fmt.Errorf("audit Cronos genesis: %w", err)
		}
		summary.GenesisAudited = true
		summary.CandidateHeights++
		summary.ExpectedNewAccounts += uint64(len(genesisExpected.newAccounts))
		summary.ExpectedCodeWrites += uint64(len(genesisExpected.codeWrites))
		if outcome.get {
			summary.S3Gets++
			summary.ObjectBytes += outcome.objectBytes
		}
		genesisFindings := cronosGenesisOutcomeFindings(
			outcome, make(map[common.Hash]struct{}), unavailableReported,
		)
		for _, finding := range genesisFindings {
			if err := appendWorldAuditFinding(findingsLog, finding); err != nil {
				return report, err
			}
			recordWorldAuditFinding(&summary, finding)
			summary.GenesisFindings++
		}
		for _, codeHash := range sortedCodeHashes(outcome.actual.codes) {
			if err := appendAvailableCode(codesLog, availableCodes, codeHash); err != nil {
				return report, err
			}
		}
		checkpoint.Summary = summary
		saver.checkpoint.Summary = summary
		if err := saver.Flush(false); err != nil {
			return report, err
		}
	}

	if summary.Frontier >= options.FirstHeight {
		fmt.Fprintf(os.Stderr, "audit replay projection first=%d frontier=%d\n", options.FirstHeight, summary.Frontier)
		replayed := uint64(0)
		err := iterateArchiveWorldStateDeltasContext(
			ctx, archive, options.EVMDenom, options.IAVLCacheSize, options.IAVLConcurrency,
			options.IAVLRunHeights, options.FirstHeight, summary.Frontier,
			func(delta worldStateDelta) error {
				projection.apply(delta)
				replayed++
				if replayed%worldAuditProgressEvery == 0 || delta.height == summary.Frontier {
					fmt.Fprintf(os.Stderr, "audit replay progress frontier=%d replayed=%d\n", delta.height, replayed)
				}
				return nil
			},
		)
		if err != nil {
			return report, fmt.Errorf("replay audit projection: %w", err)
		}
	}

	startHeight := options.FirstHeight
	if summary.Frontier != 0 {
		startHeight = summary.Frontier + 1
	}
	if startHeight <= options.FinalHeight {
		if objects == nil {
			objects, err = newRefillS3ObjectStore(ctx, options.Region, options.Parallel.Concurrency)
			if err != nil {
				return report, err
			}
		}
		rootCursor, err := newArchiveRootCursor(archive, startHeight)
		if err != nil {
			return report, err
		}
		err = runOrderedPipeline(
			ctx, uint64(startHeight), options.Parallel.Concurrency, options.Parallel.Window,
			func(pipelineCtx context.Context, emit func(uint64, worldAuditTask) error) error {
				return iterateArchiveWorldStateDeltasContext(
					pipelineCtx, archive, options.EVMDenom, options.IAVLCacheSize, options.IAVLConcurrency,
					options.IAVLRunHeights, startHeight, options.FinalHeight,
					func(delta worldStateDelta) error {
						expected := projection.apply(delta)
						root, parent, err := nextWorldAuditRoots(rootCursor, delta.height)
						if err != nil {
							return err
						}
						task, err := makeWorldAuditTask(
							delta.height, root, parent, options.Prefix, expected,
						)
						if err != nil {
							return err
						}
						return emit(uint64(delta.height), task)
					},
				)
			},
			func(workerCtx context.Context, task worldAuditTask) (worldAuditFetch, error) {
				return fetchWorldAuditTask(workerCtx, options.Bucket, task, objects)
			},
			func(sequence uint64, outcome worldAuditFetch) error {
				height := int64(sequence)
				if outcome.task.height != height {
					return fmt.Errorf("audit outcome height %d, want %d", outcome.task.height, height)
				}
				summary.Processed++
				summary.ExpectedNewAccounts += uint64(len(outcome.task.expected.newAccounts))
				summary.ExpectedDeletedAccounts += uint64(len(outcome.task.expected.deletedAccounts))
				summary.ExpectedCodeWrites += uint64(len(outcome.task.expected.codeWrites))
				summary.CodeDeletes += uint64(len(outcome.task.expected.codeDeletes))
				switch {
				case outcome.task.root == outcome.task.parent:
					summary.SkippedEqualRoot++
				case outcome.task.skip:
					summary.SkippedNoChanges++
				default:
					summary.CandidateHeights++
				}
				if outcome.get {
					summary.S3Gets++
					summary.ObjectBytes += outcome.objectBytes
				}
				findings := worldAuditOutcomeFindings(outcome, availableCodes, unavailableReported)
				if !outcome.task.skip && outcome.contentFinding == nil && !outcome.missingObject {
					for _, codeHash := range sortedCodeHashes(outcome.actual.codes) {
						if err := appendAvailableCode(codesLog, availableCodes, codeHash); err != nil {
							return err
						}
					}
				}
				for _, finding := range findings {
					if err := appendWorldAuditFinding(findingsLog, finding); err != nil {
						return err
					}
					recordWorldAuditFinding(&summary, finding)
				}
				summary.Frontier = height
				if err := saver.Advance(height, summary); err != nil {
					return err
				}
				if summary.Processed%worldAuditProgressEvery == 0 || height == options.FinalHeight {
					elapsed := time.Since(started).Seconds()
					fmt.Fprintf(
						os.Stderr,
						"audit progress frontier=%d processed=%d candidates=%d gets=%d defects=%d warnings=%d rate=%.2f blocks/s\n",
						height, summary.Processed, summary.CandidateHeights, summary.S3Gets,
						summary.Defects, summary.Warnings, float64(summary.Processed)/elapsed,
					)
				}
				return nil
			},
		)
		if err != nil {
			return report, err
		}
	}
	if err := saver.Flush(true); err != nil {
		return report, err
	}
	duration := time.Since(started).Seconds()
	report = worldAuditReport{
		Schema: worldAuditCheckpointSchema, Implementation: implementation,
		ArchiveIdentity: identity, Initialization: initialization,
		FirstHeight: options.FirstHeight, FinalHeight: options.FinalHeight, Summary: summary,
		FindingsPath: findingsPath, FindingsSHA256: findingsLog.SHA256(),
		AvailableCodesPath: codesPath, AvailableCodesCount: len(availableCodes),
		CheckpointPath: checkpointPath, Completed: true, DurationSeconds: duration,
		BlocksPerSecond: float64(summary.Processed) / duration, GenesisAudited: summary.GenesisAudited,
		CodeHistoryComplete: !options.Partial && options.FirstHeight == 2,
	}
	if _, err := atomicJSON(filepath.Join(output, worldAuditSummaryName), report); err != nil {
		return report, err
	}
	return report, nil
}

func nextWorldAuditRoots(
	cursor *archiveRootCursor,
	height int64,
) (common.Hash, common.Hash, error) {
	root, parent, err := cursor.Next(height)
	if err != nil {
		return common.Hash{}, common.Hash{}, err
	}
	if height == 2 {
		parent = cronosGenesisStateRoot
	}
	return root, parent, nil
}

func makeWorldAuditTask(
	height int64,
	root, parent common.Hash,
	prefix string,
	expected expectedWorldState,
) (worldAuditTask, error) {
	hasChanges := len(expected.newAccounts) != 0 || len(expected.deletedAccounts) != 0 ||
		len(expected.codeWrites) != 0
	if root == parent && hasChanges {
		return worldAuditTask{}, fmt.Errorf(
			"height %d has canonical world-state changes behind equal app roots",
			height,
		)
	}
	return worldAuditTask{
		height:   height,
		root:     root,
		parent:   parent,
		key:      fmt.Sprintf("%s/%s/stateDiff", prefix, strings.ToLower(root.Hex())),
		skip:     !hasChanges || root == parent,
		expected: expected,
	}, nil
}

func worldAuditOutcomeFindings(
	outcome worldAuditFetch,
	availableCodes map[common.Hash]struct{},
	unavailableReported map[common.Hash]struct{},
) []worldAuditFinding {
	switch {
	case outcome.missingObject:
		return []worldAuditFinding{{
			Height: outcome.task.height, Severity: auditSeverityDefect, Kind: "missing_s3_object",
			Detail: outcome.task.key,
		}}
	case outcome.contentFinding != nil:
		return []worldAuditFinding{*outcome.contentFinding}
	case outcome.task.skip:
		return nil
	default:
		return compareExpectedWorldState(
			outcome.task.height,
			outcome.task.expected,
			outcome.actual,
			availableCodes,
			unavailableReported,
		)
	}
}

func cronosGenesisOutcomeFindings(
	outcome worldAuditFetch,
	availableCodes map[common.Hash]struct{},
	unavailableReported map[common.Hash]struct{},
) []worldAuditFinding {
	findings := worldAuditOutcomeFindings(outcome, availableCodes, unavailableReported)
	if outcome.missingObject || outcome.contentFinding != nil {
		return findings
	}
	expectedAccounts := make(map[common.Hash]struct{}, len(outcome.task.expected.newAccounts))
	for _, account := range outcome.task.expected.newAccounts {
		expectedAccounts[account.Address] = struct{}{}
	}
	actualAccounts := make([]common.Hash, 0, len(outcome.actual.accounts))
	for address := range outcome.actual.accounts {
		actualAccounts = append(actualAccounts, address)
	}
	sort.Slice(actualAccounts, func(i, j int) bool {
		return bytes.Compare(actualAccounts[i][:], actualAccounts[j][:]) < 0
	})
	for _, address := range actualAccounts {
		if _, found := expectedAccounts[address]; !found {
			findings = append(findings, worldAuditFinding{
				Height: 1, Severity: auditSeverityDefect, Kind: "unexpected_genesis_new_account",
				WireAddress: address.Hex(),
			})
		}
	}
	deletedAccounts := make([]common.Hash, 0, len(outcome.actual.deleted))
	for address := range outcome.actual.deleted {
		deletedAccounts = append(deletedAccounts, address)
	}
	sort.Slice(deletedAccounts, func(i, j int) bool {
		return bytes.Compare(deletedAccounts[i][:], deletedAccounts[j][:]) < 0
	})
	for _, address := range deletedAccounts {
		findings = append(findings, worldAuditFinding{
			Height: 1, Severity: auditSeverityDefect, Kind: "unexpected_genesis_deleted_account",
			WireAddress: address.Hex(),
		})
	}
	expectedCodes := make(map[common.Hash]struct{}, len(outcome.task.expected.codeWrites))
	for _, code := range outcome.task.expected.codeWrites {
		expectedCodes[code.CodeHash] = struct{}{}
	}
	for _, codeHash := range sortedCodeHashes(outcome.actual.codes) {
		if _, found := expectedCodes[codeHash]; !found {
			findings = append(findings, worldAuditFinding{
				Height: 1, Severity: auditSeverityDefect, Kind: "unexpected_genesis_new_code",
				CodeHash: codeHash.Hex(),
			})
		}
	}
	return findings
}

func recordWorldAuditFinding(summary *worldAuditSummary, finding worldAuditFinding) {
	summary.Findings++
	summary.FindingsByKind[finding.Kind]++
	if finding.Severity == auditSeverityDefect {
		summary.Defects++
	} else {
		summary.Warnings++
	}
}

func sortedCodeHashes(codes map[common.Hash][]byte) []common.Hash {
	hashes := make([]common.Hash, 0, len(codes))
	for codeHash := range codes {
		hashes = append(hashes, codeHash)
	}
	sort.Slice(hashes, func(i, j int) bool {
		return bytes.Compare(hashes[i][:], hashes[j][:]) < 0
	})
	return hashes
}

func fetchWorldAuditTask(
	ctx context.Context,
	bucket string,
	task worldAuditTask,
	objects objectStore,
) (worldAuditFetch, error) {
	outcome := worldAuditFetch{task: task}
	if task.skip {
		return outcome, nil
	}
	object, err := objects.Get(ctx, bucket, task.key)
	if err != nil {
		if isObjectNotFound(err) {
			outcome.get = true
			outcome.missingObject = true
			return outcome, nil
		}
		return outcome, err
	}
	outcome.get = true
	outcome.objectBytes = uint64(len(object.Body))
	var diff dtypes.BlockStorageDiff
	if err := rlp.DecodeBytes(object.Body, &diff); err != nil {
		outcome.contentFinding = &worldAuditFinding{
			Height: task.height, Severity: auditSeverityDefect, Kind: "invalid_state_diff_rlp", Detail: err.Error(),
		}
		return outcome, nil
	}
	if diff.Hash != task.root || diff.ParentHash != task.parent {
		outcome.contentFinding = &worldAuditFinding{
			Height: task.height, Severity: auditSeverityDefect, Kind: "wrong_state_diff_roots",
			Expected: fmt.Sprintf("%s/%s", task.root, task.parent),
			Actual:   fmt.Sprintf("%s/%s", diff.Hash, diff.ParentHash),
		}
		return outcome, nil
	}
	actual, err := normalizeWorldState(diff)
	if err != nil {
		outcome.contentFinding = &worldAuditFinding{
			Height: task.height, Severity: auditSeverityDefect, Kind: "invalid_state_diff_fields", Detail: err.Error(),
		}
		return outcome, nil
	}
	outcome.actual = actual
	return outcome, nil
}

func isObjectNotFound(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "NoSuchKey" || apiErr.ErrorCode() == "NotFound") {
		return true
	}
	var responseErr *smithyhttp.ResponseError
	return errors.As(err, &responseErr) && responseErr.HTTPStatusCode() == 404
}

func appendWorldAuditFinding(log *appendAuditLog, finding worldAuditFinding) error {
	body, err := json.Marshal(finding)
	if err != nil {
		return err
	}
	body = append(body, '\n')
	return log.Append(body)
}

func appendAvailableCode(log *appendAuditLog, available map[common.Hash]struct{}, codeHash common.Hash) error {
	if _, found := available[codeHash]; found {
		return nil
	}
	if err := log.Append(codeHash[:]); err != nil {
		return err
	}
	available[codeHash] = struct{}{}
	return nil
}

func loadAvailableCodeLog(path string, size int64, available map[common.Hash]struct{}) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	for offset := int64(0); offset < size; offset += commonHashLength {
		var body [commonHashLength]byte
		if _, err := io.ReadFull(file, body[:]); err != nil {
			return err
		}
		codeHash := common.BytesToHash(body[:])
		if _, found := available[codeHash]; found {
			return fmt.Errorf("available-code log duplicates %s", codeHash)
		}
		available[codeHash] = struct{}{}
	}
	return nil
}

func loadUnavailableCodeFindings(
	path string,
	size int64,
	unavailable map[common.Hash]struct{},
) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	reader := bufio.NewReader(io.LimitReader(file, size))
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) != 0 {
			var finding worldAuditFinding
			if decodeErr := json.Unmarshal(bytes.TrimSpace(line), &finding); decodeErr != nil {
				return decodeErr
			}
			if (finding.Kind == "code_unavailable_by_height" || finding.Kind == "account_code_unavailable") &&
				finding.CodeHash != "" {
				unavailable[common.HexToHash(finding.CodeHash)] = struct{}{}
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
