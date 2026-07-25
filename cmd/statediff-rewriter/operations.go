package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cosmos/iavl"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/evmos/ethermint/debank/statediff"
	dtypes "github.com/evmos/ethermint/debank/types"
	"golang.org/x/sync/errgroup"
)

const (
	backupProofSchema = "statediff-rewriter-backup-proof/v1"
	applyMode         = "apply"
	rollbackMode      = "rollback"
	verifyMode        = "verify"
)

type backupProof struct {
	Schema         string `json:"schema"`
	ManifestSHA256 string `json:"manifest_sha256"`
	Kind           string `json:"kind"`
	SnapshotID     string `json:"snapshot_id"`
	Location       string `json:"location"`
	Status         string `json:"status"`
	Independent    bool   `json:"independent_restore"`
}

type writeReport struct {
	PlannedChanged      int64   `json:"planned_changed"`
	CheckpointFrontier  int64   `json:"checkpoint_frontier"`
	CompletedThisRun    int64   `json:"completed_this_run"`
	AlreadyTarget       int64   `json:"already_target"`
	PUTs                int64   `json:"put_attempts"`
	ConfirmedPUTs       int64   `json:"confirmed_puts"`
	UncertainPUTs       int64   `json:"uncertain_puts_reconciled"`
	Conflicts           int64   `json:"conditional_conflicts_observed_this_run"`
	CumulativePUTs      uint64  `json:"cumulative_put_attempts"`
	CumulativeAlready   uint64  `json:"cumulative_already_target"`
	CumulativeUncertain uint64  `json:"cumulative_uncertain_puts_reconciled"`
	CumulativeConflicts uint64  `json:"cumulative_conditional_conflicts_observed"`
	Concurrency         int     `json:"concurrency"`
	DurationSeconds     float64 `json:"duration_seconds"`
	ObjectsPerSecond    float64 `json:"objects_per_second"`
}

type preparedWrite struct {
	record       packRecord
	needsPUT     bool
	ifMatch      string
	observedETag string
}

type writeOutcome struct {
	ordinal      uint64
	postETag     string
	putOutcome   string
	putAttempted bool
	conflict     bool
}

type writeAudit struct {
	PUTAttempts   uint64
	ConfirmedPUTs uint64
	UncertainPUTs uint64
	AlreadyTarget uint64
	Conflicts     uint64
}

type writeMetrics struct {
	completed     atomic.Int64
	alreadyTarget atomic.Int64
	putAttempts   atomic.Int64
	confirmedPUTs atomic.Int64
	uncertainPUTs atomic.Int64
	conflicts     atomic.Int64
}

func checkedAddUint64(left, right uint64, label string) (uint64, error) {
	if left > ^uint64(0)-right {
		return 0, fmt.Errorf("%s overflows uint64", label)
	}
	return left + right, nil
}

func validateBackupProof(ctx context.Context, path, manifestPath, manifestHash, sourceSnapshotID string, activeFilesystem filesystemStatus) error {
	if err := requireRegularFile(path, "backup proof"); err != nil {
		return err
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var proof backupProof
	if err := decodeStrictJSON(body, &proof, "backup proof"); err != nil {
		return err
	}
	if proof.Schema != backupProofSchema || proof.ManifestSHA256 != manifestHash || proof.Kind != "ebs-snapshot-restore" ||
		proof.SnapshotID == "" || proof.SnapshotID == sourceSnapshotID || proof.Location == "" ||
		proof.Status != "completed" || !proof.Independent {
		return fmt.Errorf("backup proof is incomplete or belongs to another manifest")
	}
	sourceDir, err := filepath.EvalSymlinks(filepath.Dir(manifestPath))
	if err != nil {
		return fmt.Errorf("resolve source plan: %w", err)
	}
	backupInfo, err := os.Lstat(proof.Location)
	if err != nil {
		return fmt.Errorf("stat restored backup: %w", err)
	}
	if !backupInfo.IsDir() || backupInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("restored backup location must be a non-symlink directory")
	}
	backupDir, err := filepath.EvalSymlinks(proof.Location)
	if err != nil {
		return fmt.Errorf("resolve restored backup: %w", err)
	}
	if sourceDir == backupDir {
		return fmt.Errorf("backup proof points to the active plan directory")
	}
	backupFilesystem, err := requireReadOnlyFilesystem(backupDir, "restored backup")
	if err != nil {
		return err
	}
	if backupFilesystem.Device == activeFilesystem.Device {
		return fmt.Errorf("restored backup and active plan are on the same filesystem device")
	}
	if err := requireReadOnlyPlanManifest(backupDir, backupFilesystem); err != nil {
		return err
	}
	backupManifest, backupHash, err := loadPlanManifestContext(ctx, filepath.Join(backupDir, filepath.Base(manifestPath)))
	if err != nil {
		return fmt.Errorf("validate restored backup: %w", err)
	}
	if err := requireReadOnlyPlanArtifacts(backupDir, backupManifest, backupFilesystem); err != nil {
		return fmt.Errorf("validate restored backup artifacts: %w", err)
	}
	if backupHash != manifestHash {
		return fmt.Errorf("restored backup manifest differs from active manifest")
	}
	return nil
}

func runWriteMode(ctx context.Context, manifestPath, checkpointPath, backupPath, mode string, objects objectStore) (writeReport, error) {
	options := defaultParallelOptions()
	options.Concurrency, options.Window = 1, 1
	options.CheckpointEvery = 1
	return runWriteModeWithOptions(ctx, manifestPath, checkpointPath, backupPath, mode, options, objects)
}

func runWriteModeWithOptions(
	ctx context.Context,
	manifestPath, checkpointPath, backupPath, mode string,
	options parallelOptions,
	objects objectStore,
) (writeReport, error) {
	started := time.Now()
	if mode != applyMode && mode != rollbackMode {
		return writeReport{}, fmt.Errorf("unsupported write mode %q", mode)
	}
	if err := options.validate(true); err != nil {
		return writeReport{}, err
	}
	planDir, err := requireSealedPlanDirectory(manifestPath)
	if err != nil {
		return writeReport{}, err
	}
	activeFilesystem, err := requireReadOnlyFilesystem(planDir, "active sealed plan")
	if err != nil {
		return writeReport{}, err
	}
	if err := requireReadOnlyPlanManifest(planDir, activeFilesystem); err != nil {
		return writeReport{}, err
	}
	manifest, manifestHash, err := loadPlanManifestContext(ctx, manifestPath)
	if err != nil {
		return writeReport{}, err
	}
	if err := requireProductionPlan(manifest, mode); err != nil {
		return writeReport{}, err
	}
	if err := requireRuntimeBuildIdentity(manifest); err != nil {
		return writeReport{}, err
	}
	if err := requireReadOnlyPlanArtifacts(planDir, manifest, activeFilesystem); err != nil {
		return writeReport{}, err
	}
	if mode == applyMode {
		if err := validateBackupProof(ctx, backupPath, manifestPath, manifestHash, manifest.SnapshotID, activeFilesystem); err != nil {
			return writeReport{}, fmt.Errorf("apply requires completed independent backup proof: %w", err)
		}
	}
	checkpointPath, err = canonicalFileInExistingDirectory(checkpointPath, mode+" checkpoint")
	if err != nil {
		return writeReport{}, err
	}
	locks, err := acquireStagingLocks(checkpointPath, filepath.Join(filepath.Dir(checkpointPath), "statediff-rewriter-write"))
	if err != nil {
		return writeReport{}, err
	}
	defer func() { _ = releaseStagingLocks(locks) }()
	if err := syncDir(filepath.Dir(checkpointPath)); err != nil {
		return writeReport{}, fmt.Errorf("sync %s checkpoint directory: %w", mode, err)
	}
	cp, err := loadCheckpoint(checkpointPath, manifest.RunID, manifestHash, mode)
	if err != nil {
		return writeReport{}, err
	}
	if cp.Frontier > uint64(manifest.Changed) || (cp.Frontier == 0) != (cp.Height == 0) {
		return writeReport{}, fmt.Errorf("%s checkpoint frontier/height is outside the manifest", mode)
	}
	checkpointReport := writeReport{
		PlannedChanged: manifest.Changed, CheckpointFrontier: int64(cp.Frontier), Concurrency: options.Concurrency,
		CumulativePUTs: cp.PUTAttempts, CumulativeAlready: cp.AlreadyTarget, CumulativeUncertain: cp.UncertainPUTs,
		CumulativeConflicts: cp.Conflicts,
	}
	initialFrontier := cp.Frontier
	journalPath := writeJournalPath(checkpointPath)
	journal, journalFound, err := loadWriteJournal(journalPath, manifest.RunID, manifestHash, mode)
	if err != nil {
		return writeReport{}, err
	}
	if journalFound && (journal.BatchRecords > uint64(options.Window) || journal.BatchBytes > options.MaxInFlightBytes) {
		return writeReport{}, fmt.Errorf(
			"%s write journal batch needs window >= %d and max-inflight-bytes >= %d",
			mode, journal.BatchRecords, journal.BatchBytes,
		)
	}
	if journalFound && journal.End < cp.Frontier {
		return writeReport{}, fmt.Errorf("%s write journal end %d is behind checkpoint %d", mode, journal.End, cp.Frontier)
	}
	if journalFound && journal.End == cp.Frontier {
		if journal.State != writeJournalObserved {
			return writeReport{}, fmt.Errorf("%s checkpoint %d crosses unresolved issued journal %d-%d", mode, cp.Frontier, journal.Start, journal.End)
		}
		if err := removeWriteJournal(journalPath); err != nil {
			return writeReport{}, err
		}
		journalFound = false
	}
	if manifest.Changed > 0 && cp.Frontier == uint64(manifest.Changed) {
		return checkpointReport, fmt.Errorf("%s checkpoint is already complete", mode)
	}
	if err := validatePlanForWriteContext(ctx, filepath.Dir(manifestPath), manifest); err != nil {
		return writeReport{}, fmt.Errorf("%s plan preflight: %w", mode, err)
	}
	report := checkpointReport
	var metrics writeMetrics
	stream := newPackStream(filepath.Dir(manifestPath), manifest)
	defer stream.Close()
	if cp.Frontier > 0 {
		if err := advanceWriteCheckpointPrefix(mode, cp, stream, options); err != nil {
			report.CompletedThisRun = metrics.completed.Load()
			report.AlreadyTarget = metrics.alreadyTarget.Load()
			report.DurationSeconds = time.Since(started).Seconds()
			return report, err
		}
	}

	for {
		var batch []packRecord
		if journalFound {
			if journal.Start != cp.Frontier+1 || journal.End > uint64(manifest.Changed) {
				return writeReport{}, fmt.Errorf("%s write journal range %d-%d does not follow checkpoint %d", mode, journal.Start, journal.End, cp.Frontier)
			}
			batch, err = readWriteBatch(stream, options, journal.End)
		} else {
			batch, err = readWriteBatch(stream, options, 0)
		}
		if err != nil {
			return writeReport{}, err
		}
		if len(batch) == 0 {
			break
		}
		if journalFound && (batch[0].Ordinal != journal.Start || batch[len(batch)-1].Ordinal != journal.End) {
			return writeReport{}, fmt.Errorf("%s write journal range does not match pack records", mode)
		}
		if journalFound {
			if err := validateRecoveredBatch(journal, batch); err != nil {
				return writeReport{}, err
			}
		}
		if objects == nil {
			objects, err = newS3ObjectStore(ctx, manifest.Region, options.Concurrency)
			if err != nil {
				return writeReport{}, err
			}
		}
		audit, err := processWriteBatch(ctx, manifest, manifestHash, mode, journalPath, batch, journal, journalFound, options.Concurrency, objects, &metrics)
		if err != nil {
			report.CompletedThisRun = metrics.completed.Load()
			report.AlreadyTarget = metrics.alreadyTarget.Load()
			report.PUTs = metrics.putAttempts.Load()
			report.ConfirmedPUTs = metrics.confirmedPUTs.Load()
			report.UncertainPUTs = metrics.uncertainPUTs.Load()
			report.Conflicts = metrics.conflicts.Load()
			report.DurationSeconds = time.Since(started).Seconds()
			return report, err
		}
		last := batch[len(batch)-1]
		cp.Frontier, cp.Height = last.Ordinal, last.Height
		if cp.PUTAttempts, err = checkedAddUint64(cp.PUTAttempts, audit.PUTAttempts, "checkpoint PUT attempts"); err != nil {
			return report, err
		}
		if cp.ConfirmedPUTs, err = checkedAddUint64(cp.ConfirmedPUTs, audit.ConfirmedPUTs, "checkpoint confirmed PUTs"); err != nil {
			return report, err
		}
		if cp.UncertainPUTs, err = checkedAddUint64(cp.UncertainPUTs, audit.UncertainPUTs, "checkpoint uncertain PUTs"); err != nil {
			return report, err
		}
		if cp.AlreadyTarget, err = checkedAddUint64(cp.AlreadyTarget, audit.AlreadyTarget, "checkpoint already-target count"); err != nil {
			return report, err
		}
		if cp.Conflicts, err = checkedAddUint64(cp.Conflicts, audit.Conflicts, "checkpoint conditional conflict count"); err != nil {
			return report, err
		}
		if err := saveCheckpoint(checkpointPath, cp); err != nil {
			return report, err
		}
		if err := removeWriteJournal(journalPath); err != nil {
			return report, err
		}
		report.CheckpointFrontier = int64(cp.Frontier)
		report.CumulativePUTs = cp.PUTAttempts
		report.CumulativeAlready = cp.AlreadyTarget
		report.CumulativeUncertain = cp.UncertainPUTs
		report.CumulativeConflicts = cp.Conflicts
		journal, journalFound = writeJournal{}, false
	}
	report.CompletedThisRun = metrics.completed.Load()
	report.AlreadyTarget = metrics.alreadyTarget.Load()
	report.PUTs = metrics.putAttempts.Load()
	report.ConfirmedPUTs = metrics.confirmedPUTs.Load()
	report.UncertainPUTs = metrics.uncertainPUTs.Load()
	report.Conflicts = metrics.conflicts.Load()
	report.DurationSeconds = time.Since(started).Seconds()
	if report.DurationSeconds > 0 {
		report.ObjectsPerSecond = float64(report.CompletedThisRun) / report.DurationSeconds
	}
	if report.CheckpointFrontier != manifest.Changed {
		return report, fmt.Errorf("%s continuous checkpoint covers %d records, want %d", mode, report.CheckpointFrontier, manifest.Changed)
	}
	expectedThisRun := manifest.Changed - int64(initialFrontier)
	if report.CompletedThisRun != expectedThisRun {
		return report, fmt.Errorf("%s completed %d records this run, want %d", mode, report.CompletedThisRun, expectedThisRun)
	}
	return report, nil
}

func advanceWriteCheckpointPrefix(
	mode string,
	cp checkpoint,
	stream *packStream,
	options parallelOptions,
) error {
	var last packRecord
	for last.Ordinal < cp.Frontier {
		batch, err := readWritePrefixBatch(stream, options, cp.Frontier)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return fmt.Errorf("%s checkpoint ordinal %d was not found", mode, cp.Frontier)
		}
		last = batch[len(batch)-1]
	}
	if last.Ordinal != cp.Frontier {
		return fmt.Errorf("%s checkpoint ordinal %d was not found", mode, cp.Frontier)
	}
	if last.Height != cp.Height {
		return fmt.Errorf("%s checkpoint ordinal %d maps to height %d, not %d", mode, cp.Frontier, last.Height, cp.Height)
	}
	return nil
}

func readWritePrefixBatch(stream *packStream, options parallelOptions, frontier uint64) ([]packRecord, error) {
	maxRecords := uint64(options.Window)
	var batch []packRecord
	var bytes int64
	for uint64(len(batch)) < maxRecords {
		record, ok, err := stream.Peek()
		if err != nil {
			return nil, err
		}
		if !ok || record.Ordinal > frontier {
			break
		}
		weight := estimatedPackRecordBytes(record)
		if weight > options.MaxInFlightBytes {
			return nil, fmt.Errorf("pack record %d needs %d in-flight bytes, limit is %d", record.Ordinal, weight, options.MaxInFlightBytes)
		}
		if len(batch) > 0 && bytes > options.MaxInFlightBytes-weight {
			break
		}
		record, _, err = stream.Next()
		if err != nil {
			return nil, err
		}
		batch = append(batch, record)
		bytes += weight
		if record.Ordinal == frontier {
			break
		}
	}
	return batch, nil
}

func readWriteBatch(stream *packStream, options parallelOptions, forcedEnd uint64) ([]packRecord, error) {
	maxRecords := uint64(options.Window)
	if forcedEnd == 0 && options.CheckpointEvery < maxRecords {
		maxRecords = options.CheckpointEvery
	}
	var batch []packRecord
	var bytes int64
	for uint64(len(batch)) < maxRecords {
		record, ok, err := stream.Peek()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		if forcedEnd != 0 && record.Ordinal > forcedEnd {
			break
		}
		weight := estimatedPackRecordBytes(record)
		if weight > options.MaxInFlightBytes {
			return nil, fmt.Errorf("pack record %d needs %d in-flight bytes, limit is %d", record.Ordinal, weight, options.MaxInFlightBytes)
		}
		if len(batch) > 0 && bytes > options.MaxInFlightBytes-weight {
			if forcedEnd != 0 {
				return nil, fmt.Errorf("recovered write journal exceeds max-inflight-bytes %d", options.MaxInFlightBytes)
			}
			break
		}
		record, _, err = stream.Next()
		if err != nil {
			return nil, err
		}
		batch = append(batch, record)
		bytes += weight
		if forcedEnd != 0 && record.Ordinal == forcedEnd {
			break
		}
	}
	if forcedEnd != 0 && (len(batch) == 0 || batch[len(batch)-1].Ordinal != forcedEnd) {
		return nil, fmt.Errorf("write journal end ordinal %d was not found in pack", forcedEnd)
	}
	return batch, nil
}

func validateRecoveredBatch(journal writeJournal, records []packRecord) error {
	if uint64(len(records)) != journal.BatchRecords {
		return fmt.Errorf("recovered batch has %d records, journal has %d", len(records), journal.BatchRecords)
	}
	var bytes int64
	for _, record := range records {
		weight := estimatedPackRecordBytes(record)
		if weight <= 0 || weight > journal.BatchBytes-bytes {
			return fmt.Errorf("recovered batch byte count is invalid")
		}
		bytes += weight
	}
	if bytes != journal.BatchBytes {
		return fmt.Errorf("recovered batch has %d estimated bytes, journal has %d", bytes, journal.BatchBytes)
	}
	return nil
}

func processWriteBatch(
	ctx context.Context,
	manifest planManifest,
	manifestHash, mode, journalPath string,
	records []packRecord,
	existing writeJournal,
	recovering bool,
	concurrency int,
	objects objectStore,
	metrics *writeMetrics,
) (writeAudit, error) {
	var prepared []preparedWrite
	var err error
	if mode == applyMode && !recovering {
		prepared, err = prepareFreshApplyBatch(records)
	} else {
		prepared, err = prepareWriteBatch(ctx, manifest, records, mode, concurrency, objects)
	}
	if err != nil {
		return writeAudit{}, err
	}
	journal := existing
	if recovering {
		if err := matchJournalToPrepared(journal, prepared, mode); err != nil {
			return writeAudit{}, err
		}
	} else {
		journal = newIssuedWriteJournal(manifest, manifestHash, mode, prepared)
		if len(journal.Intents) > 0 {
			if err := saveWriteJournal(journalPath, journal); err != nil {
				return writeAudit{}, fmt.Errorf("persist %s write intents: %w", mode, err)
			}
		}
	}
	outcomes, writeErr := executeWriteBatch(ctx, manifest, prepared, mode, concurrency, objects, metrics)
	if len(journal.Intents) == 0 {
		return writeAudit{AlreadyTarget: uint64(len(prepared))}, writeErr
	}
	if err := updateJournalOutcomes(&journal, outcomes, mode); err != nil {
		return writeAudit{}, err
	}
	if writeErr == nil && len(journal.Results) == len(journal.Intents) {
		journal.State = writeJournalObserved
	} else {
		journal.State = writeJournalIssued
	}
	journalErr := saveWriteJournal(journalPath, journal)
	if writeErr != nil || journalErr != nil {
		return writeAudit{}, errors.Join(writeErr, journalErr)
	}
	if journal.State != writeJournalObserved {
		return writeAudit{}, fmt.Errorf("%s batch %d-%d has unresolved write intents", mode, journal.Start, journal.End)
	}
	return journalAudit(journal)
}

func prepareFreshApplyBatch(records []packRecord) ([]preparedWrite, error) {
	prepared := make([]preparedWrite, len(records))
	for index, record := range records {
		if record.OldETag == "" {
			return nil, fmt.Errorf("apply height %d has no sealed old ETag", record.Height)
		}
		prepared[index] = preparedWrite{record: record, needsPUT: true, ifMatch: record.OldETag}
	}
	return prepared, nil
}

func prepareWriteBatch(
	ctx context.Context,
	manifest planManifest,
	records []packRecord,
	mode string,
	concurrency int,
	objects objectStore,
) ([]preparedWrite, error) {
	prepared := make([]preparedWrite, len(records))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(concurrency)
	for index := range records {
		group.Go(func() error {
			item, err := prepareWriteRecord(groupCtx, manifest, records[index], mode, objects)
			if err != nil {
				return err
			}
			prepared[index] = item
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return prepared, nil
}

func prepareWriteRecord(
	ctx context.Context,
	manifest planManifest,
	record packRecord,
	mode string,
	objects objectStore,
) (preparedWrite, error) {
	sourceSHA, targetSHA := record.OldSHA256, record.NewSHA256
	if mode == rollbackMode {
		sourceSHA, targetSHA = targetSHA, sourceSHA
	}
	current, err := objects.Get(ctx, manifest.Bucket, record.Key)
	if err != nil {
		return preparedWrite{}, fmt.Errorf("%s get height %d: %w", mode, record.Height, err)
	}
	switch sha256Hash(current.Body) {
	case targetSHA:
		if err := verifyStoredRecordTarget(record, current, targetSHA); err != nil {
			return preparedWrite{}, fmt.Errorf("%s verify existing target height %d: %w", mode, record.Height, err)
		}
		return preparedWrite{record: record, observedETag: current.ETag}, nil
	case sourceSHA:
		if !reflect.DeepEqual(current.Headers, record.Headers) {
			return preparedWrite{}, fmt.Errorf("%s height %d source object metadata changed", mode, record.Height)
		}
		if mode == applyMode && current.ETag != record.OldETag {
			return preparedWrite{}, fmt.Errorf("apply height %d old bytes have changed ETag", record.Height)
		}
	default:
		return preparedWrite{}, fmt.Errorf("%s height %d has third-party content", mode, record.Height)
	}
	return preparedWrite{record: record, needsPUT: true, ifMatch: current.ETag}, nil
}

func executeWriteBatch(
	ctx context.Context,
	manifest planManifest,
	prepared []preparedWrite,
	mode string,
	concurrency int,
	objects objectStore,
	metrics *writeMetrics,
) ([]writeOutcome, error) {
	outcomes := make([]writeOutcome, len(prepared))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(concurrency)
	for index := range prepared {
		if !prepared[index].needsPUT {
			outcomes[index] = writeOutcome{
				ordinal:  prepared[index].record.Ordinal,
				postETag: prepared[index].observedETag, putOutcome: writeOutcomeReconciled,
			}
			metrics.alreadyTarget.Add(1)
			metrics.completed.Add(1)
			continue
		}
		group.Go(func() error {
			outcome, err := executePreparedWrite(groupCtx, manifest, prepared[index], mode, objects, metrics)
			outcomes[index] = outcome
			if err != nil {
				return err
			}
			return nil
		})
	}
	return outcomes, group.Wait()
}

func executePreparedWrite(
	ctx context.Context,
	manifest planManifest,
	prepared preparedWrite,
	mode string,
	objects objectStore,
	metrics *writeMetrics,
) (writeOutcome, error) {
	record := prepared.record
	targetBody, targetSHA := record.NewBody, record.NewSHA256
	if mode == rollbackMode {
		targetBody, targetSHA = record.OldBody, record.OldSHA256
	}

	metrics.putAttempts.Add(1)
	putResult, putErr := objects.Put(ctx, manifest.Bucket, record.Key, targetBody, prepared.ifMatch, record.Headers)
	if mode == applyMode {
		return resolveApplyPut(ctx, manifest, prepared, putResult, putErr, objects, metrics)
	}
	if errors.Is(putErr, errObjectConflict) {
		metrics.conflicts.Add(1)
		return writeOutcome{ordinal: record.Ordinal, putAttempted: true, conflict: true}, fmt.Errorf("%s PUT height %d has a conditional conflict: %w", mode, record.Height, putErr)
	}
	if putErr == nil {
		metrics.confirmedPUTs.Add(1)
	}
	observed, getErr := objects.Get(ctx, manifest.Bucket, record.Key)
	if getErr != nil {
		outcome := ""
		if putErr == nil {
			outcome = writeOutcomeConfirmed
		}
		return writeOutcome{ordinal: record.Ordinal, putAttempted: true, putOutcome: outcome}, errors.Join(
			fmt.Errorf("%s verify GET height %d: %w", mode, record.Height, getErr),
			putErr,
		)
	}
	if err := verifyStoredRecordTarget(record, observed, targetSHA); err != nil {
		outcome := ""
		if putErr == nil {
			outcome = writeOutcomeConfirmed
		}
		return writeOutcome{ordinal: record.Ordinal, putAttempted: true, putOutcome: outcome}, errors.Join(
			fmt.Errorf("%s verify height %d: %w", mode, record.Height, err),
			putErr,
		)
	}
	if putErr != nil {
		metrics.uncertainPUTs.Add(1)
	}
	metrics.completed.Add(1)
	outcome := writeOutcomeConfirmed
	if putErr != nil {
		outcome = writeOutcomeUncertain
	}
	return writeOutcome{
		ordinal: record.Ordinal, postETag: observed.ETag, putOutcome: outcome, putAttempted: true,
	}, nil
}

func resolveApplyPut(
	ctx context.Context,
	manifest planManifest,
	prepared preparedWrite,
	result putResult,
	putErr error,
	objects objectStore,
	metrics *writeMetrics,
) (writeOutcome, error) {
	record := prepared.record
	if putErr == nil {
		if result.ETag == "" {
			putErr = fmt.Errorf("%w: PUT response has no ETag", errObjectWriteUncertain)
		} else {
			metrics.confirmedPUTs.Add(1)
			metrics.completed.Add(1)
			return writeOutcome{
				ordinal: record.Ordinal, postETag: result.ETag, putOutcome: writeOutcomeConfirmed, putAttempted: true,
			}, nil
		}
	}
	if !errors.Is(putErr, errObjectConflict) && !errors.Is(putErr, errObjectWriteUncertain) {
		return writeOutcome{ordinal: record.Ordinal, putAttempted: true}, fmt.Errorf("apply PUT height %d: %w", record.Height, putErr)
	}
	if errors.Is(putErr, errObjectConflict) {
		metrics.conflicts.Add(1)
	}
	conflict := errors.Is(putErr, errObjectConflict)

	current, getErr := objects.Get(ctx, manifest.Bucket, record.Key)
	if getErr != nil {
		return writeOutcome{ordinal: record.Ordinal, putAttempted: true, conflict: conflict}, errors.Join(
			fmt.Errorf("apply classify GET height %d: %w", record.Height, getErr),
			putErr,
		)
	}
	switch sha256Hash(current.Body) {
	case record.NewSHA256:
		if err := verifyStoredRecordTarget(record, current, record.NewSHA256); err != nil {
			return writeOutcome{ordinal: record.Ordinal, putAttempted: true, conflict: conflict}, errors.Join(
				fmt.Errorf("apply classify target height %d: %w", record.Height, err),
				putErr,
			)
		}
		outcome := writeOutcomeUncertain
		if errors.Is(putErr, errObjectConflict) {
			metrics.alreadyTarget.Add(1)
			outcome = writeOutcomeReconciled
		} else {
			metrics.uncertainPUTs.Add(1)
		}
		metrics.completed.Add(1)
		return writeOutcome{
			ordinal: record.Ordinal, postETag: current.ETag, putOutcome: outcome, putAttempted: true, conflict: conflict,
		}, nil
	case record.OldSHA256:
		if err := verifyStoredRecordTarget(record, current, record.OldSHA256); err != nil {
			return writeOutcome{ordinal: record.Ordinal, putAttempted: true, conflict: conflict}, errors.Join(
				fmt.Errorf("apply classify source height %d: %w", record.Height, err),
				putErr,
			)
		}
		if current.ETag != prepared.ifMatch {
			return writeOutcome{ordinal: record.Ordinal, putAttempted: true, conflict: conflict}, errors.Join(
				fmt.Errorf("apply height %d source ETag changed after conditional PUT", record.Height),
				putErr,
			)
		}
		return writeOutcome{ordinal: record.Ordinal, putAttempted: true, conflict: conflict}, errors.Join(
			fmt.Errorf("apply PUT height %d left source content; resume can safely retry", record.Height),
			putErr,
		)
	default:
		return writeOutcome{ordinal: record.Ordinal, putAttempted: true, conflict: conflict}, errors.Join(
			fmt.Errorf("apply height %d has third-party content after PUT", record.Height),
			putErr,
		)
	}
}

func newIssuedWriteJournal(manifest planManifest, manifestHash, mode string, prepared []preparedWrite) writeJournal {
	journal := writeJournal{
		Schema: writeJournalSchema, RunID: manifest.RunID, ManifestHash: manifestHash, Operation: mode,
		Start: prepared[0].record.Ordinal, End: prepared[len(prepared)-1].record.Ordinal, State: writeJournalIssued,
		BatchRecords: uint64(len(prepared)),
	}
	for _, item := range prepared {
		journal.BatchBytes += estimatedPackRecordBytes(item.record)
		if !item.needsPUT {
			journal.AlreadyTarget++
			continue
		}
		journal.Intents = append(journal.Intents, writeIntent{
			Operation: mode, Ordinal: item.record.Ordinal, Height: item.record.Height, Key: item.record.Key,
			OldSHA256: hex.EncodeToString(item.record.OldSHA256[:]), NewSHA256: hex.EncodeToString(item.record.NewSHA256[:]),
			IfMatch: item.ifMatch,
		})
	}
	return journal
}

func matchJournalToPrepared(journal writeJournal, prepared []preparedWrite, mode string) error {
	byOrdinal := make(map[uint64]writeIntent, len(journal.Intents))
	for _, intent := range journal.Intents {
		byOrdinal[intent.Ordinal] = intent
	}
	observed := make(map[uint64]struct{}, len(journal.Results))
	for _, result := range journal.Results {
		observed[result.Ordinal] = struct{}{}
	}
	for _, item := range prepared {
		intent, found := byOrdinal[item.record.Ordinal]
		if !found {
			if item.needsPUT {
				return fmt.Errorf("%s recovered source height %d has no persisted write intent", mode, item.record.Height)
			}
			continue
		}
		if intent.Operation != mode || intent.Height != item.record.Height || intent.Key != item.record.Key ||
			intent.OldSHA256 != hex.EncodeToString(item.record.OldSHA256[:]) ||
			intent.NewSHA256 != hex.EncodeToString(item.record.NewSHA256[:]) {
			return fmt.Errorf("%s write intent %d differs from sealed pack", mode, intent.Ordinal)
		}
		if item.needsPUT && intent.IfMatch != item.ifMatch {
			return fmt.Errorf("%s write intent %d If-Match ETag changed during recovery", mode, intent.Ordinal)
		}
		if item.needsPUT {
			if _, found := observed[intent.Ordinal]; found {
				return fmt.Errorf("%s write intent %d previously observed its target but now sees source content", mode, intent.Ordinal)
			}
			if intent.ConfirmedPUTs != 0 || intent.UncertainPUTs != 0 {
				return fmt.Errorf("%s write intent %d has a persisted PUT outcome but now sees source content", mode, intent.Ordinal)
			}
		}
		delete(byOrdinal, item.record.Ordinal)
	}
	if len(byOrdinal) != 0 {
		return fmt.Errorf("%s write journal contains intents outside recovered batch", mode)
	}
	return nil
}

func updateJournalOutcomes(journal *writeJournal, outcomes []writeOutcome, mode string) error {
	byOrdinal := make(map[uint64]writeOutcome, len(outcomes))
	for _, outcome := range outcomes {
		if outcome.ordinal != 0 {
			byOrdinal[outcome.ordinal] = outcome
		}
	}
	existing := make(map[uint64]writeObservedResult, len(journal.Results))
	for _, result := range journal.Results {
		existing[result.Ordinal] = result
	}
	results := make([]writeObservedResult, 0, len(journal.Intents))
	for index := range journal.Intents {
		intent := &journal.Intents[index]
		outcome, found := byOrdinal[intent.Ordinal]
		if !found {
			continue
		}
		if result, found := existing[intent.Ordinal]; found {
			results = append(results, result)
			continue
		}
		if outcome.putAttempted {
			var err error
			intent.PUTAttempts, err = checkedAddUint64(intent.PUTAttempts, 1, "write intent PUT attempts")
			if err != nil {
				return err
			}
			switch outcome.putOutcome {
			case writeOutcomeConfirmed:
				intent.ConfirmedPUTs, err = checkedAddUint64(intent.ConfirmedPUTs, 1, "write intent confirmed PUTs")
			case writeOutcomeUncertain:
				intent.UncertainPUTs, err = checkedAddUint64(intent.UncertainPUTs, 1, "write intent uncertain PUTs")
			}
			if err != nil {
				return err
			}
			if outcome.conflict {
				intent.Conflicts, err = checkedAddUint64(intent.Conflicts, 1, "write intent conditional conflicts")
			}
			if err != nil {
				return err
			}
		}
		if outcome.postETag == "" {
			continue
		}
		resultOutcome := outcome.putOutcome
		if resultOutcome == writeOutcomeReconciled {
			switch {
			case intent.ConfirmedPUTs > 0:
				resultOutcome = writeOutcomeConfirmed
			case intent.UncertainPUTs > 0:
				resultOutcome = writeOutcomeUncertain
			case outcome.putAttempted:
				// A conditional conflict is a definite non-write. Seeing the target
				// afterwards means this object was already complete.
			default:
				// The intent was durable before the PUT. Seeing its target without a
				// durable result proves that an earlier attempt may have committed.
				if intent.PUTAttempts == 0 {
					intent.PUTAttempts = 1
				}
				var err error
				intent.UncertainPUTs, err = checkedAddUint64(intent.UncertainPUTs, 1, "write intent uncertain PUTs")
				if err != nil {
					return err
				}
				resultOutcome = writeOutcomeUncertain
			}
		}
		observedSHA := intent.NewSHA256
		if mode == rollbackMode {
			observedSHA = intent.OldSHA256
		}
		results = append(results, writeObservedResult{
			Ordinal: intent.Ordinal, ObservedSHA256: observedSHA,
			PostPUTETag: outcome.postETag, Outcome: resultOutcome,
		})
	}
	journal.Results = results
	return nil
}

func journalAudit(journal writeJournal) (writeAudit, error) {
	audit := writeAudit{AlreadyTarget: journal.AlreadyTarget}
	for _, intent := range journal.Intents {
		var err error
		if audit.PUTAttempts, err = checkedAddUint64(audit.PUTAttempts, intent.PUTAttempts, "journal PUT attempts"); err != nil {
			return writeAudit{}, err
		}
		if audit.ConfirmedPUTs, err = checkedAddUint64(audit.ConfirmedPUTs, intent.ConfirmedPUTs, "journal confirmed PUTs"); err != nil {
			return writeAudit{}, err
		}
		if audit.UncertainPUTs, err = checkedAddUint64(audit.UncertainPUTs, intent.UncertainPUTs, "journal uncertain PUTs"); err != nil {
			return writeAudit{}, err
		}
		if audit.Conflicts, err = checkedAddUint64(audit.Conflicts, intent.Conflicts, "journal conditional conflicts"); err != nil {
			return writeAudit{}, err
		}
	}
	for _, result := range journal.Results {
		if result.Outcome != writeOutcomeReconciled {
			continue
		}
		var err error
		audit.AlreadyTarget, err = checkedAddUint64(audit.AlreadyTarget, 1, "journal already-target count")
		if err != nil {
			return writeAudit{}, err
		}
	}
	return audit, nil
}

func verifyStoredRecordTarget(record packRecord, object storedObject, expectedSHA [32]byte) error {
	if err := verifyRecordTarget(record, object.Body, expectedSHA); err != nil {
		return err
	}
	if !reflect.DeepEqual(object.Headers, record.Headers) {
		return fmt.Errorf("object metadata changed")
	}
	return nil
}

func verifyRecordTarget(record packRecord, body []byte, expectedSHA [32]byte) error {
	if sha256Hash(body) != expectedSHA {
		return fmt.Errorf("body SHA-256 mismatch")
	}
	var diff dtypes.BlockStorageDiff
	if err := rlp.DecodeBytes(body, &diff); err != nil {
		return err
	}
	digest, err := retainedDigest(diff)
	if err != nil {
		return err
	}
	if digest != record.RetainedSHA256 {
		return fmt.Errorf("retained fields changed")
	}
	return nil
}

type verifyReport struct {
	Processed        int64   `json:"processed"`
	VerifiedChanged  int64   `json:"verified_changed"`
	SkippedEqual     int64   `json:"skipped_equal_root"`
	SemanticChanges  int64   `json:"semantic_changes_remaining"`
	S3GETs           int64   `json:"s3_gets_this_run"`
	Concurrency      int     `json:"concurrency"`
	DurationSeconds  float64 `json:"duration_seconds"`
	ObjectsPerSecond float64 `json:"objects_per_second"`
}

type verifyTask struct {
	height    uint64
	root      common.Hash
	parent    common.Hash
	key       string
	canonical []dtypes.AccountStorageDiff
	planned   *packRecord
	bytes     int64
}

type verifyOutcome struct {
	height  uint64
	changed bool
	skipped bool
	got     bool
	bytes   int64
}

func runVerify(ctx context.Context, manifestPath, checkpointDir string, objects objectStore) (verifyReport, error) {
	options := defaultParallelOptions()
	options.Concurrency, options.Window = 1, 1
	options.CheckpointEvery = 1
	return runVerifyWithOptions(ctx, manifestPath, checkpointDir, options, objects)
}

func runVerifyWithOptions(
	ctx context.Context,
	manifestPath, checkpointDir string,
	options parallelOptions,
	objects objectStore,
) (verifyReport, error) {
	started := time.Now()
	if err := options.validate(true); err != nil {
		return verifyReport{}, err
	}
	if options.MaxInFlightBytes < maxObjectOperationBytes+maxObjectSize {
		return verifyReport{}, fmt.Errorf("verify max-inflight-bytes must be at least %d", maxObjectOperationBytes+maxObjectSize)
	}
	planDir, err := requireSealedPlanDirectory(manifestPath)
	if err != nil {
		return verifyReport{}, err
	}
	activeFilesystem, err := requireReadOnlyFilesystem(planDir, "active sealed plan")
	if err != nil {
		return verifyReport{}, err
	}
	if err := requireReadOnlyPlanManifest(planDir, activeFilesystem); err != nil {
		return verifyReport{}, err
	}
	manifest, manifestHash, err := loadPlanManifestContext(ctx, manifestPath)
	if err != nil {
		return verifyReport{}, err
	}
	if err := requireProductionPlan(manifest, "verify"); err != nil {
		return verifyReport{}, err
	}
	if err := requireRuntimeBuildIdentity(manifest); err != nil {
		return verifyReport{}, err
	}
	if err := requireReadOnlyPlanArtifacts(planDir, manifest, activeFilesystem); err != nil {
		return verifyReport{}, err
	}
	var directArchive *archiveReader
	var dumpInfo dumpManifest
	var iterateVerifySource func(context.Context, func(int64, []dtypes.AccountStorageDiff) error) error
	switch effectivePlanSourceMode(manifest) {
	case planSourceDumpV1:
		dumpDir, err := requireSealedDumpDirectory(manifest.DumpPath)
		if err != nil {
			return verifyReport{}, err
		}
		if _, err := requireReadOnlyFilesystem(dumpDir, "sealed dump"); err != nil {
			return verifyReport{}, err
		}
		dumpBody, err := readRegularFileNoFollow(filepath.Join(dumpDir, "dump-manifest.v1.json"), "sealed dump manifest")
		if err != nil {
			return verifyReport{}, err
		}
		if sha256Hex(dumpBody) != manifest.DumpManifestHash {
			return verifyReport{}, fmt.Errorf("dump manifest hash mismatch")
		}
		if err := decodeStrictJSON(dumpBody, &dumpInfo, "dump manifest"); err != nil {
			return verifyReport{}, err
		}
		if manifest.Schema == segmentManifestSchema {
			if manifest.DumpProducer == nil {
				return verifyReport{}, fmt.Errorf("production segment has no dump producer identity")
			}
			if err := validateDumpContext(dumpInfo, *manifest.DumpProducer); err != nil {
				return verifyReport{}, err
			}
			if manifest.FirstHeight < dumpInfo.FirstVersion || manifest.FinalHeight > dumpInfo.LastVersion {
				return verifyReport{}, fmt.Errorf("sealed dump does not cover production segment")
			}
		} else {
			if err := validateDumpContext(dumpInfo, dumpContext{
				Schema: dumpManifestSchema, FirstVersion: 1, LastVersion: manifest.FinalHeight,
				SnapshotID: manifest.SnapshotID, ArchiveIdentity: manifest.ArchiveIdentity,
				CronosCommit: manifest.CronosCommit, EthermintCommit: manifest.EthermintCommit,
				IAVLCommit:  manifest.IAVLCommit,
				ImageDigest: manifest.ImageDigest, BuildTags: manifest.BuildTags,
			}); err != nil {
				return verifyReport{}, err
			}
		}
		if _, err := requireReadOnlySealedDump(manifest.DumpPath, dumpInfo); err != nil {
			return verifyReport{}, err
		}
		iterateVerifySource = func(iterCtx context.Context, callback func(int64, []dtypes.AccountStorageDiff) error) error {
			consume := func(height int64, changeSet *iavl.ChangeSet) error {
				if height == 1 {
					return callback(height, nil)
				}
				canonical, err := statediff.CanonicalStorageDiff(changeSet)
				if err != nil {
					return fmt.Errorf("height %d canonical storage: %w", height, err)
				}
				return callback(height, canonical)
			}
			if manifest.Schema == segmentManifestSchema {
				return iterateSealedDumpRangeContext(
					iterCtx, manifest.DumpPath, dumpInfo, manifest.FirstHeight, manifest.FinalHeight, consume,
				)
			}
			return iterateSealedDumpContext(iterCtx, manifest.DumpPath, dumpInfo, consume)
		}
	case planSourceDirectV1:
		directArchive, err = openArchive(manifest.ArchiveIdentity.Home)
		if err != nil {
			return verifyReport{}, err
		}
		defer directArchive.Close()
		directIdentity, err := directArchive.identity()
		if err != nil {
			return verifyReport{}, err
		}
		if directIdentity != manifest.ArchiveIdentity {
			return verifyReport{}, fmt.Errorf("direct IAVL archive identity differs from sealed plan")
		}
		iterateVerifySource = func(iterCtx context.Context, callback func(int64, []dtypes.AccountStorageDiff) error) error {
			return iterateArchiveDirectStorageDiffsContext(
				iterCtx, directArchive, defaultDumpCacheSize, defaultDirectIAVLConcurrency,
				defaultDirectIAVLRunHeights, manifest.FirstHeight, manifest.FinalHeight, callback,
			)
		}
	default:
		return verifyReport{}, fmt.Errorf("unsupported plan source mode %q", manifest.SourceMode)
	}
	checkpointDir, err = ensureNonSymlinkDirectory(checkpointDir, "verify checkpoint directory")
	if err != nil {
		return verifyReport{}, err
	}
	checkpointPath := filepath.Join(checkpointDir, "verify.json")
	locks, err := acquireStagingLocks(checkpointPath, filepath.Join(checkpointDir, "statediff-rewriter-write"))
	if err != nil {
		return verifyReport{}, err
	}
	defer func() { _ = releaseStagingLocks(locks) }()
	cpPath := checkpointPath
	cp, err := loadCheckpoint(cpPath, manifest.RunID, manifestHash, "verify")
	if err != nil {
		return verifyReport{}, err
	}
	if cp.Frontier == 0 {
		if cp.Height != 0 || cp.Changed != 0 {
			return verifyReport{}, fmt.Errorf("verify checkpoint has a non-zero height or changed frontier at zero")
		}
	} else if cp.Frontier < uint64(manifest.FirstHeight) || cp.Frontier > uint64(manifest.FinalHeight) ||
		cp.Height != cp.Frontier || cp.Changed > uint64(manifest.Changed) {
		return verifyReport{}, fmt.Errorf("verify checkpoint is outside the manifest")
	}
	if cp.Frontier == uint64(manifest.FinalHeight) {
		return verifyReport{}, fmt.Errorf("verify checkpoint is already complete; use a new checkpoint directory to revalidate current S3 state")
	}
	if err := validatePlanForWriteContext(ctx, filepath.Dir(manifestPath), manifest); err != nil {
		return verifyReport{}, fmt.Errorf("verify plan preflight: %w", err)
	}
	batcher, err := newCheckpointBatcher(cpPath, cp, options.CheckpointEvery, options.CheckpointInterval)
	if err != nil {
		return verifyReport{}, err
	}
	limiter, err := newByteLimiter(options.MaxInFlightBytes)
	if err != nil {
		return verifyReport{}, err
	}
	rootStream, err := newHeightRootStream(filepath.Join(filepath.Dir(manifestPath), manifest.HeightRootIndex), uint64(manifest.FirstHeight))
	if err != nil {
		return verifyReport{}, err
	}
	defer rootStream.Close()
	packStream := newPackStream(filepath.Dir(manifestPath), manifest)
	defer packStream.Close()

	report := verifyReport{Concurrency: options.Concurrency}
	var semanticChanges atomic.Int64
	var changedFrontier uint64
	checkpointMatched := cp.Frontier == 0
	firstSequence := uint64(manifest.FirstHeight)
	if objects == nil {
		objects, err = newS3ObjectStore(ctx, manifest.Region, options.Concurrency)
		if err != nil {
			return verifyReport{}, err
		}
	}
	pipelineErr := runOrderedPipeline(ctx, firstSequence, options.Concurrency, options.Window,
		func(pipelineCtx context.Context, emit func(uint64, verifyTask) error) error {
			previousRoot := manifest.InitialParentRoot
			var changedSeen uint64
			err := iterateVerifySource(pipelineCtx, func(height int64, canonical []dtypes.AccountStorageDiff) error {
				if height == 1 {
					return nil
				}
				if height < manifest.FirstHeight || height > manifest.FinalHeight {
					return fmt.Errorf("source height %d is outside manifest range %d-%d", height, manifest.FirstHeight, manifest.FinalHeight)
				}
				rootRecord, err := rootStream.Next()
				if err != nil {
					return err
				}
				parent := previousRoot
				previousRoot = rootRecord.Root
				key := fmt.Sprintf("%s/%s/stateDiff", manifest.Prefix, strings.ToLower(rootRecord.Root.Hex()))
				var planned *packRecord
				record, ok, err := packStream.Peek()
				if err != nil {
					return err
				}
				if ok && record.Height < uint64(height) {
					return fmt.Errorf("pack record height %d precedes source height %d", record.Height, height)
				}
				if ok && record.Height == uint64(height) {
					record, _, err = packStream.Next()
					if err != nil {
						return err
					}
					if record.Key != key {
						return fmt.Errorf("pack key %s does not match height %d root", record.Key, height)
					}
					planned = &record
					changedSeen++
				}
				if rootRecord.Root == parent {
					if len(canonical) != 0 || planned != nil {
						return fmt.Errorf("height %d has changes behind an equal-root short circuit", height)
					}
				}
				if uint64(height) <= cp.Frontier {
					if uint64(height) == cp.Frontier {
						if changedSeen != cp.Changed {
							return fmt.Errorf("verify checkpoint height %d contains %d changed records, not %d", height, changedSeen, cp.Changed)
						}
						checkpointMatched = true
					}
				}
				if uint64(height) > cp.Frontier && !checkpointMatched {
					return fmt.Errorf("verify checkpoint height %d was not found", cp.Frontier)
				}
				weight, err := canonicalObjectOperationBytes(canonical, rootRecord.Root == parent)
				if err != nil {
					return fmt.Errorf("height %d canonical storage reservation: %w", height, err)
				}
				if err := limiter.Acquire(pipelineCtx, weight); err != nil {
					return err
				}
				task := verifyTask{
					height: uint64(height), root: rootRecord.Root, parent: parent, key: key,
					canonical: canonical, planned: planned, bytes: weight,
				}
				if err := emit(uint64(height), task); err != nil {
					limiter.Release(weight)
					return err
				}
				return nil
			})
			if err != nil {
				return err
			}
			if !checkpointMatched {
				return fmt.Errorf("verify checkpoint height %d was not found", cp.Frontier)
			}
			if err := rootStream.Finish(uint64(manifest.FinalHeight)); err != nil {
				return err
			}
			if _, ok, err := packStream.Peek(); err != nil {
				return err
			} else if ok {
				return fmt.Errorf("pack has records after final height")
			}
			if int64(changedSeen) != manifest.Changed {
				return fmt.Errorf("pack merge saw %d changed records, want %d", changedSeen, manifest.Changed)
			}
			return nil
		},
		func(workerCtx context.Context, task verifyTask) (verifyOutcome, error) {
			outcome, err := processVerifyTask(workerCtx, manifest.Bucket, task, objects, &semanticChanges)
			if err != nil {
				limiter.Release(task.bytes)
				return verifyOutcome{}, err
			}
			outcome.bytes = task.bytes
			return outcome, nil
		},
		func(_ uint64, outcome verifyOutcome) error {
			defer limiter.Release(outcome.bytes)
			report.Processed++
			if outcome.skipped {
				report.SkippedEqual++
			}
			if outcome.got {
				report.S3GETs++
			}
			if outcome.changed {
				changedFrontier++
				report.VerifiedChanged++
			}
			if outcome.height <= cp.Frontier {
				if outcome.height == cp.Frontier && changedFrontier != cp.Changed {
					return fmt.Errorf("verify checkpoint height %d contains %d changed records, not %d", cp.Frontier, changedFrontier, cp.Changed)
				}
				return nil
			}
			batcher.checkpoint.Changed = changedFrontier
			return batcher.Advance(outcome.height, outcome.height)
		},
	)
	flushErr := batcher.Flush()
	report.SemanticChanges = semanticChanges.Load()
	report.DurationSeconds = time.Since(started).Seconds()
	if report.DurationSeconds > 0 {
		report.ObjectsPerSecond = float64(report.S3GETs) / report.DurationSeconds
	}
	if pipelineErr != nil || flushErr != nil {
		return report, errors.Join(pipelineErr, flushErr)
	}
	if directArchive != nil {
		identity, err := directArchive.identity()
		if err != nil {
			return report, err
		}
		if identity != manifest.ArchiveIdentity {
			return report, fmt.Errorf("direct IAVL archive identity changed during verify")
		}
	}
	if report.VerifiedChanged != manifest.Changed || changedFrontier != uint64(manifest.Changed) {
		return report, fmt.Errorf("verified changed %d, want %d", report.VerifiedChanged, manifest.Changed)
	}
	if report.Processed != manifest.Processed || report.SkippedEqual != manifest.SkippedEqualRoot {
		return report, fmt.Errorf("verified blocks %d, want %d", report.Processed, manifest.Processed)
	}
	return report, nil
}

func processVerifyTask(
	ctx context.Context,
	bucket string,
	task verifyTask,
	objects objectStore,
	semanticChanges *atomic.Int64,
) (verifyOutcome, error) {
	outcome := verifyOutcome{height: task.height, changed: task.planned != nil}
	if task.root == task.parent {
		if len(task.canonical) != 0 || task.planned != nil {
			return verifyOutcome{}, fmt.Errorf("height %d has changes behind an equal-root short circuit", task.height)
		}
		outcome.skipped = true
		return outcome, nil
	}
	object, err := objects.Get(ctx, bucket, task.key)
	if err != nil {
		return verifyOutcome{}, fmt.Errorf("verify GET height %d: %w", task.height, err)
	}
	outcome.got = true
	var diff dtypes.BlockStorageDiff
	if err := rlp.DecodeBytes(object.Body, &diff); err != nil {
		return verifyOutcome{}, fmt.Errorf("verify decode height %d: %w", task.height, err)
	}
	if diff.Hash != task.root || diff.ParentHash != task.parent {
		return verifyOutcome{}, fmt.Errorf("height %d root mismatch", task.height)
	}
	equal, _, _, _, err := compareStorage(diff.StorageDiff, task.canonical)
	if err != nil {
		return verifyOutcome{}, err
	}
	if !equal {
		semanticChanges.Add(1)
		return verifyOutcome{}, fmt.Errorf("height %d storage is not canonical", task.height)
	}
	if task.planned != nil {
		if sha256Hash(object.Body) != task.planned.NewSHA256 {
			return verifyOutcome{}, fmt.Errorf("height %d body SHA-256 mismatch", task.height)
		}
		digest, err := retainedDigest(diff)
		if err != nil {
			return verifyOutcome{}, err
		}
		if digest != task.planned.RetainedSHA256 {
			return verifyOutcome{}, fmt.Errorf("height %d retained fields changed", task.height)
		}
		if !reflect.DeepEqual(object.Headers, task.planned.Headers) {
			return verifyOutcome{}, fmt.Errorf("height %d object metadata changed", task.height)
		}
	}
	return outcome, nil
}
