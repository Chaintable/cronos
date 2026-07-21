package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"github.com/cosmos/iavl"
	"github.com/ethereum/go-ethereum/rlp"

	"github.com/evmos/ethermint/debank/statediff"
	dtypes "github.com/evmos/ethermint/debank/types"
)

type backupProof struct {
	ManifestSHA256 string `json:"manifest_sha256"`
	Kind           string `json:"kind"`
	Location       string `json:"location"`
	Status         string `json:"status"`
}

type writeReport struct {
	PlannedChanged int64 `json:"planned_changed"`
	AppliedChanged int64 `json:"applied_changed"`
	AlreadyTarget  int64 `json:"already_target"`
	PUTs           int64 `json:"puts"`
}

func validateBackupProof(path, manifestPath, manifestHash string) error {
	body, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var proof backupProof
	if err := json.Unmarshal(body, &proof); err != nil {
		return err
	}
	if proof.ManifestSHA256 != manifestHash || proof.Kind == "" || proof.Location == "" || proof.Status != "completed" {
		return fmt.Errorf("backup proof is incomplete or belongs to another manifest")
	}
	sourceDir, err := filepath.EvalSymlinks(filepath.Dir(manifestPath))
	if err != nil {
		return fmt.Errorf("resolve source plan: %w", err)
	}
	backupDir, err := filepath.EvalSymlinks(proof.Location)
	if err != nil {
		return fmt.Errorf("resolve restored backup: %w", err)
	}
	if sourceDir == backupDir {
		return fmt.Errorf("backup proof points to the active plan directory")
	}
	_, backupHash, err := loadPlanManifest(filepath.Join(backupDir, filepath.Base(manifestPath)))
	if err != nil {
		return fmt.Errorf("validate restored backup: %w", err)
	}
	if backupHash != manifestHash {
		return fmt.Errorf("restored backup manifest differs from active manifest")
	}
	return nil
}

func runWriteMode(ctx context.Context, manifestPath, checkpointPath, backupPath, mode string, objects objectStore) (writeReport, error) {
	if mode != "apply" && mode != "rollback" {
		return writeReport{}, fmt.Errorf("unsupported write mode %q", mode)
	}
	manifest, manifestHash, err := loadPlanManifest(manifestPath)
	if err != nil {
		return writeReport{}, err
	}
	if err := requireFullPlan(manifest, mode); err != nil {
		return writeReport{}, err
	}
	if mode == "apply" {
		if err := validateBackupProof(backupPath, manifestPath, manifestHash); err != nil {
			return writeReport{}, fmt.Errorf("apply requires completed independent backup proof: %w", err)
		}
	}
	cp, err := loadCheckpoint(checkpointPath, manifest.RunID, manifestHash, mode)
	if err != nil {
		return writeReport{}, err
	}
	report := writeReport{PlannedChanged: manifest.Changed, AppliedChanged: int64(cp.Frontier)}
	err = iteratePack(filepath.Dir(manifestPath), manifest, func(record packRecord) error {
		if record.Ordinal <= cp.Frontier {
			return nil
		}
		sourceBody, targetBody := record.OldBody, record.NewBody
		sourceSHA, targetSHA := record.OldSHA256, record.NewSHA256
		if mode == "rollback" {
			sourceBody, targetBody = targetBody, sourceBody
			sourceSHA, targetSHA = targetSHA, sourceSHA
		}
		current, err := objects.Get(ctx, manifest.Bucket, record.Key)
		if err != nil {
			return fmt.Errorf("%s get height %d: %w", mode, record.Height, err)
		}
		currentSHA := sha256Hash(current.Body)
		switch currentSHA {
		case targetSHA:
			report.AlreadyTarget++
		case sourceSHA:
			if mode == "apply" && current.ETag != record.OldETag {
				return fmt.Errorf("apply height %d old bytes have changed ETag", record.Height)
			}
			if err := objects.Put(ctx, manifest.Bucket, record.Key, targetBody, current.ETag, record.Headers); err != nil {
				observed, getErr := objects.Get(ctx, manifest.Bucket, record.Key)
				if getErr != nil || sha256Hash(observed.Body) != targetSHA {
					return fmt.Errorf("%s PUT height %d failed and target is not visible: %w", mode, record.Height, err)
				}
			}
			report.PUTs++
		default:
			return fmt.Errorf("%s height %d has third-party content", mode, record.Height)
		}
		verified, err := objects.Get(ctx, manifest.Bucket, record.Key)
		if err != nil {
			return err
		}
		if err := verifyRecordTarget(record, verified.Body, targetSHA); err != nil {
			return fmt.Errorf("%s verify height %d: %w", mode, record.Height, err)
		}
		if !reflect.DeepEqual(verified.Headers, record.Headers) {
			return fmt.Errorf("%s verify height %d: object metadata changed", mode, record.Height)
		}
		report.AppliedChanged++
		cp.Frontier, cp.Height = record.Ordinal, record.Height
		return saveCheckpoint(checkpointPath, cp)
	})
	return report, err
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
	Processed       int64 `json:"processed"`
	VerifiedChanged int64 `json:"verified_changed"`
	SkippedEqual    int64 `json:"skipped_equal_root"`
	SemanticChanges int64 `json:"semantic_changes_remaining"`
}

func runVerify(ctx context.Context, manifestPath, checkpointDir string, objects objectStore) (verifyReport, error) {
	manifest, manifestHash, err := loadPlanManifest(manifestPath)
	if err != nil {
		return verifyReport{}, err
	}
	if err := requireFullPlan(manifest, "verify"); err != nil {
		return verifyReport{}, err
	}
	report := verifyReport{}
	changedCheckpoint := filepath.Join(checkpointDir, "verify-changed.json")
	changedCP, err := loadCheckpoint(changedCheckpoint, manifest.RunID, manifestHash, "verify-changed")
	if err != nil {
		return report, err
	}
	if changedCP.Frontier > uint64(manifest.Changed) || (changedCP.Height != 0 && (changedCP.Height < uint64(manifest.FirstHeight) || changedCP.Height > uint64(manifest.FinalHeight))) {
		return report, fmt.Errorf("changed checkpoint is outside manifest range")
	}
	report.VerifiedChanged = int64(changedCP.Frontier)
	if err := iteratePack(filepath.Dir(manifestPath), manifest, func(record packRecord) error {
		if record.Ordinal <= changedCP.Frontier {
			return nil
		}
		object, err := objects.Get(ctx, manifest.Bucket, record.Key)
		if err != nil {
			return err
		}
		if err := verifyRecordTarget(record, object.Body, record.NewSHA256); err != nil {
			return fmt.Errorf("verify planned height %d: %w", record.Height, err)
		}
		report.VerifiedChanged++
		changedCP.Frontier, changedCP.Height = record.Ordinal, record.Height
		return saveCheckpoint(changedCheckpoint, changedCP)
	}); err != nil {
		return report, err
	}

	dumpBody, err := os.ReadFile(filepath.Join(manifest.DumpPath, "dump-manifest.v1.json"))
	if err != nil {
		return report, err
	}
	if sha256Hex(dumpBody) != manifest.DumpManifestHash {
		return report, fmt.Errorf("dump manifest hash mismatch")
	}
	var dumpInfo dumpManifest
	if err := json.Unmarshal(dumpBody, &dumpInfo); err != nil {
		return report, err
	}
	dumpSchema, dumpFirst := dumpManifestSchema, int64(1)
	if manifest.Schema == pilotManifestSchema {
		dumpSchema, dumpFirst = pilotDumpManifestSchema, manifest.FirstHeight
	}
	if err := validateDumpContext(dumpInfo, dumpContext{
		Schema: dumpSchema, FirstVersion: dumpFirst, LastVersion: manifest.FinalHeight,
		SnapshotID: manifest.SnapshotID, ArchiveIdentity: manifest.ArchiveIdentity,
		CronosCommit: manifest.CronosCommit, EthermintCommit: manifest.EthermintCommit,
		ImageDigest: manifest.ImageDigest, BuildTags: manifest.BuildTags,
	}); err != nil {
		return report, err
	}
	archive, err := openArchive(manifest.ArchiveIdentity.Home)
	if err != nil {
		return report, err
	}
	defer archive.Close()
	identity, err := archive.identity()
	if err != nil {
		return report, err
	}
	if identity != manifest.ArchiveIdentity {
		return report, fmt.Errorf("archive identity differs from sealed manifest")
	}
	blockCheckpoint := filepath.Join(checkpointDir, "verify-blocks.json")
	blockCP, err := loadCheckpoint(blockCheckpoint, manifest.RunID, manifestHash, "verify-blocks")
	if err != nil {
		return report, err
	}
	if blockCP.Frontier != 0 && (blockCP.Frontier < uint64(manifest.FirstHeight) || blockCP.Frontier > uint64(manifest.FinalHeight)) {
		return report, fmt.Errorf("block checkpoint is outside manifest range")
	}
	if blockCP.Frontier >= uint64(manifest.FirstHeight) {
		report.Processed = int64(blockCP.Frontier) - manifest.FirstHeight + 1
	}
	err = iterateSealedDump(manifest.DumpPath, dumpInfo, func(height int64, changeSet *iavl.ChangeSet) error {
		if height == 1 && manifest.FirstHeight == 2 {
			return nil
		}
		if height < manifest.FirstHeight || height > manifest.FinalHeight {
			return fmt.Errorf("dump height %d is outside manifest range %d-%d", height, manifest.FirstHeight, manifest.FinalHeight)
		}
		if uint64(height) <= blockCP.Frontier {
			return nil
		}
		canonical, err := statediff.CanonicalStorageDiff(changeSet)
		if err != nil {
			return err
		}
		root, parent, err := archiveRoots(archive, height)
		if err != nil {
			return err
		}
		if root == parent {
			if len(canonical) != 0 {
				return fmt.Errorf("height %d remains undeliverable behind equal-root short circuit", height)
			}
			report.SkippedEqual++
		} else {
			key := fmt.Sprintf("%s/%s/stateDiff", manifest.Prefix, root.Hex())
			object, err := objects.Get(ctx, manifest.Bucket, key)
			if err != nil {
				return err
			}
			var diff dtypes.BlockStorageDiff
			if err := rlp.DecodeBytes(object.Body, &diff); err != nil {
				return err
			}
			if diff.Hash != root || diff.ParentHash != parent {
				return fmt.Errorf("height %d root mismatch", height)
			}
			equal, _, _, _, err := compareStorage(diff.StorageDiff, canonical)
			if err != nil {
				return err
			}
			if !equal {
				report.SemanticChanges++
				return fmt.Errorf("height %d storage is not canonical", height)
			}
		}
		report.Processed++
		blockCP.Frontier, blockCP.Height = uint64(height), uint64(height)
		return saveCheckpoint(blockCheckpoint, blockCP)
	})
	if err != nil {
		return report, err
	}
	if report.VerifiedChanged != manifest.Changed {
		return report, fmt.Errorf("verified changed %d, want %d", report.VerifiedChanged, manifest.Changed)
	}
	if report.Processed != manifest.Processed {
		return report, fmt.Errorf("verified blocks %d, want %d", report.Processed, manifest.Processed)
	}
	return report, nil
}
