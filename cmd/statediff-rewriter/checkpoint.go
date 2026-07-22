package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
)

const checkpointSchema = "statediff-rewriter-checkpoint/v1"

type checkpoint struct {
	Schema        string `json:"schema"`
	Checksum      string `json:"checksum"`
	RunID         string `json:"run_id"`
	ManifestHash  string `json:"manifest_sha256"`
	Mode          string `json:"mode"`
	Frontier      uint64 `json:"frontier"`
	Height        uint64 `json:"height"`
	Changed       uint64 `json:"changed_frontier,omitempty"`
	PUTAttempts   uint64 `json:"put_attempts,omitempty"`
	ConfirmedPUTs uint64 `json:"confirmed_puts,omitempty"`
	UncertainPUTs uint64 `json:"uncertain_puts_verified,omitempty"`
	AlreadyTarget uint64 `json:"already_target,omitempty"`
}

func checkpointChecksum(cp checkpoint) (string, error) {
	cp.Checksum = ""
	body, err := json.Marshal(cp)
	if err != nil {
		return "", err
	}
	return sha256Hex(body), nil
}

func loadCheckpoint(path, runID, manifestHash, mode string) (checkpoint, error) {
	body, found, err := readMutableStateFile(path, "checkpoint")
	if !found && err == nil {
		return checkpoint{Schema: checkpointSchema, RunID: runID, ManifestHash: manifestHash, Mode: mode}, nil
	}
	if err != nil {
		return checkpoint{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var cp checkpoint
	if err := decoder.Decode(&cp); err != nil {
		return checkpoint{}, fmt.Errorf("decode checkpoint: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return checkpoint{}, fmt.Errorf("checkpoint has trailing JSON")
	}
	if cp.Schema != checkpointSchema {
		return checkpoint{}, fmt.Errorf("unsupported checkpoint schema %q", cp.Schema)
	}
	wantChecksum, err := checkpointChecksum(cp)
	if err != nil {
		return checkpoint{}, fmt.Errorf("hash checkpoint: %w", err)
	}
	if cp.Checksum == "" || cp.Checksum != wantChecksum {
		return checkpoint{}, fmt.Errorf("checkpoint checksum mismatch")
	}
	if cp.RunID != runID || cp.ManifestHash != manifestHash || cp.Mode != mode {
		return checkpoint{}, fmt.Errorf("checkpoint does not match run/manifest/mode")
	}
	if err := validateCheckpoint(cp); err != nil {
		return checkpoint{}, err
	}
	return cp, nil
}

func saveCheckpoint(path string, cp checkpoint) error {
	if cp.Schema != "" && cp.Schema != checkpointSchema {
		return fmt.Errorf("unsupported checkpoint schema %q", cp.Schema)
	}
	cp.Schema = checkpointSchema
	if err := validateCheckpoint(cp); err != nil {
		return err
	}
	if err := validateMutableStatePath(path, "checkpoint"); err != nil {
		return err
	}
	checksum, err := checkpointChecksum(cp)
	if err != nil {
		return fmt.Errorf("hash checkpoint: %w", err)
	}
	cp.Checksum = checksum
	_, err = atomicJSON(path, cp)
	return err
}

func validateCheckpoint(cp checkpoint) error {
	if cp.Schema != checkpointSchema || cp.RunID == "" || cp.ManifestHash == "" ||
		(cp.Mode != applyMode && cp.Mode != rollbackMode && cp.Mode != verifyMode) {
		return fmt.Errorf("checkpoint schema or identity is invalid")
	}
	if (cp.Frontier == 0) != (cp.Height == 0) || (cp.Frontier == 0 && cp.Changed != 0) {
		return fmt.Errorf("checkpoint frontier and height are inconsistent")
	}
	if cp.Mode == verifyMode {
		if cp.PUTAttempts != 0 || cp.ConfirmedPUTs != 0 || cp.UncertainPUTs != 0 || cp.AlreadyTarget != 0 {
			return fmt.Errorf("verify checkpoint contains write audit counters")
		}
		return nil
	}
	if cp.Changed != 0 || cp.ConfirmedPUTs > cp.PUTAttempts ||
		cp.UncertainPUTs > cp.PUTAttempts-cp.ConfirmedPUTs ||
		cp.ConfirmedPUTs > cp.Frontier || cp.UncertainPUTs > cp.Frontier-cp.ConfirmedPUTs ||
		cp.AlreadyTarget != cp.Frontier-cp.ConfirmedPUTs-cp.UncertainPUTs {
		return fmt.Errorf("checkpoint write audit counters are inconsistent")
	}
	return nil
}

func loadPlanManifest(path string) (planManifest, string, error) {
	return loadPlanManifestContext(context.Background(), path)
}

func loadPlanManifestContext(ctx context.Context, path string) (planManifest, string, error) {
	if ctx == nil {
		return planManifest{}, "", fmt.Errorf("load plan manifest context is required")
	}
	dir, err := requireSealedPlanDirectory(path)
	if err != nil {
		return planManifest{}, "", err
	}
	body, err := readRegularFileNoFollow(path, "sealed plan manifest")
	if err != nil {
		return planManifest{}, "", err
	}
	var manifest planManifest
	if err := decodeStrictJSON(body, &manifest, "plan manifest"); err != nil {
		return planManifest{}, "", err
	}
	if (manifest.Schema != manifestSchema && manifest.Schema != pilotManifestSchema) || !manifest.Sealed {
		return planManifest{}, "", fmt.Errorf("manifest is not sealed schema v1")
	}
	if manifest.RunID == "" || manifest.Bucket != defaultBucket || manifest.Prefix != defaultPrefix || manifest.Region != defaultRegion {
		return planManifest{}, "", fmt.Errorf("manifest target is invalid")
	}
	if !validCreatedAt(manifest.CreatedAt) || manifest.SnapshotID == "" ||
		manifest.ArchiveIdentity.Home == "" || manifest.ArchiveIdentity.DatabaseIdentity == "" ||
		manifest.ArchiveIdentity.LatestVersion < 2 || manifest.ArchiveIdentity.FinalCommitHash == "" {
		return planManifest{}, "", fmt.Errorf("manifest build or snapshot identity is incomplete")
	}
	if err := validateBuildIdentity(
		manifest.CronosCommit, manifest.EthermintCommit, manifest.IAVLCommit, manifest.ImageDigest, manifest.BuildTags,
	); err != nil {
		return planManifest{}, "", fmt.Errorf("manifest build identity: %w", err)
	}
	if !validSHA256Hex(manifest.DumpManifestHash) || !filepath.IsAbs(manifest.DumpPath) ||
		filepath.Ext(filepath.Clean(manifest.DumpPath)) != ".sealed" {
		return planManifest{}, "", fmt.Errorf("manifest dump identity is incomplete")
	}
	if manifest.FirstHeight < 2 || manifest.FinalHeight < manifest.FirstHeight || manifest.FinalHeight > manifest.ArchiveIdentity.LatestVersion {
		return planManifest{}, "", fmt.Errorf("manifest height range is invalid")
	}
	if manifest.Schema == manifestSchema && (manifest.FirstHeight != 2 || manifest.FinalHeight != manifest.ArchiveIdentity.LatestVersion) {
		return planManifest{}, "", fmt.Errorf("full manifest does not cover blocks 2 through archive latest")
	}
	expectedProcessed := manifest.FinalHeight - manifest.FirstHeight + 1
	if manifest.Processed != expectedProcessed || manifest.Processed < 0 || manifest.Changed < 0 ||
		manifest.Unchanged < 0 || manifest.SkippedEqualRoot < 0 || manifest.ChangedCanonical < 0 ||
		manifest.ChangedConflict < 0 || manifest.OldBytes < 0 || manifest.NewBytes < 0 ||
		manifest.Changed > manifest.Processed || manifest.Unchanged > manifest.Processed-manifest.Changed ||
		manifest.SkippedEqualRoot != manifest.Processed-manifest.Changed-manifest.Unchanged ||
		manifest.ChangedCanonical > manifest.Changed || manifest.ChangedConflict > manifest.Changed {
		return planManifest{}, "", fmt.Errorf("manifest block counts are inconsistent")
	}
	if manifest.Processed > math.MaxInt64/rootRecordSize {
		return planManifest{}, "", fmt.Errorf("manifest root index size overflows int64")
	}
	if manifest.HeightRootIndex == "" || manifest.HeightRootIndexSHA256 == "" || manifest.RootIndex == "" ||
		manifest.RootIndexSHA256 == "" || manifest.RootMultisetSHA256 == "" {
		return planManifest{}, "", fmt.Errorf("manifest has incomplete root indexes")
	}
	if !validSHA256Hex(manifest.HeightRootIndexSHA256) || !validSHA256Hex(manifest.RootIndexSHA256) ||
		!validSHA256Hex(manifest.RootMultisetSHA256) {
		return planManifest{}, "", fmt.Errorf("manifest root index hashes are malformed")
	}
	artifacts := make(map[string]string, 2+len(manifest.Chunks)*2)
	addArtifact := func(name, label string) error {
		if previous, found := artifacts[name]; found {
			return fmt.Errorf("manifest artifact %q is reused by %s and %s", name, previous, label)
		}
		artifacts[name] = label
		return nil
	}
	if err := addArtifact(manifest.HeightRootIndex, "height root index"); err != nil {
		return planManifest{}, "", err
	}
	if err := addArtifact(manifest.RootIndex, "sorted root index"); err != nil {
		return planManifest{}, "", err
	}
	heightRootPath, err := requirePlanArtifact(dir, manifest.HeightRootIndex, "height root index")
	if err != nil {
		return planManifest{}, "", err
	}
	heightRootHash, heightRootSize, err := hashFileContext(ctx, heightRootPath)
	if err != nil {
		return planManifest{}, "", err
	}
	if heightRootHash != manifest.HeightRootIndexSHA256 || heightRootSize != manifest.Processed*rootRecordSize {
		return planManifest{}, "", fmt.Errorf("height root index changed or has the wrong size")
	}
	rootPath, err := requirePlanArtifact(dir, manifest.RootIndex, "sorted root index")
	if err != nil {
		return planManifest{}, "", err
	}
	rootHash, rootSize, err := hashFileContext(ctx, rootPath)
	if err != nil {
		return planManifest{}, "", err
	}
	if rootHash != manifest.RootIndexSHA256 || rootSize != manifest.Processed*rootRecordSize {
		return planManifest{}, "", fmt.Errorf("root index changed or has the wrong size")
	}
	heightMultiset, heightCount, err := rootMultisetSHA256Context(ctx, heightRootPath)
	if err != nil {
		return planManifest{}, "", err
	}
	sortedMultiset, sortedCount, err := rootMultisetSHA256Context(ctx, rootPath)
	if err != nil {
		return planManifest{}, "", err
	}
	if heightCount != manifest.Processed || sortedCount != manifest.Processed ||
		heightMultiset != manifest.RootMultisetSHA256 || sortedMultiset != manifest.RootMultisetSHA256 {
		return planManifest{}, "", fmt.Errorf("root indexes do not contain the sealed multiset")
	}
	var chunkRecords int64
	for number, chunk := range manifest.Chunks {
		if err := ctx.Err(); err != nil {
			return planManifest{}, "", err
		}
		if chunk.Number != number || chunk.Pack != fmt.Sprintf("chunk-%06d.pack", number) ||
			chunk.Index != fmt.Sprintf("chunk-%06d.idx", number) || chunk.Records < 1 || chunk.PackSize < 1 ||
			!validSHA256Hex(chunk.PackSHA256) || !validSHA256Hex(chunk.IndexSHA256) {
			return planManifest{}, "", fmt.Errorf("pack chunk %d metadata is invalid", number)
		}
		if chunkRecords > manifest.Changed-chunk.Records {
			return planManifest{}, "", fmt.Errorf("pack chunk records exceed manifest changed count")
		}
		chunkRecords += chunk.Records
		if err := addArtifact(chunk.Pack, fmt.Sprintf("pack chunk %d", number)); err != nil {
			return planManifest{}, "", err
		}
		if err := addArtifact(chunk.Index, fmt.Sprintf("pack index %d", number)); err != nil {
			return planManifest{}, "", err
		}
		packPath, err := requirePlanArtifact(dir, chunk.Pack, "pack chunk")
		if err != nil {
			return planManifest{}, "", err
		}
		packHash, packSize, err := hashFileContext(ctx, packPath)
		if err != nil {
			return planManifest{}, "", err
		}
		if packHash != chunk.PackSHA256 || packSize != chunk.PackSize {
			return planManifest{}, "", fmt.Errorf("pack chunk changed: %s", chunk.Pack)
		}
		indexPath, err := requirePlanArtifact(dir, chunk.Index, "pack index")
		if err != nil {
			return planManifest{}, "", err
		}
		indexHash, _, err := hashFileContext(ctx, indexPath)
		if err != nil {
			return planManifest{}, "", err
		}
		if indexHash != chunk.IndexSHA256 {
			return planManifest{}, "", fmt.Errorf("pack index changed: %s", chunk.Index)
		}
	}
	if chunkRecords != manifest.Changed || (manifest.Changed == 0) != (len(manifest.Chunks) == 0) {
		return planManifest{}, "", fmt.Errorf("pack chunk records differ from manifest changed count")
	}
	artifacts["manifest.v1.json"] = "plan manifest"
	entries, err := os.ReadDir(dir)
	if err != nil {
		return planManifest{}, "", err
	}
	for _, entry := range entries {
		if entry.Name() == "plan.checkpoint.json" {
			checkpoint, found, err := loadPlanCheckpoint(filepath.Join(dir, entry.Name()), manifest)
			if err != nil {
				return planManifest{}, "", err
			}
			if !found || checkpoint.Frontier != uint64(manifest.FinalHeight) ||
				checkpoint.Manifest.RunID != manifest.RunID || checkpoint.Manifest.CreatedAt != manifest.CreatedAt ||
				!samePlanProgress(checkpoint.Manifest, manifest) {
				return planManifest{}, "", fmt.Errorf("sealed plan checkpoint does not describe the final manifest")
			}
			continue
		}
		if _, ok := artifacts[entry.Name()]; !ok {
			return planManifest{}, "", fmt.Errorf("unexpected sealed plan artifact: %s", entry.Name())
		}
	}
	return manifest, sha256Hex(body), nil
}

func samePlanProgress(left, right planManifest) bool {
	return left.Processed == right.Processed && left.Changed == right.Changed &&
		left.ChangedCanonical == right.ChangedCanonical && left.ChangedConflict == right.ChangedConflict &&
		left.Unchanged == right.Unchanged && left.SkippedEqualRoot == right.SkippedEqualRoot &&
		left.SlotsAdded == right.SlotsAdded && left.SlotsRemoved == right.SlotsRemoved &&
		left.SlotsChanged == right.SlotsChanged && left.OldBytes == right.OldBytes && left.NewBytes == right.NewBytes
}

func requireFullPlan(manifest planManifest, operation string) error {
	if manifest.Schema != manifestSchema {
		return fmt.Errorf("pilot plans support plan only; %s is disabled", operation)
	}
	return nil
}
