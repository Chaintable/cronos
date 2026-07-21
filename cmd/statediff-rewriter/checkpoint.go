package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type checkpoint struct {
	RunID        string `json:"run_id"`
	ManifestHash string `json:"manifest_sha256"`
	Mode         string `json:"mode"`
	Frontier     uint64 `json:"frontier"`
	Height       uint64 `json:"height"`
}

func loadCheckpoint(path, runID, manifestHash, mode string) (checkpoint, error) {
	body, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return checkpoint{RunID: runID, ManifestHash: manifestHash, Mode: mode}, nil
	}
	if err != nil {
		return checkpoint{}, err
	}
	var cp checkpoint
	if err := json.Unmarshal(body, &cp); err != nil {
		return checkpoint{}, fmt.Errorf("decode checkpoint: %w", err)
	}
	if cp.RunID != runID || cp.ManifestHash != manifestHash || cp.Mode != mode {
		return checkpoint{}, fmt.Errorf("checkpoint does not match run/manifest/mode")
	}
	return cp, nil
}

func saveCheckpoint(path string, cp checkpoint) error {
	_, err := atomicJSON(path, cp)
	return err
}

func loadPlanManifest(path string) (planManifest, string, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return planManifest{}, "", err
	}
	var manifest planManifest
	if err := json.Unmarshal(body, &manifest); err != nil {
		return planManifest{}, "", err
	}
	if manifest.Schema != manifestSchema || !manifest.Sealed {
		return planManifest{}, "", fmt.Errorf("manifest is not sealed schema v1")
	}
	if !strings.HasSuffix(filepath.Clean(filepath.Dir(path)), ".sealed") {
		return planManifest{}, "", fmt.Errorf("manifest is not inside a sealed plan directory")
	}
	if manifest.RunID == "" || manifest.Bucket != defaultBucket || manifest.Prefix != defaultPrefix || manifest.Region != defaultRegion {
		return planManifest{}, "", fmt.Errorf("manifest target is invalid")
	}
	if manifest.SnapshotID == "" || manifest.ImageDigest == "" || manifest.CronosCommit == "" || manifest.EthermintCommit == "" {
		return planManifest{}, "", fmt.Errorf("manifest build or snapshot identity is incomplete")
	}
	if manifest.RootIndex == "" || manifest.RootIndexSHA256 == "" {
		return planManifest{}, "", fmt.Errorf("manifest has no root index")
	}
	rootHash, _, err := hashFile(filepath.Join(filepath.Dir(path), manifest.RootIndex))
	if err != nil {
		return planManifest{}, "", err
	}
	if rootHash != manifest.RootIndexSHA256 {
		return planManifest{}, "", fmt.Errorf("root index changed")
	}
	for _, chunk := range manifest.Chunks {
		packHash, packSize, err := hashFile(filepath.Join(filepath.Dir(path), chunk.Pack))
		if err != nil {
			return planManifest{}, "", err
		}
		if packHash != chunk.PackSHA256 || packSize != chunk.PackSize {
			return planManifest{}, "", fmt.Errorf("pack chunk changed: %s", chunk.Pack)
		}
		indexHash, _, err := hashFile(filepath.Join(filepath.Dir(path), chunk.Index))
		if err != nil {
			return planManifest{}, "", err
		}
		if indexHash != chunk.IndexSHA256 {
			return planManifest{}, "", fmt.Errorf("pack index changed: %s", chunk.Index)
		}
	}
	return manifest, sha256Hex(body), nil
}
