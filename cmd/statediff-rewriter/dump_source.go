package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	dumpSourceSchema   = "cronos-changeset-dump-source/v1"
	dumpSourceFileName = "dump-source.v1.json"
)

type dumpSourceManifest struct {
	Schema    string      `json:"schema"`
	Checksum  string      `json:"checksum"`
	CreatedAt string      `json:"created_at"`
	Context   dumpContext `json:"context"`
}

func dumpSourceChecksum(source dumpSourceManifest) (string, error) {
	source.Checksum = ""
	body, err := json.Marshal(source)
	if err != nil {
		return "", err
	}
	return sha256Hex(body), nil
}

func validateDumpSourceContext(context dumpContext) error {
	if context.Schema != dumpManifestSchema && context.Schema != pilotDumpManifestSchema {
		return fmt.Errorf("unsupported dump source schema %q", context.Schema)
	}
	if context.LegacyValidation != "" && context.LegacyValidation != legacyValidationTrustedSet {
		return fmt.Errorf("unsupported legacy validation mode %q", context.LegacyValidation)
	}
	if context.FirstVersion < 1 || context.LastVersion < context.FirstVersion {
		return fmt.Errorf("invalid dump source range %d-%d", context.FirstVersion, context.LastVersion)
	}
	if context.SnapshotID == "" || context.ArchiveIdentity.Home == "" ||
		context.ArchiveIdentity.DatabaseIdentity == "" ||
		context.ArchiveIdentity.LatestVersion < context.LastVersion || context.ArchiveIdentity.FinalCommitHash == "" {
		return fmt.Errorf("dump source identity is incomplete")
	}
	if err := validateBuildIdentity(
		context.CronosCommit, context.EthermintCommit, context.IAVLCommit, context.ImageDigest, context.BuildTags,
	); err != nil {
		return err
	}
	if context.Schema == dumpManifestSchema &&
		(context.FirstVersion != 1 || context.LastVersion != context.ArchiveIdentity.LatestVersion) {
		return fmt.Errorf("full dump source does not cover 1 through archive latest")
	}
	return nil
}

func ensureDumpSource(output string, expected dumpContext) (string, error) {
	if err := validateDumpSourceContext(expected); err != nil {
		return "", err
	}
	path := filepath.Join(output, dumpSourceFileName)
	source, hash, found, err := loadDumpSource(path)
	if err != nil {
		return "", err
	}
	if found {
		if source.Context != expected {
			return "", fmt.Errorf("dump source identity differs from this extraction")
		}
		return hash, nil
	}
	entries, err := os.ReadDir(output)
	if err != nil {
		return "", err
	}
	for _, entry := range entries {
		if entry.Name() != "evm" {
			return "", fmt.Errorf("dump staging has %s but no source manifest", entry.Name())
		}
		evmEntries, err := os.ReadDir(filepath.Join(output, "evm"))
		if err != nil {
			return "", err
		}
		if len(evmEntries) != 0 {
			return "", fmt.Errorf("dump staging contains EVM output but no source manifest")
		}
	}
	source = dumpSourceManifest{
		Schema: dumpSourceSchema, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano), Context: expected,
	}
	checksum, err := dumpSourceChecksum(source)
	if err != nil {
		return "", err
	}
	source.Checksum = checksum
	body, err := atomicJSONNoReplace(path, source)
	if err != nil {
		return "", err
	}
	return sha256Hex(body), nil
}

func loadDumpSource(path string) (dumpSourceManifest, string, bool, error) {
	body, err := readRegularFileNoFollow(path, "dump source manifest")
	if errors.Is(err, os.ErrNotExist) {
		return dumpSourceManifest{}, "", false, nil
	}
	if err != nil {
		return dumpSourceManifest{}, "", false, err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var source dumpSourceManifest
	if err := decoder.Decode(&source); err != nil {
		return dumpSourceManifest{}, "", false, fmt.Errorf("decode dump source: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return dumpSourceManifest{}, "", false, fmt.Errorf("dump source has trailing JSON")
	}
	if source.Schema != dumpSourceSchema || !validCreatedAt(source.CreatedAt) {
		return dumpSourceManifest{}, "", false, fmt.Errorf("unsupported or incomplete dump source manifest")
	}
	checksum, err := dumpSourceChecksum(source)
	if err != nil {
		return dumpSourceManifest{}, "", false, err
	}
	if source.Checksum == "" || source.Checksum != checksum {
		return dumpSourceManifest{}, "", false, fmt.Errorf("dump source checksum mismatch")
	}
	if err := validateDumpSourceContext(source.Context); err != nil {
		return dumpSourceManifest{}, "", false, err
	}
	return source, sha256Hex(body), true, nil
}
