package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func validTestDumpContext() dumpContext {
	return dumpContext{
		Schema: pilotDumpManifestSchema, FirstVersion: 100, LastVersion: 109,
		SnapshotID: "snap-test",
		ArchiveIdentity: archiveIdentity{
			Home: "/archive", DatabaseIdentity: "db-test", LatestVersion: 200, FinalCommitHash: "0x200",
		},
		CronosCommit: testCronosCommit, EthermintCommit: testEthermintCommit, IAVLCommit: testIAVLCommit,
		ImageDigest: testImageDigest, BuildTags: testBuildTags,
	}
}

func TestEnsureDumpSourcePinsIdentityBeforeOutput(t *testing.T) {
	staging := filepath.Join(t.TempDir(), "dump.staging")
	require.NoError(t, os.MkdirAll(filepath.Join(staging, "evm"), 0o755))
	context := validTestDumpContext()
	hash, err := ensureDumpSource(staging, context)
	require.NoError(t, err)
	require.Len(t, hash, 64)

	reusedHash, err := ensureDumpSource(staging, context)
	require.NoError(t, err)
	require.Equal(t, hash, reusedHash)

	context.SnapshotID = "snap-other"
	_, err = ensureDumpSource(staging, context)
	require.ErrorContains(t, err, "identity differs")

	context = validTestDumpContext()
	context.LegacyValidation = legacyValidationTrustedSet
	_, err = ensureDumpSource(staging, context)
	require.ErrorContains(t, err, "identity differs")
}

func TestValidateDumpSourceContextRejectsUnknownLegacyValidation(t *testing.T) {
	context := validTestDumpContext()
	context.LegacyValidation = "unchecked"
	require.ErrorContains(t, validateDumpSourceContext(context), "unsupported legacy validation mode")
}

func TestEnsureDumpSourceRefusesToRelabelExistingOutput(t *testing.T) {
	staging := filepath.Join(t.TempDir(), "dump.staging")
	writeDumpFile(t, staging, "block-100.zz", []byte("existing"))
	_, err := ensureDumpSource(staging, validTestDumpContext())
	require.ErrorContains(t, err, "contains EVM output but no source manifest")
}

func TestLoadDumpSourceRejectsMutationUnknownAndTrailingJSON(t *testing.T) {
	staging := filepath.Join(t.TempDir(), "dump.staging")
	require.NoError(t, os.MkdirAll(filepath.Join(staging, "evm"), 0o755))
	_, err := ensureDumpSource(staging, validTestDumpContext())
	require.NoError(t, err)
	path := filepath.Join(staging, dumpSourceFileName)
	body, err := os.ReadFile(path)
	require.NoError(t, err)

	mutated := append([]byte(nil), body...)
	mutated[len(mutated)-3] ^= 1
	require.NoError(t, os.WriteFile(path, mutated, 0o600))
	_, _, _, err = loadDumpSource(path)
	require.Error(t, err)

	writeTestDumpSource(t, staging, validTestDumpContext())
	body, err = os.ReadFile(path)
	require.NoError(t, err)
	unknown := append(append([]byte(nil), body[:len(body)-2]...), []byte(",\n  \"unknown\": true\n}\n")...)
	require.NoError(t, os.WriteFile(path, unknown, 0o600))
	_, _, _, err = loadDumpSource(path)
	require.ErrorContains(t, err, "unknown field")

	writeTestDumpSource(t, staging, validTestDumpContext())
	body, err = os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(body, []byte("{}")...), 0o600))
	_, _, _, err = loadDumpSource(path)
	require.ErrorContains(t, err, "trailing JSON")

	writeTestDumpSource(t, staging, validTestDumpContext())
	source, _, found, err := loadDumpSource(path)
	require.NoError(t, err)
	require.True(t, found)
	source.CreatedAt = "not-a-time"
	source.Checksum, err = dumpSourceChecksum(source)
	require.NoError(t, err)
	_, err = atomicJSON(path, source)
	require.NoError(t, err)
	_, _, _, err = loadDumpSource(path)
	require.ErrorContains(t, err, "incomplete dump source manifest")
}
