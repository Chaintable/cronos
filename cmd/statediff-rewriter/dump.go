package main

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/cosmos/iavl"
	"github.com/evmos/ethermint/debank/statediff"
)

const (
	dumpManifestSchema      = "cronos-changeset-dump/v1"
	pilotDumpManifestSchema = "cronos-changeset-pilot-dump/v1"
)

const maxChangeSetPayload = uint64(256 << 20)

type countingWriter struct{ n int64 }

type contextReader struct {
	ctx    context.Context
	reader *bufio.Reader
}

type dumpContext struct {
	Schema           string          `json:"dump_schema"`
	FirstVersion     int64           `json:"first_version"`
	LastVersion      int64           `json:"last_version"`
	SnapshotID       string          `json:"snapshot_id"`
	ArchiveIdentity  archiveIdentity `json:"archive_identity"`
	CronosCommit     string          `json:"cronos_commit"`
	EthermintCommit  string          `json:"ethermint_commit"`
	IAVLCommit       string          `json:"iavl_commit"`
	ImageDigest      string          `json:"image_digest"`
	BuildTags        string          `json:"build_tags"`
	LegacyValidation string          `json:"legacy_validation,omitempty"`
}

func (w *countingWriter) Write(body []byte) (int, error) {
	w.n += int64(len(body))
	return len(body), nil
}

func (reader contextReader) Read(body []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.reader.Read(body)
}

func (reader contextReader) ReadByte() (byte, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.reader.ReadByte()
}

func scanZlibChangeSets(path string, fn func(int64, *iavl.ChangeSet) error) (dumpFileManifest, error) {
	return scanZlibChangeSetsContext(context.Background(), path, fn)
}

func scanZlibChangeSetsContext(ctx context.Context, path string, fn func(int64, *iavl.ChangeSet) error) (dumpFileManifest, error) {
	if ctx == nil {
		return dumpFileManifest{}, fmt.Errorf("scan changesets context is required")
	}
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return dumpFileManifest{}, err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return dumpFileManifest{}, err
	}
	if !stat.Mode().IsRegular() {
		return dumpFileManifest{}, fmt.Errorf("changeset dump must be a regular non-symlink file: %s", path)
	}

	hasher := sha256.New()
	counter := &countingWriter{}
	compressed := bufio.NewReader(io.TeeReader(file, io.MultiWriter(hasher, counter)))
	zreader, err := zlib.NewReader(contextReader{ctx: ctx, reader: compressed})
	if err != nil {
		return dumpFileManifest{}, fmt.Errorf("open zlib %s: %w", path, err)
	}
	reader := bufio.NewReader(zreader)

	var first, last, records int64
	for {
		if err := ctx.Err(); err != nil {
			_ = zreader.Close()
			return dumpFileManifest{}, err
		}
		_, peekErr := reader.Peek(1)
		if errors.Is(peekErr, io.EOF) {
			break
		}
		if peekErr != nil {
			_ = zreader.Close()
			return dumpFileManifest{}, fmt.Errorf("read zlib %s: %w", path, peekErr)
		}
		version, changeSet, readErr := readChangeSet(reader)
		if readErr != nil {
			_ = zreader.Close()
			return dumpFileManifest{}, fmt.Errorf("read changeset %s record %d: %w", path, records+1, readErr)
		}
		if records == 0 {
			first = version
		} else if version != last+1 {
			_ = zreader.Close()
			return dumpFileManifest{}, fmt.Errorf("non-contiguous changeset in %s: got %d after %d", path, version, last)
		}
		last = version
		records++
		if fn != nil {
			if err := fn(version, changeSet); err != nil {
				_ = zreader.Close()
				return dumpFileManifest{}, err
			}
		}
	}
	if err := zreader.Close(); err != nil {
		return dumpFileManifest{}, fmt.Errorf("close zlib %s: %w", path, err)
	}
	if _, err := compressed.Peek(1); !errors.Is(err, io.EOF) {
		if err == nil {
			return dumpFileManifest{}, fmt.Errorf("zlib file %s has trailing data", path)
		}
		return dumpFileManifest{}, fmt.Errorf("read zlib trailer %s: %w", path, err)
	}
	if counter.n != stat.Size() {
		return dumpFileManifest{}, fmt.Errorf("zlib file %s was not fully consumed: %d of %d bytes", path, counter.n, stat.Size())
	}
	if records == 0 {
		return dumpFileManifest{}, fmt.Errorf("empty changeset file %s", path)
	}
	return dumpFileManifest{
		Path: filepath.Base(path), SHA256: hex.EncodeToString(hasher.Sum(nil)), Size: stat.Size(),
		FirstVersion: first, LastVersion: last, Records: records,
	}, nil
}

func readChangeSet(reader *bufio.Reader) (int64, *iavl.ChangeSet, error) {
	var header [16]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return 0, nil, err
	}
	version := int64(binary.LittleEndian.Uint64(header[:8]))
	size := binary.LittleEndian.Uint64(header[8:])
	if size > maxChangeSetPayload {
		return 0, nil, fmt.Errorf("changeset payload %d exceeds limit %d", size, maxChangeSetPayload)
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return 0, nil, err
	}
	payloadReader := bytes.NewReader(payload)
	changeSet := &iavl.ChangeSet{}
	for payloadReader.Len() > 0 {
		deletion, err := payloadReader.ReadByte()
		if err != nil {
			return 0, nil, err
		}
		if deletion > 1 {
			return 0, nil, fmt.Errorf("invalid deletion marker %d", deletion)
		}
		keyLength, err := binary.ReadUvarint(payloadReader)
		if err != nil {
			return 0, nil, err
		}
		if keyLength > uint64(payloadReader.Len()) {
			return 0, nil, io.ErrUnexpectedEOF
		}
		pair := &iavl.KVPair{Delete: deletion == 1, Key: make([]byte, keyLength)}
		if _, err := io.ReadFull(payloadReader, pair.Key); err != nil {
			return 0, nil, err
		}
		if !pair.Delete {
			valueLength, err := binary.ReadUvarint(payloadReader)
			if err != nil {
				return 0, nil, err
			}
			if valueLength > uint64(payloadReader.Len()) {
				return 0, nil, io.ErrUnexpectedEOF
			}
			pair.Value = make([]byte, valueLength)
			if _, err := io.ReadFull(payloadReader, pair.Value); err != nil {
				return 0, nil, err
			}
		}
		changeSet.Pairs = append(changeSet.Pairs, pair)
	}
	return version, changeSet, nil
}

func writeChangeSet(writer io.Writer, version int64, changeSet *iavl.ChangeSet) error {
	var payloadSize uint64
	for _, pair := range changeSet.Pairs {
		if pair == nil {
			return fmt.Errorf("nil changeset pair")
		}
		pairSize := uint64(1 + uvarintLength(uint64(len(pair.Key))) + len(pair.Key))
		if !pair.Delete {
			pairSize += uint64(uvarintLength(uint64(len(pair.Value))) + len(pair.Value))
		}
		if pairSize > maxChangeSetPayload-payloadSize {
			return fmt.Errorf("changeset payload exceeds limit %d", maxChangeSetPayload)
		}
		payloadSize += pairSize
	}

	var header [16]byte
	binary.LittleEndian.PutUint64(header[:8], uint64(version))
	binary.LittleEndian.PutUint64(header[8:], payloadSize)
	if err := writeBytes(writer, header[:]); err != nil {
		return err
	}
	var scratch [binary.MaxVarintLen64]byte
	for _, pair := range changeSet.Pairs {
		deletion := byte(0)
		if pair.Delete {
			deletion = 1
		}
		if err := writeBytes(writer, []byte{deletion}); err != nil {
			return err
		}
		n := binary.PutUvarint(scratch[:], uint64(len(pair.Key)))
		if err := writeBytes(writer, scratch[:n]); err != nil {
			return err
		}
		if err := writeBytes(writer, pair.Key); err != nil {
			return err
		}
		if pair.Delete {
			continue
		}
		n = binary.PutUvarint(scratch[:], uint64(len(pair.Value)))
		if err := writeBytes(writer, scratch[:n]); err != nil {
			return err
		}
		if err := writeBytes(writer, pair.Value); err != nil {
			return err
		}
	}
	return nil
}

func uvarintLength(value uint64) int {
	var scratch [binary.MaxVarintLen64]byte
	return binary.PutUvarint(scratch[:], value)
}

func writeBytes(writer io.Writer, body []byte) error {
	for len(body) > 0 {
		written, err := writer.Write(body)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		body = body[written:]
	}
	return nil
}

func sealDump(stagingPath string, contexts ...dumpContext) (string, dumpManifest, string, error) {
	return sealDumpContext(context.Background(), stagingPath, contexts...)
}

func sealDumpContext(ctx context.Context, stagingPath string, contexts ...dumpContext) (string, dumpManifest, string, error) {
	if ctx == nil {
		return "", dumpManifest{}, "", fmt.Errorf("seal dump context is required")
	}
	var err error
	stagingPath, err = requireDumpDirectory(stagingPath, ".staging", "dump staging")
	if err != nil {
		return "", dumpManifest{}, "", err
	}
	files, err := filepath.Glob(filepath.Join(stagingPath, "evm", "*.zz"))
	if err != nil {
		return "", dumpManifest{}, "", err
	}
	if len(files) == 0 {
		return "", dumpManifest{}, "", fmt.Errorf("no evm/*.zz files in %s", stagingPath)
	}

	manifest := dumpManifest{Schema: dumpManifestSchema, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if len(contexts) > 1 {
		return "", dumpManifest{}, "", fmt.Errorf("multiple dump contexts")
	}
	if len(contexts) == 1 {
		context := contexts[0]
		sourcePath := filepath.Join(stagingPath, dumpSourceFileName)
		if err := requireRegularFile(sourcePath, "dump source manifest"); err != nil {
			return "", dumpManifest{}, "", err
		}
		source, sourceHash, found, err := loadDumpSource(sourcePath)
		if err != nil {
			return "", dumpManifest{}, "", err
		}
		if !found {
			return "", dumpManifest{}, "", fmt.Errorf("execution dump staging has no source manifest")
		}
		if context.LegacyValidation == "" && source.Context.LegacyValidation == legacyValidationTrustedSet {
			context.LegacyValidation = legacyValidationTrustedSet
		}
		if source.Context != context {
			return "", dumpManifest{}, "", fmt.Errorf("dump source identity differs from plan")
		}
		manifest.Schema = context.Schema
		manifest.SnapshotID, manifest.ArchiveIdentity = context.SnapshotID, context.ArchiveIdentity
		manifest.CronosCommit, manifest.EthermintCommit = context.CronosCommit, context.EthermintCommit
		manifest.IAVLCommit = context.IAVLCommit
		manifest.ImageDigest, manifest.BuildTags = context.ImageDigest, context.BuildTags
		manifest.SourceManifestSHA256 = sourceHash
	}
	for _, path := range files {
		if err := requireRegularFile(path, "dump staging file"); err != nil {
			return "", dumpManifest{}, "", err
		}
		entry, err := scanZlibChangeSetsContext(ctx, path, func(version int64, changeSet *iavl.ChangeSet) error {
			if version != 1 {
				return nil
			}
			storage, err := statediff.CanonicalStorageDiff(changeSet)
			if err != nil {
				return fmt.Errorf("validate genesis changeset: %w", err)
			}
			if len(storage) != 0 {
				return fmt.Errorf("genesis changeset contains EVM storage")
			}
			return nil
		})
		if err != nil {
			return "", dumpManifest{}, "", err
		}
		entry.Path = filepath.Join("evm", entry.Path)
		manifest.Files = append(manifest.Files, entry)
	}
	sort.Slice(manifest.Files, func(i, j int) bool {
		return manifest.Files[i].FirstVersion < manifest.Files[j].FirstVersion
	})
	manifest.FirstVersion = manifest.Files[0].FirstVersion
	manifest.LastVersion = manifest.Files[len(manifest.Files)-1].LastVersion
	expectedFirst, expectedLast := int64(1), manifest.LastVersion
	if len(contexts) == 1 {
		expectedFirst, expectedLast = contexts[0].FirstVersion, contexts[0].LastVersion
	}
	if manifest.FirstVersion != expectedFirst {
		return "", dumpManifest{}, "", fmt.Errorf("dump starts at version %d, want %d", manifest.FirstVersion, expectedFirst)
	}
	if manifest.LastVersion != expectedLast {
		return "", dumpManifest{}, "", fmt.Errorf("dump ends at version %d, want %d", manifest.LastVersion, expectedLast)
	}
	for i, file := range manifest.Files {
		manifest.Records += file.Records
		if i > 0 && file.FirstVersion != manifest.Files[i-1].LastVersion+1 {
			return "", dumpManifest{}, "", fmt.Errorf("dump gap or overlap between %s and %s", manifest.Files[i-1].Path, file.Path)
		}
	}
	expectedRecords := manifest.LastVersion - manifest.FirstVersion + 1
	if manifest.Records != expectedRecords {
		return "", dumpManifest{}, "", fmt.Errorf("dump has %d records for versions %d through %d", manifest.Records, manifest.FirstVersion, manifest.LastVersion)
	}
	if len(contexts) == 1 {
		if err := validateDumpContext(manifest, contexts[0]); err != nil {
			return "", dumpManifest{}, "", err
		}
	}
	if err := ctx.Err(); err != nil {
		return "", dumpManifest{}, "", err
	}

	body, err := atomicJSON(filepath.Join(stagingPath, "dump-manifest.v1.json"), manifest)
	if err != nil {
		return "", dumpManifest{}, "", err
	}
	if err := validateDumpArtifactSet(stagingPath, manifest); err != nil {
		return "", dumpManifest{}, "", err
	}
	sealedPath := strings.TrimSuffix(filepath.Clean(stagingPath), ".staging") + ".sealed"
	if _, err := os.Lstat(sealedPath); err == nil {
		return "", dumpManifest{}, "", fmt.Errorf("sealed dump already exists: %s", sealedPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", dumpManifest{}, "", err
	}
	if err := syncDir(stagingPath); err != nil {
		return "", dumpManifest{}, "", err
	}
	if err := ctx.Err(); err != nil {
		return "", dumpManifest{}, "", err
	}
	if err := renameNoReplace(stagingPath, sealedPath); err != nil {
		return "", dumpManifest{}, "", err
	}
	if err := syncDir(filepath.Dir(sealedPath)); err != nil {
		return "", dumpManifest{}, "", err
	}
	return sealedPath, manifest, sha256Hex(body), nil
}

func prepareDump(path string, expected dumpContext) (string, dumpManifest, string, error) {
	return prepareDumpContext(context.Background(), path, expected)
}

func prepareDumpContext(ctx context.Context, path string, expected dumpContext) (string, dumpManifest, string, error) {
	if ctx == nil {
		return "", dumpManifest{}, "", fmt.Errorf("prepare dump context is required")
	}
	cleanPath := filepath.Clean(path)
	if strings.HasSuffix(cleanPath, ".staging") {
		sealedPath := strings.TrimSuffix(cleanPath, ".staging") + ".sealed"
		_, stagingErr := os.Lstat(cleanPath)
		_, sealedErr := os.Lstat(sealedPath)
		if stagingErr != nil && !errors.Is(stagingErr, os.ErrNotExist) {
			return "", dumpManifest{}, "", fmt.Errorf("stat staging dump %s: %w", cleanPath, stagingErr)
		}
		if sealedErr != nil && !errors.Is(sealedErr, os.ErrNotExist) {
			return "", dumpManifest{}, "", fmt.Errorf("stat sealed dump %s: %w", sealedPath, sealedErr)
		}
		switch {
		case stagingErr == nil && sealedErr == nil:
			return "", dumpManifest{}, "", fmt.Errorf("both staging and sealed dumps exist: %s and %s", cleanPath, sealedPath)
		case stagingErr == nil:
			return sealDumpContext(ctx, cleanPath, expected)
		case sealedErr == nil:
			cleanPath = sealedPath
		default:
			return "", dumpManifest{}, "", fmt.Errorf("dump does not exist: %s or %s", cleanPath, sealedPath)
		}
	}
	if !strings.HasSuffix(cleanPath, ".sealed") {
		return "", dumpManifest{}, "", fmt.Errorf("dump path must end in .staging or .sealed")
	}
	cleanPath, err := requireSealedDumpDirectory(cleanPath)
	if err != nil {
		return "", dumpManifest{}, "", err
	}
	body, err := readRegularFileNoFollow(filepath.Join(cleanPath, "dump-manifest.v1.json"), "sealed dump manifest")
	if err != nil {
		return "", dumpManifest{}, "", err
	}
	var manifest dumpManifest
	if err := decodeStrictJSON(body, &manifest, "dump manifest"); err != nil {
		return "", dumpManifest{}, "", err
	}
	if err := validateDumpContext(manifest, expected); err != nil {
		return "", dumpManifest{}, "", err
	}
	if err := validateDumpArtifactSet(cleanPath, manifest); err != nil {
		return "", dumpManifest{}, "", err
	}
	if err := iterateSealedDumpContext(ctx, cleanPath, manifest, func(int64, *iavl.ChangeSet) error { return nil }); err != nil {
		return "", dumpManifest{}, "", err
	}
	return cleanPath, manifest, sha256Hex(body), nil
}

func iterateSealedDump(path string, manifest dumpManifest, fn func(int64, *iavl.ChangeSet) error) error {
	return iterateSealedDumpContext(context.Background(), path, manifest, fn)
}

func iterateSealedDumpContext(ctx context.Context, path string, manifest dumpManifest, fn func(int64, *iavl.ChangeSet) error) error {
	if ctx == nil {
		return fmt.Errorf("iterate sealed dump context is required")
	}
	path, err := requireSealedDumpDirectory(path)
	if err != nil {
		return err
	}
	if err := validateDumpArtifactSet(path, manifest); err != nil {
		return err
	}
	if manifest.SourceManifestSHA256 != "" {
		sourcePath := filepath.Join(path, dumpSourceFileName)
		if err := requireRegularFile(sourcePath, "sealed dump source manifest"); err != nil {
			return err
		}
		_, sourceHash, found, err := loadDumpSource(sourcePath)
		if err != nil {
			return err
		}
		if !found || sourceHash != manifest.SourceManifestSHA256 {
			return fmt.Errorf("sealed dump source manifest changed")
		}
	}
	expected := manifest.FirstVersion
	var records int64
	for _, file := range manifest.Files {
		filePath, err := requireDumpArtifact(path, file.Path)
		if err != nil {
			return err
		}
		entry, err := scanZlibChangeSetsContext(ctx, filePath, func(version int64, changeSet *iavl.ChangeSet) error {
			if version != expected {
				return fmt.Errorf("dump version %d, want %d", version, expected)
			}
			expected++
			return fn(version, changeSet)
		})
		if err != nil {
			return err
		}
		if entry.SHA256 != file.SHA256 || entry.Size != file.Size || entry.FirstVersion != file.FirstVersion || entry.LastVersion != file.LastVersion || entry.Records != file.Records {
			return fmt.Errorf("sealed dump file changed: %s", file.Path)
		}
		records += entry.Records
	}
	if expected != manifest.LastVersion+1 {
		return fmt.Errorf("dump ended at %d, want %d", expected-1, manifest.LastVersion)
	}
	expectedRecords := manifest.LastVersion - manifest.FirstVersion + 1
	if records != manifest.Records || records != expectedRecords {
		return fmt.Errorf("dump has %d records, manifest has %d for versions %d through %d", records, manifest.Records, manifest.FirstVersion, manifest.LastVersion)
	}
	return nil
}

func iterateSealedDumpRangeContext(
	ctx context.Context,
	path string,
	manifest dumpManifest,
	first, last int64,
	fn func(int64, *iavl.ChangeSet) error,
) error {
	if ctx == nil {
		return fmt.Errorf("iterate sealed dump range context is required")
	}
	if fn == nil {
		return fmt.Errorf("iterate sealed dump range callback is required")
	}
	if first < 1 || last < first {
		return fmt.Errorf("invalid sealed dump range %d-%d", first, last)
	}
	if first < manifest.FirstVersion || last > manifest.LastVersion {
		return fmt.Errorf(
			"sealed dump range %d-%d is outside manifest range %d-%d",
			first, last, manifest.FirstVersion, manifest.LastVersion,
		)
	}
	path, err := requireSealedDumpDirectory(path)
	if err != nil {
		return err
	}
	if err := validateDumpArtifactSet(path, manifest); err != nil {
		return err
	}
	if manifest.SourceManifestSHA256 != "" {
		sourcePath := filepath.Join(path, dumpSourceFileName)
		if err := requireRegularFile(sourcePath, "sealed dump source manifest"); err != nil {
			return err
		}
		_, sourceHash, found, err := loadDumpSource(sourcePath)
		if err != nil {
			return err
		}
		if !found || sourceHash != manifest.SourceManifestSHA256 {
			return fmt.Errorf("sealed dump source manifest changed")
		}
	}

	var delivered int64
	var touched bool
	for _, file := range manifest.Files {
		if file.LastVersion < first || file.FirstVersion > last {
			continue
		}
		touched = true
		filePath, err := requireDumpArtifact(path, file.Path)
		if err != nil {
			return err
		}
		entry, err := scanZlibChangeSetsContext(ctx, filePath, func(version int64, changeSet *iavl.ChangeSet) error {
			if version < first || version > last {
				return nil
			}
			expected := first + delivered
			if version != expected {
				return fmt.Errorf("dump version %d, want %d", version, expected)
			}
			delivered++
			return fn(version, changeSet)
		})
		if err != nil {
			return err
		}
		if entry.SHA256 != file.SHA256 || entry.Size != file.Size ||
			entry.FirstVersion != file.FirstVersion || entry.LastVersion != file.LastVersion ||
			entry.Records != file.Records {
			return fmt.Errorf("sealed dump file changed: %s", file.Path)
		}
	}
	expectedRecords := last - first + 1
	if !touched || delivered != expectedRecords {
		return fmt.Errorf(
			"sealed dump range %d-%d contains %d records, want %d",
			first, last, delivered, expectedRecords,
		)
	}
	return nil
}

func validateSealedDumpArtifactsContext(ctx context.Context, path string, manifest dumpManifest) error {
	if ctx == nil {
		return fmt.Errorf("validate sealed dump context is required")
	}
	path, err := requireSealedDumpDirectory(path)
	if err != nil {
		return err
	}
	if manifest.FirstVersion < 1 || manifest.LastVersion < manifest.FirstVersion ||
		manifest.Records != manifest.LastVersion-manifest.FirstVersion+1 {
		return fmt.Errorf("sealed dump range or record count is invalid")
	}
	if err := validateDumpArtifactSet(path, manifest); err != nil {
		return err
	}
	if manifest.SourceManifestSHA256 != "" {
		sourcePath := filepath.Join(path, dumpSourceFileName)
		if err := requireRegularFile(sourcePath, "sealed dump source manifest"); err != nil {
			return err
		}
		_, sourceHash, found, err := loadDumpSource(sourcePath)
		if err != nil {
			return err
		}
		if !found || sourceHash != manifest.SourceManifestSHA256 {
			return fmt.Errorf("sealed dump source manifest changed")
		}
	}
	seen := make(map[string]struct{}, len(manifest.Files))
	expected := manifest.FirstVersion
	var records int64
	for _, file := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, found := seen[file.Path]; found {
			return fmt.Errorf("duplicate sealed dump file %q", file.Path)
		}
		seen[file.Path] = struct{}{}
		filePath, err := requireDumpArtifact(path, file.Path)
		if err != nil {
			return err
		}
		if file.FirstVersion != expected || file.LastVersion < file.FirstVersion ||
			file.Records != file.LastVersion-file.FirstVersion+1 || file.Size < 1 || !validSHA256Hex(file.SHA256) {
			return fmt.Errorf("sealed dump file metadata is invalid: %s", file.Path)
		}
		hash, size, err := hashFileContext(ctx, filePath)
		if err != nil {
			return err
		}
		if hash != file.SHA256 || size != file.Size {
			return fmt.Errorf("sealed dump file changed: %s", file.Path)
		}
		if file.Records > manifest.Records-records {
			return fmt.Errorf("sealed dump file records exceed manifest: %s", file.Path)
		}
		expected = file.LastVersion + 1
		records += file.Records
	}
	if len(manifest.Files) == 0 || expected != manifest.LastVersion+1 || records != manifest.Records ||
		records != manifest.LastVersion-manifest.FirstVersion+1 {
		return fmt.Errorf("sealed dump file coverage differs from manifest")
	}
	return nil
}

func validateDumpArtifactSet(path string, manifest dumpManifest) error {
	rootEntries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	allowedRoot := map[string]struct{}{"evm": {}, "dump-manifest.v1.json": {}}
	if manifest.SourceManifestSHA256 != "" {
		allowedRoot[dumpSourceFileName] = struct{}{}
	}
	for _, entry := range rootEntries {
		if _, ok := allowedRoot[entry.Name()]; !ok {
			return fmt.Errorf("unexpected dump artifact: %s", entry.Name())
		}
	}
	wantFiles := make(map[string]struct{}, len(manifest.Files))
	for _, file := range manifest.Files {
		if _, err := requireDumpArtifact(path, file.Path); err != nil {
			return err
		}
		if _, found := wantFiles[file.Path]; found {
			return fmt.Errorf("duplicate dump file %q", file.Path)
		}
		wantFiles[file.Path] = struct{}{}
	}
	evmEntries, err := os.ReadDir(filepath.Join(path, "evm"))
	if err != nil {
		return err
	}
	if len(evmEntries) != len(wantFiles) {
		return fmt.Errorf("dump EVM artifact set differs from manifest")
	}
	for _, entry := range evmEntries {
		name := filepath.Join("evm", entry.Name())
		if _, ok := wantFiles[name]; !ok {
			return fmt.Errorf("unexpected dump EVM artifact: %s", entry.Name())
		}
		if err := requireRegularFile(filepath.Join(path, name), "dump EVM artifact"); err != nil {
			return err
		}
	}
	return nil
}

func validateSealedDumpManifest(path string, expected dumpManifest, expectedHash string) error {
	body, err := readRegularFileNoFollow(filepath.Join(path, "dump-manifest.v1.json"), "sealed dump manifest")
	if err != nil {
		return err
	}
	if sha256Hex(body) != expectedHash {
		return fmt.Errorf("sealed dump manifest changed")
	}
	var current dumpManifest
	if err := decodeStrictJSON(body, &current, "dump manifest"); err != nil {
		return err
	}
	if !reflect.DeepEqual(current, expected) {
		return fmt.Errorf("sealed dump manifest identity changed")
	}
	return nil
}

func validateDumpContext(manifest dumpManifest, context dumpContext) error {
	if err := validateDumpSourceContext(context); err != nil {
		return err
	}
	if !validCreatedAt(manifest.CreatedAt) {
		return fmt.Errorf("dump manifest creation time is missing or malformed")
	}
	checks := []struct {
		name  string
		equal bool
		got   any
		want  any
	}{
		{"schema", manifest.Schema == context.Schema, manifest.Schema, context.Schema},
		{"first version", manifest.FirstVersion == context.FirstVersion, manifest.FirstVersion, context.FirstVersion},
		{"last version", manifest.LastVersion == context.LastVersion, manifest.LastVersion, context.LastVersion},
		{"snapshot ID", manifest.SnapshotID == context.SnapshotID, manifest.SnapshotID, context.SnapshotID},
		{"archive identity", manifest.ArchiveIdentity == context.ArchiveIdentity, manifest.ArchiveIdentity, context.ArchiveIdentity},
		{"Cronos commit", manifest.CronosCommit == context.CronosCommit, manifest.CronosCommit, context.CronosCommit},
		{"Ethermint commit", manifest.EthermintCommit == context.EthermintCommit, manifest.EthermintCommit, context.EthermintCommit},
		{"IAVL commit", manifest.IAVLCommit == context.IAVLCommit, manifest.IAVLCommit, context.IAVLCommit},
		{"image digest", manifest.ImageDigest == context.ImageDigest, manifest.ImageDigest, context.ImageDigest},
		{"build tags", manifest.BuildTags == context.BuildTags, manifest.BuildTags, context.BuildTags},
	}
	for _, check := range checks {
		if !check.equal {
			return fmt.Errorf("sealed dump identity does not match this run: %s is %v, want %v", check.name, check.got, check.want)
		}
	}
	if manifest.SnapshotID == "" || manifest.ImageDigest == "" {
		return fmt.Errorf("snapshot ID and image digest are required to seal an execution dump")
	}
	if manifest.SourceManifestSHA256 == "" {
		return fmt.Errorf("execution dump has no source manifest hash")
	}
	if manifest.Schema == dumpManifestSchema && manifest.ArchiveIdentity.LatestVersion != manifest.LastVersion {
		return fmt.Errorf("dump final version %d differs from archive %d", manifest.LastVersion, manifest.ArchiveIdentity.LatestVersion)
	}
	if manifest.LastVersion > manifest.ArchiveIdentity.LatestVersion {
		return fmt.Errorf("dump final version %d exceeds archive %d", manifest.LastVersion, manifest.ArchiveIdentity.LatestVersion)
	}
	return nil
}
