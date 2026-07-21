package main

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

type dumpContext struct {
	Schema          string
	FirstVersion    int64
	LastVersion     int64
	SnapshotID      string
	ArchiveIdentity archiveIdentity
	CronosCommit    string
	EthermintCommit string
	ImageDigest     string
	BuildTags       string
}

func (w *countingWriter) Write(body []byte) (int, error) {
	w.n += int64(len(body))
	return len(body), nil
}

func scanZlibChangeSets(path string, fn func(int64, *iavl.ChangeSet) error) (dumpFileManifest, error) {
	file, err := os.Open(path)
	if err != nil {
		return dumpFileManifest{}, err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return dumpFileManifest{}, err
	}

	hasher := sha256.New()
	counter := &countingWriter{}
	compressed := bufio.NewReader(io.TeeReader(file, io.MultiWriter(hasher, counter)))
	zreader, err := zlib.NewReader(compressed)
	if err != nil {
		return dumpFileManifest{}, fmt.Errorf("open zlib %s: %w", path, err)
	}
	if multistream, ok := zreader.(interface{ Multistream(bool) }); ok {
		multistream.Multistream(false)
	}
	reader := bufio.NewReader(zreader)

	var first, last, records int64
	for {
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

func sealDump(stagingPath string, contexts ...dumpContext) (string, dumpManifest, string, error) {
	if !strings.HasSuffix(filepath.Clean(stagingPath), ".staging") {
		return "", dumpManifest{}, "", fmt.Errorf("dump path must end in .staging")
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
		manifest.Schema = context.Schema
		manifest.SnapshotID, manifest.ArchiveIdentity = context.SnapshotID, context.ArchiveIdentity
		manifest.CronosCommit, manifest.EthermintCommit = context.CronosCommit, context.EthermintCommit
		manifest.ImageDigest, manifest.BuildTags = context.ImageDigest, context.BuildTags
	}
	for _, path := range files {
		entry, err := scanZlibChangeSets(path, func(version int64, changeSet *iavl.ChangeSet) error {
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

	body, err := atomicJSON(filepath.Join(stagingPath, "dump-manifest.v1.json"), manifest)
	if err != nil {
		return "", dumpManifest{}, "", err
	}
	sealedPath := strings.TrimSuffix(filepath.Clean(stagingPath), ".staging") + ".sealed"
	if _, err := os.Stat(sealedPath); err == nil {
		return "", dumpManifest{}, "", fmt.Errorf("sealed dump already exists: %s", sealedPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", dumpManifest{}, "", err
	}
	if err := syncDir(stagingPath); err != nil {
		return "", dumpManifest{}, "", err
	}
	if err := os.Rename(stagingPath, sealedPath); err != nil {
		return "", dumpManifest{}, "", err
	}
	if err := syncDir(filepath.Dir(sealedPath)); err != nil {
		return "", dumpManifest{}, "", err
	}
	return sealedPath, manifest, sha256Hex(body), nil
}

func prepareDump(path string, context dumpContext) (string, dumpManifest, string, error) {
	cleanPath := filepath.Clean(path)
	if strings.HasSuffix(cleanPath, ".staging") {
		return sealDump(cleanPath, context)
	}
	if !strings.HasSuffix(cleanPath, ".sealed") {
		return "", dumpManifest{}, "", fmt.Errorf("dump path must end in .staging or .sealed")
	}
	body, err := os.ReadFile(filepath.Join(cleanPath, "dump-manifest.v1.json"))
	if err != nil {
		return "", dumpManifest{}, "", err
	}
	var manifest dumpManifest
	if err := json.Unmarshal(body, &manifest); err != nil {
		return "", dumpManifest{}, "", err
	}
	if err := validateDumpContext(manifest, context); err != nil {
		return "", dumpManifest{}, "", err
	}
	if err := iterateSealedDump(cleanPath, manifest, func(int64, *iavl.ChangeSet) error { return nil }); err != nil {
		return "", dumpManifest{}, "", err
	}
	return cleanPath, manifest, sha256Hex(body), nil
}

func iterateSealedDump(path string, manifest dumpManifest, fn func(int64, *iavl.ChangeSet) error) error {
	expected := manifest.FirstVersion
	var records int64
	for _, file := range manifest.Files {
		entry, err := scanZlibChangeSets(filepath.Join(path, file.Path), func(version int64, changeSet *iavl.ChangeSet) error {
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

func validateDumpContext(manifest dumpManifest, context dumpContext) error {
	if context.Schema != dumpManifestSchema && context.Schema != pilotDumpManifestSchema {
		return fmt.Errorf("unsupported dump schema %q", context.Schema)
	}
	if context.FirstVersion < 1 || context.LastVersion < context.FirstVersion {
		return fmt.Errorf("invalid expected dump range %d-%d", context.FirstVersion, context.LastVersion)
	}
	if manifest.Schema != context.Schema || manifest.FirstVersion != context.FirstVersion || manifest.LastVersion != context.LastVersion ||
		manifest.SnapshotID != context.SnapshotID || manifest.ArchiveIdentity != context.ArchiveIdentity ||
		manifest.CronosCommit != context.CronosCommit || manifest.EthermintCommit != context.EthermintCommit ||
		manifest.ImageDigest != context.ImageDigest || manifest.BuildTags != context.BuildTags {
		return fmt.Errorf("sealed dump identity does not match this run")
	}
	if manifest.SnapshotID == "" || manifest.ImageDigest == "" {
		return fmt.Errorf("snapshot ID and image digest are required to seal an execution dump")
	}
	if manifest.Schema == dumpManifestSchema && manifest.ArchiveIdentity.LatestVersion != manifest.LastVersion {
		return fmt.Errorf("dump final version %d differs from archive %d", manifest.LastVersion, manifest.ArchiveIdentity.LatestVersion)
	}
	if manifest.LastVersion > manifest.ArchiveIdentity.LatestVersion {
		return fmt.Errorf("dump final version %d exceeds archive %d", manifest.LastVersion, manifest.ArchiveIdentity.LatestVersion)
	}
	return nil
}
