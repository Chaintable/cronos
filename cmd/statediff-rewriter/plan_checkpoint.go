package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

const planCheckpointSchema = "statediff-rewriter-plan-checkpoint/v1"

type planCheckpoint struct {
	Schema    string          `json:"schema"`
	Checksum  string          `json:"checksum"`
	Frontier  uint64          `json:"frontier"`
	PrefixSHA string          `json:"prefix_sha256"`
	RootBytes int64           `json:"root_bytes"`
	Manifest  planManifest    `json:"manifest"`
	Pack      packWriterState `json:"pack"`
}

func planCheckpointChecksum(checkpoint planCheckpoint) (string, error) {
	checkpoint.Checksum = ""
	body, err := json.Marshal(checkpoint)
	if err != nil {
		return "", err
	}
	return sha256Hex(body), nil
}

func restorePlanRootIndex(dir string, size int64) (string, error) {
	tmpPath := filepath.Join(dir, "roots.by-height.tmp")
	finalPath := filepath.Join(dir, "roots.by-height")
	found, err := restoreNoReplacePartial(tmpPath, finalPath, "resumable height root index")
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("resumable height root index is missing")
	}
	if err := truncateCheckpointFile(tmpPath, size); err != nil {
		return "", err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == "roots.sorted" || name == "manifest.v1.json" || strings.HasPrefix(name, "roots-sort-") {
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				return "", err
			}
		}
	}
	return tmpPath, nil
}

func loadPlanCheckpoint(path string, expected planManifest) (planCheckpoint, bool, error) {
	body, found, err := readMutableStateFile(path, "plan checkpoint")
	if !found && err == nil {
		return planCheckpoint{}, false, nil
	}
	if err != nil {
		return planCheckpoint{}, false, err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var checkpoint planCheckpoint
	if err := decoder.Decode(&checkpoint); err != nil {
		return planCheckpoint{}, false, fmt.Errorf("decode plan checkpoint: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return planCheckpoint{}, false, fmt.Errorf("plan checkpoint has trailing JSON")
	}
	if checkpoint.Schema != planCheckpointSchema {
		return planCheckpoint{}, false, fmt.Errorf("unsupported plan checkpoint schema %q", checkpoint.Schema)
	}
	wantChecksum, err := planCheckpointChecksum(checkpoint)
	if err != nil {
		return planCheckpoint{}, false, fmt.Errorf("hash plan checkpoint: %w", err)
	}
	if checkpoint.Checksum == "" || checkpoint.Checksum != wantChecksum {
		return planCheckpoint{}, false, fmt.Errorf("plan checkpoint checksum mismatch")
	}
	if !samePlanIdentity(checkpoint.Manifest, expected) {
		return planCheckpoint{}, false, fmt.Errorf("plan checkpoint identity differs from this run")
	}
	if checkpoint.Manifest.RunID == "" || checkpoint.Manifest.CreatedAt == "" ||
		!validCreatedAt(checkpoint.Manifest.CreatedAt) || !validSHA256Hex(checkpoint.PrefixSHA) ||
		checkpoint.Frontier > uint64(expected.FinalHeight) ||
		checkpoint.Frontier != 0 && checkpoint.Frontier < uint64(expected.FirstHeight) {
		return planCheckpoint{}, false, fmt.Errorf("plan checkpoint frontier or run identity is invalid")
	}
	var wantProcessed int64
	if checkpoint.Frontier != 0 {
		wantProcessed = int64(checkpoint.Frontier) - expected.FirstHeight + 1
	}
	manifest := checkpoint.Manifest
	if checkpoint.Frontier == 0 && checkpoint.PrefixSHA != strings.Repeat("0", 64) {
		return planCheckpoint{}, false, fmt.Errorf("zero-frontier plan checkpoint has a non-zero prefix hash")
	}
	if manifest.Processed != wantProcessed || manifest.Processed < 0 || manifest.Changed < 0 || manifest.Unchanged < 0 ||
		manifest.SkippedEqualRoot < 0 || manifest.ChangedCanonical < 0 || manifest.ChangedConflict < 0 ||
		manifest.OldBytes < 0 || manifest.NewBytes < 0 || manifest.Changed > manifest.Processed ||
		manifest.Unchanged > manifest.Processed-manifest.Changed ||
		manifest.SkippedEqualRoot != manifest.Processed-manifest.Changed-manifest.Unchanged ||
		manifest.ChangedCanonical > manifest.Changed || manifest.ChangedConflict > manifest.Changed ||
		manifest.Processed > math.MaxInt64/rootRecordSize || checkpoint.RootBytes != manifest.Processed*rootRecordSize {
		return planCheckpoint{}, false, fmt.Errorf("plan checkpoint counters are inconsistent")
	}
	if checkpoint.Pack.Chunk < 0 || checkpoint.Pack.Chunk != len(checkpoint.Pack.Chunks) ||
		checkpoint.Pack.PackOffset < 0 || checkpoint.Pack.IndexOffset < 0 || checkpoint.Pack.Records < 0 ||
		(checkpoint.Pack.Records == 0 && (checkpoint.Pack.PackOffset != 0 || checkpoint.Pack.IndexOffset != 0)) ||
		(checkpoint.Pack.Records > 0 && (checkpoint.Pack.PackOffset == 0 || checkpoint.Pack.IndexOffset == 0)) {
		return planCheckpoint{}, false, fmt.Errorf("plan checkpoint pack state is invalid")
	}
	var packRecords int64
	for number, chunk := range checkpoint.Pack.Chunks {
		if chunk.Number != number || chunk.Pack != fmt.Sprintf("chunk-%06d.pack", number) ||
			chunk.Index != fmt.Sprintf("chunk-%06d.idx", number) || chunk.Records < 1 || chunk.PackSize < 1 ||
			!validSHA256Hex(chunk.PackSHA256) || !validSHA256Hex(chunk.IndexSHA256) {
			return planCheckpoint{}, false, fmt.Errorf("plan checkpoint completed chunk %d is invalid", number)
		}
		if packRecords > math.MaxInt64-chunk.Records {
			return planCheckpoint{}, false, fmt.Errorf("plan checkpoint pack record count overflows")
		}
		packRecords += chunk.Records
	}
	if packRecords > math.MaxInt64-checkpoint.Pack.Records {
		return planCheckpoint{}, false, fmt.Errorf("plan checkpoint pack record count overflows")
	}
	packRecords += checkpoint.Pack.Records
	if packRecords != manifest.Changed {
		return planCheckpoint{}, false, fmt.Errorf("plan checkpoint pack has %d records, manifest has %d", packRecords, manifest.Changed)
	}
	return checkpoint, true, nil
}

func samePlanIdentity(left, right planManifest) bool {
	return left.Schema == right.Schema && left.Sealed == right.Sealed && left.Bucket == right.Bucket &&
		left.Prefix == right.Prefix && left.Region == right.Region && left.FirstHeight == right.FirstHeight &&
		left.FinalHeight == right.FinalHeight && left.CronosCommit == right.CronosCommit &&
		left.EthermintCommit == right.EthermintCommit && left.IAVLCommit == right.IAVLCommit &&
		effectivePlanSourceMode(left) == effectivePlanSourceMode(right) && left.DumpPath == right.DumpPath &&
		left.DumpManifestHash == right.DumpManifestHash && left.ArchiveIdentity == right.ArchiveIdentity &&
		left.SnapshotID == right.SnapshotID && left.ImageDigest == right.ImageDigest && left.BuildTags == right.BuildTags
}

type planCheckpointSaver struct {
	path        string
	rootBuffer  *bufio.Writer
	rootFile    *os.File
	writer      *packWriter
	minFree     uint64
	every       uint64
	interval    time.Duration
	advances    uint64
	dirty       bool
	frontier    uint64
	prefixSHA   common.Hash
	manifest    planManifest
	lastPersist time.Time
	now         func() time.Time
}

func newPlanCheckpointSaver(
	path string,
	rootBuffer *bufio.Writer,
	rootFile *os.File,
	writer *packWriter,
	minFree uint64,
	every uint64,
	interval time.Duration,
) (*planCheckpointSaver, error) {
	if minFree == 0 || every == 0 || interval <= 0 {
		return nil, fmt.Errorf("plan min-free, checkpoint interval, and record count must be positive")
	}
	return &planCheckpointSaver{
		path: path, rootBuffer: rootBuffer, rootFile: rootFile, writer: writer, minFree: minFree,
		every: every, interval: interval, lastPersist: time.Now(), now: time.Now,
	}, nil
}

func (saver *planCheckpointSaver) Initialize(manifest planManifest) error {
	if saver.frontier != 0 || saver.dirty || saver.advances != 0 {
		return fmt.Errorf("plan checkpoint saver is already initialized")
	}
	if manifest.Processed != 0 || manifest.Changed != 0 || manifest.Unchanged != 0 || manifest.SkippedEqualRoot != 0 {
		return fmt.Errorf("initial plan checkpoint has non-zero counters")
	}
	saver.manifest = manifest
	saver.dirty = true
	return saver.Flush()
}

func (saver *planCheckpointSaver) Advance(frontier uint64, manifest planManifest, prefixSHA common.Hash) error {
	if frontier <= saver.frontier {
		return fmt.Errorf("plan checkpoint frontier %d does not advance beyond %d", frontier, saver.frontier)
	}
	saver.frontier = frontier
	saver.prefixSHA = prefixSHA
	saver.manifest = manifest
	saver.advances++
	saver.dirty = true
	if saver.advances >= saver.every || saver.now().Sub(saver.lastPersist) >= saver.interval {
		return saver.Flush()
	}
	return nil
}

func (saver *planCheckpointSaver) Flush() error {
	if !saver.dirty {
		return nil
	}
	if err := saver.rootBuffer.Flush(); err != nil {
		return err
	}
	if err := saver.rootFile.Sync(); err != nil {
		return err
	}
	pack, err := saver.writer.Sync()
	if err != nil {
		return err
	}
	stat, err := saver.rootFile.Stat()
	if err != nil {
		return err
	}
	if saver.manifest.Processed < 0 || saver.manifest.Processed > math.MaxInt64/rootRecordSize {
		return fmt.Errorf("plan checkpoint root index size overflows")
	}
	wantRootBytes := saver.manifest.Processed * rootRecordSize
	if stat.Size() != wantRootBytes {
		return fmt.Errorf("height root index has %d bytes, want %d", stat.Size(), wantRootBytes)
	}
	manifest := saver.manifest
	manifest.Chunks = append([]chunkManifest(nil), pack.Chunks...)
	checkpoint := planCheckpoint{
		Schema: planCheckpointSchema, Frontier: saver.frontier, PrefixSHA: hex.EncodeToString(saver.prefixSHA[:]), RootBytes: stat.Size(),
		Manifest: manifest, Pack: pack,
	}
	checkpoint.Checksum, err = planCheckpointChecksum(checkpoint)
	if err != nil {
		return err
	}
	if err := validateMutableStatePath(saver.path, "plan checkpoint"); err != nil {
		return err
	}
	_, err = atomicJSONWithMinFree(saver.path, checkpoint, saver.minFree)
	if err != nil {
		return err
	}
	saver.advances = 0
	saver.dirty = false
	saver.lastPersist = saver.now()
	return nil
}
