package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	worldAuditCheckpointSchema = "statediff-rewriter-world-state-audit-checkpoint/v1"
	worldAuditScannerVersion   = "cronos-world-state-audit/v1"
)

type worldAuditImplementationIdentity struct {
	ScannerVersion  string `json:"scanner_version"`
	BuildID         string `json:"build_id"`
	CronosCommit    string `json:"cronos_commit"`
	EthermintCommit string `json:"ethermint_commit"`
	IAVLCommit      string `json:"iavl_commit"`
	BuildTags       string `json:"build_tags"`
}

func currentWorldAuditImplementationIdentity() worldAuditImplementationIdentity {
	identity := currentBuildIdentity()
	return worldAuditImplementationIdentity{
		ScannerVersion:  worldAuditScannerVersion,
		BuildID:         buildCommit(),
		CronosCommit:    identity.CronosCommit,
		EthermintCommit: identity.EthermintCommit,
		IAVLCommit:      identity.IAVLCommit,
		BuildTags:       identity.BuildTags,
	}
}

type worldAuditSummary struct {
	Frontier                int64             `json:"frontier"`
	GenesisAudited          bool              `json:"genesis_audited"`
	GenesisFindings         uint64            `json:"genesis_findings"`
	Processed               uint64            `json:"processed"`
	CandidateHeights        uint64            `json:"candidate_heights"`
	S3Gets                  uint64            `json:"s3_gets"`
	SkippedNoChanges        uint64            `json:"skipped_no_changes"`
	SkippedEqualRoot        uint64            `json:"skipped_equal_root"`
	ExpectedNewAccounts     uint64            `json:"expected_new_accounts"`
	ExpectedDeletedAccounts uint64            `json:"expected_deleted_accounts"`
	ExpectedCodeWrites      uint64            `json:"expected_code_writes"`
	CodeDeletes             uint64            `json:"code_deletes"`
	Findings                uint64            `json:"findings"`
	Defects                 uint64            `json:"defects"`
	Warnings                uint64            `json:"warnings"`
	FindingsByKind          map[string]uint64 `json:"findings_by_kind"`
	ObjectBytes             uint64            `json:"object_bytes"`
}

type worldAuditCheckpoint struct {
	Schema          string                           `json:"schema"`
	Checksum        string                           `json:"checksum"`
	Implementation  worldAuditImplementationIdentity `json:"implementation"`
	ArchiveIdentity archiveIdentity                  `json:"archive_identity"`
	Bucket          string                           `json:"bucket"`
	Prefix          string                           `json:"prefix"`
	Region          string                           `json:"region"`
	EVMDenom        string                           `json:"evm_denom"`
	FirstHeight     int64                            `json:"first_height"`
	FinalHeight     int64                            `json:"final_height"`
	Partial         bool                             `json:"partial"`
	FindingsPath    string                           `json:"findings_path"`
	FindingsBytes   int64                            `json:"findings_bytes"`
	FindingsSHA256  string                           `json:"findings_sha256"`
	CodesPath       string                           `json:"codes_path"`
	CodesBytes      int64                            `json:"codes_bytes"`
	CodesSHA256     string                           `json:"codes_sha256"`
	Summary         worldAuditSummary                `json:"summary"`
	Completed       bool                             `json:"completed"`
}

func newWorldAuditSummary() worldAuditSummary {
	return worldAuditSummary{FindingsByKind: make(map[string]uint64)}
}

func worldAuditCheckpointChecksum(checkpoint worldAuditCheckpoint) (string, error) {
	checkpoint.Checksum = ""
	body, err := canonicalJSON(checkpoint)
	if err != nil {
		return "", err
	}
	return sha256Hex(body), nil
}

func canonicalJSON(value any) ([]byte, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return body, nil
}

type appendAuditLog struct {
	path   string
	file   *os.File
	writer *bufio.Writer
	hash   hash.Hash
	bytes  int64
}

func createAppendAuditLog(path string) (*appendAuditLog, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	return &appendAuditLog{
		path: path, file: file, writer: bufio.NewWriterSize(file, 1<<20), hash: sha256.New(),
	}, nil
}

func restoreAppendAuditLog(path string, size int64, expectedHash string) (*appendAuditLog, error) {
	if size < 0 || !validSHA256Hex(expectedHash) {
		return nil, fmt.Errorf("invalid append log checkpoint for %s", path)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = file.Close()
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() < size {
		return nil, fmt.Errorf("append log %s has %d bytes, checkpoint requires %d", path, info.Size(), size)
	}
	if err := file.Truncate(size); err != nil {
		return nil, err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	hasher := sha256.New()
	if _, err := io.CopyN(hasher, file, size); err != nil {
		return nil, err
	}
	if actual := hex.EncodeToString(hasher.Sum(nil)); actual != expectedHash {
		return nil, fmt.Errorf("append log %s hash is %s, checkpoint has %s", path, actual, expectedHash)
	}
	if _, err := file.Seek(size, io.SeekStart); err != nil {
		return nil, err
	}
	closeOnError = false
	return &appendAuditLog{
		path: path, file: file, writer: bufio.NewWriterSize(file, 1<<20),
		hash: hasher, bytes: size,
	}, nil
}

func validateCompletedAuditLog(path string, size int64, expectedHash string) error {
	if size < 0 || !validSHA256Hex(expectedHash) {
		return fmt.Errorf("invalid completed append log checkpoint for %s", path)
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() != size {
		return fmt.Errorf("completed append log %s has %d bytes, checkpoint requires exactly %d", path, info.Size(), size)
	}
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return err
	}
	if actual := hex.EncodeToString(hasher.Sum(nil)); actual != expectedHash {
		return fmt.Errorf("completed append log %s hash is %s, checkpoint has %s", path, actual, expectedHash)
	}
	return nil
}

func (log *appendAuditLog) Append(body []byte) error {
	if log == nil || log.writer == nil {
		return fmt.Errorf("append log is not open")
	}
	if _, err := log.writer.Write(body); err != nil {
		return err
	}
	if _, err := log.hash.Write(body); err != nil {
		return err
	}
	log.bytes += int64(len(body))
	return nil
}

func (log *appendAuditLog) Flush() error {
	if err := log.writer.Flush(); err != nil {
		return err
	}
	return log.file.Sync()
}

func (log *appendAuditLog) Close() error {
	if log == nil || log.file == nil {
		return nil
	}
	if err := log.writer.Flush(); err != nil {
		_ = log.file.Close()
		return err
	}
	return log.file.Close()
}

func (log *appendAuditLog) SHA256() string {
	return hex.EncodeToString(log.hash.Sum(nil))
}

type worldAuditCheckpointSaver struct {
	path        string
	findings    *appendAuditLog
	codes       *appendAuditLog
	checkpoint  worldAuditCheckpoint
	every       uint64
	interval    time.Duration
	advances    uint64
	lastPersist time.Time
}

func (saver *worldAuditCheckpointSaver) Advance(frontier int64, summary worldAuditSummary) error {
	if frontier <= saver.checkpoint.Summary.Frontier {
		return fmt.Errorf("audit frontier %d does not advance beyond %d", frontier, saver.checkpoint.Summary.Frontier)
	}
	saver.checkpoint.Summary = summary
	saver.checkpoint.Summary.Frontier = frontier
	saver.advances++
	if saver.advances < saver.every && time.Since(saver.lastPersist) < saver.interval {
		return nil
	}
	return saver.Flush(false)
}

func (saver *worldAuditCheckpointSaver) Flush(completed bool) error {
	if saver == nil || saver.findings == nil || saver.codes == nil {
		return fmt.Errorf("audit checkpoint saver is not initialized")
	}
	if err := saver.findings.Flush(); err != nil {
		return err
	}
	if err := saver.codes.Flush(); err != nil {
		return err
	}
	saver.checkpoint.FindingsBytes = saver.findings.bytes
	saver.checkpoint.FindingsSHA256 = saver.findings.SHA256()
	saver.checkpoint.CodesBytes = saver.codes.bytes
	saver.checkpoint.CodesSHA256 = saver.codes.SHA256()
	saver.checkpoint.Completed = completed
	checksum, err := worldAuditCheckpointChecksum(saver.checkpoint)
	if err != nil {
		return err
	}
	saver.checkpoint.Checksum = checksum
	if _, err := atomicJSON(saver.path, saver.checkpoint); err != nil {
		return err
	}
	saver.advances = 0
	saver.lastPersist = time.Now()
	return nil
}

func loadWorldAuditCheckpoint(path string, expected worldAuditCheckpoint) (worldAuditCheckpoint, bool, error) {
	body, found, err := readMutableStateFile(path, "world-state audit checkpoint")
	if err != nil || !found {
		return worldAuditCheckpoint{}, found, err
	}
	var checkpoint worldAuditCheckpoint
	if err := decodeStrictJSON(body, &checkpoint, "world-state audit checkpoint"); err != nil {
		return worldAuditCheckpoint{}, false, err
	}
	if checkpoint.Schema != worldAuditCheckpointSchema {
		return worldAuditCheckpoint{}, false, fmt.Errorf("unsupported world-state audit checkpoint schema %q", checkpoint.Schema)
	}
	wantChecksum, err := worldAuditCheckpointChecksum(checkpoint)
	if err != nil {
		return worldAuditCheckpoint{}, false, err
	}
	if checkpoint.Checksum == "" || checkpoint.Checksum != wantChecksum {
		return worldAuditCheckpoint{}, false, fmt.Errorf("world-state audit checkpoint checksum mismatch")
	}
	if checkpoint.ArchiveIdentity != expected.ArchiveIdentity ||
		checkpoint.Implementation != expected.Implementation ||
		checkpoint.Bucket != expected.Bucket || checkpoint.Prefix != expected.Prefix ||
		checkpoint.Region != expected.Region || checkpoint.EVMDenom != expected.EVMDenom ||
		checkpoint.FirstHeight != expected.FirstHeight || checkpoint.FinalHeight != expected.FinalHeight ||
		checkpoint.Partial != expected.Partial ||
		checkpoint.FindingsPath != expected.FindingsPath || checkpoint.CodesPath != expected.CodesPath {
		return worldAuditCheckpoint{}, false, fmt.Errorf("world-state audit checkpoint does not match this run")
	}
	summary := checkpoint.Summary
	if summary.FindingsByKind == nil || summary.Frontier < 0 ||
		(summary.Frontier != 0 && (summary.Frontier < checkpoint.FirstHeight || summary.Frontier > checkpoint.FinalHeight)) ||
		(summary.Frontier == 0) != (summary.Processed == 0) ||
		(summary.Frontier != 0 && summary.Processed != uint64(summary.Frontier-checkpoint.FirstHeight+1)) ||
		summary.GenesisFindings > summary.Findings || summary.Defects+summary.Warnings != summary.Findings ||
		checkpoint.FindingsBytes < 0 || checkpoint.CodesBytes < 0 || checkpoint.CodesBytes%commonHashLength != 0 ||
		!validSHA256Hex(checkpoint.FindingsSHA256) || !validSHA256Hex(checkpoint.CodesSHA256) ||
		(checkpoint.Completed && (!summary.GenesisAudited || summary.Frontier != checkpoint.FinalHeight)) {
		return worldAuditCheckpoint{}, false, fmt.Errorf("world-state audit checkpoint counters are invalid")
	}
	return checkpoint, true, nil
}

const commonHashLength = 32
