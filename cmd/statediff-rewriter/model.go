package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	dtypes "github.com/evmos/ethermint/debank/types"
)

const (
	manifestSchema        = "statediff-rewriter-manifest/v1"
	pilotManifestSchema   = "statediff-rewriter-pilot-manifest/v1"
	segmentManifestSchema = "statediff-rewriter-segment-manifest/v1"
	planSourceDumpV1      = "dump-v1"
	planSourceDirectV1    = "direct-iavl-v1"
	packSchema            = uint64(1)
	defaultBucket         = "chaintable-nodex-pipeline--apne1-az4--x-s3"
	defaultPrefix         = "25/4cc51d2e"
	defaultRegion         = "ap-northeast-1"
	maxObjectSize         = int64(64 << 20)
	chunkSize             = int64(512 << 20)
)

type dumpFileManifest struct {
	Path         string `json:"path"`
	SHA256       string `json:"sha256"`
	Size         int64  `json:"size"`
	FirstVersion int64  `json:"first_version"`
	LastVersion  int64  `json:"last_version"`
	Records      int64  `json:"records"`
}

type dumpManifest struct {
	Schema               string             `json:"schema"`
	CreatedAt            string             `json:"created_at"`
	SnapshotID           string             `json:"snapshot_id,omitempty"`
	ArchiveIdentity      archiveIdentity    `json:"archive_identity,omitempty"`
	CronosCommit         string             `json:"cronos_commit,omitempty"`
	EthermintCommit      string             `json:"ethermint_commit,omitempty"`
	IAVLCommit           string             `json:"iavl_commit,omitempty"`
	ImageDigest          string             `json:"image_digest,omitempty"`
	BuildTags            string             `json:"build_tags,omitempty"`
	SourceManifestSHA256 string             `json:"source_manifest_sha256,omitempty"`
	FirstVersion         int64              `json:"first_version"`
	LastVersion          int64              `json:"last_version"`
	Records              int64              `json:"records"`
	Files                []dumpFileManifest `json:"files"`
}

type chunkManifest struct {
	Number      int    `json:"number"`
	Pack        string `json:"pack"`
	PackSHA256  string `json:"pack_sha256"`
	PackSize    int64  `json:"pack_size"`
	Index       string `json:"index"`
	IndexSHA256 string `json:"index_sha256"`
	Records     int64  `json:"records"`
}

type planManifest struct {
	Schema                string          `json:"schema"`
	Sealed                bool            `json:"sealed"`
	RunID                 string          `json:"run_id"`
	CreatedAt             string          `json:"created_at"`
	Bucket                string          `json:"bucket"`
	Prefix                string          `json:"prefix"`
	Region                string          `json:"region"`
	FirstHeight           int64           `json:"first_height"`
	FinalHeight           int64           `json:"final_height"`
	CronosCommit          string          `json:"cronos_commit"`
	EthermintCommit       string          `json:"ethermint_commit"`
	IAVLCommit            string          `json:"iavl_commit"`
	SnapshotID            string          `json:"snapshot_id"`
	ImageDigest           string          `json:"image_digest"`
	BuildTags             string          `json:"build_tags"`
	SourceMode            string          `json:"source_mode,omitempty"`
	DumpPath              string          `json:"dump_path,omitempty"`
	DumpManifestHash      string          `json:"dump_manifest_sha256,omitempty"`
	DumpProducer          *dumpContext    `json:"dump_producer,omitempty"`
	ArchiveIdentity       archiveIdentity `json:"archive_identity"`
	InitialParentRoot     common.Hash     `json:"initial_parent_root"`
	Processed             int64           `json:"processed"`
	Changed               int64           `json:"changed"`
	ChangedCanonical      int64           `json:"changed_noncanonical"`
	ChangedConflict       int64           `json:"changed_conflicting_old_values"`
	Unchanged             int64           `json:"unchanged"`
	SkippedEqualRoot      int64           `json:"skipped_equal_root"`
	SlotsAdded            uint64          `json:"slots_added"`
	SlotsRemoved          uint64          `json:"slots_removed"`
	SlotsChanged          uint64          `json:"slots_changed"`
	OldBytes              int64           `json:"old_bytes"`
	NewBytes              int64           `json:"new_bytes"`
	HeightRootIndex       string          `json:"height_root_index"`
	HeightRootIndexSHA256 string          `json:"height_root_index_sha256"`
	RootIndex             string          `json:"root_index"`
	RootIndexSHA256       string          `json:"root_index_sha256"`
	RootMultisetSHA256    string          `json:"root_multiset_sha256"`
	Chunks                []chunkManifest `json:"chunks"`
}

func effectivePlanSourceMode(manifest planManifest) string {
	if manifest.SourceMode == "" {
		return planSourceDumpV1
	}
	return manifest.SourceMode
}

type archiveIdentity struct {
	Home             string `json:"home"`
	DatabaseIdentity string `json:"database_identity"`
	LatestVersion    int64  `json:"latest_version"`
	FinalCommitHash  string `json:"final_commit_hash"`
}

type objectHeaders struct {
	Metadata             []byte
	CacheControl         string
	ContentDisposition   string
	ContentEncoding      string
	ContentLanguage      string
	ContentType          string
	WebsiteRedirect      string
	StorageClass         string
	ServerSideEncryption string
}

type packRecord struct {
	Schema          uint64
	Ordinal         uint64
	Height          uint64
	Key             string
	OldETag         string
	OldBody         []byte
	NewBody         []byte
	OldSHA256       common.Hash
	NewSHA256       common.Hash
	Headers         objectHeaders
	RetainedSHA256  common.Hash
	SlotsAdded      uint64
	SlotsRemoved    uint64
	SlotsChanged    uint64
	NoncanonicalOld bool
	ConflictingOld  bool
}

type retainedFields struct {
	Hash            common.Hash
	ParentHash      common.Hash
	NewAccounts     []dtypes.NewAccount
	DeletedAccounts []common.Hash
	NewCodes        []dtypes.NewCode
}

type storageKey struct {
	Address common.Hash
	Index   common.Hash
}

type normalizedStorage struct {
	Values       map[storageKey][32]byte
	Noncanonical bool
	Conflict     bool
}

type slotStats struct {
	Added   uint64
	Removed uint64
	Changed uint64
}

func normalizeStorage(storage []dtypes.AccountStorageDiff) (normalizedStorage, error) {
	result := normalizedStorage{Values: make(map[storageKey][32]byte)}
	addresses := make(map[common.Hash]struct{})
	var previousAddress common.Hash
	for accountIndex, account := range storage {
		if accountIndex > 0 && bytes.Compare(previousAddress[:], account.Address[:]) >= 0 {
			result.Noncanonical = true
		}
		previousAddress = account.Address
		if len(account.Values) == 0 {
			result.Noncanonical = true
		}
		if _, ok := addresses[account.Address]; ok {
			result.Noncanonical = true
		}
		addresses[account.Address] = struct{}{}
		var previousIndex common.Hash
		for valueIndex, pair := range account.Values {
			if valueIndex > 0 && bytes.Compare(previousIndex[:], pair.Index[:]) >= 0 {
				result.Noncanonical = true
			}
			previousIndex = pair.Index
			if pair.Value == nil {
				return result, fmt.Errorf("storage account %d value %d is nil", accountIndex, valueIndex)
			}
			key := storageKey{Address: account.Address, Index: pair.Index}
			var value [32]byte
			pair.Value.WriteToSlice(value[:])
			if previous, ok := result.Values[key]; ok {
				result.Noncanonical = true
				if previous != value {
					result.Conflict = true
				}
				continue
			}
			result.Values[key] = value
		}
	}
	return result, nil
}

func compareStorage(oldStorage, canonical []dtypes.AccountStorageDiff) (bool, bool, bool, slotStats, error) {
	oldValues, err := normalizeStorage(oldStorage)
	if err != nil {
		return false, false, false, slotStats{}, err
	}
	newValues, err := normalizeStorage(canonical)
	if err != nil {
		return false, false, false, slotStats{}, err
	}
	if newValues.Noncanonical {
		return false, false, false, slotStats{}, fmt.Errorf("canonical storage is noncanonical")
	}
	var stats slotStats
	for key, newValue := range newValues.Values {
		oldValue, ok := oldValues.Values[key]
		switch {
		case !ok:
			stats.Added++
		case oldValue != newValue:
			stats.Changed++
		}
	}
	for key := range oldValues.Values {
		if _, ok := newValues.Values[key]; !ok {
			stats.Removed++
		}
	}
	equal := !oldValues.Noncanonical && stats == (slotStats{})
	return equal, oldValues.Noncanonical, oldValues.Conflict, stats, nil
}

func retainedDigest(diff dtypes.BlockStorageDiff) (common.Hash, error) {
	body, err := rlp.EncodeToBytes(retainedFields{
		Hash: diff.Hash, ParentHash: diff.ParentHash, NewAccounts: diff.NewAccounts,
		DeletedAccounts: diff.DeletedAccounts, NewCodes: diff.NewCodes,
	})
	if err != nil {
		return common.Hash{}, err
	}
	return sha256Hash(body), nil
}

func sha256Hash(body []byte) common.Hash {
	sum := sha256.Sum256(body)
	return common.BytesToHash(sum[:])
}

func sha256Hex(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func canonicalMetadata(metadata map[string]string) ([]byte, error) {
	keys := make([]string, 0, len(metadata))
	for key := range metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	ordered := make([][2]string, 0, len(keys))
	for _, key := range keys {
		ordered = append(ordered, [2]string{key, metadata[key]})
	}
	return json.Marshal(ordered)
}

func decodeMetadata(body []byte) (map[string]string, error) {
	var ordered [][2]string
	if err := json.Unmarshal(body, &ordered); err != nil {
		return nil, err
	}
	result := make(map[string]string, len(ordered))
	for _, item := range ordered {
		if _, ok := result[item[0]]; ok {
			return nil, fmt.Errorf("duplicate metadata key %q", item[0])
		}
		result[item[0]] = item[1]
	}
	return result, nil
}
