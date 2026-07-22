package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/cosmos/iavl"
	iavldb "github.com/cosmos/iavl/db"
)

const (
	defaultLegacySortChunkBytes = int64(512 << 20)
	defaultLegacySortMaxChunks  = 512
	legacyHashLength            = 32
	legacyOrphanRecordLength    = legacyHashLength + 8 + 8
	legacyValidationGraph       = "graph"
	legacyValidationTrustedSet  = "trusted-node-set"
)

type legacyScanOptions struct {
	TempDir      string
	FirstVersion int64
	LastVersion  int64

	SortChunkSize int64
	MaxSortChunks int
	MinFree       uint64
	TrustNodeSet  bool
}

type legacyScanReport struct {
	LegacyFirstVersion int64
	LegacyLastVersion  int64
	Roots              int64
	Nodes              int64
	Leaves             int64
	StorageLeaves      int64
	Orphans            int64
	StorageOrphans     int64
	Branches           int64
	GraphNodes         int64
	CoverageRecords    int64
	Validation         string
}

type legacyRecordSource interface {
	InspectRoots(func(iavl.LegacyRootRecord) error) error
	InspectNodes(func(iavl.LegacyNodeRecord) error) error
	InspectOrphans(func(iavl.LegacyOrphanRecord) error) error
}

type iavlLegacyRecordSource struct{ db iavldb.DB }

func (source iavlLegacyRecordSource) InspectRoots(callback func(iavl.LegacyRootRecord) error) error {
	return iavl.InspectLegacyRoots(source.db, callback)
}

func (source iavlLegacyRecordSource) InspectNodes(callback func(iavl.LegacyNodeRecord) error) error {
	return iavl.InspectLegacyNodes(source.db, callback)
}

func (source iavlLegacyRecordSource) InspectOrphans(callback func(iavl.LegacyOrphanRecord) error) error {
	return iavl.InspectLegacyOrphans(source.db, callback)
}

func legacyLatestRootVersion(db iavldb.DB) (returnVersion int64, returnErr error) {
	if db == nil {
		return 0, fmt.Errorf("legacy database is required")
	}
	iterator, err := db.ReverseIterator([]byte{'r'}, []byte{'s'})
	if err != nil {
		return 0, fmt.Errorf("open legacy root iterator: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, iterator.Close()) }()
	if !iterator.Valid() {
		if err := iterator.Error(); err != nil {
			return 0, fmt.Errorf("read legacy root iterator: %w", err)
		}
		return 0, nil
	}
	key, value := iterator.Key(), iterator.Value()
	if len(key) != 9 || key[0] != 'r' {
		return 0, fmt.Errorf("invalid latest legacy root key %x", key)
	}
	version := int64(binary.BigEndian.Uint64(key[1:]))
	if version < 1 {
		return 0, fmt.Errorf("invalid latest legacy root version %d", version)
	}
	if len(value) != 0 && len(value) != legacyHashLength {
		return 0, fmt.Errorf("latest legacy root %d has hash length %d", version, len(value))
	}
	return version, nil
}

func scanLegacyChangeSets(
	ctx context.Context,
	source legacyRecordSource,
	options legacyScanOptions,
	emit func(int64, *iavl.ChangeSet) error,
) (report legacyScanReport, returnErr error) {
	if source == nil {
		return report, fmt.Errorf("legacy record source is required")
	}
	if options.FirstVersion < 1 || options.LastVersion < options.FirstVersion {
		return report, fmt.Errorf("invalid legacy scan range %d-%d", options.FirstVersion, options.LastVersion)
	}
	if options.TempDir == "" {
		return report, fmt.Errorf("legacy scan temporary directory is required")
	}
	if options.SortChunkSize < externalRecordMemoryBytes {
		return report, fmt.Errorf("legacy sort chunk size must be at least %d", externalRecordMemoryBytes)
	}
	if options.MaxSortChunks < 2 {
		return report, fmt.Errorf("legacy max sort chunks must be at least 2")
	}
	if emit == nil {
		return report, fmt.Errorf("legacy changeset callback is required")
	}
	workDir, err := os.MkdirTemp(options.TempDir, ".statediff-legacy-*")
	if err != nil {
		return report, fmt.Errorf("create legacy scan directory: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, removeLegacyWorkDir(workDir))
	}()

	deltaSorter, err := newExternalSorterWithContext(
		ctx, workDir, options.SortChunkSize, options.MaxSortChunks, options.MinFree, legacyDeltaLess,
	)
	if err != nil {
		return report, err
	}
	defer func() { returnErr = errors.Join(returnErr, deltaSorter.Close()) }()
	orphanSorter, err := newExternalSorterWithContext(
		ctx, workDir, options.SortChunkSize, options.MaxSortChunks, options.MinFree, legacyOrphanLess,
	)
	if err != nil {
		return report, err
	}
	defer func() { returnErr = errors.Join(returnErr, orphanSorter.Close()) }()
	var coverageSorter *externalSorter
	if !options.TrustNodeSet {
		coverageSorter, err = newExternalSorterWithContext(
			ctx, workDir, options.SortChunkSize, options.MaxSortChunks, options.MinFree, legacyCoverageLess,
		)
		if err != nil {
			return report, err
		}
		defer func() { returnErr = errors.Join(returnErr, coverageSorter.Close()) }()
	}
	report.Validation = legacyValidationGraph
	if options.TrustNodeSet {
		report.Validation = legacyValidationTrustedSet
	}
	expectedRoot := int64(1)
	err = source.InspectRoots(func(root iavl.LegacyRootRecord) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if root.Version != expectedRoot {
			return fmt.Errorf("legacy root version %d, want %d", root.Version, expectedRoot)
		}
		if report.Roots == 0 {
			report.LegacyFirstVersion = root.Version
		}
		report.LegacyLastVersion = root.Version
		report.Roots++
		expectedRoot++
		if len(root.Hash) == 0 {
			return nil
		}
		if len(root.Hash) != legacyHashLength {
			return fmt.Errorf("legacy root %d has hash length %d", root.Version, len(root.Hash))
		}
		if coverageSorter == nil {
			return nil
		}
		body, err := encodeLegacyCoverage(legacyCoverageRecord{
			Hash: root.Hash, Kind: legacyCoverageRoot, FromVersion: root.Version, ToVersion: root.Version,
		})
		if err != nil {
			return err
		}
		report.CoverageRecords++
		return coverageSorter.Feed(body)
	})
	if err != nil {
		return report, fmt.Errorf("inspect legacy roots: %w", err)
	}
	if report.Roots == 0 {
		return report, fmt.Errorf("legacy root set is empty")
	}
	if options.LastVersion > report.LegacyLastVersion {
		return report, fmt.Errorf(
			"legacy scan ends at %d, beyond last legacy root %d",
			options.LastVersion, report.LegacyLastVersion,
		)
	}
	err = source.InspectOrphans(func(orphan iavl.LegacyOrphanRecord) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		report.Orphans++
		body, err := encodeLegacyOrphanRecord(orphan)
		if err != nil {
			return err
		}
		return orphanSorter.Feed(body)
	})
	if err != nil {
		return report, fmt.Errorf("inspect legacy orphans: %w", err)
	}
	orphanReader, err := orphanSorter.Finalize()
	if err != nil {
		return report, fmt.Errorf("finalize legacy orphan sort: %w", err)
	}
	if err := scanLegacyNodesWithOrphans(
		ctx, source, orphanReader, report.LegacyLastVersion,
		options.FirstVersion, options.LastVersion, deltaSorter, &report,
		coverageSorter,
	); err != nil {
		return report, err
	}
	if err := orphanReader.Close(); err != nil {
		return report, err
	}
	if coverageSorter != nil {
		coverageReader, err := coverageSorter.Finalize()
		if err != nil {
			return report, fmt.Errorf("finalize legacy reachability sort: %w", err)
		}
		report.GraphNodes, err = validateLegacyCoverage(ctx, source, coverageReader, report.Nodes)
		if err != nil {
			return report, err
		}
		if err := coverageReader.Close(); err != nil {
			return report, err
		}
	}

	deltaReader, err := deltaSorter.Finalize()
	if err != nil {
		return report, fmt.Errorf("finalize legacy delta sort: %w", err)
	}
	if err := reduceLegacyDeltas(ctx, deltaReader, options.FirstVersion, options.LastVersion, emit); err != nil {
		return report, fmt.Errorf("reduce legacy storage deltas: %w", err)
	}
	if err := deltaReader.Close(); err != nil {
		return report, err
	}
	return report, nil
}

func removeLegacyWorkDir(path string) error {
	if path == "" || filepath.Base(path) == "." || filepath.Base(path) == string(filepath.Separator) {
		return fmt.Errorf("refuse to remove invalid legacy work directory %q", path)
	}
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("remove legacy work directory: %w", err)
	}
	return nil
}

type legacyOrphanRecord struct {
	Hash        []byte
	ToVersion   int64
	FromVersion int64
}

func encodeLegacyOrphanRecord(orphan iavl.LegacyOrphanRecord) ([]byte, error) {
	if len(orphan.Hash) != legacyHashLength || orphan.FromVersion < 1 || orphan.ToVersion < orphan.FromVersion {
		return nil, fmt.Errorf(
			"invalid legacy orphan %d-%d hash length %d",
			orphan.FromVersion, orphan.ToVersion, len(orphan.Hash),
		)
	}
	body := make([]byte, legacyOrphanRecordLength)
	copy(body[:legacyHashLength], orphan.Hash)
	binary.BigEndian.PutUint64(body[legacyHashLength:legacyHashLength+8], uint64(orphan.ToVersion))
	binary.BigEndian.PutUint64(body[legacyHashLength+8:], uint64(orphan.FromVersion))
	return body, nil
}

func decodeLegacyOrphanRecord(body []byte) (legacyOrphanRecord, error) {
	if len(body) != legacyOrphanRecordLength {
		return legacyOrphanRecord{}, fmt.Errorf("legacy orphan record has length %d, want %d", len(body), legacyOrphanRecordLength)
	}
	record := legacyOrphanRecord{
		Hash:        append([]byte(nil), body[:legacyHashLength]...),
		ToVersion:   int64(binary.BigEndian.Uint64(body[legacyHashLength : legacyHashLength+8])),
		FromVersion: int64(binary.BigEndian.Uint64(body[legacyHashLength+8:])),
	}
	if record.FromVersion < 1 || record.ToVersion < record.FromVersion {
		return legacyOrphanRecord{}, fmt.Errorf("invalid legacy orphan lifetime %d-%d", record.FromVersion, record.ToVersion)
	}
	return record, nil
}

func legacyOrphanLess(left, right []byte) bool { return bytes.Compare(left, right) < 0 }

func scanLegacyNodesWithOrphans(
	ctx context.Context,
	source legacyRecordSource,
	orphans byteRecordReader,
	legacyLast, firstVersion, lastVersion int64,
	deltas *externalSorter,
	report *legacyScanReport,
	coverage *externalSorter,
) error {
	orphanBody, err := nextOptionalRecord(orphans)
	if err != nil {
		return err
	}
	var previousOrphanHash []byte
	advanceOrphan := func() error {
		if orphanBody != nil {
			orphan, decodeErr := decodeLegacyOrphanRecord(orphanBody)
			if decodeErr != nil {
				return decodeErr
			}
			if previousOrphanHash != nil && bytes.Equal(previousOrphanHash, orphan.Hash) {
				return fmt.Errorf("legacy node hash %x has multiple orphan lifetimes", orphan.Hash)
			}
			previousOrphanHash = append(previousOrphanHash[:0], orphan.Hash...)
		}
		var readErr error
		orphanBody, readErr = nextOptionalRecord(orphans)
		if readErr == nil && orphanBody != nil {
			next, decodeErr := decodeLegacyOrphanRecord(orphanBody)
			if decodeErr != nil {
				return decodeErr
			}
			if bytes.Equal(previousOrphanHash, next.Hash) {
				return fmt.Errorf("legacy node hash %x has multiple orphan lifetimes", previousOrphanHash)
			}
		}
		return readErr
	}

	var previousNodeHash []byte
	err = source.InspectNodes(func(node iavl.LegacyNodeRecord) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		report.Nodes++
		if len(node.Hash) != legacyHashLength {
			return fmt.Errorf("legacy node has hash length %d", len(node.Hash))
		}
		if previousNodeHash != nil && bytes.Compare(previousNodeHash, node.Hash) >= 0 {
			return fmt.Errorf("legacy node hashes are not strictly increasing at %x", node.Hash)
		}
		previousNodeHash = append(previousNodeHash[:0], node.Hash...)
		if node.Version < 1 || node.Version > legacyLast {
			return fmt.Errorf("legacy node %x has version %d outside 1-%d", node.Hash, node.Version, legacyLast)
		}
		lifetimeEnd := legacyLast
		if orphanBody != nil {
			orphan, decodeErr := decodeLegacyOrphanRecord(orphanBody)
			if decodeErr != nil {
				return decodeErr
			}
			switch bytes.Compare(orphan.Hash, node.Hash) {
			case -1:
				return fmt.Errorf("legacy orphan %x references a missing node", orphan.Hash)
			case 0:
				if orphan.FromVersion != node.Version {
					return fmt.Errorf(
						"legacy node %x was created at %d but orphan lifetime starts at %d",
						node.Hash, node.Version, orphan.FromVersion,
					)
				}
				if orphan.ToVersion >= legacyLast {
					return fmt.Errorf(
						"legacy node %x orphan ends at %d, not before latest legacy root %d",
						node.Hash, orphan.ToVersion, legacyLast,
					)
				}
				lifetimeEnd = orphan.ToVersion
				if node.IsLeaf() && len(node.Key) > 0 && node.Key[0] == 0x02 {
					if len(node.Key) != legacyStorageKeyLength {
						return fmt.Errorf("legacy storage node %x has key length %d", node.Hash, len(node.Key))
					}
					report.StorageOrphans++
					deleteVersion := orphan.ToVersion + 1
					if deleteVersion >= firstVersion && deleteVersion <= lastVersion {
						delta, encodeErr := encodeLegacyDelta(legacyDelta{
							Version: deleteVersion, Key: node.Key, Kind: legacyDeltaOld,
						})
						if encodeErr != nil {
							return encodeErr
						}
						if feedErr := deltas.Feed(delta); feedErr != nil {
							return feedErr
						}
					}
				}
				if err := advanceOrphan(); err != nil {
					return err
				}
			}
		}
		if coverage != nil {
			self, encodeErr := encodeLegacyCoverage(legacyCoverageRecord{
				Hash: node.Hash, Kind: legacyCoverageSelf, FromVersion: node.Version, ToVersion: lifetimeEnd,
			})
			if encodeErr != nil {
				return encodeErr
			}
			if err := coverage.Feed(self); err != nil {
				return err
			}
			report.CoverageRecords++
		}

		if !node.IsLeaf() {
			report.Branches++
			if coverage == nil {
				return nil
			}
			children := []struct {
				hash []byte
				kind byte
			}{{node.LeftHash, legacyCoverageLeft}, {node.RightHash, legacyCoverageRight}}
			for _, child := range children {
				record, err := encodeLegacyCoverage(legacyCoverageRecord{
					Hash: child.hash, Kind: child.kind, FromVersion: node.Version, ToVersion: lifetimeEnd,
				})
				if err != nil {
					return err
				}
				if err := coverage.Feed(record); err != nil {
					return err
				}
				report.CoverageRecords++
			}
			return nil
		}
		report.Leaves++
		if len(node.Key) == 0 || node.Key[0] != 0x02 {
			return nil
		}
		if len(node.Key) != legacyStorageKeyLength {
			return fmt.Errorf("legacy storage node %x has key length %d", node.Hash, len(node.Key))
		}
		if len(node.Value) < 1 || len(node.Value) > 32 {
			return fmt.Errorf("legacy storage node %x has value length %d", node.Hash, len(node.Value))
		}
		report.StorageLeaves++
		if node.Version < firstVersion || node.Version > lastVersion {
			return nil
		}
		delta, encodeErr := encodeLegacyDelta(legacyDelta{
			Version: node.Version, Key: node.Key, Kind: legacyDeltaNew, Value: node.Value,
		})
		if encodeErr != nil {
			return encodeErr
		}
		return deltas.Feed(delta)
	})
	if err != nil {
		return fmt.Errorf("inspect legacy nodes: %w", err)
	}
	if orphanBody != nil {
		orphan, err := decodeLegacyOrphanRecord(orphanBody)
		if err != nil {
			return err
		}
		return fmt.Errorf("legacy orphan %x references a missing node", orphan.Hash)
	}
	return nil
}

func nextOptionalRecord(reader byteRecordReader) ([]byte, error) {
	body, err := reader.Next()
	if errors.Is(err, io.EOF) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if body == nil {
		return nil, fmt.Errorf("record reader returned nil without EOF")
	}
	return body, nil
}
