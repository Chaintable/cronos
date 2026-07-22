package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/cosmos/iavl"
)

const (
	legacyCoverageSelf byte = iota
	legacyCoverageRoot
	legacyCoverageLeft
	legacyCoverageRight

	legacyCoverageRecordLength = legacyHashLength + 1 + 8 + 8
)

// legacyCoverageRecord describes either a node's complete lifetime (self), or
// one root/parent reference to that node. A valid persistent tree has exactly
// one incoming reference for every saved version in the node's lifetime.
type legacyCoverageRecord struct {
	Hash        []byte
	Kind        byte
	FromVersion int64
	ToVersion   int64
}

func encodeLegacyCoverage(record legacyCoverageRecord) ([]byte, error) {
	if len(record.Hash) != legacyHashLength || record.FromVersion < 1 ||
		record.ToVersion < record.FromVersion || record.Kind > legacyCoverageRight {
		return nil, fmt.Errorf("invalid legacy coverage record")
	}
	body := make([]byte, legacyCoverageRecordLength)
	copy(body, record.Hash)
	body[legacyHashLength] = record.Kind
	binary.BigEndian.PutUint64(body[legacyHashLength+1:], uint64(record.FromVersion))
	binary.BigEndian.PutUint64(body[legacyHashLength+9:], uint64(record.ToVersion))
	return body, nil
}

func decodeLegacyCoverage(body []byte) (legacyCoverageRecord, error) {
	if len(body) != legacyCoverageRecordLength {
		return legacyCoverageRecord{}, fmt.Errorf(
			"legacy coverage has length %d, want %d", len(body), legacyCoverageRecordLength,
		)
	}
	record := legacyCoverageRecord{
		Hash:        append([]byte(nil), body[:legacyHashLength]...),
		Kind:        body[legacyHashLength],
		FromVersion: int64(binary.BigEndian.Uint64(body[legacyHashLength+1 : legacyHashLength+9])),
		ToVersion:   int64(binary.BigEndian.Uint64(body[legacyHashLength+9:])),
	}
	if record.Kind > legacyCoverageRight || record.FromVersion < 1 || record.ToVersion < record.FromVersion {
		return legacyCoverageRecord{}, fmt.Errorf("invalid legacy coverage lifetime or kind")
	}
	return record, nil
}

func legacyCoverageLess(left, right []byte) bool {
	if order := bytes.Compare(left[:legacyHashLength], right[:legacyHashLength]); order != 0 {
		return order < 0
	}
	leftKind, rightKind := left[legacyHashLength], right[legacyHashLength]
	if leftKind == legacyCoverageSelf || rightKind == legacyCoverageSelf {
		return leftKind < rightKind
	}
	if order := bytes.Compare(left[legacyHashLength+1:], right[legacyHashLength+1:]); order != 0 {
		return order < 0
	}
	return leftKind < rightKind
}

func validateLegacyCoverage(
	ctx context.Context,
	source legacyRecordSource,
	coverage byteRecordReader,
	expectedNodes int64,
) (int64, error) {
	coverageBody, err := nextOptionalRecord(coverage)
	if err != nil {
		return 0, err
	}
	var previousHash []byte
	var checked int64
	err = source.InspectNodes(func(node iavl.LegacyNodeRecord) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if previousHash != nil && bytes.Compare(previousHash, node.Hash) >= 0 {
			return fmt.Errorf("legacy graph node hashes are not strictly increasing at %x", node.Hash)
		}
		previousHash = append(previousHash[:0], node.Hash...)
		if coverageBody == nil {
			return fmt.Errorf("legacy node %x has no reachability record", node.Hash)
		}
		self, err := decodeLegacyCoverage(coverageBody)
		if err != nil {
			return err
		}
		if order := bytes.Compare(self.Hash, node.Hash); order != 0 {
			if order < 0 {
				return fmt.Errorf("legacy reachability references missing node %x", self.Hash)
			}
			return fmt.Errorf("legacy node %x has no reachability record", node.Hash)
		}
		if self.Kind != legacyCoverageSelf || self.FromVersion != node.Version {
			return fmt.Errorf("legacy node %x has invalid self lifetime", node.Hash)
		}
		coverageBody, err = nextOptionalRecord(coverage)
		if err != nil {
			return err
		}
		coveredTo := int64(0)
		for coverageBody != nil {
			record, decodeErr := decodeLegacyCoverage(coverageBody)
			if decodeErr != nil {
				return decodeErr
			}
			if !bytes.Equal(record.Hash, node.Hash) {
				break
			}
			if record.Kind == legacyCoverageSelf {
				return fmt.Errorf("legacy node %x has duplicate self lifetimes", node.Hash)
			}
			if record.FromVersion < node.Version || record.ToVersion > self.ToVersion {
				return fmt.Errorf(
					"legacy node %x coverage %d-%d is outside lifetime %d-%d",
					node.Hash, record.FromVersion, record.ToVersion, node.Version, self.ToVersion,
				)
			}
			if coveredTo == 0 {
				if record.FromVersion != node.Version {
					return fmt.Errorf(
						"legacy node %x reachability starts at %d, want %d",
						node.Hash, record.FromVersion, node.Version,
					)
				}
			} else if record.FromVersion != coveredTo+1 {
				if record.FromVersion <= coveredTo {
					return fmt.Errorf(
						"legacy node %x has overlapping reachability at version %d",
						node.Hash, record.FromVersion,
					)
				}
				return fmt.Errorf("legacy node %x has reachability gap after version %d", node.Hash, coveredTo)
			}
			coveredTo = record.ToVersion
			coverageBody, err = nextOptionalRecord(coverage)
			if err != nil {
				return err
			}
		}
		if coveredTo != self.ToVersion {
			return fmt.Errorf(
				"legacy node %x reachability ends at %d, want %d", node.Hash, coveredTo, self.ToVersion,
			)
		}
		checked++
		return nil
	})
	if err != nil {
		return checked, fmt.Errorf("validate legacy graph reachability: %w", err)
	}
	if coverageBody != nil {
		record, err := decodeLegacyCoverage(coverageBody)
		if err != nil {
			return checked, err
		}
		return checked, fmt.Errorf("legacy reachability references missing node %x", record.Hash)
	}
	if checked != expectedNodes {
		return checked, fmt.Errorf("legacy graph checked %d nodes, want %d", checked, expectedNodes)
	}
	return checked, nil
}
