package main

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cosmos/iavl"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/evmos/ethermint/debank/statediff"
)

func legacySampleVersions(first, last int64, count int) ([]int64, error) {
	if first < 1 || last < first || count < 1 {
		return nil, fmt.Errorf("invalid legacy sample range/count %d-%d/%d", first, last, count)
	}
	total := last - first + 1
	if int64(count) >= total {
		versions := make([]int64, 0, total)
		for version := first; version <= last; version++ {
			versions = append(versions, version)
		}
		return versions, nil
	}
	if count == 1 {
		return []int64{first}, nil
	}
	versions := make([]int64, 0, count)
	for index := 0; index < count; index++ {
		version := first + int64(index)*(total-1)/int64(count-1)
		if len(versions) == 0 || versions[len(versions)-1] != version {
			versions = append(versions, version)
		}
	}
	return versions, nil
}

func verifyLegacyDumpSamples(
	ctx context.Context,
	evmDir string,
	ranges []versionRange,
	reference stateChangeSource,
	count int,
) (int, error) {
	if len(ranges) == 0 || reference == nil {
		return 0, fmt.Errorf("legacy sample ranges and reference traverser are required")
	}
	versions, err := legacySampleVersions(ranges[0].First, ranges[len(ranges)-1].Last, count)
	if err != nil {
		return 0, err
	}
	wanted := make(map[int64]struct{}, len(versions))
	actual := make(map[int64][]byte, len(versions))
	for _, version := range versions {
		wanted[version] = struct{}{}
	}
	for _, blockRange := range ranges {
		_, err := scanZlibChangeSetsContext(ctx, dumpRangePath(evmDir, blockRange), func(version int64, changeSet *iavl.ChangeSet) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if _, ok := wanted[version]; !ok {
				return nil
			}
			canonical, err := statediff.CanonicalStorageDiff(changeSet)
			if err != nil {
				return fmt.Errorf("decode sequential legacy version %d: %w", version, err)
			}
			body, err := rlp.EncodeToBytes(canonical)
			if err != nil {
				return err
			}
			actual[version] = body
			return nil
		})
		if err != nil {
			return 0, err
		}
	}
	for _, version := range versions {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		actualBody, ok := actual[version]
		if !ok {
			return 0, fmt.Errorf("legacy sample version %d is missing from dump", version)
		}
		callbacks := 0
		var referenceBody []byte
		err := traverseVerifiedStateChanges(reference, version, version, func(gotVersion int64, changeSet *iavl.ChangeSet) error {
			callbacks++
			if callbacks != 1 || gotVersion != version {
				return fmt.Errorf("legacy reference returned version %d callback %d, want %d once", gotVersion, callbacks, version)
			}
			canonical, err := statediff.CanonicalStorageDiff(changeSet)
			if err != nil {
				return err
			}
			referenceBody, err = rlp.EncodeToBytes(canonical)
			return err
		})
		if err != nil {
			return 0, fmt.Errorf("legacy reference version %d: %w", version, err)
		}
		if callbacks != 1 {
			return 0, fmt.Errorf("legacy reference version %d invoked %d callbacks", version, callbacks)
		}
		if !bytes.Equal(actualBody, referenceBody) {
			return 0, fmt.Errorf("legacy sequential/reference storage mismatch at version %d", version)
		}
	}
	return len(versions), nil
}
