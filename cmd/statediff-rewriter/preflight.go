package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	dtypes "github.com/evmos/ethermint/debank/types"
)

func validatePlanForWriteContext(ctx context.Context, dir string, manifest planManifest) error {
	if ctx == nil {
		return fmt.Errorf("plan preflight context is required")
	}
	if err := validateSortedRootsContext(ctx, filepath.Join(dir, manifest.RootIndex), manifest); err != nil {
		return err
	}
	roots, err := newHeightRootStream(filepath.Join(dir, manifest.HeightRootIndex), uint64(manifest.FirstHeight))
	if err != nil {
		return err
	}
	defer roots.Close()

	stream := newPackStream(dir, manifest)
	defer stream.Close()
	var currentHeight uint64
	var currentRoot, parentRoot common.Hash
	var slotsAdded, slotsRemoved, slotsChanged uint64
	var oldBytes, newBytes, changedCanonical, changedConflict int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		record, ok, err := stream.Next()
		if err != nil {
			return fmt.Errorf("preflight pack: %w", err)
		}
		if !ok {
			break
		}
		for currentHeight < record.Height {
			root, err := roots.Next()
			if err != nil {
				return fmt.Errorf("preflight roots for height %d: %w", record.Height, err)
			}
			currentHeight = root.Height
			parentRoot, currentRoot = currentRoot, root.Root
		}
		if currentHeight != record.Height {
			return fmt.Errorf("pack height %d precedes root height %d", record.Height, currentHeight)
		}
		if currentRoot == parentRoot {
			return fmt.Errorf("pack height %d uses an equal-root object key", record.Height)
		}
		wantKey := fmt.Sprintf("%s/%s/stateDiff", manifest.Prefix, strings.ToLower(currentRoot.Hex()))
		if record.Key != wantKey {
			return fmt.Errorf("pack height %d key %s does not match root key %s", record.Height, record.Key, wantKey)
		}
		if record.OldETag == "" || record.OldSHA256 == record.NewSHA256 {
			return fmt.Errorf("pack record %d has an empty old ETag or equal old/new body", record.Ordinal)
		}
		if err := validatePackRecordBodies(record, currentRoot, parentRoot); err != nil {
			return fmt.Errorf("preflight record %d: %w", record.Ordinal, err)
		}
		if slotsAdded+record.SlotsAdded < slotsAdded || slotsRemoved+record.SlotsRemoved < slotsRemoved ||
			slotsChanged+record.SlotsChanged < slotsChanged {
			return fmt.Errorf("preflight pack slot counters overflow")
		}
		slotsAdded += record.SlotsAdded
		slotsRemoved += record.SlotsRemoved
		slotsChanged += record.SlotsChanged
		oldBytes += int64(len(record.OldBody))
		newBytes += int64(len(record.NewBody))
		if record.NoncanonicalOld {
			changedCanonical++
		}
		if record.ConflictingOld {
			changedConflict++
		}
	}
	for currentHeight < uint64(manifest.FinalHeight) {
		if err := ctx.Err(); err != nil {
			return err
		}
		root, err := roots.Next()
		if err != nil {
			return fmt.Errorf("finish preflight roots: %w", err)
		}
		currentHeight = root.Height
	}
	if err := roots.Finish(uint64(manifest.FinalHeight)); err != nil {
		return err
	}
	if manifest.SlotsAdded != slotsAdded || manifest.SlotsRemoved != slotsRemoved || manifest.SlotsChanged != slotsChanged ||
		manifest.NewBytes != newBytes || manifest.ChangedCanonical != changedCanonical || manifest.ChangedConflict != changedConflict {
		return fmt.Errorf("preflight pack statistics differ from manifest")
	}
	if manifest.OldBytes < oldBytes {
		return fmt.Errorf("preflight changed old bodies have %d bytes, manifest scanned %d", oldBytes, manifest.OldBytes)
	}
	return nil
}

func validatePackRecordBodies(record packRecord, root, parent common.Hash) error {
	if err := verifyRecordTarget(record, record.OldBody, record.OldSHA256); err != nil {
		return fmt.Errorf("old body: %w", err)
	}
	if err := verifyRecordTarget(record, record.NewBody, record.NewSHA256); err != nil {
		return fmt.Errorf("new body: %w", err)
	}
	if err := verifyRawRetainedFields(record.OldBody, record.NewBody); err != nil {
		return err
	}
	var oldDiff, newDiff dtypes.BlockStorageDiff
	if err := rlp.DecodeBytes(record.OldBody, &oldDiff); err != nil {
		return fmt.Errorf("decode old body: %w", err)
	}
	if err := rlp.DecodeBytes(record.NewBody, &newDiff); err != nil {
		return fmt.Errorf("decode new body: %w", err)
	}
	for label, diff := range map[string]dtypes.BlockStorageDiff{"old": oldDiff, "new": newDiff} {
		if diff.Hash != root || diff.ParentHash != parent {
			return fmt.Errorf("%s body root mismatch", label)
		}
	}
	if record.SlotsAdded != 0 || record.SlotsRemoved != 0 || record.SlotsChanged != 0 ||
		record.NoncanonicalOld || record.ConflictingOld {
		return fmt.Errorf("pack record contains disabled storage comparison statistics")
	}
	return nil
}

func validateSortedRootsContext(ctx context.Context, path string, manifest planManifest) error {
	if ctx == nil {
		return fmt.Errorf("validate sorted roots context is required")
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	reader := bufio.NewReaderSize(file, 1<<20)
	var previous rootRecord
	var count int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		record, err := readRootRecord(reader)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read sorted root index: %w", err)
		}
		if record.Height < uint64(manifest.FirstHeight) || record.Height > uint64(manifest.FinalHeight) {
			return fmt.Errorf("sorted root height %d is outside manifest", record.Height)
		}
		if count > 0 {
			if !rootLess(previous, record) {
				return fmt.Errorf("sorted root index is not strictly ordered")
			}
			if record.Root == previous.Root && record.Height != previous.Height+1 {
				return fmt.Errorf("state root %s is reused by non-adjacent heights %d and %d", record.Root, previous.Height, record.Height)
			}
		}
		previous = record
		count++
	}
	if count != manifest.Processed {
		return fmt.Errorf("sorted root index has %d records, want %d", count, manifest.Processed)
	}
	return nil
}
