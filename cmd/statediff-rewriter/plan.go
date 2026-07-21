package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	storetypes "cosmossdk.io/store/types"
	"github.com/cosmos/iavl"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"

	"github.com/cosmos/cosmos-sdk/version"
	"github.com/evmos/ethermint/debank/statediff"
	dtypes "github.com/evmos/ethermint/debank/types"
)

type planOptions struct {
	DumpStaging string
	ArchiveHome string
	Output      string
	Bucket      string
	Prefix      string
	Region      string
	MinFree     uint64
	SnapshotID  string
	ImageDigest string
}

func runPlan(ctx context.Context, options planOptions, objects objectStore) (planManifest, string, error) {
	archive, err := openArchive(options.ArchiveHome)
	if err != nil {
		return planManifest{}, "", err
	}
	defer archive.Close()
	identity, err := archive.identity()
	if err != nil {
		return planManifest{}, "", err
	}
	cronosCommit, ethermintCommit := buildCommits()
	sealedDump, dumpInfo, dumpHash, err := prepareDump(options.DumpStaging, dumpContext{
		SnapshotID: options.SnapshotID, ArchiveIdentity: identity, CronosCommit: cronosCommit,
		EthermintCommit: ethermintCommit, ImageDigest: options.ImageDigest, BuildTags: version.BuildTags,
	})
	if err != nil {
		return planManifest{}, "", err
	}
	if dumpInfo.LastVersion != identity.LatestVersion {
		return planManifest{}, "", fmt.Errorf("dump final version %d differs from archive %d", dumpInfo.LastVersion, identity.LatestVersion)
	}
	if !strings.HasSuffix(filepath.Clean(options.Output), ".staging") {
		return planManifest{}, "", fmt.Errorf("plan output must end in .staging")
	}
	if err := os.Mkdir(options.Output, 0o755); err != nil {
		return planManifest{}, "", err
	}
	writer, err := newPackWriter(options.Output, chunkSize)
	if err != nil {
		return planManifest{}, "", err
	}
	rootIndexPath := filepath.Join(options.Output, "roots.raw.tmp")
	rootIndex, err := os.OpenFile(rootIndexPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return planManifest{}, "", err
	}

	manifest := planManifest{
		Schema: manifestSchema, Sealed: true, RunID: fmt.Sprintf("bugb-%d", time.Now().UTC().UnixNano()),
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano), Bucket: options.Bucket, Prefix: strings.TrimSuffix(options.Prefix, "/"), Region: options.Region,
		FirstHeight: 2, FinalHeight: identity.LatestVersion, CronosCommit: cronosCommit, EthermintCommit: ethermintCommit,
		DumpPath: sealedDump, DumpManifestHash: dumpHash, ArchiveIdentity: identity,
		SnapshotID: options.SnapshotID, ImageDigest: options.ImageDigest, BuildTags: version.BuildTags,
	}
	var ordinal uint64
	err = iterateSealedDump(sealedDump, dumpInfo, func(height int64, changeSet *iavl.ChangeSet) error {
		if height == 1 {
			return nil
		}
		if err := ensureFreeSpace(options.Output, options.MinFree); err != nil {
			return err
		}
		canonical, err := statediff.CanonicalStorageDiff(changeSet)
		if err != nil {
			return fmt.Errorf("height %d canonical storage: %w", height, err)
		}
		root, parent, err := archiveRoots(archive, height)
		if err != nil {
			return err
		}
		if err := writeRootRecord(rootIndex, root, uint64(height)); err != nil {
			return err
		}
		manifest.Processed++
		if root == parent {
			if len(canonical) != 0 {
				return fmt.Errorf("height %d has storage changes behind an equal-root short circuit", height)
			}
			manifest.SkippedEqualRoot++
			return nil
		}
		key := fmt.Sprintf("%s/%s/stateDiff", manifest.Prefix, strings.ToLower(root.Hex()))
		object, err := objects.Get(ctx, manifest.Bucket, key)
		if err != nil {
			return fmt.Errorf("get height %d key %s: %w", height, key, err)
		}
		manifest.OldBytes += int64(len(object.Body))
		record, changed, err := makePackRecord(uint64(height), key, object, root, parent, canonical)
		if err != nil {
			return fmt.Errorf("analyze height %d: %w", height, err)
		}
		if !changed {
			manifest.Unchanged++
			return nil
		}
		ordinal++
		record.Ordinal = ordinal
		if err := writer.Write(record); err != nil {
			return err
		}
		manifest.Changed++
		manifest.NewBytes += int64(len(record.NewBody))
		if record.NoncanonicalOld {
			manifest.ChangedCanonical++
		}
		if record.ConflictingOld {
			manifest.ChangedConflict++
		}
		return nil
	})
	if err != nil {
		_ = rootIndex.Close()
		return planManifest{}, "", err
	}
	if err := rootIndex.Sync(); err != nil {
		_ = rootIndex.Close()
		return planManifest{}, "", err
	}
	if err := rootIndex.Close(); err != nil {
		return planManifest{}, "", err
	}
	rootStat, err := os.Stat(rootIndexPath)
	if err != nil {
		return planManifest{}, "", err
	}
	if err := ensureFreeSpace(options.Output, options.MinFree+uint64(rootStat.Size())*2); err != nil {
		return planManifest{}, "", fmt.Errorf("root index external sort: %w", err)
	}
	manifest.RootIndex, manifest.RootIndexSHA256, err = checkDuplicateRoots(rootIndexPath, options.Output)
	if err != nil {
		return planManifest{}, "", err
	}
	manifest.Chunks, err = writer.Close()
	if err != nil {
		return planManifest{}, "", err
	}
	finalIdentity, err := archive.identity()
	if err != nil {
		return planManifest{}, "", err
	}
	if finalIdentity != identity {
		return planManifest{}, "", fmt.Errorf("archive identity changed during plan")
	}
	body, err := atomicJSON(filepath.Join(options.Output, "manifest.v1.json"), manifest)
	if err != nil {
		return planManifest{}, "", err
	}
	manifestHash := sha256Hex(body)
	sealedPlan := strings.TrimSuffix(filepath.Clean(options.Output), ".staging") + ".sealed"
	if _, err := os.Stat(sealedPlan); err == nil {
		return planManifest{}, "", fmt.Errorf("sealed plan already exists: %s", sealedPlan)
	} else if !errors.Is(err, os.ErrNotExist) {
		return planManifest{}, "", err
	}
	if err := syncDir(options.Output); err != nil {
		return planManifest{}, "", err
	}
	if err := os.Rename(options.Output, sealedPlan); err != nil {
		return planManifest{}, "", err
	}
	if err := syncDir(filepath.Dir(sealedPlan)); err != nil {
		return planManifest{}, "", err
	}
	return manifest, filepath.Join(sealedPlan, "manifest.v1.json") + "#sha256=" + manifestHash, nil
}

func makePackRecord(height uint64, key string, object storedObject, root, parent common.Hash, canonical []dtypes.AccountStorageDiff) (packRecord, bool, error) {
	var old dtypes.BlockStorageDiff
	if err := rlp.DecodeBytes(object.Body, &old); err != nil {
		return packRecord{}, false, fmt.Errorf("decode state diff: %w", err)
	}
	if old.Hash != root || old.ParentHash != parent {
		return packRecord{}, false, fmt.Errorf("root mismatch: object %s/%s archive %s/%s", old.Hash, old.ParentHash, root, parent)
	}
	equal, noncanonical, conflict, stats, err := compareStorage(old.StorageDiff, canonical)
	if err != nil {
		return packRecord{}, false, err
	}
	if equal {
		return packRecord{}, false, nil
	}
	newDiff := old
	newDiff.StorageDiff = canonical
	newBody, err := rlp.EncodeToBytes(newDiff)
	if err != nil {
		return packRecord{}, false, err
	}
	if err := verifyReencoded(old, canonical, newBody); err != nil {
		return packRecord{}, false, err
	}
	retained, err := retainedDigest(old)
	if err != nil {
		return packRecord{}, false, err
	}
	return packRecord{
		Schema: packSchema, Height: height, Key: key, OldETag: object.ETag,
		OldBody: object.Body, NewBody: newBody, OldSHA256: sha256Hash(object.Body), NewSHA256: sha256Hash(newBody),
		Headers: object.Headers, RetainedSHA256: retained, SlotsAdded: stats.Added, SlotsRemoved: stats.Removed,
		SlotsChanged: stats.Changed, NoncanonicalOld: noncanonical, ConflictingOld: conflict,
	}, true, nil
}

func archiveRoots(archive *archiveReader, height int64) (common.Hash, common.Hash, error) {
	rootInfo, err := archive.commitInfo(height - 1)
	if err != nil {
		return common.Hash{}, common.Hash{}, err
	}
	root := common.BytesToHash(rootInfo.Hash())
	if height == 2 {
		return root, common.BytesToHash((storetypes.CommitInfo{}).Hash()), nil
	}
	parentInfo, err := archive.commitInfo(height - 2)
	if err != nil {
		return common.Hash{}, common.Hash{}, err
	}
	return root, common.BytesToHash(parentInfo.Hash()), nil
}

func verifyReencoded(old dtypes.BlockStorageDiff, canonical []dtypes.AccountStorageDiff, body []byte) error {
	var decoded dtypes.BlockStorageDiff
	if err := rlp.DecodeBytes(body, &decoded); err != nil {
		return err
	}
	equal, noncanonical, conflict, stats, err := compareStorage(decoded.StorageDiff, canonical)
	if err != nil {
		return err
	}
	if !equal || noncanonical || conflict || stats != (slotStats{}) {
		return fmt.Errorf("replacement storage is not canonical")
	}
	oldRetained, err := retainedDigest(old)
	if err != nil {
		return err
	}
	newRetained, err := retainedDigest(decoded)
	if err != nil {
		return err
	}
	if oldRetained != newRetained {
		return fmt.Errorf("replacement changed retained fields")
	}
	return nil
}

func ensureFreeSpace(path string, minimum uint64) error {
	if minimum == 0 {
		return fmt.Errorf("min-free-bytes must be greater than zero")
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return err
	}
	available := stat.Bavail * uint64(stat.Bsize)
	if available < minimum {
		return fmt.Errorf("filesystem free space %d is below min-free-bytes %d", available, minimum)
	}
	return nil
}

func buildCommits() (string, string) {
	cronosCommit := version.Commit
	ethermintCommit := "unknown"
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" && cronosCommit == "" {
				cronosCommit = setting.Value
			}
		}
		for _, dependency := range info.Deps {
			if dependency.Path == "github.com/evmos/ethermint" && dependency.Replace != nil {
				parts := strings.Split(dependency.Replace.Version, "-")
				if len(parts) > 0 {
					ethermintCommit = parts[len(parts)-1]
				}
			}
		}
	}
	if cronosCommit == "" {
		cronosCommit = "unknown"
	}
	return cronosCommit, ethermintCommit
}
