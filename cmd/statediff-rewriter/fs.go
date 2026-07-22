package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
)

type freeSpaceReserver struct {
	path     string
	minFree  uint64
	mu       sync.Mutex
	reserved uint64
}

type reservedWriter struct {
	writer   io.Writer
	reserver *freeSpaceReserver
}

var statFilesystem = syscall.Statfs

func filesystemSpace(path string) (uint64, uint64, error) {
	var stat syscall.Statfs_t
	if err := statFilesystem(path, &stat); err != nil {
		return 0, 0, err
	}
	if stat.Bsize <= 0 {
		return 0, 0, fmt.Errorf("filesystem block size must be positive")
	}
	blockSize := uint64(stat.Bsize)
	if stat.Bavail > math.MaxUint64/blockSize {
		return 0, 0, fmt.Errorf("filesystem free-space value overflows uint64")
	}
	return stat.Bavail * blockSize, blockSize, nil
}

func newFreeSpaceReserver(path string, minFree uint64) (*freeSpaceReserver, error) {
	if minFree == 0 {
		return nil, fmt.Errorf("min-free-bytes must be greater than zero")
	}
	return &freeSpaceReserver{path: path, minFree: minFree}, nil
}

func (reserver *freeSpaceReserver) reserve(bytes uint64) (func(), error) {
	reserver.mu.Lock()
	defer reserver.mu.Unlock()
	available, blockSize, err := filesystemSpace(reserver.path)
	if err != nil {
		return nil, err
	}
	blocks := bytes / blockSize
	if bytes%blockSize != 0 {
		blocks++
	}
	maxBlocks := math.MaxUint64 / blockSize
	if maxBlocks < 2 || blocks > maxBlocks-2 {
		return nil, fmt.Errorf("filesystem write reservation overflows uint64")
	}
	allocation := (blocks + 2) * blockSize
	if reserver.reserved > math.MaxUint64-allocation {
		return nil, fmt.Errorf("filesystem concurrent reservations overflow uint64")
	}
	totalReserved := reserver.reserved + allocation
	if reserver.minFree > math.MaxUint64-totalReserved {
		return nil, fmt.Errorf("filesystem free-space requirement overflows uint64")
	}
	required := reserver.minFree + totalReserved
	if available < required {
		return nil, fmt.Errorf("filesystem free space %d cannot preserve min-free-bytes %d with %d bytes reserved", available, reserver.minFree, totalReserved)
	}
	reserver.reserved = totalReserved
	return func() {
		reserver.mu.Lock()
		reserver.reserved -= allocation
		reserver.mu.Unlock()
	}, nil
}

func (writer reservedWriter) Write(body []byte) (int, error) {
	release, err := writer.reserver.reserve(uint64(len(body)))
	if err != nil {
		return 0, err
	}
	defer release()
	return writer.writer.Write(body)
}

func acquireStagingLocks(paths ...string) ([]*os.File, error) {
	unique := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		base := filepath.Clean(path)
		if strings.HasSuffix(base, ".staging") {
			base = strings.TrimSuffix(base, ".staging")
		} else if strings.HasSuffix(base, ".sealed") {
			base = strings.TrimSuffix(base, ".sealed")
		}
		unique[base+".lock"] = struct{}{}
	}
	lockPaths := make([]string, 0, len(unique))
	for path := range unique {
		lockPaths = append(lockPaths, path)
	}
	sort.Strings(lockPaths)
	locks := make([]*os.File, 0, len(lockPaths))
	for _, path := range lockPaths {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
		if err == nil {
			info, statErr := file.Stat()
			if statErr != nil {
				err = statErr
			} else {
				stat, ok := info.Sys().(*syscall.Stat_t)
				if !info.Mode().IsRegular() || !ok || stat.Nlink != 1 {
					err = fmt.Errorf("staging lock must be a regular file with one link")
				}
			}
		}
		if err == nil {
			err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		}
		if err != nil {
			if file != nil {
				_ = file.Close()
			}
			for _, lock := range locks {
				_ = lock.Close()
			}
			return nil, fmt.Errorf("acquire staging lock %s: %w", path, err)
		}
		locks = append(locks, file)
	}
	return locks, nil
}

func releaseStagingLocks(locks []*os.File) error {
	var errs []error
	for _, lock := range locks {
		errs = append(errs, lock.Close())
	}
	return errors.Join(errs...)
}

func openLockedPartial(path string) (*os.File, error) {
	if info, err := os.Lstat(path); err == nil {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !info.Mode().IsRegular() || !ok || stat.Nlink != 1 {
			return nil, fmt.Errorf("partial file must be a regular non-symlink file with one link: %s", path)
		}
		if err := os.Remove(path); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	closeWithError := func(cause error) (*os.File, error) {
		return nil, errors.Join(cause, file.Close())
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return closeWithError(fmt.Errorf("lock partial file %s: %w", path, err))
	}
	return file, nil
}

func commitFileNoReplace(partial, target string) error {
	if filepath.Dir(partial) != filepath.Dir(target) {
		return fmt.Errorf("partial and target files must share a directory")
	}
	if err := os.Link(partial, target); err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(target)); err != nil {
		return err
	}
	if err := os.Remove(partial); err != nil {
		return err
	}
	return syncDir(filepath.Dir(target))
}

type noReplacePartialState uint8

const (
	noReplaceMissing noReplacePartialState = iota
	noReplacePartialOnly
	noReplaceTargetOnly
	noReplaceLinkedBoth
)

func inspectNoReplacePartial(partial, target, label string) (noReplacePartialState, error) {
	partialInfo, partialErr := os.Lstat(partial)
	targetInfo, targetErr := os.Lstat(target)
	if partialErr != nil && !errors.Is(partialErr, os.ErrNotExist) {
		return noReplaceMissing, partialErr
	}
	if targetErr != nil && !errors.Is(targetErr, os.ErrNotExist) {
		return noReplaceMissing, targetErr
	}
	partialFound := partialErr == nil
	targetFound := targetErr == nil
	if !partialFound && !targetFound {
		return noReplaceMissing, nil
	}
	validate := func(path string, info os.FileInfo, wantLinks uint64) error {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !info.Mode().IsRegular() || !ok || uint64(stat.Nlink) != wantLinks {
			return fmt.Errorf("%s must be a regular non-symlink file with %d link(s): %s", label, wantLinks, path)
		}
		return nil
	}
	switch {
	case partialFound && targetFound:
		if !os.SameFile(partialInfo, targetInfo) {
			return noReplaceMissing, fmt.Errorf("%s partial and final files contain conflicting inodes", label)
		}
		if err := validate(partial, partialInfo, 2); err != nil {
			return noReplaceMissing, err
		}
		return noReplaceLinkedBoth, nil
	case partialFound:
		if err := validate(partial, partialInfo, 1); err != nil {
			return noReplaceMissing, err
		}
		return noReplacePartialOnly, nil
	case targetFound:
		if err := validate(target, targetInfo, 1); err != nil {
			return noReplaceMissing, err
		}
		return noReplaceTargetOnly, nil
	}
	return noReplaceMissing, fmt.Errorf("inspect %s reached an invalid state", label)
}

func restoreNoReplacePartialState(partial, target string, state noReplacePartialState) error {
	switch state {
	case noReplaceMissing, noReplacePartialOnly:
		return nil
	case noReplaceTargetOnly:
		if err := renameNoReplace(target, partial); err != nil {
			return err
		}
	case noReplaceLinkedBoth:
		if err := os.Remove(target); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown no-replace recovery state %d", state)
	}
	return syncDir(filepath.Dir(partial))
}

func restoreNoReplacePartial(partial, target, label string) (bool, error) {
	state, err := inspectNoReplacePartial(partial, target, label)
	if err != nil {
		return false, err
	}
	if state == noReplaceMissing {
		return false, nil
	}
	if err := restoreNoReplacePartialState(partial, target, state); err != nil {
		return false, err
	}
	return true, nil
}

func readRegularFileNoFollow(path, label string) ([]byte, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s must be a regular non-symlink file: %s", label, path)
	}
	return io.ReadAll(file)
}

func readMutableStateFile(path, label string) ([]byte, bool, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("open %s: %w", label, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, false, fmt.Errorf("stat %s: %w", label, err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || !ok || stat.Nlink != 1 {
		return nil, false, fmt.Errorf("%s must be a regular non-symlink file with one link: %s", label, path)
	}
	body, err := io.ReadAll(file)
	if err != nil {
		return nil, false, fmt.Errorf("read %s: %w", label, err)
	}
	return body, true, nil
}

func validateMutableStatePath(path, label string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat %s: %w", label, err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || !ok || stat.Nlink != 1 {
		return fmt.Errorf("%s must be absent or a regular non-symlink file with one link: %s", label, path)
	}
	return nil
}

func canonicalFileInExistingDirectory(path, label string) (string, error) {
	absolute, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	parent := filepath.Dir(absolute)
	info, err := os.Lstat(parent)
	if err != nil {
		return "", fmt.Errorf("stat %s directory: %w", label, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("%s directory must be a non-symlink directory: %s", label, parent)
	}
	parent, err = filepath.EvalSymlinks(parent)
	if err != nil {
		return "", fmt.Errorf("resolve %s directory: %w", label, err)
	}
	return filepath.Join(parent, filepath.Base(absolute)), nil
}

func ensureNonSymlinkDirectory(path, label string) (string, error) {
	absolute, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	if info, err := os.Lstat(absolute); err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("%s must be a non-symlink directory: %s", label, absolute)
		}
		return filepath.EvalSymlinks(absolute)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return "", fmt.Errorf("resolve %s parent: %w", label, err)
	}
	created := filepath.Join(parent, filepath.Base(absolute))
	if err := os.Mkdir(created, 0o700); err != nil {
		return "", err
	}
	if err := syncDir(parent); err != nil {
		return "", err
	}
	return created, nil
}

func atomicWrite(path string, body []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(mode); err != nil {
		return err
	}
	if _, err := tmp.Write(body); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	ok = true
	return nil
}

func atomicWriteNoReplace(path string, body []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}()
	if err := tmp.Chmod(mode); err != nil {
		return err
	}
	if _, err := tmp.Write(body); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Link(tmpName, path); err != nil {
		return err
	}
	if err := os.Remove(tmpName); err != nil {
		return err
	}
	return syncDir(dir)
}

func atomicJSON(path string, value any) ([]byte, error) {
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	body = append(body, '\n')
	if err := atomicWrite(path, body, 0o644); err != nil {
		return nil, err
	}
	return body, nil
}

func atomicJSONNoReplace(path string, value any) ([]byte, error) {
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	body = append(body, '\n')
	if err := atomicWriteNoReplace(path, body, 0o600); err != nil {
		return nil, err
	}
	return body, nil
}

func atomicJSONWithMinFree(path string, value any, minFree uint64) ([]byte, error) {
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	body = append(body, '\n')
	reserver, err := newFreeSpaceReserver(filepath.Dir(path), minFree)
	if err != nil {
		return nil, err
	}
	release, err := reserver.reserve(uint64(len(body)))
	if err != nil {
		return nil, err
	}
	defer release()
	if err := atomicWrite(path, body, 0o600); err != nil {
		return nil, err
	}
	return body, nil
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("fsync directory %s: %w", path, err)
	}
	return nil
}
