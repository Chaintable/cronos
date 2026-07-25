package main

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

type filesystemStatus struct {
	Device   uint64
	ReadOnly bool
}

var inspectArtifactFilesystem = statArtifactFilesystem

func statArtifactFilesystem(path string) (filesystemStatus, error) {
	var filesystem syscall.Statfs_t
	if err := syscall.Statfs(path, &filesystem); err != nil {
		return filesystemStatus{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return filesystemStatus{}, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return filesystemStatus{}, fmt.Errorf("stat %s returned an unsupported file identity", path)
	}
	return filesystemStatus{Device: uint64(stat.Dev), ReadOnly: uint64(filesystem.Flags)&1 != 0}, nil
}

func requireReadOnlyFilesystem(path, label string) (filesystemStatus, error) {
	status, err := inspectArtifactFilesystem(path)
	if err != nil {
		return filesystemStatus{}, fmt.Errorf("inspect %s filesystem: %w", label, err)
	}
	if !status.ReadOnly {
		return filesystemStatus{}, fmt.Errorf("%s filesystem must be mounted read-only", label)
	}
	return status, nil
}

func requireRegularFile(path, label string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s must be a regular non-symlink file: %s", label, path)
	}
	return nil
}

func requireSealedPlanDirectory(path string) (string, error) {
	dir := filepath.Clean(filepath.Dir(path))
	if filepath.Base(path) != "manifest.v1.json" || !filepath.IsLocal(filepath.Base(path)) {
		return "", fmt.Errorf("sealed plan manifest must be named manifest.v1.json")
	}
	if filepath.Clean(path) != filepath.Join(dir, "manifest.v1.json") {
		return "", fmt.Errorf("sealed plan manifest must be directly inside its plan directory")
	}
	if filepath.Ext(dir) != ".sealed" {
		return "", fmt.Errorf("manifest is not inside a sealed plan directory")
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("sealed plan path must be a non-symlink directory: %s", dir)
	}
	if err := requireRegularFile(path, "sealed plan manifest"); err != nil {
		return "", err
	}
	return dir, nil
}

func requireDumpDirectory(path, suffix, label string) (string, error) {
	dir, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	if filepath.Ext(dir) != suffix {
		return "", fmt.Errorf("%s path must end in %s", label, suffix)
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("%s path must be a non-symlink directory: %s", label, dir)
	}
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		return "", err
	}
	evmDir := filepath.Join(dir, "evm")
	evmInfo, err := os.Lstat(evmDir)
	if err != nil {
		return "", err
	}
	if !evmInfo.IsDir() || evmInfo.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("%s evm path must be a non-symlink directory: %s", label, evmDir)
	}
	return dir, nil
}

func requireSealedDumpDirectory(path string) (string, error) {
	dir, err := requireDumpDirectory(path, ".sealed", "sealed dump")
	if err != nil {
		return "", err
	}
	if err := requireRegularFile(filepath.Join(dir, "dump-manifest.v1.json"), "sealed dump manifest"); err != nil {
		return "", err
	}
	return dir, nil
}

func requireDumpArtifact(dir, name string) (string, error) {
	base := filepath.Base(name)
	if base == "." || !filepath.IsLocal(base) || name != filepath.Join("evm", base) {
		return "", fmt.Errorf("dump file path must be evm/<basename>: %q", name)
	}
	path := filepath.Join(dir, name)
	if err := requireRegularFile(path, "dump file"); err != nil {
		return "", err
	}
	return path, nil
}

func requireReadOnlySealedDump(path string, manifest dumpManifest) (string, error) {
	dir, err := requireSealedDumpDirectory(path)
	if err != nil {
		return "", err
	}
	root, err := requireReadOnlyFilesystem(dir, "sealed dump")
	if err != nil {
		return "", err
	}
	artifacts := []string{filepath.Join(dir, "evm"), filepath.Join(dir, "dump-manifest.v1.json")}
	if manifest.SourceManifestSHA256 != "" {
		source := filepath.Join(dir, dumpSourceFileName)
		if err := requireRegularFile(source, "sealed dump source manifest"); err != nil {
			return "", err
		}
		artifacts = append(artifacts, source)
	}
	for _, file := range manifest.Files {
		artifact, err := requireDumpArtifact(dir, file.Path)
		if err != nil {
			return "", err
		}
		artifacts = append(artifacts, artifact)
	}
	for _, artifact := range artifacts {
		status, err := requireReadOnlyFilesystem(artifact, "sealed dump artifact")
		if err != nil {
			return "", err
		}
		if status.Device != root.Device {
			return "", fmt.Errorf("sealed dump artifact must be on the dump filesystem: %s", artifact)
		}
	}
	return dir, nil
}

func requireReadOnlyPlanManifest(dir string, root filesystemStatus) error {
	path := filepath.Join(dir, "manifest.v1.json")
	if err := requireRegularFile(path, "sealed plan manifest"); err != nil {
		return err
	}
	status, err := requireReadOnlyFilesystem(path, "sealed plan manifest")
	if err != nil {
		return err
	}
	if status.Device != root.Device {
		return fmt.Errorf("sealed plan manifest must be on the plan filesystem")
	}
	return nil
}

func requireReadOnlyPlanArtifacts(dir string, manifest planManifest, root filesystemStatus) error {
	if err := requireReadOnlyPlanManifest(dir, root); err != nil {
		return err
	}
	names := []struct {
		name  string
		label string
	}{
		{manifest.HeightRootIndex, "height root index"},
		{manifest.RootIndex, "sorted root index"},
	}
	for number, chunk := range manifest.Chunks {
		names = append(names,
			struct {
				name  string
				label string
			}{chunk.Pack, fmt.Sprintf("pack chunk %d", number)},
			struct {
				name  string
				label string
			}{chunk.Index, fmt.Sprintf("pack index %d", number)},
		)
	}
	for _, artifact := range names {
		path, err := requirePlanArtifact(dir, artifact.name, artifact.label)
		if err != nil {
			return err
		}
		status, err := requireReadOnlyFilesystem(path, artifact.label)
		if err != nil {
			return err
		}
		if status.Device != root.Device {
			return fmt.Errorf("%s must be on the plan filesystem", artifact.label)
		}
	}
	return nil
}

func requirePlanArtifact(dir, name, label string) (string, error) {
	if name == "" || name != filepath.Base(name) || name == "." || !filepath.IsLocal(name) {
		return "", fmt.Errorf("%s path must be a basename inside the sealed plan: %q", label, name)
	}
	path := filepath.Join(dir, name)
	if err := requireRegularFile(path, label); err != nil {
		return "", err
	}
	return path, nil
}
