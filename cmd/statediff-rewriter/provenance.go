package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cosmos/cosmos-sdk/version"
)

type buildIdentity struct {
	CronosCommit    string
	EthermintCommit string
	IAVLCommit      string
	BuildTags       string
	ImageDigest     string
}

const (
	runtimeImageDigestEnvironment = "STATEDIFF_REWRITER_IMAGE_DIGEST"
	unknownBuildIdentity          = "unknown"
)

var currentBuildIdentity = func() buildIdentity {
	cronosCommit, ethermintCommit, iavlCommit := buildCommits()
	return buildIdentity{
		CronosCommit: cronosCommit, EthermintCommit: ethermintCommit,
		IAVLCommit: iavlCommit, BuildTags: version.BuildTags,
		ImageDigest: os.Getenv(runtimeImageDigestEnvironment),
	}
}

func knownBuildCommit(commit string) bool {
	if len(commit) < 12 || len(commit) > 64 || len(commit)%2 != 0 {
		return false
	}
	body, err := hex.DecodeString(commit)
	if err != nil {
		return false
	}
	for _, value := range body {
		if value != 0 {
			return true
		}
	}
	return false
}

func validSHA256Digest(digest string) bool {
	if len(digest) != len("sha256:")+64 || !strings.HasPrefix(digest, "sha256:") {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(digest, "sha256:"))
	return err == nil
}

func validSHA256Hex(digest string) bool {
	if len(digest) != 64 {
		return false
	}
	_, err := hex.DecodeString(digest)
	return err == nil
}

func hasRequiredBuildTags(tags string) bool {
	seen := make(map[string]bool)
	for _, tag := range strings.FieldsFunc(tags, func(r rune) bool { return r == ',' || r == ' ' }) {
		seen[tag] = true
	}
	return seen["rocksdb"] && seen["grocksdb_clean_link"]
}

func validCreatedAt(value string) bool {
	if value == "" {
		return false
	}
	_, err := time.Parse(time.RFC3339Nano, value)
	return err == nil
}

func validateBuildIdentity(
	cronosCommit, ethermintCommit, iavlCommit, imageDigest, buildTags string,
) error {
	if !knownBuildCommit(cronosCommit) || !knownBuildCommit(ethermintCommit) || !knownBuildCommit(iavlCommit) {
		return fmt.Errorf("build commits are incomplete or malformed")
	}
	if !validSHA256Digest(imageDigest) {
		return fmt.Errorf("image digest must be sha256 followed by 64 hexadecimal characters")
	}
	if !hasRequiredBuildTags(buildTags) {
		return fmt.Errorf("build tags must contain rocksdb and grocksdb_clean_link")
	}
	return nil
}

func requireRuntimeBuildIdentity(manifest planManifest) error {
	return requireRuntimeBuild(
		manifest.CronosCommit, manifest.EthermintCommit, manifest.IAVLCommit, manifest.ImageDigest, manifest.BuildTags,
	)
}

func requireRuntimeBuild(cronosCommit, ethermintCommit, iavlCommit, imageDigest, buildTags string) error {
	current := currentBuildIdentity()
	if !validSHA256Digest(current.ImageDigest) {
		return fmt.Errorf("%s must contain the running image sha256 digest", runtimeImageDigestEnvironment)
	}
	if err := validateBuildIdentity(
		current.CronosCommit, current.EthermintCommit, current.IAVLCommit, current.ImageDigest, current.BuildTags,
	); err != nil {
		return fmt.Errorf("runtime build identity: %w", err)
	}
	if err := validateBuildIdentity(cronosCommit, ethermintCommit, iavlCommit, imageDigest, buildTags); err != nil {
		return fmt.Errorf("requested build identity: %w", err)
	}
	if current.CronosCommit != cronosCommit || current.EthermintCommit != ethermintCommit ||
		current.IAVLCommit != iavlCommit || current.BuildTags != buildTags || current.ImageDigest != imageDigest {
		return fmt.Errorf(
			"runtime build identity differs from requested identity: got %s/%s/%s tags=%q image=%q, want %s/%s/%s tags=%q image=%q",
			current.CronosCommit, current.EthermintCommit, current.IAVLCommit, current.BuildTags, current.ImageDigest,
			cronosCommit, ethermintCommit, iavlCommit, buildTags, imageDigest,
		)
	}
	return nil
}
