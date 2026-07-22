package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	sdkversion "github.com/cosmos/cosmos-sdk/version"
)

func main() {
	if !rocksDBBuild {
		fmt.Fprintln(os.Stderr, "statediff-rewriter was not built with rocksdb and grocksdb_clean_link tags")
		os.Exit(1)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	if err := newRootCommand().ExecuteContext(ctx); err != nil {
		cancel()
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	cancel()
}

func newRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:          "statediff-rewriter",
		Short:        "Plan, verify, conditionally apply, or roll back canonical Cronos storage diffs",
		Args:         cobra.NoArgs,
		Version:      buildCommit(),
		SilenceUsage: true,
		PersistentPreRunE: func(*cobra.Command, []string) error {
			if !rocksDBBuild {
				return fmt.Errorf("this binary was not built with rocksdb and grocksdb_clean_link tags")
			}
			return nil
		},
	}
	root.SetVersionTemplate("{{.Version}}\n")
	commands := []*cobra.Command{newDumpCommand(), newPlanCommand(), newApplyCommand(), newVerifyCommand(), newRollbackCommand()}
	for _, command := range commands {
		command.Args = cobra.NoArgs
	}
	root.AddCommand(commands...)
	return root
}

func buildCommit() string {
	if sdkversion.Commit == "" {
		return unknownBuildIdentity
	}
	return sdkversion.Commit
}

func newPlanCommand() *cobra.Command {
	options := planOptions{
		Bucket: defaultBucket, Prefix: defaultPrefix, Region: defaultRegion,
		IAVLConcurrency: defaultDirectIAVLConcurrency, Parallel: defaultParallelOptions(),
	}
	command := &cobra.Command{
		Use:   "plan",
		Short: "Build a read-only changed-object plan from a sealed dump or frozen IAVL",
		RunE: func(command *cobra.Command, _ []string) error {
			firstSet := command.Flags().Changed("pilot-first-height")
			finalSet := command.Flags().Changed("pilot-final-height")
			if firstSet != finalSet {
				return fmt.Errorf("pilot-first-height and pilot-final-height must be set together")
			}
			options.Pilot = firstSet
			manifest, location, err := runPlan(command.Context(), options, nil)
			if err != nil {
				return err
			}
			manifestHash, _, err := hashFile(location)
			if err != nil {
				return err
			}
			return printJSON(struct {
				Manifest       planManifest `json:"manifest"`
				Location       string       `json:"location"`
				ManifestSHA256 string       `json:"manifest_sha256"`
			}{manifest, location, manifestHash})
		},
	}
	command.Flags().StringVar(&options.DumpStaging, "dump", "", "changeset dump directory ending in .staging or .sealed")
	command.Flags().BoolVar(&options.Direct, "direct", false, "read changesets directly from the frozen archive IAVL")
	command.Flags().IntVar(&options.IAVLCacheSize, "iavl-cache-size", defaultDumpCacheSize, "IAVL node-cache entries per direct traversal worker")
	command.Flags().IntVar(&options.IAVLConcurrency, "iavl-concurrency", options.IAVLConcurrency, "parallel IAVL shards for direct traversal")
	command.Flags().StringVar(&options.ArchiveHome, "home", "", "frozen Cronos archive home")
	command.Flags().StringVar(&options.Output, "output", "", "plan directory ending in .staging")
	command.Flags().Uint64Var(&options.MinFree, "min-free-bytes", 0, "stop before filesystem free space falls below this value")
	command.Flags().StringVar(&options.SnapshotID, "snapshot-id", "", "completed final-F EBS snapshot ID")
	command.Flags().StringVar(&options.ImageDigest, "image-digest", "", "immutable rewriter image digest")
	command.Flags().Int64Var(&options.PilotFirstHeight, "pilot-first-height", 0, "first height of a read-only pilot plan; requires pilot-final-height")
	command.Flags().Int64Var(&options.PilotFinalHeight, "pilot-final-height", 0, "final height of a read-only pilot plan; requires pilot-first-height")
	addParallelFlags(command, &options.Parallel, true, true)
	_ = command.MarkFlagRequired("home")
	_ = command.MarkFlagRequired("output")
	_ = command.MarkFlagRequired("min-free-bytes")
	_ = command.MarkFlagRequired("snapshot-id")
	_ = command.MarkFlagRequired("image-digest")
	return command
}

func newApplyCommand() *cobra.Command {
	var manifest, checkpoint, backup string
	parallel := defaultParallelOptions()
	command := &cobra.Command{
		Use:   "apply",
		Short: "Conditionally write the new bodies from a sealed plan",
		RunE: func(command *cobra.Command, _ []string) error {
			report, err := runWriteModeWithOptions(command.Context(), manifest, checkpoint, backup, applyMode, parallel, nil)
			if err != nil {
				return err
			}
			return printJSON(report)
		},
	}
	command.Flags().StringVar(&manifest, "manifest", "", "sealed manifest.v1.json")
	command.Flags().StringVar(&checkpoint, "checkpoint", "", "apply checkpoint path")
	command.Flags().StringVar(&backup, "backup-proof", "", "completed independent backup proof JSON")
	addParallelFlags(command, &parallel, true, false)
	_ = command.MarkFlagRequired("manifest")
	_ = command.MarkFlagRequired("checkpoint")
	_ = command.MarkFlagRequired("backup-proof")
	return command
}

func newVerifyCommand() *cobra.Command {
	var manifest, checkpointDir string
	parallel := defaultParallelOptions()
	command := &cobra.Command{
		Use:   "verify",
		Short: "GET and validate every height in a sealed plan",
		RunE: func(command *cobra.Command, _ []string) error {
			report, err := runVerifyWithOptions(command.Context(), manifest, checkpointDir, parallel, nil)
			if err != nil {
				return err
			}
			return printJSON(report)
		},
	}
	command.Flags().StringVar(&manifest, "manifest", "", "sealed manifest.v1.json")
	command.Flags().StringVar(&checkpointDir, "checkpoint-dir", "", "verify checkpoint directory")
	addParallelFlags(command, &parallel, true, true)
	_ = command.MarkFlagRequired("manifest")
	_ = command.MarkFlagRequired("checkpoint-dir")
	return command
}

func newRollbackCommand() *cobra.Command {
	var manifest, checkpoint string
	parallel := defaultParallelOptions()
	command := &cobra.Command{
		Use:   "rollback",
		Short: "Conditionally restore old bodies from a sealed plan",
		RunE: func(command *cobra.Command, _ []string) error {
			report, err := runWriteModeWithOptions(command.Context(), manifest, checkpoint, "", rollbackMode, parallel, nil)
			if err != nil {
				return err
			}
			return printJSON(report)
		},
	}
	command.Flags().StringVar(&manifest, "manifest", "", "sealed manifest.v1.json")
	command.Flags().StringVar(&checkpoint, "checkpoint", "", "rollback checkpoint path")
	addParallelFlags(command, &parallel, true, false)
	_ = command.MarkFlagRequired("manifest")
	_ = command.MarkFlagRequired("checkpoint")
	return command
}

func addParallelFlags(command *cobra.Command, options *parallelOptions, withCheckpoint, withCheckpointInterval bool) {
	command.Flags().IntVar(&options.Concurrency, "concurrency", options.Concurrency, "maximum concurrent object operations")
	command.Flags().IntVar(&options.Window, "window", options.Window, "maximum emitted but not continuously committed operations")
	command.Flags().Int64Var(&options.MaxInFlightBytes, "max-inflight-bytes", options.MaxInFlightBytes, "maximum estimated memory held by in-flight object operations")
	if withCheckpoint {
		command.Flags().Uint64Var(&options.CheckpointEvery, "checkpoint-every", options.CheckpointEvery, "persist a continuous checkpoint after this many operations")
	}
	if withCheckpointInterval {
		command.Flags().DurationVar(&options.CheckpointInterval, "checkpoint-interval", options.CheckpointInterval, "maximum time between continuous checkpoint writes")
	}
}

func printJSON(value any) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
