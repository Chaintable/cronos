package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

func main() {
	if !rocksDBBuild {
		fmt.Fprintln(os.Stderr, "statediff-rewriter was not built with rocksdb and grocksdb_clean_link tags")
		os.Exit(1)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := newRootCommand().ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:          "statediff-rewriter",
		Short:        "Plan, verify, conditionally apply, or roll back canonical Cronos storage diffs",
		SilenceUsage: true,
		PersistentPreRunE: func(*cobra.Command, []string) error {
			if !rocksDBBuild {
				return fmt.Errorf("this binary was not built with rocksdb and grocksdb_clean_link tags")
			}
			return nil
		},
	}
	root.AddCommand(newPlanCommand(), newApplyCommand(), newVerifyCommand(), newRollbackCommand())
	return root
}

func newPlanCommand() *cobra.Command {
	options := planOptions{Bucket: defaultBucket, Prefix: defaultPrefix, Region: defaultRegion}
	command := &cobra.Command{
		Use:   "plan",
		Short: "Seal a strict changeset dump and build a read-only changed-object plan",
		RunE: func(command *cobra.Command, _ []string) error {
			firstSet := command.Flags().Changed("pilot-first-height")
			finalSet := command.Flags().Changed("pilot-final-height")
			if firstSet != finalSet {
				return fmt.Errorf("pilot-first-height and pilot-final-height must be set together")
			}
			options.Pilot = firstSet
			objects, err := newS3ObjectStore(command.Context(), options.Region)
			if err != nil {
				return err
			}
			manifest, location, err := runPlan(command.Context(), options, objects)
			if err != nil {
				return err
			}
			return printJSON(struct {
				Manifest planManifest `json:"manifest"`
				Location string       `json:"location"`
			}{manifest, location})
		},
	}
	command.Flags().StringVar(&options.DumpStaging, "dump", "", "changeset dump directory ending in .staging or .sealed")
	command.Flags().StringVar(&options.ArchiveHome, "home", "", "frozen Cronos archive home")
	command.Flags().StringVar(&options.Output, "output", "", "plan directory ending in .staging")
	command.Flags().Uint64Var(&options.MinFree, "min-free-bytes", 0, "stop before filesystem free space falls below this value")
	command.Flags().StringVar(&options.SnapshotID, "snapshot-id", "", "completed final-F EBS snapshot ID")
	command.Flags().StringVar(&options.ImageDigest, "image-digest", "", "immutable rewriter image digest")
	command.Flags().Int64Var(&options.PilotFirstHeight, "pilot-first-height", 0, "first height of a read-only pilot plan; requires pilot-final-height")
	command.Flags().Int64Var(&options.PilotFinalHeight, "pilot-final-height", 0, "final height of a read-only pilot plan; requires pilot-first-height")
	_ = command.MarkFlagRequired("dump")
	_ = command.MarkFlagRequired("home")
	_ = command.MarkFlagRequired("output")
	_ = command.MarkFlagRequired("min-free-bytes")
	_ = command.MarkFlagRequired("snapshot-id")
	_ = command.MarkFlagRequired("image-digest")
	return command
}

func newApplyCommand() *cobra.Command {
	var manifest, checkpoint, backup string
	command := &cobra.Command{
		Use:   "apply",
		Short: "Conditionally write the new bodies from a sealed plan",
		RunE: func(command *cobra.Command, _ []string) error {
			plan, _, err := loadPlanManifest(manifest)
			if err != nil {
				return err
			}
			if err := requireFullPlan(plan, "apply"); err != nil {
				return err
			}
			objects, err := newS3ObjectStore(command.Context(), plan.Region)
			if err != nil {
				return err
			}
			report, err := runWriteMode(command.Context(), manifest, checkpoint, backup, "apply", objects)
			if err != nil {
				return err
			}
			return printJSON(report)
		},
	}
	command.Flags().StringVar(&manifest, "manifest", "", "sealed manifest.v1.json")
	command.Flags().StringVar(&checkpoint, "checkpoint", "", "apply checkpoint path")
	command.Flags().StringVar(&backup, "backup-proof", "", "completed independent backup proof JSON")
	_ = command.MarkFlagRequired("manifest")
	_ = command.MarkFlagRequired("checkpoint")
	_ = command.MarkFlagRequired("backup-proof")
	return command
}

func newVerifyCommand() *cobra.Command {
	var manifest, checkpointDir string
	command := &cobra.Command{
		Use:   "verify",
		Short: "GET and validate every height in a sealed plan",
		RunE: func(command *cobra.Command, _ []string) error {
			plan, _, err := loadPlanManifest(manifest)
			if err != nil {
				return err
			}
			if err := requireFullPlan(plan, "verify"); err != nil {
				return err
			}
			objects, err := newS3ObjectStore(command.Context(), plan.Region)
			if err != nil {
				return err
			}
			report, err := runVerify(command.Context(), manifest, checkpointDir, objects)
			if err != nil {
				return err
			}
			return printJSON(report)
		},
	}
	command.Flags().StringVar(&manifest, "manifest", "", "sealed manifest.v1.json")
	command.Flags().StringVar(&checkpointDir, "checkpoint-dir", "", "verify checkpoint directory")
	_ = command.MarkFlagRequired("manifest")
	_ = command.MarkFlagRequired("checkpoint-dir")
	return command
}

func newRollbackCommand() *cobra.Command {
	var manifest, checkpoint string
	command := &cobra.Command{
		Use:   "rollback",
		Short: "Conditionally restore old bodies from a sealed plan",
		RunE: func(command *cobra.Command, _ []string) error {
			plan, _, err := loadPlanManifest(manifest)
			if err != nil {
				return err
			}
			if err := requireFullPlan(plan, "rollback"); err != nil {
				return err
			}
			objects, err := newS3ObjectStore(command.Context(), plan.Region)
			if err != nil {
				return err
			}
			report, err := runWriteMode(command.Context(), manifest, checkpoint, "", "rollback", objects)
			if err != nil {
				return err
			}
			return printJSON(report)
		},
	}
	command.Flags().StringVar(&manifest, "manifest", "", "sealed manifest.v1.json")
	command.Flags().StringVar(&checkpoint, "checkpoint", "", "rollback checkpoint path")
	_ = command.MarkFlagRequired("manifest")
	_ = command.MarkFlagRequired("checkpoint")
	return command
}

func printJSON(value any) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
