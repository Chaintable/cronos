package cmd

import (
	"fmt"
	"strconv"

	dbm "github.com/cosmos/cosmos-db"
	"github.com/crypto-org-chain/cronos/app"
	"github.com/crypto-org-chain/cronos/cmd/cronosd/opendb"
	"github.com/spf13/cobra"

	"github.com/cosmos/cosmos-sdk/server"
)

// RollbackToVersionCmd force-rolls the application multistore back to an explicit
// target version. Unlike the standard `rollback` command it loads the store at the
// target version instead of the latest, so it can recover a store left at mixed
// per-store versions by an interrupted `rollback` (where `LoadLatestVersion` then
// fails with "wanted to load target N but only found up to N-1").
func RollbackToVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "rollback-to-version [height]",
		Short: "Force-rollback the application multistore to a specific version (assumes rocksdb backend)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			target, err := strconv.ParseInt(args[0], 10, 64)
			if err != nil {
				return err
			}

			ctx := server.GetServerContextFromCmd(cmd)
			db, err := opendb.OpenDB(ctx.Viper, ctx.Config.RootDir, dbm.RocksDBBackend)
			if err != nil {
				return err
			}
			defer db.Close()

			cronosApp := app.New(ctx.Logger, db, nil, false, ctx.Viper, server.DefaultBaseappOptions(ctx.Viper)...)
			// Load at the target version (every store has it) to populate the loaded
			// store set that RollbackToVersion iterates over.
			if err := cronosApp.LoadHeight(target); err != nil {
				return fmt.Errorf("load height %d: %w", target, err)
			}
			if err := cronosApp.CommitMultiStore().RollbackToVersion(target); err != nil {
				return fmt.Errorf("rollback to version %d: %w", target, err)
			}

			cmd.Printf("rolled back application multistore to version %d\n", target)
			return nil
		},
	}
}
