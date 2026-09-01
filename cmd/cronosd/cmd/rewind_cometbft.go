package cmd

import (
	"fmt"
	"strconv"

	cmtcfg "github.com/cometbft/cometbft/config"
	cmtstate "github.com/cometbft/cometbft/state"
	cmtstore "github.com/cometbft/cometbft/store"
	"github.com/cosmos/cosmos-sdk/server"
	"github.com/spf13/cobra"
)

const rewindProgressInterval int64 = 1000

// RewindCometBFTCmd removes the CometBFT block and consensus-state suffix above
// a target height. Application and index databases are intentionally handled by
// separate recovery steps.
func RewindCometBFTCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "rewind-cometbft [height]",
		Short: "Rewind CometBFT blocks and consensus state to a specific height",
		Long: `Rewind CometBFT blocks and consensus state to a specific height.

This command always deletes blocks above the target. It does not modify the
application, transaction-index, or EVM-index databases. Stop cronosd before
running it and rewind those databases as separate recovery steps.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			target, err := strconv.ParseInt(args[0], 10, 64)
			if err != nil {
				return fmt.Errorf("parse target height: %w", err)
			}
			if target <= 0 {
				return fmt.Errorf("target height must be positive: %d", target)
			}

			ctx := server.GetServerContextFromCmd(cmd)
			cfg := ctx.Config

			blockDB, err := cmtcfg.DefaultDBProvider(&cmtcfg.DBContext{ID: "blockstore", Config: cfg})
			if err != nil {
				return fmt.Errorf("open blockstore: %w", err)
			}
			defer blockDB.Close()
			blockStore := cmtstore.NewBlockStore(blockDB)

			stateDB, err := cmtcfg.DefaultDBProvider(&cmtcfg.DBContext{ID: "state", Config: cfg})
			if err != nil {
				return fmt.Errorf("open state store: %w", err)
			}
			defer stateDB.Close()
			stateStore := cmtstate.NewStore(stateDB, cmtstate.StoreOptions{
				DiscardABCIResponses: cfg.Storage.DiscardABCIResponses,
			})

			initialState, err := stateStore.Load()
			if err != nil {
				return fmt.Errorf("load CometBFT state: %w", err)
			}
			cmd.Printf("rewinding CometBFT state=%d blockstore=%d target=%d\n",
				initialState.LastBlockHeight, blockStore.Height(), target)

			height, hash, removed, err := rewindCometBFT(blockStore, stateStore, target, func(height, removed int64) {
				if removed%rewindProgressInterval == 0 {
					cmd.Printf("rewind progress height=%d removed=%d\n", height, removed)
				}
			})
			if err != nil {
				return err
			}

			cmd.Printf("rewound CometBFT to height %d app_hash=%X removed=%d\n", height, hash, removed)
			return nil
		},
	}
}

func rewindCometBFT(
	blockStore *cmtstore.BlockStore,
	stateStore cmtstate.Store,
	target int64,
	progress func(height, removed int64),
) (int64, []byte, int64, error) {
	if target <= 0 {
		return 0, nil, 0, fmt.Errorf("target height must be positive: %d", target)
	}
	if base := blockStore.Base(); base > 0 && target < base {
		return 0, nil, 0, fmt.Errorf("target %d is below blockstore base %d", target, base)
	}

	var removed int64
	for {
		currentState, err := stateStore.Load()
		if err != nil {
			return 0, nil, removed, fmt.Errorf("load CometBFT state: %w", err)
		}
		if currentState.IsEmpty() {
			return 0, nil, removed, fmt.Errorf("no CometBFT state found")
		}

		stateHeight := currentState.LastBlockHeight
		blockHeight := blockStore.Height()
		if stateHeight < target || blockHeight < target {
			return 0, nil, removed, fmt.Errorf(
				"target %d is above available state=%d or blockstore=%d", target, stateHeight, blockHeight,
			)
		}
		if stateHeight == target && blockHeight == target {
			return target, currentState.AppHash, removed, nil
		}

		height, _, err := cmtstate.Rollback(blockStore, stateStore, true)
		if err != nil {
			return 0, nil, removed, fmt.Errorf(
				"rewind CometBFT from state=%d blockstore=%d: %w", stateHeight, blockHeight, err,
			)
		}
		if height < target {
			return 0, nil, removed, fmt.Errorf("rewind crossed target %d and reached %d", target, height)
		}

		removed++
		if progress != nil {
			progress(height, removed)
		}
	}
}
