package cmd

import (
	"encoding/json"
	"fmt"
	store "github.com/cosmos/cosmos-sdk/store/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/evmos/ethermint/x/evm/keeper"
	evm "github.com/evmos/ethermint/x/evm/types"

	proto "github.com/tendermint/tendermint/proto/tendermint/types"
	"math/big"
	"os"
	"path/filepath"
	"strconv"

	"github.com/cosmos/cosmos-sdk/client/flags"
	"github.com/cosmos/cosmos-sdk/server"
	"github.com/evmos/ethermint/debank/replay"
	"github.com/spf13/cobra"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/rpc/client/http"
	dbm "github.com/tendermint/tm-db"
)

// replayBlocks advances only the private replay database. All normal node
// databases and keeper files under the node home are left to the node.
func (a appCreator) replayBlocks() *cobra.Command {
	var archiveDir string
	cmd := &cobra.Command{
		Use:   "replay-blocks ARCHIVE-RPC FIRST LAST",
		Short: "Execute canonical Cosmos blocks against a private replay checkpoint",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			first, err := strconv.ParseInt(args[1], 10, 64)
			if err != nil {
				return err
			}
			last, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				return err
			}
			if first < 6542800 || last > 11608759 || first > last {
				return fmt.Errorf("range %d..%d outside this binary's mainnet interval", first, last)
			}
			source, err := http.New(args[0], "/websocket")
			if err != nil {
				return err
			}
			ctx := server.GetServerContextFromCmd(cmd)
			home := ctx.Viper.GetString(flags.FlagHome)
			private := filepath.Join(home, "replay")
			data := filepath.Join(private, "data")
			if err = os.MkdirAll(data, 0700); err != nil {
				return err
			}
			backend := ctx.Viper.GetString("app-db-backend")
			if backend == "" {
				backend = "goleveldb"
			}
			var db dbm.DB
			if archiveDir != "" {
				if first != last {
					return fmt.Errorf("archive replay expects a single height")
				}
				archive, closeArchive, openErr := openReplayArchive(archiveDir)
				if openErr != nil {
					return openErr
				}
				defer closeArchive()
				db, err = replay.OpenParentState(archive, first-1)
			} else {
				db, err = dbm.NewDB("application", dbm.BackendType(backend), data)
			}
			if err != nil {
				return err
			}
			defer db.Close()
			ctx.Viper.Set(flags.FlagHome, private)
			// Historical checkpoints contain the versioned IAVL tree.
			ctx.Viper.Set(server.FlagDisableIAVLFastNode, true)
			writes := replay.NewStateWrites("basecro")
			application := a.newReplayApp(log.NewNopLogger(), db, nil, ctx.Viper, writes)
			cms := application.NewUncachedContext(false, proto.Header{}).MultiStore().(store.CommitMultiStore)
			out := json.NewEncoder(cmd.OutOrStdout())
			for h := first; h <= last; h++ {
				previous, err := cms.CacheMultiStoreWithVersion(h - 1)
				if err != nil {
					return err
				}
				var observations []keeper.ReplayTransaction
				var traceErr error
				application.EvmKeeper.SetReplaySink(func(obs keeper.ReplayTransaction, err error) {
					if err != nil {
						traceErr = err
						return
					}
					observations = append(observations, obs)
				})
				r, err := replay.FromArchive(cmd.Context(), source, application, h)
				if err != nil {
					return fmt.Errorf("replay %d: %w", h, err)
				}
				if traceErr != nil {
					return fmt.Errorf("trace %d: %w", h, traceErr)
				}
				diff, err := writes.Diff(application.AppCodec(), previous, cms)
				if err != nil {
					return fmt.Errorf("committed state diff %d: %w", h, err)
				}
				writes.NextBlock()
				post := application.NewUncachedContext(false, *r.Block.Header.ToProto())
				validator, ok := application.StakingKeeper.GetValidatorByConsAddr(post, sdk.ConsAddress(r.Block.ProposerAddress))
				if !ok {
					return fmt.Errorf("missing proposer at %d", h)
				}
				params := application.GetConsensusParams(post)
				if params == nil || params.Block == nil {
					return fmt.Errorf("missing consensus gas limit at %d", h)
				}
				fee, err := application.EvmKeeper.BaseFee(sdk.WrapSDKContext(post), &evm.QueryBaseFeeRequest{})
				if err != nil {
					return err
				}
				var baseFee *big.Int
				if fee.BaseFee != nil {
					baseFee = fee.BaseFee.BigInt()
				}
				bundle, err := r.Bundle(a.encCfg.TxConfig.TxDecoder(), common.BytesToAddress(validator.GetOperator()), params.Block.MaxGas, baseFee, diff, observations)
				if err != nil {
					return fmt.Errorf("assemble block %d: %w", h, err)
				}
				if err = out.Encode(bundle); err != nil {
					return err
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&archiveDir, "archive-state-dir", "", "Read the parent version from a sealed application database in this directory")
	return cmd
}
