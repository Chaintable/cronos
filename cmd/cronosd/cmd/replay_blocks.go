package cmd

import (
	"encoding/json"
	"fmt"
	store "github.com/cosmos/cosmos-sdk/store/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	proto "github.com/tendermint/tendermint/proto/tendermint/types"
	"github.com/tharsis/ethermint/x/evm/keeper"
	"os"
	"path/filepath"
	"strconv"

	"github.com/cosmos/cosmos-sdk/client/flags"
	"github.com/cosmos/cosmos-sdk/server"
	"github.com/spf13/cobra"
	abci "github.com/tendermint/tendermint/abci/types"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/rpc/client/http"
	tm "github.com/tendermint/tendermint/types"
	dbm "github.com/tendermint/tm-db"
	"github.com/tharsis/ethermint/debank/replay"
)

// replayBlocks advances only the private replay database. All normal node
// databases and keeper files under the node home are left to the node.
func (a appCreator) replayBlocks() *cobra.Command {
	var lastOnly bool
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
			if first < 1 || last > 2693799 || first > last {
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
			db, err := dbm.NewDB("application", dbm.BackendType(backend), data)
			if err != nil {
				return err
			}
			defer db.Close()
			ctx.Viper.Set(flags.FlagHome, private)
			writes := replay.NewStateWrites("basecro")
			application := a.newAppWithCache(log.NewNopLogger(), db, nil, ctx.Viper, writes)
			cms := application.NewUncachedContext(false, proto.Header{}).MultiStore().(store.CommitMultiStore)

			if first == 1 {
				if application.Info(abci.RequestInfo{}).LastBlockHeight != 0 {
					return fmt.Errorf("genesis replay requires an empty replay database")
				}
				doc, err := tm.GenesisDocFromFile(filepath.Join(home, "config", "genesis.json"))
				if err != nil {
					return err
				}
				if doc.ChainID != "cronosmainnet_25-1" || doc.InitialHeight != 1 {
					return fmt.Errorf("unexpected genesis identity")
				}
				validators := make([]*tm.Validator, len(doc.Validators))
				for i, v := range doc.Validators {
					validators[i] = tm.NewValidator(v.PubKey, v.Power)
				}
				params := tm.TM2PB.ConsensusParams(doc.ConsensusParams)
				application.InitChain(abci.RequestInitChain{Time: doc.GenesisTime, ChainId: doc.ChainID, InitialHeight: doc.InitialHeight, ConsensusParams: params, Validators: tm.TM2PB.ValidatorUpdates(tm.NewValidatorSet(validators)), AppStateBytes: doc.AppState})
			}
			out := cmd.OutOrStdout()
			for h := first; h <= last; h++ {
				var previous store.MultiStore
				if h > 1 {
					previous, err = cms.CacheMultiStoreWithVersion(h - 1)
					if err != nil {
						return fmt.Errorf("open committed state %d: %w", h-1, err)
					}
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
					return fmt.Errorf("trace block %d: %w", h, traceErr)
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
				bundle, err := r.Bundle(a.encCfg.TxConfig.TxDecoder(), common.BytesToAddress(validator.GetOperator()), params.Block.MaxGas, diff, observations)
				if err != nil {
					return fmt.Errorf("assemble block %d: %w", h, err)
				}
				if lastOnly && h != last {
					continue
				}
				data, err := json.Marshal(bundle)
				if err != nil {
					return err
				}
				if !lastOnly {
					if err = saveBundle(bundlePath(home, h, bundle.Header.Hash), data); err != nil {
						return err
					}
				}
				if _, err = out.Write(append(data, '\n')); err != nil {
					return err
				}
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&lastOnly, "last-only", false, "Return only the last block without caching intermediate results")
	return cmd
}
