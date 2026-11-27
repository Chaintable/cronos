package app

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"time"

	"cosmossdk.io/log"
	pruningtypes "cosmossdk.io/store/pruning/types"
	upgradetypes "cosmossdk.io/x/upgrade/types"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	dbm "github.com/cosmos/cosmos-db"
	"github.com/cosmos/cosmos-sdk/baseapp"
	"github.com/cosmos/cosmos-sdk/client/flags"
	servertypes "github.com/cosmos/cosmos-sdk/server/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/evmos/ethermint/debank/replay"
	"github.com/evmos/ethermint/debank/tracer"
	"github.com/evmos/ethermint/x/evm/keeper"
	evmtypes "github.com/evmos/ethermint/x/evm/types"
	"github.com/spf13/viper"
)

// The JSON-RPC server finds the executor by a runtime type assertion.
var _ replay.Executor = (*App)(nil)

// ReplayFirstHeight is the v1.5 upgrade block; v1.6 activates after ReplayLastHeight.
const (
	ReplayChainID     = "cronosmainnet_25-1"
	ReplayFirstHeight = 38432212
	ReplayLastHeight  = 45062799
)

// CheckReplayHeight selects the execution rules compiled into this binary.
func (app *App) CheckReplayHeight(chainID string, height int64) error {
	if chainID != ReplayChainID || height < ReplayFirstHeight || height > ReplayLastHeight {
		return fmt.Errorf("unsupported execution rules for %s at %d", chainID, height)
	}
	return nil
}

// BlockFileOnly reads CRONOS_REPLAY_OUTPUT: "full" (default) or "blockfile",
// which returns only the block file and skips state-diff work.
func (app *App) BlockFileOnly() (bool, error) {
	switch mode := os.Getenv("CRONOS_REPLAY_OUTPUT"); mode {
	case "", "full":
		return false, nil
	case "blockfile":
		return true, nil
	default:
		return false, fmt.Errorf("invalid CRONOS_REPLAY_OUTPUT %q (expected full or blockfile)", mode)
	}
}

// ReplayBlock executes the block in the long-lived replay worker, isolating
// its app instances and IAVL working trees from consensus.
func (app *App) ReplayBlock(ctx context.Context, req replay.Request) (*replay.Result, error) {
	if req.Request == nil {
		return nil, fmt.Errorf("missing finalize request")
	}
	if err := app.CheckReplayHeight(req.ChainID, req.Request.Height); err != nil {
		return nil, err
	}
	start := time.Now()
	logger := app.Logger().With("module", "replay")
	result, err := app.replayWorker.call(ctx, app.replayHome, logger, req)
	if err != nil {
		return nil, fmt.Errorf("replay worker: %w", err)
	}
	logger.Debug("replay finished", "height", req.Request.Height, "total_ms", time.Since(start).Milliseconds())
	return result, nil
}

// ReplayAppOptions configures a discarded per-request replay app. Async IAVL
// pruning would start a goroutine per store that keeps the whole app alive.
func ReplayAppOptions(chainID string) []func(*baseapp.BaseApp) {
	return []func(*baseapp.BaseApp){
		baseapp.SetChainID(chainID),
		baseapp.SetIAVLDisableFastNode(true),
		baseapp.SetIAVLSyncPruning(true),
	}
}

// NewReplayApp builds a private app for an independent replay.
func NewReplayApp(logger log.Logger, db dbm.DB, opts servertypes.AppOptions, chainID string) *App {
	return New(logger, db, nil, false, opts, ReplayAppOptions(chainID)...)
}

// LoadReplayParent loads the parent version, applying the upgrade's store
// layout when the next block activates it.
func (app *App) LoadReplayParent(height int64) error {
	if height+1 != ReplayFirstHeight {
		return app.LoadHeight(height)
	}
	if err := app.UpgradeKeeper.DumpUpgradeInfoToDisk(ReplayFirstHeight, upgradetypes.Plan{Name: "v1.5", Height: ReplayFirstHeight}); err != nil {
		return err
	}
	if err := app.CommitMultiStore().LoadVersionAndUpgrade(height, activationStoreUpgrades()); err != nil {
		return err
	}
	return app.BaseApp.Init()
}

// OpenReplayApp loads a parent version once for a segment. Its commits stay
// in a private overlay; no replay writes reach the underlying archive.
func OpenReplayApp(db dbm.DB, home, chainID string, parent int64) (*App, error) {
	opts := viper.New()
	opts.Set(flags.FlagHome, home)
	opts.Set(flags.FlagChainID, chainID)
	options := append(ReplayAppOptions(chainID), baseapp.SetPruning(pruningtypes.NewPruningOptions(pruningtypes.PruningNothing)))
	opts.Set(FlagDisableOptimisticExecution, true)
	a := New(log.NewNopLogger(), newOverlayDB(db), nil, false, opts, options...)
	if err := a.LoadReplayParent(parent); err != nil {
		return nil, fmt.Errorf("open parent multistore at %d: %w", parent, err)
	}
	if err := a.RefreshBlockList(a.NewUncachedContext(false, cmtproto.Header{})); err != nil {
		return nil, err
	}
	return a, nil
}

// ReplayNextBlock executes the block on the app's current commit, which must
// be the requested parent, and commits it so the segment can continue.
func (app *App) ReplayNextBlock(req replay.Request) (*replay.Result, error) {
	if req.Request == nil || len(req.ParentHash) != 32 {
		return nil, fmt.Errorf("invalid historical finalize request")
	}
	parent := req.Request.Height - 1
	if id := app.LastCommitID(); id.Version != parent || !bytes.Equal(id.Hash, req.ParentHash) {
		return nil, fmt.Errorf("parent commit at %d: got %d/%X, want %X", parent, id.Version, id.Hash, req.ParentHash)
	}
	txs := ObserveReplayTransactions(app.EvmKeeper)
	defer app.EvmKeeper.SetTransactionObserver(nil)
	finalized, err := app.FinalizeBlock(req.Request)
	if err != nil {
		return nil, err
	}
	if _, err = app.Commit(); err != nil {
		return nil, err
	}
	if id := app.LastCommitID(); id.Version != req.Request.Height || !bytes.Equal(id.Hash, finalized.AppHash) {
		return nil, fmt.Errorf("private commit differs from finalized block %d", req.Request.Height)
	}
	return &replay.Result{Finalize: finalized, Transactions: *txs}, nil
}

// ObserveReplayTransactions attaches a collector to a private sequential keeper.
func ObserveReplayTransactions(k *keeper.Keeper) *[]replay.Transaction {
	var txs []replay.Transaction
	k.SetTransactionObserver(func(ctx sdk.Context, hash common.Hash) (*tracing.Hooks, func(*evmtypes.MsgEthereumTxResponse, error)) {
		logger, finish := tracer.NewReplayTracer(hash)
		tx := replay.Transaction{CosmosIndex: ctx.TxIndex(), MessageIndex: ctx.MsgIndex(), Hash: hash}
		return logger, func(result *evmtypes.MsgEthereumTxResponse, err error) {
			if err == nil {
				tx.GasUsed = result.GasUsed
				tx.Failed = result.Failed()
				tx.Trace, err = finish(result.VmError)
			}
			if err != nil {
				tx.Error = err.Error()
			}
			txs = append(txs, tx)
		}
	})
	return &txs
}
