package app

import (
	"context"
	"fmt"
	"os"
	"time"

	"cosmossdk.io/log"
	dbm "github.com/cosmos/cosmos-db"
	"github.com/cosmos/cosmos-sdk/baseapp"
	servertypes "github.com/cosmos/cosmos-sdk/server/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/evmos/ethermint/debank/replay"
	"github.com/evmos/ethermint/debank/tracer"
	"github.com/evmos/ethermint/x/evm/keeper"
	evmtypes "github.com/evmos/ethermint/x/evm/types"
)

// The JSON-RPC server finds the executor by a runtime type assertion.
var _ replay.Executor = (*App)(nil)

// ReplayFirstHeight is the v1.4 upgrade block; v1.5 activates after ReplayLastHeight.
const (
	ReplayFirstHeight = 17155000
	ReplayLastHeight  = 38432211
)

// CheckReplayHeight selects the execution rules compiled into this binary.
func (app *App) CheckReplayHeight(chainID string, height int64) error {
	if chainID != "cronosmainnet_25-1" || height < ReplayFirstHeight || height > ReplayLastHeight {
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

// NewReplayApp builds an app that replays height. At the upgrade block the
// stores it adds start from empty in-memory trees.
func NewReplayApp(logger log.Logger, db dbm.DB, opts servertypes.AppOptions, chainID string, height int64) *App {
	return newApp(logger, db, nil, false, opts, height == ReplayFirstHeight, ReplayAppOptions(chainID)...)
}

// LoadReplayParent loads the parent version, applying the upgrade's store
// layout when the next block activates it.
func (app *App) LoadReplayParent(height int64) error {
	if height+1 != ReplayFirstHeight {
		return app.LoadHeight(height)
	}
	if err := app.CommitMultiStore().LoadVersionAndUpgrade(height, activationStoreUpgrades()); err != nil {
		return err
	}
	return app.BaseApp.Init()
}

// ObserveReplayTransactions attaches a collector to a private sequential keeper.
func ObserveReplayTransactions(k *keeper.Keeper) *[]replay.Transaction {
	var txs []replay.Transaction
	k.SetTransactionObserver(func(ctx sdk.Context, hash common.Hash) (vm.EVMLogger, func(*evmtypes.MsgEthereumTxResponse, error)) {
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
