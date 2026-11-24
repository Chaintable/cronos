package app

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/cosmos/cosmos-sdk/baseapp"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/evmos/ethermint/debank/replay"
	"github.com/evmos/ethermint/debank/tracer"
	"github.com/evmos/ethermint/x/evm/keeper"
	evmtypes "github.com/evmos/ethermint/x/evm/types"
)

// The JSON-RPC server finds the executor by a runtime type assertion.
var _ replay.Executor = (*App)(nil)

// CheckReplayHeight selects the execution rules compiled into this binary.
func (app *App) CheckReplayHeight(chainID string, height int64) error {
	if chainID != "cronosmainnet_25-1" || height < 58825800 || height >= 101485000 {
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

// ObserveReplayTransactions attaches a collector to a private sequential keeper.
func ObserveReplayTransactions(k *keeper.Keeper) *[]replay.Transaction {
	var txs []replay.Transaction
	k.SetTransactionObserver(func(ctx sdk.Context, hash common.Hash) (*tracing.Hooks, func(*evmtypes.EVMResult, error)) {
		hooks, finish := tracer.NewReplayTracer(hash)
		tx := replay.Transaction{CosmosIndex: ctx.TxIndex(), MessageIndex: ctx.MsgIndex(), Hash: hash}
		return hooks, func(result *evmtypes.EVMResult, err error) {
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
