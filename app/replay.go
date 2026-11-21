package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/evmos/ethermint/debank/replay"
	"github.com/evmos/ethermint/debank/tracer"
	"github.com/evmos/ethermint/x/evm/keeper"
	evmtypes "github.com/evmos/ethermint/x/evm/types"
)

// CheckReplayHeight selects the execution rules compiled into this binary.
func (app *App) CheckReplayHeight(chainID string, height int64) error {
	if chainID != "cronosmainnet_25-1" || height < 38432212 || height >= 45062800 {
		return fmt.Errorf("unsupported execution rules for %s at %d", chainID, height)
	}
	return nil
}

// ReplayBlock isolates the app constructor, keepers and IAVL working trees
// from consensus. CommandContext terminates outstanding work on cancellation.
func (app *App) ReplayBlock(ctx context.Context, req replay.Request) (*replay.Result, error) {
	if req.Request == nil {
		return nil, fmt.Errorf("missing finalize request")
	}
	if err := app.CheckReplayHeight(req.ChainID, req.Request.Height); err != nil {
		return nil, err
	}
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	input, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	temp, err := os.MkdirTemp("", "cronos-replay-worker-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(temp)
	cmd := exec.CommandContext(ctx, exe, "replay-finalize-worker", app.replayHome)
	cmd.Env = append(os.Environ(), "TMPDIR="+temp)
	cmd.Stdin = bytes.NewReader(input)
	var diagnostic bytes.Buffer
	cmd.Stderr = &diagnostic
	out, err := cmd.Output()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, fmt.Errorf("finalize worker: %w: %s", err, diagnostic.String())
	}
	return replay.ReadResult(bytes.NewReader(out))
}

// ObserveReplayTransactions attaches a collector to a private sequential keeper.
func ObserveReplayTransactions(k *keeper.Keeper) *[]replay.Transaction {
	var txs []replay.Transaction
	k.SetTransactionObserver(func(ctx sdk.Context, hash common.Hash) (*tracing.Hooks, func(*evmtypes.MsgEthereumTxResponse, error)) {
		hooks, finish := tracer.NewReplayTracer(hash)
		tx := replay.Transaction{CosmosIndex: ctx.TxIndex(), MessageIndex: ctx.MsgIndex(), Hash: hash}
		return hooks, func(result *evmtypes.MsgEthereumTxResponse, err error) {
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

// LoadReplayParent applies the activation store layout to the requested
// historical version before BaseApp initializes its working contexts.
func (app *App) LoadReplayParent(height int64) error {
	if height+1 != 38432212 {
		return app.LoadHeight(height)
	}
	if err := app.CommitMultiStore().LoadVersionAndUpgrade(height, activationStoreUpgrades()); err != nil {
		return err
	}
	return app.BaseApp.Init()
}
