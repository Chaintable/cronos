package app

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/bytedance/sonic"
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
	if chainID != "cronosmainnet_25-1" || height < 101485000 {
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

// ReplayBlock isolates the app constructor, keepers and IAVL working trees
// from consensus. CommandContext terminates outstanding work on cancellation.
func (app *App) ReplayBlock(ctx context.Context, req replay.Request) (*replay.Result, error) {
	if req.Request == nil {
		return nil, fmt.Errorf("missing finalize request")
	}
	if err := app.CheckReplayHeight(req.ChainID, req.Request.Height); err != nil {
		return nil, err
	}
	start := time.Now()
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	input, err := sonic.ConfigStd.Marshal(req)
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
	result, err := replay.ReadResult(bytes.NewReader(out))
	if err != nil {
		return nil, err
	}
	app.Logger().Debug("replay worker finished", "height", req.Request.Height,
		"total_ms", time.Since(start).Milliseconds(), "worker", strings.TrimSpace(diagnostic.String()))
	return result, nil
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
