package app

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/cosmos/cosmos-sdk/baseapp"
	"github.com/cosmos/cosmos-sdk/client/flags"
	pruningtypes "github.com/cosmos/cosmos-sdk/store/pruning/types"
	"github.com/cosmos/cosmos-sdk/store/rootmulti"
	storetypes "github.com/cosmos/cosmos-sdk/store/types"

	dbm "github.com/cometbft/cometbft-db"
	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/libs/log"
	tmstate "github.com/cometbft/cometbft/proto/tendermint/state"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/evmos/ethermint/debank/replay"
	"github.com/evmos/ethermint/debank/tracer"
	evmkeeper "github.com/evmos/ethermint/x/evm/keeper"
	evmtypes "github.com/evmos/ethermint/x/evm/types"
	"github.com/spf13/viper"
)

// ReplayChainID, ReplayFirstHeight and ReplayLastHeight bound the mainnet
// blocks whose execution rules this binary carries. ReplayFirstHeight is the
// Titan upgrade block.
const (
	ReplayChainID     = "cronosmainnet_25-1"
	ReplayFirstHeight = 13184000
	ReplayLastHeight  = 13519999
)

// ReplayExecutor runs replays in the long-lived `cronosd replay-worker` child.
type ReplayExecutor struct {
	home    string
	logger  log.Logger
	process replayProcess
}

var _ replay.Executor = (*ReplayExecutor)(nil)

// NewReplayExecutor replays on the archive whose databases are in home/data.
func NewReplayExecutor(home string, logger log.Logger) *ReplayExecutor {
	return &ReplayExecutor{home: home, logger: logger.With("module", "replay")}
}

func (e *ReplayExecutor) CheckReplayHeight(chainID string, height int64) error {
	if chainID != ReplayChainID || height < ReplayFirstHeight || height > ReplayLastHeight {
		return fmt.Errorf("unsupported execution rules for %s at %d", chainID, height)
	}
	return nil
}

// BlockFileOnly reads CRONOS_REPLAY_OUTPUT: "full" (default) or "blockfile",
// which returns only the block file and skips state-diff work.
func (e *ReplayExecutor) BlockFileOnly() (bool, error) {
	switch mode := os.Getenv("CRONOS_REPLAY_OUTPUT"); mode {
	case "", "full":
		return false, nil
	case "blockfile":
		return true, nil
	default:
		return false, fmt.Errorf("invalid CRONOS_REPLAY_OUTPUT %q (expected full or blockfile)", mode)
	}
}

func (e *ReplayExecutor) ReplayBlock(ctx context.Context, req replay.Request) (*replay.Result, error) {
	if req.BeginBlock == nil {
		return nil, fmt.Errorf("missing begin block request")
	}
	if err := e.CheckReplayHeight(req.ChainID, req.BeginBlock.Header.Height); err != nil {
		return nil, err
	}
	start := time.Now()
	result, err := e.process.call(ctx, e.home, e.logger, req)
	if err != nil {
		return nil, fmt.Errorf("replay worker: %w", err)
	}
	e.logger.Debug("replay finished", "height", req.BeginBlock.Header.Height, "total_ms", time.Since(start).Milliseconds())
	return result, nil
}

// ReplayOnDB executes the requested block on a fresh app over db at the
// parent height. Writes, including the commit that yields the AppHash, stay
// in an in-memory overlay that is discarded afterwards. A sealed BaseApp
// cannot be reloaded at another height, so every request builds its own app.
func ReplayOnDB(db dbm.DB, home string, req replay.Request, mark func(string)) (*replay.Result, error) {
	if req.BeginBlock == nil || req.BeginBlock.Header.Height < 2 || len(req.ParentHash) != 32 {
		return nil, fmt.Errorf("invalid historical replay request")
	}
	opts := viper.New()
	opts.Set(flags.FlagHome, home)
	opts.Set(flags.FlagChainID, req.ChainID)
	parent := req.BeginBlock.Header.Height - 1
	parentDB, err := replay.OpenParentState(db, parent)
	if err != nil {
		return nil, err
	}
	defer parentDB.Close()
	upgrade := req.BeginBlock.Header.Height == ReplayFirstHeight
	full := os.Getenv("CRONOS_REPLAY_OUTPUT") != "blockfile"
	var writes *replay.StateWrites
	if full {
		writes = replay.NewStateWrites("basecro")
	}
	overlay := parentDB
	logger := log.NewNopLogger()
	a := New(logger, overlay, nil, false, true, map[int64]bool{}, home, 0, MakeEncodingConfig(), opts, baseapp.SetChainID(req.ChainID),
		// Loading a version by scanning every saved root is far too slow on an archive.
		func(b *baseapp.BaseApp) {
			cms := rootmulti.NewStore(overlay, logger)
			cms.SetLazyLoading(true)
			cms.SetIAVLDisableFastNode(true)
			cms.SetPruning(pruningtypes.NewPruningOptions(pruningtypes.PruningNothing))
			if full {
				cms.SetInterBlockCache(writes)
			}
			if upgrade {
				b.SetCMS(upgradeLoadingStore{cms, &titanStoreUpgrades})
			} else {
				b.SetCMS(cms)
			}
		},
	)
	mark("new_app")

	if err := a.LoadHeight(parent); err != nil {
		return nil, fmt.Errorf("open parent multistore at %d: %w", parent, err)
	}
	if id := a.LastCommitID(); id.Version != parent || !bytes.Equal(id.Hash, req.ParentHash) {
		return nil, fmt.Errorf("parent commit at %d: got %d/%X, want %X", parent, id.Version, id.Hash, req.ParentHash)
	}
	mark("load_height")
	var before storetypes.CacheMultiStore
	cms := a.CommitMultiStore()
	if full {
		before, err = cms.CacheMultiStoreWithVersion(parent)
		if err != nil {
			return nil, err
		}
	}
	obs := ObserveReplayTransactions(a.EvmKeeper)
	res := &tmstate.ABCIResponses{DeliverTxs: make([]*abci.ResponseDeliverTx, len(req.Txs))}
	begin := a.BeginBlock(*req.BeginBlock)
	res.BeginBlock = &begin
	for i, tx := range req.Txs {
		obs.cosmos, obs.msg = i, 0
		r := a.DeliverTx(abci.RequestDeliverTx{Tx: tx})
		res.DeliverTxs[i] = &r
		obs.settle(&r)
	}
	end := a.EndBlock(abci.RequestEndBlock{Height: req.BeginBlock.Header.Height})
	res.EndBlock = &end
	mark("execute")
	commit := a.Commit()
	mark("commit")
	out := &replay.Result{Responses: res, AppHash: commit.Data, Transactions: obs.txs}
	if full {
		out.StateDiff, err = writes.Diff(a.AppCodec(), before, cms)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// upgradeLoadingStore loads the parent version with the upgrade block's store
// changes, so a new store starts at the upgrade height as on mainnet.
type upgradeLoadingStore struct {
	*rootmulti.Store
	upgrades *storetypes.StoreUpgrades
}

func (s upgradeLoadingStore) LoadVersion(ver int64) error {
	return s.LoadVersionAndUpgrade(ver, s.upgrades)
}

// replayObserver records one observation per signed EVM message in execution
// order. A trace is finished after its Cosmos transaction, whose block gas
// failure drops the message state.
type replayObserver struct {
	cosmos, msg int
	txs         []replay.Transaction
	pending     []func(*abci.ResponseDeliverTx)
}

// ObserveReplayTransactions attaches a collector to a private sequential keeper.
func ObserveReplayTransactions(k *evmkeeper.Keeper) *replayObserver {
	o := &replayObserver{}
	k.SetTransactionObserver(o.observe)
	return o
}

func (o *replayObserver) observe(hash common.Hash) (vm.EVMLogger, func(*evmtypes.MsgEthereumTxResponse, error)) {
	t, finish := tracer.NewReplayTracer(hash)
	index := len(o.txs)
	o.txs = append(o.txs, replay.Transaction{CosmosIndex: o.cosmos, MessageIndex: o.msg, Hash: hash})
	o.msg++
	return t, func(res *evmtypes.MsgEthereumTxResponse, err error) {
		o.pending = append(o.pending, func(r *abci.ResponseDeliverTx) {
			tx := &o.txs[index]
			// A panic inside the EVM unwinds past the named results; the
			// SDK recovers it and fails the Cosmos transaction.
			if res == nil && err == nil {
				err = fmt.Errorf("execution panicked (code %d)", r.Code)
			}
			if err == nil {
				tx.GasUsed = res.GasUsed
				tx.Failed = res.Failed()
				vmError := res.VmError
				exceeded := replay.ExceedsBlockGas(r)
				if exceeded && vmError == "" {
					vmError = replay.BlockGasExceeded
				}
				tx.Trace, err = finish(vmError)
				if exceeded {
					// The receipt has no logs; the dropped ones are error events.
					tx.Failed = true
				}
			}
			if err != nil {
				tx.Error = err.Error()
			}
		})
	}
}

// settle finishes the observations of the Cosmos transaction that returned r.
func (o *replayObserver) settle(r *abci.ResponseDeliverTx) {
	for _, f := range o.pending {
		f(r)
	}
	o.pending = nil
}
