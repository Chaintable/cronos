//go:build rocksdb

package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"cosmossdk.io/log/v2"
	proto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/cosmos/cosmos-sdk/baseapp"
	"github.com/cosmos/cosmos-sdk/client/flags"
	storetypes "github.com/cosmos/cosmos-sdk/store/v2/types"
	"github.com/crypto-org-chain/cronos/app"
	"github.com/crypto-org-chain/cronos/cmd/cronosd/opendb"
	"github.com/evmos/ethermint/debank/replay"
	"github.com/spf13/viper"
)

// RunReplayWorker executes one request in a fresh process. App construction
// initializes process-global codecs, so constructing a second app inside the
// serving process would race with transaction decoding.
func RunReplayWorker(home string, input io.Reader, output io.Writer) error {
	var in replay.Request
	if err := json.NewDecoder(input).Decode(&in); err != nil {
		return err
	}
	if in.Request == nil || in.Request.Height < 2 || len(in.ParentHash) != 32 {
		return fmt.Errorf("invalid historical finalize request")
	}
	// Upgrade metadata and keeper scratch files belong to the replay.
	scratch, err := os.MkdirTemp("", "cronos-replay-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(scratch)
	db, err := opendb.OpenReplayDB(home, filepath.Join(scratch, "secondary"))
	if err != nil {
		return fmt.Errorf("open parent application.db: %w", err)
	}
	defer db.Close()
	opts := viper.New()
	opts.Set(flags.FlagHome, scratch)
	opts.Set(flags.FlagChainID, in.ChainID)
	opts.Set(app.FlagDisableOptimisticExecution, true)
	app.SetConfig()
	a := app.New(log.NewNopLogger(), db, false, opts, baseapp.SetChainID(in.ChainID), baseapp.SetIAVLDisableFastNode(true))
	parent := in.Request.Height - 1
	if err = a.LoadHeight(parent); err != nil {
		return fmt.Errorf("open parent multistore at %d: %w", parent, err)
	}
	id := a.LastCommitID()
	if id.Version != parent || !bytes.Equal(id.Hash, in.ParentHash) {
		return fmt.Errorf("parent commit at %d: got %d/%X, want %X", parent, id.Version, id.Hash, in.ParentHash)
	}
	if err = a.RefreshBlockList(a.NewUncachedContext(false, proto.Header{})); err != nil {
		return err
	}
	// FinalizeBlock computes WorkingHash on the private IAVL trees. Commit
	// would persist them and is deliberately never called here.
	keys := []storetypes.StoreKey{a.GetKey("acc"), a.GetKey("bank"), a.GetKey("evm")}
	a.CommitMultiStore().AddListeners(keys)
	txs := app.ObserveReplayTransactions(a.EvmKeeper)
	res, err := a.FinalizeBlock(in.Request)
	if err != nil {
		return err
	}
	if err := verifyReplayChanges(a, in.Request.Height, keys); err != nil {
		return err
	}
	return replay.WriteResult(output, replay.Result{Finalize: res, Transactions: *txs})
}
