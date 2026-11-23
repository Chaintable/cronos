//go:build rocksdb

package cmd

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cosmossdk.io/log/v2"
	"github.com/bytedance/sonic"
	proto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/cosmos/cosmos-sdk/baseapp"
	"github.com/cosmos/cosmos-sdk/client/flags"
	"github.com/crypto-org-chain/cronos/app"
	"github.com/crypto-org-chain/cronos/cmd/cronosd/opendb"
	"github.com/evmos/ethermint/debank/replay"
	"github.com/spf13/viper"
)

// RunReplayWorker executes one request in a fresh process. App construction
// initializes process-global codecs, so constructing a second app inside the
// serving process would race with transaction decoding.
func RunReplayWorker(home string, input io.Reader, output io.Writer) error {
	timing := newWorkerTiming()
	defer timing.report(os.Stderr)
	var in replay.Request
	if err := sonic.ConfigStd.NewDecoder(input).Decode(&in); err != nil {
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
	timing.mark("decode")
	db, err := opendb.OpenReplayDB(home, filepath.Join(scratch, "secondary"))
	if err != nil {
		return fmt.Errorf("open parent application.db: %w", err)
	}
	timing.mark("open_db")
	defer db.Close()
	opts := viper.New()
	opts.Set(flags.FlagHome, scratch)
	opts.Set(flags.FlagChainID, in.ChainID)
	opts.Set(app.FlagDisableOptimisticExecution, true)
	app.SetConfig()
	a := app.New(log.NewNopLogger(), db, false, opts, baseapp.SetChainID(in.ChainID), baseapp.SetIAVLDisableFastNode(true))
	timing.mark("new_app")
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
	timing.mark("load_height")
	// FinalizeBlock computes WorkingHash on the private IAVL trees. Commit
	// would persist them and is deliberately never called here.
	txs := app.ObserveReplayTransactions(a.EvmKeeper)
	res, err := a.FinalizeBlock(in.Request)
	if err != nil {
		return err
	}
	timing.mark("finalize")
	if err = replay.WriteResult(output, replay.Result{Finalize: res, Transactions: *txs}); err != nil {
		return err
	}
	timing.mark("write")
	return nil
}

// workerTiming reports phase milliseconds on stderr for the caller's log.
type workerTiming struct {
	last  time.Time
	parts []string
}

func newWorkerTiming() *workerTiming { return &workerTiming{last: time.Now()} }

func (t *workerTiming) mark(name string) {
	now := time.Now()
	t.parts = append(t.parts, fmt.Sprintf("%s_ms=%d", name, now.Sub(t.last).Milliseconds()))
	t.last = now
}

func (t *workerTiming) report(w io.Writer) {
	fmt.Fprintln(w, "replay worker timing", strings.Join(t.parts, " "))
}
