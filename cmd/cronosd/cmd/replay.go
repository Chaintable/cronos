//go:build rocksdb

package cmd

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"cosmossdk.io/log"
	"github.com/bytedance/sonic"
	proto "github.com/cometbft/cometbft/proto/tendermint/types"
	dbm "github.com/cosmos/cosmos-db"
	"github.com/cosmos/cosmos-sdk/client/flags"
	"github.com/crypto-org-chain/cronos/v2/app"
	"github.com/crypto-org-chain/cronos/v2/cmd/cronosd/opendb"
	"github.com/evmos/ethermint/debank/replay"
	"github.com/spf13/viper"
)

// RunReplayWorker serves replay requests on a unix socket until stdin closes,
// which happens when the parent process exits. One secondary database handle
// and its block cache are shared; every request builds a fresh app over it,
// because a sealed BaseApp cannot be reloaded at another height without Commit.
func RunReplayWorker(home, socket string, stdin io.Reader, stderr io.Writer) error {
	workers, err := replayIntEnv("CRONOS_REPLAY_WORKERS", 4, 1)
	if err != nil {
		return err
	}
	cacheMB, err := replayIntEnv("CRONOS_REPLAY_BLOCK_CACHE_MB", 1024, 8)
	if err != nil {
		return err
	}
	// A static archive has nothing to catch up with; skip it for backfills.
	catchUpEnabled, err := replayBoolEnv("CRONOS_REPLAY_CATCH_UP", true)
	if err != nil {
		return err
	}
	// Each request builds a fresh app, so the worker trades memory for less GC.
	gogc, err := replayIntEnv("CRONOS_REPLAY_GOGC", 400, 1)
	if err != nil {
		return err
	}
	debug.SetGCPercent(gogc)
	scratch, err := os.MkdirTemp("", "cronos-replay-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(scratch)
	db, catchUp, err := opendb.OpenReplayDB(home, filepath.Join(scratch, "secondary"), uint64(cacheMB)<<20)
	if err != nil {
		return fmt.Errorf("open parent application.db: %w", err)
	}
	defer db.Close()
	app.SetConfig()

	listener, err := net.Listen("unix", socket)
	if err != nil {
		return err
	}
	defer listener.Close()
	go func() {
		_, _ = io.Copy(io.Discard, stdin)
		listener.Close()
	}()

	if !catchUpEnabled {
		catchUp = nil
	}
	w := &replayWorker{db: sharedDB{db}, catchUp: catchUp, scratch: scratch, slots: make(chan struct{}, workers), log: stderr}
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		conn, err := listener.Accept()
		if err != nil {
			return nil
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer conn.Close()
			w.serve(conn)
		}()
	}
}

type replayWorker struct {
	db      dbm.DB
	catchUp func() error // nil when catch-up is disabled
	mu      sync.Mutex   // serializes catch-up with the primary
	scratch string
	slots   chan struct{}
	log     io.Writer
	logMu   sync.Mutex
}

// sharedDB keeps the handle open when a per-request app is discarded.
type sharedDB struct{ dbm.DB }

func (sharedDB) Close() error { return nil }

func (w *replayWorker) serve(conn net.Conn) {
	var req replay.Request
	if err := sonic.ConfigStd.NewDecoder(conn).Decode(&req); err != nil {
		_ = replay.WriteError(conn, err)
		return
	}
	w.slots <- struct{}{}
	defer func() { <-w.slots }()
	timing := newWorkerTiming()
	res, err := w.replay(req, timing)
	if err != nil {
		_ = replay.WriteError(conn, err)
		return
	}
	if err = replay.WriteResult(conn, *res); err != nil {
		return
	}
	timing.mark("write")
	w.logMu.Lock()
	timing.report(w.log, req.Request.Height)
	w.logMu.Unlock()
}

func (w *replayWorker) replay(in replay.Request, timing *workerTiming) (res *replay.Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("replay panic: %v", r)
		}
	}()
	if in.Request == nil || in.Request.Height < 2 || len(in.ParentHash) != 32 {
		return nil, fmt.Errorf("invalid historical finalize request")
	}
	if w.catchUp != nil {
		w.mu.Lock()
		err = w.catchUp()
		w.mu.Unlock()
		if err != nil {
			return nil, fmt.Errorf("catch up with application.db: %w", err)
		}
	}
	timing.mark("catch_up")
	// Upgrade metadata and keeper scratch files belong to this request.
	home, err := os.MkdirTemp(w.scratch, "app-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(home)
	opts := viper.New()
	opts.Set(flags.FlagHome, home)
	opts.Set(flags.FlagChainID, in.ChainID)
	// No block executor is set: the observer requires sequential execution.
	// Optimistic execution only starts from ProcessProposal, never called here.
	a := app.NewReplayApp(log.NewNopLogger(), w.db, opts, in.ChainID, in.Request.Height)
	timing.mark("new_app")
	parent := in.Request.Height - 1
	if err = a.LoadReplayParent(parent); err != nil {
		return nil, fmt.Errorf("open parent multistore at %d: %w", parent, err)
	}
	id := a.LastCommitID()
	if id.Version != parent || !bytes.Equal(id.Hash, in.ParentHash) {
		return nil, fmt.Errorf("parent commit at %d: got %d/%X, want %X", parent, id.Version, id.Hash, in.ParentHash)
	}
	if err = a.RefreshBlockList(a.NewUncachedContext(false, proto.Header{})); err != nil {
		return nil, err
	}
	timing.mark("load_height")
	// FinalizeBlock computes WorkingHash on the private IAVL trees. Commit
	// would persist them and is deliberately never called here.
	txs := app.ObserveReplayTransactions(a.EvmKeeper)
	finalized, err := a.FinalizeBlock(in.Request)
	if err != nil {
		return nil, err
	}
	timing.mark("finalize")
	return &replay.Result{Finalize: finalized, Transactions: *txs}, nil
}

func replayIntEnv(key string, fallback, minimum int) (int, error) {
	raw, set := os.LookupEnv(key)
	if !set {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minimum {
		return 0, fmt.Errorf("invalid %s=%q: expected integer >= %d", key, raw, minimum)
	}
	return value, nil
}

func replayBoolEnv(key string, fallback bool) (bool, error) {
	raw, set := os.LookupEnv(key)
	if !set {
		return fallback, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("invalid %s=%q: expected true or false", key, raw)
	}
	return value, nil
}

// workerTiming reports phase milliseconds on stderr for the parent's log.
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

func (t *workerTiming) report(w io.Writer, height int64) {
	fmt.Fprintln(w, "replay worker timing", "height="+strconv.FormatInt(height, 10), strings.Join(t.parts, " "))
}
