package cmd

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
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
	"github.com/evmos/ethermint/debank/replay"
	"github.com/spf13/viper"
)

type replayWorker struct {
	db       dbm.DB
	catchUp  func() error // nil when catch-up is disabled
	mu       sync.Mutex   // serializes catch-up with the primary
	scratch  string
	slots    chan struct{}
	log      io.Writer
	logMu    sync.Mutex
	sessions *replaySessions
}

// sharedDB keeps the handle open when a per-request app is discarded.
type sharedDB struct{ dbm.DB }

func (sharedDB) Close() error { return nil }

func (w *replayWorker) serve(conn net.Conn) {
	r := bufio.NewReader(conn)
	prefix, err := r.Peek(1)
	if err != nil {
		return
	}
	var segment *replay.Segment
	var height int64
	if prefix[0] == 'S' {
		header, err := readSegmentHeader(r)
		if err != nil {
			_ = replay.WriteError(conn, err)
			return
		}
		if header.Drop {
			_ = app.WriteReplaySegmentAck(conn, w.sessions.drop(header.Segment, header.Height))
			return
		}
		segment, height = &header.Segment, header.Height
	}
	var req replay.Request
	err = sonic.ConfigStd.NewDecoder(r).Decode(&req)
	if err == nil && segment != nil {
		err = checkSegmentRequest(req, height)
	}
	if err != nil {
		_ = replay.WriteError(conn, err)
		return
	}
	// A segment connection doubles as its verification lease. Observe a
	// disconnect while queued and release an unacknowledged app on timeout.
	var decision chan bool
	if segment != nil {
		if err := conn.SetReadDeadline(time.Now().Add(w.sessions.timeout)); err != nil {
			return
		}
		decision = make(chan bool, 1)
		go func() {
			decision <- app.ReadReplaySegmentDecision(r) == nil
		}()
	}
	select {
	case w.slots <- struct{}{}:
	case <-decision:
		return
	}
	defer func() { <-w.slots }()
	session, err := w.sessions.acquire(segment, req)
	if err != nil {
		_ = replay.WriteError(conn, err)
		return
	}
	// Any failure, including a disconnect before verification, invalidates
	// the tentative state. Only a positive ACK can retain this app.
	released := false
	defer func() {
		if !released {
			w.sessions.release(session, req, nil, false)
		}
	}()

	timing := newWorkerTiming()
	reused := session.app != nil
	var res *replay.Result
	if segment == nil {
		res, err = w.replay(req, timing)
	} else {
		res, err = w.replaySegment(session, req, timing)
	}
	if err != nil {
		_ = replay.WriteError(conn, err)
		return
	}
	if err = replay.WriteResult(conn, *res); err != nil {
		return
	}
	timing.mark("write")
	if segment != nil {
		if !<-decision {
			return
		}
		w.sessions.release(session, req, res.Finalize.AppHash, true)
		released = true
		if err := app.WriteReplaySegmentAck(conn, nil); err != nil {
			return
		}
		timing.parts = append(timing.parts, fmt.Sprintf("segment_start=%d segment_end=%d segment_reused=%t", segment.Start, segment.End, reused))
	}
	w.logMu.Lock()
	timing.report(w.log, req.Request.Height)
	if nodes, ok := w.db.(*replayNodeCache); ok && req.Request.Height%1000 == 0 {
		nodes.report(w.log)
	}
	w.logMu.Unlock()
}

// replaySegment opens the segment's app at the parent on first use and then
// executes and commits each block on it.
func (w *replayWorker) replaySegment(s *replaySession, req replay.Request, timing *workerTiming) (res *replay.Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("replay panic: %v", r)
		}
	}()
	if req.Request == nil || req.Request.Height < 2 || len(req.ParentHash) != 32 {
		return nil, fmt.Errorf("invalid historical finalize request")
	}
	if s.app == nil {
		if err = w.catchUpPrimary(); err != nil {
			return nil, err
		}
		timing.mark("catch_up")
		if s.home, err = os.MkdirTemp(w.scratch, "app-"); err != nil {
			return nil, err
		}
		if s.app, err = app.OpenReplayApp(w.db, s.home, req.ChainID, req.Request.Height-1); err != nil {
			return nil, err
		}
		timing.mark("load_height")
	}
	res, err = s.app.ReplayNextBlock(req)
	timing.mark("finalize")
	return res, err
}

func (w *replayWorker) catchUpPrimary() error {
	if w.catchUp == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.catchUp(); err != nil {
		return fmt.Errorf("catch up with application.db: %w", err)
	}
	return nil
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
	if err = w.catchUpPrimary(); err != nil {
		return nil, err
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
	opts.Set(app.FlagDisableOptimisticExecution, true)
	// No block executor is set: the observer requires sequential execution.
	a := app.NewReplayApp(log.NewNopLogger(), w.db, opts, in.ChainID)
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
