//go:build replay
// +build replay

package cmd

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	dbm "github.com/cometbft/cometbft-db"

	"github.com/crypto-org-chain/cronos/v2/app"
	"github.com/crypto-org-chain/cronos/v2/cmd/cronosd/opendb"
	"github.com/evmos/ethermint/debank/replay"
)

// RunReplayWorker serves replay requests on a unix socket until stdin closes,
// which happens when the parent process exits. One read-only database handle
// and its block cache are shared by every request.
func RunReplayWorker(home, socket string, stdin io.Reader, stderr io.Writer) error {
	workers, err := replayIntEnv("CRONOS_REPLAY_WORKERS", 4, 1)
	if err != nil {
		return err
	}
	cacheMB, err := replayIntEnv("CRONOS_REPLAY_BLOCK_CACHE_MB", 1024, 8)
	if err != nil {
		return err
	}
	// A bounded table cache lets a small worker open a multi-terabyte archive.
	maxOpenFiles, err := replayIntEnv("CRONOS_REPLAY_MAX_OPEN_FILES", -1, 1)
	if err != nil {
		return err
	}
	// Trade memory for less GC while replay apps are executing.
	gogc, err := replayIntEnv("CRONOS_REPLAY_GOGC", 400, 1)
	if err != nil {
		return err
	}
	debug.SetGCPercent(gogc)
	scratch, err := ioutil.TempDir("", "cronos-replay-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(scratch)
	var useFilter bool
	if raw, set := os.LookupEnv("CRONOS_REPLAY_FILTER"); set {
		useFilter, err = strconv.ParseBool(raw)
		if err != nil {
			return fmt.Errorf("invalid CRONOS_REPLAY_FILTER=%q: expected a boolean", raw)
		}
	}
	db, err := opendb.OpenReadOnly(filepath.Join(home, "data"), "application", uint64(cacheMB)<<20, maxOpenFiles, useFilter)
	if err != nil {
		return fmt.Errorf("open parent application.db: %w", err)
	}
	defer db.Close()
	nodeCacheMB, err := replayIntEnv("CRONOS_REPLAY_NODE_CACHE_MB", 0, 0)
	if err != nil {
		return err
	}
	if nodeCacheMB > 0 {
		nodes, err := newReplayNodeCache(db, nodeCacheMB)
		if err != nil {
			return err
		}
		db = nodes
		defer nodes.cache.Reset()
		defer nodes.report(stderr)
	}
	app.SetConfig()

	listener, err := net.Listen("unix", socket)
	if err != nil {
		return err
	}
	defer listener.Close()
	go func() {
		_, _ = io.Copy(ioutil.Discard, stdin)
		listener.Close()
	}()

	timeoutSeconds, err := replayIntEnv("CRONOS_REPLAY_TIMEOUT_SECONDS", 600, 1)
	if err != nil {
		return err
	}
	w := &replayWorker{
		db: db, scratch: scratch, slots: make(chan struct{}, workers), log: stderr,
		sessions: newReplaySessions(workers, time.Duration(timeoutSeconds)*time.Second),
	}
	defer w.sessions.close()
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
	db       dbm.DB
	scratch  string
	slots    chan struct{}
	log      io.Writer
	logMu    sync.Mutex
	sessions *replaySessions
}

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
	req, err := replay.ReadRequest(r)
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
			w.sessions.release(session, req, false)
		}
	}()

	timing := newWorkerTiming()
	reused := session.app != nil
	// SDK execution cannot be cancelled safely in-process. Keep its deadline
	// active even if the RPC client disconnects. The parent replaces this
	// child on the next call and every unacknowledged height is replayed.
	var watchdog *time.Timer
	if segment != nil {
		watchdog = time.AfterFunc(w.sessions.timeout, func() {
			fmt.Fprintf(os.Stderr, "replay worker execution timed out at height %d\n", height)
			os.Exit(1)
		})
	}
	res, err := w.replay(session, req, timing)
	if watchdog != nil {
		watchdog.Stop()
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
		w.sessions.release(session, req, true)
		released = true
		if err := app.WriteReplaySegmentAck(conn, nil); err != nil {
			return
		}
		timing.parts = append(timing.parts, fmt.Sprintf("segment_start=%d segment_end=%d segment_reused=%t", segment.Start, segment.End, reused))
	}
	w.logMu.Lock()
	timing.report(w.log, req.BeginBlock.Header.Height)
	if req.BeginBlock.Header.Height%1000 == 0 {
		if nodes, ok := w.db.(*replayNodeCache); ok {
			nodes.report(w.log)
		}
	}
	w.logMu.Unlock()
}

func (w *replayWorker) replay(s *replaySession, req replay.Request, timing *workerTiming) (res *replay.Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("replay panic: %v", r)
		}
	}()
	if s.app == nil {
		s.home, err = ioutil.TempDir(w.scratch, "app-")
		if err != nil {
			return nil, err
		}
		s.app, err = app.OpenReplayApp(w.db, s.home, req.ChainID, req.BeginBlock.Header.Height-1, timing.mark)
		if err != nil {
			return nil, err
		}
	}
	return s.app.ReplayNextBlock(req, timing.mark)
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
